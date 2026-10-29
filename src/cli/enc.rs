//! `osc enc scan` and `osc enc reencrypt`: walk a prefix and inspect or
//! rewrite each object, a bounded number at a time.

use super::output::{redact_url, sanitize, show_url};
use super::transfer::{run_weighted, weight};
use super::tree::URL_PATH_SEGMENTS;
use super::{CliError, keyring};
use crate::client::Error as StorageError;
use crate::encryption::EncryptionConfig;
use crate::encryption::envelope::inspect_encryption_raw;
use crate::encryption::keys::KeyError;
use crate::text::is_unsafe_char;
use crate::{Client, Codec, ObjectStorageClient, ReencryptOptions, Reencrypted};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use percent_encoding::utf8_percent_encode;
use std::collections::BTreeMap;
use tokio::sync::Semaphore;

/// Parses an RFC 3339 time for `--before`.
pub(super) fn parse_rfc3339(text: &str) -> Result<DateTime<Utc>, String> {
    DateTime::parse_from_rfc3339(text)
        .map(|t| t.with_timezone(&Utc))
        .map_err(|e| format!("not an RFC 3339 time (for example 2026-01-31T00:00:00Z): {e}"))
}

/// `text` as a JSON string literal. Control characters and the bidirectional
/// controls are escaped, so the line is safe on a terminal too.
fn json_str(text: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if is_unsafe_char(c) => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// A JSON object of counts, in the order given.
fn json_counts<'a>(counts: impl IntoIterator<Item = (&'a str, usize)>) -> String {
    let fields: Vec<String> = counts
        .into_iter()
        .map(|(name, n)| format!("{}:{n}", json_str(name)))
        .collect();
    format!("{{\"summary\":{{{}}}}}", fields.join(","))
}

/// An object's URL and its listed size in bytes.
type Listed = (String, u64);

/// The URLs and listed sizes of the objects under `prefix`. A listing error
/// comes as an item.
fn object_urls<C: Codec>(
    client: &Client<C>,
    prefix: &str,
) -> Result<BoxStream<'static, Result<Listed, StorageError>>, StorageError> {
    let mut root = crate::registry::parse_url(prefix)?;
    if !root.path().ends_with('/') {
        let path = format!("{}/", root.path());
        root.set_path(&path);
    }
    let names = client.list_meta_stream(root.as_str())?;
    Ok(names
        .map_ok(move |(name, size)| {
            // The listed name is in the store's path form; each segment is
            // escaped so that it resolves back to the same object.
            let segments: Vec<String> = name
                .split('/')
                .map(|s| utf8_percent_encode(s, URL_PATH_SEGMENTS).to_string())
                .collect();
            let mut url = root.clone();
            let path = format!("{}{}", url.path(), segments.join("/"));
            url.set_path(&path);
            (url.to_string(), size)
        })
        .boxed())
}

/// What a header read made of an object.
enum Scanned {
    Encrypted(Vec<String>),
    NotEncrypted,
    /// The header could not be read; the text says why.
    Unreadable(String),
}

/// Counts of a scan, keyed by the labels the summary shows.
#[derive(Default)]
struct ScanCounts {
    keys: BTreeMap<String, usize>,
    no_key: usize,
    not_encrypted: usize,
    unreadable: usize,
}

/// What a walk saw go wrong in the listing: how many errors, and the first.
#[derive(Default)]
pub(super) struct Listing {
    pub errors: usize,
    pub first: Option<StorageError>,
}

impl Listing {
    /// The first listing error, if there was one.
    fn into_result(self) -> Result<(), CliError> {
        self.first.map_or(Ok(()), |e| Err(e.into()))
    }
}

/// Hands every item of `results` to `on_item`. A listing error is counted and
/// the first kept, but the walk goes on, so the items already in flight
/// finish and are reported.
///
/// # Errors
///
/// What `on_item` returns, which stops the walk.
pub(super) async fn drain<T>(
    mut results: impl Stream<Item = Result<T, StorageError>> + Unpin,
    mut on_item: impl FnMut(T) -> Result<(), CliError>,
) -> Result<Listing, CliError> {
    let mut listing = Listing::default();
    while let Some(result) = results.next().await {
        match result {
            Ok(item) => on_item(item)?,
            Err(e) => {
                listing.errors += 1;
                listing.first.get_or_insert(e);
            }
        }
    }
    Ok(listing)
}

pub(super) async fn scan(
    client: &ObjectStorageClient,
    prefix: &str,
    jobs: usize,
    key: Option<&str>,
    json: bool,
) -> Result<(), CliError> {
    let results = object_urls(client, prefix)?
        .map(|item| async move {
            let (url, _) = item?;
            let scanned = match inspect_encryption_raw(client, &url).await {
                Ok(info) => Scanned::Encrypted(info.key_names),
                Err(StorageError::NotEncrypted { .. }) => Scanned::NotEncrypted,
                Err(e) => Scanned::Unreadable(e.to_string()),
            };
            Ok::<_, StorageError>((url, scanned))
        })
        .buffer_unordered(jobs);

    let mut counts = ScanCounts::default();
    let listing = drain(results, |(url, scanned)| {
        let (ids, status, error) = match &scanned {
            Scanned::Encrypted(ids) => {
                if ids.is_empty() {
                    counts.no_key += 1;
                }
                for id in ids {
                    *counts.keys.entry(id.clone()).or_default() += 1;
                }
                (ids.as_slice(), "encrypted", None)
            }
            Scanned::NotEncrypted => {
                counts.not_encrypted += 1;
                (&[][..], "not-encrypted", None)
            }
            Scanned::Unreadable(error) => {
                counts.unreadable += 1;
                (&[][..], "unreadable", Some(error))
            }
        };
        if json {
            let ids: Vec<String> = ids.iter().map(|id| json_str(id)).collect();
            let error = error.map_or_else(String::new, |e| format!(",\"error\":{}", json_str(e)));
            outln!(
                "{{\"url\":{},\"key_ids\":[{}],\"status\":\"{status}\"{error}}}",
                json_str(&redact_url(&url)),
                ids.join(",")
            )
        } else {
            if key.is_some_and(|k| ids.iter().any(|id| id == k)) {
                outln!("{}", show_url(&url))?;
            }
            if let Some(error) = error {
                errln!("unreadable: {}: {}", show_url(&url), sanitize(error));
            }
            Ok(())
        }
    })
    .await?;

    if json {
        let fixed = [
            ("no-key", counts.no_key),
            ("not-encrypted", counts.not_encrypted),
            ("unreadable", counts.unreadable),
            ("listing-errors", listing.errors),
        ];
        let keys = counts.keys.iter().map(|(k, n)| (k.as_str(), *n));
        outln!("{}", json_counts(keys.chain(fixed)))?;
    } else if key.is_none() {
        print_table(&counts, listing.errors)?;
    }
    // A scan is complete only when it exits 0.
    listing.into_result()?;
    if counts.unreadable > 0 {
        return Err(CliError::ScanIncomplete {
            unreadable: counts.unreadable,
        });
    }
    Ok(())
}

fn print_table(counts: &ScanCounts, listing_errors: usize) -> Result<(), CliError> {
    let mut rows: Vec<(String, usize)> = counts
        .keys
        .iter()
        .map(|(id, n)| (sanitize(id), *n))
        .collect();
    if counts.no_key > 0 {
        rows.push(("(no osc-file key)".to_owned(), counts.no_key));
    }
    rows.push(("(not encrypted)".to_owned(), counts.not_encrypted));
    rows.push(("(unreadable)".to_owned(), counts.unreadable));
    if listing_errors > 0 {
        rows.push(("(listing errors)".to_owned(), listing_errors));
    }
    let width = rows
        .iter()
        .map(|(label, _)| label.chars().count())
        .chain([6])
        .max()
        .unwrap_or(6);
    outln!("{:<width$}  OBJECTS", "KEY_ID")?;
    for (label, n) in rows {
        outln!("{label:<width$}  {n}")?;
    }
    Ok(())
}

/// How many objects ended each way.
#[derive(Default)]
struct Outcomes {
    rewritten: usize,
    would_rewrite: usize,
    skipped: usize,
    not_encrypted: usize,
    after_cutoff: usize,
    changed: usize,
    error: usize,
}

impl Outcomes {
    const fn count(&mut self, outcome: &Result<Reencrypted, StorageError>) {
        match outcome {
            Ok(Reencrypted::Rewritten) => self.rewritten += 1,
            Ok(Reencrypted::WouldRewrite) => self.would_rewrite += 1,
            Ok(Reencrypted::Skipped) => self.skipped += 1,
            Ok(Reencrypted::NotEncrypted) => self.not_encrypted += 1,
            Ok(Reencrypted::AfterCutoff) => self.after_cutoff += 1,
            Ok(Reencrypted::Changed) => self.changed += 1,
            Err(_) => self.error += 1,
        }
    }
}

pub(super) async fn reencrypt(
    client: &ObjectStorageClient,
    keyring: keyring::KeySources,
    prefix: &str,
    opts: ReencryptOptions,
    jobs: usize,
) -> Result<(), CliError> {
    let Some(ring) = keyring::open(keyring).await? else {
        return Err(CliError::KeyringRequired);
    };
    let status = ring.status();
    if status.active == opts.from {
        // Refused once, not once per object.
        return Err(StorageError::InvalidInput(format!(
            "{} is the active key; rotate the keyring first",
            sanitize(&opts.from)
        ))
        .into());
    }
    // Objects can only be rewritten from a key that the keyring can unwrap.
    if !status.keys.iter().any(|k| k.id == opts.from) {
        return Err(KeyError::UnknownId {
            id: opts.from.clone(),
        }
        .into());
    }
    let encrypting = client.with_encryption(EncryptionConfig::new(ring));
    let (client, opts) = (&encrypting, &opts);

    // A rewrite of a large object buffers several upload parts, so it draws
    // more of the `jobs` permits, as in `put -r`. A dry run uploads nothing.
    let budget = Semaphore::new(jobs);
    let results = object_urls(client, prefix)?
        .map(|item| {
            let budget = &budget;
            async move {
                let (url, size) = item?;
                let weight = if opts.dry_run { 1 } else { weight(size, jobs) };
                let outcome = run_weighted(budget, weight, client.reencrypt(&url, opts))
                    .await
                    .map_err(|e| StorageError::from(std::io::Error::other(e)))?;
                Ok::<_, StorageError>((url, outcome))
            }
        })
        .buffer_unordered(jobs);

    let mut counts = Outcomes::default();
    let listing = drain(results, |(url, outcome)| {
        counts.count(&outcome);
        let url = json_str(&redact_url(&url));
        match outcome {
            Ok(done) => outln!("{{\"url\":{url},\"outcome\":\"{}\"}}", done.as_str()),
            Err(e) => outln!(
                "{{\"url\":{url},\"outcome\":\"error\",\"error\":{}}}",
                json_str(&e.to_string())
            ),
        }
    })
    .await?;
    outln!(
        "{}",
        json_counts([
            ("rewritten", counts.rewritten),
            ("would-rewrite", counts.would_rewrite),
            ("skipped", counts.skipped),
            ("not-encrypted", counts.not_encrypted),
            ("after-cutoff", counts.after_cutoff),
            ("changed", counts.changed),
            ("error", counts.error),
            ("listing-errors", listing.errors),
        ])
    )?;
    listing.into_result()?;
    if counts.error + counts.changed + counts.after_cutoff > 0 {
        return Err(CliError::ReencryptIncomplete {
            errors: counts.error,
            changed: counts.changed,
            after_cutoff: counts.after_cutoff,
        });
    }
    Ok(())
}
