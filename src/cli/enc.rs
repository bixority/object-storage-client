//! `osc enc scan` and `osc enc reencrypt`: walk a prefix and inspect or
//! rewrite each object, a bounded number at a time.

use super::output::{out, sanitize, show_url};
use super::transfer::{run_weighted, weight};
use super::{CliError, keyring};
use crate::client::Error as StorageError;
use crate::codec::redact_url;
use crate::encryption::envelope::inspect_encryption_raw;
use crate::encryption::keys::KeyError;
use crate::encryption::{EncryptionConfig, Envelope};
use crate::text::is_unsafe_char;
use crate::{Client, Codec, ObjectStorageClient, ReencryptOptions, Reencrypted};
use chrono::{DateTime, Utc};
use clap::Subcommand;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use std::collections::BTreeMap;
use tokio::sync::Semaphore;

#[derive(Subcommand)]
pub(super) enum EncCommand {
    /// Print the envelope details of an encrypted object. Needs no keys, and
    /// the output is not authenticated.
    Inspect { url: String },
    /// Count the objects under a prefix per key ID, from their headers. Needs
    /// no keys, and the key IDs are not authenticated. The scan is complete
    /// only when it exits 0: unreadable objects or a listing error give exit
    /// 1 (a listing error names the offending key; remove that object with
    /// the provider's tools and run again).
    Scan {
        prefix: String,
        /// Objects inspected at once.
        #[arg(long, default_value_t = 16, value_parser = clap::value_parser!(u16).range(1..))]
        jobs: u16,
        /// Print only the URLs of the objects that name this key ID.
        #[arg(long, value_name = "ID", conflicts_with = "json")]
        key: Option<String>,
        /// One JSON object per object, then a summary object.
        #[arg(long)]
        json: bool,
    },
    /// Rewrite the objects under a prefix that are encrypted with key `--from`
    /// under the active key. Needs a keyring. Prints one JSON line per object
    /// and a summary; the exit code is 1 if any object failed, changed while
    /// it was rewritten, or is after `--before`, or if the listing failed.
    Reencrypt {
        prefix: String,
        /// The key ID to move objects away from (not the active key).
        #[arg(long, value_name = "ID")]
        from: String,
        /// Leave objects last modified at or after this RFC 3339 time alone
        /// (also objects whose store reports no time). The store's time is not
        /// authenticated: a provider or anyone who can set file times can
        /// backdate it, so check candidates against an independent record.
        #[arg(long, value_name = "TIME", value_parser = parse_rfc3339)]
        before: Option<chrono::DateTime<chrono::Utc>>,
        /// Objects rewritten at once. A rewrite of an object of 8 MiB or more
        /// holds about 40 MiB, so at most JOBS/4 (at least one) of those run
        /// together.
        #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..))]
        jobs: u16,
        /// Report what would be rewritten; write nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

/// `osc enc ...`.
pub(super) async fn run(
    cmd: EncCommand,
    client: &ObjectStorageClient,
    source: Option<keyring::Source>,
) -> Result<(), CliError> {
    match cmd {
        EncCommand::Inspect { url } => {
            // Inspection needs no keys: the keyring is not even read.
            let info = inspect_encryption_raw(client, &super::to_url(&url)?).await?;
            print_info(&info).await
        }
        EncCommand::Scan {
            prefix,
            jobs,
            key,
            json,
        } => scan(client, &prefix, usize::from(jobs), key.as_deref(), json).await,
        EncCommand::Reencrypt {
            prefix,
            from,
            before,
            jobs,
            dry_run,
        } => {
            let opts = ReencryptOptions {
                from,
                before,
                dry_run,
            };
            reencrypt(client, source, &prefix, opts, usize::from(jobs)).await
        }
    }
}

/// Prints one field per line. Header contents are unauthenticated, so
/// control characters are escaped before they reach a terminal.
async fn print_info(info: &crate::EncryptionInfo) -> Result<(), CliError> {
    outln!("format: {}", sanitize(&info.format)).await?;
    outln!("message_id: {}", sanitize(&info.message_id)).await?;
    let names: Vec<String> = info.key_names.iter().map(|n| sanitize(n)).collect();
    outln!("key_names: {}", names.join(", ")).await?;
    outln!("frame_length: {}", info.frame_length).await?;
    for (k, v) in &info.context {
        outln!("context.{}={}", sanitize(k), sanitize(v)).await?;
    }
    Ok(())
}

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
    let root = super::dir_root(prefix)?;
    let names = client.list_meta_stream(root.as_str())?;
    Ok(names
        .map_ok(move |(name, size)| (super::name_url(&root, &name), size))
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
pub(super) async fn drain<T, Fut: Future<Output = Result<(), CliError>>>(
    mut results: impl Stream<Item = Result<T, StorageError>> + Unpin,
    mut on_item: impl FnMut(T) -> Fut,
) -> Result<Listing, CliError> {
    let mut listing = Listing::default();
    while let Some(result) = results.next().await {
        match result {
            Ok(item) => on_item(item).await?,
            Err(e) => {
                listing.errors += 1;
                listing.first.get_or_insert(e);
            }
        }
    }
    Ok(listing)
}

/// Rewrites one object of listed `size`. A large object is first probed as a
/// dry run at one permit, and takes the large weight only when it will be
/// rewritten, so objects that are skipped do not wait for it. The probe's
/// permit is released before the weight is taken: holding both would deadlock.
async fn reencrypt_one(
    client: &Client<Envelope>,
    budget: &Semaphore,
    url: &str,
    size: u64,
    opts: &ReencryptOptions,
    probe: &ReencryptOptions,
    jobs: usize,
) -> Result<Reencrypted, StorageError> {
    let weight = if opts.dry_run { 1 } else { weight(size, jobs) };
    if weight > 1 {
        let outcome = run_weighted(budget, 1, client.reencrypt(url, probe)).await;
        if !matches!(outcome, Ok(Reencrypted::WouldRewrite)) {
            return outcome;
        }
    }
    run_weighted(budget, weight, client.reencrypt(url, opts)).await
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
            let (url, size) = item?;
            // An envelope message is never empty. Zero-byte entries include
            // console folder markers, which cannot be read as objects.
            if size == 0 {
                return Ok::<_, StorageError>((url, Scanned::NotEncrypted));
            }
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
        let line = if json {
            let ids: Vec<String> = ids.iter().map(|id| json_str(id)).collect();
            let error = error.map_or_else(String::new, |e| format!(",\"error\":{}", json_str(e)));
            Some(format!(
                "{{\"url\":{},\"key_ids\":[{}],\"status\":\"{status}\"{error}}}",
                json_str(&redact_url(&url)),
                ids.join(",")
            ))
        } else {
            let line = key
                .is_some_and(|k| ids.iter().any(|id| id == k))
                .then(|| show_url(&url));
            if let Some(error) = error {
                errln!("unreadable: {}: {}", show_url(&url), sanitize(error));
            }
            line
        };
        async move {
            match line {
                Some(line) => out(line).await,
                None => Ok(()),
            }
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
        outln!("{}", json_counts(keys.chain(fixed))).await?;
    } else if key.is_none() {
        print_table(&counts, listing.errors).await?;
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

async fn print_table(counts: &ScanCounts, listing_errors: usize) -> Result<(), CliError> {
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
    outln!("{:<width$}  OBJECTS", "KEY_ID").await?;
    for (label, n) in rows {
        outln!("{label:<width$}  {n}").await?;
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
    source: Option<keyring::Source>,
    prefix: &str,
    opts: ReencryptOptions,
    jobs: usize,
) -> Result<(), CliError> {
    let Some(source) = source else {
        return Err(CliError::KeyringRequired);
    };
    let ring = keyring::open(source).await?;
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
    let probe = ReencryptOptions {
        dry_run: true,
        ..opts.clone()
    };
    let results = object_urls(client, prefix)?
        .map(|item| {
            let (budget, probe) = (&budget, &probe);
            async move {
                let (url, size) = item?;
                // Zero-byte entries (console folder markers) hold no message.
                if size == 0 {
                    return Ok((url, Ok(Reencrypted::NotEncrypted)));
                }
                let outcome = reencrypt_one(client, budget, &url, size, opts, probe, jobs).await;
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
    )
    .await?;
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

#[cfg(test)]
mod tests;
