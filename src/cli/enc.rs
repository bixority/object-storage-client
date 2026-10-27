//! `osc enc scan` and `osc enc reencrypt`: walk a prefix and inspect or
//! rewrite each object, a bounded number at a time.

use super::{
    CliError, URL_PATH_SEGMENTS, is_unsafe_char, keyring, redact_url, sanitize, show_url, to_url,
};
use crate::client::Error as StorageError;
use crate::encryption::EncryptionConfig;
use crate::encryption::envelope::inspect_encryption_raw;
use crate::{Client, Codec, ObjectStorageClient, ReencryptOptions, Reencrypted};
use chrono::{DateTime, Utc};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use percent_encoding::utf8_percent_encode;
use std::collections::BTreeMap;
use std::path::PathBuf;

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

/// The URLs of the objects under `prefix`. A listing error comes as an item.
fn object_urls<C: Codec>(
    client: &Client<C>,
    prefix: &str,
) -> Result<BoxStream<'static, Result<String, StorageError>>, StorageError> {
    let mut root = url::Url::parse(&to_url(prefix))?;
    if !root.path().ends_with('/') {
        let path = format!("{}/", root.path());
        root.set_path(&path);
    }
    let names = client.list_stream(root.as_str())?;
    Ok(names
        .map_ok(move |name| {
            // The listed name is in the store's path form; each segment is
            // escaped so that it resolves back to the same object.
            let segments: Vec<String> = name
                .split('/')
                .map(|s| utf8_percent_encode(s, URL_PATH_SEGMENTS).to_string())
                .collect();
            let mut url = root.clone();
            let path = format!("{}{}", url.path(), segments.join("/"));
            url.set_path(&path);
            url.to_string()
        })
        .boxed())
}

/// What a header read made of an object.
enum Scanned {
    Encrypted(Vec<String>),
    NotEncrypted,
    Unreadable,
}

/// Counts of a scan, keyed by the labels the summary shows.
#[derive(Default)]
struct ScanCounts {
    keys: BTreeMap<String, usize>,
    no_key: usize,
    not_encrypted: usize,
    unreadable: usize,
}

pub(super) async fn scan(
    client: &ObjectStorageClient,
    prefix: &str,
    jobs: usize,
    key: Option<&str>,
    json: bool,
) -> Result<(), CliError> {
    let mut results = object_urls(client, prefix)?
        .map(|item| async move {
            let url = item?;
            let scanned = match inspect_encryption_raw(client, &url).await {
                Ok(info) => Scanned::Encrypted(info.key_names),
                Err(StorageError::NotEncrypted { .. }) => Scanned::NotEncrypted,
                Err(_) => Scanned::Unreadable,
            };
            Ok::<_, StorageError>((url, scanned))
        })
        .buffer_unordered(jobs);

    let mut counts = ScanCounts::default();
    let mut failed = None;
    while let Some(result) = results.next().await {
        let (url, scanned) = match result {
            Ok(done) => done,
            Err(e) => {
                failed = Some(e);
                break;
            }
        };
        let (ids, status) = match &scanned {
            Scanned::Encrypted(ids) => {
                if ids.is_empty() {
                    counts.no_key += 1;
                }
                for id in ids {
                    *counts.keys.entry(id.clone()).or_default() += 1;
                }
                (ids.as_slice(), "encrypted")
            }
            Scanned::NotEncrypted => {
                counts.not_encrypted += 1;
                (&[][..], "not-encrypted")
            }
            Scanned::Unreadable => {
                counts.unreadable += 1;
                (&[][..], "unreadable")
            }
        };
        if json {
            let ids: Vec<String> = ids.iter().map(|id| json_str(id)).collect();
            println!(
                "{{\"url\":{},\"key_ids\":[{}],\"status\":\"{status}\"}}",
                json_str(&redact_url(&url)),
                ids.join(",")
            );
        } else {
            if key.is_some_and(|k| ids.iter().any(|id| id == k)) {
                println!("{}", show_url(&url));
            }
            if status == "unreadable" {
                eprintln!("unreadable: {}", show_url(&url));
            }
        }
    }

    if json {
        let fixed = [
            ("no-key", counts.no_key),
            ("not-encrypted", counts.not_encrypted),
            ("unreadable", counts.unreadable),
        ];
        let keys = counts.keys.iter().map(|(k, n)| (k.as_str(), *n));
        println!("{}", json_counts(keys.chain(fixed)));
    } else if key.is_none() {
        print_table(&counts);
    }
    failed.map_or(Ok(()), |e| Err(e.into()))
}

fn print_table(counts: &ScanCounts) {
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
    let width = rows
        .iter()
        .map(|(label, _)| label.chars().count())
        .chain([6])
        .max()
        .unwrap_or(6);
    println!("{:<width$}  OBJECTS", "KEY_ID");
    for (label, n) in rows {
        println!("{label:<width$}  {n}");
    }
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
    keyring: Option<PathBuf>,
    prefix: &str,
    opts: ReencryptOptions,
    jobs: usize,
) -> Result<(), CliError> {
    let Some(ring) = keyring::open(keyring).await? else {
        return Err(CliError::KeyringRequired);
    };
    if ring.status().active == opts.from {
        // Refused once, not once per object.
        return Err(StorageError::InvalidInput(format!(
            "{} is the active key; rotate the keyring first",
            sanitize(&opts.from)
        ))
        .into());
    }
    let client = ObjectStorageClient::new().with_encryption(EncryptionConfig::new(ring));
    let (client, opts) = (&client, &opts);

    let mut results = object_urls(client, prefix)?
        .map(|item| async move {
            let url = item?;
            let outcome = client.reencrypt(&url, opts).await;
            Ok::<_, StorageError>((url, outcome))
        })
        .buffer_unordered(jobs);

    let mut counts = Outcomes::default();
    let mut failed = None;
    while let Some(result) = results.next().await {
        let (url, outcome) = match result {
            Ok(done) => done,
            Err(e) => {
                failed = Some(e);
                break;
            }
        };
        counts.count(&outcome);
        let url = json_str(&redact_url(&url));
        match outcome {
            Ok(done) => println!("{{\"url\":{url},\"outcome\":\"{}\"}}", done.as_str()),
            Err(e) => println!(
                "{{\"url\":{url},\"outcome\":\"error\",\"error\":{}}}",
                json_str(&e.to_string())
            ),
        }
    }
    println!(
        "{}",
        json_counts([
            ("rewritten", counts.rewritten),
            ("would-rewrite", counts.would_rewrite),
            ("skipped", counts.skipped),
            ("not-encrypted", counts.not_encrypted),
            ("after-cutoff", counts.after_cutoff),
            ("changed", counts.changed),
            ("error", counts.error),
        ])
    );
    if let Some(e) = failed {
        return Err(e.into());
    }
    if counts.error + counts.changed + counts.after_cutoff > 0 {
        return Err(CliError::ReencryptIncomplete {
            errors: counts.error,
            changed: counts.changed,
            after_cutoff: counts.after_cutoff,
        });
    }
    Ok(())
}
