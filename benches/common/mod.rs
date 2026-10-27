//! Shared by the `throughput` bench and the `perf_guards` test: allocation
//! measurement and helpers for clients over `file://` directories.
#![allow(dead_code)]

use alloc_counter::Usage;
use bytes::Bytes;
use chrono::NaiveDate;
use futures::{Stream, StreamExt};
use object_storage_client::encryption::{EncryptionConfig, Keyring, new_keyring, rotate_keyring};
use object_storage_client::{EncryptedObjectStorageClient, ObjectStorageClient, Result};
use std::path::Path;
use std::sync::Arc;

pub const MIB: usize = 1 << 20;

/// Runs `f` and reports its allocations. The counters are global, so callers
/// measure one thing at a time per process.
pub async fn measure<T>(f: impl AsyncFnOnce() -> T) -> (T, Usage) {
    let base = alloc_counter::start();
    let out = f().await;
    (out, alloc_counter::finish(base))
}

fn config_for(text: &str) -> EncryptionConfig {
    EncryptionConfig::new(Arc::new(Keyring::parse(text).expect("parse")))
}

/// Two encrypted clients over keyrings with different active keys, so that a
/// copy between them re-encrypts: the second keyring also holds the first
/// client's key.
pub fn encrypted_pair(
    first: &str,
    second: &str,
) -> (EncryptedObjectStorageClient, EncryptedObjectStorageClient) {
    let day = NaiveDate::from_ymd_opt(2026, 10, 27).expect("date");
    let one = new_keyring(first, day).expect("keyring");
    let both = rotate_keyring(&one, second, day).expect("rotate");
    (
        EncryptedObjectStorageClient::new(config_for(&one)),
        EncryptedObjectStorageClient::new(config_for(&both)),
    )
}

pub fn plain() -> ObjectStorageClient {
    ObjectStorageClient::new()
}

pub fn url(dir: &Path, name: &str) -> String {
    format!("file://{}/{name}", dir.display())
}

/// `len` bytes of a repeating pattern.
pub fn payload(len: usize) -> Bytes {
    Bytes::from(
        (0..len)
            .map(|i| u8::try_from(i % 251).unwrap_or(0))
            .collect::<Vec<u8>>(),
    )
}

/// The data as `n`-byte chunks that share its buffer, made lazily.
pub fn chunked(data: &Bytes, n: usize) -> impl Stream<Item = Result<Bytes>> + Send + use<> {
    let data = data.clone();
    futures::stream::iter(
        (0..data.len())
            .step_by(n)
            .map(move |from| Ok(data.slice(from..data.len().min(from + n)))),
    )
}

/// Reads a stream to its end and returns the byte count.
pub async fn drain_len(mut s: impl Stream<Item = Result<Bytes>> + Unpin) -> usize {
    let mut n = 0;
    while let Some(c) = s.next().await {
        n += c.expect("chunk").len();
    }
    n
}
