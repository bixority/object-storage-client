//! Helpers shared by the test crates; each crate uses a different subset.
#![allow(dead_code)]

use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::path::Path;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// `len` bytes of a repeating, non-trivial pattern.
pub fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// Sorted names of the entries of `dir`.
pub fn names(dir: &Path) -> Vec<String> {
    let mut all: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    all.sort();
    all
}

/// The data as a stream of `n`-byte chunks, which share the data's buffer.
pub fn chunked<E>(data: Bytes, n: usize) -> impl Stream<Item = Result<Bytes, E>> + Send + use<E> {
    futures::stream::iter(
        (0..data.len())
            .step_by(n)
            .map(move |from| Ok(data.slice(from..data.len().min(from + n)))),
    )
}

/// Polls `stream` to its end and counts the errors. Unlike [`drain`] it does
/// not stop at the first error, so it also shows that a failed stream ends.
pub async fn count_errors_to_end<T, E>(
    mut stream: impl Stream<Item = Result<T, E>> + Unpin,
) -> usize {
    let mut errors = 0;
    while let Some(item) = stream.next().await {
        errors += usize::from(item.is_err());
    }
    errors
}

/// Concatenates a stream's chunks, stopping at the first error.
pub async fn drain<E>(
    mut stream: impl Stream<Item = Result<Bytes, E>> + Unpin,
) -> Result<Vec<u8>, E> {
    let mut out = Vec::new();
    while let Some(chunk) = stream.next().await {
        out.extend_from_slice(&chunk?);
    }
    Ok(out)
}
