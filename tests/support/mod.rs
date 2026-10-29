//! Helpers shared by the test crates; each crate uses a different subset.
#![allow(dead_code)]

use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::path::Path;

#[cfg(feature = "cli")]
pub mod mock;

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

/// The `osc` binary under test, run with a cleared environment: only `PATH`
/// (the keyring-command tests need `sh`) and an empty `HOME` are set, so
/// nothing of the developer's environment, proxies or `~/.aws` reaches it.
/// Callers add the variables they mean to set.
///
/// Dereferences to [`std::process::Command`]. Keep it alive until the child
/// exits: it owns the empty home directory.
#[cfg(feature = "cli")]
pub struct Osc {
    cmd: std::process::Command,
    home: tempfile::TempDir,
}

#[cfg(feature = "cli")]
impl Osc {
    pub fn new() -> Self {
        let home = tempfile::tempdir().expect("home");
        let mut cmd = std::process::Command::new(env!("CARGO_BIN_EXE_osc"));
        cmd.env_clear().env("HOME", home.path());
        // Not part of osc's behaviour: PATH finds `sh`, the profile file lets
        // a coverage run collect data from children.
        for var in ["PATH", "LLVM_PROFILE_FILE"] {
            if let Some(value) = std::env::var_os(var) {
                cmd.env(var, value);
            }
        }
        Self { cmd, home }
    }

    /// The empty `HOME` of the child.
    pub fn home(&self) -> &Path {
        self.home.path()
    }
}

#[cfg(feature = "cli")]
impl std::ops::Deref for Osc {
    type Target = std::process::Command;
    fn deref(&self) -> &Self::Target {
        &self.cmd
    }
}

#[cfg(feature = "cli")]
impl std::ops::DerefMut for Osc {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.cmd
    }
}
