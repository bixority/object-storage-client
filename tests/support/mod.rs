//! Helpers shared by the test crates; each crate uses a different subset.
#![allow(dead_code)]

use bytes::Bytes;
use futures::{Stream, StreamExt};
use std::path::Path;

pub mod mini_http;
#[cfg(feature = "cli")]
pub mod mock;

pub type TestResult = Result<(), Box<dyn std::error::Error>>;

/// `len` bytes of a repeating, non-trivial pattern.
pub fn pattern(len: usize) -> Vec<u8> {
    (0..len)
        .map(|i| u8::try_from(i % 251).unwrap_or(0))
        .collect()
}

/// An encryption config over the keyring in `text`.
#[cfg(feature = "encryption")]
pub fn config_for(text: &str) -> object_storage_client::encryption::EncryptionConfig {
    use object_storage_client::encryption::{EncryptionConfig, Keyring};
    EncryptionConfig::new(std::sync::Arc::new(Keyring::parse(text).expect("parse")))
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

    /// Runs the child to the end with a null stdin and captured output. Fails,
    /// instead of hanging, if it does not finish within `limit`.
    pub fn output_within(&mut self, limit: std::time::Duration) -> std::process::Output {
        use std::io::Read;
        use std::process::Stdio;
        use std::time::Instant;
        self.cmd
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = self.cmd.spawn().expect("spawn osc");
        // Drained on threads so a full pipe cannot stall the child.
        let mut out_pipe = child.stdout.take().expect("stdout");
        let mut err_pipe = child.stderr.take().expect("stderr");
        let out_reader = std::thread::spawn(move || {
            let mut v = Vec::new();
            let _ = out_pipe.read_to_end(&mut v);
            v
        });
        let err_reader = std::thread::spawn(move || {
            let mut v = Vec::new();
            let _ = err_pipe.read_to_end(&mut v);
            v
        });
        let deadline = Instant::now() + limit;
        let status = loop {
            if let Some(status) = child.try_wait().expect("try_wait") {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("osc did not finish within {limit:?}");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        std::process::Output {
            status,
            stdout: out_reader.join().expect("stdout reader"),
            stderr: err_reader.join().expect("stderr reader"),
        }
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
