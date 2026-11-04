//! Command-line interface for the object storage client.
//!
//! Parses arguments with `clap` and dispatches each subcommand against a
//! [`Client`], plain or (with a keyring) encrypting. The longer
//! subcommands are delegated to helpers so the dispatch stays small and
//! readable. Not part of the public API: it is exported only so the binary can
//! reach it.

use crate::client::Error as StorageError;
use crate::encryption::EncryptionConfig;
use crate::encryption::keys::KeyError;
use crate::{Client, Codec, ObjectStorageClient, SignOptions};
use args::{Commands, ObjectCommand};
use futures::StreamExt;
use output::{show_url, write_stdout};
use std::time::Duration;
use thiserror::Error;
use transfer::{get, get_stream, put};

// First, so that its macros are in scope in the modules after it.
#[macro_use]
mod output;
mod args;
mod enc;
#[cfg(unix)]
mod harden;
mod keyring;
mod transfer;
mod tree;

pub use args::Cli;
#[cfg(unix)]
#[doc(hidden)]
pub use harden::harden_process;
#[cfg(feature = "gui")]
pub(crate) use keyring::{KeySources, open as open_keyring, source as key_source};

/// The multi-thread runtime of `osc` and `osc-gui`. Re-exported for the
/// binaries.
///
/// One worker per core is more than a transfer can use on a big machine and
/// costs start-up time, so the count is capped at 4; an explicit
/// `TOKIO_WORKER_THREADS` still wins.
///
/// # Errors
///
/// The runtime could not be started.
#[doc(hidden)]
pub fn build_runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    if std::env::var_os("TOKIO_WORKER_THREADS").is_none() {
        builder.worker_threads(
            std::thread::available_parallelism()
                .map_or(1, usize::from)
                .min(4),
        );
    }
    builder.build()
}

/// Waits for the cancelled uploads' clean-up (aborting multipart uploads and
/// removing staging files). Re-exported for the binary.
#[doc(hidden)]
pub use crate::upload::drain_pending_aborts;

#[doc(hidden)]
pub use output::sanitize;
/// Errors surfaced by the `osc` command-line interface.
///
/// Storage failures are delegated to the library's [`StorageError`]; the
/// remaining variants attach the offending local path to an I/O failure so the
/// user can see which file could not be read, written or created.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum CliError {
    /// A failure from the underlying storage operation.
    #[error(transparent)]
    Storage(#[from] StorageError),

    /// The local source file for an upload could not be read.
    #[error("failed to read {path}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// A downloaded object could not be written to its local destination.
    #[error("failed to write {path}")]
    WriteFile {
        path: String,
        #[source]
        source: std::io::Error,
    },

    /// A directory was provided for upload without the recursive flag.
    #[error("directory {path} requires --recursive to upload")]
    DirectoryRecursiveRequired { path: String },

    /// A local path cannot be mapped to a destination key inside the target.
    #[error("refusing to upload {path}: {reason}")]
    UnsafePath { path: String, reason: &'static str },

    /// Writing streamed bytes to the output sink failed.
    #[error("failed to write output")]
    Output(#[source] std::io::Error),

    /// A background task panicked or was cancelled.
    #[error("internal error: a background task failed")]
    Task(#[source] tokio::task::JoinError),

    /// `--require-encryption` is set but no key source is configured.
    #[error(
        "--require-encryption is set but no key source is configured: use --keyring (OSC_KEYRING_FILE) or OSC_KEYRING_COMMAND"
    )]
    EncryptionRequired,

    /// A key-file problem.
    #[error(transparent)]
    Key(#[from] KeyError),

    /// Pre-signed URLs cannot be made for encrypted objects.
    #[error("sign is not available with a keyring")]
    SignWithKeys,

    /// Both `--keyring` (`OSC_KEYRING_FILE`) and `OSC_KEYRING_COMMAND` are set.
    #[error("--keyring (OSC_KEYRING_FILE) and OSC_KEYRING_COMMAND are both set; use only one")]
    KeyringSourcesConflict,

    /// `OSC_KEYRING_COMMAND` could not be run or read.
    #[error("OSC_KEYRING_COMMAND could not be run")]
    KeyringCommandStart(#[source] std::io::Error),

    /// `OSC_KEYRING_COMMAND` exited unsuccessfully (its output is discarded).
    #[error("OSC_KEYRING_COMMAND failed: {0}")]
    KeyringCommandStatus(String),

    /// `OSC_KEYRING_COMMAND` printed more than 64 KiB.
    #[error("OSC_KEYRING_COMMAND printed more than 64 KiB")]
    KeyringCommandTooLarge,

    /// `OSC_KEYRING_COMMAND` did not finish in time and was killed.
    #[error("OSC_KEYRING_COMMAND did not finish within {} seconds", .0.as_secs())]
    KeyringCommandTimeout(Duration),

    /// `OSC_KEYRING_COMMAND` printed text that is not UTF-8.
    #[error("OSC_KEYRING_COMMAND printed text that is not valid UTF-8")]
    KeyringCommandNotUtf8,

    /// Reading a keyring from standard input failed.
    #[error("failed to read the keyring from standard input")]
    KeyringStdin(#[source] std::io::Error),

    /// The keyring on standard input is larger than 64 KiB.
    #[error("the keyring on standard input is larger than 64 KiB")]
    KeyringInputTooLarge,

    /// The keyring on standard input is not UTF-8.
    #[error("the keyring on standard input is not valid UTF-8")]
    KeyringInputNotUtf8,

    /// A keyring was about to be written to a terminal.
    #[error("refusing to write a keyring to a terminal; redirect standard output or use --force")]
    KeyringToTerminal,

    /// `keyring status` without `--configured` on a terminal.
    #[error(
        "keyring status reads a keyring from standard input; pipe one in, or pass --configured to check the configured key source"
    )]
    KeyringStatusTerminal,

    /// `keyring add`, `activate`, `rotate` or `retire` with a terminal on standard input.
    #[error(
        "this keyring command reads a keyring from standard input; pipe one in, e.g. `< keyring`"
    )]
    KeyringStdinTerminal,

    /// `keyring status --configured` with no key source.
    #[error(
        "--configured needs a key source: use --keyring (OSC_KEYRING_FILE) or OSC_KEYRING_COMMAND"
    )]
    KeyringNotConfigured,

    /// `enc reencrypt` without a keyring.
    #[error("enc reencrypt needs a keyring: use --keyring or OSC_KEYRING_COMMAND")]
    KeyringRequired,

    /// `enc reencrypt` left objects unfinished (details are in its output).
    #[error(
        "re-encryption incomplete: {errors} failed, {changed} changed meanwhile, {after_cutoff} after the cutoff"
    )]
    ReencryptIncomplete {
        errors: usize,
        changed: usize,
        after_cutoff: usize,
    },

    /// `enc scan` could not read some objects, so its counts are incomplete.
    #[error("scan incomplete: {unreadable} objects could not be read")]
    ScanIncomplete { unreadable: usize },

    /// `keyring status`: the active key is older than the limit.
    #[error("the active key {id} is {age_days} days old, over the limit of {max_age_days} days")]
    KeyOverdue {
        id: String,
        age_days: i64,
        max_age_days: i64,
    },
}

/// Exit code when the output pipe is closed: 128 + SIGPIPE.
const EXIT_BROKEN_PIPE: u8 = 141;

impl CliError {
    /// The process exit code: 3 for an overdue key (`keyring status`), 141
    /// (128 + SIGPIPE, as for a tool killed by it) for a closed output pipe,
    /// 1 for every other failure.
    #[doc(hidden)]
    #[must_use]
    pub fn exit_code(&self) -> u8 {
        if matches!(self, Self::KeyOverdue { .. }) {
            return keyring::EXIT_KEY_OVERDUE;
        }
        if self.is_broken_pipe() {
            EXIT_BROKEN_PIPE
        } else {
            1
        }
    }

    /// Whether the output pipe was closed by its reader.
    #[doc(hidden)]
    #[must_use]
    pub fn is_broken_pipe(&self) -> bool {
        matches!(self, Self::Output(e) if e.kind() == std::io::ErrorKind::BrokenPipe)
    }
}

/// Resolves a URL or local path operand exactly as the library does.
fn to_url(s: &str) -> Result<String, CliError> {
    // The parser would resolve `..` and drop tabs; the library never sees them.
    crate::client::check_raw_url(s)?;
    Ok(crate::registry::parse_url(s)?.into())
}

/// The URL of the directory `prefix`, ending in `/`, to build child URLs on.
fn dir_root(prefix: &str) -> Result<url::Url, StorageError> {
    crate::client::check_raw_url(prefix)?;
    let mut root = crate::registry::parse_url(prefix)?;
    if !root.path().ends_with('/') {
        let path = format!("{}/", root.path());
        root.set_path(&path);
    }
    Ok(root)
}

/// The URL of the listed `name` under `root`. The name is in the store's path
/// form; each segment is escaped so that the URL resolves back to the same
/// object.
fn name_url(root: &url::Url, name: &str) -> String {
    crate::registry::child_url(root.as_str(), name.split('/'))
}

impl Cli {
    /// Sets the key source command (the value of `OSC_KEYRING_COMMAND`). Only
    /// the binary reads that variable.
    #[doc(hidden)]
    #[must_use]
    pub fn with_keyring_command(mut self, command: Option<std::ffi::OsString>) -> Self {
        self.keyring_command = command;
        self
    }

    /// Dispatch the parsed command against a fresh client.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying storage operation.
    pub async fn run(self) -> Result<(), CliError> {
        let Self {
            keyring,
            keyring_command,
            profile,
            require_encryption,
            command,
        } = self;
        let source = keyring::source(keyring::KeySources {
            file: keyring,
            command: keyring_command,
        })?;
        // The only client built in the CLI: every arm must use it, so that
        // `--profile` reaches every command.
        let client = match profile {
            Some(profile) => ObjectStorageClient::new().with_s3_profile(profile)?,
            None => ObjectStorageClient::new(),
        };

        match command {
            Commands::Object(cmd) => {
                if cmd.needs_keys() {
                    // Nothing is read when no source is configured.
                    match source {
                        Some(source) => {
                            let cfg = EncryptionConfig::new(keyring::open(source).await?);
                            return dispatch(&client.with_encryption(cfg), cmd).await;
                        }
                        None if require_encryption => return Err(CliError::EncryptionRequired),
                        None => {}
                    }
                }
                dispatch(&client, cmd).await
            }

            Commands::Sign(args) => {
                if source.is_some() {
                    return Err(CliError::SignWithKeys);
                }
                if require_encryption {
                    // A URL signed for an encrypted object cannot be made, and
                    // a plain one is what the flag refuses.
                    return Err(CliError::EncryptionRequired);
                }
                let options = SignOptions {
                    content_length: args.content_length,
                    content_type: args.content_type,
                };
                let signed = client
                    .get_pre_signed_url(
                        &to_url(&args.url)?,
                        args.method.parse()?,
                        Duration::from_secs(args.expires_in),
                        &options,
                    )
                    .await?;
                outln!("{signed}").await?;
                Ok(())
            }

            Commands::Keyring(cmd) => keyring::run(cmd, source).await,

            Commands::Enc(cmd) => enc::run(cmd, &client, source).await,
        }
    }
}

async fn dispatch<C: Codec>(client: &Client<C>, cmd: ObjectCommand) -> Result<(), CliError> {
    match cmd {
        ObjectCommand::Cp { src, dst } => {
            let (src, dst) = (to_url(&src)?, to_url(&dst)?);
            outln!("Copy {} -> {}", show_url(&src), show_url(&dst)).await?;
            client.copy(&src, &dst).await?;
        }
        ObjectCommand::Mv { src, dst } => {
            let (src, dst) = (to_url(&src)?, to_url(&dst)?);
            outln!("Move {} -> {}", show_url(&src), show_url(&dst)).await?;
            client.move_object(&src, &dst).await?;
        }
        ObjectCommand::Put {
            src,
            dst,
            recursive,
        } => put(client, &src, &dst, recursive).await?,
        ObjectCommand::Get { src, dst } => get(client, &src, &dst).await?,
        ObjectCommand::GetStream { src, dst } => get_stream(client, &src, dst.as_deref()).await?,
        ObjectCommand::Ls { url, url_output } => {
            // Names print as they arrive, a page or so at a time: one write
            // per batch of names that are ready, not one per name. An error
            // mid-listing fails the command after what was listed.
            let root = url_output.then(|| dir_root(&url)).transpose()?;
            let mut batches = client.list_stream(&to_url(&url)?)?.ready_chunks(1024);
            while let Some(batch) = batches.next().await {
                let mut text = String::new();
                let mut failed = None;
                for item in batch {
                    match item {
                        Ok(name) => {
                            match &root {
                                Some(root) => text.push_str(&sanitize(&name_url(root, &name))),
                                None => text.push_str(&sanitize(&name)),
                            }
                            text.push('\n');
                        }
                        Err(e) => {
                            failed = Some(e);
                            break;
                        }
                    }
                }
                if !text.is_empty() {
                    write_stdout(&text).await?;
                }
                if let Some(e) = failed {
                    return Err(e.into());
                }
            }
        }
        ObjectCommand::Rm { url } => {
            let url = to_url(&url)?;
            outln!("Delete {}", show_url(&url)).await?;
            client.delete(&url).await?;
        }
        ObjectCommand::Exists { url } => outln!("{}", client.exists(&to_url(&url)?).await?).await?,
        ObjectCommand::Mb { url } => {
            let url = to_url(&url)?;
            outln!("Create bucket {}", show_url(&url)).await?;
            client.create_bucket(&url).await?;
        }
        ObjectCommand::BucketExists { url } => {
            outln!("{}", client.bucket_exists(&to_url(&url)?).await?).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
