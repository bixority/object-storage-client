//! Command-line interface for the object storage client.
//!
//! Parses arguments with `clap` and dispatches each subcommand against a
//! [`Client`], plain or (with a keyring) encrypting. The longer
//! subcommands are delegated to helpers so the dispatch stays small and
//! readable. Not part of the public API: it is exported only so the binary can
//! reach it.

use crate::client::Error as StorageError;
#[cfg(feature = "encryption")]
use crate::encryption::EncryptionConfig;
#[cfg(feature = "encryption")]
use crate::encryption::keys::KeyError;
use crate::{Client, Codec, ObjectStorageClient, SignOptions};
#[cfg(feature = "encryption")]
use args::EncCommand;
use args::{Commands, ObjectCommand};
use futures::StreamExt;
use output::show_url;
use std::time::Duration;
use thiserror::Error;
use transfer::{get, get_stream, put};

// First, so that its macros are in scope in the modules after it.
#[macro_use]
mod output;
mod args;
#[cfg(feature = "encryption")]
mod enc;
#[cfg(feature = "encryption")]
mod keyring;
mod transfer;
mod tree;

pub use args::Cli;

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
    #[cfg(feature = "encryption")]
    #[error(transparent)]
    Key(#[from] KeyError),

    /// Pre-signed URLs cannot be made for encrypted objects.
    #[cfg(feature = "encryption")]
    #[error("sign is not available with a keyring")]
    SignWithKeys,

    /// Both `--keyring` (`OSC_KEYRING_FILE`) and `OSC_KEYRING_COMMAND` are set.
    #[cfg(feature = "encryption")]
    #[error("--keyring (OSC_KEYRING_FILE) and OSC_KEYRING_COMMAND are both set; use only one")]
    KeyringSourcesConflict,

    /// `OSC_KEYRING_COMMAND` could not be run or read.
    #[cfg(feature = "encryption")]
    #[error("OSC_KEYRING_COMMAND could not be run")]
    KeyringCommandStart(#[source] std::io::Error),

    /// `OSC_KEYRING_COMMAND` exited unsuccessfully (its output is discarded).
    #[cfg(feature = "encryption")]
    #[error("OSC_KEYRING_COMMAND failed: {0}")]
    KeyringCommandStatus(String),

    /// `OSC_KEYRING_COMMAND` printed more than 64 KiB.
    #[cfg(feature = "encryption")]
    #[error("OSC_KEYRING_COMMAND printed more than 64 KiB")]
    KeyringCommandTooLarge,

    /// `OSC_KEYRING_COMMAND` did not finish in time and was killed.
    #[cfg(feature = "encryption")]
    #[error("OSC_KEYRING_COMMAND did not finish within {} seconds", .0.as_secs())]
    KeyringCommandTimeout(Duration),

    /// `OSC_KEYRING_COMMAND` printed text that is not UTF-8.
    #[cfg(feature = "encryption")]
    #[error("OSC_KEYRING_COMMAND printed text that is not valid UTF-8")]
    KeyringCommandNotUtf8,

    /// Reading a keyring from standard input failed.
    #[cfg(feature = "encryption")]
    #[error("failed to read the keyring from standard input")]
    KeyringStdin(#[source] std::io::Error),

    /// The keyring on standard input is larger than 64 KiB.
    #[cfg(feature = "encryption")]
    #[error("the keyring on standard input is larger than 64 KiB")]
    KeyringInputTooLarge,

    /// The keyring on standard input is not UTF-8.
    #[cfg(feature = "encryption")]
    #[error("the keyring on standard input is not valid UTF-8")]
    KeyringInputNotUtf8,

    /// A keyring was about to be written to a terminal.
    #[cfg(feature = "encryption")]
    #[error("refusing to write a keyring to a terminal; redirect standard output or use --force")]
    KeyringToTerminal,

    /// `keyring status` without `--configured` on a terminal.
    #[cfg(feature = "encryption")]
    #[error(
        "keyring status reads a keyring from standard input; pipe one in, or pass --configured to check the configured key source"
    )]
    KeyringStatusTerminal,

    /// `keyring status --configured` with no key source.
    #[cfg(feature = "encryption")]
    #[error(
        "--configured needs a key source: use --keyring (OSC_KEYRING_FILE) or OSC_KEYRING_COMMAND"
    )]
    KeyringNotConfigured,

    /// `enc reencrypt` without a keyring.
    #[cfg(feature = "encryption")]
    #[error("enc reencrypt needs a keyring: use --keyring or OSC_KEYRING_COMMAND")]
    KeyringRequired,

    /// `enc reencrypt` left objects unfinished (details are in its output).
    #[cfg(feature = "encryption")]
    #[error(
        "re-encryption incomplete: {errors} failed, {changed} changed meanwhile, {after_cutoff} after the cutoff"
    )]
    ReencryptIncomplete {
        errors: usize,
        changed: usize,
        after_cutoff: usize,
    },

    /// `enc scan` could not read some objects, so its counts are incomplete.
    #[cfg(feature = "encryption")]
    #[error("scan incomplete: {unreadable} objects could not be read")]
    ScanIncomplete { unreadable: usize },

    /// `keyring status`: the active key is older than the limit.
    #[cfg(feature = "encryption")]
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
        #[cfg(feature = "encryption")]
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
    Ok(crate::registry::parse_url(s)?.into())
}

impl Cli {
    /// Sets the key source command (the value of `OSC_KEYRING_COMMAND`). Only
    /// the binary reads that variable; without the `encryption` feature this
    /// does nothing.
    #[doc(hidden)]
    #[must_use]
    #[cfg_attr(
        not(feature = "encryption"),
        expect(unused_mut, reason = "keys need the encryption feature")
    )]
    pub fn with_keyring_command(mut self, command: Option<std::ffi::OsString>) -> Self {
        #[cfg(feature = "encryption")]
        {
            self.keyring_command = command;
        }
        #[cfg(not(feature = "encryption"))]
        drop(command);
        self
    }

    /// Dispatch the parsed command against a fresh client.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying storage operation.
    pub async fn run(self) -> Result<(), CliError> {
        let Self {
            #[cfg(feature = "encryption")]
            keyring,
            #[cfg(feature = "encryption")]
            keyring_command,
            profile,
            require_encryption,
            command,
        } = self;
        #[cfg(feature = "encryption")]
        let keyring = keyring::KeySources {
            file: keyring,
            command: keyring_command,
        };
        // The only client built in the CLI: every arm must use it, so that
        // `--profile` reaches every command.
        let client = match profile {
            Some(profile) => ObjectStorageClient::new().with_s3_profile(profile)?,
            None => ObjectStorageClient::new(),
        };

        match command {
            Commands::Object(cmd) => {
                #[cfg(feature = "encryption")]
                if cmd.needs_keys() {
                    // Nothing is read when no source is configured.
                    match keyring::open(keyring).await? {
                        Some(ring) => {
                            let cfg = EncryptionConfig::new(ring);
                            return dispatch(&client.with_encryption(cfg), cmd).await;
                        }
                        None if require_encryption => return Err(CliError::EncryptionRequired),
                        None => {}
                    }
                } else {
                    // The keys are not read, but a conflicting pair of
                    // sources is still an error.
                    keyring::is_configured(keyring)?;
                }
                // This build has no keys at all.
                #[cfg(not(feature = "encryption"))]
                if require_encryption && cmd.needs_keys() {
                    return Err(CliError::EncryptionRequired);
                }
                dispatch(&client, cmd).await
            }

            Commands::Sign(args) => {
                #[cfg(feature = "encryption")]
                if keyring::is_configured(keyring)? {
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
                outln!("{signed}")?;
                Ok(())
            }

            #[cfg(feature = "encryption")]
            Commands::Keyring(cmd) => keyring::run(cmd, keyring).await,

            #[cfg(feature = "encryption")]
            Commands::Enc(EncCommand::Inspect { url }) => {
                // Inspection needs no keys: the keyring is not even read.
                let info =
                    crate::encryption::envelope::inspect_encryption_raw(&client, &to_url(&url)?)
                        .await?;
                print_info(&info)
            }

            #[cfg(feature = "encryption")]
            Commands::Enc(EncCommand::Scan {
                prefix,
                jobs,
                key,
                json,
            }) => enc::scan(&client, &prefix, usize::from(jobs), key.as_deref(), json).await,

            #[cfg(feature = "encryption")]
            Commands::Enc(EncCommand::Reencrypt {
                prefix,
                from,
                before,
                jobs,
                dry_run,
            }) => {
                let opts = crate::ReencryptOptions {
                    from,
                    before,
                    dry_run,
                };
                enc::reencrypt(&client, keyring, &prefix, opts, usize::from(jobs)).await
            }
        }
    }
}

async fn dispatch<C: Codec>(client: &Client<C>, cmd: ObjectCommand) -> Result<(), CliError> {
    match cmd {
        ObjectCommand::Cp { src, dst } => {
            let (src, dst) = (to_url(&src)?, to_url(&dst)?);
            outln!("Copy {} -> {}", show_url(&src), show_url(&dst))?;
            client.copy(&src, &dst).await?;
        }
        ObjectCommand::Mv { src, dst } => {
            let (src, dst) = (to_url(&src)?, to_url(&dst)?);
            outln!("Move {} -> {}", show_url(&src), show_url(&dst))?;
            client.move_object(&src, &dst).await?;
        }
        ObjectCommand::Put {
            src,
            dst,
            recursive,
        } => put(client, &src, &dst, recursive).await?,
        ObjectCommand::Get { src, dst } => get(client, &src, &dst).await?,
        ObjectCommand::GetStream { src, dst } => get_stream(client, &src, dst.as_deref()).await?,
        ObjectCommand::Ls { url } => {
            // Names print as they arrive; an error mid-listing fails the
            // command after what was listed.
            let mut names = client.list_stream(&to_url(&url)?)?;
            while let Some(item) = names.next().await {
                outln!("{}", sanitize(&item?))?;
            }
        }
        ObjectCommand::Rm { url } => {
            let url = to_url(&url)?;
            outln!("Delete {}", show_url(&url))?;
            client.delete(&url).await?;
        }
        ObjectCommand::Exists { url } => outln!("{}", client.exists(&to_url(&url)?).await?)?,
        ObjectCommand::Mb { url } => {
            let url = to_url(&url)?;
            outln!("Create bucket {}", show_url(&url))?;
            client.create_bucket(&url).await?;
        }
        ObjectCommand::BucketExists { url } => {
            outln!("{}", client.bucket_exists(&to_url(&url)?).await?)?;
        }
    }
    Ok(())
}

/// Prints one field per line. Header contents are unauthenticated, so
/// control characters are escaped before they reach a terminal.
#[cfg(feature = "encryption")]
fn print_info(info: &crate::EncryptionInfo) -> Result<(), CliError> {
    outln!("format: {}", sanitize(&info.format))?;
    outln!("message_id: {}", sanitize(&info.message_id))?;
    let names: Vec<String> = info.key_names.iter().map(|n| sanitize(n)).collect();
    outln!("key_names: {}", names.join(", "))?;
    outln!("frame_length: {}", info.frame_length)?;
    for (k, v) in &info.context {
        outln!("context.{}={}", sanitize(k), sanitize(v))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
