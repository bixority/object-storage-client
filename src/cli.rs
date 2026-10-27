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
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};
use futures::{Stream, StreamExt, TryStreamExt};
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::Duration;
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::Semaphore;
use tokio_util::io::ReaderStream;

#[cfg(feature = "encryption")]
mod enc;
#[cfg(feature = "encryption")]
mod keyring;

/// Read and write buffer size for local files: the local store's read chunk,
/// and above the upload coalescing threshold, so uploaded chunks go into parts
/// without a copy.
const FILE_BUF: usize = 512 * 1024;

/// Files `put -r` uploads at once.
const FILES_IN_FLIGHT: usize = 8;

/// Size from which a file counts as large for `put -r` (one upload part).
const LARGE_FILE: u64 = 8 * 1024 * 1024;

/// Permits a large file holds, so at most `FILES_IN_FLIGHT / 4` run together.
const LARGE_FILE_WEIGHT: u32 = 4;

/// Waits for the cancelled uploads' clean-up (aborting multipart uploads and
/// removing staging files). Re-exported for the binary.
#[doc(hidden)]
pub use crate::upload::drain_pending_aborts;

/// Longest file name the common file systems accept, in bytes.
const NAME_MAX: usize = 255;

const fn is_unsafe_char(c: char) -> bool {
    c.is_control()
        || matches!(
            c,
            '\u{061C}'
                | '\u{200E}'
                | '\u{200F}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202A}'..='\u{202E}'
                | '\u{2066}'..='\u{2069}'
        )
}

/// Escapes control characters (C0, DEL, C1) and bidirectional controls as
/// `\u{..}`, so a name taken from a file system or a store cannot inject
/// terminal escape sequences or reorder the text around it.
///
/// For text printed to a terminal only; never for object bytes.
#[doc(hidden)]
#[must_use]
pub fn sanitize(text: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        if is_unsafe_char(c) {
            let _ = write!(out, "\\u{{{:x}}}", u32::from(c));
        } else {
            out.push(c);
        }
    }
    out
}

/// Removes what a URL carries beyond its location: the password (the user
/// name stays) and the query and fragment, which may hold tokens. A dropped
/// query or fragment leaves `?…` behind. Text that does not parse as a URL is
/// cut at the first `?` or `#` and loses any `user:password@` in its authority.
#[must_use]
pub(crate) fn redact_url(text: &str) -> String {
    if let Ok(mut url) = url::Url::parse(text) {
        let had_more = url.query().is_some() || url.fragment().is_some();
        let _ = url.set_password(None);
        url.set_query(None);
        url.set_fragment(None);
        let mut out = url.to_string();
        if had_more {
            out.push_str("?…");
        }
        return out;
    }
    let cut = text.find(['?', '#']);
    let head = cut.map_or(text, |i| &text[..i]);
    let head = match head.split_once("://") {
        Some((scheme, rest)) => {
            let (auth, path) = rest
                .split_once('/')
                .map_or((rest, None), |(a, p)| (a, Some(p)));
            let auth = auth.rsplit_once('@').map_or(auth, |(_, host)| host);
            path.map_or_else(
                || format!("{scheme}://{auth}"),
                |p| format!("{scheme}://{auth}/{p}"),
            )
        }
        None => head.to_string(),
    };
    if cut.is_some() {
        format!("{head}?…")
    } else {
        head
    }
}

/// A URL made safe for a status line: redacted, then escaped.
fn show_url(text: &str) -> String {
    sanitize(&redact_url(text))
}

/// Wraps an I/O failure on the local source `path`.
fn read_err(path: &Path) -> impl Fn(std::io::Error) -> CliError {
    |source| CliError::ReadFile {
        path: path.display().to_string(),
        source,
    }
}

/// A local path made safe for a status line.
fn show_path(path: &Path) -> String {
    sanitize(&path.display().to_string())
}

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

    /// `keyring status`: the active key is older than the limit.
    #[cfg(feature = "encryption")]
    #[error("the active key {id} is {age_days} days old, over the limit of {max_age_days} days")]
    KeyOverdue {
        id: String,
        age_days: i64,
        max_age_days: i64,
    },
}

impl CliError {
    /// The process exit code: 3 for an overdue key (`keyring status`), 1 for
    /// every other failure.
    #[doc(hidden)]
    #[must_use]
    pub const fn exit_code(&self) -> u8 {
        #[cfg(feature = "encryption")]
        if matches!(self, Self::KeyOverdue { .. }) {
            return keyring::EXIT_KEY_OVERDUE;
        }
        1
    }
}

#[derive(Parser)]
#[command(name = "osc")]
#[command(about = "Unified object storage client CLI", long_about = None)]
pub struct Cli {
    /// Keyring file; enables encryption. The environment variable
    /// `OSC_KEYRING_COMMAND` (a shell command that prints a keyring) is the
    /// alternative source; setting both is an error.
    #[cfg(feature = "encryption")]
    #[arg(long, global = true, env = "OSC_KEYRING_FILE", value_name = "FILE")]
    keyring: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(flatten)]
    Object(ObjectCommand),
    /// Generate a pre-signed URL (S3, GCS and Azure only).
    Sign(SignArgs),
    /// Create, rotate and inspect keyrings (standard input to standard output).
    #[cfg(feature = "encryption")]
    #[command(subcommand)]
    Keyring(keyring::KeyringCommand),
    /// Inspect and re-encrypt encrypted objects.
    #[cfg(feature = "encryption")]
    #[command(subcommand)]
    Enc(EncCommand),
}

#[derive(Args)]
struct SignArgs {
    url: String,
    /// HTTP method the URL authorizes: GET, PUT, POST, DELETE or HEAD.
    #[arg(short, long, default_value = "GET")]
    method: String,
    /// Validity duration in seconds.
    #[arg(short, long, default_value_t = 3600)]
    expires_in: u64,
    /// Bind a required Content-Length (bytes) into the signature (S3 only).
    #[arg(long)]
    content_length: Option<u64>,
    /// Bind a required Content-Type into the signature (S3 only).
    #[arg(long)]
    content_type: Option<String>,
}

/// Subcommands that work on objects and run against either client.
#[derive(Subcommand)]
enum ObjectCommand {
    Cp {
        src: String,
        dst: String,
    },
    Mv {
        src: String,
        dst: String,
    },
    Put {
        src: PathBuf,
        dst: String,
        /// Recursively upload a directory.
        #[arg(short, long)]
        recursive: bool,
    },
    Get {
        src: String,
        dst: PathBuf,
    },
    /// Stream an object to standard output; with FILE, behaves like `get`.
    GetStream {
        src: String,
        dst: Option<PathBuf>,
    },
    Ls {
        url: String,
    },
    Rm {
        url: String,
    },
    /// Check whether an object exists, printing `true` or `false`.
    Exists {
        url: String,
    },
    /// Create a bucket / container (or, for local paths, a directory).
    Mb {
        url: String,
    },
    /// Check whether a bucket / container exists, printing `true` or `false`.
    BucketExists {
        url: String,
    },
}

#[cfg(feature = "encryption")]
#[derive(Subcommand)]
enum EncCommand {
    /// Print the envelope details of an encrypted object. Needs no keys, and
    /// the output is not authenticated.
    Inspect { url: String },
    /// Count the objects under a prefix per key ID, from their headers. Needs
    /// no keys, and the key IDs are not authenticated.
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
    /// it was rewritten, or is after `--before`.
    Reencrypt {
        prefix: String,
        /// The key ID to move objects away from (not the active key).
        #[arg(long, value_name = "ID")]
        from: String,
        /// Leave objects last modified at or after this RFC 3339 time alone.
        #[arg(long, value_name = "TIME", value_parser = enc::parse_rfc3339)]
        before: Option<chrono::DateTime<chrono::Utc>>,
        /// Objects rewritten at once.
        #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..))]
        jobs: u16,
        /// Report what would be rewritten; write nothing.
        #[arg(long)]
        dry_run: bool,
    },
}

/// What stays unescaped when a relative file path is appended to a URL.
///
/// Applied to one path component at a time, so `/`, `\` and `%` are always
/// escaped.
const URL_PATH_SEGMENTS: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.');

fn to_url(s: &str) -> String {
    if s.contains("://") {
        s.to_string()
    } else {
        // Deliberately not canonicalized: the path becomes the bound `osc:key`
        // (D9), so it must resolve the same whether or not the file exists, as
        // in the library.
        let path = std::path::absolute(s).unwrap_or_else(|_| PathBuf::from(s));

        // Percent-encodes what a URL path cannot carry (`%`, `#`, `?`, ...).
        url::Url::from_file_path(&path).map_or_else(
            |()| format!("file:///{}", path.to_string_lossy()),
            String::from,
        )
    }
}

impl Cli {
    /// Dispatch the parsed command against a fresh client.
    ///
    /// # Errors
    ///
    /// Propagates any error returned by the underlying storage operation.
    pub async fn run(self) -> Result<(), CliError> {
        let Self {
            #[cfg(feature = "encryption")]
            keyring,
            command,
        } = self;
        let client = ObjectStorageClient::new();

        match command {
            Commands::Object(cmd) => {
                #[cfg(feature = "encryption")]
                if let Some(ring) = keyring::open(keyring).await? {
                    let cfg = EncryptionConfig::new(ring);
                    return dispatch(&client.with_encryption(cfg), cmd).await;
                }
                dispatch(&client, cmd).await
            }

            Commands::Sign(args) => {
                #[cfg(feature = "encryption")]
                if keyring::is_configured(keyring)? {
                    return Err(CliError::SignWithKeys);
                }
                let options = SignOptions {
                    content_length: args.content_length,
                    content_type: args.content_type,
                };
                let signed = client
                    .get_pre_signed_url(
                        &to_url(&args.url),
                        args.method.parse()?,
                        Duration::from_secs(args.expires_in),
                        &options,
                    )
                    .await?;
                println!("{signed}");
                Ok(())
            }

            #[cfg(feature = "encryption")]
            Commands::Keyring(cmd) => keyring::run(cmd, keyring).await,

            #[cfg(feature = "encryption")]
            Commands::Enc(EncCommand::Inspect { url }) => {
                // Inspection needs no keys: the keyring is not even read.
                let info =
                    crate::encryption::envelope::inspect_encryption_raw(&client, &to_url(&url))
                        .await?;
                print_info(&info);
                Ok(())
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
                enc::reencrypt(keyring, &prefix, opts, usize::from(jobs)).await
            }
        }
    }
}

async fn dispatch<C: Codec>(client: &Client<C>, cmd: ObjectCommand) -> Result<(), CliError> {
    match cmd {
        ObjectCommand::Cp { src, dst } => {
            let (src, dst) = (to_url(&src), to_url(&dst));
            println!("Copy {} -> {}", show_url(&src), show_url(&dst));
            client.copy(&src, &dst).await?;
        }
        ObjectCommand::Mv { src, dst } => {
            let (src, dst) = (to_url(&src), to_url(&dst));
            println!("Move {} -> {}", show_url(&src), show_url(&dst));
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
            let mut names = client.list_stream(&to_url(&url))?;
            while let Some(item) = names.next().await {
                println!("{}", sanitize(&item?));
            }
        }
        ObjectCommand::Rm { url } => {
            let url = to_url(&url);
            println!("Delete {}", show_url(&url));
            client.delete(&url).await?;
        }
        ObjectCommand::Exists { url } => println!("{}", client.exists(&to_url(&url)).await?),
        ObjectCommand::Mb { url } => {
            let url = to_url(&url);
            println!("Create bucket {}", show_url(&url));
            client.create_bucket(&url).await?;
        }
        ObjectCommand::BucketExists { url } => {
            println!("{}", client.bucket_exists(&to_url(&url)).await?);
        }
    }
    Ok(())
}

/// Prints one field per line. Header contents are unauthenticated, so
/// control characters are escaped before they reach a terminal.
#[cfg(feature = "encryption")]
fn print_info(info: &crate::EncryptionInfo) {
    println!("format: {}", sanitize(&info.format));
    println!("message_id: {}", sanitize(&info.message_id));
    let names: Vec<String> = info.key_names.iter().map(|n| sanitize(n)).collect();
    println!("key_names: {}", names.join(", "));
    println!("frame_length: {}", info.frame_length);
    for (k, v) in &info.context {
        println!("context.{}={}", sanitize(k), sanitize(v));
    }
}

async fn put<C: Codec>(
    client: &Client<C>,
    src: &Path,
    dst: &str,
    recursive: bool,
) -> Result<(), CliError> {
    let src_metadata = fs::metadata(src).map_err(read_err(src))?;

    if src_metadata.is_dir() {
        if !recursive {
            return Err(CliError::DirectoryRecursiveRequired {
                path: src.display().to_string(),
            });
        }

        let mut dst_root = to_url(dst);
        if !dst_root.ends_with('/') {
            dst_root.push('/');
        }

        println!(
            "Recursive upload {} -> {}",
            show_path(src),
            show_url(&dst_root)
        );
        upload_dir_recursive(client, src, &dst_root).await?;
    } else {
        let dst_url = to_url(dst);

        println!("Upload {} -> {}", show_path(src), show_url(&dst_url));
        let file = tokio::fs::File::open(src).await.map_err(read_err(src))?;
        put_file(client, file, &dst_url).await?;
    }

    Ok(())
}

/// Streams one local file to `dst_url`, so memory use does not grow with the
/// file size.
async fn put_file<C: Codec>(
    client: &Client<C>,
    file: tokio::fs::File,
    dst_url: &str,
) -> Result<(), CliError> {
    let meta = file.metadata().await.ok();
    // A `file://` destination keeps the source's permission bits, as a copy does.
    #[cfg(unix)]
    let source_mode = {
        use std::os::unix::fs::PermissionsExt;
        meta.as_ref().map(|m| m.permissions().mode() & 0o777)
    };
    #[cfg(not(unix))]
    let source_mode = None;
    let size = meta.as_ref().map(std::fs::Metadata::len);
    let chunks =
        ReaderStream::with_capacity(file, FILE_BUF).map(|chunk| chunk.map_err(StorageError::from));
    client
        .put_local_file(dst_url, chunks.boxed(), source_mode, size)
        .await?;
    Ok(())
}

/// Device and inode of a file found by the directory walk (Unix).
pub(crate) type FileId = (u64, u64);

/// Opens a file found by the recursive walk for upload, refusing anything that
/// is no longer the file the walk saw.
///
/// On Unix the final component is opened with `O_NOFOLLOW | O_NONBLOCK`, so a
/// symlink is refused and a FIFO cannot block the open. The opened handle must
/// then be a regular file with the device and inode recorded by the walk,
/// which also catches an intermediate directory swapped for a symlink. Without
/// a recorded `expected` identity (non-Unix) the file is opened as is.
///
/// # Errors
///
/// [`CliError::UnsafePath`] when the path changed since the walk,
/// [`CliError::ReadFile`] when it cannot be opened.
#[cfg(any(test, not(unix)))]
pub(crate) fn open_verified(
    path: &Path,
    expected: Option<FileId>,
) -> Result<tokio::fs::File, CliError> {
    let read_err = read_err(path);
    let changed = |reason| CliError::UnsafePath {
        path: path.display().to_string(),
        reason,
    };

    let mut opts = fs::OpenOptions::new();
    opts.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let flags = rustix::fs::OFlags::NOFOLLOW | rustix::fs::OFlags::NONBLOCK;
        opts.custom_flags(flags.bits().cast_signed());
    }
    let file = opts.open(path).map_err(|source| {
        #[cfg(unix)]
        if source.raw_os_error() == Some(rustix::io::Errno::LOOP.raw_os_error()) {
            return changed("path is now a symbolic link");
        }
        read_err(source)
    })?;

    let meta = file.metadata().map_err(read_err)?;
    if !meta.is_file() {
        return Err(changed("path is no longer a regular file"));
    }
    if expected.is_some() && platform::file_id(&meta) != expected {
        return Err(changed("file changed after the directory scan"));
    }
    Ok(tokio::fs::File::from_std(file))
}

/// Removes a file when dropped, unless [`Self::keep`] was called. Covers
/// errors and a cancelled download alike.
struct TempFile(Option<PathBuf>);

impl TempFile {
    fn keep(mut self) {
        self.0 = None;
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take() {
            let _ = fs::remove_file(path);
        }
    }
}

/// What differs by platform: file identity and owner.
#[cfg(unix)]
mod platform {
    #[cfg(test)]
    use super::FileId;
    use std::fs::Metadata;
    use std::os::unix::fs::MetadataExt;

    /// The identity of `meta`.
    #[cfg(test)]
    #[expect(clippy::unnecessary_wraps, reason = "`None` on other platforms")]
    pub fn file_id(meta: &Metadata) -> Option<FileId> {
        Some((meta.dev(), meta.ino()))
    }

    /// Permission bits and owner of the file being replaced.
    #[expect(clippy::unnecessary_wraps, reason = "`None` on other platforms")]
    pub fn mode_and_owner(meta: &Metadata) -> Option<(u32, u32)> {
        Some((meta.mode(), meta.uid()))
    }

    pub fn effective_uid() -> u32 {
        rustix::process::geteuid().as_raw()
    }
}

#[cfg(not(unix))]
mod platform {
    use super::FileId;
    use std::fs::Metadata;

    pub const fn file_id(_: &Metadata) -> Option<FileId> {
        None
    }

    pub const fn mode_and_owner(_: &Metadata) -> Option<(u32, u32)> {
        None
    }

    pub const fn effective_uid() -> u32 {
        0
    }
}

/// Decides the modes of the download temp file: the mode it is created with
/// and the mode it gets just before it replaces the destination.
///
/// The mode of a file we own is kept for both; anything else is a new file
/// ([`crate::fsmode::modes`] with no source): created private (0600), then
/// `0o666 & !umask`.
#[must_use]
pub(crate) fn download_modes(
    existing: Option<(u32, u32)>,
    euid: u32,
    umask: Option<u32>,
) -> (Option<u32>, Option<u32>) {
    crate::fsmode::owned_mode(existing, euid).map_or_else(
        || crate::fsmode::modes(None, euid, None, umask),
        |mode| (Some(mode), Some(mode)),
    )
}

/// Name of the download temp file for `name`: `<name>.osc-tmp-<suffix>`, with
/// the name part cut (on a character boundary) so the whole stays within
/// `NAME_MAX` bytes.
#[must_use]
pub(crate) fn temp_file_name(name: &std::ffi::OsStr, suffix: &str) -> std::ffi::OsString {
    let tail = format!(".osc-tmp-{suffix}");
    let mut name = name.to_string_lossy().into_owned();
    let room = NAME_MAX.saturating_sub(tail.len());
    if name.len() > room {
        name.truncate(name.floor_char_boundary(room));
    }
    name.push_str(&tail);
    name.into()
}

/// Creates the download temp file exclusively. With `mode` (Unix only) it is
/// created with at most those bits, so it is never more readable than the file
/// it replaces, not even before the data is written.
pub(crate) async fn create_temp_file(
    path: &Path,
    mode: Option<u32>,
) -> std::io::Result<tokio::fs::File> {
    let mut opts = tokio::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    if let Some(mode) = mode {
        opts.mode(mode & 0o777);
    }
    #[cfg(not(unix))]
    let _ = mode;
    opts.open(path).await
}

/// Downloads to a temporary file next to `dst` and renames it into place only
/// after the stream ended cleanly, so a failed or tampered download never
/// leaves a partial `dst`.
async fn get<C: Codec>(client: &Client<C>, src: &str, dst: &Path) -> Result<(), CliError> {
    let src_url = to_url(src);

    println!("Download {} -> {}", show_url(&src_url), show_path(dst));

    let write_err = |source| CliError::WriteFile {
        path: dst.display().to_string(),
        source,
    };

    let mut stream = client.get_stream(&src_url).await?;

    // Only a regular file (or absent path) can be replaced atomically. A FIFO,
    // device or `/dev/fd/N` is written through.
    let meta = tokio::fs::metadata(dst).await.ok();
    let direct = meta.as_ref().is_some_and(|m| !m.is_file());
    // A symlink is followed, so the temp file and the rename target are the
    // link's target.
    let dst = if meta.is_some() {
        tokio::fs::canonicalize(dst).await.map_err(write_err)?
    } else {
        dst.to_path_buf()
    };
    let dst = dst.as_path();

    let mut final_mode: Option<u32> = None;
    let (file, guard) = if direct {
        let file = tokio::fs::File::create(dst).await.map_err(write_err)?;
        (file, TempFile(None))
    } else {
        let name = dst.file_name().ok_or_else(|| {
            write_err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "no file name",
            ))
        })?;
        let mut suffix = [0u8; 8];
        aws_lc_rs::rand::fill(&mut suffix)
            .map_err(|_| write_err(std::io::Error::other("random number generator failed")))?;
        let tmp = dst.with_file_name(temp_file_name(name, &crate::hex::encode(&suffix)));

        // The mode of a file we own is kept; anything else starts private
        // (0600) and gets the default mode just before the rename, so a
        // partial plaintext is never readable by others.
        let (create_mode, mode) = download_modes(
            meta.as_ref().and_then(platform::mode_and_owner),
            platform::effective_uid(),
            crate::fsmode::current_umask(),
        );
        final_mode = mode;
        let file = create_temp_file(&tmp, create_mode)
            .await
            .map_err(write_err)?;
        let guard = TempFile(Some(tmp));
        (file, guard)
    };

    let mut out = BufWriter::with_capacity(FILE_BUF, file);
    write_stream(&mut stream, &mut out, write_err).await?;
    if let Some(tmp) = &guard.0 {
        out.get_ref().sync_all().await.map_err(write_err)?;
        #[cfg(unix)]
        if let Some(mode) = final_mode {
            use std::os::unix::fs::PermissionsExt;
            out.get_ref()
                .set_permissions(fs::Permissions::from_mode(mode))
                .await
                .map_err(write_err)?;
        }
        #[cfg(not(unix))]
        let _ = final_mode;
        drop(out);
        tokio::fs::rename(tmp, dst).await.map_err(write_err)?;
    }
    guard.keep();
    Ok(())
}

/// Stream an object to standard output, or to FILE exactly like `get`.
async fn get_stream<C: Codec>(
    client: &Client<C>,
    src: &str,
    dst: Option<&Path>,
) -> Result<(), CliError> {
    if let Some(dst) = dst {
        return get(client, src, dst).await;
    }
    let src_url = to_url(src);

    // Standard output carries the object bytes only.
    eprintln!("Streaming {}", show_url(&src_url));

    let mut stream = client.get_stream(&src_url).await?;
    let mut out = BufWriter::with_capacity(FILE_BUF, tokio::io::stdout());
    write_stream(&mut stream, &mut out, CliError::Output).await
}

/// Writes every chunk of `stream` to `out` and flushes it; `err` wraps a write
/// failure. A stream error (a failed or tampered read) is returned as is.
async fn write_stream<W: AsyncWrite + Unpin>(
    stream: &mut (impl Stream<Item = crate::Result<Bytes>> + Unpin),
    out: &mut BufWriter<W>,
    err: impl Fn(std::io::Error) -> CliError,
) -> Result<(), CliError> {
    while let Some(chunk) = stream.next().await {
        out.write_all(&chunk?).await.map_err(&err)?;
    }
    out.flush().await.map_err(err)?;
    Ok(())
}

/// A regular file found by [`SourceTree::scan`].
#[derive(Debug, Clone)]
pub(crate) struct TreeFile {
    /// Path relative to the scanned root.
    pub rel: PathBuf,
    /// Identity of the file itself.
    id: Option<FileId>,
    /// Identity of each directory between the root and the file, outermost
    /// first.
    #[cfg(unix)]
    dirs: Vec<FileId>,
}

/// A source directory tree scanned for a recursive upload.
///
/// On Unix the walk goes through directory file descriptors: the root is
/// opened once and held, every subdirectory is opened with `openat` relative to
/// its parent with `O_NOFOLLOW` and checked against the identity the listing
/// reported. [`Self::open`] later walks from the same root descriptor and
/// re-checks every directory on the chain, so swapping a directory for a
/// symlink at any point cannot redirect the upload outside the tree. Symlinks
/// inside the tree are skipped, as before.
#[derive(Debug)]
pub(crate) struct SourceTree {
    root_path: PathBuf,
    #[cfg(unix)]
    root: std::os::fd::OwnedFd,
}

/// Deepest directory nesting a scan follows; each level holds a descriptor.
#[cfg(unix)]
const MAX_TREE_DEPTH: usize = 128;

#[cfg(unix)]
const fn stat_id(st: &rustix::fs::Stat) -> FileId {
    #[allow(clippy::unnecessary_cast, clippy::cast_sign_loss)]
    (st.st_dev as u64, st.st_ino as u64)
}

#[cfg(unix)]
fn io_err(path: &Path, e: rustix::io::Errno) -> CliError {
    CliError::ReadFile {
        path: path.display().to_string(),
        source: e.into(),
    }
}

/// Maps the errors `O_NOFOLLOW | O_DIRECTORY` gives for a swapped-in symlink
/// or non-directory to a refusal, anything else to a read error.
#[cfg(unix)]
fn swapped_or_io(path: &Path, e: rustix::io::Errno) -> CliError {
    use rustix::io::Errno;
    if e == Errno::LOOP || e == Errno::NOTDIR {
        CliError::UnsafePath {
            path: path.display().to_string(),
            reason: "path changed after the directory scan",
        }
    } else {
        io_err(path, e)
    }
}

#[cfg(unix)]
impl SourceTree {
    /// Opens `root` (as named, so a symlink given by the user is followed) and
    /// lists the tree below it.
    ///
    /// # Errors
    ///
    /// [`CliError::ReadFile`] when a directory cannot be read,
    /// [`CliError::UnsafePath`] when one is swapped during the scan or the tree
    /// is nested too deep.
    pub fn scan(root: &Path) -> Result<(Self, Vec<TreeFile>), CliError> {
        Self::scan_with_hook(root, &mut |_| {})
    }

    /// [`Self::scan`] calling `hook` with a subdirectory's path after the
    /// listing reported it and before it is opened. Lets tests swap it
    /// deterministically.
    ///
    /// # Errors
    ///
    /// As [`Self::scan`].
    pub fn scan_with_hook(
        root: &Path,
        hook: &mut dyn FnMut(&Path),
    ) -> Result<(Self, Vec<TreeFile>), CliError> {
        use rustix::fs::{Mode, OFlags};
        let fd = rustix::fs::open(
            root,
            OFlags::DIRECTORY | OFlags::RDONLY | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| io_err(root, e))?;
        let mut files = Vec::new();
        let mut rel = PathBuf::new();
        let mut dirs = Vec::new();
        scan_dir(root, &fd, &mut rel, &mut dirs, &mut files, hook)?;
        Ok((
            Self {
                root_path: root.to_path_buf(),
                root: fd,
            },
            files,
        ))
    }

    /// The path shown for `file` in messages.
    #[must_use]
    pub fn display_path(&self, file: &TreeFile) -> PathBuf {
        self.root_path.join(&file.rel)
    }

    /// Opens `file` for reading, refusing anything that is no longer what the
    /// scan saw: a directory on the way replaced (by a symlink or anything
    /// else), the file replaced, or the file no longer a regular file.
    ///
    /// # Errors
    ///
    /// [`CliError::UnsafePath`] when the tree changed since the scan,
    /// [`CliError::ReadFile`] when it cannot be opened.
    pub fn open(&self, file: &TreeFile) -> Result<tokio::fs::File, CliError> {
        use rustix::fs::{Mode, OFlags, openat};
        use std::os::fd::{AsFd, OwnedFd};
        let path = self.display_path(file);
        let changed = |reason| CliError::UnsafePath {
            path: path.display().to_string(),
            reason,
        };
        let dir_flags = OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::RDONLY;

        let comps: Vec<_> = file.rel.components().collect();
        let (name, parents) = comps.split_last().ok_or_else(|| changed("path is empty"))?;
        if parents.len() != file.dirs.len() {
            return Err(changed("file changed after the directory scan"));
        }

        let mut held: Option<OwnedFd> = None;
        for (comp, id) in parents.iter().zip(&file.dirs) {
            let cur = held.as_ref().map_or_else(|| self.root.as_fd(), AsFd::as_fd);
            let next = openat(cur, comp.as_os_str(), dir_flags, Mode::empty())
                .map_err(|e| swapped_or_io(&path, e))?;
            let st = rustix::fs::fstat(&next).map_err(|e| io_err(&path, e))?;
            if stat_id(&st) != *id {
                return Err(changed("directory changed after the directory scan"));
            }
            held = Some(next);
        }

        let cur = held.as_ref().map_or_else(|| self.root.as_fd(), AsFd::as_fd);
        let fd = openat(
            cur,
            name.as_os_str(),
            OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|e| swapped_or_io(&path, e))?;
        let st = rustix::fs::fstat(&fd).map_err(|e| io_err(&path, e))?;
        if rustix::fs::FileType::from_raw_mode(st.st_mode) != rustix::fs::FileType::RegularFile {
            return Err(changed("path is no longer a regular file"));
        }
        if file.id.is_some() && Some(stat_id(&st)) != file.id {
            return Err(changed("file changed after the directory scan"));
        }
        Ok(tokio::fs::File::from_std(fs::File::from(fd)))
    }
}

/// Lists the directory held in `dir_fd` (shown as `dir_path`) and recurses into
/// its subdirectories, each opened relative to `dir_fd` without following
/// symlinks.
#[cfg(unix)]
fn scan_dir(
    dir_path: &Path,
    dir_fd: &std::os::fd::OwnedFd,
    rel: &mut PathBuf,
    dirs: &mut Vec<FileId>,
    files: &mut Vec<TreeFile>,
    hook: &mut dyn FnMut(&Path),
) -> Result<(), CliError> {
    use rustix::fs::{AtFlags, FileType, Mode, OFlags, openat, statat};
    use std::os::unix::ffi::OsStrExt;

    if dirs.len() >= MAX_TREE_DEPTH {
        return Err(CliError::UnsafePath {
            path: dir_path.display().to_string(),
            reason: "directory nesting is too deep",
        });
    }

    let mut subdirs = Vec::new();
    {
        let listing = rustix::fs::Dir::read_from(dir_fd).map_err(|e| io_err(dir_path, e))?;
        for entry in listing {
            let entry = entry.map_err(|e| io_err(dir_path, e))?;
            let raw = entry.file_name().to_bytes();
            if raw == b"." || raw == b".." {
                continue;
            }
            let name = std::ffi::OsStr::from_bytes(raw).to_os_string();
            let path = dir_path.join(&name);
            // Like `DirEntry::metadata`: a symlink is neither a file nor a
            // directory here and is skipped.
            let st = statat(dir_fd, name.as_os_str(), AtFlags::SYMLINK_NOFOLLOW)
                .map_err(|e| io_err(&path, e))?;
            match FileType::from_raw_mode(st.st_mode) {
                FileType::Directory => subdirs.push((name, stat_id(&st))),
                FileType::RegularFile => {
                    files.push(TreeFile {
                        rel: rel.join(&name),
                        id: Some(stat_id(&st)),
                        dirs: dirs.clone(),
                    });
                }
                _ => {}
            }
        }
    }

    for (name, id) in subdirs {
        let path = dir_path.join(&name);
        hook(&path);
        let fd = openat(
            dir_fd,
            name.as_os_str(),
            OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC | OFlags::RDONLY,
            Mode::empty(),
        )
        .map_err(|e| swapped_or_io(&path, e))?;
        let opened = stat_id(&rustix::fs::fstat(&fd).map_err(|e| io_err(&path, e))?);
        if opened != id {
            return Err(CliError::UnsafePath {
                path: path.display().to_string(),
                reason: "directory changed during the scan",
            });
        }
        rel.push(&name);
        dirs.push(id);
        let res = scan_dir(&path, &fd, rel, dirs, files, hook);
        dirs.pop();
        rel.pop();
        res?;
    }
    Ok(())
}

#[cfg(not(unix))]
impl SourceTree {
    /// Lists the tree below `root`. Without directory descriptors there is no
    /// identity check on directories; files are opened by path.
    ///
    /// # Errors
    ///
    /// [`CliError::ReadFile`] when a directory cannot be read.
    pub fn scan(root: &Path) -> Result<(Self, Vec<TreeFile>), CliError> {
        let read_err = |path: &Path, source| CliError::ReadFile {
            path: path.display().to_string(),
            source,
        };
        let mut files = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir).map_err(|e| read_err(&dir, e))? {
                let entry = entry.map_err(|e| read_err(&dir, e))?;
                let path = entry.path();
                let meta = entry.metadata().map_err(|e| read_err(&path, e))?;
                if meta.is_dir() {
                    stack.push(path);
                } else if meta.is_file() {
                    let rel = path
                        .strip_prefix(root)
                        .expect("path discovered via root must start with root")
                        .to_path_buf();
                    files.push(TreeFile {
                        rel,
                        id: platform::file_id(&meta),
                    });
                }
            }
        }
        Ok((
            Self {
                root_path: root.to_path_buf(),
            },
            files,
        ))
    }

    /// The path shown for `file` in messages.
    #[must_use]
    pub fn display_path(&self, file: &TreeFile) -> PathBuf {
        self.root_path.join(&file.rel)
    }

    /// Opens `file` for reading.
    ///
    /// # Errors
    ///
    /// As [`open_verified`].
    pub fn open(&self, file: &TreeFile) -> Result<tokio::fs::File, CliError> {
        open_verified(&self.display_path(file), file.id)
    }
}

/// Recursively upload the contents of `src_dir` to the destination prefix.
async fn upload_dir_recursive<C: Codec>(
    client: &Client<C>,
    src_dir: &Path,
    dst_root_url: &str,
) -> Result<(), CliError> {
    let root = src_dir.to_path_buf();
    let (tree, files) = tokio::task::spawn_blocking(move || SourceTree::scan(&root))
        .await
        .map_err(|e| CliError::ReadFile {
            path: src_dir.display().to_string(),
            source: std::io::Error::other(e),
        })??;
    let tree = &tree;

    // Files in flight at once, and the budget that large files draw on. A file
    // of at least one part buffers several parts while it uploads, so it takes
    // `LARGE_FILE_WEIGHT` of the `FILES_IN_FLIGHT` permits and only a couple of
    // them run together, which bounds memory to a few part buffers.
    let budget = Semaphore::new(FILES_IN_FLIGHT);

    // `try_collect` stops at the first failure: no further file is started and
    // the uploads still running are dropped, which aborts them. `main` then
    // waits for those aborts to finish.
    futures::stream::iter(files)
        .map(|file| {
            let budget = &budget;
            async move {
                let dst_url = destination_url(dst_root_url, &file.rel)?;
                let path = tree.display_path(&file);

                println!("Upload {} -> {}", show_path(&path), show_url(&dst_url));
                let source = tree.open(&file)?;
                let weight = match source.metadata().await {
                    Ok(meta) if meta.len() < LARGE_FILE => 1,
                    _ => LARGE_FILE_WEIGHT,
                };
                // The semaphore is never closed.
                let _permit =
                    budget
                        .acquire_many(weight)
                        .await
                        .map_err(|e| CliError::ReadFile {
                            path: path.display().to_string(),
                            source: std::io::Error::other(e),
                        })?;
                put_file(client, source, &dst_url).await
            }
        })
        .buffer_unordered(FILES_IN_FLIGHT)
        .try_collect::<()>()
        .await
}

/// Appends `rel_path` to the destination prefix `dst_root_url` (ending in `/`).
///
/// Only plain UTF-8 components are accepted and each is percent-encoded on its
/// own, so no file name can add, remove or reinterpret a path segment. The
/// parsed result must still lie under the prefix.
///
/// # Errors
///
/// [`CliError::UnsafePath`] for a non-normal or non-UTF-8 component, or a URL
/// that leaves the destination prefix.
pub(crate) fn destination_url(dst_root_url: &str, rel_path: &Path) -> Result<String, CliError> {
    let unsafe_path = |reason| CliError::UnsafePath {
        path: rel_path.display().to_string(),
        reason,
    };

    let mut encoded = Vec::new();
    for component in rel_path.components() {
        let Component::Normal(name) = component else {
            return Err(unsafe_path("path has a non-normal component"));
        };
        let name = name
            .to_str()
            .ok_or_else(|| unsafe_path("file name is not valid UTF-8"))?;
        encoded.push(utf8_percent_encode(name, URL_PATH_SEGMENTS).to_string());
    }
    if encoded.is_empty() {
        return Err(unsafe_path("path is empty"));
    }
    let dst_url = format!("{dst_root_url}{}", encoded.join("/"));

    let root = url::Url::parse(dst_root_url).map_err(|_| unsafe_path("invalid destination"))?;
    let parsed = url::Url::parse(&dst_url).map_err(|_| unsafe_path("invalid destination"))?;
    let root_path = if root.path().ends_with('/') {
        root.path().to_string()
    } else {
        format!("{}/", root.path())
    };
    let inside = parsed.scheme() == root.scheme()
        && parsed.host_str() == root.host_str()
        && parsed.port() == root.port()
        && parsed.path().starts_with(&root_path)
        && parsed.path().len() > root_path.len();
    if !inside {
        return Err(unsafe_path("destination leaves the target prefix"));
    }
    Ok(dst_url)
}

#[cfg(test)]
mod tests;
