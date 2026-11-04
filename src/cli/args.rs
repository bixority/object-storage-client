//! The command-line grammar.

use super::keyring;
use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "osc", version)]
#[command(about = "Unified object storage client CLI", long_about = None)]
pub struct Cli {
    /// Keyring file; enables encryption. The environment variable
    /// `OSC_KEYRING_COMMAND` (a shell command that prints a keyring) is the
    /// alternative source; setting both is an error.
    #[arg(long, global = true, env = "OSC_KEYRING_FILE", value_name = "FILE")]
    pub(super) keyring: Option<PathBuf>,

    /// The shell command of `OSC_KEYRING_COMMAND`, set by the binary only
    /// ([`Cli::with_keyring_command`]), so that tests never read the
    /// environment.
    #[arg(skip)]
    pub(super) keyring_command: Option<std::ffi::OsString>,

    /// AWS profile for `s3://` credentials (static keys, SSO, ...). It is the
    /// only credential source: `S3_*` / `AWS_*` keys are not used, `AWS_PROFILE`
    /// is not read and `AWS_ENDPOINT_URL` is refused. A `credential_process`
    /// entry in the profile makes osc run that command.
    #[arg(long, global = true, value_name = "NAME")]
    pub(super) profile: Option<String>,

    /// Fail `put`, `get`, `get-stream`, `cp`, `mv` and `sign` unless a key
    /// source is configured, instead of sending or reading plaintext. Also set
    /// by `OSC_REQUIRE_ENCRYPTION` (`1`, `true`, `yes`, `on`; `0` and the like
    /// leave it off).
    #[arg(
        long,
        global = true,
        env = "OSC_REQUIRE_ENCRYPTION",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub(super) require_encryption: bool,

    #[command(subcommand)]
    pub(super) command: Commands,
}

#[derive(Subcommand)]
pub(super) enum Commands {
    #[command(flatten)]
    Object(ObjectCommand),
    /// Generate a pre-signed URL (S3, GCS and Azure only).
    Sign(SignArgs),
    /// Create, rotate and inspect keyrings (standard input to standard output).
    #[command(subcommand)]
    Keyring(keyring::KeyringCommand),
    /// Inspect and re-encrypt encrypted objects.
    #[command(subcommand)]
    Enc(super::enc::EncCommand),
}

#[derive(Args)]
pub(super) struct SignArgs {
    /// Object to sign for, for example `s3://bucket/key`.
    pub(super) url: String,
    /// HTTP method the URL authorizes: GET, PUT, POST, DELETE or HEAD.
    #[arg(
        short,
        long,
        default_value = "GET",
        ignore_case = true,
        value_parser = ["GET", "PUT", "POST", "DELETE", "HEAD"]
    )]
    pub(super) method: String,
    /// Validity in seconds, 1 to 604800 (7 days). An s3:// URL signed with
    /// temporary credentials stops working when that session ends, whatever
    /// this says.
    #[arg(short, long, default_value_t = 3600)]
    pub(super) expires_in: u64,
    /// Bind a required Content-Length (bytes) into the signature (S3 only).
    #[arg(long)]
    pub(super) content_length: Option<u64>,
    /// Bind a required Content-Type into the signature (S3 only).
    #[arg(long)]
    pub(super) content_type: Option<String>,
}

/// Subcommands that work on objects and run against either client.
#[derive(Subcommand)]
pub(super) enum ObjectCommand {
    /// Copy an object, within a store or between stores.
    Cp {
        /// Source URL or local path.
        src: String,
        /// Destination URL or local path.
        dst: String,
    },
    /// Move an object: copy it, then delete the source.
    Mv {
        /// Source URL or local path.
        src: String,
        /// Destination URL or local path.
        dst: String,
    },
    /// Upload a local file, or a directory with `--recursive`.
    Put {
        /// Local file or directory.
        src: PathBuf,
        /// Destination URL.
        dst: String,
        /// Recursively upload a directory.
        #[arg(short, long)]
        recursive: bool,
    },
    /// Download an object to a local file.
    Get {
        /// Source URL.
        src: String,
        /// Local file to write.
        dst: PathBuf,
    },
    /// Stream an object to standard output; with FILE, behaves like `get`.
    ///
    /// Standard output is complete only if osc exits 0. A failed read, or a
    /// truncated or tampered encrypted object, stops the output part way;
    /// every byte written was authenticated, but the object may be cut short.
    /// In a pipeline use set -o pipefail, or give FILE for an all-or-nothing
    /// download.
    GetStream {
        /// Source URL.
        src: String,
        /// Local file to write instead of standard output.
        dst: Option<PathBuf>,
    },
    /// List the objects under a URL, one name per line.
    ///
    /// Names are the literal names in the store. A name with `%` in it is not
    /// a URL: use --url to print URLs that address the listed objects, for
    /// example to pass them to `osc rm`.
    Ls {
        /// Bucket or prefix URL.
        url: String,
        /// Print the full URL of each object instead of its name.
        #[arg(long = "url", id = "url_output")]
        url_output: bool,
    },
    /// Delete an object. An http(s) URL that is a `WebDAV` collection is
    /// refused: deleting it would remove everything in it.
    Rm {
        /// Object URL.
        url: String,
    },
    /// Check whether an object exists, printing `true` or `false`.
    Exists { url: String },
    /// Create a bucket / container (or, for local paths, a directory).
    Mb { url: String },
    /// Check whether a bucket / container exists, printing `true` or `false`.
    BucketExists { url: String },
}

impl ObjectCommand {
    /// Whether the command passes object bytes through the codec. The others
    /// never read the keyring: no key source command runs, no key is loaded,
    /// and `--require-encryption` does not apply. No wildcard, so a new
    /// command must be classified.
    pub(super) const fn needs_keys(&self) -> bool {
        match self {
            Self::Cp { .. }
            | Self::Mv { .. }
            | Self::Put { .. }
            | Self::Get { .. }
            | Self::GetStream { .. } => true,
            Self::Ls { .. }
            | Self::Rm { .. }
            | Self::Exists { .. }
            | Self::Mb { .. }
            | Self::BucketExists { .. } => false,
        }
    }
}
