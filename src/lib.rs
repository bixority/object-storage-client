//! Object storage client for S3, GCS, Azure Blob, plain HTTP and `file://`
//! URLs, with optional client-side encryption.
//!
//! Items marked `#[doc(hidden)]` are test hooks: they are not part of the
//! public API and may change in any release, outside semver.

mod backend;
#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod cli;
pub mod client;
pub mod codec;
#[cfg(feature = "encryption")]
pub mod encryption;
mod fsmode;
pub(crate) mod hex;
#[cfg(feature = "python")]
mod python;
mod registry;
pub mod sign;
mod text;
mod upload;

#[cfg(feature = "encryption")]
pub use client::EncryptedObjectStorageClient;
pub use client::{Client, EncryptionInfo, Error, ObjectMetadata, ObjectStorageClient, Result};
#[cfg(feature = "encryption")]
pub use client::{ReencryptOptions, Reencrypted};
pub use codec::{Codec, Plain};
pub use sign::{SignMethod, SignOptions};
