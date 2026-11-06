//! Object storage client for S3, GCS, Azure Blob, plain HTTP and `file://`
//! URLs, with optional client-side encryption.
//!
//! Items marked `#[doc(hidden)]` are test hooks: they are not part of the
//! public API and may change in any release, outside semver.
//!
//! Layout: `core` holds the library shared by every front end; `cli` is the
//! `osc` binary's code and `gui` the `osc-gui` binary's. `core` is re-exported
//! under the module names the library has always had, so its paths are the
//! same for users and inside the crate.

mod core;

#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod cli;
#[cfg(feature = "gui")]
#[doc(hidden)]
pub mod gui;

pub use core::client;
pub use core::codec;
#[cfg(feature = "encryption")]
pub use core::encryption;
#[cfg(feature = "python")]
pub(crate) use core::python;
pub use core::sign;
pub(crate) use core::{backend, fsmode, hex, registry, text, upload};

#[cfg(feature = "encryption")]
pub use client::EncryptedObjectStorageClient;
pub use client::{Client, EncryptionInfo, Error, ObjectMetadata, ObjectStorageClient, Result};
#[cfg(feature = "encryption")]
pub use client::{ReencryptOptions, Reencrypted};
pub use codec::{Codec, Plain};
pub use sign::{SignMethod, SignOptions};
