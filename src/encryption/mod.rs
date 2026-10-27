//! Client-side encryption core: ESDK v2 compatible message format (suite
//! `0x0478`).
//!
//! The format core (`header`, `frame`, `suite`, `context`) is I/O-free;
//! `envelope` reads and writes object stores and [`keys::keyring`] reads the
//! keyring file.

pub mod context;
pub mod envelope;
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) mod fixtures;
pub(crate) mod frame;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
pub(crate) mod header;
pub mod keys;
pub(crate) mod suite;
#[cfg(test)]
mod tamper_tests;

pub use context::{CallerContext, EncryptionContext};
pub use envelope::{EncryptionConfig, Envelope};
pub use header::{DecodeLimits, EncryptedDataKey, FormatError};
pub use keys::{DataKey, KeyInfo, Keyring, KeyringStatus, new_keyring, retire_key, rotate_keyring};
pub use suite::DecryptError;
