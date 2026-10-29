//! Key material types and the key-provider abstraction.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use aws_lc_rs::rand;
use zeroize::Zeroizing;

use super::context::EncryptionContext;
use super::header::{FormatError, short_debug};

pub use super::header::EncryptedDataKey;

pub mod keyring;
#[expect(clippy::redundant_pub_crate)]
pub(crate) mod raw_aes;

pub use keyring::{
    KeyInfo, Keyring, KeyringStatus, activate_key, add_key, new_keyring, retire_key, rotate_keyring,
};

/// A 32-byte data encryption key. Zeroized on drop; deliberately not `Clone`,
/// `Debug` or `Serialize`.
///
/// The bytes live in a zeroing heap box, so moving a `DataKey` moves a pointer
/// and leaves no unwiped stack copy behind.
pub struct DataKey(Box<Zeroizing<[u8; 32]>>);

impl DataKey {
    /// A fresh random key, written straight into its zeroizing buffer.
    ///
    /// # Errors
    /// [`KeyError::Rng`] if random generation fails (see that variant).
    pub(crate) fn random() -> Result<Self, KeyError> {
        let mut key = Box::new(Zeroizing::new([0u8; 32]));
        rand::fill(&mut key[..]).map_err(|_| KeyError::Rng)?;
        Ok(Self(key))
    }

    /// Copies exactly 32 bytes into a zeroizing buffer. `None` for any other
    /// length.
    pub(crate) fn from_slice(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }
        let mut key = Box::new(Zeroizing::new([0u8; 32]));
        key.copy_from_slice(bytes);
        Some(Self(key))
    }

    pub(crate) fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Key management errors. Never carries key material.
#[derive(thiserror::Error)]
#[non_exhaustive]
pub enum KeyError {
    /// None of the EDKs names a key this provider holds. `key_ids` are the key
    /// names of the EDKs in this provider's namespace; `foreign_provider_ids`
    /// are the namespaces of the EDKs that belong to other providers. Both come
    /// from the unauthenticated header.
    #[error("{}", unknown_key_message(.key_ids, .foreign_provider_ids))]
    UnknownKey {
        key_ids: Vec<String>,
        foreign_provider_ids: Vec<String>,
    },
    /// The named key failed to unwrap the data key.
    #[error("unwrapping the data key with key {key_id:?} failed")]
    UnwrapFailed { key_id: String },
    /// A keyring file is writable by its group or accessible to others.
    #[error("keyring file {} has mode {mode:o}; group write and access for others must be off", path.display())]
    Permissions { path: PathBuf, mode: u32 },
    /// A keyring file is owned by someone else.
    #[error("keyring file {} is not owned by the current user or root", path.display())]
    NotOwner { path: PathBuf },
    /// The keyring path is not a regular file.
    #[error("keyring file {} is not a regular file", path.display())]
    NotRegularFile { path: PathBuf },
    /// The check value of a key does not match its material.
    #[error("key {id:?} failed its check value")]
    BadCheck { id: String },
    /// The key ID does not end in the check value prefix.
    #[error("key {id:?}: the id does not match its check value")]
    IdMismatch { id: String },
    /// Random generation failed. AWS-LC aborts the process on an internal
    /// RNG failure instead of reporting it, so the built-in key paths do not
    /// return this; custom [`KeyProvider`]s may.
    #[error("the random number generator failed")]
    Rng,
    /// The encryption context cannot be serialized.
    #[error("invalid encryption context: {0}")]
    Context(#[from] FormatError),
    /// Two keys declare the same ID (identical lines are rejected too).
    #[error("duplicate key id {id:?}")]
    DuplicateId { id: String },
    /// The active key is not among the listed keys.
    #[error("active key {id:?} not found")]
    ActiveKeyMissing { id: String },
    /// The keyring text is malformed. `line` is 1-based; 0 means the document
    /// as a whole.
    #[error("{}", malformed_message(*line, reason))]
    Malformed { line: usize, reason: &'static str },
    /// The keyring already holds the most keys it may.
    #[error("a keyring holds at most {max} keys")]
    TooManyKeys { max: usize },
    /// The active key cannot be retired.
    #[error("key {id:?} is the active key and cannot be retired")]
    RetireActive { id: String },
    /// The keyring has no key with this ID.
    #[error("no key {id:?} in the keyring")]
    UnknownId { id: String },
    /// An I/O error on a keyring file.
    #[error("keyring file {}: {source}", path.display())]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
}

fn malformed_message(line: usize, reason: &str) -> String {
    if line == 0 {
        format!("keyring is malformed: {reason}")
    } else {
        format!("keyring line {line} is malformed: {reason}")
    }
}

impl KeyError {
    /// Wraps an I/O error at `path`; the path is only copied on error.
    pub(crate) fn io<E: Into<std::io::Error>>(path: &Path) -> impl FnOnce(E) -> Self + use<'_, E> {
        move |e| Self::Io {
            path: path.to_path_buf(),
            source: e.into(),
        }
    }
}

/// A bounded `Debug` view of a list of names from an untrusted header.
struct Names<'a>(&'a [String]);

impl std::fmt::Debug for Names<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]", listed(self.0))
    }
}

// Manual so that the untrusted ID lists are truncated in `{:?}` too.
impl std::fmt::Debug for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownKey {
                key_ids,
                foreign_provider_ids,
            } => f
                .debug_struct("UnknownKey")
                .field("key_ids", &Names(key_ids))
                .field("foreign_provider_ids", &Names(foreign_provider_ids))
                .finish(),
            Self::UnwrapFailed { key_id } => f
                .debug_struct("UnwrapFailed")
                .field("key_id", &format_args!("{}", short_debug(key_id)))
                .finish(),
            Self::Permissions { path, mode } => f
                .debug_struct("Permissions")
                .field("path", path)
                .field("mode", mode)
                .finish(),
            Self::NotOwner { path } => f.debug_struct("NotOwner").field("path", path).finish(),
            Self::NotRegularFile { path } => f
                .debug_struct("NotRegularFile")
                .field("path", path)
                .finish(),
            Self::BadCheck { id } => f.debug_struct("BadCheck").field("id", id).finish(),
            Self::IdMismatch { id } => f.debug_struct("IdMismatch").field("id", id).finish(),
            Self::Rng => f.write_str("Rng"),
            Self::Context(e) => f.debug_tuple("Context").field(e).finish(),
            Self::DuplicateId { id } => f.debug_struct("DuplicateId").field("id", id).finish(),
            Self::ActiveKeyMissing { id } => {
                f.debug_struct("ActiveKeyMissing").field("id", id).finish()
            }
            Self::Malformed { line, reason } => f
                .debug_struct("Malformed")
                .field("line", line)
                .field("reason", reason)
                .finish(),
            Self::TooManyKeys { max } => f.debug_struct("TooManyKeys").field("max", max).finish(),
            Self::RetireActive { id } => f.debug_struct("RetireActive").field("id", id).finish(),
            Self::UnknownId { id } => f.debug_struct("UnknownId").field("id", id).finish(),
            Self::Io { path, source } => f
                .debug_struct("Io")
                .field("path", path)
                .field("source", source)
                .finish(),
        }
    }
}

/// Longest list of names echoed into an [`KeyError::UnknownKey`] message.
const MAX_LISTED: usize = 8;

fn listed(names: &[String]) -> String {
    let mut out: Vec<String> = names
        .iter()
        .take(MAX_LISTED)
        .map(|n| short_debug(n))
        .collect();
    if names.len() > MAX_LISTED {
        out.push(format!("... and {} more", names.len() - MAX_LISTED));
    }
    out.join(", ")
}

fn unknown_key_message(key_ids: &[String], foreign: &[String]) -> String {
    if key_ids.is_empty() && !foreign.is_empty() {
        format!(
            "none of the message's keys belongs to this provider; it was encrypted with key provider(s) {}",
            listed(foreign)
        )
    } else if foreign.is_empty() {
        format!(
            "no known key among the message's keys: [{}]",
            listed(key_ids)
        )
    } else {
        format!(
            "no known key among the message's keys: [{}] (other key provider(s) present: {})",
            listed(key_ids),
            listed(foreign)
        )
    }
}

/// Source of data-key wrapping keys.
///
/// Implementations outside this crate are not supported in Phase 1: the
/// [`DataKey`] constructors and accessors are crate-private, so a foreign
/// implementation cannot create or read data keys.
#[async_trait]
pub trait KeyProvider: Send + Sync + std::fmt::Debug {
    /// Provider ID written into EDKs (`osc-file`).
    fn namespace(&self) -> &str;

    /// Name of the key new messages are wrapped with (owned: a reloadable
    /// provider cannot lend a name from a snapshot that may be swapped).
    fn active_key_name(&self) -> String;

    /// Wraps `dek` under the active key, binding `ctx`.
    ///
    /// # Errors
    /// Provider specific.
    async fn wrap(
        &self,
        dek: &DataKey,
        ctx: &EncryptionContext,
    ) -> Result<Vec<EncryptedDataKey>, KeyError>;

    /// Recovers the data key from the first EDK this provider can open.
    ///
    /// # Errors
    /// [`KeyError::UnknownKey`] when no EDK names a held key,
    /// [`KeyError::UnwrapFailed`] when a named key fails to open its EDK.
    async fn unwrap(
        &self,
        edks: &[EncryptedDataKey],
        ctx: &EncryptionContext,
    ) -> Result<DataKey, KeyError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_key_needs_exactly_32_bytes() {
        assert!(DataKey::from_slice(&[1; 31]).is_none());
        assert!(DataKey::from_slice(&[1; 33]).is_none());
        assert_eq!(
            DataKey::from_slice(&[1; 32]).expect("key").as_bytes(),
            &[1; 32]
        );
    }

    fn names(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("k{i}")).collect()
    }

    #[test]
    fn unknown_key_message_names_each_cause() {
        let own = KeyError::UnknownKey {
            key_ids: names(2),
            foreign_provider_ids: vec![],
        };
        assert_eq!(
            own.to_string(),
            "no known key among the message's keys: [\"k0\", \"k1\"]"
        );
        let foreign = KeyError::UnknownKey {
            key_ids: vec![],
            foreign_provider_ids: vec!["aws-kms".to_owned()],
        };
        assert!(foreign.to_string().contains("\"aws-kms\""));
        assert!(foreign.to_string().contains("key provider(s)"));
        let both = KeyError::UnknownKey {
            key_ids: names(1),
            foreign_provider_ids: vec!["aws-kms".to_owned()],
        };
        let m = both.to_string();
        assert!(m.contains("\"k0\"") && m.contains("other key provider(s) present: \"aws-kms\""));
    }

    #[test]
    fn hostile_id_lists_are_truncated_in_display_and_debug() {
        let e = KeyError::UnknownKey {
            key_ids: names(MAX_LISTED + 5),
            foreign_provider_ids: vec!["x".repeat(500)],
        };
        for text in [e.to_string(), format!("{e:?}")] {
            assert!(text.contains("... and 5 more"), "{text}");
            assert!(text.contains("k7") && !text.contains("k8"), "{text}");
            assert!(text.len() < 400, "{} bytes", text.len());
        }
    }

    #[test]
    fn debug_of_every_variant_names_it_and_cuts_untrusted_ids() {
        let p = PathBuf::from("/keys/a.key");
        let cases: Vec<(KeyError, &str)> = vec![
            (
                KeyError::UnwrapFailed {
                    key_id: "y".repeat(500),
                },
                "UnwrapFailed",
            ),
            (
                KeyError::Permissions {
                    path: p.clone(),
                    mode: 0o644,
                },
                "Permissions",
            ),
            (KeyError::NotOwner { path: p.clone() }, "NotOwner"),
            (
                KeyError::NotRegularFile { path: p.clone() },
                "NotRegularFile",
            ),
            (KeyError::BadCheck { id: "a".into() }, "BadCheck"),
            (KeyError::IdMismatch { id: "a".into() }, "IdMismatch"),
            (KeyError::Rng, "Rng"),
            (KeyError::Context(FormatError::NotOsc), "Context"),
            (KeyError::DuplicateId { id: "a".into() }, "DuplicateId"),
            (
                KeyError::ActiveKeyMissing { id: "a".into() },
                "ActiveKeyMissing",
            ),
            (
                KeyError::Malformed {
                    line: 3,
                    reason: "bad",
                },
                "Malformed",
            ),
            (KeyError::TooManyKeys { max: 256 }, "TooManyKeys"),
            (KeyError::RetireActive { id: "a".into() }, "RetireActive"),
            (KeyError::UnknownId { id: "a".into() }, "UnknownId"),
            (
                KeyError::Io {
                    path: p,
                    source: std::io::Error::other("boom"),
                },
                "Io",
            ),
        ];
        for (e, name) in cases {
            let d = format!("{e:?}");
            assert!(d.starts_with(name), "{d}");
            assert!(d.len() < 300, "{name}: {} bytes", d.len());
            assert_ne!(e.to_string(), "");
        }
    }
}
