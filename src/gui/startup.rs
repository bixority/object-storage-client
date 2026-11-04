//! Resolves the key source exactly as `osc` does.

use super::args::Args;
use super::model::Encryption;
use crate::cli::{CliError, KeySources, key_source, open_keyring};
use crate::encryption::EncryptionConfig;

/// The encryption every connection's client gets, and where its keys came from.
pub type Keys = Option<(EncryptionConfig, Encryption)>;

/// Opens the keyring if a key source is configured; with none, `None`
/// (plaintext) unless `--require-encryption` forbids it.
pub async fn open_keys(args: &Args) -> Result<Keys, CliError> {
    let encryption = if args.keyring.is_some() {
        Encryption::File
    } else {
        Encryption::Command
    };
    let source = key_source(KeySources {
        file: args.keyring.clone(),
        command: args.keyring_command.clone(),
    })?;
    match source {
        Some(source) => Ok(Some((
            EncryptionConfig::new(open_keyring(source).await?),
            encryption,
        ))),
        None if args.require_encryption => Err(CliError::EncryptionRequired),
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args {
            keyring: None,
            keyring_command: None,
            require_encryption: false,
        }
    }

    #[tokio::test]
    async fn both_key_sources_conflict() {
        let dir = tempfile::tempdir().expect("tempdir");
        let args = Args {
            keyring: Some(dir.path().join("k")),
            keyring_command: Some("true".into()),
            ..args()
        };
        assert!(matches!(
            open_keys(&args).await,
            Err(CliError::KeyringSourcesConflict)
        ));
    }

    #[tokio::test]
    async fn required_encryption_without_keys_fails() {
        let args = Args {
            require_encryption: true,
            ..args()
        };
        assert!(matches!(
            open_keys(&args).await,
            Err(CliError::EncryptionRequired)
        ));
    }

    #[tokio::test]
    async fn no_key_source_is_plain() {
        assert!(matches!(open_keys(&args()).await, Ok(None)));
    }
}
