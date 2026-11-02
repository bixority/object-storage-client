//! Builds the client exactly as `osc` does.

use super::args::Args;
use super::model::Encryption;
use crate::cli::{CliError, KeySources, open_keyring};
use crate::encryption::EncryptionConfig;
use crate::{EncryptedObjectStorageClient, ObjectStorageClient};

/// The client the GUI works through.
pub enum Opened {
    Plain(ObjectStorageClient),
    Encrypted(EncryptedObjectStorageClient, Encryption),
}

/// Opens the client: profile, then key source, then the encryption policy.
pub async fn open_client(args: &Args) -> Result<Opened, CliError> {
    let client = match &args.profile {
        Some(profile) => ObjectStorageClient::new().with_s3_profile(profile.clone())?,
        None => ObjectStorageClient::new(),
    };
    let encryption = if args.keyring.is_some() {
        Encryption::File
    } else {
        Encryption::Command
    };
    let sources = KeySources {
        file: args.keyring.clone(),
        command: args.keyring_command.clone(),
    };
    match open_keyring(sources).await? {
        Some(ring) => Ok(Opened::Encrypted(
            client.with_encryption(EncryptionConfig::new(ring)),
            encryption,
        )),
        None if args.require_encryption => Err(CliError::EncryptionRequired),
        None => Ok(Opened::Plain(client)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> Args {
        Args {
            keyring: None,
            keyring_command: None,
            profile: None,
            require_encryption: false,
            left: None,
            right: None,
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
            open_client(&args).await,
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
            open_client(&args).await,
            Err(CliError::EncryptionRequired)
        ));
    }

    #[tokio::test]
    async fn no_key_source_is_plain() {
        assert!(matches!(open_client(&args()).await, Ok(Opened::Plain(_))));
    }
}
