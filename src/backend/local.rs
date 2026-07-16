//! Local filesystem backend.
//!
//! "Buckets" map to directories: creation is `mkdir -p` and existence is an
//! is-directory check on the URL path. Pre-signing is unsupported.

use super::Backend;
use crate::client::{Error, Result};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use url::Url;

pub struct LocalBackend {
    store: Arc<dyn ObjectStore>,
}

impl LocalBackend {
    pub fn new() -> Self {
        Self {
            store: Arc::new(LocalFileSystem::new()),
        }
    }
}

#[async_trait::async_trait]
impl Backend for LocalBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    // The trait ties the return to `&self`; this backend's scheme is a fixed
    // literal, so the lifetime tie is unavoidable here.
    #[allow(clippy::unnecessary_literal_bound)]
    fn scheme(&self) -> &str {
        "file"
    }

    /// Creates the directory at the URL path (and any missing parents).
    async fn create_bucket(&self, _client: &reqwest::Client, url: &Url) -> Result<()> {
        let path = url.path();
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| Error::Generic(format!("Failed to create directory {path}: {e}")))?;
        Ok(())
    }

    /// Reports whether the URL path exists and is a directory.
    async fn bucket_exists(&self, _client: &reqwest::Client, url: &Url) -> Result<bool> {
        let path = url.path();
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(meta.is_dir()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Generic(format!(
                "Failed to stat directory {path}: {e}"
            ))),
        }
    }
}
