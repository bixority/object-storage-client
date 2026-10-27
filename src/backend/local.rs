//! Local filesystem backend.
//!
//! "Buckets" map to directories: creation is `mkdir -p` and existence is an
//! is-directory check on the URL path. Pre-signing is unsupported.

use super::Backend;
use crate::client::{Error, Result};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use std::path::PathBuf;
use std::sync::Arc;
use url::Url;

pub struct LocalBackend {
    #[cfg_attr(not(unix), allow(dead_code))]
    fs: Arc<LocalFileSystem>,
    store: Arc<dyn ObjectStore>,
}

impl LocalBackend {
    pub fn new() -> Self {
        let fs = Arc::new(LocalFileSystem::new());
        Self {
            store: fs.clone(),
            fs,
        }
    }
}

/// The URL path with its percent-encoding removed, as a file system path.
fn decoded_path(url: &Url) -> Result<String> {
    percent_encoding::percent_decode_str(url.path())
        .decode_utf8()
        .map(std::borrow::Cow::into_owned)
        .map_err(|e| Error::InvalidInput(format!("Invalid path in URL: {e}")))
}

#[async_trait::async_trait]
impl Backend for LocalBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    #[cfg(unix)]
    fn local_path(&self, path: &ObjectPath) -> Result<Option<PathBuf>> {
        Ok(Some(self.fs.path_to_filesystem(path)?))
    }

    fn scheme(&self) -> &'static str {
        "file"
    }

    /// Creates the directory at the URL path (and any missing parents).
    async fn create_bucket(&self, _client: &reqwest::Client, url: &Url) -> Result<()> {
        let path = &decoded_path(url)?;
        tokio::fs::create_dir_all(path)
            .await
            .map_err(|e| Error::Generic(format!("Failed to create directory {path}: {e}")))?;
        Ok(())
    }

    /// Reports whether the URL path exists and is a directory.
    async fn bucket_exists(&self, _client: &reqwest::Client, url: &Url) -> Result<bool> {
        let path = &decoded_path(url)?;
        match tokio::fs::metadata(path).await {
            Ok(meta) => Ok(meta.is_dir()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Generic(format!(
                "Failed to stat directory {path}: {e}"
            ))),
        }
    }
}
