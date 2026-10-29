//! Local filesystem backend.
//!
//! "Buckets" map to directories: creation is `mkdir -p` and existence is an
//! is-directory check on the URL path. Pre-signing is unsupported.

use super::Backend;
use crate::client::{Error, Result};
use crate::registry::object_path;
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

/// The file system path of an object path: the one mapping every local
/// operation uses (object, bucket and delete calls alike). It goes through a
/// `file:///` URL, which on Windows yields `C:\data` for a first part `C:`
/// (a plain `push` would give the drive-relative `C:data`), and a bare drive
/// part maps to the drive root.
///
/// # Errors
///
/// [`Error::InvalidInput`] when the path has no mapping, for example a path
/// without a drive on Windows.
pub fn fs_path(path: &ObjectPath) -> Result<PathBuf> {
    let unmappable = || Error::InvalidInput("path cannot be mapped to a local file".into());
    let mut url = Url::parse("file:///").expect("static URL is valid");
    {
        let mut segments = url.path_segments_mut().map_err(|()| unmappable())?;
        segments.pop_if_empty().extend(path.parts());
        if cfg!(windows) && is_bare_drive(path) {
            segments.push("");
        }
    }
    url.to_file_path().map_err(|()| unmappable())
}

/// Whether the path is the single part `X:`.
fn is_bare_drive(path: &ObjectPath) -> bool {
    let mut parts = path.parts();
    match (parts.next(), parts.next()) {
        (Some(part), None) => {
            let part = part.as_ref().as_bytes();
            part.len() == 2 && part[0].is_ascii_alphabetic() && part[1] == b':'
        }
        _ => false,
    }
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
        let path = fs_path(&object_path(url)?)?;
        tokio::fs::create_dir_all(&path).await.map_err(|e| {
            Error::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to create directory {}: {e}", path.display()),
            ))
        })?;
        Ok(())
    }

    /// Reports whether the URL path exists and is a directory.
    async fn bucket_exists(&self, _client: &reqwest::Client, url: &Url) -> Result<bool> {
        let path = fs_path(&object_path(url)?)?;
        match tokio::fs::metadata(&path).await {
            Ok(meta) => Ok(meta.is_dir()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to stat directory {}: {e}", path.display()),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn fs_path_matches_object_store() {
        let fs = LocalFileSystem::new();
        for p in ["tmp/a b", "a", "a/b/c", "with space/x%20y"] {
            let path = ObjectPath::from(p);
            assert_eq!(
                fs_path(&path).expect("maps"),
                fs.path_to_filesystem(&path).expect("maps"),
                "{p}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn windows_drive_paths_map_to_drive_paths() {
        assert_eq!(
            fs_path(&ObjectPath::from("C:/x")).expect("maps"),
            PathBuf::from(r"C:\x")
        );
        assert_eq!(
            fs_path(&ObjectPath::from("C:")).expect("maps"),
            PathBuf::from(r"C:\")
        );
        let err = fs_path(&ObjectPath::from("x")).err();
        assert!(matches!(err, Some(Error::InvalidInput(_))), "{err:?}");
    }

    #[tokio::test]
    async fn buckets_round_trip_on_a_temp_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = Url::from_directory_path(dir.path().join("new/bucket")).expect("url");
        let (backend, http) = (LocalBackend::new(), reqwest::Client::new());
        assert!(!backend.bucket_exists(&http, &url).await.expect("exists"));
        backend.create_bucket(&http, &url).await.expect("create");
        assert!(backend.bucket_exists(&http, &url).await.expect("exists"));
    }
}
