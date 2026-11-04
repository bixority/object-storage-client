//! Local filesystem backend.
//!
//! "Buckets" map to directories: creation is `mkdir -p` and existence is an
//! is-directory check on the URL path. Pre-signing is unsupported.

use super::Backend;
use crate::client::local_delete::is_listed_name;
use crate::client::{Error, Result};
use crate::registry::object_path;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::fs::Metadata;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::SystemTime;
use url::Url;
use walkdir::{DirEntry, WalkDir};

pub struct LocalBackend {
    #[cfg_attr(not(unix), allow(dead_code))]
    fs: Arc<LocalFileSystem>,
    store: Arc<dyn ObjectStore>,
}

impl LocalBackend {
    pub fn new() -> Self {
        let fs = Arc::new(LocalFileSystem::new().with_fsync(true));
        Self {
            store: Arc::new(NoFollowLocal(fs.clone())),
            fs,
        }
    }
}

/// `LocalFileSystem` whose listings skip symlinks. `object_store` lists with
/// `follow_links(true)`, so a link below the prefix would list (and let the
/// caller read, move or delete) files outside it. Every other operation is
/// the inner store's: an object URL that itself names a link still resolves
/// to the file behind it. A link is never listed, and never descended into;
/// the prefix itself is resolved as named.
#[derive(Debug)]
struct NoFollowLocal(Arc<LocalFileSystem>);

impl fmt::Display for NoFollowLocal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&*self.0, f)
    }
}

#[async_trait]
impl ObjectStore for NoFollowLocal {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.0.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.0.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.0.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<ObjectPath>>,
    ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
        self.0.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        list_no_follow(prefix, None)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&ObjectPath>,
        offset: &ObjectPath,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        list_no_follow(prefix, Some(offset))
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let root = prefix_to_fs(&prefix)?;
        spawn_blocking(move || {
            let walk = WalkDir::new(root)
                .min_depth(1)
                .max_depth(1)
                .follow_links(false);
            let mut common_prefixes = BTreeSet::new();
            let mut objects = Vec::new();
            for entry in walk {
                let Some(entry) = walk_entry(entry)? else {
                    continue;
                };
                let kind = entry.file_type();
                if kind.is_symlink() {
                    continue;
                }
                let location = fs_to_path(entry.path())?;
                if !kind.is_dir() && !is_valid_file_path(&location) {
                    continue;
                }
                let Some(name) = location.prefix_match(&prefix).and_then(|mut p| p.next()) else {
                    continue;
                };
                if kind.is_dir() {
                    common_prefixes.insert(prefix.clone().join(name));
                } else if let Some(meta) = entry_meta(&entry, location)? {
                    objects.push(meta);
                }
            }
            Ok(ListResult {
                common_prefixes: common_prefixes.into_iter().collect(),
                objects,
                extensions: http::Extensions::default(),
            })
        })
        .await
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.0.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.0.rename_opts(from, to, options).await
    }
}

/// Whether `object_store` lists a file with this path (it hides in-flight
/// upload staging files).
fn is_valid_file_path(path: &ObjectPath) -> bool {
    path.filename().is_some_and(is_listed_name)
}

fn generic(source: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> object_store::Error {
    object_store::Error::Generic {
        store: "LocalFileSystem",
        source: source.into(),
    }
}

/// The file system path of a listing prefix; the empty prefix is the root.
fn prefix_to_fs(prefix: &ObjectPath) -> object_store::Result<PathBuf> {
    fs_path(prefix).map_err(|e| generic(e.to_string()))
}

fn fs_to_path(path: &std::path::Path) -> object_store::Result<ObjectPath> {
    ObjectPath::from_absolute_path(path).map_err(generic)
}

/// A walk result, with a vanished entry as `None`: `LocalFileSystem` skips
/// those too.
fn walk_entry(
    res: std::result::Result<DirEntry, walkdir::Error>,
) -> object_store::Result<Option<DirEntry>> {
    match res {
        Ok(entry) => Ok(Some(entry)),
        Err(e) => match e.io_error() {
            Some(io) if io.kind() == std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(generic(e)),
        },
    }
}

fn entry_meta(entry: &DirEntry, location: ObjectPath) -> object_store::Result<Option<ObjectMeta>> {
    match entry.metadata() {
        Ok(meta) => Ok(Some(convert_metadata(&meta, location))),
        Err(e) => match e.io_error() {
            Some(io) if io.kind() == std::io::ErrorKind::NotFound => Ok(None),
            _ => Err(generic(format!(
                "Unable to access metadata for {location}: {e}"
            ))),
        },
    }
}

// `convert_metadata`, `get_etag` and `get_inode` mirror object_store 0.14.2
// (`local.rs`), so the e_tag of a listed file equals the one `head` returns.
fn convert_metadata(metadata: &Metadata, location: ObjectPath) -> ObjectMeta {
    let last_modified = metadata
        .modified()
        .expect("Modified file time should be supported on this platform");
    ObjectMeta {
        location,
        last_modified: DateTime::<Utc>::from(last_modified),
        size: metadata.len(),
        e_tag: Some(get_etag(metadata)),
        version: None,
    }
}

fn get_etag(metadata: &Metadata) -> String {
    let inode = get_inode(metadata);
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(SystemTime::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("\"{inode:x}-{mtime:x}-{size:x}\"")
}

#[cfg(unix)]
fn get_inode(metadata: &Metadata) -> u64 {
    std::os::unix::fs::MetadataExt::ino(metadata)
}

#[cfg(not(unix))]
fn get_inode(_metadata: &Metadata) -> u64 {
    0
}

/// Runs a blocking closure on the tokio blocking pool.
async fn spawn_blocking<T: Send + 'static>(
    f: impl FnOnce() -> object_store::Result<T> + Send + 'static,
) -> object_store::Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| object_store::Error::JoinError { source: e })?
}

/// Entries read per blocking call.
const CHUNK_SIZE: usize = 1024;

/// The recursive no-follow listing: `LocalFileSystem::list_with_maybe_offset`
/// from `object_store` 0.14.2 with `follow_links(false)` and symlinks skipped.
fn list_no_follow(
    prefix: Option<&ObjectPath>,
    offset: Option<&ObjectPath>,
) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
    let root = match prefix_to_fs(&prefix.cloned().unwrap_or_default()) {
        Ok(root) => root,
        Err(e) => return futures::stream::once(async { Err(e) }).boxed(),
    };
    let offset = offset.cloned();
    let walk = WalkDir::new(root).min_depth(1).follow_links(false);
    let entries = walk.into_iter().filter_map(move |res| {
        // The offset filter runs before the stat, as in object_store.
        if let (Some(offset), Ok(entry)) = (offset.as_ref(), res.as_ref()) {
            match fs_to_path(entry.path()) {
                Ok(path) if path <= *offset => return None,
                Err(e) => return Some(Err(e)),
                Ok(_) => {}
            }
        }
        let entry = match walk_entry(res).transpose()? {
            Ok(entry) => entry,
            Err(e) => return Some(Err(e)),
        };
        if !entry.file_type().is_file() {
            return None;
        }
        match fs_to_path(entry.path()) {
            Ok(path) if is_valid_file_path(&path) => entry_meta(&entry, path).transpose(),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        }
    });

    // Without a tokio runtime there is no need to chunk the blocking reads.
    if tokio::runtime::Handle::try_current().is_err() {
        return futures::stream::iter(entries).boxed();
    }
    futures::stream::try_unfold(
        (entries, VecDeque::with_capacity(CHUNK_SIZE)),
        |(mut entries, mut buffer)| async move {
            if buffer.is_empty() {
                (entries, buffer) = tokio::task::spawn_blocking(move || {
                    for _ in 0..CHUNK_SIZE {
                        match entries.next() {
                            Some(r) => buffer.push_back(r),
                            None => break,
                        }
                    }
                    (entries, buffer)
                })
                .await
                .map_err(|e| object_store::Error::JoinError { source: e })?;
            }
            match buffer.pop_front() {
                Some(Err(e)) => Err(e),
                Some(Ok(meta)) => Ok(Some((meta, (entries, buffer)))),
                None => Ok(None),
            }
        },
    )
    .boxed()
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
    async fn create_bucket(&self, url: &Url) -> Result<()> {
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
    async fn bucket_exists(&self, url: &Url) -> Result<bool> {
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
        let backend = LocalBackend::new();
        assert!(!backend.bucket_exists(&url).await.expect("exists"));
        backend.create_bucket(&url).await.expect("create");
        assert!(backend.bucket_exists(&url).await.expect("exists"));
    }

    /// `outside/{x,y/z}` beside `top/{real.txt,sub/inner.txt,data -> ../outside,
    /// f -> ../outside/x}`.
    #[cfg(unix)]
    fn linked_tree() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        std::fs::create_dir_all(root.join("outside/y")).expect("mkdir");
        std::fs::create_dir_all(root.join("top/sub")).expect("mkdir");
        std::fs::write(root.join("outside/x"), "x").expect("write");
        std::fs::write(root.join("outside/y/z"), "z").expect("write");
        std::fs::write(root.join("top/real.txt"), "r").expect("write");
        std::fs::write(root.join("top/sub/inner.txt"), "i").expect("write");
        std::os::unix::fs::symlink("../outside", root.join("top/data")).expect("link");
        std::os::unix::fs::symlink("../outside/x", root.join("top/f")).expect("link");
        dir
    }

    #[cfg(unix)]
    fn object_path_of(path: &std::path::Path) -> ObjectPath {
        ObjectPath::from_absolute_path(path).expect("absolute")
    }

    #[cfg(unix)]
    async fn names(store: &Arc<dyn ObjectStore>, prefix: &ObjectPath) -> Vec<String> {
        use futures::TryStreamExt;
        let mut names: Vec<_> = store
            .list(Some(prefix))
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .expect("list");
        names.sort();
        names
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_skips_symlinks() {
        let dir = linked_tree();
        let store = LocalBackend::new().store().clone();
        let top = object_path_of(&dir.path().join("top"));
        let got = names(&store, &top).await;
        let want: Vec<_> = ["real.txt", "sub/inner.txt"]
            .iter()
            .map(|n| format!("{top}/{n}"))
            .collect();
        assert_eq!(got, want);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_resolves_a_symlinked_prefix_as_named() {
        let dir = linked_tree();
        let store = LocalBackend::new().store().clone();
        let data = object_path_of(&dir.path().join("top/data"));
        let got = names(&store, &data).await;
        assert_eq!(got, [format!("{data}/x"), format!("{data}/y/z")]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_with_delimiter_skips_symlinks() {
        let dir = linked_tree();
        let store = LocalBackend::new().store().clone();
        let top = object_path_of(&dir.path().join("top"));
        let listed = store.list_with_delimiter(Some(&top)).await.expect("list");
        let folders: Vec<_> = listed
            .common_prefixes
            .iter()
            .map(ToString::to_string)
            .collect();
        let objects: Vec<_> = listed
            .objects
            .iter()
            .map(|m| m.location.to_string())
            .collect();
        assert_eq!(folders, [format!("{top}/sub")]);
        assert_eq!(objects, [format!("{top}/real.txt")]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_names_with_percent_and_space_round_trip() {
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::create_dir(dir.path().join("d")).expect("mkdir");
        std::fs::write(dir.path().join("d/a b%20c.txt"), "v").expect("write");
        let store = LocalBackend::new().store().clone();
        let prefix = object_path_of(&dir.path().join("d"));
        let listed: Vec<_> = store.list(Some(&prefix)).try_collect().await.expect("list");
        let [meta] = listed.as_slice() else {
            panic!("{listed:?}");
        };
        assert_eq!(meta.location.filename(), Some("a b%20c.txt"));
        let got = store.get(&meta.location).await.expect("get");
        assert_eq!(got.bytes().await.expect("bytes").as_ref(), b"v");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn listed_e_tag_equals_head_e_tag() {
        use futures::TryStreamExt;
        use object_store::ObjectStoreExt;
        let dir = linked_tree();
        let store = LocalBackend::new().store().clone();
        let top = object_path_of(&dir.path().join("top"));
        let listed: Vec<_> = store.list(Some(&top)).try_collect().await.expect("list");
        assert_eq!(listed.len(), 2);
        for meta in listed {
            let head = store.head(&meta.location).await.expect("head");
            assert!(meta.e_tag.is_some());
            assert_eq!(meta, head);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn list_with_offset_honours_the_offset() {
        use futures::TryStreamExt;
        let dir = tempfile::tempdir().expect("tempdir");
        for n in ["a", "b", "c"] {
            std::fs::write(dir.path().join(n), n).expect("write");
        }
        // A link after the offset is still skipped.
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.path().join("a"), dir.path().join("d")).expect("link");
        let store = LocalBackend::new().store().clone();
        let prefix = object_path_of(dir.path());
        let offset = prefix.clone().join("a");
        let mut got: Vec<_> = store
            .list_with_offset(Some(&prefix), &offset)
            .map_ok(|m| m.location.to_string())
            .try_collect()
            .await
            .expect("list");
        got.sort();
        assert_eq!(got, [format!("{prefix}/b"), format!("{prefix}/c")]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_missing_prefix_lists_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = LocalBackend::new().store().clone();
        let prefix = object_path_of(&dir.path().join("gone"));
        assert_eq!(names(&store, &prefix).await, Vec::<String>::new());
        let listed = store
            .list_with_delimiter(Some(&prefix))
            .await
            .expect("list");
        assert!(listed.objects.is_empty() && listed.common_prefixes.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn list_without_a_runtime_skips_symlinks() {
        use futures::TryStreamExt;
        let dir = linked_tree();
        let store = LocalBackend::new().store().clone();
        let top = object_path_of(&dir.path().join("top"));
        let listed: Vec<_> =
            futures::executor::block_on(store.list(Some(&top)).try_collect()).expect("list");
        assert_eq!(listed.len(), 2, "{listed:?}");
    }
}
