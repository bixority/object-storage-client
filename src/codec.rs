//! The [`Codec`] seam between [`Client`](crate::Client) and stored bytes.
//!
//! [`Plain`] is the identity codec. The trait is sealed: only this crate
//! provides implementations.

use crate::backend::{Backend, body_stream};
use crate::client::{Error, ObjectMetadata, Result};
use crate::upload::{UploadOptions, upload_stream};
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{
    Attribute, GetOptions, GetResult, GetResultPayload, ObjectMeta, ObjectStore, ObjectStoreExt,
    path::Path as ObjectPath,
};
use std::collections::BTreeMap;
use std::sync::Arc;
use url::Url;

/// The most memory reserved up front for a body whose size the provider
/// declared. Buffers grow as data arrives, so a hostile length costs nothing.
pub(crate) const MAX_PREALLOC: usize = 16 << 20;

/// Caller-supplied key/value pairs a codec may bind to an object. The
/// encrypted client reserves keys starting with `osc:` or `aws-crypto-`.
pub type CallerContext = BTreeMap<String, String>;

/// A boxed stream of byte chunks.
pub type ByteStream = BoxStream<'static, Result<Bytes>>;

pub(crate) mod sealed {
    use super::{
        Backend, ByteStream, Bytes, CallerContext, GetResult, ObjectMetadata, ObjectPath, Result,
        Url,
    };
    use futures::stream::BoxStream;
    use std::sync::Arc;

    /// Where an object lives: its parsed URL and its path within the store.
    #[derive(Debug, Clone)]
    pub struct Location {
        pub url: Url,
        pub path: ObjectPath,
    }

    /// A [`Location`] together with the backend serving it.
    #[derive(Clone)]
    pub struct Resolved {
        pub backend: Arc<dyn Backend>,
        pub loc: Location,
    }

    /// The seam methods of a [`Codec`](super::Codec).
    pub trait CodecImpl {
        /// Encodes a whole object for storage.
        fn encode(
            &self,
            loc: &Location,
            ctx: &CallerContext,
            data: Bytes,
        ) -> impl Future<Output = Result<Bytes>> + Send;

        /// Encodes a stream of plaintext chunks into stored bytes.
        fn encode_stream(&self, loc: Location, ctx: CallerContext, plain: ByteStream)
        -> ByteStream;

        /// Decodes a stream of stored bytes into plaintext chunks.
        fn decode(
            &self,
            loc: Location,
            stored: BoxStream<'static, object_store::Result<Bytes>>,
        ) -> ByteStream;

        /// Reads and decodes a whole object.
        fn get_bytes(
            &self,
            loc: &Location,
            result: GetResult,
        ) -> impl Future<Output = Result<Bytes>> + Send;

        /// Describes an object: a HEAD for [`Plain`]; for the encrypted codec,
        /// one ranged read of the header, with `size_bytes` and `encryption` taken
        /// from unauthenticated data.
        fn describe(
            &self,
            backend: &dyn Backend,
            loc: &Location,
        ) -> impl Future<Output = Result<ObjectMetadata>> + Send;

        /// Copies (or moves, when `delete_source`) an object between locations.
        fn copy(
            &self,
            src: &Resolved,
            dst: &Resolved,
            delete_source: bool,
        ) -> impl Future<Output = Result<()>> + Send;
    }
}

pub(crate) use sealed::{Location, Resolved};

/// Transforms object bytes on their way to and from a store. Sealed: only this
/// crate provides implementations, and the seam methods are private to it.
pub trait Codec: sealed::CodecImpl + Clone + Send + Sync + 'static {}

/// The identity codec: objects are stored as given.
#[derive(Clone, Copy, Debug, Default)]
pub struct Plain;

impl Codec for Plain {}

impl sealed::CodecImpl for Plain {
    fn encode(
        &self,
        _loc: &Location,
        _ctx: &CallerContext,
        data: Bytes,
    ) -> impl Future<Output = Result<Bytes>> + Send {
        std::future::ready(Ok(data))
    }

    fn encode_stream(&self, _loc: Location, _ctx: CallerContext, plain: ByteStream) -> ByteStream {
        plain
    }

    fn decode(
        &self,
        _loc: Location,
        stored: BoxStream<'static, object_store::Result<Bytes>>,
    ) -> ByteStream {
        stored.map(|chunk| chunk.map_err(Error::from)).boxed()
    }

    async fn get_bytes(&self, _loc: &Location, result: GetResult) -> Result<Bytes> {
        if matches!(result.payload, GetResultPayload::File(..)) {
            return Ok(result.bytes().await?);
        }
        // The range is the provider's claim: reserve a bounded amount and
        // grow as data arrives instead of trusting it.
        let declared = usize::try_from(result.range.end - result.range.start).unwrap_or(usize::MAX);
        let mut out = BytesMut::with_capacity(declared.min(MAX_PREALLOC));
        let mut stream = body_stream(result);
        while let Some(chunk) = stream.next().await {
            out.extend_from_slice(&chunk?);
        }
        Ok(out.freeze())
    }

    async fn describe(&self, backend: &dyn Backend, loc: &Location) -> Result<ObjectMetadata> {
        let options = GetOptions {
            head: true,
            ..Default::default()
        };

        let result = backend.store().get_opts(&loc.path, options).await?;
        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());
        let meta = result.meta;

        Ok(ObjectMetadata {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size_bytes: meta.size,
            stored_size_bytes: meta.size,
            content_type,
            e_tag: meta.e_tag,
            version: meta.version,
            encryption: None,
        })
    }

    async fn copy(&self, src: &Resolved, dst: &Resolved, delete_source: bool) -> Result<()> {
        // A copy or move onto itself is a no-op: a rename of an object onto
        // itself is copy-then-delete on cloud stores and would destroy it.
        if same_object(&src.loc, &dst.loc) {
            return Ok(());
        }
        if server_side_copy(src, dst, delete_source, None).await? {
            return Ok(());
        }
        let from_store = src.backend.store();
        let got = from_store.get(&src.loc.path).await?;
        let read = got.meta.clone();
        let attributes = copied_attributes(
            &got.attributes,
            store_identity(&src.loc.url).0 == store_identity(&dst.loc.url).0,
        );
        let stored = body_stream(got)
            .map(|chunk| chunk.map_err(Error::from))
            .boxed();
        let options = UploadOptions {
            attributes,
            ..UploadOptions::copying_from(src.backend.as_ref(), &src.loc.path, read.size)
        };
        upload_stream(dst.backend.as_ref(), &dst.loc.path, stored, &options).await?;
        if delete_source {
            delete_unchanged_source(src, &read).await?;
        }
        Ok(())
    }
}

/// The attributes a streamed copy keeps: the content headers always, user
/// metadata only within one kind of store (`same_provider`), and never the
/// storage class, whose values are provider-specific.
fn copied_attributes(
    attributes: &object_store::Attributes,
    same_provider: bool,
) -> object_store::Attributes {
    attributes
        .iter()
        .filter(|(name, _)| match name {
            Attribute::ContentType
            | Attribute::ContentEncoding
            | Attribute::ContentDisposition
            | Attribute::ContentLanguage
            | Attribute::CacheControl => true,
            Attribute::Metadata(_) => same_provider,
            _ => false,
        })
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}

/// Deletes the source of a streamed move, but only if it is still the object
/// that was read and copied: `read` is the metadata of the body GET.
///
/// Two names of one origin (an IP and a hostname, a CNAME) cannot be told
/// apart by comparing URLs, and then the copy overwrote the source itself.
/// A concurrent writer is caught the same way. A source that is gone is left
/// alone; one that differs in any metadata field the store provides is kept
/// and reported as [`Error::SourceChanged`].
///
/// This is a best effort: a rewrite with identical bytes, in the same second,
/// on a store that reports neither e-tag nor version is indistinguishable
/// from the original, and a writer can still land between the HEAD and the
/// delete.
pub(crate) async fn delete_unchanged_source(src: &Resolved, read: &ObjectMeta) -> Result<()> {
    let store = src.backend.store();
    match store.head(&src.loc.path).await {
        Ok(now) if now == *read => {}
        Ok(_) => {
            return Err(Error::SourceChanged {
                url: redacted(&src.loc.url),
            });
        }
        Err(object_store::Error::NotFound { .. }) => return Ok(()),
        Err(e) => return Err(e.into()),
    }
    match store.delete(&src.loc.path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e.into()),
    }
}

/// A URL for messages: no userinfo, query or fragment.
pub(crate) fn redacted(url: &Url) -> String {
    let mut url = url.clone();
    let _ = url.set_username("");
    let _ = url.set_password(None);
    url.set_query(None);
    url.set_fragment(None);
    url.to_string()
}

/// The store a URL addresses, normalised so aliases compare equal: `gs` and
/// `gcs`; the Azure schemes (`az`, `wasb`, `wasbs`, `abfs`, `abfss`), which all
/// take the URL host as the container of the one configured account; and
/// `http` with `https` (host and explicit port, scheme default ports being
/// ignored). HTTP hosts compare case-insensitively and without one trailing
/// dot; S3 bucket names compare exactly, because legacy and S3-compatible
/// buckets are case-sensitive (GCS and Azure names are lowercase anyway).
/// `file` ignores the host, as the local backend does (`file://127.0.0.1/D/x`
/// aliases `file:///D/x`).
///
/// This is only an identity for deciding that two URLs name the same object;
/// it cannot see that two host names reach one origin. It says nothing about
/// which backend serves a URL: the registry keys backends on the full scheme,
/// userinfo, host and port, and a server-side copy needs the same backend
/// (see [`server_side_copy`]).
fn store_identity(url: &Url) -> (String, Option<String>, Option<u16>) {
    let lower = || url.host_str().map(str::to_ascii_lowercase);
    match url.scheme() {
        "file" => ("file".into(), None, None),
        "gs" | "gcs" => ("gcs".into(), lower(), None),
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => ("azure".into(), lower(), None),
        "http" | "https" => {
            let host = lower().map(|h| h.strip_suffix('.').unwrap_or(&h).to_owned());
            ("http".into(), host, url.port())
        }
        other => (other.into(), url.host_str().map(str::to_owned), None),
    }
}

/// Whether two URLs are served by the same store (see [`store_identity`]).
pub(crate) fn same_store(a: &Url, b: &Url) -> bool {
    store_identity(a) == store_identity(b)
}

/// The file a `file://` location refers to, as the local backend resolves it.
fn local_file(loc: &Location) -> std::path::PathBuf {
    std::path::Path::new("/").join(loc.path.as_ref())
}

/// Whether two existing paths are the same file, whatever route reaches it
/// (symlinked directories, a case-insensitive file system). A path that does
/// not exist is not the same file as anything.
fn same_file(a: &std::path::Path, b: &std::path::Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        match (std::fs::metadata(a), std::fs::metadata(b)) {
            (Ok(ma), Ok(mb)) => ma.dev() == mb.dev() && ma.ino() == mb.ino(),
            _ => false,
        }
    }
    #[cfg(not(unix))]
    {
        match (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
            (Ok(ca), Ok(cb)) => ca == cb,
            _ => false,
        }
    }
}

/// Whether two locations carry the same name in the same store, without
/// looking at the file system.
pub(crate) fn same_name(a: &Location, b: &Location) -> bool {
    same_store(&a.url, &b.url) && a.path == b.path
}

/// Whether two locations name the same object: the same path in the same
/// store, or, for `file`, two paths that reach one existing file.
pub(crate) fn same_object(a: &Location, b: &Location) -> bool {
    if !same_store(&a.url, &b.url) {
        return false;
    }
    a.path == b.path || (a.url.scheme() == "file" && same_file(&local_file(a), &local_file(b)))
}

/// The largest source a single S3 `CopyObject` accepts (5 GiB).
const S3_MAX_COPY_SOURCE: u64 = 5 << 30;

/// Copies or renames inside one store. Returns `false` when the caller must
/// stream instead: the locations are not served by the very same backend
/// (the same registry entry, not merely equal-looking names, which may differ
/// in bucket case or credentials), a `file` copy (`object_store` makes it a hard
/// link, so file copies always stream through the private-staging writer), a
/// `file` rename crossed devices, or an S3 source is too large for one
/// `CopyObject`. `size` is the source size when the caller knows it; S3 looks
/// it up otherwise.
///
/// A `file` move is an atomic rename. Every other move is a copy followed by
/// a delete that happens only if the source is still what was copied
/// ([`delete_unchanged_source`]), because `object_store`'s default rename
/// deletes unconditionally.
pub(crate) async fn server_side_copy(
    src: &Resolved,
    dst: &Resolved,
    delete_source: bool,
    size: Option<u64>,
) -> Result<bool> {
    if !Arc::ptr_eq(&src.backend, &dst.backend) {
        return Ok(false);
    }
    let store = src.backend.store();
    let scheme = src.loc.url.scheme();
    if scheme == "file" {
        if !delete_source {
            return Ok(false);
        }
        return match store.rename(&src.loc.path, &dst.loc.path).await {
            Ok(()) => Ok(true),
            Err(e) if crosses_devices(&e) => Ok(false),
            Err(e) => Err(e.into()),
        };
    }
    let before = if delete_source {
        Some(store.head(&src.loc.path).await?)
    } else {
        None
    };
    if scheme == "s3" {
        let known = match (size, &before) {
            (Some(size), _) => size,
            (None, Some(meta)) => meta.size,
            (None, None) => store.head(&src.loc.path).await?.size,
        };
        if known > S3_MAX_COPY_SOURCE {
            return Ok(false);
        }
    }
    store.copy(&src.loc.path, &dst.loc.path).await?;
    if let Some(before) = before {
        delete_unchanged_source(src, &before).await?;
    }
    Ok(true)
}

/// Whether an I/O error of kind `CrossesDevices` (EXDEV) is anywhere in the
/// error's source chain.
fn crosses_devices(err: &(dyn std::error::Error + 'static)) -> bool {
    std::iter::successors(Some(err), |e| e.source()).any(|e| {
        e.downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::CrossesDevices)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(url: &str) -> Location {
        let url = Url::parse(url).expect("url");
        let path = ObjectPath::from_url_path(url.path()).expect("path");
        Location { url, path }
    }

    #[test]
    fn file_urls_are_the_same_store_whatever_the_host() {
        // `same_object` stats file paths: keep them in a private tempdir.
        let tmp = tempfile::tempdir().expect("tempdir");
        let file = |name: &str| {
            url::Url::from_file_path(tmp.path().join(name))
                .expect("file URL")
                .to_string()
        };
        let x = file("x");
        let (a, b) = (at(&x), at(&x.replacen("file://", "file://127.0.0.1", 1)));
        assert!(same_object(&a, &b));
        assert!(!same_object(&a, &at(&file("y"))));
        assert!(!same_object(&at("s3://one/x"), &at("s3://two/x")));
        assert!(same_object(&at("s3://one/x"), &at("s3://one/x")));
        assert!(!same_store(
            &Url::parse("s3://b/x").expect("url"),
            &Url::parse("gs://b/x").expect("url")
        ));
    }

    #[tokio::test]
    async fn get_bytes_does_not_trust_the_declared_length() {
        let big = 1_u64 << 50;
        let chunks: Vec<object_store::Result<Bytes>> =
            vec![Ok(Bytes::from_static(b"ab")), Ok(Bytes::from_static(b"cd"))];
        let result = GetResult {
            payload: GetResultPayload::Stream(futures::stream::iter(chunks).boxed()),
            meta: ObjectMeta {
                location: ObjectPath::from("a"),
                last_modified: chrono::DateTime::UNIX_EPOCH,
                size: big,
                e_tag: None,
                version: None,
            },
            range: 0..big,
            attributes: object_store::Attributes::new(),
            extensions: http::Extensions::new(),
        };
        let got = Plain
            .get_bytes(&at("test://b/a"), result)
            .await
            .expect("bytes");
        assert_eq!(got.as_ref(), b"abcd");
    }

    fn store_eq(a: &str, b: &str) -> bool {
        same_store(&Url::parse(a).expect("url"), &Url::parse(b).expect("url"))
    }

    #[test]
    fn store_identity_normalises_aliases() {
        assert!(store_eq("gs://b/x", "gcs://b/y"));
        assert!(store_eq("gs://B/x", "gcs://b/x"));
        assert!(!store_eq("gs://b/x", "gcs://c/x"));
        for s in ["az", "wasb", "wasbs", "abfs", "abfss"] {
            assert!(store_eq("az://c/x", &format!("{s}://c/x")), "{s}");
            assert!(!store_eq("az://c/x", &format!("{s}://d/x")), "{s}");
        }
        assert!(!store_eq("az://b/x", "gs://b/x"));
        assert!(!store_eq("s3://b/x", "gs://b/x"));
        assert!(store_eq("http://h/x", "https://H/x"));
        assert!(store_eq("http://h:8080/x", "https://h:8080/x"));
        assert!(!store_eq("http://h:8080/x", "http://h/x"));
        assert!(!store_eq("http://h/x", "http://other/x"));
        assert!(!store_eq("http://h:1/x", "http://h:2/x"));
        // S3 bucket names are case-sensitive on legacy and compatible stores.
        assert!(!store_eq("s3://Bucket/x", "s3://bucket/x"));
        assert!(store_eq("s3://bucket/x", "s3://bucket/y"));
        // One trailing dot of an HTTP host is the same host.
        assert!(store_eq("http://h./x", "http://h/x"));
        assert!(store_eq("https://H.:8080/x", "http://h:8080/x"));
        assert!(!store_eq("http://h../x", "http://h/x"));
        assert!(!store_eq("s3://one/x", "s3://two/x"));
    }

    #[test]
    fn aliased_urls_name_the_same_object() {
        assert!(same_object(&at("gs://b/x"), &at("gcs://b/x")));
        assert!(!same_object(&at("gs://b/x"), &at("gcs://b/y")));
    }

    #[cfg(unix)]
    #[test]
    fn file_paths_through_a_symlink_are_the_same_object() {
        let dir = tempfile::tempdir().expect("tempdir");
        let real = dir.path().join("real");
        std::fs::create_dir(&real).expect("mkdir");
        std::os::unix::fs::symlink(&real, dir.path().join("link")).expect("symlink");
        std::fs::write(real.join("a"), b"x").expect("write");
        let url = |p: &str| format!("file://{}/{p}", dir.path().display());
        assert!(same_object(&at(&url("link/a")), &at(&url("real/a"))));
        // The destination must exist to be the same file.
        assert!(!same_object(&at(&url("link/a")), &at(&url("real/b"))));
        assert!(!same_object(&at(&url("link/missing")), &at(&url("real/b"))));
    }

    use crate::codec::sealed::CodecImpl as _;
    use object_store::memory::InMemory;

    fn resolved(backend: &Arc<dyn Backend>, url: &str) -> Resolved {
        Resolved {
            backend: Arc::clone(backend),
            loc: at(url),
        }
    }

    fn memory_backend() -> (Arc<InMemory>, Arc<dyn Backend>) {
        let store = Arc::new(InMemory::new());
        let backend: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: store.clone(),
            multipart: false,
        });
        (store, backend)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_move_whose_sync_fails_keeps_its_source() {
        let (store, memory) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "payload".into())
            .await
            .expect("put");
        let dir = tempfile::tempdir().expect("tempdir");
        let out = dir.path().join("out");
        let url = Url::from_file_path(&out).expect("url");
        let client = crate::Client::<Plain>::default();
        let (local, path) = client.get_backend(&url).expect("backend");
        let target = local.local_path(&path).expect("path").expect("local");
        let src = resolved(&memory, "test://b/a");
        let dst = Resolved {
            backend: local,
            loc: Location { url, path },
        };

        crate::upload::SYNC_FAULTS
            .lock()
            .expect("lock")
            .push(target.clone());
        let moved = Plain.copy(&src, &dst, true).await;
        crate::upload::SYNC_FAULTS
            .lock()
            .expect("lock")
            .retain(|p| *p != target);

        assert!(moved.is_err());
        assert!(store.head(&ObjectPath::from("a")).await.is_ok());
        assert_eq!(std::fs::read_dir(dir.path()).expect("dir").count(), 0);
    }

    /// Equal-looking names do not make a server-side copy: only the very same
    /// backend does, otherwise the copy would run as the source's bucket or
    /// credentials.
    #[tokio::test]
    async fn server_side_copy_needs_the_same_backend() {
        let (store, one) = memory_backend();
        let (other_store, two) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "x".into())
            .await
            .expect("put");

        let src = resolved(&one, "test://b/a");
        let same = resolved(&one, "test://b/c");
        let elsewhere = resolved(&two, "test://B/c");
        assert!(
            server_side_copy(&src, &same, false, None)
                .await
                .expect("copy")
        );
        assert!(store.head(&ObjectPath::from("c")).await.is_ok());
        assert!(
            !server_side_copy(&src, &elsewhere, false, None)
                .await
                .expect("skip")
        );
        assert!(other_store.head(&ObjectPath::from("c")).await.is_err());
    }

    #[tokio::test]
    async fn plain_copy_between_backends_with_equal_names_streams() {
        let (store, one) = memory_backend();
        let (other_store, two) = memory_backend();
        store
            .put(&ObjectPath::from("a"), "x".into())
            .await
            .expect("put");
        let (src, dst) = (resolved(&one, "s3://Logs/a"), resolved(&two, "s3://logs/b"));

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(store.head(&ObjectPath::from("a")).await.is_err());
        assert!(store.head(&ObjectPath::from("b")).await.is_err());
        assert!(other_store.head(&ObjectPath::from("b")).await.is_ok());
    }

    /// What the [`Spy`] store does to the source right after a copy.
    #[derive(Debug, Default)]
    enum AfterCopy {
        #[default]
        Nothing,
        /// A concurrent writer replaces the source.
        Rewrite,
        /// A concurrent deleter removes the source.
        Delete,
    }

    /// An `InMemory` store that counts `HEAD` and copy calls and can change the
    /// source right after a copy.
    #[derive(Debug, Default)]
    struct Spy {
        inner: InMemory,
        heads: std::sync::atomic::AtomicUsize,
        copies: std::sync::atomic::AtomicUsize,
        after_copy: AfterCopy,
    }

    impl std::fmt::Display for Spy {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("Spy")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for Spy {
        async fn put_opts(
            &self,
            location: &ObjectPath,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &ObjectPath,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &ObjectPath,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            if options.head {
                self.heads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &ObjectPath,
            to: &ObjectPath,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.copies
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.inner.copy_opts(from, to, options).await?;
            match self.after_copy {
                AfterCopy::Nothing => {}
                AfterCopy::Rewrite => {
                    self.inner.put(from, "newer bytes".into()).await?;
                }
                AfterCopy::Delete => self.inner.delete(from).await?,
            }
            Ok(())
        }
    }

    impl Spy {
        fn count(counter: &std::sync::atomic::AtomicUsize) -> usize {
            counter.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    async fn spy_backend(after_copy: AfterCopy) -> (Arc<Spy>, Arc<dyn Backend>) {
        let spy = Arc::new(Spy {
            after_copy,
            ..Spy::default()
        });
        spy.inner
            .put(&ObjectPath::from("a"), "old bytes".into())
            .await
            .expect("put");
        let backend: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: spy.clone(),
            multipart: false,
        });
        (spy, backend)
    }

    #[tokio::test]
    async fn a_same_store_move_keeps_a_source_rewritten_after_the_copy() {
        let (spy, backend) = spy_backend(AfterCopy::Rewrite).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        let err = Plain.copy(&src, &dst, true).await.expect_err("changed");
        assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
        let source = spy.inner.get(&ObjectPath::from("a")).await.expect("source");
        assert_eq!(
            source.bytes().await.expect("bytes").as_ref(),
            b"newer bytes"
        );
        let copy = spy.inner.get(&ObjectPath::from("c")).await.expect("copy");
        assert_eq!(copy.bytes().await.expect("bytes").as_ref(), b"old bytes");
    }

    #[tokio::test]
    async fn a_same_store_move_deletes_an_unchanged_source() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    #[tokio::test]
    async fn a_same_store_move_of_a_vanished_source_is_ok() {
        let (spy, backend) = spy_backend(AfterCopy::Delete).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        Plain.copy(&src, &dst, true).await.expect("move");
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    #[tokio::test]
    async fn an_s3_source_over_5_gib_streams_instead() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        let done = server_side_copy(&src, &dst, false, Some(6 << 30))
            .await
            .expect("decision");
        assert!(!done);
        assert_eq!(Spy::count(&spy.copies), 0);
        assert_eq!(Spy::count(&spy.heads), 0);
    }

    #[tokio::test]
    async fn a_small_s3_source_copies_server_side() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "s3://b/a"),
            resolved(&backend, "s3://b/c"),
        );

        assert!(
            server_side_copy(&src, &dst, false, Some(1))
                .await
                .expect("copy")
        );
        assert_eq!((Spy::count(&spy.heads), Spy::count(&spy.copies)), (0, 1));

        assert!(
            server_side_copy(&src, &dst, false, None)
                .await
                .expect("copy")
        );
        assert_eq!((Spy::count(&spy.heads), Spy::count(&spy.copies)), (1, 2));
    }

    #[tokio::test]
    async fn other_schemes_ignore_the_s3_limit() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "gs://b/a"),
            resolved(&backend, "gs://b/c"),
        );

        assert!(
            server_side_copy(&src, &dst, false, Some(6 << 30))
                .await
                .expect("copy")
        );
        assert_eq!(Spy::count(&spy.copies), 1);
    }

    #[tokio::test]
    async fn a_file_copy_never_runs_server_side() {
        let (spy, backend) = spy_backend(AfterCopy::Nothing).await;
        let (src, dst) = (
            resolved(&backend, "file:///a"),
            resolved(&backend, "file:///c"),
        );

        assert!(
            !server_side_copy(&src, &dst, false, None)
                .await
                .expect("copy")
        );
        assert_eq!(Spy::count(&spy.copies), 0);

        // A move on one device stays a rename.
        assert!(
            server_side_copy(&src, &dst, true, None)
                .await
                .expect("move")
        );
        assert!(spy.inner.head(&ObjectPath::from("a")).await.is_err());
        assert!(spy.inner.head(&ObjectPath::from("c")).await.is_ok());
    }

    async fn put_with_attributes(store: &InMemory, name: &str, body: Bytes) {
        let mut attributes = object_store::Attributes::new();
        attributes.insert(Attribute::ContentType, "image/png".into());
        attributes.insert(Attribute::CacheControl, "max-age=60".into());
        attributes.insert(Attribute::StorageClass, "COLD".into());
        attributes.insert(Attribute::Metadata("owner".into()), "me".into());
        let opts = object_store::PutOptions {
            attributes,
            ..Default::default()
        };
        store
            .put_opts(&ObjectPath::from(name), body.into(), opts)
            .await
            .expect("put");
    }

    async fn attributes_of(store: &InMemory, name: &str) -> object_store::Attributes {
        let head = GetOptions {
            head: true,
            ..Default::default()
        };
        store
            .get_opts(&ObjectPath::from(name), head)
            .await
            .expect("head")
            .attributes
    }

    #[tokio::test]
    async fn a_streamed_copy_keeps_the_content_type() {
        for (multipart, len) in [(false, 5), (true, 9 << 20)] {
            let store = Arc::new(InMemory::new());
            let target = Arc::new(InMemory::new());
            let backend = |store: &Arc<InMemory>| -> Arc<dyn Backend> {
                Arc::new(crate::backend::TestBackend {
                    store: store.clone(),
                    multipart,
                })
            };
            let (one, two) = (backend(&store), backend(&target));
            put_with_attributes(&store, "a", Bytes::from(vec![7_u8; len])).await;

            let (src, dst) = (resolved(&one, "s3://b/a"), resolved(&two, "s3://c/b"));
            Plain.copy(&src, &dst, false).await.expect("copy");

            let kept = attributes_of(&target, "b").await;
            assert_eq!(
                kept.get(&Attribute::ContentType).map(AsRef::as_ref),
                Some("image/png"),
                "multipart={multipart}"
            );
            assert_eq!(
                kept.get(&Attribute::CacheControl).map(AsRef::as_ref),
                Some("max-age=60"),
                "multipart={multipart}"
            );
            assert!(kept.get(&Attribute::StorageClass).is_none());
        }
    }

    #[tokio::test]
    async fn metadata_is_kept_only_within_one_provider() {
        let store = Arc::new(InMemory::new());
        let target = Arc::new(InMemory::new());
        let backend = |store: &Arc<InMemory>| -> Arc<dyn Backend> {
            Arc::new(crate::backend::TestBackend {
                store: store.clone(),
                multipart: false,
            })
        };
        let (one, two) = (backend(&store), backend(&target));
        put_with_attributes(&store, "a", Bytes::from_static(b"x")).await;
        let owner = Attribute::Metadata("owner".into());

        let src = resolved(&one, "s3://b/a");
        Plain
            .copy(&src, &resolved(&two, "s3://c/same"), false)
            .await
            .expect("s3 to s3");
        Plain
            .copy(&src, &resolved(&two, "gs://c/other"), false)
            .await
            .expect("s3 to gs");

        let same = attributes_of(&target, "same").await;
        assert_eq!(same.get(&owner).map(AsRef::as_ref), Some("me"));
        let other = attributes_of(&target, "other").await;
        assert!(other.get(&owner).is_none());
        assert!(other.get(&Attribute::ContentType).is_some());
    }

    #[test]
    fn copying_from_carries_no_attributes() {
        let (_, backend) = memory_backend();
        let options = UploadOptions::copying_from(backend.as_ref(), &ObjectPath::from("a"), 1);
        assert!(options.attributes.is_empty());
    }

    async fn source_meta(store: &InMemory, name: &str) -> ObjectMeta {
        store.head(&ObjectPath::from(name)).await.expect("head")
    }

    #[tokio::test]
    async fn a_source_is_deleted_only_if_it_is_what_was_copied() {
        let (store, backend) = memory_backend();
        let path = ObjectPath::from("a");
        let src = resolved(&backend, "test://b/a");

        // Unchanged: deleted.
        store.put(&path, "one".into()).await.expect("put");
        let read = source_meta(&store, "a").await;
        delete_unchanged_source(&src, &read).await.expect("delete");
        assert!(store.head(&path).await.is_err());

        // Gone: nothing to do.
        delete_unchanged_source(&src, &read).await.expect("gone");

        // Overwritten after the read (even with the same size): kept.
        store.put(&path, "one".into()).await.expect("put");
        let read = source_meta(&store, "a").await;
        store.put(&path, "two".into()).await.expect("put");
        let err = delete_unchanged_source(&src, &read)
            .await
            .expect_err("changed");
        assert!(matches!(err, Error::SourceChanged { .. }), "{err:?}");
        assert!(store.head(&path).await.is_ok());
    }

    #[test]
    fn a_cross_device_error_is_found_through_the_source_chain() {
        let wrap = |source: std::io::Error| object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(source),
        };
        assert!(crosses_devices(&wrap(std::io::Error::from(
            std::io::ErrorKind::CrossesDevices
        ))));
        assert!(!crosses_devices(&wrap(std::io::Error::from_raw_os_error(
            2
        ))));
    }

    #[test]
    fn source_changed_names_the_object_without_credentials() {
        let e = Error::SourceChanged {
            url: redacted(&Url::parse("http://u:pw@h/a?sig=1#f").expect("url")),
        };
        assert_eq!(
            e.to_string(),
            "source changed during move; it was not deleted: http://h/a"
        );
    }
}
