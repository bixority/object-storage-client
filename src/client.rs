use crate::backend::{Backend, body_stream};
use crate::codec::{ByteStream, CallerContext, Codec, Location, Plain, Resolved, same_name};
#[cfg(feature = "encryption")]
use crate::encryption::{EncryptionConfig, Envelope};
use crate::registry::{StoreRegistry, object_path, origin, parse_url};
use crate::sign::{MAX_PRESIGN_EXPIRY, SignMethod, SignOptions};
use crate::text;
use crate::upload::{UploadOptions, put_bytes, upload_stream};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use object_store::{ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url::Url;

pub(crate) mod local_delete;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// A storage error. Its message may include the request URL, so do not
    /// put secrets in object URLs (queries and user names are refused).
    ///
    /// Provider text in it is escaped and capped when the error is converted
    /// (see the `From<object_store::Error>` impl), so it is safe to log.
    #[error("Object store error: {0}")]
    ObjectStore(#[source] object_store::Error),

    /// The store refused the request as unauthenticated or forbidden (HTTP
    /// 401 or 403), from any call: object calls, `create_bucket` and
    /// `bucket_exists`. Python raises `PermissionError`.
    ///
    /// The text is escaped and capped like the other provider text.
    #[error("Access denied: {0}")]
    Denied(String),

    /// The object is archived and cannot be read until it is restored (S3
    /// `GLACIER`, `DEEP_ARCHIVE`) or rehydrated (Azure `Archive` tier). Python
    /// raises `PermissionError`.
    ///
    /// The text is escaped and capped like the other provider text.
    #[error("Archived: {0}")]
    Archived(String),

    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

    /// A local file operation of this crate failed (the `file://` store, the
    /// staging of writes and downloads, the local delete walk). Operations that
    /// go through `object_store` report [`Error::ObjectStore`] instead.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Unsupported scheme: {0}")]
    UnsupportedScheme(String),

    #[error("Pre-signed URLs are not supported for scheme: {0}")]
    SigningUnsupported(String),

    #[error("Bucket creation is not supported for scheme: {0}")]
    BucketCreationUnsupported(String),

    #[error("Bucket existence checks are not supported for scheme: {0}")]
    BucketExistenceUnsupported(String),

    /// An exception raised by Python code, passed through unchanged.
    #[cfg(feature = "python")]
    #[error("{0}")]
    Python(pyo3::PyErr),

    /// The object is not an encrypted object; read it with the plain client.
    #[cfg(feature = "encryption")]
    #[error("Not an encrypted object: {url}")]
    NotEncrypted { url: String },

    /// The stored bytes are malformed or failed authentication: tampering,
    /// truncation, a wrong binding or a data key that does not unwrap. A data
    /// key that fails to unwrap counts as tampering because the keyring's
    /// check value rules out a genuine key mismatch.
    ///
    /// Deliberately without `#[from]`, so a `?` cannot turn a "not encrypted"
    /// result into this variant.
    #[cfg(feature = "encryption")]
    #[error("Decryption error: {0}")]
    Decryption(crate::encryption::DecryptError),

    /// A key problem other than a failed unwrap: no key for the object is
    /// available ([`KeyError::UnknownKey`](crate::encryption::keys::KeyError)),
    /// or the key provider failed. A failed unwrap on read is reported as
    /// [`Error::Decryption`].
    ///
    /// `UnknownKey` is derived from unauthenticated header data, so besides a
    /// missing key it may also indicate tampering.
    #[cfg(feature = "encryption")]
    #[error("Key error: {0}")]
    Key(#[from] crate::encryption::keys::KeyError),

    /// A caller-supplied value was rejected before any I/O: for example a
    /// reserved or oversized encryption context.
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// A move copied the source but the source no longer matches what was
    /// copied (overwritten by a concurrent writer, or by the move's own write
    /// through another name for the same object), so it was not deleted. The
    /// destination stays written.
    #[error("source changed during move; it was not deleted: {url}")]
    SourceChanged { url: String },

    /// An update, copy or move was abandoned because the object changed (or
    /// vanished) after it was read. Nothing was written and a move kept its
    /// source.
    #[error("the object changed during the update; nothing was written")]
    Changed,

    #[error("Generic error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Metadata for an object in storage.
///
/// Returned by [`Client::get_object_metadata`]. For encrypted objects,
/// `size_bytes` and `encryption` are derived from the unauthenticated stored
/// header and length: anyone able to modify the stored object can change them.
/// Alongside the
/// fields carried by `object_store`'s `ObjectMeta` (location, last modified
/// time, size, e-tag and version) this also exposes the stored `Content-Type`,
/// read from the object's attributes — useful for verifying that a completed
/// upload wrote the bytes and content type a presigned URL was issued for.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ObjectMetadata {
    /// The object's path within the store.
    pub location: String,
    /// The last modification time reported by the store.
    pub last_modified: chrono::DateTime<chrono::Utc>,
    /// The object size in bytes.
    pub size_bytes: u64,
    /// The size as stored; equal to `size_bytes` for plain objects.
    pub stored_size_bytes: u64,
    /// The stored `Content-Type`, or `None` if the backend reported none.
    pub content_type: Option<String>,
    /// The object's e-tag, if the backend reports one.
    pub e_tag: Option<String>,
    /// The object's version, if the backend reports one.
    pub version: Option<String>,
    /// Encryption details, or `None` for plain objects.
    pub encryption: Option<EncryptionInfo>,
}

/// Envelope details of an encrypted object.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EncryptionInfo {
    pub format: String,
    /// Message ID, hex-encoded.
    pub message_id: String,
    pub key_names: Vec<String>,
    pub frame_length: u32,
    pub context: BTreeMap<String, String>,
}

/// A unified object storage client that handles multiple backends based on URL
/// schemes, storing objects through the codec `C`.
#[derive(Clone)]
pub struct Client<C: Codec = Plain> {
    registry: Arc<StoreRegistry>,
    codec: C,
}

/// The plain (unencrypted) client.
pub type ObjectStorageClient = Client<Plain>;

/// Settings of [`Client::reencrypt`](Client::<Envelope>::reencrypt).
#[cfg(feature = "encryption")]
#[derive(Debug, Clone)]
pub struct ReencryptOptions {
    /// The key ID whose objects are rewritten under the active key.
    pub from: String,
    /// Only objects last modified before this time are rewritten.
    ///
    /// The cutoff compares the modification time the store reports, which is
    /// not authenticated. It only screens out forgeries by holders of leaked
    /// storage credentials on stores that set the time themselves at commit.
    /// The provider, anyone who can set file times on `file://` or NFS, and
    /// S3 multipart uploads started before the cutoff are exceptions. An
    /// object for which the store reports no time counts as after the cutoff.
    /// If a party that may hold the leaked key can also influence
    /// modification times, treat every object under that key as suspect and
    /// check candidates against an independent record (application records,
    /// provider audit logs or inventory) before re-encrypting.
    pub before: Option<chrono::DateTime<chrono::Utc>>,
    /// Decide only; never write.
    pub dry_run: bool,
}

/// What [`Client::reencrypt`](Client::<Envelope>::reencrypt) did to an object.
#[cfg(feature = "encryption")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reencrypted {
    /// The object was rewritten under the active key.
    Rewritten,
    /// Dry run: the object would have been rewritten.
    WouldRewrite,
    /// No key of the object is the `from` key (this includes objects already
    /// under the active key).
    Skipped,
    /// The object is not an encrypted object.
    NotEncrypted,
    /// The object was last modified at or after `before`, or the store
    /// reports no modification time; left alone. The time is not
    /// authenticated: see [`ReencryptOptions::before`].
    AfterCutoff,
    /// The object changed or vanished while it was being rewritten; the newer
    /// content is untouched.
    Changed,
}

#[cfg(feature = "encryption")]
impl Reencrypted {
    /// The outcome as a stable lowercase name, e.g. `would-rewrite`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rewritten => "rewritten",
            Self::WouldRewrite => "would-rewrite",
            Self::Skipped => "skipped",
            Self::NotEncrypted => "not-encrypted",
            Self::AfterCutoff => "after-cutoff",
            Self::Changed => "changed",
        }
    }
}

/// The client that encrypts every object it writes and decrypts every object
/// it reads.
#[cfg(feature = "encryption")]
pub type EncryptedObjectStorageClient = Client<crate::encryption::Envelope>;

impl<C: Codec> Client<C> {
    /// Parses a URL string into a [`Url`], treating absolute or relative paths
    /// without a scheme as `file://` URLs. A local path is treated exactly like
    /// the `file://` URL of its absolute path, so `..` is resolved lexically.
    #[doc(hidden)]
    pub fn parse_url(url: &str) -> Result<Url> {
        parse_url(url)
    }

    /// Resolves the provider-specific [`Backend`] for a given URL, building it
    /// on first use and caching it by (scheme, userinfo, host, port).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    pub(crate) fn get_backend(&self, url: &Url) -> Result<(Arc<dyn Backend>, ObjectPath)> {
        self.registry.resolve(url)
    }

    /// Serves the origin of `url` from `store`, for unit tests.
    #[cfg(all(test, any(feature = "cli", feature = "encryption", feature = "gui")))]
    pub(crate) fn preload_store(&self, url: &str, store: Arc<dyn ObjectStore>) {
        let backend = crate::backend::TestBackend {
            store,
            multipart: true,
        };
        self.registry
            .preload(&parse_url(url).expect("url"), Arc::new(backend));
    }

    /// Number of distinct backends this client's registry has built.
    #[doc(hidden)]
    pub fn backend_count(&self) -> usize {
        self.registry.len()
    }

    /// Resolves the correct `ObjectStore` for a given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    #[doc(hidden)]
    pub fn get_store(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let (backend, path) = self.get_backend(url)?;
        Ok((Arc::clone(backend.store()), path))
    }

    /// Resolves an object URL: as [`Self::resolve_prefix`], and the URL must
    /// name an object, not just a bucket.
    pub(crate) fn resolve(&self, url: &str) -> Result<Resolved> {
        self.resolve_checked(url, Dots::OffFile, true)
    }

    /// Resolves a URL that may be a bucket or a prefix (list, bucket calls).
    fn resolve_prefix(&self, url: &str) -> Result<Resolved> {
        self.resolve_checked(url, Dots::OffFile, false)
    }

    /// Parses and checks `raw` before any backend is built, then resolves it.
    fn resolve_checked(&self, raw: &str, dots: Dots, needs_object: bool) -> Result<Resolved> {
        self.resolve_url(parse_checked(raw, dots, needs_object)?)
    }

    /// Identity of this client's registry, told apart from other clients'.
    fn registry_id(&self) -> usize {
        Arc::as_ptr(&self.registry) as usize
    }

    /// Resolves the checked `url`.
    fn resolve_url(&self, url: Url) -> Result<Resolved> {
        let (backend, path) = self.registry.resolve(&url)?;
        Ok(Resolved {
            backend,
            registry: self.registry_id(),
            loc: Location { url, path },
        })
    }

    /// Resolves the prefix of a recursive delete, refusing the inputs that
    /// would delete everything the process may delete. It never deletes.
    fn resolve_delete_prefix(&self, url: &str) -> Result<Resolved> {
        if url.trim().is_empty() {
            return Err(Error::InvalidInput(
                "delete_by_prefix needs a non-empty URL".into(),
            ));
        }
        let r = self.resolve_checked(url, Dots::Everywhere, false)?;
        // A root (`/`, or a drive root on Windows) has no parent.
        if r.loc.url.scheme() == "file" && crate::backend::fs_path(&r.loc.path)?.parent().is_none()
        {
            return Err(Error::InvalidInput(
                "delete_by_prefix refuses the file system root".into(),
            ));
        }
        Ok(r)
    }

    /// Resolves both ends of a copy or move, sharing one backend between URLs
    /// of one origin so that a server-side copy stays possible.
    fn resolve_pair(&self, from: &str, to: &str) -> Result<(Resolved, Resolved)> {
        let from = parse_checked(from, Dots::OffFile, true)?;
        let to = parse_checked(to, Dots::OffFile, true)?;
        let ((src, src_path), (dst, dst_path)) = self.registry.resolve_pair(&from, &to)?;
        Ok((
            Resolved {
                backend: src,
                registry: self.registry_id(),
                loc: Location {
                    url: from,
                    path: src_path,
                },
            },
            Resolved {
                backend: dst,
                registry: self.registry_id(),
                loc: Location {
                    url: to,
                    path: dst_path,
                },
            },
        ))
    }

    /// Stream object's data from the given URL.
    ///
    /// With an encrypting codec each chunk is authenticated before it is
    /// yielded, but the data is provisional until the stream ends cleanly:
    /// truncation or tampering surfaces as an `Err` at the point it is
    /// detected. After an `Err` item the stream ends; the object is unverified
    /// and must not be treated as complete.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error fetching the object from the store.
    pub async fn get_stream(&self, url: &str) -> Result<ByteStream> {
        let r = self.resolve(url)?;
        let result = r.backend.store().get(&r.loc.path).await?;
        Ok(self.codec.decode(r.loc, body_stream(result)))
    }

    /// Downloads an object's data from the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error fetching the object from the store.
    pub async fn get(&self, url: &str) -> Result<Bytes> {
        let r = self.resolve(url)?;
        let result = r.backend.store().get(&r.loc.path).await?;
        self.codec.get_bytes(&r.loc, result).await
    }

    /// Uploads an object's data to the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error putting the object to the store.
    pub async fn put<D>(&self, url: &str, data: D) -> Result<()>
    where
        D: Into<Bytes>,
    {
        self.put_inner(url, data.into(), &CallerContext::new())
            .await
    }

    async fn put_inner(&self, url: &str, data: Bytes, ctx: &CallerContext) -> Result<()> {
        let r = self.resolve(url)?;
        let defaults = UploadOptions::default();
        // A large body takes the streaming path, so an encrypting codec seals
        // it in slices beside the upload and never holds the whole ciphertext.
        // `http(s)` buffers everything anyway, so it keeps the one-shot path.
        if data.len() >= defaults.part_size
            && (r.backend.supports_multipart() || r.backend.local_path(&r.loc.path)?.is_some())
        {
            let opts = UploadOptions {
                size_hint: Some(data.len() as u64),
                ..defaults
            };
            let chunk: Result<Bytes> = Ok(data);
            return self
                .put_stream_inner(
                    url,
                    futures::stream::iter([chunk]).boxed(),
                    ctx.clone(),
                    &opts,
                )
                .await;
        }
        let stored = self.codec.encode(&r.loc, ctx, data).await?;
        put_bytes(r.backend.as_ref(), &r.loc.path, stored).await
    }

    /// Uploads a stream of chunks to the given URL.
    ///
    /// Input shorter than one part (8 MiB) is stored with a single `put`;
    /// longer input uses a multipart upload with bounded in-flight parts. As
    /// the size is unknown, parts are 8 MiB and providers allow 10,000 parts,
    /// so input beyond about 78 GiB fails the upload. The
    /// object is committed only when the input ends cleanly; on an input error,
    /// or if the returned future is dropped, the upload is aborted. The `http(s)`
    /// scheme has no multipart upload, so its input is buffered in memory.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid, the input yields an error, or the
    /// store rejects the upload.
    pub async fn put_stream<S>(&self, url: &str, data: S) -> Result<()>
    where
        S: Stream<Item = Result<Bytes>> + Send + 'static,
    {
        self.put_stream_inner(
            url,
            data.boxed(),
            CallerContext::new(),
            &UploadOptions::default(),
        )
        .await
    }

    /// [`Self::put_stream`] of a local file with permission bits `source_mode`:
    /// a new `file://` destination takes them (less the umask), as a copy does,
    /// so uploading a private file does not create a world-readable one.
    #[cfg_attr(not(feature = "cli"), allow(dead_code))]
    pub(crate) async fn put_local_file(
        &self,
        url: &str,
        data: ByteStream,
        source_mode: Option<u32>,
        size_hint: Option<u64>,
    ) -> Result<()> {
        let opts = UploadOptions {
            source_mode,
            size_hint,
            ..UploadOptions::default()
        };
        self.put_stream_inner(url, data, CallerContext::new(), &opts)
            .await
    }

    pub(crate) async fn put_stream_inner(
        &self,
        url: &str,
        data: ByteStream,
        ctx: CallerContext,
        opts: &UploadOptions,
    ) -> Result<()> {
        let r = self.resolve(url)?;
        // Before the upload creates directories or staging files.
        self.codec.check_write(&r.loc, &ctx)?;
        let stored = self.codec.encode_stream(r.loc.clone(), ctx, data);
        upload_stream(r.backend.as_ref(), &r.loc.path, stored, opts).await
    }

    /// Deletes an object at the given URL.
    ///
    /// On `http(s)://` a `PROPFIND` first checks what the URL names: a
    /// `WebDAV` collection is refused, because deleting it would remove
    /// everything in it; see [`delete_by_prefix`](Self::delete_by_prefix).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - The URL names a `WebDAV` collection ([`Error::InvalidInput`]), or the
    ///   server cannot say what it names.
    /// - There is an error deleting the object from the store.
    pub async fn delete(&self, url: &str) -> Result<()> {
        let r = self.resolve(url)?;
        refuse_collection(
            &r,
            "deleting it would remove everything in it; use delete_by_prefix",
        )
        .await?;
        r.backend.store().delete(&r.loc.path).await?;
        Ok(())
    }

    /// Deletes the objects at `urls` through the store's batched
    /// `delete_stream` (1,000 keys per request on S3 and Azure), and calls
    /// `each(index, result)` once per URL as its outcome is known. A key that
    /// is already gone counts as deleted. A URL that does not resolve fails on
    /// its own. Unlike [`delete`](Self::delete) it does not look for a
    /// `WebDAV` collection, so callers keep `http(s)://` and `file://` URLs
    /// on `delete`.
    ///
    /// A failure the store does not tie to a key is given to the keys that
    /// got no result, in order, or to all of them when the counts differ.
    #[cfg(feature = "gui")]
    pub(crate) async fn delete_many(
        &self,
        urls: &[String],
        mut each: impl FnMut(usize, Result<()>),
    ) {
        type Group = (Arc<dyn Backend>, Vec<(usize, ObjectPath)>);
        let mut groups: Vec<Group> = Vec::new();
        for (index, url) in urls.iter().enumerate() {
            match self.resolve(url) {
                Err(e) => each(index, Err(e)),
                Ok(r) => match groups.iter_mut().find(|(b, _)| Arc::ptr_eq(b, &r.backend)) {
                    Some((_, targets)) => targets.push((index, r.loc.path)),
                    None => groups.push((r.backend, vec![(index, r.loc.path)])),
                },
            }
        }
        for (backend, targets) in groups {
            delete_batch(backend.store().as_ref(), targets, &mut each).await;
        }
    }

    /// Deletes all objects under the given URL prefix.
    ///
    /// Cloud stores list the keys strictly below the prefix and delete them
    /// through the store's `delete_stream` (batched where the backend
    /// supports it). The prefix's own folder marker (a key ending in `/`) is
    /// not below it and stays, as does every other key ending in `/`: such
    /// keys cannot be addressed through `object_store`. A key that vanishes
    /// during the run counts as deleted. A `file://` prefix is walked without
    /// following symlinks (links are deleted as links), directories are kept,
    /// `name#<digits>` staging files are skipped and trees deeper than 128
    /// levels stop the walk with [`Error::InvalidInput`]; files deleted before that
    /// point stay deleted.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error listing or deleting objects from the store.
    ///
    /// An empty or whitespace-only URL, and a `file://` prefix that resolves to
    /// the file system root, are rejected with [`Error::InvalidInput`]: they
    /// would delete everything the process may delete. A cloud URL naming only
    /// a bucket (an empty object prefix) deletes every object in the bucket.
    pub async fn delete_by_prefix(&self, url: &str) -> Result<()> {
        let r = self.resolve_delete_prefix(url)?;
        if r.loc.url.scheme() == "file" {
            // object_store's local listing follows symlinked directories, so a
            // link below the prefix would redirect the deletes outside it.
            return local_delete::delete_by_prefix(r.loc.path).await;
        }
        delete_listed(r.backend.store(), r.loc.path).await
    }

    /// Lists objects under the given URL prefix. On `file://`, symlinks below
    /// the prefix are not listed or entered; the prefix itself is resolved as
    /// named.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error listing objects from the store.
    pub async fn list(&self, url: &str) -> Result<Vec<String>> {
        self.list_stream(url)?.try_collect().await
    }

    /// Streams the names of the objects under the given URL prefix, relative
    /// to it, in the order the store lists them, so a huge listing need not be
    /// held in memory. After an `Err` item the listing is incomplete.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or the scheme is unsupported;
    /// listing errors come as stream items.
    pub fn list_stream(&self, url: &str) -> Result<BoxStream<'static, Result<String>>> {
        Ok(self.list_meta_stream(url)?.map_ok(|(name, _)| name).boxed())
    }

    /// [`Self::list_stream`] with each object's listed size in bytes (for an
    /// encrypted object the stored size, which is not authenticated).
    pub(crate) fn list_meta_stream(
        &self,
        url: &str,
    ) -> Result<BoxStream<'static, Result<(String, u64)>>> {
        let r = self.resolve_prefix(url)?;
        let (store, path) = (r.backend.store(), r.loc.path);
        Ok(relative_listing(store.list(Some(&path)), path))
    }

    /// One page of the immediate children of the directory `url`: folder names
    /// and objects, as single-segment names relative to it. `page_token` is a
    /// `next` from an earlier page; stores without paged listing return one
    /// complete page, and an empty one for a token. A missing directory is an
    /// empty page. The order is the store's. For `file://`, symlinks are not
    /// folders or objects: their names are in `links`.
    ///
    /// A provider root (`s3://`, `gs://`, `az://` and the other bucket schemes
    /// with no bucket) lists the buckets or containers of the credentials as
    /// folders, sorted, in one page with no `next` (a token gives an empty
    /// page, and `max` is ignored). A folder name is the bucket name itself,
    /// not escaped: `<scheme>://<name>/` opens it. No other operation accepts
    /// a provider root.
    #[cfg(feature = "gui")]
    pub(crate) async fn list_dir(
        &self,
        url: &str,
        page_token: Option<String>,
        max: usize,
    ) -> Result<DirPage> {
        let url = parse_checked(url, Dots::OffFile, false)?;
        if let Some(scheme) = crate::registry::provider_root(&url) {
            if page_token.is_some() {
                return Ok(DirPage::default());
            }
            return Ok(DirPage {
                folders: self.registry.list_buckets(scheme).await?,
                ..DirPage::default()
            });
        }
        let r = self.resolve_url(url)?;
        let path = r.loc.path;
        let first = page_token.is_none();
        let listed = r.backend.list_page(&path, max, page_token).await?;
        let next = listed.next;
        let links = if first && r.backend.scheme() == "file" {
            local_links(crate::backend::fs_path(&path)?).await?
        } else {
            Vec::new()
        };
        let folders = listed
            .prefixes
            .iter()
            .filter_map(|p| relative_str(p, &path).map(str::to_owned))
            .collect();
        let objects = listed
            .objects
            .into_iter()
            .filter_map(|o| {
                relative_str(&o.location, &path).map(|name| DirObject {
                    name: name.to_owned(),
                    size: o.size,
                    modified: o.modified,
                    state: o.state,
                    class: o.class,
                })
            })
            .collect();
        Ok(DirPage {
            folders,
            objects,
            links,
            next,
        })
    }

    /// Retrieves metadata for the object at the given URL.
    ///
    /// The plain client issues a single HEAD request via
    /// [`GetOptions::head`](object_store::GetOptions::head); the encrypted
    /// client issues one ranged GET of 1 KiB for the header instead (a second of at most
    /// `DecodeLimits::max_header_len` bytes, 16 KiB by default, when the header is larger), followed by a HEAD
    /// only when that GET fails with an error other than not-found. Either
    /// way it returns the location, last modified time, size, e-tag and version from the object
    /// metadata together with the `Content-Type` read from the object
    /// attributes. A missing object is reported as an error
    /// ([`object_store::Error::NotFound`], surfaced as `FileNotFoundError` from
    /// the Python bindings); use [`Self::exists`] for a non-erroring presence
    /// check. For encrypted objects `size_bytes` and `encryption` are
    /// unauthenticated (see [`ObjectMetadata`]).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - The object does not exist.
    /// - There is an error retrieving metadata from the store.
    pub async fn get_object_metadata(&self, url: &str) -> Result<ObjectMetadata> {
        let r = self.resolve(url)?;
        self.codec.describe(r.backend.as_ref(), &r.loc).await
    }

    /// Returns whether an object exists at the given URL.
    ///
    /// Issues a single HEAD request through the backend. A missing object
    /// yields `Ok(false)` rather than an error; any other failure (invalid
    /// URL, unsupported scheme, network/permission error) is propagated.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - The store fails for a reason other than the object not existing.
    pub async fn exists(&self, url: &str) -> Result<bool> {
        let r = self.resolve(url)?;
        let (store, path) = (r.backend.store(), r.loc.path);

        match store.head(&path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Creates the bucket / container identified by `url`.
    ///
    /// The bucket is identified by the URL's scheme and host; any path
    /// component is ignored, except for `file://` URLs where the path is the
    /// directory to create. `object_store` exposes no bucket-management API
    /// (and bundles no provider SDK), so this is implemented per backend by
    /// issuing each provider's native bucket-creation request, **signed with
    /// `object_store`'s own public authorizers** so no signing logic is
    /// reimplemented and no new dependency is introduced. The per-provider
    /// implementations live in the `crate::backend` modules:
    ///
    /// - `file://` — creates the directory (and any missing parents).
    /// - `s3://` — S3 `CreateBucket` (`PUT /<bucket>`), signed with
    ///   `AwsAuthorizer`. Idempotent against `BucketAlreadyOwnedByYou`; an
    ///   error for `BucketAlreadyExists` (another account owns the name) and
    ///   for every other failure.
    /// - `gs://`, `gcs://` — GCS JSON API `buckets.insert`, authenticated with
    ///   the backend's `OAuth2` bearer credential. On HTTP 409 the project's
    ///   bucket list is read: the call succeeds only if `GOOGLE_CLOUD_PROJECT`
    ///   lists the bucket (it needs `storage.buckets.list`), and fails if
    ///   another project owns the name.
    /// - `az://`, `wasb(s)://`, `abfs(s)://` — Azure `Create Container`
    ///   (`PUT /<container>?restype=container`), signed with `AzureAuthorizer`.
    ///   Idempotent against `ContainerAlreadyExists` / HTTP 409.
    ///
    /// Because `object_store` does not expose a built store's resolved
    /// endpoint, region, account or project, those are read from configuration
    /// (the same environment the backend was built from):
    /// `S3_REGION` / `AWS_ENDPOINT`(`S3_ENDPOINT`) / `S3_ALLOW_HTTP` for S3,
    /// `GOOGLE_CLOUD_PROJECT` (`GCS_PROJECT_ID`) and `GCS_BASE_URL` (else
    /// `GOOGLE_BASE_URL`) for GCS, and the account name and endpoint of the
    /// Azure store (`AZURE_STORAGE_ACCOUNT_NAME`, `AZURE_STORAGE_ENDPOINT` or
    /// `AZURE_ENDPOINT`) for Azure. The Azure emulator and Fabric modes are
    /// refused unless an endpoint is set. A store that runs unsigned
    /// (`*_SKIP_SIGNATURE`) sends these requests unsigned too.
    ///
    /// An HTTP 401 or 403 answer is [`Error::Denied`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme does not support bucket creation.
    /// - Required configuration (GCS project, Azure account) is missing.
    /// - The backend rejects the request for a reason other than the bucket
    ///   already existing.
    pub async fn create_bucket(&self, url: &str) -> Result<()> {
        let r = self.resolve_prefix(url)?;
        r.backend.create_bucket(&r.loc.url).await
    }

    /// Returns whether the bucket / container identified by `url` exists.
    ///
    /// As with [`Self::create_bucket`], the bucket is identified by the URL's
    /// scheme and host; any path component is ignored, except for `file://`
    /// URLs where the path is the directory to probe. `object_store` exposes no
    /// bucket-management API, so this is implemented per backend:
    ///
    /// - `file://` — checks that the path exists and is a directory.
    /// - `s3://`, `gs://`, `az://` (and variants) — issues the provider's
    ///   dedicated metadata probe (S3 `HeadBucket`, Azure `Get Container
    ///   Properties`, GCS `buckets.get`) rather than listing objects. Success
    ///   means the bucket exists and `404 Not Found` that it is missing. `401`
    ///   and `403` (also the answer to a wrong key or signature) are errors:
    ///   existence is unknown.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme does not support bucket existence checks.
    /// - The backend fails for a reason other than the bucket not existing.
    pub async fn bucket_exists(&self, url: &str) -> Result<bool> {
        let r = self.resolve_prefix(url)?;
        r.backend.bucket_exists(&r.loc.url).await
    }

    /// Copies an object from one URL to another.
    /// Supports cross-provider copies. Copying an object onto itself does
    /// nothing.
    ///
    /// The plain client first reads the start of the source with one extra
    /// ranged request. It refuses an encrypted object that is bound to its
    /// name when the destination name differs, with [`Error::InvalidInput`]:
    /// copy it with the encrypted client. The encrypted client always decrypts
    /// and re-encrypts, never copying server-side, even inside one bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either URL is invalid.
    /// - Schemes are unsupported.
    /// - There is an error copying between stores.
    pub async fn copy(&self, from_url: &str, to_url: &str) -> Result<()> {
        let (src, dst) = self.resolve_pair(from_url, to_url)?;
        self.codec.copy(&src, &dst, false).await
    }

    /// Moves an object from one URL to another.
    /// Supports cross-provider moves. Moving an object onto itself does
    /// nothing. The checks and costs of [`copy`](Self::copy) apply.
    ///
    /// Every move, server-side or streamed, deletes the source only if it
    /// still matches what was copied: the object's metadata is compared before
    /// the delete. If it was gone, nothing is deleted; if it changed, for
    /// instance because a writer replaced it or the destination is another
    /// name for the source, the source is kept and [`Error::SourceChanged`] is
    /// returned; the destination stays written. A rewrite with identical bytes
    /// in the same second, on a store reporting no e-tag or version, is
    /// indistinguishable. Only a move inside one `file://` file system is an
    /// atomic rename.
    ///
    /// On `http(s)://` a `PROPFIND` first checks the source: a `WebDAV`
    /// collection is refused with [`Error::InvalidInput`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either URL is invalid.
    /// - Schemes are unsupported.
    /// - There is an error moving between stores.
    /// - The source changed during the move ([`Error::SourceChanged`]).
    pub async fn move_object(&self, from_url: &str, to_url: &str) -> Result<()> {
        let (src, dst) = self.resolve_pair(from_url, to_url)?;
        // A store's rename is copy then delete, which would delete the object.
        // Other aliases of one object (symlinks) are left to the codec: the
        // encrypted one must still re-bind the object to its new name.
        if same_name(&src.loc, &dst.loc) {
            return Ok(());
        }
        self.move_resolved(&src, &dst).await
    }

    /// Moves between resolved ends: refuses a `WebDAV` collection, which the
    /// delete would remove with everything in it, then copies and deletes.
    async fn move_resolved(&self, src: &Resolved, dst: &Resolved) -> Result<()> {
        refuse_collection(
            src,
            "moving it would remove everything in it; move the objects in it one by one",
        )
        .await?;
        self.codec.copy(src, dst, true).await
    }

    /// Copies (or moves, when `delete_source`) `from_url` read through `self`
    /// to `to_url` written through `dst`. One registry behaves as
    /// [`copy`](Self::copy) / [`move_object`](Self::move_object);
    /// two registries never copy server-side, as their credentials differ.
    ///
    /// A move between two registries onto the same name is refused with
    /// [`Error::InvalidInput`] before any I/O, because both may reach one
    /// object and the source is kept. A copy onto the same name is allowed.
    #[cfg(feature = "gui")]
    pub(crate) async fn transfer(
        &self,
        dst: &Self,
        from_url: &str,
        to_url: &str,
        size: Option<u64>,
        delete_source: bool,
    ) -> Result<()> {
        let one = Arc::ptr_eq(&self.registry, &dst.registry);
        if one && delete_source {
            return self.move_object(from_url, to_url).await;
        }
        let (src, to) = if one {
            self.resolve_pair(from_url, to_url)?
        } else {
            (self.resolve(from_url)?, dst.resolve(to_url)?)
        };
        if !delete_source {
            return self.codec.copy_sized(&src, &to, size).await;
        }
        if same_name(&src.loc, &to.loc) {
            return Err(Error::InvalidInput(
                "the two connections may reach the same object, so the move is refused \
                 and the source is kept"
                    .into(),
            ));
        }
        self.move_resolved(&src, &to).await
    }
}

impl Default for Client<Plain> {
    fn default() -> Self {
        Self {
            registry: Arc::default(),
            codec: Plain,
        }
    }
}

#[cfg(feature = "encryption")]
impl Client<Envelope> {
    /// A standalone encrypted client with its own store registry. It takes no
    /// AWS profile; to use one, call
    /// `ObjectStorageClient::new().with_s3_profile(p)?.with_encryption(cfg)`.
    #[must_use]
    pub fn new(cfg: EncryptionConfig) -> Self {
        Self {
            registry: Arc::default(),
            codec: Envelope::new(cfg),
        }
    }

    /// Reads the envelope details of an encrypted object from its header with
    /// one ranged read. Needs no keys.
    ///
    /// The result is **unauthenticated**: the header tag is not verified, so
    /// anyone able to modify the stored object can change what is reported.
    /// Use a read to authenticate it.
    ///
    /// # Errors
    ///
    /// [`Error::NotEncrypted`] for objects that are not envelope messages,
    /// [`Error::Decryption`] for malformed headers, and storage errors.
    pub async fn inspect_encryption(&self, url: &str) -> Result<EncryptionInfo> {
        let r = self.resolve(url)?;
        self.codec.inspect(r.backend.as_ref(), &r.loc).await
    }

    /// Rewrites an object encrypted under the key `opts.from` so that it is
    /// encrypted under the active key, at the same URL and with the same
    /// plaintext and caller context. See [`Reencrypted`] for the outcomes.
    ///
    /// The object is read once for its header (which also yields its version
    /// and modification time; the cutoff and the dry run are decided on that
    /// version), then read in full pinned to that version, decrypted with full
    /// authentication and encrypted again with a fresh data key and message
    /// ID. The result replaces the object only if it is still the version that
    /// was read; otherwise nothing is written and the outcome is
    /// [`Reencrypted::Changed`]. A final header read checks that the new
    /// object names a key of the keyring and no longer `opts.from`.
    ///
    /// As with a re-encrypting copy, the content type and user metadata of
    /// the object are not carried over.
    ///
    /// The object keeps its encryption context and its object-key binding
    /// (`osc:key`, or none), whatever the client's `bind_object_key`. A
    /// binding client refuses an object bound to another name, also in a dry
    /// run, as its reads would.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidInput`] if `opts.from` is the active key (rotate the
    /// keyring first), [`Error::Key`] with `UnknownId` if the
    /// keyring does not hold `opts.from`, both before any I/O, an error if the
    /// object cannot be decrypted (it is left untouched), or a storage or key
    /// error.
    pub async fn reencrypt(&self, url: &str, opts: &ReencryptOptions) -> Result<Reencrypted> {
        let r = self.resolve(url)?;
        self.codec.reencrypt(&r, opts).await
    }

    /// Like [`Self::get`], and the object must carry every pair of `required`
    /// in its encryption context (extra stored pairs are allowed). The check
    /// happens on the header, before any key is unwrapped; a stored context
    /// value that was edited to match fails authentication afterwards.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidInput`] if `required` holds a reserved (`osc:`,
    /// `aws-crypto-`) key, before any I/O; [`Error::Decryption`] with
    /// [`DecryptError::ContextMismatch`](crate::encryption::DecryptError::ContextMismatch)
    /// if a pair is missing or differs; otherwise as [`Self::get`].
    pub async fn get_with_context(&self, url: &str, required: &CallerContext) -> Result<Bytes> {
        Envelope::check_required(required)?;
        let r = self.resolve(url)?;
        let result = r.backend.store().get(&r.loc.path).await?;
        self.codec
            .get_bytes_requiring(&r.loc, result, required.clone())
            .await
    }

    /// Like [`Self::get_stream`], and the object must carry every pair of
    /// `required` in its encryption context. A mismatch is the first item of
    /// the stream, before any plaintext.
    ///
    /// # Errors
    ///
    /// As [`Self::get_with_context`].
    pub async fn get_stream_with_context(
        &self,
        url: &str,
        required: &CallerContext,
    ) -> Result<ByteStream> {
        Envelope::check_required(required)?;
        let r = self.resolve(url)?;
        let result = r.backend.store().get(&r.loc.path).await?;
        Ok(self
            .codec
            .decode_requiring(r.loc, body_stream(result), required.clone()))
    }

    /// A plain view sharing this client's store registry.
    #[must_use]
    pub fn without_encryption(&self) -> Client<Plain> {
        Client {
            registry: Arc::clone(&self.registry),
            codec: Plain,
        }
    }

    /// Like [`Self::put`], binding `ctx` into the encryption context. Keys
    /// starting with `osc:` or `aws-crypto-` are rejected before any I/O.
    ///
    /// # Errors
    ///
    /// Returns an error if the context is rejected, the URL is invalid, key
    /// wrapping fails or the store rejects the upload.
    pub async fn put_with_context(
        &self,
        url: &str,
        data: impl Into<Bytes>,
        ctx: &CallerContext,
    ) -> Result<()> {
        self.put_inner(url, data.into(), ctx).await
    }

    /// Like [`Self::put_stream`], binding `ctx` into the encryption context.
    /// A rejected context fails before any request is made.
    ///
    /// # Errors
    ///
    /// Returns an error if the context is rejected, the URL is invalid, the
    /// input yields an error, key wrapping fails or the store rejects the upload.
    pub async fn put_stream_with_context<S>(
        &self,
        url: &str,
        data: S,
        ctx: CallerContext,
    ) -> Result<()>
    where
        S: Stream<Item = Result<Bytes>> + Send + 'static,
    {
        self.put_stream_inner(url, data.boxed(), ctx, &UploadOptions::default())
            .await
    }
}

impl Client<Plain> {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A client whose backends read their configuration from `vars` instead of
    /// the process environment.
    ///
    /// Test-only: credentials normally come from the environment, never from
    /// constructor arguments. This exists so integration tests can point a
    /// client at a local container without mutating global state.
    #[doc(hidden)]
    #[must_use]
    pub fn with_vars(vars: std::collections::HashMap<String, String>) -> Self {
        Self {
            registry: Arc::new(StoreRegistry::with_vars(vars)),
            codec: Plain,
        }
    }

    /// Like [`Self::with_vars`], but the backends honour the system proxy
    /// setting.
    #[cfg(feature = "gui")]
    #[must_use]
    pub(crate) fn isolated(vars: std::collections::HashMap<String, String>) -> Self {
        Self {
            registry: Arc::new(StoreRegistry::isolated(vars)),
            codec: Plain,
        }
    }

    /// A client whose S3 stores take their credentials from the AWS profile
    /// `profile` (static keys, SSO, assumed roles and `credential_process`
    /// helpers, from `~/.aws/config` and `~/.aws/credentials`).
    ///
    /// The profile is the only credential source: static keys in
    /// `S3_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID` are not used, `AWS_PROFILE` is
    /// not read, and `AWS_ENDPOINT_URL` is refused (use `S3_ENDPOINT`). A
    /// `credential_process` entry makes the library run that command, so the
    /// profile files must be trusted. The profile is resolved on the first
    /// signed request; a profile that cannot be resolved is an error, never a fall back
    /// to instance metadata. Unlike [`Self::with_encryption`] this does not
    /// share the store registry: the result has its own store registry, but
    /// keeps this client's configuration source.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidInput`] when the profile name is empty or only
    /// whitespace.
    pub fn with_s3_profile(&self, profile: impl Into<String>) -> Result<Self> {
        let profile = profile.into();
        if profile.trim().is_empty() {
            return Err(Error::InvalidInput("the AWS profile name is empty".into()));
        }
        Ok(Self {
            registry: Arc::new(self.registry.with_profile(profile)),
            codec: Plain,
        })
    }

    /// An encrypting view sharing this client's store registry.
    #[cfg(feature = "encryption")]
    #[must_use]
    pub fn with_encryption(&self, cfg: EncryptionConfig) -> Client<Envelope> {
        Client {
            registry: Arc::clone(&self.registry),
            codec: Envelope::new(cfg),
        }
    }

    /// Generates a pre-signed URL granting time-limited access to the object at
    /// `url` for the given HTTP `method`, valid for `expires_in`.
    ///
    /// The returned URL embeds the credentials needed for the request, so it can
    /// be handed to a client that has no access to the storage credentials
    /// (e.g. a browser performing a direct upload or download).
    ///
    /// `options` may bind a required `Content-Length` and/or `Content-Type`
    /// into the signature (S3 only); the client must then send exactly those
    /// header values. Pass [`SignOptions::default`] for plain signing.
    ///
    /// Supported for S3 (`s3://`), Google Cloud Storage (`gs://`/`gcs://`) and
    /// Azure Blob Storage (`az://`, `wasb(s)://`, `abfs(s)://`). Local and
    /// plain HTTP backends do not support signing.
    ///
    /// An `s3://` URL signed with temporary credentials stops working when that
    /// session ends, whatever `expires_in` says.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `expires_in` is under 1 second or over 7 days (604800 s):
    ///   [`Error::InvalidInput`]. Temporary credentials may cap the real
    ///   lifetime of a URL further.
    /// - The URL is invalid.
    /// - The URL has a `.`/`..` path segment (also percent-encoded), a query or
    ///   a fragment, or names no object (a bucket only): [`Error::InvalidInput`].
    /// - The scheme is unsupported, or does not support pre-signed URLs.
    /// - `options` binds extra headers on a non-S3 backend.
    /// - The backend fails to sign the request.
    pub async fn get_pre_signed_url(
        &self,
        url: &str,
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<String> {
        check_presign_expiry(expires_in)?;
        let r = self.resolve_checked(url, Dots::Everywhere, true)?;
        r.backend
            .pre_signed_urls(
                std::slice::from_ref(&r.loc.path),
                method,
                expires_in,
                options,
            )
            .await?
            .pop()
            .ok_or_else(|| Error::Generic("Backend returned no pre-signed URL".into()))
    }

    /// Generates pre-signed URLs for multiple objects sharing the same backend
    /// (scheme, userinfo, host and port), using a single signer.
    ///
    /// All `urls` must resolve to the same backend; otherwise an error is
    /// returned. This is more efficient than calling [`Self::get_pre_signed_url`]
    /// in a loop when signing many objects in the same bucket/container.
    ///
    /// `options` applies the same `Content-Length` / `Content-Type` binding to
    /// every URL (S3 only); see [`Self::get_pre_signed_url`]. An `s3://` URL
    /// signed with temporary credentials stops working when that session ends,
    /// whatever `expires_in` says.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `expires_in` is under 1 second or over 7 days (604800 s):
    ///   [`Error::InvalidInput`].
    /// - `urls` is empty, or any URL is invalid.
    /// - Any URL has a `.`/`..` path segment (also percent-encoded), a query or
    ///   a fragment, or names no object (a bucket only): [`Error::InvalidInput`].
    /// - The URLs resolve to different backends.
    /// - The scheme is unsupported, or does not support pre-signed URLs.
    /// - `options` binds extra headers on a non-S3 backend.
    /// - The backend fails to sign the requests.
    pub async fn get_pre_signed_urls(
        &self,
        urls: &[&str],
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<Vec<String>> {
        check_presign_expiry(expires_in)?;
        let Some((first, rest)) = urls.split_first() else {
            return Err(Error::InvalidInput("No URLs provided for signing".into()));
        };

        let first = self.resolve_checked(first, Dots::Everywhere, true)?;

        let mut paths = Vec::with_capacity(urls.len());
        paths.push(first.loc.path.clone());

        for url in rest {
            let parsed = parse_checked(url, Dots::Everywhere, true)?;
            if origin(&parsed) != origin(&first.loc.url) {
                return Err(Error::InvalidInput(
                    "All URLs must belong to the same backend (scheme, userinfo, host and port)"
                        .into(),
                ));
            }
            paths.push(object_path(&parsed)?);
        }

        first
            .backend
            .pre_signed_urls(&paths, method, expires_in, options)
            .await
    }
}

/// Where `.` and `..` path segments are refused.
#[derive(Clone, Copy)]
enum Dots {
    /// Everywhere but `file://` URLs and local paths, which resolve them
    /// lexically (the CLI builds `file:///cwd/../x`).
    OffFile,
    /// For every scheme, local paths included: for operations where a
    /// rewritten path would act on the wrong objects.
    Everywhere,
}

/// The most of an object path a converted store error keeps.
pub(crate) const ERROR_PATH_CAP: usize = 1024;
/// The most of a provider message a converted store error keeps.
pub(crate) const ERROR_SOURCE_CAP: usize = 4096;

/// Escaped provider text standing in for a store error's source.
#[derive(Debug)]
struct EscapedText(String);

impl std::fmt::Display for EscapedText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for EscapedText {}

type BoxedSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// [`Error::Archived`] when a provider answer names an archived object: HTTP
/// 401 or 403 with `InvalidObjectState` (S3), or 409 with `BlobArchived`
/// (Azure). `raw` is the provider text to look in, `shown` its escaped form.
pub(crate) fn archived(status: u16, raw: &str, shown: &str) -> Option<Error> {
    let fix = match status {
        401 | 403 if raw.contains("InvalidObjectState") => "restored",
        409 if raw.contains("BlobArchived") => "rehydrated",
        _ => return None,
    };
    Some(Error::Archived(format!(
        "the object cannot be read until it is {fix}: {shown}"
    )))
}

/// Rebuilds a store error with control and bidi characters escaped and its
/// text capped. `object_store` copies provider text (error bodies, listed
/// keys, hrefs) into its messages verbatim; anyone able to write a key could
/// otherwise forge log lines or drive the terminal of whoever reads them.
impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        use object_store::Error as E;
        let source = |s: BoxedSource| -> BoxedSource {
            Box::new(EscapedText(text::escape_display(&s, ERROR_SOURCE_CAP)))
        };
        let path = |p: String| text::escape(&p, ERROR_PATH_CAP);
        Self::ObjectStore(match e {
            e @ (E::PermissionDenied { .. } | E::Unauthenticated { .. }) => {
                let shown = text::escape_display(&e, ERROR_SOURCE_CAP);
                return archived(403, &e.to_string(), &shown).unwrap_or(Self::Denied(shown));
            }
            E::NotFound { path: p, source: s } => E::NotFound {
                path: path(p),
                source: source(s),
            },
            E::Precondition { path: p, source: s } => E::Precondition {
                path: path(p),
                source: source(s),
            },
            E::AlreadyExists { path: p, source: s } => {
                if let Some(e) = archived(
                    409,
                    &s.to_string(),
                    &text::escape_display(&s, ERROR_SOURCE_CAP),
                ) {
                    return e;
                }
                E::AlreadyExists {
                    path: path(p),
                    source: source(s),
                }
            }
            E::NotModified { path: p, source: s } => E::NotModified {
                path: path(p),
                source: source(s),
            },
            E::Generic { store, source: s } => E::Generic {
                store,
                source: source(s),
            },
            e @ (E::NotImplemented { .. } | E::JoinError { .. }) => e,
            other => {
                let store = match &other {
                    E::UnknownConfigurationKey { store, .. } => store,
                    _ => "object_store",
                };
                E::Generic {
                    store,
                    source: Box::new(EscapedText(text::escape_display(&other, ERROR_SOURCE_CAP))),
                }
            }
        })
    }
}

/// Refuses a pre-signed URL lifetime outside 1 second to 7 days, for every
/// scheme. It runs before any URL is parsed or backend built, so it needs no
/// configuration, and no caller value reaches Azure's overflowing `chrono` add.
fn check_presign_expiry(expires_in: Duration) -> Result<()> {
    if (Duration::from_secs(1)..=MAX_PRESIGN_EXPIRY).contains(&expires_in) {
        return Ok(());
    }
    Err(Error::InvalidInput(
        "pre-signed URL expiry must be between 1 second and 7 days (604800 s)".into(),
    ))
}

/// Parses `raw` and runs [`check_input`] on it, so no entry point can parse a
/// URL without checking it.
fn parse_checked(raw: &str, dots: Dots, needs_object: bool) -> Result<Url> {
    let url = parse_url(raw)?;
    check_input(raw, &url, dots, needs_object)?;
    Ok(url)
}

/// Rejects input the `url` crate would silently rewrite, before any backend is
/// built or request sent: tabs, newlines and surrounding whitespace, a query or
/// fragment (they would be cut off the key), user names and passwords, a `.` or
/// `..` segment in any spelling (`%2e%2E`) where `dots` says so, and, if
/// `needs_object`, a URL that names no object (a bucket only, or a path ending
/// in `/`). The checks run
/// on the raw string; the messages never quote it, as it may carry tokens.
fn check_input(raw: &str, url: &Url, dots: Dots, needs_object: bool) -> Result<()> {
    check_raw_url(raw)?;
    // Local paths never have either: `Url::from_file_path` escapes `?` and `#`.
    if url.query().is_some() || url.fragment().is_some() {
        return Err(Error::InvalidInput(
            "URLs must not contain a query or fragment".into(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(Error::InvalidInput(
            "user names and passwords in URLs are not supported; configure credentials \
             through the provider's environment variables"
                .into(),
        ));
    }
    // `check_raw_url` has refused dots in every URL but `file:`.
    if matches!(dots, Dots::Everywhere) && url.scheme() == "file" {
        check_dot_segments(raw, crate::registry::url_form(raw).is_some())?;
    }
    // A trailing `/` would be dropped from the object path, so the operation
    // would address a different object than the one typed.
    if needs_object && (url.path().ends_with('/') || object_path(url)?.as_ref().is_empty()) {
        return Err(Error::InvalidInput(format!(
            "{} names a bucket or a prefix, not an object",
            crate::codec::redacted(url)
        )));
    }
    Ok(())
}

/// The checks of [`check_input`] that need only the raw string, which the URL
/// parser would rewrite: tabs, newlines and surrounding whitespace, and a `.`
/// or `..` segment in a URL that is not `file:`. A local path passes. The CLI
/// runs it on each operand before it parses the operand.
pub(crate) fn check_raw_url(raw: &str) -> Result<()> {
    let Some(url) = crate::registry::url_form(raw) else {
        return Ok(());
    };
    // The URL parser drops tabs and newlines anywhere, and the C0 controls
    // and spaces around the input.
    if raw.contains(['\t', '\n', '\r'])
        || raw.starts_with(|c: char| c <= ' ')
        || raw.ends_with(|c: char| c <= ' ')
    {
        return Err(Error::InvalidInput(
            "URLs must not contain tabs, newlines or surrounding whitespace".into(),
        ));
    }
    if url.scheme() != "file" {
        check_dot_segments(raw, true)?;
    }
    Ok(())
}

/// Refuses a `.` or `..` path segment in `raw`, also percent-encoded, and a
/// `.` or `..` bucket or host. `is_url` tells a URL (see
/// [`registry::url_form`](crate::registry::url_form)) from a local path.
fn check_dot_segments(raw: &str, is_url: bool) -> Result<()> {
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    let is_dot = |segment: &str| matches!(decode(segment).as_str(), "." | "..");
    let path = if is_url {
        // The scheme ends at the first `:`. The URL parser skips any run of
        // `/` and `\` after it (`file:/a`, `http:\\h/a`, `s3:///b`).
        let rest = raw.split_once(':').map_or(raw, |(_, rest)| rest);
        let rest = rest.trim_start_matches(['/', '\\']);
        // The authority ends at the first `/`. It is the bucket or container
        // for s3/gs/az, and the signed URL is re-parsed with `{bucket}/{key}`,
        // so a `.`/`..` bucket would move the request to another bucket.
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        if is_dot(authority) || decode(authority).contains('\\') {
            return Err(Error::InvalidInput(
                "URLs must not have a '.' or '..' bucket or host".into(),
            ));
        }
        path
    } else {
        raw
    };
    // A backslash separates segments for the special schemes (http, https).
    if path.split(['/', '\\']).any(is_dot) {
        return Err(Error::InvalidInput(
            "URLs must not contain a '.' or '..' path segment".into(),
        ));
    }
    Ok(())
}

/// One page of [`Client::list_dir`].
#[cfg(feature = "gui")]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct DirPage {
    pub(crate) folders: Vec<String>,
    pub(crate) objects: Vec<DirObject>,
    /// The symlinks of a `file://` directory; empty for other stores.
    pub(crate) links: Vec<String>,
    /// The token for the next page; `None` on the last one.
    pub(crate) next: Option<String>,
}

/// The names of the symlinks directly in the directory `dir`, without the
/// names a listing hides. A missing path, or one that is not a directory, has
/// none.
#[cfg(feature = "gui")]
async fn local_links(dir: std::path::PathBuf) -> Result<Vec<String>> {
    tokio::task::spawn_blocking(move || {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(Vec::new());
            }
            Err(e) => return Err(Error::Io(e)),
        };
        let mut links = Vec::new();
        for entry in entries {
            let entry = entry?;
            // An entry removed since the read is skipped, as listings do.
            match entry.file_type() {
                Ok(kind) if kind.is_symlink() => {}
                Ok(_) => continue,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(Error::Io(e)),
            }
            if let Some(name) = entry.file_name().to_str()
                && local_delete::is_listed_name(name)
            {
                links.push(name.to_owned());
            }
        }
        Ok(links)
    })
    .await
    .map_err(|e| Error::Generic(format!("list_dir task failed: {e}")))?
}

/// An object listed by [`Client::list_dir`].
#[cfg(feature = "gui")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DirObject {
    pub(crate) name: String,
    pub(crate) size: u64,
    pub(crate) modified: chrono::DateTime<chrono::Utc>,
    /// Whether it can be read now; stores that do not report it say `Online`.
    pub(crate) state: crate::backend::ObjectState,
    /// The storage class or access tier, when the store reports one.
    pub(crate) class: Option<String>,
}

/// The name of `location` relative to `prefix`, or `None` when it is not
/// strictly below it: the prefix's own marker (`p/` is the path `p`), a key
/// `/` at the bucket root, or an entry outside the prefix.
fn relative_str<'a>(location: &'a ObjectPath, prefix: &ObjectPath) -> Option<&'a str> {
    let rest = location.as_ref().strip_prefix(prefix.as_ref())?;
    let rest = if prefix.as_ref().is_empty() {
        rest
    } else {
        rest.strip_prefix('/')?
    };
    (!rest.is_empty()).then_some(rest)
}

/// Maps a metadata listing to the names strictly below `prefix` and the
/// listed sizes.
fn relative_listing(
    listing: BoxStream<'static, object_store::Result<object_store::ObjectMeta>>,
    prefix: ObjectPath,
) -> BoxStream<'static, Result<(String, u64)>> {
    listing
        .try_filter_map(move |meta| {
            std::future::ready(Ok(
                relative_str(&meta.location, &prefix).map(|name| (name.to_owned(), meta.size))
            ))
        })
        .map_err(Error::from)
        .boxed()
}

/// Refuses an http(s) URL that names a `WebDAV` collection: a delete of it
/// removes the collection and all its members. `consequence` finishes the
/// sentence "... is a `WebDAV` collection; ".
async fn refuse_collection(r: &Resolved, consequence: &str) -> Result<()> {
    if r.backend.is_collection(&r.loc.path).await? {
        return Err(Error::InvalidInput(format!(
            "{} is a WebDAV collection; {consequence}",
            crate::codec::redacted(&r.loc.url)
        )));
    }
    Ok(())
}

/// One `delete_stream` call for `targets` (URL index and key), reporting each
/// through `each`; see [`Client::delete_many`].
#[cfg(feature = "gui")]
async fn delete_batch(
    store: &dyn ObjectStore,
    targets: Vec<(usize, ObjectPath)>,
    each: &mut impl FnMut(usize, Result<()>),
) {
    let mut order = Vec::new();
    let mut waiting: std::collections::HashMap<ObjectPath, Vec<usize>> =
        std::collections::HashMap::new();
    for (index, path) in targets {
        let indexes = waiting.entry(path.clone()).or_default();
        if indexes.is_empty() {
            order.push(path.clone());
        }
        indexes.push(index);
    }
    let paths = futures::stream::iter(order.clone().into_iter().map(Ok)).boxed();
    let mut stray = Vec::new();
    let mut deleted = store.delete_stream(paths);
    while let Some(result) = deleted.next().await {
        let path = match result {
            Ok(path) => path,
            Err(object_store::Error::NotFound { path, .. })
                if waiting.contains_key(&ObjectPath::from(path.as_str())) =>
            {
                ObjectPath::from(path.as_str())
            }
            Err(e) => {
                stray.push(e);
                continue;
            }
        };
        for index in waiting.remove(&path).unwrap_or_default() {
            each(index, Ok(()));
        }
    }
    let left: Vec<ObjectPath> = order
        .into_iter()
        .filter(|path| waiting.contains_key(path))
        .collect();
    let shared = stray.first().map_or_else(
        || "the store gave no result for the key".to_owned(),
        ToString::to_string,
    );
    let mut stray: Vec<Option<object_store::Error>> = if stray.len() == left.len() {
        stray.into_iter().map(Some).collect()
    } else {
        left.iter().map(|_| None).collect()
    };
    for (path, error) in left.iter().zip(&mut stray) {
        for index in waiting.remove(path).unwrap_or_default() {
            let error = error
                .take()
                .unwrap_or_else(|| object_store::Error::Generic {
                    store: "delete_stream",
                    source: shared.clone().into(),
                });
            each(index, Err(error.into()));
        }
    }
}

/// Deletes the keys strictly below `prefix`. A key already gone counts as
/// deleted; any other error stops the run.
async fn delete_listed(store: &dyn ObjectStore, prefix: ObjectPath) -> Result<()> {
    let owned = prefix.clone();
    let paths = store
        .list(Some(&prefix))
        .try_filter(move |meta| std::future::ready(relative_str(&meta.location, &owned).is_some()))
        .map_ok(|meta| meta.location);
    let mut deleted = store.delete_stream(paths.boxed());
    while let Some(result) = deleted.next().await {
        match result {
            Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> ObjectStorageClient {
        ObjectStorageClient::with_vars(std::collections::HashMap::new())
    }

    #[test]
    fn an_empty_profile_name_is_refused() {
        for bad in ["", "  ", "\t"] {
            let err = client().with_s3_profile(bad).err();
            assert!(
                matches!(&err, Some(Error::InvalidInput(m)) if m.contains("profile name is empty")),
                "{bad:?}: {err:?}"
            );
        }
        client().with_s3_profile("dev").expect("a named profile");
    }

    mod webdav_guard {
        use super::*;
        use crate::backend::stub::{Reply, Stub, serve};

        const COLLECTION: &str = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:propstat>\
            <D:prop><D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat>\
            </D:response></D:multistatus>";
        const FILE: &str = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:propstat>\
            <D:prop><D:resourcetype/></D:prop></D:propstat></D:response></D:multistatus>";

        fn lines(stub: &Stub) -> Vec<String> {
            stub.requests()
                .iter()
                .map(|r| r.line.replace(" HTTP/1.1", ""))
                .collect()
        }

        fn is_collection_error(err: &Error) -> bool {
            matches!(err, Error::InvalidInput(m) if m.contains("is a WebDAV collection"))
        }

        #[tokio::test]
        async fn delete_refuses_a_collection_and_sends_no_delete() {
            let stub = serve(vec![Reply::new(207, COLLECTION)]).await;
            let err = client()
                .delete(&format!("{}/photos", stub.base))
                .await
                .expect_err("collection");
            assert!(is_collection_error(&err), "{err:?}");
            assert!(err.to_string().contains("delete_by_prefix"), "{err}");
            assert_eq!(lines(&stub), ["PROPFIND /photos"]);
        }

        #[cfg(feature = "gui")]
        #[tokio::test]
        async fn a_move_to_another_client_refuses_a_collection() {
            let stub = serve(vec![Reply::new(207, COLLECTION)]).await;
            let dir = tempfile::tempdir().expect("tempdir");
            let to = url::Url::from_file_path(dir.path().join("b"))
                .expect("file URL")
                .to_string();
            let other = ObjectStorageClient::isolated(std::collections::HashMap::new());
            let err = client()
                .transfer(&other, &format!("{}/a", stub.base), &to, None, true)
                .await
                .expect_err("collection");
            assert!(is_collection_error(&err), "{err:?}");
            assert_eq!(lines(&stub), ["PROPFIND /a"]);
            assert!(!dir.path().join("b").exists());
        }

        #[tokio::test]
        async fn delete_goes_ahead_for_a_file_or_a_server_without_webdav() {
            for probe in [
                Reply::new(207, FILE),
                Reply::new(405, ""),
                Reply::new(404, ""),
            ] {
                let stub = serve(vec![probe, Reply::new(204, "")]).await;
                client()
                    .delete(&format!("{}/photos", stub.base))
                    .await
                    .expect("delete");
                assert_eq!(lines(&stub), ["PROPFIND /photos", "DELETE /photos"]);
            }
        }

        #[tokio::test]
        async fn delete_fails_closed_when_the_probe_fails() {
            let stub = serve(vec![Reply::new(500, "")]).await;
            let err = client()
                .delete(&format!("{}/photos", stub.base))
                .await
                .expect_err("probe failed");
            assert!(err.to_string().contains("500"), "{err}");
            assert_eq!(lines(&stub), ["PROPFIND /photos"]);
        }

        #[tokio::test]
        async fn move_refuses_a_collection_before_any_copy_or_delete() {
            let stub = serve(vec![Reply::new(207, COLLECTION)]).await;
            let err = client()
                .move_object(&format!("{}/a", stub.base), &format!("{}/b", stub.base))
                .await
                .expect_err("collection");
            assert!(is_collection_error(&err), "{err:?}");
            assert_eq!(lines(&stub), ["PROPFIND /a"]);
        }

        #[tokio::test]
        async fn move_fails_closed_when_the_probe_fails() {
            let stub = serve(vec![Reply::new(500, "")]).await;
            client()
                .move_object(&format!("{}/a", stub.base), &format!("{}/b", stub.base))
                .await
                .expect_err("probe failed");
            assert_eq!(lines(&stub), ["PROPFIND /a"]);
        }

        /// A copy onto an existing destination (the server answers 412 to
        /// `Overwrite: F`) is streamed with a PUT instead.
        #[tokio::test]
        async fn copy_onto_an_existing_destination_streams_a_put() {
            let object = || {
                Reply::new(200, "hello").header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            };
            let stub = serve(vec![
                // The header probe: a ranged read, then a plain one.
                object(),
                object(),
                Reply::new(412, ""),
                object(),
                Reply::new(201, ""),
            ])
            .await;
            client()
                .copy(&format!("{}/a", stub.base), &format!("{}/b", stub.base))
                .await
                .expect("copy");
            let requests = stub.requests();
            let sent = lines(&stub);
            assert_eq!(sent[2], "COPY /a");
            assert!(requests[2].headers.contains("overwrite: F"));
            assert_eq!(sent[4], "PUT /b");
            assert_eq!(requests[4].body, b"hello");
            assert_eq!(sent.len(), 5, "{sent:?}");
        }

        #[tokio::test]
        async fn move_goes_ahead_for_a_server_without_webdav() {
            let object = |status| {
                Reply::new(status, "hello").header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            };
            let stub = serve(vec![
                Reply::new(405, ""),
                // The header probe (a ranged read, then a plain one), then the
                // move's own HEAD.
                object(200),
                object(200),
                object(200),
                Reply::new(201, ""),
                object(200),
                Reply::new(204, ""),
            ])
            .await;
            client()
                .move_object(&format!("{}/a", stub.base), &format!("{}/b", stub.base))
                .await
                .expect("move");
            let sent = lines(&stub);
            assert_eq!(sent[0], "PROPFIND /a");
            assert!(sent.contains(&"COPY /a".to_owned()), "{sent:?}");
            assert_eq!(sent.last().map(String::as_str), Some("DELETE /a"));
            assert_eq!(sent.len(), 7, "{sent:?}");
        }
    }

    #[test]
    fn delete_prefix_guard_refuses_empty_and_root_inputs() {
        let client = client();
        for bad in ["", "  ", "\t\n", "file:///", "file://localhost/", "/"] {
            let err = client.resolve_delete_prefix(bad).err();
            assert!(
                matches!(err, Some(Error::InvalidInput(_))),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[cfg(windows)]
    #[test]
    fn delete_prefix_guard_refuses_drive_roots() {
        for bad in [r"C:\", "file:///C:/", "file:///x"] {
            let err = client().resolve_delete_prefix(bad).err();
            assert!(
                matches!(err, Some(Error::InvalidInput(_))),
                "{bad:?}: {err:?}"
            );
        }
    }

    #[test]
    fn delete_prefix_guard_accepts_a_directory_below_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let url = url::Url::from_file_path(dir.path().join("p")).expect("file URL");
        let r = client()
            .resolve_delete_prefix(url.as_str())
            .expect("accepted");
        assert!(r.loc.path.as_ref().ends_with('p'), "{:?}", r.loc.path);
    }

    #[test]
    fn delete_prefix_guard_accepts_a_bucket_url() {
        let vars =
            std::collections::HashMap::from([("AWS_REGION".to_owned(), "eu-west-1".to_owned())]);
        let client = ObjectStorageClient::with_vars(vars);
        for url in ["s3://b", "s3://b/"] {
            let r = client.resolve_delete_prefix(url).expect(url);
            assert!(r.loc.path.as_ref().is_empty(), "{url}");
        }
    }

    /// A listing entry whose size is the length of its key.
    fn meta(key: &str) -> object_store::ObjectMeta {
        object_store::ObjectMeta {
            location: ObjectPath::from(key),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: key.len() as u64,
            e_tag: None,
            version: None,
        }
    }

    /// A store whose listing is fixed and whose deletes are recorded.
    #[derive(Debug)]
    struct ListingStore {
        keys: Vec<&'static str>,
        missing: Option<&'static str>,
        broken: Option<&'static str>,
        requested: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl ListingStore {
        fn new(missing: Option<&'static str>, broken: Option<&'static str>) -> Self {
            Self {
                keys: vec!["p", "p/a", "p/b", "q/x"],
                missing,
                broken,
                requested: Arc::default(),
            }
        }
    }

    impl std::fmt::Display for ListingStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("ListingStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for ListingStore {
        async fn put_opts(
            &self,
            _: &ObjectPath,
            _: object_store::PutPayload,
            _: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            unimplemented!()
        }

        async fn put_multipart_opts(
            &self,
            _: &ObjectPath,
            _: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            unimplemented!()
        }

        async fn get_opts(
            &self,
            _: &ObjectPath,
            _: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            unimplemented!()
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<ObjectPath>>,
        ) -> BoxStream<'static, object_store::Result<ObjectPath>> {
            let (missing, broken) = (self.missing, self.broken);
            let requested = Arc::clone(&self.requested);
            locations
                .map(move |location| {
                    let location = location?;
                    requested
                        .lock()
                        .expect("lock")
                        .push(location.as_ref().to_owned());
                    if Some(location.as_ref()) == missing {
                        return Err(object_store::Error::NotFound {
                            path: location.to_string(),
                            source: "gone".into(),
                        });
                    }
                    if Some(location.as_ref()) == broken {
                        return Err(object_store::Error::Generic {
                            store: "fake",
                            source: "boom".into(),
                        });
                    }
                    Ok(location)
                })
                .boxed()
        }

        fn list(
            &self,
            _: Option<&ObjectPath>,
        ) -> BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
            let metas: Vec<_> = self.keys.iter().map(|k| Ok(meta(k))).collect();
            futures::stream::iter(metas).boxed()
        }

        async fn list_with_delimiter(
            &self,
            _: Option<&ObjectPath>,
        ) -> object_store::Result<object_store::ListResult> {
            unimplemented!()
        }

        async fn copy_opts(
            &self,
            _: &ObjectPath,
            _: &ObjectPath,
            _: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            unimplemented!()
        }
    }

    #[test]
    fn relative_names() {
        let table = [
            ("p", "p", None),
            ("p/a", "p", Some("a")),
            ("p/a/b", "p", Some("a/b")),
            ("q/a", "p", None),
            ("pq", "p", None),
            ("a", "", Some("a")),
            ("", "", None),
        ];
        for (location, prefix, want) in table {
            let location_path = ObjectPath::from(location);
            let got = relative_str(&location_path, &ObjectPath::from(prefix));
            assert_eq!(got, want, "{location:?} under {prefix:?}");
        }
    }

    #[tokio::test]
    async fn delete_listed_skips_the_marker_alias_and_tolerates_vanished_keys() {
        let store = ListingStore::new(Some("p/b"), None);
        delete_listed(&store, ObjectPath::from("p"))
            .await
            .expect("ok");
        assert_eq!(*store.requested.lock().expect("lock"), ["p/a", "p/b"]);

        let store = ListingStore::new(None, Some("p/a"));
        let err = delete_listed(&store, ObjectPath::from("p")).await.err();
        assert!(matches!(err, Some(Error::ObjectStore(_))), "{err:?}");
    }

    #[tokio::test]
    async fn listed_names_are_relative_and_inside_the_prefix() {
        let store = ListingStore::new(None, None);
        let prefix = ObjectPath::from("p");
        let names: Vec<_> = relative_listing(store.list(Some(&prefix)), prefix)
            .try_collect()
            .await
            .expect("names");
        // The sizes are the listed ones: the length of the whole key.
        assert_eq!(names, [("a".to_owned(), 3), ("b".to_owned(), 3)]);
    }

    fn hostile_source() -> String {
        "line\n\x1b[2J\x07\u{202E}".repeat(20_000)
    }

    #[test]
    fn store_errors_are_escaped_and_capped() {
        use object_store::Error as E;
        let path = || "p\n\x1b[2Jq".to_owned();
        let source = || -> Box<dyn std::error::Error + Send + Sync> { hostile_source().into() };
        let cases = [
            E::NotFound {
                path: path(),
                source: source(),
            },
            E::Precondition {
                path: path(),
                source: source(),
            },
            E::AlreadyExists {
                path: path(),
                source: source(),
            },
            E::NotModified {
                path: path(),
                source: source(),
            },
            E::PermissionDenied {
                path: path(),
                source: source(),
            },
            E::Unauthenticated {
                path: path(),
                source: source(),
            },
            E::Generic {
                store: "S3",
                source: source(),
            },
        ];
        for original in cases {
            let name = std::mem::discriminant(&original);
            let denied = matches!(
                original,
                E::PermissionDenied { .. } | E::Unauthenticated { .. }
            );
            let err = Error::from(original);
            match &err {
                Error::Denied(_) => assert!(denied, "{err:?}"),
                Error::ObjectStore(inner) => {
                    assert!(!denied, "{err:?}");
                    assert_eq!(std::mem::discriminant(inner), name, "{err:?}");
                }
                _ => panic!("not a store error: {err:?}"),
            }
            let (display, debug) = (err.to_string(), format!("{err:?}"));
            assert!(display.len() < 6 * 1024, "{} bytes", display.len());
            assert!(debug.len() < 12 * 1024, "{} bytes", debug.len());
            for text in [&display, &debug] {
                assert!(!text.chars().any(crate::text::is_unsafe_char), "{text}");
            }
        }

        let err = Error::from(E::NotFound {
            path: "k".into(),
            source: "short and clean".into(),
        });
        assert_eq!(
            err.to_string(),
            "Object store error: Object at location k not found: short and clean"
        );
    }

    #[test]
    fn archived_provider_errors_are_typed() {
        use object_store::Error as E;
        let denied = |text: &str| E::PermissionDenied {
            path: "k".into(),
            source: text.into(),
        };
        let s3 = "<Error><Code>InvalidObjectState</Code><Message>x\u{1b}[2J</Message></Error>";
        let err = Error::from(denied(s3));
        assert!(
            matches!(&err, Error::Archived(m) if m.contains("restored")),
            "{err:?}"
        );
        assert!(
            !err.to_string().chars().any(crate::text::is_unsafe_char),
            "{err}"
        );
        let err = Error::from(E::Unauthenticated {
            path: "k".into(),
            source: s3.into(),
        });
        assert!(matches!(err, Error::Archived(_)), "{err:?}");
        let err = Error::from(denied("<Code>AccessDenied</Code>"));
        assert!(matches!(err, Error::Denied(_)), "{err:?}");

        let conflict = |text: &str| E::AlreadyExists {
            path: "k".into(),
            source: text.into(),
        };
        let err = Error::from(conflict("<Code>BlobArchived</Code>"));
        assert!(
            matches!(&err, Error::Archived(m) if m.contains("rehydrated")),
            "{err:?}"
        );
        let err = Error::from(conflict("<Code>BlobAlreadyExists</Code>"));
        assert!(
            matches!(err, Error::ObjectStore(E::AlreadyExists { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn listing_errors_quote_keys_escaped() {
        let source = ObjectPath::parse("a\n\x1b[2Jb").expect_err("invalid key");
        let err = Error::from(object_store::Error::InvalidPath { source });
        let text = err.to_string();
        assert!(text.contains("\\u{a}\\u{1b}[2J"), "{text}");
        assert!(!text.chars().any(crate::text::is_unsafe_char), "{text}");
    }

    #[cfg(unix)]
    mod symlinks {
        use super::*;

        fn dir_url(dir: &std::path::Path) -> String {
            let mut u = url::Url::from_file_path(dir).expect("file URL").to_string();
            if !u.ends_with('/') {
                u.push('/');
            }
            u
        }

        /// `outside/{x,y/z}` beside `top/{real.txt,data -> ../outside,
        /// f -> ../outside/x}`.
        fn linked_tree() -> tempfile::TempDir {
            let t = tempfile::tempdir().expect("tempdir");
            let root = t.path();
            std::fs::create_dir_all(root.join("outside/y")).expect("mkdir");
            std::fs::create_dir_all(root.join("top")).expect("mkdir");
            std::fs::write(root.join("outside/x"), "x").expect("write");
            std::fs::write(root.join("outside/y/z"), "z").expect("write");
            std::fs::write(root.join("top/real.txt"), "r").expect("write");
            std::os::unix::fs::symlink("../outside", root.join("top/data")).expect("link");
            std::os::unix::fs::symlink("../outside/x", root.join("top/f")).expect("link");
            t
        }

        #[tokio::test]
        async fn list_skips_links_to_files_and_directories() {
            let t = linked_tree();
            let url = dir_url(&t.path().join("top"));
            let c = client();
            assert_eq!(c.list(&url).await.expect("list"), ["real.txt"]);
            let streamed: Vec<_> = c
                .list_stream(&url)
                .expect("stream")
                .try_collect()
                .await
                .expect("collect");
            assert_eq!(streamed, ["real.txt"]);
        }

        #[tokio::test]
        async fn list_resolves_a_linked_prefix_as_named() {
            let t = linked_tree();
            let mut names = client()
                .list(&dir_url(&t.path().join("top/data")))
                .await
                .expect("list");
            names.sort();
            assert_eq!(names, ["x", "y/z"]);
        }

        #[tokio::test]
        async fn delete_of_a_link_leaves_its_target() {
            let t = linked_tree();
            let url = url::Url::from_file_path(t.path().join("top/f"))
                .expect("file URL")
                .to_string();
            client().delete(&url).await.expect("delete");
            assert!(t.path().join("top/f").symlink_metadata().is_err());
            assert_eq!(
                std::fs::read(t.path().join("outside/x")).expect("read"),
                b"x"
            );
        }
    }

    #[cfg(feature = "gui")]
    mod transfer {
        use super::*;
        use crate::encryption::keys::Keyring;

        struct Side {
            dir: tempfile::TempDir,
            plain: ObjectStorageClient,
        }

        impl Side {
            fn new() -> Self {
                Self {
                    dir: tempfile::tempdir().expect("tempdir"),
                    plain: ObjectStorageClient::isolated(std::collections::HashMap::new()),
                }
            }

            fn url(&self, name: &str) -> String {
                url::Url::from_file_path(self.dir.path().join(name))
                    .expect("file URL")
                    .to_string()
            }

            fn exists(&self, name: &str) -> bool {
                self.dir.path().join(name).exists()
            }
        }

        fn config() -> EncryptionConfig {
            let ring = Keyring::from_material("kek-1", &[("kek-1", &[7; 32])]);
            EncryptionConfig::new(Arc::new(ring))
        }

        #[tokio::test]
        async fn a_failed_move_keeps_the_source() {
            let (a, b) = (Side::new(), Side::new());
            a.plain.put(&a.url("x"), "payload").await.expect("put");
            // The destination directory cannot be created below a file.
            b.plain.put(&b.url("f"), "file").await.expect("put");
            let result = a
                .plain
                .transfer(&b.plain, &a.url("x"), &b.url("f/y"), None, true)
                .await;
            assert!(result.is_err());
            assert!(a.exists("x"));
        }

        #[tokio::test]
        async fn the_same_url_is_copied_but_a_move_is_refused() {
            let a = Side::new();
            a.plain.put(&a.url("x"), "payload").await.expect("put");
            // Another client over the same URL: the copy is not a no-op.
            let same = ObjectStorageClient::isolated(std::collections::HashMap::new());
            let url = a.url("x");
            a.plain
                .transfer(&same, &url, &url, None, false)
                .await
                .expect("copy");
            let err = a
                .plain
                .transfer(&same, &url, &url, None, true)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
            assert_eq!(a.plain.get(&url).await.expect("kept"), "payload");
            // Within one client a copy or move onto itself is still a no-op.
            a.plain
                .transfer(&a.plain, &url, &url, None, true)
                .await
                .expect("no-op");
            assert_eq!(a.plain.get(&url).await.expect("kept"), "payload");
        }

        /// Each client over its own in-memory store for the same URLs.
        fn stored_pair(
            scheme_a: &str,
            scheme_b: &str,
        ) -> (
            (ObjectStorageClient, Arc<object_store::memory::InMemory>),
            (ObjectStorageClient, Arc<object_store::memory::InMemory>),
        ) {
            let iso = || ObjectStorageClient::isolated(std::collections::HashMap::new());
            let (a, b) = (iso(), iso());
            let (sa, sb) = (
                Arc::new(object_store::memory::InMemory::new()),
                Arc::new(object_store::memory::InMemory::new()),
            );
            a.preload_store(scheme_a, sa.clone());
            b.preload_store(scheme_b, sb.clone());
            ((a, sa), (b, sb))
        }

        #[tokio::test]
        async fn a_transfer_reads_through_self_and_writes_through_dst() {
            use object_store::path::Path;
            let ((a, sa), (b, sb)) = stored_pair("s3://b", "s3://b");
            sa.put(&Path::from("x"), "one".into()).await.expect("put");
            a.transfer(&b, "s3://b/x", "s3://b/x", None, false)
                .await
                .expect("copy");
            let got = sb.get(&Path::from("x")).await.expect("copied");
            assert_eq!(got.bytes().await.expect("bytes").as_ref(), b"one");
            a.transfer(&b, "s3://b/x", "s3://b/y", None, true)
                .await
                .expect("move");
            assert!(sa.head(&Path::from("x")).await.is_err());
            assert!(sb.head(&Path::from("y")).await.is_ok());
            assert!(sa.head(&Path::from("y")).await.is_err());
        }

        #[tokio::test]
        async fn a_transfer_between_clients_needs_object_urls() {
            let (a, b) = (Side::new(), Side::new());
            a.plain.put(&a.url("x"), "payload").await.expect("put");
            let cases = [
                (a.url("x"), format!("{}/", b.url("d"))),
                (a.url("x"), "s3:///".to_owned()),
                ("s3:///".to_owned(), b.url("y")),
            ];
            for (from, to) in cases {
                for delete in [false, true] {
                    let err = a
                        .plain
                        .transfer(&b.plain, &from, &to, None, delete)
                        .await
                        .expect_err("refused");
                    assert!(
                        matches!(err, Error::InvalidInput(_)),
                        "{from} -> {to}: {err:?}"
                    );
                }
            }
            assert!(a.exists("x") && !b.exists("d") && !b.exists("y"));
        }

        #[tokio::test]
        async fn encrypted_clients_decrypt_and_re_encrypt() {
            let (a, b) = (Side::new(), Side::new());
            let cfg = config();
            let (ea, eb) = (
                a.plain.with_encryption(cfg.clone()),
                b.plain.with_encryption(cfg),
            );
            ea.put(&a.url("x"), "secret").await.expect("put");
            ea.transfer(&eb, &a.url("x"), &b.url("y"), None, false)
                .await
                .expect("copy");
            assert_eq!(eb.get(&b.url("y")).await.expect("get"), "secret");
            let stored = b.plain.get(&b.url("y")).await.expect("raw");
            assert_ne!(stored.as_ref(), b"secret");
            ea.transfer(&eb, &a.url("x"), &b.url("z"), None, true)
                .await
                .expect("move");
            assert_eq!(eb.get(&b.url("z")).await.expect("get"), "secret");
            assert!(!a.exists("x"));
            // Equal URLs: the copy rewrites the object, the move is refused.
            let same = ObjectStorageClient::isolated(std::collections::HashMap::new())
                .with_encryption(config());
            let url = a.url("y");
            ea.put(&url, "again").await.expect("put");
            let before = a.plain.get(&url).await.expect("raw");
            ea.transfer(&same, &url, &url, None, false)
                .await
                .expect("copy");
            assert_ne!(a.plain.get(&url).await.expect("raw"), before);
            let err = ea
                .transfer(&same, &url, &url, None, true)
                .await
                .expect_err("refused");
            assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
            assert_eq!(ea.get(&url).await.expect("kept"), "again");
        }
    }

    #[cfg(feature = "gui")]
    mod list_dir {
        use super::*;
        use crate::registry::child_url;

        fn dir_url(dir: &std::path::Path) -> String {
            let mut u = url::Url::from_file_path(dir).expect("file URL").to_string();
            if !u.ends_with('/') {
                u.push('/');
            }
            u
        }

        fn write(dir: &std::path::Path, rel: &str, bytes: &[u8]) {
            let p = dir.join(rel);
            std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
            std::fs::write(p, bytes).expect("write");
        }

        /// A client over a stub that answers with the buckets `b` and `a`.
        async fn root_client() -> (ObjectStorageClient, crate::backend::stub::Stub) {
            use crate::backend::stub::{Reply, serve};
            let body = "<ListAllMyBucketsResult><Buckets>\
                        <Bucket><Name>b</Name></Bucket><Bucket><Name>a</Name></Bucket>\
                        </Buckets></ListAllMyBucketsResult>";
            let stub = serve(vec![Reply::new(200, body)]).await;
            let vars = std::collections::HashMap::from(
                [
                    ("S3_ENDPOINT", stub.base.as_str()),
                    ("S3_ALLOW_HTTP", "true"),
                    ("S3_REGION", "us-east-1"),
                    ("S3_ACCESS_KEY_ID", "k"),
                    ("S3_SECRET_ACCESS_KEY", "s"),
                ]
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
            );
            (ObjectStorageClient::with_vars(vars), stub)
        }

        #[tokio::test]
        async fn a_profile_client_lists_with_the_profiles_keys_region_and_endpoint() {
            use crate::backend::stub::{Reply, serve};
            let body = "<ListAllMyBucketsResult><Buckets><Bucket><Name>a</Name></Bucket>\
                        </Buckets></ListAllMyBucketsResult>";
            let stub = serve(vec![Reply::new(200, body)]).await;
            let dir = tempfile::tempdir().expect("tempdir");
            let config = dir.path().join("config");
            std::fs::write(
                &config,
                format!(
                    "[profile dev]\nregion = eu-west-3\nendpoint_url = {}\n\
                     aws_access_key_id = AKIDDEV\naws_secret_access_key = devsecret\n",
                    stub.base
                ),
            )
            .expect("write");
            let vars = std::collections::HashMap::from([
                ("AWS_CONFIG_FILE".to_owned(), config.display().to_string()),
                ("S3_ALLOW_HTTP".to_owned(), "true".to_owned()),
            ]);
            let c = ObjectStorageClient::isolated(vars)
                .with_s3_profile("dev")
                .expect("profile");
            assert_eq!(
                c.list_dir("s3:///", None, 10).await.expect("list").folders,
                ["a"]
            );
            let headers = stub.requests()[0].headers.to_ascii_lowercase();
            assert!(headers.contains("credential=akiddev/"), "{headers}");
            assert!(headers.contains("/eu-west-3/s3/aws4_request"), "{headers}");
        }

        #[tokio::test]
        async fn every_bucket_scheme_root_lists() {
            use crate::backend::stub::{Reply, serve};
            let vars = |p: &[(&str, &str)]| {
                std::collections::HashMap::from_iter(
                    p.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())),
                )
            };
            for root in ["gs:///", "gcs:///"] {
                let stub = serve(vec![Reply::new(200, r#"{"items":[{"name":"g"}]}"#)]).await;
                let c = ObjectStorageClient::with_vars(vars(&[
                    ("GCS_BASE_URL", &stub.base),
                    ("GOOGLE_BEARER_TOKEN", "tok"),
                    ("GOOGLE_CLOUD_PROJECT", "p"),
                ]));
                assert_eq!(c.list_dir(root, None, 1).await.expect(root).folders, ["g"]);
            }
            let body = "<EnumerationResults><Containers><Container><Name>z</Name></Container>\
                        </Containers><NextMarker /></EnumerationResults>";
            for root in ["az:///", "wasb:///", "wasbs:///", "abfs:///", "abfss:///"] {
                let stub = serve(vec![Reply::new(200, body)]).await;
                let endpoint = format!("{}/acct", stub.base);
                let c = ObjectStorageClient::with_vars(vars(&[
                    ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                    ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
                    ("AZURE_STORAGE_ENDPOINT", &endpoint),
                    ("AZURE_ALLOW_HTTP", "true"),
                ]));
                assert_eq!(c.list_dir(root, None, 1).await.expect(root).folders, ["z"]);
            }
        }

        #[tokio::test]
        async fn a_provider_root_lists_the_buckets_as_folders() {
            for root in ["s3://", "s3:///"] {
                let (c, stub) = root_client().await;
                let page = c.list_dir(root, None, 1).await.expect("list");
                assert_eq!(page.folders, ["a", "b"], "{root}");
                assert!(page.objects.is_empty() && page.links.is_empty());
                assert_eq!(page.next, None);
                assert_eq!(stub.requests().len(), 1);
                assert_eq!(c.backend_count(), 0, "nothing is cached");
            }
        }

        #[tokio::test]
        async fn a_provider_root_with_a_token_is_an_empty_page() {
            let (c, stub) = root_client().await;
            let page = c.list_dir("s3:///", Some("t".into()), 10).await;
            assert_eq!(page.expect("ok"), DirPage::default());
            assert!(stub.requests().is_empty());
        }

        #[tokio::test]
        async fn an_s3_listing_carries_the_object_states() {
            use crate::backend::{
                ObjectState,
                stub::{Reply, serve},
            };
            let item = |key: &str, class: &str| {
                format!(
                    "<Contents><Key>{key}</Key><LastModified>2024-05-06T07:08:09.000Z</LastModified>\
                     <Size>7</Size><StorageClass>{class}</StorageClass></Contents>"
                )
            };
            let body = format!(
                "<ListBucketResult><Prefix>d/</Prefix>{}{}{}\
                 <CommonPrefixes><Prefix>d/sub/</Prefix></CommonPrefixes>\
                 <IsTruncated>true</IsTruncated><NextContinuationToken>n</NextContinuationToken>\
                 </ListBucketResult>",
                item("d/", "STANDARD"),
                item("d/cold &amp; deep", "DEEP_ARCHIVE"),
                item("d/warm", "STANDARD"),
            );
            let stub = serve(vec![Reply::new(200, body)]).await;
            let c = ObjectStorageClient::with_vars(std::collections::HashMap::from(
                [
                    ("S3_ENDPOINT", stub.base.as_str()),
                    ("S3_ALLOW_HTTP", "true"),
                    ("S3_REGION", "us-east-1"),
                    ("S3_ACCESS_KEY_ID", "k"),
                    ("S3_SECRET_ACCESS_KEY", "s"),
                ]
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
            ));
            let page = c.list_dir("s3://bkt/d/", None, 3).await.expect("list");
            assert_eq!(page.folders, ["sub"]);
            let seen: Vec<_> = page
                .objects
                .iter()
                .map(|o| (o.name.as_str(), o.size, o.state, o.class.as_deref()))
                .collect();
            assert_eq!(
                seen,
                [
                    (
                        "cold & deep",
                        7,
                        ObjectState::Archived,
                        Some("DEEP_ARCHIVE")
                    ),
                    ("warm", 7, ObjectState::Online, Some("STANDARD")),
                ]
            );
            assert_eq!(page.next.as_deref(), Some("n"));
            let line = &stub.requests()[0].line;
            assert!(line.contains("prefix=d%2F&max-keys=3"), "{line}");
        }

        #[tokio::test]
        async fn an_azure_listing_carries_the_object_states() {
            use crate::backend::{
                ObjectState,
                stub::{Reply, serve},
            };
            let blob = |name: &str, props: &str| {
                format!(
                    "<Blob><Name>{name}</Name><Properties><Content-Length>7</Content-Length>\
                     <Last-Modified>Mon, 06 May 2024 07:08:09 GMT</Last-Modified>{props}\
                     </Properties></Blob>"
                )
            };
            let body = format!(
                "<EnumerationResults><Blobs>{}{}{}\
                 <BlobPrefix><Name>d/sub/</Name></BlobPrefix></Blobs>\
                 <NextMarker>m</NextMarker></EnumerationResults>",
                blob("d/cold", "<AccessTier>Archive</AccessTier>"),
                blob(
                    "d/going",
                    "<AccessTier>Archive</AccessTier>\
                     <ArchiveStatus>rehydrate-pending-to-hot</ArchiveStatus>"
                ),
                blob("d/warm", "<AccessTier>Hot</AccessTier>"),
            );
            let stub = serve(vec![Reply::new(200, body)]).await;
            let c = ObjectStorageClient::with_vars(std::collections::HashMap::from(
                [
                    ("AZURE_STORAGE_ENDPOINT", stub.base.as_str()),
                    ("AZURE_ALLOW_HTTP", "true"),
                    ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                    ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
                ]
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
            ));
            let page = c.list_dir("az://c/d/", None, 3).await.expect("list");
            assert_eq!(page.folders, ["sub"]);
            let seen: Vec<_> = page
                .objects
                .iter()
                .map(|o| (o.name.as_str(), o.state, o.class.as_deref()))
                .collect();
            assert_eq!(
                seen,
                [
                    ("cold", ObjectState::Archived, Some("Archive")),
                    ("going", ObjectState::Restoring, Some("Archive")),
                    ("warm", ObjectState::Online, Some("Hot")),
                ]
            );
            assert_eq!(page.next.as_deref(), Some("m"));
        }

        #[tokio::test]
        async fn no_other_operation_accepts_a_provider_root() {
            let (c, stub) = root_client().await;
            for root in ["s3://", "s3:///", "gs:///", "az:///"] {
                assert!(c.list(root).await.is_err(), "{root}");
                assert!(c.exists(root).await.is_err(), "{root}");
                assert!(c.get(root).await.is_err(), "{root}");
            }
            assert!(stub.requests().is_empty());
        }

        #[tokio::test]
        async fn list_dir_splits_folders_and_objects() {
            let t = tempfile::tempdir().expect("tempdir");
            write(t.path(), "a/x.txt", b"x");
            write(t.path(), "b/y.txt", b"y");
            write(t.path(), "c.txt", b"abc");
            let page = client()
                .list_dir(&dir_url(t.path()), None, 10)
                .await
                .expect("list");
            let mut folders = page.folders.clone();
            folders.sort();
            assert_eq!(folders, ["a", "b"]);
            assert_eq!(page.objects.len(), 1, "{page:?}");
            assert_eq!(page.objects[0].name, "c.txt");
            assert_eq!(page.objects[0].size, 3);
            assert_eq!(page.next, None);
        }

        #[tokio::test]
        async fn list_dir_names_round_trip() {
            let t = tempfile::tempdir().expect("tempdir");
            let mut names = vec!["a b.txt", "c%d.txt"];
            if cfg!(windows) {
                names.push("é#.txt");
            } else {
                names.push("é#?.txt");
            }
            for n in &names {
                write(t.path(), n, n.as_bytes());
            }
            let dir = dir_url(t.path());
            let c = client();
            let page = c.list_dir(&dir, None, 10).await.expect("list");
            assert_eq!(page.objects.len(), names.len(), "{page:?}");
            for o in &page.objects {
                let got = c
                    .get(&child_url(&dir, [o.name.as_str()]))
                    .await
                    .expect("get");
                assert_eq!(got.as_ref(), o.name.as_bytes());
            }
        }

        #[cfg(unix)]
        #[tokio::test]
        async fn list_dir_reports_links_apart() {
            let t = tempfile::tempdir().expect("tempdir");
            write(t.path(), "outside/x", b"x");
            write(t.path(), "top/real.txt", b"r");
            write(t.path(), "top/sub/y", b"y");
            std::os::unix::fs::symlink("../outside", t.path().join("top/data")).expect("link");
            std::os::unix::fs::symlink("../outside/x", t.path().join("top/f")).expect("link");
            let page = client()
                .list_dir(&dir_url(&t.path().join("top")), None, 10)
                .await
                .expect("list");
            assert_eq!(page.folders, ["sub"]);
            let objects: Vec<_> = page.objects.iter().map(|o| o.name.as_str()).collect();
            assert_eq!(objects, ["real.txt"]);
            let mut links = page.links.clone();
            links.sort();
            assert_eq!(links, ["data", "f"]);
        }

        #[tokio::test]
        async fn list_dir_missing_directory_is_empty() {
            let t = tempfile::tempdir().expect("tempdir");
            let url = dir_url(&t.path().join("missing"));
            let page = client().list_dir(&url, None, 10).await;
            assert_eq!(page.expect("ok"), DirPage::default());
        }

        #[tokio::test]
        async fn list_dir_on_a_file_is_empty() {
            let t = tempfile::tempdir().expect("tempdir");
            write(t.path(), "c.txt", b"abc");
            let url = dir_url(t.path()) + "c.txt";
            let page = client().list_dir(&url, None, 10).await.expect("ok");
            assert!(
                page.folders.is_empty() && page.objects.is_empty(),
                "{page:?}"
            );
        }

        #[tokio::test]
        async fn list_dir_token_on_unpaged_store_is_empty() {
            let t = tempfile::tempdir().expect("tempdir");
            write(t.path(), "c.txt", b"abc");
            let page = client()
                .list_dir(&dir_url(t.path()), Some("x".into()), 10)
                .await;
            assert_eq!(page.expect("ok"), DirPage::default());
        }
    }
}
