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

mod local_delete;

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

    /// An update was abandoned because the object changed (or vanished)
    /// after it was read. Nothing was written.
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
        let url = parse_url(raw)?;
        check_input(raw, &url, dots, needs_object)?;
        let (backend, path) = self.registry.resolve(&url)?;
        Ok(Resolved {
            backend,
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
        let (raw_from, raw_to) = (from, to);
        let (from, to) = (parse_url(raw_from)?, parse_url(raw_to)?);
        check_input(raw_from, &from, Dots::OffFile, true)?;
        check_input(raw_to, &to, Dots::OffFile, true)?;
        let ((src, src_path), (dst, dst_path)) = self.registry.resolve_pair(&from, &to)?;
        Ok((
            Resolved {
                backend: src,
                loc: Location {
                    url: from,
                    path: src_path,
                },
            },
            Resolved {
                backend: dst,
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
        let stored = self.codec.encode_stream(r.loc.clone(), ctx, data);
        upload_stream(r.backend.as_ref(), &r.loc.path, stored, opts).await
    }

    /// Deletes an object at the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error deleting the object from the store.
    pub async fn delete(&self, url: &str) -> Result<()> {
        let r = self.resolve(url)?;
        r.backend.store().delete(&r.loc.path).await?;
        Ok(())
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
    /// levels are refused with [`Error::InvalidInput`].
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

    /// Lists objects under the given URL prefix.
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

    /// Retrieves metadata for the object at the given URL.
    ///
    /// The plain client issues a single HEAD request via
    /// [`GetOptions::head`](object_store::GetOptions::head); the encrypted
    /// client issues one ranged GET of at most
    /// `DecodeLimits::max_header_len` bytes (16 KiB by default) for the header instead, followed by a HEAD
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
    ///   the backend's `OAuth2` bearer credential. On HTTP 409 the bucket's
    ///   metadata is read: the call succeeds only if the caller can see the
    ///   bucket, and fails if another project owns the name.
    /// - `az://`, `wasb(s)://`, `abfs(s)://` — Azure `Create Container`
    ///   (`PUT /<container>?restype=container`), signed with `AzureAuthorizer`.
    ///   Idempotent against `ContainerAlreadyExists` / HTTP 409.
    ///
    /// Because `object_store` does not expose a built store's resolved
    /// endpoint, region, account or project, those are read from configuration
    /// (the same environment the backend was built from):
    /// `S3_REGION` / `AWS_ENDPOINT`(`S3_ENDPOINT`) / `S3_ALLOW_HTTP` for S3,
    /// `GOOGLE_CLOUD_PROJECT` (`GCS_PROJECT_ID`) for GCS, and
    /// `AZURE_STORAGE_ACCOUNT_NAME` / `AZURE_STORAGE_ENDPOINT` for Azure.
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
        r.backend
            .create_bucket(self.registry.http()?, &r.loc.url)
            .await
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
        r.backend
            .bucket_exists(self.registry.http()?, &r.loc.url)
            .await
    }

    /// Copies an object from one URL to another.
    /// Supports cross-provider copies. Copying an object onto itself does
    /// nothing.
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
    /// nothing.
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
        self.codec.copy(&src, &dst, true).await
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
    /// keyring first), an error if the object cannot be decrypted (it is left
    /// untouched), or a storage or key error.
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
            let parsed = Self::parse_url(url)?;
            check_input(url, &parsed, Dots::Everywhere, true)?;
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
const ERROR_PATH_CAP: usize = 1024;
/// The most of a provider message a converted store error keeps.
const ERROR_SOURCE_CAP: usize = 4096;

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

/// Rebuilds a store error with control and bidi characters escaped and its
/// text capped. `object_store` copies provider text (error bodies, listed
/// keys, hrefs) into its messages verbatim; anyone able to write a key could
/// otherwise forge log lines or drive the terminal of whoever reads them.
impl From<object_store::Error> for Error {
    fn from(e: object_store::Error) -> Self {
        use object_store::Error as E;
        let source = |s: BoxedSource| -> BoxedSource {
            Box::new(EscapedText(text::escape(&s.to_string(), ERROR_SOURCE_CAP)))
        };
        let path = |p: String| text::escape(&p, ERROR_PATH_CAP);
        Self::ObjectStore(match e {
            E::NotFound { path: p, source: s } => E::NotFound {
                path: path(p),
                source: source(s),
            },
            E::Precondition { path: p, source: s } => E::Precondition {
                path: path(p),
                source: source(s),
            },
            E::AlreadyExists { path: p, source: s } => E::AlreadyExists {
                path: path(p),
                source: source(s),
            },
            E::NotModified { path: p, source: s } => E::NotModified {
                path: path(p),
                source: source(s),
            },
            E::PermissionDenied { path: p, source: s } => E::PermissionDenied {
                path: path(p),
                source: source(s),
            },
            E::Unauthenticated { path: p, source: s } => E::Unauthenticated {
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
                    source: Box::new(EscapedText(text::escape(
                        &other.to_string(),
                        ERROR_SOURCE_CAP,
                    ))),
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

/// Rejects input the `url` crate would silently rewrite, before any backend is
/// built or request sent: tabs, newlines and surrounding whitespace, a query or
/// fragment (they would be cut off the key), user names and passwords, a `.` or
/// `..` segment in any spelling (`%2e%2E`) where `dots` says so, and, if
/// `needs_object`, a URL that names no object (a bucket only, or a path ending
/// in `/`). The checks run
/// on the raw string; the messages never quote it, as it may carry tokens.
fn check_input(raw: &str, url: &Url, dots: Dots, needs_object: bool) -> Result<()> {
    let is_url = raw.contains("://");
    // The URL parser drops tabs and newlines anywhere, and the C0 controls
    // and spaces around the input.
    if is_url
        && (raw.contains(['\t', '\n', '\r'])
            || raw.starts_with(|c: char| c <= ' ')
            || raw.ends_with(|c: char| c <= ' '))
    {
        return Err(Error::InvalidInput(
            "URLs must not contain tabs, newlines or surrounding whitespace".into(),
        ));
    }
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
    if matches!(dots, Dots::Everywhere) || url.scheme() != "file" {
        check_dot_segments(raw)?;
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

/// Refuses a `.` or `..` path segment in `raw`, also percent-encoded, and a
/// `.` or `..` bucket or host. `raw` is a URL if it has a scheme separator and
/// a local path otherwise.
fn check_dot_segments(raw: &str) -> Result<()> {
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    let is_dot = |segment: &str| matches!(decode(segment).as_str(), "." | "..");
    let path = if let Some((_, rest)) = raw.split_once("://") {
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

/// The name of `location` relative to `prefix`, or `None` when it is not
/// strictly below it: the prefix's own marker (`p/` is the path `p`), a key
/// `/` at the bucket root, or an entry outside the prefix.
fn relative_name(location: &ObjectPath, prefix: &ObjectPath) -> Option<String> {
    let parts: Vec<String> = location
        .prefix_match(prefix)?
        .map(|part| part.as_ref().to_owned())
        .collect();
    (!parts.is_empty()).then(|| parts.join("/"))
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
                relative_name(&meta.location, &prefix).map(|name| (name, meta.size))
            ))
        })
        .map_err(Error::from)
        .boxed()
}

/// Deletes the keys strictly below `prefix`. A key already gone counts as
/// deleted; any other error stops the run.
async fn delete_listed(store: &dyn ObjectStore, prefix: ObjectPath) -> Result<()> {
    let owned = prefix.clone();
    let paths = store
        .list(Some(&prefix))
        .try_filter(move |meta| std::future::ready(relative_name(&meta.location, &owned).is_some()))
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
            let got = relative_name(&ObjectPath::from(location), &ObjectPath::from(prefix));
            assert_eq!(got.as_deref(), want, "{location:?} under {prefix:?}");
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
            let err = Error::from(original);
            let Error::ObjectStore(inner) = &err else {
                panic!("not a store error: {err:?}");
            };
            assert_eq!(std::mem::discriminant(inner), name, "{err:?}");
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
    fn listing_errors_quote_keys_escaped() {
        let source = ObjectPath::parse("a\n\x1b[2Jb").expect_err("invalid key");
        let err = Error::from(object_store::Error::InvalidPath { source });
        let text = err.to_string();
        assert!(text.contains("\\u{a}\\u{1b}[2J"), "{text}");
        assert!(!text.chars().any(crate::text::is_unsafe_char), "{text}");
    }
}
