use crate::backend::{Backend, body_stream};
use crate::codec::{ByteStream, CallerContext, Codec, Location, Plain, Resolved, same_name};
#[cfg(feature = "encryption")]
use crate::encryption::{EncryptionConfig, Envelope};
use crate::registry::{StoreRegistry, object_path, origin, parse_url};
use crate::sign::{SignMethod, SignOptions};
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
    /// embed secrets (userinfo, SAS or other query tokens) in object URLs.
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    #[error("Invalid URL: {0}")]
    InvalidUrl(#[from] url::ParseError),

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

/// The client that encrypts every object it writes and decrypts every object
/// it reads.
/// Settings of [`Client::reencrypt`](Client::<Envelope>::reencrypt).
#[cfg(feature = "encryption")]
#[derive(Debug, Clone)]
pub struct ReencryptOptions {
    /// The key ID whose objects are rewritten under the active key.
    pub from: String,
    /// Only objects last modified before this time are rewritten.
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
    /// The object was last modified at or after `before`; left alone.
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

#[cfg(feature = "encryption")]
pub type EncryptedObjectStorageClient = Client<crate::encryption::Envelope>;

impl<C: Codec> Client<C> {
    /// Parses a URL string into a [`Url`], treating absolute or relative paths
    /// without a scheme as `file://` URLs.
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
    #[doc(hidden)]
    pub fn get_backend(&self, url: &Url) -> Result<(Arc<dyn Backend>, ObjectPath)> {
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

    pub(crate) fn resolve(&self, url: &str) -> Result<Resolved> {
        let url = parse_url(url)?;
        let (backend, path) = self.registry.resolve(&url)?;
        Ok(Resolved {
            backend,
            loc: Location { url, path },
        })
    }

    /// Resolves both ends of a copy or move, sharing one backend between URLs
    /// of one origin so that a server-side copy stays possible.
    fn resolve_pair(&self, from: &str, to: &str) -> Result<(Resolved, Resolved)> {
        let (from, to) = (parse_url(from)?, parse_url(to)?);
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
    /// This method uses the underlying store's `delete_stream` to perform
    /// efficient batch deletions concurrently where supported by the backend.
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
        if url.trim().is_empty() {
            return Err(Error::InvalidInput(
                "delete_by_prefix needs a non-empty URL".into(),
            ));
        }
        let r = self.resolve(url)?;
        if r.loc.url.scheme() == "file" && r.loc.path.as_ref().is_empty() {
            return Err(Error::InvalidInput(
                "delete_by_prefix refuses the file system root".into(),
            ));
        }
        if r.loc.url.scheme() == "file" {
            // object_store's local listing follows symlinked directories, so a
            // link below the prefix would redirect the deletes outside it.
            return local_delete::delete_by_prefix(r.loc.path).await;
        }
        let (store, path) = (r.backend.store(), r.loc.path);
        let paths = store.list(Some(&path)).map_ok(|meta| meta.location);
        store
            .delete_stream(paths.boxed())
            .try_for_each(|_| async { Ok(()) })
            .await?;
        Ok(())
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
        let r = self.resolve(url)?;
        let (store, path) = (r.backend.store(), r.loc.path);
        let prefix = if path.as_ref().is_empty() {
            String::new()
        } else {
            format!("{path}/")
        };
        Ok(store
            .list(Some(&path))
            .map_ok(move |meta| {
                let location = meta.location.to_string();
                let relative = location.strip_prefix(&prefix).unwrap_or(&location);
                relative.strip_prefix('/').unwrap_or(relative).to_owned()
            })
            .map_err(Error::from)
            .boxed())
    }

    /// Retrieves metadata for the object at the given URL.
    ///
    /// The plain client issues a single HEAD request via
    /// [`GetOptions::head`](object_store::GetOptions::head); the encrypted
    /// client issues one or two ranged GETs for the header instead. Either
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
    ///   `AwsAuthorizer`. Idempotent against `BucketAlreadyOwnedByYou` /
    ///   `BucketAlreadyExists`.
    /// - `gs://`, `gcs://` — GCS JSON API `buckets.insert`, authenticated with
    ///   the backend's `OAuth2` bearer credential. Idempotent against HTTP 409.
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
        let r = self.resolve(url)?;
        r.backend
            .create_bucket(self.registry.http(), &r.loc.url)
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
    ///   Properties`, GCS `buckets.get`) rather than listing objects. A `404
    ///   Not Found` means the bucket is missing; success or `403 Forbidden`
    ///   means it exists.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme does not support bucket existence checks.
    /// - The backend fails for a reason other than the bucket not existing.
    pub async fn bucket_exists(&self, url: &str) -> Result<bool> {
        let r = self.resolve(url)?;
        r.backend
            .bucket_exists(self.registry.http(), &r.loc.url)
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
    /// Between stores (or two names the client cannot tell are one store) a
    /// move streams the source to the destination and then deletes the source,
    /// but only if the source still matches what was read: the object's
    /// metadata is compared before the delete. If it was gone, nothing is
    /// deleted; if it changed, for instance because the destination is another
    /// name for the source, the source is kept and [`Error::SourceChanged`]
    /// is returned; the destination stays written. A rewrite with identical
    /// bytes in the same second, on a store reporting no e-tag or version, is
    /// indistinguishable.
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
    /// A standalone encrypted client with its own store registry.
    #[must_use]
    pub fn new(cfg: EncryptionConfig) -> Self {
        Self {
            registry: Arc::default(),
            codec: Envelope::new(cfg),
        }
    }

    /// Reads the envelope details of an encrypted object from its header with
    /// ranged reads. Needs no keys.
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
    /// # Errors
    ///
    /// [`Error::InvalidInput`] if `opts.from` is the active key (rotate the
    /// keyring first), an error if the object cannot be decrypted (it is left
    /// untouched), or a storage or key error.
    pub async fn reencrypt(&self, url: &str, opts: &ReencryptOptions) -> Result<Reencrypted> {
        let r = self.resolve(url)?;
        self.codec.reencrypt(&r, opts).await
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
    /// # Errors
    ///
    /// Returns an error if:
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
        check_presign_input(url)?;
        let r = self.resolve(url)?;
        // The local backend rejects signing itself; keep its error.
        if r.loc.url.scheme() != "file" {
            check_presign_path(&r.loc.path)?;
        }
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
    /// every URL (S3 only); see [`Self::get_pre_signed_url`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
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
        let Some((first, rest)) = urls.split_first() else {
            return Err(Error::Generic("No URLs provided for signing".into()));
        };

        for url in urls {
            check_presign_input(url)?;
        }
        let first = self.resolve(first)?;

        let mut paths = Vec::with_capacity(urls.len());
        paths.push(first.loc.path.clone());

        for url in rest {
            let parsed = Self::parse_url(url)?;
            if origin(&parsed) != origin(&first.loc.url) {
                return Err(Error::Generic(
                    "All URLs must belong to the same backend (scheme, userinfo, host and port)"
                        .into(),
                ));
            }
            paths.push(object_path(&parsed)?);
        }
        if first.loc.url.scheme() != "file" {
            for path in &paths {
                check_presign_path(path)?;
            }
        }

        first
            .backend
            .pre_signed_urls(&paths, method, expires_in, options)
            .await
    }
}

/// Rejects a pre-sign input the `url` crate would silently rewrite: a dot
/// segment in any spelling (`.`, `..`, `%2e`, `%2E%2e`), which it resolves
/// while parsing, so `uploads/../other/x` would sign `other/x`; and, for a URL,
/// a query or fragment, which it would drop. The check runs on the raw string,
/// before any normalisation.
fn check_presign_input(raw: &str) -> Result<()> {
    // The URL parser drops tabs and newlines anywhere in its input.
    let raw: String = raw
        .chars()
        .filter(|c| !matches!(c, '\t' | '\n' | '\r'))
        .collect();
    let path = if let Some((_, rest)) = raw.split_once("://") {
        if rest.contains(['?', '#']) {
            return Err(Error::InvalidInput(
                "Pre-signed URL input must not contain a query or fragment".into(),
            ));
        }
        // The authority ends at the first `/`. It is the bucket or container
        // for s3/gs/az, and the signed URL is re-parsed with `{bucket}/{key}`,
        // so a `.`/`..` bucket would move the request to another bucket.
        let (authority, path) = rest.split_once('/').unwrap_or((rest, ""));
        let host = percent_encoding::percent_decode_str(authority).decode_utf8_lossy();
        if host == "." || host == ".." || host.contains(['/', '\\']) {
            return Err(Error::InvalidInput(
                "Pre-signed URL input must not have a '.' or '..' bucket or host".into(),
            ));
        }
        path
    } else {
        // A local path: `?` and `#` are ordinary file name characters.
        raw.as_str()
    };
    // A backslash separates segments for the special schemes (http, https).
    let has_dot_segment = path.split(['/', '\\']).any(|segment| {
        let decoded = percent_encoding::percent_decode_str(segment).decode_utf8_lossy();
        decoded == "." || decoded == ".."
    });
    if has_dot_segment {
        return Err(Error::InvalidInput(
            "Pre-signed URL input must not contain a '.' or '..' path segment".into(),
        ));
    }
    Ok(())
}

/// Rejects an empty object path: signing it would sign the bucket root
/// (list, create or delete the bucket) instead of an object.
fn check_presign_path(path: &ObjectPath) -> Result<()> {
    if path.as_ref().is_empty() {
        return Err(Error::InvalidInput(
            "Pre-signed URL input must name an object, not a bucket".into(),
        ));
    }
    Ok(())
}
