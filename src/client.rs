use crate::backend::{self, Backend};
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::{Attribute, GetOptions, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use url::Url;

#[derive(Error, Debug)]
pub enum Error {
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

    #[error("Generic error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Metadata for an object in storage, as reported by a HEAD request.
///
/// Returned by [`ObjectStorageClient::get_object_metadata`]. Alongside the
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
    /// The stored `Content-Type`, or `None` if the backend reported none.
    pub content_type: Option<String>,
    /// The object's e-tag, if the backend reports one.
    pub e_tag: Option<String>,
    /// The object's version, if the backend reports one.
    pub version: Option<String>,
}

/// A unified object storage client that handles multiple backends based on URL schemes.
#[derive(Clone, Default)]
pub struct ObjectStorageClient {
    /// Cache of resolved backends keyed on (scheme, host), so each provider's
    /// `object_store` is built once and reused across calls.
    stores: Arc<DashMap<StoreKey, Arc<dyn Backend>>>,
    /// Shared HTTP client for bucket operations and other direct requests.
    client: reqwest::Client,
}

type StoreKey = (String, Option<String>);

impl ObjectStorageClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stores: Arc::new(DashMap::new()),
            client: reqwest::Client::new(),
        }
    }

    /// Parses a URL string into a [`Url`], treating absolute or relative paths
    /// without a scheme as `file://` URLs.
    #[doc(hidden)]
    pub fn parse_url(url: &str) -> Result<Url> {
        match Url::parse(url) {
            Ok(parsed) => {
                // On Windows, an absolute path like `C:\path` might be parsed
                // as having scheme `c`. If the scheme is a single character,
                // we treat it as a local filesystem path instead.
                if parsed.scheme().len() == 1 {
                    Self::path_to_file_url(url)
                } else {
                    Ok(parsed)
                }
            }
            Err(url::ParseError::RelativeUrlWithoutBase) => Self::path_to_file_url(url),
            Err(e) => Err(e.into()),
        }
    }

    /// Converts a local filesystem path (absolute or relative) into a `file://`
    /// [`Url`].
    fn path_to_file_url(path_str: &str) -> Result<Url> {
        let abs_path = if path_str.starts_with("~/") || path_str == "~" {
            let home = homedir::my_home()
                .map_err(|e| Error::Generic(format!("Failed to determine home directory: {e}")))?
                .ok_or_else(|| Error::Generic("Failed to determine home directory".into()))?;
            if path_str == "~" {
                home
            } else {
                home.join(&path_str[2..])
            }
        } else {
            let path = std::path::Path::new(path_str);
            if path.is_absolute() {
                path.to_path_buf()
            } else {
                std::env::current_dir()?.join(path)
            }
        };
        Url::from_file_path(&abs_path).map_err(|()| {
            Error::Generic(format!(
                "Failed to convert path to URL: {}",
                abs_path.display()
            ))
        })
    }

    /// Resolves the provider-specific [`Backend`] for a given URL, building it
    /// on first use and caching it by (scheme, host) so subsequent calls reuse
    /// the same store and credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    #[doc(hidden)]
    pub fn get_backend(&self, url: &Url) -> Result<(Arc<dyn Backend>, ObjectPath)> {
        let scheme = url.scheme();
        let host = url.host_str();
        let path = if scheme == "file" {
            url.path()
        } else {
            // For non-`file` schemes the host is the bucket/container and the
            // object key is the path with its leading `/` removed. A bucket-only
            // URL (e.g. `s3://bucket`) has an empty path, so strip the slash
            // rather than slicing `[1..]`, which would panic on `""`.
            url.path().strip_prefix('/').unwrap_or("")
        };
        let object_path = ObjectPath::from(path);

        let key = (scheme.to_string(), host.map(ToString::to_string));
        if let Some(backend) = self.stores.get(&key) {
            return Ok((Arc::clone(backend.value()), object_path));
        }

        let backend = backend::build(url)?;
        self.stores.insert(key, Arc::clone(&backend));
        Ok((backend, object_path))
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

    /// Stream object's data from the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error fetching the object from the store.
    pub async fn get_stream(
        &self,
        url: &str,
    ) -> Result<BoxStream<'static, object_store::Result<Bytes>>> {
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        let result = store.get(&path).await?;
        let stream = result.into_stream();
        Ok(stream)
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
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        let result = store.get(&path).await?;
        let bytes = result.bytes().await?;
        Ok(bytes)
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
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        store.put(&path, data.into().into()).await?;
        Ok(())
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
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        store.delete(&path).await?;
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
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        let prefix = path.to_string();
        let prefix_with_slash = if !prefix.is_empty() && !prefix.ends_with('/') {
            Some(format!("{prefix}/"))
        } else {
            None
        };

        let mut list_stream = store.list(Some(&path));
        let mut results = Vec::new();
        while let Some(meta) = list_stream.next().await {
            let meta = meta?;
            let mut location = meta.location.to_string();

            if let Some(p) = &prefix_with_slash {
                if location.starts_with(p) {
                    location = location[p.len()..].to_string();
                }
            } else if !prefix.is_empty() && location.starts_with(&prefix) {
                location = location[prefix.len()..].to_string();
            }

            if location.starts_with('/') {
                location = location[1..].to_string();
            }
            results.push(location);
        }
        Ok(results)
    }

    /// Retrieves metadata for the object at the given URL.
    ///
    /// Issues a single HEAD request via [`GetOptions::head`], returning the
    /// location, last modified time, size, e-tag and version from the object
    /// metadata together with the `Content-Type` read from the object
    /// attributes. A missing object is reported as an error
    /// ([`object_store::Error::NotFound`], surfaced as `FileNotFoundError` from
    /// the Python bindings); use [`Self::exists`] for a non-erroring presence
    /// check.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - The object does not exist.
    /// - There is an error retrieving metadata from the store.
    pub async fn get_object_metadata(&self, url: &str) -> Result<ObjectMetadata> {
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        let options = GetOptions {
            head: true,
            ..Default::default()
        };

        let result = store.get_opts(&path, options).await?;
        let content_type = result
            .attributes
            .get(&Attribute::ContentType)
            .map(|value| value.as_ref().to_string());
        let meta = result.meta;

        Ok(ObjectMetadata {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size_bytes: meta.size,
            content_type,
            e_tag: meta.e_tag,
            version: meta.version,
        })
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
        let parsed_url = Self::parse_url(url)?;
        let (store, path) = self.get_store(&parsed_url)?;

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
        let parsed_url = Self::parse_url(url)?;
        let (backend, _) = self.get_backend(&parsed_url)?;
        backend.create_bucket(&self.client, &parsed_url).await
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
        let parsed_url = Self::parse_url(url)?;
        let (backend, _) = self.get_backend(&parsed_url)?;
        backend.bucket_exists(&self.client, &parsed_url).await
    }

    /// Copies an object from one URL to another.
    /// Supports cross-provider copies.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either URL is invalid.
    /// - Schemes are unsupported.
    /// - There is an error copying between stores.
    pub async fn copy(&self, from_url: &str, to_url: &str) -> Result<()> {
        let from_parsed_url = Self::parse_url(from_url)?;
        let to_parsed_url = Self::parse_url(to_url)?;

        let (from_store, from_path) = self.get_store(&from_parsed_url)?;
        let (to_store, to_path) = self.get_store(&to_parsed_url)?;

        // Try intra-store copy if they are the same store instance.
        // For simplicity and to handle cross-provider accurately without complex store comparison,
        // we check if they have the same base URL (scheme and host).
        let same_store = from_parsed_url.scheme() == to_parsed_url.scheme()
            && from_parsed_url.host_str() == to_parsed_url.host_str();

        if same_store {
            match from_store.copy(&from_path, &to_path).await {
                Ok(()) => {}
                Err(e) if e.to_string().contains("os error 18") => {
                    // Fallback for cross-device copy
                    let result = from_store.as_ref().get(&from_path).await?;
                    let bytes = result.bytes().await?;
                    to_store.put(&to_path, bytes.into()).await?;
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            // Cross-provider copy
            let result = from_store.get(&from_path).await?;
            let bytes = result.bytes().await?;
            to_store.put(&to_path, bytes.into()).await?;
        }
        Ok(())
    }

    /// Moves an object from one URL to another.
    /// Supports cross-provider moves.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either URL is invalid.
    /// - Schemes are unsupported.
    /// - There is an error moving between stores.
    pub async fn move_object(&self, from_url: &str, to_url: &str) -> Result<()> {
        let from_parsed_url = Self::parse_url(from_url)?;
        let to_parsed_url = Self::parse_url(to_url)?;

        let (from_store, from_path) = self.get_store(&from_parsed_url)?;
        let (to_store, to_path) = self.get_store(&to_parsed_url)?;

        let same_store = from_parsed_url.scheme() == to_parsed_url.scheme()
            && from_parsed_url.host_str() == to_parsed_url.host_str();

        if same_store {
            match from_store.rename(&from_path, &to_path).await {
                Ok(()) => {}
                Err(e) if e.to_string().contains("os error 18") => {
                    // Fallback for cross-device move
                    let result = from_store.as_ref().get(&from_path).await?;
                    let bytes = result.bytes().await?;
                    to_store.put(&to_path, bytes.into()).await?;
                    from_store.delete(&from_path).await?;
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            // Cross-provider move
            let result = from_store.get(&from_path).await?;
            let bytes = result.bytes().await?;
            to_store.put(&to_path, bytes.into()).await?;
            from_store.delete(&from_path).await?;
        }
        Ok(())
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
        let parsed_url = Self::parse_url(url)?;
        let (backend, path) = self.get_backend(&parsed_url)?;
        backend
            .pre_signed_url(&path, method, expires_in, options)
            .await
    }

    /// Generates pre-signed URLs for multiple objects sharing the same backend
    /// (scheme and host), using a single signer.
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

        let first_url = Self::parse_url(first)?;
        let (backend, first_path) = self.get_backend(&first_url)?;

        let mut paths = Vec::with_capacity(urls.len());
        paths.push(first_path);

        for url in rest {
            let parsed = Self::parse_url(url)?;
            if parsed.scheme() != first_url.scheme() || parsed.host_str() != first_url.host_str() {
                return Err(Error::Generic(
                    "All URLs must belong to the same backend (scheme and host)".into(),
                ));
            }
            let (_, path) = self.get_backend(&parsed)?;
            paths.push(path);
        }

        backend
            .pre_signed_urls(&paths, method, expires_in, options)
            .await
    }
}
