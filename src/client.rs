use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use futures::stream::BoxStream;
use http::Method;
use object_store::signer::Signer;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
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

    #[error("Unsupported scheme: {0}")]
    UnsupportedScheme(String),

    #[error("Pre-signed URLs are not supported for scheme: {0}")]
    SigningUnsupported(String),

    #[error("Generic error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// HTTP method a pre-signed URL authorizes.
///
/// This is a backend-agnostic abstraction over the HTTP verb so that callers
/// (including the Python and CLI front-ends) do not need to depend on the
/// `http`/`reqwest` crates directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignMethod {
    /// Download an object.
    Get,
    /// Upload (overwrite) an object.
    Put,
    /// Upload via an HTTP POST (e.g. browser form uploads).
    Post,
    /// Delete an object.
    Delete,
    /// Fetch object metadata only.
    Head,
}

impl SignMethod {
    fn into_http(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Put => Method::PUT,
            Self::Post => Method::POST,
            Self::Delete => Method::DELETE,
            Self::Head => Method::HEAD,
        }
    }
}

impl std::str::FromStr for SignMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "GET" => Ok(Self::Get),
            "PUT" => Ok(Self::Put),
            "POST" => Ok(Self::Post),
            "DELETE" => Ok(Self::Delete),
            "HEAD" => Ok(Self::Head),
            other => Err(Error::Generic(format!("Invalid HTTP method: {other}"))),
        }
    }
}

/// Metadata for an object in storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub location: String,
    pub last_modified: chrono::DateTime<chrono::Utc>,
    pub size: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
}

impl From<ObjectMeta> for Metadata {
    fn from(meta: ObjectMeta) -> Self {
        Self {
            location: meta.location.to_string(),
            last_modified: meta.last_modified,
            size: meta.size,
            e_tag: meta.e_tag,
            version: meta.version,
        }
    }
}

/// A resolved storage backend: the object store plus, when the backend supports
/// it, a [`Signer`] for generating pre-signed URLs.
#[derive(Clone)]
struct Backend {
    store: Arc<dyn ObjectStore>,
    signer: Option<Arc<dyn Signer>>,
}

/// A unified object storage client that handles multiple backends based on URL schemes.
#[derive(Clone, Default)]
pub struct ObjectStorageClient {
    /// Cache for resolved backends based on (scheme, host).
    stores: Arc<DashMap<StoreKey, Backend>>,
}

type StoreKey = (String, Option<String>);

impl ObjectStorageClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stores: Arc::new(DashMap::new()),
        }
    }

    /// Resolves the correct backend (store + optional signer) for a given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    fn get_backend(&self, url: &Url) -> Result<(Backend, ObjectPath)> {
        let scheme = url.scheme();
        let host = url.host_str();
        let path = if scheme == "file" {
            url.path()
        } else {
            &url.path()[1..]
        };
        let object_path = ObjectPath::from(path);

        let key = (scheme.to_string(), host.map(ToString::to_string));
        if let Some(backend) = self.stores.get(&key) {
            return Ok((backend.value().clone(), object_path));
        }

        let backend = match scheme {
            "s3" => {
                let s3_secure = std::env::var("S3_SECURE").unwrap_or_else(|_| "true".into());

                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;
                let mut builder =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);

                if let Ok(region) = std::env::var("S3_REGION") {
                    builder = builder.with_region(region);
                }

                if let Ok(access_key_id) = std::env::var("S3_ACCESS_KEY_ID") {
                    builder = builder.with_token("").with_access_key_id(access_key_id);
                }

                if let Ok(secret_access_key) = std::env::var("S3_SECRET_ACCESS_KEY") {
                    builder = builder.with_secret_access_key(secret_access_key);
                }

                if s3_secure == "false" {
                    builder = builder.with_allow_http(true);
                }

                let store = Arc::new(builder.build()?);
                Backend {
                    signer: Some(Arc::clone(&store) as Arc<dyn Signer>),
                    store,
                }
            }
            "gs" | "gcs" => {
                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;
                let builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);

                let store = Arc::new(builder.build()?);
                Backend {
                    signer: Some(Arc::clone(&store) as Arc<dyn Signer>),
                    store,
                }
            }
            "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
                let container =
                    host.ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;
                let builder = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container);

                let store = Arc::new(builder.build()?);
                Backend {
                    signer: Some(Arc::clone(&store) as Arc<dyn Signer>),
                    store,
                }
            }
            "http" | "https" => {
                let builder = object_store::http::HttpBuilder::new().with_url(url.as_str());
                Backend {
                    store: Arc::new(builder.build()?),
                    signer: None,
                }
            }
            "file" => Backend {
                store: Arc::new(object_store::local::LocalFileSystem::new()),
                signer: None,
            },
            _ => return Err(Error::UnsupportedScheme(scheme.to_string())),
        };

        self.stores.insert(key, backend.clone());
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
    fn get_store(&self, url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let (backend, path) = self.get_backend(url)?;
        Ok((backend.store, path))
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
        let parsed_url = Url::parse(url)?;
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
        let parsed_url = Url::parse(url)?;
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
        let parsed_url = Url::parse(url)?;
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
        let parsed_url = Url::parse(url)?;
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
        let parsed_url = Url::parse(url)?;
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

    /// Retrieves metadata for an object at the given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported.
    /// - There is an error retrieving metadata from the store.
    pub async fn head(&self, url: &str) -> Result<Metadata> {
        let parsed_url = Url::parse(url)?;
        let (store, path) = self.get_store(&parsed_url)?;
        let meta = store.head(&path).await?;
        Ok(meta.into())
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
        let from_parsed_url = Url::parse(from_url)?;
        let to_parsed_url = Url::parse(to_url)?;

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
        let from_parsed_url = Url::parse(from_url)?;
        let to_parsed_url = Url::parse(to_url)?;

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
    /// Supported for S3 (`s3://`), Google Cloud Storage (`gs://`/`gcs://`) and
    /// Azure Blob Storage (`az://`, `wasb(s)://`, `abfs(s)://`). Local and
    /// plain HTTP backends do not support signing.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL is invalid.
    /// - The scheme is unsupported, or does not support pre-signed URLs.
    /// - The backend fails to sign the request.
    pub async fn get_pre_signed_url(
        &self,
        url: &str,
        method: SignMethod,
        expires_in: Duration,
    ) -> Result<String> {
        let parsed_url = Url::parse(url)?;
        let (backend, path) = self.get_backend(&parsed_url)?;
        let signer = backend
            .signer
            .ok_or_else(|| Error::SigningUnsupported(parsed_url.scheme().to_string()))?;
        let pre_signed_url = signer
            .signed_url(method.into_http(), &path, expires_in)
            .await?;
        Ok(pre_signed_url.into())
    }

    /// Generates pre-signed URLs for multiple objects sharing the same backend
    /// (scheme and host), using a single signer.
    ///
    /// All `urls` must resolve to the same backend; otherwise an error is
    /// returned. This is more efficient than calling [`Self::get_pre_signed_url`]
    /// in a loop when signing many objects in the same bucket/container.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `urls` is empty, or any URL is invalid.
    /// - The URLs resolve to different backends.
    /// - The scheme is unsupported, or does not support pre-signed URLs.
    /// - The backend fails to sign the requests.
    pub async fn get_pre_signed_urls(
        &self,
        urls: &[&str],
        method: SignMethod,
        expires_in: Duration,
    ) -> Result<Vec<String>> {
        let Some((first, rest)) = urls.split_first() else {
            return Err(Error::Generic("No URLs provided for signing".into()));
        };

        let first_url = Url::parse(first)?;
        let (backend, first_path) = self.get_backend(&first_url)?;
        let signer = backend
            .signer
            .ok_or_else(|| Error::SigningUnsupported(first_url.scheme().to_string()))?;

        let mut paths = Vec::with_capacity(urls.len());
        paths.push(first_path);

        for url in rest {
            let parsed = Url::parse(url)?;
            if parsed.scheme() != first_url.scheme() || parsed.host_str() != first_url.host_str() {
                return Err(Error::Generic(
                    "All URLs must belong to the same backend (scheme and host)".into(),
                ));
            }
            let (_, path) = self.get_backend(&parsed)?;
            paths.push(path);
        }

        let pre_signed_url = signer
            .signed_urls(method.into_http(), &paths, expires_in)
            .await?;
        Ok(pre_signed_url.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_local_file_ops() -> Result<()> {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let url = format!("file://{}", file_path.to_str().unwrap());

        let client = ObjectStorageClient::new();

        // Put
        let data = b"hello world";
        client.put(&url, &data[..]).await?;

        // Get
        let retrieved = client.get(&url).await?;
        assert_eq!(retrieved.as_ref(), data);

        // Head
        let meta = client.head(&url).await?;
        assert_eq!(meta.size, data.len() as u64);

        // List (using directory URL)
        let dir_url = format!("file://{}", dir.path().to_str().unwrap());
        let list = client.list(&dir_url).await?;
        assert!(list.iter().any(|p| p.ends_with("test.txt")));

        // Delete
        client.delete(&url).await?;
        assert!(fs::metadata(&file_path).is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_copy_move() -> Result<()> {
        let dir = tempdir().unwrap();
        let src_path = dir.path().join("src.txt");
        let dst_path = dir.path().join("dst.txt");
        let src_url = format!("file://{}", src_path.to_str().unwrap());
        let dst_url = format!("file://{}", dst_path.to_str().unwrap());

        let client = ObjectStorageClient::new();
        let data = b"copy move test";

        // Test Copy
        client.put(&src_url, &data[..]).await?;
        client.copy(&src_url, &dst_url).await?;
        assert_eq!(client.get(&dst_url).await?.as_ref(), data);
        assert_eq!(client.get(&src_url).await?.as_ref(), data);

        // Test Move
        let dst_url2 = format!("file://{}.2", dst_path.to_str().unwrap());
        client.move_object(&dst_url, &dst_url2).await?;
        assert_eq!(client.get(&dst_url2).await?.as_ref(), data);
        Ok(())
    }

    #[tokio::test]
    async fn test_store_caching() -> Result<()> {
        let client = ObjectStorageClient::new();
        let url = Url::parse("file:///tmp")?;

        let (store1, _) = client.get_store(&url)?;
        let (store2, _) = client.get_store(&url)?;

        // Check if both Arcs point to the same ObjectStore instance.
        // We can compare the raw pointers of the trait objects.
        assert!(Arc::ptr_eq(&store1, &store2));

        // Different URL but same store (scheme + host)
        let url3 = Url::parse("file:///other")?;
        let (store3, _) = client.get_store(&url3)?;
        assert!(Arc::ptr_eq(&store1, &store3));

        Ok(())
    }

    #[tokio::test]
    async fn test_get_pre_signed_url_unsupported_for_local() {
        let client = ObjectStorageClient::new();
        let err = client
            .get_pre_signed_url(
                "file:///tmp/foo.txt",
                SignMethod::Get,
                Duration::from_mins(1),
            )
            .await
            .expect_err("local filesystem must not support signing");

        assert!(matches!(err, Error::SigningUnsupported(scheme) if scheme == "file"));
    }

    #[tokio::test]
    async fn test_sign_method_from_str() {
        assert_eq!("get".parse::<SignMethod>().unwrap(), SignMethod::Get);
        assert_eq!("PUT".parse::<SignMethod>().unwrap(), SignMethod::Put);
        assert_eq!("Delete".parse::<SignMethod>().unwrap(), SignMethod::Delete);
        assert!("PATCH".parse::<SignMethod>().is_err());
    }

    #[tokio::test]
    async fn test_get_pre_signed_urls_rejects_mixed_backends() {
        let client = ObjectStorageClient::new();
        let err = client
            .get_pre_signed_urls(
                &["file:///tmp/a.txt", "s3://bucket/b.txt"],
                SignMethod::Get,
                Duration::from_mins(1),
            )
            .await
            .expect_err("mixed backends must be rejected");

        // The first URL (file) is resolved first and is itself unsupported.
        assert!(matches!(err, Error::SigningUnsupported(_)));
    }

    #[tokio::test]
    async fn test_clone_client() -> Result<()> {
        let client = ObjectStorageClient::new();
        let client_clone = client.clone();
        let url = Url::parse("file:///tmp")?;

        let (store1, _) = client.get_store(&url)?;
        let (store2, _) = client_clone.get_store(&url)?;

        // Cloned client should share the same stores.
        assert!(Arc::ptr_eq(&store1, &store2));

        Ok(())
    }
}
