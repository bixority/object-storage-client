use bytes::Bytes;
use dashmap::DashMap;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, path::Path as ObjectPath};
use std::sync::Arc;
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

    #[error("Generic error: {0}")]
    Generic(String),
}

pub type Result<T> = std::result::Result<T, Error>;

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

/// A unified object storage client that handles multiple backends based on URL schemes.
#[derive(Clone, Default)]
pub struct ObjectStorageClient {
    /// Cache for `ObjectStore` instances based on (scheme, host).
    stores: Arc<DashMap<StoreKey, Arc<dyn ObjectStore>>>,
}

type StoreKey = (String, Option<String>);

impl ObjectStorageClient {
    #[must_use]
    pub fn new() -> Self {
        Self {
            stores: Arc::new(DashMap::new()),
        }
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
        let scheme = url.scheme();
        let host = url.host_str();
        let path = if scheme == "file" {
            url.path()
        } else {
            &url.path()[1..]
        };
        let object_path = ObjectPath::from(path);

        let key = (scheme.to_string(), host.map(std::string::ToString::to_string));
        if let Some(store) = self.stores.get(&key) {
            return Ok((Arc::clone(store.value()), object_path));
        }

        let store: Arc<dyn ObjectStore> = match scheme {
            "s3" => {
                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;
                let builder =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);

                Arc::new(builder.build()?)
            }
            "gs" | "gcs" => {
                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;
                let builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);

                Arc::new(builder.build()?)
            }
            "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
                let container =
                    host.ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;
                let builder = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container);

                Arc::new(builder.build()?)
            }
            "http" | "https" => {
                let builder = object_store::http::HttpBuilder::new().with_url(url.as_str());
                Arc::new(builder.build()?)
            }
            "file" => Arc::new(object_store::local::LocalFileSystem::new()),
            _ => return Err(Error::UnsupportedScheme(scheme.to_string())),
        };

        self.stores.insert(key, Arc::clone(&store));
        Ok((store, object_path))
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
        let url = Url::parse("file:///tmp").unwrap();

        let (store1, _) = client.get_store(&url)?;
        let (store2, _) = client.get_store(&url)?;

        // Check if both Arcs point to the same ObjectStore instance.
        // We can compare the raw pointers of the trait objects.
        assert!(Arc::ptr_eq(&store1, &store2));

        // Different URL but same store (scheme + host)
        let url3 = Url::parse("file:///other").unwrap();
        let (store3, _) = client.get_store(&url3)?;
        assert!(Arc::ptr_eq(&store1, &store3));

        Ok(())
    }

    #[tokio::test]
    async fn test_clone_client() -> Result<()> {
        let client = ObjectStorageClient::new();
        let client_clone = client.clone();
        let url = Url::parse("file:///tmp").unwrap();

        let (store1, _) = client.get_store(&url)?;
        let (store2, _) = client_clone.get_store(&url)?;

        // Cloned client should share the same stores.
        assert!(Arc::ptr_eq(&store1, &store2));

        Ok(())
    }
}
