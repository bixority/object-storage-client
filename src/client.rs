use bytes::Bytes;
use futures::StreamExt;
use object_store::{ObjectMeta, ObjectStore, ObjectStoreExt, PutPayload, path::Path as ObjectPath};
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

/// A unified object storage client that handles multiple backends based on URL schemes.
pub struct ObjectStorageClient {
    // We'll likely need a registry or a way to get the correct store for a given URL.
    // Since arrow-rs's object_store works with buckets/containers,
    // we may need to cache or dynamically create stores.
    // For now, let's keep it simple and handle S3, GCS, Azure, and Local.
}

impl Default for ObjectStorageClient {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectStorageClient {
    #[must_use]
    pub fn new() -> Self {
        Self {}
    }

    /// Resolves the correct `ObjectStore` for a given URL.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The URL scheme is unsupported.
    /// - The URL is missing required components (e.g., bucket name for S3/GCS).
    /// - There is an error building the underlying `ObjectStore`.
    fn get_store(url: &Url) -> Result<(Arc<dyn ObjectStore>, ObjectPath)> {
        let scheme = url.scheme();
        let host = url.host_str();
        let path = if scheme == "file" {
            url.path()
        } else {
            &url.path()[1..]
        };
        let object_path = ObjectPath::from(path);

        match scheme {
            "s3" => {
                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;
                let builder =
                    object_store::aws::AmazonS3Builder::from_env().with_bucket_name(bucket);

                let store = builder.build()?;
                Ok((Arc::new(store), object_path))
            }
            "gs" | "gcs" => {
                let bucket =
                    host.ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;
                let builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                    .with_bucket_name(bucket);

                let store = builder.build()?;
                Ok((Arc::new(store), object_path))
            }
            "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
                let container =
                    host.ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;
                let builder = object_store::azure::MicrosoftAzureBuilder::from_env()
                    .with_container_name(container);

                let store = builder.build()?;
                Ok((Arc::new(store), object_path))
            }
            "http" | "https" => {
                let builder = object_store::http::HttpBuilder::new().with_url(url.as_str());
                let store = builder.build()?;
                Ok((Arc::new(store), ObjectPath::from("")))
            }
            "file" => {
                let store = object_store::local::LocalFileSystem::new();
                Ok((Arc::new(store), object_path))
            }
            _ => Err(Error::UnsupportedScheme(scheme.to_string())),
        }
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
        let (store, path) = Self::get_store(&parsed_url)?;
        let result = store.as_ref().get(&path).await?;
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
    pub async fn put(&self, url: &str, data: &[u8]) -> Result<()> {
        let parsed_url = Url::parse(url)?;
        let (store, path) = Self::get_store(&parsed_url)?;
        store.as_ref().put(&path, PutPayload::from(data.to_vec())).await?;
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
        let (store, path) = Self::get_store(&parsed_url)?;
        store.as_ref().delete(&path).await?;
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
        let (store, path) = Self::get_store(&parsed_url)?;
        let mut prefix = path.to_string();
        if !prefix.is_empty() && !prefix.ends_with('/') {
            prefix.push('/');
        }
        let mut list_stream = store.as_ref().list(Some(&path));
        let mut results = Vec::new();
        while let Some(meta) = list_stream.next().await {
            let meta = meta?;
            let mut location = meta.location.to_string();
            if !prefix.is_empty() && location.starts_with(&prefix) {
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
    pub async fn head(&self, url: &str) -> Result<ObjectMeta> {
        let parsed_url = Url::parse(url)?;
        let (store, path) = Self::get_store(&parsed_url)?;
        let meta = store.as_ref().head(&path).await?;
        Ok(meta)
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

        let (from_store, from_path) = Self::get_store(&from_parsed_url)?;
        let (to_store, to_path) = Self::get_store(&to_parsed_url)?;

        // Try intra-store copy if they are the same store instance.
        // For simplicity and to handle cross-provider accurately without complex store comparison,
        // we check if they have the same base URL (scheme and host).
        let same_store = from_parsed_url.scheme() == to_parsed_url.scheme()
            && from_parsed_url.host_str() == to_parsed_url.host_str();

        if same_store {
            match from_store.as_ref().copy(&from_path, &to_path).await {
                Ok(()) => {}
                Err(e) if e.to_string().contains("os error 18") => {
                    // Fallback for cross-device copy
                    let result = from_store.as_ref().get(&from_path).await?;
                    let bytes = result.bytes().await?;
                    to_store
                        .as_ref()
                        .put(&to_path, PutPayload::from(bytes))
                        .await?;
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            // Cross-provider copy
            let result = from_store.as_ref().get(&from_path).await?;
            let bytes = result.bytes().await?;
            to_store
                .as_ref()
                .put(&to_path, PutPayload::from(bytes))
                .await?;
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

        let (from_store, from_path) = Self::get_store(&from_parsed_url)?;
        let (to_store, to_path) = Self::get_store(&to_parsed_url)?;

        let same_store = from_parsed_url.scheme() == to_parsed_url.scheme()
            && from_parsed_url.host_str() == to_parsed_url.host_str();

        if same_store {
            match from_store.as_ref().rename(&from_path, &to_path).await {
                Ok(()) => {}
                Err(e) if e.to_string().contains("os error 18") => {
                    // Fallback for cross-device move
                    let result = from_store.as_ref().get(&from_path).await?;
                    let bytes = result.bytes().await?;
                    to_store
                        .as_ref()
                        .put(&to_path, PutPayload::from(bytes))
                        .await?;
                    from_store.as_ref().delete(&from_path).await?;
                }
                Err(e) => return Err(e.into()),
            }
        } else {
            // Cross-provider move
            let result = from_store.as_ref().get(&from_path).await?;
            let bytes = result.bytes().await?;
            to_store
                .as_ref()
                .put(&to_path, PutPayload::from(bytes))
                .await?;
            from_store.as_ref().delete(&from_path).await?;
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
        client.put(&url, data).await?;

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
        client.put(&src_url, data).await?;
        client.copy(&src_url, &dst_url).await?;
        assert_eq!(client.get(&dst_url).await?.as_ref(), data);
        assert_eq!(client.get(&src_url).await?.as_ref(), data);

        // Test Move
        let dst_url2 = format!("file://{}.2", dst_path.to_str().unwrap());
        client.move_object(&dst_url, &dst_url2).await?;
        assert_eq!(client.get(&dst_url2).await?.as_ref(), data);
        assert!(client.get(&dst_url).await.is_err());

        Ok(())
    }
}
