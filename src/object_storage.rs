use crate::backends::azure::AzureBackend;
use crate::backends::common::Backend;
use crate::backends::local::LocalBackend;
use crate::backends::s3::S3Backend;
use crate::{Config, ObjectInfo};
use aws_config::meta::region::RegionProviderChain;
use bytes::Bytes;
use futures::Stream;
use std::sync::Arc;
use tracing::instrument;

/// Unified object storage client.
#[derive(Clone)]
pub struct ObjectStorage {
    s3_client: Option<Arc<S3Client>>,
    azure_client: Option<Arc<BlobServiceClient>>,
}

impl ObjectStorage {
    /// Creates a new storage client.
    #[instrument]
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let s3_client = if config.s3_region.is_some() || std::env::var("AWS_REGION").is_ok() {
            let region_provider = RegionProviderChain::default_provider()
                .or_else(config.s3_region.as_deref().unwrap_or("us-east-1"));
            let cfg = aws_config::from_env().region(region_provider).load().await;
            Some(Arc::new(S3Client::new(&cfg)))
        } else {
            None
        };

        let azure_client = if let Some(account) = config.azure_account {
            let credential = Arc::new(DefaultAzureCredential::default());
            Arc::new(BlobServiceClient::new(account, credential))
        } else {
            None
        };

        Ok(Self {
            s3_client,
            azure_client,
        })
    }

    fn get_backend(&self, backend_str: &str, resource: &str) -> anyhow::Result<Arc<dyn Backend>> {
        match backend_str {
            "s3" => {
                let client = self
                    .s3_client
                    .as_ref()
                    .context("S3 client not configured")?
                    .clone();
                Ok(Arc::new(S3Backend {
                    client,
                    bucket: resource.to_string(),
                }))
            }
            "azure" => {
                let service = self
                    .azure_client
                    .as_ref()
                    .context("Azure client not configured")?
                    .clone();
                let container_client = service.container_client(resource);
                Ok(Arc::new(AzureBackend { container_client }))
            }
            "file" => Ok(Arc::new(LocalBackend)),
            _ => Err(anyhow::anyhow!("Unsupported backend: {}", backend_str)),
        }
    }

    /// Parses a URI into (backend, resource, path).
    pub fn parse_uri(uri: &str) -> anyhow::Result<(String, String, String)> {
        if uri.starts_with("s3://") {
            let after = &uri[5..];
            if let Some(slash_pos) = after.find('/') {
                let bucket = after[..slash_pos].to_string();
                let path = after[slash_pos + 1..].to_string();
                Ok(("s3".to_string(), bucket, path))
            } else {
                Ok(("s3".to_string(), after.to_string(), "".to_string()))
            }
        } else if uri.starts_with("azure://") {
            let after = &uri[8..];
            if let Some(slash_pos) = after.find('/') {
                let container = after[..slash_pos].to_string();
                let path = after[slash_pos + 1..].to_string();
                Ok(("azure".to_string(), container, path))
            } else {
                Ok(("azure".to_string(), after.to_string(), "".to_string()))
            }
        } else if uri.starts_with("file://") {
            let path = &uri[7..];
            Ok(("file".to_string(), "".to_string(), path.to_string()))
        } else {
            Err(anyhow::anyhow!(
                "Invalid URI prefix. Use 'file://', 's3://', or 'azure://'"
            ))
        }
    }

    /// Lists objects by URI prefix.
    #[instrument(skip(self))]
    pub async fn list(&self, prefix: &str) -> anyhow::Result<Vec<ObjectInfo>> {
        let (backend_str, resource, path_prefix) = Self::parse_uri(prefix)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.list(&path_prefix).await
    }

    /// Gets a byte stream for the object.
    #[instrument(skip(self))]
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> anyhow::Result<Box<dyn Stream<Item = anyhow::Result<Bytes, anyhow::Error>> + Unpin + Send>>
    {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.get_stream(&path).await
    }

    /// Puts an object from a byte stream.
    #[instrument(skip(self, stream))]
    pub async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = anyhow::Result<Bytes, anyhow::Error>> + Unpin + Send + 'static,
        content_length: Option<u64>,
    ) -> anyhow::Result<()> {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.put_stream(&path, stream, content_length).await
    }

    /// Deletes an object.
    #[instrument(skip(self))]
    pub async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.delete(&path).await
    }

    /// Copies an object (streams across backends, native within).
    #[instrument(skip(self))]
    pub async fn copy(&self, src: &str, dest: &str) -> anyhow::Result<()> {
        let (src_backend_str, src_resource, src_path) = Self::parse_uri(src)?;
        let (dest_backend_str, dest_resource, dest_path) = Self::parse_uri(dest)?;
        if src_backend_str == dest_backend_str && src_resource == dest_resource {
            let backend = self.get_backend(&src_backend_str, &src_resource)?;
            backend.copy(&src_path, &dest_path).await
        } else {
            let mut src_stream = self.get_stream(src).await?;
            self.put_stream(dest, &mut src_stream, None).await
        }
    }
}
