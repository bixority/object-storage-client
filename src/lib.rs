#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

//! A thread-safe, async object storage library supporting local filesystem, AWS S3, and Azure Blob Storage.
//! URIs use prefixes: `file:///path`, `s3://bucket/key`, `azure://container/key`.
//!
//! # Features
//! - Async operations with Tokio.
//! - Streaming support for get/put/copy (cross-backend via stream piping).
//! - Multipart/block uploads for large streaming puts.
//! - Native copy for same-backend efficiency.
//! - Layered: Config → Clients → Backend traits → Unified API.
//! - Resource-efficient: Shared Arc clients, chunked streaming without full buffering.
//! - Python bindings via PyO3 for async usage in Python 3.14+.
//!
//! # Usage
//! ```
//! use object_storage::{Config, ObjectStorage, ObjectInfo};
//! use futures::StreamExt;
//! use bytes::Bytes;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let config = Config {
//!         s3_region: Some("us-east-1".to_string()),
//!         azure_account: Some("myaccount".to_string()),
//!     };
//!     let store = ObjectStorage::new(config).await?;
//!
//!     // List
//!     let objects: Vec<ObjectInfo> = store.list("s3://my-bucket/prefix/").await?;
//!
//!     // Get stream
//!     let stream = store.get_stream("file:///path/to/file.txt").await?;
//!     // Consume: while let Some(chunk) = stream.next().await { ... }
//!
//!     // Put stream
//!     let data_stream = futures::stream::iter(vec![Ok(Bytes::from("hello"))]);
//!     store.put_stream("s3://my-bucket/file.txt", data_stream, None).await?;
//!
//!     // Delete
//!     store.delete("azure://my-container/file.txt").await?;
//!
//!     // Copy (cross-backend streams, same uses native)
//!     store.copy("s3://src-bucket/file.txt", "azure://dest-container/file.txt").await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Python Usage
//! The library compiles to a Python module named `object_storage`.
//! Build with `maturin develop` or `cargo build --release` and copy the .so/.pyd.
//! Methods are async, use `await`.
//! Currently, streaming is internal; get/put use full bytes. TODO: Add async gen/iter support.
//!
//! ```python
//! import asyncio
//! from object_storage import Config, ObjectStorage, ObjectInfo
//!
//! async def main():
//!     config = Config()
//!     config.s3_region = "us-east-1"
//!     config.azure_account = "myaccount"
//!     store = await ObjectStorage.new(config)
//!     objects = await store.list("s3://my-bucket/prefix/")
//!     data = await store.get("file:///path/to/file.txt")
//!     await store.put("s3://my-bucket/file.txt", b"hello")
//!     await store.delete("azure://my-container/file.txt")
//!     await store.copy("s3://src-bucket/file.txt", "azure://dest-container/file.txt")
//!
//! asyncio.run(main())
//! ```

mod py;

use anyhow::{Context, Result};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{
    types::{CompletedMultipartUpload, CompletedPart},
    Client as S3Client,
};

use base64::engine::general_purpose;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{Stream, StreamExt};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use aws_sdk_s3::primitives::ByteStream;
use tracing::{info, instrument};
use walkdir::WalkDir;

/// Configuration for object storage backends.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Optional S3 region (defaults to environment if unset).
    pub s3_region: Option<String>,
    /// Optional Azure storage account name (required for Azure support).
    pub azure_account: Option<String>,
}

/// Metadata for an object.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    /// Full URI key (e.g., "s3://bucket/key.txt").
    pub key: String,
    /// Object size in bytes.
    pub size: Option<u64>,
    /// Last modified timestamp.
    pub last_modified: Option<DateTime<Utc>>,
    /// ETag (storage-specific).
    pub etag: Option<String>,
}

#[async_trait::async_trait]
/// Trait for backend-specific operations.
pub trait Backend: Send + Sync {
    /// Lists objects with the given path prefix.
    async fn list(&self, path_prefix: &str) -> Result<Vec<ObjectInfo>>;

    /// Gets a stream of object bytes.
    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>>;

    /// Puts an object from a byte stream (supports multipart/block for large data).
    async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
        content_length: Option<u64>,
    ) -> Result<()>;

    /// Deletes an object.
    async fn delete(&self, key: &str) -> Result<()>;

    /// Copies an object within the same backend (native where possible).
    async fn copy(&self, src_key: &str, dest_key: &str) -> Result<()>;
}

/// Unified object storage client.
#[derive(Clone)]
pub struct ObjectStorage {
    s3_client: Option<Arc<S3Client>>,
    azure_client: Option<Arc<BlobServiceClient>>,
}

impl ObjectStorage {
    /// Creates a new storage client.
    #[instrument]
    pub async fn new(config: Config) -> Result<Self> {
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

    fn get_backend(
        &self,
        backend_str: &str,
        resource: &str,
    ) -> Result<Arc<dyn Backend>> {
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
    pub fn parse_uri(uri: &str) -> Result<(String, String, String)> {
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
            Err(anyhow::anyhow!("Invalid URI prefix. Use 'file://', 's3://', or 'azure://'"))
        }
    }

    /// Lists objects by URI prefix.
    #[instrument(skip(self))]
    pub async fn list(&self, prefix: &str) -> Result<Vec<ObjectInfo>> {
        let (backend_str, resource, path_prefix) = Self::parse_uri(prefix)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.list(&path_prefix).await
    }

    /// Gets a byte stream for the object.
    #[instrument(skip(self))]
    pub async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>> {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.get_stream(&path).await
    }

    /// Puts an object from a byte stream.
    #[instrument(skip(self, stream))]
    pub async fn put_stream(
        &self,
        key: &str,
        stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send + 'static,
        content_length: Option<u64>,
    ) -> Result<()> {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.put_stream(&path, stream, content_length).await
    }

    /// Deletes an object.
    #[instrument(skip(self))]
    pub async fn delete(&self, key: &str) -> Result<()> {
        let (backend_str, resource, path) = Self::parse_uri(key)?;
        let backend = self.get_backend(&backend_str, &resource)?;
        backend.delete(&path).await
    }

    /// Copies an object (streams across backends, native within).
    #[instrument(skip(self))]
    pub async fn copy(&self, src: &str, dest: &str) -> Result<()> {
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

/// S3 backend implementation.
struct S3Backend {
    client: Arc<S3Client>,
    bucket: String,
}

#[async_trait::async_trait]
impl Backend for S3Backend {
    async fn list(&self, path_prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut objects = vec![];
        let mut continuation_token = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(path_prefix);
            if let Some(token) = continuation_token {
                req = req.continuation_token(token);
            }
            let resp = req.send().await?;
            if let Some(contents) = resp.contents {
                for obj in contents {
                    let key = obj.key.context("Missing S3 object key")?;
                    let full_key = format!("s3://{}/{}", self.bucket, key);
                    objects.push(ObjectInfo {
                        key: full_key,
                        size: obj.size.map(|s| s as u64),
                        last_modified: obj.last_modified.map(|lm| lm.0),
                        etag: obj.e_tag,
                    });
                }
            }
            continuation_token = resp.next_continuation_token;
            if continuation_token.is_none() {
                break;
            }
        }
        Ok(objects)
    }

    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(Box::pin(resp.body.map_err(anyhow::Error::from)))
    }

    async fn put_stream(
        &self,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
        _content_length: Option<u64>,
    ) -> Result<()> {
        let upload_resp = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        let upload_id = upload_resp.upload_id.context("Missing upload ID")?;
        let mut parts = vec![];
        let mut part_number: i32 = 1;
        let chunk_size = 5 * 1024 * 1024; // 5MB
        let mut buffer = vec![];
        while let Some(item) = stream.next().await {
            buffer.extend_from_slice(&item?);
            while buffer.len() >= chunk_size {
                let part_data = buffer.drain(..chunk_size).collect::<Vec<_>>();
                let body = ByteStream::from(part_data);
                let part_resp = self
                    .client
                    .upload_part()
                    .bucket(&self.bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .part_number(part_number)
                    .body(body)
                    .send()
                    .await?;
                parts.push(
                    CompletedPart::builder()
                        .set_e_tag(part_resp.e_tag)
                        .part_number(part_number)
                        .build(),
                );
                part_number += 1;
            }
        }
        if !buffer.is_empty() {
            let body = ByteStream::from(buffer);
            let part_resp = self
                .client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await?;
            parts.push(
                CompletedPart::builder()
                    .set_e_tag(part_resp.e_tag)
                    .part_number(part_number)
                    .build(),
            );
        }

        let multipart = CompletedMultipartUpload::builder().set_parts(Some(parts)).build();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(multipart)
            .send()
            .await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await?;
        Ok(())
    }

    async fn copy(&self, src_key: &str, dest_key: &str) -> Result<()> {
        self.client
            .copy_object()
            .copy_source(format!("{}/{}", self.bucket, src_key))
            .bucket(&self.bucket)
            .key(dest_key)
            .send()
            .await?;
        Ok(())
    }
}

/// Azure backend implementation.
struct AzureBackend {
    container_client: ContainerClient,
}

#[async_trait::async_trait]
impl Backend for AzureBackend {
    async fn list(&self, path_prefix: &str) -> Result<Vec<ObjectInfo>> {
        let mut stream = self
            .container_client
            .list_blobs()
            .prefix(path_prefix)
            .into_stream();
        let mut objects = vec![];
        while let Some(page_resp) = stream.next().await {
            let page = page_resp?;
            for blob in page.blobs.blobs() {
                let full_key = format!("azure://{}/{}", self.container_client.container_name(), blob.name);
                objects.push(ObjectInfo {
                    key: full_key,
                    size: blob.properties.content_length,
                    last_modified: blob.properties.last_modified.map(|lm| lm.0),
                    etag: blob.properties.etag.clone(),
                });
            }
        }
        Ok(objects)
    }

    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>> {
        let blob_client = self.container_client.blob_client(key);
        let stream = blob_client.get_content().await.map_err(anyhow::Error::from)?;
        Ok(Box::pin(futures::stream::iter(stream).map(Ok)))
    }

    async fn put_stream(
        &self,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
        _content_length: Option<u64>,
    ) -> Result<()> {
        let blob_client = self.container_client.blob_client(key);
        let mut block_ids = vec![];
        let mut part_number = 0u32;
        let chunk_size = 4 * 1024 * 1024; // 4MB
        let mut buffer = vec![];
        while let Some(item) = stream.next().await {
            buffer.extend_from_slice(&item?);
            while buffer.len() >= chunk_size {
                let part_data = buffer.drain(..chunk_size).collect::<Vec<_>>();
                let block_id = general_purpose::STANDARD.encode(part_number.to_be_bytes());
                blob_client.put_block(&block_id, part_data).await?;
                block_ids.push(block_id);
                part_number += 1;
            }
        }
        if !buffer.is_empty() {
            let block_id = general_purpose::STANDARD.encode(part_number.to_be_bytes());
            blob_client.put_block(&block_id, buffer).await?;
            block_ids.push(block_id);
        }
        blob_client.put_block_list(block_ids).await?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.container_client.blob_client(key).delete().await?;
        Ok(())
    }

    async fn copy(&self, src_key: &str, dest_key: &str) -> Result<()> {
        let src_blob = self.container_client.blob_client(src_key);
        let src_url = src_blob.url()?;
        self.container_client.blob_client(dest_key).copy_from_url(src_url).await?;
        Ok(())
    }
}

/// Local filesystem backend implementation.
struct LocalBackend;

#[async_trait::async_trait]
impl Backend for LocalBackend {
    async fn list(&self, path_prefix: &str) -> Result<Vec<ObjectInfo>> {
        let prefix_path = Path::new(path_prefix);
        let objects = tokio::task::spawn_blocking(move || {
            let mut objs = vec![];
            for entry in WalkDir::new(prefix_path)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
            {
                if entry.file_type().is_file() {
                    let p_str = entry.path().to_str().context("Invalid path")?.to_string();
                    let meta = entry.metadata()?;
                    let size = Some(meta.len());
                    let last_modified = meta.modified().ok().and_then(|t| t.duration_since(UNIX_EPOCH).ok()).map(|d| {
                        DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos()).unwrap_or_default()
                    });
                    objs.push(ObjectInfo {
                        key: format!("file://{}", p_str),
                        size,
                        last_modified,
                        etag: None,
                    });
                }
            }
            Ok(objs)
        }).await??;
        Ok(objects)
    }

    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>> {
        let file = tokio::fs::File::open(key).await?;
        let stream = tokio::io::BufReader::new(file).lines().map(|l| l.map(|s| Bytes::from(s.into_bytes() + b"\n")).map_err(anyhow::Error::from));
        Ok(Box::pin(stream))
    }

    async fn put_stream(
        &self,
        key: &str,
        mut stream: impl Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send,
        _content_length: Option<u64>,
    ) -> Result<()> {
        let mut file = tokio::fs::File::create(key).await?;
        while let Some(item) = stream.next().await {
            tokio::io::AsyncWriteExt::write_all(&mut file, &item?).await?;
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<()> {
        tokio::fs::remove_file(key).await
    }

    async fn copy(&self, src_key: &str, dest_key: &str) -> Result<()> {
        tokio::fs::copy(src_key, dest_key).await.map(|_| ())
    }
}
