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

mod backends;
mod object_storage;
mod py;

use chrono::{DateTime, Utc};
use futures::StreamExt;

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
