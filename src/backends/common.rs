use futures::Stream;
use crate::ObjectInfo;

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
