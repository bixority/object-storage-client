use crate::ObjectInfo;
use crate::backends::common::Backend;
use chrono::DateTime;
use futures::Stream;
use std::path::Path;
use std::time::UNIX_EPOCH;
use walkdir::WalkDir;

pub struct LocalBackend;

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
                    let last_modified = meta
                        .modified()
                        .ok()
                        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                        .map(|d| {
                            DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
                                .unwrap_or_default()
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
        })
        .await??;
        Ok(objects)
    }

    async fn get_stream(
        &self,
        key: &str,
    ) -> Result<Box<dyn Stream<Item = Result<Bytes, anyhow::Error>> + Unpin + Send>> {
        let file = tokio::fs::File::open(key).await?;
        let stream = tokio::io::BufReader::new(file).lines().map(|l| {
            l.map(|s| Bytes::from(s.into_bytes() + b"\n"))
                .map_err(anyhow::Error::from)
        });
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
