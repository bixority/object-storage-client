use crate::backends::common::Backend;
use crate::ObjectInfo;

pub struct AzureBackend {
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
                let full_key = format!(
                    "azure://{}/{}",
                    self.container_client.container_name(),
                    blob.name
                );
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
        let stream = blob_client
            .get_content()
            .await
            .map_err(anyhow::Error::from)?;
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
        self.container_client
            .blob_client(dest_key)
            .copy_from_url(src_url)
            .await?;
        Ok(())
    }
}
