use std::sync::Arc;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use futures::Stream;
use crate::backends::common::Backend;
use crate::ObjectInfo;

pub struct S3Backend {
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

        let multipart = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
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
