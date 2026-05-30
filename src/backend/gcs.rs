//! Google Cloud Storage backend.

use super::{
    Backend, bucket_exists_from_response, generic_pre_signed_url, generic_pre_signed_urls,
};
use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::path::Path as ObjectPath;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// GCS backend. Retains the concrete [`GoogleCloudStorage`] store so its
/// `OAuth2` bearer credential can be read for the `buckets.insert` call and for
/// pre-signing.
pub(crate) struct GcsBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<GoogleCloudStorage>,
    scheme: String,
}

impl GcsBackend {
    /// Build the GCS store from the environment via
    /// [`GoogleCloudStorageBuilder::from_env`].
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket (URL host) is missing or the store fails
    /// to build.
    pub(crate) fn from_env(scheme: &str, host: Option<&str>) -> Result<Self> {
        let bucket = host.ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;
        let inner = Arc::new(
            GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket)
                .build()?,
        );
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
        })
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Creates a GCS bucket via the JSON API `buckets.insert`, authenticated
    /// with the backend's `OAuth2` bearer token (the service-account / metadata
    /// flow already implemented by `object_store`). Treats HTTP 409 as success.
    async fn create_bucket(&self, client: &reqwest::Client, url: &Url) -> Result<()> {
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;

        // buckets.insert requires the enclosing project; object_store does not
        // track it (it is irrelevant to object operations).
        let project = std::env::var("GOOGLE_CLOUD_PROJECT")
            .or_else(|_| std::env::var("GCS_PROJECT_ID"))
            .map_err(|_| {
                Error::Generic(
                    "GCS bucket creation requires GOOGLE_CLOUD_PROJECT (or GCS_PROJECT_ID)".into(),
                )
            })?;

        let credential = self.inner.credentials().get_credential().await?;
        let body = Bytes::from(format!("{{\"name\":\"{bucket}\"}}"));
        let encoded_project: String =
            percent_encoding::utf8_percent_encode(&project, percent_encoding::NON_ALPHANUMERIC)
                .collect();
        let uri = format!("https://storage.googleapis.com/storage/v1/b?project={encoded_project}");

        let response = client
            .post(uri)
            .bearer_auth(&credential.bearer)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| Error::Generic(format!("GCS buckets.insert request failed: {e}")))?;

        let status = response.status();
        if status.is_success() || status.as_u16() == 409 {
            // Idempotent: 409 means the bucket already exists.
            return Ok(());
        }
        let text = response.text().await.unwrap_or_default();
        Err(Error::Generic(format!(
            "GCS buckets.insert failed (HTTP {status}): {text}"
        )))
    }

    /// Checks bucket existence via the JSON API `buckets.get`
    /// (`GET /storage/v1/b/<bucket>`), authenticated with the backend's `OAuth2`
    /// bearer token. This metadata-only probe avoids listing the bucket's
    /// objects: HTTP `200` means it exists, `404` that it does not, and `403`
    /// that it exists but is not visible to these credentials.
    async fn bucket_exists(&self, client: &reqwest::Client, url: &Url) -> Result<bool> {
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;

        let credential = self.inner.credentials().get_credential().await?;
        let encoded_bucket: String =
            percent_encoding::utf8_percent_encode(bucket, percent_encoding::NON_ALPHANUMERIC)
                .collect();
        let uri = format!("https://storage.googleapis.com/storage/v1/b/{encoded_bucket}");

        let response = client
            .get(uri)
            .bearer_auth(&credential.bearer)
            .send()
            .await
            .map_err(|e| Error::Generic(format!("GCS buckets.get request failed: {e}")))?;

        bucket_exists_from_response(response, "GCS").await
    }

    async fn pre_signed_url(
        &self,
        path: &ObjectPath,
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<String> {
        generic_pre_signed_url(self.inner.as_ref(), path, method, expires_in, options).await
    }

    async fn pre_signed_urls(
        &self,
        paths: &[ObjectPath],
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<Vec<String>> {
        generic_pre_signed_urls(self.inner.as_ref(), paths, method, expires_in, options).await
    }
}
