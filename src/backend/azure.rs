//! Azure Blob Storage backend.

use super::{
    Backend, bucket_exists_from_response, generic_pre_signed_url, generic_pre_signed_urls,
    send_signed,
};
use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use http::Method;
use object_store::ObjectStore;
use object_store::azure::{AzureAuthorizer, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path as ObjectPath;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Azure backend. Retains the concrete [`MicrosoftAzure`] store so its storage
/// credentials can be read for the `Create Container` call and for pre-signing.
pub(crate) struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<MicrosoftAzure>,
    scheme: String,
}

impl AzureBackend {
    /// Build the Azure store from the environment via
    /// [`MicrosoftAzureBuilder::from_env`].
    ///
    /// # Errors
    ///
    /// Returns an error if the container (URL host) is missing or the store
    /// fails to build.
    pub(crate) fn from_env(scheme: &str, host: Option<&str>) -> Result<Self> {
        let container =
            host.ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;
        let inner = Arc::new(
            MicrosoftAzureBuilder::from_env()
                .with_container_name(container)
                .build()?,
        );
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
        })
    }

    /// Resolve the `(account, blob endpoint)` the container-management requests
    /// target. `object_store` does not expose the resolved account/endpoint, so
    /// this mirrors the configuration the store was built from.
    ///
    /// # Errors
    ///
    /// Returns an error if `AZURE_STORAGE_ACCOUNT_NAME` is unset.
    fn account_and_endpoint() -> Result<(String, String)> {
        let account = std::env::var("AZURE_STORAGE_ACCOUNT_NAME").map_err(|_| {
            Error::Generic("Azure container operations require AZURE_STORAGE_ACCOUNT_NAME".into())
        })?;
        let endpoint = std::env::var("AZURE_STORAGE_ENDPOINT")
            .unwrap_or_else(|_| format!("https://{account}.blob.core.windows.net"));
        Ok((account, endpoint))
    }
}

#[async_trait::async_trait]
impl Backend for AzureBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Issues an Azure `Create Container` request
    /// (`PUT /<container>?restype=container`), signed with `object_store`'s
    /// [`AzureAuthorizer`]. Treats `ContainerAlreadyExists` / HTTP 409 as
    /// success.
    async fn create_bucket(&self, client: &reqwest::Client, url: &Url) -> Result<()> {
        let container = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;

        let (account, endpoint) = Self::account_and_endpoint()?;
        let uri = format!(
            "{}/{container}?restype=container",
            endpoint.trim_end_matches('/')
        );

        let credential = self.inner.credentials().get_credential().await?;
        let mut request: HttpRequest = http::Request::builder()
            .method(Method::PUT)
            .uri(&uri)
            .header(http::header::CONTENT_LENGTH, "0")
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build Azure request: {e}")))?;

        // Reuse object_store's SharedKey / SAS / Bearer authorization.
        AzureAuthorizer::new(credential.as_ref(), &account).authorize(&mut request);

        let response = send_signed(client, request, Bytes::new()).await?;
        let status = response.status();
        if status.is_success() || status.as_u16() == 409 {
            return Ok(());
        }

        let text = response.text().await.unwrap_or_default();
        if text.contains("ContainerAlreadyExists") {
            return Ok(());
        }
        Err(Error::Generic(format!(
            "Azure CreateContainer failed (HTTP {status}): {text}"
        )))
    }

    /// Issues an Azure `Get Container Properties` request
    /// (`HEAD /<container>?restype=container`), signed with `object_store`'s
    /// [`AzureAuthorizer`]. This metadata-only probe avoids listing the
    /// container's blobs: HTTP `200` means it exists, `404` that it does not.
    async fn bucket_exists(&self, client: &reqwest::Client, url: &Url) -> Result<bool> {
        let container = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;

        let (account, endpoint) = Self::account_and_endpoint()?;
        let uri = format!(
            "{}/{container}?restype=container",
            endpoint.trim_end_matches('/')
        );

        let credential = self.inner.credentials().get_credential().await?;
        let mut request: HttpRequest = http::Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build Azure request: {e}")))?;

        AzureAuthorizer::new(credential.as_ref(), &account).authorize(&mut request);

        let response = send_signed(client, request, Bytes::new()).await?;
        bucket_exists_from_response(response, "Azure").await
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
