//! Azure Blob Storage backend.

use super::{Backend, Vars, bucket_exists_from_response, error_body, send_signed};
use crate::client::{Error, Result};
use bytes::Bytes;
use http::Method;
use object_store::ObjectStore;
use object_store::azure::{AzureAuthorizer, AzureConfigKey, MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::signer::Signer;
use std::sync::Arc;
use url::Url;

/// Azure backend. Retains the concrete [`MicrosoftAzure`] store so its storage
/// credentials can be read for the `Create Container` call and for pre-signing.
pub struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<MicrosoftAzure>,
    scheme: String,
    container: String,
    vars: Vars,
}

impl AzureBackend {
    /// Build the Azure store from `vars` (the environment, via
    /// [`MicrosoftAzureBuilder::from_env`], unless injected).
    ///
    /// # Errors
    ///
    /// Returns an error if the container (URL host) is missing or the store
    /// fails to build.
    pub fn new(scheme: &str, host: Option<&str>, vars: &Vars) -> Result<Self> {
        let container =
            host.ok_or_else(|| Error::Generic("Missing container in Azure URL".into()))?;
        let mut builder = match vars {
            Vars::Env => MicrosoftAzureBuilder::from_env(),
            Vars::Map(_) => vars
                .config_entries::<AzureConfigKey>("AZURE_")
                .into_iter()
                .fold(MicrosoftAzureBuilder::new(), |b, (k, v)| {
                    b.with_config(k, v)
                }),
        };
        builder = builder.with_container_name(container);
        let inner = Arc::new(builder.build()?);
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
            container: container.to_string(),
            vars: vars.clone(),
        })
    }

    /// Resolve the `(account, blob endpoint)` the container-management requests
    /// target. `object_store` does not expose the resolved account/endpoint, so
    /// this mirrors the configuration the store was built from.
    ///
    /// # Errors
    ///
    /// Returns an error if `AZURE_STORAGE_ACCOUNT_NAME` is unset, or if the
    /// endpoint is plain `http://` while `AZURE_ALLOW_HTTP` does not allow it:
    /// the credential (key, SAS or bearer token) would travel in the clear.
    /// That is the setting `object_store` requires for the object path too.
    fn account_and_endpoint(&self) -> Result<(String, String)> {
        let account = self.vars.get("AZURE_STORAGE_ACCOUNT_NAME").ok_or_else(|| {
            Error::Generic("Azure container operations require AZURE_STORAGE_ACCOUNT_NAME".into())
        })?;
        let endpoint = self
            .vars
            .get("AZURE_STORAGE_ENDPOINT")
            .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
        let plain = endpoint
            .get(..7)
            .is_some_and(|p| p.eq_ignore_ascii_case("http://"));
        let allowed = self
            .vars
            .get("AZURE_ALLOW_HTTP")
            .and_then(|v| super::parse_bool(&v))
            .unwrap_or(false);
        if plain && !allowed {
            return Err(Error::Generic(
                "Azure container operations over plain http need AZURE_ALLOW_HTTP=true".into(),
            ));
        }
        Ok((account, endpoint))
    }

    /// A container-level request (`<method> /<container>?restype=container`),
    /// authorized with `object_store`'s `SharedKey` / SAS / Bearer scheme.
    async fn container_request(&self, method: Method) -> Result<HttpRequest> {
        let (account, endpoint) = self.account_and_endpoint()?;
        let uri = format!(
            "{}/{}?restype=container",
            endpoint.trim_end_matches('/'),
            self.container
        );

        let credential = self.inner.credentials().get_credential().await?;
        let mut builder = http::Request::builder().method(&method).uri(&uri);
        if method == Method::PUT {
            builder = builder.header(http::header::CONTENT_LENGTH, "0");
        }
        let mut request: HttpRequest = builder
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build Azure request: {e}")))?;

        AzureAuthorizer::new(credential.as_ref(), &account)
            .try_authorize(&mut request)
            .map_err(|e| Error::Generic(format!("Failed to sign Azure request: {e}")))?;
        Ok(request)
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
    async fn create_bucket(&self, client: &reqwest::Client, _url: &Url) -> Result<()> {
        let request = self.container_request(Method::PUT).await?;
        let response = send_signed(client, request, Bytes::new()).await?;
        let status = response.status();
        if status.is_success() || status.as_u16() == 409 {
            return Ok(());
        }

        let text = error_body(response).await;
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
    async fn bucket_exists(&self, client: &reqwest::Client, _url: &Url) -> Result<bool> {
        let request = self.container_request(Method::HEAD).await?;
        let response = send_signed(client, request, Bytes::new()).await?;
        bucket_exists_from_response(response, "Azure").await
    }

    fn signer(&self) -> Option<&dyn Signer> {
        Some(self.inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backend(pairs: &[(&str, &str)]) -> AzureBackend {
        AzureBackend::new("az", Some("c"), &Vars::from_pairs(pairs)).expect("build")
    }

    #[test]
    fn container_calls_never_use_plain_http_unless_allowed() {
        let base = [
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
        ];
        let with = |extra: &[(&str, &str)]| backend(&[&base[..], extra].concat());

        let default = with(&[]).account_and_endpoint().expect("endpoint");
        assert_eq!(default.1, "https://acct.blob.core.windows.net");

        let https = with(&[("AZURE_STORAGE_ENDPOINT", "https://h:1")]);
        assert!(https.account_and_endpoint().is_ok());

        let http = [("AZURE_STORAGE_ENDPOINT", "http://127.0.0.1:10000/acct")];
        assert!(with(&http).account_and_endpoint().is_err());
        let off = [&http[..], &[("AZURE_ALLOW_HTTP", "false")][..]].concat();
        assert!(with(&off).account_and_endpoint().is_err());
        let on = [&http[..], &[("AZURE_ALLOW_HTTP", "true")][..]].concat();
        assert!(with(&on).account_and_endpoint().is_ok());
    }
}
