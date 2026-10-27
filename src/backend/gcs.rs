//! Google Cloud Storage backend.

use super::{Backend, Vars, bucket_exists_from_response, error_body};
use crate::client::{Error, Result};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::signer::Signer;
use std::sync::Arc;
use url::Url;

/// GCS backend. Retains the concrete [`GoogleCloudStorage`] store so its
/// `OAuth2` bearer credential can be read for the `buckets.insert` call and for
/// pre-signing.
pub struct GcsBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<GoogleCloudStorage>,
    scheme: String,
    bucket: String,
    base_url: String,
    vars: Vars,
}

impl GcsBackend {
    /// Build the GCS store from `vars` (the environment, via
    /// [`GoogleCloudStorageBuilder::from_env`], unless injected).
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket (URL host) is missing or the store fails
    /// to build.
    pub fn new(scheme: &str, host: Option<&str>, vars: &Vars) -> Result<Self> {
        let bucket = host.ok_or_else(|| Error::Generic("Missing bucket in GCS URL".into()))?;
        let mut builder = match vars {
            Vars::Env => GoogleCloudStorageBuilder::from_env(),
            Vars::Map(_) => vars
                .config_entries::<GoogleConfigKey>("GOOGLE_")
                .into_iter()
                .fold(GoogleCloudStorageBuilder::new(), |b, (k, v)| {
                    b.with_config(k, v)
                }),
        };
        builder = builder.with_bucket_name(bucket);
        let inner = Arc::new(builder.build()?);
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            base_url: base_url(vars),
            vars: vars.clone(),
        })
    }
}

/// Default origin of the GCS JSON API.
const DEFAULT_BASE_URL: &str = "https://storage.googleapis.com";

/// The JSON API origin: `GCS_BASE_URL` (without a trailing slash) or the
/// public default.
fn base_url(vars: &Vars) -> String {
    vars.get("GCS_BASE_URL")
        .filter(|u| !u.trim().is_empty())
        .map_or_else(
            || DEFAULT_BASE_URL.to_string(),
            |u| u.trim().trim_end_matches('/').to_string(),
        )
}

/// Attaches the bearer token to `request`, but only when `base` is an https
/// origin so a token is never sent in clear text to a custom `GCS_BASE_URL`.
/// `disable_oauth` (emulators) yields an empty bearer: no header is sent.
fn with_bearer(
    request: reqwest::RequestBuilder,
    base: &str,
    bearer: &str,
) -> reqwest::RequestBuilder {
    let https = base
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"));
    if bearer.is_empty() || !https {
        request
    } else {
        request.bearer_auth(bearer)
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
    async fn create_bucket(&self, client: &reqwest::Client, _url: &Url) -> Result<()> {
        // buckets.insert requires the enclosing project; object_store does not
        // track it (it is irrelevant to object operations).
        let project = self
            .vars
            .get("GOOGLE_CLOUD_PROJECT")
            .or_else(|| self.vars.get("GCS_PROJECT_ID"))
            .ok_or_else(|| {
                Error::Generic(
                    "GCS bucket creation requires GOOGLE_CLOUD_PROJECT (or GCS_PROJECT_ID)".into(),
                )
            })?;

        let credential = self.inner.credentials().get_credential().await?;
        let body = Bytes::from(format!("{{\"name\":\"{}\"}}", self.bucket));
        let encoded_project: String =
            percent_encoding::utf8_percent_encode(&project, percent_encoding::NON_ALPHANUMERIC)
                .collect();
        let uri = format!("{}/storage/v1/b?project={encoded_project}", self.base_url);

        let request = client
            .post(uri)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body);
        let request = with_bearer(request, &self.base_url, &credential.bearer);
        let response = request
            .send()
            .await
            .map_err(|e| Error::Generic(format!("GCS buckets.insert request failed: {e}")))?;

        let status = response.status();
        if status.is_success() || status.as_u16() == 409 {
            // Idempotent: 409 means the bucket already exists.
            return Ok(());
        }
        let text = error_body(response).await;
        Err(Error::Generic(format!(
            "GCS buckets.insert failed (HTTP {status}): {text}"
        )))
    }

    /// Checks bucket existence via the JSON API `buckets.get`
    /// (`GET /storage/v1/b/<bucket>`), authenticated with the backend's `OAuth2`
    /// bearer token. This metadata-only probe avoids listing the bucket's
    /// objects: HTTP `200` means it exists, `404` that it does not, and `403`
    /// that it exists but is not visible to these credentials.
    async fn bucket_exists(&self, client: &reqwest::Client, _url: &Url) -> Result<bool> {
        let credential = self.inner.credentials().get_credential().await?;
        let encoded_bucket: String =
            percent_encoding::utf8_percent_encode(&self.bucket, percent_encoding::NON_ALPHANUMERIC)
                .collect();
        let uri = format!("{}/storage/v1/b/{encoded_bucket}", self.base_url);

        let request = with_bearer(client.get(uri), &self.base_url, &credential.bearer);
        let response = request
            .send()
            .await
            .map_err(|e| Error::Generic(format!("GCS buckets.get request failed: {e}")))?;

        bucket_exists_from_response(response, "GCS").await
    }

    fn signer(&self) -> Option<&dyn Signer> {
        Some(self.inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn base_url_defaults_to_googleapis() {
        assert_eq!(
            base_url(&Vars::from_pairs(&[])),
            "https://storage.googleapis.com"
        );
        assert_eq!(
            base_url(&Vars::from_pairs(&[("GCS_BASE_URL", "  ")])),
            "https://storage.googleapis.com"
        );
    }

    #[test]
    fn base_url_override_drops_trailing_slash() {
        assert_eq!(
            base_url(&Vars::from_pairs(&[(
                "GCS_BASE_URL",
                "http://127.0.0.1:4443/"
            )])),
            "http://127.0.0.1:4443"
        );
    }

    #[test]
    fn bearer_is_sent_only_over_https() {
        let client = reqwest::Client::new();
        let header = |base: &str, token: &str| {
            with_bearer(client.get("http://localhost/x"), base, token)
                .build()
                .expect("request")
                .headers()
                .get(http::header::AUTHORIZATION)
                .cloned()
        };
        assert!(header("https://storage.googleapis.com", "tok").is_some());
        assert!(header("HTTPS://example.com", "tok").is_some());
        assert!(header("http://127.0.0.1:4443", "tok").is_none());
        assert!(header("https://storage.googleapis.com", "").is_none());
    }
}
