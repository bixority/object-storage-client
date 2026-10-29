//! Google Cloud Storage backend.

use super::{
    Backend, BoundedConnector, Vars, bucket_exists_from_response, default_timeouts, error_body,
    request_failed,
};
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
        let bucket = host.ok_or_else(|| Error::InvalidInput("Missing bucket in GCS URL".into()))?;
        let inner = Arc::new(configured(bucket, vars).build()?);
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            base_url: base_url(vars),
            vars: vars.clone(),
        })
    }

    /// `buckets.get` (`GET /storage/v1/b/<bucket>`) with the bearer token.
    async fn get_bucket(
        &self,
        client: &reqwest::Client,
        bearer: &str,
    ) -> Result<reqwest::Response> {
        let encoded_bucket: String =
            percent_encoding::utf8_percent_encode(&self.bucket, percent_encoding::NON_ALPHANUMERIC)
                .collect();
        let uri = format!("{}/storage/v1/b/{encoded_bucket}", self.base_url);
        with_bearer(client.get(uri), &self.base_url, bearer)
            .send()
            .await
            .map_err(|e| request_failed("GCS buckets.get request", e))
    }

    /// After a `409` from `buckets.insert`: succeeds only if the bucket's
    /// metadata is readable, i.e. the name is ours and not another project's.
    async fn check_owned(&self, client: &reqwest::Client, bearer: &str) -> Result<()> {
        let response = self.get_bucket(client, bearer).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        Err(Error::Generic(format!(
            "GCS bucket `{}` already exists and belongs to another project, or these \
             credentials cannot use it (HTTP {status})",
            self.bucket
        )))
    }
}

/// Default origin of the GCS JSON API.
const DEFAULT_BASE_URL: &str = "https://storage.googleapis.com";

/// The builder for `bucket` with `vars` and the crate's defaults applied.
fn configured(bucket: &str, vars: &Vars) -> GoogleCloudStorageBuilder {
    let mut builder = match vars {
        Vars::Env => GoogleCloudStorageBuilder::from_env(),
        Vars::Map(_) => vars
            .config_entries::<GoogleConfigKey>("GOOGLE_")
            .into_iter()
            .fold(GoogleCloudStorageBuilder::new(), |b, (k, v)| {
                b.with_config(k, v)
            }),
    };
    for (key, value) in default_timeouts(vars, "GOOGLE_") {
        builder = builder.with_config(GoogleConfigKey::Client(key), value);
    }
    builder
        .with_bucket_name(bucket)
        .with_http_connector(BoundedConnector::default())
}

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
    /// flow already implemented by `object_store`). On HTTP 409 the name is taken,
    /// so the bucket's metadata is read with the same credential: `200` means
    /// the caller's project owns it (success), anything else means another
    /// project does.
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
            .map_err(|e| request_failed("GCS buckets.insert request", e))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        if status.as_u16() == 409 {
            return self.check_owned(client, &credential.bearer).await;
        }
        let text = error_body(response).await;
        Err(Error::Generic(format!(
            "GCS buckets.insert failed (HTTP {status}): {text}"
        )))
    }

    /// Checks bucket existence via the JSON API `buckets.get`
    /// (`GET /storage/v1/b/<bucket>`), authenticated with the backend's `OAuth2`
    /// bearer token. This metadata-only probe avoids listing the bucket's
    /// objects: HTTP `200` means it exists and `404` that it does not. A `403` or
    /// `401` is an error: existence is unknown.
    async fn bucket_exists(&self, client: &reqwest::Client, _url: &Url) -> Result<bool> {
        let credential = self.inner.credentials().get_credential().await?;
        let response = self.get_bucket(client, &credential.bearer).await?;
        bucket_exists_from_response(response, "GCS").await
    }

    fn signer(&self) -> Option<&dyn Signer> {
        Some(self.inner.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::stub::{Reply, Request, serve};
    use object_store::ClientConfigKey;

    /// Runs `create_bucket` against a stub that answers `replies` in order.
    async fn create(replies: Vec<Reply>) -> (Result<()>, Vec<Request>) {
        let stub = serve(replies).await;
        let backend = GcsBackend::new(
            "gs",
            Some("bkt"),
            &Vars::from_pairs(&[
                ("GCS_BASE_URL", &stub.base),
                ("GOOGLE_CLOUD_PROJECT", "proj"),
                ("GOOGLE_BEARER_TOKEN", "tok"),
            ]),
        )
        .expect("build");
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let url = Url::parse("gs://bkt").expect("url");
        let result = backend.create_bucket(&client, &url).await;
        (result, stub.requests())
    }

    #[tokio::test]
    async fn gcs_conflict_on_an_owned_bucket_succeeds() {
        let (result, requests) = create(vec![Reply::new(409, ""), Reply::new(200, "{}")]).await;
        result.expect("owned bucket");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].line, "POST /storage/v1/b?project=proj HTTP/1.1");
        assert_eq!(requests[1].line, "GET /storage/v1/b/bkt HTTP/1.1");
    }

    #[tokio::test]
    async fn gcs_conflict_on_a_foreign_bucket_fails() {
        for status in [403, 404] {
            let (result, _) = create(vec![Reply::new(409, ""), Reply::new(status, "")]).await;
            let err = result.expect_err("foreign bucket");
            let text = err.to_string();
            assert!(text.contains("`bkt`"), "{text}");
            assert!(text.contains("another project"), "{text}");
        }
    }
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

    #[test]
    fn timeouts_default_to_a_day_and_five_minutes_and_the_users_win() {
        let key = |k| GoogleConfigKey::Client(k);
        let default = configured("b", &Vars::from_pairs(&[]));
        assert_eq!(
            default
                .get_config_value(&key(ClientConfigKey::Timeout))
                .as_deref(),
            Some("86400s")
        );
        assert_eq!(
            default
                .get_config_value(&key(ClientConfigKey::ReadTimeout))
                .as_deref(),
            Some("300s")
        );
        let set = configured(
            "b",
            &Vars::from_pairs(&[("GOOGLE_TIMEOUT", "7s"), ("GOOGLE_READ_TIMEOUT", "9s")]),
        );
        assert_eq!(
            set.get_config_value(&key(ClientConfigKey::Timeout))
                .as_deref(),
            Some("7s")
        );
        assert_eq!(
            set.get_config_value(&key(ClientConfigKey::ReadTimeout))
                .as_deref(),
            Some("9s")
        );
    }
}
