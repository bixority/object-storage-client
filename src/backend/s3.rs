//! Amazon S3 (and S3-compatible, e.g. `MinIO` / `SeaweedFS`) backend.

use super::{Backend, Vars, bucket_exists_from_response, error_body, parse_bool, send_signed};
use crate::client::{Error, Result};
use crate::sign::{self, SignMethod, SignOptions};
use bytes::Bytes;
use http::Method;
use object_store::ObjectStore;
use object_store::aws::{
    AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsAuthorizer, AwsCredential,
};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// S3 backend. Retains the concrete [`AmazonS3`] store so credentials can be
/// read for header-bound signing and `CreateBucket`, alongside the type-erased
/// store used for generic object operations.
pub struct S3Backend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<AmazonS3>,
    /// When set, requests are sent unsigned (anonymous) and the credential
    /// provider is never consulted. This is what keeps a non-AWS store from
    /// reaching for IMDS (`http://169.254.169.254`); see [`S3Backend::new`].
    skip_signature: bool,
    bucket: String,
    vars: Vars,
}

impl S3Backend {
    /// Build the S3 store from the environment, mirroring the configuration the
    /// rest of the client reads (`S3_REGION`, `S3_ACCESS_KEY_ID`,
    /// `S3_SECRET_ACCESS_KEY`, `S3_ALLOW_HTTP`) on top of
    /// [`AmazonS3Builder::from_env`].
    ///
    /// Most settings are left to `object_store`'s own env handling (region,
    /// `AWS_ENDPOINT_URL_S3`, path-style addressing). The one behaviour it gets
    /// wrong for S3-compatible stores is the *credential* fallback: with no
    /// credentials configured it defaults to the EC2 instance-metadata provider
    /// (`http://169.254.169.254`), which against a non-AWS endpoint merely hangs.
    /// So when a custom endpoint is configured and no credential source is
    /// present, signing is skipped (anonymous access) rather than falling back
    /// to IMDS. An explicit `AWS_SKIP_SIGNATURE` (the variable `object_store`
    /// itself honours) overrides this either way.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket (URL host) is missing or the store fails
    /// to build.
    pub fn new(host: Option<&str>, vars: &Vars) -> Result<Self> {
        let get_var = |k: &str| vars.get(k);
        let s3_allow_http = get_var("S3_ALLOW_HTTP").unwrap_or_else(|| "false".into());
        let bucket = host.ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;
        let mut builder = match vars {
            Vars::Env => AmazonS3Builder::from_env(),
            Vars::Map(_) => vars
                .config_entries::<AmazonS3ConfigKey>("AWS_")
                .into_iter()
                .fold(AmazonS3Builder::new(), |b, (k, v)| b.with_config(k, v)),
        }
        .with_bucket_name(bucket);

        if let Some(region) = get_var("S3_REGION") {
            builder = builder.with_region(region);
        }

        // Honour the crate's own `S3_ENDPOINT` alongside the `AWS_*` names
        // `from_env` already reads, so every endpoint name resolves the store to
        // the same place as `endpoint_and_region`.
        let endpoint = Self::configured_endpoint(vars);
        if let Some(ep) = endpoint.as_deref() {
            builder = builder.with_endpoint(ep);
        }

        if let Some(access_key_id) = get_var("S3_ACCESS_KEY_ID") {
            builder = builder.with_token("").with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = get_var("S3_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if s3_allow_http == "true"
            || endpoint
                .as_deref()
                .is_some_and(|ep| ep.starts_with("http://"))
        {
            builder = builder.with_allow_http(true);
        }

        let skip_signature = Self::explicit_skip_signature(vars)
            .unwrap_or_else(|| endpoint.is_some() && !Self::has_credentials(vars));
        if skip_signature {
            builder = builder.with_skip_signature(true);
        }

        let inner = Arc::new(builder.build()?);
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            skip_signature,
            bucket: bucket.to_string(),
            vars: vars.clone(),
        })
    }

    /// The S3 endpoint explicitly configured in the environment, in the same
    /// precedence `AmazonS3Builder::from_env` uses (`AWS_ENDPOINT_URL_S3` wins),
    /// extended with the crate's own `S3_ENDPOINT`. `None` means "no custom
    /// endpoint" — i.e. real AWS, where the IMDS credential fallback is valid.
    fn configured_endpoint(vars: &Vars) -> Option<String> {
        [
            "S3_ENDPOINT",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT",
            "AWS_ENDPOINT_URL",
        ]
        .into_iter()
        .find_map(|key| vars.get(key).filter(|v| !v.is_empty()))
    }

    /// Whether the environment provides any credential source other than the
    /// instance-metadata (IMDS) fallback: static keys (either `S3_*` or `AWS_*`),
    /// web-identity (IRSA) or container credentials. When none of these is set
    /// against a guest endpoint, IMDS is the only thing left for `object_store`
    /// to try — which is exactly what we want to avoid.
    fn has_credentials(vars: &Vars) -> bool {
        let set = |key: &str| vars.get(key).is_some_and(|v| !v.is_empty());
        set("S3_ACCESS_KEY_ID")
            || set("AWS_ACCESS_KEY_ID")
            || set("AWS_WEB_IDENTITY_TOKEN_FILE")
            || set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            || set("AWS_CONTAINER_CREDENTIALS_FULL_URI")
    }

    /// An explicit signing override from `AWS_SKIP_SIGNATURE` — the same
    /// variable `object_store` reads for the object data path — so our
    /// bucket-management and pre-signing paths stay in lockstep with the built
    /// store. `None` means the variable is unset (decide automatically); an
    /// unrecognised value is also `None` here and surfaces as a build error from
    /// `object_store` itself. Parsing mirrors `object_store`'s boolean grammar.
    fn explicit_skip_signature(vars: &Vars) -> Option<bool> {
        parse_bool(&vars.get("AWS_SKIP_SIGNATURE")?)
    }

    /// The signing credential, or `None` when signing is skipped.
    ///
    /// When `skip_signature` is set this returns `None` *without* consulting the
    /// credential provider. That matters because [`AmazonS3::credentials`] hands
    /// back the raw provider (the IMDS provider for an unconfigured store), so
    /// calling `get_credential` on it directly — as the bucket-management and
    /// pre-signing paths do — would defeat `skip_signature` and hit
    /// `http://169.254.169.254` even when object reads/writes do not.
    async fn credential(&self) -> Result<Option<Arc<AwsCredential>>> {
        if self.skip_signature {
            return Ok(None);
        }
        Ok(Some(self.inner.credentials().get_credential().await?))
    }

    /// Resolve the `(endpoint, region)` the bucket-management requests target.
    ///
    /// Mirrors the env the store was built from: `from_env` uses `S3_REGION`,
    /// then falls back to `AmazonS3Builder::from_env`, which reads `AWS_REGION`
    /// / `AWS_DEFAULT_REGION`. Region drives both the `SigV4` scope and (for
    /// `CreateBucket`) the `LocationConstraint`, so it must match the built
    /// store.
    fn endpoint_and_region(&self) -> (String, String) {
        let vars = &self.vars;
        let region = vars
            .get("S3_REGION")
            .or_else(|| vars.get("AWS_REGION"))
            .or_else(|| vars.get("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|| "us-east-1".into());
        // The AWS default endpoint is always https, whatever `S3_ALLOW_HTTP`
        // says: that setting is for custom endpoints, and the object path
        // (object_store) talks https to AWS too. A custom endpoint is used as
        // configured, as for the object path (an `http://` endpoint enables
        // plain HTTP for the store, see `S3Backend::new`).
        let endpoint = Self::configured_endpoint(vars)
            .unwrap_or_else(|| format!("https://s3.{region}.amazonaws.com"));
        (endpoint, region)
    }

    /// A bucket-level request (`/<bucket>`) carrying `body`, `SigV4`-signed
    /// unless signing is skipped. The credential comes from
    /// [`S3Backend::credential`], so `skip_signature` never reaches IMDS.
    async fn bucket_request(&self, method: Method, body: Bytes) -> Result<HttpRequest> {
        let (endpoint, region) = self.endpoint_and_region();
        // Path-style addressing keeps the request endpoint-agnostic, which is
        // what MinIO / SeaweedFS expect and AWS also accepts.
        let uri = format!("{}/{}", endpoint.trim_end_matches('/'), self.bucket);
        let mut request: HttpRequest = http::Request::builder()
            .method(method)
            .uri(&uri)
            .body(HttpRequestBody::from(body))
            .map_err(|e| Error::Generic(format!("Failed to build S3 request: {e}")))?;

        // Reuse object_store's SigV4 implementation — no signing reimplemented.
        // When signing is skipped (anonymous store) the request is sent as-is.
        if let Some(credential) = self.credential().await? {
            AwsAuthorizer::new(credential.as_ref(), "s3", &region)
                .try_authorize(&mut request, None)
                .map_err(|e| Error::Generic(format!("Failed to sign S3 request: {e}")))?;
        }
        Ok(request)
    }
}

#[async_trait::async_trait]
impl Backend for S3Backend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &'static str {
        "s3"
    }

    /// Issues an S3 `CreateBucket` request, signed with `object_store`'s
    /// [`AwsAuthorizer`] (`SigV4`). A `LocationConstraint` body is sent for every
    /// region except `us-east-1`, per the S3 API. Treats
    /// `BucketAlreadyOwnedByYou` / `BucketAlreadyExists` as success.
    async fn create_bucket(&self, client: &reqwest::Client, _url: &Url) -> Result<()> {
        let region = self.endpoint_and_region().1;
        let body = if region == "us-east-1" {
            Bytes::new()
        } else {
            Bytes::from(format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <LocationConstraint>{region}</LocationConstraint>\
                 </CreateBucketConfiguration>"
            ))
        };

        let request = self.bucket_request(Method::PUT, body.clone()).await?;
        let response = send_signed(client, request, body).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let text = error_body(response).await;
        if text.contains("BucketAlreadyOwnedByYou") || text.contains("BucketAlreadyExists") {
            // Idempotent: a bucket we already own (or that already exists).
            return Ok(());
        }
        Err(Error::Generic(format!(
            "S3 CreateBucket failed (HTTP {status}): {text}"
        )))
    }

    /// Issues an S3 `HeadBucket` request (`HEAD /<bucket>`), signed with
    /// `object_store`'s [`AwsAuthorizer`] (`SigV4`). This is a metadata-only
    /// probe that avoids the `ListBucket` payload: HTTP `200` means the bucket
    /// exists, `404` that it does not, and `403` that it exists but is not
    /// visible to these credentials.
    async fn bucket_exists(&self, client: &reqwest::Client, _url: &Url) -> Result<bool> {
        let request = self.bucket_request(Method::HEAD, Bytes::new()).await?;
        let response = send_signed(client, request, Bytes::new()).await?;
        bucket_exists_from_response(response, "S3").await
    }

    /// Host-only signing via the object store's signer, re-signed with the bound
    /// `Content-Length` / `Content-Type` headers when `options` requests them.
    async fn pre_signed_urls(
        &self,
        paths: &[ObjectPath],
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<Vec<String>> {
        // `object_store`'s signer reads the raw credential provider, so under
        // `skip_signature` it would hit IMDS. Pre-signing is meaningless without
        // credentials anyway, so fail clearly instead. This must stay the first
        // statement, and this override never falls through to the default.
        if self.skip_signature {
            return Err(Error::Generic(
                "Cannot pre-sign S3 URLs without credentials (this store runs unsigned)".into(),
            ));
        }

        let http_method = method.into_http();
        let bases = self
            .inner
            .signed_urls(http_method.clone(), paths, expires_in)
            .await?;

        if options.is_empty() {
            return Ok(bases.into_iter().map(Into::into).collect());
        }

        // Resolve credentials once, then re-sign each URL with the bound headers.
        let credential = self.inner.credentials().get_credential().await?;
        let region = self.endpoint_and_region().1;
        Ok(bases
            .into_iter()
            .map(|mut base| {
                // Drop the host-only query produced by the object store's signer;
                // we generate our own SigV4 query over the bound headers.
                base.set_query(None);
                sign::presign_s3(
                    &base,
                    &http_method,
                    expires_in,
                    &credential,
                    &region,
                    options,
                )
                .into()
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::ObjectStoreExt;

    #[test]
    fn test_endpoint_and_region_scheme_logic() {
        // The AWS default endpoint is https even when plain HTTP is allowed,
        // so bucket calls never send credentials in the clear to AWS.
        let http = S3Backend::new(Some("b"), &Vars::from_pairs(&[("S3_ALLOW_HTTP", "true")]))
            .expect("build");
        let (endpoint, region) = http.endpoint_and_region();
        assert_eq!(endpoint, format!("https://s3.{region}.amazonaws.com"));

        // A custom endpoint is used as configured.
        let custom = S3Backend::new(
            Some("b"),
            &Vars::from_pairs(&[
                ("S3_ALLOW_HTTP", "true"),
                ("S3_ENDPOINT", "http://127.0.0.1:9000"),
            ]),
        )
        .expect("build");
        assert_eq!(custom.endpoint_and_region().0, "http://127.0.0.1:9000");

        let https = S3Backend::new(Some("b"), &Vars::from_pairs(&[("S3_ALLOW_HTTP", "false")]))
            .expect("build");
        let (endpoint_https, _) = https.endpoint_and_region();
        assert!(
            endpoint_https.starts_with("https://"),
            "Should be https when allow_http is false, got {endpoint_https}"
        );
    }

    #[tokio::test]
    async fn map_vars_configure_endpoint_and_region_without_env() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[
                ("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:3900"),
                ("AWS_REGION", "eu-north-1"),
                ("AWS_ACCESS_KEY_ID", "k"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ]),
        )
        .expect("build");
        let (endpoint, region) = backend.endpoint_and_region();
        assert_eq!(endpoint, "http://127.0.0.1:3900");
        assert_eq!(region, "eu-north-1");
        assert!(!backend.skip_signature);
        // The built store itself must carry the Map-provided config.
        let url = backend
            .inner
            .signed_url(Method::GET, &ObjectPath::from("o"), Duration::from_secs(60))
            .await
            .expect("sign");
        let url = url.as_str();
        assert!(url.starts_with("http://127.0.0.1:3900/"), "{url}");
        assert!(url.contains("eu-north-1"), "{url}");
        assert!(url.contains("k%2F"), "{url}");
    }

    #[test]
    fn map_vars_without_credentials_skip_signature_for_custom_endpoint() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://127.0.0.1:3900")]),
        )
        .expect("build");
        assert!(backend.skip_signature);
    }

    #[tokio::test]
    async fn an_unsigned_store_refuses_to_pre_sign_without_a_credential_lookup() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://127.0.0.1:3900")]),
        )
        .expect("build");
        assert!(backend.skip_signature);
        // A credential lookup would go to IMDS and hang; this fails at once.
        let err = backend
            .pre_signed_urls(
                &[ObjectPath::from("o")],
                SignMethod::Get,
                Duration::from_mins(1),
                &SignOptions::default(),
            )
            .await
            .expect_err("unsigned");
        assert!(matches!(err, Error::Generic(m) if m.contains("without credentials")));
    }

    #[tokio::test]
    async fn an_unsigned_store_builds_bucket_requests_without_a_credential_lookup() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://127.0.0.1:3900/")]),
        )
        .expect("build");
        // A credential lookup would go to IMDS and hang.
        let request = backend
            .bucket_request(Method::HEAD, Bytes::new())
            .await
            .expect("request");
        assert_eq!(request.uri(), "http://127.0.0.1:3900/bkt");
        assert!(request.headers().get(http::header::AUTHORIZATION).is_none());
    }

    #[tokio::test]
    async fn test_regression_builder_error() {
        // Verify that providing an http endpoint now automatically enables allow_http,
        // preventing the "builder error".
        let v = Vars::from_pairs(&[
            ("S3_ENDPOINT", "http://localhost:9000"),
            ("S3_ALLOW_HTTP", "false"),
            ("S3_ACCESS_KEY_ID", "minioadmin"),
            ("S3_SECRET_ACCESS_KEY", "minioadmin"),
            ("S3_REGION", "us-east-1"),
        ]);

        let backend = S3Backend::new(Some("api-proxy"), &v).expect("Failed to create S3Backend");
        let path = ObjectPath::from("audit/test.txt");

        // We expect a connection error (since no MinIO is running at localhost:9000),
        // but NOT a "builder error". A connection error means the request was successfully built.
        let result = backend.store().put(&path, Bytes::from("data").into()).await;

        if let Err(e) = result {
            let err_str = format!("{e}");
            assert!(
                !err_str.contains("builder error"),
                "Should NOT have failed with builder error, got: {err_str}"
            );
        }
    }
}
