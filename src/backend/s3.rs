//! Amazon S3 (and S3-compatible, e.g. `MinIO` / `SeaweedFS`) backend.

use super::{Backend, bucket_exists_from_response, send_signed};
use crate::client::{Error, Result};
use crate::sign::{self, SignMethod, SignOptions};
use bytes::Bytes;
use http::Method;
use object_store::ObjectStore;
use object_store::aws::{AmazonS3, AmazonS3Builder, AwsAuthorizer, AwsCredential};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// S3 backend. Retains the concrete [`AmazonS3`] store so credentials can be
/// read for header-bound signing and `CreateBucket`, alongside the type-erased
/// store used for generic object operations.
pub(crate) struct S3Backend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<AmazonS3>,
    /// When set, requests are sent unsigned (anonymous) and the credential
    /// provider is never consulted. This is what keeps a non-AWS store from
    /// reaching for IMDS (`http://169.254.169.254`); see [`S3Backend::from_env`].
    skip_signature: bool,
}

impl S3Backend {
    /// Build the S3 store from the environment, mirroring the configuration the
    /// rest of the client reads (`S3_REGION`, `S3_ACCESS_KEY_ID`,
    /// `S3_SECRET_ACCESS_KEY`, `S3_SECURE`) on top of
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
    pub(crate) fn from_env(host: Option<&str>) -> Result<Self> {
        let s3_secure = std::env::var("S3_SECURE").unwrap_or_else(|_| "true".into());
        let bucket = host.ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);

        if let Ok(region) = std::env::var("S3_REGION") {
            builder = builder.with_region(region);
        }

        // Honour the crate's own `S3_ENDPOINT` alongside the `AWS_*` names
        // `from_env` already reads, so every endpoint name resolves the store to
        // the same place as `endpoint_and_region`.
        let endpoint = Self::configured_endpoint();
        if let Some(ep) = endpoint.as_deref() {
            builder = builder.with_endpoint(ep);
        }

        if let Ok(access_key_id) = std::env::var("S3_ACCESS_KEY_ID") {
            builder = builder.with_token("").with_access_key_id(access_key_id);
        }

        if let Ok(secret_access_key) = std::env::var("S3_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if s3_secure == "false" {
            builder = builder.with_allow_http(true);
        }

        let skip_signature = Self::explicit_skip_signature()
            .unwrap_or_else(|| endpoint.is_some() && !Self::has_credentials());
        if skip_signature {
            builder = builder.with_skip_signature(true);
        }

        let inner = Arc::new(builder.build()?);
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            skip_signature,
        })
    }

    /// The S3 endpoint explicitly configured in the environment, in the same
    /// precedence `AmazonS3Builder::from_env` uses (`AWS_ENDPOINT_URL_S3` wins),
    /// extended with the crate's own `S3_ENDPOINT`. `None` means "no custom
    /// endpoint" — i.e. real AWS, where the IMDS credential fallback is valid.
    fn configured_endpoint() -> Option<String> {
        [
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT",
            "AWS_ENDPOINT_URL",
            "S3_ENDPOINT",
        ]
        .into_iter()
        .find_map(|key| std::env::var(key).ok().filter(|v| !v.is_empty()))
    }

    /// Whether the environment provides any credential source other than the
    /// instance-metadata (IMDS) fallback: static keys (either `S3_*` or `AWS_*`),
    /// web-identity (IRSA) or container credentials. When none of these is set
    /// against a custom endpoint, IMDS is the only thing left for `object_store`
    /// to try — which is exactly what we want to avoid.
    fn has_credentials() -> bool {
        let set = |key: &str| std::env::var(key).is_ok_and(|v| !v.is_empty());
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
    fn explicit_skip_signature() -> Option<bool> {
        let value = std::env::var("AWS_SKIP_SIGNATURE").ok()?;
        match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "on" | "yes" | "y" => Some(true),
            "0" | "false" | "off" | "no" | "n" => Some(false),
            _ => None,
        }
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

    /// The concrete store as an `object_store` [`Signer`].
    fn signer(&self) -> Arc<dyn Signer> {
        Arc::clone(&self.inner) as Arc<dyn Signer>
    }

    /// Resolve the `(endpoint, region)` the bucket-management requests target.
    ///
    /// Mirrors the env the store was built from: `from_env` uses `S3_REGION`,
    /// then falls back to `AmazonS3Builder::from_env`, which reads `AWS_REGION`
    /// / `AWS_DEFAULT_REGION`. Region drives both the `SigV4` scope and (for
    /// `CreateBucket`) the `LocationConstraint`, so it must match the built
    /// store.
    fn endpoint_and_region() -> (String, String) {
        let region = std::env::var("S3_REGION")
            .or_else(|_| std::env::var("AWS_REGION"))
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .unwrap_or_else(|_| "us-east-1".into());
        let secure = std::env::var("S3_SECURE").map_or(true, |v| v != "false");
        let endpoint = Self::configured_endpoint().unwrap_or_else(|| {
            let scheme = if secure { "https" } else { "http" };
            format!("{scheme}://s3.{region}.amazonaws.com")
        });
        (endpoint, region)
    }
}

#[async_trait::async_trait]
impl Backend for S3Backend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    // The trait ties the return to `&self`; this backend's scheme is a fixed
    // literal, so the lifetime tie is unavoidable here.
    #[allow(clippy::unnecessary_literal_bound)]
    fn scheme(&self) -> &str {
        "s3"
    }

    /// Issues an S3 `CreateBucket` request, signed with `object_store`'s
    /// [`AwsAuthorizer`] (`SigV4`). A `LocationConstraint` body is sent for every
    /// region except `us-east-1`, per the S3 API. Treats
    /// `BucketAlreadyOwnedByYou` / `BucketAlreadyExists` as success.
    async fn create_bucket(&self, client: &reqwest::Client, url: &Url) -> Result<()> {
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;

        let (endpoint, region) = Self::endpoint_and_region();
        // Path-style addressing keeps the request endpoint-agnostic, which is
        // what MinIO / SeaweedFS expect and AWS also accepts.
        let uri = format!("{}/{bucket}", endpoint.trim_end_matches('/'));

        let body = if region == "us-east-1" {
            Bytes::new()
        } else {
            Bytes::from(format!(
                "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
                 <LocationConstraint>{region}</LocationConstraint>\
                 </CreateBucketConfiguration>"
            ))
        };

        let mut request: HttpRequest = http::Request::builder()
            .method(Method::PUT)
            .uri(&uri)
            .body(HttpRequestBody::from(body.clone()))
            .map_err(|e| Error::Generic(format!("Failed to build S3 request: {e}")))?;

        // Reuse object_store's SigV4 implementation — no signing reimplemented.
        // When signing is skipped (anonymous store) the request is sent as-is,
        // which also avoids touching the IMDS credential provider.
        if let Some(credential) = self.credential().await? {
            AwsAuthorizer::new(credential.as_ref(), "s3", &region)
                .try_authorize(&mut request, None)
                .map_err(|e| Error::Generic(format!("Failed to sign S3 request: {e}")))?;
        }

        let response = send_signed(client, request, body).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        let text = response.text().await.unwrap_or_default();
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
    async fn bucket_exists(&self, client: &reqwest::Client, url: &Url) -> Result<bool> {
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::Generic("Missing bucket in S3 URL".into()))?;

        let (endpoint, region) = Self::endpoint_and_region();
        // Path-style addressing, as in create_bucket.
        let uri = format!("{}/{bucket}", endpoint.trim_end_matches('/'));

        let mut request: HttpRequest = http::Request::builder()
            .method(Method::HEAD)
            .uri(&uri)
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build S3 request: {e}")))?;

        if let Some(credential) = self.credential().await? {
            AwsAuthorizer::new(credential.as_ref(), "s3", &region)
                .try_authorize(&mut request, None)
                .map_err(|e| Error::Generic(format!("Failed to sign S3 request: {e}")))?;
        }

        let response = send_signed(client, request, Bytes::new()).await?;
        bucket_exists_from_response(response, "S3").await
    }

    /// Host-only signing via the object store's signer, re-signed with the bound
    /// `Content-Length` / `Content-Type` headers when `options` requests them.
    async fn pre_signed_url(
        &self,
        path: &ObjectPath,
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<String> {
        // `object_store`'s signer reads the raw credential provider, so under
        // `skip_signature` it would hit IMDS. Pre-signing is meaningless without
        // credentials anyway, so fail clearly instead.
        if self.skip_signature {
            return Err(Error::Generic(
                "Cannot pre-sign an S3 URL without credentials (this store runs unsigned)".into(),
            ));
        }

        let base = self
            .signer()
            .signed_url(method.into_http(), path, expires_in)
            .await?;

        if options.is_empty() {
            return Ok(base.into());
        }

        let credential = self.inner.credentials().get_credential().await?;
        let region = sign::s3_region();
        let mut base = base;
        // Drop the host-only query produced by the object store's signer; we
        // generate our own SigV4 query over the bound headers.
        base.set_query(None);
        Ok(sign::presign_s3(
            &base,
            &method.into_http(),
            expires_in,
            &credential,
            &region,
            options.bound_headers(),
        )
        .into())
    }

    async fn pre_signed_urls(
        &self,
        paths: &[ObjectPath],
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<Vec<String>> {
        if self.skip_signature {
            return Err(Error::Generic(
                "Cannot pre-sign S3 URLs without credentials (this store runs unsigned)".into(),
            ));
        }

        let bases = self
            .signer()
            .signed_urls(method.into_http(), paths, expires_in)
            .await?;

        if options.is_empty() {
            return Ok(bases.into_iter().map(Into::into).collect());
        }

        // Resolve credentials once, then re-sign each URL with the bound headers.
        let credential = self.inner.credentials().get_credential().await?;
        let region = sign::s3_region();
        let http_method = method.into_http();

        let mut pre_signed = Vec::with_capacity(bases.len());
        for mut base in bases {
            base.set_query(None);
            pre_signed.push(
                sign::presign_s3(
                    &base,
                    &http_method,
                    expires_in,
                    &credential,
                    &region,
                    options.bound_headers(),
                )
                .into(),
            );
        }
        Ok(pre_signed)
    }
}
