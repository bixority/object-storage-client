//! Amazon S3 (and S3-compatible, e.g. `MinIO` / `SeaweedFS`) backend.

use super::aws_profile;
use super::{
    AwsProfile, Backend, BoundedConnector, Vars, bucket_exists_from_response, default_timeouts,
    error_body, parse_bool, send_signed,
};
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

/// How requests address the bucket. `S3_ADDRESSING_STYLE` sets it; when that is
/// unset, `AWS_VIRTUAL_HOSTED_STYLE_REQUEST` decides, and path is the default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AddressingStyle {
    /// `https://<endpoint>/<bucket>/<key>`: the bucket is in the path.
    Path,
    /// `https://<bucket>.<endpoint>/<key>`: the bucket is in the host.
    Virtual,
}

impl AddressingStyle {
    /// Parses `path` or `virtual`, ignoring case.
    fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "path" => Some(Self::Path),
            "virtual" => Some(Self::Virtual),
            _ => None,
        }
    }

    const fn is_virtual(self) -> bool {
        matches!(self, Self::Virtual)
    }
}

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
    /// The region, resolved once: it signs object and bucket requests alike.
    region: String,
    /// The configured custom endpoint; `None` means AWS.
    endpoint: Option<String>,
    /// How the bucket is addressed, for object and bucket requests alike.
    addressing: AddressingStyle,
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
    /// With a `profile`, credentials come from that AWS profile only (SSO and
    /// assumed roles and `credential_process` helpers included), whatever static
    /// keys the environment holds; `AWS_PROFILE` is never read. The profile is
    /// resolved lazily on the first signed request and never falls back to
    /// IMDS. The region is `S3_REGION`, then `AWS_REGION` / `AWS_DEFAULT_REGION`,
    /// then the profile's `region`, else `us-east-1`. The profile's S3
    /// `endpoint_url` (the `services` entry, else the global one) comes after
    /// the endpoint variables, unless `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` or
    /// the profile's `ignore_configured_endpoint_urls` is true. A global
    /// `AWS_ENDPOINT_URL` is refused with a profile, because the SDK would send
    /// the profile's SSO and STS tokens there.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket (URL host) is missing, a profile is
    /// combined with `AWS_ENDPOINT_URL`, the profile's files cannot be read, or
    /// the store fails to build.
    pub fn new(host: Option<&str>, vars: &Vars, profile: Option<&AwsProfile>) -> Result<Self> {
        let get_var = |k: &str| vars.get(k);
        let s3_allow_http = get_var("S3_ALLOW_HTTP").unwrap_or_else(|| "false".into());
        let bucket = host.ok_or_else(|| Error::InvalidInput("Missing bucket in S3 URL".into()))?;
        let addressing_override = Self::addressing_override(vars)?;
        // One boolean decides both this check and whether the profile signs. An
        // explicit profile is the only credential source.
        let uses_profile = profile.is_some();
        if uses_profile && get_var("AWS_ENDPOINT_URL").is_some_and(|v| !v.is_empty()) {
            // The SDK's SSO and STS clients honour the global endpoint too, so
            // their tokens would go to the storage endpoint.
            return Err(Error::InvalidInput(
                "AWS_ENDPOINT_URL also redirects the AWS profile's SSO and STS requests and \
                 their tokens; set the S3 endpoint with S3_ENDPOINT or AWS_ENDPOINT_URL_S3"
                    .into(),
            ));
        }
        let mut builder = match vars {
            Vars::Env => AmazonS3Builder::from_env(),
            Vars::Map(_) => vars
                .config_entries::<AmazonS3ConfigKey>("AWS_")
                .into_iter()
                .fold(AmazonS3Builder::new(), |b, (k, v)| b.with_config(k, v)),
        }
        .with_bucket_name(bucket);
        for (key, value) in default_timeouts(vars, "AWS_") {
            builder = builder.with_config(AmazonS3ConfigKey::Client(key), value);
        }

        // The profile files are read once, and only when a setting is missing
        // from the variables: the region or the endpoint.
        let variable_region = Self::variable_region(vars);
        let variable_endpoint = Self::configured_endpoint(vars);
        let settings = match profile {
            Some(profile) if variable_region.is_none() || variable_endpoint.is_none() => {
                Some(profile.settings()?)
            }
            _ => None,
        };
        let region = variable_region
            .or_else(|| settings.and_then(|s| s.region.clone()))
            .unwrap_or_else(|| "us-east-1".into());
        builder = builder.with_region(&region);

        // Honour the crate's own `S3_ENDPOINT` alongside the `AWS_*` names, then
        // the profile's S3 endpoint. `object_store` prefers the `S3Endpoint` key
        // over every generic endpoint key, so the store and the bucket requests
        // use one server.
        let endpoint = variable_endpoint.or_else(|| settings.and_then(|s| s.s3_endpoint.clone()));
        if let Some(ep) = endpoint.as_deref() {
            builder = builder.with_config(AmazonS3ConfigKey::S3Endpoint, ep);
        }
        // The store reads the same setting for object calls, so writing the
        // resolved style back keeps bucket and object requests in agreement.
        let addressing = addressing_override.unwrap_or_else(|| {
            let virtual_hosted = builder
                .get_config_value(&AmazonS3ConfigKey::VirtualHostedStyleRequest)
                .and_then(|v| parse_bool(&v))
                .unwrap_or(false);
            if virtual_hosted {
                AddressingStyle::Virtual
            } else {
                AddressingStyle::Path
            }
        });
        builder = builder.with_config(
            AmazonS3ConfigKey::VirtualHostedStyleRequest,
            addressing.is_virtual().to_string(),
        );

        if let Some(access_key_id) = get_var("S3_ACCESS_KEY_ID") {
            builder = builder.with_token("").with_access_key_id(access_key_id);
        }

        if let Some(secret_access_key) = get_var("S3_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_access_key);
        }

        if let Some(profile) = profile {
            // The registry's one provider: every bucket shares its cache.
            builder = builder.with_credentials(profile.credentials());
        }

        if s3_allow_http == "true"
            || endpoint
                .as_deref()
                .is_some_and(|ep| ep.starts_with("http://"))
        {
            builder = builder.with_allow_http(true);
        }

        let skip_signature = Self::explicit_skip_signature(vars)
            .unwrap_or_else(|| endpoint.is_some() && !Self::has_credentials(vars, uses_profile));
        if skip_signature {
            builder = builder.with_skip_signature(true);
        }

        let inner = Arc::new(
            builder
                .with_http_connector(BoundedConnector::default())
                .build()?,
        );
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            skip_signature,
            bucket: bucket.to_string(),
            region,
            endpoint,
            addressing,
        })
    }

    /// The addressing style `S3_ADDRESSING_STYLE` names, if it is set. Empty is
    /// unset. Any other value is an error, so a typo cannot silently fall back
    /// to the default.
    fn addressing_override(vars: &Vars) -> Result<Option<AddressingStyle>> {
        let Some(value) = vars.get("S3_ADDRESSING_STYLE").filter(|v| !v.is_empty()) else {
            return Ok(None);
        };
        AddressingStyle::parse(&value).map(Some).ok_or_else(|| {
            Error::InvalidInput(format!(
                "S3_ADDRESSING_STYLE must be \"path\" or \"virtual\", not \"{value}\""
            ))
        })
    }

    /// The S3 endpoint explicitly configured in the variables: the crate's
    /// own `S3_ENDPOINT`, then `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT` and
    /// `AWS_ENDPOINT_URL`; empty values are ignored. `None` means "no custom
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

    /// The region a variable names: `S3_REGION`, `AWS_REGION`, then
    /// `AWS_DEFAULT_REGION`; empty values are ignored.
    fn variable_region(vars: &Vars) -> Option<String> {
        vars.get("S3_REGION")
            .filter(|v| !v.is_empty())
            .or_else(|| aws_profile::env_region(vars))
    }

    /// Whether the environment provides any credential source other than the
    /// instance-metadata (IMDS) fallback: a named profile, static keys (either
    /// `S3_*` or `AWS_*`), web identity (IRSA) or container credentials. It
    /// mirrors `object_store` 0.14's provider order, which uses web identity
    /// only with `AWS_ROLE_ARN` and pod identity only with
    /// `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`; any other combination ends at
    /// IMDS. When none of these is set against a guest endpoint, IMDS is the
    /// only thing left for `object_store` to try — which is exactly what we
    /// want to avoid.
    fn has_credentials(vars: &Vars, uses_profile: bool) -> bool {
        let set = |key: &str| vars.get(key).is_some_and(|v| !v.is_empty());
        uses_profile
            || set("S3_ACCESS_KEY_ID")
            || set("AWS_ACCESS_KEY_ID")
            || (set("AWS_WEB_IDENTITY_TOKEN_FILE") && set("AWS_ROLE_ARN"))
            || set("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            || (set("AWS_CONTAINER_CREDENTIALS_FULL_URI")
                && set("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE"))
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

    /// The `(endpoint, region)` the bucket-management requests target, both
    /// resolved once in [`S3Backend::new`]. The region drives the `SigV4` scope
    /// and the `LocationConstraint` of `CreateBucket`. The AWS default endpoint
    /// is always https, whatever `S3_ALLOW_HTTP` says: that setting is for
    /// custom endpoints. A custom endpoint is used as configured (an `http://`
    /// endpoint enables plain HTTP for the store, see [`S3Backend::new`]).
    fn endpoint_and_region(&self) -> (String, String) {
        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region));
        (endpoint, self.region.clone())
    }

    /// A bucket-level request (`/<bucket>`) carrying `body`, `SigV4`-signed
    /// unless signing is skipped. The credential comes from
    /// [`S3Backend::credential`], so `skip_signature` never reaches IMDS.
    async fn bucket_request(&self, method: Method, body: Bytes) -> Result<HttpRequest> {
        let (endpoint, region) = self.endpoint_and_region();
        let endpoint = endpoint.trim_end_matches('/');
        let uri = match (self.addressing, &self.endpoint) {
            // The store uses a custom endpoint as is in this mode.
            (AddressingStyle::Virtual, Some(_)) => format!("{endpoint}/"),
            (AddressingStyle::Virtual, None) => {
                format!("https://{}.s3.{region}.amazonaws.com/", self.bucket)
            }
            // Path-style addressing keeps the request endpoint-agnostic, which
            // is what MinIO / SeaweedFS expect and AWS also accepts.
            (AddressingStyle::Path, _) => format!("{endpoint}/{}", self.bucket),
        };
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

    #[cfg(test)]
    fn s3_signing(&self) -> Option<(*const (), String)> {
        Some((
            Arc::as_ptr(self.inner.credentials()).cast(),
            self.region.clone(),
        ))
    }

    fn scheme(&self) -> &'static str {
        "s3"
    }

    /// Issues an S3 `CreateBucket` request, signed with `object_store`'s
    /// [`AwsAuthorizer`] (`SigV4`). A `LocationConstraint` body is sent for every
    /// region except `us-east-1`, per the S3 API. Succeeds for a `2xx` response
    /// and for a `409` whose error document carries `BucketAlreadyOwnedByYou`
    /// (the bucket is already ours, so the call is idempotent). Every other
    /// response is an error, including `BucketAlreadyExists`: another account
    /// owns that global name.
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
        if status.as_u16() == 409 && text.contains("<Code>BucketAlreadyOwnedByYou</Code>") {
            return Ok(());
        }
        if text.contains("<Code>BucketAlreadyExists</Code>") {
            return Err(Error::Generic(format!(
                "S3 bucket `{}` already exists and belongs to another account, \
                 or these credentials cannot use it (BucketAlreadyExists)",
                self.bucket
            )));
        }
        Err(Error::Generic(format!(
            "S3 CreateBucket failed (HTTP {status}): {text}"
        )))
    }

    /// Issues an S3 `HeadBucket` request (`HEAD /<bucket>`), signed with
    /// `object_store`'s [`AwsAuthorizer`] (`SigV4`). This is a metadata-only
    /// probe that avoids the `ListBucket` payload: HTTP `200` means the bucket
    /// exists and `404` that it does not. A `403` (also the answer to a wrong
    /// key or signature) or `401` is an error: existence is unknown.
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

        // The bound header values are signed, so they must be valid header values.
        if let Some(ty) = &options.content_type
            && http::HeaderValue::from_str(ty).is_err()
        {
            return Err(Error::InvalidInput(
                "content_type is not a valid HTTP header value".into(),
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
        let region = &self.region;
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
                    region,
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
    use crate::backend::stub::{Reply, Request, serve};
    use object_store::ObjectStoreExt;

    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client")
    }

    /// A backend for bucket `bkt` that talks to the stub at `base`.
    fn stubbed(base: &str) -> S3Backend {
        S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[
                ("S3_ENDPOINT", base),
                ("S3_ALLOW_HTTP", "true"),
                ("S3_REGION", "us-east-1"),
                ("S3_ACCESS_KEY_ID", "k"),
                ("S3_SECRET_ACCESS_KEY", "s"),
            ]),
            None,
        )
        .expect("build")
    }

    fn error_document(code: &str, message: &str) -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
             <Message>{message}</Message><BucketName>bkt</BucketName>\
             <RequestId>r</RequestId></Error>"
        )
    }

    async fn create(status: u16, body: String) -> (Result<()>, Vec<Request>) {
        let stub = serve(vec![Reply::new(status, body)]).await;
        let url = Url::parse("s3://bkt").expect("url");
        let result = stubbed(&stub.base).create_bucket(&client(), &url).await;
        (result, stub.requests())
    }

    #[tokio::test]
    async fn create_bucket_accepts_a_bucket_we_own() {
        let doc = error_document("BucketAlreadyOwnedByYou", "Your previous request");
        assert!(create(409, doc).await.0.is_ok());
    }

    #[tokio::test]
    async fn create_bucket_rejects_a_name_owned_by_someone_else() {
        let doc = error_document(
            "BucketAlreadyExists",
            "The requested bucket name is not available",
        );
        let err = create(409, doc).await.0.expect_err("foreign bucket");
        assert!(err.to_string().contains("BucketAlreadyExists"), "{err}");
    }

    #[tokio::test]
    async fn create_bucket_ignores_the_name_outside_a_code_element() {
        let doc = error_document("InternalError", "BucketAlreadyOwnedByYou");
        assert!(create(500, doc).await.0.is_err());
    }

    #[tokio::test]
    async fn create_bucket_accepts_200() {
        let (result, requests) = create(200, String::new()).await;
        assert!(result.is_ok());
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].line, "PUT /bkt HTTP/1.1");
        assert_eq!(requests[0].body.len(), 0);
        assert!(
            requests[0]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: aws4-hmac-sha256"),
            "{}",
            requests[0].headers
        );
    }

    #[test]
    fn test_endpoint_and_region_scheme_logic() {
        // The AWS default endpoint is https even when plain HTTP is allowed,
        // so bucket calls never send credentials in the clear to AWS.
        let http = S3Backend::new(
            Some("b"),
            &Vars::from_pairs(&[("S3_ALLOW_HTTP", "true")]),
            None,
        )
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
            None,
        )
        .expect("build");
        assert_eq!(custom.endpoint_and_region().0, "http://127.0.0.1:9000");

        let https = S3Backend::new(
            Some("b"),
            &Vars::from_pairs(&[("S3_ALLOW_HTTP", "false")]),
            None,
        )
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
            None,
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
            None,
        )
        .expect("build");
        assert!(backend.skip_signature);
    }

    #[tokio::test]
    async fn an_unsigned_store_refuses_to_pre_sign_without_a_credential_lookup() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://127.0.0.1:3900")]),
            None,
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
            None,
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
    async fn an_http_endpoint_enables_plain_http_even_when_allow_http_is_false() {
        let stub = serve(vec![Reply::new(200, "").header("ETag", "\"abc\"")]).await;
        let backend = S3Backend::new(
            Some("api-proxy"),
            &Vars::from_pairs(&[
                ("S3_ENDPOINT", stub.base.as_str()),
                ("S3_ALLOW_HTTP", "false"),
                ("S3_ACCESS_KEY_ID", "minioadmin"),
                ("S3_SECRET_ACCESS_KEY", "minioadmin"),
                ("S3_REGION", "us-east-1"),
            ]),
            None,
        )
        .expect("build");
        let path = ObjectPath::from("audit/test.txt");
        backend
            .store()
            .put(&path, Bytes::from("data").into())
            .await
            .expect("plain HTTP put");

        let requests = stub.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].line, "PUT /api-proxy/audit/test.txt HTTP/1.1");
        assert!(
            requests[0]
                .headers
                .to_ascii_lowercase()
                .contains("credential=minioadmin/"),
            "{}",
            requests[0].headers
        );
        assert_eq!(requests[0].body, b"data");
    }

    const DEV: &str = "[profile dev]\nregion = eu-west-3\naws_access_key_id = AKIDDEV\n\
                       aws_secret_access_key = devsecret\naws_session_token = tok\n";

    /// Variables that name profile files written to a temporary directory
    /// (kept alive by the returned guard), plus `extra`.
    fn profiled(
        config: &str,
        credentials: Option<&str>,
        extra: &[(&str, &str)],
    ) -> (tempfile::TempDir, Vars) {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut map: std::collections::HashMap<String, String> = extra
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let config_path = dir.path().join("config");
        std::fs::write(&config_path, config).expect("write config");
        map.insert("AWS_CONFIG_FILE".into(), config_path.display().to_string());
        if let Some(credentials) = credentials {
            let path = dir.path().join("credentials");
            std::fs::write(&path, credentials).expect("write credentials");
            map.insert(
                "AWS_SHARED_CREDENTIALS_FILE".into(),
                path.display().to_string(),
            );
        }
        (dir, Vars::Map(Arc::new(map)))
    }

    /// A backend for bucket `bkt` signing through the AWS profile `profile`.
    fn with_aws_profile(vars: &Vars, profile: &str) -> Result<S3Backend> {
        S3Backend::new(
            Some("bkt"),
            vars,
            Some(&AwsProfile::new(profile.to_owned(), vars)),
        )
    }

    /// A backend for profile `profile` of `DEV`, with `extra` variables.
    fn dev_backend(profile: &str, extra: &[(&str, &str)]) -> (tempfile::TempDir, S3Backend) {
        let (dir, vars) = profiled(DEV, None, extra);
        let backend = with_aws_profile(&vars, profile).expect("build");
        (dir, backend)
    }

    #[tokio::test]
    async fn an_injected_map_reads_only_the_files_it_names() {
        let (_dir, vars) = profiled(
            "[profile dev]\nregion = eu-west-3\naws_access_key_id = AKIDFILE\n\
             aws_secret_access_key = s\n",
            None,
            &[("S3_ENDPOINT", "http://127.0.0.1:1")],
        );
        let backend = with_aws_profile(&vars, "dev").expect("build");
        assert_eq!(backend.endpoint_and_region().1, "eu-west-3");
        let credential = backend.credential().await.expect("credential");
        assert_eq!(credential.expect("signed").key_id, "AKIDFILE");
    }

    #[tokio::test]
    async fn an_injected_map_without_profile_files_reads_none() {
        let backend = with_aws_profile(
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://127.0.0.1:1")]),
            "dev",
        )
        .expect("build");
        // Signing asks the profile, which no injected file defines.
        assert!(!backend.skip_signature);
        let err = backend.credential().await.expect_err("no files");
        let text = err.to_string();
        assert!(
            text.contains("AWS profile `dev` credentials unavailable"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_profile_that_the_files_do_not_define_is_named_in_the_error() {
        let (_dir, vars) = profiled(
            "[profile other]\nregion = eu-west-3\n",
            None,
            &[("S3_ENDPOINT", "http://127.0.0.1:1")],
        );
        let backend = with_aws_profile(&vars, "missing").expect("build");
        let err = backend.credential().await.expect_err("missing profile");
        assert!(
            err.to_string()
                .contains("profile `missing` was not defined"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn an_explicit_profile_beats_static_keys() {
        for key in ["S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"] {
            let (_dir, backend) = dev_backend(
                "dev",
                &[
                    (key, "AKIDSTATIC"),
                    ("S3_SECRET_ACCESS_KEY", "x"),
                    ("AWS_SECRET_ACCESS_KEY", "x"),
                ],
            );
            let credential = backend.credential().await.expect("credential");
            assert_eq!(credential.expect("signed").key_id, "AKIDDEV", "{key}");
            let url = signed(&backend).await;
            assert!(url.as_str().contains("AKIDDEV"), "{key}: {url}");
        }
    }

    fn with_profile(pairs: &[(&str, &str)]) -> Result<S3Backend> {
        with_aws_profile(&Vars::from_pairs(pairs), "p")
    }

    #[test]
    fn a_profile_refuses_the_global_endpoint_variable() {
        for endpoint in ["http://127.0.0.1:3900", "https://127.0.0.1:3900"] {
            let Err(Error::InvalidInput(message)) = with_profile(&[("AWS_ENDPOINT_URL", endpoint)])
            else {
                panic!("expected InvalidInput for {endpoint}");
            };
            assert!(message.contains("AWS_ENDPOINT_URL_S3"), "{message}");
            assert!(message.contains("S3_ENDPOINT"), "{message}");
            assert!(!message.contains("127.0.0.1"), "{message}");
        }
    }

    #[test]
    fn a_profile_with_static_keys_still_refuses_the_global_endpoint() {
        let result = with_profile(&[
            ("AWS_ENDPOINT_URL", "http://127.0.0.1:3900"),
            ("AWS_ACCESS_KEY_ID", "AKIDSTATIC"),
            ("AWS_SECRET_ACCESS_KEY", "x"),
        ]);
        assert!(matches!(result, Err(Error::InvalidInput(_))));
    }

    #[test]
    fn a_profile_accepts_the_s3_specific_endpoints() {
        // AWS_REGION keeps the profile files unread.
        for key in ["AWS_ENDPOINT_URL_S3", "S3_ENDPOINT"] {
            with_profile(&[(key, "http://127.0.0.1:3900"), ("AWS_REGION", "eu-north-1")])
                .unwrap_or_else(|e| panic!("{key}: {e}"));
        }
    }

    #[test]
    fn an_empty_global_endpoint_counts_as_unset() {
        with_profile(&[("AWS_ENDPOINT_URL", ""), ("AWS_REGION", "eu-north-1")]).expect("build");
    }

    #[test]
    fn without_a_profile_the_global_endpoint_still_works() {
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[
                ("AWS_ENDPOINT_URL", "http://127.0.0.1:3900"),
                ("AWS_ACCESS_KEY_ID", "AKID"),
                ("AWS_SECRET_ACCESS_KEY", "x"),
            ]),
            None,
        )
        .expect("build");
        assert_eq!(backend.endpoint_and_region().0, "http://127.0.0.1:3900");
    }

    /// A backend with static keys and no profile, built from `pairs`.
    fn keyed(pairs: &[(&str, &str)]) -> S3Backend {
        let mut all = vec![("S3_ACCESS_KEY_ID", "k"), ("S3_SECRET_ACCESS_KEY", "s")];
        all.extend_from_slice(pairs);
        S3Backend::new(Some("bkt"), &Vars::from_pairs(&all), None).expect("build")
    }

    async fn signed(backend: &S3Backend) -> Url {
        backend
            .inner
            .signed_url(Method::GET, &ObjectPath::from("o"), Duration::from_secs(60))
            .await
            .expect("sign")
    }

    #[tokio::test]
    async fn region_order_and_empty_values() {
        let rows: [(&[(&str, &str)], &str); 5] = [
            (&[("S3_REGION", "s-1"), ("AWS_REGION", "a-1")], "s-1"),
            (
                &[("AWS_REGION", "a-1"), ("AWS_DEFAULT_REGION", "d-1")],
                "a-1",
            ),
            (&[("AWS_DEFAULT_REGION", "d-1")], "d-1"),
            (
                &[
                    ("S3_REGION", ""),
                    ("AWS_REGION", ""),
                    ("AWS_DEFAULT_REGION", "d-1"),
                ],
                "d-1",
            ),
            (&[], "us-east-1"),
        ];
        for (pairs, want) in rows {
            let backend = keyed(pairs);
            assert_eq!(backend.endpoint_and_region().1, want, "{pairs:?}");
            let url = signed(&backend).await;
            assert!(
                url.as_str().contains(&format!("%2F{want}%2Fs3%2F")),
                "{pairs:?}: {url}"
            );
        }
    }

    #[tokio::test]
    async fn the_store_and_bucket_requests_use_one_endpoint() {
        let rows = [
            (
                [
                    ("S3_ENDPOINT", "http://127.0.0.1:1"),
                    ("AWS_ENDPOINT_URL_S3", "http://127.0.0.1:2"),
                ],
                "127.0.0.1:1",
            ),
            (
                [
                    ("AWS_ENDPOINT", "http://127.0.0.1:3"),
                    ("AWS_ENDPOINT_URL", "http://127.0.0.1:4"),
                ],
                "127.0.0.1:3",
            ),
        ];
        for (pairs, host) in rows {
            let backend = keyed(&pairs);
            let url = signed(&backend).await;
            assert_eq!(url.authority(), host, "{pairs:?}");
            let request = backend
                .bucket_request(Method::HEAD, Bytes::new())
                .await
                .expect("request");
            assert_eq!(
                request.uri().authority().map(http::uri::Authority::as_str),
                Some(host)
            );
        }
    }

    #[tokio::test]
    async fn virtual_hosted_bucket_requests_use_the_endpoint_as_is() {
        let backend = keyed(&[
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
            ("S3_ENDPOINT", "http://127.0.0.1:1"),
        ]);
        let request = backend
            .bucket_request(Method::PUT, Bytes::new())
            .await
            .expect("request");
        assert_eq!(request.uri(), "http://127.0.0.1:1/");

        let backend = keyed(&[
            ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
            ("AWS_REGION", "eu-west-1"),
        ]);
        let request = backend
            .bucket_request(Method::PUT, Bytes::new())
            .await
            .expect("request");
        assert_eq!(request.uri(), "https://bkt.s3.eu-west-1.amazonaws.com/");
    }

    /// The bucket-level URI a store with `pairs` sends to, against a custom
    /// endpoint.
    async fn bucket_uri(pairs: &[(&str, &str)]) -> String {
        let mut all = vec![("S3_ENDPOINT", "http://127.0.0.1:1")];
        all.extend_from_slice(pairs);
        let request = keyed(&all)
            .bucket_request(Method::PUT, Bytes::new())
            .await
            .expect("request");
        request.uri().to_string()
    }

    #[tokio::test]
    async fn addressing_style_defaults_to_path() {
        assert_eq!(bucket_uri(&[]).await, "http://127.0.0.1:1/bkt");
    }

    #[tokio::test]
    async fn s3_addressing_style_selects_the_style_case_insensitively() {
        assert_eq!(
            bucket_uri(&[("S3_ADDRESSING_STYLE", "virtual")]).await,
            "http://127.0.0.1:1/"
        );
        assert_eq!(
            bucket_uri(&[("S3_ADDRESSING_STYLE", "PATH")]).await,
            "http://127.0.0.1:1/bkt"
        );
    }

    #[tokio::test]
    async fn s3_addressing_style_overrides_virtual_hosted_style_request() {
        assert_eq!(
            bucket_uri(&[
                ("S3_ADDRESSING_STYLE", "path"),
                ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true"),
            ])
            .await,
            "http://127.0.0.1:1/bkt"
        );
        assert_eq!(
            bucket_uri(&[
                ("S3_ADDRESSING_STYLE", "virtual"),
                ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "false"),
            ])
            .await,
            "http://127.0.0.1:1/"
        );
    }

    #[tokio::test]
    async fn empty_s3_addressing_style_is_unset() {
        assert_eq!(
            bucket_uri(&[
                ("S3_ADDRESSING_STYLE", ""),
                ("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true")
            ])
            .await,
            "http://127.0.0.1:1/"
        );
    }

    #[tokio::test]
    async fn object_requests_follow_s3_addressing_style() {
        let path = signed(&keyed(&[("S3_ENDPOINT", "http://127.0.0.1:1")])).await;
        assert_eq!(path.path(), "/bkt/o");

        let virtual_hosted = signed(&keyed(&[
            ("S3_ENDPOINT", "http://127.0.0.1:1"),
            ("S3_ADDRESSING_STYLE", "virtual"),
        ]))
        .await;
        // A custom endpoint is used as is in this mode: the bucket is not in the
        // path, and the endpoint is expected to be scoped to it.
        assert_eq!(virtual_hosted.host_str(), Some("127.0.0.1"));
        assert_eq!(virtual_hosted.path(), "/o");
    }

    #[test]
    fn unknown_s3_addressing_style_is_rejected() {
        let all = [
            ("S3_ACCESS_KEY_ID", "k"),
            ("S3_SECRET_ACCESS_KEY", "s"),
            ("S3_ADDRESSING_STYLE", "hostname"),
        ];
        let err = S3Backend::new(Some("bkt"), &Vars::from_pairs(&all), None)
            .err()
            .expect("rejected");
        assert!(
            matches!(&err, Error::InvalidInput(msg) if msg.contains("S3_ADDRESSING_STYLE")),
            "{err:?}"
        );
    }

    /// Whether a store with `pairs` and a custom endpoint runs unsigned.
    fn unsigned(pairs: &[(&str, &str)]) -> bool {
        let mut all = vec![("S3_ENDPOINT", "http://127.0.0.1:1")];
        all.extend_from_slice(pairs);
        S3Backend::new(Some("bkt"), &Vars::from_pairs(&all), None)
            .expect("build")
            .skip_signature
    }

    #[test]
    fn half_a_web_identity_pair_is_not_a_credential_source() {
        let token = ("AWS_WEB_IDENTITY_TOKEN_FILE", "/nonexistent");
        assert!(unsigned(&[token]));
        assert!(!unsigned(&[
            token,
            ("AWS_ROLE_ARN", "arn:aws:iam::1:role/r")
        ]));
    }

    #[test]
    fn half_a_pod_identity_pair_is_not_a_credential_source() {
        let uri = ("AWS_CONTAINER_CREDENTIALS_FULL_URI", "http://127.0.0.1:2/c");
        assert!(unsigned(&[uri]));
        assert!(!unsigned(&[
            uri,
            ("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", "/nonexistent")
        ]));
    }

    #[test]
    fn the_relative_container_uri_alone_is_a_credential_source() {
        assert!(!unsigned(&[(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "/c"
        )]));
    }

    /// A profile, the variables, and the region that must result.
    type Row<'a> = (&'a str, &'a [(&'a str, &'a str)], &'a str);

    #[tokio::test]
    async fn region_order_with_a_profile_region() {
        let config = format!(
            "{DEV}[profile chained]\nsource_profile = dev\naws_access_key_id = AKIDCH\n\
             aws_secret_access_key = s\n"
        );
        let rows: [Row<'_>; 6] = [
            ("dev", &[], "eu-west-3"),
            ("dev", &[("AWS_DEFAULT_REGION", "d-1")], "d-1"),
            (
                "dev",
                &[("AWS_REGION", "a-1"), ("AWS_DEFAULT_REGION", "d-1")],
                "a-1",
            ),
            ("dev", &[("S3_REGION", "s-1"), ("AWS_REGION", "a-1")], "s-1"),
            ("dev", &[("AWS_REGION", "")], "eu-west-3"),
            ("chained", &[], "eu-west-3"),
        ];
        for (profile, pairs, want) in rows {
            let (_dir, vars) = profiled(&config, None, pairs);
            let backend = with_aws_profile(&vars, profile).expect("build");
            assert_eq!(backend.endpoint_and_region().1, want, "{profile} {pairs:?}");
            let url = signed(&backend).await;
            assert!(
                url.as_str().contains(&format!("%2F{want}%2Fs3%2F")),
                "{profile} {pairs:?}: {url}"
            );
        }
    }

    #[tokio::test]
    async fn profile_keys_come_from_either_file() {
        let in_credentials = "[dev]\naws_access_key_id = AKIDDEV\naws_secret_access_key = s\n";
        let in_config = "[profile dev]\naws_access_key_id = AKIDDEV\naws_secret_access_key = s\n";
        for (config, credentials) in [
            ("[profile other]\n", Some(in_credentials)),
            (in_config, None),
        ] {
            let (_dir, vars) = profiled(config, credentials, &[]);
            let backend = with_aws_profile(&vars, "dev").expect("build");
            let credential = backend.credential().await.expect("credential");
            assert_eq!(credential.expect("signed").key_id, "AKIDDEV", "{config}");
        }
    }

    #[tokio::test]
    async fn pre_signed_urls_carry_the_profile_session_token() {
        let (_dir, backend) = dev_backend("dev", &[]);
        let bound = SignOptions {
            content_length: Some(5),
            content_type: Some("text/plain".into()),
        };
        for options in [SignOptions::default(), bound] {
            let urls = backend
                .pre_signed_urls(
                    &[ObjectPath::from("o")],
                    SignMethod::Get,
                    Duration::from_mins(1),
                    &options,
                )
                .await
                .expect("sign");
            let url = &urls[0];
            assert!(url.contains("X-Amz-Credential=AKIDDEV%2F"), "{url}");
            assert!(url.contains("%2Feu-west-3%2Fs3%2Faws4_request"), "{url}");
            assert!(url.contains("X-Amz-Security-Token=tok"), "{url}");
        }
    }

    #[tokio::test]
    async fn a_content_type_that_is_not_a_header_value_is_invalid_input() {
        let (_dir, backend) = dev_backend("dev", &[]);
        let options = SignOptions {
            content_length: None,
            content_type: Some("text/plain\r\nx-evil: 1".into()),
        };
        let err = backend
            .pre_signed_urls(
                &[ObjectPath::from("o")],
                SignMethod::Put,
                Duration::from_mins(1),
                &options,
            )
            .await
            .expect_err("invalid header value");
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
    }

    #[tokio::test]
    async fn bucket_requests_are_signed_with_the_profile() {
        let (_dir, backend) = dev_backend("dev", &[("S3_ENDPOINT", "http://127.0.0.1:1")]);
        let request = backend
            .bucket_request(Method::HEAD, Bytes::new())
            .await
            .expect("request");
        let header = |name: &str| {
            request
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_owned()
        };
        let authorization = header("authorization");
        assert!(
            authorization.contains("Credential=AKIDDEV/"),
            "{authorization}"
        );
        assert!(
            authorization.contains("/eu-west-3/s3/aws4_request"),
            "{authorization}"
        );
        assert_eq!(header("x-amz-security-token"), "tok");
    }

    #[tokio::test]
    async fn create_bucket_uses_the_profile_region() {
        let stub = serve(vec![Reply::new(200, "")]).await;
        let (_dir, backend) = dev_backend("dev", &[("S3_ENDPOINT", stub.base.as_str())]);
        let url = Url::parse("s3://bkt").expect("url");
        backend
            .create_bucket(&client(), &url)
            .await
            .expect("create");
        let requests = stub.requests();
        assert_eq!(requests.len(), 1);
        let body = String::from_utf8_lossy(&requests[0].body);
        assert!(
            body.contains("<LocationConstraint>eu-west-3</LocationConstraint>"),
            "{body}"
        );
    }

    const ENDPOINT_PROFILE: &str = "[profile p]\nendpoint_url = http://127.0.0.1:9\n\
                                    region = eu-west-3\naws_access_key_id = AKIDP\n\
                                    aws_secret_access_key = s\n";

    fn endpoint_backend(extra: &[(&str, &str)]) -> (tempfile::TempDir, S3Backend) {
        let (dir, vars) = profiled(ENDPOINT_PROFILE, None, extra);
        let backend = with_aws_profile(&vars, "p").expect("build");
        (dir, backend)
    }

    #[tokio::test]
    async fn a_profile_endpoint_is_used_for_s3() {
        let (_dir, backend) = endpoint_backend(&[]);
        assert_eq!(backend.endpoint_and_region().0, "http://127.0.0.1:9");
        // The http:// endpoint enables plain HTTP in the store, which signs for it.
        let url = signed(&backend).await;
        assert!(url.as_str().starts_with("http://127.0.0.1:9/"), "{url}");
    }

    #[tokio::test]
    async fn s3_endpoint_beats_the_profile_endpoint() {
        let (_dir, backend) = endpoint_backend(&[("S3_ENDPOINT", "http://127.0.0.1:8")]);
        assert_eq!(backend.endpoint_and_region().0, "http://127.0.0.1:8");
        let url = signed(&backend).await;
        assert!(url.as_str().starts_with("http://127.0.0.1:8/"), "{url}");
    }

    #[tokio::test]
    async fn ignore_configured_endpoint_urls_drops_the_profile_endpoint() {
        let (_dir, backend) = endpoint_backend(&[("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")]);
        assert_eq!(
            backend.endpoint_and_region().0,
            "https://s3.eu-west-3.amazonaws.com"
        );
    }

    /// A store for bucket `bkt` on a stub at `base`, unsigned, plus `extra`.
    fn stub_store(base: &str, extra: &[(&str, &str)]) -> Arc<dyn ObjectStore> {
        let pairs = [("S3_ENDPOINT", base), ("AWS_SKIP_SIGNATURE", "true")];
        let vars = Vars::from_pairs(&[&pairs[..], extra].concat());
        Arc::clone(
            S3Backend::new(Some("bkt"), &vars, None)
                .expect("build")
                .store(),
        )
    }

    /// Real time, not a paused clock: the stub is a socket, and a paused clock
    /// fires timers while the bytes are still in flight.
    #[tokio::test]
    async fn a_slow_body_is_not_cut_off() {
        // Each gap (300 ms) is shorter than the read timeout (1 s), and the
        // whole body takes longer than the timeout: only the gap counts.
        let base = crate::backend::stub::serve_slow(6, 6, Duration::from_millis(300)).await;
        let store = stub_store(&base, &[("AWS_READ_TIMEOUT", "1s")]);
        let got = store.get(&"o".into()).await.expect("get").bytes().await;
        assert_eq!(got.expect("body").len(), 6 * crate::backend::stub::CHUNK);
    }

    #[tokio::test]
    async fn a_stalled_body_fails_after_the_read_timeout() {
        // No body bytes, then silence: the read timeout (1 s) must end the read.
        let base = crate::backend::stub::serve_slow(1, 0, Duration::ZERO).await;
        let store = stub_store(&base, &[("AWS_READ_TIMEOUT", "1s")]);
        let start = std::time::Instant::now();
        let outcome = store.get(&"o".into()).await.expect("headers").bytes().await;
        outcome.expect_err("stalled");
        assert!(
            start.elapsed() >= Duration::from_secs(1),
            "{:?}",
            start.elapsed()
        );
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{:?}",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn a_read_timeout_set_by_the_user_wins() {
        let base = crate::backend::stub::serve_slow(1, 0, Duration::ZERO).await;
        let store = stub_store(&base, &[("AWS_READ_TIMEOUT", "2s")]);
        let start = std::time::Instant::now();
        let outcome = store.get(&"o".into()).await.expect("headers").bytes().await;
        outcome.expect_err("stalled");
        // The default is five minutes: failing within seconds means the user's value won.
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "{:?}",
            start.elapsed()
        );
    }
}
