//! Amazon S3 (and S3-compatible, e.g. `MinIO` / `SeaweedFS`) backend.

use super::aws_profile;
use super::{
    AwsProfile, Backend, BucketClient, Shared, Transport, Vars, bucket_exists_from_response,
    create_failed, default_timeouts, error_body, parse_bool, read_document, redact_error_body,
    send_signed, send_with_retry,
};
use crate::client::{Error, Result};
use crate::sign::{self, SignMethod, SignOptions};
use bytes::Bytes;
use http::Method;
use object_store::aws::{
    AmazonS3, AmazonS3Builder, AmazonS3ConfigKey, AwsAuthorizer, AwsCredential,
};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectMeta, ObjectStore, StaticCredentialProvider};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// The settings behind [`S3Backend`]'s `provider_policy`.
const POLICY_KEYS: [&str; 5] = [
    "aws_server_side_encryption",
    "aws_sse_kms_key_id",
    "aws_sse_bucket_key_enabled",
    "aws_sse_customer_key_base64",
    "aws_s3_express",
];

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
    /// The client of the bucket calls, with the store's transport settings.
    http: BucketClient,
    /// `AWS_REQUEST_PAYER`: pre-signed URLs carry `x-amz-request-payer`.
    request_payer: bool,
    /// A setting that the store's own requests turn into encryption headers
    /// (`AWS_SERVER_SIDE_ENCRYPTION` and its key settings), or `AWS_S3_EXPRESS`:
    /// the hand-built copy request does not carry them, so such a store streams.
    provider_policy: bool,
}

impl S3Backend {
    /// Build the S3 store from the environment, with the HTTP clients and
    /// credential provider of `shared`, mirroring the configuration the
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
    pub fn with_shared(
        host: Option<&str>,
        vars: &Vars,
        profile: Option<&AwsProfile>,
        shared: &Shared,
    ) -> Result<Self> {
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

        builder = Self::with_s3_keys(vars, builder);

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
        // The registry's one provider for the other sources (static keys, web
        // identity, container credentials, IMDS), except for a profile, which
        // has its own, and for a store that skips signing, whose provider must
        // never be called.
        let share = !uses_profile && !skip_signature;
        if share && let Some(credentials) = shared.aws.get() {
            builder = builder.with_credentials(Arc::clone(credentials));
        }

        let request_payer = builder
            .get_config_value(&AmazonS3ConfigKey::RequestPayer)
            .and_then(|v| parse_bool(&v))
            .unwrap_or(false);
        let provider_policy = Self::provider_policy(&builder);
        let http = BucketClient::new(Transport::read(
            |k| builder.get_config_value(&AmazonS3ConfigKey::Client(k)),
            vars,
        ));
        let inner = Arc::new(builder.with_http_connector(shared.connector()).build()?);
        if share {
            // A racing store loses; both hold equivalent providers.
            let _ = shared.aws.set(Arc::clone(inner.credentials()));
        }
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            skip_signature,
            bucket: bucket.to_string(),
            region,
            endpoint,
            addressing,
            http,
            request_payer,
            provider_policy,
        })
    }

    /// Whether `builder` holds a setting behind `provider_policy`.
    fn provider_policy(builder: &AmazonS3Builder) -> bool {
        POLICY_KEYS.iter().any(|name| {
            name.parse::<AmazonS3ConfigKey>()
                .ok()
                .and_then(|key| builder.get_config_value(&key))
                .is_some_and(|v| *name != "aws_s3_express" || parse_bool(&v) == Some(true))
        })
    }

    /// As [`Self::with_shared`], with providers and clients of its own.
    #[cfg(test)]
    pub fn new(host: Option<&str>, vars: &Vars, profile: Option<&AwsProfile>) -> Result<Self> {
        Self::with_shared(host, vars, profile, &Shared::default())
    }

    /// Applies `S3_ACCESS_KEY_ID` and `S3_SECRET_ACCESS_KEY` to `builder`.
    ///
    /// `S3_*` keys are long-lived, so they never carry a session token:
    /// `AWS_SESSION_TOKEN` belongs to the ambient `AWS_*` identity. The builder
    /// cannot unset a token, which is why the credential is explicit. Its
    /// secret is `S3_SECRET_ACCESS_KEY`, else the builder's own; with neither,
    /// the builder reports the missing secret when it builds.
    fn with_s3_keys(vars: &Vars, mut builder: AmazonS3Builder) -> AmazonS3Builder {
        if let Some(access_key_id) = vars.get("S3_ACCESS_KEY_ID") {
            let secret_key = vars
                .get("S3_SECRET_ACCESS_KEY")
                .or_else(|| builder.get_config_value(&AmazonS3ConfigKey::SecretAccessKey));
            if let Some(secret_key) = secret_key {
                builder = builder.with_credentials(Arc::new(StaticCredentialProvider::new(
                    AwsCredential {
                        key_id: access_key_id.clone(),
                        secret_key,
                        token: None,
                    },
                )));
            }
            builder = builder.with_access_key_id(access_key_id);
        }
        if let Some(secret_access_key) = vars.get("S3_SECRET_ACCESS_KEY") {
            builder = builder.with_secret_access_key(secret_access_key);
        }
        builder
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

    /// The endpoint path-style requests target, resolved once in
    /// [`S3Backend::new`]. The AWS default endpoint is always https, whatever
    /// `S3_ALLOW_HTTP` says: that setting is for custom endpoints. A custom
    /// endpoint is used as configured (an `http://` endpoint enables plain HTTP
    /// for the store, see [`S3Backend::new`]).
    fn endpoint(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", self.region))
    }

    /// A bucket-level request (`/<bucket>`) carrying `body`, `SigV4`-signed
    /// unless signing is skipped. The credential comes from
    /// [`S3Backend::credential`], so `skip_signature` never reaches IMDS.
    async fn bucket_request(&self, method: Method, body: Bytes) -> Result<HttpRequest> {
        let uri = match (self.addressing, self.endpoint.as_deref()) {
            // The store uses a custom endpoint as is in this mode.
            (AddressingStyle::Virtual, Some(endpoint)) => {
                format!("{}/", endpoint.trim_end_matches('/'))
            }
            (AddressingStyle::Virtual, None) => {
                format!("https://{}.s3.{}.amazonaws.com/", self.bucket, self.region)
            }
            // Path-style addressing keeps the request endpoint-agnostic, which
            // is what MinIO / SeaweedFS expect and AWS also accepts.
            (AddressingStyle::Path, _) => {
                format!("{}/{}", self.endpoint().trim_end_matches('/'), self.bucket)
            }
        };
        let mut request: HttpRequest = http::Request::builder()
            .method(method)
            .uri(&uri)
            .body(HttpRequestBody::from(body))
            .map_err(|e| Error::Generic(format!("Failed to build S3 request: {e}")))?;

        // Reuse object_store's SigV4 implementation — no signing reimplemented.
        // When signing is skipped (anonymous store) the request is sent as-is.
        if let Some(credential) = self.credential().await? {
            AwsAuthorizer::new(credential.as_ref(), "s3", &self.region)
                .try_authorize(&mut request, None)
                .map_err(|e| Error::Generic(format!("Failed to sign S3 request: {e}")))?;
        }
        Ok(request)
    }

    /// `source` as the backend a `CopyObject` into this bucket may read from,
    /// or `None` when it must stream. Both sides must sign, with the very same
    /// credential provider, and reach the same endpoint and region in the same
    /// addressing style. Requester-pays buckets stream, so do stores with provider-side
    /// encryption settings or S3 Express (see `provider_policy`), and so does a custom
    /// endpoint in virtual-hosted style, where the endpoint names one bucket.
    fn copy_source<'a>(&self, source: &'a dyn Backend) -> Option<&'a Self> {
        let other = source.as_any()?.downcast_ref::<Self>()?;
        let usable = !self.skip_signature
            && !other.skip_signature
            && !self.request_payer
            && !other.request_payer
            && !self.provider_policy
            && !other.provider_policy
            && self.region == other.region
            && self.endpoint == other.endpoint
            && self.addressing == other.addressing
            && !(self.addressing.is_virtual() && self.endpoint.is_some())
            && Arc::ptr_eq(self.inner.credentials(), other.inner.credentials());
        usable.then_some(other)
    }

    /// The URI of the object `path` in this bucket, in the addressing style of
    /// the store. The key is encoded as `object_store` encodes it.
    fn object_uri(&self, path: &ObjectPath) -> String {
        let key = encode_key(path);
        if self.addressing.is_virtual() {
            // `copy_source` refused a custom endpoint in this mode.
            format!(
                "https://{}.s3.{}.amazonaws.com/{key}",
                self.bucket, self.region
            )
        } else {
            format!(
                "{}/{}/{key}",
                self.endpoint().trim_end_matches('/'),
                self.bucket
            )
        }
    }
}

/// The key of `path` as a URI path: everything but unreserved characters and
/// `/` is percent-encoded, as `object_store` does for its own requests.
fn encode_key(path: &ObjectPath) -> String {
    const KEY: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'.')
        .remove(b'_')
        .remove(b'~')
        .remove(b'/');
    percent_encoding::utf8_percent_encode(path.as_ref(), KEY).to_string()
}

/// Whether `text`, the start of a `200` answer, is a `CopyObjectResult`
/// document. Only that counts as a copy that happened.
fn is_copy_result(text: &str) -> bool {
    let mut rest = text.trim_start_matches(|c: char| c == '\u{feff}' || c.is_whitespace());
    if rest.starts_with("<?xml")
        && let Some(end) = rest.find("?>")
    {
        rest = rest[end + 2..].trim_start();
    }
    rest.strip_prefix("<CopyObjectResult")
        .and_then(|after| after.chars().next())
        .is_some_and(|c| c == '>' || c == '/' || c.is_whitespace())
}

/// What `CopyObject` answered, as the caller of [`Backend::copy_from`] sees it.
/// S3 may answer `200` and put an `<Error>` document in the body, after white
/// space that keeps a long request alive, so the body decides, not only the
/// status: a `2xx` is a copy only if the body is a `CopyObjectResult`. Any other
/// body, an empty one included, is an error. `501` and `NotImplemented` (some
/// S3-compatible servers) mean the server cannot copy, and the caller streams.
fn copy_outcome(status: reqwest::StatusCode, text: &str) -> Result<bool> {
    if status.is_success() && is_copy_result(text) {
        return Ok(true);
    }
    let text = crate::text::escape(&redact_error_body(text), 4096);
    let code = |code: &str| text.contains(&format!("<Code>{code}</Code>"));
    if status.as_u16() == 501 || code("NotImplemented") {
        return Ok(false);
    }
    if status.as_u16() == 412 || code("PreconditionFailed") {
        return Err(Error::SourceChanged { url: String::new() });
    }
    if status.is_success() {
        return Err(Error::Generic(format!(
            "S3 CopyObject did not report a copy (HTTP {status}): {text}"
        )));
    }
    Err(create_failed("S3 CopyObject", status, &text))
}

#[async_trait::async_trait]
impl Backend for S3Backend {
    #[cfg(feature = "gui")]
    fn paginated(&self) -> Option<&dyn object_store::list::PaginatedListStore> {
        Some(self.inner.as_ref())
    }

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

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn copies_from(&self, src: &dyn Backend) -> bool {
        self.copy_source(src).is_some()
    }

    /// A signed `CopyObject`: `PUT` of the destination key with
    /// `x-amz-copy-source: /<bucket>/<key>`, and `x-amz-copy-source-if-match`
    /// with the source's e-tag when `known` has one. The source's metadata is
    /// copied (the default `MetadataDirective`); no storage class is sent, as
    /// a streamed copy keeps none either. See [`S3Backend::copy_source`] for
    /// when this runs; a source over 5 GiB never gets here (the caller
    /// streams it). A `501` answer means the server has no `CopyObject`, and
    /// the caller streams. A `200` whose body is an `<Error>` document is a
    /// failure, and `412` is [`Error::SourceChanged`].
    async fn copy_from(
        &self,
        src: &dyn Backend,
        src_path: &ObjectPath,
        dst_path: &ObjectPath,
        known: Option<&ObjectMeta>,
    ) -> Result<bool> {
        let Some(source) = self.copy_source(src) else {
            return Ok(false);
        };
        let copy_source = format!("/{}/{}", source.bucket, encode_key(src_path));
        let mut builder = http::Request::builder()
            .method(Method::PUT)
            .uri(self.object_uri(dst_path))
            .header("x-amz-copy-source", copy_source);
        if let Some(etag) = known.and_then(|meta| meta.e_tag.as_deref()) {
            // An e-tag that is no header value cannot be matched: stream.
            let Ok(etag) = http::HeaderValue::from_str(etag) else {
                return Ok(false);
            };
            builder = builder.header("x-amz-copy-source-if-match", etag);
        }
        let request = builder
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build S3 request: {e}")))?;
        let client = self.http.get()?;
        let response = send_with_retry(|| async {
            let mut request = request.clone();
            let credential = self.credential().await?.ok_or_else(|| {
                Error::Generic(
                    "Cannot copy on S3 without credentials (this store runs unsigned)".into(),
                )
            })?;
            AwsAuthorizer::new(credential.as_ref(), "s3", &self.region)
                .try_authorize(&mut request, None)
                .map_err(|e| Error::Generic(format!("Failed to sign S3 request: {e}")))?;
            send_signed(client, request).await
        })
        .await?;
        let status = response.status();
        let text = if status.is_success() {
            read_document(response).await?
        } else {
            error_body(response).await
        };
        copy_outcome(status, &text)
    }

    /// Issues an S3 `CreateBucket` request, signed with `object_store`'s
    /// [`AwsAuthorizer`] (`SigV4`). A `LocationConstraint` body is sent for every
    /// region except `us-east-1`, per the S3 API. Succeeds for a `2xx` response
    /// and for a `409` whose error document carries `BucketAlreadyOwnedByYou`
    /// (the bucket is already ours, so the call is idempotent). Every other
    /// response is an error, including `BucketAlreadyExists`: another account
    /// owns that global name.
    async fn create_bucket(&self, _url: &Url) -> Result<()> {
        let region = &self.region;
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
        let response = send_signed(self.http.get()?, request).await?;
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
        Err(create_failed("S3 CreateBucket", status, &text))
    }

    /// Issues an S3 `HeadBucket` request (`HEAD /<bucket>`), signed with
    /// `object_store`'s [`AwsAuthorizer`] (`SigV4`). This is a metadata-only
    /// probe that avoids the `ListBucket` payload: HTTP `200` means the bucket
    /// exists and `404` that it does not. A `403` (also the answer to a wrong
    /// key or signature) or `401` is an error: existence is unknown.
    async fn bucket_exists(&self, _url: &Url) -> Result<bool> {
        let request = self.bucket_request(Method::HEAD, Bytes::new()).await?;
        let response = send_signed(self.http.get()?, request).await?;
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
        bases
            .into_iter()
            .map(|mut base| {
                // Drop the host-only query produced by the object store's signer;
                // we generate our own SigV4 query over the bound headers.
                base.set_query(None);
                // The signer added this parameter, and it must be signed too.
                if self.request_payer {
                    base.query_pairs_mut()
                        .append_pair("x-amz-request-payer", "requester");
                }
                sign::presign_s3(
                    &base,
                    &http_method,
                    expires_in,
                    &credential,
                    region,
                    options,
                )
                .map(Into::into)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::stub::{Reply, Request, serve, serve_cut};
    use object_store::ObjectStoreExt;

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
        let result = stubbed(&stub.base).create_bucket(&url).await;
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
    async fn bucket_calls_follow_the_stores_transport_settings() {
        let proxy = serve(vec![Reply::new(200, "")]).await;
        let backend = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[
                ("S3_ENDPOINT", "http://bucket.invalid:9"),
                ("S3_REGION", "us-east-1"),
                ("S3_ACCESS_KEY_ID", "k"),
                ("S3_SECRET_ACCESS_KEY", "s"),
                ("AWS_PROXY_URL", &proxy.base),
                ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
            ]),
            None,
        )
        .expect("build");
        let transport = backend.http.transport();
        assert!(transport.allow_invalid_certificates);
        assert!(transport.allow_http, "an http endpoint allows http");
        let url = Url::parse("s3://bkt").expect("url");
        assert!(backend.bucket_exists(&url).await.expect("exists"));
        assert_eq!(
            proxy.requests()[0].line,
            "HEAD http://bucket.invalid:9/bkt HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn create_bucket_reports_401_and_403_as_denied() {
        for status in [401, 403] {
            let doc = error_document("AccessDenied", "Access Denied");
            let err = create(status, doc).await.0.expect_err("denied");
            assert!(matches!(err, Error::Denied(_)), "{err:?}");
            assert!(err.to_string().contains("AccessDenied"), "{err}");
        }
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
        assert_eq!(
            http.endpoint(),
            format!("https://s3.{}.amazonaws.com", http.region)
        );

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
        assert_eq!(custom.endpoint(), "http://127.0.0.1:9000");

        let https = S3Backend::new(
            Some("b"),
            &Vars::from_pairs(&[("S3_ALLOW_HTTP", "false")]),
            None,
        )
        .expect("build");
        let endpoint_https = https.endpoint();
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
        assert_eq!(backend.endpoint(), "http://127.0.0.1:3900");
        assert_eq!(backend.region, "eu-north-1");
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
        assert_eq!(backend.region, "eu-west-3");
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
        assert_eq!(backend.endpoint(), "http://127.0.0.1:3900");
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
            assert_eq!(backend.region, want, "{pairs:?}");
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
            assert_eq!(backend.region, want, "{profile} {pairs:?}");
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

    /// Headers of a bucket request, and the pre-signed URLs, of a store built
    /// from `pairs` (static keys on the AWS endpoint, so nothing is sent).
    async fn token_traces(pairs: &[(&str, &str)]) -> (Option<String>, Vec<String>) {
        let backend = S3Backend::new(Some("bkt"), &Vars::from_pairs(pairs), None).expect("build");
        let request = backend
            .bucket_request(Method::HEAD, Bytes::new())
            .await
            .expect("request");
        let header = request
            .headers()
            .get("x-amz-security-token")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let bound = SignOptions {
            content_length: Some(5),
            content_type: Some("text/plain".into()),
        };
        let mut urls = Vec::new();
        for options in [SignOptions::default(), bound] {
            urls.extend(
                backend
                    .pre_signed_urls(
                        &[ObjectPath::from("o")],
                        SignMethod::Get,
                        Duration::from_mins(1),
                        &options,
                    )
                    .await
                    .expect("sign"),
            );
        }
        (header, urls)
    }

    #[tokio::test]
    async fn s3_keys_ignore_the_ambient_session_token() {
        let (header, urls) = token_traces(&[
            ("S3_ACCESS_KEY_ID", "k"),
            ("S3_SECRET_ACCESS_KEY", "s"),
            ("AWS_SESSION_TOKEN", "ambient"),
        ])
        .await;
        assert_eq!(header, None);
        assert_eq!(urls.len(), 2);
        for url in urls {
            assert!(
                !url.to_ascii_lowercase().contains("security-token"),
                "{url}"
            );
            assert!(url.contains("X-Amz-Credential=k%2F"), "{url}");
        }
    }

    #[tokio::test]
    async fn s3_keys_take_the_secret_from_the_aws_variable_when_s3_has_none() {
        let (header, urls) = token_traces(&[
            ("S3_ACCESS_KEY_ID", "k"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
            ("AWS_SESSION_TOKEN", "ambient"),
        ])
        .await;
        assert_eq!(header, None);
        assert!(urls.iter().all(|u| u.contains("X-Amz-Credential=k%2F")));
    }

    #[test]
    fn s3_keys_without_any_secret_fail_to_build() {
        let vars = Vars::from_pairs(&[("S3_ACCESS_KEY_ID", "k")]);
        assert!(S3Backend::new(Some("bkt"), &vars, None).is_err());
    }

    #[tokio::test]
    async fn aws_keys_still_carry_the_session_token() {
        let (header, urls) = token_traces(&[
            ("AWS_ACCESS_KEY_ID", "k"),
            ("AWS_SECRET_ACCESS_KEY", "s"),
            ("AWS_SESSION_TOKEN", "tok"),
        ])
        .await;
        assert_eq!(header.as_deref(), Some("tok"));
        assert_eq!(urls.len(), 2);
        for url in urls {
            assert!(url.contains("X-Amz-Security-Token=tok"), "{url}");
        }
    }

    #[tokio::test]
    async fn a_header_bound_url_keeps_the_request_payer() {
        let sign = |payer: &'static str| async move {
            let backend = keyed(&[("AWS_REQUEST_PAYER", payer)]);
            let mut urls = backend
                .pre_signed_urls(
                    &[ObjectPath::from("o")],
                    SignMethod::Put,
                    Duration::from_mins(1),
                    &SignOptions {
                        content_length: Some(5),
                        content_type: None,
                    },
                )
                .await
                .expect("sign");
            urls.remove(0)
        };
        let url = sign("true").await;
        assert!(url.contains("x-amz-request-payer=requester"), "{url}");
        let url = sign("false").await;
        assert!(!url.contains("x-amz-request-payer"), "{url}");
    }

    #[tokio::test]
    async fn a_bucket_name_that_is_not_a_uri_host_is_an_error_not_a_panic() {
        let backend = S3Backend::new(
            Some("a{b"),
            &Vars::from_pairs(&[
                ("S3_ACCESS_KEY_ID", "k"),
                ("S3_SECRET_ACCESS_KEY", "s"),
                ("S3_ADDRESSING_STYLE", "virtual"),
            ]),
            None,
        )
        .expect("build");
        let options = SignOptions {
            content_length: None,
            content_type: Some("text/plain".into()),
        };
        let err = backend
            .pre_signed_urls(
                &[ObjectPath::from("o")],
                SignMethod::Put,
                Duration::from_mins(1),
                &options,
            )
            .await
            .expect_err("invalid host");
        assert!(matches!(err, Error::InvalidInput(_)), "{err:?}");
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
        backend.create_bucket(&url).await.expect("create");
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
        assert_eq!(backend.endpoint(), "http://127.0.0.1:9");
        // The http:// endpoint enables plain HTTP in the store, which signs for it.
        let url = signed(&backend).await;
        assert!(url.as_str().starts_with("http://127.0.0.1:9/"), "{url}");
    }

    #[tokio::test]
    async fn s3_endpoint_beats_the_profile_endpoint() {
        let (_dir, backend) = endpoint_backend(&[("S3_ENDPOINT", "http://127.0.0.1:8")]);
        assert_eq!(backend.endpoint(), "http://127.0.0.1:8");
        let url = signed(&backend).await;
        assert!(url.as_str().starts_with("http://127.0.0.1:8/"), "{url}");
    }

    #[tokio::test]
    async fn ignore_configured_endpoint_urls_drops_the_profile_endpoint() {
        let (_dir, backend) = endpoint_backend(&[("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS", "true")]);
        assert_eq!(backend.endpoint(), "https://s3.eu-west-3.amazonaws.com");
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

    fn shared_pointer(backend: &S3Backend) -> *const () {
        Arc::as_ptr(backend.inner.credentials()).cast()
    }

    #[test]
    fn buckets_of_one_registry_share_a_credential_provider() {
        // IMDS (no keys, no endpoint) and static keys.
        for pairs in [
            &[("AWS_REGION", "eu-west-1")][..],
            &[
                ("AWS_REGION", "eu-west-1"),
                ("AWS_ACCESS_KEY_ID", "k"),
                ("AWS_SECRET_ACCESS_KEY", "s"),
            ][..],
        ] {
            let vars = Vars::from_pairs(pairs);
            let shared = Shared::default();
            let first = S3Backend::with_shared(Some("a"), &vars, None, &shared).expect("a");
            let second = S3Backend::with_shared(Some("b"), &vars, None, &shared).expect("b");
            assert_eq!(shared_pointer(&first), shared_pointer(&second));
            let held = shared.aws.get().expect("stored");
            assert_eq!(Arc::as_ptr(held).cast::<()>(), shared_pointer(&first));
            // Another registry has its own.
            let other = S3Backend::new(Some("a"), &vars, None).expect("other");
            assert_ne!(shared_pointer(&first), shared_pointer(&other));
        }
    }

    #[test]
    fn a_store_that_skips_signing_takes_no_shared_provider() {
        let shared = Shared::default();
        let signed = Vars::from_pairs(&[("AWS_REGION", "eu-west-1")]);
        let first = S3Backend::with_shared(Some("a"), &signed, None, &shared).expect("a");
        // A custom endpoint without credentials skips signing.
        let anonymous = Vars::from_pairs(&[("S3_ENDPOINT", "http://h"), ("S3_REGION", "r")]);
        let skipping = S3Backend::with_shared(Some("b"), &anonymous, None, &shared).expect("b");
        assert!(skipping.skip_signature);
        assert_ne!(shared_pointer(&first), shared_pointer(&skipping));
        assert_eq!(
            Arc::as_ptr(shared.aws.get().expect("stored")).cast::<()>(),
            shared_pointer(&first)
        );
        // It does not fill an empty one either.
        let empty = Shared::default();
        S3Backend::with_shared(Some("b"), &anonymous, None, &empty).expect("b");
        assert!(empty.aws.get().is_none());
    }

    fn copy_vars(base: &str) -> Vec<(&str, &str)> {
        vec![
            ("S3_ENDPOINT", base),
            ("S3_ALLOW_HTTP", "true"),
            ("S3_REGION", "us-east-1"),
            ("S3_ACCESS_KEY_ID", "k"),
            ("S3_SECRET_ACCESS_KEY", "s"),
        ]
    }

    /// Backends for buckets `bkt` (destination) and `src bkt` on the stub at
    /// `base`, of one registry.
    fn copy_pair(base: &str) -> (S3Backend, S3Backend) {
        let vars = Vars::from_pairs(&copy_vars(base));
        let shared = Shared::default();
        let build =
            |bucket| S3Backend::with_shared(Some(bucket), &vars, None, &shared).expect("build");
        (build("bkt"), build("src"))
    }

    fn meta(etag: Option<&str>) -> ObjectMeta {
        ObjectMeta {
            location: ObjectPath::from("a"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 1,
            e_tag: etag.map(str::to_owned),
            version: None,
        }
    }

    const COPY_RESULT: &str = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
        <CopyObjectResult><ETag>\"e\"</ETag></CopyObjectResult>";

    /// `copy_from` of `a b/c` to `d/e` against a stub answering `reply`.
    async fn cross_copy(reply: Reply, known: Option<&ObjectMeta>) -> (Result<bool>, Vec<Request>) {
        let stub = serve(vec![reply]).await;
        let (dst, src) = copy_pair(&stub.base);
        let done = dst
            .copy_from(
                &src,
                &ObjectPath::from("a b/c"),
                &ObjectPath::from("d/e"),
                known,
            )
            .await;
        (done, stub.requests())
    }

    #[tokio::test]
    async fn a_cross_bucket_copy_is_a_signed_copy_object() {
        let known = meta(Some("\"abc\""));
        let (done, requests) = cross_copy(Reply::new(200, COPY_RESULT), Some(&known)).await;
        assert!(done.expect("copy"));
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].line, "PUT /bkt/d/e HTTP/1.1");
        let headers = requests[0].headers.to_ascii_lowercase();
        assert!(
            headers.contains("x-amz-copy-source: /src/a%20b/c\r\n")
                || headers.contains("x-amz-copy-source: /src/a%20b/c"),
            "{headers}"
        );
        assert!(
            headers.contains("x-amz-copy-source-if-match: \"abc\""),
            "{headers}"
        );
        assert!(
            headers.contains("authorization: aws4-hmac-sha256"),
            "{headers}"
        );
        let signed = headers
            .split("signedheaders=")
            .nth(1)
            .and_then(|rest| rest.split(',').next())
            .expect("signed headers");
        assert!(signed.contains("x-amz-copy-source;"), "{signed}");
        assert!(signed.contains("x-amz-copy-source-if-match"), "{signed}");
        assert_eq!(requests[0].body.len(), 0);
    }

    #[tokio::test]
    async fn a_copy_without_a_known_e_tag_sends_no_condition() {
        let (done, requests) = cross_copy(Reply::new(200, COPY_RESULT), None).await;
        assert!(done.expect("copy"));
        let headers = requests[0].headers.to_ascii_lowercase();
        assert!(!headers.contains("if-match"), "{headers}");
    }

    #[tokio::test]
    async fn an_error_document_in_a_200_is_a_failure() {
        let doc = error_document("InternalError", "We encountered an internal error");
        let (done, _) = cross_copy(Reply::new(200, doc), None).await;
        let err = done.expect_err("error in 200");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");
        assert!(err.to_string().contains("InternalError"), "{err}");
    }

    #[tokio::test]
    async fn a_server_without_copy_object_streams() {
        let (done, _) = cross_copy(Reply::new(501, ""), None).await;
        assert!(!done.expect("501"));
        let doc = error_document("NotImplemented", "not here");
        let (done, _) = cross_copy(Reply::new(200, doc), None).await;
        assert!(!done.expect("NotImplemented in 200"));
    }

    #[tokio::test]
    async fn a_changed_source_and_a_denial_are_errors() {
        let (done, _) = cross_copy(
            Reply::new(412, error_document("PreconditionFailed", "no")),
            Some(&meta(Some("\"abc\""))),
        )
        .await;
        assert!(matches!(
            done.expect_err("412"),
            Error::SourceChanged { .. }
        ));
        let (done, _) =
            cross_copy(Reply::new(403, error_document("AccessDenied", "no")), None).await;
        let err = done.expect_err("403");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        assert!(err.to_string().contains("AccessDenied"), "{err}");
    }

    #[tokio::test]
    async fn copies_stay_in_the_provider_account_and_endpoint() {
        let stub = serve(vec![]).await;
        let (dst, src) = copy_pair(&stub.base);
        assert!(dst.copies_from(&src));

        let refuse = |pairs: &[(&str, &str)], shared: &Shared| {
            let vars = Vars::from_pairs(pairs);
            S3Backend::with_shared(Some("src"), &vars, None, shared).expect("other")
        };
        let base = copy_vars(&stub.base);
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut pairs = base.clone();
            pairs.retain(|(k, _)| !extra.iter().any(|(e, _)| e == k));
            pairs.extend_from_slice(extra);
            pairs
        };
        // Another registry holds another credential provider.
        assert!(!dst.copies_from(&refuse(&base, &Shared::default())));
        // The same registry, another endpoint, region or addressing style.
        let shared = Shared::default();
        let first = refuse(&base, &shared);
        for extra in [
            [("S3_ENDPOINT", "http://other.invalid:9")],
            [("S3_REGION", "eu-west-1")],
            [("S3_ADDRESSING_STYLE", "virtual")],
            [("AWS_REQUEST_PAYER", "true")],
        ] {
            let other = refuse(&with(&extra), &shared);
            assert!(first.copies_from(&first), "same bucket reads itself");
            assert!(!first.copies_from(&other), "{extra:?}");
            assert!(!other.copies_from(&first), "{extra:?}");
        }
        // A store that skips signing, on either side.
        let skipping = refuse(&with(&[("AWS_SKIP_SIGNATURE", "true")]), &shared);
        assert!(skipping.skip_signature);
        assert!(!first.copies_from(&skipping) && !skipping.copies_from(&first));
        // Another provider.
        let memory: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: Arc::new(object_store::memory::InMemory::new()),
            multipart: false,
        });
        assert!(!dst.copies_from(memory.as_ref()));
        assert!(
            !dst.copy_from(memory.as_ref(), &"a".into(), &"b".into(), None)
                .await
                .expect("refused")
        );
        assert!(stub.requests().is_empty());
    }

    #[tokio::test]
    async fn virtual_hosting_on_a_custom_endpoint_streams() {
        let stub = serve(vec![]).await;
        let mut pairs = copy_vars(&stub.base);
        pairs.push(("S3_ADDRESSING_STYLE", "virtual"));
        let vars = Vars::from_pairs(&pairs);
        let shared = Shared::default();
        let a = S3Backend::with_shared(Some("a"), &vars, None, &shared).expect("a");
        let b = S3Backend::with_shared(Some("b"), &vars, None, &shared).expect("b");
        assert!(!a.copies_from(&b));
    }

    #[test]
    fn object_uris_follow_the_addressing_style() {
        let path = ObjectPath::from("d e/f");
        let path_style = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[("S3_ENDPOINT", "http://h:9/"), ("S3_REGION", "r")]),
            None,
        )
        .expect("build");
        assert_eq!(path_style.object_uri(&path), "http://h:9/bkt/d%20e/f");
        let aws = S3Backend::new(
            Some("bkt"),
            &Vars::from_pairs(&[
                ("S3_REGION", "eu-west-1"),
                ("S3_ADDRESSING_STYLE", "virtual"),
            ]),
            None,
        )
        .expect("build");
        assert_eq!(
            aws.object_uri(&path),
            "https://bkt.s3.eu-west-1.amazonaws.com/d%20e/f"
        );
    }

    /// Runs a copy or move of `src bkt/a` to `bkt/b` through `server_side_copy`
    /// against a stub answering `replies`.
    async fn copy_through_codec(
        replies: Vec<Reply>,
        delete_source: bool,
    ) -> (Result<bool>, Vec<String>) {
        copy_on(serve(replies).await, delete_source).await
    }

    async fn copy_on(
        stub: crate::backend::stub::Stub,
        delete_source: bool,
    ) -> (Result<bool>, Vec<String>) {
        use crate::codec::sealed::{Location, Resolved};
        let (dst, src) = copy_pair(&stub.base);
        let at = |backend: S3Backend, url: &str| {
            let url = Url::parse(url).expect("url");
            let path = ObjectPath::from_url_path(url.path()).expect("path");
            Resolved {
                backend: Arc::new(backend),
                loc: Location { url, path },
            }
        };
        let done = crate::codec::server_side_copy(
            &at(src, "s3://src/a"),
            &at(dst, "s3://bkt/b"),
            delete_source,
            None,
        )
        .await;
        let lines = stub
            .requests()
            .into_iter()
            .map(|r| r.line.split(" HTTP").next().unwrap_or_default().to_owned())
            .collect();
        (done, lines)
    }

    fn head_reply() -> Reply {
        Reply::new(200, "")
            .header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .header("ETag", "\"0x1\"")
    }

    #[tokio::test]
    async fn a_cross_bucket_move_deletes_the_source_only_after_the_copy() {
        let replies = vec![
            head_reply(),
            Reply::new(200, COPY_RESULT),
            head_reply(),
            head_reply(),
            Reply::new(
                200,
                "<DeleteResult><Deleted><Key>a</Key></Deleted></DeleteResult>",
            ),
        ];
        let (done, lines) = copy_through_codec(replies, true).await;
        assert!(done.expect("move"));
        assert_eq!(
            lines,
            [
                "HEAD /src/a",
                "PUT /bkt/b",
                "HEAD /bkt/b",
                "HEAD /src/a",
                "POST /src?delete"
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_cross_bucket_copy_keeps_the_source() {
        let doc = error_document("InternalError", "oops");
        for reply in [Reply::new(200, doc.clone()), Reply::new(500, doc)] {
            let (done, lines) = copy_through_codec(vec![head_reply(), reply], true).await;
            done.expect_err("failed copy");
            assert_eq!(lines, ["HEAD /src/a", "PUT /bkt/b"]);
        }
        let changed = Reply::new(412, error_document("PreconditionFailed", "no"));
        let (done, lines) = copy_through_codec(vec![head_reply(), changed], true).await;
        let err = done.expect_err("changed");
        assert!(
            matches!(&err, Error::SourceChanged { url } if url == "s3://src/a"),
            "{err:?}"
        );
        assert_eq!(lines, ["HEAD /src/a", "PUT /bkt/b"]);
    }

    #[tokio::test]
    async fn a_move_streams_when_the_server_cannot_copy() {
        let (done, lines) = copy_through_codec(vec![head_reply(), Reply::new(501, "")], true).await;
        assert!(!done.expect("501"));
        assert_eq!(lines, ["HEAD /src/a", "PUT /bkt/b"]);
    }

    #[tokio::test]
    async fn a_copy_over_five_gib_streams_without_a_copy_request() {
        use crate::codec::sealed::{Location, Resolved};
        let stub = serve(vec![]).await;
        let (dst, src) = copy_pair(&stub.base);
        let at = |backend: S3Backend, url: &str| {
            let url = Url::parse(url).expect("url");
            let path = ObjectPath::from_url_path(url.path()).expect("path");
            Resolved {
                backend: Arc::new(backend),
                loc: Location { url, path },
            }
        };
        let done = crate::codec::server_side_copy(
            &at(src, "s3://src/a"),
            &at(dst, "s3://bkt/b"),
            false,
            Some(6 << 30),
        )
        .await;
        assert!(!done.expect("too large"));
        assert!(stub.requests().is_empty());
    }

    /// A move whose copy answer is not a `CopyObjectResult` keeps its source:
    /// no request after the `PUT` deletes anything.
    #[tokio::test]
    async fn a_move_without_a_copy_result_keeps_the_source() {
        let keep_alive = " ".repeat(10_000);
        for body in [
            "",
            "   \n  ",
            "<html>hi</html>",
            &format!("{keep_alive}<CopyObjectRes"),
        ] {
            let replies = vec![head_reply(), Reply::new(200, body)];
            let (done, lines) = copy_through_codec(replies, true).await;
            done.expect_err(body);
            assert_eq!(lines, ["HEAD /src/a", "PUT /bkt/b"], "{body:?}");
        }
        // White space, then the connection is cut.
        let (done, lines) = copy_on(serve_cut(" ".repeat(10_000)).await, true).await;
        done.expect_err("cut");
        assert!(lines.iter().all(|l| !l.starts_with("POST")), "{lines:?}");
    }

    #[tokio::test]
    async fn keep_alive_white_space_before_the_result_is_a_success() {
        let body = format!("{}{COPY_RESULT}", " ".repeat(10_000));
        let (done, requests) = cross_copy(Reply::new(200, body), None).await;
        assert!(done.expect("copy"));
        assert_eq!(requests.len(), 1);
        // And an error document after it is a failure.
        let doc = format!(
            "{}{}",
            " ".repeat(10_000),
            error_document("InternalError", "late")
        );
        let (done, _) = cross_copy(Reply::new(200, doc), None).await;
        assert!(
            done.expect_err("late error")
                .to_string()
                .contains("InternalError")
        );
    }

    #[tokio::test]
    async fn a_move_keeps_the_source_when_the_destination_is_smaller() {
        let short = Reply::new(200, "hello")
            .header("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
            .header("ETag", "\"0x1\"");
        let replies = vec![head_reply(), Reply::new(200, COPY_RESULT), short];
        let (done, lines) = copy_through_codec(replies, true).await;
        assert!(
            done.expect_err("size")
                .to_string()
                .contains("source was kept")
        );
        assert_eq!(lines, ["HEAD /src/a", "PUT /bkt/b", "HEAD /bkt/b"]);
    }

    #[tokio::test]
    async fn transient_copy_failures_are_retried_up_to_three_attempts() {
        let busy = || Reply::new(503, error_document("SlowDown", "later"));
        let (done, requests) = cross_copy_all(vec![
            busy(),
            Reply::new(500, ""),
            Reply::new(200, COPY_RESULT),
        ])
        .await;
        assert!(done.expect("third attempt"));
        assert_eq!(requests.len(), 3);
        let (done, requests) = cross_copy_all(vec![busy(), busy(), busy()]).await;
        assert!(done.is_err());
        assert_eq!(requests.len(), 3);
        // Other answers are final, and so is a 2xx.
        let (done, requests) =
            cross_copy_all(vec![Reply::new(403, error_document("AccessDenied", "no"))]).await;
        assert!(done.is_err());
        assert_eq!(requests.len(), 1);
        let (done, requests) = cross_copy_all(vec![Reply::new(200, "")]).await;
        assert!(done.is_err());
        assert_eq!(requests.len(), 1);
    }

    async fn cross_copy_all(replies: Vec<Reply>) -> (Result<bool>, Vec<Request>) {
        let stub = serve(replies).await;
        let (dst, src) = copy_pair(&stub.base);
        let done = dst
            .copy_from(&src, &ObjectPath::from("a"), &ObjectPath::from("b"), None)
            .await;
        (done, stub.requests())
    }

    #[tokio::test]
    async fn provider_side_encryption_and_express_make_the_copy_stream() {
        let stub = serve(vec![]).await;
        let base = copy_vars(&stub.base);
        let build = |extra: &[(&'static str, &'static str)], shared: &Shared| {
            let mut pairs = base.clone();
            pairs.extend_from_slice(extra);
            let vars = Vars::from_pairs(&pairs);
            // An Express bucket name carries the zone.
            let bucket = if extra.contains(&("AWS_S3_EXPRESS", "true")) {
                "src--use1-az4--x-s3"
            } else {
                "src"
            };
            S3Backend::with_shared(Some(bucket), &vars, None, shared).expect("build")
        };
        let key = "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=";
        let shared = Shared::default();
        let plain = build(&[], &shared);
        let express_off = build(&[("AWS_S3_EXPRESS", "false")], &shared);
        assert!(plain.copies_from(&express_off));
        for extra in [
            &[("AWS_SERVER_SIDE_ENCRYPTION", "AES256")][..],
            &[
                ("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms"),
                ("AWS_SSE_KMS_KEY_ID", "k"),
            ][..],
            &[
                ("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms"),
                ("AWS_SSE_BUCKET_KEY_ENABLED", "true"),
            ][..],
            &[
                ("AWS_SERVER_SIDE_ENCRYPTION", "sse-c"),
                ("AWS_SSE_CUSTOMER_KEY_BASE64", key),
            ][..],
            &[("AWS_S3_EXPRESS", "true")][..],
        ] {
            let other = build(extra, &shared);
            assert!(!plain.copies_from(&other), "{extra:?}");
            assert!(!other.copies_from(&plain), "{extra:?}");
            let done = plain
                .copy_from(&other, &ObjectPath::from("a"), &ObjectPath::from("b"), None)
                .await;
            assert!(!done.expect("refused"), "{extra:?}");
        }
        assert!(stub.requests().is_empty());
    }
}
