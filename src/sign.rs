//! AWS `SigV4` query-string ("pre-signed URL") generation with support for
//! binding extra headers (`Content-Length`, `Content-Type`) into the signature.
//!
//! The generic [`object_store::signer::Signer`] only signs the `host` header,
//! so it cannot enforce the size or content type of an upload. This module
//! signs the `SigV4` query with `aws-sigv4` and additionally signs the headers
//! a caller wants the client to be forced to send. A client using such a URL MUST send
//! exactly those header values, otherwise S3 / `SeaweedFS` reject the request
//! with a 403 signature mismatch — pushing size/type enforcement down to the
//! object store.

use crate::client::{Error, Result};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation, SigningSettings,
    UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use chrono::{DateTime, Utc};
use http::Method;
use object_store::aws::AwsCredential;
use std::time::Duration;
use url::Url;

/// The longest validity of a pre-signed URL, for every scheme.
///
/// 7 days is the `SigV4` and GCS V4 maximum and the longest Azure
/// user-delegation SAS. It also keeps Azure's `chrono` arithmetic
/// (`now + expires_in`) in range.
pub const MAX_PRESIGN_EXPIRY: Duration = Duration::from_hours(7 * 24);

/// HTTP method a pre-signed URL authorizes.
///
/// This is a backend-agnostic abstraction over the HTTP verb so that callers
/// (including the Python and CLI front-ends) do not need to depend on the
/// `http`/`reqwest` crates directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignMethod {
    /// Download an object.
    Get,
    /// Upload (overwrite) an object.
    Put,
    /// Upload via an HTTP POST (e.g. browser form uploads).
    Post,
    /// Delete an object.
    Delete,
    /// Fetch object metadata only.
    Head,
}

impl SignMethod {
    pub(crate) const fn into_http(self) -> Method {
        match self {
            Self::Get => Method::GET,
            Self::Put => Method::PUT,
            Self::Post => Method::POST,
            Self::Delete => Method::DELETE,
            Self::Head => Method::HEAD,
        }
    }
}

impl std::str::FromStr for SignMethod {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_uppercase().as_str() {
            "GET" => Ok(Self::Get),
            "PUT" => Ok(Self::Put),
            "POST" => Ok(Self::Post),
            "DELETE" => Ok(Self::Delete),
            "HEAD" => Ok(Self::Head),
            _ => Err(Error::InvalidInput(format!(
                "unknown HTTP method {s}; expected GET, PUT, POST, DELETE or HEAD"
            ))),
        }
    }
}

/// Extra request properties to bind into a pre-signed URL's signature.
///
/// When set, the client using the URL is forced to send exactly these header
/// values (`Content-Length` / `Content-Type`), letting the object store reject
/// uploads of the wrong size or type up front. Binding extra headers is only
/// supported for S3 backends; for other schemes a non-default value is an
/// error. The default (all `None`) reproduces the host-only signing behavior.
#[derive(Debug, Clone, Default)]
pub struct SignOptions {
    /// Required `Content-Length` (in bytes) the client must send.
    pub content_length: Option<u64>,
    /// Required `Content-Type` the client must send.
    pub content_type: Option<String>,
}

impl SignOptions {
    /// `true` when no extra headers are requested, so plain host-only signing
    /// via the object store's own signer suffices.
    pub(crate) const fn is_empty(&self) -> bool {
        self.content_length.is_none() && self.content_type.is_none()
    }
}

/// Error returned when `Content-Length`/`Content-Type` binding is requested for
/// a backend other than S3, which is the only one this client can re-sign with
/// extra headers.
pub(crate) fn content_binding_unsupported() -> Error {
    Error::InvalidInput(
        "binding Content-Length/Content-Type into a pre-signed URL is only supported for S3 \
         (s3://) backends"
            .into(),
    )
}

/// Generate a `SigV4` pre-signed URL for `base`
///
/// (a clean resource URL, i.e. scheme, host and path with no query) authorizing `method` for
/// `expires_in`, binding the `content_length` and/or `content_type` of `options` into the
/// signature so the client is forced to send those exact header values.
///
/// `base` is expected to already encode the correct host and path style
/// (path-style vs virtual-hosted), which is why callers derive it from
/// `object_store`'s own signer rather than reconstructing it here.
///
/// # Panics
///
/// If a header value of `options` is not valid in an HTTP header (the client
/// checks this first and returns an error).
#[must_use]
pub fn presign_s3(
    base: &Url,
    method: &Method,
    expires_in: Duration,
    cred: &AwsCredential,
    region: &str,
    options: &SignOptions,
) -> Url {
    presign_s3_at(base, method, expires_in, cred, region, options, Utc::now())
}

/// As [`presign_s3`], but with an explicit signing timestamp (for testing
/// against fixed `SigV4` vectors).
///
/// # Panics
///
/// As [`presign_s3`].
#[doc(hidden)]
#[must_use]
pub fn presign_s3_at(
    base: &Url,
    method: &Method,
    expires_in: Duration,
    cred: &AwsCredential,
    region: &str,
    options: &SignOptions,
    date: DateTime<Utc>,
) -> Url {
    let identity = Credentials::new(
        &cred.key_id,
        &cred.secret_key,
        cred.token.clone(),
        None,
        "object-storage-client",
    )
    .into();
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(expires_in);
    // S3 encodes the path once and rejects a normalised one.
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("s3")
        .time(date.into())
        .settings(settings)
        .build()
        .expect("every signing parameter is set")
        .into();

    // The host is added by the signer from the URL.
    let length = options.content_length.map(|len| len.to_string());
    let headers = length
        .as_deref()
        .map(|len| ("content-length", len))
        .into_iter()
        .chain(
            options
                .content_type
                .as_deref()
                .map(|ty| ("content-type", ty)),
        );
    let request = SignableRequest::new(
        method.as_str(),
        base.as_str(),
        headers,
        SignableBody::UnsignedPayload,
    )
    .expect("a parsed URL is a valid URI");
    let (instructions, _) = sign(request, &params)
        .expect("the headers are valid and the credential is set")
        .into_parts();

    let mut url = base.clone();
    url.query_pairs_mut().extend_pairs(instructions.params());
    url
}
