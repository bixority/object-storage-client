//! AWS `SigV4` query-string ("pre-signed URL") generation with support for
//! binding extra headers (`Content-Length`, `Content-Type`) into the signature.
//!
//! The generic [`object_store::signer::Signer`] only signs the `host` header,
//! so it cannot enforce the size or content type of an upload. This module
//! re-implements the `SigV4` query-signing algorithm (mirroring the one inside
//! `object_store`'s AWS backend) but additionally signs the headers a caller
//! wants the client to be forced to send. A client using such a URL MUST send
//! exactly those header values, otherwise S3 / `SeaweedFS` reject the request
//! with a 403 signature mismatch — pushing size/type enforcement down to the
//! object store.

use crate::client::{Backend, Error, Result};
use chrono::{DateTime, Utc};
use http::Method;
use object_store::aws::AwsCredential;
use object_store::path::Path as ObjectPath;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use ring::{digest, hmac};
use std::time::Duration;
use url::Url;

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
    fn into_http(self) -> Method {
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
            other => Err(Error::Generic(format!("Invalid HTTP method: {other}"))),
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
    fn is_empty(&self) -> bool {
        self.content_length.is_none() && self.content_type.is_none()
    }

    /// Borrow these options as the lower-level [`BoundHeaders`].
    fn bound_headers(&self) -> BoundHeaders<'_> {
        BoundHeaders {
            content_length: self.content_length,
            content_type: self.content_type.as_deref(),
        }
    }
}

/// Generate a pre-signed URL for a single object on an already-resolved
/// `backend` (whose `scheme` and object `path` have been determined by the
/// caller).
///
/// When `options` is empty this is the object store's own host-only signing;
/// otherwise the host-only URL is re-signed binding the requested headers,
/// which is only possible for S3 backends.
///
/// # Errors
///
/// Returns an error if the backend does not support signing, `options` binds
/// extra headers on a non-S3 backend, or the backend fails to sign.
pub(crate) async fn pre_signed_url(
    backend: &Backend,
    scheme: &str,
    path: &ObjectPath,
    method: SignMethod,
    expires_in: Duration,
    options: &SignOptions,
) -> Result<String> {
    let signer = backend
        .signer
        .as_ref()
        .ok_or_else(|| Error::SigningUnsupported(scheme.to_string()))?;
    let base = signer
        .signed_url(method.into_http(), path, expires_in)
        .await?;

    if options.is_empty() {
        return Ok(base.into());
    }

    let pre_signed_url = sign_with_headers(backend, base, method, expires_in, options).await?;
    Ok(pre_signed_url.into())
}

/// Generate pre-signed URLs for multiple objects that share `backend` (and
/// therefore one signer / set of credentials). `paths` are the object paths
/// already resolved by the caller, all belonging to the same `scheme`.
///
/// # Errors
///
/// Returns an error if the backend does not support signing, `options` binds
/// extra headers on a non-S3 backend, or the backend fails to sign.
pub(crate) async fn pre_signed_urls(
    backend: &Backend,
    scheme: &str,
    paths: &[ObjectPath],
    method: SignMethod,
    expires_in: Duration,
    options: &SignOptions,
) -> Result<Vec<String>> {
    let signer = backend
        .signer
        .as_ref()
        .ok_or_else(|| Error::SigningUnsupported(scheme.to_string()))?;
    let bases = signer
        .signed_urls(method.into_http(), paths, expires_in)
        .await?;

    if options.is_empty() {
        return Ok(bases.into_iter().map(Into::into).collect());
    }

    // Resolve credentials once, then re-sign each URL with the bound headers.
    let s3 = backend
        .s3
        .as_ref()
        .ok_or_else(content_binding_unsupported)?;
    let credential = s3.credentials().get_credential().await?;
    let region = s3_region();
    let http_method = method.into_http();

    let mut pre_signed_urls = Vec::with_capacity(bases.len());
    for mut base in bases {
        base.set_query(None);
        pre_signed_urls.push(
            presign_s3(
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
    Ok(pre_signed_urls)
}

/// Re-sign `base` (an `object_store`-produced signed URL) binding the headers
/// in `options` into the signature. Only S3 backends carry the credentials
/// needed for this; other backends yield an error.
async fn sign_with_headers(
    backend: &Backend,
    mut base: Url,
    method: SignMethod,
    expires_in: Duration,
    options: &SignOptions,
) -> Result<Url> {
    let s3 = backend
        .s3
        .as_ref()
        .ok_or_else(content_binding_unsupported)?;
    let credential = s3.credentials().get_credential().await?;
    let region = s3_region();

    // Drop the host-only query produced by the object store's signer; we
    // generate our own SigV4 query over the bound headers.
    base.set_query(None);

    Ok(presign_s3(
        &base,
        &method.into_http(),
        expires_in,
        &credential,
        &region,
        options.bound_headers(),
    ))
}

/// Error returned when `Content-Length`/`Content-Type` binding is requested for
/// a backend other than S3, which is the only one this client can re-sign with
/// extra headers.
fn content_binding_unsupported() -> Error {
    Error::Generic(
        "binding Content-Length/Content-Type into a pre-signed URL is only supported for S3 \
         (s3://) backends"
            .into(),
    )
}

/// Resolve the AWS region used for `SigV4` signing, mirroring how the S3 backend
/// is configured: explicit `S3_REGION`, then the standard AWS environment
/// variables, falling back to `us-east-1`.
fn s3_region() -> String {
    std::env::var("S3_REGION")
        .or_else(|_| std::env::var("AWS_REGION"))
        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
        .unwrap_or_else(|_| "us-east-1".to_string())
}

const ALGORITHM: &str = "AWS4-HMAC-SHA256";
const SERVICE: &str = "s3";
/// The payload is supplied directly by the client, so it is never signed.
const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";

/// Characters that do *not* need to be percent-encoded in the AWS canonical
/// form. Matches `object_store`'s `STRICT_ENCODE_SET`.
const STRICT_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'.')
    .remove(b'_')
    .remove(b'~');

/// Request headers to bind into the `SigV4` signature, forcing the client using
/// the pre-signed URL to send exactly these values.
#[derive(Clone, Copy, Default)]
pub(crate) struct BoundHeaders<'a> {
    pub content_length: Option<u64>,
    pub content_type: Option<&'a str>,
}

fn hmac_sha256(secret: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> hmac::Tag {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_ref());
    hmac::sign(&key, bytes.as_ref())
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

fn hex_digest(bytes: &[u8]) -> String {
    hex_encode(digest::digest(&digest::SHA256, bytes).as_ref())
}

/// Collapse leading/trailing and repeated internal whitespace, matching the
/// header value normalization S3 performs when validating a signature.
fn normalize_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Derive the `SigV4` signing key and sign `to_sign`.
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
fn sign_string(cred: &AwsCredential, to_sign: &str, date_stamp: &str, region: &str) -> String {
    let date_hmac = hmac_sha256(format!("AWS4{}", cred.secret_key), date_stamp);
    let region_hmac = hmac_sha256(date_hmac.as_ref(), region);
    let service_hmac = hmac_sha256(region_hmac.as_ref(), SERVICE);
    let signing_hmac = hmac_sha256(service_hmac.as_ref(), b"aws4_request");
    hex_encode(hmac_sha256(signing_hmac.as_ref(), to_sign).as_ref())
}

/// Canonicalize the query parameters of `url` into AWS canonical form: sorted
/// by key, strictly percent-encoded. Mirrors `object_store`'s implementation.
fn canonicalize_query(url: &Url) -> String {
    use std::fmt::Write;

    if url.query().is_none_or(str::is_empty) {
        return String::new();
    }

    let mut pairs = url.query_pairs().collect::<Vec<_>>();
    pairs.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let mut encoded = String::new();
    let mut first = true;
    for (key, value) in pairs {
        if !first {
            encoded.push('&');
        }
        first = false;
        let _ = write!(
            encoded,
            "{}={}",
            utf8_percent_encode(key.as_ref(), &STRICT_ENCODE_SET),
            utf8_percent_encode(value.as_ref(), &STRICT_ENCODE_SET)
        );
    }
    encoded
}

/// Generate a `SigV4` pre-signed URL for `base` (a clean resource URL, i.e.
/// scheme, host and path with no query) authorizing `method` for `expires_in`,
/// binding the supplied `content_length` and/or `content_type` into the
/// signature so the client is forced to send those exact header values.
///
/// `base` is expected to already encode the correct host and path style
/// (path-style vs virtual-hosted), which is why callers derive it from
/// `object_store`'s own signer rather than reconstructing it here.
#[must_use]
pub(crate) fn presign_s3(
    base: &Url,
    method: &Method,
    expires_in: Duration,
    cred: &AwsCredential,
    region: &str,
    headers: BoundHeaders<'_>,
) -> Url {
    presign_s3_at(base, method, expires_in, cred, region, headers, Utc::now())
}

/// As [`presign_s3`], but with an explicit signing timestamp (for testing
/// against fixed `SigV4` vectors).
#[must_use]
fn presign_s3_at(
    base: &Url,
    method: &Method,
    expires_in: Duration,
    cred: &AwsCredential,
    region: &str,
    headers: BoundHeaders<'_>,
    date: DateTime<Utc>,
) -> Url {
    let date_stamp = date.format("%Y%m%d").to_string();
    let amz_date = date.format("%Y%m%dT%H%M%SZ").to_string();
    let scope = format!("{date_stamp}/{region}/{SERVICE}/aws4_request");

    // Headers to sign, sorted by (lower-case) name as required by SigV4.
    let host = base[url::Position::BeforeHost..url::Position::AfterPort].to_string();
    let mut signed: Vec<(&str, String)> = Vec::with_capacity(3);
    if let Some(len) = headers.content_length {
        signed.push(("content-length", len.to_string()));
    }
    if let Some(content_type) = headers.content_type {
        signed.push(("content-type", normalize_whitespace(content_type)));
    }
    signed.push(("host", host));
    signed.sort_by_key(|(name, _)| *name);
    let headers = signed;

    let signed_headers = headers
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(";");

    let mut url = base.clone();
    {
        let mut query = url.query_pairs_mut();
        query
            .append_pair("X-Amz-Algorithm", ALGORITHM)
            .append_pair("X-Amz-Credential", &format!("{}/{scope}", cred.key_id))
            .append_pair("X-Amz-Date", &amz_date)
            .append_pair("X-Amz-Expires", &expires_in.as_secs().to_string())
            .append_pair("X-Amz-SignedHeaders", &signed_headers);

        if let Some(token) = &cred.token {
            query.append_pair("X-Amz-Security-Token", token);
        }
    }

    let canonical_query = canonicalize_query(&url);

    let mut canonical_headers = String::new();
    for (name, value) in &headers {
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        canonical_headers.push_str(value);
        canonical_headers.push('\n');
    }

    // For S3 the path is encoded only once, so it is used verbatim.
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        method.as_str(),
        url.path(),
        canonical_query,
        canonical_headers,
        signed_headers,
        UNSIGNED_PAYLOAD,
    );

    let string_to_sign = format!(
        "{ALGORITHM}\n{amz_date}\n{scope}\n{}",
        hex_digest(canonical_request.as_bytes()),
    );

    let signature = sign_string(cred, &string_to_sign, &date_stamp, region);
    url.query_pairs_mut()
        .append_pair("X-Amz-Signature", &signature);

    url
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_method_from_str() {
        assert_eq!("get".parse::<SignMethod>().unwrap(), SignMethod::Get);
        assert_eq!("PUT".parse::<SignMethod>().unwrap(), SignMethod::Put);
        assert_eq!("Delete".parse::<SignMethod>().unwrap(), SignMethod::Delete);
        assert!("PATCH".parse::<SignMethod>().is_err());
    }

    /// Reproduces the canonical AWS `SigV4` "Example: GET Object" pre-signed URL
    /// from the AWS documentation, pinning the signing timestamp so the
    /// resulting signature can be compared against the published value. This
    /// guards the whole canonical-request / string-to-sign / signing pipeline.
    #[test]
    fn matches_aws_documented_vector() {
        use chrono::TimeZone;

        let cred = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            token: None,
        };
        // Virtual-hosted style base URL, as in the AWS example.
        let base = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        let date = Utc.with_ymd_and_hms(2013, 5, 24, 0, 0, 0).unwrap();

        let signed = presign_s3_at(
            &base,
            &Method::GET,
            Duration::from_hours(24),
            &cred,
            "us-east-1",
            BoundHeaders::default(),
            date,
        );

        let signature = signed
            .query_pairs()
            .find(|(k, _)| k == "X-Amz-Signature")
            .map(|(_, v)| v.into_owned())
            .unwrap();

        assert_eq!(
            signature,
            "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
        );
    }

    fn test_credential() -> AwsCredential {
        AwsCredential {
            key_id: "AKIDEXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            token: None,
        }
    }

    #[test]
    fn presign_binds_content_headers() {
        let base = Url::parse("https://s3.amazonaws.com/my-bucket/some/key.bin").unwrap();
        let signed = presign_s3(
            &base,
            &Method::PUT,
            Duration::from_mins(15),
            &test_credential(),
            "us-east-1",
            BoundHeaders {
                content_length: Some(1234),
                content_type: Some("application/octet-stream"),
            },
        );

        let query = signed.query().unwrap();
        assert!(signed.as_str().contains("X-Amz-Signature="));
        // Signed headers are sorted and include the bound content headers.
        assert!(query.contains("X-Amz-SignedHeaders=content-length%3Bcontent-type%3Bhost"));
        assert!(query.contains("X-Amz-Expires=900"));
    }

    #[test]
    fn presign_host_only_when_no_headers() {
        let base = Url::parse("https://s3.amazonaws.com/my-bucket/key").unwrap();
        let signed = presign_s3(
            &base,
            &Method::GET,
            Duration::from_mins(1),
            &test_credential(),
            "eu-west-1",
            BoundHeaders::default(),
        );

        assert!(signed.query().unwrap().contains("X-Amz-SignedHeaders=host"));
    }

    #[test]
    fn security_token_is_included_when_present() {
        let mut cred = test_credential();
        cred.token = Some("session-token-value".to_string());
        let base = Url::parse("https://s3.amazonaws.com/bucket/key").unwrap();
        let signed = presign_s3(
            &base,
            &Method::PUT,
            Duration::from_mins(5),
            &cred,
            "us-east-1",
            BoundHeaders {
                content_length: Some(10),
                content_type: None,
            },
        );

        assert!(
            signed
                .as_str()
                .contains("X-Amz-Security-Token=session-token-value")
        );
    }
}
