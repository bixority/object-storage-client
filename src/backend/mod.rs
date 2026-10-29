//! Provider-specific storage backends.
//!
//! Each supported URL scheme resolves to a [`Backend`] implementation living in
//! its own module: [`s3`], [`gcs`], [`azure`], [`http`] and [`local`]. Generic
//! object operations (get / put / list / ...) all go through [`Backend::store`];
//! the bucket-management and pre-signing operations that `object_store` does not
//! abstract are implemented per provider by the trait methods below.
//!
//! Backends are built lazily by [`build`] and cached by the
//! [`ObjectStorageClient`](crate::ObjectStorageClient) keyed on (scheme, userinfo, host, port),
//! so each provider's `object_store` — and the credential / HTTP machinery it
//! carries — is constructed once and reused across calls rather than rebuilt on
//! every request.

mod aws_profile;
mod azure;
mod body;
mod connector;
mod gcs;
mod http;
mod local;
mod s3;
#[cfg(test)]
mod stub;

use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
pub use aws_profile::AwsProfile;
pub use body::body_stream;
use bytes::Bytes;
use connector::BoundedConnector;
pub use local::fs_path;
use object_store::client::HttpRequest;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ClientConfigKey, ObjectStore};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// Where backend configuration is read from.
///
/// Production clients use [`Vars::Env`], the process environment. [`Vars::Map`]
/// is an injected, fixed set of variables used by tests so parallel tests never
/// share (or mutate) global environment state.
#[derive(Clone, Default)]
pub enum Vars {
    #[default]
    Env,
    Map(Arc<HashMap<String, String>>),
}

impl Vars {
    /// An injected variable set, for tests.
    #[cfg(test)]
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self::Map(Arc::new(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        ))
    }

    /// The value of `key`, if set.
    pub fn get(&self, key: &str) -> Option<String> {
        match self {
            Self::Env => std::env::var(key).ok(),
            Self::Map(map) => map.get(key).cloned(),
        }
    }

    /// For [`Vars::Map`], the entries whose name starts with `prefix` and whose
    /// lower-cased name parses as a builder config key `K`, mirroring what the
    /// `object_store` builders' `from_env` does with the process environment.
    /// Empty for [`Vars::Env`], where `from_env` does the reading itself.
    fn config_entries<K: FromStr>(&self, prefix: &str) -> Vec<(K, String)> {
        match self {
            Self::Env => Vec::new(),
            Self::Map(map) => map
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .filter_map(|(k, v)| Some((k.to_ascii_lowercase().parse().ok()?, v.clone())))
                .collect(),
        }
    }
}

/// How long a stalled object request may stay silent: the time to the response
/// headers (including sending the request body) and every gap between body reads.
pub const READ_TIMEOUT: Duration = Duration::from_mins(5);

/// The longest an object request may take in total (an effectively unlimited
/// stand-in for `object_store`'s 30 s deadline, which also covers the body).
const TOTAL_TIMEOUT: Duration = Duration::from_hours(24);

/// The client timeouts every object-store builder gets on top of its own
/// defaults: a 24 h total deadline instead of 30 s, and a 5 minute read
/// timeout. A value the user set (`<prefix>TIMEOUT`, `<prefix>READ_TIMEOUT`)
/// is left alone. The connect timeout keeps `object_store`'s 5 s.
fn default_timeouts(vars: &Vars, prefix: &str) -> Vec<(ClientConfigKey, String)> {
    [
        ("TIMEOUT", ClientConfigKey::Timeout, TOTAL_TIMEOUT),
        ("READ_TIMEOUT", ClientConfigKey::ReadTimeout, READ_TIMEOUT),
    ]
    .into_iter()
    .filter(|(name, ..)| vars.get(&format!("{prefix}{name}")).is_none())
    .map(|(_, key, value)| (key, format!("{}s", value.as_secs())))
    .collect()
}

/// A boolean setting in `object_store`'s grammar; `None` for anything else.
fn parse_bool(value: &str) -> Option<bool> {
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "on" | "yes" | "y" => Some(true),
        "0" | "false" | "off" | "no" | "n" => Some(false),
        _ => None,
    }
}

/// A resolved, provider-specific storage backend.
///
/// Implementations are small wrappers over an `object_store` plus whatever extra
/// provider state (concrete store, credentials) is needed for the operations
/// `object_store` does not expose. They are constructed once per (scheme, userinfo, host, port)
/// and shared behind an `Arc`.
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    /// The underlying object store used for generic read / write / list
    /// operations.
    fn store(&self) -> &Arc<dyn ObjectStore>;

    /// The URL scheme this backend was built for, used in error messages.
    fn scheme(&self) -> &str;

    /// For S3 backends, the identity of the credential provider requests are
    /// signed with and the signing region; tests compare them across backends.
    #[cfg(test)]
    fn s3_signing(&self) -> Option<(*const (), String)> {
        None
    }

    /// Whether the store supports multipart uploads. `false` makes
    /// [`upload_stream`](crate::upload::upload_stream) buffer and issue one `put`.
    fn supports_multipart(&self) -> bool {
        true
    }

    /// The file system path `path` maps to, for the local backend on Unix;
    /// `None` for every other backend. Writes to such a path go through the
    /// crate's own private-staging writer instead of the store's `put`.
    fn local_path(&self, _path: &ObjectPath) -> Result<Option<std::path::PathBuf>> {
        Ok(None)
    }

    /// Create the bucket / container identified by `url`.
    ///
    /// Defaults to [`Error::BucketCreationUnsupported`]; providers that support
    /// it override this.
    async fn create_bucket(&self, _client: &reqwest::Client, _url: &Url) -> Result<()> {
        Err(Error::BucketCreationUnsupported(self.scheme().to_string()))
    }

    /// Whether the bucket / container identified by `url` exists.
    ///
    /// Defaults to [`Error::BucketExistenceUnsupported`]; providers that support
    /// it override this.
    async fn bucket_exists(&self, _client: &reqwest::Client, _url: &Url) -> Result<bool> {
        Err(Error::BucketExistenceUnsupported(self.scheme().to_string()))
    }

    /// The `object_store` signer for host-only pre-signing, if the store has one.
    fn signer(&self) -> Option<&dyn Signer> {
        None
    }

    /// Generate pre-signed URLs for several objects sharing this backend.
    ///
    /// Signs host-only through [`Backend::signer`]; backends that bind headers
    /// (S3) override this.
    ///
    /// # Errors
    ///
    /// Returns [`Error::SigningUnsupported`] without a signer, an error if
    /// `options` requests header binding (unsupported off S3), or the signer's
    /// error.
    async fn pre_signed_urls(
        &self,
        paths: &[ObjectPath],
        method: SignMethod,
        expires_in: Duration,
        options: &SignOptions,
    ) -> Result<Vec<String>> {
        let signer = self
            .signer()
            .ok_or_else(|| Error::SigningUnsupported(self.scheme().to_string()))?;
        if !options.is_empty() {
            return Err(crate::sign::content_binding_unsupported());
        }
        let urls = signer
            .signed_urls(method.into_http(), paths, expires_in)
            .await?;
        Ok(urls.into_iter().map(Into::into).collect())
    }
}

/// Build the backend for `url`'s scheme, reading provider configuration from
/// `vars` (the environment unless injected). `profile` is the registry's AWS
/// profile for S3 credentials; other schemes ignore it.
///
/// Called once per registry key; the [`ObjectStorageClient`](crate::ObjectStorageClient)
/// caches the result.
///
/// # Errors
///
/// Returns an error if the scheme is unsupported, required URL components are
/// missing, or the underlying `object_store` fails to build.
pub fn build(url: &Url, vars: &Vars, profile: Option<&AwsProfile>) -> Result<Arc<dyn Backend>> {
    let scheme = url.scheme();
    let host = url.host_str();

    let backend: Arc<dyn Backend> = match scheme {
        "s3" => Arc::new(s3::S3Backend::new(host, vars, profile)?),
        "gs" | "gcs" => Arc::new(gcs::GcsBackend::new(scheme, host, vars)?),
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
            Arc::new(azure::AzureBackend::new(scheme, host, vars)?)
        }
        "http" | "https" => Arc::new(http::HttpBackend::new(scheme, url)?),
        "file" => Arc::new(local::LocalBackend::new()),
        _ => return Err(Error::UnsupportedScheme(scheme.to_string())),
    };

    Ok(backend)
}

/// Interpret the HTTP response of a direct bucket / container existence probe,
/// shared by the cloud backends.
///
/// These probes hit each provider's dedicated metadata endpoint — S3
/// `HeadBucket`, Azure `Get Container Properties`, GCS `buckets.get` — rather
/// than listing the bucket's contents, so they avoid the `list` payload and the
/// `s3:ListBucket`-style permission a listing requires.
///
/// A `2xx` means the bucket exists. `404` means it does not. `401` / `403` mean
/// the request was denied: the provider answers a wrong key or signature the
/// same way, so existence is unknown and the call fails. Any other status is
/// an error too.
pub async fn bucket_exists_from_response(
    response: reqwest::Response,
    scheme: &str,
) -> Result<bool> {
    let status = response.status();
    if status.is_success() {
        return Ok(true);
    }
    match status.as_u16() {
        404 => Ok(false),
        401 | 403 => {
            // HEAD responses carry no body; Azure names the reason in a header.
            let code = response
                .headers()
                .get("x-ms-error-code")
                .and_then(|v| v.to_str().ok())
                .map(|c| {
                    c.chars()
                        .filter(|c| !c.is_control())
                        .take(64)
                        .collect::<String>()
                })
                .map_or_else(String::new, |c| format!(", {c}"));
            Err(Error::Generic(format!(
                "{scheme} bucket existence check was denied (HTTP {status}{code}); \
                 existence is unknown, check the credentials and permissions"
            )))
        }
        _ => {
            let text = error_body(response).await;
            Err(Error::Generic(format!(
                "{scheme} bucket existence check failed (HTTP {status}): {text}"
            )))
        }
    }
}

/// The start of an error response's body, for messages: at most 4 KiB read,
/// with control and bidi characters escaped (`crate::text::escape`). A server
/// that answers with a huge or hostile body costs neither memory nor a
/// corrupted terminal.
pub async fn error_body(mut response: reqwest::Response) -> String {
    const LIMIT: usize = 4096;
    let mut body = Vec::new();
    while body.len() < LIMIT
        && let Ok(Some(chunk)) = response.chunk().await
    {
        body.extend_from_slice(&chunk);
    }
    body.truncate(LIMIT);
    crate::text::escape(&redact_error_body(&String::from_utf8_lossy(&body)), LIMIT)
}

/// The elements of an XML error document that are kept.
const KEPT_ELEMENTS: [&str; 7] = [
    "Code",
    "Message",
    "RequestId",
    "HostId",
    "Resource",
    "Key",
    "BucketName",
];

/// Withholds signing details from a provider's XML error document.
///
/// On `SignatureDoesNotMatch` S3 echoes the canonical request, which holds the
/// signed `x-amz-security-token`, and Azure's `AuthenticationErrorDetail`
/// carries the string to sign. A document whose root is `Error` is rebuilt
/// from the [`KEPT_ELEMENTS`] only, so `CanonicalRequest`, `StringToSign`,
/// `Token-N` and every other element are dropped, and so is an element whose
/// closing tag was cut off. `Code` stays, which the bucket-creation checks
/// need. Any other text (a JSON error, plain text) is returned unchanged.
pub fn redact_error_body(text: &str) -> String {
    let mut rest = text.trim_start();
    if rest.starts_with("<?xml")
        && let Some(end) = rest.find("?>")
    {
        rest = rest[end + 2..].trim_start();
    }
    let Some(mut rest) = rest.strip_prefix("<Error>") else {
        return text.to_owned();
    };
    let mut out = String::from("<Error>");
    // `rest` is positioned between the children of the root.
    while let Some(open) = rest.find('<') {
        rest = &rest[open + 1..];
        let Some(tag_end) = rest.find('>') else { break };
        let tag = &rest[..tag_end];
        rest = &rest[tag_end + 1..];
        if tag.starts_with(['/', '?', '!']) || tag.ends_with('/') {
            continue;
        }
        let name = tag.split_whitespace().next().unwrap_or_default();
        let closing = format!("</{name}>");
        let Some(close) = rest.find(&closing) else {
            break;
        };
        let content = &rest[..close];
        if KEPT_ELEMENTS.contains(&name) && !content.contains('<') {
            let _ = write!(out, "<{name}>{content}{closing}");
        }
        rest = &rest[close + closing.len()..];
    }
    out.push_str("</Error>");
    out
}

/// Sends a request already signed by an `object_store` authorizer.
///
/// The authorizer mutates an [`HttpRequest`] (method, URI and the signed
/// headers); this transfers those onto the shared `reqwest` client and
/// dispatches them, re-attaching the body the signature was computed over.
pub async fn send_signed(
    client: &reqwest::Client,
    request: HttpRequest,
    body: Bytes,
) -> Result<reqwest::Response> {
    let (parts, _) = request.into_parts();
    client
        .request(parts.method, parts.uri.to_string())
        .headers(parts.headers)
        .body(body)
        .send()
        .await
        .map_err(|e| request_failed("Signed request", e))
}

/// Formats a transport failure of a direct request.
///
/// `reqwest` appends the full request URL to its error text, and authorizers
/// may carry a credential in the query string (an Azure SAS `sig=`). The URL
/// is therefore replaced by its redacted form: scheme, host, port and path.
pub fn request_failed(what: &str, e: reqwest::Error) -> Error {
    let location = e
        .url()
        .map_or_else(|| "the endpoint".to_owned(), crate::codec::redacted);
    let e = e.without_url();
    Error::Generic(format!("{what} to {location} failed: {e}"))
}

/// Wraps an arbitrary store as a [`Backend`] for unit tests.
#[cfg(test)]
pub struct TestBackend {
    pub store: Arc<dyn ObjectStore>,
    pub multipart: bool,
}

#[cfg(test)]
#[async_trait::async_trait]
impl Backend for TestBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &'static str {
        "test"
    }

    fn supports_multipart(&self) -> bool {
        self.multipart
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn transport_errors_never_carry_the_query() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        drop(listener);
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let e = client
            .get(format!(
                "http://127.0.0.1:{port}/c?restype=container&sig=TOPSECRET"
            ))
            .send()
            .await
            .expect_err("nothing listens");
        let err = request_failed("Signed request", e);
        let text = format!("{err} {err:?}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        assert!(!text.contains("restype"), "{text}");
        assert!(text.contains("127.0.0.1"), "{text}");
    }

    async fn error_body_of(body: Vec<u8>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let (mut conn, _) = listener.accept().await.expect("accept");
            let _ = conn.read(&mut [0; 1024]).await;
            let head = format!(
                "HTTP/1.1 500 Oops\r\nContent-Length: {}\r\n\r\n",
                body.len()
            );
            let _ = conn.write_all(head.as_bytes()).await;
            let _ = conn.write_all(&body).await;
        });
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let response = client
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect("get");
        error_body(response).await
    }

    /// `bucket_exists_from_response` on a HEAD answered with `reply`.
    async fn probe(reply: stub::Reply) -> Result<bool> {
        let stub = stub::serve(vec![reply]).await;
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .expect("client");
        let response = client.head(&stub.base).send().await.expect("head");
        bucket_exists_from_response(response, "Azure").await
    }

    #[tokio::test]
    async fn denied_probes_are_errors() {
        let err = probe(stub::Reply::new(401, "")).await.expect_err("401");
        assert!(err.to_string().contains("401"), "{err}");

        let err =
            probe(stub::Reply::new(403, "").header("x-ms-error-code", "AuthenticationFailed"))
                .await
                .expect_err("403 with a code");
        let text = err.to_string();
        assert!(
            text.contains("403") && text.contains("AuthenticationFailed"),
            "{text}"
        );

        let err = probe(stub::Reply::new(403, "")).await.expect_err("403");
        let text = err.to_string();
        assert!(text.contains("403") && text.contains("denied"), "{text}");
    }

    #[tokio::test]
    async fn found_and_missing_buckets() {
        assert!(probe(stub::Reply::new(200, "")).await.expect("200"));
        assert!(!probe(stub::Reply::new(404, "")).await.expect("404"));
    }

    #[tokio::test]
    async fn error_bodies_are_capped_and_stripped_of_control_characters() {
        let mut body = b"\x1b[31mred\x07 text\n".to_vec();
        body.extend(std::iter::repeat_n(b'a', 100_000));
        let text = error_body_of(body).await;
        assert!(
            text.starts_with("\\u{1b}[31mred\\u{7} text\\u{a}a"),
            "{text}"
        );
        assert!(text.contains("...[+"), "{text}");
        assert!(text.len() < 4096 + 32, "{} bytes", text.len());
        assert!(!text.chars().any(crate::text::is_unsafe_char));

        let text = error_body_of("a\u{202E}b\u{2066}".as_bytes().to_vec()).await;
        assert_eq!(text, "a\\u{202e}b\\u{2066}");

        assert_eq!(error_body_of(b"short".to_vec()).await, "short");
    }

    const SENTINEL: &str = "SENTINEL-TOKEN-0123";

    fn hex(text: &str) -> String {
        crate::hex::encode(text.as_bytes())
    }

    fn s3_signature_error() -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\
             <Code>SignatureDoesNotMatch</Code>\
             <Message>The request signature we calculated does not match</Message>\
             <AWSAccessKeyId>AKIDEXAMPLE</AWSAccessKeyId>\
             <StringToSign>AWS4-HMAC-SHA256 {SENTINEL}</StringToSign>\
             <SignatureProvided>abc</SignatureProvided>\
             <CanonicalRequest>GET\n/k\nx-amz-security-token:{SENTINEL}\n</CanonicalRequest>\
             <CanonicalRequestBytes>{}</CanonicalRequestBytes>\
             <Token-0>{SENTINEL}</Token-0>\
             <RequestId>R1</RequestId><HostId>H1</HostId></Error>",
            hex(SENTINEL)
        )
    }

    #[test]
    fn signature_mismatch_bodies_lose_the_canonical_request() {
        let text = redact_error_body(&s3_signature_error());
        assert!(!text.contains(SENTINEL), "{text}");
        assert!(!text.contains(&hex(SENTINEL)), "{text}");
        assert!(
            text.contains("<Code>SignatureDoesNotMatch</Code>"),
            "{text}"
        );
        assert!(text.contains("does not match</Message>"), "{text}");
        assert!(text.contains("<RequestId>R1</RequestId>"), "{text}");
    }

    #[test]
    fn azure_authentication_detail_is_dropped() {
        let body = format!(
            "<?xml version=\"1.0\"?><Error><Code>AuthenticationFailed</Code>\
             <Message>Server failed to authenticate the request.</Message>\
             <AuthenticationErrorDetail>The MAC signature found in the HTTP request is not \
             the same. String-to-Sign: x-ms-encryption-key:{SENTINEL}</AuthenticationErrorDetail>\
             </Error>"
        );
        let text = redact_error_body(&body);
        assert!(!text.contains(SENTINEL), "{text}");
        assert!(text.contains("<Code>AuthenticationFailed</Code>"), "{text}");
    }

    #[test]
    fn a_body_cut_mid_element_stays_clean() {
        let body = s3_signature_error();
        let cut = body.find(SENTINEL).expect("sentinel") + 4;
        // Cut inside StringToSign, then inside CanonicalRequest.
        for end in [cut, body.rfind("CanonicalRequest>").expect("tag") - 10] {
            let text = redact_error_body(&body[..end]);
            assert!(!text.contains("SENTINEL"), "{text}");
            assert!(text.contains("SignatureDoesNotMatch"), "{text}");
        }
    }

    #[test]
    fn non_xml_bodies_pass_through() {
        let json = r#"{"error":{"code":403,"message":"Access denied: <b>x</b>"}}"#;
        assert_eq!(redact_error_body(json), json);
        assert_eq!(redact_error_body("plain <text>"), "plain <text>");
        assert_eq!(redact_error_body(""), "");
    }
}
