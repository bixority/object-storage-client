//! An HTTP connector that never lets a provider size an allocation.
//!
//! `object_store` reads error bodies whole, and preallocates a buffer from
//! the response's declared length. A storage endpoint (or any URL the service
//! is asked to read) that declares an absurd `Content-Length` would abort the
//! process on the allocation. [`BoundedConnector`] wraps another connector so
//! that every response is safe to collect: successful bodies report a
//! size hint that is capped, and error bodies are cut to a few KiB.

use super::{Vars, parse_bool};
use crate::client::Error;
use crate::codec::MAX_PREALLOC;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use http_body::{Body, Frame, SizeHint};
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    ReqwestConnector,
};
use object_store::{ClientConfigKey, ClientOptions};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::task::{Context, Poll};
use std::time::Duration;

/// The most of an error response body that is kept.
const ERROR_BODY_LIMIT: usize = 4096;
/// How long reading an error body may take.
const ERROR_BODY_DEADLINE: Duration = Duration::from_secs(5);

/// How many clients a [`ClientCache`] keeps. A registry needs a handful (the
/// data, STS and metadata clients of each provider); at the cap the cache is
/// emptied, and later stores build theirs again.
const MAX_SHARED_CLIENTS: usize = 32;

/// Wraps `C` so that its responses are bounded; see the module documentation.
#[derive(Debug)]
pub struct BoundedConnector<C = ReqwestConnector>(C);

impl BoundedConnector<SharedConnector> {
    /// A connector whose clients come from `cache`.
    pub fn shared(cache: &Arc<ClientCache>) -> Self {
        Self(SharedConnector {
            inner: ReqwestConnector::default(),
            cache: Arc::clone(cache),
        })
    }
}

impl<C: HttpConnector> HttpConnector for BoundedConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.0.connect(options)?;
        Ok(HttpClient::new(Bounded { inner }))
    }
}

/// The `HttpClient`s of a registry, keyed on the options they were built with.
///
/// Building a client loads the system CA store and starts a connection pool,
/// so stores with the same options share one. The options are the key because
/// `object_store` asks for different clients (data, STS, instance metadata)
/// with different options, and `ClientOptions` has no `Hash`: the key is its
/// `Debug` text. The key can hold a proxy URL with credentials, so it is never
/// logged or printed.
#[derive(Default)]
pub struct ClientCache {
    clients: Mutex<HashMap<String, HttpClient>>,
}

impl std::fmt::Debug for ClientCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never the keys.
        f.debug_struct("ClientCache").finish_non_exhaustive()
    }
}

/// Builds clients with `C`, once per distinct set of options in its cache.
#[derive(Debug)]
pub struct SharedConnector<C = ReqwestConnector> {
    inner: C,
    cache: Arc<ClientCache>,
}

impl<C: HttpConnector> HttpConnector for SharedConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let key = format!("{options:?}");
        let lock = || {
            self.cache
                .clients
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
        };
        if let Some(client) = lock().get(&key) {
            return Ok(client.clone());
        }
        // Built outside the lock: it reads the CA store. A racing build is
        // dropped and both callers get the first one stored.
        let client = self.inner.connect(options)?;
        let mut clients = lock();
        if clients.len() >= MAX_SHARED_CLIENTS && !clients.contains_key(&key) {
            clients.clear();
        }
        Ok(clients.entry(key).or_insert(client).clone())
    }
}

#[derive(Debug)]
struct Bounded {
    inner: HttpClient,
}

#[async_trait]
impl HttpService for Bounded {
    async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
        let response = self.inner.execute(req).await?;
        let status = response.status();
        let (parts, body) = response.into_parts();
        let body = if status.is_success() || status.is_redirection() {
            HttpResponseBody::new(Clamped(body))
        } else {
            HttpResponseBody::from(error_text(body).await)
        };
        Ok(HttpResponse::from_parts(parts, body))
    }
}

/// At most [`ERROR_BODY_LIMIT`] bytes of `body`, as valid UTF-8 (the callers
/// decode it as text). Reading stops at the end, at the first error and after
/// [`ERROR_BODY_DEADLINE`]; what was read by then is kept.
async fn error_text(body: HttpResponseBody) -> Bytes {
    let mut kept = Vec::new();
    let mut chunks = body.bytes_stream();
    let read = async {
        while kept.len() < ERROR_BODY_LIMIT
            && let Some(Ok(chunk)) = chunks.next().await
        {
            let room = ERROR_BODY_LIMIT - kept.len();
            kept.extend_from_slice(&chunk[..chunk.len().min(room)]);
        }
    };
    let _ = tokio::time::timeout(ERROR_BODY_DEADLINE, read).await;
    let text = super::redact_error_body(&String::from_utf8_lossy(&kept));
    let end = text.floor_char_boundary(ERROR_BODY_LIMIT);
    Bytes::copy_from_slice(&text.as_bytes()[..end])
}

/// The transport settings of a store that its bucket calls follow: the
/// `ClientConfigKey`s of the store's builder that `reqwest` supports. They are
/// read before the store is built, because a built store does not expose them.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "a mirror of independent on/off client settings"
)]
pub struct Transport {
    pub allow_http: bool,
    pub allow_invalid_certificates: bool,
    pub no_system_certificates: bool,
    pub http1_only: bool,
    pub http2_only: bool,
    pub proxy_url: Option<String>,
    pub proxy_ca_certificate: Option<String>,
    pub proxy_excludes: Option<String>,
    pub user_agent: Option<String>,
    /// Ignore the proxy variables of the process. Injected variables never
    /// read the process environment, and neither do their proxy settings.
    pub no_system_proxy: bool,
}

impl Transport {
    /// The settings `get` returns for the builder's client keys.
    pub fn read(get: impl Fn(ClientConfigKey) -> Option<String>, vars: &Vars) -> Self {
        let flag = |key| get(key).and_then(|v| parse_bool(&v)).unwrap_or(false);
        Self {
            allow_http: flag(ClientConfigKey::AllowHttp),
            allow_invalid_certificates: flag(ClientConfigKey::AllowInvalidCertificates),
            no_system_certificates: flag(ClientConfigKey::NoSystemCertificates),
            http1_only: flag(ClientConfigKey::Http1Only),
            http2_only: flag(ClientConfigKey::Http2Only),
            proxy_url: get(ClientConfigKey::ProxyUrl),
            proxy_ca_certificate: get(ClientConfigKey::ProxyCaCertificate),
            proxy_excludes: get(ClientConfigKey::ProxyExcludes),
            user_agent: get(ClientConfigKey::UserAgent),
            no_system_proxy: matches!(vars, Vars::Map(_)),
        }
    }

    /// The bucket-call client for these settings. A server that does not
    /// answer cannot hang a call: connecting is given 5 seconds and a whole
    /// request 30, whatever timeouts the store has. It does not follow
    /// redirects: a signed request is never re-sent elsewhere, so a 3xx is an
    /// error.
    fn client(&self) -> crate::client::Result<reqwest::Client> {
        let failed =
            |e: reqwest::Error| Error::Generic(format!("HTTP client: {}", e.without_url()));
        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .redirect(reqwest::redirect::Policy::none())
            .https_only(!self.allow_http);
        if let Some(agent) = &self.user_agent {
            builder = builder.user_agent(agent);
        }
        if self.no_system_proxy {
            builder = builder.no_proxy();
        }
        let mut certificates = Vec::new();
        if let Some(url) = &self.proxy_url {
            let mut proxy = reqwest::Proxy::all(url).map_err(failed)?;
            if let Some(excludes) = &self.proxy_excludes {
                proxy = proxy.no_proxy(reqwest::NoProxy::from_string(excludes));
            }
            if let Some(pem) = &self.proxy_ca_certificate {
                certificates
                    .push(reqwest::tls::Certificate::from_pem(pem.as_bytes()).map_err(failed)?);
            }
            builder = builder.proxy(proxy);
        }
        builder = if self.no_system_certificates {
            builder.tls_certs_only(certificates)
        } else {
            builder.tls_certs_merge(certificates)
        };
        if self.http1_only {
            builder = builder.http1_only();
        }
        if self.http2_only {
            builder = builder.http2_prior_knowledge();
        }
        if self.allow_invalid_certificates {
            builder = builder.danger_accept_invalid_certs(true);
        }
        builder.build().map_err(failed)
    }
}

/// A store's own client for its bucket calls (`create_bucket`,
/// `bucket_exists`), built from the store's [`Transport`] on first use.
pub struct BucketClient {
    transport: Transport,
    client: OnceLock<reqwest::Client>,
}

impl BucketClient {
    pub const fn new(transport: Transport) -> Self {
        Self {
            transport,
            client: OnceLock::new(),
        }
    }

    /// The settings the client is built from.
    #[cfg(test)]
    pub const fn transport(&self) -> &Transport {
        &self.transport
    }

    /// The client, built on first use.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Generic`] if the client cannot be built: an invalid
    /// proxy URL or certificate, or TLS or resolver initialisation fails.
    pub fn get(&self) -> crate::client::Result<&reqwest::Client> {
        if let Some(client) = self.client.get() {
            return Ok(client);
        }
        let client = self.transport.client()?;
        // A racing builder simply loses; `OnceLock::get_or_try_init` is unstable.
        Ok(self.client.get_or_init(|| client))
    }
}

/// A body whose size hint cannot ask for more than [`MAX_PREALLOC`] bytes up
/// front: `collect_bytes` reserves the hint's lower bound.
#[derive(Debug)]
struct Clamped(HttpResponseBody);

impl Body for Clamped {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
        Pin::new(&mut self.0).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.0.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let inner = self.0.size_hint();
        let mut hint = SizeHint::new();
        if let Some(upper) = inner.upper() {
            hint.set_upper(upper);
        }
        let cap = u64::try_from(MAX_PREALLOC).unwrap_or(u64::MAX);
        hint.set_lower(inner.lower().min(cap).min(hint.upper().unwrap_or(u64::MAX)));
        hint
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::aws::AmazonS3Builder;
    use object_store::http::HttpBuilder;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt, RetryConfig};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A body that carries `frames` but declares an exact size of 2^50.
    struct Liar(VecDeque<Bytes>);

    impl Body for Liar {
        type Data = Bytes;
        type Error = HttpError;

        fn poll_frame(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Bytes>, HttpError>>> {
            Poll::Ready(self.0.pop_front().map(|b| Ok(Frame::data(b))))
        }

        fn size_hint(&self) -> SizeHint {
            SizeHint::with_exact(1 << 50)
        }
    }

    /// Answers every request with `status` and two 3000-byte frames under a
    /// body that claims to be 2^50 bytes long.
    #[derive(Debug, Clone)]
    struct Hostile {
        status: u16,
    }

    #[async_trait]
    impl HttpService for Hostile {
        async fn call(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let frames =
                VecDeque::from([Bytes::from(vec![b'x'; 3000]), Bytes::from(vec![b'y'; 3000])]);
            let mut response = HttpResponse::new(HttpResponseBody::new(Liar(frames)));
            *response.status_mut() = http::StatusCode::from_u16(self.status).expect("status");
            Ok(response)
        }
    }

    impl HttpConnector for Hostile {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    fn bounded(status: u16) -> BoundedConnector<Hostile> {
        BoundedConnector(Hostile { status })
    }

    /// Counts the clients its inner connector is asked to build.
    #[derive(Debug)]
    struct Counting(Arc<AtomicUsize>);

    impl HttpConnector for Counting {
        fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Hostile { status: 200 }.connect(options)
        }
    }

    /// A store for `bucket` whose clients come from `cache`.
    fn s3_over(cache: &Arc<ClientCache>, built: &Arc<AtomicUsize>, bucket: &str, allow_http: bool) {
        let connector = BoundedConnector(SharedConnector {
            inner: Counting(Arc::clone(built)),
            cache: Arc::clone(cache),
        });
        AmazonS3Builder::new()
            .with_bucket_name(bucket)
            .with_region("us-east-1")
            .with_access_key_id("k")
            .with_secret_access_key("s")
            .with_endpoint("http://example.invalid")
            .with_allow_http(allow_http)
            .with_http_connector(connector)
            .build()
            .expect("s3");
    }

    #[test]
    fn stores_with_the_same_options_share_a_client() {
        let cache = Arc::<ClientCache>::default();
        let built = Arc::<AtomicUsize>::default();
        let count = || built.load(Ordering::SeqCst);
        s3_over(&cache, &built, "a", true);
        assert_eq!(count(), 1);
        // Another bucket, the same options.
        s3_over(&cache, &built, "b", true);
        assert_eq!(count(), 1);
        // Other options need their own client.
        s3_over(&cache, &built, "a", false);
        assert_eq!(count(), 2);
        s3_over(&cache, &built, "c", false);
        assert_eq!(count(), 2);
    }

    #[test]
    fn a_full_client_cache_starts_over() {
        let cache = Arc::<ClientCache>::default();
        let built = Arc::<AtomicUsize>::default();
        let count = || built.load(Ordering::SeqCst);
        let connector = SharedConnector {
            inner: Counting(Arc::clone(&built)),
            cache: Arc::clone(&cache),
        };
        for n in 0..MAX_SHARED_CLIENTS {
            let options = ClientOptions::new()
                .with_user_agent(format!("agent-{n}").try_into().expect("agent"));
            connector.connect(&options).expect("connect");
        }
        assert_eq!(count(), MAX_SHARED_CLIENTS);
        connector.connect(&ClientOptions::new()).expect("connect");
        assert_eq!(cache.clients.lock().expect("lock").len(), 1);
        assert!(!format!("{cache:?}").contains("agent"));
    }

    fn request() -> HttpRequest {
        http::Request::new(object_store::client::HttpRequestBody::empty())
    }

    #[tokio::test]
    async fn oversized_error_bodies_are_capped() {
        let client = bounded(403)
            .connect(&ClientOptions::new())
            .expect("connect");
        let response = client.execute(request()).await.expect("response");
        assert_eq!(response.status(), 403);
        let body = response.into_body().bytes().await.expect("body");
        assert_eq!(body.len(), ERROR_BODY_LIMIT);

        let client = bounded(200)
            .connect(&ClientOptions::new())
            .expect("connect");
        let response = client.execute(request()).await.expect("response");
        let hint = response.body().size_hint();
        assert!(hint.lower() <= MAX_PREALLOC as u64, "{hint:?}");
        assert_eq!(hint.upper(), Some(1 << 50));
    }

    #[tokio::test]
    async fn stores_survive_a_hostile_content_length() {
        let retry = RetryConfig {
            max_retries: 0,
            ..RetryConfig::default()
        };
        let s3 = AmazonS3Builder::new()
            .with_bucket_name("b")
            .with_endpoint("http://example.invalid")
            .with_allow_http(true)
            .with_skip_signature(true)
            .with_region("us-east-1")
            .with_retry(retry.clone())
            .with_http_connector(bounded(403))
            .build()
            .expect("s3");
        let http = HttpBuilder::new()
            .with_url("http://example.invalid")
            .with_client_options(ClientOptions::new().with_allow_http(true))
            .with_retry(retry)
            .with_http_connector(bounded(403))
            .build()
            .expect("http");
        let stores: [&dyn ObjectStore; 2] = [&s3, &http];
        for store in stores {
            let path = Path::from("k");
            let errors = [
                store.get(&path).await.map(drop).expect_err("get"),
                store
                    .put(&path, "x".into())
                    .await
                    .map(drop)
                    .expect_err("put"),
                store
                    .list_with_delimiter(None)
                    .await
                    .map(drop)
                    .expect_err("list"),
            ];
            for e in errors {
                let text = e.to_string();
                assert!(text.len() < 8192, "{} bytes", text.len());
            }
        }
    }

    /// Answers 403 `SignatureDoesNotMatch` with a body that echoes the
    /// request's `x-amz-security-token` header, as S3 does in its canonical
    /// request.
    #[derive(Debug, Clone)]
    struct Echo;

    #[async_trait]
    impl HttpService for Echo {
        async fn call(&self, req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let token = req
                .headers()
                .get("x-amz-security-token")
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default();
            let body = format!(
                "<Error><Code>SignatureDoesNotMatch</Code><Message>mismatch</Message>\
                 <CanonicalRequest>GET\n/k\nx-amz-security-token:{token}\n</CanonicalRequest>\
                 </Error>"
            );
            let mut response = HttpResponse::new(HttpResponseBody::from(Bytes::from(body)));
            *response.status_mut() = http::StatusCode::FORBIDDEN;
            Ok(response)
        }
    }

    impl HttpConnector for Echo {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn an_s3_signature_error_does_not_leak_the_session_token() {
        const SENTINEL: &str = "SENTINEL-SESSION-TOKEN";
        let s3 = AmazonS3Builder::new()
            .with_bucket_name("b")
            .with_endpoint("http://example.invalid")
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_access_key_id("AKIDEXAMPLE")
            .with_secret_access_key("secret")
            .with_token(SENTINEL)
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .with_http_connector(BoundedConnector(Echo))
            .build()
            .expect("s3");
        let err = s3.get(&Path::from("k")).await.map(drop).expect_err("403");
        let (display, debug) = (err.to_string(), format!("{err:?}"));
        assert!(!display.contains(SENTINEL), "{display}");
        assert!(!debug.contains(SENTINEL), "{debug}");
        assert!(display.contains("SignatureDoesNotMatch"), "{display}");
    }

    /// Answers 403 with a body that starts with the UTF-8 byte order mark, as
    /// Azure does, and holds a signing detail.
    #[derive(Debug, Clone)]
    struct Bom;

    #[async_trait]
    impl HttpService for Bom {
        async fn call(&self, _req: HttpRequest) -> Result<HttpResponse, HttpError> {
            let mut body = b"\xEF\xBB\xBF".to_vec();
            body.extend_from_slice(
                b"<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>AuthenticationFailed</Code>\
                  <AuthenticationErrorDetail>x-ms-encryption-key:SENTINEL-CPK\
                  </AuthenticationErrorDetail></Error>",
            );
            let mut response = HttpResponse::new(HttpResponseBody::from(Bytes::from(body)));
            *response.status_mut() = http::StatusCode::FORBIDDEN;
            Ok(response)
        }
    }

    impl HttpConnector for Bom {
        fn connect(&self, _options: &ClientOptions) -> object_store::Result<HttpClient> {
            Ok(HttpClient::new(self.clone()))
        }
    }

    #[tokio::test]
    async fn a_byte_order_mark_does_not_bypass_the_redaction() {
        let s3 = AmazonS3Builder::new()
            .with_bucket_name("b")
            .with_endpoint("http://example.invalid")
            .with_allow_http(true)
            .with_region("us-east-1")
            .with_access_key_id("AKIDEXAMPLE")
            .with_secret_access_key("secret")
            .with_retry(RetryConfig {
                max_retries: 0,
                ..RetryConfig::default()
            })
            .with_http_connector(BoundedConnector(Bom))
            .build()
            .expect("s3");
        let err = s3.get(&Path::from("k")).await.map(drop).expect_err("403");
        let (display, debug) = (err.to_string(), format!("{err:?}"));
        assert!(!display.contains("SENTINEL-CPK"), "{display}");
        assert!(!debug.contains("SENTINEL-CPK"), "{debug}");
        assert!(display.contains("AuthenticationFailed"), "{display}");
    }

    fn plain() -> Transport {
        Transport {
            allow_http: true,
            no_system_proxy: true,
            ..Transport::default()
        }
    }

    #[test]
    fn the_transport_is_read_from_the_stores_client_keys() {
        use object_store::aws::AmazonS3ConfigKey;
        let vars = Vars::from_pairs(&[
            ("AWS_ALLOW_INVALID_CERTIFICATES", "true"),
            ("AWS_PROXY_URL", "http://proxy.invalid:3128"),
            ("AWS_PROXY_EXCLUDES", "localhost"),
            ("AWS_USER_AGENT", "agent/1"),
            ("AWS_HTTP2_ONLY", "true"),
            ("AWS_DISABLE_SYSTEM_CERTIFICATES", "true"),
        ]);
        let builder = vars
            .config_entries::<AmazonS3ConfigKey>("AWS_")
            .into_iter()
            .fold(AmazonS3Builder::new(), |b, (k, v)| b.with_config(k, v));
        let got = Transport::read(
            |k| builder.get_config_value(&AmazonS3ConfigKey::Client(k)),
            &vars,
        );
        assert_eq!(
            got,
            Transport {
                allow_http: false,
                allow_invalid_certificates: true,
                no_system_certificates: true,
                // `object_store`'s own default.
                http1_only: true,
                http2_only: true,
                proxy_url: Some("http://proxy.invalid:3128".into()),
                proxy_ca_certificate: None,
                proxy_excludes: Some("localhost".into()),
                user_agent: Some("agent/1".into()),
                no_system_proxy: true,
            }
        );
        assert!(!Transport::read(|_| None, &Vars::Env).no_system_proxy);
    }

    #[tokio::test]
    async fn the_proxy_setting_reaches_the_bucket_client() {
        use crate::backend::stub::{Reply, serve};
        let proxy = serve(vec![Reply::new(200, "")]).await;
        let transport = Transport {
            proxy_url: Some(proxy.base.clone()),
            user_agent: Some("agent/1".into()),
            ..plain()
        };
        let client = BucketClient::new(transport);
        let response = client
            .get()
            .expect("client")
            .get("http://bucket.invalid:9/x")
            .send()
            .await
            .expect("through the proxy");
        assert_eq!(response.status(), 200);
        let seen = proxy.requests();
        assert_eq!(seen[0].line, "GET http://bucket.invalid:9/x HTTP/1.1");
        assert!(
            seen[0]
                .headers
                .to_ascii_lowercase()
                .contains("user-agent: agent/1"),
            "{}",
            seen[0].headers
        );
    }

    #[test]
    fn a_bad_proxy_is_an_error_when_the_client_is_first_needed() {
        let client = BucketClient::new(Transport {
            proxy_url: Some("not a url".into()),
            ..plain()
        });
        let err = client.get().expect_err("bad proxy");
        assert!(err.to_string().contains("HTTP client"), "{err}");
    }

    #[tokio::test]
    async fn plain_http_needs_allow_http() {
        let client = BucketClient::new(Transport {
            no_system_proxy: true,
            ..Transport::default()
        });
        let err = client
            .get()
            .expect("client")
            .get("http://127.0.0.1:9/")
            .send()
            .await
            .expect_err("https only");
        assert!(err.is_builder(), "{err}");
    }

    #[tokio::test]
    async fn the_bucket_client_does_not_follow_redirects() {
        use crate::backend::stub::{Reply, serve};
        let target = serve(vec![Reply::new(200, "")]).await;
        let location: &'static str = Box::leak(format!("{}/x", target.base).into_boxed_str());
        let first = serve(vec![Reply::new(302, "").header("Location", location)]).await;
        let client = BucketClient::new(plain());
        let response = client
            .get()
            .expect("client")
            .get(format!("{}/x", first.base))
            .send()
            .await
            .expect("answer");
        assert_eq!(response.status(), 302);
        assert!(target.requests().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_server_cannot_hang_a_bucket_request() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        // Accepts and then says nothing.
        let silent = tokio::spawn(async move {
            let (conn, _) = listener.accept().await.expect("accept");
            std::future::pending::<()>().await;
            drop(conn);
        });
        let client = BucketClient::new(plain());
        let err = client
            .get()
            .expect("client")
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect_err("timed out");
        assert!(err.is_timeout(), "{err}");
        silent.abort();
    }
}
