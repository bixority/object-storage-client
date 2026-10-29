//! An HTTP connector that never lets a provider size an allocation.
//!
//! `object_store` reads error bodies whole, and preallocates a buffer from
//! the response's declared length. A storage endpoint (or any URL the service
//! is asked to read) that declares an absurd `Content-Length` would abort the
//! process on the allocation. [`BoundedConnector`] wraps another connector so
//! that every response is safe to collect: successful bodies report a
//! size hint that is capped, and error bodies are cut to a few KiB.

use crate::codec::MAX_PREALLOC;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use http_body::{Body, Frame, SizeHint};
use object_store::ClientOptions;
use object_store::client::{
    HttpClient, HttpConnector, HttpError, HttpRequest, HttpResponse, HttpResponseBody, HttpService,
    ReqwestConnector,
};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// The most of an error response body that is kept.
const ERROR_BODY_LIMIT: usize = 4096;
/// How long reading an error body may take.
const ERROR_BODY_DEADLINE: Duration = Duration::from_secs(5);

/// Wraps `C` so that its responses are bounded; see the module documentation.
#[derive(Debug)]
pub struct BoundedConnector<C = ReqwestConnector>(C);

impl Default for BoundedConnector {
    fn default() -> Self {
        Self(ReqwestConnector::default())
    }
}

impl<C: HttpConnector> HttpConnector for BoundedConnector<C> {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let inner = self.0.connect(options)?;
        Ok(HttpClient::new(Bounded { inner }))
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
}
