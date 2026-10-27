//! Plain HTTP / `WebDAV` backend.
//!
//! Supports only generic object operations; bucket management and pre-signing
//! fall back to the [`Backend`] trait's "unsupported" defaults.

use super::Backend;
use crate::client::Result;
use object_store::http::HttpBuilder;
use object_store::{ClientOptions, ObjectStore};
use std::sync::Arc;
use url::Url;

pub struct HttpBackend {
    store: Arc<dyn ObjectStore>,
    scheme: String,
}

impl HttpBackend {
    /// Build an HTTP store rooted at the origin of `url`. Plain-text HTTP is
    /// allowed only for an `http://` URL, so an `https://` store never talks
    /// to a non-TLS endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the store fails to build.
    pub fn new(scheme: &str, url: &Url) -> Result<Self> {
        let options = ClientOptions::new().with_allow_http(url.scheme() == "http");
        let store = HttpBuilder::new()
            .with_url(base_url(url).as_str())
            .with_client_options(options)
            .build()?;
        Ok(Self {
            store: Arc::new(store),
            scheme: scheme.to_string(),
        })
    }
}

/// The origin of `url` (`scheme://[userinfo@]host[:port]/`): no path, query or
/// fragment. The store is shared by every URL on that origin, so it must not
/// carry the first URL's path or query string; object paths are resolved
/// against this root.
fn base_url(url: &Url) -> Url {
    let mut base = url.clone();
    base.set_path("/");
    base.set_query(None);
    base.set_fragment(None);
    base
}

#[async_trait::async_trait]
impl Backend for HttpBackend {
    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    fn supports_multipart(&self) -> bool {
        false
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn base_url_keeps_only_the_origin() {
        let url = Url::parse("https://user:pw@Example.com:8443/a/b%20c?token=1#frag").unwrap();
        assert_eq!(base_url(&url).as_str(), "https://user:pw@example.com:8443/");
        let plain = Url::parse("http://h/x/y").unwrap();
        assert_eq!(base_url(&plain).as_str(), "http://h/");
    }

    /// A store built from the base URL of one request serves every path on
    /// that origin: each request targets its own path, with no prefix or query
    /// from the first URL.
    #[tokio::test]
    async fn requests_use_each_urls_own_path() {
        use object_store::ObjectStoreExt as _;
        use object_store::path::Path as ObjectPath;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let server = tokio::spawn(async move {
            let mut lines = Vec::new();
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.expect("accept");
                let mut buf = vec![0u8; 4096];
                let n = sock.read(&mut buf).await.expect("read");
                let text = String::from_utf8_lossy(&buf[..n]).into_owned();
                lines.push(text.lines().next().unwrap_or_default().to_owned());
                sock.write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nhi",
                )
                .await
                .expect("write");
            }
            lines
        });

        // The real constructor: an `http://` URL must work without TLS.
        let first = Url::parse(&format!("http://127.0.0.1:{port}/first/a?token=secret")).unwrap();
        let backend = HttpBackend::new("http", &first).unwrap();
        let store = backend.store();
        for path in ["first/a", "second/b"] {
            let got = store.get(&ObjectPath::from(path)).await.unwrap();
            assert_eq!(got.bytes().await.unwrap().as_ref(), b"hi");
        }
        let lines = server.await.unwrap();
        assert_eq!(lines[0], "GET /first/a HTTP/1.1");
        assert_eq!(lines[1], "GET /second/b HTTP/1.1");
    }
}
