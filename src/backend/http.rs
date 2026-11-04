//! Plain HTTP / `WebDAV` backend.
//!
//! Supports only generic object operations; bucket management and pre-signing
//! fall back to the [`Backend`] trait's "unsupported" defaults.

use super::{Backend, BucketClient, READ_TIMEOUT, Shared, Transport, request_failed};
use crate::client::{Error, Result};
use object_store::http::HttpBuilder;
use object_store::path::Path as ObjectPath;
use object_store::{ClientOptions, ObjectStore};
use std::sync::Arc;
use url::Url;

pub struct HttpBackend {
    store: Arc<dyn ObjectStore>,
    scheme: String,
    /// The origin the store is rooted at.
    base: Url,
    /// For the `PROPFIND` of [`Backend::is_collection`]; the store's own
    /// client is not reachable.
    http: BucketClient,
}

/// The most of a `PROPFIND` reply that is read.
const PROPFIND_LIMIT: usize = 256 * 1024;

/// A `Depth: 0` request for the resource type, nothing else.
const PROPFIND_BODY: &str =
    r#"<?xml version="1.0"?><propfind xmlns="DAV:"><prop><resourcetype/></prop></propfind>"#;

impl HttpBackend {
    /// As [`Self::with_shared`], with clients of its own.
    #[cfg(test)]
    pub fn new(scheme: &str, url: &Url) -> Result<Self> {
        Self::with_shared(scheme, url, &Shared::default())
    }

    /// Build an HTTP store rooted at the origin of `url`, with the HTTP clients
    /// of `shared`. Plain-text HTTP is allowed only for an `http://` URL, so an
    /// `https://` store never talks to a non-TLS endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error if the store fails to build.
    pub fn with_shared(scheme: &str, url: &Url, shared: &Shared) -> Result<Self> {
        // No total deadline: a download may take as long as it takes. A stalled
        // request is caught by the read timeout instead.
        let options = ClientOptions::new()
            .with_allow_http(url.scheme() == "http")
            .with_timeout_disabled()
            .with_read_timeout(READ_TIMEOUT);
        let store = HttpBuilder::new()
            .with_url(base_url(url).as_str())
            .with_client_options(options)
            .with_http_connector(shared.connector())
            .build()?;
        Ok(Self {
            store: Arc::new(store),
            scheme: scheme.to_string(),
            base: base_url(url),
            http: BucketClient::new(Transport {
                allow_http: url.scheme() == "http",
                ..Transport::default()
            }),
        })
    }
}

/// The origin of `url` (`scheme://host[:port]/`): no path, query or
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

/// Whether a `PROPFIND` reply says the resource is a collection: a
/// `collection` element, with any or no namespace prefix, inside
/// `resourcetype`. A plain text scan; the reply is a few hundred bytes.
fn names_collection(xml: &str) -> bool {
    let mut in_type = false;
    let mut rest = xml;
    while let Some(start) = rest.find('<') {
        rest = &rest[start + 1..];
        // Comments, CDATA sections and processing instructions hold no tags.
        let skipped = [("!--", "-->"), ("![CDATA[", "]]>"), ("?", "?>")]
            .into_iter()
            .find_map(|(open, close)| rest.strip_prefix(open).map(|tail| (tail, close)));
        if let Some((tail, close)) = skipped {
            rest = tail
                .find(close)
                .map_or("", |end| &tail[end + close.len()..]);
            continue;
        }
        let Some(end) = rest.find('>') else {
            break;
        };
        let tag = &rest[..end];
        rest = &rest[end..];
        let (closing, tag) = tag.strip_prefix('/').map_or((false, tag), |t| (true, t));
        let self_closing = tag.ends_with('/');
        let name = tag
            .split(|c: char| c.is_whitespace() || c == '/')
            .next()
            .unwrap_or_default();
        let local = name.rsplit(':').next().unwrap_or_default();
        match local {
            "resourcetype" => in_type = !closing && !self_closing,
            "collection" if in_type && !closing => return true,
            _ => {}
        }
    }
    false
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

    /// Asks the server with `PROPFIND`, `Depth: 0`, anonymously like the
    /// store's own requests. `207` answers by the resource type; `404`, `405`
    /// and `501` (no such resource, or no `WebDAV`) mean it is not one. Any
    /// other answer is an error: a delete must not go ahead on a guess.
    async fn is_collection(&self, path: &ObjectPath) -> Result<bool> {
        // The URL the store addresses the object with.
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|()| Error::Generic("the HTTP URL cannot hold a path".into()))?
            .extend(path.parts());
        let method = reqwest::Method::from_bytes(b"PROPFIND")
            .map_err(|e| Error::Generic(format!("PROPFIND method: {e}")))?;
        let mut response = self
            .http
            .get()?
            .request(method, url.clone())
            .header("Depth", "0")
            .header("Content-Type", "application/xml")
            .body(PROPFIND_BODY)
            .send()
            .await
            .map_err(|e| request_failed("PROPFIND", e))?;
        match response.status().as_u16() {
            207 => {}
            404 | 405 | 501 => return Ok(false),
            status => {
                return Err(Error::Generic(format!(
                    "PROPFIND on {} answered HTTP {status}; cannot tell whether it is a collection",
                    crate::codec::redacted(&url)
                )));
            }
        }
        let mut body = Vec::new();
        while body.len() < PROPFIND_LIMIT
            && let Some(chunk) = response
                .chunk()
                .await
                .map_err(|e| request_failed("PROPFIND", e))?
        {
            body.extend_from_slice(&chunk);
        }
        Ok(names_collection(&String::from_utf8_lossy(&body)))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn base_url_keeps_only_the_origin() {
        let url = Url::parse("https://Example.com:8443/a/b%20c?token=1#frag").unwrap();
        assert_eq!(base_url(&url).as_str(), "https://example.com:8443/");
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
        let reply = || stub::Reply::new(200, "hi");
        let stub = stub::serve(vec![reply(), reply()]).await;

        // The real constructor: an `http://` URL must work without TLS.
        let first = Url::parse(&format!("{}/first/a?token=secret", stub.base)).unwrap();
        let backend = HttpBackend::new("http", &first).unwrap();
        let store = backend.store();
        for path in ["first/a", "second/b"] {
            let got = store.get(&ObjectPath::from(path)).await.unwrap();
            assert_eq!(got.bytes().await.unwrap().as_ref(), b"hi");
        }
        let requests = stub.requests();
        assert_eq!(requests[0].line, "GET /first/a HTTP/1.1");
        assert_eq!(requests[1].line, "GET /second/b HTTP/1.1");
    }

    #[test]
    fn collections_are_found_whatever_the_prefix_and_spacing() {
        let reply = |ty: &str| {
            format!(
                "<?xml version=\"1.0\"?><D:multistatus xmlns:D=\"DAV:\"><D:response>\
                 <D:propstat><D:prop>{ty}</D:prop></D:propstat></D:response></D:multistatus>"
            )
        };
        for ty in [
            "<D:resourcetype><D:collection/></D:resourcetype>",
            "<resourcetype><collection/></resourcetype>",
            "<d:resourcetype>\n  <d:collection />\n</d:resourcetype>",
            "<ns0:resourcetype><ns0:collection></ns0:collection></ns0:resourcetype>",
            "<D:resourcetype ><D:other/><D:collection\n/></D:resourcetype >",
        ] {
            assert!(names_collection(&reply(ty)), "{ty}");
        }
        for ty in [
            "<D:resourcetype/>",
            "<D:resourcetype></D:resourcetype>",
            "<D:resourcetype><D:other/></D:resourcetype>",
            // Outside `resourcetype`, in a comment or in text.
            "<D:displayname><D:collection/></D:displayname><D:resourcetype/>",
            "<D:resourcetype/><!-- <D:collection/> -->",
            "<D:resourcetype/><D:displayname><![CDATA[<collection/>]]></D:displayname>",
            "<D:resourcetype/><D:displayname>collection</D:displayname>",
        ] {
            assert!(!names_collection(&reply(ty)), "{ty}");
        }
        assert!(!names_collection(""));
        assert!(!names_collection("<resourcetype><collection"));
    }

    async fn collection_answer(replies: Vec<stub::Reply>) -> (Result<bool>, stub::Stub) {
        let stub = stub::serve(replies).await;
        let url = Url::parse(&format!("{}/x", stub.base)).unwrap();
        let backend = HttpBackend::new("http", &url).unwrap();
        let answer = backend.is_collection(&ObjectPath::from("d/a b")).await;
        (answer, stub)
    }

    const COLLECTION: &str = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:propstat><D:prop>\
        <D:resourcetype><D:collection/></D:resourcetype></D:prop></D:propstat></D:response>\
        </D:multistatus>";

    #[tokio::test]
    async fn propfind_names_the_object_url_and_reads_the_resource_type() {
        let (answer, stub) = collection_answer(vec![stub::Reply::new(207, COLLECTION)]).await;
        assert!(answer.unwrap());
        let request = &stub.requests()[0];
        assert_eq!(request.line, "PROPFIND /d/a%20b HTTP/1.1");
        assert!(
            request.headers.to_ascii_lowercase().contains("depth: 0"),
            "{}",
            request.headers
        );
        let plain = "<D:multistatus xmlns:D=\"DAV:\"><D:response><D:propstat><D:prop>\
            <D:resourcetype/></D:prop></D:propstat></D:response></D:multistatus>";
        let (answer, _) = collection_answer(vec![stub::Reply::new(207, plain)]).await;
        assert!(!answer.unwrap());
    }

    #[tokio::test]
    async fn no_webdav_or_no_resource_is_not_a_collection() {
        for status in [404, 405, 501] {
            let (answer, _) = collection_answer(vec![stub::Reply::new(status, "")]).await;
            assert!(!answer.unwrap(), "{status}");
        }
    }

    #[tokio::test]
    async fn any_other_answer_fails_closed() {
        for status in [200, 302, 401, 403, 500, 503] {
            let (answer, _) = collection_answer(vec![stub::Reply::new(status, COLLECTION)]).await;
            let err = answer.expect_err(&status.to_string());
            assert!(err.to_string().contains(&status.to_string()), "{err}");
        }
    }

    use super::super::stub;
    use object_store::ObjectStoreExt as _;
    use object_store::path::Path as ObjectPath;
    use std::time::Duration;

    async fn get_bytes(base: &str) -> object_store::Result<bytes::Bytes> {
        let url = Url::parse(&format!("{base}/slow")).unwrap();
        let backend = HttpBackend::new("http", &url).unwrap();
        backend
            .store()
            .get(&ObjectPath::from("slow"))
            .await?
            .bytes()
            .await
    }

    #[tokio::test(start_paused = true)]
    async fn a_slow_body_is_not_cut_off() {
        // 120 s of virtual time in all: well past the old 30 s total deadline.
        let base = stub::serve_slow(6, 6, Duration::from_secs(20)).await;
        let got = get_bytes(&base).await.unwrap();
        assert_eq!(got.as_ref(), vec![b'x'; 6 * stub::CHUNK].as_slice());
    }

    #[tokio::test(start_paused = true)]
    async fn a_stalled_body_fails_within_the_read_timeout() {
        let base = stub::serve_slow(1, 0, Duration::ZERO).await;
        let start = tokio::time::Instant::now();
        let err = get_bytes(&base).await.unwrap_err();
        let text = format!("{err:?}").to_lowercase();
        assert!(
            text.contains("timed out") || text.contains("timeout"),
            "{text}"
        );
        assert!(
            start.elapsed() <= READ_TIMEOUT + Duration::from_secs(1),
            "{:?}",
            start.elapsed()
        );
        assert!(start.elapsed() >= READ_TIMEOUT, "{:?}", start.elapsed());
    }
}
