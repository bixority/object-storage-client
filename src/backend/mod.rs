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

mod azure;
mod body;
mod gcs;
mod http;
mod local;
mod s3;

use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
pub use body::body_stream;
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::client::HttpRequest;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use std::collections::HashMap;
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
/// `vars` (the environment unless injected).
///
/// Called once per registry key; the [`ObjectStorageClient`](crate::ObjectStorageClient)
/// caches the result.
///
/// # Errors
///
/// Returns an error if the scheme is unsupported, required URL components are
/// missing, or the underlying `object_store` fails to build.
pub fn build(url: &Url, vars: &Vars) -> Result<Arc<dyn Backend>> {
    let scheme = url.scheme();
    let host = url.host_str();

    let backend: Arc<dyn Backend> = match scheme {
        "s3" => Arc::new(s3::S3Backend::new(host, vars)?),
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
/// the caller cannot see it but it does exist, which we report as present to
/// match the list-based probe this replaced. Any other status is an error.
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
        401 | 403 => Ok(true),
        _ => {
            let text = error_body(response).await;
            Err(Error::Generic(format!(
                "{scheme} bucket existence check failed (HTTP {status}): {text}"
            )))
        }
    }
}

/// The start of an error response's body, for messages: at most 4 KiB, with
/// control characters (terminal escapes) replaced by spaces. A server that
/// answers with a huge or hostile body costs neither memory nor a corrupted
/// terminal.
pub async fn error_body(mut response: reqwest::Response) -> String {
    const LIMIT: usize = 4096;
    let mut body = Vec::new();
    while body.len() < LIMIT
        && let Ok(Some(chunk)) = response.chunk().await
    {
        body.extend_from_slice(&chunk);
    }
    body.truncate(LIMIT);
    String::from_utf8_lossy(&body)
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect()
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
        .map_err(|e| Error::Generic(format!("Signed request failed: {e}")))
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
        let response = reqwest::get(format!("http://{addr}/")).await.expect("get");
        error_body(response).await
    }

    #[tokio::test]
    async fn error_bodies_are_capped_and_stripped_of_control_characters() {
        let mut body = b"\x1b[31mred\x07 text\n".to_vec();
        body.extend(std::iter::repeat_n(b'a', 100_000));
        let text = error_body_of(body).await;
        assert_eq!(text.len(), 4096);
        assert!(text.starts_with(" [31mred  text a"), "{text}");
        assert!(!text.chars().any(char::is_control));

        assert_eq!(error_body_of(b"short".to_vec()).await, "short");
    }
}
