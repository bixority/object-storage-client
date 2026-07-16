//! Provider-specific storage backends.
//!
//! Each supported URL scheme resolves to a [`Backend`] implementation living in
//! its own module: [`s3`], [`gcs`], [`azure`], [`http`] and [`local`]. Generic
//! object operations (get / put / list / ...) all go through [`Backend::store`];
//! the bucket-management and pre-signing operations that `object_store` does not
//! abstract are implemented per provider by the trait methods below.
//!
//! Backends are built lazily by [`build`] and cached by the
//! [`ObjectStorageClient`](crate::ObjectStorageClient) keyed on (scheme, host),
//! so each provider's `object_store` — and the credential / HTTP machinery it
//! carries — is constructed once and reused across calls rather than rebuilt on
//! every request.

mod azure;
mod gcs;
mod http;
mod local;
mod s3;

use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use object_store::ObjectStore;
use object_store::client::HttpRequest;
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// A resolved, provider-specific storage backend.
///
/// Implementations are small wrappers over an `object_store` plus whatever extra
/// provider state (concrete store, credentials) is needed for the operations
/// `object_store` does not expose. They are constructed once per (scheme, host)
/// and shared behind an `Arc`.
#[async_trait::async_trait]
pub trait Backend: Send + Sync {
    /// The underlying object store used for generic read / write / list
    /// operations.
    fn store(&self) -> &Arc<dyn ObjectStore>;

    /// The URL scheme this backend was built for, used in error messages.
    fn scheme(&self) -> &str;

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

    /// Generate a pre-signed URL for a single object.
    ///
    /// Defaults to [`Error::SigningUnsupported`].
    async fn pre_signed_url(
        &self,
        _path: &ObjectPath,
        _method: SignMethod,
        _expires_in: Duration,
        _options: &SignOptions,
    ) -> Result<String> {
        Err(Error::SigningUnsupported(self.scheme().to_string()))
    }

    /// Generate pre-signed URLs for several objects sharing this backend.
    ///
    /// Defaults to [`Error::SigningUnsupported`].
    async fn pre_signed_urls(
        &self,
        _paths: &[ObjectPath],
        _method: SignMethod,
        _expires_in: Duration,
        _options: &SignOptions,
    ) -> Result<Vec<String>> {
        Err(Error::SigningUnsupported(self.scheme().to_string()))
    }
}

/// Build the backend for `url`'s scheme, reading provider configuration from the
/// environment.
///
/// Called once per (scheme, host); the [`ObjectStorageClient`](crate::ObjectStorageClient)
/// caches the result.
///
/// # Errors
///
/// Returns an error if the scheme is unsupported, required URL components are
/// missing, or the underlying `object_store` fails to build.
pub fn build(url: &Url) -> Result<Arc<dyn Backend>> {
    let scheme = url.scheme();
    let host = url.host_str();

    let backend: Arc<dyn Backend> = match scheme {
        "s3" => Arc::new(s3::S3Backend::from_env(host)?),
        "gs" | "gcs" => Arc::new(gcs::GcsBackend::from_env(scheme, host)?),
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
            Arc::new(azure::AzureBackend::from_env(scheme, host)?)
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
            let text = response.text().await.unwrap_or_default();
            Err(Error::Generic(format!(
                "{scheme} bucket existence check failed (HTTP {status}): {text}"
            )))
        }
    }
}

/// Host-only pre-signing through an `object_store` [`Signer`], used by the cloud
/// backends that do not support binding extra headers (GCS, Azure).
///
/// # Errors
///
/// Returns an error if `options` requests header binding (unsupported off S3) or
/// the signer fails.
pub async fn generic_pre_signed_url(
    signer: &dyn Signer,
    path: &ObjectPath,
    method: SignMethod,
    expires_in: Duration,
    options: &SignOptions,
) -> Result<String> {
    if !options.is_empty() {
        return Err(crate::sign::content_binding_unsupported());
    }
    let url = signer
        .signed_url(method.into_http(), path, expires_in)
        .await?;
    Ok(url.into())
}

/// Batch counterpart of [`generic_pre_signed_url`].
///
/// # Errors
///
/// Returns an error if `options` requests header binding (unsupported off S3) or
/// the signer fails.
pub async fn generic_pre_signed_urls(
    signer: &dyn Signer,
    paths: &[ObjectPath],
    method: SignMethod,
    expires_in: Duration,
    options: &SignOptions,
) -> Result<Vec<String>> {
    if !options.is_empty() {
        return Err(crate::sign::content_binding_unsupported());
    }
    let urls = signer
        .signed_urls(method.into_http(), paths, expires_in)
        .await?;
    Ok(urls.into_iter().map(Into::into).collect())
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
