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
pub mod stub;

use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
pub use aws_profile::AwsProfile;
pub use body::FILE_BUF;
pub use body::body_stream;
#[cfg(feature = "gui")]
pub use connector::http_client;
use connector::{BoundedConnector, BucketClient, ClientCache, SharedConnector, Transport};
pub use local::fs_path;
use object_store::aws::AwsCredentialProvider;
use object_store::azure::AzureCredentialProvider;
use object_store::client::HttpRequest;
use object_store::gcp::{GcpCredentialProvider, GcpSigningCredentialProvider};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ClientConfigKey, ObjectMeta, ObjectStore};
#[cfg(feature = "gui")]
pub use s3::{PROFILE_ENDPOINT_REFUSED, S3Backend};
use std::collections::HashMap;
use std::fmt::Write as _;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;
use url::Url;

/// What the backends of one registry share: HTTP clients and credential
/// providers. A registry owns one, so its stores share connections and
/// tokens, and registries stay apart.
///
/// Sharing a credential provider is safe because the variables are fixed for
/// the registry's life: every store of a family would build the same provider,
/// whatever its bucket. A store that skips signing never takes part, because
/// its builder holds a provider that must not be called. The AWS profile
/// provider has its own sharing, see [`AwsProfile`].
///
/// The first store built stores its providers (`object_store` does not let the
/// crate build them), and later stores are given them. They are kept for the
/// registry's life, so evicting a store does not drop a warm token.
#[derive(Default)]
pub struct Shared {
    clients: Arc<ClientCache>,
    aws: OnceLock<AwsCredentialProvider>,
    gcs: OnceLock<(GcpCredentialProvider, GcpSigningCredentialProvider)>,
    azure: OnceLock<AzureCredentialProvider>,
    /// The regions that S3 reported for buckets, see [`BucketRegions`].
    regions: Arc<BucketRegions>,
}

/// The region of each S3 bucket, as learned from S3's own answers (the
/// `x-amz-bucket-region` header of a redirect), by bucket name. Poison-tolerant.
#[derive(Default)]
pub struct BucketRegions(Mutex<HashMap<String, String>>);

impl BucketRegions {
    /// The region learned for `bucket`, if any.
    pub fn get(&self, bucket: &str) -> Option<String> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(bucket)
            .cloned()
    }

    /// Records `region` as the region of `bucket`.
    #[cfg(feature = "gui")]
    pub fn learn(&self, bucket: &str, region: &str) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(bucket.to_owned(), region.to_owned());
    }
}

impl Shared {
    /// Records `region` as the region of the S3 bucket `bucket`.
    #[cfg(all(test, feature = "gui"))]
    pub fn learn_region(&self, bucket: &str, region: &str) {
        self.regions.learn(bucket, region);
    }

    /// The region learned for the S3 bucket `bucket`, if any.
    #[cfg(feature = "gui")]
    pub fn learned_region(&self, bucket: &str) -> Option<String> {
        self.regions.get(bucket)
    }

    /// A connector for a store's HTTP clients, drawn from the shared cache.
    fn connector(&self) -> BoundedConnector<SharedConnector> {
        BoundedConnector::shared(&self.clients)
    }
}

/// Where backend configuration is read from.
///
/// Production clients use [`Vars::Env`], the process environment. [`Vars::Map`]
/// is an injected, fixed set of variables, so clients never share (or mutate)
/// global environment state. Such a client ignores the system proxy setting
/// unless `system_proxy` is set.
#[derive(Clone, Default)]
pub enum Vars {
    #[default]
    Env,
    Map {
        vars: Arc<HashMap<String, String>>,
        system_proxy: bool,
    },
}

impl Vars {
    /// An injected variable set, for tests.
    #[cfg(test)]
    pub fn from_pairs(pairs: &[(&str, &str)]) -> Self {
        Self::map(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
        )
    }

    /// An injected variable set that ignores the system proxy setting.
    pub fn map(vars: HashMap<String, String>) -> Self {
        Self::Map {
            vars: Arc::new(vars),
            system_proxy: false,
        }
    }

    /// The value of `key`, if set.
    pub fn get(&self, key: &str) -> Option<String> {
        match self {
            Self::Env => std::env::var(key).ok(),
            Self::Map { vars, .. } => vars.get(key).cloned(),
        }
    }

    /// For [`Vars::Map`], the entries whose name starts with `prefix` and whose
    /// lower-cased name parses as a builder config key `K`, mirroring what the
    /// `object_store` builders' `from_env` does with the process environment.
    /// Empty for [`Vars::Env`], where `from_env` does the reading itself.
    fn config_entries<K: FromStr>(&self, prefix: &str) -> Vec<(K, String)> {
        match self {
            Self::Env => Vec::new(),
            Self::Map { vars, .. } => vars
                .iter()
                .filter(|(k, _)| k.starts_with(prefix))
                .filter_map(|(k, v)| Some((k.to_ascii_lowercase().parse().ok()?, v.clone())))
                .collect(),
        }
    }
}

/// How long an object request may stay silent: the time to the response
/// headers and every gap between body reads. The first covers sending the
/// request body, so each upload request (one put or one part) must be sent and
/// answered within this time, even while bytes flow steadily.
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

    /// The store's paged, delimiter-aware listing, if it has one (S3, GCS and
    /// Azure); `None` for stores that only offer `list_with_delimiter`.
    #[cfg(feature = "gui")]
    fn paginated(&self) -> Option<&dyn object_store::list::PaginatedListStore> {
        None
    }

    /// One page of the immediate children of the directory `dir` (the root
    /// when empty), at most `max` entries, continuing from the `next` of an
    /// earlier page. Stores without paged listing return one complete page,
    /// and an empty one for a token; a missing directory is an empty page.
    ///
    /// The default lists through the store and reports every object
    /// [`ObjectState::Online`]; S3 and Azure override it to report archives.
    ///
    /// # Errors
    ///
    /// Returns an error if the store refuses or fails the listing.
    #[cfg(feature = "gui")]
    async fn list_page(
        &self,
        dir: &ObjectPath,
        max: usize,
        token: Option<String>,
    ) -> Result<ListPage> {
        list_through_store(self, dir, max, token).await
    }

    /// The file system path `path` maps to, for the local backend on Unix;
    /// `None` for every other backend. Writes to such a path go through the
    /// crate's own private-staging writer instead of the store's `put`.
    fn local_path(&self, _path: &ObjectPath) -> Result<Option<std::path::PathBuf>> {
        Ok(None)
    }

    /// Whether `path` names a collection (a directory) whose deletion would
    /// remove everything in it. Only the http(s) backend can say so; the
    /// default is `false`.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot tell.
    async fn is_collection(&self, _path: &ObjectPath) -> Result<bool> {
        Ok(false)
    }

    /// Create the bucket / container identified by `url`.
    ///
    /// Defaults to [`Error::BucketCreationUnsupported`]; providers that support
    /// it override this.
    async fn create_bucket(&self, _url: &Url) -> Result<()> {
        Err(Error::BucketCreationUnsupported(self.scheme().to_string()))
    }

    /// Whether the bucket / container identified by `url` exists.
    ///
    /// Defaults to [`Error::BucketExistenceUnsupported`]; providers that support
    /// it override this.
    async fn bucket_exists(&self, _url: &Url) -> Result<bool> {
        Err(Error::BucketExistenceUnsupported(self.scheme().to_string()))
    }

    /// Waits until the server-side copy that wrote `path` is complete.
    /// `server_side_copy` calls it after every copy and before a move deletes
    /// its source. Defaults to doing nothing: most stores copy atomically.
    ///
    /// # Errors
    ///
    /// Returns an error if the copy failed or did not finish in time.
    async fn finish_copy(&self, _path: &ObjectPath) -> Result<()> {
        Ok(())
    }

    /// This backend as [`Any`](std::any::Any), for the backends that can copy
    /// from another one of their own type (see [`Backend::copies_from`]).
    fn as_any(&self) -> Option<&dyn std::any::Any> {
        None
    }

    /// Whether [`Backend::copy_from`] may copy from `src` server-side: the same
    /// provider type, reached through the same endpoint and region, with the
    /// very same credential provider, and neither side skipping signatures.
    /// Defaults to `false`.
    fn copies_from(&self, _src: &dyn Backend) -> bool {
        false
    }

    /// Copies `src_path` of `src` to `dst_path` of this backend inside the
    /// provider, for a source in another bucket or container. `known` is the
    /// source's metadata when the caller has it: its e-tag (S3, Azure) or
    /// generation (GCS) makes the copy fail if the source has changed.
    ///
    /// Returns `false` when nothing was copied and the caller must stream:
    /// [`Backend::copies_from`] refuses `src`, or the server cannot do it.
    /// `server_side_copy` calls [`Backend::finish_copy`] afterwards, and a move
    /// deletes its source only after that. Defaults to `false`.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider refused or failed the copy, and
    /// [`Error::SourceChanged`] (with an empty URL, which the caller fills in)
    /// if the source no longer matches `known`.
    async fn copy_from(
        &self,
        _src: &dyn Backend,
        _src_path: &ObjectPath,
        _dst_path: &ObjectPath,
        _known: Option<&ObjectMeta>,
    ) -> Result<bool> {
        Ok(false)
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
pub fn build(
    url: &Url,
    vars: &Vars,
    profile: Option<&AwsProfile>,
    shared: &Shared,
) -> Result<Arc<dyn Backend>> {
    let scheme = url.scheme();
    let host = url.host_str();

    let backend: Arc<dyn Backend> = match scheme {
        "s3" => Arc::new(s3::S3Backend::with_shared(host, vars, profile, shared)?),
        "gs" | "gcs" => Arc::new(gcs::GcsBackend::with_shared(scheme, host, vars, shared)?),
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => Arc::new(azure::AzureBackend::with_shared(
            scheme, host, vars, shared,
        )?),
        "http" | "https" => Arc::new(http::HttpBackend::with_shared(scheme, url, shared)?),
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
            let code = ms_error_code(response.headers())
                .map_or_else(String::new, |c| format!(", {}", crate::text::escape(c, 64)));
            Err(Error::Denied(format!(
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

/// The error for a bucket call `what` (creation, listing) that the provider
/// answered with `status` and the error text `text`. An archived object is
/// [`Error::Archived`], HTTP 401 and 403 are [`Error::Denied`], every other status [`Error::Generic`].
pub fn create_failed(what: &str, status: reqwest::StatusCode, text: &str) -> Error {
    crate::client::archived(status.as_u16(), text, text).unwrap_or_else(|| {
        if matches!(status.as_u16(), 401 | 403) {
            Error::Denied(format!("{what} was denied (HTTP {status}): {text}"))
        } else {
            Error::Generic(format!("{what} failed (HTTP {status}): {text}"))
        }
    })
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

/// The most of an answer's document that [`read_document`] keeps.
const DOCUMENT_LIMIT: usize = 64 * 1024;

/// The body of a successful `response` as text: the start of its document,
/// after any leading white space, at most 64 KiB. A body that fails midway
/// (a reset, a read timeout) is an error, so a cut-off answer is never taken
/// for an empty one. The white space some servers send to keep a long request
/// alive is not counted, and is not kept.
///
/// # Errors
///
/// Returns an error if reading the body fails.
pub async fn read_document(mut response: reqwest::Response) -> Result<String> {
    let mut body = Vec::new();
    while body.len() < DOCUMENT_LIMIT {
        let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| request_failed("Reading the answer", e))?
        else {
            break;
        };
        let chunk = if body.is_empty() {
            chunk.trim_ascii_start()
        } else {
            &chunk[..]
        };
        body.extend_from_slice(chunk);
    }
    body.truncate(DOCUMENT_LIMIT);
    Ok(String::from_utf8_lossy(&body).into_owned())
}

/// The name a backend gets when it is built only to list buckets. No request
/// names it, and the backend is never cached or used for objects.
#[cfg(feature = "gui")]
const LISTING_ONLY: &str = "listing-only";

/// The names of all buckets (S3, GCS) or containers (Azure) that the
/// credentials for `scheme` can list, sorted. Reads configuration like
/// [`build`].
///
/// # Errors
///
/// Returns an error if the scheme has no buckets, the backend cannot be built,
/// or the provider refuses or fails the listing (a refusal is
/// [`Error::Denied`]).
#[cfg(feature = "gui")]
pub async fn list_buckets(
    scheme: &str,
    vars: &Vars,
    profile: Option<&AwsProfile>,
    shared: &Shared,
) -> Result<Vec<String>> {
    let host = Some(LISTING_ONLY);
    match scheme {
        "s3" => {
            s3::S3Backend::with_shared(host, vars, profile, shared)?
                .list_buckets()
                .await
        }
        "gs" | "gcs" => {
            gcs::GcsBackend::with_shared(scheme, host, vars, shared)?
                .list_buckets()
                .await
        }
        "az" | "wasb" | "wasbs" | "abfs" | "abfss" => {
            azure::AzureBackend::with_shared(scheme, host, vars, shared)?
                .list_buckets()
                .await
        }
        _ => Err(Error::UnsupportedScheme(scheme.to_string())),
    }
}

/// The most of a listing answer that [`read_listing`] keeps.
#[cfg(feature = "gui")]
const LISTING_LIMIT: usize = 16 * 1024 * 1024;

/// The listing of [`Backend::list_page`] through the store, for the default
/// and for the stores whose own listing cannot be used.
#[cfg(feature = "gui")]
pub async fn list_through_store<B: Backend + ?Sized>(
    backend: &B,
    dir: &ObjectPath,
    max: usize,
    token: Option<String>,
) -> Result<ListPage> {
    use object_store::list::PaginatedListOptions;
    let (listed, next) = if let Some(store) = backend.paginated() {
        let prefix = (!dir.as_ref().is_empty()).then(|| format!("{dir}/"));
        let opts = PaginatedListOptions {
            delimiter: Some("/".into()),
            max_keys: Some(max),
            page_token: token,
            ..Default::default()
        };
        let page = store.list_paginated(prefix.as_deref(), opts).await?;
        (page.result, page.page_token)
    } else if token.is_some() {
        return Ok(ListPage::default());
    } else {
        match backend.store().list_with_delimiter(Some(dir)).await {
            Ok(listed) => (listed, None),
            Err(object_store::Error::NotFound { .. }) => return Ok(ListPage::default()),
            Err(e) => return Err(e.into()),
        }
    };
    Ok(ListPage {
        prefixes: listed.common_prefixes,
        objects: listed
            .objects
            .into_iter()
            .map(|m| ListedObject {
                location: m.location,
                size: m.size,
                modified: m.last_modified,
                state: ObjectState::Online,
                class: None,
            })
            .collect(),
        next,
    })
}

/// Whether an object can be read now.
#[cfg(feature = "gui")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectState {
    /// Readable.
    Online,
    /// Offline until restored (S3) or rehydrated (Azure).
    Archived,
    /// A restore or rehydration is running.
    Restoring,
}

/// An object of a [`ListPage`].
#[cfg(feature = "gui")]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedObject {
    pub location: ObjectPath,
    pub size: u64,
    pub modified: chrono::DateTime<chrono::Utc>,
    pub state: ObjectState,
    /// The provider's storage class or access tier, when it reports one.
    pub class: Option<String>,
}

/// One page of [`Backend::list_page`].
#[cfg(feature = "gui")]
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ListPage {
    pub prefixes: Vec<ObjectPath>,
    pub objects: Vec<ListedObject>,
    pub next: Option<String>,
}

/// The body of a successful listing `response` (else an error naming `what`),
/// at most 16 MiB.
///
/// # Errors
///
/// Returns an error if reading the body fails or it is longer than the limit.
#[cfg(feature = "gui")]
pub async fn read_listing(mut response: reqwest::Response, what: &str) -> Result<Vec<u8>> {
    let status = response.status();
    if !status.is_success() {
        let text = error_body(response).await;
        return Err(create_failed(what, status, &text));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|e| request_failed("Reading the answer", e))?
    {
        if body.len() + chunk.len() > LISTING_LIMIT {
            return Err(Error::Generic("the bucket listing is too large".into()));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// One page of an XML bucket listing: the `Name` texts and the text of
/// `next_tag` (`None` when empty or absent).
///
/// # Errors
///
/// As [`read_listing`], or if sending the request fails.
#[cfg(feature = "gui")]
async fn xml_listing_page(
    http: &reqwest::Client,
    request: HttpRequest,
    what: &str,
    next_tag: &str,
) -> Result<(Vec<String>, Option<String>)> {
    let body = read_listing(send_signed(http, request).await?, what).await?;
    let xml = String::from_utf8_lossy(&body);
    let next = xml_texts(&xml, next_tag).pop().filter(|t| !t.is_empty());
    Ok((xml_texts(&xml, "Name"), next))
}

/// Collects the names of all pages, sorted. `fetch` gets the token of the page
/// to read (`None` for the first) and returns its names and the next token.
///
/// # Errors
///
/// Returns the error of `fetch`, or one if a page repeats its own token.
#[cfg(feature = "gui")]
async fn collect_pages<F, Fut>(mut fetch: F) -> Result<Vec<String>>
where
    F: FnMut(Option<String>) -> Fut,
    Fut: std::future::Future<Output = Result<(Vec<String>, Option<String>)>>,
{
    let mut names = Vec::new();
    let mut token = None;
    loop {
        let (page, next) = fetch(token.clone()).await?;
        names.extend(page);
        match next {
            None => break,
            Some(next) if token.as_ref() == Some(&next) => {
                return Err(Error::Generic("the bucket listing does not advance".into()));
            }
            Some(next) => token = Some(next),
        }
    }
    names.sort();
    Ok(names)
}

/// `text` as one path segment or query value: everything but unreserved
/// characters is percent-encoded.
pub fn encode(text: &str) -> String {
    percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
}

/// The text of every `<tag>` element of `xml` (one without attributes), in
/// document order, with the predefined and numeric entities resolved. An empty
/// element (`<tag />`) has no text and is skipped.
#[cfg(feature = "gui")]
fn xml_texts(xml: &str, tag: &str) -> Vec<String> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut texts = Vec::new();
    let mut rest = xml;
    while let Some((_, after)) = rest.split_once(&open) {
        let Some((text, tail)) = after.split_once(&close) else {
            break;
        };
        texts.push(xml_unescape(text));
        rest = tail;
    }
    texts
}

/// The inner text of every `<tag>` element of `xml` (one without attributes),
/// in document order, entities not resolved.
#[cfg(feature = "gui")]
fn xml_blocks<'a>(xml: &'a str, tag: &str) -> Vec<&'a str> {
    let (open, close) = (format!("<{tag}>"), format!("</{tag}>"));
    let mut blocks = Vec::new();
    let mut rest = xml;
    while let Some((_, after)) = rest.split_once(&open) {
        let Some((block, tail)) = after.split_once(&close) else {
            break;
        };
        blocks.push(block);
        rest = tail;
    }
    blocks
}

/// `text` with `&lt;`, `&gt;`, `&amp;`, `&quot;`, `&apos;` and numeric
/// character references resolved; any other `&` is kept.
#[cfg(feature = "gui")]
fn xml_unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some((before, after)) = rest.split_once('&') {
        out.push_str(before);
        let entity = after.split_once(';').and_then(|(name, tail)| {
            let c = match name {
                "lt" => '<',
                "gt" => '>',
                "amp" => '&',
                "quot" => '"',
                "apos" => '\'',
                _ => {
                    let digits = name.strip_prefix('#')?;
                    let code = digits
                        .strip_prefix(['x', 'X'])
                        .map_or_else(|| digits.parse(), |hex| u32::from_str_radix(hex, 16));
                    char::from_u32(code.ok()?)?
                }
            };
            Some((c, tail))
        });
        if let Some((c, tail)) = entity {
            out.push(c);
            rest = tail;
        } else {
            out.push('&');
            rest = after;
        }
    }
    out.push_str(rest);
    out
}

/// How many times a copy request is sent at most.
const COPY_ATTEMPTS: u32 = 3;
/// The pause before the second attempt; it doubles after each.
const COPY_RETRY_PAUSE: Duration = Duration::from_millis(200);

/// Sends the copy request `send` builds, again after a pause when it fails
/// before any answer (a connection error) or is answered `429`, `500`, `502`,
/// `503` or `504`, up to three attempts in all. Any other answer, and the last
/// one, is returned. A request that was answered `2xx` is never sent again.
///
/// # Errors
///
/// Returns the error of the last attempt if none was answered.
pub async fn send_with_retry<F, Fut>(mut send: F) -> Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<reqwest::Response>>,
{
    let mut pause = COPY_RETRY_PAUSE;
    for attempt in 1..=COPY_ATTEMPTS {
        let last = attempt == COPY_ATTEMPTS;
        match send().await {
            Ok(response)
                if !last && matches!(response.status().as_u16(), 429 | 500 | 502 | 503 | 504) => {}
            Err(_) if !last => {}
            done => return done,
        }
        tokio::time::sleep(pause).await;
        pause *= 2;
    }
    unreachable!("the last attempt returns")
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
/// need. A leading byte order mark and attributes on the root are accepted. Any
/// other text (a JSON error, an HTML page, plain text) is returned unchanged.
pub fn redact_error_body(text: &str) -> String {
    // Azure starts its documents with a byte order mark, which is not white space.
    fn skip(s: &str) -> &str {
        s.trim_start_matches(|c: char| c == '\u{feff}' || c.is_whitespace())
    }
    let mut rest = skip(text);
    if rest.starts_with("<?xml")
        && let Some(end) = rest.find("?>")
    {
        rest = skip(&rest[end + 2..]);
    }
    // The root is `<Error>`, or `<Error` followed by white space and attributes.
    let root = rest
        .strip_prefix("<Error")
        .and_then(|after| match after.chars().next() {
            Some('>') => Some(&after[1..]),
            Some(c) if c.is_whitespace() => after.find('>').map(|end| &after[end + 1..]),
            _ => None,
        });
    let Some(mut rest) = root else {
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

/// The `x-ms-error-code` header of an Azure response, raw. Escape it before
/// it goes into a message.
pub fn ms_error_code(headers: &reqwest::header::HeaderMap) -> Option<&str> {
    headers.get("x-ms-error-code")?.to_str().ok()
}

/// Sends a request already signed by an `object_store` authorizer.
///
/// The authorizer mutates an [`HttpRequest`] (method, URI and the signed
/// headers); this transfers those onto the backend's bucket-call `reqwest`
/// client and dispatches them with the body the signature was computed over.
pub async fn send_signed(
    client: &reqwest::Client,
    request: HttpRequest,
) -> Result<reqwest::Response> {
    let (parts, body) = request.into_parts();
    let body = body
        .as_bytes()
        .cloned()
        .ok_or_else(|| Error::Generic("signed request has a streaming body".into()))?;
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

    #[cfg(feature = "gui")]
    #[test]
    fn xml_text_is_found_and_unescaped() {
        let xml = "<a><Name>x&amp;y</Name><Name/><Name>&#x1F600;&#65;&bogus;&amp</Name><Name>cut";
        assert_eq!(xml_texts(xml, "Name"), ["x&y", "\u{1F600}A&bogus;&amp"]);
        assert_eq!(
            xml_texts("<NextMarker />", "NextMarker"),
            Vec::<String>::new()
        );
    }

    #[tokio::test]
    async fn transport_errors_never_carry_the_query() {
        let port = stub::closed_port();
        let client = stub::client();
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
        let body = String::from_utf8(body).expect("utf-8 body");
        let stub = stub::serve(vec![stub::Reply::new(500, body)]).await;
        let client = stub::client();
        let response = client
            .get(format!("{}/", stub.base))
            .send()
            .await
            .expect("get");
        error_body(response).await
    }

    /// `bucket_exists_from_response` on a HEAD answered with `reply`.
    async fn probe(reply: stub::Reply) -> Result<bool> {
        let stub = stub::serve(vec![reply]).await;
        let client = stub::client();
        let response = client.head(&stub.base).send().await.expect("head");
        bucket_exists_from_response(response, "Azure").await
    }

    #[tokio::test]
    async fn denied_probes_are_errors() {
        let err = probe(stub::Reply::new(401, "")).await.expect_err("401");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        assert!(err.to_string().contains("401"), "{err}");
        let err = probe(stub::Reply::new(403, "")).await.expect_err("403");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        let err = probe(stub::Reply::new(500, "")).await.expect_err("500");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");

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

    #[tokio::test(start_paused = true)]
    async fn a_request_that_never_gets_an_answer_is_sent_three_times() {
        let port = stub::closed_port();
        let client = stub::client();
        let attempts = std::sync::atomic::AtomicU32::new(0);
        let result = send_with_retry(|| async {
            attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            client
                .put(format!("http://127.0.0.1:{port}/"))
                .send()
                .await
                .map_err(|e| request_failed("Copy", e))
        })
        .await;
        assert!(result.is_err());
        assert_eq!(attempts.load(std::sync::atomic::Ordering::Relaxed), 3);
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

    fn azure_authentication_error() -> String {
        format!(
            "<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n<Error><Code>AuthenticationFailed</Code>\
             <Message>Server failed to authenticate the request.</Message>\
             <AuthenticationErrorDetail>String-to-Sign: x-ms-encryption-key:{SENTINEL}\
             </AuthenticationErrorDetail></Error>"
        )
    }

    #[test]
    fn a_leading_byte_order_mark_does_not_bypass_redaction() {
        let body = azure_authentication_error();
        for text in [
            format!("\u{feff}{body}"),
            format!("\u{feff} \n\u{feff}{body}"),
            body.replace("?>\r\n", "?>\u{feff}\r\n"),
            body.replace("<?xml version=\"1.0\" encoding=\"utf-8\"?>\r\n", "\u{feff}"),
        ] {
            let text = redact_error_body(&text);
            assert!(!text.contains(SENTINEL), "{text}");
            assert_eq!(
                text,
                "<Error><Code>AuthenticationFailed</Code>\
                 <Message>Server failed to authenticate the request.</Message></Error>"
            );
        }
    }

    #[test]
    fn a_root_with_attributes_is_redacted() {
        let body = azure_authentication_error()
            .replace("<Error>", "<Error xmlns=\"urn:x\"\n note=\"a&gt;b\">");
        let text = redact_error_body(&body);
        assert!(!text.contains(SENTINEL), "{text}");
        assert!(
            text.starts_with("<Error><Code>AuthenticationFailed</Code>"),
            "{text}"
        );
        // A different root, or a longer name, is not an `Error` document.
        for other in [
            "<Errors><Code>x</Code></Errors>",
            "<html><body>502</body></html>",
        ] {
            assert_eq!(redact_error_body(other), other);
        }
    }

    #[tokio::test]
    async fn a_byte_order_mark_does_not_bypass_error_body_redaction() {
        let mut body = b"\xEF\xBB\xBF".to_vec();
        body.extend_from_slice(azure_authentication_error().as_bytes());
        let text = error_body_of(body).await;
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

    #[test]
    fn a_failed_creation_is_denied_only_for_401_and_403() {
        let status = |c| reqwest::StatusCode::from_u16(c).expect("status");
        for code in [401, 403] {
            let err = create_failed("S3 CreateBucket", status(code), "AccessDenied");
            assert!(matches!(err, Error::Denied(_)), "{err:?}");
            assert!(err.to_string().contains("AccessDenied"), "{err}");
        }
        let err = create_failed("S3 CreateBucket", status(500), "boom");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");
        assert!(err.to_string().contains("HTTP 500"), "{err}");
    }

    #[test]
    fn an_archived_object_is_told_from_a_denied_or_conflicting_call() {
        let status = |c| reqwest::StatusCode::from_u16(c).expect("status");
        let err = create_failed(
            "S3 CopyObject",
            status(403),
            "<Code>InvalidObjectState</Code>",
        );
        assert!(matches!(err, Error::Archived(_)), "{err:?}");
        assert!(err.to_string().contains("restored"), "{err}");
        let err = create_failed("Azure Copy Blob", status(409), "BlobArchived <Error/>");
        assert!(matches!(err, Error::Archived(_)), "{err:?}");
        assert!(err.to_string().contains("rehydrated"), "{err}");
        // The code alone does not count under another status.
        let err = create_failed("S3 CopyObject", status(403), "<Code>AccessDenied</Code>");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        let err = create_failed("Azure Copy Blob", status(409), "BlobAlreadyExists");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");
        let err = create_failed("Azure Copy Blob", status(500), "BlobArchived");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");
    }
}
