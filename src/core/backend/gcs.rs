//! Google Cloud Storage backend.

use super::{
    Backend, BucketClient, Shared, Transport, Vars, bucket_exists_from_response,
    conditional_delete_outcome, create_failed, default_timeouts, delete_plain, encode, error_body,
    request_failed, send_with_retry,
};
#[cfg(feature = "gui")]
use super::{collect_pages, read_listing};
use crate::client::{Error, Result};
use crate::sign::{SignMethod, SignOptions};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

/// GCS backend. Retains the concrete [`GoogleCloudStorage`] store so its
/// `OAuth2` bearer credential can be read for the `buckets.insert` call and for
/// pre-signing.
pub struct GcsBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<GoogleCloudStorage>,
    scheme: String,
    bucket: String,
    base_url: String,
    /// The project `buckets.insert` and `buckets.list` are scoped to.
    project: Option<String>,
    /// Set by `GOOGLE_SKIP_SIGNATURE`: requests carry no bearer token.
    skip_signature: bool,
    /// The client of the bucket calls, with the store's transport settings.
    http: BucketClient,
}

impl GcsBackend {
    /// Build the GCS store with the HTTP clients and credential providers of
    /// `shared`, from `vars` (the environment, via
    /// [`GoogleCloudStorageBuilder::from_env`], unless injected).
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket (URL host) is missing or the store fails
    /// to build.
    pub fn with_shared(
        scheme: &str,
        host: Option<&str>,
        vars: &Vars,
        shared: &Shared,
    ) -> Result<Self> {
        let bucket = host.ok_or_else(|| Error::InvalidInput("Missing bucket in GCS URL".into()))?;
        let mut builder = configured(bucket, vars).with_http_connector(shared.connector());
        let base_url = base_url(vars, &builder);
        let skip_signature = builder
            .get_config_value(&GoogleConfigKey::SkipSignature)
            .and_then(|v| super::parse_bool(&v))
            .unwrap_or(false);
        let http = BucketClient::new(Transport::read(
            |k| builder.get_config_value(&GoogleConfigKey::Client(k)),
            vars,
        ));
        // The registry's one pair of providers, except for a store that skips
        // signing, whose provider must never be called. A service-account key
        // is still parsed on every build; it also sets the base URL.
        let share = !skip_signature;
        if share && let Some((credentials, signing)) = shared.gcs.get() {
            builder = builder
                .with_credentials(Arc::clone(credentials))
                .with_signing_credentials(Arc::clone(signing));
        }
        let inner = Arc::new(builder.build()?);
        if share {
            // A racing store loses; both hold equivalent providers.
            let _ = shared.gcs.set((
                Arc::clone(inner.credentials()),
                Arc::clone(inner.signing_credentials()),
            ));
        }
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
            bucket: bucket.to_string(),
            base_url,
            // buckets.insert requires the enclosing project; object_store does
            // not track it (it is irrelevant to object operations).
            project: vars
                .get("GOOGLE_CLOUD_PROJECT")
                .or_else(|| vars.get("GCS_PROJECT_ID")),
            skip_signature,
            http,
        })
    }

    /// As [`Self::with_shared`], with providers and clients of its own.
    #[cfg(test)]
    pub fn new(scheme: &str, host: Option<&str>, vars: &Vars) -> Result<Self> {
        Self::with_shared(scheme, host, vars, &Shared::default())
    }

    /// The bearer token, or an empty one (no header is sent) when signing is
    /// skipped. Under `skip_signature` this never consults the credential
    /// provider, which for a store without credentials is the GCE metadata
    /// server.
    async fn bearer(&self) -> Result<String> {
        if self.skip_signature {
            return Ok(String::new());
        }
        Ok(self
            .inner
            .credentials()
            .get_credential()
            .await?
            .bearer
            .clone())
    }

    /// `buckets.get` (`GET /storage/v1/b/<bucket>`) with the bearer token.
    async fn get_bucket(&self, bearer: &str) -> Result<reqwest::Response> {
        let uri = format!("{}/storage/v1/b/{}", self.base_url, encode(&self.bucket));
        with_bearer(self.http.get()?.get(uri), &self.base_url, bearer)
            .send()
            .await
            .map_err(|e| request_failed("GCS buckets.get request", e))
    }

    /// `buckets.list` for the project: the names of all its buckets, sorted,
    /// following `nextPageToken`.
    #[cfg(feature = "gui")]
    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        let project = self.project.as_deref().ok_or_else(|| {
            Error::Generic(
                "listing GCS buckets requires GOOGLE_CLOUD_PROJECT (or GCS_PROJECT_ID); \
                 a bucket can still be opened by its URL, gs://bucket"
                    .into(),
            )
        })?;
        let bearer = self.bearer().await?;
        collect_pages(|token| {
            let (bearer, project) = (&bearer, project);
            async move {
                let mut uri = format!(
                    "{}/storage/v1/b?project={}&fields=items/name,nextPageToken",
                    self.base_url,
                    encode(project)
                );
                if let Some(token) = token {
                    uri = format!("{uri}&pageToken={}", encode(&token));
                }
                let response = with_bearer(self.http.get()?.get(uri), &self.base_url, bearer)
                    .send()
                    .await
                    .map_err(|e| request_failed("GCS buckets.list request", e))?;
                let body = read_listing(response, "GCS buckets.list").await?;
                let page: serde_json::Value = serde_json::from_slice(&body).map_err(|_| {
                    Error::Generic("GCS buckets.list answered a document that is not JSON".into())
                })?;
                let names = page["items"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|item| item["name"].as_str().map(str::to_owned))
                    .collect();
                let next = page["nextPageToken"]
                    .as_str()
                    .filter(|t| !t.is_empty())
                    .map(str::to_owned);
                Ok((names, next))
            }
        })
        .await
    }

    /// `source` as the backend an `objects.rewrite` into this bucket may read
    /// from, or `None` when it must stream. Both sides must sign, with the very
    /// same credential providers, against the same JSON API origin.
    fn copy_source<'a>(&self, source: &'a dyn Backend) -> Option<&'a Self> {
        let other = source.as_any()?.downcast_ref::<Self>()?;
        let usable = !self.skip_signature
            && !other.skip_signature
            && self.base_url == other.base_url
            && Arc::ptr_eq(self.inner.credentials(), other.inner.credentials());
        usable.then_some(other)
    }

    /// After a `409` from `buckets.insert`: succeeds only if `project` lists
    /// the bucket. `buckets.get` would not do, because another project's
    /// bucket can be readable by anyone. The request is
    /// `buckets.list?prefix=<bucket>&maxResults=1`: names sort, so the bucket
    /// is the first item when it is in the project. Anything else, a failed
    /// answer included, is an error.
    async fn check_owned(&self, bearer: &str, project: &str) -> Result<()> {
        let uri = format!(
            "{}/storage/v1/b?project={}&prefix={}&maxResults=1&fields=items/name",
            self.base_url,
            encode(project),
            encode(&self.bucket)
        );
        let response = with_bearer(self.http.get()?.get(uri), &self.base_url, bearer)
            .send()
            .await
            .map_err(|e| request_failed("GCS buckets.list request", e))?;
        let status = response.status();
        let listed = if status.is_success() {
            let body = read_limited(response).await;
            serde_json::from_slice::<serde_json::Value>(&body)
                .is_ok_and(|v| v["items"][0]["name"].as_str() == Some(self.bucket.as_str()))
        } else {
            false
        };
        if listed {
            return Ok(());
        }
        Err(Error::Generic(format!(
            "GCS bucket `{}` already exists and belongs to another project, or these \
             credentials cannot list the project's buckets (HTTP {status})",
            self.bucket
        )))
    }
}

/// The most `objects.rewrite` calls one copy may take. A copy that stays
/// within one location and storage class is done by the first call; a large
/// one across locations or classes takes several, each moving a bounded part.
/// A copy that is still not done after this many calls fails and leaves no
/// destination object, because GCS creates it only when the rewrite is done.
const REWRITE_CALLS_MAX: usize = 1000;

/// The most of a `buckets.list` answer that is read. A longer one is cut and
/// then fails to parse. The request asks for one item, so a real answer is
/// small; an emulator that ignores `maxResults` may send more.
const LIST_LIMIT: usize = 256 * 1024;

/// The first [`LIST_LIMIT`] bytes of `response`'s body, unescaped.
async fn read_limited(mut response: reqwest::Response) -> Vec<u8> {
    let mut body = Vec::new();
    while body.len() < LIST_LIMIT
        && let Ok(Some(chunk)) = response.chunk().await
    {
        body.extend_from_slice(&chunk);
    }
    body.truncate(LIST_LIMIT);
    body
}

/// Default origin of the GCS JSON API.
const DEFAULT_BASE_URL: &str = "https://storage.googleapis.com";

/// The builder for `bucket` with `vars` and the crate's defaults applied.
fn configured(bucket: &str, vars: &Vars) -> GoogleCloudStorageBuilder {
    let mut builder = match vars {
        Vars::Env => GoogleCloudStorageBuilder::from_env(),
        Vars::Map { .. } => vars
            .config_entries::<GoogleConfigKey>("GOOGLE_")
            .into_iter()
            .fold(GoogleCloudStorageBuilder::new(), |b, (k, v)| {
                b.with_config(k, v)
            }),
    };
    for (key, value) in default_timeouts(vars, "GOOGLE_") {
        builder = builder.with_config(GoogleConfigKey::Client(key), value);
    }
    builder.with_bucket_name(bucket)
}

/// The JSON API origin (without a trailing slash): `GCS_BASE_URL`, else the
/// `GOOGLE_BASE_URL` the store was configured with, else the public default.
fn base_url(vars: &Vars, builder: &GoogleCloudStorageBuilder) -> String {
    vars.get("GCS_BASE_URL")
        .filter(|u| !u.trim().is_empty())
        .or_else(|| {
            builder
                .get_config_value(&GoogleConfigKey::BaseUrl)
                .filter(|u| !u.trim().is_empty())
        })
        .map_or_else(
            || DEFAULT_BASE_URL.to_string(),
            |u| u.trim().trim_end_matches('/').to_string(),
        )
}

/// The TLS settings `vars` give a GCS store: the JSON API origin and the
/// builder's certificate setting.
#[cfg(feature = "gui")]
pub(super) fn tls(vars: &Vars) -> super::Tls {
    let builder = configured("", vars);
    super::Tls {
        https: !super::is_plain(&base_url(vars, &builder)),
        allow_invalid_certificates: builder
            .get_config_value(&GoogleConfigKey::Client(
                object_store::ClientConfigKey::AllowInvalidCertificates,
            ))
            .and_then(|v| super::parse_bool(&v))
            .unwrap_or(false),
    }
}

/// Attaches the bearer token to `request`, but only when `base` is an https
/// origin so a token is never sent in clear text to a custom `GCS_BASE_URL`.
/// `disable_oauth` (emulators) yields an empty bearer: no header is sent.
fn with_bearer(
    request: reqwest::RequestBuilder,
    base: &str,
    bearer: &str,
) -> reqwest::RequestBuilder {
    let https = base
        .get(..8)
        .is_some_and(|p| p.eq_ignore_ascii_case("https://"));
    if bearer.is_empty() || !https {
        request
    } else {
        request.bearer_auth(bearer)
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    #[cfg(feature = "gui")]
    fn paginated(&self) -> Option<&dyn object_store::list::PaginatedListStore> {
        Some(self.inner.as_ref())
    }

    fn store(&self) -> &Arc<dyn ObjectStore> {
        &self.store
    }

    fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Creates a GCS bucket via the JSON API `buckets.insert`, authenticated
    /// with the backend's `OAuth2` bearer token (the service-account / metadata
    /// flow already implemented by `object_store`). On HTTP 409 the name is
    /// taken, so the project's bucket list is read with the same credential:
    /// the call succeeds only if the project lists the bucket, anything else
    /// means another project owns it. That needs `storage.buckets.list`.
    async fn create_bucket(&self, _url: &Url) -> Result<()> {
        let project = self.project.as_deref().ok_or_else(|| {
            Error::Generic(
                "GCS bucket creation requires GOOGLE_CLOUD_PROJECT (or GCS_PROJECT_ID)".into(),
            )
        })?;

        let bearer = self.bearer().await?;
        let body = Bytes::from(format!("{{\"name\":\"{}\"}}", self.bucket));
        let uri = format!("{}/storage/v1/b?project={}", self.base_url, encode(project));

        let request = self
            .http
            .get()?
            .post(uri)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body);
        let request = with_bearer(request, &self.base_url, &bearer);
        let response = request
            .send()
            .await
            .map_err(|e| request_failed("GCS buckets.insert request", e))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        if status.as_u16() == 409 {
            return self.check_owned(&bearer, project).await;
        }
        let text = error_body(response).await;
        Err(create_failed("GCS buckets.insert", status, &text))
    }

    /// Checks bucket existence via the JSON API `buckets.get`
    /// (`GET /storage/v1/b/<bucket>`), authenticated with the backend's `OAuth2`
    /// bearer token. This metadata-only probe avoids listing the bucket's
    /// objects: HTTP `200` means it exists and `404` that it does not. A `403` or
    /// `401` is an error: existence is unknown.
    async fn bucket_exists(&self, _url: &Url) -> Result<bool> {
        let bearer = self.bearer().await?;
        let response = self.get_bucket(&bearer).await?;
        bucket_exists_from_response(response, "GCS").await
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn copies_from(&self, src: &dyn Backend) -> bool {
        self.copy_source(src).is_some()
    }

    /// The JSON API `objects.rewrite`
    /// (`POST /storage/v1/b/<src bucket>/o/<src object>/rewriteTo/b/<bucket>/o/<object>`)
    /// with the backend's bearer token, and `ifSourceGenerationMatch` when
    /// `known` has the source's generation. The call is repeated with the
    /// `rewriteToken` of each answer until the answer says `done`, at most
    /// [`REWRITE_CALLS_MAX`] times. With no request body the destination takes
    /// the source's metadata; no storage class is sent, as a streamed copy
    /// keeps none either. A `412` is [`Error::SourceChanged`].
    async fn copy_from(
        &self,
        src: &dyn Backend,
        src_path: &ObjectPath,
        dst_path: &ObjectPath,
        known: Option<&ObjectMeta>,
    ) -> Result<bool> {
        let Some(source) = self.copy_source(src) else {
            return Ok(false);
        };
        let bearer = self.bearer().await?;
        let object_uri = format!(
            "{}/storage/v1/b/{}/o/{}/rewriteTo/b/{}/o/{}",
            self.base_url,
            encode(&source.bucket),
            encode(src_path.as_ref()),
            encode(&self.bucket),
            encode(dst_path.as_ref()),
        );
        let mut query = Vec::new();
        if let Some(generation) = known.and_then(|meta| meta.version.as_deref()) {
            query.push(format!("ifSourceGenerationMatch={}", encode(generation)));
        }
        let mut token: Option<String> = None;
        for _ in 0..REWRITE_CALLS_MAX {
            let mut pairs = query.clone();
            if let Some(token) = &token {
                pairs.push(format!("rewriteToken={}", encode(token)));
            }
            let uri = if pairs.is_empty() {
                object_uri.clone()
            } else {
                format!("{object_uri}?{}", pairs.join("&"))
            };
            let client = self.http.get()?;
            let response = send_with_retry(|| async {
                with_bearer(
                    client.post(&uri).body(Bytes::new()),
                    &self.base_url,
                    &bearer,
                )
                .send()
                .await
                .map_err(|e| request_failed("GCS objects.rewrite request", e))
            })
            .await?;
            let status = response.status();
            if status.as_u16() == 412 {
                return Err(Error::SourceChanged { url: String::new() });
            }
            if !status.is_success() {
                let text = error_body(response).await;
                return Err(create_failed("GCS objects.rewrite", status, &text));
            }
            let body = read_limited(response).await;
            let answer = serde_json::from_slice::<serde_json::Value>(&body).map_err(|_| {
                Error::Generic("GCS objects.rewrite answered with a body that is not JSON".into())
            })?;
            if answer["done"].as_bool() == Some(true) {
                return Ok(true);
            }
            token = Some(
                answer["rewriteToken"]
                    .as_str()
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| {
                        Error::Generic(
                            "GCS objects.rewrite is not done and gave no rewriteToken".into(),
                        )
                    })?
                    .to_owned(),
            );
        }
        Err(Error::Generic(format!(
            "GCS objects.rewrite was not done after {REWRITE_CALLS_MAX} calls; the source was kept"
        )))
    }

    /// The JSON API `objects.delete`
    /// (`DELETE /storage/v1/b/<bucket>/o/<object>`) with the backend's bearer
    /// token and `ifGenerationMatch`, the generation in `read`. A `412` is
    /// [`Error::SourceChanged`], and `404` is success. Without a generation
    /// (`0` means "no live object" and is never sent), or on a `501` answer,
    /// the delete is the plain one of the store. Transient failures are
    /// retried, see [`send_with_retry`].
    async fn delete_if_unchanged(&self, path: &ObjectPath, read: &ObjectMeta) -> Result<()> {
        let Some(generation) = read
            .version
            .as_deref()
            .filter(|v| v.parse::<u64>().is_ok_and(|g| g > 0))
        else {
            return delete_plain(self.store().as_ref(), path).await;
        };
        let bearer = self.bearer().await?;
        let uri = format!(
            "{}/storage/v1/b/{}/o/{}?ifGenerationMatch={}",
            self.base_url,
            encode(&self.bucket),
            encode(path.as_ref()),
            encode(generation),
        );
        let client = self.http.get()?;
        let response = send_with_retry(|| async {
            with_bearer(client.delete(&uri), &self.base_url, &bearer)
                .send()
                .await
                .map_err(|e| request_failed("GCS objects.delete request", e))
        })
        .await?;
        let status = response.status();
        let text = if status.is_success() || status.as_u16() == 404 {
            String::new()
        } else {
            error_body(response).await
        };
        let done = conditional_delete_outcome(
            "GCS objects.delete",
            status,
            status.as_u16() == 412,
            false,
            &text,
        )?;
        if done {
            Ok(())
        } else {
            delete_plain(self.store().as_ref(), path).await
        }
    }

    /// `None` under `skip_signature`: the store's signer would read the
    /// credential provider, which for a store without credentials is the GCE
    /// metadata server.
    fn signer(&self) -> Option<&dyn Signer> {
        (!self.skip_signature).then_some(self.inner.as_ref() as &dyn Signer)
    }

    /// Host-only signing, several URLs at a time: without a private key each
    /// URL is a `signBlob` request, which the store's own batch loop sends one
    /// after another.
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
        let signing = paths
            .iter()
            .map(|path| signer.signed_url(method.into_http(), path, expires_in))
            .collect();
        sign_ordered(signing).await
    }
}

/// Signatures made at a time at most, since `signBlob` is limited per project.
const SIGN_CONCURRENCY: usize = 8;

/// The URLs `signing` makes, in its order. The first failure is returned and
/// the signatures still running are dropped.
async fn sign_ordered<Fut>(signing: Vec<Fut>) -> Result<Vec<String>>
where
    Fut: Future<Output = object_store::Result<Url>>,
{
    let urls: Vec<Url> = futures::stream::iter(signing)
        .buffered(SIGN_CONCURRENCY)
        .try_collect()
        .await?;
    Ok(urls.into_iter().map(Into::into).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::stub::{Reply, Request, serve};
    use object_store::ClientConfigKey;

    /// Runs `create_bucket` against a stub that answers `replies` in order.
    async fn create(replies: Vec<Reply>) -> (Result<()>, Vec<Request>) {
        let stub = serve(replies).await;
        let backend = GcsBackend::new(
            "gs",
            Some("bkt"),
            &Vars::from_pairs(&[
                ("GCS_BASE_URL", &stub.base),
                ("GOOGLE_CLOUD_PROJECT", "proj"),
                ("GOOGLE_BEARER_TOKEN", "tok"),
            ]),
        )
        .expect("build");
        let url = Url::parse("gs://bkt").expect("url");
        let result = backend.create_bucket(&url).await;
        (result, stub.requests())
    }

    // Emulators end the document with a newline.
    const LISTED: &str = "{\"items\":[{\"name\":\"bkt\"}]}\n";

    #[tokio::test]
    async fn gcs_conflict_on_an_owned_bucket_succeeds() {
        let (result, requests) = create(vec![Reply::new(409, ""), Reply::new(200, LISTED)]).await;
        result.expect("owned bucket");
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].line, "POST /storage/v1/b?project=proj HTTP/1.1");
        assert_eq!(
            requests[1].line,
            "GET /storage/v1/b?project=proj&prefix=bkt&maxResults=1&fields=items/name HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn gcs_conflict_on_a_foreign_bucket_fails() {
        let answers = [
            (200, "{}"),
            (200, r#"{"items":[{"name":"bkt-other"}]}"#),
            (200, "not json"),
            (403, ""),
            (404, ""),
        ];
        for (status, body) in answers {
            let (result, _) = create(vec![Reply::new(409, ""), Reply::new(status, body)]).await;
            let err = result.expect_err("foreign bucket");
            let text = err.to_string();
            assert!(text.contains("`bkt`"), "{text}");
            assert!(text.contains("another project"), "{text}");
        }
    }

    #[tokio::test]
    async fn gcs_forbidden_creation_is_denied() {
        for status in [401, 403] {
            let (result, _) = create(vec![Reply::new(status, "")]).await;
            let err = result.expect_err("denied");
            assert!(matches!(err, Error::Denied(_)), "{err:?}");
        }
        let (result, _) = create(vec![Reply::new(500, "")]).await;
        assert!(matches!(result, Err(Error::Generic(_))));
    }

    /// A backend that runs unsigned, with no token, against `base`.
    fn unsigned(base: &str) -> GcsBackend {
        GcsBackend::new(
            "gs",
            Some("bkt"),
            &Vars::from_pairs(&[
                ("GCS_BASE_URL", base),
                ("GOOGLE_CLOUD_PROJECT", "proj"),
                ("GOOGLE_SKIP_SIGNATURE", "true"),
            ]),
        )
        .expect("build")
    }

    #[tokio::test]
    async fn skip_signature_never_reaches_the_credential_provider() {
        let stub = serve(vec![Reply::new(200, "{}"), Reply::new(200, "{}")]).await;
        let backend = unsigned(&stub.base);
        let url = Url::parse("gs://bkt").expect("url");
        // The credential provider would try the GCE metadata server, which
        // cannot be redirected here, so a short limit separates the two.
        let calls = async {
            assert!(backend.bucket_exists(&url).await?);
            backend.create_bucket(&url).await
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), calls)
            .await
            .expect("no credential lookup")
            .expect("calls");
        for request in stub.requests() {
            let headers = request.headers.to_ascii_lowercase();
            assert!(!headers.contains("authorization"), "{headers}");
        }
        assert!(backend.signer().is_none());
    }

    fn paths(n: usize) -> Vec<ObjectPath> {
        (0..n).map(|i| ObjectPath::from(format!("o{i}"))).collect()
    }

    #[tokio::test]
    async fn signing_runs_several_at_a_time_and_keeps_the_order() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (running, peak) = (AtomicUsize::new(0), AtomicUsize::new(0));
        let paths = paths(20);
        let signing = (0..paths.len()).map(|index| {
            let (running, peak) = (&running, &peak);
            async move {
                let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(now, Ordering::SeqCst);
                // Earlier paths finish later, so unordered output would show.
                tokio::time::sleep(Duration::from_millis(40 - 2 * index as u64)).await;
                running.fetch_sub(1, Ordering::SeqCst);
                Ok(Url::parse(&format!("https://h/{index}")).expect("url"))
            }
        });
        let urls = sign_ordered(signing.collect()).await.expect("signed");
        let want: Vec<_> = (0..20).map(|i| format!("https://h/{i}")).collect();
        assert_eq!(urls, want);
        assert_eq!(peak.load(Ordering::SeqCst), SIGN_CONCURRENCY);
    }

    #[tokio::test]
    async fn the_first_failed_signature_fails_the_call() {
        let paths = paths(12);
        let signing = paths.iter().map(|path| {
            let name = path.to_string();
            async move {
                if name == "o3" || name == "o9" {
                    Err(object_store::Error::Generic {
                        store: "GCS",
                        source: format!("{name} refused").into(),
                    })
                } else {
                    Ok(Url::parse("https://h/x").expect("url"))
                }
            }
        });
        let result = sign_ordered(signing.collect()).await;
        let err = result.expect_err("fails").to_string();
        assert!(err.contains("o3 refused"), "{err}");
    }

    #[test]
    fn bucket_calls_allow_plain_http_for_an_emulator() {
        // `object_store`'s GCS builder allows http by default.
        let backend = unsigned("http://127.0.0.1:4443");
        assert!(backend.http.transport().allow_http);
        let invalid = GcsBackend::new(
            "gs",
            Some("bkt"),
            &Vars::from_pairs(&[("GOOGLE_ALLOW_INVALID_CERTIFICATES", "true")]),
        )
        .expect("build");
        assert!(invalid.http.transport().allow_invalid_certificates);
    }

    #[test]
    fn base_url_falls_back_to_the_stores_google_base_url() {
        let base = |pairs: &[(&str, &str)]| {
            let vars = Vars::from_pairs(pairs);
            base_url(&vars, &configured("b", &vars))
        };
        assert_eq!(
            base(&[("GOOGLE_BASE_URL", "http://127.0.0.1:4443/")]),
            "http://127.0.0.1:4443"
        );
        assert_eq!(
            base(&[
                ("GCS_BASE_URL", "http://127.0.0.1:1"),
                ("GOOGLE_BASE_URL", "http://127.0.0.1:2")
            ]),
            "http://127.0.0.1:1"
        );
    }

    #[test]
    fn base_url_defaults_to_googleapis() {
        let base = |pairs: &[(&str, &str)]| {
            let vars = Vars::from_pairs(pairs);
            base_url(&vars, &configured("b", &vars))
        };
        assert_eq!(base(&[]), "https://storage.googleapis.com");
        assert_eq!(
            base(&[("GCS_BASE_URL", "  ")]),
            "https://storage.googleapis.com"
        );
    }

    #[test]
    fn base_url_override_drops_trailing_slash() {
        let vars = Vars::from_pairs(&[("GCS_BASE_URL", "http://127.0.0.1:4443/")]);
        assert_eq!(
            base_url(&vars, &configured("b", &vars)),
            "http://127.0.0.1:4443"
        );
    }

    #[test]
    fn bearer_is_sent_only_over_https() {
        let client = reqwest::Client::new();
        let header = |base: &str, token: &str| {
            with_bearer(client.get("http://localhost/x"), base, token)
                .build()
                .expect("request")
                .headers()
                .get(http::header::AUTHORIZATION)
                .cloned()
        };
        assert!(header("https://storage.googleapis.com", "tok").is_some());
        assert!(header("HTTPS://example.com", "tok").is_some());
        assert!(header("http://127.0.0.1:4443", "tok").is_none());
        assert!(header("https://storage.googleapis.com", "").is_none());
    }

    #[test]
    fn timeouts_default_to_a_day_and_five_minutes_and_the_users_win() {
        let key = |k| GoogleConfigKey::Client(k);
        let default = configured("b", &Vars::from_pairs(&[]));
        assert_eq!(
            default
                .get_config_value(&key(ClientConfigKey::Timeout))
                .as_deref(),
            Some("86400s")
        );
        assert_eq!(
            default
                .get_config_value(&key(ClientConfigKey::ReadTimeout))
                .as_deref(),
            Some("300s")
        );
        let set = configured(
            "b",
            &Vars::from_pairs(&[("GOOGLE_TIMEOUT", "7s"), ("GOOGLE_READ_TIMEOUT", "9s")]),
        );
        assert_eq!(
            set.get_config_value(&key(ClientConfigKey::Timeout))
                .as_deref(),
            Some("7s")
        );
        assert_eq!(
            set.get_config_value(&key(ClientConfigKey::ReadTimeout))
                .as_deref(),
            Some("9s")
        );
    }

    const KEY: &str = r#"{"private_key": "k", "private_key_id": "i", "client_email": "e", "disable_oauth": true}"#;

    #[test]
    fn buckets_of_one_registry_share_credential_providers() {
        let vars = Vars::from_pairs(&[("GOOGLE_SERVICE_ACCOUNT_KEY", KEY)]);
        let shared = Shared::default();
        let a = GcsBackend::with_shared("gs", Some("a"), &vars, &shared).expect("a");
        let b = GcsBackend::with_shared("gs", Some("b"), &vars, &shared).expect("b");
        assert!(Arc::ptr_eq(a.inner.credentials(), b.inner.credentials()));
        assert!(Arc::ptr_eq(
            a.inner.signing_credentials(),
            b.inner.signing_credentials()
        ));
        let (held, signing) = shared.gcs.get().expect("stored");
        assert!(Arc::ptr_eq(held, a.inner.credentials()));
        assert!(Arc::ptr_eq(signing, a.inner.signing_credentials()));
        let other = GcsBackend::new("gs", Some("a"), &vars).expect("other");
        assert!(!Arc::ptr_eq(
            a.inner.credentials(),
            other.inner.credentials()
        ));
    }

    #[test]
    fn a_store_that_skips_signing_takes_no_shared_provider() {
        let shared = Shared::default();
        let vars = Vars::from_pairs(&[("GOOGLE_SKIP_SIGNATURE", "true")]);
        GcsBackend::with_shared("gs", Some("a"), &vars, &shared).expect("a");
        assert!(shared.gcs.get().is_none());
        let signed = Vars::from_pairs(&[("GOOGLE_SERVICE_ACCOUNT_KEY", KEY)]);
        let first = GcsBackend::with_shared("gs", Some("b"), &signed, &shared).expect("b");
        let skipping = GcsBackend::with_shared("gs", Some("c"), &vars, &shared).expect("c");
        assert!(!Arc::ptr_eq(
            first.inner.credentials(),
            skipping.inner.credentials()
        ));
    }

    fn rewrite_vars(base: &str) -> Vec<(&str, &str)> {
        vec![("GCS_BASE_URL", base), ("GOOGLE_BEARER_TOKEN", "tok")]
    }

    /// Backends for buckets `bkt` (destination) and `src` of one registry,
    /// talking to the stub at `base`.
    fn rewrite_pair(base: &str) -> (GcsBackend, GcsBackend) {
        let vars = Vars::from_pairs(&rewrite_vars(base));
        let shared = Shared::default();
        let build =
            |bucket| GcsBackend::with_shared("gs", Some(bucket), &vars, &shared).expect("build");
        (build("bkt"), build("src"))
    }

    fn meta(version: Option<&str>) -> ObjectMeta {
        ObjectMeta {
            location: ObjectPath::from("a"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 1,
            e_tag: None,
            version: version.map(str::to_owned),
        }
    }

    fn partial(token: &str) -> Reply {
        Reply::new(
            200,
            format!(
                "{{\"done\":false,\"rewriteToken\":\"{token}\",\"totalBytesRewritten\":\"1\"}}"
            ),
        )
    }

    fn done() -> Reply {
        Reply::new(200, "{\"done\":true,\"resource\":{\"name\":\"d/e\"}}")
    }

    /// `copy_from` of `a b/c` to `d/e` against a stub answering `replies`.
    async fn rewrite(
        replies: Vec<Reply>,
        known: Option<&ObjectMeta>,
    ) -> (Result<bool>, Vec<Request>) {
        let stub = serve(replies).await;
        let (dst, src) = rewrite_pair(&stub.base);
        let result = dst
            .copy_from(
                &src,
                &ObjectPath::from("a b/c"),
                &ObjectPath::from("d/e"),
                known,
            )
            .await;
        (result, stub.requests())
    }

    #[tokio::test]
    async fn a_cross_bucket_copy_is_one_rewrite() {
        let known = meta(Some("1234"));
        let (result, requests) = rewrite(vec![done()], Some(&known)).await;
        assert!(result.expect("rewrite"));
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].line,
            "POST /storage/v1/b/src/o/a%20b%2Fc/rewriteTo/b/bkt/o/d%2Fe\
             ?ifSourceGenerationMatch=1234 HTTP/1.1"
        );
        assert_eq!(requests[0].body.len(), 0);
    }

    #[tokio::test]
    async fn a_rewrite_without_a_known_generation_sends_no_condition() {
        let (result, requests) = rewrite(vec![done()], None).await;
        assert!(result.expect("rewrite"));
        assert!(!requests[0].line.contains('?'), "{}", requests[0].line);
    }

    #[tokio::test]
    async fn a_rewrite_continues_with_its_token_until_done() {
        let known = meta(Some("7"));
        let replies = vec![partial("T 1"), partial("T2"), done()];
        let (result, requests) = rewrite(replies, Some(&known)).await;
        assert!(result.expect("rewrite"));
        let queries: Vec<_> = requests
            .iter()
            .map(|r| r.line.split('?').nth(1).unwrap_or_default().to_owned())
            .collect();
        assert_eq!(
            queries,
            [
                "ifSourceGenerationMatch=7 HTTP/1.1",
                "ifSourceGenerationMatch=7&rewriteToken=T%201 HTTP/1.1",
                "ifSourceGenerationMatch=7&rewriteToken=T2 HTTP/1.1",
            ]
        );
    }

    #[tokio::test]
    async fn a_rewrite_that_never_finishes_stops_at_the_bound() {
        let replies = (0..REWRITE_CALLS_MAX).map(|_| partial("T")).collect();
        let (result, requests) = rewrite(replies, None).await;
        let err = result.expect_err("bound");
        assert!(err.to_string().contains("source was kept"), "{err}");
        assert_eq!(requests.len(), REWRITE_CALLS_MAX);
    }

    #[tokio::test]
    async fn an_unfinished_rewrite_without_a_token_is_an_error() {
        let (result, requests) = rewrite(vec![Reply::new(200, "{\"done\":false}")], None).await;
        assert!(result.is_err());
        assert_eq!(requests.len(), 1);
        let (result, _) = rewrite(vec![Reply::new(200, "not json")], None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_failed_rewrite_is_an_error_and_a_changed_source_is_reported() {
        let (result, _) = rewrite(vec![Reply::new(412, "{}")], Some(&meta(Some("1")))).await;
        assert!(matches!(
            result.expect_err("412"),
            Error::SourceChanged { .. }
        ));
        let (result, _) = rewrite(vec![Reply::new(403, "denied\x1b[31m")], None).await;
        let err = result.expect_err("403");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        assert!(err.to_string().contains("denied\\u{1b}"), "{err}");
        let (result, _) = rewrite(vec![Reply::new(500, "boom")], None).await;
        assert!(matches!(result.expect_err("500"), Error::Generic(_)));
    }

    #[tokio::test]
    async fn rewrites_stay_in_the_provider_origin_and_registry() {
        let stub = serve(vec![]).await;
        let (dst, src) = rewrite_pair(&stub.base);
        assert!(dst.copies_from(&src));

        let build = |pairs: &[(&str, &str)], shared: &Shared| {
            GcsBackend::with_shared("gs", Some("src"), &Vars::from_pairs(pairs), shared)
                .expect("other")
        };
        // Another registry holds other credential providers.
        assert!(!dst.copies_from(&build(&rewrite_vars(&stub.base), &Shared::default())));
        // The same registry, another origin or a store that skips signing.
        let shared = Shared::default();
        let first = build(&rewrite_vars(&stub.base), &shared);
        let moved = build(
            &[
                ("GCS_BASE_URL", "http://other.invalid:9"),
                ("GOOGLE_BEARER_TOKEN", "tok"),
            ],
            &shared,
        );
        assert!(!first.copies_from(&moved) && !moved.copies_from(&first));
        let skipping = build(
            &[
                ("GCS_BASE_URL", &stub.base),
                ("GOOGLE_SKIP_SIGNATURE", "true"),
            ],
            &shared,
        );
        assert!(skipping.skip_signature);
        assert!(!first.copies_from(&skipping) && !skipping.copies_from(&first));
        // Another provider.
        let memory: Arc<dyn Backend> = Arc::new(crate::backend::TestBackend {
            store: Arc::new(object_store::memory::InMemory::new()),
            multipart: false,
        });
        assert!(!dst.copies_from(memory.as_ref()));
        assert!(
            !dst.copy_from(memory.as_ref(), &"a".into(), &"b".into(), None)
                .await
                .expect("refused")
        );
        assert!(stub.requests().is_empty());
    }

    #[tokio::test]
    async fn each_rewrite_call_is_retried_up_to_three_attempts() {
        let busy = || Reply::new(503, "{}");
        let (result, requests) =
            rewrite(vec![busy(), partial("T"), busy(), busy(), done()], None).await;
        assert!(result.expect("rewrite"));
        assert_eq!(requests.len(), 5);
        let (result, requests) = rewrite(vec![busy(), busy(), busy()], None).await;
        assert!(result.is_err());
        assert_eq!(requests.len(), 3);
    }

    #[cfg(feature = "gui")]
    mod list_buckets {
        use super::*;

        fn backend(base: &str, extra: &[(&str, &str)]) -> GcsBackend {
            let mut pairs = vec![("GCS_BASE_URL", base), ("GOOGLE_BEARER_TOKEN", "tok")];
            pairs.extend_from_slice(extra);
            GcsBackend::new("gs", Some("bkt"), &Vars::from_pairs(&pairs)).expect("build")
        }

        async fn list(replies: Vec<Reply>) -> (Result<Vec<String>>, Vec<Request>) {
            let stub = serve(replies).await;
            let result = backend(&stub.base, &[("GOOGLE_CLOUD_PROJECT", "my proj")])
                .list_buckets()
                .await;
            (result, stub.requests())
        }

        #[tokio::test]
        async fn pages_are_followed_and_the_names_sorted() {
            let (names, requests) = list(vec![
                Reply::new(
                    200,
                    r#"{"items":[{"name":"zeta"},{"name":"al\u0026pha"}],"nextPageToken":"p/1"}"#,
                ),
                Reply::new(200, "{\"items\":[{\"name\":\"mid\"}]}\n"),
            ])
            .await;
            assert_eq!(names.expect("list"), ["al&pha", "mid", "zeta"]);
            let query = "fields=items/name,nextPageToken";
            assert_eq!(
                requests[0].line,
                format!("GET /storage/v1/b?project=my%20proj&{query} HTTP/1.1")
            );
            assert_eq!(
                requests[1].line,
                format!("GET /storage/v1/b?project=my%20proj&{query}&pageToken=p%2F1 HTTP/1.1")
            );
        }

        #[tokio::test]
        async fn no_buckets_is_an_empty_list() {
            for body in ["{}", r#"{"items":[]}"#] {
                let (names, _) = list(vec![Reply::new(200, body)]).await;
                assert!(names.expect("list").is_empty(), "{body}");
            }
        }

        #[tokio::test]
        async fn without_a_project_no_request_is_sent() {
            let stub = serve(vec![]).await;
            let err = backend(&stub.base, &[])
                .list_buckets()
                .await
                .expect_err("no project");
            assert!(err.to_string().contains("GOOGLE_CLOUD_PROJECT"), "{err}");
            assert!(stub.requests().is_empty());
        }
    }

    /// `delete_if_unchanged` of `a b/c` against a stub answering `replies`.
    async fn conditional_delete(
        replies: Vec<Reply>,
        read: &ObjectMeta,
    ) -> (Result<()>, Vec<Request>) {
        let stub = serve(replies).await;
        // The store, which a plain delete goes through, talks to the stub too.
        let mut pairs = rewrite_vars(&stub.base);
        pairs.push(("GOOGLE_BASE_URL", &stub.base));
        let dst = GcsBackend::with_shared(
            "gs",
            Some("bkt"),
            &Vars::from_pairs(&pairs),
            &Shared::default(),
        )
        .expect("build");
        let result = dst
            .delete_if_unchanged(&ObjectPath::from("a b/c"), read)
            .await;
        (result, stub.requests())
    }

    #[tokio::test]
    async fn a_conditional_delete_carries_the_generation() {
        let (done, requests) =
            conditional_delete(vec![Reply::new(204, "")], &meta(Some("1234"))).await;
        done.expect("delete");
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].line,
            "DELETE /storage/v1/b/bkt/o/a%20b%2Fc?ifGenerationMatch=1234 HTTP/1.1"
        );
    }

    #[tokio::test]
    async fn a_conditional_delete_maps_the_answers() {
        let read = meta(Some("7"));
        let (done, requests) = conditional_delete(vec![Reply::new(412, "{}")], &read).await;
        let err = done.expect_err("changed");
        assert!(
            matches!(&err, Error::SourceChanged { url } if url.is_empty()),
            "{err:?}"
        );
        assert_eq!(requests.len(), 1);
        let (done, _) = conditional_delete(vec![Reply::new(404, "{}")], &read).await;
        done.expect("gone");
        let (done, _) = conditional_delete(vec![Reply::new(409, "{}")], &read).await;
        assert!(matches!(done.expect_err("409"), Error::Generic(_)));
    }

    #[tokio::test]
    async fn a_server_without_conditional_deletes_gets_a_plain_delete() {
        let replies = vec![Reply::new(501, ""), Reply::new(204, "")];
        let (done, requests) = conditional_delete(replies, &meta(Some("7"))).await;
        done.expect("fallback");
        assert_eq!(requests.len(), 2);
        assert!(requests[0].line.contains("ifGenerationMatch=7"));
        assert!(!requests[1].line.contains("ifGenerationMatch"));
    }

    #[tokio::test]
    async fn a_delete_without_a_usable_generation_is_plain() {
        for version in [None, Some("0"), Some("abc")] {
            let (done, requests) =
                conditional_delete(vec![Reply::new(204, "")], &meta(version)).await;
            done.expect("delete");
            assert_eq!(requests.len(), 1, "{version:?}");
            assert!(requests[0].line.starts_with("DELETE "), "{version:?}");
            assert!(
                !requests[0].line.contains("ifGenerationMatch"),
                "{version:?}"
            );
        }
    }
}
