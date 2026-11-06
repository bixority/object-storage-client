//! Azure Blob Storage backend.

use super::{
    Backend, BucketClient, Shared, Transport, Vars, bucket_exists_from_response, create_failed,
    default_timeouts, error_body, ms_error_code, send_signed, send_with_retry,
};
#[cfg(feature = "gui")]
use super::{
    ListPage, ListedObject, ObjectState, collect_pages, encode, read_listing, xml_blocks,
    xml_listing_page, xml_texts,
};
use crate::client::{Error, Result};
use http::Method;
use object_store::ClientConfigKey;
use object_store::azure::{
    AzureAuthorizer, AzureConfigKey, AzureCredential, MicrosoftAzure, MicrosoftAzureBuilder,
};
use object_store::client::{HttpRequest, HttpRequestBody};
use object_store::path::Path as ObjectPath;
use object_store::signer::Signer;
use object_store::{ObjectMeta, ObjectStore};
use std::sync::Arc;
use std::time::Duration;
use url::Url;

// Unit tests run on the real clock against a loopback stub and use shorter
// values. A paused tokio clock jumps to the HTTP timeouts while a loopback
// connect is still pending on macOS, so it cannot be used for these.

/// The first pause between two polls of a pending server-side copy.
#[cfg(not(test))]
const COPY_POLL_START: Duration = Duration::from_millis(250);
#[cfg(test)]
const COPY_POLL_START: Duration = Duration::from_millis(5);
/// The longest pause between two polls; the pause doubles up to this.
#[cfg(not(test))]
const COPY_POLL_MAX: Duration = Duration::from_secs(4);
#[cfg(test)]
const COPY_POLL_MAX: Duration = Duration::from_millis(40);
/// How long a copy may stay pending before the call fails. The copy itself is
/// not aborted and may still finish.
#[cfg(not(test))]
const COPY_WAIT_MAX: Duration = Duration::from_secs(120);
#[cfg(test)]
const COPY_WAIT_MAX: Duration = Duration::from_secs(1);

/// Azure backend. Retains the concrete [`MicrosoftAzure`] store so its storage
/// credentials can be read for the `Create Container` call and for pre-signing.
pub struct AzureBackend {
    store: Arc<dyn ObjectStore>,
    inner: Arc<MicrosoftAzure>,
    scheme: String,
    container: String,
    /// The target of the container calls, read from the configured builder.
    target: Target,
    /// Set by `AZURE_SKIP_SIGNATURE`: requests are sent unsigned.
    skip_signature: bool,
    /// The client of the container calls, with the store's transport settings.
    http: BucketClient,
    /// `AZURE_STORAGE_ENCRYPTION_KEY` is set: the store's own requests carry the
    /// customer-provided key headers, the hand-built copy request does not.
    customer_key: bool,
}

/// What the container-management requests are sent to, read from the builder
/// the store is built from (so every `object_store` alias applies).
struct Target {
    account: Option<String>,
    endpoint: Option<String>,
    allow_http: bool,
    /// The setting that moves the store's endpoint in a way these calls do not
    /// follow, if one is set.
    mode: Option<&'static str>,
}

impl Target {
    fn read(builder: &MicrosoftAzureBuilder) -> Self {
        let get = |key: &AzureConfigKey| builder.get_config_value(key);
        let flag =
            |key: &AzureConfigKey| get(key).and_then(|v| super::parse_bool(&v)) == Some(true);
        let mode = if flag(&AzureConfigKey::UseEmulator) {
            Some("AZURE_STORAGE_USE_EMULATOR")
        } else if flag(&AzureConfigKey::UseFabricEndpoint) {
            Some("AZURE_USE_FABRIC_ENDPOINT")
        } else {
            None
        };
        Self {
            account: get(&AzureConfigKey::AccountName),
            endpoint: get(&AzureConfigKey::Endpoint),
            allow_http: get(&AzureConfigKey::Client(ClientConfigKey::AllowHttp))
                .and_then(|v| super::parse_bool(&v))
                .unwrap_or(false),
            mode,
        }
    }
}

impl AzureBackend {
    /// Build the Azure store with the HTTP clients and credential provider of
    /// `shared`, from `vars` (the environment, via
    /// [`MicrosoftAzureBuilder::from_env`], unless injected).
    ///
    /// # Errors
    ///
    /// Returns an error if the container (URL host) is missing or the store
    /// fails to build.
    pub fn with_shared(
        scheme: &str,
        host: Option<&str>,
        vars: &Vars,
        shared: &Shared,
    ) -> Result<Self> {
        let container =
            host.ok_or_else(|| Error::InvalidInput("Missing container in Azure URL".into()))?;
        let mut builder = configured(container, vars).with_http_connector(shared.connector());
        let target = Target::read(&builder);
        let skip_signature = builder
            .get_config_value(&AzureConfigKey::SkipSignature)
            .and_then(|v| super::parse_bool(&v))
            .unwrap_or(false);
        let customer_key = builder
            .get_config_value(&AzureConfigKey::EncryptionKey)
            .is_some();
        let http = BucketClient::new(Transport::read(
            |k| builder.get_config_value(&AzureConfigKey::Client(k)),
            vars,
        ));
        // The registry's one provider, except for a store that skips signing,
        // whose provider must never be called.
        let share = !skip_signature;
        if share && let Some(credentials) = shared.azure.get() {
            builder = builder.with_credentials(Arc::clone(credentials));
        }
        let inner = Arc::new(builder.build()?);
        if share {
            // A racing store loses; both hold equivalent providers.
            let _ = shared.azure.set(Arc::clone(inner.credentials()));
        }
        Ok(Self {
            store: Arc::clone(&inner) as Arc<dyn ObjectStore>,
            inner,
            scheme: scheme.to_string(),
            container: container.to_string(),
            target,
            skip_signature,
            http,
            customer_key,
        })
    }

    /// As [`Self::with_shared`], with providers and clients of its own.
    #[cfg(test)]
    pub fn new(scheme: &str, host: Option<&str>, vars: &Vars) -> Result<Self> {
        Self::with_shared(scheme, host, vars, &Shared::default())
    }

    /// The `(account, blob endpoint)` the container-management requests
    /// target, as the store's builder was configured: the account name and
    /// endpoint with every alias `object_store` accepts.
    ///
    /// # Errors
    ///
    /// Returns an error if the account name is unset, if the emulator or the
    /// Fabric endpoint is selected without an explicit endpoint (the store
    /// derives its own address then, which these calls do not), or if the
    /// endpoint is plain `http://` while `AZURE_ALLOW_HTTP` does not allow it:
    /// the credential (key, SAS or bearer token) would travel in the clear.
    /// That is the setting `object_store` requires for the object path too.
    fn account_and_endpoint(&self) -> Result<(String, String)> {
        let target = &self.target;
        if let Some(mode) = target.mode
            && target.endpoint.is_none()
        {
            return Err(Error::Generic(format!(
                "Azure container operations do not follow {mode}; set AZURE_STORAGE_ENDPOINT"
            )));
        }
        let account = target.account.clone().ok_or_else(|| {
            Error::Generic("Azure container operations require AZURE_STORAGE_ACCOUNT_NAME".into())
        })?;
        let endpoint = target
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://{account}.blob.core.windows.net"));
        let plain = endpoint
            .get(..7)
            .is_some_and(|p| p.eq_ignore_ascii_case("http://"));
        if plain && !target.allow_http {
            return Err(Error::Generic(
                "Azure container operations over plain http need AZURE_ALLOW_HTTP=true".into(),
            ));
        }
        Ok((account, endpoint))
    }

    /// The storage credential, or `None` when signing is skipped.
    ///
    /// Under `skip_signature` this never consults the credential provider:
    /// [`MicrosoftAzure::credentials`] hands back the raw provider, which for a
    /// store without credentials is the instance metadata service.
    async fn credential(&self) -> Result<Option<Arc<AzureCredential>>> {
        if self.skip_signature {
            return Ok(None);
        }
        Ok(Some(self.inner.credentials().get_credential().await?))
    }

    /// A container-level request (`<method> /<container>?restype=container`),
    /// authorized with `object_store`'s `SharedKey` / SAS / Bearer scheme
    /// unless signing is skipped.
    async fn container_request(&self, method: Method) -> Result<HttpRequest> {
        let (account, endpoint) = self.account_and_endpoint()?;
        let uri = format!(
            "{}/{}?restype=container",
            endpoint.trim_end_matches('/'),
            self.container
        );
        self.signed_request(method, &uri, &account, &[]).await
    }

    /// `List Containers` (`GET /?comp=list` on the account endpoint): the names
    /// of all containers of the account, sorted, following `NextMarker`.
    #[cfg(feature = "gui")]
    pub async fn list_buckets(&self) -> Result<Vec<String>> {
        collect_pages(|marker| async move {
            let (account, endpoint) = self.account_and_endpoint()?;
            let mut uri = format!("{}/?comp=list", endpoint.trim_end_matches('/'));
            if let Some(marker) = marker {
                uri = format!("{uri}&marker={}", encode(&marker));
            }
            let request = self
                .signed_request(Method::GET, &uri, &account, &[])
                .await?;
            xml_listing_page(
                self.http.get()?,
                request,
                "Azure List Containers",
                "NextMarker",
            )
            .await
        })
        .await
    }

    /// A blob-level request for `path`, built and authorized like
    /// [`container_request`](Self::container_request). The path is encoded as
    /// `object_store` encodes it.
    async fn blob_request(&self, method: Method, path: &ObjectPath) -> Result<HttpRequest> {
        self.blob_request_with(method, path, &[]).await
    }

    /// As [`blob_request`](Self::blob_request), with `headers` set before the
    /// request is authorized, so that they are signed.
    async fn blob_request_with(
        &self,
        method: Method,
        path: &ObjectPath,
        headers: &[(&'static str, http::HeaderValue)],
    ) -> Result<HttpRequest> {
        let (account, url) = self.blob_url(path)?;
        self.signed_request(method, url.as_str(), &account, headers)
            .await
    }

    /// The account and the URL of the blob `path` in this container.
    fn blob_url(&self, path: &ObjectPath) -> Result<(String, Url)> {
        let (account, endpoint) = self.account_and_endpoint()?;
        let mut url = Url::parse(&endpoint)
            .map_err(|e| Error::Generic(format!("Invalid Azure endpoint: {e}")))?;
        url.path_segments_mut()
            .map_err(|()| Error::Generic("Invalid Azure endpoint".into()))?
            .pop_if_empty()
            .push(&self.container)
            .extend(path.parts());
        Ok((account, url))
    }

    /// An empty-bodied request to `uri` with `headers`, signed unless signing
    /// is skipped.
    async fn signed_request(
        &self,
        method: Method,
        uri: &str,
        account: &str,
        headers: &[(&'static str, http::HeaderValue)],
    ) -> Result<HttpRequest> {
        let credential = self.credential().await?;
        let mut builder = http::Request::builder().method(&method).uri(uri);
        if method == Method::PUT {
            builder = builder.header(http::header::CONTENT_LENGTH, "0");
        }
        for (name, value) in headers {
            builder = builder.header(*name, value);
        }
        let mut request: HttpRequest = builder
            .body(HttpRequestBody::empty())
            .map_err(|e| Error::Generic(format!("Failed to build Azure request: {e}")))?;

        if let Some(credential) = credential {
            AzureAuthorizer::new(credential.as_ref(), account)
                .try_authorize(&mut request)
                .map_err(|e| Error::Generic(format!("Failed to sign Azure request: {e}")))?;
        }
        Ok(request)
    }

    /// `source` as the backend a `Copy Blob` into this container may read
    /// from, or `None` when it must stream. It must sign, with the very same
    /// credential provider, in the same account (the same endpoint). The
    /// store's endpoint must be usable for direct calls too, and neither side
    /// has a customer-provided encryption key, which the copy request would not
    /// carry.
    fn copy_source<'a>(&self, source: &'a dyn Backend) -> Option<&'a Self> {
        let other = source.as_any()?.downcast_ref::<Self>()?;
        let usable = !self.skip_signature
            && !other.skip_signature
            && !self.customer_key
            && !other.customer_key
            && Arc::ptr_eq(self.inner.credentials(), other.inner.credentials())
            && self.account_and_endpoint().ok()? == other.account_and_endpoint().ok()?;
        usable.then_some(other)
    }

    /// One `Get Blob Properties` on `path`: whether its copy is still pending.
    /// A copy that failed or was aborted is an error.
    async fn copy_pending(&self, path: &ObjectPath) -> Result<bool> {
        let request = self.blob_request(Method::HEAD, path).await?;
        let response = send_signed(self.http.get()?, request).await?;
        let status = response.status();
        let header = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned)
        };
        if !status.is_success() {
            let code =
                crate::text::escape(ms_error_code(response.headers()).unwrap_or_default(), 64);
            return Err(create_failed("Azure Get Blob Properties", status, &code));
        }
        let copy = header("x-ms-copy-status").unwrap_or_default();
        match copy.as_str() {
            "" | "success" => Ok(false),
            "pending" => Ok(true),
            _ => {
                let description = header("x-ms-copy-status-description").unwrap_or_default();
                Err(Error::Generic(format!(
                    "Azure copy to {}/{} ended as {}: {}; the source was kept",
                    self.container,
                    crate::text::escape(path.as_ref(), 256),
                    crate::text::escape(&copy, 32),
                    crate::text::escape(&description, 256),
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl Backend for AzureBackend {
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

    /// `List Blobs` with delimiter `/`, which reports each blob's access tier
    /// and rehydration: `Archive` is archived, `rehydrate-pending-*` is
    /// restoring. Directory entries of a hierarchical namespace are skipped,
    /// as `object_store` skips them.
    #[cfg(feature = "gui")]
    async fn list_page(
        &self,
        dir: &ObjectPath,
        max: usize,
        token: Option<String>,
    ) -> Result<ListPage> {
        let (account, endpoint) = self.account_and_endpoint()?;
        let prefix = if dir.as_ref().is_empty() {
            String::new()
        } else {
            format!("{dir}/")
        };
        let mut uri = format!(
            "{}/{}?restype=container&comp=list&delimiter=%2F&maxresults={max}",
            endpoint.trim_end_matches('/'),
            self.container
        );
        if !prefix.is_empty() {
            uri = format!("{uri}&prefix={}", encode(&prefix));
        }
        if let Some(token) = token {
            uri = format!("{uri}&marker={}", encode(&token));
        }
        let client = self.http.get()?;
        let response = send_with_retry(|| async {
            let request = self
                .signed_request(Method::GET, &uri, &account, &[])
                .await?;
            send_signed(client, request).await
        })
        .await?;
        let body = read_listing(response, "Azure List Blobs").await?;
        let xml = String::from_utf8_lossy(&body);
        let invalid = |e: object_store::path::Error| object_store::Error::from(e);
        let mut objects = Vec::new();
        for block in xml_blocks(&xml, "Blob") {
            let Some(name) = xml_texts(block, "Name").into_iter().next() else {
                continue;
            };
            let props = xml_blocks(block, "Properties")
                .into_iter()
                .next()
                .unwrap_or_default();
            let text = |tag| xml_texts(props, tag).into_iter().next();
            if text("ResourceType").as_deref() == Some("directory") || name.len() <= prefix.len() {
                continue;
            }
            let bad =
                |what: &str, v: &str| Error::Generic(format!("Azure listing: bad {what} {v:?}"));
            let (Some(size), Some(modified)) = (text("Content-Length"), text("Last-Modified"))
            else {
                return Err(Error::Generic(
                    "Azure listing: a blob has no properties".into(),
                ));
            };
            let class = text("AccessTier");
            let state = if text("ArchiveStatus").is_some_and(|s| s.starts_with("rehydrate-pending"))
            {
                ObjectState::Restoring
            } else if class.as_deref() == Some("Archive") {
                ObjectState::Archived
            } else {
                ObjectState::Online
            };
            objects.push(ListedObject {
                location: ObjectPath::parse(&name).map_err(invalid)?,
                size: size.parse().map_err(|_| bad("Content-Length", &size))?,
                modified: chrono::NaiveDateTime::parse_from_str(
                    &modified,
                    "%a, %d %b %Y %H:%M:%S GMT",
                )
                .map_err(|_| bad("Last-Modified", &modified))?
                .and_utc(),
                state,
                class,
            });
        }
        let prefixes = xml_blocks(&xml, "BlobPrefix")
            .into_iter()
            .filter_map(|block| xml_texts(block, "Name").into_iter().next())
            .map(|p| ObjectPath::parse(p).map_err(invalid))
            .collect::<std::result::Result<_, _>>()?;
        let next = xml_texts(&xml, "NextMarker")
            .pop()
            .filter(|t| !t.is_empty());
        Ok(ListPage {
            prefixes,
            objects,
            next,
        })
    }

    /// Issues an Azure `Create Container` request
    /// (`PUT /<container>?restype=container`), signed with `object_store`'s
    /// [`AzureAuthorizer`]. Containers are scoped to the storage account, so
    /// HTTP 409 with the `x-ms-error-code` `ContainerAlreadyExists` means the
    /// container is already ours (success). Any other 409, such as
    /// `ContainerBeingDeleted`, is an error.
    async fn create_bucket(&self, _url: &Url) -> Result<()> {
        let request = self.container_request(Method::PUT).await?;
        let response = send_signed(self.http.get()?, request).await?;
        let status = response.status();
        if status.is_success() {
            return Ok(());
        }

        if status.as_u16() == 409 {
            let code = ms_error_code(response.headers()).unwrap_or_default();
            if code == "ContainerAlreadyExists" {
                return Ok(());
            }
            let code = crate::text::escape(code, 64);
            return Err(Error::Generic(format!(
                "Azure CreateContainer failed (HTTP 409, {code}): the container is not usable"
            )));
        }

        let text = error_body(response).await;
        Err(create_failed("Azure CreateContainer", status, &text))
    }

    /// Issues an Azure `Get Container Properties` request
    /// (`HEAD /<container>?restype=container`), signed with `object_store`'s
    /// [`AzureAuthorizer`]. This metadata-only probe avoids listing the
    /// container's blobs: HTTP `200` means it exists, `404` that it does not.
    async fn bucket_exists(&self, _url: &Url) -> Result<bool> {
        let request = self.container_request(Method::HEAD).await?;
        let response = send_signed(self.http.get()?, request).await?;
        // Azure answers an anonymous request for a private container with 404,
        // so under `skip_signature` a 404 does not show the container is absent.
        if self.skip_signature && response.status().as_u16() == 404 {
            return Err(Error::Generic(
                "Azure container not found, or not anonymously readable \
                 (this store runs unsigned)"
                    .into(),
            ));
        }
        bucket_exists_from_response(response, "Azure").await
    }

    fn signer(&self) -> Option<&dyn Signer> {
        Some(self.inner.as_ref())
    }

    fn as_any(&self) -> Option<&dyn std::any::Any> {
        Some(self)
    }

    fn copies_from(&self, src: &dyn Backend) -> bool {
        self.copy_source(src).is_some()
    }

    /// An Azure `Copy Blob` (`PUT <blob>` with `x-ms-copy-source: <source blob
    /// URL>`), for a source container of the same account, authorized with this
    /// backend's credential: the same `SharedKey` or bearer token reads the
    /// source. `x-ms-source-if-match` carries the e-tag in `known`. The
    /// properties and metadata of the source are copied, and no access tier is
    /// sent, as a streamed copy keeps none either. A SAS token is for one
    /// container and cannot read the source, so that store streams.
    /// `CannotVerifyCopySource` means the credential cannot read the source,
    /// and the caller streams as well. A `412` with the error code `SourceConditionNotMet` is
    /// [`Error::SourceChanged`]. Transient failures are retried, see
    /// [`send_with_retry`].
    ///
    /// The copy may still be pending when this returns; `server_side_copy`
    /// waits for it in [`finish_copy`](Backend::finish_copy).
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
        if matches!(
            self.credential().await?.as_deref(),
            Some(AzureCredential::SASToken(_))
        ) {
            return Ok(false);
        }
        let (_, source_url) = source.blob_url(src_path)?;
        let value = |text: &str| {
            http::HeaderValue::from_str(text)
                .map_err(|_| Error::Generic("Azure copy source is not a header value".into()))
        };
        let mut headers = vec![("x-ms-copy-source", value(source_url.as_str())?)];
        if let Some(etag) = known.and_then(|meta| meta.e_tag.as_deref()) {
            // An e-tag that is no header value cannot be matched: stream.
            let Ok(etag) = value(etag) else {
                return Ok(false);
            };
            headers.push(("x-ms-source-if-match", etag));
        }
        let client = self.http.get()?;
        let response = send_with_retry(|| async {
            let request = self
                .blob_request_with(Method::PUT, dst_path, &headers)
                .await?;
            send_signed(client, request).await
        })
        .await?;
        let status = response.status();
        if status.is_success() {
            return Ok(true);
        }
        let code = crate::text::escape(ms_error_code(response.headers()).unwrap_or_default(), 64);
        if status.as_u16() == 412 && code == "SourceConditionNotMet" {
            return Err(Error::SourceChanged { url: String::new() });
        }
        if code == "CannotVerifyCopySource" {
            return Ok(false);
        }
        let text = error_body(response).await;
        Err(create_failed(
            "Azure Copy Blob",
            status,
            &format!("{code} {text}"),
        ))
    }

    /// Waits for the `Copy Blob` that wrote `path` to finish. Azure may answer
    /// it `202` with `x-ms-copy-status: pending` and finish later, which
    /// `object_store` does not see. The destination is polled with
    /// `Get Blob Properties`, pausing 250 ms and doubling up to 4 s, for at
    /// most 120 s. A copy that failed or was aborted is an error, and so is
    /// one still pending after that; neither deletes the source.
    async fn finish_copy(&self, path: &ObjectPath) -> Result<()> {
        let mut waited = Duration::ZERO;
        let mut pause = COPY_POLL_START;
        while self.copy_pending(path).await? {
            if waited >= COPY_WAIT_MAX {
                return Err(Error::Generic(format!(
                    "Azure copy to {}/{} is still pending after {} s; the source was kept",
                    self.container,
                    crate::text::escape(path.as_ref(), 256),
                    COPY_WAIT_MAX.as_secs(),
                )));
            }
            let pause_now = pause.min(COPY_WAIT_MAX.saturating_sub(waited));
            tokio::time::sleep(pause_now).await;
            waited += pause_now;
            pause = (pause * 2).min(COPY_POLL_MAX);
        }
        Ok(())
    }
}

/// The builder for `container` with `vars` and the crate's defaults applied.
fn configured(container: &str, vars: &Vars) -> MicrosoftAzureBuilder {
    let mut builder = match vars {
        Vars::Env => MicrosoftAzureBuilder::from_env(),
        Vars::Map { .. } => vars
            .config_entries::<AzureConfigKey>("AZURE_")
            .into_iter()
            .fold(MicrosoftAzureBuilder::new(), |b, (k, v)| {
                b.with_config(k, v)
            }),
    };
    for (key, value) in default_timeouts(vars, "AZURE_") {
        builder = builder.with_config(AzureConfigKey::Client(key), value);
    }
    builder.with_container_name(container)
}

/// The TLS settings `vars` give an Azure store: the blob endpoint (the
/// storage emulator is plain `http`) and the builder's certificate setting.
#[cfg(feature = "gui")]
pub(super) fn tls(vars: &Vars) -> super::Tls {
    let builder = configured("", vars);
    let flag = |key: &AzureConfigKey| {
        builder
            .get_config_value(key)
            .and_then(|v| super::parse_bool(&v))
            .unwrap_or(false)
    };
    let plain = builder
        .get_config_value(&AzureConfigKey::Endpoint)
        .map_or_else(
            || flag(&AzureConfigKey::UseEmulator),
            |endpoint| super::is_plain(&endpoint),
        );
    super::Tls {
        https: !plain,
        allow_invalid_certificates: flag(&AzureConfigKey::Client(
            ClientConfigKey::AllowInvalidCertificates,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::stub::{Reply, closed_port, serve};

    fn backend(pairs: &[(&str, &str)]) -> AzureBackend {
        AzureBackend::new("az", Some("c"), &Vars::from_pairs(pairs)).expect("build")
    }

    #[test]
    fn container_calls_never_use_plain_http_unless_allowed() {
        let base = [
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
        ];
        let with = |extra: &[(&str, &str)]| backend(&[&base[..], extra].concat());

        let default = with(&[]).account_and_endpoint().expect("endpoint");
        assert_eq!(default.1, "https://acct.blob.core.windows.net");

        let https = with(&[("AZURE_STORAGE_ENDPOINT", "https://h:1")]);
        assert!(https.account_and_endpoint().is_ok());

        let http = [("AZURE_STORAGE_ENDPOINT", "http://127.0.0.1:10000/acct")];
        assert!(with(&http).account_and_endpoint().is_err());
        let off = [&http[..], &[("AZURE_ALLOW_HTTP", "false")][..]].concat();
        assert!(with(&off).account_and_endpoint().is_err());
        let on = [&http[..], &[("AZURE_ALLOW_HTTP", "true")][..]].concat();
        assert!(with(&on).account_and_endpoint().is_ok());
    }

    #[test]
    fn container_calls_follow_the_stores_endpoint_aliases() {
        let base = [("AZURE_STORAGE_ACCOUNT_NAME", "acct")];
        let with = |extra: &[(&str, &str)]| backend(&[&base[..], extra].concat());

        let alias = with(&[("AZURE_ENDPOINT", "https://h:1")]);
        assert_eq!(
            alias.account_and_endpoint().expect("alias").1,
            "https://h:1"
        );
        let plain = with(&[
            ("AZURE_ENDPOINT", "http://h:1"),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        assert!(plain.account_and_endpoint().is_ok());
        let off = with(&[("AZURE_ENDPOINT", "http://h:1")]);
        assert!(off.account_and_endpoint().is_err());
    }

    #[test]
    fn container_calls_follow_the_stores_transport_settings() {
        let on = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
            ("AZURE_ALLOW_INVALID_CERTIFICATES", "true"),
            ("AZURE_PROXY_URL", "http://proxy.invalid:3128"),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        let transport = on.http.transport();
        assert!(transport.allow_invalid_certificates && transport.allow_http);
        assert_eq!(
            transport.proxy_url.as_deref(),
            Some("http://proxy.invalid:3128")
        );
        let off = backend(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")]);
        assert!(!off.http.transport().allow_http);
    }

    #[test]
    fn container_calls_refuse_the_emulator_and_fabric_modes_without_an_endpoint() {
        for (var, name) in [
            ("AZURE_STORAGE_USE_EMULATOR", "AZURE_STORAGE_USE_EMULATOR"),
            ("AZURE_USE_FABRIC_ENDPOINT", "AZURE_USE_FABRIC_ENDPOINT"),
        ] {
            let pairs = [("AZURE_STORAGE_ACCOUNT_NAME", "acct"), (var, "true")];
            let err = backend(&pairs).account_and_endpoint().expect_err("mode");
            assert!(err.to_string().contains(name), "{err}");
            // An explicit endpoint is used as it is.
            let with_endpoint = [&pairs[..], &[("AZURE_STORAGE_ENDPOINT", "https://h:1")][..]];
            let ok = backend(&with_endpoint.concat()).account_and_endpoint();
            assert_eq!(ok.expect("endpoint").1, "https://h:1");
        }
        // The emulator needs no account name; the refusal comes first.
        let err = backend(&[("AZURE_STORAGE_USE_EMULATOR", "true")])
            .account_and_endpoint()
            .expect_err("emulator");
        assert!(err.to_string().contains("USE_EMULATOR"), "{err}");
    }

    /// A backend that runs unsigned against the stub `base`. The managed
    /// identity endpoint points at a port nothing listens on.
    fn unsigned(base: &str) -> AzureBackend {
        let endpoint = format!("{base}/acct");
        let msi = format!("http://127.0.0.1:{}/msi", closed_port());
        backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
            ("AZURE_SKIP_SIGNATURE", "true"),
            ("AZURE_MSI_ENDPOINT", &msi),
        ])
    }

    #[tokio::test]
    async fn skip_signature_sends_unsigned_container_requests() {
        let stub = serve(vec![Reply::new(200, ""), Reply::new(201, "")]).await;
        let backend = unsigned(&stub.base);
        let url = Url::parse("az://c/").expect("url");
        let calls = async {
            assert!(backend.bucket_exists(&url).await?);
            backend.create_bucket(&url).await
        };
        tokio::time::timeout(std::time::Duration::from_secs(5), calls)
            .await
            .expect("no credential lookup")
            .expect("calls");
        let requests = stub.requests();
        assert_eq!(requests[0].line, "HEAD /acct/c?restype=container HTTP/1.1");
        assert_eq!(requests[1].line, "PUT /acct/c?restype=container HTTP/1.1");
        for request in requests {
            let headers = request.headers.to_ascii_lowercase();
            assert!(!headers.contains("authorization"), "{headers}");
        }
    }

    #[tokio::test]
    async fn an_anonymous_404_is_not_a_missing_container() {
        let stub = serve(vec![Reply::new(404, "")]).await;
        let url = Url::parse("az://c/").expect("url");
        let err = unsigned(&stub.base)
            .bucket_exists(&url)
            .await
            .expect_err("unknown");
        assert!(err.to_string().contains("anonymously"), "{err}");
    }

    #[tokio::test]
    async fn azure_forbidden_creation_is_denied() {
        let stub = serve(vec![Reply::new(403, "")]).await;
        let endpoint = format!("{}/acct", stub.base);
        let backend = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        let url = Url::parse("az://c/").expect("url");
        let err = backend.create_bucket(&url).await.expect_err("403");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
    }

    /// Runs `create_bucket` against a stub answering 409 with `code`.
    async fn create_conflict(code: &'static str) -> Result<()> {
        let stub = serve(vec![Reply::new(409, "").header("x-ms-error-code", code)]).await;
        let endpoint = format!("{}/acct", stub.base);
        let backend = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        let url = Url::parse("az://c/").expect("url");
        let result = backend.create_bucket(&url).await;
        assert_eq!(
            stub.requests()[0].line,
            "PUT /acct/c?restype=container HTTP/1.1"
        );
        result
    }

    #[tokio::test]
    async fn azure_already_exists_succeeds() {
        create_conflict("ContainerAlreadyExists")
            .await
            .expect("owned container");
    }

    #[tokio::test]
    async fn azure_being_deleted_fails() {
        let err = create_conflict("ContainerBeingDeleted")
            .await
            .expect_err("being deleted");
        assert!(err.to_string().contains("ContainerBeingDeleted"), "{err}");
    }

    #[tokio::test]
    async fn a_failed_container_call_does_not_print_the_sas() {
        let port = closed_port();
        let endpoint = format!("http://127.0.0.1:{port}/acct");
        let backend = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_SAS_KEY", "sv=2022-11-02&sig=TOPSECRET"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        let url = Url::parse("az://c/").expect("url");
        let err = backend
            .bucket_exists(&url)
            .await
            .expect_err("nothing listens");
        let text = format!("{err} {err:?}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        assert!(text.contains("127.0.0.1"), "{text}");
    }

    const MODIFIED: &str = "Wed, 21 Oct 2015 07:28:00 GMT";

    /// A `HEAD` answer for a blob, with the copy state `copy` if given.
    fn blob_head(copy: Option<&'static str>) -> Reply {
        let reply = Reply::new(200, "")
            .header("Last-Modified", MODIFIED)
            .header("ETag", "\"0x1\"");
        match copy {
            Some(c) => reply.header("x-ms-copy-status", c),
            None => reply,
        }
    }

    /// The answer to the batch request `object_store` deletes with.
    fn batch_deleted() -> Reply {
        Reply::new(
            202,
            "--b\r\nContent-Type: application/http\r\nContent-ID: 0\r\n\r\n\
             HTTP/1.1 202 Accepted\r\nx-ms-version: 2018-11-09\r\n\r\n--b--\r\n",
        )
        .header("Content-Type", "multipart/mixed; boundary=b")
    }

    fn storage(base: &str) -> AzureBackend {
        let endpoint = format!("{base}/acct");
        backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ])
    }

    /// Copies or moves `a b` to `c` inside one container served by `replies`.
    async fn copy_in_stub(
        replies: Vec<Reply>,
        delete_source: bool,
    ) -> (crate::backend::stub::Stub, Result<bool>) {
        use crate::codec::sealed::{Location, Resolved};
        let stub = serve(replies).await;
        let backend: Arc<dyn Backend> = Arc::new(storage(&stub.base));
        let at = |url: &str| {
            let url = Url::parse(url).expect("url");
            let path = ObjectPath::from_url_path(url.path()).expect("path");
            Resolved {
                backend: Arc::clone(&backend),
                registry: 0,
                loc: Location { url, path },
            }
        };
        let done = crate::codec::server_side_copy(
            &at("az://c/a%20b"),
            &at("az://c/d/e"),
            delete_source,
            None,
        )
        .await;
        (stub, done)
    }

    fn lines(stub: &crate::backend::stub::Stub) -> Vec<String> {
        stub.requests()
            .into_iter()
            .map(|r| r.line.split(" HTTP").next().unwrap_or_default().to_owned())
            .collect()
    }

    #[tokio::test]
    async fn a_pending_copy_is_waited_for() {
        let replies = vec![
            Reply::new(202, ""),
            blob_head(Some("pending")),
            blob_head(Some("pending")),
            blob_head(Some("success")),
        ];
        let (stub, done) = copy_in_stub(replies, false).await;
        assert!(done.expect("copy"));
        assert_eq!(
            lines(&stub),
            [
                "PUT /acct/c/d/e",
                "HEAD /acct/c/d/e",
                "HEAD /acct/c/d/e",
                "HEAD /acct/c/d/e"
            ]
        );
        let head = stub.requests()[1].headers.to_ascii_lowercase();
        assert!(head.contains("authorization: sharedkey"), "{head}");
    }

    #[tokio::test]
    async fn a_move_deletes_the_source_only_after_the_copy_succeeded() {
        let replies = vec![
            blob_head(None),
            Reply::new(202, ""),
            blob_head(Some("pending")),
            blob_head(Some("success")),
            blob_head(None),
            batch_deleted(),
        ];
        let (stub, done) = copy_in_stub(replies, true).await;
        assert!(done.expect("move"));
        assert_eq!(
            lines(&stub),
            [
                "HEAD /acct/c/a%20b",
                "PUT /acct/c/d/e",
                "HEAD /acct/c/d/e",
                "HEAD /acct/c/d/e",
                "HEAD /acct/c/a%20b",
                "POST /acct/c?restype=container&comp=batch"
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_copy_keeps_the_source() {
        let replies = vec![
            blob_head(None),
            Reply::new(202, ""),
            blob_head(Some("failed")).header("x-ms-copy-status-description", "bad blob"),
        ];
        let (stub, done) = copy_in_stub(replies, true).await;
        let err = done.expect_err("failed copy").to_string();
        assert!(err.contains("failed") && err.contains("bad"), "{err}");
        assert!(lines(&stub).iter().all(|l| !l.starts_with("POST")));
    }

    #[tokio::test]
    async fn a_copy_pending_past_the_bound_is_an_error_and_keeps_the_source() {
        let mut replies = vec![blob_head(None), Reply::new(202, "")];
        replies.extend((0..60).map(|_| blob_head(Some("pending"))));
        let start = tokio::time::Instant::now();
        let (stub, done) = copy_in_stub(replies, true).await;
        let err = done.expect_err("pending").to_string();
        assert!(err.contains("still pending"), "{err}");
        assert!(err.contains("source was kept"), "{err}");
        let waited = start.elapsed();
        assert!(
            waited >= COPY_WAIT_MAX && waited < COPY_WAIT_MAX * 2,
            "{waited:?}"
        );
        assert!(lines(&stub).iter().all(|l| !l.starts_with("POST")));
    }

    #[tokio::test]
    async fn a_failed_status_check_does_not_print_the_sas() {
        let stub = serve(vec![
            Reply::new(403, "").header("x-ms-error-code", "AuthFailed"),
        ])
        .await;
        let endpoint = format!("{}/acct", stub.base);
        let backend = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_SAS_KEY", "sv=2022-11-02&sig=TOPSECRET"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ]);
        let err = backend
            .finish_copy(&ObjectPath::from("x"))
            .await
            .expect_err("403");
        let text = format!("{err} {err:?}");
        assert!(matches!(err, Error::Denied(_)), "{text}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        assert!(stub.requests()[0].line.contains("sig=TOPSECRET"));
    }

    #[test]
    fn timeouts_default_to_a_day_and_five_minutes_and_the_users_win() {
        let key = |k| AzureConfigKey::Client(k);
        let default = configured("c", &Vars::from_pairs(&[]));
        let read = |b: &MicrosoftAzureBuilder, k| b.get_config_value(&key(k));
        assert_eq!(
            read(&default, ClientConfigKey::Timeout).as_deref(),
            Some("86400s")
        );
        assert_eq!(
            read(&default, ClientConfigKey::ReadTimeout).as_deref(),
            Some("300s")
        );
        let set = configured(
            "c",
            &Vars::from_pairs(&[("AZURE_TIMEOUT", "7s"), ("AZURE_READ_TIMEOUT", "9s")]),
        );
        assert_eq!(read(&set, ClientConfigKey::Timeout).as_deref(), Some("7s"));
        assert_eq!(
            read(&set, ClientConfigKey::ReadTimeout).as_deref(),
            Some("9s")
        );
    }

    #[test]
    fn containers_of_one_registry_share_a_credential_provider() {
        let vars = Vars::from_pairs(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")]);
        let shared = Shared::default();
        let a = AzureBackend::with_shared("az", Some("a"), &vars, &shared).expect("a");
        let b = AzureBackend::with_shared("az", Some("b"), &vars, &shared).expect("b");
        assert!(Arc::ptr_eq(a.inner.credentials(), b.inner.credentials()));
        assert!(Arc::ptr_eq(
            shared.azure.get().expect("stored"),
            a.inner.credentials()
        ));
        let other = AzureBackend::new("az", Some("a"), &vars).expect("other");
        assert!(!Arc::ptr_eq(
            a.inner.credentials(),
            other.inner.credentials()
        ));
    }

    #[test]
    fn a_store_that_skips_signing_takes_no_shared_provider() {
        let shared = Shared::default();
        let skipping = Vars::from_pairs(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_SKIP_SIGNATURE", "true"),
        ]);
        AzureBackend::with_shared("az", Some("a"), &skipping, &shared).expect("a");
        assert!(shared.azure.get().is_none());
        let signed = Vars::from_pairs(&[("AZURE_STORAGE_ACCOUNT_NAME", "acct")]);
        let first = AzureBackend::with_shared("az", Some("b"), &signed, &shared).expect("b");
        let second = AzureBackend::with_shared("az", Some("c"), &skipping, &shared).expect("c");
        assert!(!Arc::ptr_eq(
            first.inner.credentials(),
            second.inner.credentials()
        ));
    }

    fn copy_vars(base: &str) -> Vec<(&'static str, String)> {
        vec![
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct".to_owned()),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5".to_owned()),
            ("AZURE_STORAGE_ENDPOINT", format!("{base}/acct")),
            ("AZURE_ALLOW_HTTP", "true".to_owned()),
        ]
    }

    fn container(container: &str, pairs: &[(&str, String)], shared: &Shared) -> AzureBackend {
        let pairs: Vec<(&str, &str)> = pairs.iter().map(|(k, v)| (*k, v.as_str())).collect();
        AzureBackend::with_shared("az", Some(container), &Vars::from_pairs(&pairs), shared)
            .expect("build")
    }

    /// Backends for containers `dst` and `src` of one registry on the stub.
    fn copy_pair(base: &str) -> (AzureBackend, AzureBackend) {
        let (pairs, shared) = (copy_vars(base), Shared::default());
        (
            container("dst", &pairs, &shared),
            container("src", &pairs, &shared),
        )
    }

    fn known(etag: &str) -> ObjectMeta {
        ObjectMeta {
            location: ObjectPath::from("a b"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 1,
            e_tag: Some(etag.to_owned()),
            version: None,
        }
    }

    async fn cross_copy(
        reply: Reply,
        known: Option<&ObjectMeta>,
    ) -> (Result<bool>, Vec<crate::backend::stub::Request>) {
        let stub = serve(vec![reply]).await;
        let (dst, src) = copy_pair(&stub.base);
        let done = dst
            .copy_from(
                &src,
                &ObjectPath::from("a b"),
                &ObjectPath::from("d/e"),
                known,
            )
            .await;
        (done, stub.requests())
    }

    #[tokio::test]
    async fn a_cross_container_copy_is_a_signed_copy_blob() {
        let meta = known("\"0x1\"");
        let (done, requests) = cross_copy(Reply::new(202, ""), Some(&meta)).await;
        assert!(done.expect("copy"));
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].line, "PUT /acct/dst/d/e HTTP/1.1");
        let headers = requests[0].headers.to_ascii_lowercase();
        assert!(
            headers.contains("x-ms-copy-source: http://127.0.0.1:")
                && headers.contains("/acct/src/a%20b"),
            "{headers}"
        );
        assert!(
            headers.contains("x-ms-source-if-match: \"0x1\""),
            "{headers}"
        );
        assert!(headers.contains("authorization: sharedkey"), "{headers}");
        assert_eq!(requests[0].body.len(), 0);

        let (done, requests) = cross_copy(Reply::new(201, ""), None).await;
        assert!(done.expect("copy"));
        let headers = requests[0].headers.to_ascii_lowercase();
        assert!(!headers.contains("source-if-match"), "{headers}");
    }

    #[tokio::test]
    async fn a_credential_that_cannot_read_the_source_streams() {
        let reply = Reply::new(403, "").header("x-ms-error-code", "CannotVerifyCopySource");
        let (done, _) = cross_copy(reply, None).await;
        assert!(!done.expect("CannotVerifyCopySource"));

        // A SAS token is for one container: nothing is sent.
        let stub = serve(vec![]).await;
        let mut pairs = copy_vars(&stub.base);
        pairs.retain(|(k, _)| *k != "AZURE_STORAGE_ACCOUNT_KEY");
        pairs.push((
            "AZURE_STORAGE_SAS_KEY",
            "sv=2022-11-02&sig=TOPSECRET".to_owned(),
        ));
        let shared = Shared::default();
        let (dst, src) = (
            container("dst", &pairs, &shared),
            container("src", &pairs, &shared),
        );
        assert!(dst.copies_from(&src));
        let done = dst
            .copy_from(&src, &ObjectPath::from("a"), &ObjectPath::from("b"), None)
            .await;
        assert!(!done.expect("sas"));
        assert!(stub.requests().is_empty());
    }

    #[tokio::test]
    async fn a_changed_source_and_other_failures_are_errors() {
        let (done, _) = cross_copy(
            Reply::new(412, "").header("x-ms-error-code", "SourceConditionNotMet"),
            Some(&known("\"0x1\"")),
        )
        .await;
        assert!(matches!(
            done.expect_err("412"),
            Error::SourceChanged { .. }
        ));
        let (done, _) = cross_copy(
            Reply::new(403, "").header("x-ms-error-code", "AuthFailed"),
            None,
        )
        .await;
        let err = done.expect_err("403");
        assert!(matches!(err, Error::Denied(_)), "{err:?}");
        assert!(err.to_string().contains("AuthFailed"), "{err}");
        let (done, _) = cross_copy(Reply::new(409, "boom\x1b"), None).await;
        let text = done.expect_err("409").to_string();
        assert!(text.contains("boom\\u{1b}"), "{text}");
    }

    #[tokio::test]
    async fn copies_stay_in_the_account_and_registry() {
        let stub = serve(vec![]).await;
        let (dst, src) = copy_pair(&stub.base);
        assert!(dst.copies_from(&src));

        let pairs = copy_vars(&stub.base);
        // Another registry holds another credential provider.
        assert!(!dst.copies_from(&container("src", &pairs, &Shared::default())));
        // The same registry, another account or endpoint.
        let shared = Shared::default();
        let first = container("src", &pairs, &shared);
        for (key, value) in [
            ("AZURE_STORAGE_ACCOUNT_NAME", "other"),
            ("AZURE_STORAGE_ENDPOINT", "http://other.invalid:9/acct"),
        ] {
            let mut changed = pairs.clone();
            changed.retain(|(k, _)| *k != key);
            changed.push((key, value.to_owned()));
            let other = container("src", &changed, &shared);
            assert!(
                !first.copies_from(&other) && !other.copies_from(&first),
                "{key}"
            );
        }
        // A store that skips signing, on either side.
        let mut skipping = pairs.clone();
        skipping.push(("AZURE_SKIP_SIGNATURE", "true".to_owned()));
        let skipping = container("src", &skipping, &shared);
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

    /// Runs a copy or move of `src/a b` to `dst/d/e` through `server_side_copy`.
    async fn copy_through_codec(
        replies: Vec<Reply>,
        delete_source: bool,
    ) -> (Result<bool>, Vec<String>) {
        use crate::codec::sealed::{Location, Resolved};
        let stub = serve(replies).await;
        let (dst, src) = copy_pair(&stub.base);
        let at = |backend: AzureBackend, url: &str| {
            let url = Url::parse(url).expect("url");
            let path = ObjectPath::from_url_path(url.path()).expect("path");
            Resolved {
                backend: Arc::new(backend),
                registry: 0,
                loc: Location { url, path },
            }
        };
        let done = crate::codec::server_side_copy(
            &at(src, "az://src/a"),
            &at(dst, "az://dst/d/e"),
            delete_source,
            None,
        )
        .await;
        (done, lines(&stub))
    }

    #[tokio::test]
    async fn a_cross_container_move_deletes_the_source_only_after_the_copy_finished() {
        let replies = vec![
            blob_head(None),
            Reply::new(202, ""),
            blob_head(Some("pending")),
            blob_head(Some("success")),
            blob_head(None),
            blob_head(None),
            batch_deleted(),
        ];
        let (done, lines) = copy_through_codec(replies, true).await;
        assert!(done.expect("move"));
        assert_eq!(
            lines,
            [
                "HEAD /acct/src/a",
                "PUT /acct/dst/d/e",
                "HEAD /acct/dst/d/e",
                "HEAD /acct/dst/d/e",
                "HEAD /acct/dst/d/e",
                "HEAD /acct/src/a",
                "POST /acct/src?restype=container&comp=batch"
            ]
        );
    }

    #[tokio::test]
    async fn a_failed_or_refused_cross_container_copy_keeps_the_source() {
        let failed = vec![
            blob_head(None),
            Reply::new(202, ""),
            blob_head(Some("failed")).header("x-ms-copy-status-description", "bad blob"),
        ];
        let (done, lines) = copy_through_codec(failed, true).await;
        done.expect_err("failed copy");
        assert!(lines.iter().all(|l| !l.starts_with("POST")), "{lines:?}");

        let changed = Reply::new(412, "").header("x-ms-error-code", "SourceConditionNotMet");
        let (done, lines) = copy_through_codec(vec![blob_head(None), changed], true).await;
        let err = done.expect_err("changed");
        assert!(
            matches!(&err, Error::SourceChanged { url } if url == "az://src/a"),
            "{err:?}"
        );
        assert_eq!(lines, ["HEAD /acct/src/a", "PUT /acct/dst/d/e"]);

        let denied = Reply::new(403, "").header("x-ms-error-code", "CannotVerifyCopySource");
        let (done, lines) = copy_through_codec(vec![blob_head(None), denied], true).await;
        assert!(!done.expect("stream"));
        assert_eq!(lines, ["HEAD /acct/src/a", "PUT /acct/dst/d/e"]);
    }

    #[tokio::test]
    async fn only_a_failed_source_condition_is_a_changed_source() {
        let (done, _) = cross_copy(
            Reply::new(412, "").header("x-ms-error-code", "LeaseIdMismatchWithBlobOperation"),
            Some(&known("\"0x1\"")),
        )
        .await;
        let err = done.expect_err("412");
        assert!(matches!(err, Error::Generic(_)), "{err:?}");
    }

    #[tokio::test]
    async fn transient_copy_failures_are_retried_up_to_three_attempts() {
        let busy = || Reply::new(503, "").header("x-ms-error-code", "ServerBusy");
        let stub = serve(vec![busy(), busy(), Reply::new(202, "")]).await;
        let (dst, src) = copy_pair(&stub.base);
        let (a, b) = (ObjectPath::from("a"), ObjectPath::from("b"));
        assert!(dst.copy_from(&src, &a, &b, None).await.expect("third"));
        assert_eq!(stub.requests().len(), 3);
        let stub = serve(vec![busy(), busy(), busy()]).await;
        let (dst, src) = copy_pair(&stub.base);
        assert!(dst.copy_from(&src, &a, &b, None).await.is_err());
        assert_eq!(stub.requests().len(), 3);
    }

    #[tokio::test]
    async fn a_customer_provided_key_makes_the_copy_stream() {
        let stub = serve(vec![]).await;
        let pairs = copy_vars(&stub.base);
        let shared = Shared::default();
        let plain = container("dst", &pairs, &shared);
        let mut keyed = pairs.clone();
        keyed.push((
            "AZURE_STORAGE_ENCRYPTION_KEY",
            "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=".to_owned(),
        ));
        let other = container("src", &keyed, &shared);
        assert!(!plain.copies_from(&other) && !other.copies_from(&plain));
        let done = plain
            .copy_from(&other, &ObjectPath::from("a"), &ObjectPath::from("b"), None)
            .await;
        assert!(!done.expect("refused"));
        assert!(stub.requests().is_empty());
    }

    #[cfg(feature = "gui")]
    mod list_buckets {
        use super::*;
        use crate::backend::stub::Request;

        fn page(names: &[&str], marker: &str) -> Reply {
            let containers = names
                .iter()
                .map(|n| format!("<Container><Name>{n}</Name><Properties /></Container>"))
                .collect::<Vec<_>>()
                .concat();
            let marker = if marker.is_empty() {
                "<NextMarker />".to_owned()
            } else {
                format!("<NextMarker>{marker}</NextMarker>")
            };
            Reply::new(
                200,
                format!(
                    "\u{feff}<?xml version=\"1.0\"?><EnumerationResults ServiceEndpoint=\"e\">\
                     <Prefix /><Containers>{containers}</Containers>{marker}</EnumerationResults>"
                ),
            )
        }

        async fn list(replies: Vec<Reply>) -> (Result<Vec<String>>, Vec<Request>) {
            let stub = serve(replies).await;
            let endpoint = format!("{}/acct", stub.base);
            let result = backend(&[
                ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
                ("AZURE_STORAGE_ENDPOINT", &endpoint),
                ("AZURE_ALLOW_HTTP", "true"),
            ])
            .list_buckets()
            .await;
            (result, stub.requests())
        }

        #[tokio::test]
        async fn pages_are_followed_and_the_names_sorted() {
            let (names, requests) = list(vec![
                page(&["zeta", "alpha"], "1!8!Zm9v/+=="),
                page(&["mid"], ""),
            ])
            .await;
            assert_eq!(names.expect("list"), ["alpha", "mid", "zeta"]);
            assert_eq!(requests[0].line, "GET /acct/?comp=list HTTP/1.1");
            assert_eq!(
                requests[1].line,
                "GET /acct/?comp=list&marker=1%218%21Zm9v%2F%2B%3D%3D HTTP/1.1"
            );
            assert!(
                requests[0]
                    .headers
                    .to_ascii_lowercase()
                    .contains("authorization: sharedkey acct:"),
                "{}",
                requests[0].headers
            );
        }
    }

    #[tokio::test]
    async fn reading_an_archived_blob_is_archived() {
        use object_store::ObjectStoreExt;
        let body = "<Error><Code>BlobArchived</Code><Message>This operation is not permitted on an archived blob.</Message></Error>";
        let stub = serve(vec![Reply::new(409, body)]).await;
        let endpoint = format!("{}/acct", stub.base);
        let err = backend(&[
            ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
            ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
            ("AZURE_STORAGE_ENDPOINT", &endpoint),
            ("AZURE_ALLOW_HTTP", "true"),
        ])
        .store()
        .get(&ObjectPath::from("o"))
        .await
        .expect_err("archived");
        let err = Error::from(err);
        assert!(matches!(err, Error::Archived(_)), "{err:?}");
    }

    #[cfg(feature = "gui")]
    mod list_page {
        use super::*;
        use crate::backend::stub::Request;

        fn blob(name: &str, props: &str) -> String {
            format!(
                "<Blob><Name>{name}</Name><Properties><Content-Length>42</Content-Length>\
                 <Last-Modified>Mon, 06 May 2024 07:08:09 GMT</Last-Modified>{props}\
                 </Properties><Metadata><Name>x</Name></Metadata></Blob>"
            )
        }

        fn page(inner: &str, marker: &str) -> Reply {
            let marker = if marker.is_empty() {
                "<NextMarker />".to_owned()
            } else {
                format!("<NextMarker>{marker}</NextMarker>")
            };
            Reply::new(
                200,
                format!(
                    "\u{feff}<?xml version=\"1.0\"?><EnumerationResults ServiceEndpoint=\"e\">\
                     <Prefix>d/</Prefix><Blobs>{inner}</Blobs>{marker}</EnumerationResults>"
                ),
            )
        }

        async fn list(
            replies: Vec<Reply>,
            dir: &str,
            tokens: &[Option<&str>],
        ) -> (Vec<Result<ListPage>>, Vec<Request>) {
            let stub = serve(replies).await;
            let endpoint = format!("{}/acct", stub.base);
            let b = backend(&[
                ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                ("AZURE_STORAGE_ACCOUNT_KEY", "a2V5"),
                ("AZURE_STORAGE_ENDPOINT", &endpoint),
                ("AZURE_ALLOW_HTTP", "true"),
            ]);
            let mut pages = Vec::new();
            for token in tokens {
                let token = token.map(str::to_owned);
                pages.push(b.list_page(&ObjectPath::from(dir), 7, token).await);
            }
            (pages, stub.requests())
        }

        #[tokio::test]
        async fn tiers_map_to_states() {
            let inner = [
                blob("d/cold", "<AccessTier>Archive</AccessTier>"),
                blob(
                    "d/going",
                    "<AccessTier>Archive</AccessTier>\
                     <ArchiveStatus>rehydrate-pending-to-cool</ArchiveStatus>",
                ),
                blob("d/hot", "<AccessTier>Hot</AccessTier>"),
                blob("d/cool", "<AccessTier>Cool</AccessTier>"),
                blob("d/none", ""),
                blob(
                    "d/a&amp;b &lt;c&gt; \u{e9}",
                    "<AccessTier>Archive</AccessTier>",
                ),
                blob("d/dir", "<ResourceType>directory</ResourceType>"),
                blob("d/", ""),
                "<BlobPrefix><Name>d/sub&amp;dir/</Name></BlobPrefix>".to_owned(),
            ]
            .concat();
            let (mut pages, requests) = list(vec![page(&inner, "")], "d", &[None]).await;
            let got = pages.remove(0).expect("list");
            let seen: Vec<_> = got
                .objects
                .iter()
                .map(|o| (o.location.as_ref(), o.state, o.class.as_deref()))
                .collect();
            assert_eq!(
                seen,
                [
                    ("d/cold", ObjectState::Archived, Some("Archive")),
                    ("d/going", ObjectState::Restoring, Some("Archive")),
                    ("d/hot", ObjectState::Online, Some("Hot")),
                    ("d/cool", ObjectState::Online, Some("Cool")),
                    ("d/none", ObjectState::Online, None),
                    ("d/a&b <c> \u{e9}", ObjectState::Archived, Some("Archive")),
                ]
            );
            assert_eq!(got.objects[0].size, 42);
            assert_eq!(
                got.objects[0].modified.to_rfc3339(),
                "2024-05-06T07:08:09+00:00"
            );
            let prefixes: Vec<_> = got.prefixes.iter().map(AsRef::as_ref).collect();
            assert_eq!(prefixes, ["d/sub&dir"]);
            assert_eq!(got.next, None);
            assert_eq!(
                requests[0].line,
                "GET /acct/c?restype=container&comp=list&delimiter=%2F&maxresults=7&prefix=d%2F HTTP/1.1"
            );
            assert!(
                requests[0]
                    .headers
                    .to_ascii_lowercase()
                    .contains("authorization: sharedkey acct:"),
                "{}",
                requests[0].headers
            );
        }

        #[tokio::test]
        async fn a_marker_continues_the_listing() {
            let replies = vec![
                page(&blob("a", ""), "1!8!Zm9v/+=="),
                page(&blob("b", ""), ""),
            ];
            let tokens = [None, Some("1!8!Zm9v/+==")];
            let (mut pages, requests) = list(replies, "", &tokens).await;
            let second = pages.pop().expect("second").expect("list");
            let first = pages.pop().expect("first").expect("list");
            assert_eq!(first.next.as_deref(), Some("1!8!Zm9v/+=="));
            assert_eq!(second.next, None);
            assert_eq!(
                requests[0].line,
                "GET /acct/c?restype=container&comp=list&delimiter=%2F&maxresults=7 HTTP/1.1"
            );
            assert_eq!(
                requests[1].line,
                "GET /acct/c?restype=container&comp=list&delimiter=%2F&maxresults=7&marker=1%218%21Zm9v%2F%2B%3D%3D HTTP/1.1"
            );
        }

        #[tokio::test]
        async fn a_busy_page_is_retried() {
            let busy = Reply::new(503, "<Error><Code>ServerBusy</Code></Error>");
            let (mut pages, requests) =
                list(vec![busy, page(&blob("a", ""), "")], "", &[None]).await;
            assert_eq!(pages.remove(0).expect("list").objects.len(), 1);
            assert_eq!(requests.len(), 2);
        }

        #[tokio::test]
        async fn a_denied_page_is_denied() {
            let denied = Reply::new(403, "<Error><Code>AuthorizationFailure</Code></Error>");
            let (mut pages, _) = list(vec![denied], "", &[None]).await;
            let err = pages.remove(0).expect_err("denied");
            assert!(matches!(err, Error::Denied(_)), "{err:?}");
        }
    }
}
