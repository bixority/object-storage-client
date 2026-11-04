//! Credentials from a named AWS profile (static keys, SSO, assumed roles, ...).
//!
//! The profile is resolved by the AWS SDK's `ProfileFileCredentialsProvider`,
//! which supports `sso_*` keys and `sso-session` sections. It reads the shared
//! config and credentials files only: there is no environment chain, and no
//! implicit fallback to instance metadata (IMDS) or any other source, so a
//! profile that is missing or cannot be resolved is an error rather than a
//! silent switch to another identity. A `credential_source` set in the profile
//! itself (`Ec2InstanceMetadata`, `EcsContainer`, `Environment`) is honoured,
//! because it is explicit user configuration.
//!
//! The files are the SDK defaults (`AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`,
//! `~/.aws`) for [`Vars::Env`]; an injected [`Vars::Map`] reads only the files it
//! names (see [`files`]), never the real home directory.
//!
//! Role and web-identity profiles call STS, which needs a region: the
//! environment's `AWS_REGION`, else the profile's `region`, else `us-east-1`.
//! Configuration errors show their cause; every other failure shows a fixed
//! text, because a `credential_process` helper's stderr or a response body can
//! carry secrets. A failed lookup drops the SDK provider, so the next call
//! re-reads the profile files and reports the real reason. While the cached
//! credential has at least a minute left, a slow or failed refresh is not an
//! error: the cached credential is served and the refresh is retried after 30
//! seconds.

use super::{Vars, parse_bool};
use crate::client::{Error, Result};
use async_trait::async_trait;
use aws_config::Region;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::ProvideCredentials;
use aws_credential_types::provider::error::CredentialsError;
use aws_runtime::env_config::EnvConfigValue;
use aws_runtime::env_config::file::{EnvConfigFileKind, EnvConfigFiles};
use aws_types::os_shim_internal::{Env, Fs};
use futures::FutureExt as _;
use futures::future::{BoxFuture, Shared};
use object_store::aws::AwsCredential;
use object_store::{CredentialProvider, Error as StoreError};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};
use std::time::{Duration, SystemTime};
use tokio::time::Instant;
use tracing::instrument::WithSubscriber;
use tracing::subscriber::NoSubscriber;

/// Refresh credentials this long before they expire.
const REFRESH_MARGIN: Duration = Duration::from_mins(5);

/// A cached credential with less than this left is no longer served in place of
/// a refresh, so a pre-signed URL never starts with a few seconds to live.
const MIN_LEFT: Duration = Duration::from_mins(1);

/// How long a failed refresh is not retried while the cached credential is
/// still usable.
const RETRY_BACKOFF: Duration = Duration::from_secs(30);

/// How long a lookup that nobody finished may still be joined. A helper that
/// hangs forever then does not block a new lookup until the process restarts.
const STALE_LOOKUP: Duration = Duration::from_mins(5);

/// The longest one credential lookup may take, lock wait included. It equals
/// the registry's request budget; the SDK clients have no read timeout.
const LOOKUP_TIMEOUT: Duration = Duration::from_secs(30);

/// The legacy global STS region, used when nothing names one.
const FALLBACK_STS_REGION: &str = "us-east-1";

/// The profile files `vars` selects, or `None` for the SDK defaults.
///
/// [`Vars::Env`] returns `None`, so production reads the files the AWS CLI
/// reads. An injected map reads only `AWS_CONFIG_FILE` and
/// `AWS_SHARED_CREDENTIALS_FILE` from itself, and never the real `~/.aws`.
pub fn files(vars: &Vars) -> Option<EnvConfigFiles> {
    let Vars::Map { .. } = vars else {
        return None;
    };
    // `build` panics on an empty set, so an empty config file is always present.
    let mut builder = EnvConfigFiles::builder()
        .include_default_config_file(false)
        .include_default_credentials_file(false)
        .with_contents(EnvConfigFileKind::Config, "");
    for (key, kind) in [
        ("AWS_CONFIG_FILE", EnvConfigFileKind::Config),
        (
            "AWS_SHARED_CREDENTIALS_FILE",
            EnvConfigFileKind::Credentials,
        ),
    ] {
        if let Some(path) = vars.get(key).filter(|v| !v.is_empty()) {
            builder = builder.with_file(kind, path);
        }
    }
    Some(builder.build())
}

/// `AWS_REGION`, then `AWS_DEFAULT_REGION`; empty values are ignored. This is
/// the AWS-only region, the one `STS` is called in.
pub fn env_region(vars: &Vars) -> Option<String> {
    ["AWS_REGION", "AWS_DEFAULT_REGION"]
        .into_iter()
        .find_map(|key| vars.get(key).filter(|v| !v.is_empty()))
}

/// A resolved credential and when it stops being usable.
#[derive(Clone)]
struct Cached {
    credential: Arc<AwsCredential>,
    expires: Option<SystemTime>,
}

impl Cached {
    fn is_fresh(&self, now: SystemTime) -> bool {
        self.expires.is_none_or(|at| now + REFRESH_MARGIN < at)
    }

    /// Whether it may stand in for a refresh that is slow or failed.
    fn is_usable(&self, now: SystemTime) -> bool {
        self.expires.is_none_or(|at| now + MIN_LEFT < at)
    }
}

/// One credential lookup that every caller can await. The error is text, so
/// the output is `Clone` and no SDK error escapes.
type Lookup = Shared<BoxFuture<'static, std::result::Result<Cached, String>>>;

/// A lookup that has started and not been finished by any caller.
struct InFlight {
    id: u64,
    started: Instant,
    lookup: Lookup,
}

/// What the lock guards. It is never held across an await.
#[derive(Default)]
struct State {
    /// The SDK provider, dropped after any failure.
    provider: Option<Arc<ProfileFileCredentialsProvider>>,
    /// The last good credential.
    cached: Option<Cached>,
    in_flight: Option<InFlight>,
    /// Ids for `in_flight`.
    started: u64,
    /// No refresh before this, while the cached credential is usable.
    retry_after: Option<Instant>,
}

/// What a caller does next.
enum Step {
    Ready(Arc<AwsCredential>),
    /// Await the lookup. `fallback` replaces its failure.
    Await {
        id: u64,
        lookup: Lookup,
        fallback: Option<Arc<AwsCredential>>,
    },
}

/// The first half of a decision, taken under the lock.
enum Plan {
    Done(Step),
    /// Start a lookup. The provider is the cached one, if any; building a new
    /// one reads the profile files, which must not happen under the lock.
    Start {
        provider: Option<Arc<ProfileFileCredentialsProvider>>,
        fallback: Option<Arc<AwsCredential>>,
    },
}

/// Test seams: replacements for what the SDK would reach over the network or
/// the process environment.
#[cfg(test)]
#[derive(Default)]
struct Hooks {
    /// The HTTP client the SDK's STS and SSO clients use.
    http_client: Option<aws_smithy_runtime_api::client::http::SharedHttpClient>,
    /// Named credential sources, registered on every (re)built provider. A
    /// source named like a built-in (`Environment`) replaces it.
    sources: Vec<(
        &'static str,
        aws_credential_types::provider::SharedCredentialsProvider,
    )>,
}

/// An [`object_store`] credential provider backed by one AWS profile.
///
/// Construction does no I/O; the SDK provider is built on the first
/// [`get_credential`](CredentialProvider::get_credential). Resolved credentials
/// are reused until shortly before they expire (SSO credentials are temporary).
pub struct ProfileCredentials {
    profile: String,
    /// `AWS_REGION` / `AWS_DEFAULT_REGION`, the first choice for STS.
    env_region: Option<String>,
    /// The profile files; `None` means the SDK defaults.
    files: Option<EnvConfigFiles>,
    #[cfg(test)]
    hooks: Hooks,
    state: Mutex<State>,
}

// Only the profile name: never anything that could carry key material, which
// includes the contents of in-memory profile files.
impl fmt::Debug for ProfileCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProfileCredentials")
            .field("profile", &self.profile)
            .finish_non_exhaustive()
    }
}

impl ProfileCredentials {
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Decide what the caller does: use the cache, or await a lookup, starting
    /// one if none is running.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "one lock checks for a running lookup and registers the new one"
    )]
    fn step(&self) -> Step {
        let (provider, fallback) = match self.plan() {
            Plan::Done(step) => return step,
            Plan::Start { provider, fallback } => (provider, fallback),
        };
        // Building reads the profile files, and the SDK logs that.
        let provider = provider.unwrap_or_else(|| {
            tracing::subscriber::with_default(NoSubscriber::default(), || {
                Arc::new(self.build_provider())
            })
        });
        let tick = Instant::now();
        let mut state = self.state();
        // Another caller may have started one meanwhile.
        if let Some(f) = &state.in_flight {
            return Step::Await {
                id: f.id,
                lookup: f.lookup.clone(),
                fallback,
            };
        }
        let provider = Arc::clone(state.provider.get_or_insert(provider));
        let lookup = Self::start(self.profile.clone(), provider);
        state.started += 1;
        let id = state.started;
        state.in_flight = Some(InFlight {
            id,
            started: tick,
            lookup: lookup.clone(),
        });
        Step::Await {
            id,
            lookup,
            fallback,
        }
    }

    fn plan(&self) -> Plan {
        let (now, tick) = (SystemTime::now(), Instant::now());
        let mut state = self.state();
        if let Some(c) = state.cached.as_ref().filter(|c| c.is_fresh(now)) {
            return Plan::Done(Step::Ready(Arc::clone(&c.credential)));
        }
        let fallback = state
            .cached
            .as_ref()
            .filter(|c| c.is_usable(now))
            .map(|c| Arc::clone(&c.credential));
        // A lookup that nobody finishes (its caller was dropped, or it hung) is
        // abandoned. With a fallback it may run for one lookup timeout; without
        // one it is joined longer, so a helper that is still running is not
        // started twice.
        let limit = if fallback.is_some() {
            LOOKUP_TIMEOUT
        } else {
            STALE_LOOKUP
        };
        if state
            .in_flight
            .as_ref()
            .is_some_and(|f| tick.duration_since(f.started) >= limit)
        {
            state.in_flight = None;
            state.provider = None;
            if fallback.is_some() {
                tracing::warn!(
                    "AWS profile `{}` credential refresh did not finish; using the cached credential",
                    self.profile
                );
            }
        }
        // A usable credential never waits for a refresh that is already running
        // or that failed a moment ago.
        if let Some(credential) = &fallback
            && (state.in_flight.is_some() || state.retry_after.is_some_and(|t| tick < t))
        {
            return Plan::Done(Step::Ready(Arc::clone(credential)));
        }
        if let Some(f) = &state.in_flight {
            return Plan::Done(Step::Await {
                id: f.id,
                lookup: f.lookup.clone(),
                fallback,
            });
        }
        Plan::Start {
            provider: state.provider.clone(),
            fallback,
        }
    }

    /// The lookup itself. The SDK traces each request with its headers, and the
    /// redaction list of aws-smithy-runtime-api leaves out
    /// `x-amz-sso_bearer_token`, so it runs without a subscriber: no host log
    /// may see that token, nor a helper's error text.
    fn start(profile: String, provider: Arc<ProfileFileCredentialsProvider>) -> Lookup {
        async move {
            let creds = provider
                .provide_credentials()
                .await
                .map_err(|e| describe(&profile, &e))?;
            Ok(Cached {
                credential: Arc::new(AwsCredential {
                    key_id: creds.access_key_id().to_owned(),
                    secret_key: creds.secret_access_key().to_owned(),
                    token: creds.session_token().map(str::to_owned),
                }),
                expires: creds.expiry(),
            })
        }
        .with_subscriber(NoSubscriber::default())
        .boxed()
        .shared()
    }

    /// Record the outcome of lookup `id`; only the first caller to see it does.
    /// A failed provider is dropped and rebuilt, re-reading the files.
    fn finish(&self, id: u64, result: &std::result::Result<Cached, String>) {
        let mut state = self.state();
        if state.in_flight.as_ref().is_none_or(|f| f.id != id) {
            return;
        }
        state.in_flight = None;
        if let Ok(cached) = result {
            state.cached = Some(cached.clone());
            state.retry_after = None;
        } else {
            state.provider = None;
            state.retry_after = Some(Instant::now() + RETRY_BACKOFF);
        }
    }

    /// Resolve the credential: the cached one while it is fresh, else the
    /// shared lookup. Callers share one lookup, so a SSO request or a
    /// `credential_process` helper runs once however many wait.
    async fn lookup(&self) -> object_store::Result<Arc<AwsCredential>> {
        let (id, lookup, fallback) = match self.step() {
            Step::Ready(credential) => return Ok(credential),
            Step::Await {
                id,
                lookup,
                fallback,
            } => (id, lookup, fallback),
        };
        // Every caller is bounded by its own 30 s. With a fallback, a timeout
        // abandons the lookup, so the refresh is retried after the back-off.
        // Without one the lookup stays for the next caller to join (see
        // `STALE_LOOKUP`). The helper process it started is not killed. The
        // text is fixed: nothing from the SDK.
        let waited = tokio::time::timeout(LOOKUP_TIMEOUT, lookup).await;
        let timed_out = waited.is_err();
        let result = waited.unwrap_or_else(|_| {
            Err(format!(
                "AWS profile `{}` credentials unavailable: timed out after {} s",
                self.profile,
                LOOKUP_TIMEOUT.as_secs()
            ))
        });
        if !timed_out || fallback.is_some() {
            self.finish(id, &result);
        }
        match result {
            Ok(cached) => Ok(cached.credential),
            // The text is already redacted, so it may be logged.
            Err(text) => fallback.map_or_else(
                || {
                    Err(StoreError::Generic {
                        store: "S3",
                        source: text.clone().into(),
                    })
                },
                |credential| {
                    tracing::warn!("{text}; using the cached credential");
                    Ok(credential)
                },
            ),
        }
    }

    /// A provider for `profile`. Reads nothing until credentials are requested.
    ///
    /// `env_region` is the region STS is called in; without it the profile's
    /// own `region` (or `us-east-1`) is used. `files` are the profile files
    /// (see [`files`]); `None` means the SDK defaults.
    pub fn new(profile: String, env_region: Option<String>, files: Option<EnvConfigFiles>) -> Self {
        Self {
            profile,
            env_region,
            files,
            #[cfg(test)]
            hooks: Hooks::default(),
            state: Mutex::new(State::default()),
        }
    }

    /// The SDK provider. Building it does no I/O apart from the profile lookup.
    fn build_provider(&self) -> ProfileFileCredentialsProvider {
        let region = self.env_region.clone().unwrap_or_else(|| {
            settings(&self.profile, self.files.clone(), true)
                .ok()
                .and_then(|s| s.region)
                .unwrap_or_else(|| FALLBACK_STS_REGION.to_owned())
        });
        // `without_region` keeps the sleep implementation retries need; the
        // default region chain would end at IMDS.
        let config = ProviderConfig::without_region().with_region(Some(Region::new(region)));
        #[cfg(test)]
        let config = match &self.hooks.http_client {
            Some(client) => config.with_http_client(client.clone()),
            None => config,
        };
        let builder = ProfileFileCredentialsProvider::builder()
            .profile_name(&self.profile)
            .configure(&config);
        #[cfg(test)]
        let builder = self
            .hooks
            .sources
            .iter()
            .fold(builder, |b, (name, source)| {
                b.with_custom_provider(*name, source.clone())
            });
        match &self.files {
            Some(files) => builder.profile_files(files.clone()),
            None => builder,
        }
        .build()
    }
}

/// The state of one named AWS profile, built once per registry and shared by
/// every S3 backend it creates: the S3 settings are read once, the credential
/// provider re-reads the profile files when it is (re)built, and all backends
/// sign through one credential cache.
pub struct AwsProfile {
    name: String,
    files: Option<EnvConfigFiles>,
    /// `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS`: skip the profile's endpoint.
    ignore_endpoints: bool,
    /// Kept only after a successful read, so a fixed file is picked up later.
    settings: OnceLock<Settings>,
    credentials: Arc<ProfileCredentials>,
}

// Only the profile name: the in-memory files could carry key material.
impl fmt::Debug for AwsProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsProfile")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl AwsProfile {
    /// The profile `name`, with its files and STS region taken from `vars`.
    /// Does no I/O.
    pub fn new(name: String, vars: &Vars) -> Self {
        let files = files(vars);
        let env_region = env_region(vars);
        let ignore_endpoints = vars
            .get("AWS_IGNORE_CONFIGURED_ENDPOINT_URLS")
            .and_then(|v| parse_bool(&v))
            .unwrap_or(false);
        Self {
            credentials: Arc::new(ProfileCredentials::new(
                name.clone(),
                env_region,
                files.clone(),
            )),
            name,
            files,
            ignore_endpoints,
            settings: OnceLock::new(),
        }
    }

    /// The profile's region and S3 endpoint, read on first use.
    ///
    /// # Errors
    ///
    /// As [`settings`]; a failed read is retried by the next call.
    pub fn settings(&self) -> Result<&Settings> {
        if let Some(settings) = self.settings.get() {
            return Ok(settings);
        }
        let read = settings(&self.name, self.files.clone(), self.ignore_endpoints)?;
        Ok(self.settings.get_or_init(|| read))
    }

    /// The shared credential provider.
    pub fn credentials(&self) -> Arc<ProfileCredentials> {
        Arc::clone(&self.credentials)
    }
}

/// What a profile says about S3, read in one pass.
#[derive(Debug, PartialEq, Eq)]
pub struct Settings {
    /// The profile's `region`, following `source_profile`.
    pub region: Option<String>,
    /// The profile's S3 `endpoint_url`: the `services` S3 entry, else the
    /// global `endpoint_url`.
    pub s3_endpoint: Option<String>,
}

/// The messages of `error` and its causes, without repeats.
fn causes(error: &(dyn std::error::Error + 'static)) -> Vec<String> {
    let mut causes: Vec<String> = Vec::new();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if causes.last() != Some(&text) {
            causes.push(text);
        }
        source = cause.source();
    }
    causes
}

/// The region and S3 endpoint of `profile`, loaded from `files` (the SDK
/// defaults when `None`). `ignore_endpoints` skips the endpoint lookup, as
/// `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS` asks; the profile's own
/// `ignore_configured_endpoint_urls` does the same.
///
/// The SDK's loader is async but performs plain blocking file reads and uses
/// no runtime primitive, so one poll completes it: this works on any thread,
/// inside a runtime or not.
///
/// # Errors
///
/// Returns [`Error::Generic`] naming the profile when its files cannot be read
/// or parsed, or when the load does not complete in one poll (an SDK change).
/// A missing profile is not an error here: its credentials report it.
pub fn settings(
    profile: &str,
    files: Option<EnvConfigFiles>,
    ignore_endpoints: bool,
) -> Result<Settings> {
    let files = files.unwrap_or_default();
    let loaded = aws_config::profile::load(
        &Fs::real(),
        &Env::real(),
        &files,
        Some(profile.to_owned().into()),
    )
    .now_or_never()
    .ok_or_else(|| {
        Error::Generic(format!(
            "AWS profile `{profile}` could not be read: the profile loader did not finish"
        ))
    })?;
    let set = loaded.map_err(|e| {
        let mut parts = vec![e.to_string()];
        parts.extend(causes(&e));
        Error::Generic(format!(
            "AWS profile `{profile}` could not be read: {}",
            parts.join(": ")
        ))
    })?;

    let mut region = None;
    let mut seen = vec![profile.to_owned()];
    let mut current = set.get_profile(profile);
    while let Some(p) = current {
        if let Some(r) = p.get("region") {
            region = Some(r.to_owned());
            break;
        }
        current = p
            .get("source_profile")
            .filter(|next| !seen.iter().any(|s| s == next))
            .inspect(|next| seen.push((*next).to_owned()))
            .and_then(|next| set.get_profile(next));
    }

    let profile_ignores = set
        .get("ignore_configured_endpoint_urls")
        .and_then(parse_bool)
        .unwrap_or(false);
    // The empty environment keeps `AWS_ENDPOINT_URL*` out of this lookup: the
    // caller reads those through `Vars`.
    let s3_endpoint = if ignore_endpoints || profile_ignores {
        None
    } else {
        EnvConfigValue::new()
            .profile("endpoint_url")
            .service_id("S3")
            .load(&Env::from_slice(&[]), Some(&set))
            .map(|(value, _)| value.into_owned())
            .filter(|v| !v.is_empty())
    };
    Ok(Settings {
        region,
        s3_endpoint,
    })
}

/// What to tell the caller about a failed credential lookup.
///
/// Configuration errors (profile not found, SSO settings missing) show the
/// text of their causes: profile and section names, paths and line numbers.
/// Every other kind, notably provider and unhandled errors, can carry the
/// stderr of a `credential_process` or a response body, so only a fixed hint is
/// shown. The SDK's `Debug` output is never used: it prints raw responses.
fn describe(profile: &str, error: &CredentialsError) -> String {
    let prefix = format!("AWS profile `{profile}` credentials unavailable");
    match error {
        CredentialsError::CredentialsNotLoaded(_) | CredentialsError::InvalidConfiguration(_) => {
            let causes = causes(error);
            if causes.iter().any(|c| c.contains("credentials-login")) {
                return format!(
                    "AWS profile `{profile}` uses login_session, which object-storage-client \
                     does not support; export its credentials with `aws configure \
                     export-credentials --profile {profile} --format process` and use the \
                     result as a credential_process"
                );
            }
            if causes.is_empty() {
                format!("{prefix}: no credentials found")
            } else {
                format!("{prefix}: {}", causes.join(": "))
            }
        }
        _ => format!(
            "{prefix}: the credential source or role assumption failed \
             (for an SSO profile, run `aws sso login --profile {profile}`)"
        ),
    }
}

#[async_trait]
impl CredentialProvider for ProfileCredentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        self.lookup().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_credential_types::Credentials;
    use aws_credential_types::provider::{SharedCredentialsProvider, future};
    use aws_smithy_runtime_api::client::http::{
        HttpConnector, HttpConnectorFuture, SharedHttpClient, SharedHttpConnector, http_client_fn,
    };
    use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
    use aws_smithy_types::body::SdkBody;
    use std::fmt::Write as _;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tracing::field::{Field, Visit};
    use tracing::span::{Attributes, Id, Record};
    use tracing::{Event, Metadata, Subscriber};

    /// In-memory profile files holding only `config`.
    fn files(config: &str) -> EnvConfigFiles {
        EnvConfigFiles::builder()
            .include_default_config_file(false)
            .include_default_credentials_file(false)
            .with_contents(EnvConfigFileKind::Config, config)
            .build()
    }

    /// A provider for `profile` reading `files`.
    fn creds(profile: &str, files: EnvConfigFiles) -> ProfileCredentials {
        ProfileCredentials::new(profile.to_owned(), None, Some(files))
    }

    /// What the recording connector saw of one request.
    #[derive(Debug)]
    struct Seen {
        authorization: String,
        body: String,
    }

    #[derive(Debug)]
    struct Recorder {
        seen: Arc<StdMutex<Vec<Seen>>>,
        reply: String,
    }

    impl HttpConnector for Recorder {
        fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
            let authorization = request
                .headers()
                .get("authorization")
                .unwrap_or_default()
                .to_owned();
            let body = request
                .body()
                .bytes()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .unwrap_or_default();
            self.seen.lock().expect("lock").push(Seen {
                authorization,
                body,
            });
            let response = HttpResponse::new(
                200u16.try_into().expect("status"),
                SdkBody::from(self.reply.clone()),
            );
            HttpConnectorFuture::ready(Ok(response))
        }
    }

    /// An HTTP client that answers every request with `reply` and records it.
    fn recording(reply: &str) -> (SharedHttpClient, Arc<StdMutex<Vec<Seen>>>) {
        let seen = Arc::new(StdMutex::new(Vec::new()));
        let (shared, reply) = (Arc::clone(&seen), reply.to_owned());
        let client = http_client_fn(move |_, _| {
            SharedHttpConnector::new(Recorder {
                seen: Arc::clone(&shared),
                reply: reply.clone(),
            })
        });
        (client, seen)
    }

    fn sts_reply(action: &str) -> String {
        format!(
            "<{action}Response xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
             <{action}Result><Credentials><AccessKeyId>ASIAROLE</AccessKeyId>\
             <SecretAccessKey>rolesecret</SecretAccessKey>\
             <SessionToken>roletok</SessionToken>\
             <Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>\
             </{action}Result></{action}Response>"
        )
    }

    fn role_files(base_region: &str, region: &str) -> EnvConfigFiles {
        files(&format!(
            "[profile base]\naws_access_key_id = AKIDBASE\naws_secret_access_key = basesecret\n\
             {base_region}[profile r]\nrole_arn = arn:aws:iam::123456789012:role/r\n\
             source_profile = base\n{region}"
        ))
    }

    /// Resolve profile `r` of `config` with STS answered by the recorder and
    /// return the one request's `SigV4` authorization header.
    async fn sts_scope(config: EnvConfigFiles, env_region: Option<&str>) -> String {
        let (client, seen) = recording(&sts_reply("AssumeRole"));
        let creds = ProfileCredentials {
            hooks: Hooks {
                http_client: Some(client),
                ..Hooks::default()
            },
            env_region: env_region.map(str::to_owned),
            ..creds("r", config)
        };
        let got = creds.get_credential().await.expect("assumed role");
        assert_eq!(got.key_id, "ASIAROLE");
        assert_eq!(got.token.as_deref(), Some("roletok"));
        let auth = {
            let seen = seen.lock().expect("lock");
            assert_eq!(seen.len(), 1);
            seen[0].authorization.clone()
        };
        assert!(auth.contains("Credential=AKIDBASE/"), "{auth}");
        auth
    }

    #[tokio::test]
    async fn a_role_profile_assumes_the_role_in_the_profile_region() {
        let auth = sts_scope(role_files("", "region = eu-west-3\n"), None).await;
        assert!(auth.contains("/eu-west-3/sts/aws4_request"), "{auth}");
    }

    #[tokio::test]
    async fn the_environment_region_wins_for_sts() {
        let auth = sts_scope(role_files("", "region = eu-west-3\n"), Some("ap-south-1")).await;
        assert!(auth.contains("/ap-south-1/sts/aws4_request"), "{auth}");
    }

    #[tokio::test]
    async fn the_sts_region_is_inherited_through_source_profile() {
        let auth = sts_scope(role_files("region = eu-north-1\n", ""), None).await;
        assert!(auth.contains("/eu-north-1/sts/aws4_request"), "{auth}");
    }

    #[tokio::test]
    async fn sts_falls_back_to_us_east_1() {
        let auth = sts_scope(role_files("", ""), None).await;
        assert!(auth.contains("/us-east-1/sts/aws4_request"), "{auth}");
    }

    #[tokio::test]
    async fn a_web_identity_profile_reaches_sts() {
        let dir = tempfile::tempdir().expect("tempdir");
        let token = dir.path().join("token");
        std::fs::write(&token, "dummy-web-identity-token").expect("write");
        let (client, seen) = recording(&sts_reply("AssumeRoleWithWebIdentity"));
        let creds = ProfileCredentials {
            hooks: Hooks {
                http_client: Some(client),
                ..Hooks::default()
            },
            ..creds(
                "wi",
                files(&format!(
                    "[profile wi]\nrole_arn = arn:aws:iam::123456789012:role/wi\n\
                     web_identity_token_file = {}\nregion = eu-west-3\n",
                    token.display()
                )),
            )
        };
        let got = creds.get_credential().await.expect("web identity");
        assert_eq!(got.key_id, "ASIAROLE");
        let seen = seen.lock().expect("lock");
        assert_eq!(seen.len(), 1);
        assert!(
            seen[0].body.contains("Action=AssumeRoleWithWebIdentity"),
            "{}",
            seen[0].body
        );
    }

    #[tokio::test]
    async fn static_keys_of_the_named_profile_are_returned() {
        let creds = creds(
            "dev",
            files(
                "[default]\naws_access_key_id = WRONG\naws_secret_access_key = WRONG\n\
                 [profile dev]\naws_access_key_id = AKIDDEV\naws_secret_access_key = s3cr3t\n\
                 aws_session_token = tok\nregion = eu-west-3\n",
            ),
        );
        let got = creds.get_credential().await.expect("credential");
        assert_eq!(got.key_id, "AKIDDEV");
        assert_eq!(got.secret_key, "s3cr3t");
        assert_eq!(got.token.as_deref(), Some("tok"));
        // Served again without a second resolution.
        assert!(Arc::ptr_eq(
            &got,
            &creds.get_credential().await.expect("again")
        ));
        assert!(!format!("{creds:?}").contains("s3cr3t"));
    }

    #[test]
    fn describe_shows_configuration_causes() {
        #[derive(Debug)]
        struct Outer(Inner);
        #[derive(Debug)]
        struct Inner;
        impl fmt::Display for Outer {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("outer cause")
            }
        }
        impl fmt::Display for Inner {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("inner cause")
            }
        }
        impl std::error::Error for Inner {}
        impl std::error::Error for Outer {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                Some(&self.0)
            }
        }
        let not_loaded = CredentialsError::not_loaded("profile `p` was not defined: x");
        assert!(describe("p", &not_loaded).contains("profile `p` was not defined: x"));

        let text = describe("p", &CredentialsError::invalid_configuration(Outer(Inner)));
        assert_eq!(text.matches("outer cause").count(), 1, "{text}");
        assert_eq!(text.matches("inner cause").count(), 1, "{text}");
    }

    #[test]
    fn describe_redacts_provider_and_unhandled_errors() {
        let fixed = "AWS profile `p` credentials unavailable: the credential source or role \
                     assumption failed (for an SSO profile, run `aws sso login --profile p`)";
        for error in [
            CredentialsError::provider_error("TOPSECRET"),
            CredentialsError::unhandled("TOPSECRET"),
        ] {
            assert_eq!(describe("p", &error), fixed);
        }
    }

    #[tokio::test]
    async fn a_login_session_profile_is_reported_as_unsupported() {
        let creds = creds(
            "lg",
            files("[profile lg]\nlogin_session = arn:aws:iam::123456789012:user/Admin\n"),
        );
        let text = creds
            .get_credential()
            .await
            .expect_err("unsupported")
            .to_string();
        assert!(text.contains("uses login_session"), "{text}");
        assert!(text.contains("export-credentials --profile lg"), "{text}");
    }

    #[tokio::test]
    async fn an_unknown_profile_names_the_cause() {
        let creds = creds("nope", files("[profile other]\n"));
        let text = creds
            .get_credential()
            .await
            .expect_err("missing")
            .to_string();
        assert!(text.contains("profile `nope` was not defined"), "{text}");
        assert!(!text.contains("169.254.169.254"), "{text}");
    }

    #[tokio::test]
    async fn an_incomplete_sso_profile_names_the_missing_settings() {
        let creds = creds(
            "s",
            files(
                "[profile s]\nsso_start_url = https://example.invalid/start\n\
                 sso_region = us-east-1\n",
            ),
        );
        let text = creds
            .get_credential()
            .await
            .expect_err("incomplete")
            .to_string();
        assert!(text.contains("sso_account_id"), "{text}");
    }

    #[tokio::test]
    async fn a_failed_profile_reports_the_same_reason_twice() {
        let creds = creds(
            "dev",
            files(
                "[profile dev]\nsso_session = corp\nsso_account_id = 123456789012\n\
                 sso_role_name = Reader\n",
            ),
        );
        for call in ["first", "second"] {
            let text = creds
                .get_credential()
                .await
                .expect_err("missing sso-session")
                .to_string();
            assert!(text.contains("sso-session named `corp`"), "{call}: {text}");
        }
    }

    #[tokio::test]
    async fn a_fixed_profile_file_is_picked_up() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");
        std::fs::write(&path, "[profile other]\n").expect("write");
        let creds = creds(
            "dev",
            EnvConfigFiles::builder()
                .include_default_config_file(false)
                .include_default_credentials_file(false)
                .with_file(EnvConfigFileKind::Config, &path)
                .build(),
        );
        creds.get_credential().await.expect_err("no such profile");
        std::fs::write(
            &path,
            "[profile dev]\naws_access_key_id = AKIDDEV\naws_secret_access_key = s\n",
        )
        .expect("rewrite");
        let got = creds.get_credential().await.expect("fixed");
        assert_eq!(got.key_id, "AKIDDEV");
    }

    #[tokio::test]
    async fn a_malformed_secret_line_is_not_echoed() {
        let creds = creds("p", files("[profile p]\naws_secret_access_key TOPSECRET\n"));
        let err = creds.get_credential().await.expect_err("malformed");
        let text = format!("{err} {err:?}");
        assert!(!text.contains("TOPSECRET"), "{text}");
    }

    /// Counts events and keeps the text of their fields.
    #[derive(Clone, Default)]
    struct Capture(Arc<StdMutex<Vec<String>>>);

    struct Fields(String);
    impl Visit for Fields {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            let _ = write!(self.0, "{}={value:?} ", field.name());
        }
    }

    impl Subscriber for Capture {
        fn enabled(&self, _: &Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &Attributes<'_>) -> Id {
            Id::from_u64(1)
        }
        fn record(&self, _: &Id, _: &Record<'_>) {}
        fn record_follows_from(&self, _: &Id, _: &Id) {}
        fn event(&self, event: &Event<'_>) {
            let mut fields = Fields(String::new());
            event.record(&mut fields);
            self.0.lock().expect("lock").push(fields.0);
        }
        fn enter(&self, _: &Id) {}
        fn exit(&self, _: &Id) {}
    }

    #[tokio::test]
    async fn sdk_events_during_a_lookup_are_suppressed() {
        let config = "[profile dev]\naws_access_key_id = AKIDDEV\naws_secret_access_key = s\n";
        let capture = Capture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());

        // Control: the SDK does emit events for the same lookup.
        let raw = ProfileFileCredentialsProvider::builder()
            .profile_name("dev")
            .profile_files(files(config))
            .build();
        raw.provide_credentials().await.expect("raw lookup");
        let control = capture.0.lock().expect("lock").len();
        assert!(
            control >= 1,
            "the SDK logged nothing, so this proves nothing"
        );

        capture.0.lock().expect("lock").clear();
        let creds = creds("dev", files(config));
        creds.get_credential().await.expect("lookup");
        assert_eq!(capture.0.lock().expect("lock").len(), 0);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credential_process_stderr_never_reaches_tracing() {
        let capture = Capture::default();
        let _guard = tracing::subscriber::set_default(capture.clone());
        let creds = creds(
            "proc",
            files(
                "[profile proc]\n\
                 credential_process = sh -c 'echo TOPSECRET >&2; exit 1'\n",
            ),
        );
        creds.get_credential().await.expect_err("process fails");
        let events = capture.0.lock().expect("lock").join("\n");
        assert!(!events.contains("TOPSECRET"), "{events}");

        // Control: without the wrapper the SDK does log the helper's stderr.
        let raw = ProfileFileCredentialsProvider::builder()
            .profile_name("proc")
            .profile_files(files(
                "[profile proc]\ncredential_process = sh -c 'echo TOPSECRET >&2; exit 1'\n",
            ))
            .build();
        raw.provide_credentials().await.expect_err("process fails");
        let events = capture.0.lock().expect("lock").join("\n");
        assert!(events.contains("TOPSECRET"), "control saw: {events}");
    }

    // ---- the Vars-injected files (AWS-08) ----

    #[test]
    fn the_real_environment_keeps_the_sdk_default_files() {
        assert!(super::files(&Vars::Env).is_none());
    }

    #[tokio::test]
    async fn a_credentials_file_from_the_map_is_read() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("credentials");
        std::fs::write(
            &path,
            "[dev]\naws_access_key_id = AKIDFILE\naws_secret_access_key = filesecret\n",
        )
        .expect("write");
        let vars =
            Vars::from_pairs(&[("AWS_SHARED_CREDENTIALS_FILE", path.to_str().expect("utf8"))]);
        let creds = ProfileCredentials::new("dev".into(), None, super::files(&vars));
        let got = creds.get_credential().await.expect("credential");
        assert_eq!(got.key_id, "AKIDFILE");
        assert_eq!(got.secret_key, "filesecret");
    }

    #[tokio::test]
    async fn a_map_without_file_names_defines_no_profile() {
        let creds =
            ProfileCredentials::new("dev".into(), None, super::files(&Vars::from_pairs(&[])));
        let text = creds
            .get_credential()
            .await
            .expect_err("no files")
            .to_string();
        // An empty file set has no profiles at all, so the SDK finds no source.
        assert!(
            text.contains("AWS profile `dev` credentials unavailable: no credentials found"),
            "{text}"
        );
    }

    /// A profile whose `credential_process` runs the shell script `body`,
    /// which first creates `<dir>/ran`. Returns the provider and the marker.
    #[cfg(unix)]
    fn process_profile(
        dir: &std::path::Path,
        body: &str,
    ) -> (ProfileCredentials, std::path::PathBuf) {
        let marker = dir.join("ran");
        let script = dir.join("helper.sh");
        std::fs::write(&script, format!("touch '{}'\n{body}\n", marker.display()))
            .expect("write script");
        let config = format!(
            "[profile proc]\ncredential_process = /bin/sh {}\n",
            script.display()
        );
        (creds("proc", files(&config)), marker)
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credential_process_success() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (creds, marker) = process_profile(
            dir.path(),
            r#"printf '%s' '{"Version":1,"AccessKeyId":"AKIDPROC","SecretAccessKey":"procsecret","SessionToken":"proctok"}'"#,
        );
        let got = creds.get_credential().await.expect("helper credentials");
        assert_eq!(got.key_id, "AKIDPROC");
        assert_eq!(got.secret_key, "procsecret");
        assert_eq!(got.token.as_deref(), Some("proctok"));
        assert!(marker.exists(), "the helper did not run");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn credential_process_failure_is_redacted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (creds, marker) = process_profile(dir.path(), "echo TOPSECRET >&2\nexit 1");
        let err = creds.get_credential().await.expect_err("helper fails");
        let text = format!("{err} {err:?}");
        assert!(text.contains("`proc`"), "{text}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        assert!(marker.exists(), "the helper did not run");
    }

    // ---- the credential cache (AWS-09) ----

    #[derive(Clone, Copy, Debug)]
    enum Outcome {
        Credentials,
        FailFirst,
        /// Every call after the first fails.
        FailLater,
        /// Every call after the first never answers.
        HangLater,
        NotLoaded,
    }

    /// A credential source that counts its calls.
    #[derive(Clone, Debug)]
    struct Fake {
        calls: Arc<AtomicUsize>,
        key: &'static str,
        expiry: Option<SystemTime>,
        delay: Duration,
        outcome: Outcome,
    }

    impl Fake {
        fn new(key: &'static str, expiry: Option<SystemTime>) -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                key,
                expiry,
                delay: Duration::ZERO,
                outcome: Outcome::Credentials,
            }
        }

        fn calls(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }
    }

    impl ProvideCredentials for Fake {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            let this = self.clone();
            future::ProvideCredentials::new(async move {
                let n = this.calls.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(this.delay).await;
                match this.outcome {
                    Outcome::NotLoaded => Err(CredentialsError::not_loaded("fake has nothing")),
                    Outcome::FailFirst if n == 0 => {
                        Err(CredentialsError::provider_error("TOPSECRET"))
                    }
                    Outcome::FailLater if n > 0 => {
                        Err(CredentialsError::provider_error("TOPSECRET"))
                    }
                    Outcome::HangLater if n > 0 => std::future::pending().await,
                    _ => Ok(Credentials::new(
                        this.key,
                        "fakesecret",
                        Some("faketoken".into()),
                        this.expiry,
                        "fake",
                    )),
                }
            })
        }
    }

    /// Profile `profile` of `config`, with `fake` registered as credential
    /// source `name`.
    fn with_fake(
        profile: &str,
        config: &str,
        name: &'static str,
        fake: &Fake,
    ) -> ProfileCredentials {
        ProfileCredentials {
            hooks: Hooks {
                sources: vec![(name, SharedCredentialsProvider::new(fake.clone()))],
                ..Hooks::default()
            },
            ..creds(profile, files(config))
        }
    }

    const FAKE_PROFILE: &str = "[profile t]\ncredential_source = Fake\n";

    fn cached(expires: Option<SystemTime>) -> Cached {
        Cached {
            credential: Arc::new(AwsCredential {
                key_id: "k".into(),
                secret_key: "s".into(),
                token: None,
            }),
            expires,
        }
    }

    #[test]
    fn is_fresh_honours_the_refresh_margin() {
        let now = SystemTime::now();
        let s = Duration::from_secs(1);
        assert!(cached(None).is_fresh(now));
        assert!(cached(Some(now + REFRESH_MARGIN + s)).is_fresh(now));
        assert!(!cached(Some(now + REFRESH_MARGIN)).is_fresh(now));
        assert!(!cached(Some(now - s)).is_fresh(now));
    }

    #[tokio::test]
    async fn a_credential_without_expiry_is_fetched_once() {
        let fake = Fake::new("AKIDFAKE", None);
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let first = creds.get_credential().await.expect("first");
        let second = creds.get_credential().await.expect("second");
        assert_eq!(fake.calls(), 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[tokio::test]
    async fn a_credential_far_from_expiry_is_reused() {
        let fake = Fake::new(
            "AKIDFAKE",
            Some(SystemTime::now() + Duration::from_hours(1)),
        );
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        creds.get_credential().await.expect("first");
        creds.get_credential().await.expect("second");
        assert_eq!(fake.calls(), 1);
    }

    #[tokio::test]
    async fn a_credential_inside_the_margin_is_refreshed() {
        let fake = Fake::new("AKIDFAKE", Some(SystemTime::now() + Duration::from_mins(4)));
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let first = creds.get_credential().await.expect("first");
        let second = creds.get_credential().await.expect("second");
        assert_eq!(fake.calls(), 2);
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_callers_share_one_fetch() {
        let mut fake = Fake::new("AKIDFAKE", None);
        fake.delay = Duration::from_secs(1);
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let all = futures::future::join_all((0..16).map(|_| creds.get_credential())).await;
        assert_eq!(fake.calls(), 1);
        let first = all[0].as_ref().expect("first");
        for got in &all {
            assert!(Arc::ptr_eq(first, got.as_ref().expect("credential")));
        }
    }

    #[tokio::test]
    async fn a_failure_is_not_cached() {
        let mut fake = Fake::new("AKIDFAKE", None);
        fake.outcome = Outcome::FailFirst;
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let err = creds.get_credential().await.expect_err("first call fails");
        let text = format!("{err} {err:?}");
        assert!(text.contains("`t`"), "{text}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        let got = creds.get_credential().await.expect("second call succeeds");
        assert_eq!(got.key_id, "AKIDFAKE");
        assert_eq!(fake.calls(), 2);
    }

    /// Pending forever on the first `silent` calls, then a credential.
    #[derive(Clone, Debug)]
    struct Silent {
        silent: Arc<std::sync::atomic::AtomicBool>,
        calls: Arc<AtomicUsize>,
    }

    impl ProvideCredentials for Silent {
        fn provide_credentials<'a>(&'a self) -> future::ProvideCredentials<'a>
        where
            Self: 'a,
        {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let silent = self.silent.load(Ordering::SeqCst);
            future::ProvideCredentials::new(async move {
                if silent {
                    std::future::pending::<()>().await;
                }
                Ok(Credentials::new("AKIDLATE", "s", None, None, "silent"))
            })
        }
    }

    fn silent_profile() -> (ProfileCredentials, Arc<std::sync::atomic::AtomicBool>) {
        let (creds, flag, _) = counted_silent_profile();
        (creds, flag)
    }

    /// As [`silent_profile`], with the count of calls to the source.
    fn counted_silent_profile() -> (
        ProfileCredentials,
        Arc<std::sync::atomic::AtomicBool>,
        Arc<AtomicUsize>,
    ) {
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let calls = Arc::new(AtomicUsize::new(0));
        let source = Silent {
            silent: Arc::clone(&flag),
            calls: Arc::clone(&calls),
        };
        let creds = ProfileCredentials {
            hooks: Hooks {
                sources: vec![("Fake", SharedCredentialsProvider::new(source))],
                ..Hooks::default()
            },
            ..creds("t", files(FAKE_PROFILE))
        };
        (creds, flag, calls)
    }

    #[tokio::test(start_paused = true)]
    async fn a_silent_credential_source_times_out() {
        let (creds, _) = silent_profile();
        let start = tokio::time::Instant::now();
        let err = creds.get_credential().await.expect_err("times out");
        assert_eq!(start.elapsed(), Duration::from_secs(30));
        let text = err.to_string();
        assert!(text.contains("`t`") && text.contains("timed out"), "{text}");
    }

    #[tokio::test(start_paused = true)]
    async fn queued_callers_are_bounded_by_their_own_deadline() {
        let (creds, flag) = silent_profile();
        let start = tokio::time::Instant::now();
        let caller = |delay: Duration| {
            let creds = &creds;
            async move {
                tokio::time::sleep(delay).await;
                let result = creds.get_credential().await;
                (result, start.elapsed())
            }
        };
        let ((a, a_at), (b, b_at)) =
            tokio::join!(caller(Duration::ZERO), caller(Duration::from_secs(10)));
        for (result, at, expected) in [(a, a_at, 30), (b, b_at, 40)] {
            let text = result.expect_err("times out").to_string();
            assert!(text.contains("timed out"), "{text}");
            assert_eq!(at, Duration::from_secs(expected));
        }

        // The silent lookup is joined until it is stale; then a new one runs.
        flag.store(false, Ordering::SeqCst);
        tokio::time::sleep(STALE_LOOKUP).await;
        let got = creds.get_credential().await.expect("recovers");
        assert_eq!(got.key_id, "AKIDLATE");
    }

    #[tokio::test(start_paused = true)]
    async fn a_timed_out_lookup_is_joined_not_started_again() {
        let (creds, flag, calls) = counted_silent_profile();
        for _ in 0..2 {
            let err = creds.get_credential().await.expect_err("times out");
            assert!(err.to_string().contains("timed out"), "{err}");
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // A lookup nobody finished is not joined for ever.
        flag.store(false, Ordering::SeqCst);
        tokio::time::sleep(STALE_LOOKUP).await;
        let got = creds.get_credential().await.expect("new lookup");
        assert_eq!(got.key_id, "AKIDLATE");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// A provider whose first credential has 4 minutes left (usable, not
    /// fresh) and whose later lookups behave as `outcome`.
    async fn almost_expired(outcome: Outcome) -> (Fake, ProfileCredentials, Arc<AwsCredential>) {
        let mut fake = Fake::new("AKIDFAKE", Some(SystemTime::now() + Duration::from_mins(4)));
        fake.outcome = outcome;
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let first = creds.get_credential().await.expect("first");
        assert_eq!(fake.calls(), 1);
        (fake, creds, first)
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_refresh_serves_the_usable_credential_and_backs_off() {
        let (fake, creds, first) = almost_expired(Outcome::FailLater).await;
        let got = creds.get_credential().await.expect("served from cache");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 2);

        // No second attempt before the back-off ends.
        tokio::time::sleep(Duration::from_secs(29)).await;
        let got = creds.get_credential().await.expect("still cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 2);

        tokio::time::sleep(Duration::from_secs(1)).await;
        let got = creds.get_credential().await.expect("still cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hanging_refresh_does_not_hold_back_other_callers() {
        let (fake, creds, first) = almost_expired(Outcome::HangLater).await;
        let start = tokio::time::Instant::now();
        let other = async {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let got = creds.get_credential().await.expect("cached");
            (got, start.elapsed())
        };
        let (refreshing, (got, at)) = tokio::join!(creds.get_credential(), other);
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(at, Duration::from_secs(1));
        // The refresh caller gives up at its deadline and also gets the cache.
        assert!(Arc::ptr_eq(&first, &refreshing.expect("cached")));
        assert_eq!(start.elapsed(), LOOKUP_TIMEOUT);
        assert_eq!(fake.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_hanging_refresh_is_retried_before_the_fallback_runs_out() {
        let (fake, creds, first) = almost_expired(Outcome::HangLater).await;
        let start = tokio::time::Instant::now();
        let got = creds.get_credential().await.expect("cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 2);

        // The abandoned lookup is not joined; the back-off ends, a new one runs.
        tokio::time::sleep(RETRY_BACKOFF).await;
        let got = creds.get_credential().await.expect("cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 3);
        assert!(start.elapsed() < Duration::from_mins(4));
    }

    #[tokio::test(start_paused = true)]
    async fn a_dropped_refresh_is_abandoned_and_started_again() {
        let (fake, creds, first) = almost_expired(Outcome::HangLater).await;
        // The starter goes away mid-lookup.
        tokio::time::timeout(Duration::from_secs(1), creds.get_credential())
            .await
            .expect_err("cancelled");
        assert_eq!(fake.calls(), 2);

        // Within the lookup timeout the lookup is left alone.
        let got = creds.get_credential().await.expect("cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 2);

        // After it, a later caller abandons it and starts a new one.
        tokio::time::sleep(LOOKUP_TIMEOUT).await;
        let got = creds.get_credential().await.expect("cached");
        assert!(Arc::ptr_eq(&first, &got));
        assert_eq!(fake.calls(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn an_expired_credential_is_not_served_after_a_failed_refresh() {
        let mut fake = Fake::new("AKIDFAKE", Some(SystemTime::now() - Duration::from_secs(1)));
        fake.outcome = Outcome::FailLater;
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        creds.get_credential().await.expect("first");
        let err = creds.get_credential().await.expect_err("expired");
        let text = format!("{err} {err:?}");
        assert!(text.contains("`t`"), "{text}");
        assert!(!text.contains("TOPSECRET"), "{text}");
        assert_eq!(fake.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_credential_with_under_a_minute_left_is_not_served_after_a_failure() {
        let mut fake = Fake::new("AKIDFAKE", Some(SystemTime::now() + MIN_LEFT / 2));
        fake.outcome = Outcome::FailLater;
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        creds.get_credential().await.expect("first");
        creds.get_credential().await.expect_err("too little left");
    }

    #[tokio::test]
    async fn the_session_token_is_kept() {
        let fake = Fake::new("AKIDFAKE", None);
        let creds = with_fake("t", FAKE_PROFILE, "Fake", &fake);
        let got = creds.get_credential().await.expect("credential");
        assert_eq!(got.token.as_deref(), Some("faketoken"));
        let debug = format!("{creds:?}");
        assert!(
            !debug.contains("fakesecret") && !debug.contains("faketoken"),
            "{debug}"
        );
    }

    #[tokio::test]
    async fn an_explicit_credential_source_is_honoured() {
        let config = "[profile env]\ncredential_source = Environment\n";
        let fake = Fake::new("AKIDENV", None);
        let creds = with_fake("env", config, "Environment", &fake);
        let got = creds.get_credential().await.expect("credential");
        assert_eq!(got.key_id, "AKIDENV");
        assert_eq!(fake.calls(), 1);

        // A source with nothing to give is an error, never another identity.
        let mut empty = Fake::new("AKIDENV", None);
        empty.outcome = Outcome::NotLoaded;
        let creds = with_fake("env", config, "Environment", &empty);
        let err = creds.get_credential().await.expect_err("nothing loaded");
        assert!(err.to_string().contains("`env`"), "{err}");
        assert_eq!(empty.calls(), 1);
    }

    #[tokio::test]
    async fn credential_source_with_role_arn_assumes_the_role() {
        let config = "[profile envrole]\nrole_arn = arn:aws:iam::123456789012:role/r\n\
                      credential_source = Environment\nregion = eu-west-3\n";
        let fake = Fake::new("AKIDENV", None);
        let (client, seen) = recording(&sts_reply("AssumeRole"));
        let creds = ProfileCredentials {
            hooks: Hooks {
                http_client: Some(client),
                sources: vec![("Environment", SharedCredentialsProvider::new(fake.clone()))],
            },
            ..creds("envrole", files(config))
        };
        let got = creds.get_credential().await.expect("assumed role");
        assert_eq!(got.key_id, "ASIAROLE");
        assert_eq!(fake.calls(), 1);
        let seen = seen.lock().expect("lock");
        assert_eq!(seen.len(), 1);
        assert!(
            seen[0].authorization.contains("Credential=AKIDENV/"),
            "{}",
            seen[0].authorization
        );
    }

    #[test]
    fn the_debug_output_of_a_profile_shows_only_its_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");
        std::fs::write(&path, "[profile dev]\naws_secret_access_key = TOPSECRET\n").expect("write");
        let vars = Vars::from_pairs(&[("AWS_CONFIG_FILE", path.to_str().expect("utf8"))]);
        let profile = AwsProfile::new("dev".into(), &vars);
        let text = format!("{profile:?} {:?}", profile.credentials());
        assert!(
            text.contains("dev") && !text.contains("TOPSECRET"),
            "{text}"
        );
        assert!(!text.contains("config"), "{text}");
    }

    // ---- profile settings (AWS-12) ----

    #[test]
    fn settings_follow_source_profile_and_read_the_s3_endpoint() {
        let read = |config: &str| settings("p", Some(files(config)), false).expect("settings");

        let got = read(
            "[profile p]\nsource_profile = base\nendpoint_url = https://global.example\n\
             [profile base]\nregion = eu-north-1\n",
        );
        assert_eq!(got.region.as_deref(), Some("eu-north-1"));
        assert_eq!(got.s3_endpoint.as_deref(), Some("https://global.example"));

        // The services form wins over the global endpoint_url.
        let got = read(
            "[profile p]\nregion = r-1\nservices = local\nendpoint_url = https://global.example\n\
             [services local]\ns3 =\n  endpoint_url = http://127.0.0.1:8\n",
        );
        assert_eq!(got.s3_endpoint.as_deref(), Some("http://127.0.0.1:8"));
        assert_eq!(got.region.as_deref(), Some("r-1"));

        // The profile can switch configured endpoints off.
        let got = read(
            "[profile p]\nignore_configured_endpoint_urls = true\n\
             endpoint_url = https://global.example\n",
        );
        assert_eq!(got.s3_endpoint, None);

        // So can the caller (AWS_IGNORE_CONFIGURED_ENDPOINT_URLS).
        let got = settings(
            "p",
            Some(files(
                "[profile p]\nendpoint_url = https://global.example\n",
            )),
            true,
        )
        .expect("settings");
        assert_eq!(got.s3_endpoint, None);
    }

    #[test]
    fn a_source_profile_cycle_ends() {
        let got = settings(
            "a",
            Some(files(
                "[profile a]\nsource_profile = b\n[profile b]\nsource_profile = a\n",
            )),
            false,
        )
        .expect("settings");
        assert_eq!(got.region, None);
    }

    #[test]
    fn an_unparsable_file_is_an_error_naming_the_profile() {
        let err = settings("dev", Some(files("[profile dev\nregion = x\n")), false)
            .expect_err("unparsable");
        assert!(err.to_string().contains("`dev`"), "{err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn settings_complete_in_one_poll_inside_a_runtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("config");
        std::fs::write(&path, "[profile dev]\nregion = eu-west-3\n").expect("write");
        let files = EnvConfigFiles::builder()
            .include_default_config_file(false)
            .include_default_credentials_file(false)
            .with_file(EnvConfigFileKind::Config, &path)
            .build();
        let got = settings("dev", Some(files), false).expect("one poll is enough");
        assert_eq!(got.region.as_deref(), Some("eu-west-3"));
    }
}
