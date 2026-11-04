//! AWS IAM Identity Center (SSO) device login.
//!
//! [`start`] registers a public client and asks for a device code, so the GUI
//! can show the user code; [`finish`] polls until the user approves and writes
//! the token to the cache the AWS CLI and the SDK share, where `aws_profile.rs`
//! finds it on the next call.

use crate::client::Error;
use crate::{Result, hex, text};
use aws_lc_rs::digest::{SHA1_FOR_LEGACY_USE_ONLY, digest};
use aws_sdk_ssooidc::config::{BehaviorVersion, Region};
use aws_sdk_ssooidc::error::SdkError;
use aws_sdk_ssooidc::operation::create_token::CreateTokenOutput;
use chrono::{DateTime, Utc};
use std::fmt;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::time::Instant;

const DEVICE_GRANT: &str = "urn:ietf:params:oauth:grant-type:device_code";
/// `slow_down` adds this much to the polling interval (RFC 8628, 3.5).
const SLOW_DOWN: Duration = Duration::from_secs(5);
/// The cap on provider text in an error.
const ERROR_TEXT_MAX: usize = 300;

/// What a device login needs to know about an SSO profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SsoLogin {
    /// The name of the `sso-session` section, or the start URL of a legacy
    /// profile: the AWS CLI hashes it to name the token cache file.
    pub identifier: String,
    pub start_url: String,
    pub region: String,
    /// The `sso_registration_scopes` of the session; empty for a legacy
    /// profile.
    pub scopes: Vec<String>,
}

/// A started device login, waiting for the user to approve it.
pub struct Pending {
    /// The code the user types at [`Pending::url`] if the page does not
    /// already carry it.
    pub user_code: String,
    /// The page to approve the login on (with the code filled in when the
    /// service provides that).
    pub url: String,
    client: aws_sdk_ssooidc::Client,
    login: SsoLogin,
    device_code: String,
    client_id: String,
    client_secret: String,
    registration_expires: DateTime<Utc>,
    interval: Duration,
    deadline: Instant,
}

// Hand-written: the device code and the client secret must not be printed.
impl fmt::Debug for Pending {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pending")
            .field("user_code", &self.user_code)
            .field("url", &self.url)
            .field("login", &self.login)
            .finish_non_exhaustive()
    }
}

/// `<home>/.aws/sso/cache`, where the AWS CLI keeps SSO tokens.
pub fn default_cache_dir(host: &super::Host) -> Option<PathBuf> {
    Some(host.home()?.join(".aws/sso/cache"))
}

/// Registers the client and starts the device authorization.
pub async fn start(login: &SsoLogin) -> Result<Pending> {
    start_at(login, None).await
}

/// [`start`] against another SSO OIDC endpoint (tests).
async fn start_at(login: &SsoLogin, endpoint: Option<&str>) -> Result<Pending> {
    let mut config = aws_sdk_ssooidc::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new(login.region.clone()));
    if let Some(endpoint) = endpoint {
        config = config.endpoint_url(endpoint);
    }
    let client = aws_sdk_ssooidc::Client::from_conf(config.build());

    let mut register = client
        .register_client()
        .client_name("osc")
        .client_type("public");
    for scope in &login.scopes {
        register = register.scopes(scope);
    }
    let registered = register
        .send()
        .await
        .map_err(|e| failure("registering the client", &e))?;
    let (Some(client_id), Some(client_secret)) = (registered.client_id, registered.client_secret)
    else {
        return Err(Error::Generic(
            "SSO login failed: the service returned no client".into(),
        ));
    };
    let registration_expires = DateTime::from_timestamp(registered.client_secret_expires_at, 0)
        .ok_or_else(|| Error::Generic("SSO login failed: bad client expiry".into()))?;

    let started = client
        .start_device_authorization()
        .client_id(&client_id)
        .client_secret(&client_secret)
        .start_url(&login.start_url)
        .send()
        .await
        .map_err(|e| failure("starting the device authorization", &e))?;
    let (Some(device_code), Some(user_code), Some(url)) = (
        started.device_code,
        started.user_code,
        started
            .verification_uri_complete
            .or(started.verification_uri),
    ) else {
        return Err(Error::Generic(
            "SSO login failed: the service returned no device code".into(),
        ));
    };

    Ok(Pending {
        user_code,
        url,
        client,
        login: login.clone(),
        device_code,
        client_id,
        client_secret,
        registration_expires,
        interval: Duration::from_secs(u64::try_from(started.interval).unwrap_or(0).max(1)),
        deadline: Instant::now()
            + Duration::from_secs(u64::try_from(started.expires_in).unwrap_or(0)),
    })
}

/// Waits for the user to approve the login and writes the token into
/// `cache_dir` (see [`default_cache_dir`]).
pub async fn finish(mut pending: Pending, cache_dir: &Path) -> Result<()> {
    let token = poll(&mut pending).await?;
    let Some(access_token) = token.access_token else {
        return Err(Error::Generic(
            "SSO login failed: the service returned no token".into(),
        ));
    };
    let now = Utc::now();
    let mut cached = serde_json::json!({
        "startUrl": pending.login.start_url,
        "region": pending.login.region,
        "accessToken": access_token,
        "expiresAt": timestamp(now + Duration::from_secs(u64::try_from(token.expires_in).unwrap_or(0))),
        "clientId": pending.client_id,
        "clientSecret": pending.client_secret,
        "registrationExpiresAt": timestamp(pending.registration_expires),
    });
    if let Some(refresh_token) = token.refresh_token {
        cached["refreshToken"] = refresh_token.into();
    }
    write_cache(
        cache_dir,
        &pending.login.identifier,
        cached.to_string().as_bytes(),
    )?;
    Ok(())
}

/// Calls `CreateToken` every interval until it succeeds, fails for good, or the
/// device code expires.
async fn poll(pending: &mut Pending) -> Result<CreateTokenOutput> {
    loop {
        if Instant::now() >= pending.deadline {
            return Err(Error::Generic(
                "SSO login timed out: the login was not approved in time".into(),
            ));
        }
        tokio::time::sleep(pending.interval).await;
        let sent = pending
            .client
            .create_token()
            .grant_type(DEVICE_GRANT)
            .client_id(&pending.client_id)
            .client_secret(&pending.client_secret)
            .device_code(&pending.device_code)
            .send()
            .await;
        let err = match sent {
            Ok(token) => return Ok(token),
            Err(err) => err,
        };
        match err.as_service_error() {
            Some(e) if e.is_authorization_pending_exception() => {}
            Some(e) if e.is_slow_down_exception() => pending.interval += SLOW_DOWN,
            Some(e) if e.is_expired_token_exception() => {
                return Err(Error::Generic(
                    "SSO login expired: the device code expired before it was approved".into(),
                ));
            }
            Some(e) if e.is_access_denied_exception() => {
                return Err(Error::Denied("the SSO login was denied".into()));
            }
            _ => return Err(failure("waiting for the approval", &err)),
        }
    }
}

/// An error without secrets: only the SDK's error chain, which carries the
/// service's error code and message. The SDK's own debug rendering (the raw
/// response, body included) is deliberately not used.
fn failure<E, R>(what: &str, err: &SdkError<E, R>) -> Error
where
    E: std::error::Error + 'static,
    R: fmt::Debug,
{
    let mut chain = err.to_string();
    let mut source = std::error::Error::source(err);
    while let Some(e) = source {
        let _ = write!(chain, ": {e}");
        source = e.source();
    }
    Error::Generic(format!(
        "SSO login failed while {what}: {}",
        text::escape(&chain, ERROR_TEXT_MAX)
    ))
}

/// `YYYY-MM-DDTHH:MM:SSZ`, the form the SDK's cache reader accepts.
fn timestamp(time: DateTime<Utc>) -> String {
    time.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Writes `<cache_dir>/<hex sha1(identifier)>.json` with mode 0600: to a new
/// temporary file in the same directory, renamed over the target. The rename
/// replaces a symlink at the target instead of writing through it.
fn write_cache(cache_dir: &Path, identifier: &str, contents: &[u8]) -> std::io::Result<()> {
    create_private_dir(cache_dir)?;
    let name = hex::encode(digest(&SHA1_FOR_LEGACY_USE_ONLY, identifier.as_bytes()).as_ref());
    let target = cache_dir.join(format!("{name}.json"));
    let temp = cache_dir.join(format!(".{name}.{}.tmp", std::process::id()));
    let written = create_private_file(&temp)
        .and_then(|mut file| file.write_all(contents).and_then(|()| file.sync_all()))
        .and_then(|()| std::fs::rename(&temp, &target));
    if written.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    written
}

fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    let mut builder = std::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
    builder.create(dir)
}

fn create_private_file(path: &Path) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::backend::stub::{self, Reply};
    use serde_json::Value;

    const REGISTERED: &str = r#"{"clientId":"cid","clientSecret":"csecret","clientIdIssuedAt":1767225600,"clientSecretExpiresAt":1893456000}"#;
    const TOKEN: &str = r#"{"accessToken":"atoken","tokenType":"Bearer","expiresIn":28800,"refreshToken":"rtoken"}"#;

    fn login() -> SsoLogin {
        SsoLogin {
            identifier: "my-sso".into(),
            start_url: "https://example.awsapps.com/start".into(),
            region: "eu-west-1".into(),
            scopes: vec!["sso:account:access".into()],
        }
    }

    fn started(interval: u32) -> Reply {
        Reply::new(
            200,
            format!(
                r#"{{"deviceCode":"dcode","userCode":"ABCD-EFGH","verificationUri":"https://device.sso.example/","verificationUriComplete":"https://device.sso.example/?user_code=ABCD-EFGH","expiresIn":600,"interval":{interval}}}"#
            ),
        )
    }

    /// SSO OIDC answers an OAuth error with HTTP 400, an OAuth-style body and
    /// the modelled exception name in `x-amzn-errortype`. The SDK takes the
    /// code from that header (or the body's `code` / `__type`), not from the
    /// body's `error` (aws-sdk-ssooidc `json_errors.rs`, `parse_error_metadata`).
    fn oauth_error(oauth: &str, exception: &'static str) -> Reply {
        Reply::new(
            400,
            format!(r#"{{"error":"{oauth}","error_description":"stub"}}"#),
        )
        .header("x-amzn-errortype", exception)
    }

    fn pending() -> Reply {
        oauth_error("authorization_pending", "AuthorizationPendingException")
    }

    /// Starts a login against a stub that answers `polls` to `CreateToken`.
    async fn started_login(polls: Vec<Reply>) -> (stub::Stub, Pending) {
        let mut replies = vec![Reply::new(200, REGISTERED), started(0)];
        replies.extend(polls);
        let stub = stub::serve(replies).await;
        let mut pending = start_at(&login(), Some(&stub.base)).await.unwrap();
        pending.interval = Duration::ZERO;
        (stub, pending)
    }

    /// The AWS CLI's name for the cache file of `my-sso` (`sha1sum`).
    fn cache_file(dir: &Path) -> PathBuf {
        dir.join("cache/0ad374308c5a4e22f723adf10145eafad7c4031c.json")
    }

    #[tokio::test]
    async fn start_registers_and_returns_the_code() {
        let stub = stub::serve(vec![Reply::new(200, REGISTERED), started(0)]).await;
        let pending = start_at(&login(), Some(&stub.base)).await.unwrap();
        assert_eq!(pending.user_code, "ABCD-EFGH");
        assert_eq!(
            pending.url,
            "https://device.sso.example/?user_code=ABCD-EFGH"
        );
        assert_eq!(pending.interval, Duration::from_secs(1), "at least 1 s");

        let requests = stub.requests();
        assert!(requests[0].line.starts_with("POST /client/register "));
        let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
        assert_eq!(body["clientName"], "osc");
        assert_eq!(body["clientType"], "public");
        assert_eq!(body["scopes"][0], "sso:account:access");
        assert!(requests[1].line.starts_with("POST /device_authorization "));
        let body: Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(body["startUrl"], "https://example.awsapps.com/start");
        assert_eq!(body["clientSecret"], "csecret");
    }

    #[tokio::test]
    async fn debug_hides_the_secrets() {
        let (_stub, pending) = started_login(vec![]).await;
        let shown = format!("{pending:?}");
        assert!(shown.contains("ABCD-EFGH"));
        for secret in ["csecret", "dcode", "cid"] {
            assert!(!shown.contains(secret), "{secret} leaked: {shown}");
        }
    }

    #[tokio::test]
    async fn finish_writes_the_token_cache() {
        let dir = tempfile::tempdir().unwrap();
        let (stub, pending) = started_login(vec![pending(), Reply::new(200, TOKEN)]).await;
        finish(pending, &dir.path().join("cache")).await.unwrap();

        let polls: Vec<_> = stub.requests().split_off(2);
        assert_eq!(polls.len(), 2);
        assert!(polls[0].line.starts_with("POST /token "));
        let body: Value = serde_json::from_slice(&polls[1].body).unwrap();
        assert_eq!(body["grantType"], DEVICE_GRANT);
        assert_eq!(body["deviceCode"], "dcode");
        assert_eq!(body["clientId"], "cid");

        let cached: Value =
            serde_json::from_slice(&std::fs::read(cache_file(dir.path())).unwrap()).unwrap();
        let object = cached.as_object().unwrap();
        assert_eq!(object.len(), 8, "{cached}");
        assert_eq!(cached["startUrl"], "https://example.awsapps.com/start");
        assert_eq!(cached["region"], "eu-west-1");
        assert_eq!(cached["accessToken"], "atoken");
        assert_eq!(cached["clientId"], "cid");
        assert_eq!(cached["clientSecret"], "csecret");
        assert_eq!(cached["refreshToken"], "rtoken");
        assert_eq!(cached["registrationExpiresAt"], "2030-01-01T00:00:00Z");
        // aws-config parses it as RFC 3339 UTC without an offset
        // (`DateTime::from_str(.., Format::DateTime)`, sso/cache.rs).
        let expires = cached["expiresAt"].as_str().unwrap();
        let parsed = DateTime::parse_from_rfc3339(expires).unwrap();
        assert!(expires.ends_with('Z') && expires.len() == 20, "{expires}");
        let left = (parsed.to_utc() - Utc::now()).num_seconds();
        assert!((28_700..=28_800).contains(&left), "{left}");
    }

    #[tokio::test]
    async fn refresh_token_is_optional() {
        let dir = tempfile::tempdir().unwrap();
        let token = r#"{"accessToken":"atoken","tokenType":"Bearer","expiresIn":60}"#;
        let (_stub, pending) = started_login(vec![Reply::new(200, token)]).await;
        finish(pending, &dir.path().join("cache")).await.unwrap();
        let cached: Value =
            serde_json::from_slice(&std::fs::read(cache_file(dir.path())).unwrap()).unwrap();
        assert!(cached.get("refreshToken").is_none());
    }

    #[tokio::test]
    async fn slow_down_adds_five_seconds_to_the_interval() {
        let (stub, mut pending) =
            started_login(vec![oauth_error("slow_down", "SlowDownException")]).await;
        pending.interval = Duration::from_millis(1);
        // The wait after the slow-down is longer than 5 s: stop there and look.
        let waited = tokio::time::timeout(Duration::from_millis(500), poll(&mut pending)).await;
        assert!(waited.is_err(), "still polling");
        assert_eq!(pending.interval, Duration::from_millis(1) + SLOW_DOWN);
        assert_eq!(stub.requests().len(), 3);
    }

    #[tokio::test]
    async fn polling_stops_at_the_deadline() {
        let (stub, mut pending) = started_login(vec![pending()]).await;
        pending.deadline = Instant::now();
        let err = finish(pending, Path::new("/nonexistent"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("timed out"), "{err}");
        assert_eq!(stub.requests().len(), 2, "no CreateToken call");
    }

    #[tokio::test]
    async fn start_falls_back_to_the_plain_uri_and_uses_the_interval() {
        let body = r#"{"deviceCode":"d","userCode":"U","verificationUri":"https://device.sso.example/","expiresIn":600,"interval":7}"#;
        let stub = stub::serve(vec![Reply::new(200, REGISTERED), Reply::new(200, body)]).await;
        let pending = start_at(&login(), Some(&stub.base)).await.unwrap();
        assert_eq!(pending.url, "https://device.sso.example/");
        assert_eq!(pending.interval, Duration::from_secs(7));
    }

    #[tokio::test]
    async fn a_legacy_login_registers_without_scopes() {
        let stub = stub::serve(vec![Reply::new(200, REGISTERED), started(0)]).await;
        let legacy = SsoLogin {
            scopes: Vec::new(),
            ..login()
        };
        start_at(&legacy, Some(&stub.base)).await.unwrap();
        let body: Value = serde_json::from_slice(&stub.requests()[0].body).unwrap();
        assert!(body.get("scopes").is_none(), "{body}");
    }

    #[tokio::test]
    async fn secrets_in_a_malformed_answer_stay_out_of_errors() {
        let bad_register =
            r#"{"clientId":"cid","clientSecret":"csecret","clientSecretExpiresAt":"oops"}"#;
        let stub = stub::serve(vec![Reply::new(200, bad_register)]).await;
        let err = start_at(&login(), Some(&stub.base)).await.unwrap_err();
        assert!(!err.to_string().contains("csecret"), "{err}");

        let bad_token = r#"{"accessToken":"atoken","refreshToken":"rtoken","expiresIn":"oops"}"#;
        let (_stub, pending) = started_login(vec![Reply::new(200, bad_token)]).await;
        let err = finish(pending, Path::new("/nonexistent"))
            .await
            .unwrap_err()
            .to_string();
        for secret in ["atoken", "rtoken", "csecret"] {
            assert!(!err.contains(secret), "{err}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn a_failed_write_leaves_no_temp_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = dir.path().join("cache");
        std::fs::create_dir_all(cache_file(dir.path())).unwrap();
        assert!(write_cache(&cache, "my-sso", b"{}").is_err());
        let names: Vec<_> = std::fs::read_dir(&cache)
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["0ad374308c5a4e22f723adf10145eafad7c4031c.json"]);
    }

    #[tokio::test]
    async fn expired_code_fails_without_a_file() {
        let dir = tempfile::tempdir().unwrap();
        let (_stub, pending) = started_login(vec![oauth_error(
            "expired_token",
            "ExpiredTokenException:http://internal.amazon.com/coral/",
        )])
        .await;
        let err = finish(pending, &dir.path().join("cache"))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("expired"), "{err}");
        assert!(!dir.path().join("cache").exists());
    }

    #[tokio::test]
    async fn denied_login_is_reported() {
        let dir = tempfile::tempdir().unwrap();
        let (_stub, pending) =
            started_login(vec![oauth_error("access_denied", "AccessDeniedException")]).await;
        let err = finish(pending, dir.path()).await.unwrap_err();
        assert!(matches!(err, Error::Denied(_)), "{err}");
    }

    #[tokio::test]
    async fn other_failures_name_the_case_without_secrets() {
        let dir = tempfile::tempdir().unwrap();
        let (_stub, pending) =
            started_login(vec![oauth_error("invalid_grant", "InvalidGrantException")]).await;
        let err = finish(pending, dir.path()).await.unwrap_err().to_string();
        assert!(err.contains("InvalidGrant"), "{err}");
        for secret in ["csecret", "dcode"] {
            assert!(!err.contains(secret), "{err}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cache_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let (_stub, pending) = started_login(vec![Reply::new(200, TOKEN)]).await;
        finish(pending, &dir.path().join("cache")).await.unwrap();
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode(&dir.path().join("cache")), 0o700);
        assert_eq!(mode(&cache_file(dir.path())), 0o600);
        let entries = std::fs::read_dir(dir.path().join("cache")).unwrap().count();
        assert_eq!(entries, 1, "no temp file left behind");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_symlink_at_the_target_is_replaced_not_followed() {
        let dir = tempfile::tempdir().unwrap();
        let victim = dir.path().join("victim");
        std::fs::write(&victim, "keep").unwrap();
        std::fs::create_dir(dir.path().join("cache")).unwrap();
        std::os::unix::fs::symlink(&victim, cache_file(dir.path())).unwrap();

        let (_stub, pending) = started_login(vec![Reply::new(200, TOKEN)]).await;
        finish(pending, &dir.path().join("cache")).await.unwrap();

        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "keep");
        let meta = std::fs::symlink_metadata(cache_file(dir.path())).unwrap();
        assert!(meta.is_file());
    }
}
