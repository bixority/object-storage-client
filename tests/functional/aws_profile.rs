//! End-to-end AWS profile tests: the `osc` binary in a cleared environment
//! against loopback stand-ins for the SSO portal, STS and S3, plus an
//! instance-metadata canary that must never be contacted.

use crate::support::Osc;
use crate::support::mock::{Mock, Request, Response};
use std::fmt::Write as _;
use std::io::Read;
use std::path::Path;
use std::process::{Output, Stdio};
use std::time::{Duration, Instant};

const CACHED_TOKEN: &str = "cached-token";
const REGION: &str = "eu-west-3";
const SSO_SESSION_CONFIG: &str = "\
[profile dev]
sso_session = corp
sso_account_id = 123456789012
sso_role_name = Developer
region = eu-west-3

[sso-session corp]
sso_start_url = https://corp.example/start
sso_region = eu-west-1
";
const LEGACY_SSO_CONFIG: &str = "\
[profile dev]
sso_start_url = https://legacy.example/start
sso_region = eu-west-1
sso_account_id = 123456789012
sso_role_name = Developer
region = eu-west-3
";

const PORTAL_BODY: &str = r#"{"roleCredentials":{"accessKeyId":"ASIASSO","secretAccessKey":"ssosecret","sessionToken":"sso-tok","expiration":4102444800000}}"#;
const STS_BODY: &str = "<AssumeRoleResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">\
<AssumeRoleResult><Credentials><AccessKeyId>ASIAROLE</AccessKeyId>\
<SecretAccessKey>rolesecret</SecretAccessKey><SessionToken>role-tok</SessionToken>\
<Expiration>2099-01-01T00:00:00Z</Expiration></Credentials>\
<AssumedRoleUser><AssumedRoleId>AROAX:osc</AssumedRoleId>\
<Arn>arn:aws:sts::123456789012:assumed-role/Chain/osc</Arn></AssumedRoleUser>\
</AssumeRoleResult><ResponseMetadata><RequestId>r</RequestId></ResponseMetadata>\
</AssumeRoleResponse>";

/// Every secret value the mocks hand out or the files hold; none may reach
/// the child's output.
const SECRETS: [&str; 8] = [
    CACHED_TOKEN,
    "ssosecret",
    "sso-tok",
    "rolesecret",
    "role-tok",
    "envsecret",
    "procsecret",
    "proc-tok",
];

/// The stand-in services, and the child's view of them.
struct Rig {
    portal: Mock,
    sts: Mock,
    s3: Mock,
    imds: Mock,
}

impl Rig {
    fn new() -> Self {
        Self {
            portal: Mock::start(|_| Response::json(PORTAL_BODY)),
            sts: Mock::start(|_| Response::xml(STS_BODY)),
            s3: Mock::start(|_| Response {
                status: 200,
                headers: vec![
                    ("Content-Length", "3".into()),
                    ("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT".into()),
                    ("ETag", "\"abc\"".into()),
                ],
                body: String::new(),
            }),
            imds: Mock::canary(),
        }
    }

    /// The child: nothing but its own empty `HOME` and these endpoints.
    fn osc(&self) -> Osc {
        let mut osc = Osc::new();
        osc.env("AWS_ENDPOINT_URL_SSO", self.portal.url())
            .env("AWS_ENDPOINT_URL_STS", self.sts.url())
            .env("AWS_ENDPOINT_URL_S3", self.s3.url())
            .env("S3_ALLOW_HTTP", "true")
            .env("AWS_EC2_METADATA_SERVICE_ENDPOINT", self.imds.url())
            .env("AWS_METADATA_ENDPOINT", self.imds.url());
        osc
    }

    /// The canary never saw anything, not even a connection.
    fn assert_no_imds(&self) {
        assert_eq!(self.imds.connections(), 0, "instance metadata contacted");
        assert!(self.imds.requests().is_empty());
    }
}

fn write_aws_file(osc: &Osc, name: &str, text: &str) {
    let dir = osc.home().join(".aws");
    std::fs::create_dir_all(&dir).expect("mkdir .aws");
    std::fs::write(dir.join(name), text).expect("write aws file");
}

/// The SSO token cache entry `aws-config` looks up: the file is named after
/// the SHA-1 of the `sso-session` name, or of the start URL in the legacy form.
fn write_sso_cache(home: &Path, identifier: &str, expires_at: &str) {
    let digest = aws_lc_rs::digest::digest(
        &aws_lc_rs::digest::SHA1_FOR_LEGACY_USE_ONLY,
        identifier.as_bytes(),
    );
    let name = digest.as_ref().iter().fold(String::new(), |mut hex, b| {
        let _ = write!(hex, "{b:02x}");
        hex
    });
    let dir = home.join(".aws/sso/cache");
    std::fs::create_dir_all(&dir).expect("mkdir cache");
    let json = format!(r#"{{"accessToken":"{CACHED_TOKEN}","expiresAt":"{expires_at}"}}"#);
    std::fs::write(dir.join(format!("{name}.json")), json).expect("write cache");
}

/// Runs the child to the end. Fails, instead of hanging, after 30 s.
fn run(mut osc: Osc, args: &[&str]) -> Output {
    osc.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = osc.spawn().expect("spawn osc");
    // Drained on threads so a full pipe cannot stall the child.
    let mut out_pipe = child.stdout.take().expect("stdout");
    let mut err_pipe = child.stderr.take().expect("stderr");
    let out_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = out_pipe.read_to_end(&mut v);
        v
    });
    let err_reader = std::thread::spawn(move || {
        let mut v = Vec::new();
        let _ = err_pipe.read_to_end(&mut v);
        v
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    let status = loop {
        if let Some(status) = child.try_wait().expect("try_wait") {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("osc did not finish within 30 s");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    Output {
        status,
        stdout: out_reader.join().expect("stdout reader"),
        stderr: err_reader.join().expect("stderr reader"),
    }
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// `osc --profile <profile> exists s3://bkt/k` succeeded and printed `true`,
/// and neither stream carries a secret.
fn assert_exists_ok(out: &Output) {
    let (so, se) = (text(&out.stdout), text(&out.stderr));
    assert_eq!(out.status.code(), Some(0), "stderr: {se}");
    assert_eq!(so.trim(), "true", "stderr: {se}");
    assert_no_secrets(out);
}

fn assert_no_secrets(out: &Output) {
    let all = format!("{}{}", text(&out.stdout), text(&out.stderr));
    for secret in SECRETS {
        assert!(!all.contains(secret), "`{secret}` leaked: {all}");
    }
}

/// Every S3 request is signed with `key` in `region` and carries `token`.
fn assert_s3_signed(requests: &[Request], key: &str, region: &str, token: &str) {
    assert!(!requests.is_empty(), "no S3 request was made");
    for r in requests {
        let scope = r.credential_scope().expect("signed request");
        assert!(scope.starts_with(&format!("{key}/")), "{scope}");
        assert!(
            scope.ends_with(&format!("/{region}/s3/aws4_request")),
            "{scope}"
        );
        assert_eq!(r.header("x-amz-security-token"), Some(token));
    }
}

/// The portal saw exactly one lookup, made with the cached login token.
fn assert_one_portal_lookup(rig: &Rig) {
    let seen = rig.portal.requests();
    assert_eq!(seen.len(), 1, "{seen:?}");
    let r = &seen[0];
    assert!(
        r.line.starts_with("GET /federation/credentials?"),
        "{}",
        r.line
    );
    assert!(r.line.contains("account_id=123456789012"), "{}", r.line);
    assert!(r.line.contains("role_name=Developer"), "{}", r.line);
    assert_eq!(r.header("x-amz-sso_bearer_token"), Some(CACHED_TOKEN));
}

fn sso_profile_signs_with_the_role_key(config: &str, cache_identifier: &str) {
    let rig = Rig::new();
    let osc = rig.osc();
    write_aws_file(&osc, "config", config);
    write_sso_cache(osc.home(), cache_identifier, "2099-01-01T00:00:00Z");
    let out = run(osc, &["--profile", "dev", "exists", "s3://bkt/k"]);
    assert_exists_ok(&out);
    assert_one_portal_lookup(&rig);
    assert_s3_signed(&rig.s3.requests(), "ASIASSO", REGION, "sso-tok");
    assert!(rig.sts.requests().is_empty());
    rig.assert_no_imds();
}

#[test]
fn sso_session_profile_signs_with_the_role_credentials() {
    sso_profile_signs_with_the_role_key(SSO_SESSION_CONFIG, "corp");
}

#[test]
fn legacy_sso_profile_signs_with_the_role_credentials() {
    sso_profile_signs_with_the_role_key(LEGACY_SSO_CONFIG, "https://legacy.example/start");
}

#[test]
fn sso_then_role_assumes_the_role_in_the_profile_region() {
    let rig = Rig::new();
    let osc = rig.osc();
    write_aws_file(
        &osc,
        "config",
        &format!(
            "{SSO_SESSION_CONFIG}
[profile chained]
role_arn = arn:aws:iam::123456789012:role/Chain
source_profile = dev
region = eu-north-1
"
        ),
    );
    write_sso_cache(osc.home(), "corp", "2099-01-01T00:00:00Z");
    let out = run(osc, &["--profile", "chained", "exists", "s3://bkt/k"]);
    assert_exists_ok(&out);
    assert_one_portal_lookup(&rig);

    let sts = rig.sts.requests();
    assert_eq!(sts.len(), 1, "{sts:?}");
    let scope = sts[0].credential_scope().expect("signed STS request");
    assert!(scope.starts_with("ASIASSO/"), "{scope}");
    assert!(scope.ends_with("/eu-north-1/sts/aws4_request"), "{scope}");
    assert_eq!(sts[0].header("x-amz-security-token"), Some("sso-tok"));
    assert!(sts[0].body.contains("Action=AssumeRole"), "{}", sts[0].body);
    assert!(
        sts[0].body.contains("role%2FChain"),
        "wrong role: {}",
        sts[0].body
    );

    assert_s3_signed(&rig.s3.requests(), "ASIAROLE", "eu-north-1", "role-tok");
    rig.assert_no_imds();
}

#[test]
fn an_environment_sourced_role_is_assumed_with_the_environment_key() {
    let rig = Rig::new();
    let mut osc = rig.osc();
    osc.env("AWS_ACCESS_KEY_ID", "AKIDENVBASE")
        .env("AWS_SECRET_ACCESS_KEY", "envsecret");
    write_aws_file(
        &osc,
        "config",
        "[profile envrole]
role_arn = arn:aws:iam::123456789012:role/Env
credential_source = Environment
region = eu-west-3
",
    );
    let out = run(osc, &["--profile", "envrole", "exists", "s3://bkt/k"]);
    assert_exists_ok(&out);

    let sts = rig.sts.requests();
    assert_eq!(sts.len(), 1, "{sts:?}");
    let scope = sts[0].credential_scope().expect("signed STS request");
    assert!(scope.starts_with("AKIDENVBASE/"), "{scope}");
    assert!(scope.ends_with("/eu-west-3/sts/aws4_request"), "{scope}");
    assert!(sts[0].body.contains("role%2FEnv"), "{}", sts[0].body);

    // The role key signs S3; the environment key never does.
    assert_s3_signed(&rig.s3.requests(), "ASIAROLE", REGION, "role-tok");
    assert!(rig.portal.requests().is_empty());
    rig.assert_no_imds();
}

#[cfg(unix)]
#[test]
fn a_credential_process_profile_signs_with_the_helper_credentials() {
    let rig = Rig::new();
    let osc = rig.osc();
    let script = osc.home().join("helper.sh");
    std::fs::write(
        &script,
        "printf '%s' '{\"Version\":1,\"AccessKeyId\":\"ASIAPROC\",\
         \"SecretAccessKey\":\"procsecret\",\"SessionToken\":\"proc-tok\"}'\n",
    )
    .expect("script");
    write_aws_file(
        &osc,
        "config",
        &format!(
            "[profile dev]\ncredential_process = /bin/sh {}\nregion = eu-west-3\n",
            script.display()
        ),
    );
    let out = run(osc, &["--profile", "dev", "exists", "s3://bkt/k"]);
    assert_exists_ok(&out);
    assert_s3_signed(&rig.s3.requests(), "ASIAPROC", REGION, "proc-tok");
    assert!(rig.portal.requests().is_empty() && rig.sts.requests().is_empty());
    rig.assert_no_imds();
}

#[test]
fn an_sso_profile_without_a_login_fails_with_the_login_hint() {
    // No cache file at all, then an expired one.
    for cache in [None, Some("2000-01-01T00:00:00Z")] {
        let rig = Rig::new();
        let osc = rig.osc();
        write_aws_file(&osc, "config", SSO_SESSION_CONFIG);
        // A secret in another profile must not show up in the error.
        write_aws_file(
            &osc,
            "credentials",
            "[other]\naws_access_key_id = AKIDOTHER\naws_secret_access_key = FILESECRET\n",
        );
        if let Some(expires_at) = cache {
            write_sso_cache(osc.home(), "corp", expires_at);
        }
        let out = run(osc, &["--profile", "dev", "exists", "s3://bkt/k"]);
        let se = text(&out.stderr);
        assert_eq!(out.status.code(), Some(1), "{cache:?}: {se}");
        assert!(se.contains("`dev`"), "{cache:?}: {se}");
        assert!(
            se.contains("aws sso login --profile dev"),
            "{cache:?}: {se}"
        );
        assert!(!se.contains("FILESECRET") && !text(&out.stdout).contains("FILESECRET"));
        assert_no_secrets(&out);
        assert!(rig.portal.requests().is_empty(), "{cache:?}");
        assert!(rig.s3.requests().is_empty(), "{cache:?}");
        rig.assert_no_imds();
    }
}

#[test]
fn a_missing_profile_fails_without_imds() {
    let rig = Rig::new();
    let osc = rig.osc();
    write_aws_file(&osc, "config", SSO_SESSION_CONFIG);
    let out = run(osc, &["--profile", "nope", "exists", "s3://bkt/k"]);
    let se = text(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "{se}");
    assert!(se.contains("profile `nope` was not defined"), "{se}");
    assert!(rig.s3.requests().is_empty());
    rig.assert_no_imds();
}
