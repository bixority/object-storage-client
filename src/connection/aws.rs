//! AWS connections: the environment and the profiles in `~/.aws`.

use super::{Connection, Discovery, Host, Login, Provider, Source, SsoLogin};
use aws_config::profile::ProfileSet;
use aws_runtime::env_config::file::EnvConfigFiles;
use aws_types::os_shim_internal::Fs;
use futures::FutureExt;
use std::path::PathBuf;

/// The scope the AWS CLI registers a session with when it names none.
const DEFAULT_SCOPE: &str = "sso:account:access";

/// The keys that give a profile a credential source in the AWS SDK; a profile
/// with none of them fails with "no credentials found", as with the AWS CLI.
const CREDENTIAL_KEYS: [&str; 13] = [
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "credential_process",
    "credential_source",
    "login_session",
    "role_arn",
    "web_identity_token_file",
    "sso_session",
    "sso_start_url",
    "sso_account_id",
    "sso_role_name",
    "sso_region",
];

/// The environment connection, when the variables hold credentials or an
/// endpoint.
pub(super) fn env(host: &Host) -> Option<Connection> {
    host.any(&[
        "AWS_ACCESS_KEY_ID",
        "S3_ACCESS_KEY_ID",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "S3_ENDPOINT",
        "AWS_ENDPOINT_URL",
        "AWS_ENDPOINT_URL_S3",
    ])
    .then(|| Connection::env("aws-env", "AWS · environment", Provider::S3))
}

/// One connection per profile of the config and credentials files, sorted by
/// name. The files are those the AWS CLI reads (`AWS_CONFIG_FILE` and
/// `AWS_SHARED_CREDENTIALS_FILE` honoured). A profile without credentials
/// (only settings such as `region`) is left out and named in a note.
pub(super) fn profiles(host: &Host, out: &mut Discovery) {
    // The loader only does blocking file reads, so one poll completes it.
    let loaded =
        aws_config::profile::load(&Fs::real(), &host.env, &EnvConfigFiles::default(), None)
            .now_or_never();
    let set = match loaded {
        Some(Ok(set)) => set,
        Some(Err(e)) => {
            out.notes
                .push(format!("AWS profiles could not be read: {e}"));
            return;
        }
        None => {
            out.notes
                .push("AWS profiles could not be read: the loader did not finish".into());
            return;
        }
    };
    let file = |var: &str, default: &str| -> Option<PathBuf> {
        let path = host.var(var).map_or_else(
            || host.home().map(|home| home.join(".aws").join(default)),
            |v| Some(host.path(&v)),
        )?;
        path.is_file().then_some(path)
    };
    let config = file("AWS_CONFIG_FILE", "config");
    let credentials = file("AWS_SHARED_CREDENTIALS_FILE", "credentials");

    let mut names: Vec<&str> = set.profiles().collect();
    names.sort_unstable();
    let mut without = Vec::new();
    for name in names {
        let Some(profile) = set.get_profile(name) else {
            continue;
        };
        if !CREDENTIAL_KEYS.iter().any(|key| profile.get(key).is_some()) {
            without.push(name);
            continue;
        }
        let sso = profile.get("sso_session").is_some() || profile.get("sso_start_url").is_some();
        let hint = if sso {
            " · SSO"
        } else if profile.get("role_arn").is_some() {
            " · role"
        } else if profile.get("credential_process").is_some() {
            " · process"
        } else if profile.get("aws_access_key_id").is_some() {
            " · keys"
        } else {
            ""
        };
        out.connections.push(Connection {
            id: format!("aws-profile:{name}"),
            label: format!("AWS · {name}{hint}"),
            provider: Provider::S3,
            source: Source::AwsProfile {
                name: name.to_owned(),
                config: config.clone(),
                credentials: credentials.clone(),
            },
            login: sso_login(&set, name).map(Login::AwsSso),
        });
    }
    if !without.is_empty() {
        out.notes.push(format!(
            "AWS profiles without credentials: {}; add keys or SSO settings to use them",
            without.join(", ")
        ));
    }
}

/// The device-login settings of an SSO profile, or of the SSO profile a role
/// profile takes its credentials from (`source_profile`, a few hops at most):
/// from its `sso-session` section when it names one, else (legacy) from the
/// profile itself.
fn sso_login(set: &ProfileSet, name: &str) -> Option<SsoLogin> {
    let mut profile = set.get_profile(name)?;
    for _ in 0..4 {
        if profile.get("sso_session").is_some() || profile.get("sso_start_url").is_some() {
            break;
        }
        profile = set.get_profile(profile.get("source_profile")?)?;
    }
    if let Some(session_name) = profile.get("sso_session") {
        let session = set.sso_session(session_name)?;
        let scopes: Vec<String> = session
            .get("sso_registration_scopes")
            .map(|s| {
                s.split(|c: char| c == ',' || c.is_whitespace())
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .filter(|s: &Vec<String>| !s.is_empty())
            .unwrap_or_else(|| vec![DEFAULT_SCOPE.to_owned()]);
        return Some(SsoLogin {
            identifier: session_name.to_owned(),
            start_url: session.get("sso_start_url")?.to_owned(),
            region: session.get("sso_region")?.to_owned(),
            scopes,
        });
    }
    let start_url = profile.get("sso_start_url")?;
    Some(SsoLogin {
        identifier: start_url.to_owned(),
        start_url: start_url.to_owned(),
        region: profile.get("sso_region")?.to_owned(),
        scopes: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::super::tests::{host, write};
    use super::*;

    const CONFIG: &str = "\
[default]
region = us-east-1
aws_access_key_id = AKIDDEFAULT
aws_secret_access_key = topsecret

[profile sso]
sso_session = corp
sso_account_id = 123456789012
sso_role_name = Admin

[sso-session corp]
sso_start_url = https://corp.awsapps.com/start
sso_region = eu-west-1
sso_registration_scopes = sso:account:access, extra:scope

[profile bare-session]
sso_session = bare

[sso-session bare]
sso_start_url = https://bare.awsapps.com/start
sso_region = us-west-2

[profile legacy]
sso_start_url = https://old.awsapps.com/start
sso_region = us-east-2
sso_account_id = 123456789012
sso_role_name = Admin

[profile role]
role_arn = arn:aws:iam::123456789012:role/x
source_profile = default

[profile role-on-sso]
role_arn = arn:aws:iam::123456789012:role/y
source_profile = sso

[profile proc]
credential_process = /bin/true

[profile minio]
endpoint_url = http://minio.local:9000
aws_access_key_id = minio
aws_secret_access_key = minio123
";

    fn scan(host: &Host) -> Discovery {
        let mut out = Discovery::default();
        profiles(host, &mut out);
        out
    }

    fn find<'a>(found: &'a Discovery, name: &str) -> &'a Connection {
        found
            .connections
            .iter()
            .find(|c| c.id == format!("aws-profile:{name}"))
            .expect(name)
    }

    #[test]
    fn every_profile_kind_is_listed_sorted_with_a_hint() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join(".aws/config"), CONFIG);
        write(
            &dir.path().join(".aws/credentials"),
            "[extra]\naws_access_key_id = AKIDEXTRA\naws_secret_access_key = s\n",
        );
        let found = scan(&host(dir.path(), &[]));
        let labels: Vec<_> = found.connections.iter().map(|c| c.label.as_str()).collect();
        assert_eq!(
            labels,
            [
                "AWS · bare-session · SSO",
                "AWS · default · keys",
                "AWS · extra · keys",
                "AWS · legacy · SSO",
                "AWS · minio · keys",
                "AWS · proc · process",
                "AWS · role · role",
                "AWS · role-on-sso · role",
                "AWS · sso · SSO",
            ]
        );
        assert_eq!(found.notes, Vec::<String>::new());
        let Source::AwsProfile {
            config,
            credentials,
            ..
        } = &find(&found, "extra").source
        else {
            panic!("not a profile");
        };
        assert_eq!(
            config.as_deref(),
            Some(dir.path().join(".aws/config").as_path())
        );
        assert_eq!(
            credentials.as_deref(),
            Some(dir.path().join(".aws/credentials").as_path())
        );
        let shown = format!("{found:?}");
        assert!(!shown.contains("topsecret") && !shown.contains("minio123"));
    }

    #[test]
    fn sso_profiles_carry_their_login_settings() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join(".aws/config"), CONFIG);
        let found = scan(&host(dir.path(), &[]));
        assert_eq!(
            find(&found, "sso").login,
            Some(Login::AwsSso(SsoLogin {
                identifier: "corp".into(),
                start_url: "https://corp.awsapps.com/start".into(),
                region: "eu-west-1".into(),
                scopes: vec!["sso:account:access".into(), "extra:scope".into()],
            }))
        );
        assert_eq!(
            find(&found, "bare-session").login,
            Some(Login::AwsSso(SsoLogin {
                identifier: "bare".into(),
                start_url: "https://bare.awsapps.com/start".into(),
                region: "us-west-2".into(),
                scopes: vec![DEFAULT_SCOPE.into()],
            }))
        );
        assert_eq!(
            find(&found, "legacy").login,
            Some(Login::AwsSso(SsoLogin {
                identifier: "https://old.awsapps.com/start".into(),
                start_url: "https://old.awsapps.com/start".into(),
                region: "us-east-2".into(),
                scopes: Vec::new(),
            }))
        );
        for plain in ["default", "role", "proc", "minio"] {
            assert_eq!(find(&found, plain).login, None, "{plain}");
        }
        assert_eq!(find(&found, "role-on-sso").login, find(&found, "sso").login);
    }

    #[test]
    fn the_file_overrides_are_honoured() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(
            &dir.path().join(".aws/config"),
            "[profile home]\nrole_arn = r\n",
        );
        let custom = dir.path().join("elsewhere/cfg");
        write(&custom, "[profile custom]\nrole_arn = r\n");
        let creds = dir.path().join("elsewhere/creds");
        write(
            &creds,
            "[viacreds]\naws_access_key_id = k\naws_secret_access_key = s\n",
        );
        let host = host(
            dir.path(),
            &[
                ("AWS_CONFIG_FILE", custom.to_str().expect("utf8")),
                ("AWS_SHARED_CREDENTIALS_FILE", "~/elsewhere/creds"),
            ],
        );
        let found = scan(&host);
        let ids: Vec<_> = found.connections.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(ids, ["aws-profile:custom", "aws-profile:viacreds"]);
        let Source::AwsProfile {
            config,
            credentials,
            ..
        } = &found.connections[0].source
        else {
            panic!("not a profile");
        };
        assert_eq!(config.as_deref(), Some(custom.as_path()));
        assert_eq!(credentials.as_deref(), Some(creds.as_path()));
    }

    #[test]
    fn the_environment_connection_needs_a_credential_or_endpoint_variable() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(
            env(&host(
                dir.path(),
                &[("AWS_PROFILE", "x"), ("AWS_REGION", "r")]
            ))
            .is_none()
        );
        assert!(env(&host(dir.path(), &[("S3_ENDPOINT", "")])).is_none());
        for key in [
            "AWS_ACCESS_KEY_ID",
            "S3_ACCESS_KEY_ID",
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "S3_ENDPOINT",
            "AWS_ENDPOINT_URL",
            "AWS_ENDPOINT_URL_S3",
        ] {
            let found = env(&host(dir.path(), &[(key, "v")])).expect(key);
            assert_eq!(
                (found.id.as_str(), found.provider),
                ("aws-env", Provider::S3)
            );
        }
    }

    #[test]
    fn an_unreadable_config_is_a_note() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join(".aws/config"), "[profile x\nfoo");
        let found = scan(&host(dir.path(), &[]));
        assert_eq!(found.connections.len(), 0);
        assert_eq!(found.notes.len(), 1);
        assert!(found.notes[0].starts_with("AWS profiles could not be read"));
    }
}
