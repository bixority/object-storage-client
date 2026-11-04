//! GCS connections: the environment and the gcloud configurations.

use super::{Connection, Discovery, Host, Login, Provider, Source};
use std::path::{Path, PathBuf};

/// What the user runs to create the application default credentials.
const LOGIN: &str = "gcloud auth application-default login";

/// The environment connection, when a variable names credentials or a
/// server.
pub(super) fn env(host: &Host) -> Option<Connection> {
    host.any(&[
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_SERVICE_ACCOUNT",
        "GOOGLE_SERVICE_ACCOUNT_PATH",
        "GOOGLE_SERVICE_ACCOUNT_KEY",
        "GOOGLE_BEARER_TOKEN",
        "GOOGLE_BASE_URL",
        "GCS_BASE_URL",
    ])
    .then(|| Connection::env("gcs-env", "GCS · environment", Provider::Gcs))
}

/// One connection per gcloud configuration with usable credentials, sorted by
/// name. The credentials are the configuration's `credential_file_override`,
/// else the application default credentials of the gcloud directory (both must
/// be `authorized_user` or `service_account` files, the only types the store
/// reads). A configuration without any gets a note naming the login command.
pub(super) fn configurations(host: &Host, out: &mut Discovery) {
    let Some(dir) = gcloud_dir(host) else {
        return;
    };
    let Ok(entries) = std::fs::read_dir(dir.join("configurations")) else {
        return;
    };
    let mut names: Vec<String> = entries
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .filter_map(|f| f.strip_prefix("config_").map(str::to_owned))
        .collect();
    names.sort_unstable();
    let adc = dir.join("application_default_credentials.json");
    let mut without = Vec::new();
    for name in names {
        let text =
            std::fs::read_to_string(dir.join("configurations").join(format!("config_{name}")))
                .unwrap_or_default();
        let project = ini_get(&text, "core", "project");
        let credentials = ini_get(&text, "auth", "credential_file_override")
            .map(|p| host.path(&p))
            .or_else(|| adc.is_file().then(|| adc.clone()));
        let Some(credentials) = credentials else {
            without.push(name);
            continue;
        };
        match credential_type(&credentials) {
            Some(t) if t == "authorized_user" || t == "service_account" => {}
            Some(t) => {
                out.notes.push(format!(
                    "GCS configuration `{name}`: credentials of type {t:?} are not supported \
                     (use authorized_user or service_account)"
                ));
                continue;
            }
            None => {
                out.notes.push(format!(
                    "GCS configuration `{name}`: {} is not a readable credentials file",
                    credentials.display()
                ));
                continue;
            }
        }
        let label = project.as_ref().map_or_else(
            || format!("GCS · {name}"),
            |p| format!("GCS · {name} ({p})"),
        );
        out.connections.push(Connection {
            id: format!("gcs-gcloud:{name}"),
            label,
            provider: Provider::Gcs,
            source: Source::Gcs {
                credentials,
                project,
            },
            login: Some(Login::Command(LOGIN)),
        });
    }
    if !without.is_empty() {
        out.notes.push(format!(
            "GCS: no credentials for gcloud configuration {}; run `{LOGIN}`",
            without.join(", ")
        ));
    }
}

/// `$CLOUDSDK_CONFIG`, else `~/.config/gcloud` (`%APPDATA%\gcloud` on Windows).
fn gcloud_dir(host: &Host) -> Option<PathBuf> {
    if let Some(dir) = host.var("CLOUDSDK_CONFIG") {
        return Some(host.path(&dir));
    }
    if cfg!(windows) {
        return Some(PathBuf::from(host.var("APPDATA")?).join("gcloud"));
    }
    Some(host.home()?.join(".config").join("gcloud"))
}

/// The `type` of a credentials JSON file; `None` when unreadable. The rest of
/// the file (keys, tokens) is dropped unread.
fn credential_type(path: &Path) -> Option<String> {
    let json: serde_json::Value = serde_json::from_slice(&std::fs::read(path).ok()?).ok()?;
    json.get("type")?.as_str().map(str::to_owned)
}

/// The value of `key` in `[section]` of an INI text: `key = value` lines, `#`
/// and `;` comments. The first match wins.
fn ini_get(text: &str, section: &str, key: &str) -> Option<String> {
    let mut current = "";
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            current = name.trim();
        } else if current == section
            && let Some((k, v)) = line.split_once('=')
            && k.trim() == key
            && !v.trim().is_empty()
        {
            return Some(v.trim().to_owned());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::super::tests::{host, write};
    use super::*;

    const ADC: &str = r#"{"type":"authorized_user","client_secret":"hunter2","refresh_token":"r"}"#;

    fn scan(host: &Host) -> Discovery {
        let mut out = Discovery::default();
        configurations(host, &mut out);
        out
    }

    #[test]
    fn the_ini_reader_handles_sections_comments_and_spacing() {
        let text = "# c\n; c\n[core]\nproject=p1\naccount = a\n\n[auth]\n  project = no\n\
                    credential_file_override =  /k.json \n[core]\nproject = later\n";
        assert_eq!(ini_get(text, "core", "project").as_deref(), Some("p1"));
        assert_eq!(
            ini_get(text, "auth", "credential_file_override").as_deref(),
            Some("/k.json")
        );
        assert_eq!(ini_get(text, "auth", "project").as_deref(), Some("no"));
        assert_eq!(ini_get(text, "core", "missing"), None);
        assert_eq!(ini_get(text, "other", "project"), None);
    }

    #[test]
    fn configurations_share_the_adc_file_and_may_override_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gcloud = dir.path().join(".config/gcloud");
        write(&gcloud.join("application_default_credentials.json"), ADC);
        write(
            &gcloud.join("configurations/config_work"),
            "[core]\nproject = work-1\n",
        );
        let key = dir.path().join("sa.json");
        write(&key, r#"{"type":"service_account","private_key":"x"}"#);
        write(
            &gcloud.join("configurations/config_alpha"),
            &format!(
                "[core]\nproject = a-1\n[auth]\ncredential_file_override = {}\n",
                key.display()
            ),
        );
        write(&gcloud.join("configurations/config_noproject"), "[core]\n");
        write(&gcloud.join("configurations/ignored"), "[core]\n");
        let found = scan(&host(dir.path(), &[]));
        assert!(found.notes.is_empty(), "{:?}", found.notes);
        let got: Vec<_> = found
            .connections
            .iter()
            .map(|c| (c.id.as_str(), c.label.as_str(), &c.source))
            .collect();
        let adc = gcloud.join("application_default_credentials.json");
        let source = |credentials: &PathBuf, project: Option<&str>| Source::Gcs {
            credentials: credentials.clone(),
            project: project.map(str::to_owned),
        };
        assert_eq!(
            got,
            [
                (
                    "gcs-gcloud:alpha",
                    "GCS · alpha (a-1)",
                    &source(&key, Some("a-1"))
                ),
                (
                    "gcs-gcloud:noproject",
                    "GCS · noproject",
                    &source(&adc, None)
                ),
                (
                    "gcs-gcloud:work",
                    "GCS · work (work-1)",
                    &source(&adc, Some("work-1"))
                ),
            ]
        );
        assert_eq!(found.connections[0].login, Some(Login::Command(LOGIN)));
        assert!(!format!("{found:?}").contains("hunter2"));
    }

    #[test]
    fn cloudsdk_config_moves_the_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        let custom = dir.path().join("custom");
        write(&custom.join("application_default_credentials.json"), ADC);
        write(
            &custom.join("configurations/config_default"),
            "[core]\nproject = p\n",
        );
        let found = scan(&host(
            dir.path(),
            &[("CLOUDSDK_CONFIG", custom.to_str().expect("utf8"))],
        ));
        assert_eq!(found.connections.len(), 1);
    }

    #[test]
    fn an_unsupported_credentials_type_is_a_note() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gcloud = dir.path().join(".config/gcloud");
        write(
            &gcloud.join("application_default_credentials.json"),
            r#"{"type":"external_account"}"#,
        );
        write(&gcloud.join("configurations/config_default"), "[core]\n");
        let found = scan(&host(dir.path(), &[]));
        assert_eq!(found.connections, Vec::new());
        assert_eq!(found.notes.len(), 1);
        assert!(
            found.notes[0].contains("external_account"),
            "{:?}",
            found.notes
        );
    }

    #[test]
    fn no_credentials_is_a_note_with_the_login_command() {
        let dir = tempfile::tempdir().expect("tempdir");
        let gcloud = dir.path().join(".config/gcloud");
        write(
            &gcloud.join("configurations/config_default"),
            "[core]\nproject = p\n",
        );
        let found = scan(&host(dir.path(), &[]));
        assert_eq!(found.connections, Vec::new());
        assert_eq!(
            found.notes,
            [
                "GCS: no credentials for gcloud configuration default; run `gcloud auth application-default login`"
            ]
        );
    }

    #[test]
    fn the_environment_connection_needs_a_gcs_variable() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(env(&host(dir.path(), &[("GOOGLE_CLOUD_PROJECT", "p")])).is_none());
        for key in [
            "GOOGLE_APPLICATION_CREDENTIALS",
            "GOOGLE_SERVICE_ACCOUNT",
            "GOOGLE_SERVICE_ACCOUNT_PATH",
            "GOOGLE_SERVICE_ACCOUNT_KEY",
            "GOOGLE_BEARER_TOKEN",
            "GOOGLE_BASE_URL",
            "GCS_BASE_URL",
        ] {
            let found = env(&host(dir.path(), &[(key, "v")])).expect(key);
            assert_eq!(
                (found.id.as_str(), found.provider),
                ("gcs-env", Provider::Gcs)
            );
        }
    }
}
