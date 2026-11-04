//! Azure connections: the environment and the accounts of the Azure CLI.
//!
//! Two phases: [`subscriptions`] reads `azureProfile.json` locally, and
//! [`discover_azure`] asks the Azure Resource Manager (ARM) for the storage
//! accounts of each subscription, with a token from `az`.

use super::{Connection, Discovery, Host, Login, Provider, Source};
use crate::backend::{read_listing, request_failed};
use std::ffi::OsStr;
use std::time::Duration;
use url::Url;

const LOGIN: &str = "az login";

/// How long `az` may take to print a token.
const AZ_TIMEOUT: Duration = Duration::from_secs(30);

/// A subscription to list the storage accounts of.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscription {
    /// A validated GUID: it is passed to `az` and put in a URL.
    id: String,
    name: String,
    /// The ARM base URL of the subscription's cloud.
    arm: &'static str,
}

/// The environment connection, when an account name is set. Only
/// `AZURE_STORAGE_ACCOUNT_NAME` counts: the store does not read the `az` CLI's
/// shorter `AZURE_STORAGE_ACCOUNT`.
pub(super) fn env(host: &Host) -> Option<Connection> {
    host.any(&["AZURE_STORAGE_ACCOUNT_NAME"])
        .then(|| Connection::env("azure-env", "Azure · environment", Provider::Azure))
}

/// The enabled subscriptions of the Azure CLI profile that sit in the default
/// subscription's tenant (the tenant `object_store`'s `az` credential uses).
/// Subscriptions of other tenants, clouds this crate does not know and a
/// profile that cannot be read become notes. No profile is no result.
pub(super) fn subscriptions(host: &Host, out: &mut Discovery) -> Vec<Subscription> {
    let Some(dir) = host
        .var("AZURE_CONFIG_DIR")
        .map(|d| host.path(&d))
        .or_else(|| host.home().map(|h| h.join(".azure")))
    else {
        return Vec::new();
    };
    let Ok(bytes) = std::fs::read(dir.join("azureProfile.json")) else {
        return Vec::new();
    };
    // `az` writes a UTF-8 byte order mark.
    let bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(&bytes);
    let Ok(profile) = serde_json::from_slice::<serde_json::Value>(bytes) else {
        out.notes
            .push("Azure: azureProfile.json is not valid JSON".into());
        return Vec::new();
    };
    let all = profile["subscriptions"]
        .as_array()
        .map_or(&[][..], Vec::as_slice);
    let text = |s: &'static str, v: &serde_json::Value| v[s].as_str().map(str::to_owned);
    let Some(tenant) = all
        .iter()
        .find(|s| s["isDefault"].as_bool() == Some(true))
        .and_then(|s| text("tenantId", s))
    else {
        if !all.is_empty() {
            out.notes.push(
                "Azure: no default subscription; run `az account set --subscription <id>`".into(),
            );
        }
        return Vec::new();
    };

    let mut found = Vec::new();
    let mut elsewhere = 0;
    for sub in all
        .iter()
        .filter(|s| s["state"].as_str() == Some("Enabled"))
    {
        let (Some(id), Some(name)) = (text("id", sub), text("name", sub)) else {
            continue;
        };
        if !is_guid(&id) {
            continue;
        }
        if text("tenantId", sub).as_deref() != Some(&tenant) {
            elsewhere += 1;
            continue;
        }
        let cloud = sub["environmentName"].as_str().unwrap_or("AzureCloud");
        let Some(arm) = arm_base(cloud) else {
            out.notes.push(format!(
                "Azure subscription {name}: the cloud {cloud:?} is not supported"
            ));
            continue;
        };
        found.push(Subscription { id, name, arm });
    }
    if elsewhere > 0 {
        out.notes.push(format!(
            "Azure: {elsewhere} subscription(s) in other tenants skipped; \
             `az account set --subscription <id>` to use them"
        ));
    }
    found
}

/// The ARM base URL of an `az` cloud name.
fn arm_base(cloud: &str) -> Option<&'static str> {
    Some(match cloud {
        "AzureCloud" => "https://management.azure.com",
        "AzureChinaCloud" => "https://management.chinacloudapi.cn",
        "AzureUSGovernment" => "https://management.usgovcloudapi.net",
        _ => return None,
    })
}

/// Whether `id` is a GUID in the 8-4-4-4-12 form.
fn is_guid(id: &str) -> bool {
    id.len() == 36
        && id.char_indices().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == '-',
            _ => c.is_ascii_hexdigit(),
        })
}

/// The storage accounts of `subscriptions`, one connection each, sorted by
/// label. Any failure (no `az`, not logged in, no permission) becomes a note.
pub async fn discover_azure(subscriptions: &[Subscription]) -> Discovery {
    accounts(subscriptions, None, async |id| {
        az_token(OsStr::new("az"), id).await
    })
    .await
}

/// `az account get-access-token` for `subscription`, run without a shell
/// (`cmd /C` on Windows, where `az` is a script) through `program` (`az`, or a
/// stand-in in tests). The error text never holds the token or `az`'s output.
async fn az_token(program: &OsStr, subscription: String) -> Result<String, String> {
    let mut command = if cfg!(windows) {
        let mut command = tokio::process::Command::new("cmd");
        command.arg("/C").arg(program);
        command
    } else {
        tokio::process::Command::new(program)
    };
    command
        .args(["account", "get-access-token", "--subscription"])
        .arg(subscription)
        .args(["--output", "json"])
        .kill_on_drop(true);
    let output = match tokio::time::timeout(AZ_TIMEOUT, command.output()).await {
        Err(_) => return Err("`az` timed out".into()),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => {
            return Err("`az` is not installed".into());
        }
        Ok(Err(_)) => return Err("`az` could not be run".into()),
        Ok(Ok(output)) => output,
    };
    if !output.status.success() {
        return Err(format!("`az` could not get a token; run `{LOGIN}`"));
    }
    serde_json::from_slice::<serde_json::Value>(&output.stdout)
        .ok()
        .and_then(|v| v["accessToken"].as_str().map(str::to_owned))
        .ok_or_else(|| "`az` printed no access token".into())
}

/// [`discover_azure`] with its seams: `arm_override` replaces every ARM base
/// URL (and allows plain `http`), `token` gets the ARM token of a subscription.
async fn accounts(
    subscriptions: &[Subscription],
    arm_override: Option<&str>,
    token: impl AsyncFn(String) -> Result<String, String>,
) -> Discovery {
    let mut out = Discovery::default();
    if subscriptions.is_empty() {
        return out;
    }
    let http = match crate::backend::http_client(arm_override.is_some()) {
        Ok(http) => http,
        Err(e) => {
            out.notes.push(format!("Azure: {e}"));
            return out;
        }
    };
    for sub in subscriptions {
        let note = |why: &str| format!("Azure subscription {}: {why}", sub.name);
        let token = match token(sub.id.clone()).await {
            Ok(token) => token,
            Err(why) => {
                out.notes.push(note(&why));
                continue;
            }
        };
        let first = format!(
            "subscriptions/{}/providers/Microsoft.Storage/storageAccounts?api-version=2023-05-01",
            sub.id
        );
        let Ok(base) = Url::parse(arm_override.unwrap_or(sub.arm)) else {
            out.notes.push(note("the ARM address is invalid"));
            continue;
        };
        let Ok(first) = base.join(&first) else {
            out.notes.push(note("the ARM address is invalid"));
            continue;
        };
        let mut notes = Vec::new();
        for (account, endpoint) in list_accounts(&http, &base, first, &token, &mut notes).await {
            out.connections.push(Connection {
                id: format!("azure-cli:{}/{account}", sub.id),
                label: format!("Azure · {account} ({})", sub.name),
                provider: Provider::Azure,
                source: Source::AzureCli { account, endpoint },
                login: Some(Login::Command(LOGIN)),
            });
        }
        out.notes.extend(notes.iter().map(|why| note(why)));
    }
    out.connections.sort_by(|a, b| a.label.cmp(&b.label));
    out
}

/// The `(name, https blob endpoint)` of every account, following `nextLink`
/// only to the origin of `base` (the token goes nowhere else). Problems go to
/// `notes`; what was listed before one is kept.
async fn list_accounts(
    http: &reqwest::Client,
    base: &Url,
    first: Url,
    token: &str,
    notes: &mut Vec<String>,
) -> Vec<(String, String)> {
    let mut found = Vec::new();
    let mut seen = std::collections::HashSet::new();
    let mut next = Some(first);
    while let Some(url) = next.take() {
        if !seen.insert(url.clone()) {
            notes.push("the storage account pages repeat; the listing was stopped".into());
            break;
        }
        let what = "listing the storage accounts";
        let body = match http.get(url).bearer_auth(token).send().await {
            Ok(response) => read_listing(response, what).await,
            Err(e) => Err(request_failed("Listing the storage accounts", e)),
        };
        let body = match body {
            Ok(body) => body,
            Err(e) => {
                notes.push(e.to_string());
                break;
            }
        };
        let Ok(page) = serde_json::from_slice::<serde_json::Value>(&body) else {
            notes.push("ARM sent an unreadable answer".into());
            break;
        };
        for account in page["value"].as_array().map_or(&[][..], Vec::as_slice) {
            let name = account["name"].as_str().filter(|n| !n.is_empty());
            let blob = account["properties"]["primaryEndpoints"]["blob"]
                .as_str()
                .filter(|b| Url::parse(b).is_ok_and(|u| u.scheme() == "https"));
            if let (Some(name), Some(blob)) = (name, blob) {
                found.push((name.to_owned(), blob.to_owned()));
            }
        }
        if let Some(link) = page["nextLink"].as_str().filter(|l| !l.is_empty()) {
            match base.join(link) {
                Ok(url) if url.origin() == base.origin() => next = Some(url),
                _ => notes.push("a page link pointing elsewhere was not followed".into()),
            }
        }
    }
    found
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::tests::{host, write};
    use super::*;
    use crate::backend::stub::{Reply, serve};

    const TENANT: &str = "11111111-1111-1111-1111-111111111111";
    const SUB_A: &str = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";
    const SUB_B: &str = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb";

    fn sub(id: &str, name: &str, state: &str, tenant: &str, default: bool) -> String {
        format!(
            r#"{{"id":"{id}","name":"{name}","state":"{state}","tenantId":"{tenant}","isDefault":{default},"environmentName":"AzureCloud"}}"#
        )
    }

    fn profile(dir: &std::path::Path, subs: &[String]) {
        write(
            &dir.join(".azure/azureProfile.json"),
            &format!("\u{feff}{{\"subscriptions\":[{}]}}", subs.join(",")),
        );
    }

    fn scan(dir: &std::path::Path) -> (Vec<Subscription>, Discovery) {
        let mut out = Discovery::default();
        let subs = subscriptions(&host(dir, &[]), &mut out);
        (subs, out)
    }

    #[test]
    fn the_profile_gives_enabled_subscriptions_of_the_default_tenant() {
        let dir = tempfile::tempdir().expect("tempdir");
        profile(
            dir.path(),
            &[
                sub(SUB_A, "Prod", "Enabled", TENANT, true),
                sub(SUB_B, "Dev", "Enabled", TENANT, false),
                sub(
                    "cccccccc-cccc-cccc-cccc-cccccccccccc",
                    "Old",
                    "Disabled",
                    TENANT,
                    false,
                ),
                sub(
                    "dddddddd-dddd-dddd-dddd-dddddddddddd",
                    "Other",
                    "Enabled",
                    "22222222",
                    false,
                ),
                sub(
                    "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
                    "Other2",
                    "Enabled",
                    "22222222",
                    false,
                ),
                sub("not a guid; rm -rf /", "Bad", "Enabled", TENANT, false),
            ],
        );
        let (subs, out) = scan(dir.path());
        let ids: Vec<_> = subs
            .iter()
            .map(|s| (s.id.as_str(), s.name.as_str()))
            .collect();
        assert_eq!(ids, [(SUB_A, "Prod"), (SUB_B, "Dev")]);
        assert_eq!(out.notes.len(), 1);
        assert!(
            out.notes[0].starts_with("Azure: 2 subscription(s) in other tenants"),
            "{:?}",
            out.notes
        );
        assert!(out.notes[0].contains("az account set"));
    }

    #[test]
    fn an_unknown_cloud_is_a_note() {
        let dir = tempfile::tempdir().expect("tempdir");
        profile(
            dir.path(),
            &[
                sub(SUB_A, "Far", "Enabled", TENANT, true)
                    .replace("AzureCloud", "AzureGermanCloud"),
            ],
        );
        let (subs, out) = scan(dir.path());
        assert_eq!(subs, Vec::new());
        assert!(out.notes[0].contains("AzureGermanCloud"), "{:?}", out.notes);
    }

    #[test]
    fn a_broken_profile_is_a_note() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(&dir.path().join(".azure/azureProfile.json"), "{");
        let (subs, out) = scan(dir.path());
        assert!(subs.is_empty() && out.notes.len() == 1);
    }

    #[test]
    fn azure_config_dir_moves_the_profile() {
        let dir = tempfile::tempdir().expect("tempdir");
        let custom = dir.path().join("az");
        write(
            &custom.join("azureProfile.json"),
            &format!(
                "{{\"subscriptions\":[{}]}}",
                sub(SUB_A, "P", "Enabled", TENANT, true)
            ),
        );
        let mut out = Discovery::default();
        let host = host(
            dir.path(),
            &[("AZURE_CONFIG_DIR", custom.to_str().expect("utf8"))],
        );
        assert_eq!(subscriptions(&host, &mut out).len(), 1);
    }

    #[test]
    fn guids_are_checked_strictly() {
        assert!(is_guid(SUB_A) && is_guid("AbCdEf01-2345-6789-abcd-ef0123456789"));
        for bad in [
            "",
            "x",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa",
            "aaaaaaaa_aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "gaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "--output",
        ] {
            assert!(!is_guid(bad), "{bad}");
        }
    }

    #[test]
    fn the_environment_connection_needs_an_account_name() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(env(&host(dir.path(), &[])).is_none());
        assert!(env(&host(dir.path(), &[("AZURE_STORAGE_ACCOUNT", "a")])).is_none());
        assert!(env(&host(dir.path(), &[("AZURE_STORAGE_ACCOUNT_NAME", "a")])).is_some());
    }

    fn subscription(id: &str, name: &str) -> Subscription {
        Subscription {
            id: id.into(),
            name: name.into(),
            arm: "https://management.azure.com",
        }
    }

    async fn token(_: String) -> Result<String, String> {
        Ok("SECRET-TOKEN".into())
    }

    fn page(accounts: &[(&str, &str)], next: Option<&str>) -> String {
        let value: Vec<_> = accounts
            .iter()
            .map(|(n, b)| {
                format!(r#"{{"name":"{n}","properties":{{"primaryEndpoints":{{"blob":"{b}"}}}}}}"#)
            })
            .collect();
        let next = next.map_or(String::new(), |n| format!(r#","nextLink":"{n}""#));
        format!(r#"{{"value":[{}]{next}}}"#, value.join(","))
    }

    #[tokio::test]
    async fn accounts_are_paged_and_sorted_and_non_https_endpoints_skipped() {
        let first = page(
            &[
                ("zeta", "https://zeta.blob.core.windows.net/"),
                ("plain", "http://plain.blob.core.windows.net/"),
            ],
            Some("/page2?token=x"),
        );
        let second = page(&[("alpha", "https://alpha.blob.core.windows.net/")], None);
        let stub = serve(vec![Reply::new(200, first), Reply::new(200, second)]).await;
        let found = accounts(&[subscription(SUB_A, "Prod")], Some(&stub.base), token).await;
        assert!(found.notes.is_empty(), "{:?}", found.notes);
        let got: Vec<_> = found
            .connections
            .iter()
            .map(|c| (c.id.clone(), c.label.clone(), c.source.clone()))
            .collect();
        assert_eq!(
            got,
            [
                (
                    format!("azure-cli:{SUB_A}/alpha"),
                    "Azure · alpha (Prod)".to_owned(),
                    Source::AzureCli {
                        account: "alpha".into(),
                        endpoint: "https://alpha.blob.core.windows.net/".into()
                    }
                ),
                (
                    format!("azure-cli:{SUB_A}/zeta"),
                    "Azure · zeta (Prod)".to_owned(),
                    Source::AzureCli {
                        account: "zeta".into(),
                        endpoint: "https://zeta.blob.core.windows.net/".into()
                    }
                ),
            ]
        );
        assert_eq!(found.connections[0].login, Some(Login::Command("az login")));
        let requests = stub.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].line.starts_with(&format!(
            "GET /subscriptions/{SUB_A}/providers/Microsoft.Storage/storageAccounts?api-version=2023-05-01 "
        )), "{}", requests[0].line);
        assert!(requests[1].line.starts_with("GET /page2?token=x "));
        assert!(
            requests[0]
                .headers
                .to_ascii_lowercase()
                .contains("authorization: bearer secret-token")
        );
        assert!(!format!("{found:?}").contains("SECRET-TOKEN"));
    }

    #[tokio::test]
    async fn a_next_link_to_another_host_is_refused_and_the_token_stays_home() {
        let foreign = serve(vec![Reply::new(200, page(&[], None))]).await;
        let link = format!("{}/steal", foreign.base);
        let body = page(&[("a", "https://a.blob.core.windows.net/")], Some(&link));
        let stub = serve(vec![Reply::new(200, body)]).await;
        let found = accounts(&[subscription(SUB_A, "Prod")], Some(&stub.base), token).await;
        assert_eq!(found.connections.len(), 1);
        assert_eq!(found.notes.len(), 1);
        assert!(found.notes[0].contains("not followed"), "{:?}", found.notes);
        assert!(foreign.requests().is_empty());
        assert!(!found.notes[0].contains("SECRET"));
    }

    #[tokio::test]
    async fn repeating_page_links_stop_the_listing() {
        let one = page(&[("a", "https://a.blob.core.windows.net/")], Some("/p2"));
        let two = page(&[("b", "https://b.blob.core.windows.net/")], Some("/p2"));
        let stub = serve(vec![Reply::new(200, one), Reply::new(200, two)]).await;
        let found = accounts(&[subscription(SUB_A, "Prod")], Some(&stub.base), token).await;
        assert_eq!(found.connections.len(), 2);
        assert_eq!(stub.requests().len(), 2);
        assert!(found.notes[0].contains("repeat"), "{:?}", found.notes);
    }

    #[tokio::test]
    async fn failures_become_notes_without_the_token() {
        let stub = serve(vec![Reply::new(403, "AuthorizationFailed")]).await;
        let found = accounts(
            &[subscription(SUB_A, "Prod"), subscription(SUB_B, "Dev")],
            Some(&stub.base),
            |id: String| async move {
                if id == SUB_B {
                    Err("`az` could not get a token; run `az login`".to_owned())
                } else {
                    Ok("SECRET-TOKEN".to_owned())
                }
            },
        )
        .await;
        assert_eq!(found.connections, Vec::new());
        assert_eq!(
            found.notes,
            [
                "Azure subscription Prod: Access denied: listing the storage accounts was denied \
                 (HTTP 403 Forbidden): AuthorizationFailed",
                "Azure subscription Dev: `az` could not get a token; run `az login`",
            ]
        );
    }

    #[tokio::test]
    async fn no_subscriptions_means_no_requests_and_an_unreachable_arm_is_a_note() {
        assert_eq!(accounts(&[], None, token).await, Discovery::default());
        let base = format!("http://127.0.0.1:{}", crate::backend::stub::closed_port());
        let found = accounts(&[subscription(SUB_A, "Prod")], Some(&base), token).await;
        assert_eq!(found.notes.len(), 1);
        assert!(
            found.notes[0].starts_with("Azure subscription Prod: Generic error: Listing the storage accounts to http://127.0.0.1:"),
            "{:?}",
            found.notes
        );
        assert!(found.notes[0].ends_with("failed: error sending request"));
    }

    /// Writes an executable `az` stand-in and returns its path.
    #[cfg(unix)]
    fn fake_az(dir: &std::path::Path, script: &str) -> std::path::PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join("az");
        write(&path, &format!("#!/bin/sh\n{script}\n"));
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    /// [`az_token`] on a script just written: a child that another test
    /// thread forked while the script was open for writing keeps it busy
    /// (ETXTBSY) until that child execs, so a failed spawn is tried again.
    #[cfg(unix)]
    async fn run_fake(az: &std::path::Path) -> Result<String, String> {
        for _ in 0..50 {
            match az_token(az.as_os_str(), SUB_A.into()).await {
                Err(e) if e == "`az` could not be run" => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                done => return done,
            }
        }
        Err("`az` stayed busy".into())
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn az_is_asked_for_the_subscription_token_without_a_shell() {
        let dir = tempfile::tempdir().unwrap();
        let args = dir.path().join("args");
        let az = fake_az(
            dir.path(),
            &format!(
                "printf '%s\\n' \"$@\" > {}\necho '{{\"accessToken\":\"T\"}}'",
                args.display()
            ),
        );
        assert_eq!(run_fake(&az).await, Ok("T".into()));
        let args = std::fs::read_to_string(args).unwrap();
        assert_eq!(
            args.lines().collect::<Vec<_>>(),
            [
                "account",
                "get-access-token",
                "--subscription",
                SUB_A,
                "--output",
                "json"
            ]
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn az_failures_never_show_its_output() {
        let dir = tempfile::tempdir().unwrap();
        let az = fake_az(dir.path(), "echo SECRET; echo SECRET >&2; exit 1");
        let err = run_fake(&az).await.unwrap_err();
        assert!(err.contains("az login") && !err.contains("SECRET"), "{err}");
        let missing = dir.path().join("nonexistent");
        assert_eq!(
            az_token(missing.as_os_str(), SUB_A.into()).await,
            Err("`az` is not installed".into())
        );
    }
}
