//! The object-storage connections a user already has: AWS profiles, provider
//! environment variables, gcloud configurations and Azure CLI accounts.
//!
//! Discovery reads only local files and variables (names and metadata, never
//! credentials), contacts no provider except the Azure Resource Manager lookup
//! of [`azure::discover_azure`], and never triggers a login. Nothing here holds a
//! secret, so `Debug` is derived.

mod aws;
pub mod azure;
mod gcp;
pub mod sso;

use crate::Result;
use crate::client::{Client, ObjectStorageClient};
use aws_runtime::fs_util::Os;
use aws_types::os_shim_internal::Env;
use std::collections::HashMap;
use std::path::PathBuf;

use azure::Subscription;
use sso::SsoLogin;

/// Which URL schemes a connection serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Provider {
    /// `file://` and `http(s)://`.
    Local,
    S3,
    Gcs,
    Azure,
}

impl Provider {
    /// Whether this provider serves URLs of `scheme`.
    pub const fn serves(self, scheme: &str) -> bool {
        match self {
            Self::Local => matches!(scheme.as_bytes(), b"file" | b"http" | b"https"),
            Self::S3 => matches!(scheme.as_bytes(), b"s3"),
            Self::Gcs => matches!(scheme.as_bytes(), b"gs" | b"gcs"),
            Self::Azure => matches!(
                scheme.as_bytes(),
                b"az" | b"wasb" | b"wasbs" | b"abfs" | b"abfss"
            ),
        }
    }

    /// The location a connection starts at, as address text: the current
    /// directory, or the provider root that lists the buckets.
    pub const fn root(self) -> &'static str {
        match self {
            Self::Local => ".",
            Self::S3 => "s3://",
            Self::Gcs => "gs://",
            Self::Azure => "az://",
        }
    }
}

/// Where a connection's settings come from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// The process environment, as [`ObjectStorageClient::new`] reads it.
    Env,
    /// An AWS profile in the given (existing) config and credentials files.
    AwsProfile {
        name: String,
        config: Option<PathBuf>,
        credentials: Option<PathBuf>,
    },
    Gcs {
        credentials: PathBuf,
        project: Option<String>,
    },
    AzureCli {
        account: String,
        endpoint: String,
    },
}

/// How to repair a connection that cannot authenticate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Login {
    AwsSso(SsoLogin),
    /// A command the user runs in a terminal.
    Command(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connection {
    pub id: String,
    pub label: String,
    pub provider: Provider,
    pub source: Source,
    pub login: Option<Login>,
}

/// What a scan found: connections, and notes for what could not be used.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Discovery {
    pub connections: Vec<Connection>,
    pub notes: Vec<String>,
}

impl Connection {
    /// The always-present local file system and HTTP connection.
    #[must_use]
    pub fn local() -> Self {
        Self::env("local", "Local", Provider::Local)
    }

    fn env(id: &str, label: &str, provider: Provider) -> Self {
        Self {
            id: id.into(),
            label: label.into(),
            provider,
            source: Source::Env,
            login: None,
        }
    }

    /// The settings of an isolated client: only what the connection names, so
    /// the process environment (`S3_ENDPOINT`, `AWS_PROFILE`, ...) cannot leak
    /// in. `None` for [`Source::Env`], which is the process environment.
    fn vars(&self) -> Option<HashMap<String, String>> {
        let mut vars = HashMap::new();
        let mut set = |key: &str, value: String| vars.insert(key.to_owned(), value);
        match &self.source {
            Source::Env => return None,
            Source::AwsProfile {
                config,
                credentials,
                ..
            } => {
                for (key, path) in [
                    ("AWS_CONFIG_FILE", config),
                    ("AWS_SHARED_CREDENTIALS_FILE", credentials),
                ] {
                    if let Some(path) = path {
                        set(key, path.display().to_string());
                    }
                }
            }
            Source::Gcs {
                credentials,
                project,
            } => {
                set(
                    "GOOGLE_APPLICATION_CREDENTIALS",
                    credentials.display().to_string(),
                );
                if let Some(project) = project {
                    set("GOOGLE_CLOUD_PROJECT", project.clone());
                }
            }
            Source::AzureCli { account, endpoint } => {
                set("AZURE_STORAGE_ACCOUNT_NAME", account.clone());
                set("AZURE_STORAGE_ENDPOINT", endpoint.clone());
                set("AZURE_USE_AZURE_CLI", "true".into());
            }
        }
        Some(vars)
    }

    /// A client for this connection. Building it does no I/O and no login.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`](crate::Error::InvalidInput) for an AWS
    /// profile with an empty name.
    pub fn client(&self) -> Result<ObjectStorageClient> {
        let Some(vars) = self.vars() else {
            return Ok(ObjectStorageClient::new());
        };
        let client = Client::isolated(vars);
        match &self.source {
            Source::AwsProfile { name, .. } => client.with_s3_profile(name),
            _ => Ok(client),
        }
    }
}

/// The variables and home directory discovery looks at.
pub struct Host {
    env: Env,
}

impl Host {
    /// The process environment, read variable by variable.
    pub fn real() -> Self {
        Self { env: Env::real() }
    }

    /// A host whose variables are exactly `vars`, so tests never read the
    /// real environment or home directory.
    #[cfg(test)]
    pub(crate) fn from_vars(vars: HashMap<String, String>) -> Self {
        Self {
            env: Env::from(vars),
        }
    }

    /// The value of `key`, if set and not empty.
    fn var(&self, key: &str) -> Option<String> {
        self.env.get(key).ok().filter(|v| !v.is_empty())
    }

    fn any(&self, keys: &[&str]) -> bool {
        keys.iter().any(|k| self.var(k).is_some())
    }

    /// The home directory by the AWS SDK's rule, which its profile loader uses.
    fn home(&self) -> Option<PathBuf> {
        aws_runtime::fs_util::home_dir(&self.env, Os::real()).map(PathBuf::from)
    }

    /// `value` as a path, with a leading `~/` expanded to the home directory.
    fn path(&self, value: &str) -> PathBuf {
        match (value.strip_prefix("~/"), self.home()) {
            (Some(rest), Some(home)) => home.join(rest),
            _ => PathBuf::from(value),
        }
    }
}

/// Everything found without the network: Local, the environment connections
/// (AWS, GCS, Azure), AWS profiles, gcloud configurations and Azure
/// subscriptions to look up with [`azure::discover_azure`].
pub fn discover(host: &Host) -> (Discovery, Vec<Subscription>) {
    let mut found = Discovery {
        connections: vec![Connection::local()],
        notes: Vec::new(),
    };
    found.connections.extend(
        [aws::env(host), gcp::env(host), azure::env(host)]
            .into_iter()
            .flatten(),
    );
    aws::profiles(host, &mut found);
    gcp::configurations(host, &mut found);
    let subscriptions = azure::subscriptions(host, &mut found);
    (found, subscriptions)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn host(home: &std::path::Path, extra: &[(&str, &str)]) -> Host {
        let mut env = pairs(extra);
        env.insert("HOME".to_owned(), home.display().to_string());
        Host::from_vars(env)
    }

    fn pairs(items: &[(&str, &str)]) -> HashMap<String, String> {
        items
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn each_provider_serves_exactly_its_schemes() {
        let table = [
            (Provider::Local, &["file", "http", "https"][..]),
            (Provider::S3, &["s3"]),
            (Provider::Gcs, &["gs", "gcs"]),
            (Provider::Azure, &["az", "wasb", "wasbs", "abfs", "abfss"]),
        ];
        let all: Vec<&str> = table
            .iter()
            .flat_map(|(_, schemes)| *schemes)
            .copied()
            .collect();
        for (provider, schemes) in table {
            for scheme in &all {
                assert_eq!(
                    provider.serves(scheme),
                    schemes.contains(scheme),
                    "{provider:?} {scheme}"
                );
            }
            if provider != Provider::Local {
                let root = provider.root().trim_end_matches("://");
                assert!(provider.serves(root), "{provider:?} root {root}");
            }
        }
    }

    pub(super) fn write(path: &std::path::Path, text: &str) {
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, text).expect("write");
    }

    #[test]
    fn discovery_lists_local_then_env_then_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        let home = dir.path();
        write(&home.join(".aws/config"), "[profile b]\n[profile a]\n");
        write(
            &home.join(".config/gcloud/configurations/config_default"),
            "[core]\nproject = p\n",
        );
        write(
            &home.join(".azure/azureProfile.json"),
            r#"{"subscriptions":[{"id":"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa","name":"P","state":"Enabled","tenantId":"t","isDefault":true}]}"#,
        );
        write(
            &home.join(".config/gcloud/application_default_credentials.json"),
            r#"{"type":"authorized_user"}"#,
        );
        let host = host(
            home,
            &[
                ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                ("AWS_ACCESS_KEY_ID", "k"),
                ("GOOGLE_BEARER_TOKEN", "t"),
            ],
        );
        let (found, subscriptions) = discover(&host);
        let ids: Vec<_> = found.connections.iter().map(|c| c.id.as_str()).collect();
        assert_eq!(
            ids,
            [
                "local",
                "aws-env",
                "gcs-env",
                "azure-env",
                "aws-profile:a",
                "aws-profile:b",
                "gcs-gcloud:default"
            ]
        );
        assert_eq!(subscriptions.len(), 1);
        assert!(found.notes.is_empty(), "{:?}", found.notes);
    }

    #[test]
    fn nothing_is_found_in_an_empty_home() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (found, subscriptions) = discover(&host(dir.path(), &[]));
        assert_eq!(found.connections, [Connection::local()]);
        assert!(found.notes.is_empty() && subscriptions.is_empty());
    }

    fn profile(config: Option<&str>) -> Connection {
        Connection {
            id: "aws-profile:dev".into(),
            label: "AWS · dev".into(),
            provider: Provider::S3,
            source: Source::AwsProfile {
                name: "dev".into(),
                config: config.map(PathBuf::from),
                credentials: None,
            },
            login: None,
        }
    }

    #[test]
    fn isolated_settings_name_only_what_the_connection_has() {
        let vars = |source| {
            Connection {
                source,
                ..profile(None)
            }
            .vars()
        };
        let gcs = |project| Source::Gcs {
            credentials: "/k.json".into(),
            project,
        };
        assert!(Connection::local().vars().is_none());
        assert_eq!(
            profile(Some("/c")).vars(),
            Some(pairs(&[("AWS_CONFIG_FILE", "/c")]))
        );
        assert_eq!(profile(None).vars(), Some(HashMap::new()));
        assert_eq!(
            vars(gcs(Some("proj".into()))),
            Some(pairs(&[
                ("GOOGLE_APPLICATION_CREDENTIALS", "/k.json"),
                ("GOOGLE_CLOUD_PROJECT", "proj"),
            ]))
        );
        let azure = Source::AzureCli {
            account: "acct".into(),
            endpoint: "https://acct.blob.core.windows.net/".into(),
        };
        assert_eq!(
            vars(azure),
            Some(pairs(&[
                ("AZURE_STORAGE_ACCOUNT_NAME", "acct"),
                (
                    "AZURE_STORAGE_ENDPOINT",
                    "https://acct.blob.core.windows.net/"
                ),
                ("AZURE_USE_AZURE_CLI", "true"),
            ]))
        );
        assert_eq!(
            vars(gcs(None)),
            Some(pairs(&[("GOOGLE_APPLICATION_CREDENTIALS", "/k.json")]))
        );
    }

    #[test]
    fn a_profile_connection_applies_the_profile() {
        let mut connection = profile(None);
        let Source::AwsProfile { name, .. } = &mut connection.source else {
            panic!("not a profile");
        };
        *name = " ".into();
        assert!(matches!(
            connection.client(),
            Err(crate::Error::InvalidInput(_))
        ));
    }

    #[tokio::test]
    async fn a_profile_client_takes_its_endpoint_from_the_profile() {
        use crate::backend::stub::{Reply, serve};
        let body = "<ListAllMyBucketsResult><Buckets><Bucket><Name>a</Name></Bucket>\
                    </Buckets></ListAllMyBucketsResult>";
        let stub = serve(vec![Reply::new(200, body)]).await;
        let dir = tempfile::tempdir().expect("tempdir");
        let config = dir.path().join("config");
        write(
            &config,
            &format!(
                "[profile dev]\nregion = eu-west-3\nendpoint_url = {}\n\
                 aws_access_key_id = AKID\naws_secret_access_key = secret\n",
                stub.base
            ),
        );
        let host = host(
            dir.path(),
            &[("AWS_CONFIG_FILE", config.to_str().expect("utf8"))],
        );
        let mut found = Discovery::default();
        aws::profiles(&host, &mut found);
        let client = found.connections[0].client().expect("client");
        assert_eq!(
            client
                .list_dir("s3:///", None, 10)
                .await
                .expect("list")
                .folders,
            ["a"]
        );
    }
}
