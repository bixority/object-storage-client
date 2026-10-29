//! Shared test infrastructure: one Garage (and one Azurite, see `azurite`, and
//! one fake-gcs-server, see `gcs`) container per machine.
//!
//! The container is started lazily by the first test that asks for a client and
//! is never dropped. It is also *reusable* (`ReuseDirective::Always` under a
//! stable container name): the next test run finds the running (or stopped)
//! container and reuses it, so local runs leave one Garage and one Azurite
//! behind instead of a new pair per run. Tests use unique prefixes and bucket
//! names, so data left by earlier runs does not matter. A reused container
//! skips testcontainers' wait conditions, so readiness is decided by an actual
//! S3 / Blob request (the probe passed to `start_shared`).
//!
//! The ports are published on 127.0.0.1 only, because the long-lived
//! containers carry well-known test keys. testcontainers-rs has no Ryuk
//! reaper here (and reusable containers are exempt from removal anyway), so
//! the containers outlive the test process; CI runners are discarded.
//!
//! Garage is pinned, and its container name carries the image tag
//! (`osc-it-garage-v2.5.0`). Azurite and fake-gcs-server follow `latest` on
//! purpose, under the fixed names `osc-it-azurite` and `osc-it-gcs`.
//! testcontainers reuses a container by name whatever image it was created
//! from, and only pulls an image that is missing. A reused local container
//! therefore keeps the image it was created from, while a fresh CI runner
//! pulls the newest. To test against the newest `latest` (or after changing
//! anything else about the container setup, for example `.docker/garage.toml`),
//! pull the image and remove the containers by label:
//! `podman rm -f $(podman ps -aq --filter label=org.bixority.object-storage-client)`
//! (use `docker` instead of `podman` where applicable; the label values are
//! `it-garage`, `it-azurite` and `it-gcs`).

pub mod azurite;
pub mod gcs;
pub mod provider;

use object_storage_client::ObjectStorageClient;
use std::collections::HashMap;
use std::future::Future;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::bollard::models::PortBinding;
use testcontainers::core::{ContainerPort, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerRequest, GenericImage, ImageExt, ReuseDirective};

pub const BUCKET: &str = "default";
const S3_PORT: u16 = 3900;

/// Host port Garage's S3 API is mapped to, or why starting it failed (cached,
/// so later tests fail at once instead of retrying the slow path).
static GARAGE_PORT: OnceLock<Result<u16, String>> = OnceLock::new();

/// Upper bound for starting a container and getting a successful probe.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(120);

pub const ACCESS_KEY: &str = "rootroot";
pub const SECRET_KEY: &str = "rootrootrootroot";
pub const REGION: &str = "eu-north-1";

fn vars_with(port: u16) -> HashMap<String, String> {
    HashMap::from([
        ("S3_ENDPOINT".into(), endpoint_of(port)),
        ("S3_REGION".into(), REGION.into()),
        ("S3_ACCESS_KEY_ID".into(), ACCESS_KEY.into()),
        ("S3_SECRET_ACCESS_KEY".into(), SECRET_KEY.into()),
        ("S3_ALLOW_HTTP".into(), "true".into()),
    ])
}

fn endpoint_of(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn client_with(port: u16) -> ObjectStorageClient {
    ObjectStorageClient::with_vars(vars_with(port))
}

/// Starts (or reuses) a container, publishes `port` on 127.0.0.1 only and
/// blocks until `probe(host_port)` succeeds, at most [`STARTUP_TIMEOUT`].
///
/// Runs on a dedicated thread with its own runtime, because each `#[tokio::test]`
/// has a short-lived runtime of its own while the container must outlive them.
/// The outcome, including a failure, is stored in `slot`.
///
/// # Panics
/// If the container does not start or become ready in time.
pub fn start_shared<F, Fut>(
    slot: &'static OnceLock<Result<u16, String>>,
    name: &'static str,
    request: ContainerRequest<GenericImage>,
    port: u16,
    probe: F,
) -> u16
where
    F: Fn(u16) -> Fut + Send + 'static,
    Fut: Future<Output = bool>,
{
    let outcome = slot.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            rt.block_on(async {
                let startup = async {
                    let container = request
                        .with_container_name(name)
                        .with_reuse(ReuseDirective::Always)
                        .with_host_config_modifier(move |host| {
                            host.publish_all_ports = Some(false);
                            host.port_bindings = Some(std::collections::HashMap::from([(
                                format!("{port}/tcp"),
                                Some(vec![PortBinding {
                                    host_ip: Some("127.0.0.1".into()),
                                    host_port: Some(String::new()),
                                }]),
                            )]));
                        })
                        .start()
                        .await
                        .map_err(|e| {
                            format!("start {name} (is a container runtime available?): {e}")
                        })?;
                    let host_port = container
                        .get_host_port_ipv4(ContainerPort::Tcp(port))
                        .await
                        .map_err(|e| format!("mapped port of {name}: {e}"))?;
                    // Log lines precede API readiness and are skipped for a
                    // reused container; the probe is the source of truth.
                    while !probe(host_port).await {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                    Ok::<_, String>((container, host_port))
                };
                let result = match tokio::time::timeout(STARTUP_TIMEOUT, startup).await {
                    Ok(Ok((container, host_port))) => Ok((container, host_port)),
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(format!(
                        "{name} not ready within {}s",
                        STARTUP_TIMEOUT.as_secs()
                    )),
                };
                match result {
                    Ok((_container, host_port)) => {
                        let _ = tx.send(Ok(host_port));
                        // Keep the runtime alive for the whole process.
                        std::future::pending::<()>().await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            });
        });
        rx.recv()
            .unwrap_or_else(|_| Err(format!("{name} startup thread died")))
    });
    match outcome {
        Ok(port) => *port,
        Err(e) => panic!("shared container {name} unavailable: {e}"),
    }
}

/// Starts (or reuses) Garage and blocks until the S3 API accepts requests.
fn start_garage() -> u16 {
    let request = GenericImage::new("docker.io/dxflrs/garage", "v2.5.0")
        .with_exposed_port(S3_PORT.tcp())
        .with_wait_for(WaitFor::message_on_either_std("Creating default bucket"))
        .with_copy_to(
            "/etc/garage.toml",
            include_bytes!("../../../.docker/garage.toml").to_vec(),
        )
        .with_cmd(["/garage", "server", "--single-node", "--default-bucket"])
        .with_env_var("GARAGE_DEFAULT_ACCESS_KEY", ACCESS_KEY)
        .with_env_var("GARAGE_DEFAULT_SECRET_KEY", SECRET_KEY)
        .with_env_var("GARAGE_DEFAULT_BUCKET", BUCKET)
        .with_label("org.bixority.object-storage-client", "it-garage");
    start_shared(
        &GARAGE_PORT,
        "osc-it-garage-v2.5.0",
        request,
        S3_PORT,
        |port| async move {
            client_with(port)
                .bucket_exists(&format!("s3://{BUCKET}/"))
                .await
                .unwrap_or(false)
        },
    )
}

/// A client wired to the shared Garage container. No environment needed.
pub fn garage_client() -> ObjectStorageClient {
    client_with(start_garage())
}

/// The `S3_*` settings of the shared Garage container.
pub fn garage_vars() -> HashMap<String, String> {
    vars_with(start_garage())
}

/// Garage's S3 endpoint URL (`http://127.0.0.1:<port>`).
#[cfg(feature = "encryption")]
pub fn garage_endpoint() -> String {
    endpoint_of(start_garage())
}

/// One client for all emulators: S3, Azure and GCS settings do not overlap, so
/// a single registry serves `s3://`, `az://` and `gs://` URLs (cross-provider
/// copies).
pub fn both_client() -> ObjectStorageClient {
    let mut vars = garage_vars();
    vars.extend(azurite::azurite_vars());
    vars.extend(gcs::gcs_vars());
    ObjectStorageClient::with_vars(vars)
}

/// `s3://default/<prefix>/` with a prefix unique to this call, so parallel
/// tests never see each other's objects.
pub fn unique_prefix(test: &str) -> String {
    format!("s3://{BUCKET}/{test}-{}/", uuid_like())
}

/// `s3://it-<unique>/`: a bucket name no other test (or earlier run) used.
pub fn unique_bucket() -> String {
    format!("s3://it-{}/", uuid_like())
}

/// `az://it-<unique>/`: a container name no other test (or earlier run) used.
/// Azure names are 3-63 lowercase alphanumerics or dashes.
pub fn unique_container() -> String {
    format!("az://it-{}/", uuid_like())
}

/// `gs://it-<unique>/`: a bucket name no other test (or earlier run) used.
pub fn unique_gcs_bucket() -> String {
    format!("gs://it-{}/", uuid_like())
}

/// A name part that is unique per call, per process and per run: the start
/// time and the process id (both in hex) plus a counter, so back-to-back calls
/// never collide whatever the clock resolution.
fn uuid_like() -> String {
    use std::sync::LazyLock;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static RUN: LazyLock<String> = LazyLock::new(|| {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        format!("{nanos:x}-{:x}", std::process::id())
    });
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    format!("{}-{}", *RUN, COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn unique_names_never_repeat() {
        let mut seen = HashSet::new();
        for _ in 0..10_000 {
            for name in [unique_bucket(), unique_container(), unique_gcs_bucket()] {
                assert!(seen.insert(name.clone()), "repeated name {name}");
            }
        }
        assert_eq!(seen.len(), 30_000);
    }

    #[test]
    fn container_names_are_valid_for_azure() {
        for _ in 0..1_000 {
            let url = unique_container();
            let name = url
                .strip_prefix("az://")
                .and_then(|n| n.strip_suffix('/'))
                .expect("az://<name>/");
            assert!((3..=63).contains(&name.len()), "{name}");
            assert!(
                name.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-'),
                "{name}"
            );
            assert!(!name.contains("--"), "{name}");
            assert!(!name.starts_with('-') && !name.ends_with('-'), "{name}");
        }
    }
}
