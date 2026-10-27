//! One reusable, localhost-only fake-gcs-server (GCS emulator) container,
//! started lazily and never dropped; see the module docs of `common` for the
//! reuse behaviour and the cleanup command (label value `it-gcs`).
//!
//! # Emulator limitations
//!
//! `object_store` reads, lists and deletes through the GCS XML API
//! (`GET|DELETE /<bucket>/<object>`, `GET /<bucket>?prefix=`), which
//! fake-gcs-server 1.56 serves. It also *writes* through the XML API (`PUT
//! /<bucket>/<object>`, copy via `x-goog-copy-source`, multipart via `POST
//! ...?uploads` and `PUT ...?partNumber=`), and the emulator routes all of
//! those to its JSON upload handler, which answers `400 invalid uploadType`
//! (it only knows `media`, `multipart` and `resumable`, all selected with a
//! query parameter `object_store` does not send). So `put`, `put_stream`,
//! `copy` and `move_object` cannot work against it. Tests seed objects with
//! the JSON upload API instead (see [`seed`]) and exercise the client's read,
//! list, delete and bucket paths.
//!
//! floci-gcp 0.11.0 (`docker.io/floci/floci-gcp`, checked 2026-10-27) is no
//! replacement yet: it stores XML API writes, multipart included, but a simple
//! `PUT` answers without an `ETag` header (floci-gcp issue #526) and `GET`
//! without `Last-Modified`, and `object_store` rejects both responses.

use super::start_shared;
use object_storage_client::ObjectStorageClient;
use std::collections::HashMap;
use std::sync::OnceLock;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

const GCS_PORT: u16 = 4443;

/// Project id used for bucket creation (any value; the emulator ignores it).
pub const PROJECT: &str = "osc-it";

/// Host port the emulator is mapped to, or the cached startup failure.
static GCS_HOST_PORT: OnceLock<Result<u16, String>> = OnceLock::new();

fn endpoint_of(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

fn vars_with(port: u16) -> HashMap<String, String> {
    // `disable_oauth` makes object_store send no credentials; the key fields
    // only need to parse.
    let key = format!(
        r#"{{"private_key":"","private_key_id":"","client_email":"","disable_oauth":true,"gcs_base_url":"{}"}}"#,
        endpoint_of(port)
    );
    HashMap::from([
        ("GOOGLE_SERVICE_ACCOUNT_KEY".into(), key),
        ("GOOGLE_CLOUD_PROJECT".into(), PROJECT.into()),
        ("GCS_BASE_URL".into(), endpoint_of(port)),
    ])
}

fn client_with(port: u16) -> ObjectStorageClient {
    ObjectStorageClient::with_vars(vars_with(port))
}

/// Starts (or reuses) the emulator and blocks until the JSON API answers.
///
/// The XML API routes only match requests whose `Host` equals `-public-host`;
/// a value without a port matches any port, so `127.0.0.1` fits the randomly
/// mapped host port.
fn start_gcs() -> u16 {
    let request = GenericImage::new("docker.io/fsouza/fake-gcs-server", "latest")
        .with_exposed_port(GCS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_either_std("server started at"))
        .with_cmd([
            "-scheme",
            "http",
            "-backend",
            "memory",
            "-port",
            "4443",
            "-public-host",
            "127.0.0.1",
        ])
        .with_label("org.bixority.object-storage-client", "it-gcs");
    start_shared(
        &GCS_HOST_PORT,
        "osc-it-gcs",
        request,
        GCS_PORT,
        |port| async move {
            client_with(port)
                .bucket_exists("gs://readiness-probe/")
                .await
                .is_ok()
        },
    )
}

/// A client wired to the shared emulator. No environment needed.
pub fn gcs_client() -> ObjectStorageClient {
    client_with(start_gcs())
}

/// The `GOOGLE_*` / `GCS_*` settings of the shared emulator.
pub fn gcs_vars() -> HashMap<String, String> {
    vars_with(start_gcs())
}

/// Stores `body` as `<bucket_url><name>` (`gs://bucket/`) through the emulator's JSON upload API
/// (`uploadType=media`), bypassing the client; the bucket must exist.
///
/// # Errors
/// If the request fails or the emulator rejects it.
pub async fn seed(
    bucket_url: &str,
    name: &str,
    content_type: &str,
    body: impl Into<reqwest::Body>,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket = bucket_url.trim_start_matches("gs://").trim_end_matches('/');
    let mut url = url::Url::parse(&format!(
        "{}/upload/storage/v1/b/{bucket}/o",
        endpoint_of(start_gcs())
    ))?;
    url.query_pairs_mut()
        .append_pair("uploadType", "media")
        .append_pair("name", name);
    let response = reqwest::Client::new()
        .post(url)
        .header(reqwest::header::CONTENT_TYPE, content_type)
        .body(body)
        .send()
        .await?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("seed {bucket}/{name}: HTTP {status}").into());
    }
    Ok(())
}
