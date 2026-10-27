//! One reusable, localhost-only Azurite (Azure Blob emulator) container, started
//! lazily and never dropped; see the module docs of `common` for the reuse
//! behaviour and the cleanup command (label value `it-azurite`).

use super::start_shared;
use object_storage_client::ObjectStorageClient;
use std::collections::HashMap;
use std::sync::OnceLock;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::{GenericImage, ImageExt};

/// Azurite's well-known development account and key (public, not a secret).
const ACCOUNT: &str = "devstoreaccount1";
const ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
const BLOB_PORT: u16 = 10000;

/// Host port Azurite's blob service is mapped to, or the cached startup failure.
static AZURITE_PORT: OnceLock<Result<u16, String>> = OnceLock::new();

fn vars_with(port: u16) -> HashMap<String, String> {
    // Azurite serves path-style URLs: `<endpoint>/<account>/<container>/<blob>`.
    HashMap::from([
        ("AZURE_STORAGE_ACCOUNT_NAME".into(), ACCOUNT.into()),
        ("AZURE_STORAGE_ACCOUNT_KEY".into(), ACCOUNT_KEY.into()),
        (
            "AZURE_STORAGE_ENDPOINT".into(),
            format!("http://127.0.0.1:{port}/{ACCOUNT}"),
        ),
        ("AZURE_ALLOW_HTTP".into(), "true".into()),
    ])
}

fn client_with(port: u16) -> ObjectStorageClient {
    ObjectStorageClient::with_vars(vars_with(port))
}

/// Starts (or reuses) Azurite and blocks until the blob service accepts requests.
fn start_azurite() -> u16 {
    let request = GenericImage::new("mcr.microsoft.com/azure-storage/azurite", "latest")
        .with_exposed_port(BLOB_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "Azurite Blob service successfully listens",
        ))
        .with_cmd([
            "azurite-blob",
            "--blobHost",
            "0.0.0.0",
            "--skipApiVersionCheck",
        ])
        .with_label("org.bixority.object-storage-client", "it-azurite");
    start_shared(
        &AZURITE_PORT,
        "osc-it-azurite",
        request,
        BLOB_PORT,
        |port| async move {
            client_with(port)
                .bucket_exists("az://readiness-probe/")
                .await
                .is_ok()
        },
    )
}

/// A client wired to the shared Azurite container. No environment needed.
pub fn azurite_client() -> ObjectStorageClient {
    client_with(start_azurite())
}

/// The `AZURE_*` settings of the shared Azurite container.
pub fn azurite_vars() -> HashMap<String, String> {
    vars_with(start_azurite())
}
