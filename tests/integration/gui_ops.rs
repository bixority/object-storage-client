//! GUI operations (paged one-level listing, folder copy) against the real
//! store emulators. They need a container runtime (docker or podman); run with
//! `cargo test --test integration gui_ -- --ignored`.

use crate::common::azurite::azurite_client;
use crate::common::gcs::{gcs_client, seed};
use crate::common::provider::{Provider, base};
use crate::common::{both_client, garage_client, unique_gcs_bucket, unique_prefix};
use crate::support::{TestResult, pattern};
use object_storage_client::ObjectStorageClient;
use object_storage_client::gui::location::Location;
use object_storage_client::gui::model::{Batch, Entry, OnExisting, OpKind};
use object_storage_client::gui::ops::{Progress, execute, list_page_with, plan};
use std::collections::BTreeSet;

const OBJECTS: [&str; 5] = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"];

/// Writes one object under `root` through the client, or through the JSON
/// upload API for GCS (the emulator rejects the client's writes).
async fn write(
    client: &ObjectStorageClient,
    gcs: bool,
    root: &str,
    key: &str,
    body: &[u8],
) -> TestResult {
    if gcs {
        seed(root, key, "application/octet-stream", body.to_vec()).await?;
    } else {
        client.put(&format!("{root}{key}"), body.to_vec()).await?;
    }
    Ok(())
}

/// Seeds 5 objects, 2 folders with one object each and a zero-byte marker
/// `f1/`, pages through `root` with 3 entries per page and checks the result.
async fn check_paging(client: &ObjectStorageClient, gcs: bool, root: &str) -> TestResult {
    for name in OBJECTS {
        write(client, gcs, root, name, b"x").await?;
    }
    write(client, gcs, root, "f1/inner.txt", b"x").await?;
    write(client, gcs, root, "f2/inner.txt", b"x").await?;
    if gcs {
        // The client refuses a trailing-slash object URL, so only GCS (seeded
        // through the JSON API) gets the zero-byte folder marker.
        write(client, gcs, root, "f1/", b"").await?;
    }

    let dir = Location::parse(root)?;
    let mut names = Vec::new();
    let mut folders = BTreeSet::new();
    let mut pages = 0;
    let mut token = None;
    loop {
        let page = list_page_with(client, &dir, token, 3).await?;
        pages += 1;
        assert!(pages < 20, "paging does not terminate");
        for entry in &page.entries {
            if let Entry::Folder { name } = entry {
                folders.insert(name.clone());
            }
            names.push(entry.name().to_owned());
        }
        token = page.next;
        if token.is_none() {
            break;
        }
    }
    if gcs {
        // GCS is paged (`max-keys` on the XML API), but fake-gcs-server
        // ignores `max-keys` and answers with everything: the page count is
        // the emulator's, so only the names are checked.
        assert!(pages >= 1);
    } else {
        assert!(pages >= 2, "expected at least 2 pages, got {pages}");
    }
    assert!(
        names.iter().all(|n| !n.is_empty()),
        "empty name in {names:?}"
    );
    let unique: BTreeSet<_> = names.iter().cloned().collect();
    assert_eq!(unique.len(), names.len(), "a name repeats: {names:?}");
    let mut expected: BTreeSet<String> = OBJECTS.iter().map(|s| (*s).to_owned()).collect();
    expected.extend(["f1".to_owned(), "f2".to_owned()]);
    assert_eq!(unique, expected);
    assert_eq!(folders, BTreeSet::from(["f1".to_owned(), "f2".to_owned()]));
    Ok(())
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gui_list_pages_s3() -> TestResult {
    let client = garage_client();
    check_paging(&client, false, &unique_prefix("gui-pages")).await
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gui_list_pages_azure() -> TestResult {
    let client = azurite_client();
    let root = base(&client, Provider::Azure, "gui-pages").await?;
    check_paging(&client, false, &root).await
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gui_list_pages_gcs() -> TestResult {
    let client = gcs_client();
    let root = unique_gcs_bucket();
    client.create_bucket(&root).await?;
    check_paging(&client, true, &root).await
}

#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gui_folder_copy_s3_to_azure() -> TestResult {
    let client = both_client();
    let src_root = unique_prefix("gui-copy");
    let dst_root = base(&client, Provider::Azure, "gui-copy").await?;

    let files: Vec<(String, Vec<u8>)> = vec![
        ("dir/one.bin".into(), pattern(10)),
        ("dir/two.bin".into(), pattern(100_000)),
        ("dir/sub/three.bin".into(), pattern(7)),
    ];
    for (key, body) in &files {
        write(&client, false, &src_root, key, body).await?;
    }

    let batch = Batch {
        kind: OpKind::Copy,
        src_dir: Location::parse(&src_root)?,
        entries: vec![Entry::Folder { name: "dir".into() }],
        dst_dir: Location::parse(&dst_root)?,
        rename_to: None,
    };
    let planned = plan(&client, &client, batch).await;
    assert!(planned.failures.is_empty(), "{:?}", planned.failures);
    let progress = Progress::default();
    let outcome = execute(
        &client,
        &client,
        planned,
        OnExisting::Overwrite,
        &progress,
        &|_, _, _| {},
    )
    .await;
    assert!(outcome.failures.is_empty(), "{:?}", outcome.failures);
    assert_eq!(outcome.done, files.len());

    for (key, body) in &files {
        let back = client.get(&format!("{dst_root}{key}")).await?;
        assert!(back.as_ref() == body.as_slice(), "content of {key}");
    }
    Ok(())
}

/// A blob put with the `Archive` access tier lists as offline.
#[tokio::test]
#[ignore = "needs a container runtime (docker or podman)"]
async fn gui_list_archived_blob_azure() -> TestResult {
    use crate::common::azurite::azurite_vars;
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::{Attribute, Attributes, ObjectStore, PutOptions, PutPayload};

    let client = azurite_client();
    let root = base(&client, Provider::Azure, "gui-archive").await?;
    let (container, prefix) = root
        .strip_prefix("az://")
        .and_then(|rest| rest.split_once('/'))
        .ok_or("unexpected root")?;
    let vars = azurite_vars();
    let store = MicrosoftAzureBuilder::new()
        .with_account(&vars["AZURE_STORAGE_ACCOUNT_NAME"])
        .with_access_key(&vars["AZURE_STORAGE_ACCOUNT_KEY"])
        .with_endpoint(vars["AZURE_STORAGE_ENDPOINT"].clone())
        .with_allow_http(true)
        .with_container_name(container)
        .build()?;
    let path = object_store::path::Path::from(format!("{prefix}cold.bin"));
    let mut attributes = Attributes::new();
    attributes.insert(Attribute::StorageClass, "Archive".into());
    let opts = PutOptions {
        attributes,
        ..PutOptions::default()
    };
    store
        .put_opts(&path, PutPayload::from_static(b"x"), opts)
        .await?;
    write(&client, false, &root, "warm.bin", b"x").await?;

    let page = list_page_with(&client, &Location::parse(&root)?, None, 10).await?;
    let archived: Vec<_> = page
        .entries
        .iter()
        .filter_map(|e| match e {
            Entry::Object { name, state, .. } => Some((name.as_str(), format!("{state:?}"))),
            _ => None,
        })
        .collect();
    assert_eq!(
        archived,
        [
            ("cold.bin", "Archived".to_owned()),
            ("warm.bin", "Online".to_owned())
        ]
    );
    Ok(())
}
