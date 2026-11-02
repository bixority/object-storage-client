use super::*;
use crate::ObjectStorageClient;
use crate::encryption::EncryptionConfig;
use crate::encryption::keys::Keyring;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

fn loc(path: &Path) -> Location {
    let url = url::Url::from_directory_path(path).expect("absolute path");
    Location::parse(url.as_str()).expect("parses")
}

fn write(path: &Path, body: &str) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("mkdir");
    }
    fs::write(path, body).expect("write");
}

fn read(path: &Path) -> String {
    fs::read_to_string(path).expect("read")
}

fn folder(name: &str) -> Entry {
    Entry::Folder {
        name: name.to_owned(),
    }
}

fn object(name: &str) -> Entry {
    Entry::Object {
        name: name.to_owned(),
        size: 0,
        modified: chrono::DateTime::default(),
    }
}

fn batch(kind: OpKind, src: &Location, entries: Vec<Entry>, dst: &Location) -> Batch {
    Batch {
        kind,
        src_dir: src.clone(),
        entries,
        dst_dir: dst.clone(),
        rename_to: None,
    }
}

fn quiet(_: usize, _: usize, _: &str) {}

fn transfers(plan: &Plan) -> Vec<(String, String, String)> {
    let mut out: Vec<_> = plan
        .items
        .iter()
        .map(|item| match item {
            Item::Transfer { src, dst, rel, .. } => (src.clone(), dst.clone(), rel.clone()),
            other => panic!("unexpected item {other:?}"),
        })
        .collect();
    out.sort();
    out
}

/// A temp dir with `src/a/{1,2}`, `src/b.txt` and an empty `dst/`.
fn fixture() -> (tempfile::TempDir, Location, Location) {
    let dir = tempfile::tempdir().expect("tempdir");
    write(&dir.path().join("src/a/1"), "one");
    write(&dir.path().join("src/a/2"), "two");
    write(&dir.path().join("src/b.txt"), "bee");
    fs::create_dir_all(dir.path().join("dst")).expect("mkdir");
    let src = loc(&dir.path().join("src"));
    let dst = loc(&dir.path().join("dst"));
    (dir, src, dst)
}

#[tokio::test]
async fn list_page_maps_entries() {
    let (_dir, src, _dst) = fixture();
    let client = ObjectStorageClient::new();
    let page = list_page(&client, &src, None).await.expect("lists");
    let mut names: Vec<_> = page
        .entries
        .iter()
        .map(|entry| (entry.name().to_owned(), entry.is_folder()))
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec![("a".to_owned(), true), ("b.txt".to_owned(), false)]
    );
    assert!(page.entries.iter().any(|entry| matches!(
        entry,
        Entry::Object { name, size: 3, .. } if name == "b.txt"
    )));
    assert_eq!(page.next, None);
}

#[tokio::test]
async fn list_page_error_is_display_text() {
    let client = ObjectStorageClient::new();
    let dir = Location::parse("http://127.0.0.1:1/").expect("parses");
    let err = list_page(&client, &dir, None)
        .await
        .expect_err("connection refused");
    assert_ne!(err, "");
    assert!(!err.chars().any(char::is_control), "{err:?}");
}

#[tokio::test]
async fn plan_expands_folders() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![folder("a"), object("b.txt")], &dst),
    )
    .await;
    assert_eq!(plan.failures, []);
    assert!(plan.checked);
    assert_eq!(
        transfers(&plan),
        vec![
            (
                src.child_object("a/1").expect("valid name"),
                dst.child_object("a/1").expect("valid name"),
                "a/1".to_owned()
            ),
            (
                src.child_object("a/2").expect("valid name"),
                dst.child_object("a/2").expect("valid name"),
                "a/2".to_owned()
            ),
            (
                src.child_object("b.txt").expect("valid name"),
                dst.child_object("b.txt").expect("valid name"),
                "b.txt".to_owned()
            ),
        ]
    );
}

#[tokio::test]
async fn plan_marks_existing_destinations() {
    let (dir, src, dst) = fixture();
    write(&dir.path().join("dst/b.txt"), "old");
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![folder("a"), object("b.txt")], &dst),
    )
    .await;
    assert!(plan.checked);
    assert_eq!(plan.existing(), 1);
    assert!(plan.items.iter().all(|item| matches!(
        item,
        Item::Transfer {
            exists: Some(_),
            ..
        }
    )));
}

#[tokio::test]
async fn plan_rename_targets_new_name() {
    let (_dir, src, _dst) = fixture();
    let client = ObjectStorageClient::new();
    let mut renaming = batch(OpKind::Rename, &src, vec![folder("a")], &src);
    renaming.rename_to = Some("z".to_owned());
    let plan = plan(&client, renaming).await;
    let dsts: Vec<_> = transfers(&plan)
        .into_iter()
        .map(|(_, dst, _)| dst)
        .collect();
    assert_eq!(
        dsts,
        vec![
            src.child_object("z/1").expect("valid name"),
            src.child_object("z/2").expect("valid name")
        ]
    );
}

#[tokio::test]
async fn plan_delete_has_no_io() {
    let dir = tempfile::tempdir().expect("tempdir");
    let src = loc(&dir.path().join("missing"));
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(OpKind::Delete, &src, vec![folder("f"), object("o")], &src),
    )
    .await;
    assert_eq!(plan.failures, []);
    assert_eq!(
        plan.items,
        vec![
            Item::DeletePrefix {
                url: src.child_dir("f").expect("valid name").as_str().to_owned(),
                rel: "f".to_owned(),
            },
            Item::DeleteObject {
                url: src.child_object("o").expect("valid name"),
                rel: "o".to_owned(),
            },
        ]
    );
}

#[tokio::test]
async fn execute_copies_and_skips() {
    let (dir, src, dst) = fixture();
    write(&dir.path().join("dst/b.txt"), "old");
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![folder("a"), object("b.txt")], &dst),
    )
    .await;
    let calls = StdMutex::new(Vec::new());
    let notify = |processed: usize, total: usize, current: &str| {
        calls
            .lock()
            .expect("lock")
            .push((processed, total, current.to_owned()));
    };
    let progress = Progress::default();
    let outcome = execute(&client, plan, OnExisting::Skip, &progress, &notify).await;
    assert_eq!(outcome.done, 2);
    assert!(!outcome.cancelled);
    assert_eq!(outcome.failures, []);
    assert_eq!(read(&dir.path().join("dst/b.txt")), "old");
    assert_eq!(read(&dir.path().join("dst/a/1")), "one");
    assert_eq!(read(&dir.path().join("dst/a/2")), "two");
    let mut calls = calls.into_inner().expect("lock");
    calls.sort();
    assert_eq!(
        calls.iter().map(|c| (c.0, c.1)).collect::<Vec<_>>(),
        vec![(1, 2), (2, 2)]
    );
}

#[tokio::test]
async fn execute_collects_failures_and_continues() {
    let (dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let mut plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![object("b.txt"), folder("a")], &dst),
    )
    .await;
    plan.items
        .retain(|item| matches!(item, Item::Transfer { rel, .. } if rel != "a/2"));
    plan.items.push(Item::Transfer {
        src: src.child_object("gone").expect("valid name"),
        dst: dst.child_object("gone").expect("valid name"),
        rel: "gone".to_owned(),
        exists: Some(false),
    });
    let progress = Progress::default();
    let outcome = execute(&client, plan, OnExisting::Overwrite, &progress, &quiet).await;
    assert_eq!(outcome.done, 2);
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(outcome.failures[0].name, "gone");
    assert_ne!(outcome.failures[0].error, "");
    assert_eq!(progress.done.load(Ordering::SeqCst), 3);
    assert_eq!(progress.succeeded.load(Ordering::SeqCst), 2);
    assert_eq!(read(&dir.path().join("dst/b.txt")), "bee");
    assert_eq!(read(&dir.path().join("dst/a/1")), "one");
}

#[tokio::test]
async fn execute_lists_plan_failures_first() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = Plan {
        kind: OpKind::Copy,
        items: vec![Item::Transfer {
            src: src.child_object("gone").expect("valid name"),
            dst: dst.child_object("gone").expect("valid name"),
            rel: "gone".to_owned(),
            exists: None,
        }],
        checked: false,
        failures: vec![Failure {
            name: "planned".to_owned(),
            error: "listing failed".to_owned(),
        }],
    };
    let outcome = execute(
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    let names: Vec<_> = outcome.failures.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["planned", "gone"]);
    assert_eq!(outcome.done, 0);
}

#[tokio::test]
async fn move_folder_removes_sources() {
    let (dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(&client, batch(OpKind::Move, &src, vec![folder("a")], &dst)).await;
    let outcome = execute(
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.done, 2);
    assert_eq!(outcome.failures, []);
    assert!(!dir.path().join("src/a/1").exists());
    assert!(!dir.path().join("src/a/2").exists());
    assert_eq!(read(&dir.path().join("dst/a/1")), "one");
    assert_eq!(read(&dir.path().join("dst/a/2")), "two");
}

#[tokio::test]
async fn delete_folder_and_object() {
    let (dir, src, _dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(
            OpKind::Delete,
            &src,
            vec![folder("a"), object("b.txt")],
            &src,
        ),
    )
    .await;
    let outcome = execute(
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.done, 2);
    assert_eq!(outcome.failures, []);
    assert!(!dir.path().join("src/a/1").exists());
    assert!(!dir.path().join("src/a/2").exists());
    assert!(!dir.path().join("src/b.txt").exists());
}

#[tokio::test]
async fn copy_folder_with_awkward_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let names = ["a b", "c%d", "é#"];
    for name in names {
        write(&dir.path().join("src/f").join(name), name);
    }
    fs::create_dir_all(dir.path().join("dst")).expect("mkdir");
    let src = loc(&dir.path().join("src"));
    let dst = loc(&dir.path().join("dst"));
    let client = ObjectStorageClient::new();
    let plan = plan(&client, batch(OpKind::Copy, &src, vec![folder("f")], &dst)).await;
    assert_eq!(plan.failures, []);
    let mut rels: Vec<_> = transfers(&plan)
        .into_iter()
        .map(|(_, _, rel)| rel)
        .collect();
    rels.sort();
    assert_eq!(rels, vec!["f/a b", "f/c%d", "f/é#"]);
    let outcome = execute(
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.failures, []);
    assert_eq!(outcome.done, 3);
    for name in names {
        assert_eq!(read(&dir.path().join("dst/f").join(name)), name);
    }
    let page = list_page(&client, &dst.child_dir("f").expect("valid name"), None)
        .await
        .expect("lists");
    let mut listed: Vec<_> = page.entries.iter().map(|e| e.name().to_owned()).collect();
    listed.sort();
    assert_eq!(listed, names);
}

#[tokio::test]
async fn encrypted_copy_round_trips() {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::create_dir_all(dir.path().join("src")).expect("mkdir");
    fs::create_dir_all(dir.path().join("dst")).expect("mkdir");
    let src = loc(&dir.path().join("src"));
    let dst = loc(&dir.path().join("dst"));
    let bare = ObjectStorageClient::new();
    let keyring = Keyring::from_material("k1", &[("k1", &[7u8; 32])]);
    let client = bare.with_encryption(EncryptionConfig::new(Arc::new(keyring)));
    let secret = b"top secret payload".as_slice();
    client
        .put(&src.child_object("s.bin").expect("valid name"), secret)
        .await
        .expect("put");
    let plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![object("s.bin")], &dst),
    )
    .await;
    let outcome = execute(
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.failures, []);
    assert_eq!(outcome.done, 1);
    let copied = client
        .get(&dst.child_object("s.bin").expect("valid name"))
        .await
        .expect("get");
    assert_eq!(copied.as_ref(), secret);
    let raw = bare
        .get(&dst.child_object("s.bin").expect("valid name"))
        .await
        .expect("raw get");
    assert_ne!(raw.as_ref(), secret);
    assert!(
        !raw.windows(secret.len()).any(|window| window == secret),
        "plaintext leaked"
    );
}

#[test]
fn futures_are_send() {
    fn send<T: Send>(_: &T) {}
    let client = ObjectStorageClient::new();
    let dir = Location::parse("file:///nowhere/").expect("parses");
    let progress = Progress::default();
    send(&list_page(&client, &dir, None));
    send(&plan(&client, batch(OpKind::Copy, &dir, vec![], &dir)));
    let empty = Plan {
        kind: OpKind::Copy,
        items: vec![],
        checked: true,
        failures: vec![],
    };
    send(&execute(
        &client,
        empty,
        OnExisting::Skip,
        &progress,
        &quiet,
    ));
}

#[tokio::test]
async fn plan_failed_check_counts_as_existing() {
    let (_dir, src, _dst) = fixture();
    let dst = Location::parse("http://127.0.0.1:1/d/").expect("parses");
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        batch(OpKind::Copy, &src, vec![object("b.txt")], &dst),
    )
    .await;
    assert!(plan.checked);
    assert!(matches!(
        plan.items.as_slice(),
        [Item::Transfer {
            exists: Some(true),
            ..
        }]
    ));
    assert_eq!(plan.existing(), 1);
}

#[tokio::test]
async fn plan_rejects_dot_segment_names() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(&client, batch(OpKind::Copy, &src, vec![folder("..")], &dst)).await;
    assert_eq!(plan.items, []);
    assert_eq!(
        plan.failures,
        [Failure {
            name: "..".to_owned(),
            error: "invalid name".to_owned(),
        }]
    );
}
