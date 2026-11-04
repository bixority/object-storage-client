use super::*;
use crate::ObjectStorageClient;
use crate::encryption::EncryptionConfig;
use crate::encryption::keys::Keyring;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::Ordering;

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
async fn a_huge_single_page_arrives_sorted_and_cut_one_past_the_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    for i in (0..MAX_ENTRIES + 5).rev() {
        fs::write(dir.path().join(format!("{i:06}")), "").expect("write");
    }
    fs::create_dir(dir.path().join("zz")).expect("mkdir");
    let page = list_page(&ObjectStorageClient::new(), &loc(dir.path()), None)
        .await
        .expect("lists");
    assert_eq!(page.entries.len(), MAX_ENTRIES + 1);
    assert!(
        page.entries
            .is_sorted_by(|a, b| Entry::listing_order(a, b).is_le())
    );
    assert_eq!(page.entries[0], folder("zz"));
    assert_eq!(page.entries[1].name(), "000000");
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

/// Two clients that share nothing, as two connections do.
fn two_clients() -> (ObjectStorageClient, ObjectStorageClient) {
    (
        ObjectStorageClient::isolated(std::collections::HashMap::default()),
        ObjectStorageClient::isolated(std::collections::HashMap::default()),
    )
}

#[tokio::test]
async fn folders_are_listed_through_the_source_and_checked_through_the_destination() {
    let (from, to) = two_clients();
    for (client, keys) in [
        (&from, ["f/1", "f/2"].as_slice()),
        (&to, ["f/1"].as_slice()),
    ] {
        client.preload_store("s3://b/", Arc::new(object_store::memory::InMemory::new()));
        for key in keys {
            client
                .put(&format!("s3://b/{key}"), key.as_bytes().to_vec())
                .await
                .expect("put");
        }
    }
    let at = Location::parse("s3://b/").expect("parses");
    let plan = plan(&from, &to, batch(OpKind::Copy, &at, vec![folder("f")], &at)).await;
    let exists: Vec<_> = plan
        .items
        .iter()
        .map(|item| match item {
            Item::Transfer { rel, exists, .. } => (rel.as_str(), *exists),
            other => panic!("unexpected item {other:?}"),
        })
        .collect();
    assert_eq!(exists, [("f/1", Some(true)), ("f/2", Some(false))]);
    // A move onto the same name through two clients is refused, source kept.
    let moved = Plan {
        kind: OpKind::Move,
        ..plan.clone()
    };
    let outcome = execute(
        &from,
        &to,
        moved,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!((outcome.done, outcome.failures.len()), (0, 2));
    assert!(from.exists("s3://b/f/1").await.expect("exists"));
    let outcome = execute(
        &from,
        &to,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!((outcome.done, outcome.failures), (2, vec![]));
    assert_eq!(to.get("s3://b/f/2").await.expect("get").as_ref(), b"f/2");
}

#[tokio::test]
async fn plan_expands_folders() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
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
    let plan = plan(&client, &client, renaming).await;
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
    let outcome = execute(&client, &client, plan, OnExisting::Skip, &progress, &notify).await;
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
        size: 1,
        exists: Some(false),
    });
    let progress = Progress::default();
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &progress,
        &quiet,
    )
    .await;
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
async fn execute_lists_seeded_plan_failures_first() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = Plan {
        kind: OpKind::Copy,
        items: vec![Item::Transfer {
            src: src.child_object("gone").expect("valid name"),
            dst: dst.child_object("gone").expect("valid name"),
            rel: "gone".to_owned(),
            size: 1,
            exists: None,
        }],
        checked: false,
        failures: Vec::new(),
    };
    // The worker seeds the progress with the plan's failures.
    let progress = Progress::new(vec![Failure {
        name: "planned".to_owned(),
        error: "listing failed".to_owned(),
    }]);
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &progress,
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
    let plan = plan(
        &client,
        &client,
        batch(OpKind::Move, &src, vec![folder("a")], &dst),
    )
    .await;
    let outcome = execute(
        &client,
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
    let plan = plan(
        &client,
        &client,
        batch(OpKind::Copy, &src, vec![folder("f")], &dst),
    )
    .await;
    assert_eq!(plan.failures, []);
    let mut rels: Vec<_> = transfers(&plan)
        .into_iter()
        .map(|(_, _, rel)| rel)
        .collect();
    rels.sort();
    assert_eq!(rels, vec!["f/a b", "f/c%d", "f/é#"]);
    let outcome = execute(
        &client,
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
        &client,
        batch(OpKind::Copy, &src, vec![object("s.bin")], &dst),
    )
    .await;
    let outcome = execute(
        &client,
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
    send(&plan(
        &client,
        &client,
        batch(OpKind::Copy, &dir, vec![], &dir),
    ));
    let empty = Plan {
        kind: OpKind::Copy,
        items: vec![],
        checked: true,
        failures: vec![],
    };
    send(&execute(
        &client,
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
    let plan = plan(
        &client,
        &client,
        batch(OpKind::Copy, &src, vec![folder("..")], &dst),
    )
    .await;
    assert_eq!(plan.items, []);
    assert_eq!(
        plan.failures,
        [Failure {
            name: "..".to_owned(),
            error: "invalid name".to_owned(),
        }]
    );
}

#[cfg(unix)]
mod symlinks {
    use super::*;
    use std::os::unix::fs::symlink;

    fn link(name: &str) -> Entry {
        Entry::Link {
            name: name.to_owned(),
        }
    }

    /// `outside/{x,y/z}` and `src/{real.txt,data -> ../outside,f -> ../outside/x}`
    /// beside an empty `dst/`.
    fn linked() -> (tempfile::TempDir, Location, Location) {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path();
        write(&root.join("outside/x"), "x");
        write(&root.join("outside/y/z"), "z");
        write(&root.join("src/proj/real.txt"), "real");
        write(&root.join("src/top.txt"), "top");
        symlink("../../outside", root.join("src/proj/data")).expect("link");
        symlink("../../outside/x", root.join("src/proj/f")).expect("link");
        symlink("../outside", root.join("src/data")).expect("link");
        fs::create_dir_all(root.join("dst")).expect("mkdir");
        let (src, dst) = (loc(&root.join("src")), loc(&root.join("dst")));
        (dir, src, dst)
    }

    fn outside_intact(root: &Path) {
        assert_eq!(read(&root.join("outside/x")), "x");
        assert_eq!(read(&root.join("outside/y/z")), "z");
    }

    #[tokio::test]
    async fn list_page_shows_links_as_links() {
        let (_dir, src, _dst) = linked();
        let page = list_page(&ObjectStorageClient::new(), &src, None)
            .await
            .expect("lists");
        let mut kinds: Vec<_> = page
            .entries
            .iter()
            .map(|e| (e.name().to_owned(), matches!(e, Entry::Link { .. })))
            .collect();
        kinds.sort();
        assert_eq!(
            kinds,
            [
                ("data".to_owned(), true),
                ("proj".to_owned(), false),
                ("top.txt".to_owned(), false)
            ]
        );
    }

    #[tokio::test]
    async fn deleting_a_linked_folder_removes_only_the_link() {
        let (dir, src, _dst) = linked();
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Delete, &src, vec![link("data")], &src),
        )
        .await;
        assert_eq!(plan.failures, []);
        assert!(matches!(plan.items.as_slice(), [Item::DeleteObject { .. }]));
        let outcome = execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(outcome.done, 1);
        assert_eq!(outcome.failures, []);
        assert!(dir.path().join("src/data").symlink_metadata().is_err());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn copy_of_a_folder_with_links_stays_inside_it() {
        let (dir, src, dst) = linked();
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Copy, &src, vec![folder("proj")], &dst),
        )
        .await;
        assert_eq!(plan.failures, []);
        let rels: Vec<_> = transfers(&plan).into_iter().map(|t| t.2).collect();
        assert_eq!(rels, ["proj/real.txt"]);
        execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(read(&dir.path().join("dst/proj/real.txt")), "real");
        assert!(!dir.path().join("dst/proj/data").exists());
        assert!(!dir.path().join("dst/proj/f").exists());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn move_of_a_folder_with_links_leaves_the_link_targets() {
        let (dir, src, dst) = linked();
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Move, &src, vec![folder("proj")], &dst),
        )
        .await;
        assert_eq!(plan.items.len(), 1, "{plan:?}");
        execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(read(&dir.path().join("dst/proj/real.txt")), "real");
        assert!(!dir.path().join("dst/proj/data").exists());
        assert!(dir.path().join("src/proj/data").symlink_metadata().is_ok());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn rename_of_a_folder_with_links_leaves_the_link_targets() {
        let (dir, src, _dst) = linked();
        let client = ObjectStorageClient::new();
        let mut renaming = batch(OpKind::Rename, &src, vec![folder("proj")], &src);
        renaming.rename_to = Some("proj2".to_owned());
        let plan = plan(&client, &client, renaming).await;
        assert_eq!(plan.items.len(), 1, "{plan:?}");
        execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(read(&dir.path().join("src/proj2/real.txt")), "real");
        assert!(!dir.path().join("src/proj2/data").exists());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn a_selected_link_is_not_copied() {
        let (dir, src, dst) = linked();
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(
                OpKind::Copy,
                &src,
                vec![link("data"), object("top.txt")],
                &dst,
            ),
        )
        .await;
        assert_eq!(
            plan.failures,
            [Failure {
                name: "data".to_owned(),
                error: "symbolic link skipped".to_owned(),
            }]
        );
        assert_eq!(plan.items.len(), 1);
        execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert!(dir.path().join("dst/data").symlink_metadata().is_err());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn a_selected_link_moved_to_another_store_is_skipped() {
        let (_dir, src, _dst) = linked();
        let other = Location::parse("s3://bucket/").expect("parses");
        let plan = plan(
            &ObjectStorageClient::new(),
            &ObjectStorageClient::new(),
            batch(OpKind::Move, &src, vec![link("data")], &other),
        )
        .await;
        assert_eq!(plan.items, []);
        assert_eq!(plan.failures[0].error, "symbolic link skipped");
    }

    #[tokio::test]
    async fn a_selected_link_moved_to_a_file_folder_is_renamed() {
        let (dir, src, dst) = linked();
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Move, &src, vec![link("data")], &dst),
        )
        .await;
        assert_eq!(plan.failures, []);
        assert!(matches!(
            plan.items.as_slice(),
            [Item::RenameLink {
                exists: Some(false),
                ..
            }]
        ));
        let outcome = execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!((outcome.done, outcome.failures.len()), (1, 0));
        assert!(dir.path().join("src/data").symlink_metadata().is_err());
        let moved = dir.path().join("dst/data");
        assert!(moved.symlink_metadata().expect("moved").is_symlink());
        outside_intact(dir.path());
    }

    #[tokio::test]
    async fn a_moved_link_honours_skip_for_an_existing_destination() {
        let (dir, src, dst) = linked();
        write(&dir.path().join("dst/data"), "mine");
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Move, &src, vec![link("data")], &dst),
        )
        .await;
        assert_eq!(plan.existing(), 1);
        let outcome = execute(
            &client,
            &client,
            plan,
            OnExisting::Skip,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(outcome.done, 0);
        assert_eq!(read(&dir.path().join("dst/data")), "mine");
        assert!(dir.path().join("src/data").symlink_metadata().is_ok());
    }

    #[tokio::test]
    async fn a_moved_link_does_not_silently_replace_a_dangling_link() {
        let (dir, src, dst) = linked();
        let existing = dir.path().join("dst/data");
        std::os::unix::fs::symlink(dir.path().join("nowhere"), &existing).expect("link");
        let client = ObjectStorageClient::new();
        let plan = plan(
            &client,
            &client,
            batch(OpKind::Move, &src, vec![link("data")], &dst),
        )
        .await;
        // `head` would follow the dangling link and see nothing there.
        assert_eq!(plan.existing(), 1);
        let outcome = execute(
            &client,
            &client,
            plan,
            OnExisting::Skip,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!(outcome.done, 0);
        assert_eq!(
            std::fs::read_link(&existing).expect("still a link"),
            dir.path().join("nowhere")
        );
        assert!(dir.path().join("src/data").symlink_metadata().is_ok());
    }

    #[tokio::test]
    async fn renaming_a_link_renames_the_link() {
        let (dir, src, _dst) = linked();
        let client = ObjectStorageClient::new();
        let mut renaming = batch(OpKind::Rename, &src, vec![link("data")], &src);
        renaming.rename_to = Some("moved".to_owned());
        let plan = plan(&client, &client, renaming).await;
        assert_eq!(plan.failures, []);
        let outcome = execute(
            &client,
            &client,
            plan,
            OnExisting::Overwrite,
            &Progress::default(),
            &quiet,
        )
        .await;
        assert_eq!((outcome.done, outcome.failures.len()), (1, 0));
        assert!(dir.path().join("src/data").symlink_metadata().is_err());
        let moved = dir.path().join("src/moved");
        assert!(moved.symlink_metadata().expect("renamed").is_symlink());
        outside_intact(dir.path());
    }
}

#[tokio::test]
async fn plan_carries_the_listed_size() {
    let (_dir, src, dst) = fixture();
    let client = ObjectStorageClient::new();
    let plan = plan(
        &client,
        &client,
        batch(OpKind::Copy, &src, vec![folder("a"), object("b.txt")], &dst),
    )
    .await;
    let mut sizes: Vec<_> = plan
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Transfer { rel, size, .. } => Some((rel.clone(), *size)),
            _ => None,
        })
        .collect();
    sizes.sort();
    // A selected object brings the size of its row, which `object` sets to 0.
    assert_eq!(
        sizes,
        [("a/1".into(), 3), ("a/2".into(), 3), ("b.txt".into(), 0)]
    );
}

#[tokio::test]
async fn plan_caps_the_total_across_folders() {
    let dir = tempfile::tempdir().expect("tempdir");
    for name in ["f/1", "f/2", "g/1", "g/2"] {
        write(&dir.path().join("src").join(name), "x");
    }
    fs::create_dir_all(dir.path().join("dst")).expect("mkdir");
    let src = loc(&dir.path().join("src"));
    let dst = loc(&dir.path().join("dst"));
    let client = ObjectStorageClient::new();
    let both = || batch(OpKind::Move, &src, vec![folder("f"), folder("g")], &dst);
    // Each folder fits on its own; the two together do not.
    let plan = plan_with(&client, &client, both(), 3).await;
    assert_eq!(plan.items, []);
    assert_eq!(plan.failures.len(), 1);
    assert_eq!(plan.failures[0].name, src.display());
    assert_eq!(
        plan.failures[0].error,
        "more than 3 objects; select a deeper folder"
    );
    // Exactly at the cap is fine.
    let plan = plan_with(&client, &client, both(), 4).await;
    assert_eq!(plan.failures, []);
    assert_eq!(plan.items.len(), 4);
}

/// An `InMemory` store that also lists the console folder marker `p/sub/` the
/// way S3 does: as `p/sub`, with size 0. The marker is not an addressable key.
#[derive(Debug, Default)]
struct MarkerStore {
    inner: object_store::memory::InMemory,
}

impl std::fmt::Display for MarkerStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("MarkerStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for MarkerStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let marker = object_store::ObjectMeta {
            location: object_store::path::Path::from("p/sub"),
            last_modified: chrono::DateTime::UNIX_EPOCH,
            size: 0,
            e_tag: None,
            version: None,
        };
        futures::stream::iter([Ok(marker)])
            .chain(self.inner.list(prefix))
            .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[tokio::test]
async fn folder_markers_are_skipped_not_failed() {
    let client = ObjectStorageClient::new();
    client.preload_store("s3://b/", Arc::new(MarkerStore::default()));
    client
        .put("s3://b/p/sub/x", b"payload".to_vec())
        .await
        .expect("put");
    let src = Location::parse("s3://b/").expect("parses");
    let dst = Location::parse("s3://b/q/").expect("parses");
    let plan = plan(
        &client,
        &client,
        batch(OpKind::Copy, &src, vec![folder("p")], &dst),
    )
    .await;
    // Both the marker and the object are planned.
    assert_eq!(plan.items.len(), 2);
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.failures, []);
    assert_eq!(outcome.done, 1);
    assert_eq!(
        client.get("s3://b/q/p/sub/x").await.expect("copied"),
        b"payload".as_slice()
    );
}

/// An `InMemory` store that counts `delete_stream` calls and `HEAD`s, fails
/// the deletes of `failing` keys and delays listings of `slow` prefixes.
#[derive(Debug, Default)]
struct SpyStore {
    inner: Arc<object_store::memory::InMemory>,
    delete_streams: std::sync::atomic::AtomicUsize,
    heads: std::sync::atomic::AtomicUsize,
    failing: Vec<&'static str>,
    slow: Vec<(&'static str, u64)>,
}

impl SpyStore {
    fn count(counter: &std::sync::atomic::AtomicUsize) -> usize {
        counter.load(Ordering::SeqCst)
    }
}

impl std::fmt::Display for SpyStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SpyStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for SpyStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        if options.head {
            self.heads.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        use object_store::ObjectStoreExt as _;
        self.delete_streams.fetch_add(1, Ordering::SeqCst);
        let failing = self.failing.clone();
        let inner = Arc::clone(&self.inner);
        locations
            .then(move |path| {
                let (failing, inner) = (failing.clone(), Arc::clone(&inner));
                async move {
                    let path = path?;
                    if failing.contains(&path.as_ref()) {
                        return Err(object_store::Error::Generic {
                            store: "spy",
                            source: "refused".into(),
                        });
                    }
                    inner.delete(&path).await?;
                    Ok(path)
                }
            })
            .boxed()
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        let inner = self.inner.list(prefix);
        let slow = prefix.and_then(|p| self.slow.iter().find(|(name, _)| *name == p.as_ref()));
        let delay = slow.map(|(_, ms)| std::time::Duration::from_millis(*ms));
        futures::stream::once(async move {
            if let Some(delay) = delay {
                tokio::time::sleep(delay).await;
            }
            inner
        })
        .flatten()
        .boxed()
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

async fn spy_client(spy: SpyStore) -> (ObjectStorageClient, Arc<SpyStore>) {
    let spy = Arc::new(spy);
    let client = ObjectStorageClient::new();
    client.preload_store("s3://b/", spy.clone());
    for key in ["d/a", "d/b", "d/c", "d/d", "a/1", "b.txt", "c/1"] {
        client
            .put(&format!("s3://b/{key}"), b"abc".to_vec())
            .await
            .expect("put");
    }
    (client, spy)
}

fn deletes(names: &[&str]) -> Plan {
    Plan {
        kind: OpKind::Delete,
        items: names
            .iter()
            .map(|name| Item::DeleteObject {
                url: format!("s3://b/d/{name}"),
                rel: (*name).to_owned(),
            })
            .collect(),
        checked: true,
        failures: Vec::new(),
    }
}

#[tokio::test]
async fn selected_objects_are_deleted_with_one_batched_call() {
    let (client, spy) = spy_client(SpyStore::default()).await;
    let outcome = execute(
        &client,
        &client,
        deletes(&["a", "b", "c", "d"]),
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.failures, []);
    assert_eq!(outcome.done, 4);
    assert_eq!(SpyStore::count(&spy.delete_streams), 1);
    assert!(client.get("s3://b/d/a").await.is_err());
}

#[tokio::test]
async fn a_batched_delete_reports_each_failure_and_forgives_a_missing_key() {
    let spy = SpyStore {
        failing: vec!["d/b", "d/d"],
        ..SpyStore::default()
    };
    let (client, spy) = spy_client(spy).await;
    // `gone` was never stored: already deleted counts as deleted.
    let plan = deletes(&["a", "b", "gone", "c", "d"]);
    let calls = StdMutex::new(0_usize);
    let notify = |_: usize, _: usize, _: &str| *calls.lock().expect("lock") += 1;
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &notify,
    )
    .await;
    assert_eq!(outcome.done, 3);
    assert_eq!(*calls.lock().expect("lock"), 5);
    let mut failed: Vec<_> = outcome.failures.iter().map(|f| f.name.as_str()).collect();
    failed.sort_unstable();
    assert_eq!(failed, ["b", "d"]);
    assert!(outcome.failures.iter().all(|f| f.error.contains("refused")));
    assert_eq!(SpyStore::count(&spy.delete_streams), 1);
    // The refused keys are still there.
    assert!(client.get("s3://b/d/b").await.is_ok());
    assert!(client.get("s3://b/d/a").await.is_err());
}

#[tokio::test]
async fn an_unresolvable_url_fails_alone_in_a_batched_delete() {
    let (client, spy) = spy_client(SpyStore::default()).await;
    let mut plan = deletes(&["a", "c"]);
    plan.items.insert(
        1,
        Item::DeleteObject {
            url: "s3://b/d/x?y=1".to_owned(),
            rel: "x".to_owned(),
        },
    );
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!(outcome.done, 2);
    assert_eq!(outcome.failures.len(), 1);
    assert_eq!(outcome.failures[0].name, "x");
    assert_eq!(SpyStore::count(&spy.delete_streams), 1);
}

#[tokio::test]
async fn a_same_store_copy_does_not_head_the_source_for_its_size() {
    let (client, spy) = spy_client(SpyStore::default()).await;
    let src = Location::parse("s3://b/d/").expect("parses");
    let dst = Location::parse("s3://b/out/").expect("parses");
    let entries = ["a", "b", "c", "d"]
        .map(|name| Entry::Object {
            name: name.to_owned(),
            size: 3,
            modified: chrono::DateTime::default(),
        })
        .to_vec();
    let mut plan = plan(&client, &client, batch(OpKind::Copy, &src, entries, &dst)).await;
    let checks = SpyStore::count(&spy.heads);
    assert_eq!(checks, 4, "one existence check per destination");
    for item in &mut plan.items {
        if let Item::Transfer { exists, .. } = item {
            *exists = None;
        }
    }
    let outcome = execute(
        &client,
        &client,
        plan,
        OnExisting::Overwrite,
        &Progress::default(),
        &quiet,
    )
    .await;
    assert_eq!((outcome.done, outcome.failures.len()), (4, 0));
    assert_eq!(SpyStore::count(&spy.heads), checks, "no source HEAD");
    assert_eq!(
        client.get("s3://b/out/a").await.expect("copied"),
        b"abc".as_slice()
    );
}

#[tokio::test(start_paused = true)]
async fn folders_are_listed_together_and_planned_in_selection_order() {
    let spy = SpyStore {
        slow: vec![("a", 300), ("c", 200)],
        ..SpyStore::default()
    };
    let (client, _spy) = spy_client(spy).await;
    let src = Location::parse("s3://b/").expect("parses");
    let dst = Location::parse("s3://b/out/").expect("parses");
    let entries = vec![folder("a"), object("b.txt"), folder("c")];
    // Virtual time: the delays are tokio sleeps, so a busy machine cannot
    // make the timing flaky.
    let started = tokio::time::Instant::now();
    let plan = plan(&client, &client, batch(OpKind::Copy, &src, entries, &dst)).await;
    assert_eq!(plan.failures, []);
    let rels: Vec<_> = plan
        .items
        .iter()
        .filter_map(|item| match item {
            Item::Transfer { rel, .. } => Some(rel.as_str()),
            _ => None,
        })
        .collect();
    // The slower folder finishes last but stays first.
    assert_eq!(rels, ["a/1", "b.txt", "c/1"]);
    // One after the other the two listings would take 500 ms.
    assert!(started.elapsed() < std::time::Duration::from_millis(500));
}
