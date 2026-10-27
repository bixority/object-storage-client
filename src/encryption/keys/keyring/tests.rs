use super::*;
use crate::encryption::keys::KeyProvider;

const fn day(y: i32, m: u32, d: u32) -> NaiveDate {
    match NaiveDate::from_ymd_opt(y, m, d) {
        Some(d) => d,
        None => panic!("date"),
    }
}

fn ctx() -> EncryptionContext {
    EncryptionContext::new_reserved(Some("a/b"))
}

fn dek() -> DataKey {
    DataKey::from_slice(&[9; 32]).expect("32 bytes")
}

/// `{prefix}-{first 8 hex of the check}`.
fn id_for(prefix: &str, material: &[u8; 32]) -> String {
    format!(
        "{prefix}-{}",
        crate::hex::encode(&check_value(material)[..4])
    )
}

/// A `key=` line with the fields in the canonical order.
fn key_line(id: &str, created: &str, material: &[u8; 32]) -> String {
    format!(
        "key={id} created={created} material={} check={}",
        crate::hex::encode(material),
        crate::hex::encode(&check_value(material))
    )
}

const OLD: [u8; 32] = [1; 32];
const NEW: [u8; 32] = [2; 32];

fn old_id() -> String {
    id_for("kek-2025-10", &OLD)
}

fn new_id() -> String {
    id_for("kek-2026-10", &NEW)
}

/// Two keys, the newer one active.
fn sample() -> String {
    format!(
        "{MAGIC}\nactive={}\n{}\n{}\n",
        new_id(),
        key_line(&old_id(), "2025-10-20", &OLD),
        key_line(&new_id(), "2026-10-26", &NEW),
    )
}

/// Replaces the first occurrence of `from` in `sample()`.
fn edited(from: &str, to: &str) -> String {
    assert!(sample().contains(from), "{from}");
    sample().replacen(from, to, 1)
}

fn parse_err(text: &str) -> KeyError {
    Keyring::parse(text).expect_err("rejected")
}

fn line_of(err: &KeyError) -> usize {
    match err {
        KeyError::Malformed { line, .. } => *line,
        other => panic!("not Malformed: {other:?}"),
    }
}

fn assert_no_material(text: &str) {
    for secret in [OLD, NEW] {
        assert!(!text.contains(&crate::hex::encode(&secret)), "{text}");
        assert!(!text.contains(&format!("{secret:?}")), "{text}");
    }
}

#[test]
fn a_valid_keyring_parses_and_reports_its_keys() {
    let ring = Keyring::parse(&sample()).expect("parse");
    assert_eq!(ring.active_key_name(), new_id());
    assert_eq!(ring.namespace(), "osc-file");
    let status = ring.status_on(day(2026, 10, 28));
    assert_eq!(status.active, new_id());
    assert_eq!(status.active_age_days, 2);
    assert_eq!(status.last_reload_error, None);
    assert_eq!(
        status.keys,
        [
            KeyInfo {
                id: new_id(),
                created: day(2026, 10, 26)
            },
            KeyInfo {
                id: old_id(),
                created: day(2025, 10, 20)
            },
        ]
    );
}

#[test]
fn bom_crlf_blank_lines_whitespace_and_any_order_are_accepted() {
    let (old, new) = (old_id(), new_id());
    let text = format!(
        "\u{feff}\r\n  {MAGIC}  \r\n\r\n\t{}\r\n  key={old}\tcheck={}  material={}   created=2025-10-20 \r\nactive={new}\r\n\r\n",
        key_line(&new, "2026-10-26", &NEW),
        crate::hex::encode(&check_value(&OLD)),
        crate::hex::encode(&OLD),
    );
    let ring = Keyring::parse(&text).expect("parse");
    assert_eq!(ring.active_key_name(), new);
    assert_eq!(ring.status().keys.len(), 2);
}

#[test]
fn each_malformed_case_is_rejected_with_its_line() {
    let (old, new) = (old_id(), new_id());
    let old_line = key_line(&old, "2025-10-20", &OLD);
    let material = crate::hex::encode(&OLD);
    let check = crate::hex::encode(&check_value(&OLD));
    let cases: Vec<(String, usize)> = vec![
        // header
        (sample().replacen(MAGIC, "osc-keyring-v2", 1), 1),
        (sample().replacen(MAGIC, "", 1), 2),
        (String::new(), 0),
        (format!("{MAGIC}\n"), 0),
        // lines
        (edited("active=", "activ="), 2),
        (edited(&old_line, &format!("{old_line}\nsurprise")), 4),
        (edited(&old_line, &old_line.replacen("key=", "KEY=", 1)), 3),
        // fields
        (edited(&old_line, &format!("{old_line} extra=1")), 3),
        (
            edited(&old_line, &format!("{old_line} created=2025-10-21")),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen(" created=2025-10-20", "", 1)),
            3,
        ),
        (
            edited(
                &old_line,
                &old_line.replacen(&format!(" check={check}"), "", 1),
            ),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen(" material=", " material", 1)),
            3,
        ),
        // values
        (
            edited(
                &old_line,
                &old_line.replacen(&format!("key={old}"), "key=", 1),
            ),
            3,
        ),
        (
            edited(
                &old_line,
                &old_line.replacen(&format!("key={old}"), "key=a/b", 1),
            ),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen("2025-10-20", "2025-13-20", 1)),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen("2025-10-20", "20251020", 1)),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen(&material, &material[..62], 1)),
            3,
        ),
        (
            edited(
                &old_line,
                &old_line.replacen(&material, &format!("{}zz", &material[..62]), 1),
            ),
            3,
        ),
        (
            edited(&old_line, &old_line.replacen(&check, &check[..14], 1)),
            3,
        ),
        (
            edited(
                &old_line,
                &old_line.replacen(&check, &format!("{}zz", &check[..14]), 1),
            ),
            3,
        ),
        // active
        (
            edited(
                &format!("active={new}"),
                &format!("active={new}\nactive={new}"),
            ),
            3,
        ),
        (edited(&format!("active={new}\n"), ""), 0),
        (edited(&format!("active={new}"), "active="), 2),
    ];
    for (text, line) in cases {
        let err = parse_err(&text);
        assert_eq!(line_of(&err), line, "{err}\n{text}");
        assert_no_material(&err.to_string());
        assert_no_material(&format!("{err:?}"));
    }
}

#[test]
fn duplicates_and_active_problems() {
    let old_line = key_line(&old_id(), "2025-10-20", &OLD);
    // Identical duplicate lines are rejected too.
    let err = parse_err(&edited(&old_line, &format!("{old_line}\n{old_line}")));
    assert!(
        matches!(&err, KeyError::DuplicateId { id } if *id == old_id()),
        "{err:?}"
    );
    // Same ID, other material: still a duplicate (or a failed check).
    let other = key_line(&old_id(), "2025-10-20", &[3; 32]);
    assert!(matches!(
        parse_err(&edited(&old_line, &format!("{old_line}\n{other}"))),
        KeyError::BadCheck { .. } | KeyError::IdMismatch { .. } | KeyError::DuplicateId { .. }
    ));
    // Unknown active.
    let err = parse_err(&edited(&format!("active={}", new_id()), "active=nope"));
    assert!(
        matches!(&err, KeyError::ActiveKeyMissing { id } if id == "nope"),
        "{err:?}"
    );
}

#[test]
fn check_value_and_id_suffix_must_match() {
    let new = new_id();
    let line = key_line(&new, "2026-10-26", &NEW);
    // Wrong check value for the material.
    let check = crate::hex::encode(&check_value(&NEW));
    let flipped = format!(
        "{}{}",
        &check[..15],
        if check.ends_with('0') { '1' } else { '0' }
    );
    let err = parse_err(&edited(&line, &line.replacen(&check, &flipped, 1)));
    assert!(
        matches!(&err, KeyError::BadCheck { id } if *id == new),
        "{err:?}"
    );
    assert_no_material(&err.to_string());
    // Material that does not belong to the check.
    let err = parse_err(&edited(
        &line,
        &line.replacen(&crate::hex::encode(&NEW), &crate::hex::encode(&[7; 32]), 1),
    ));
    assert!(matches!(err, KeyError::BadCheck { .. }), "{err:?}");
    // The ID does not end in the check prefix (also by case).
    let suffix = &new[new.len() - 8..];
    for wrong in ["deadbeef".to_owned(), suffix.to_uppercase()] {
        let id = new.replace(suffix, &wrong);
        let text = sample().replace(&new, &id);
        let err = parse_err(&text);
        assert!(
            matches!(&err, KeyError::IdMismatch { id: got } if *got == id),
            "{err:?}"
        );
    }
}

#[test]
fn size_and_key_count_caps() {
    // Exactly 64 KiB passes, one more byte does not.
    let mut text = sample();
    text.push_str(&"\n".repeat(MAX_KEYRING_LEN - text.len()));
    assert_eq!(text.len(), MAX_KEYRING_LEN);
    Keyring::parse(&text).expect("at the cap");
    text.push('\n');
    assert_eq!(line_of(&parse_err(&text)), 0);

    let entries = |n: usize| {
        let keys: Vec<Entry> = (0..n)
            .map(|i| {
                let mut bytes = [0u8; 32];
                bytes[..8].copy_from_slice(&(i as u64).to_be_bytes());
                bytes[31] = 1;
                Entry {
                    id: id_for(&format!("k{i}"), &bytes),
                    created: day(2026, 1, 1),
                    material: Box::new(Zeroizing::new(bytes)),
                }
            })
            .collect();
        Document {
            active: keys[0].id.clone(),
            keys,
        }
    };
    let full = serialize(&entries(256)).expect("text");
    assert!(full.len() < MAX_KEYRING_LEN);
    assert_eq!(
        Keyring::parse(&full).expect("256 keys").status().keys.len(),
        256
    );
    let over = serialize(&entries(257)).expect("text");
    assert!(matches!(
        parse_err(&over),
        KeyError::TooManyKeys { max: 256 }
    ));
    assert!(matches!(
        rotate_keyring(&full, "kek", day(2026, 10, 28)).expect_err("full"),
        KeyError::TooManyKeys { .. }
    ));
}

#[test]
fn nothing_prints_key_material() {
    let ring = Keyring::parse(&sample()).expect("parse");
    assert_no_material(&format!("{ring:?}"));
    assert_no_material(&format!("{:?}", ring.status()));
    assert!(format!("{ring:?}").contains(&new_id()));
}

#[tokio::test]
async fn wrap_and_unwrap_round_trip_and_count() {
    let ring = Keyring::parse(&sample()).expect("parse");
    let edks = ring.wrap(&dek(), &ctx()).await.expect("wrap");
    assert_eq!(ring.wraps(), 1);
    assert_eq!(edk_name(&edks[0]), new_id());
    let got = ring.unwrap(&edks, &ctx()).await.expect("unwrap");
    assert_eq!(got.as_bytes(), &[9; 32]);
    // An old key still opens its EDKs.
    let old = RawAesKey::new(old_id(), &OLD)
        .wrap(NAMESPACE, &dek(), &ctx().serialize().expect("aad"))
        .expect("wrap");
    assert_eq!(
        ring.unwrap(&[old], &ctx()).await.expect("old").as_bytes(),
        &[9; 32]
    );
}

fn edk_name(edk: &EncryptedDataKey) -> String {
    super::super::raw_aes::edk_key_name(edk)
        .expect("name")
        .to_owned()
}

/// An EDK for `ctx()` wrapped by a key with `id` and `material`.
fn edk_for(id: &str, material: &[u8; 32]) -> EncryptedDataKey {
    RawAesKey::new(id.to_owned(), material)
        .wrap(NAMESPACE, &dek(), &ctx().serialize().expect("aad"))
        .expect("wrap")
}

#[test]
fn status_sorts_newest_first_with_ties_by_id() {
    let mk = |prefix: &str, created: &str, byte: u8| {
        let m = [byte; 32];
        (
            id_for(prefix, &m),
            key_line(&id_for(prefix, &m), created, &m),
        )
    };
    let (a, la) = mk("a", "2026-01-01", 1);
    let (b, lb) = mk("b", "2026-01-01", 2);
    let (c, lc) = mk("c", "2027-06-30", 3);
    let (d, ld) = mk("d", "2020-02-29", 4);
    let text = format!("{MAGIC}\nactive={d}\n{lb}\n{ld}\n{la}\n{lc}\n");
    let status = Keyring::parse(&text)
        .expect("parse")
        .status_on(day(2020, 3, 10));
    let ids: Vec<_> = status.keys.iter().map(|k| k.id.clone()).collect();
    let (first, second) = if a < b { (&a, &b) } else { (&b, &a) };
    assert_eq!(ids, [c, first.clone(), second.clone(), d]);
    assert_eq!(status.active_age_days, 10);
}

#[test]
fn new_rotate_and_retire_round_trip() {
    let today = day(2026, 10, 28);
    let first = new_keyring("kek", today).expect("new");
    assert!(first.ends_with('\n') && !first.contains('\r'));
    let ring = Keyring::parse(&first).expect("parse");
    let status = ring.status();
    assert_eq!(status.keys.len(), 1);
    assert!(
        status.active.starts_with("kek-2026-10-"),
        "{}",
        status.active
    );
    assert_eq!(status.keys[0].created, today);
    assert_eq!(status.active.len(), "kek-2026-10-".len() + 8);

    let second = rotate_keyring(&first, "kek", day(2027, 10, 1)).expect("rotate");
    let after = Keyring::parse(&second).expect("parse").status();
    assert_eq!(after.keys.len(), 2);
    assert!(after.active.starts_with("kek-2027-10-"));
    assert_ne!(after.active, status.active);
    // The old key line is kept byte for byte, in front; the new one is last.
    let lines: Vec<&str> = second.lines().collect();
    assert_eq!(lines[0], MAGIC);
    assert_eq!(lines[1], format!("active={}", after.active));
    assert_eq!(lines[2], first.lines().nth(2).expect("old key line"));
    assert!(lines[3].starts_with(&format!("key={} created=2027-10-01 ", after.active)));

    // Retire the old key: refuses the active and unknown IDs.
    let third = retire_key(&second, &status.active).expect("retire");
    let left = Keyring::parse(&third).expect("parse").status();
    assert_eq!(
        left.keys,
        [KeyInfo {
            id: after.active.clone(),
            created: day(2027, 10, 1)
        }]
    );
    assert!(matches!(
        retire_key(&second, &after.active).expect_err("active"),
        KeyError::RetireActive { .. }
    ));
    assert!(matches!(
        retire_key(&second, "nope").expect_err("unknown"),
        KeyError::UnknownId { id } if id == "nope"
    ));
}

#[test]
fn operations_normalize_and_validate() {
    let messy = format!("\u{feff}{}", sample().replace('\n', "\r\n"));
    let out = retire_key(&messy, &old_id()).expect("retire");
    assert!(out.starts_with(MAGIC) && !out.contains('\r') && out.ends_with('\n'));
    assert_eq!(out.lines().count(), 3);
    assert_eq!(
        *out,
        format!(
            "{MAGIC}\nactive={}\n{}\n",
            new_id(),
            key_line(&new_id(), "2026-10-26", &NEW)
        )
    );
    // Input order is kept.
    let rotated = rotate_keyring(&sample(), "kek", day(2026, 10, 28)).expect("rotate");
    let ids: Vec<_> = rotated
        .lines()
        .skip(2)
        .map(|l| l.split(' ').next().expect("key").to_owned())
        .collect();
    assert_eq!(ids[0], format!("key={}", old_id()));
    assert_eq!(ids[1], format!("key={}", new_id()));
    // Invalid input and prefixes are refused.
    assert!(rotate_keyring("junk", "kek", day(2026, 1, 1)).is_err());
    assert!(retire_key("junk", "x").is_err());
    for bad in ["", "a b", "a/b", &"x".repeat(97)] {
        assert!(new_keyring(bad, day(2026, 1, 1)).is_err(), "{bad:?}");
    }
    assert!(new_keyring(&"x".repeat(96), day(2026, 1, 1)).is_ok());
}

// --- file rules -----------------------------------------------------------

fn write_private(path: &Path, text: &str) {
    std::fs::write(path, text).expect("write");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
}

/// Atomically replaces `path` with a private file holding `text`.
fn replace(path: &Path, text: &str) {
    let tmp = path.with_extension("tmp");
    write_private(&tmp, text);
    std::fs::rename(&tmp, path).expect("rename");
}

fn ring_at(path: &Path) -> Keyring {
    Keyring::from_file_with(path, Duration::ZERO, Duration::ZERO).expect("open")
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn symlinks_are_followed() {
        let dir = tempfile::tempdir().expect("dir");
        write_private(&dir.path().join("real"), &sample());
        std::os::unix::fs::symlink("real", dir.path().join("link")).expect("symlink");
        let ring = Keyring::from_file(dir.path().join("link")).expect("open");
        assert_eq!(ring.active_key_name(), new_id());
    }

    #[test]
    fn access_for_others_is_refused_group_read_is_not() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        for (mode, ok) in [
            (0o600, true),
            (0o640, true),
            (0o440, true),
            (0o604, false),
            (0o601, false),
            (0o644, false),
            (0o660, true),
        ] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).expect("chmod");
            let res = Keyring::from_file(&path);
            if ok {
                res.expect("accepted");
            } else {
                assert!(
                    matches!(res.expect_err("refused"), KeyError::Permissions { mode: m, .. } if m == mode),
                    "{mode:o}"
                );
            }
        }
    }

    #[test]
    fn owner_must_be_the_user_or_root() {
        let p = Path::new("/k");
        check_owner_and_mode(p, 0o600, 1000, 1000).expect("own");
        check_owner_and_mode(p, 0o640, 0, 1000).expect("root");
        assert!(matches!(
            check_owner_and_mode(p, 0o600, 1234, 1000),
            Err(KeyError::NotOwner { .. })
        ));
        assert!(matches!(
            check_owner_and_mode(p, 0o606, 1000, 1000),
            Err(KeyError::Permissions { .. })
        ));
    }

    #[test]
    fn a_fifo_in_place_of_the_file_does_not_hang() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        rustix::fs::mknodat(
            rustix::fs::CWD,
            &path,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::from_raw_mode(0o600),
            0,
        )
        .expect("mkfifo");
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || tx.send(Keyring::from_file(path).err()).expect("send"));
        let err = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("returned")
            .expect("refused");
        assert!(matches!(err, KeyError::NotRegularFile { .. }), "{err:?}");
    }

    #[test]
    fn a_directory_and_a_missing_file_are_refused() {
        let dir = tempfile::tempdir().expect("dir");
        assert!(matches!(
            Keyring::from_file(dir.path()).expect_err("dir"),
            KeyError::NotRegularFile { .. }
        ));
        assert!(matches!(
            Keyring::from_file(dir.path().join("nope")).expect_err("missing"),
            KeyError::Io { .. }
        ));
    }

    #[test]
    fn an_oversized_file_is_refused() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(
            &path,
            &format!("{}{}", sample(), "\n".repeat(MAX_KEYRING_LEN)),
        );
        assert_eq!(line_of(&Keyring::from_file(&path).expect_err("big")), 0);
    }

    // --- reload ---

    #[test]
    fn an_unchanged_file_is_not_read_again() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);
        // Same inode, size and mtime, but unreadable content: only a re-read
        // would notice.
        let mtime = std::fs::metadata(&path)
            .expect("meta")
            .modified()
            .expect("mtime");
        let broken = sample().replacen(MAGIC, "xsc-keyring-v1", 1);
        std::fs::write(&path, broken).expect("write");
        File::options()
            .write(true)
            .open(&path)
            .expect("open")
            .set_modified(mtime)
            .expect("mtime");
        ring.check(Check::Due);
        assert_eq!(ring.status().last_reload_error, None);
        assert_eq!(ring.active_key_name(), new_id());
        // A real change is noticed.
        std::fs::write(&path, "x").expect("write");
        ring.check(Check::Due);
        assert!(ring.status().last_reload_error.is_some());
    }

    #[tokio::test]
    async fn a_kubernetes_data_symlink_swap_is_picked_up() {
        let dir = tempfile::tempdir().expect("dir");
        let root = dir.path();
        std::fs::create_dir(root.join("..2026_a")).expect("dir");
        write_private(&root.join("..2026_a/keyring"), &sample());
        std::os::unix::fs::symlink("..2026_a", root.join("..data")).expect("data");
        std::os::unix::fs::symlink("..data/keyring", root.join("keyring")).expect("link");
        let ring = ring_at(&root.join("keyring"));
        assert_eq!(ring.active_key_name(), new_id());

        let rotated = rotate_keyring(&sample(), "kek", day(2026, 10, 28)).expect("rotate");
        std::fs::create_dir(root.join("..2026_b")).expect("dir");
        write_private(&root.join("..2026_b/keyring"), &rotated);
        std::os::unix::fs::symlink("..2026_b", root.join("..data_tmp")).expect("tmp");
        std::fs::rename(root.join("..data_tmp"), root.join("..data")).expect("swap");

        ring.wrap(&dek(), &ctx()).await.expect("wrap");
        let status = ring.status();
        assert_eq!(status.keys.len(), 3);
        assert_ne!(status.active, new_id());
        assert_eq!(status.last_reload_error, None);
    }

    #[tokio::test]
    async fn the_interval_limits_checks_and_an_unknown_key_forces_one() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(
            &path,
            &format!(
                "{MAGIC}\nactive={}\n{}\n",
                old_id(),
                key_line(&old_id(), "2025-10-20", &OLD)
            ),
        );
        let ring =
            Keyring::from_file_with(&path, Duration::from_secs(3600), Duration::from_secs(3600))
                .expect("open");
        replace(&path, &sample());

        // Not due: wrap does not look.
        ring.wrap(&dek(), &ctx()).await.expect("wrap");
        assert_eq!(ring.active_key_name(), old_id());
        // An EDK of the new key forces one check, which finds it.
        let edk = edk_for(&new_id(), &NEW);
        ring.unwrap(&[edk], &ctx())
            .await
            .expect("found after reload");
        assert_eq!(ring.active_key_name(), new_id());

        // The next forced check is a long way off: another new key is not found.
        let third = [3u8; 32];
        let third_id = id_for("kek-2027-10", &third);
        let text = format!(
            "{}{}\n",
            sample(),
            key_line(&third_id, "2027-10-01", &third)
        );
        replace(&path, &text);
        let err = ring
            .unwrap(&[edk_for(&third_id, &third)], &ctx())
            .await
            .err()
            .expect("limited");
        assert!(matches!(err, KeyError::UnknownKey { .. }), "{err:?}");
        assert_eq!(ring.status().keys.len(), 2);
    }

    #[tokio::test]
    async fn an_unknown_key_after_a_check_that_found_nothing_is_unknown() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);
        let err = ring
            .unwrap(&[edk_for("kek-other", &[5; 32])], &ctx())
            .await
            .err()
            .expect("unknown");
        assert!(matches!(err, KeyError::UnknownKey { .. }), "{err:?}");
        // A fixed keyring has nothing to look at.
        let fixed = Keyring::parse(&sample()).expect("parse");
        let err = fixed
            .unwrap(&[edk_for("kek-other", &[5; 32])], &ctx())
            .await
            .err()
            .expect("unknown");
        assert!(matches!(err, KeyError::UnknownKey { .. }), "{err:?}");
    }

    #[test]
    fn an_invalid_replacement_keeps_the_old_keys_until_a_valid_one_arrives() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);

        replace(&path, "garbage\n");
        ring.check(Check::Due);
        let status = ring.status();
        assert_eq!(status.active, new_id());
        assert_eq!(status.keys.len(), 2);
        let why = status.last_reload_error.expect("error is reported");
        assert!(why.contains("line 1"), "{why}");
        assert_no_material(&why);

        // A file with wrong permissions is invalid too.
        replace(&path, &sample());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("chmod");
        ring.check(Check::Due);
        assert!(
            ring.status()
                .last_reload_error
                .expect("perm")
                .contains("mode 644")
        );

        let one = retire_key(&sample(), &old_id()).expect("retire");
        replace(&path, &one);
        ring.check(Check::Due);
        let status = ring.status();
        assert_eq!(status.last_reload_error, None);
        assert_eq!(status.keys.len(), 1);
    }

    #[test]
    fn a_file_that_disappears_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);
        std::fs::remove_file(&path).expect("rm");
        ring.check(Check::Due);
        assert!(ring.status().last_reload_error.is_some());
        assert_eq!(ring.status().keys.len(), 2);
        // A file that is back (a new one) is read again.
        write_private(&path, &sample());
        ring.check(Check::Due);
        assert_eq!(ring.status().last_reload_error, None);
    }

    #[tokio::test]
    async fn a_key_removed_by_a_reload_does_not_break_a_finished_unwrap() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);
        let edk = edk_for(&old_id(), &OLD);
        let got = ring
            .unwrap(std::slice::from_ref(&edk), &ctx())
            .await
            .expect("unwrap");

        replace(&path, &retire_key(&sample(), &old_id()).expect("retire"));
        ring.check(Check::Due);
        assert_eq!(ring.status().keys.len(), 1);
        // The data key already recovered is unaffected; new reads of the old
        // key fail.
        assert_eq!(got.as_bytes(), &[9; 32]);
        assert!(matches!(
            ring.unwrap(&[edk], &ctx()).await.err().expect("gone"),
            KeyError::UnknownKey { .. }
        ));
    }

    #[tokio::test]
    async fn wrap_counts_survive_a_reload_that_keeps_the_key() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("keyring");
        write_private(&path, &sample());
        let ring = ring_at(&path);
        ring.wrap(&dek(), &ctx()).await.expect("wrap");
        ring.wrap(&dek(), &ctx()).await.expect("wrap");
        assert_eq!(ring.wraps(), 2);

        // Another (inactive) key appended: the active key is the same.
        let extra = [4u8; 32];
        let extra_line = key_line(&id_for("kek-2024-10", &extra), "2024-10-01", &extra);
        replace(&path, &format!("{}{extra_line}\n", sample()));
        ring.check(Check::Due);
        assert_eq!(ring.status().keys.len(), 3);
        assert_eq!(ring.wraps(), 2);
        ring.wrap(&dek(), &ctx()).await.expect("wrap");
        assert_eq!(ring.wraps(), 3);

        // A rotation makes another key active; its counter starts at 0.
        replace(
            &path,
            &rotate_keyring(&sample(), "kek", day(2026, 10, 28)).expect("rotate"),
        );
        ring.check(Check::Due);
        assert_eq!(ring.wraps(), 0);
    }
}
