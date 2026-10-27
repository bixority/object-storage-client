//! The injected, reloadable [`Keyring`].
//!
//! A keyring is one UTF-8 text document (CRLF, a leading byte order mark and
//! blank lines are accepted):
//!
//! ```text
//! osc-keyring-v1
//! active=kek-2026-10-3f9a1b2c
//! key=kek-2026-10-3f9a1b2c created=2026-10-27 material=<64 hex> check=<16 hex>
//! key=kek-2025-10-8be01d44 created=2025-10-20 material=<64 hex> check=<16 hex>
//! ```
//!
//! `check` is the first 8 bytes of `HMAC-SHA256(material,
//! "osc-key-check-v1")`, and every key ID ends in the first 8 hex digits of
//! it. The `created`, `material` and `check` fields of a `key=` line may come
//! in any order. At most 256 keys and 64 KiB.
//!
//! A file-backed keyring ([`Keyring::from_file`]) follows symbolic links
//! (Kubernetes mounts every Secret key as one), opens the file without
//! blocking, and requires a regular file owned by the effective user or root
//! with no access for others (Unix). It notices a changed file on use: at
//! most every 5 seconds, and at once when an unwrap meets an unknown key (at
//! most once a second). An invalid replacement keeps the
//! previous keys and is reported by [`KeyringStatus::last_reload_error`].
//!
//! This is the only part of the encryption module that touches the file
//! system. Key bytes live in zeroizing buffers and never reach an error,
//! `Debug` output or log.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock, TryLockError};
use std::time::{Duration, Instant, SystemTime};

use async_trait::async_trait;
use aws_lc_rs::constant_time::verify_slices_are_equal;
use aws_lc_rs::hmac;
use aws_lc_rs::rand;
use chrono::{Datelike, NaiveDate};
use zeroize::Zeroizing;

use super::raw_aes::{RawAesKey, unwrap_edks};
use super::{DataKey, KeyError, KeyProvider};
use crate::encryption::context::EncryptionContext;
use crate::encryption::header::EncryptedDataKey;

/// Provider ID written into EDKs.
const NAMESPACE: &str = "osc-file";
const MAGIC: &str = "osc-keyring-v1";
const CHECK_LABEL: &[u8] = b"osc-key-check-v1";
const MAX_ID_LEN: usize = 128;
/// Upper bound on a keyring document.
pub(crate) const MAX_KEYRING_LEN: usize = 64 * 1024;
const MAX_KEYS: usize = 256;
/// How often a file-backed keyring looks at its file while in use.
const RELOAD_INTERVAL: Duration = Duration::from_secs(5);
/// Least time between two checks forced by an unknown key.
const FORCED_INTERVAL: Duration = Duration::from_secs(1);

type Material = Box<Zeroizing<[u8; 32]>>;

/// A set of key-encryption keys. Every key unwraps; exactly one (the
/// `active` key) wraps.
pub struct Keyring {
    snapshot: RwLock<Arc<Snapshot>>,
    /// `None` for a fixed keyring.
    source: Option<Source>,
}

/// An immutable view of the keys; replaced as a whole on reload.
struct Snapshot {
    active: String,
    keys: BTreeMap<String, Arc<RawAesKey>>,
    /// Kept to recognize an unchanged key on reload.
    materials: BTreeMap<String, Material>,
    /// Newest first, ties by ID.
    info: Vec<KeyInfo>,
}

struct Source {
    path: PathBuf,
    interval: Duration,
    forced_interval: Duration,
    state: Mutex<ReloadState>,
}

struct ReloadState {
    /// Of the file the current snapshot was read from.
    identity: Identity,
    last_check: Instant,
    last_forced: Option<Instant>,
    last_error: Option<String>,
}

/// What identifies the content of a file without reading it.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Identity {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    modified: Option<SystemTime>,
    len: u64,
}

impl Identity {
    fn of(meta: &std::fs::Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            #[cfg(unix)]
            dev: meta.dev(),
            #[cfg(unix)]
            ino: meta.ino(),
            modified: meta.modified().ok(),
            len: meta.len(),
        }
    }
}

/// The state of a [`Keyring`] for health checks. Never carries key material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyringStatus {
    /// ID of the key new objects are wrapped with.
    pub active: String,
    /// Every key, newest first (ties by ID).
    pub keys: Vec<KeyInfo>,
    /// Days from the active key's creation date to today (UTC).
    pub active_age_days: i64,
    /// Why the file on disk was not loaded; `None` while it is valid.
    pub last_reload_error: Option<String>,
}

/// One key of a [`KeyringStatus`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyInfo {
    pub id: String,
    pub created: NaiveDate,
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.current();
        f.debug_struct("Keyring")
            .field("active", &snap.active)
            .field("keys", &snap.keys.keys().collect::<Vec<_>>())
            .field("path", &self.source.as_ref().map(|s| &s.path))
            .finish_non_exhaustive()
    }
}

impl Keyring {
    /// Loads and validates the file. Later changes are picked up on use.
    ///
    /// # Errors
    /// [`KeyError::Io`], [`KeyError::NotRegularFile`] and (Unix)
    /// [`KeyError::Permissions`] / [`KeyError::NotOwner`] for the file, then
    /// any parse or validation error of the document.
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, KeyError> {
        Self::from_file_with(path, RELOAD_INTERVAL, FORCED_INTERVAL)
    }

    /// [`Keyring::from_file`] with other reload intervals (tests).
    pub(crate) fn from_file_with(
        path: impl Into<PathBuf>,
        interval: Duration,
        forced_interval: Duration,
    ) -> Result<Self, KeyError> {
        let path = path.into();
        let (raw, identity) = read_file(&path)?;
        let snapshot = Snapshot::build(parse_bytes(&raw)?, None);
        Ok(Self {
            snapshot: RwLock::new(Arc::new(snapshot)),
            source: Some(Source {
                path,
                interval,
                forced_interval,
                state: Mutex::new(ReloadState {
                    identity,
                    last_check: Instant::now(),
                    last_forced: None,
                    last_error: None,
                }),
            }),
        })
    }

    /// A fixed keyring from text; it never reloads.
    ///
    /// # Errors
    /// [`KeyError::Malformed`] with the line number, [`KeyError::BadCheck`],
    /// [`KeyError::IdMismatch`], [`KeyError::DuplicateId`],
    /// [`KeyError::ActiveKeyMissing`] or [`KeyError::TooManyKeys`].
    pub fn parse(text: &str) -> Result<Self, KeyError> {
        let snapshot = Snapshot::build(parse_document(text)?, None);
        Ok(Self {
            snapshot: RwLock::new(Arc::new(snapshot)),
            source: None,
        })
    }

    /// A keyring over in-memory keys, for unit tests.
    #[cfg(test)]
    pub(crate) fn from_material(active: &str, keys: &[(&str, &[u8; 32])]) -> Self {
        let doc = Document {
            active: active.to_owned(),
            keys: keys
                .iter()
                .map(|(id, m)| Entry {
                    id: (*id).to_owned(),
                    created: NaiveDate::default(),
                    material: Box::new(Zeroizing::new(**m)),
                })
                .collect(),
        };
        Self {
            snapshot: RwLock::new(Arc::new(Snapshot::build(doc, None))),
            source: None,
        }
    }

    /// The keys, the active key's age today (UTC) and the last reload error.
    #[must_use]
    pub fn status(&self) -> KeyringStatus {
        self.status_on(chrono::Utc::now().date_naive())
    }

    /// [`Keyring::status`] as of `today`.
    pub(crate) fn status_on(&self, today: NaiveDate) -> KeyringStatus {
        let snap = self.current();
        let age = snap
            .info
            .iter()
            .find(|k| k.id == snap.active)
            .map_or(0, |k| (today - k.created).num_days());
        let last_reload_error = self.source.as_ref().and_then(|s| {
            s.state
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .last_error
                .clone()
        });
        KeyringStatus {
            active: snap.active.clone(),
            keys: snap.info.clone(),
            active_age_days: age,
            last_reload_error,
        }
    }

    /// Number of wrap operations with the current active key in this process
    /// (not persisted; it survives a reload that keeps the key).
    #[must_use]
    pub fn wraps(&self) -> u64 {
        let snap = self.current();
        snap.keys.get(&snap.active).map_or(0, |k| k.wraps())
    }

    fn current(&self) -> Arc<Snapshot> {
        Arc::clone(&self.snapshot.read().unwrap_or_else(PoisonError::into_inner))
    }

    /// Looks at the file if a check is due (or, for [`Check::Forced`], if
    /// the last forced one is old enough) and swaps in a changed, valid
    /// keyring. A task that finds the check running carries on with the
    /// current snapshot.
    fn check(&self, mode: Check) {
        let Some(src) = &self.source else { return };
        let mut st = match src.state.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::Poisoned(p)) => p.into_inner(),
            Err(TryLockError::WouldBlock) => return,
        };
        let now = Instant::now();
        match mode {
            Check::Due if now.duration_since(st.last_check) < src.interval => return,
            Check::Forced
                if st
                    .last_forced
                    .is_some_and(|t| now.duration_since(t) < src.forced_interval) =>
            {
                return;
            }
            Check::Forced => st.last_forced = Some(now),
            Check::Due => {}
        }
        st.last_check = now;

        let meta = match std::fs::metadata(&src.path) {
            Ok(meta) => meta,
            Err(e) => {
                st.last_error = Some(KeyError::io(&src.path)(e).to_string());
                return;
            }
        };
        if Identity::of(&meta) == st.identity {
            st.last_error = None;
            return;
        }
        let loaded = read_file(&src.path).and_then(|(raw, identity)| {
            let doc = parse_bytes(&raw)?;
            Ok((doc, identity))
        });
        match loaded {
            Ok((doc, identity)) => {
                let fresh = Arc::new(Snapshot::build(doc, Some(&self.current())));
                *self
                    .snapshot
                    .write()
                    .unwrap_or_else(PoisonError::into_inner) = fresh;
                st.identity = identity;
                st.last_error = None;
            }
            Err(e) => st.last_error = Some(e.to_string()),
        }
    }
}

#[derive(Clone, Copy)]
enum Check {
    /// Only when the regular interval has passed.
    Due,
    /// Ignores the regular interval; limited to one per forced interval.
    Forced,
}

#[async_trait]
impl KeyProvider for Keyring {
    fn namespace(&self) -> &str {
        NAMESPACE
    }

    fn active_key_name(&self) -> String {
        self.current().active.clone()
    }

    async fn wrap(
        &self,
        dek: &DataKey,
        ctx: &EncryptionContext,
    ) -> Result<Vec<EncryptedDataKey>, KeyError> {
        self.check(Check::Due);
        let snap = self.current();
        let key = snap
            .keys
            .get(&snap.active)
            .ok_or_else(|| KeyError::ActiveKeyMissing {
                id: snap.active.clone(),
            })?;
        Ok(vec![key.wrap(NAMESPACE, dek, &ctx.serialize()?)?])
    }

    async fn unwrap(
        &self,
        edks: &[EncryptedDataKey],
        ctx: &EncryptionContext,
    ) -> Result<DataKey, KeyError> {
        self.check(Check::Due);
        let snap = self.current();
        let aad = ctx.serialize()?;
        let unknown = match unwrap_edks(NAMESPACE, &snap.keys, edks, &aad) {
            Err(e @ KeyError::UnknownKey { .. }) if self.source.is_some() => e,
            result => return result,
        };
        // The file may already hold the key: look now, then try once more.
        self.check(Check::Forced);
        let fresh = self.current();
        if Arc::ptr_eq(&snap, &fresh) {
            return Err(unknown);
        }
        unwrap_edks(NAMESPACE, &fresh.keys, edks, &aad)
    }
}

impl Snapshot {
    /// Keys that `previous` holds with the same ID and material keep their
    /// `RawAesKey` (and so their wrap counter).
    fn build(doc: Document, previous: Option<&Self>) -> Self {
        let mut keys = BTreeMap::new();
        let mut materials = BTreeMap::new();
        let mut info = Vec::with_capacity(doc.keys.len());
        for entry in doc.keys {
            let reused = previous.and_then(|p| {
                let same =
                    verify_slices_are_equal(&p.materials.get(&entry.id)?[..], &entry.material[..])
                        .is_ok();
                same.then(|| p.keys.get(&entry.id).map(Arc::clone))
                    .flatten()
            });
            let key = reused
                .unwrap_or_else(|| Arc::new(RawAesKey::new(entry.id.clone(), &entry.material)));
            info.push(KeyInfo {
                id: entry.id.clone(),
                created: entry.created,
            });
            keys.insert(entry.id.clone(), key);
            materials.insert(entry.id, entry.material);
        }
        info.sort_by(|a, b| b.created.cmp(&a.created).then_with(|| a.id.cmp(&b.id)));
        Self {
            active: doc.active,
            keys,
            materials,
            info,
        }
    }
}

// --- the document ---------------------------------------------------------

struct Entry {
    id: String,
    created: NaiveDate,
    material: Material,
}

struct Document {
    active: String,
    keys: Vec<Entry>,
}

const fn malformed(line: usize, reason: &'static str) -> KeyError {
    KeyError::Malformed { line, reason }
}

fn check_value(material: &[u8; 32]) -> [u8; 8] {
    let key = hmac::Key::new(hmac::HMAC_SHA256, material);
    let tag = hmac::sign(&key, CHECK_LABEL);
    let mut out = [0u8; 8];
    out.copy_from_slice(&tag.as_ref()[..8]);
    out
}

fn valid_id_chars(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

fn valid_id(id: &str) -> bool {
    valid_id_chars(id) && id.len() <= MAX_ID_LEN
}

fn valid_prefix(prefix: &str) -> bool {
    valid_id_chars(prefix) && prefix.len() <= MAX_ID_LEN - 32
}

fn parse_bytes(raw: &[u8]) -> Result<Document, KeyError> {
    parse_document(std::str::from_utf8(raw).map_err(|_| malformed(0, "not valid UTF-8"))?)
}

fn parse_document(text: &str) -> Result<Document, KeyError> {
    if text.len() > MAX_KEYRING_LEN {
        return Err(malformed(0, "larger than 64 KiB"));
    }
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let mut lines = text
        .lines()
        .enumerate()
        .map(|(i, l)| (i + 1, l.trim()))
        .filter(|(_, l)| !l.is_empty());
    match lines.next() {
        Some((_, MAGIC)) => {}
        Some((n, _)) => return Err(malformed(n, "missing osc-keyring-v1 header")),
        None => return Err(malformed(0, "empty keyring")),
    }
    let mut active = None;
    let mut keys: Vec<Entry> = Vec::new();
    for (n, line) in lines {
        if let Some(id) = line.strip_prefix("active=") {
            if !valid_id(id) {
                return Err(malformed(n, "invalid id"));
            }
            if active.replace(id).is_some() {
                return Err(malformed(n, "repeated active line"));
            }
        } else if let Some(rest) = line.strip_prefix("key=") {
            if keys.len() >= MAX_KEYS {
                return Err(KeyError::TooManyKeys { max: MAX_KEYS });
            }
            let entry = parse_key(n, rest)?;
            if keys.iter().any(|k| k.id == entry.id) {
                return Err(KeyError::DuplicateId { id: entry.id });
            }
            keys.push(entry);
        } else {
            return Err(malformed(n, "unknown line"));
        }
    }
    let active = active.ok_or_else(|| malformed(0, "missing active line"))?;
    if !keys.iter().any(|k| k.id == active) {
        return Err(KeyError::ActiveKeyMissing {
            id: active.to_owned(),
        });
    }
    Ok(Document {
        active: active.to_owned(),
        keys,
    })
}

/// Parses what follows `key=` on line `n`.
fn parse_key(n: usize, rest: &str) -> Result<Entry, KeyError> {
    let (id, fields) = rest.split_once([' ', '\t']).unwrap_or((rest, ""));
    if !valid_id(id) {
        return Err(malformed(n, "invalid id"));
    }
    let (mut created, mut material, mut check) = (None, None, None);
    for field in fields.split([' ', '\t']).filter(|f| !f.is_empty()) {
        let (name, value) = field
            .split_once('=')
            .ok_or_else(|| malformed(n, "field is not name=value"))?;
        let slot = match name {
            "created" => &mut created,
            "material" => &mut material,
            "check" => &mut check,
            _ => return Err(malformed(n, "unknown field")),
        };
        if slot.replace(value).is_some() {
            return Err(malformed(n, "repeated field"));
        }
    }
    let (Some(created), Some(material), Some(check)) = (created, material, check) else {
        return Err(malformed(n, "missing field"));
    };
    let created = NaiveDate::parse_from_str(created, "%Y-%m-%d")
        .map_err(|_| malformed(n, "invalid created date"))?;
    if material.len() != 64 || check.len() != 16 {
        return Err(malformed(n, "material or check has the wrong length"));
    }
    let decoded = Zeroizing::new(
        crate::hex::decode(material).ok_or_else(|| malformed(n, "material is not hex"))?,
    );
    let mut bytes = Box::new(Zeroizing::new([0u8; 32]));
    bytes.copy_from_slice(&decoded);
    let stated: [u8; 8] = crate::hex::decode(check)
        .and_then(|v| <[u8; 8]>::try_from(v).ok())
        .ok_or_else(|| malformed(n, "check is not hex"))?;

    let actual = check_value(&bytes);
    if verify_slices_are_equal(&actual, &stated).is_err() {
        return Err(KeyError::BadCheck { id: id.to_owned() });
    }
    // Exact and case-sensitive: IDs differing only in case are different EDK
    // key names and must not both pass.
    if !id.ends_with(&crate::hex::encode(&actual[..4])) {
        return Err(KeyError::IdMismatch { id: id.to_owned() });
    }
    Ok(Entry {
        id: id.to_owned(),
        created,
        material: bytes,
    })
}

/// The normalized text of `doc`: header, `active`, then the keys in order.
fn serialize(doc: &Document) -> Result<Zeroizing<String>, KeyError> {
    let cap = 32 + doc.active.len() + doc.keys.iter().map(|k| k.id.len() + 250).sum::<usize>();
    let mut out = Zeroizing::new(String::with_capacity(cap));
    let _ = writeln!(out, "{MAGIC}\nactive={}", doc.active);
    for k in &doc.keys {
        // Preallocated, so neither buffer reallocates and leaves a copy.
        let material = Zeroizing::new(crate::hex::encode(&k.material[..]));
        let _ = writeln!(
            out,
            "key={} created={} material={} check={}",
            k.id,
            k.created.format("%Y-%m-%d"),
            material.as_str(),
            crate::hex::encode(&check_value(&k.material)),
        );
    }
    if out.len() > MAX_KEYRING_LEN {
        return Err(malformed(0, "the result would be larger than 64 KiB"));
    }
    Ok(out)
}

/// A key with fresh random material and the ID
/// `{prefix}-{YYYY}-{MM}-{check[0..8]}`.
fn fresh_entry(prefix: &str, today: NaiveDate) -> Result<Entry, KeyError> {
    if !valid_prefix(prefix) {
        return Err(malformed(
            0,
            "key id prefix must be ASCII letters, digits, '-', '_' or '.'",
        ));
    }
    let mut material = Box::new(Zeroizing::new([0u8; 32]));
    rand::fill(&mut material[..]).map_err(|_| KeyError::Rng)?;
    let check = crate::hex::encode(&check_value(&material)[..4]);
    Ok(Entry {
        id: format!("{prefix}-{:04}-{:02}-{check}", today.year(), today.month()),
        created: today,
        material,
    })
}

/// A keyring with one fresh active key. The text holds key material.
///
/// # Errors
/// [`KeyError::Malformed`] for an unusable `prefix`, [`KeyError::Rng`] if
/// random generation fails (see that variant).
pub fn new_keyring(prefix: &str, today: NaiveDate) -> Result<Zeroizing<String>, KeyError> {
    let entry = fresh_entry(prefix, today)?;
    serialize(&Document {
        active: entry.id.clone(),
        keys: vec![entry],
    })
}

/// `text` with a fresh key appended and made active; the other keys stay.
///
/// # Errors
/// Any error of parsing `text`, [`KeyError::TooManyKeys`] at 256 keys,
/// [`KeyError::DuplicateId`] on an ID collision, [`KeyError::Malformed`] for
/// an unusable `prefix`, [`KeyError::Rng`].
pub fn rotate_keyring(
    text: &str,
    prefix: &str,
    today: NaiveDate,
) -> Result<Zeroizing<String>, KeyError> {
    let mut doc = parse_document(text)?;
    if doc.keys.len() >= MAX_KEYS {
        return Err(KeyError::TooManyKeys { max: MAX_KEYS });
    }
    let entry = fresh_entry(prefix, today)?;
    if doc.keys.iter().any(|k| k.id == entry.id) {
        return Err(KeyError::DuplicateId { id: entry.id });
    }
    doc.active.clone_from(&entry.id);
    doc.keys.push(entry);
    serialize(&doc)
}

/// `text` without the key `id`.
///
/// # Errors
/// Any error of parsing `text`, [`KeyError::RetireActive`] for the active
/// key, [`KeyError::UnknownId`] for an ID the keyring does not hold.
pub fn retire_key(text: &str, id: &str) -> Result<Zeroizing<String>, KeyError> {
    let mut doc = parse_document(text)?;
    if doc.active == id {
        return Err(KeyError::RetireActive { id: id.to_owned() });
    }
    let at = doc
        .keys
        .iter()
        .position(|k| k.id == id)
        .ok_or_else(|| KeyError::UnknownId { id: id.to_owned() })?;
    doc.keys.remove(at);
    serialize(&doc)
}

// --- the file -------------------------------------------------------------

/// Opens for reading without blocking (a FIFO would otherwise wait for a
/// writer). Symbolic links are followed.
#[cfg(unix)]
fn open_read(path: &Path) -> std::io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    OpenOptions::new()
        .read(true)
        .custom_flags(rustix::fs::OFlags::NONBLOCK.bits().cast_signed())
        .open(path)
}

#[cfg(not(unix))]
fn open_read(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().read(true).open(path)
}

/// Opens the keyring file, checks the open handle (regular file, owner, mode,
/// size) and reads it, returning the identity of what was read.
fn read_file(path: &Path) -> Result<(Zeroizing<Vec<u8>>, Identity), KeyError> {
    let file = open_read(path).map_err(KeyError::io(path))?;
    let meta = file.metadata().map_err(KeyError::io(path))?;
    if !meta.is_file() {
        return Err(KeyError::NotRegularFile {
            path: path.to_path_buf(),
        });
    }
    check_access(path, &meta)?;
    let too_large = || malformed(0, "larger than 64 KiB");
    let cap = u64::try_from(MAX_KEYRING_LEN).map_err(|_| too_large())?;
    if meta.len() > cap {
        return Err(too_large());
    }
    let mut raw = Zeroizing::new(Vec::with_capacity(MAX_KEYRING_LEN + 1));
    file.take(cap + 1)
        .read_to_end(&mut raw)
        .map_err(KeyError::io(path))?;
    if raw.len() > MAX_KEYRING_LEN {
        return Err(too_large());
    }
    Ok((raw, Identity::of(&meta)))
}

#[cfg(unix)]
fn check_access(path: &Path, meta: &std::fs::Metadata) -> Result<(), KeyError> {
    use std::os::unix::fs::MetadataExt;
    check_owner_and_mode(
        path,
        meta.mode() & 0o7777,
        meta.uid(),
        rustix::process::geteuid().as_raw(),
    )
}

#[cfg(not(unix))]
#[allow(clippy::unnecessary_wraps)]
fn check_access(_path: &Path, _meta: &std::fs::Metadata) -> Result<(), KeyError> {
    Ok(())
}

/// No access for others; owned by the effective user or root. Group access
/// is allowed (Kubernetes `fsGroup`).
#[cfg(unix)]
fn check_owner_and_mode(path: &Path, mode: u32, uid: u32, euid: u32) -> Result<(), KeyError> {
    if mode & 0o007 != 0 {
        return Err(KeyError::Permissions {
            path: path.to_path_buf(),
            mode,
        });
    }
    if uid != euid && uid != 0 {
        return Err(KeyError::NotOwner {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
