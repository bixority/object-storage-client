# Key rotation: injected keyring, reload, scan and re-encrypt

Status: design approved in conversation on 2026-10-27; this document is for
review before the implementation plan.

## 1. Context

Client-side envelope encryption (branch `feat/encryption-core`, unreleased)
wraps each object's data key with a key-encryption key (KEK) from a directory
of key files (`FileKeyProvider`). Rotation today is a manual README procedure.

Interview results:

| Topic | Decision |
|---|---|
| Goals | Compliance cryptoperiod, limit leak damage, respond to a leak |
| Deployment | One service, in pods on a self-managed on-prem k3s cluster; no persistent disk |
| Adversaries | Storage provider; holders of leaked storage credentials. The cluster and its nodes are trusted |
| Scale | Up to about 1 million objects or 1 TB per key |
| Cryptoperiod | 1 year |
| Old data | Old keys stay available for decryption only; no rewrite on routine rotation |
| Who rotates | An operator runs a command; the tool reports an overdue key |
| Versioning | Mostly off, sometimes on: the design covers both |
| Key management | Not the client's business: an external tool owns keys and injects them |
| Injection | Kubernetes Secret mounted as a file |
| Service language | Rust and Python, equal priority |
| CLI users | Operator laptops and in-cluster pods; never CI |
| Key directory model | Replaced by the keyring (unreleased, so no compatibility burden) |
| Keyring tooling | Stateless `osc keyring` helpers (stdin to stdout) |
| Reload | Automatic, check on use (no inotify) |
| Keyring format | Line-based text, no new dependencies |

Cloud key services (AWS KMS and similar) are excluded: the provider is not
trusted.

## 2. Responsibilities

| External key manager | object-storage-client |
|---|---|
| Generates and stores keyrings, controls access, backs them up | Reads an injected keyring |
| Schedules rotation (yearly) | Reports the active key's age (`status`, `osc keyring status`) |
| Distributes the keyring to pods (Secret) | Reloads a changed keyring without a restart |
| Audits key events | Wraps with the active key, unwraps with any key; never logs key bytes |
| | Format-aware operations: `scan` (which keys protect which objects) and `reencrypt` (move objects to the active key) |
| | Stateless helpers producing keyrings (`osc keyring new/rotate/retire`) |

## 3. Non-goals

inotify or other file watchers; KMS or Vault/OpenBao Transit; keyrings from
environment variables; keys in CI; persisted wrap counters; per-bucket keys
(possible later with one client per keyring); header re-wrap (impossible:
the header tag would reuse its GCM nonce).

## 4. Keyring document

### 4.1 Format

UTF-8 text. A leading byte order mark is ignored; CRLF and LF line endings
are accepted; blank lines and surrounding whitespace are ignored.

```text
osc-keyring-v1
active=kek-2026-10-3f9a1b2c
key=kek-2026-10-3f9a1b2c created=2026-10-27 material=<64 hex> check=<16 hex>
key=kek-2025-10-8be01d44 created=2025-10-20 material=<64 hex> check=<16 hex>
```

- Line 1 is exactly `osc-keyring-v1`.
- Exactly one `active=<id>` line.
- One `key=` line per key. After `key=<id>`, the fields `created`, `material`
  and `check` each appear exactly once, in any order, separated by spaces or
  tabs, each as `name=value`.
- Any other line or field is an error.
- `id`: the rules of today's key files (ASCII letters, digits, `-`, `_`, `.`;
  at most 128 bytes), and it must end in the first 8 hex digits of the check
  value (lowercase, exact match).
- `created`: `YYYY-MM-DD`.
- `material`: 64 hex digits (32 bytes).
- `check`: 16 hex digits, the first 8 bytes of
  `HMAC-SHA256(material, "osc-key-check-v1")`, compared in constant time.

Field semantics, ID scheme (`{prefix}-{YYYY}-{MM}-{check[0..8]}`) and check
value are unchanged from today's key files, so a key file's values move into a
keyring line as they are.

### 4.2 Validation

A keyring is accepted only if all of these hold, otherwise loading fails with
a `KeyError` naming the line number or key ID and never key bytes:

- At most 64 KiB and at most 256 keys.
- Key IDs are unique (identical duplicate lines are rejected too).
- `active` names a listed key.
- Every check value matches its material and every ID ends in its check
  prefix.

Parsed material lives in `Zeroizing` buffers; the raw text buffer is zeroized
after parsing.

### 4.3 Reading a keyring file

- Symbolic links are followed (Kubernetes mounts every Secret key as a
  symlink into `..data/`).
- The target must be a regular file, owned by the effective user or by root,
  with no permission bits for others (`mode & 0o007 == 0`). Group read is
  allowed (`fsGroup`). On non-Unix platforms these checks are skipped.
- The file is opened non-blocking (a FIFO in its place cannot hang the
  process) and read with the 64 KiB cap.
- The OpenSSH-style ancestor checks of the key directory are dropped: they do
  not fit Kubernetes mounts, and the host is trusted (section 1).

### 4.4 Provider namespace

EDKs keep the provider ID `osc-file`. Together with unchanged key IDs, objects
written by the current code decrypt with a keyring holding their key, and the
golden vectors keep their ciphertexts.

## 5. Library API

### 5.1 Rust

```rust
pub struct Keyring { /* snapshot + reload state */ }

impl Keyring {
    /// Loads and validates the file; later changes are picked up (section 6).
    pub fn from_file(path: impl Into<PathBuf>) -> Result<Self, KeyError>;
    /// A fixed keyring from text; never reloads.
    pub fn parse(text: &str) -> Result<Self, KeyError>;
    pub fn status(&self) -> KeyringStatus;
    /// Wraps with the active key in this process (not persisted).
    pub fn wraps(&self) -> u64;
}

pub struct KeyringStatus {
    pub active: String,
    pub keys: Vec<KeyInfo>,              // id, created; sorted by created, newest first
    pub active_age_days: i64,            // today (UTC) minus the active key's created date
    pub last_reload_error: Option<String>, // set while the file on disk is invalid
}
```

`Keyring` implements `KeyProvider` and is passed as today:
`EncryptionConfig::new(Arc::new(keyring))`. Callers keep their own `Arc` to
query `status()`.

`KeyProvider::active_key_name` changes from `-> &str` to `-> String`: a
reloadable provider cannot lend a name from a snapshot that may be swapped.
Its only production caller (`Envelope::names_active_key`) takes the owned
value.

Removed: `FileKeyProvider`, its key file format and generation
(`generate_key_file`, `check_file`), and the key-file-specific `KeyError`
variants that no longer apply (`DirectoryPermissions`, `DirectoryNotOwner`,
`ParentInsecure`). Remaining variants are reused with keyring wording;
`Malformed` gains a line number.

### 5.2 Python

```python
keyring = Keyring.from_file("/var/run/secrets/osc/keyring")
client = ObjectStorageClient().with_encryption(keyring)      # same keyword args as today
client = EncryptedObjectStorageClient(keyring)
keyring.status()   # {"active": str, "keys": [{"id": str, "created": "YYYY-MM-DD"}],
                   #  "active_age_days": int, "last_reload_error": str | None}
keyring.wraps      # int
```

Python has no `parse`: key text would pass through immutable Python strings
that cannot be wiped. `FileKeyProvider` is removed from the module, the type
stubs and the docs.

## 6. Reload (file-backed keyrings)

State: the current snapshot (`Arc<Snapshot>` holding the active ID and the
keys) behind a `RwLock`, the file identity of that snapshot (device, inode,
modification time, size of the symlink target) and the time of the last check.

- **Check on use:** at the start of every wrap and unwrap, if the last check
  is at least 5 seconds old, the provider stats the path (following
  symlinks). An unchanged identity ends the check.
- **Changed file:** it is read and fully validated (section 4). On success the
  new snapshot replaces the old one atomically and `last_reload_error` is
  cleared. On failure the old snapshot stays, `last_reload_error` is set, and
  the next check happens after the normal interval.
- **Single reloader:** a `Mutex` guards the check; a task that finds it held
  continues with the current snapshot instead of waiting.
- **Unknown key on unwrap:** when no EDK names a key of the snapshot, the
  provider runs the check immediately (ignoring the 5 seconds) unless the
  previous forced check was less than 1 second ago, then retries the unwrap
  once with the resulting snapshot. Only then does it return `UnknownKey`.
- **One snapshot per object:** a write takes the snapshot once and uses its
  active key for the whole object. Reads finish with the snapshot they
  unwrapped with. Removing a key never interrupts an object in flight.
- **I/O:** the stat and read are short synchronous calls on a small file
  (a tmpfs Secret mount in Kubernetes); they run inline.
- `subPath` mounts are never updated by the kubelet; the documentation
  requires mounting the whole Secret volume.

The library does not log. Services expose `status()` in their health or
readiness checks to surface `last_reload_error` and an overdue key.

## 7. CLI

### 7.1 Key sources

Exactly one source may be configured; both is a usage error; none means
unencrypted operation as today.

- `--keyring <file>` (env `OSC_KEYRING_FILE`): `Keyring::from_file`.
- `OSC_KEYRING_COMMAND`: run with `sh -c` on Unix (`cmd /C` on Windows).
  stdin is `/dev/null` (null device), stderr is inherited, stdout is captured
  into a zeroized buffer and parsed with `Keyring::parse`. Errors: non-zero
  exit (status reported, output discarded), more than 64 KiB of output, or no
  exit within 30 seconds (the child is killed).

A configured keyring enables encryption; its `active` line selects the write
key. Removed: `--keys`, `--key-id`, `OSC_KEYS_DIR`, `osc keys generate` and
`osc keys check`.

### 7.2 `osc keyring` helpers

All read a keyring from stdin (except `new`) and never store anything.

| Command | Effect | Output |
|---|---|---|
| `osc keyring new [--prefix kek]` | Fresh keyring with one key, active | keyring on stdout |
| `osc keyring rotate [--prefix kek]` | Adds a fresh key, makes it active, keeps the others | keyring on stdout |
| `osc keyring retire <id>` | Removes `<id>`; refuses the active key and unknown IDs | keyring on stdout |
| `osc keyring status [--max-age-days 365]` | Validates; reads the configured source (7.1) if one is set, otherwise stdin | table of id, created, age, active marker; never material |

- New keys use the system RNG and today's UTC date; IDs follow section 4.1.
- `new`, `rotate` and `retire` refuse to write to a terminal unless `--force`
  is given.
- Output keeps the input's key order, appends new keys at the end and
  normalizes formatting (LF line endings, single spaces).
- Exit codes: `0` success; `1` invalid input or refused operation; `2` usage
  (clap); `3` (`status` only) the active key is older than `--max-age-days`.

## 8. Scan and re-encrypt

### 8.1 Scan

`osc enc scan <prefix> [--jobs 16] [--key <id>] [--json]` lists the prefix
(`list_stream`) and runs `inspect_encryption` (one header read of at most
16 KiB) per object with at most `--jobs` in flight. The default output is a
table of counts per key ID, plus "not encrypted" and "unreadable" (with the
URLs of unreadable objects). `--key <id>` prints the URLs of objects under that
key instead. `--json` prints one JSON object per object (`url`, `key_ids`,
`status`) and a final summary object. Header key IDs are unauthenticated; scan
is an inventory, not a security check.

### 8.2 Re-encrypt (library)

`Client<Envelope>::reencrypt(&self, url: &str, opts: &ReencryptOptions) -> Result<Reencrypted>`
and Python
`EncryptedObjectStorageClient.reencrypt(url, from_key, *, before=None, dry_run=False) -> str`.

```rust
pub struct ReencryptOptions {
    pub from: String,
    /// Only objects last modified before this time are rewritten.
    pub before: Option<DateTime<Utc>>,
    /// Decide only; never write.
    pub dry_run: bool,
}
pub enum Reencrypted { Rewritten, WouldRewrite, Skipped, NotEncrypted, AfterCutoff, Changed }
```

Python returns the outcome as `"rewritten"`, `"would-rewrite"`, `"skipped"`,
`"not-encrypted"`, `"after-cutoff"` or `"changed"`. The cutoff and dry run
are decided inside the call, on the same version that is then rewritten, so
an object replaced between a separate check and the rewrite cannot slip past
`before`. `from` equal to the active key is refused (rotate first).

Steps:

1. Read the header (one ranged read), which also yields the ETag (or
   version) and `last_modified`.
2. Not encrypted: `NotEncrypted`. No EDK named `from`: `Skipped` (this
   includes objects already under the active key). `last_modified` at or
   after `before`: `AfterCutoff`. Dry run: `WouldRewrite`. The body read in
   step 3 is pinned to the step 1 version (`If-Match`); a mismatch is
   `Changed`.
3. Stream: decrypt with full authentication; encrypt under the snapshot's
   active key with a fresh data key and message ID, to the same path, with the
   caller context taken from the authenticated header (the mechanism the
   re-encrypting copy uses today). Metadata is handled as `copy` handles it.
4. Commit only if unchanged: a conditional write against the step 1 version
   where the backend supports it (S3 and Azure `If-Match`, GCS generation
   match, applied to single puts). Otherwise, and for multipart uploads, the
   client re-reads the ETag immediately before the final put or the
   multipart completion. A mismatch aborts the upload and yields `Changed`;
   the stored object is untouched.
5. Verify: read the new header and require that it no longer names `from`
   and names a key of the keyring; anything else is an error.

A failed decrypt is an error (the object is never rewritten). An interrupted
run leaves each object either old or new, because stores commit whole
objects.

### 8.3 Re-encrypt (CLI)

`osc enc reencrypt <prefix> --from <id> [--before <RFC 3339 time>] [--jobs 8] [--dry-run]`

- Lists the prefix and calls `reencrypt` per object, `--jobs` at a time,
  passing `--before` and `--dry-run` through.
- `--before`: objects with `last_modified` at or after the time are not
  touched and are reported as `after-cutoff` for manual review.
- `--dry-run`: reports `would-rewrite` instead of rewriting (exit code as
  for `rewritten`).
- Output: one JSON line per object (`url`, `outcome`, `error` when present)
  and a summary line. Exit `0` when every object ended `Rewritten`,
  `Skipped` or `NotEncrypted`; `1` when any object failed, `Changed` or was
  `after-cutoff`.
- Re-running is safe: rewritten objects are then `Skipped`.

## 9. Operations

### 9.1 Yearly rotation

```bash
kubectl get secret osc-keyring -o jsonpath='{.data.keyring}' | base64 -d \
  | osc keyring rotate \
  | kubectl create secret generic osc-keyring --from-file=keyring=/dev/stdin \
      --dry-run=client -o yaml \
  | kubectl apply -f -
```

Then take the offline backup (section 10). Pods pick up the new keyring
within the kubelet's sync delay (about a minute) and the reload interval; the
unknown-key re-check covers pods that are still behind.

### 9.2 Retiring an old key

1. `osc enc reencrypt <prefix> --from <old>` for every prefix that holds
   encrypted objects.
2. With versioning on: delete noncurrent versions (lifecycle rule or provider
   tooling); the client cannot see them.
3. `osc enc scan <prefix>` shows zero objects under `<old>`.
4. `osc keyring retire <old>` through the same pipeline as 9.1; keep an
   offline copy only while backups may need it.

### 9.3 Leak response

1. Rotate (9.1). New writes use the new key.
2. Recommended: pause the service's writes.
3. `osc enc reencrypt <prefix> --from <leaked> --before <leak time>`. Review
   `after-cutoff` objects by hand: they may be forgeries made with the leaked
   key, and re-encrypting them would make them authentic under the new key.
4. Steps 2 to 4 of 9.2 with the leaked key.

Ciphertext an attacker already copied stays readable to them; the response
protects stored objects and stops forged objects from being accepted.

## 10. Deployment and key management guidance (README)

- Pod spec: the Secret mounted as a volume (no `subPath`), `defaultMode:
  0440` with `fsGroup` for a non-root user (or `0400` when the reading user
  owns the files), a readiness check that calls `status()`.
- Starting point: a plain k3s Secret with `--secrets-encryption` enabled,
  RBAC restricting `get` on it to the service account and operators, the
  Kubernetes audit log for changes.
- After every rotation: an offline, encrypted backup of the keyring (for
  example `age`). Losing every copy of a key loses its objects.
- Later: OpenBao with External Secrets Operator for versioned history and a
  richer audit log; the client is unchanged
  (`bao kv get -field=keyring secret/osc | osc keyring rotate | bao kv put secret/osc keyring=-`).

## 11. Tests

Unit (Rust):

- Parsing: valid keyrings; BOM and CRLF; each malformed case (header, unknown
  line, unknown field, repeated field, missing field, bad ID, bad date, bad
  hex, wrong lengths); duplicate IDs; missing and unknown `active`; check
  value and ID suffix mismatches; 64 KiB and 256-key caps. No error message or
  `Debug` output contains material.
- File rules: symlink followed; others-access refused; group read accepted;
  owner other than self or root refused; FIFO in place of the file does not
  hang.
- Reload: unchanged file not re-read; a simulated `..data` symlink swap picked
  up after the interval; invalid replacement keeps the old snapshot and sets
  `last_reload_error`, a later valid file clears it; unknown-key forced check
  limited to once per second; a write spanning a swap uses one key; a key
  removed while a read is in flight does not fail that read.
- `status`: ordering, `active_age_days`.

CLI (functional tests):

- `keyring new/rotate/retire/status`: outputs parse back; rotate keeps old
  keys and switches `active`; retire refuses the active and unknown IDs;
  terminal guard and `--force`; exit codes 0, 1, 3.
- Sources: `--keyring` and `OSC_KEYRING_COMMAND` (success, non-zero exit,
  output over 64 KiB, timeout); both set is an error.
- `enc scan` counts and `--key`; `enc reencrypt` outcomes, `--before`,
  `--dry-run`, idempotent re-run, exit codes.

Integration (Garage, Azurite, `file://`; Rust and Python):

- Re-encrypt every outcome; an object modified during re-encryption
  (single put and multipart) is reported `Changed` and keeps the newer
  content.
- Scan after a rotation shows both keys; after re-encryption only the new
  one.
- Python `Keyring.from_file`, `status()`, reload after a file swap,
  `reencrypt`. Python tests use static TEST-ONLY keyring files under
  `tests/vectors/keyrings/` (copied to a private temporary file); no key logic
  is written in Python, in the package or in tests.

Existing suites keep passing with the keyring: golden vectors (keys converted
to `tests/vectors/golden/v1/keyring`, ciphertexts unchanged), ESDK interop,
checksum round trips, tamper, fuzz and performance guards.

## 12. Verified during implementation

- Conditional puts (`PutMode::Update`) through `object_store` 0.14: Azurite
  rejects a stale version (412, `Precondition`); Garage v2.5.0 silently
  ignores `If-Match` and overwrites, so there the re-check before the commit
  is the only guard; `file://` answers `NotImplemented` (re-check only). GCS
  could not be probed (the emulator rejects the client's writes). Pinned by
  probes in `tests/integration/reencrypt.rs`.
- A store that reports neither an e-tag nor a version cannot be guarded;
  `reencrypt` refuses such objects (dry runs still answer).
- A streamed (re-encrypting) `copy` carries over neither content type nor
  user metadata; `reencrypt` matches it and the documentation says so.
