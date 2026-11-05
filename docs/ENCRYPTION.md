# Encryption

Optional client-side envelope encryption (Cargo feature `encryption`, on by
default). Each object gets a fresh data key, which is wrapped by a
key-encryption key from a keyring you hold; the storage provider only sees
ciphertext. Plain clients behave exactly as before.

## What it protects, and what it does not

The provider is treated as untrusted: it may read, modify, truncate, reorder,
swap or roll back stored bytes. Provider-side encryption (SSE) is not relied on.

Still visible to the provider:

- object names, sizes, timestamps and access patterns;
- the header's encryption context (including the object key) and key IDs.

Not covered:

- rollback and deletion by the provider are not detectable;
- plaintext on a compromised client, and plaintext held in Python `bytes`;
- the object-key binding (`osc:key`, on by default) covers the key inside the
  bucket only. An object can be replaced by an object with the same key from
  another bucket or provider that was written under the same keys. With binding
  off (`bind_object_key(false)` / `bind_object_key=False`), any two objects
  under the same keys can be swapped undetected. The caller context is
  authenticated, but a read checks it only when you require it (see
  [Requiring a context](#requiring-a-context-on-reads)); buckets whose objects
  must never be interchangeable should require a context or use separate
  keyrings (KEKs);
- anything written through a plain client or plain view is stored unencrypted
  by design. Pass only the encrypted view to code that handles sensitive data.

Caveats:

- **Streamed data is provisional** until the stream ends cleanly. Truncation or
  tampering raises at the point it is detected. `get` (whole object)
  authenticates each frame as it arrives, so damage or appended data fails the
  read as soon as it is downloaded; it still returns nothing until the final
  frame verified.
- An encrypted copy or move always downloads, decrypts and re-encrypts the
  object with a fresh data key and message ID, whatever the destination: another
  name, another bucket or store, or a `file://` move. The copy keeps the
  source's caller context and, with object-key binding on, is bound to the
  destination's name. It never copies bytes or asks the store to copy or
  rename, because the header names the object's path (the object-key binding)
  and cannot be rewritten safely. This costs a full download and upload even
  inside one bucket. The source is read once, with no separate header request,
  and a source that fails verification is never committed.
- The plain client (`without_encryption`, or a build without the feature)
  refuses to copy or move an encrypted object bound to its name (in a build
  without the feature, any encrypted object) to another name, because the
  copy would be unreadable; copy it with the encrypted client. It reads the
  start of the source first, which costs one extra request.
- A lost key with no other copy means permanent data loss.
- With object-key binding (the default) the bound key of a `file://` object is
  its absolute path. Moving such a file outside `osc` (for example with `mv`)
  makes it undecryptable; copy it with `osc` instead, which re-encrypts.
- `NotEncrypted` can also mean one of the first three bytes of an encrypted
  object was damaged. Reads still fail, but `osc enc scan` and `reencrypt`
  count such an object as not encrypted. `UnknownKey` is derived from
  unauthenticated header data, so it can also indicate tampering.
- AWS-LC wipes key schedules when their key objects are freed, but not the
  short-lived copies it makes on the stack while encrypting or decrypting.
- **Core dumps.** A process that loaded a keyring keeps every KEK in memory
  for its lifetime, and a crash frees and wipes nothing (an allocation
  failure, an AWS-LC fatal error such as an RNG failure, a stack overflow or a
  crash in C code), so a core dump of that process contains every key. `osc`
  and `osc-gui` turn core dumps off for themselves (see [CLI](#cli)). A
  library user or a service must do it: see [Core dumps](#core-dumps) under
  Kubernetes.
- Data keys, message IDs and IVs come from AWS-LC's random generator, which
  reseeds after `fork`. A virtual machine restored from a memory snapshot can
  repeat its output until the next reseed, unless the CPU has RDRAND (or RNDR
  on Arm; AWS-LC ignores RDRAND on AMD CPUs before Zen 3). Do not clone
  snapshots of a running process that encrypts. If the generator fails, for
  example because a sandbox denies OS entropy, AWS-LC aborts the process
  instead of raising an error.
- Pre-signed URLs are not available on encrypted clients.

## Keys

Keys are not stored or managed by this client. An external key manager (a
Kubernetes Secret, OpenBao, ...) owns them and hands the client a **keyring**:
one text document with every key-encryption key and the ID of the active one.

```text
osc-keyring-v1
active=kek-2026-10-3f9a1b2c
key=kek-2026-10-3f9a1b2c created=2026-10-27 material=<64 hex> check=<16 hex>
key=kek-2025-10-8be01d44 created=2025-10-20 material=<64 hex> check=<16 hex>
```

- The active key wraps new data keys; every listed key can unwrap. A key ID
  ends in the first 8 hex digits of the key's check value, so a damaged or
  mismatched key fails to load. IDs use ASCII letters, digits, `-`, `_` and
  `.`, at most 128 characters.
- At most 256 keys and 64 KiB. Errors name a line number or key ID, never key
  bytes.
- A keyring file may be a symlink (Kubernetes mounts every Secret key as one).
  On Unix it must be a regular file owned by you or root that its group
  cannot write and others cannot access; group read is allowed (for
  `fsGroup`), so Kubernetes `defaultMode: 0440` keeps working. Windows and
  other platforms check nothing: keep the keyring under `%USERPROFILE%` or in
  a directory without inherited access. Like OpenSSH, the check reads the
  owner and the mode bits only. ACL grants (`getfacl`, including inherited
  default ACLs, and macOS ACEs) are not detected, so keep the keyring in a
  directory without them.
- A file-backed keyring reloads itself: on use, including `status()`, at most every 5 seconds, it
  checks whether the file changed and swaps in the new keys, and a read that
  meets an unknown key ID checks at once (at most once a second). A broken
  update keeps the previous keys and is reported by `status()` until a valid
  file arrives. Mount the whole Secret volume: a `subPath` mount is never
  updated by the kubelet. Keep the keyring file on a local or tmpfs
  filesystem: a reload runs a synchronous `stat` on the calling thread, which a
  stalled network or FUSE mount can block. `status()` never waits for a reload
  that another task is running. A keyring read from text (Rust
  `Keyring::parse`, or `OSC_KEYRING_COMMAND`) never reloads.
- `status()` (Rust and Python) returns the active key, every key's ID and
  creation date, the active key's age in days and the last reload error. It
  checks the file first (rate-limited like any use), so an idle process still
  reports a broken Secret update.
  Expose it in a health or readiness check; the library does not log.
- KEK wrapping uses random 96-bit IVs, which bounds the safe number of wraps
  per KEK (NIST SP 800-38D, about 2^32). `wraps()` (Python: the `wraps`
  property) counts wraps with the active key in the process; nothing persists
  the count. Yearly rotation at up to a million objects stays far below the
  bound.
- **Losing every copy of a key makes its objects unreadable.** Back the keyring
  up offline (encrypted, for example with `age`) after every change.

The `osc keyring` helpers produce keyrings. They read a keyring on standard
input, print the result on standard output and store nothing; they refuse to
print key material to a terminal unless given `--force`:

```bash
umask 077                                        # keyring files must not be readable by others
osc keyring new --prefix kek > keyring           # one fresh, active key
osc keyring add < keyring > keyring.new          # adds a fresh key, not active yet
osc keyring activate <ID> < keyring > keyring.new   # makes a key active
osc keyring rotate < keyring > keyring.new       # add and activate in one step (one reader only)
osc keyring retire kek-2025-10-8be01d44 < keyring > keyring.new   # never the active key
osc keyring status < keyring                     # IDs, dates, ages; exit 3 if the active key is over 365 days old (--max-age-days)
osc keyring status --configured                  # the same for the configured key source
```

Every helper that reads standard input refuses a terminal, so a forgotten
`< keyring` fails at once instead of waiting for input.
`status` reads standard input like the other helpers, even when a key source is
configured, so `osc keyring status < keyring.new` checks the new file. On a
terminal it refuses and points at `--configured`, which checks the key source
instead (`--keyring`, `OSC_KEYRING_FILE` or `OSC_KEYRING_COMMAND`, with the same
permission check as any other use).

### Kubernetes

```yaml
spec:
  securityContext:
    runAsUser: 1000
    fsGroup: 1000
  containers:
    - name: app
      volumeMounts:
        - name: osc-keyring
          mountPath: /var/run/secrets/osc
          readOnly: true
  volumes:
    - name: osc-keyring
      secret:
        secretName: osc-keyring
        defaultMode: 0440        # group-readable for fsGroup, nothing for others
```

For the `osc` CLI in a pod, a CronJob or a CI job, also set
`OSC_REQUIRE_ENCRYPTION=1` (or pass `--require-encryption`): if the keyring
file or variable is ever lost from the manifest, `put`, `get`, `get-stream`,
`cp`, `mv` and `sign` then fail instead of sending plaintext to the provider.
`ls`, `rm`, `exists`, `mb` and `bucket-exists` are not affected.

The application opens `/var/run/secrets/osc/keyring` with
`Keyring::from_file` / `Keyring.from_file`. Turn on k3s secret encryption
(`--secrets-encryption`) so etcd does not hold the keyring in plain text, and
restrict `get` on the Secret to the service account and the operators. OpenBao
with External Secrets Operator works the same way when versioned history and a
richer audit log are wanted.

#### Core dumps

The library does not change process-wide crash settings (`prctl` and
`setrlimit` break `py-spy` and `gdb` attach and leak into child processes), so
turn core dumps off for any process that holds a keyring:

- Run `ulimit -c 0` before starting the service. In a pod:
  `command: ["sh", "-c", "ulimit -c 0 && exec python -m app"]`. Or call
  `resource.setrlimit(resource.RLIMIT_CORE, (0, 0))` in Python before the
  keyring is loaded.
- Or keep no cores on the node: `Storage=none` in `coredump.conf` for
  `systemd-coredump`, or a `kernel.core_pattern` that discards them.

k3s's systemd unit sets `LimitCORE=infinity` and pods inherit it; the node's
`kernel.core_pattern` decides whether a core is written. On nodes that keep
cores (`systemd-coredump` on Fedora, RHEL, SUSE or Arch), the keyring would
land on the node's disk.

### Rotation

Yearly, or on your policy's schedule; old keys stay in the keyring for
decryption only. Rotate in two steps, so that no writer uses a key that some
reader does not have yet. A reader that is behind fails every object written
under the new key with an unknown-key error: its forced re-check only
re-reads its own, still unchanged, file.

1. Publish a new key without activating it (`add`; its ID is printed on
   standard error):

```bash
kubectl get secret osc-keyring -o jsonpath='{.data.keyring}' | base64 -d \
  | osc keyring add \
  | kubectl create secret generic osc-keyring --from-file=keyring=/dev/stdin \
      --dry-run=client -o yaml \
  | kubectl apply -f -
# OpenBao: bao kv get -field=keyring secret/osc | osc keyring add | bao kv put secret/osc keyring=-
```

2. Wait until every reader has the key: each service's `status()` lists it,
   or at least the kubelet's sync delay (about a minute) plus the 5-second
   check. Readers that get the keyring from `OSC_KEYRING_COMMAND` see it when
   they next start: each `osc` run reads it anew, but a running `osc-gui`
   keeps the keys it started with.
3. Activate it through the same pipeline, with `osc keyring activate <ID>` in
   place of `add`. Take the offline backup afterwards.

`osc keyring rotate` does both steps at once. Use it only for a keyring that a
single process reads.

There is no header re-wrap (it would reuse the header's GCM nonce), so moving
objects off a key means re-encrypting them with fresh data keys:

```bash
osc enc scan s3://bucket/                        # objects per key ID (header reads only)
osc enc scan s3://bucket/ --key kek-2025-10-8be01d44   # only the URLs of the objects under that key
osc --keyring keyring enc reencrypt s3://bucket/ --from kek-2025-10-8be01d44 --dry-run
osc --keyring keyring enc reencrypt s3://bucket/ --from kek-2025-10-8be01d44
```

`reencrypt` (`osc enc reencrypt`, Rust `Client::reencrypt`, Python
`EncryptedObjectStorageClient.reencrypt`) authenticates every object fully
before rewriting it in place with a fresh data key, keeps its encryption
context, and commits only if the object is still the version it read: the body
read is pinned to that version, the client checks the version again right
before the final write, and a single-part write is also conditional where the
store supports it (Azure and S3 honour `If-Match`; Garage ignores it, so there
the re-check is the protection). A concurrently changed object is reported as
`changed` and left as the other writer made it; a store that reports neither
an e-tag nor a version is refused. Like a re-encrypting copy, it does not carry
over content type or user metadata. Re-running is safe: rewritten objects are
then `skipped`. A `--from` key that the keyring does not hold is refused before
any object is listed or read (Rust `Error::Key`, Python `EncryptionError`), as
is the active key.

`reencrypt` keeps the object's encryption context and its object-key binding
whatever the client's `bind_object_key`: an object written without binding
stays unbound, a bound one stays bound to the same name. A binding client
refuses an object bound to another name, also in a dry run.

### Retiring an old key

Re-encrypt every prefix that holds objects under it; with versioning on, delete
the noncurrent versions (the client cannot see them); confirm with
`osc enc scan` that no object uses it, which holds only if the scan exits 0; then
`osc keyring retire` through the same pipeline as a rotation. Keep an offline
copy while backups may still need it.

Zero-byte entries, such as the folder markers the S3 and GCS consoles create
(listed as an empty `p/sub`), are counted as not encrypted without being read.

A scan or re-encryption is complete only when it exits 0. Exit 1 means objects
were unreadable (`osc enc scan` names each on standard error with the reason,
or in its `--json` line), a re-encryption left objects `error`, `changed` or
`after-cutoff`, or the listing failed: the summary then shows `listing-errors`
(`(listing errors)` in the scan's table) above 0 and the error names the
offending key (printed sanitized), for example one with an empty segment or a
control character, which the store lists as an invalid path. Remove that
object with the provider's tools and run again; see
[Unaddressable object names](BEHAVIOR.md#unaddressable-object-names).

### After a key leak

1. If storage credentials may have leaked together with the key, revoke or
   rotate them first. Rotating the KEK does not stop anyone who still holds
   storage credentials from planting forged objects or blocking names (it does
   not stop the provider itself either).
2. Add, wait and activate a new key (see Rotation), so new writes use it.
3. Pause the service's writes if you can (recommended, not required).
4. `osc enc reencrypt <prefix> --from <leaked> --before <time of the leak>`.
   Objects written at or after that time are reported as `after-cutoff` and
   left alone: anyone holding the leaked key and storage access could have
   forged them, and re-encrypting would make a forgery authentic under the new
   key. Review them by hand. The cutoff compares the modification time the store reports, which is not
   authenticated. It only screens out forgeries by holders of leaked storage
   credentials on stores that set the time themselves at commit; the provider,
   anyone who can set file times on `file://` or NFS, and S3 multipart uploads
   started before the cutoff are exceptions, and an object whose store reports no
   time is `after-cutoff`. If a party that may hold the leaked key can also
   influence modification times, treat every object under that key as suspect and
   check candidates against an independent record (application records, provider
   audit logs or inventory) before re-encrypting.
   A run that stops with `Encountered object with invalid path` met an object
   that cannot be addressed: see
   [Unaddressable object names](BEHAVIOR.md#unaddressable-object-names), and
   treat it as possibly planted.
5. Delete old versions, confirm with `osc enc scan` (it must exit 0, as must
   every `reencrypt` run), retire the leaked key. On an `http(s)://` store, do
   not trust an exit 0 unless the server answers `PROPFIND` with `Depth:
   infinity` in full (see [Listing](BEHAVIOR.md#listing)); some WebDAV servers
   silently list one level only and the scan then misses nested objects.

Ciphertext an attacker already copied stays readable to them; the response
protects what is stored and stops forged objects from being accepted.

## Rust

```rust
use object_storage_client::encryption::{EncryptionConfig, Keyring};
use object_storage_client::{EncryptedObjectStorageClient, ObjectStorageClient};
use std::sync::Arc;

let keyring = Arc::new(Keyring::from_file("/var/run/secrets/osc/keyring")?); // reloads itself
let cfg = EncryptionConfig::new(keyring.clone()); // .bind_object_key(false) weakens swap protection; .frame_length(..)?
let health = keyring.status(); // active key, key ages, last reload error

let plain = ObjectStorageClient::new();
let secure = plain.with_encryption(cfg.clone()); // shares the stores with `plain`
// or an encrypted-only client: EncryptedObjectStorageClient::new(cfg)

secure.put("s3://bucket/payroll.csv", data).await?;
let csv = secure.get("s3://bucket/payroll.csv").await?;
let stream = secure.get_stream("s3://bucket/big.bin").await?; // decrypting stream
secure.put_stream("s3://bucket/copy.bin", stream).await?;      // bounded memory

use std::collections::BTreeMap;
let ctx = BTreeMap::from([("tenant".to_owned(), "acme".to_owned())]);
secure.put_with_context("s3://bucket/a", data, &ctx).await?;
let info = secure.inspect_encryption("s3://bucket/a").await?; // header only, no keys; unauthenticated
```

```rust
// Fails with Error::Decryption(DecryptError::ContextMismatch { key }) unless
// the object carries tenant=acme (extra stored pairs are allowed):
let data = secure.get_with_context("s3://bucket/a", &ctx).await?;
let stream = secure.get_stream_with_context("s3://bucket/a", &ctx).await?;
```

Context keys starting with `osc:` or `aws-crypto-` are reserved; the match is
ASCII case-insensitive, so `OSC:key` is rejected too. Reading a
non-encrypted object with an encrypted client fails with `Error::NotEncrypted`
(an object is judged by its first three bytes); read it with the plain view (`secure.without_encryption()`).

### Requiring a context on reads

The caller context is authenticated: it cannot be changed without failing
decryption. A read does not check it unless you ask. `get_with_context` and
`get_stream_with_context` (Python: `context=` on `get_object` and
`get_object_stream`) require every given pair to be present with an equal
value; extra stored pairs are allowed. The check runs on the header before any
key is unwrapped, and a mismatch names only the key, never a value. Keys
starting with `osc:` or `aws-crypto-` are rejected before any I/O. `copy`,
`move` and `reencrypt` are unchanged.

For example, write every object of a bucket with `{"bucket": "eu-payroll"}` and
read with the same context: an object swapped in from another bucket under the
same keyring then fails with `ContextMismatch`.

## Python

```python
from object_storage_client import (
    ObjectStorageClient, EncryptedObjectStorageClient, Keyring,
    DecryptionError, UnknownKeyError, NotEncryptedError,
)

keyring = Keyring.from_file("/var/run/secrets/osc/keyring")  # reloads itself
health = keyring.status()                                    # dict: active, keys, active_age_days, last_reload_error

plain = ObjectStorageClient()
secure = plain.with_encryption(keyring)                      # shares the stores
# or an encrypted-only client: EncryptedObjectStorageClient(keyring)

await secure.put_object("s3://bucket/a.txt", b"data", context={"tenant": "acme"})
data = await secure.get_object("s3://bucket/a.txt")          # verified before returning
data = await secure.get_object("s3://bucket/a.txt", context={"tenant": "acme"})  # must carry it
await secure.put_object_stream("s3://bucket/big.bin", chunks)  # any async iterable of bytes
info = await secure.inspect_encryption("s3://bucket/a.txt")  # unauthenticated header: do not trust its context

# Between modes, pipe the streams (bounded memory):
await plain.put_object_stream("file:///tmp/a.txt", await secure.get_object_stream("s3://bucket/a.txt"))
```

Encryption failures derive from `EncryptionError` (a `RuntimeError`):
`DecryptionError`, `UnknownKeyError`, `NotEncryptedError`. Invalid arguments
(reserved or oversized context, out-of-range `frame_length`, malformed URL
paths, an unsupported scheme) raise `ValueError`. A missing object is still
`FileNotFoundError`, and an HTTP 401 or 403 from the store, or an
[archived object](BEHAVIOR.md#archived-objects), raises `PermissionError`.

## CLI

```bash
# In a pod: the mounted Secret (or OSC_KEYRING_FILE)
osc --keyring /var/run/secrets/osc/keyring put report.csv s3://bucket/report.csv

# On a laptop: a command that prints the keyring; it never touches the disk
export OSC_KEYRING_COMMAND="kubectl get secret osc-keyring -o jsonpath='{.data.keyring}' | base64 -d"
osc get s3://bucket/report.csv out.csv

osc enc inspect s3://bucket/report.csv      # unauthenticated header details; no keys needed
```

With a keyring (`--keyring FILE`, `OSC_KEYRING_FILE` or
`OSC_KEYRING_COMMAND`; setting both kinds is an error), `put`, `get`,
`get-stream`, `cp` and `mv` encrypt and decrypt transparently, and `sign` is
rejected. `ls`, `rm`, `exists`, `mb` and `bucket-exists` never read the
keyring: the key command does not run and no key is loaded (setting both kinds
of source is still an error, and so is it for `enc inspect`, `enc scan` and
the `keyring` helpers). `OSC_KEYRING_COMMAND` runs through `sh -c` (`cmd /C`
on Windows) with no standard input (log in to `kubectl`/`bao` beforehand); its
output is kept in memory that is wiped after use, and a non-zero exit, more
than 64 KiB of output, output that is not UTF-8 or more than 30 seconds is an
error. `osc` disables core dumps and same-user `ptrace` for itself (Unix;
`PR_SET_DUMPABLE` on Linux), so a crash writes no keys to disk, and the key
command inherits the zero core limit.

When the command times out, or `osc` is interrupted while it runs, the shell is
killed, but processes it started (the stages of a pipeline) may keep running.
Prefer a single command, or `exec` the last one:
`OSC_KEYRING_COMMAND="exec vault kv get -field=keyring secret/osc"`.

Set `OSC_REQUIRE_ENCRYPTION=1` (or `--require-encryption`) in CI and in
Kubernetes jobs, so a missing key source is an error and never a silent
plaintext upload. `0`, `false`, `no` and `off` leave it off.

`osc-gui` takes the same `--keyring`, `OSC_KEYRING_FILE`, `OSC_KEYRING_COMMAND`
and `--require-encryption` (`OSC_REQUIRE_ENCRYPTION`), and turns core dumps off
like `osc`. It opens the key source once, at launch: a keyring file still
reloads itself, a key command does not run again. With `--require-encryption`
and no key source it refuses to start. See [Launch](GUI.md#launch).

## Format and interop

Objects are AWS Encryption SDK message format version 2, algorithm suite
`0x0478` (AES-256-GCM with key commitment), framed with 64 KiB frames by
default. They can be decrypted by the AWS Encryption SDK with a raw AES keyring
using key namespace `osc-file` and key name = the key ID, with the 256-bit key
from the keyring's `material=` field. The byte layout is the public
[AWS Encryption SDK message format](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/message-format.html).
The encryption context holds the reserved entries `osc:fmt=1` and, when object
key binding is on, `osc:key=<object path>`, plus the caller's own entries. A
key's check value is the first 8 bytes of `HMAC-SHA256(material,
"osc-key-check-v1")`.

By default (`DecodeLimits`) a reader accepts a header of at most 16 KiB with at
most 8 wrapped data keys, and frame lengths of 4 KiB to 4 MiB; `frame_length`
must lie in that range. A writer refuses an encryption context over 12 KiB
serialized.
