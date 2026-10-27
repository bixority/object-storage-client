# Encryption

Optional client-side envelope encryption (Cargo feature `encryption`, on by
default). Each object gets a fresh data key, which is wrapped by a
key-encryption key from a keyring you hold; the storage provider only sees
ciphertext. Plain clients behave exactly as before. Full design:
[the encryption spec](superpowers/specs/2026-10-26-client-side-encryption-design.md)
and [the key rotation spec](superpowers/specs/2026-10-27-key-rotation-design.md).

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
  under the same keys can be swapped undetected;
- anything written through a plain client or plain view is stored unencrypted
  by design. Pass only the encrypted view to code that handles sensitive data.

Caveats:

- **Streamed data is provisional** until the stream ends cleanly. Truncation or
  tampering raises at the point it is detected. `get` (whole object) checks
  the header, key and binding as soon as they arrive but authenticates the
  frames after the download, so damage deep in an object is reported once it
  has been read; it never returns unverified data.
- An encrypted copy or move to another name always downloads, decrypts and
  re-encrypts the object: its header names its path (the object-key binding)
  and cannot be rewritten safely. The source is read once, with no separate
  header request.
- A lost key with no other copy means permanent data loss.
- With object-key binding (the default) the bound key of a `file://` object is
  its absolute path. Moving such a file outside `osc` (for example with `mv`)
  makes it undecryptable; copy it with `osc` instead, which re-encrypts.
- A byte copy inside one store (server-side copy or rename) does not verify the
  source; a tampered source is copied as is and detected when the destination is
  read. A copy between stores streams the bytes through a decryptor and commits
  only if they verify.
- `NotEncrypted` can also mean the first byte of an encrypted object was
  damaged. `UnknownKey` is derived from unauthenticated header data, so it can
  also indicate tampering.
- AWS-LC wipes key schedules when their key objects are freed, but not the
  short-lived copies it makes on the stack while encrypting or decrypting.
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
  mismatched key fails to load.
- At most 256 keys and 64 KiB. Errors name a line number or key ID, never key
  bytes.
- A keyring file may be a symlink (Kubernetes mounts every Secret key as one).
  On Unix it must be a regular file owned by you or root with no access for
  others; group read is allowed (for `fsGroup`).
- A file-backed keyring reloads itself: on use, at most every 5 seconds, it
  checks whether the file changed and swaps in the new keys, and a read that
  meets an unknown key ID checks at once (at most once a second). A broken
  update keeps the previous keys and is reported by `status()` until a valid
  file arrives. Mount the whole Secret volume: a `subPath` mount is never
  updated by the kubelet.
- `status()` (Rust and Python) returns the active key, every key's ID and
  creation date, the active key's age in days and the last reload error.
  Expose it in a health or readiness check; the library does not log.
- KEK wrapping uses random 96-bit IVs, which bounds the safe number of wraps
  per KEK (NIST SP 800-38D, about 2^32). `wraps()` counts wraps with the
  active key in the process; nothing persists the count. Yearly rotation at up
  to a million objects stays far below the bound.
- **Losing every copy of a key makes its objects unreadable.** Back the keyring
  up offline (encrypted, for example with `age`) after every change.

The `osc keyring` helpers produce keyrings. They read a keyring on standard
input, print the result on standard output and store nothing; they refuse to
print key material to a terminal unless given `--force`:

```bash
umask 077                                        # keyring files must not be readable by others
osc keyring new --prefix kek > keyring           # one fresh, active key
osc keyring rotate < keyring > keyring.new       # adds a fresh key and makes it active
osc keyring retire kek-2025-10-8be01d44 < keyring > keyring.new   # never the active key
osc keyring status < keyring                     # IDs, dates, ages; exit 3 if the active key is over 365 days old
```

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

The application opens `/var/run/secrets/osc/keyring` with
`Keyring::from_file` / `Keyring.from_file`. Turn on k3s secret encryption
(`--secrets-encryption`) so etcd does not hold the keyring in plain text, and
restrict `get` on the Secret to the service account and the operators. OpenBao
with External Secrets Operator works the same way when versioned history and a
richer audit log are wanted.

### Rotation

Yearly, or on your policy's schedule; old keys stay in the keyring for
decryption only:

```bash
kubectl get secret osc-keyring -o jsonpath='{.data.keyring}' | base64 -d \
  | osc keyring rotate \
  | kubectl create secret generic osc-keyring --from-file=keyring=/dev/stdin \
      --dry-run=client -o yaml \
  | kubectl apply -f -
# OpenBao: bao kv get -field=keyring secret/osc | osc keyring rotate | bao kv put secret/osc keyring=-
```

Pods pick up the new keyring within the kubelet's sync delay (about a minute)
and the 5-second check; no restart. Take the offline backup afterwards.

There is no header re-wrap (it would reuse the header's GCM nonce), so moving
objects off a key means re-encrypting them with fresh data keys:

```bash
osc enc scan s3://bucket/                        # objects per key ID (header reads only)
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
then `skipped`.

### Retiring an old key

Re-encrypt every prefix that holds objects under it; with versioning on, delete
the noncurrent versions (the client cannot see them); confirm with
`osc enc scan` that no object uses it; then `osc keyring retire` through the
same pipeline as a rotation. Keep an offline copy while backups may still need
it.

### After a key leak

1. Rotate, so new writes use a new key.
2. Pause the service's writes if you can (recommended, not required).
3. `osc enc reencrypt <prefix> --from <leaked> --before <time of the leak>`.
   Objects written at or after that time are reported as `after-cutoff` and
   left alone: anyone holding the leaked key and storage access could have
   forged them, and re-encrypting would make a forgery authentic under the new
   key. Review them by hand.
4. Delete old versions, confirm with `osc enc scan`, retire the leaked key.

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
let info = secure.inspect_encryption("s3://bucket/a").await?; // header only, no keys
```

Context keys starting with `osc:` or `aws-crypto-` are reserved; the match is
ASCII case-insensitive, so `OSC:key` is rejected too. Reading a
non-encrypted object with an encrypted client fails with `Error::NotEncrypted`;
read it with the plain view (`secure.without_encryption()`).

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
await secure.put_object_stream("s3://bucket/big.bin", chunks)  # any async iterable of bytes
info = await secure.inspect_encryption("s3://bucket/a.txt")

# Between modes, pipe the streams (bounded memory):
await plain.put_object_stream("file:///tmp/a.txt", await secure.get_object_stream("s3://bucket/a.txt"))
```

Encryption failures derive from `EncryptionError` (a `RuntimeError`):
`DecryptionError`, `UnknownKeyError`, `NotEncryptedError`. Invalid arguments
(reserved or oversized context, out-of-range `frame_length`, malformed URL
paths) raise `ValueError`. A missing object is still `FileNotFoundError`.

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
rejected. `OSC_KEYRING_COMMAND` runs through `sh -c` with no standard input
(log in to `kubectl`/`bao` beforehand); its output is kept in memory that is
wiped after use, and a non-zero exit, more than 64 KiB of output or more than
30 seconds is an error.

## Format and interop

Objects are AWS Encryption SDK message format version 2, algorithm suite
`0x0478` (AES-256-GCM with key commitment), framed with 64 KiB frames. They can
be decrypted by the AWS Encryption SDK with a raw AES keyring using key
namespace `osc-file` and key name = the key ID, with the 256-bit key from the
keyring's `material=` field. Byte layout and the `osc:` context entries are in
[the encryption spec](superpowers/specs/2026-10-26-client-side-encryption-design.md)
(section 6).
