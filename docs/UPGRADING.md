# Upgrading

## From 0.2.0 to 0.2.1

Despite the patch version number, this release contains breaking changes.
Read every entry below before upgrading from 0.2.0.

### Credentials

- With a named AWS profile (`--profile`, `profile=` or `Client::with_s3_profile`),
  the profile now overrides static keys from `S3_*` and `AWS_*` variables. In
  0.2.0 the static keys won when both were set.
- `Client::with_s3_profile` returns a `Result` and fails with
  `Error::InvalidInput` for an empty or blank profile name. `osc --profile ""`
  exits 1 and `ObjectStorageClient(profile="")` raises `ValueError`; both used
  to fail only at the first request.
- A global `AWS_ENDPOINT_URL` is refused when a profile is used. Use
  `S3_ENDPOINT` or `AWS_ENDPOINT_URL_S3` instead.
- With a named AWS profile, the profile's S3 `endpoint_url` (the `services`
  entry for S3, else the profile's `endpoint_url`) is now used when no endpoint
  variable is set, as the AWS CLI does. In 0.2.0 only the profile's region was
  read, so requests went to AWS. `AWS_IGNORE_CONFIGURED_ENDPOINT_URLS=true` or
  `ignore_configured_endpoint_urls = true` in the profile switches this off.

- Empty `S3_REGION`, `AWS_REGION` and `AWS_DEFAULT_REGION` values are ignored
  (an empty `AWS_REGION` used to hide the profile's region and gave the
  endpoint `https://s3..amazonaws.com`). With several endpoint variables set,
  the store and the bucket calls now use the same one, in this order:
  `S3_ENDPOINT`, `AWS_ENDPOINT_URL_S3`, `AWS_ENDPOINT`, `AWS_ENDPOINT_URL`.
  With `AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true`, `create_bucket` and
  `bucket_exists` follow the same addressing as the object calls.
- `AWS_WEB_IDENTITY_TOKEN_FILE` counts as a credential source only together
  with `AWS_ROLE_ARN`, and `AWS_CONTAINER_CREDENTIALS_FULL_URI` only together
  with `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`, as in `object_store`. With a
  custom endpoint and only one of a pair, requests are now sent unsigned
  instead of signed with the instance role's key.

### Errors

- Text that comes from a provider in `Error::ObjectStore` (error bodies, object
  paths, listed keys) has control and bidirectional characters escaped as
  `\u{..}` and is capped (paths at 1 KiB, messages at 4 KiB). Variants other
  than `NotFound`, `Precondition`, `AlreadyExists`, `NotModified`,
  `PermissionDenied` and `Unauthenticated` are reported as
  `object_store::Error::Generic`. `Error::ObjectStore` no longer implements
  `From` through `#[from]`; `?` on an `object_store::Error` still converts.
- Provider error documents no longer carry signing details: only `Code`,
  `Message`, `RequestId`, `HostId`, `Resource`, `Key` and `BucketName` of an
  XML error are kept, so a `SignatureDoesNotMatch` no longer shows the
  canonical request or the session token.

- Python exception types changed. Caller-argument errors now raise
  `ValueError` instead of `RuntimeError`: an unsupported scheme, an invalid
  URL, an unknown pre-sign method, an empty URL list, URLs from different
  origins and content-header binding on a non-S3 store. `frame_length`,
  `expires_in_secs` and `content_length` outside their range raise
  `ValueError` instead of `OverflowError` (ints beyond +-2**127 still raise
  `OverflowError`). HTTP 401 and 403 answers raise `PermissionError` instead
  of `RuntimeError`; `FileNotFoundError` is unchanged. Local file failures
  (`file://` staging, `create_bucket` and `bucket_exists` on a directory)
  raise the matching `OSError` subclass. Code that catches `RuntimeError` for
  these cases must catch the new types.
- Rust callers: `Error::Generic` is no longer returned for an unknown
  `SignMethod`, an empty URL list, URLs from different backends, content
  binding on a non-S3 store, or a missing bucket or container name; these are
  `Error::InvalidInput` now. Failures of the crate's own local file operations
  (staging, the `file://` bucket calls) are `Error::Io` with the original
  `ErrorKind`, no longer `Error::Generic`. Operations that go through
  `object_store` still report `Error::ObjectStore`.

### Build

- The Linux x86_64 wheels and sdist of 0.1.6 to 0.2.0 required an x86-64-v3
  (AVX2) CPU. This release builds for baseline x86-64.

### URLs

- Input the URL parser used to rewrite silently is now refused with
  `Error::InvalidInput` (Python: `ValueError`) for every operation: a query or
  fragment (`s3://b/invoice#12.pdf` used to address `invoice`), a tab, newline
  or surrounding whitespace in a URL, `.` and `..` segments (also `%2E%2E`) in
  every URL except `file://`, and a bucket-only URL for an object operation
  (`delete('s3://b')` used to send a bucket-level request). This reverses the
  0.2.0 rule that `s3://b/a/../b` addresses `b`. `delete_by_prefix` and
  pre-signing also refuse `.` and `..` in `file://` URLs and local paths.
  `delete_by_prefix('s3://b')` still deletes every object in the bucket.
- User names and passwords in URLs are refused for every scheme, for example
  `https://user:pw@host/x`. Configure credentials through the provider's
  environment variables.

### Uploads

- A write to an `http(s)://` store is buffered in memory and now fails with
  `Error::InvalidInput` (Python: `ValueError`) above 1 GiB, instead of growing
  the process until it is killed. The same applies to `osc cp` and `mv` into
  such a store and to `put_stream`.
- `put` and `put_with_context` of 8 MiB or more are sent as multipart uploads
  on S3, GCS and Azure, so they work above 5 GiB and are no longer bound to one
  request's timeout. A failed upload is aborted as for `put_stream`.
- Multipart uploads keep at most 32 MiB of parts in flight: nothing changes up
  to a `size_hint` of about 70 GiB; beyond that fewer parts run at once, so
  very large uploads are slower and use less memory.
- Python: a process that exits after cancelling a `put_object_stream` now
  waits up to 10 s at interpreter exit for the multipart abort to finish (the
  CLI already did). Exit is not delayed when nothing is pending.

### Request timeouts

- Object requests no longer fail after 30 s in total. S3, GCS, Azure and
  `http(s)://` requests now connect within 5 s, have a 24 h total deadline
  (none for `http(s)://`) and fail after 5 minutes without progress. Large
  downloads and slow consumers that used to fail with a timeout now finish. Set
  `AWS_TIMEOUT`, `AWS_READ_TIMEOUT` (or the `GOOGLE_` / `AZURE_` names) to keep
  the old limits.
- Each profile credential lookup (SSO, STS, `credential_process`) is bounded
  to 30 s and fails with a "timed out" error instead of hanging.

### Pre-signed URLs

- A validity of 0, under 1 second or over 7 days (604800 s) is refused with
  `Error::InvalidInput` (Python: `ValueError`) for every scheme, also in
  `osc sign --expires-in`. Azure account-key SAS longer than 7 days used to be
  accepted; a very large value used to crash the process.

### Listing

- `list`, `list_stream` and `delete_by_prefix` on cloud stores consider only
  keys strictly below the prefix. The prefix's own folder marker (`p/` for the
  prefix `p`) is no longer listed and no longer deleted together with the
  prefix, and a key that vanishes during `delete_by_prefix` no longer fails it.

### Local paths

- `..` in a local path is resolved lexically, like in a `file://` URL
  (`put('/a/b/../c')` writes `/a/c`). The library used to refuse it.

### Buckets

- `create_bucket` on `s3://` succeeds only for a new bucket or
  `BucketAlreadyOwnedByYou`. `BucketAlreadyExists` (another account owns the
  name) and every other failure are errors; 0.2.0 reported success.
- `create_bucket` on `gs://` checks the bucket after an HTTP 409 and fails if
  the caller cannot read it (another project owns the name). On `az://` only
  `ContainerAlreadyExists` counts as success; `ContainerBeingDeleted` and other
  409s are errors.
- `bucket_exists` raises an error on HTTP 401 and 403 instead of returning
  `true`: a wrong key or signature is answered the same way, so existence is
  unknown.

### Copy, move and `file://` writes

- `copy` to `file://` no longer makes a hard link. It streams the source into
  a new, independent file (own inode and owner, the source's permission bits).
  Code that relied on two names sharing one file must create the link itself.
  A copy of a symlink copies the file behind it.
- Every move inside one non-`file` store (S3, GCS, Azure, HTTP) now deletes the
  source only if it still matches what was copied, and returns
  `Error::SourceChanged` otherwise; the destination stays written. In 0.2.0
  such a move deleted the source unconditionally. Callers must handle
  `SourceChanged` for same-store moves too. A move costs one extra `HEAD`.
- A copy or move inside S3 whose source is larger than 5 GiB is streamed with
  a multipart upload instead of failing with a single `CopyObject`. No change
  is needed; the copy is slower than a server-side one.
- Streamed plain copies (between buckets or providers) keep `Content-Type`,
  `Content-Encoding`, `Content-Disposition`, `Content-Language` and
  `Cache-Control`, and user metadata within one kind of store. They used to
  drop them. Encrypted copies are unchanged. The storage class is never
  copied.
- `put`, `cp`, `mv` and `osc put` to a `file://` path that exists and is not a
  regular file (a directory, device, FIFO or socket) fail with
  `Error::InvalidInput` (Python: `ValueError`). In 0.2.0 they replaced the node
  with a regular file. Use `osc get` to write an object into a device or FIFO.
- On macOS and other Unix systems without `/proc`, new `file://` files and
  `osc get` downloads are now staged privately (`0600`) and finish with `0666`
  minus the umask, and a copy takes its source's permission bits. Before, they
  were created with the default mode while being written. A probe file named
  `.osc-umask-<random>` is created and removed in the destination directory.
  The destination directory must therefore allow creating files, as it
  already must for staging.

### Encryption

- On Unix, a keyring file that its group can write (mode `0660`, `0620`, ...)
  is refused with `KeyError::Permissions` (Python: `EncryptionError`), at load
  and at reload. Group read (`0640`, `0440`) is still accepted. Run
  `chmod g-w` on the file. Other platforms check no permissions.
- `reencrypt` no longer changes an object's binding to its name: it keeps the
  `osc:key` of the object (or its absence) whatever the client's
  `bind_object_key`. A client with binding refuses an object bound to another
  name, also in a dry run. `osc enc reencrypt` can now rotate objects that
  were written without binding.
- `copy(u, u)` and `move_object(u, u)` of an encrypted object do nothing,
  whatever its key. In 0.2.0 an object on an old key was re-encrypted in place
  to the active key. Use `reencrypt` for that.
- `reencrypt --before` / `before=` treats an object for which the store
  reports no modification time as after the cutoff (`after-cutoff`), where
  0.2.0 treated it as older than any cutoff. The cutoff is documented as
  unauthenticated: see the key leak procedure in
  [Encryption](ENCRYPTION.md).
- `key_names` of an object's encryption info (`describe`, `inspect_encryption`,
  `osc enc scan`) lists only the keys of EDKs of the keyring's own provider
  (`osc-file`). An object with an EDK of another provider (for example
  `aws-kms`) used to be listed under a bogus name made of that EDK's bytes;
  such EDKs are now left out, and an object with only foreign EDKs has no key
  names.
- A header whose encryption context has a field that overruns the context
  region now fails as `format error: malformed header: context field overruns
  the context`, where 0.2.0 said `truncated header`. A header cut short by the
  end of the input still says `truncated header`.

### Rust API

- `Client::get_backend` is crate-private; it was `#[doc(hidden)]` and returned
  a crate-private trait object. To compare stores use `Client::get_store`.
  Other `#[doc(hidden)]` items stay public but are test hooks, outside semver.
- `ObjectMetadata`, `EncryptionInfo`, `KeyringStatus` and `KeyInfo` are
  `#[non_exhaustive]`: read their fields, do not build them with a struct
  literal or match them exhaustively.
- The fields of `EncryptionConfig` are private. Use `EncryptionConfig::new` and
  its builder methods (`bind_object_key`, `frame_length`, `limits`).

### CLI

- `osc enc scan` exits 1 when an object could not be read (it exited 0 before)
  or when the listing failed, and `osc enc reencrypt` exits 1 on a listing
  error. Both JSON summaries carry a `"listing-errors"` count, and an
  unreadable object's JSON line has an `"error"` field.
- `osc put a.csv s3://b/reports/`, `osc put a.txt ./out/` and every other
  single-object command (`get`, `rm`, `cp`, `mv`, `exists`, `sign`, ...) now
  refuse a URL or path that ends in `/`, as do the library and Python
  (`ValueError`). They used to drop the slash and address the object `reports`
  or the file `out`. Name the object explicitly.
- `osc enc reencrypt --from ID` exits 1 when ID is not in the keyring.
- `osc` reads a URL or path operand with the library's rules, as Python does.
  A local file name that contains `:` and starts with a scheme of two or more
  letters (`data:2026.bin`) is now refused as an unsupported scheme; write
  `./data:2026.bin`. A `~` or `~/...` operand, even quoted, now means the home
  directory (it was the literal `~` directory under the working directory).
- `osc put -r` starts uploading while it still walks the directory. A read
  error in the walk now stops the run when it is reached; files listed before
  it may already be uploaded (a failed walk used to upload nothing).
- `osc keyring status` reads the keyring from standard input by default, as the
  other `keyring` helpers do (it used the configured key source when one was
  set). On a terminal it refuses. Scripts that check the live key source must
  add `--configured`; until they do they fail loudly instead of checking the
  wrong keyring.
- `osc ls`, `rm`, `exists`, `mb` and `bucket-exists` no longer read the
  keyring: `OSC_KEYRING_COMMAND` is not run for them, and a missing or broken
  keyring no longer fails them. Setting both key sources is still an error.
- `osc` exits with 141 when its standard output is a closed pipe, instead of
  panicking.
- `osc sign --method` is validated when the arguments are parsed: a value other
  than GET, PUT, POST, DELETE or HEAD (any case) is a usage error that lists
  the allowed values and exits 2. It used to exit 1 with an `unknown HTTP
  method` error.
  `osc --version` now prints `osc VERSION` (it was an unexpected argument, exit
  2), and every subcommand has a description in `osc --help`.
- Progress lines and the `enc scan` / `enc reencrypt` JSON drop the URL user
  name too, not only the password.
- Only the `osc` binary reads `OSC_KEYRING_COMMAND` now. A program that embeds
  the library's `Cli` (a doc-hidden type) and calls `run()` must pass the
  command itself with `Cli::with_keyring_command`; the `osc` command is not
  affected.

### Python

- `Keyring.status()` now checks the keyring file first (at most once per
  reload interval, 5 s), so `last_reload_error` reflects a broken or repaired
  file on an idle process. `repr(keyring)` does no I/O.
- A `ByteStream` read that was superseded by a read on another event loop and
  whose loop runs again now raises `RuntimeError` ("superseded") instead of
  taking a chunk that belongs to the new reader.
- The extension declares that it needs the GIL. Free-threaded CPython is
  unsupported: on 3.14t and later importing the module re-enables the GIL with
  a `RuntimeWarning`.
- A call made outside a running event loop no longer marks the process as
  having started the runtime, so a process forked after it can use clients.
  The error of a process forked after the runtime really started has new text
  and still contains "forked".
- `EncryptedObjectStorageClient(keyring, profile=...)` accepts an AWS profile
  (keyword-only).

### Additions

- New and additive: `Client<Envelope>::get_with_context` and
  `get_stream_with_context`, `Decryptor::require_context`,
  `DecryptError::ContextMismatch`, and the keyword-only `context=` argument of
  the encrypted Python client's `get_object` and `get_object_stream`. They
  require a stored encryption context on reads; plain reads are unchanged.
- New: `list_objects_stream(url)` on both Python clients returns an async
  iterator of `list[str]` batches (at most 1024 names or 256 KiB), for
  prefixes too large for `list_objects`; `ListStream` is exported.

## From 0.1.x to 0.2.0

### Rust API

- `get_stream` returns a `ByteStream` of `Result<Bytes, Error>` (was
  `object_store::Result<Bytes>`), so that decryption errors can surface
  mid-stream.
- New `Error` variants: `InvalidInput`, `SourceChanged` and `Changed`, plus
  `NotEncrypted`, `Decryption` and `Key` with the default `encryption` feature.
  `Error` and `cli::CliError` are now `#[non_exhaustive]`, so a downstream
  `match` needs a wildcard arm.
- `ObjectMetadata` has new public fields `stored_size_bytes` and `encryption`;
  struct literals and exhaustive destructuring from 0.1.x no longer compile.
- `sign::s3_region()` is removed. The S3 backend resolves the signing region
  from its own configuration (`S3_REGION`, then `AWS_REGION` /
  `AWS_DEFAULT_REGION`, defaulting to `us-east-1`).
- `sign::presign_s3` takes `&SignOptions` instead of `BoundHeaders`;
  `BoundHeaders` and `content_binding_unsupported()` are removed.
- `ObjectStorageClient` is now a type alias for `Client<Plain>`. Its methods
  keep their signatures, so code that uses it compiles unchanged; rustdoc lists
  them under `Client`.

### URLs

- URL paths are percent-decoded: `s3://b/a%20b` addresses the key `a b`.
  Migration: 0.1.x stored keys with spaces or escaped characters double-encoded,
  so such objects are no longer found at the same URL (and encrypted ones fail
  the `osc:key` check). Request the old escaped name to read them, then copy or
  rename them to the decoded name.
- URL paths are validated: invalid (non-UTF-8) percent-escapes and empty
  segments (for example `s3://b/a//b`, which 0.1.x silently normalized to
  `a/b`) are rejected with `Error::InvalidInput` before any I/O. Python raises
  `ValueError`. `.` and `..` segments, including `%2E%2E`, are resolved by URL
  parsing (`s3://b/a/../b` addresses `b`), so validate untrusted key names
  before building URLs from them.

### Behaviour

- `delete_by_prefix` rejects an empty URL and the `file://` root, and on
  `file://` no longer follows symlinked directories below the prefix.
- Pre-signing rejects a `.` or `..` segment or bucket, a query or fragment, and
  a URL with no object key; 0.1.x signed them.
- Moving an object onto itself does nothing; in 0.1.x a cloud store's rename
  (a copy, then a delete) could delete it. A move between stores keeps the
  source and returns `Error::SourceChanged` if it changed while being copied.
- Copies and moves between stores stream instead of holding the whole object
  in memory.

See [Behaviour and limits](BEHAVIOR.md) for the current rules.

### Python

- Rejected input raises `ValueError` instead of `RuntimeError`: malformed URL
  paths, and the `delete_objects_by_prefix` and pre-signing refusals above.
- `ByteStream`: overlapping reads now raise `RuntimeError` instead of being
  serialized.
- The Python extension is built with pyo3 `extension-module` through maturin
  1.15 or newer.

### CLI

- `osc get-stream URL` prints its "Streaming" line on standard error; 0.1.x
  printed it on standard output, mixed into the object bytes. With a FILE
  argument it behaves like `osc get`.
- Progress lines escape control characters and drop URL passwords, queries and
  fragments; `osc ls` escapes control characters in names.
