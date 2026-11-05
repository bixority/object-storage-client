# Upgrading

## From 0.4.3 to 0.4.4

### Errors

- New `Error::Archived(String)`: the object is archived and cannot be read or
  copied until it is restored (S3) or rehydrated (Azure). S3
  `InvalidObjectState` (Glacier Flexible Retrieval, Deep Archive and the
  Intelligent-Tiering archive tiers) used to be `Error::Denied`. Azure
  `BlobArchived` used to be `Error::ObjectStore(AlreadyExists { .. })`, or
  `Error::Generic` from a server-side copy. Code that matched those for an
  archived object must match `Error::Archived`. `Error` is
  `#[non_exhaustive]`, so existing code still compiles.
- Python raises `PermissionError` for it. That is unchanged for S3; for Azure
  it was `RuntimeError`.

### `osc-gui`

- An archived object (S3 `GLACIER` or `DEEP_ARCHIVE` without a restored copy,
  Azure `Archive` tier) shows ❄, and one being restored or rehydrated shows ⌛.
  Both are greyed, with the reason as hover text.
- Copy, move and rename skip such a selected object and report it as failed
  with the reason; the other selected entries still run. Delete is not
  affected. An archived object inside a selected folder, or in an S3
  Intelligent-Tiering archive tier (a listing does not show those), still
  fails when the job reaches it, now with an "Archived" error.
- An AWS profile connection is refused while `AWS_ENDPOINT_URL` is set in the
  environment, as `osc --profile` already refused it: the SDK's SSO and STS
  requests would send the profile's tokens to that endpoint. The panel shows
  the error. Unset the variable, or set `S3_ENDPOINT` or `AWS_ENDPOINT_URL_S3`.

## From 0.4.2 to 0.4.3

### `osc-gui`

- AWS profiles without credentials are no longer offered in the connection
  selector; a grey note under the connections names them. A profile needs
  keys, `credential_process`, `credential_source`, `role_arn`,
  `web_identity_token_file`, `login_session` or SSO settings, in
  `~/.aws/config` or `~/.aws/credentials`. One with only settings such as
  `region` could not be used anyway. Add credentials and press "Rescan" to
  list it.
- The "Up" button is gone. Double-click the ".." row at the top of the list
  to go up one level; Backspace still does the same.

## From 0.4.1 to 0.4.2

### `osc-gui`

- The `--profile` option and the `LEFT` and `RIGHT` arguments are removed, and
  passing them is a usage error. Both panels start on Local, the current
  directory. Pick a connection (an AWS profile, gcloud configuration, Azure
  storage account or a provider's environment variables) in the selector at
  the left of each panel's address bar, then type the address. Scripts that
  passed those arguments must drop them.
- A connection uses only its own settings. An AWS profile connection no longer
  reads `S3_ENDPOINT` and the other `S3_*` and `AWS_*` variables, which
  `--profile` did; set `endpoint_url` in the profile, or use the
  "AWS · environment" connection, which is offered when such variables are set.
- Buckets are now listed: a cloud connection opens at its root, which shows the
  buckets as folders. Typing a bucket's URL still works.
- The scheme of a typed address must match the panel's connection: Local takes
  paths, `file://` and `http(s)://`. Otherwise pick a matching connection.
- "Log in" on the error banner of an AWS SSO profile writes the token to
  `~/.aws/sso/cache` (mode 0600), as the AWS CLI does, so osc-gui now writes
  to that directory.

## From 0.3.0 to 0.4.0

This release contains breaking changes; read every section below before
upgrading. Rust API changes are listed under Errors (`Error::Denied`),
Encryption (`EncryptionConfig::new`, the now crate-private `KeyProvider` and
key types) and AWS credentials and signing (`presign_s3` returns `Result`).

### Errors

- New `Error::Denied(String)`: the store answered 401 or 403. It is returned
  from every call, including `create_bucket` and `bucket_exists`. Code that
  matched `Error::ObjectStore(object_store::Error::PermissionDenied { .. })` or
  `Unauthenticated { .. }` must match `Error::Denied` instead, and code that
  matched `Error::Generic` for a denied bucket call likewise. Python raises
  `PermissionError` for it, now also from `create_bucket` and `bucket_exists`
  (they raised `RuntimeError`).

### Encryption

- An object is judged to be encrypted by its first three bytes, not the first
  byte. A plain object of three or more bytes that starts with `0x01` or `0x02`
  (for example `02 9a 11`) is now `Error::NotEncrypted`; it used to be a
  decryption error. The reverse also holds: damage to byte 1 or 2 of an
  encrypted object is reported as `NotEncrypted` too (reads still fail), so
  `osc enc scan` and `reencrypt` count such an object as not encrypted.
- `FormatError::UnsupportedSuite` is now returned only for suite `0x0578`; any
  other suite bytes are `FormatError::NotOsc`.
- The encrypted client looks at the keyring file (at most once per check
  interval) when it asks for the active key, so it sees the key a rotation on
  disk made active. The Python `Keyring.__repr__` still never reads the file.
- `Client::reencrypt` (and Python `reencrypt`) refuses a `from` key that the
  keyring does not hold, before any listing or read: Rust gets
  `Error::Key(KeyError::UnknownId)`, Python raises `EncryptionError`. It used to
  return `Skipped` for every object. A foreign message with several data keys
  is no longer rewritten from a key the keyring lacks; the CLI already refused
  that.
- `osc enc scan` and `osc enc reencrypt` count a listed entry of size 0 as
  not encrypted without reading it. S3 and GCS list a console folder marker
  (`p/sub/`) as an empty `p/sub`; it used to count as `unreadable` or `error`
  and made both commands exit 1. `Client::reencrypt` and `inspect_encryption`
  on a direct URL are unchanged.
- `KeyProvider` is no longer public. `EncryptionConfig::new` takes an
  `Arc<Keyring>` instead of an `Arc<dyn KeyProvider>`; code that passed a
  `Keyring` compiles unchanged. A custom provider was never usable, because
  its data-key types had no public constructors or accessors. Removed from
  `object_storage_client::encryption`: `DataKey`, `EncryptedDataKey` and
  `EncryptionContext`; removed from `encryption::keys`: `KeyProvider`,
  `DataKey` and `EncryptedDataKey`. The `KeyProvider` methods
  `namespace`, `active_key_name` and `holds_key` are gone from the public API
  with it, including on `Keyring`; use `Keyring::status().active` for the
  active key's ID. `CallerContext`, `MAX_CONTEXT_LEN` and `KEY_OBJECT` stay.
- A whole-buffer `put` or `put_with_context` of 8 MiB or more on the encrypted
  client, and Python `put_object` on an encrypted client, no longer builds the
  whole ciphertext in memory before the upload starts. It is sealed in slices
  while the parts are sent (to `file://` and to stores with multipart upload),
  so peak memory falls from about twice the object to the object plus about
  40 MiB. The stored bytes are the same.
- `Keyring::status` no longer waits for a reload that another task is running;
  it reports the last known state. Keep the keyring file on a local or tmpfs
  filesystem: a reload still runs a synchronous `stat` on the calling thread.

### Copy and move

- The encrypted client now always decrypts and re-encrypts on copy and move,
  with a fresh data key and message ID. It no longer copies bytes, uses a
  server-side copy or renames a `file://` object, even inside one bucket or for
  a client with object-key binding off. Copies inside one bucket now cost a
  full download and upload.
- The plain client now refuses to copy or move an encrypted object to another
  name with `Error::InvalidInput`, because the byte copy would be unreadable:
  the header binds the object name. Copy it with the encrypted client instead.
  The same name in another bucket, and objects without a binding, still copy.
- Every plain copy or move now makes one extra ranged request to read the start
  of the source. Without the `encryption` feature, any object that starts with
  the bytes `02 04 78` is treated as bound and refused for another name.
- The plain client now copies and moves between two buckets (containers) of
  one provider server-side: S3 `CopyObject`, GCS `objects.rewrite` and Azure
  `Copy Blob` (same storage account). Before, such a copy downloaded and
  uploaded every byte. See [Copy and move](BEHAVIOR.md#copy-and-move) for when
  it applies; otherwise the copy is streamed as before. The encrypted client
  never copies server-side.
- A server-side copy sends no storage class or access tier, so the destination
  gets the provider's default for a copy, as a streamed copy does. User
  metadata and the content headers are copied by the provider.
- A store with a provider-side encryption setting (S3 `AWS_SERVER_SIDE_ENCRYPTION`
  and its key settings, Azure `AZURE_STORAGE_ENCRYPTION_KEY`) or S3 Express
  still streams between buckets, so that the write applies the setting.
- A move between buckets keeps its source when the source changed since it was
  read (`Error::SourceChanged`), and the destination is then not written.
- GCS and Azure bucket copies are tested against an emulator that is not the
  real service; S3 against Garage only. Azure with an Entra ID (bearer token)
  credential is covered by stub tests only; a server that cannot verify the
  source answers `CannotVerifyCopySource` and the copy is streamed.
- An Azure copy or move inside one container waits for `Copy Blob` to finish
  (polling the destination for at most 120 s) and fails if the copy failed,
  was aborted or is still pending after that. A move deletes its source only
  after the copy succeeded. Each Azure server-side copy costs at least one
  extra `HEAD` request, and the account needs read access to the destination.

### Listings no longer follow symlinks on `file://`

- `Client::list`, `list_stream`, Python `list_objects` and `list_objects_stream`,
  `osc ls`, `osc enc scan` and `reencrypt` no longer follow symlinks below the
  prefix. A link to a file or to a directory is not listed, and nothing below
  a linked directory is listed. In 0.3.0 they were listed, so a recursive
  copy, move, rename or re-encryption reached files outside the prefix. The
  prefix itself is still resolved as named. A caller that relied on the old
  behaviour has to list the link target itself.
- In `osc-gui`, a local symlink is shown with a link mark and acted on as the
  link: Delete removes only the link (0.3.0 deleted the files behind a linked
  folder), Rename and a Move to a local folder rename the link, and Copy or a
  Move to another store skips it with "symbolic link skipped". Links inside a
  copied, moved or renamed folder are skipped. See GUI.md.

### Moves and writes on `file://`

- A move on one file system now refuses a destination that is a directory,
  FIFO, socket or device with `Error::InvalidInput` and leaves it alone. It
  used to replace a FIFO or socket and fail with an `ObjectStore` error on a
  directory. A source that is a directory, FIFO, socket or device is refused
  with `Error::InvalidInput` by `cp` and `mv` (it was `NotFound`). A source
  symlink is moved as a link.
- A move across file systems now gives the destination the source's permission
  bits (without setuid, setgid and sticky), also over a file you own, as a move
  on one file system does. It used to keep the destination's bits.
- The local backend now syncs the directories of both ends after a rename,
  copy or delete, and on Windows syncs the files it puts. Each costs an
  `fsync`.
- On Linux, a destination directory with a default POSIX ACL now decides the
  mode of a new file through a probe file, as the kernel would, instead of the
  umask. A new file there is no longer widened to `0666` minus the umask.
- A staging file left by a failed or cancelled write is now removed on a
  blocking thread, so it may exist for a moment after the call returns.

### WebDAV collections on `http(s)://`

- `Client::delete` (`osc rm`, `delete_object`) and `move_object` (`osc mv`) on
  an `http(s)://` URL first send a `PROPFIND` with `Depth: 0`. A URL that names
  a WebDAV collection is refused with `Error::InvalidInput`; before, the server
  removed the collection and everything in it. A 404, 405 or 501 answer lets the
  call go on, any other answer fails it. This costs one extra request.
- A copy or move inside one `http(s)://` origin is sent with `Overwrite: F`
  and never replaces an existing destination. When the destination exists the
  copy is streamed with a `PUT` (buffered, limited to 1 GiB), which overwrites
  a file as before and is refused for a collection. Before, an existing
  collection was replaced and its members deleted.
- A same-origin `http(s)://` copy or move needs a server that supports WebDAV
  `COPY`. This was always so and is now documented.

### URLs

- The refusal of `..` and `.` segments, tabs, newlines and surrounding
  whitespace in URLs now also holds for URLs written without `//`
  (`file:/a/.\t.`, `http:host/a/..`, `s3:/b/k`). They used to slip through and
  were rewritten by the URL parser.
- `osc` checks each operand as you wrote it, before the URL parser rewrites it.
  `osc sign s3://b/a/../k`, `osc rm 's3://b/a<TAB>b'` and
  `osc enc reencrypt s3://b/t/../` now fail with an input error; they used to
  act on the rewritten URL. Local paths are not affected: `osc put x ../out/x`
  still resolves `..`.

### Buckets

- `create_bucket` and `bucket_exists` use the client settings of the store:
  `*_ALLOW_INVALID_CERTIFICATES`, `*_PROXY_URL`, `*_PROXY_CA_CERTIFICATE`,
  `*_PROXY_EXCLUDES`, `*_USER_AGENT`, `*_HTTP1_ONLY`, `*_HTTP2_ONLY`,
  `*_DISABLE_SYSTEM_CERTIFICATES` and `*_ALLOW_HTTP`. They used to ignore them,
  so a self-signed endpoint that worked for objects failed for `mb`. Each
  backend now has its own client (5 s connect, 30 s request, no redirects),
  and `StoreRegistry::http` is gone. Without `*_ALLOW_HTTP`, a plain `http://`
  endpoint is refused by the transport (S3 and GCS allow it as before:
  an `http://` S3 endpoint and the GCS default enable it).
- `create_bucket` on `gs://` after an HTTP 409 now lists the project's buckets
  (`buckets.list`) and succeeds only if `GOOGLE_CLOUD_PROJECT` lists the
  bucket. It needs `storage.buckets.list` besides `storage.buckets.create`. A
  bucket in another project is an error even if you can read or write it. This
  closes bucket squatting: reading a bucket's metadata does not prove you own
  it.
- With `AZURE_SKIP_SIGNATURE=true` or `GOOGLE_SKIP_SIGNATURE=true`, bucket
  calls are sent unsigned and no longer query the instance metadata service.
  `bucket_exists` on Azure raises an error for a 404 then, because an anonymous
  request for a private container is answered with 404. Pre-signing a `gs://`
  URL on such a store fails with `SigningUnsupported`.
- Azure bucket calls use the account and endpoint the store was configured with,
  aliases included (`AZURE_ENDPOINT`). They are refused with
  `AZURE_STORAGE_USE_EMULATOR` or `AZURE_USE_FABRIC_ENDPOINT` unless an
  endpoint is set. GCS bucket calls fall back to `GOOGLE_BASE_URL` when
  `GCS_BASE_URL` is unset.

### AWS credentials and signing

- `S3_ACCESS_KEY_ID` is now sent without any session token and without an empty
  `x-amz-security-token`, in requests and in pre-signed URLs.
  `AWS_SESSION_TOKEN` is still ignored with `S3_*` keys. Temporary credentials
  must use `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`,
  or a profile; the old documentation showed `S3_*` keys with a token, which
  never worked.
- `sign::presign_s3` and `presign_s3_at` return `Result<Url>` instead of `Url`
  and no longer panic. An unusable URL, such as a bucket name that is not a
  valid host in virtual-hosted style, is `Error::InvalidInput`
  (Python: `ValueError`).
- A URL signed with `content_length` or `content_type` now keeps
  `x-amz-request-payer=requester` when `AWS_REQUEST_PAYER` is set. Before, it
  was dropped and S3 answered 403.
- With an AWS profile, a cached credential with at least 1 minute left is used
  while a refresh is running, slow or failed (it is retried after 30 s), and
  the error shows once the credential has run out. A credential lookup is
  shared between callers: after a timeout the next call joins it for up to 5
  minutes, instead of starting another `credential_process`. A timed-out helper
  is not killed.

### Shared clients and credentials

- One client now shares HTTP clients (connections and the loaded system CA
  store) between stores with the same transport settings, and one credential
  provider per provider across all buckets, so tokens from web identity (IRSA),
  instance metadata, GCS OAuth or Azure AD are fetched once, not per bucket.
  The variables of a client are fixed, so every bucket would have used the same
  credentials. Stores that skip signing, and AWS profiles, are unchanged.
  Changes to the system CA store are picked up by a new `Client`, not by an
  existing one.

### CLI

- The `cli` Cargo feature now implies `encryption`, so `--features cli` alone
  builds an `osc` with the `--keyring`, `keyring` and `enc` commands. Before,
  that build had no keys and `--require-encryption` could only fail. Builds with
  default features are unchanged.
- `osc ls --url` prints the full URL of each object instead of its name. Plain
  `osc ls` still prints the literal store names, so a name with `%` in it must
  not be pasted into a URL; use `--url` to pass listed objects to other
  commands.
- The key-source check runs for every command. With `--keyring` (or
  `OSC_KEYRING_FILE`) and `OSC_KEYRING_COMMAND` both set, `enc inspect`,
  `enc scan` and the `keyring` helpers now exit 1, as `ls` and the other
  commands already did.
- `osc put -r` refuses a `file://` destination that is the source directory or
  inside it. Before, it uploaded its own output; copy to a directory outside
  the source.
- `osc put FILE` now fails, and commits nothing, when the file shrinks during
  the upload. Before, it committed the short object and exited 0.
- A timed-out `OSC_KEYRING_COMMAND`, or one still running when `osc` is
  interrupted, has its shell killed. Processes the shell started may still keep
  running; use a single command or `exec`.
- Progress and listing lines are written asynchronously: SIGINT and SIGTERM now
  end `osc` even while standard output is blocked. The output is unchanged.

### GUI folder operations

- `osc-gui` refuses to copy or move a folder into itself, and to copy or move
  into the same folder, even when the two URLs encode the path differently
  (for example `(` and `%28`).
- Keys pressed in one frame can no longer act on the previous folder's rows
  while a new folder is loading: Select all and Enter are ignored until the
  first page arrives.
- A copy, move or rename of folders that would plan more than 1,000,000
  objects fails before anything runs.
- A zero-size console folder marker below a copied, moved or renamed folder no
  longer shows up as a "not found" failure.

### Python

- The type stub now declares the client methods as `def m(...) ->
  asyncio.Future[T]` instead of `async def`. The runtime is unchanged: each
  call already started its work and returned a future. A type checker now
  rejects `asyncio.create_task(client.get_object(url))` and
  `TaskGroup.create_task(...)`, which already raised `TypeError` at run time.
  Await the future, pass it to `asyncio.gather` or `asyncio.ensure_future`, or
  wrap the call in a coroutine.
- The exit hook now also waits for multipart uploads that are still running,
  not only for aborts already started. An upload still running at exit (its
  future was never cancelled) can delay exit by up to 10 s.
- A reserved or oversized encryption context now makes
  `EncryptedObjectStorageClient.put_object_stream` (and the Rust
  `put_stream_with_context`) fail before any local I/O. A `file://` destination
  no longer gets empty parent directories in that case.
- `put_object_stream` now pipes a `ByteStream` natively, with no Python call
  per chunk, so a busy event loop no longer slows the copy. The `ByteStream`
  is consumed and closed by the call: reading it afterwards raises
  `RuntimeError`. A closed `ByteStream`, or one with a read pending, is
  refused with `RuntimeError` and nothing is written. Other async iterables
  are read as before.

### Fewer requests

- A header probe (encrypted metadata, `inspect_encryption`, `osc enc scan`,
  `osc enc reencrypt` and the plain client's copy check) first asks for 1 KiB
  instead of 16 KiB. A header that does not fit is read again, so it costs a
  second request.
- `osc enc reencrypt` takes the extra slots of a large object (8 MiB or more)
  only when it rewrites it. A large object that is skipped takes one slot, so
  a re-run goes faster. A rewritten large object reads its header twice.
- `osc-gui` deletes selected objects on a bucket store in batches, and
  copies objects within one S3 store without a `HEAD` for the size.
  Folder listings of a copy, move or rename run several at a time, and the
  existence check runs 32 at a time instead of 8.
- `get_pre_signed_urls` on GCS signs 8 URLs at a time when it has to ask the
  IAM `signBlob` API for each one.

### Performance

- Writes to `file://` go through a 512 KiB buffer. A write in progress holds
  that much more memory, and its staging file (`dst#N`) can stay empty until
  512 KiB have arrived or the stream ends.
- `osc-gui`: after a job, only the panels it touched are listed again, and the
  other keeps its loaded pages, selection and cursor. The busy indicator
  redraws about 8 times a second instead of at display rate.

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
  (none for `http(s)://`) and fail when a download goes silent for 5 minutes.
  Large downloads and slow consumers that used to fail with a timeout now
  finish. An upload is not covered by that: each upload request (one put or one
  part) must be fully sent and answered within the 5 minutes. Set
  `AWS_TIMEOUT`, `AWS_READ_TIMEOUT` (or the `GOOGLE_` / `AZURE_` names) to keep
  the old limits or to allow slower uploads; see
  [Request timeouts](CONFIGURATION.md#request-timeouts).
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
