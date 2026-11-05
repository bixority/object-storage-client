# Behaviour and limits

How the client behaves at the edges. The rules apply to Rust, Python and the
CLI alike. The file-system rules for `file://` and `osc` are implemented for
Linux and other Unix systems, and each section says where macOS or Windows
differ.

## URLs

- URL paths are percent-decoded: `s3://b/a%20b` addresses the key `a b`.
- Invalid (non-UTF-8) percent-escapes and empty segments (for example
  `s3://b/a//b`) are rejected with `Error::InvalidInput` (Python: `ValueError`)
  before any I/O.
- These are refused with `Error::InvalidInput` (Python: `ValueError`) before
  any backend is built or request sent, and the message never quotes the input:
  - a query (`?`) or fragment (`#`), which would silently cut the key
    (`s3://b/invoice#12.pdf` is not `invoice`);
  - a tab, a newline or surrounding whitespace in a URL (also one written
    without `//`, like `file:/a`; a local path may hold them). `osc` checks
    each operand as typed, before the URL parser can rewrite it;
  - user names and passwords, for every scheme: configure credentials through
    the provider's environment variables;
  - `.` and `..` segments, including `%2E%2E`, in every URL except `file://`;
  - a bucket-only URL (`s3://b`, `s3://b/`) or a URL ending in `/`
    (`s3://b/reports/`, `./out/`) for an object operation (get, put, delete,
    exists, metadata, copy, move, pre-signing): the slash used to be dropped,
    so the call addressed the object `reports`. Listing, `delete_by_prefix`
    and the bucket calls accept it; `put -r` takes its destination as a
    prefix.
- `file://` URLs and local paths resolve `..` lexically (`file:///a/b/../c` is
  `/a/c`), except in `delete_by_prefix` and pre-signing, which refuse it. In a
  local path `?` and `#` are ordinary file name characters. A local path is
  treated exactly like the `file://` URL of its absolute path, so
  `put('../out/x')` and `osc put x ../out/x` resolve `..` the same way
  (lexically, without following symlinks).
- Pre-signing applies the same rules to every URL, and `.` and `..` segments
  are refused for every scheme.
- Untrusted input. The client is not a sandbox: a URL has the full authority of
  the process. A bare path, `~`, `~/x` or a single-letter scheme such as `C:`
  becomes a `file://` URL that reads and writes anywhere the process user can,
  and `http(s)://` reaches any host. Object requests to `http(s)://` follow
  redirects (up to 10 hops, also to other hosts); bucket calls do not follow
  any. On `file://` a `..` in a key is resolved lexically and can leave the base
  directory. Build URLs from a scheme and bucket (or base directory) that the
  application fixes itself, pass only validated key names, and never forward a
  whole URL received from a user.
- Storage error messages may include the request URL: do not put secrets in
  object URLs (queries and user names are refused).

### Unaddressable object names

Some names that a store accepts cannot be addressed by any URL: a name with an
empty segment (`a//b`), a `.` or `..` segment, or an ASCII control character.
`object_store` refuses to build a path from them, so no `osc`, `Client` or
Python call can read or delete such an object, and a dot segment would even
resolve to another key.

- Listings, `osc enc scan`, `osc enc reencrypt` and `delete_by_prefix` stop at
  such an object with `Encountered object with invalid path` (the `file://`
  walk of `delete_by_prefix` says `Invalid path ...`). Control characters in
  the name are escaped in the message.
- Remove it with the provider's own tools, for example
  `aws s3api delete-objects --bucket B --delete '{"Objects":[{"Key":"a//b"}]}'`,
  which sends the key in the request body.
- Treat an unexpected one as possibly planted by someone who holds storage
  credentials; see [After a key leak](ENCRYPTION.md#after-a-key-leak).

## Copy and move

An encrypted copy or move never copies server-side and never copies bytes: it
decrypts the source and re-encrypts it to the destination, see
[Encryption](ENCRYPTION.md). A move deletes the source only after the
destination committed.

A plain copy or move first reads the start of the source with one extra ranged
request of 1 KiB (and a second one of at most 16 KiB only for a larger
header). An encrypted object whose header binds a name other than the
destination's is refused with `Error::InvalidInput` and nothing is written or
deleted, because the copy would be unreadable; a header that starts like ours
but does not parse is refused too. Other objects, and encrypted objects without
a binding or bound to the destination's name (the same name in another bucket),
are copied as is, server-side where possible. Without the `encryption` feature
only the first three bytes are checked, and any object that starts like a
message is refused when the destination name differs.

A move never deletes the object it wrote. Copying or moving an object onto
itself (the same path, scheme aliases such as `gs`/`gcs` or `az`/`abfs`, or a
`file://` path through a symlinked directory) does nothing for plain and
encrypted objects alike, whatever the key; an object on an old key stays on it
(use `reencrypt`). Only a `file://` path through a symlinked directory with
object key binding on is re-encrypted under its new name, and that rewrite is
abandoned with `Error::Changed` if the file changed meanwhile. Every move,
server-side or streamed, deletes the source only if it still matches the object
that was copied (size, last-modified, e-tag, version). If it changed, for
example because the destination is another name for the same server (an IP and a
hostname, a CNAME) or a concurrent writer replaced it, the source is kept and
`Error::SourceChanged` is returned; the destination stays written. A
same-second rewrite with identical bytes on a store that reports no e-tag or
version cannot be detected. Only a move inside one `file://` file system is an
atomic rename.

`osc-gui` gives each connection a client of its own. A copy or move between two
connections always streams, and a move between them onto the same URL is
refused with `Error::InvalidInput` and keeps the source, because both may reach
one object.

A copy or move inside one `http(s)://` origin is a WebDAV `COPY` (a move then
deletes the source as above), so the server must support `COPY`; one that does
not (405 or 501) fails the call, and the copy can be made with `get` and `put`
instead. The `COPY` is sent with `Overwrite: F`, so it never replaces anything.
When the destination already exists the copy is streamed with a `PUT`, buffered
and limited to 1 GiB like any `http(s)://` write: a file is overwritten, and a
WebDAV collection is refused by the server and left intact.

An `http(s)://` object delete or move first asks the server with a `PROPFIND`
(`Depth: 0`) what the URL names. A WebDAV collection is refused with
`Error::InvalidInput`, with or without a trailing slash, because deleting it
removes everything in it; `delete_by_prefix` removes only the files below a
prefix and keeps its directories. A 404, 405 or 501 answer (no WebDAV, or no
such resource) lets the call go on; any other answer, or a failed request,
fails it. A collection that appears between the check and the delete is not
caught. Copies are not checked.

An Azure copy or move inside one container waits for the server-side copy to
finish. Azure may answer `Copy Blob` before the copy is done, so the
destination is polled with `Get Blob Properties` (every 250 ms, doubling up to
4 s) for at most 120 s. A copy that failed or was aborted, or one still pending
after 120 s, returns an error and a move keeps its source. The pending copy is
not aborted and may still finish.

An S3 copy or move whose source is larger than 5 GiB is streamed with a
multipart upload instead of a single `CopyObject`, which S3 rejects above that
size. The size comes from one extra `HEAD` request when the caller does not
already know it. `osc-gui` passes the size its listing gave, so a copy there
sends no `HEAD`; a move still needs one.

A plain copy or move between two buckets (containers) of one provider is
server-side too, when all of these hold:

- both ends are S3, both GCS or both Azure (not across providers; the scheme
  aliases `gs`/`gcs` and `az`/`wasb(s)`/`abfs(s)` count as one provider);
- both reach the same endpoint (GCS: the same JSON API origin; Azure: the same
  storage account) and, for S3, the same region and addressing style;
- both use the very same credential provider, which one client shares between
  its buckets; a store that skips signing (anonymous) or uses a different
  provider streams, as does an Azure store whose credential is a SAS token
  (it covers one container, so it cannot read the source);
- for S3, neither bucket is requester-pays, a custom endpoint is not
  virtual-hosted, neither store uses S3 Express, and the source is at most
  5 GiB;
- neither store has a provider-side encryption setting, which the copy request
  would not carry: S3 `AWS_SERVER_SIDE_ENCRYPTION`, `AWS_SSE_KMS_KEY_ID`,
  `AWS_SSE_BUCKET_KEY_ENABLED` and `AWS_SSE_CUSTOMER_KEY_BASE64`, Azure
  `AZURE_STORAGE_ENCRYPTION_KEY`. Such a copy is streamed, and the write
  applies the setting. GCS has no such setting in the store configuration.

S3 sends a signed `CopyObject` with `x-amz-copy-source`, and
`x-amz-copy-source-if-match` when the e-tag is known (a move reads it first). A
`200` answer whose body is an `<Error>` document is a failure. A server that
answers `501 NotImplemented` makes the call stream. GCS sends `objects.rewrite`
with `ifSourceGenerationMatch` when the generation is known, and repeats it
with each `rewriteToken` until the answer says `done`, at most 1000 calls; the
destination exists only when the rewrite is done. Azure sends `Copy Blob` with
`x-ms-copy-source` (and `x-ms-source-if-match` for a known e-tag), authorized
with the destination's credential, then waits as above; a
`CannotVerifyCopySource` answer makes the call stream. A source that no longer
matches the known e-tag or generation (HTTP 412) fails the call with
`Error::SourceChanged` and writes nothing (for Azure, only with the error
code `SourceConditionNotMet`). Any other failed answer is an
error, never a silent stream, so a refused request is not retried under other
credentials. A copy request that fails before any answer, or is answered 429,
500, 502, 503 or 504, is sent again after 200 ms and 400 ms, three attempts in
all; a request answered 2xx is never sent again. Each attempt has the limits of
a bucket operation: 5 s to connect and 30 s in all, answer included. An S3
`200` counts as a copy only when its body is a `CopyObjectResult`: S3 may send
white space first and the result or an `<Error>` last, so an empty, cut-off or
other body is an error. A move deletes the source, on its own bucket, only
after the copy is complete, the destination is as large as the source was read
to be, and the source is unchanged. The provider copies the content type and
user metadata; no storage class or access tier is sent. Encrypted copies never
take this path.

A streamed plain copy keeps the source's `Content-Type`, `Content-Encoding`,
`Content-Disposition`, `Content-Language` and `Cache-Control`, and its user
metadata when source and destination are the same kind of store (S3 to S3, not
S3 to GCS). The storage class is not carried over. Encrypted copies carry no
attributes. Copies to `file://` have none to keep.

## Deleting by prefix

On Windows the `file://` walk is by path: it checks each entry with
`symlink_metadata` and never follows a link it sees, but without directory
handles a directory swapped for a link between that check and its use can
redirect the walk. Do not run it on a tree that others can change. The rest of
this section describes Unix.

`delete_by_prefix` rejects an empty URL and a `file://` prefix that resolves to
the filesystem root (`Error::InvalidInput`). A cloud URL with no object prefix
deletes every object in the bucket. Cloud stores list and batch-delete the keys
strictly below the prefix: the prefix's own folder marker and every other key
ending in `/` (console folder markers) cannot be addressed through
`object_store` and stay. A key that vanishes during the run counts as deleted,
so a repeated run succeeds. On `file://` it never follows a symlink
below the prefix: a link is deleted as a link and its target is untouched (the
prefix itself is resolved as named). Only files and links are removed;
directories stay, `name#<digits>` upload staging files are skipped, and trees
nested deeper than 128 levels stop the walk with `Error::InvalidInput`; files
deleted before that point stay deleted.

## Listing

Names are relative to the prefix and always strictly below it. Names ending in
`/` are listed without the slash, so `x` and `x/` are indistinguishable. The
prefix's own folder marker is not listed. A nested marker (`p/sub/`) is listed
as an empty `p/sub`; `osc enc scan` and `osc enc reencrypt` count a listed
entry of size 0 as not encrypted without reading it.

Names are the literal names in the store. URL paths are percent-decoded, so a
name with `%` in it, pasted after `s3://b/prefix/`, addresses another key.
`osc ls --url` prints a URL for each object that addresses exactly that object;
use it for scripting.

On `file://`, listings do not follow symlinks: a link below the prefix is
neither listed nor entered, whether it points to a file or a directory. This
covers `Client::list`, `list_stream`, Python `list_objects`, `osc ls`,
`osc enc scan` and `reencrypt`, and folder copy, move and rename in the GUI.
The prefix itself is resolved as named, so a prefix that is a link lists its
target. As with the Windows delete walk above, a directory swapped for a link
after the listing can still redirect the later per-object operations: do not
run them on a tree that others can change.

`Client::list_stream` and `osc ls` hand names over as the pages arrive, but a
page itself is held whole: S3, GCS and Azure return as many as the provider's page size allows,
and an `http(s)://` listing is a single
`PROPFIND` with `Depth: infinity` whose whole answer is read before the first
name appears. `Client::list` and Python `list_objects` hold the entire listing.
The client does not detect a server that keeps returning the same continuation
token (a hostile one can equally keep returning new ones), so such a listing
never ends. For an untrusted endpoint use `list_stream` (or Python
`list_objects_stream`) and put a timeout of your own around the call.

Every recursive `http(s)://` operation (`list`, `list_stream`, `osc ls`,
`delete_by_prefix`, `osc enc scan` and `reencrypt`, folder copy, move and
delete in the GUI) needs the server to answer `PROPFIND` with `Depth: infinity`
in full:

- Apache `mod_dav`: `DavDepthInfinity On`. By default it answers 403, which is
  reported as "permission denied".
- IIS: `allowInfinitePropfindDepth`.
- sabre/dav: `enablePropfindDepthInfinity = true`. By default it answers as
  `Depth: 1` without an error, so the listing silently stops one level deep and
  misses nested objects.

## Archived objects

Reading, copying or moving an archived object fails with `Error::Archived`
(Python: `PermissionError`): for S3 `InvalidObjectState` (a `GLACIER` or
`DEEP_ARCHIVE` object that is not restored, or an Intelligent-Tiering object in
an archive tier), for Azure `BlobArchived` (the Archive tier). Restore (S3) or
rehydrate (Azure) the object first. A delete is not affected, and neither are
`exists` and plain metadata (a `HEAD`); encrypted metadata and
`inspect_encryption` read the header and fail the same way.

## Writes to `file://`

This section describes Unix (Linux and macOS). On Windows the crate's own
writer is not used: `object_store` writes the file through its own staging
file (`dst#N`) with the folder's inherited ACL, and neither the destination's
nor a source's permissions are carried over. The two `fsync` calls, the
private staging file and the destination-type check below are Unix only.

Writes to `file://` (`put`, `put_stream`, `cp`, `mv`, `osc put`) go through a
private staging file (`dst#N`, mode `0600`) that is renamed into place only when
complete, and removed on failure or cancellation. Replacing a file you own keeps
its permission bits; a copy, move or `osc put` of a local file takes the
source's bits; anything else gets `0666` minus the umask. A move takes the
source's bits even over a file you own, as a rename keeps them; a copy onto
your own file keeps the destination's bits. Setuid, setgid and sticky bits are
never carried over.

The staging file is written through a 512 KiB buffer, one write per 512 KiB
rather than one per incoming chunk. A write in progress holds that much more
memory, and the staging file can be empty while the first 512 KiB arrive.

The umask is read from `/proc` on Linux, unless the destination directory has a
default POSIX ACL, which the kernel applies instead of the umask. Where that is
not possible (macOS and other Unix systems, or a directory with a default ACL),
it is learned by creating and removing an empty probe file
(`.osc-umask-<random>`) in the destination directory, so staging is private
and the final mode is the same everywhere. If the probe fails too, a copy is
created with its source's permission bits (the kernel applies the umask), so a
private source stays private while written; other new files then get the
system default mode.

`put`, `cp` and `mv` refuse a destination that exists and is not a regular file
(a directory, device, FIFO or socket) with `Error::InvalidInput` and touch
nothing, also for a move on one file system. `cp` and `mv` likewise refuse a
source that is a directory, FIFO, socket or device with `Error::InvalidInput`.
A source symlink is moved as a link, as `mv(1)` does. The check and the rename
are not atomic together: a destination swapped in between is replaced. A symlink at the destination is replaced by the new file and never
followed. `osc get` remains the way to stream an object into a device or FIFO.

A write is synced, the file and then its directory, before it is reported
done, so a deferred write error (full disk, quota, I/O error) fails the write
instead of leaving a short file in place, and a move never deletes its source
before the copy is durable. This costs two `fsync` calls per file. A move on
one file system is synced too: the directories of both ends are synced after
the rename (on Windows `object_store` also syncs the files it puts).

A copy to `file://` always streams through the staging file and takes the
source's mode; it never makes a hard link. A move on one file system is a
rename: the file keeps its inode and its mode, including setuid and setgid
bits, as with `mv(1)`. A move across file systems copies and then deletes the
source, and gives the same permission bits.

## Large objects and streaming

- Uploads of 8 MiB or more use multipart. When the size is known (`osc put`,
  copies and moves) the part size grows with it, up to the providers'
  10,000-part, 5 TiB limit; a `put_stream` of unknown size uses 8 MiB parts and
  so fails beyond about 78 GiB. Python `put_object_stream` accepts
  `size_hint=` (0 to 5 TiB) for uploads of known size beyond 78 GiB.
- Every write to a plain `http(s)://` destination is buffered in memory,
  because that backend has no multipart upload: `put_stream`, and also
  `copy_object`/`move_object` (and `osc cp`/`mv`) into an `http(s)://` store.
  Such a write is limited to 1 GiB and fails with `Error::InvalidInput` beyond
  that: before anything is read when the size is known, otherwise as soon as
  the buffer would pass the limit.
- `put` and `put_with_context` of 8 MiB or more are sent as multipart uploads
  wherever the store supports them, like `put_stream`; smaller bodies are one
  request. On the encrypted client such a `put` also goes to `file://`
  through the streaming path, so it seals the data in 1 MiB slices beside the
  upload and holds no whole ciphertext.
- Upload memory. A multipart upload holds up to about five parts: four in
  flight and one being filled, about 40 MiB with 8 MiB parts. The parts in
  flight are limited by a fixed 32 MiB budget (and are never fewer than one),
  so once the part size grows past 8 MiB (a `size_hint` beyond about 70 GiB)
  fewer parts run at once, down to one. Above about 278 GiB the part size
  exceeds the budget: memory is then about one part plus about one input chunk
  (a 5 TiB upload has parts of about 590 MiB, and up to about two parts when
  the input chunks are as large as a part). Input chunks of 64 KiB or more are handed over without copying;
  smaller ones are copied into blocks of their own. `osc put -r` (8 files at
  once) and `osc enc reencrypt` (`--jobs`, default 8) count an object of 8 MiB
  or more as 4 of those slots, so at most 2 such objects (about 80 MiB) run
  together with the defaults, and with `--jobs` below 4 one at a time.
  `osc enc reencrypt` takes the 4 slots only for an object it will rewrite; an
  object it skips is probed with one slot.
- `file://` objects are read in 512 KiB chunks, and a file truncated during a
  read fails the read instead of ending it early.
- `Client::list_stream(url)` streams names instead of collecting them, and
  `osc ls` prints them as they arrive.
- Python `list_objects` holds the whole listing in memory, twice while it is
  converted (a Rust `Vec` and the Python list). For a large prefix use
  `list_objects_stream(url)`, an async iterator of `list[str]` batches of at
  most 1024 names or 256 KiB of names, with the reading rules of `ByteStream`
  (including `aclose()`), or `osc ls` or the Rust `list_stream`.
- Encrypted metadata and `inspect_encryption` read the header with one ranged
  request of 1 KiB, which holds a typical header. A larger header takes a
  second ranged request of at most 16 KiB.
- Object requests connect within 5 s and have no total deadline under 24 h; a
  request that goes silent for 5 minutes (no response headers, or no body
  bytes) fails with a timeout. A slow download is therefore never cut off. An
  upload request (one put or one part) must be fully sent and answered within
  the 5 minutes: about 0.9 Mbit/s for the default 4 parts of 8 MiB in flight,
  about 28.6 Mbit/s for a 1 GiB `http(s)://` put.
  See [Configuration](CONFIGURATION.md#request-timeouts).
- Bucket operations (`create_bucket`, `bucket_exists`) and the requests of a
  server-side copy (see [Copy and move](#copy-and-move)) time out after 5 s to
  connect and 30 s per request. They never follow a redirect. They use the
  transport settings of the store: `*_ALLOW_HTTP`, `*_ALLOW_INVALID_CERTIFICATES`,
  `*_PROXY_URL`, `*_PROXY_CA_CERTIFICATE`, `*_PROXY_EXCLUDES`, `*_USER_AGENT`,
  `*_HTTP1_ONLY`, `*_HTTP2_ONLY` and `*_DISABLE_SYSTEM_CERTIFICATES` with the
  prefix of the provider (`AWS_`, `GOOGLE_`, `AZURE_`).
- The client caches at most 256 backends, keyed by (scheme, userinfo, host,
  port). When the cache is full, the least recently used backend is dropped to
  make room; calls that already hold it are not affected.
- One client shares HTTP connections and credentials across the buckets of a
  provider: stores with the same transport settings use one HTTP client
  (so the system CA store is loaded once, and a change to it is picked up only
  by a new client), and the S3, GCS and Azure stores share one credential
  provider, so a web identity, instance metadata or OAuth token is fetched once
  and not per bucket. They stay shared when a store is evicted. A store that
  skips signing takes no part, and an AWS profile has its own shared
  credential. Two clients never share anything.

## Pre-signed URLs

`get_pre_signed_url(s)` and `osc sign` refuse (`Error::InvalidInput`) input that
URL parsing would silently rewrite into a different request: a `.` or `..` path
segment or bucket in any spelling (including `%2e%2e`), a query or fragment, and
a URL with no object key (the bucket itself). A key that merely contains `..`
inside a name (`a..b`) is fine.

The validity must be between 1 second and 7 days (604800 s) for every scheme;
anything else is refused (`Error::InvalidInput`, Python: `ValueError`) before
any URL is parsed or backend built.

A URL signed with temporary credentials (a session token from SSO, an assumed
role or `AWS_SESSION_TOKEN`) carries the token and stops working when that
session ends, whatever expiry was requested. A cached profile credential can
also have as little as 1 minute left when it signs, if its renewal is slow or
failing.

## CLI

On Windows, `osc get` writes its temp file without the private mode and keeps
neither the mode nor the owner check of an existing destination, `osc put -r`
walks and opens the tree by path (no directory handles, so no identity check),
and only Ctrl-C (SIGINT) is handled as a signal. Core dumps are turned off for
`osc` on Unix only; see [Encryption](ENCRYPTION.md#core-dumps). The bullets
below describe Unix.

- `put` and `get` stream, so memory stays bounded; `get` and `get-stream FILE`
  replace the destination only after a clean end. `put -r` uploads while it
  walks the directory, so memory does not grow with the number of files.
- `osc get` follows a symlink at the destination: the file it points to is
  replaced atomically through a temp file in that file's directory, and the
  link stays. Unlike `put`, `cp` and `mv`, it also does not create missing
  parent directories. It syncs the file and then its directory before it exits
  0 (the directory sync is Unix only).
- `osc get` keeps the mode of an existing destination only if you own it, and
  never copies setuid, setgid or sticky bits. A new download, or a destination
  owned by someone else, is written to a private (`0600`) temp file and gets the
  default mode (`0666` minus the umask) just before the rename.
- `osc get-stream URL` writes only the object bytes to standard output, each
  chunk as it arrives; progress goes to standard error. The output is complete
  only if `osc` exits 0. A failed read, or a truncated or tampered encrypted
  object, stops it part way; every byte written was authenticated, but the
  object may be cut short. In a pipeline use `set -o pipefail`, or give FILE for
  an all-or-nothing download.
- `osc put --recursive` percent-encodes each file name as one key segment, so a
  `\` in a name stays part of it; non-UTF-8 names are rejected. It scans and
  opens the tree through directory handles without following symlinks
  (symlinks inside it are skipped), and refuses a file or directory that is no
  longer what the scan found. Trees nested deeper than 128 levels are refused.
  It uploads up to 8 files at once (at most two of 8 MiB or more), stops at the
  first failure, and cleans up the uploads it abandons before exiting. It
  refuses a `file://` destination that is the source directory or inside it
  (`osc put -r d d/backup/`), as `cp -r` does; a second mount of the same
  directory is not seen.
- `osc put FILE` fails, and commits nothing, when a regular file ends short of
  the size it had at the start, as when a log is truncated while it uploads. A
  file that grows is read to its end, and so are `/proc` and sysfs files.
- Progress and listing lines are written through the async runtime, so a
  SIGINT or SIGTERM ends the command (exit 130 or 143) even when standard
  output is a pipe that nobody reads.
- Names and URLs in progress lines, `ls` output and errors have control and
  bidi characters escaped as `\u{..}`; progress lines and the `enc scan` and
  `enc reencrypt` JSON drop URL userinfo (user name and password), queries and
  fragments. `osc sign` still prints the full pre-signed URL.
- On SIGINT, SIGTERM or SIGHUP, `osc` removes partial downloads and waits up to
  10 s for interrupted uploads to abort their multipart state or staging files;
  a second signal exits at once, and a file operation stuck in the kernel (for
  example opening a FIFO nobody reads) no longer keeps the process alive.
  `kill -9` can still leave a `.osc-tmp-*` file or staging file behind.

## Python

- `ByteStream` reads follow `asyncio.Queue.get()` rules: reads must not
  overlap, and a cancelled or timed-out read loses no data.
- `await stream.aclose()` (or `async with contextlib.aclosing(stream)`)
  releases the connection or file at once instead of when the object is freed.
  It is idempotent and raises `RuntimeError` while a read is pending. A read
  after it raises `RuntimeError`; it never reports end-of-stream.
- Each client method starts its operation when called and returns an
  `asyncio.Future`. Await it, pass it to `asyncio.gather` or
  `asyncio.ensure_future`, or cancel it to stop the operation.
  `asyncio.create_task` and `TaskGroup.create_task` reject it, so wrap the call
  in a coroutine for them.
- Set environment variables before the first client call, and do not modify
  `os.environ` while client calls are in flight: backends read the environment
  on worker threads without the GIL.
- Multiprocessing: use spawn or forkserver, or fork before the first client call
  (the async runtime does not survive a fork). A client used in a child
  forked after the runtime started raises `RuntimeError` instead of hanging.
- Platforms: wheels exist for GIL CPython 3.13-3.15 on manylinux
  x86_64/aarch64, macOS arm64 and Windows x86_64. Other platforms build the
  sdist with Rust 1.99 or newer. Free-threaded CPython is unsupported: 3.13t
  does not build, and on 3.14t and later importing the module re-enables the
  GIL with a `RuntimeWarning` (the module declares `gil_used`; `ByteStream`'s
  overlap check relies on the GIL).
- The extension can only be imported in the main interpreter; importing it in
  a subinterpreter raises `ImportError`. Under mod_wsgi, use
  `WSGIApplicationGroup %{GLOBAL}`.
- `get_object_stream` joins small backend chunks into reads of up to about
  256 KiB; do not rely on a chunk size. `get_object` peaks at about twice the
  object size, so stream large objects instead.
- `put_object` accepts any bytes-like object (`bytes`, `bytearray`,
  `memoryview`, `array.array`, ...) or a list of ints. `put_object_stream` reads
  its source ahead by at most 256 KiB (or 1024 chunks) and copies each chunk as
  it is yielded, so a source may reuse its buffer. A `ByteStream` passed to
  `put_object_stream` is piped natively (no Python call per chunk, so the
  bytes move even while the event loop is busy; a busy loop still delays the
  start and end of each call) and is consumed and closed, also when the upload
  then fails: later reads raise `RuntimeError`. A call refused before it starts
  (a closed stream, a read pending, no running event loop) leaves the stream as
  it was. On cancellation or error
  the multipart upload is aborted in the background, and the interpreter waits
  up to 10 s for pending aborts at exit (a forked child never does). An
  upload still running at exit, whose future was never cancelled, can delay
  exit by up to 10 s too. Azure has
  no abort: uncommitted blocks expire after 7 days. `SIGKILL` or an unhandled
  `SIGTERM` can still leave an incomplete multipart upload, so configure an
  `AbortIncompleteMultipartUpload` lifecycle rule.
- The async runtime uses `min(CPU count, 4)` worker threads; set
  `OSC_WORKER_THREADS` before the first call to change it. The blocking pool
  (result delivery, DNS, local file I/O) is capped at 64 more threads.
