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
  - a tab, a newline or surrounding whitespace in a URL;
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
- Storage error messages may include the request URL: do not put secrets in
  object URLs (queries and user names are refused).

### Unaddressable object names

Some names that a store accepts cannot be addressed by any URL: a name with an
empty segment (`a//b`), a `.` or `..` segment, or an ASCII control character.
`object_store` refuses to build a path from them, so no `osc`, `Client` or
Python call can read or delete such an object, and a dot segment would even
resolve to another key.

- Listings, `osc enc scan`, `osc enc reencrypt` and `delete_by_prefix` stop at
  such an object with `Encountered object with invalid path`.
- Remove it with the provider's own tools, for example
  `aws s3api delete-objects --bucket B --delete '{"Objects":[{"Key":"a//b"}]}'`,
  which sends the key in the request body.
- Treat an unexpected one as possibly planted by someone who holds storage
  credentials; see [After a key leak](ENCRYPTION.md#after-a-key-leak).

## Copy and move

A move never deletes the object it wrote. Copying or moving an object onto
itself (the same path, scheme aliases such as `gs`/`gcs` or `az`/`abfs`, or a
`file://` path through a symlinked directory) does nothing for plain and
encrypted objects alike, whatever the key; an object on an old key stays on it
(use `reencrypt`). Only a `file://` path through a symlinked directory with
object key binding on is re-encrypted under its new name, and that rewrite is
abandoned with `Error::Changed` if the file changed meanwhile. Every move, server-side or
streamed, deletes the source only if it still matches the object that was
copied (size, last-modified, e-tag, version). If it changed, for example
because the destination is another name for the same server (an IP and a
hostname, a CNAME) or a concurrent writer replaced it, the source is kept and
`Error::SourceChanged` is returned; the destination stays written. A
same-second rewrite with identical bytes on a store that reports no e-tag or
version cannot be detected. Only a move inside one `file://` file system is an
atomic rename.

An S3 copy or move whose source is larger than 5 GiB is streamed with a
multipart upload instead of a single `CopyObject`, which S3 rejects above that
size. The size comes from one extra `HEAD` request when the caller does not
already know it.

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
nested deeper than 128 levels are refused.

## Listing

Names are relative to the prefix and always strictly below it. Names ending in
`/` are listed without the slash, so `x` and `x/` are indistinguishable. The
prefix's own folder marker is not listed.

`Client::list_stream` and `osc ls` hand names over as the pages arrive, but a
page itself is held whole: S3, GCS and Azure return as many as the provider's page size allows,
and an `http(s)://` listing is a single
`PROPFIND` with `Depth: infinity` whose whole answer is read before the first
name appears. `Client::list` and Python `list_objects` hold the entire listing.
The client does not detect a server that keeps returning the same continuation
token (a hostile one can equally keep returning new ones), so such a listing
never ends. For an untrusted endpoint use `list_stream` (or Python
`list_objects_stream`) and put a timeout of your own around the call.

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
source's bits; anything else gets `0666` minus the umask. Setuid, setgid and
sticky bits are never carried over.

The umask is read from `/proc` on Linux. Where that is not possible (macOS and
other Unix systems), it is learned by creating and removing an empty probe file
(`.osc-umask-<random>`) in the destination directory, so staging is private
and the final mode is the same everywhere. If the probe fails too, a copy is
created with its source's permission bits (the kernel applies the umask), so a
private source stays private while written; other new files then get the
system default mode.

`put`, `cp` and `mv` refuse a destination that exists and is not a regular file
(a directory, device, FIFO or socket) with `Error::InvalidInput` and touch
nothing. A symlink at the destination is replaced by the new file and never
followed. `osc get` remains the way to stream an object into a device or FIFO.

A write is synced, the file and then its directory, before it is reported
done, so a deferred write error (full disk, quota, I/O error) fails the write
instead of leaving a short file in place, and a move never deletes its source
before the copy is durable. This costs two `fsync` calls per file.

A copy to `file://` always streams through the staging file and takes the
source's mode; it never makes a hard link. A move on one file system is a
rename.

## Large objects and streaming

- Uploads switch to multipart above 8 MiB. When the size is known (`osc put`,
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
  request.
- Upload memory. A multipart upload holds up to about five parts: four in
  flight and one being filled, about 40 MiB with 8 MiB parts. The parts in
  flight are limited by a fixed 32 MiB budget, so once the part size grows
  past 8 MiB (a `size_hint` beyond about 70 GiB) fewer parts run at once, down
  to one, and memory stays near the budget plus one part (a 5 TiB upload has
  parts of about 590 MiB and holds about two of them, one in flight and one
  filling). Input chunks of 64 KiB or more are handed over without copying;
  smaller ones are copied into blocks of their own. `osc put -r` (8 files at
  once) and `osc enc reencrypt` (`--jobs`, default 8) count an object of 8 MiB
  or more as 4 of those slots, so at most 2 such objects (about 80 MiB) run
  together with the defaults, and with `--jobs` below 4 one at a time.
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
  request of at most 16 KiB.
- Object requests connect within 5 s and have no total deadline under 24 h; a
  request that goes silent for 5 minutes (no response headers, or no body
  bytes) fails with a timeout. A slow download is therefore never cut off.
  See [Configuration](CONFIGURATION.md#request-timeouts).
- Bucket operations (`create_bucket`, `bucket_exists`) time out after 5 s to
  connect and 30 s per request.
- The client caches at most 256 backends, keyed by (scheme, userinfo, host,
  port). When the cache is full, the least recently used backend is dropped to
  make room; calls that already hold it are not affected.

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
also have as little as 5 minutes left when it signs. An `s3://` URL signed
with temporary credentials stops working when that session ends, whatever
`expires_in` says.

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
  first failure, and cleans up the uploads it abandons before exiting.
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
  it is yielded, so a source may reuse its buffer. On cancellation or error
  the multipart upload is aborted in the background, and the interpreter waits
  up to 10 s for pending aborts at exit (a forked child never does). Azure has
  no abort: uncommitted blocks expire after 7 days. `SIGKILL` or an unhandled
  `SIGTERM` can still leave an incomplete multipart upload, so configure an
  `AbortIncompleteMultipartUpload` lifecycle rule.
- The async runtime uses `min(CPU count, 4)` worker threads; set
  `OSC_WORKER_THREADS` before the first call to change it. The blocking pool
  (result delivery, DNS, local file I/O) is capped at 64 more threads.
