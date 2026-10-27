# Behaviour and limits

How the client behaves at the edges. Everything here applies to Rust, Python
and the CLI alike unless a section says otherwise.

## URLs

- URL paths are percent-decoded: `s3://b/a%20b` addresses the key `a b`.
- Invalid (non-UTF-8) percent-escapes and empty segments (for example
  `s3://b/a//b`) are rejected with `Error::InvalidInput` (Python: `ValueError`)
  before any I/O.
- `.` and `..` segments, including `%2E%2E`, are resolved by URL parsing
  (`s3://b/a/../b` addresses `b`), so validate untrusted key names before
  building URLs from them.
- Storage error messages may include the request URL: do not put secrets
  (userinfo, SAS or other query tokens) in object URLs.

## Copy and move

A move never deletes the object it wrote. Copying or moving an object onto
itself (the same path, scheme aliases such as `gs`/`gcs` or `az`/`abfs`, or a
`file://` path through a symlinked directory) does nothing for plain objects;
with object key binding on, a move through a symlink re-encrypts the file under
its new path and keeps it. A move that streams deletes the source only if it
still matches the object that was read (size, last-modified, e-tag, version).
If it changed, for example because the destination is another name for the
same server (an IP and a hostname, a CNAME) or a concurrent writer replaced it,
the source is kept and `Error::SourceChanged` is returned; the destination
stays written. A same-second rewrite with identical bytes on a store that
reports no e-tag or version cannot be detected.

## Deleting by prefix

`delete_by_prefix` rejects an empty URL and a `file://` prefix that resolves to
the filesystem root (`Error::InvalidInput`). A cloud URL with no object prefix
deletes every object in the bucket. On `file://` it never follows a symlink
below the prefix: a link is deleted as a link and its target is untouched (the
prefix itself is resolved as named). Only files and links are removed;
directories stay, `name#<digits>` upload staging files are skipped, and trees
nested deeper than 128 levels are refused.

## Writes to `file://`

Writes to `file://` (`put`, `put_stream`, `cp`, `mv`, `osc put`) go through a
private staging file (`dst#N`, mode `0600`) that is renamed into place only when
complete, and removed on failure or cancellation. Replacing a file you own keeps
its permission bits; a copy, move or `osc put` of a local file takes the
source's bits; anything else gets `0666` minus the umask. Setuid, setgid and
sticky bits are never carried over.

## Large objects and streaming

- Uploads switch to multipart above 8 MiB. When the size is known (`osc put`,
  copies and moves) the part size grows with it, up to the providers'
  10,000-part, 5 TiB limit; a `put_stream` of unknown size uses 8 MiB parts and
  so fails beyond about 78 GiB. Python `put_object_stream` accepts
  `size_hint=` (0 to 5 TiB) for uploads of known size beyond 78 GiB.
- Every write to a plain `http(s)://` destination is buffered whole in memory,
  because that backend has no multipart upload: `put_stream`, and also
  `copy_object`/`move_object` (and `osc cp`/`mv`) into an `http(s)://` store,
  which hold the entire object before sending it.
- `file://` objects are read in 512 KiB chunks, and a file truncated during a
  read fails the read instead of ending it early.
- `Client::list_stream(url)` streams names instead of collecting them, and
  `osc ls` prints them as they arrive.
- Encrypted metadata and `inspect_encryption` read the header with one ranged
  request of at most 16 KiB.
- Bucket operations (`create_bucket`, `bucket_exists`) time out after 5 s to
  connect and 30 s per request.
- The client caches at most 256 backends; further (scheme, userinfo, host,
  port) combinations are built per call.

## Pre-signed URLs

`get_pre_signed_url(s)` and `osc sign` refuse (`Error::InvalidInput`) input that
URL parsing would silently rewrite into a different request: a `.` or `..` path
segment or bucket in any spelling (including `%2e%2e`), a query or fragment, and
a URL with no object key (the bucket itself). A key that merely contains `..`
inside a name (`a..b`) is fine.

## CLI

- `put` and `get` stream, so memory stays bounded; `get` and `get-stream FILE`
  replace the destination only after a clean end.
- `osc get` keeps the mode of an existing destination only if you own it, and
  never copies setuid, setgid or sticky bits. A new download, or a destination
  owned by someone else, is written to a private (`0600`) temp file and gets the
  default mode (`0666` minus the umask) just before the rename.
- `osc get-stream URL` writes only the object bytes to standard output;
  progress goes to standard error.
- `osc put --recursive` percent-encodes each file name as one key segment, so a
  `\` in a name stays part of it; non-UTF-8 names are rejected. It scans and
  opens the tree through directory handles without following symlinks
  (symlinks inside it are skipped), and refuses a file or directory that is no
  longer what the scan found. Trees nested deeper than 128 levels are refused.
  It uploads up to 8 files at once (at most two of 8 MiB or more), stops at the
  first failure, and cleans up the uploads it abandons before exiting.
- Names and URLs in progress lines, `ls` output and errors have control and
  bidi characters escaped as `\u{..}`; progress lines drop URL passwords,
  queries and fragments. `osc sign` still prints the full pre-signed URL.
- On SIGINT, SIGTERM or SIGHUP, `osc` removes partial downloads and waits up to
  10 s for interrupted uploads to abort their multipart state or staging files;
  a second signal exits at once, and a file operation stuck in the kernel (for
  example opening a FIFO nobody reads) no longer keeps the process alive.
  `kill -9` can still leave a `.osc-tmp-*` file or staging file behind.

## Python

- `ByteStream` reads follow `asyncio.Queue.get()` rules: reads must not
  overlap, and a cancelled or timed-out read loses no data.
- Multiprocessing: use spawn or forkserver, or fork before the first use of any
  client (the async runtime does not survive a fork). A client used in a child
  forked after the runtime started raises `RuntimeError` instead of hanging.
- The extension can only be imported in the main interpreter; importing it in
  a subinterpreter raises `ImportError`. Under mod_wsgi, use
  `WSGIApplicationGroup %{GLOBAL}`.
- `get_object_stream` joins small backend chunks into reads of up to about
  256 KiB; do not rely on a chunk size. `get_object` peaks at about twice the
  object size, so stream large objects instead.
- `put_object` accepts any bytes-like object (`bytes`, `bytearray`,
  `memoryview`, `array.array`, ...) or a list of ints. `put_object_stream` reads
  its source ahead by at most 256 KiB (or 1024 chunks) and copies each chunk as
  it is yielded, so a source may reuse its buffer.
- The async runtime uses `min(CPU count, 4)` worker threads; set
  `OSC_WORKER_THREADS` before the first call to change it.
