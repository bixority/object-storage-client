# Upgrading

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
