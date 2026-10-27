# Client-side Encryption (Phase 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `EncryptedObjectStorageClient` that writes and reads objects in an ESDK-v2-compatible envelope format (suite `0x0478`). The existing `ObjectStorageClient` keeps today's behaviour, and both share one store registry.

**Architecture:**
- One generic `Client<C: Codec>`. The `Plain` codec is the identity; the `Envelope` codec calls an I/O-free crypto core (`src/encryption/`) built on `ring`.
- A new streaming upload (`put_stream`) built on `object_store` multipart backs bounded-memory pipes between plain and encrypted views.
- Python gets a second pyclass that shares its methods through one macro. The CLI becomes generic over the client.

**Tech Stack:** Rust 2024 (MSRV 1.99), `object_store` 0.14.2, `ring` 0.17, `pyo3`/`pyo3-async-runtimes` 0.30, `clap` 4, maturin; new: `zeroize`, `rustix` (unix, `process` feature), `proptest` (dev), `cargo-fuzz` (separate crate), Python `aws-encryption-sdk` (dev-only oracle).

**Spec:** `docs/superpowers/specs/2026-10-26-client-side-encryption-design.md`. Read it in full before starting any task. Section numbers below (§N, DN) refer to it.

## Global Constraints

- `unsafe_code = "forbid"`; clippy pedantic + nursery with `-D warnings`; `unwrap_used = "deny"`. Gate: `cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic` and `cargo fmt --all -- --check` clean after every task.
- Randomness only from `ring::rand::SystemRandom` (D11).
- Key bytes never cross into Python, `Debug`, `Display`, logs or error strings (D12, §10.10).
- Encrypted objects: suite `0x0478`, message format v2, framed (`0x02`), default frame length `65536`, one EDK, provider ID `"osc-file"`, context keys `"osc:fmt" = "1"` and `"osc:key" = <Path string>` (§6).
- Caller context keys starting with `"osc:"` or `"aws-crypto-"` are rejected before any I/O.
- `DecodeLimits` defaults: `max_edks = 8`, `frame_length ∈ [4096, 4194304]`, `max_header_len = 16384`.
- Encrypted writes set no `attributes` (D10).
- `put_stream`: part size 8 MiB, `max_concurrency` 4; single `put` when input < one part; HTTP buffers the whole input (D18). Commit only on a clean end of input; abort on error or drop.
- The encrypted client never falls back to plaintext (D8).
- `Codec` is sealed. `get_pre_signed_url(s)` exist only on the plain client (D6).
- Python exceptions: `EncryptionError(RuntimeError)` → `DecryptionError`, `UnknownKeyError`, `NotEncryptedError` (D17). Missing objects still raise `FileNotFoundError`.
- Cargo feature `encryption`: in `default`, and also listed in `[tool.maturin] features`.
- Releases: milestone 0 ships as 0.1.20; milestone 1 onward as 0.2.0 (D16). Each milestone is one PR; `main` stays green.

## Review Focus

1. **Empty objects:** `put(url, b"")`, `put_object_stream` over zero chunks, and plaintext sizes that are exact multiples of the frame length must round-trip on both clients. `get_object_metadata` must report `size_bytes == 0` (Task 4.2), and the stream case is covered in Tasks 1b.1 and 4.1.
2. **Unusual object keys:** spaces, `%`, non-ASCII. `put` and `get` through the same URL must agree on the bound `osc:key`, and copying to such a key must succeed (Task 4.3).
3. **Caller context on copy and move:** re-encryption keeps the caller pairs, and only `osc:key` changes. A reserved prefix in the caller context fails before any request is made (Tasks 4.1 and 4.3).
4. **Wrong chunk types:** a Python input iterator that yields `str` or `int` instead of bytes gets `TypeError`, the upload is aborted, and no object is created (Task 1b.2).
5. **Key directories as people actually edit them:** CRLF line endings and trailing whitespace are accepted. A duplicate key ID across two files is an error. An `active` ID with no matching file fails at construction, not on the first write (Task 3.2).

---

## File Structure

| Path | Responsibility |
|---|---|
| `src/client.rs` | `Client<C>`, aliases, `Error`, `ObjectMetadata`, `EncryptionInfo`, all generic operations |
| `src/registry.rs` (new) | `StoreRegistry`: backend cache (`entry()` API) + shared `reqwest::Client`; URL parsing helpers moved here |
| `src/codec.rs` (new) | sealed `Codec` trait, `Location`, `Resolved`, `CallerContext`, `Plain` |
| `src/upload.rs` (new) | `upload_stream`: single-put / multipart / HTTP-buffer strategies, `AbortGuard` |
| `src/encryption/mod.rs` (new, `cfg(feature="encryption")`) | re-exports; `EncryptionConfig`, `DecodeLimits` |
| `src/encryption/context.rs` | `EncryptionContext` (sorted map) + ESDK serialization + reserved-key validation |
| `src/encryption/header.rs` | `Header`, `HeaderCodec::{parse, serialize}` with limits |
| `src/encryption/suite.rs` | HKDF-SHA512 key derivation, commitment key, header auth tag |
| `src/encryption/frame.rs` | `Encryptor`, `Decryptor`, `FrameLayout` |
| `src/encryption/keys/mod.rs` | `DataKey`, `EncryptedDataKey`, `KeyProvider`, `KeyError` |
| `src/encryption/keys/raw_aes.rs` | Raw-AES-keyring-compatible wrap/unwrap |
| `src/encryption/keys/file.rs` | `FileKeyProvider`, key file format, `generate_key_file` |
| `src/encryption/envelope.rs` | `Envelope` codec |
| `src/hex.rs` | `encode`/`decode`, crate-level so `sign.rs` builds without the `encryption` feature (`hex_encode` moved out of `sign.rs`) |
| `src/python.rs` → `src/python/{mod.rs,stream.rs,client.rs,encryption.rs}` | module init; `ByteStream` + Python-iterator→Rust-stream adapter; shared-methods macro + both client classes; `FileKeyProvider` + exceptions |
| `src/cli.rs` | global `--keys/--key-id`, generic helpers, `keys`, `enc` subcommands |
| `python/object_storage_client/__init__.py`, `__init__.pyi` | re-exports + stubs |
| `tests/functional/suite.rs` (new) | `client_suite!` macro: shared Rust suite for any `Client<C>` |
| `tests/functional/encryption.rs` (new) | envelope client functional tests |
| `tests/integration/s3_encryption.rs` (new) | Garage tests (`#[ignore]`, same env pattern as `s3_storage_ops.rs`) |
| `tests/vectors/` (new) | ESDK oracle vectors + golden files (+ their key file) |
| `tests/python/` (new) | pytest suite |
| `scripts/esdk_oracle.py` (new) | dev-only Python ESDK vector generator / decryptor |
| `fuzz/` (new, separate crate) | fuzz targets |
| `.github/workflows/tests.yaml` (new) | push/PR: cargo test, clippy, fmt, pytest (current `ci.yaml` runs only on `release`) |

---

## Milestone 0 — bugs + Python harness (PR 1, 0.1.20)

### Task 0.1: Python test harness and PR CI

**Files:**
- Modify: `pyproject.toml`, `.gitignore`
- Create: `tests/python/conftest.py`, `tests/python/test_plain_client.py`, `.github/workflows/tests.yaml`

**Interfaces:**
- Produces: a `tmp_url` fixture (`str`, a `file://` URL of a fresh temp dir) and `uv run pytest tests/python` as the Python test command for every later task.

- [ ] **Step 1:** In `pyproject.toml`, add `[dependency-groups] dev = ["maturin>=1.9,<2", "pytest>=8", "pytest-asyncio>=0.24"]` and `[tool.pytest.ini_options] asyncio_mode = "auto"`, `testpaths = ["tests/python"]`. Add `python/object_storage_client/*.so`, `__pycache__/` and `.venv/` to `.gitignore`.
- [ ] **Step 2:** Write `test_plain_client.py`, which covers the existing API against `file://`: put/get round-trip, `get_object_metadata` keys (`location`, `last_modified`, `size_bytes`, `content_type`, `e_tag`, `version`), `object_exists` true/false, `list_objects` returning relative keys, copy/move, `delete_object`, `delete_objects_by_prefix`, `get_object` on a missing key raising `FileNotFoundError`, and `get_pre_signed_url` on `file://` raising `RuntimeError`.
- [ ] **Step 3:** Run `uv sync --group dev && uv run maturin develop && uv run pytest tests/python -v`. Expected: all pass.
- [ ] **Step 4:** Create `tests.yaml` (on `push` + `pull_request`) with three jobs: `cargo test --all-features`; fmt + clippy with the Global Constraints flags; and Python 3.14, which runs `uv sync --group dev`, `uv run maturin develop`, `uv run pytest`.
- [ ] **Step 5:** Commit: `test: add Python test harness and PR CI`.

### Task 0.2: `ByteStream` stops at EOF

**Files:**
- Modify: `src/python.rs:55-89`
- Test: `tests/python/test_plain_client.py`

- [ ] **Step 1:** Write the failing test `test_stream_async_for_terminates`. Put 200 000 bytes, collect with `async for`, and assert the joined chunks equal the data, no chunk is `None`, and the loop ends. Wrap it in `asyncio.wait_for(..., 10)`. Also add `test_stream_next_returns_empty_at_eof`.
- [ ] **Step 2:** Run `uv run maturin develop && uv run pytest -k stream`. Expected: the first test FAILS with a timeout or a `None` chunk.
- [ ] **Step 3:** In `__anext__`, resolve the end of stream to `Err(PyStopAsyncIteration::new_err(()))` instead of `Ok(None)`. Leave `next()` returning `b""`. Wrap the inner stream in `.fuse()` so polling after the end is safe.
- [ ] **Step 4:** Run the same command again. Expected: PASS.
- [ ] **Step 5:** Commit: `fix(python): raise StopAsyncIteration at end of ByteStream`.

### Task 0.3: One backend per (scheme, host); doc typo

**Files:**
- Modify: `src/client.rs:145-167`, `src/client.rs:428`
- Test: `tests/functional/client.rs`

- [ ] **Step 1:** Write the failing test `test_concurrent_first_use_builds_one_backend`. Use `#[tokio::test(flavor = "multi_thread", worker_threads = 8)]`, spawn 64 tasks calling `client.get_backend(&Url::parse("file:///tmp")?)`, and assert that all returned `Arc<dyn Backend>` are `Arc::ptr_eq`. Loop the whole test 50 times to make the race observable.
- [ ] **Step 2:** Run `cargo test --test functional concurrent_first_use`. Expected: it fails intermittently. If the race does not show up on this machine, keep the test anyway as a regression guard.
- [ ] **Step 3:** Replace get-then-insert with `self.stores.entry(key).or_try_insert_with(|| backend::build(url))` and clone the `Arc`. Fix the `excsynthia-pipelineept` typo to read `except`.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `fix: build exactly one backend per scheme and host`.

---

## Milestone 1 — generic client (PR 2, 0.2.0)

### Task 1.1: `StoreRegistry`, sealed `Codec`, `Plain`, `Client<C>`

**Files:**
- Create: `src/registry.rs`, `src/codec.rs`, `tests/functional/suite.rs`
- Modify: `src/client.rs` (whole file), `src/lib.rs`, `src/cli.rs:375-399`, `src/python.rs`, `tests/functional.rs`, `Cargo.toml` (version `0.2.0`)

**Interfaces:**
- Produces (`src/registry.rs`): `pub(crate) struct StoreRegistry { stores: DashMap<StoreKey, Arc<dyn Backend>>, http: reqwest::Client }`, plus `fn resolve(&self, url: &Url) -> Result<(Arc<dyn Backend>, ObjectPath)>` (the body of today's `get_backend`) and `fn http(&self) -> &reqwest::Client`.
- Produces (`src/codec.rs`):
  - `pub type CallerContext = std::collections::BTreeMap<String, String>;`
  - `pub struct Location { pub url: Url, pub path: ObjectPath }`
  - `pub(crate) struct Resolved { pub backend: Arc<dyn Backend>, pub loc: Location }`
  - `pub type ByteStream = BoxStream<'static, crate::Result<Bytes>>;`
  - `mod sealed { pub trait Sealed {} }`
  - `pub trait Codec: sealed::Sealed + Clone + Send + Sync + 'static`. Spell every async method as `fn …(…) -> impl Future<Output = …> + Send`. Public `async fn` in a trait trips rustc's `async_fn_in_trait` warning, which `-D warnings` turns into an error. The methods:
    - `encode(&self, loc: &Location, ctx: &CallerContext, data: Bytes) -> Result<Bytes>`
    - `encode_stream(&self, loc: Location, ctx: CallerContext, plain: ByteStream) -> ByteStream`
    - `decode(&self, loc: Location, stored: BoxStream<'static, object_store::Result<Bytes>>) -> ByteStream`
    - `get_bytes(&self, loc: &Location, result: GetResult) -> Result<Bytes>`
    - `describe(&self, backend: &dyn Backend, loc: &Location) -> Result<ObjectMetadata>`
    - `copy(&self, src: &Resolved, dst: &Resolved, delete_source: bool) -> Result<()>`
  - `#[derive(Clone, Copy, Debug, Default)] pub struct Plain;`
- Produces (`src/client.rs`):
  - `pub struct Client<C: Codec = Plain> { registry: Arc<StoreRegistry>, codec: C }` with `#[derive(Clone)]`, and `Default` for `Client<Plain>`
  - `pub type ObjectStorageClient = Client<Plain>;`
  - `get_stream(&self, url: &str) -> Result<ByteStream>` (D16)
  - `ObjectMetadata` gains `pub stored_size_bytes: u64` (equal to `size_bytes` for plain) and `pub encryption: Option<EncryptionInfo>`
  - `pub struct EncryptionInfo { pub format: String, pub message_id: String /* hex */, pub key_names: Vec<String>, pub frame_length: u32, pub context: BTreeMap<String, String> }`, defined unconditionally so plain builds compile without the feature.
- Every existing public method moves into `impl<C: Codec> Client<C>` with the same name and signature. `new`, `get_pre_signed_url` and `get_pre_signed_urls` go into `impl Client<Plain>`. `parse_url`, `get_backend` and `get_store` remain `#[doc(hidden)] pub` on `impl<C: Codec> Client<C>`.

- [ ] **Step 1:** Write `tests/functional/suite.rs` as `macro_rules! client_suite { ($m:ident, $ctor:expr) => { mod $m { … } } }`. Its tests are generic over `$ctor`: lifecycle (put/get/exists/delete), `get_stream` collect, list, copy/move within `file://`, `delete_by_prefix`, `create_bucket`/`bucket_exists` on `file://`, and a missing object giving `Error::ObjectStore(NotFound)`. Invoke `client_suite!(plain, ObjectStorageClient::new())`.
- [ ] **Step 2:** Run `cargo test --test functional`. Expected: compile failure on the `get_stream` item type (the suite uses `crate::Result`).
- [ ] **Step 3:** Implement the interfaces above. `Plain` reproduces today's bodies exactly:
  - `encode` → `Ok(data)`; `encode_stream` → returns its input
  - `decode` → `map_err(Error::from)`; `get_bytes` → `result.bytes().await?`
  - `describe` → today's HEAD body; `copy` → today's `copy`/`move_object` bodies, selected by `delete_source`
  
  Generic methods route through the codec: `put` → `encode` then `store.put`; `get` → `get_bytes`; `get_stream` → `decode`; `get_object_metadata` → `describe`; `copy`/`move_object` → `codec.copy`. `cli.rs` and `python.rs` adapt to the new stream item type.
- [ ] **Step 4:** Run `cargo test --all-features && uv run maturin develop && uv run pytest`. Expected: all pass, including the whole pre-existing suite unchanged.
- [ ] **Step 5:** Run the clippy and fmt gates from Global Constraints. Commit: `refactor!: generic Client<C: Codec> over a shared StoreRegistry`.

### Task 1b.1: `put_stream` (Rust)

**Files:**
- Create: `src/upload.rs`
- Modify: `src/client.rs`, `src/lib.rs`
- Test: unit tests inside `src/upload.rs`, plus `tests/functional/suite.rs`

**Interfaces:**
- Produces:
  - `pub(crate) struct UploadOptions { pub part_size: usize /* 8 MiB */, pub max_concurrency: usize /* 4 */ }` (`Default`)
  - `pub(crate) async fn upload_stream(backend: &dyn Backend, path: &ObjectPath, data: ByteStream, opts: &UploadOptions) -> Result<()>`
  - `Backend` gains `fn supports_multipart(&self) -> bool { true }`, overridden to `false` in `HttpBackend`.
  - `impl<C: Codec> Client<C> { pub async fn put_stream<S>(&self, url: &str, data: S) -> Result<()> where S: Stream<Item = Result<Bytes>> + Send + 'static }`, which calls `codec.encode_stream` with an empty `CallerContext`.
  - `pub(crate) async fn put_stream_inner(&self, url: &str, data: ByteStream, ctx: CallerContext) -> Result<()>`: the shared body; `put_stream` calls it with an empty context, and milestone 4's `put_stream_with_context` with the caller's.
- Algorithm:
  1. Accumulate input into a `BytesMut` until it reaches `part_size` or the input ends.
  2. If the input ends first, make one `store.put`.
  3. If the backend lacks multipart, keep accumulating to the end, then make one `put`.
  4. Otherwise `put_multipart`, wrap it in `AbortGuard` and `WriteMultipart::new_with_chunk_size(upload, part_size)`, and before each `write` call `wait_for_capacity(max_concurrency)`.
  5. On a clean end of input: `finish()` then disarm the guard. On any `Err` from the input or the store: `abort()`, then return the error.
- `AbortGuard` holds `Option<WriteMultipart>`. If it is dropped while still armed, it calls `tokio::runtime::Handle::try_current()` and spawns `writer.abort()`, ignoring the result.

- [ ] **Step 1:** Write the failing unit tests in `src/upload.rs`. They use `object_store::memory::InMemory`, plus a test `FaultStore` wrapper that counts `put`, `put_multipart` and `abort` calls and can fail a chosen part:
  - `small_input_single_put`: 3 chunks totalling 1 KiB → 1 `put`, 0 multipart.
  - `empty_input_creates_empty_object`
  - `large_input_multipart_roundtrip`: 20 MiB in random chunk sizes → bytes equal.
  - `input_error_aborts_and_leaves_no_object`: an error after 10 MiB → `abort` count 1 and `head` gives `NotFound`.
  - `dropped_future_aborts`: drop the future mid-upload, yield to the runtime, then `abort` count is 1.
  - `in_flight_parts_bounded`: `FaultStore` records peak concurrent part uploads ≤ 4.
  - `no_multipart_backend_buffers_then_puts`
- [ ] **Step 2:** Run `cargo test --lib upload`. Expected: FAIL (module missing).
- [ ] **Step 3:** Implement the algorithm above. `upload_stream` takes `&dyn Backend`, so tests need a test-only `Backend` impl wrapping an arbitrary store. Add `#[cfg(test)] pub(crate) struct TestBackend` in `src/backend/mod.rs`.
- [ ] **Step 4:** Add `put_stream` round-trip cases (empty, 1 byte, 20 MiB) to `client_suite!` against `file://`, plus `put_stream_error_leaves_no_file_and_no_staging_file`. Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat: put_stream with single-put, bounded multipart and abort-on-drop`.

### Task 1b.2: Python `put_object_stream`

**Files:**
- Restructure: `src/python.rs` → `src/python/mod.rs` (module init + error mapping) and `src/python/stream.rs` (`ByteStream`, `py_async_iter_to_stream`); the client class stays in `src/python/client.rs`
- Modify: `python/object_storage_client/__init__.pyi`
- Test: `tests/python/test_plain_client.py`

**Interfaces:**
- Produces: `pub(crate) fn py_async_iter_to_stream(iterable: &Bound<'_, PyAny>) -> PyResult<ByteStream>`.
  - It calls `__aiter__()` once.
  - The returned stream is built with `futures::stream::unfold`. On each poll, under `Python::attach`, it calls `__anext__()` and converts the awaitable with `pyo3_async_runtimes::tokio::into_future`. Then, outside the GIL, it awaits.
  - `StopAsyncIteration` → end of stream. Items accepted: `bytes`, `bytearray`, `memoryview`; anything else → `Error::Generic("put_object_stream: expected bytes-like chunk, got <type>")`. Any other exception → `Error::Generic(<repr>)`.
  - The stream ends after the first error.
  - The `Error::Generic` variant name may be refined to `Error::InputStream(String)` if clearer; the string must not include chunk contents.
- Produces: Python `async def put_object_stream(self, url: str, chunks: AsyncIterable[bytes]) -> None` on `ObjectStorageClient`.

- [ ] **Step 1:** Write the failing tests:
  - `test_put_object_stream_roundtrip`: an async generator of 300 random chunks (0–200 KiB, about 30 MiB in total) round-trips.
  - `test_put_object_stream_empty`
  - `test_put_object_stream_pipe_from_get_stream`: `await c.put_object_stream(dst, await c.get_object_stream(src))` gives equal bytes.
  - `test_put_object_stream_iterator_raises_no_object`: the generator raises `ValueError` after 3 chunks → the call raises, and `object_exists(dst)` is `False`.
  - `test_put_object_stream_wrong_chunk_type`: yields `"str"` → raises `TypeError`, no object (Review Focus 4). Map the input-type error to `PyTypeError` in the error mapper.
  - `test_put_object_stream_no_prefetch`: the generator records how many items were pulled before each yield; assert at most 1 is outstanding.
  - `test_cancel_put_object_stream_leaves_no_object`: cancel the task mid-upload, sleep 0.2 s, then the object is absent.
- [ ] **Step 2:** Run `uv run maturin develop && uv run pytest -k put_object_stream`. Expected: FAIL (`AttributeError`).
- [ ] **Step 3:** Implement the adapter and the method. Add the stub `async def put_object_stream(self, url: str, chunks: AsyncIterable[bytes]) -> None: ...`.
- [ ] **Step 4:** Run `cargo test --all-features && uv run pytest`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(python): put_object_stream with back-pressure and abort on error`.

---

## Milestone 2 — crypto core (PR 3)

Normative byte layouts come from the ESDK specification (§6 references). The values below are what this plan relies on. Task 2.4's oracle vectors are the arbiter: if a value here disagrees with the vectors, **the vectors win**, and you fix the spec and plan text in the same commit.

- **Header v2:**
  - Field order: `version u8 = 0x02 | suite u16 = 0x0478 | message_id [32] | aad_len u16 | context bytes | edk_count u16 | edks | content_type u8 = 0x02 | frame_length u32 | suite_data [32]`, then the header auth tag `[16]`. All integers are big-endian.
  - Context bytes: `count u16`, then per pair sorted by key bytes `klen u16 | key | vlen u16 | value`.
  - Each EDK: `id_len u16 | provider_id | info_len u16 | provider_info | ct_len u16 | ciphertext`.
- **Key derivation:**
  - `PRK = HKDF-Extract(SHA-512, salt = message_id, ikm = DEK)`
  - derived key = `HKDF-Expand(PRK, info = 0x0478_be || "DERIVEKEY", 32)`
  - commit key = `HKDF-Expand(PRK, info = "COMMITKEY", 32)`, stored as `suite_data` and compared in constant time (`ring::constant_time` is deprecated; use `ring::hmac::verify`-style comparison or a manual fold over XOR).
- **Header auth:** AES-256-GCM under the derived key, IV = 12 zero bytes, empty plaintext, AAD = header bytes from `version` through `suite_data` (plus any serialized required-context bytes, which are empty here).
- **Frames:**
  - Sequence numbers start at 1. IV = 8 zero bytes ‖ `seq u32`, and the reader checks it.
  - AAD = `message_id ‖ "AWSKMSEncryptionClient Frame"` (or `"AWSKMSEncryptionClient Final Frame"` for the final frame) `‖ seq u32 ‖ content_len u64`.
- **Raw AES EDK:**
  - `provider_info = key_name ‖ tag_len_bits u32 = 128 ‖ iv_len u32 = 12 ‖ iv[12]`
  - `ciphertext = enc(DEK)[32] ‖ tag[16]`
  - wrap AAD = context bytes (same serialization as above)

Verified against the ESDK Python 4.x oracle vectors (`tests/vectors/esdk`, Task 2.0): every value above matches, including key derivation, commitment, header tag, frame IV/AAD and the Raw AES wrap (the wrapping key is used directly as the AES key; there is no KDF on the KEK). Two details the list above leaves implicit:
- An empty context serializes to **zero bytes** (`aad_len = 0`, no `count u16`), and the wrap AAD is then empty too. Our writers always set `osc:fmt`, so this only matters for foreign messages.
- For plaintext that is an exact multiple of F (and > 0), the ESDK writer emits all full regular frames plus an empty final frame (`content_len = 0`). Plaintext of 0 bytes is a single empty final frame with `seq = 1`.

### Task 2.1: Context + header codec with limits

**Files:**
- Create: `src/encryption/{mod.rs,context.rs,header.rs}`, `src/hex.rs`
- Modify: `Cargo.toml` (`[features] default = ["encryption"]; encryption = ["dep:zeroize"]`, `zeroize = { version = "1", optional = true }`, dev `proptest = "1"`), `pyproject.toml` (`features = ["python", "encryption"]`), `src/lib.rs`, `src/sign.rs` (use the shared `hex::encode`)

**Interfaces:**
- Produces:
  - `pub struct EncryptionContext(BTreeMap<String, String>)` with `fn new_reserved(object_key: Option<&str>) -> Self` (sets `osc:fmt`, plus `osc:key` when given), `fn with_caller(self, caller: &CallerContext) -> Result<Self, FormatError>` (rejects reserved prefixes) and `fn serialize(&self) -> Vec<u8>`.
  - `pub struct EncryptedDataKey { pub provider_id: String, pub provider_info: Vec<u8>, pub ciphertext: Vec<u8> }` (placed here; `keys` re-exports it).
  - `pub struct Header { pub message_id: [u8; 32], pub context: EncryptionContext, pub edks: Vec<EncryptedDataKey>, pub frame_length: u32, pub commitment: [u8; 32], pub auth_tag: [u8; 16] }`
  - `pub struct DecodeLimits { pub max_edks: u16, pub min_frame_length: u32, pub max_frame_length: u32, pub max_header_len: usize }` (`Default` per Global Constraints).
  - `pub enum HeaderParse { NeedMore, Done { header: Header, header_len: usize, authenticated_bytes: Range<usize> } }`
  - `pub struct HeaderCodec;` with `fn parse(buf: &[u8], limits: &DecodeLimits) -> Result<HeaderParse, FormatError>` and `fn serialize_unauthenticated(h: &Header) -> Vec<u8>`.
  - `pub enum FormatError` (`thiserror`). Variants: `NotOsc` (first byte not `0x01`/`0x02`; `parse` returns `NeedMore` for 0-2 bytes, so the caller maps a too-short object at EOF to `NotEncrypted` later), `UnsupportedVersion`, `UnsupportedSuite(u16)`, `NotFramed`, `TooManyEdks(u16)`, `FrameLengthOutOfRange(u32)`, `HeaderTooLong`, `MissingFormatMarker`, `UnknownFormat(String)`, `ReservedContextKey(String)`, `Truncated`, `Malformed(&'static str)`.
- Parse rejects each limit **before allocating** for the field it guards. It checks `max_header_len` against the bytes consumed so far at each length-prefixed field.

- [ ] **Step 1:** Write the failing unit tests:
  - context serialization byte-exact for `{"b":"2","a":"1"}` → `00 02 00 01 61 00 01 31 00 01 62 00 01 32`
  - header serialize→parse round-trip
  - `NeedMore` for every strict prefix of a valid header
  - one test per `FormatError` variant, each built by mutating a valid header
  - `edk_count = 0xFFFF` rejected before allocation: the test feeds only 64 bytes, so it must return `TooManyEdks`, not `NeedMore`
  - caller key `"osc:x"` and `"aws-crypto-public-key"` rejected
- [ ] **Step 2:** Run `cargo test --lib encryption::`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --lib encryption::` and `cargo build --no-default-features`. Expected: PASS, and the build works with the feature off.
- [ ] **Step 5:** Commit: `feat(encryption): ESDK v2 header and context codec with decode limits`.

### Task 2.2: Suite: key derivation, commitment, header auth

**Files:** Create `src/encryption/suite.rs`.

**Interfaces:**
- Produces:
  - `pub(crate) struct MessageKeys { derived: ring::aead::LessSafeKey, commitment: [u8; 32] }`
  - `pub(crate) fn derive(dek: &DataKey, message_id: &[u8; 32]) -> MessageKeys`
  - `pub(crate) fn header_tag(keys: &MessageKeys, header_bytes: &[u8]) -> [u8; 16]`
  - `pub(crate) fn verify_header(keys: &MessageKeys, header_bytes: &[u8], commitment: &[u8; 32], tag: &[u8; 16]) -> Result<(), DecryptError>`, which checks the commitment first, in constant time.
  - `pub(crate) fn fresh_message_material(rng: &SystemRandom) -> (DataKey, [u8; 32])`
  - `pub struct DataKey(zeroize::Zeroizing<[u8; 32]>)`: no `Clone`/`Debug`/`Serialize`; `pub(crate) fn as_bytes(&self) -> &[u8; 32]`; lives in `keys/mod.rs` (create the module stub now).
  - `pub enum DecryptError` (`thiserror`): `CommitmentMismatch`, `HeaderAuth`, `FrameAuth { seq: u32 }`, `BadSequence { expected: u32, got: u32 }`, `BadIv { seq: u32 }`, `FinalFrameTooLong`, `MissingFinalFrame`, `TrailingBytes`, `BindingMismatch { expected: String, found: String }`, `Format(#[from] FormatError)`.

- [ ] **Step 1:** Write the failing tests:
  - `derive` is deterministic for a fixed DEK and message ID
  - a different message ID gives a different derived key and commitment
  - `verify_header` fails with `CommitmentMismatch` on a 1-bit flip in the commitment, and with `HeaderAuth` on a flip in the header bytes
  - `no_repeat_in_1e6`: a `HashSet` of 10^6 DEKs and message IDs from `fresh_message_material` has no duplicates (§10.6)
- [ ] **Step 2:** Run `cargo test --lib suite`. Expected: FAIL.
- [ ] **Step 3:** Implement with `ring::hkdf::{Salt, HKDF_SHA512}` and `ring::aead::{AES_256_GCM, LessSafeKey, Nonce::assume_unique_for_key}`.
- [ ] **Step 4:** Run `cargo test --lib suite`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): 0x0478 key derivation, commitment and header auth`.

### Task 2.3: `Encryptor`, `Decryptor`, `FrameLayout`

**Files:** Create `src/encryption/frame.rs`.

**Interfaces:**
- Produces:
  - `pub struct Encryptor` (not `Clone`):
    - `pub(crate) fn new(dek: &DataKey, message_id: [u8; 32], context: EncryptionContext, edks: Vec<EncryptedDataKey>, frame_length: u32) -> (Bytes, Self)`, which returns the full authenticated header
    - `pub fn update(&mut self, data: &[u8], out: &mut BytesMut)`, which emits each full regular frame eagerly
    - `pub fn finish(self, out: &mut BytesMut)`, which emits the final frame (possibly with zero length)
  - `pub struct Decryptor` (state machine `Header → Frames → Done`):
    - `pub fn new(limits: DecodeLimits, expected_key: Option<String>) -> Self`
    - `pub fn feed(&mut self, data: Bytes) -> Result<Progress, DecryptError>`
    - `pub fn header(&self) -> Option<&Header>`
    - `pub(crate) fn set_data_key(&mut self, dek: &DataKey) -> Result<(), DecryptError>`: verifies the commitment and header tag; on failure the decryptor keeps waiting for a key (try the next EDK). `unwrap_with` (Task 3.1) calls it.
    - `pub async fn unwrap_with(&mut self, keys: &dyn KeyProvider) -> Result<(), DecryptError>`. Once the header is parsed, and before any frame, `feed` returns `Progress::NeedKey`. The caller then calls `unwrap_with`, which reads `header()`, and feeds again. This keeps `feed` synchronous and I/O-free.
    - `pub fn finish(self) -> Result<(), DecryptError>`
  - `pub enum Progress { NeedMore, NeedKey, Plaintext(Vec<Bytes>) }`. Each element is one verified frame.
  - `pub struct FrameLayout { pub header_len: u64, pub frame_length: u32 }` with `fn plaintext_size(&self, stored_size: u64) -> Option<u64>` (the §6 formula; returns `None` when `r > F` or `B < 40`).
- `osc:key` is checked against `expected_key` right after parsing (`BindingMismatch`). A missing or unknown `osc:fmt` gives `Format`.
- **As built (after review hardening; supersedes the lines above where they differ):**
  - `Encryptor::new` returns `Result<(Bytes, Self), FormatError>` and validates the frame length (4096 to 4 MiB), EDK count and header length against `DecodeLimits::default()`.
  - `set_data_key` and `unwrap_with` return `Result<Progress, DecryptError>`, carrying the plaintext of frames already buffered. Callers must not drop it (`#[must_use]`).
  - The decryptor **fails closed**. Any error, including a wrong key or a transient provider error, moves it to `Failed`, and every later call errors. `unwrap_with` passes all EDKs to the provider in one call. A caller retries by restarting the read, never by trying another key on the same decryptor.
  - A non-empty `feed` while a key is awaited returns `KeyRequired`. Buffering is bounded by `max_header_len` plus one chunk, then one frame.
  - `finish` in the `Header` state returns `Format(NotOsc)` if fewer than 3 bytes were ever fed, and otherwise `Format(Truncated)`.

- [ ] **Step 1:** Write the failing tests:
  - round-trip for plaintext sizes 0, 1, F−1, F, F+1, 3F, 3F+7 with F = 4096, fed one byte at a time and in one piece
  - `FrameLayout::plaintext_size` equals the true size for each of these
  - a final frame of length F (written by a hand-built encoder variant) decrypts (§6 "both forms")
  - `finish` without a final frame → `MissingFinalFrame`
  - bytes after the final frame → `TrailingBytes`
  - two frames swapped → `BadSequence`
  - wrong `expected_key` → `BindingMismatch`
- [ ] **Step 2:** Run `cargo test --lib frame`. Expected: FAIL.
- [ ] **Step 3:** Implement. The decryptor buffers at most one frame plus 40 bytes and never yields an unverified byte.
- [ ] **Step 4:** Run `cargo test --lib frame`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): framed Encryptor/Decryptor and FrameLayout`.

> **Task 2.0 (done first, commit `test(encryption): ESDK oracle and reference vectors`):** Task 2.4 Step 1 (oracle `scripts/esdk_oracle.py` with `encrypt|decrypt|inspect|vectors`, and the vectors with `manifest.json` carrying the DEK per vector) was moved ahead of Tasks 2.1-2.3 so they test against real ESDK bytes.

### Task 2.4: ESDK oracle vectors, tamper suite, proptest, fuzz

**Files:**
- Create: `scripts/esdk_oracle.py`, `tests/vectors/esdk/*.bin` + `manifest.json`, `tests/functional/encryption_vectors.rs`, `fuzz/Cargo.toml`, `fuzz/fuzz_targets/{header_parse,decryptor_feed}.rs`
- Modify: `pyproject.toml` (`[dependency-groups] oracle = ["aws-encryption-sdk>=4"]`)

**Interfaces:**
- `scripts/esdk_oracle.py encrypt --key-hex H --key-name N --namespace osc-file --frame-length F --context k=v… --in P --out C` and `… decrypt …`.
  - Python ESDK settings: `EncryptionSDKClient(commitment_policy=REQUIRE_ENCRYPT_REQUIRE_DECRYPT)`, `algorithm=Algorithm.AES_256_GCM_HKDF_SHA512_COMMIT_KEY`, and a raw master key from `aws_encryption_sdk.key_providers.raw` with `WrappingAlgorithm.AES_256_GCM_IV12_TAG16_NO_PADDING`.
  - Vectors are generated once and committed. `manifest.json` lists the plaintext SHA-256, the key hex (test key only), and the context.
- Rust-side decrypt of the vectors needs Raw AES unwrap, which Task 3.1 builds. Until then the vectors carried a test-only `dek_hex` in the manifest; Task 3.1 removed that field (manifest and oracle) and the vector tests unwrap the EDK with the test KEK `kek-test-0001` (namespace `osc-file`) instead.

- [ ] **Step 1:** Generate the vectors: sizes 0, 1, 65535, 65536, 65537, 200000; frame lengths 4096 and 65536; context `{"osc:fmt":"1","osc:key":"dir/file.bin","tenant":"acme"}`. Run `uv run --group oracle python scripts/esdk_oracle.py …`.
- [ ] **Step 2:** Write the failing tests `esdk_vectors_decrypt` (all vectors decrypt to the manifest hash) and `osc_output_decrypts_with_esdk`. The latter is `#[ignore]` and run in CI's Python job: Rust writes ciphertexts to a temp dir, then the oracle's `decrypt` verifies them.
- [ ] **Step 3:** Fix the codec until the vectors pass. Record any spec correction in the spec.
- [ ] **Step 4:** Add the tamper tests (§10.2):
  - flip every bit of every byte of a 3-frame message → no `Ok` plaintext from `get`-style collection
  - truncate at every offset for a 2 KiB message, and at every frame boundary for a 1 MiB message
  - append a byte; duplicate, drop or reorder frames; splice a frame from another message; swap headers between two messages
  - suite downgrade to `0x0178`; content type `0x01`; `frame_length = 3` and `2^31`; context of 70 KiB (the header's context length is a `u16`, so a 70 KiB context is tested on the writer side via `with_caller`, and a declared 0xFFFF-byte context on the reader side)
  
  Add `proptest` round-trips with random plaintext (0–300 KiB), random input chunking and random output chunking, including exact multiples of F.
- [ ] **Step 5:** Create the `fuzz/` crate (`cargo fuzz init` layout, not a workspace member) with a `fuzzing` cargo feature that exposes `encryption::fuzzing::{header_parse,decryptor_feed}` harnesses (`set_data_key` is crate-private, so the target cannot call it directly) with `HeaderCodec::parse` and `Decryptor::feed` targets. Run `cargo +nightly fuzz run header_parse -- -max_total_time=60`. Expected: no crashes.
- [ ] **Step 6:** Run `cargo test --all-features`. Expected: PASS. Commit: `test(encryption): ESDK oracle vectors, tamper suite, proptest and fuzz targets`.

---

## Milestone 3 — keys (PR 3, continued)

### Task 3.1: `KeyProvider` + Raw AES wrap/unwrap

**Files:** Create `src/encryption/keys/{mod.rs,raw_aes.rs}`.

**Interfaces:**
- Produces:
  - `#[async_trait] pub trait KeyProvider: Send + Sync + std::fmt::Debug` with:
    - `fn namespace(&self) -> &str`
    - `fn active_key_name(&self) -> &str`
    - `async fn wrap(&self, dek: &DataKey, ctx: &EncryptionContext) -> Result<Vec<EncryptedDataKey>, KeyError>`
    - `async fn unwrap(&self, edks: &[EncryptedDataKey], ctx: &EncryptionContext) -> Result<DataKey, KeyError>`
  - `pub(crate) struct RawAesKey { name: String, key: LessSafeKey }`, whose `Debug` shows the name only. Methods: `fn wrap(&self, namespace: &str, dek: &DataKey, aad: &[u8], rng: &SystemRandom) -> EncryptedDataKey` and `fn unwrap(&self, edk: &EncryptedDataKey, aad: &[u8]) -> Result<DataKey, KeyError>`. Unwrap validates `provider_info` (exact name match, tag bits 128, IV length 12, total length) before decrypting.
  - `pub enum KeyError`:
    - `UnknownKey { key_ids: Vec<String> }` (the names in the EDKs; no material)
    - `UnwrapFailed { key_id: String }`
    - `Permissions { path: PathBuf, mode: u32 }`
    - `NotOwner { path: PathBuf }`
    - `BadCheck { path: PathBuf }`
    - `IdMismatch { path: PathBuf }`
    - `DuplicateId { id: String }`
    - `ActiveKeyMissing { id: String }`
    - `Malformed { path: PathBuf, reason: &'static str }`
    - `Io { path: PathBuf, source: std::io::Error }`
- A wrap counter: `AtomicU64` per key, logged nowhere yet. Exposed as `fn wraps(&self) -> u64` for the §6 bound.

- [ ] **Step 1:** Write the failing tests:
  - wrap→unwrap round-trip
  - unwrap with a different context → `UnwrapFailed`
  - EDK naming an unknown key → `UnknownKey`
  - flipped IV length in `provider_info` → `UnwrapFailed`
  - `format!("{:?}", key)` does not contain the key hex (§10.10)
  - switch the vector tests (`esdk_vectors_decrypt…`, `esdk_vectors_derive_and_verify`) from the manifest `dek_hex` to real unwrap via `Decryptor::unwrap_with` / `KeyProvider::unwrap` with the test KEK `kek-test-0001` in namespace `osc-file`, and delete `dek_hex` from the manifest and the oracle
- [ ] **Step 2:** Run `cargo test --lib keys`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS, including the vectors via unwrap.
- [ ] **Step 5:** Commit: `feat(encryption): KeyProvider and Raw-AES-compatible wrap/unwrap`.

### Task 3.2: `FileKeyProvider` + key files

**Files:**
- Create: `src/encryption/keys/file.rs`
- Modify: `Cargo.toml` (`[target.'cfg(unix)'.dependencies] rustix = { version = "1", features = ["process"] }`; this is safe `geteuid` without `unsafe` or `libc`)

**Interfaces:**
- Produces:
  - `pub struct FileKeyProvider` with:
    - `pub fn open(dir: impl AsRef<Path>, active: &str) -> Result<Self, KeyError>`, which loads every `*.key` file in the directory
    - `pub fn generate_key_file(dir: impl AsRef<Path>, prefix: &str, today: chrono::NaiveDate) -> Result<String /* id */, KeyError>`
    - `pub fn check_file(path: impl AsRef<Path>) -> Result<String /* id */, KeyError>`
  - The namespace is the constant `"osc-file"`.
- Key file format (§7): first line `osc-kek-v1`, then `id=`, `created=`, `material=` (64 hex chars) and `check=` (16 hex chars = the first 8 bytes of `HMAC-SHA256(material, "osc-key-check-v1")`). Lines may end in `\r\n`, and surrounding whitespace is trimmed. Unknown fields → `Malformed`. The `id` must end with `check[0..8]`.
- Generated ID: `{prefix}-{YYYY}-{MM}-{check[0..8]}`. The file is `{id}.key`, created with `create_new` and mode `0o600`.
- Unix: reject a file if `mode & 0o077 != 0` (`Permissions`) or its owner is not `rustix::process::geteuid()` (`NotOwner`). On non-unix, skip both checks and say so in the doc comment.

- [ ] **Step 1:** Write the failing tests:
  - generate → open → the active key wraps and unwraps
  - a second key loaded is decrypt-only (an object written with the old active key unwraps after switching active)
  - mode `0o640` → `Permissions`
  - edited `check` → `BadCheck`
  - same `id` with different material in two files → `DuplicateId` (Review Focus 5)
  - CRLF file loads (Review Focus 5)
  - `active` absent → `ActiveKeyMissing` at `open`
  - an `id` not ending in `check[0..8]` → `IdMismatch`
  - wrong key directory → `UnknownKey` names the EDK key IDs only
- [ ] **Step 2:** Run `cargo test --lib keys::file`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): FileKeyProvider with permission and check-value validation`.

---

## Milestone 4 — encrypted client (PR 4)

### Task 4.1: `Envelope` codec, `EncryptionConfig`, views, errors

**Files:**
- Create: `src/encryption/envelope.rs`, `tests/functional/encryption.rs`
- Modify: `src/encryption/mod.rs`, `src/client.rs`, `tests/functional.rs`, `tests/functional/suite.rs`

**Interfaces:**
- Produces:
  - `pub struct EncryptionConfig { pub key_provider: Arc<dyn KeyProvider>, pub bind_object_key: bool, pub frame_length: u32, pub limits: DecodeLimits }`
    - `EncryptionConfig::new(keys) -> Self` gives the defaults true / 65536 / default limits.
    - Builder methods: `bind_object_key(bool)`, `frame_length(u32) -> Result<Self>` (validated against the limits), `limits(DecodeLimits)`.
  - `#[derive(Clone)] pub struct Envelope { cfg: Arc<EncryptionConfig>, rng: SystemRandom }`
  - `pub type EncryptedObjectStorageClient = Client<Envelope>;`
  - `impl Client<Plain> { pub fn with_encryption(&self, cfg: EncryptionConfig) -> Client<Envelope> }`
  - `impl Client<Envelope> { pub fn new(cfg: EncryptionConfig) -> Self; pub fn without_encryption(&self) -> Client<Plain>; pub async fn put_with_context(&self, url: &str, data: impl Into<Bytes>, ctx: &CallerContext) -> Result<()>; pub async fn put_stream_with_context<S>(&self, url: &str, data: S, ctx: CallerContext) -> Result<()> }`
  - `Error` gains `#[cfg(feature = "encryption")]` variants: `NotEncrypted { url: String }`, `Decryption(#[from] DecryptError)`, `Key(#[from] KeyError)`. `FormatError::NotOsc` maps to `NotEncrypted`; any other `FormatError` maps to `Decryption(Format(..))`; `ReservedContextKey` maps to `Error::Generic` before any I/O.
- `Envelope` hooks:
  - `encode`: wrap, then `Encryptor`. If the payload is over 1 MiB, run it in `spawn_blocking`.
  - `encode_stream`: header first, then frames. Split each input chunk into ≤ 1 MiB slices so no single poll does more than 1 MiB of CPU work (§9).
  - `decode`: a `Decryptor` driven by the stored stream. It yields each `Progress::Plaintext` frame, including the `Progress` returned by `unwrap_with` on `NeedKey`. It calls `finish()` when the stored stream ends and ends the stream after the first error (the decryptor fails closed).
  - `get_bytes`: collect `decode`.
  - `expected_key` = `loc.path.as_ref()` when `bind_object_key` is set.

- [ ] **Step 1:** Write the failing tests in `tests/functional/encryption.rs`, plus `client_suite!(envelope, <secure client over a temp key dir>)`:
  - round-trip sizes 0, 1, 65536, 3×65536+5 via `put`/`get` and via `put_stream`/`get_stream`
  - the stored bytes start with `02 04 78`
  - plain view reading an encrypted object returns the stored bytes unchanged
  - encrypted view reading a plain object → `Error::NotEncrypted`
  - flip one stored byte → `Error::Decryption`
  - rename the stored file on disk (binding) → `Decryption(BindingMismatch)`
  - two key directories cannot read each other's objects → `Error::Key(UnknownKey)`
  - caller context `{"osc:key":"x"}` → error, and no file is created (Review Focus 3)
  - `with_encryption` and `without_encryption` share the registry: `Arc::ptr_eq` on `get_backend`
  - a truncated encrypted source piped into `plain.put_stream` leaves no destination file
  - a key with spaces, `%` and non-ASCII round-trips (Review Focus 2)
- [ ] **Step 2:** Run `cargo test --test functional encryption`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): Envelope codec and EncryptedObjectStorageClient`.

### Task 4.2: Encrypted metadata + `inspect_encryption`

**Files:** Modify `src/encryption/envelope.rs` and `src/client.rs`; test in `tests/functional/encryption.rs`.

**Interfaces:**
- Produces: `impl Client<Envelope> { pub async fn inspect_encryption(&self, url: &str) -> Result<EncryptionInfo> }`. It needs no keys and does not verify the header tag; the doc comment says the result is unauthenticated.
- `Envelope::describe`:
  1. One `get_opts` with `range: 0..4096`. If the header parse returns `NeedMore`, issue one more `get_opts` for `0..limits.max_header_len`.
  2. `size_bytes` = `FrameLayout::plaintext_size(meta.size)`; if that is `None`, return `Decryption(Format(Malformed("stored length")))`.
  3. `stored_size_bytes = meta.size`. Fill `encryption: Some(EncryptionInfo { format: "osc-esdk2-0478-v1", .. })`. `content_type`, `e_tag`, `version` and `last_modified` come from the same `GetResult`.

- [ ] **Step 1:** Write the failing tests:
  - metadata for sizes 0, 1, 65536 and 200 000 reports the plaintext `size_bytes` and the stored `stored_size_bytes`
  - a header longer than 4 KiB (caller context about 6 KiB) still describes, with exactly 2 range requests (use a `FaultStore`-style counting backend)
  - plain object → `NotEncrypted`
  - `inspect_encryption` returns the key name, frame length, the context including `osc:key`, and a 64-hex-char message ID
- [ ] **Step 2:** Run `cargo test --test functional metadata`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): ranged-read metadata and inspect_encryption`.

### Task 4.3: Encrypted copy/move

**Files:** Modify `src/encryption/envelope.rs`; test in `tests/functional/encryption.rs`.

**Interfaces:**
- **No header re-wrap (spec §6):** a second header tag under the same `(DEK, message_id)` is GCM nonce reuse. Every copy that needs a different key or context is a full decrypt + re-encrypt with a fresh DEK and message ID.
- `Envelope::copy` decision, per §8:
  - **Source key == destination key** (same `Path` string):
    - Read the header (ranged).
    - If the EDK names the active key: byte copy, server-side when the (scheme, host) pair matches (today's path, with the EXDEV fallback now streaming), otherwise `get_stream` → `upload_stream`.
    - If it names an older key: decrypt the source and `encode_stream` it under the active key with the same caller pairs and `osc:key` (fresh DEK and message ID), then `upload_stream`.
  - **Keys differ:** decode the source, then `encode_stream` with the source's caller pairs (the context minus the `osc:*` keys) under the destination `osc:key`, then `upload_stream`.
  - `move_object` deletes the source only after the destination commits.

- [ ] **Step 1:** Write the failing tests:
  - copy to a different key → the destination decrypts, has a new message ID, and keeps the caller pair `tenant=acme` (Review Focus 3)
  - same-key copy between two `file://` roots is not possible, because the keys differ (D9). Cover the same-key path with the `memory` `TestBackend` in a unit test instead: active-key source → destination bytes identical; old-key source → destination EDK names the active key, its message ID differs from the source's (fresh DEK, no header re-tag), and it decrypts to the same plaintext
  - move of a tampered source → error, source still present, no destination
  - copy to a key with non-ASCII characters (Review Focus 2)
- [ ] **Step 2:** Run `cargo test --all-features copy`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(encryption): copy/move with active-key byte copy or re-encrypt`.

### Task 4.4: Mixed use, memory bound, Garage

**Files:** Create `tests/integration/s3_encryption.rs`; modify `tests/integration.rs` and `tests/functional/encryption.rs`.

> The Garage, cross-provider and compose steps below moved to the later testcontainers follow-up phase; only the `file://` tests were done in this task.

- [ ] **Step 1:** Write the tests:
  - `mixed_views_concurrent_one_backend`: 32 tasks alternate plain and secure puts to one `file://` root, then `registry.stores.len() == 1`. Add a `#[doc(hidden)] pub fn backend_count(&self) -> usize` to make this checkable.
  - `pipe_multi_gb_bounded_memory`: `#[ignore]`, run in CI nightly or manually. Pipe a synthetic 4 GiB generator stream through `secure.put_stream` and back through `get_stream` into a hashing sink, and assert peak RSS (read `/proc/self/status` `VmHWM`) stays under 256 MiB.
  - In `s3_encryption.rs` (`#[ignore]`, Garage env as in `s3_storage_ops.rs`): round-trip for 0 B, 1 KiB and 20 MiB; an input error leaves no object and no pending multipart upload. Check the latter with a raw `ListMultipartUploads` signed request via `object_store`'s `AwsAuthorizer`, following the `backend/s3.rs` pattern. Also cover metadata, and copy secure→secure within the bucket.
- [ ] **Step 2:** Add `azurite` (`mcr.microsoft.com/azure-storage/azurite`) and `fake-gcs-server` (`fsouza/fake-gcs-server`) services to `compose.yml` (§10.8). Add `#[ignore]` tests `cross_provider_secure_copy_{s3_az,az_gs,gs_s3}` in `s3_encryption.rs` (rename the file to `cross_provider_encryption.rs` if it reads better). If `backend/azure.rs` or `backend/gcs.rs` cannot target an emulator from the environment, stop and report rather than changing backend config handling. Pointing them at an emulator is a separate decision.
- [ ] **Step 3:** Run `docker compose up -d && cargo test --all-features -- --ignored encryption`. Expected: PASS.
- [ ] **Step 4:** Commit: `test(encryption): mixed-view, bounded-memory, Garage and cross-provider tests`.

---

## Milestone 5 — Python bindings (PR 5)

### Task 5.1: Shared-methods macro, encrypted class, `FileKeyProvider`, exceptions, stubs

**Files:**
- Create: `src/python/encryption.rs`, `tests/python/test_encrypted_client.py`
- Modify: `src/python/{mod.rs,client.rs}`, `python/object_storage_client/__init__.py`, `__init__.pyi`

**Interfaces:**
- `macro_rules! client_pyclass { ($ty:ident, $inner:ty, { $($extra:tt)* }) => { #[pymethods] impl $ty { /* shared methods */ $($extra)* } } }`. It emits the **whole** `#[pymethods]` block.
  - Shared methods: `get_object`, `get_object_stream`, `delete_object`, `delete_objects_by_prefix`, `list_objects`, `object_exists`, `create_bucket`, `bucket_exists`, `copy_object`, `move_object`, `get_object_metadata`.
  - The metadata dict adds `stored_size_bytes`, `encrypted` and `encryption` only when `encryption` is `Some`.
- Plain extras: `__new__`, `put_object`, `put_object_stream`, `get_pre_signed_url(s)`, `with_encryption(key_provider: FileKeyProvider, bind_object_key: bool = True, frame_length: int = 65536)`.
- Encrypted extras: `__new__(key_provider, bind_object_key=True, frame_length=65536)`, `put_object(url, data, context: dict[str, str] | None = None)`, `put_object_stream(url, chunks, context=None)`, `inspect_encryption(url) -> dict`, `without_encryption() -> ObjectStorageClient`.
- `#[pyclass(frozen)] FileKeyProvider { inner: Arc<encryption::FileKeyProvider> }` with `__new__(directory: str, active: str)`. Its `__repr__` shows the directory and active ID only.
- Exceptions: `create_exception!(_object_storage_client, EncryptionError, PyRuntimeError)`, then `DecryptionError`, `UnknownKeyError` and `NotEncryptedError` as subclasses of `EncryptionError`. Mapping: `Error::Decryption` → `DecryptionError`; `Error::Key(UnknownKey | UnwrapFailed)` → `UnknownKeyError`; other `Error::Key` variants → `EncryptionError`; `Error::NotEncrypted` → `NotEncryptedError`.
- `__init__.py` re-exports `EncryptedObjectStorageClient`, `FileKeyProvider` and the four exceptions, and adds them to `__all__`. The stub adds `EncryptedObjectMetadata(ObjectMetadata)` with `stored_size_bytes: int`, `encrypted: bool` and `encryption: EncryptionInfo`.

- [ ] **Step 1:** Write the failing tests covering the §8 API:
  - both construction routes
  - `hasattr(secure, "get_pre_signed_url") is False`
  - encrypted round-trip for put, get and streams
  - pipes both ways (`plain.put_object_stream(dst, await secure.get_object_stream(src))` and the reverse)
  - `NotEncryptedError` and `DecryptionError` are subclasses of `RuntimeError`
  - the metadata dict keys
  - `inspect_encryption`
  - `context=` round-trip visible in `inspect_encryption`
  - a reserved context key raises
  - `repr(FileKeyProvider)` does not contain the key hex
- [ ] **Step 2:** Run `uv run maturin develop && uv run pytest tests/python/test_encrypted_client.py`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `uv run pytest`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(python): EncryptedObjectStorageClient, FileKeyProvider and exceptions`.

### Task 5.2: Cancellation, fork and ESDK round-trip from Python

**Files:** Create `tests/python/test_encryption_runtime.py`.

- [ ] **Step 1:** Write the tests:
  - `test_cancel_get_object_stream_midway`: cancel the task between chunks. A subsequent `get_object` on the same client still works, and nothing hangs.
  - `test_break_out_of_async_for`: break after the first chunk, then the client is still usable.
  - `test_fork_no_repeated_message_ids`: the parent and a forked child each write 1000 objects. Collect message IDs via `inspect_encryption` and assert there are no duplicates across both. Use `os.fork()` before any client use in the child, and a fresh event loop in the child.
  - `test_python_esdk_decrypts_osc` and `test_osc_decrypts_python_esdk`: use the `oracle` dependency group and skip if it is not installed. The test writes a key file with known material via `FileKeyProvider` generate.
- [ ] **Step 2:** Run `uv sync --group dev --group oracle && uv run maturin develop && uv run pytest tests/python -v`. Expected: PASS. Add `--group oracle` to the CI Python job.
- [ ] **Step 3:** Commit: `test(python): cancellation, fork safety and ESDK interop`.

---

## Milestone 6 — CLI (PR 6)

### Task 6.1: Global key flags, generic helpers, streaming `put`/`get`

**Files:** Modify `src/cli.rs`; test in `tests/functional/cli.rs` (new; drives `Cli::try_parse_from(..).run()`. `cli.rs` is binary-only, so first move `src/cli.rs` into the library as `pub mod cli` behind `#[doc(hidden)]` and have `main.rs` call `object_storage_client::cli::Cli`).

**Interfaces:**
- Global args: `#[arg(long, global = true, env = "OSC_KEYS_DIR")] keys: Option<PathBuf>` and `#[arg(long, global = true)] key_id: Option<String>`. `--keys` without `--key-id` → error `"--key-id is required with --keys"`.
- `Cli::run` builds `ObjectStorageClient::new()`, and when keys are given, `.with_encryption(EncryptionConfig::new(Arc::new(FileKeyProvider::open(dir, id)?)))`. It then dispatches to helpers generic over `&Client<C>` via a `macro`/generic `async fn dispatch<C: Codec>(client: &Client<C>, cmd) -> Result<(), CliError>`.
- `Sign` with keys → `CliError::SignWithKeys` (`"sign is not available with --keys"`).
- `put` (single file): `tokio::fs::File` → `ReaderStream` (from `tokio-util`, already transitive; add it as a direct dependency) → `put_stream`. Recursive upload uses the same per-file path. `get`: stream to `<dst>.osc-tmp-<random>` in the same directory, then `rename` on a clean end and remove the temp file on error.
- `CliError` gains `Key(#[from] KeyError)` and `SignWithKeys`.

- [ ] **Step 1:** Write the failing tests:
  - put/get round-trip with and without `--keys`, comparing file bytes
  - with keys, the stored object starts with `02 04 78`
  - `sign --keys …` errors
  - `get` of a tampered object leaves no `dst` and no temp file
  - `--keys` without `--key-id` errors
  - `OSC_KEYS_DIR` set but no `--key-id` → plain behaviour is **not** silently enabled; it errors
- [ ] **Step 2:** Run `cargo test --test functional cli`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`. Expected: PASS.
- [ ] **Step 5:** Commit: `feat(cli): --keys/--key-id with transparent encryption and streaming put/get`.

### Task 6.2: `osc keys generate|check`, `osc enc inspect`

**Files:** Modify `src/cli.rs`; test in `tests/functional/cli.rs`.

**Interfaces:**
- `osc keys generate --dir DIR [--prefix kek]` prints the new ID. `osc keys check FILE…` prints `ok <id>` per file and exits non-zero on the first failure.
- `osc enc inspect URL` prints the `EncryptionInfo` fields one per line (`format`, `message_id`, `key_names`, `frame_length`, `context.<k>=<v>`). It needs no keys: it builds `Client::<Envelope>` only when keys are given, and otherwise calls a `#[doc(hidden)] pub async fn inspect_encryption_raw(client: &ObjectStorageClient, url) -> Result<EncryptionInfo>` that shares the header-reading code from Task 4.2.

- [ ] **Step 1:** Write the failing tests: generate → check passes; a `0o644` file → check fails with a permissions message; `enc inspect` on an encrypted object prints `format: osc-esdk2-0478-v1` and the `osc:key` context line; `enc inspect` on a plain object errors with "not encrypted".
- [ ] **Step 2:** Run `cargo test --test functional cli`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Run `cargo test --all-features`, and run the Garage CLI test manually (`osc --keys … put/get` against `s3://`). Expected: PASS.
- [ ] **Step 5:** Commit: `feat(cli): keys generate/check and enc inspect`.

---

## Milestone 7 — docs and golden files (PR 7)

### Task 7.1: README + golden files

**Files:**
- Modify: `README.md` (fix MSRV to 1.99 and the dependency snippet to `0.2`; add an "Encryption" section)
- Create: `tests/vectors/golden/v1/*` (ciphertexts + `test.key`, mode set by the test at runtime) and `tests/functional/golden.rs`

- [ ] **Step 1:** Generate the golden files with the Phase 1 encryptor: sizes 0, 1, 65536 and 200 000; one with caller context; one written under an old key with a newer active key present. Write the test `golden_v1_decrypts`. The test copies `test.key` to a temp dir with `0o600`, because git does not preserve modes.
- [ ] **Step 2:** Run `cargo test --test functional golden`. Expected: PASS.
- [ ] **Step 3:** Write the README "Encryption" section:
  - setup (`osc keys generate`, `--keys/--key-id`, Python `with_encryption`)
  - key handling, backups and rotation (§7 policy steps 1–5)
  - the threat model (§1)
  - all of §13 verbatim in substance, including the `file://` binding and HTTP buffering caveats, and "streamed data is provisional"
- [ ] **Step 4:** Commit: `docs: encryption guide and golden ciphertexts`.

---

## Execution notes

- Order: Milestones 0 → 1 → 1b → 2 → 3 → 4 → 5 → 6 → 7. Tasks 2.x and 3.x depend only on milestone 1's module layout and can start once Task 1.1 lands. Within milestone 2, the tasks are sequential.
- Every task ends with `cargo test --all-features`, the clippy gate, the fmt gate, and (from milestone 0 on) `uv run maturin develop && uv run pytest`.
- New dependencies beyond the spec's list: `rustix` (unix, safe `geteuid`) and `tokio-util` (direct). Both are already in the lock graph transitively or are small; confirm at review.
