# Refactoring plan: smaller, more idiomatic Rust 1.99

Reviewed at commit 82dcdb3 (Rust 1.99.0, edition 2024). Line counts checked with `wc`. Feature claims in Phase 1–2 (`floor_char_boundary`, `ErrorKind::CrossesDevices`, `std::env::home_dir`, `last_key_value`/`is_some_and`, `LazyLock`, `#[expect]`) compile on rustc 1.99.0 with `-D warnings`.

## 1. Baseline and target

| Area | Files | LOC |
|---|---|---|
| Core | client.rs 756, codec.rs 507, registry.rs 222, upload.rs 668, backend/* 1440, sign.rs 290, hex.rs 58, lib.rs 19 | ~3960 |
| Encryption | src/encryption/** | 6909 |
| Surfaces | cli.rs 1031, main.rs 79, python/* 939 | ~2050 |
| Tests | tests/** (.rs and .py) | 6651 |
| **src total** | | **12918** |

Target, using the critics' corrected estimates with overlaps counted once (P6 is folded into CORE-3/CORE-4):

| Area | Estimated change |
|---|---|
| Core | −160 to −205 |
| Encryption | −140 to −175 (about −100 in production code, the rest in tests) |
| Surfaces and tests | −405 to −465 (CLI and Python bindings about −100 to −130; tests and pytest about −300 to −340) |
| **Total** | **about −700 to −850 lines, plus 3 fewer direct dependencies** (dashmap, homedir, futures-util) |

## 2. Invariants every step must preserve

- The decision to copy bytes is made only on the authenticated header (`osc:key` binding, active key). Unauthenticated data never drives a copy.
- Byte copies echo the verified stored bytes, so headers are never re-wrapped. A re-encrypting copy always gets a fresh DEK and message ID through `encode_stream`.
- An unverified server-side copy happens only on the same backend, checked with `Arc::ptr_eq`. Everything else goes through verified streaming.
- `same_object` and server-side-copy early returns happen only on the byte path. A move deletes the source only through `delete_unchanged_source`, and only when `!same_object`.
- The registry keeps exactly one cached `Arc` per (scheme, userinfo, host, port), and the cap of 256 holds.
- S3 `skip_signature` is checked before any credential or IMDS lookup, on every path: pre-sign, `create_bucket`, `bucket_exists`.
- Azure plain HTTP requires `AZURE_ALLOW_HTTP`. GCS sends the bearer token over https only. `S3_ALLOW_HTTP` keeps its exact `"true"` match.
- Content-Length binding is signed only by `sign.rs` SigV4. Other backends get `content_binding_unsupported`. The fixed-time SigV4 vectors stay byte-identical.
- URL secrets are redacted and terminal escapes sanitized in every error, status line and `caused by` line.
- The key loader keeps its directory fd, openat/linkat, O_NOFOLLOW, the dev/ino check, the ancestor check, ELOOP→Malformed, and the constant-time check-value compare.
- CLI file safety stays as is: `open_verified`, `download_modes`, the `TempFile` drop guard, the select!-driven signal cleanup and `drain_pending_aborts` (bounded at 10 s).
- Every Python entry point keeps going through `spawn` (the pid fork guard) and `client_to_py_err`.
- Encryption output stays byte-identical: golden tests and ESDK vectors.
- Parsers of untrusted input never gain a panic path (`unwrap_used` stays denied, and no `expect` on untrusted lengths).

## 3. Phased steps

**Gate sets**, referred to by letter below:
- **G-fast**:
  - `cargo fmt --check`
  - `cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::nursery`
  - `cargo clippy --all-targets --no-default-features -- -D warnings`
  - `cargo test --all-features`
- **G-doc**: `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features`
- **G-feat**:
  - `cargo check --no-default-features`
  - `cargo check --no-default-features --features python`
  - `cargo check --features fuzzing`
  - `cargo check --manifest-path fuzz/Cargo.toml`
- **G-int**: `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock TESTCONTAINERS_RYUK_DISABLED=true cargo test --all-features --test integration -- --ignored`
- **G-py**: `uv run --no-sync python -m pytest tests/python -q`, after rebuilding the extension outside the shared `.venv`.
- **G-crypto**:
  - `cargo test --lib encryption::` (baseline 128 pass, 1 ignored)
  - `tests/functional/golden.rs`
  - the ignored oracle test `rust_encrypted_messages_decrypt_with_the_python_oracle` (uv `--group oracle`)
  - a short fuzz run of `header_parse` and `decryptor_feed`

### Phase 0: CI first (prerequisite)

`.github/workflows/tests.yaml` runs on push to main and on pull requests: cargo test, the container integration tests, fmt + clippy + rustdoc, pytest on 3.13/3.14/3.15 and the ESDK oracle. `.github/workflows/ci.yaml` (build and publish) runs only on `release: published`. Missing from PR CI: the `--no-default-features` and `--features python` builds, `cargo check --manifest-path fuzz/Cargo.toml` (so `encryption::fuzzing` and the fuzz targets can rot unnoticed), and clippy with the nursery group.

- **Change:** add those as a G-feat job in `tests.yaml`.
- **Estimated change:** about +15 YAML. **Risk:** none.

Every later step is only as safe as these gates.

### Phase 1: Quick, low-risk idiom wins

| Step | What | Files | Idiom | Change | Risk / API | Gates |
|---|---|---|---|---|---|---|
| 1.1 | `name.truncate(name.floor_char_boundary(room))` | cli.rs:706-712 | `str::floor_char_boundary` (1.91) | −6 | low / none | G-fast |
| 1.2 | `for cause in iter::successors(err.source(), \|&e\| e.source())`. The `\|&e\|` pattern is required. | main.rs:72-76 | `iter::successors` | −2 | low / none | G-fast |
| 1.3 | One `fn read_err(path: &Path) -> impl FnOnce(io::Error) -> CliError`. There are 5 inline closures. open_verified (cli.rs:574) already has a local one; keep it or switch it to the helper. | cli.rs:488,515,925,933,942 | edition-2024 RPIT capture | −10 | low / none | G-fast |
| 1.4 | Turn `#[allow]` into `#[expect(.., reason)]`, only where clippy confirms the lint fires in each config. Keep `#[allow]` for `cfg(not(unix))` (keys/file.rs:549) and for nursery `redundant_pub_crate` sites that are unconfirmed. `scheme()` returns `&'static str`, and the stale "lifetime tie is unavoidable" comments go. | backend/s3.rs:203, local.rs:39, mod.rs:281; envelope.rs:1374,1379; keys/mod.rs:15,254; tamper_tests.rs:2; cli.rs:545; tests/*encryption.rs:44,92,117 | `#[expect]` (1.81), refined impl lifetime | −8 | low / none | G-fast, both feature sets |
| 1.5 | `BTreeMap::last_key_value().is_some_and(\|(p, _)\| *p >= key)` replaces `last: Option<String>`. That removes one String clone per context entry on the untrusted parse path. | encryption/context.rs:124-136 | `last_key_value`, `is_some_and` | −2 | low / none | G-crypto |
| 1.6 | Make `codec::redacted` `pub(crate)` and use it in `envelope::read_err`, so there is one URL-secret rule. | codec.rs:222, envelope.rs:141-153 | — | −6 | low, security-sensitive / none | G-fast; `not_encrypted_errors_carry_no_credentials`, `source_changed_names_the_object_without_credentials` |
| 1.7 | Make `suite::ct_eq` `pub(crate)` and use it in key-file `parse()`. Fuzz statics become `LazyLock`. | suite.rs:119, keys/file.rs:608-617, fuzzing.rs:20-24,46-50 | `LazyLock` (1.80) | −6 to −8 | low / none | G-crypto, G-feat |
| 1.8 | Detect cross-device renames by walking `Error::source` for `io::ErrorKind::CrossesDevices`, instead of `to_string().contains("os error 18")` (codec.rs:326). Add a unit test with `Generic { source: io::Error::from_raw_os_error(18) }`. | codec.rs:324-330 | `ErrorKind::CrossesDevices` (1.85) | +4 to +10 | low, security-sensitive / Windows behaviour change (it now gets the fallback) | G-fast; move tests |
| 1.9 | Fix the stale "(scheme, host)" docs and the false `registry::resolve` "does no I/O" comment (registry.rs:55). GCS reads its service-account file and the HTTP backend builds a rustls client. | backend/mod.rs:83, client.rs:158-159,717-718, registry.rs:52-56 | — | 0 | none | G-doc |

### Phase 2: Dependency removals and feature gating

| Step | What | Files | Change | Risk / API | Gates |
|---|---|---|---|---|---|
| 2.1 | Replace `futures_util::` with `futures::` and drop `futures-util`. Drop chrono's `serde` feature (object_store enables it). | cli.rs:16,958; Cargo.toml:22 | −2 | low / none | G-fast, G-feat |
| 2.2 | (CORE-4) Replace homedir with `std::env::home_dir()` plus a let-chain `strip_prefix('~')`. This also removes nix 0.30.1. Add a `~/x` test that runs in a subprocess or with an injected HOME. | registry.rs:129-153; Cargo.toml:27 | −5 | low / Windows-only behaviour change; on Unix it differs only for an empty HOME, where std is better | G-fast; `cargo tree -i nix@0.30.1` returns nothing |
| 2.3 | (CORE-2 + CORE-11, with a shared `fn origin(&Url) -> &str` = `&url[..Position::AfterPort]`) Registry becomes `Mutex<HashMap<String, Arc<dyn Backend>>>`. Look up under the lock; on a miss, drop the lock, build, re-lock and `entry().or_insert`, returning the stored Arc. Recover from poisoning with `PoisonError::into_inner`. Drop dashmap. `get_pre_signed_urls` compares origins and builds paths with the same `from_url_path` helper as resolve, instead of `get_backend` per URL. Today, URLs with a different port or userinfo get signed with the first URL's credentials. | registry.rs:5,11-23,35-95; client.rs:722-755; Cargo.toml:24 | −8 to −12, then 0 to +3 | low, security-sensitive / stricter batches (behaviour change) | G-fast; registry key and cap tests (assert the cap exactly); `server_side_copy_needs_the_same_backend`; new test that a mixed port or userinfo batch is rejected; G-int |
| 2.4 | (CORE-3 + CORE-13) Add a `cli = ["dep:clap", "dep:tokio-util", "tokio/signal", "tokio/io-std", "tokio/macros"]` feature, on by default. `#[cfg(feature = "cli")] pub mod cli`, `required-features = ["cli"]` on `[[bin]] osc`, `#[cfg(feature = "cli")]` on `tests/functional/cli.rs`. Library tokio drops `full` for `rt`, `rt-multi-thread`, `fs`, `io-util`, `sync`, `time`. | Cargo.toml:21-53; lib.rs:3; tests/functional.rs; pyproject.toml:31 | +8 to +12 | low / the doc-hidden `cli` module becomes feature-gated | G-feat (both `--no-default-features` combinations); measure the release `.so` before claiming a size win |
| 2.5 | (CORE-6, adapted to 2.4) Replace the polled `PENDING_ABORTS` with a counter that is decremented by a drop guard (which also covers cancel and panic) plus `tokio::sync::Notify`. Don't use `TaskTracker`: it needs tokio-util `rt` in the library, which conflicts with 2.4. | upload.rs:68-94 | about −5 | low / none | upload drain/abort tests; CLI Ctrl-C test |

### Phase 3: Structural deduplication in production code

| Step | What | Files | Change | Risk / API | Gates |
|---|---|---|---|---|---|
| 3.1 | (CORE-1) Keep only `pre_signed_urls` on `Backend`. Add `fn signer(&self) -> Option<&dyn Signer> { None }`. The default body refuses unsigned stores and non-empty options. Azure and GCS shrink to `signer()`. The S3 override checks `skip_signature` as its first statement and never falls through to the default. `get_pre_signed_url` uses `from_ref` and `.pop().ok_or_else(..)`. Delete `generic_pre_signed_url(s)`. | backend/mod.rs:116-141,202-245; s3.rs:295-382; azure.rs:175-193; gcs.rs:174-192; client.rs:691-702 | −85 to −95 | low, security-sensitive / error text 'an S3 URL' becomes 'S3 URLs' | G-fast; tests/unit/sign.rs; G-int (pre-sign then fetch on all 3 providers) |
| 3.2 | (CORE-7) Store `bucket: String` at construction. Add `S3Backend::bucket_request`, which must go through `self.credential()` so the skip_signature check runs, and `AzureBackend::container_request` with per-method headers (`Content-Length: 0` on create only). Compute the GCS `base_url` once. Add a test that skip_signature S3 makes no credential call on the bucket path. Drop the now unused `url` parameter from the internal trait. | s3.rs:214-293; azure.rs:106-173; gcs.rs:101-172 | −25 to −30 | low, security-sensitive / none | G-fast; G-int create_bucket/bucket_exists |
| 3.3 | (missed) Cap and sanitize server error bodies: read at most 4 KiB with `chunk()` instead of `response.text()`. This is hardening, not size. | backend/mod.rs:194; s3.rs:255; azure.rs:136; gcs.rs:143 | +10 | low, security-sensitive / none | G-fast; G-int |
| 3.4 | (CORE-10) `presign_s3[_at]` take `&SignOptions`. Delete `BoundHeaders` and `bound_headers`. Iterator joins for the canonical query and headers. `content_binding_unsupported` becomes `pub(crate)`, or moves into backend/mod.rs together with 3.1. Keep the key-only sort as it is. | sign.rs:91-130,160-185,225-267; s3.rs:328-379; tests/unit/sign.rs | −12 to −15 | medium, security-sensitive / removes public `BoundHeaders` | Fixed-time vectors byte-identical; G-int S3 content-binding against garage |
| 3.5 | (CORE-8) `list` uses `strip_prefix` + `map_ok` + `try_collect`; `delete_by_prefix` uses `try_for_each`. Keep the empty-URL and file-root guards. | client.rs:337-389 | −12 to −15 | low / none | G-fast; G-int list cases |
| 3.6 | (R1, with 1.6 done) Merge the two branches of `Envelope::copy`. Compute `byte_copy` first and keep the early returns on the byte path only. One get, one `decode_expecting`/`encode_stream` if-expression, one `upload_stream`, one guarded `delete_unchanged_source`. Destructure `StoredHeader` only after the `names_active_key` and context lookups. Keep the explanatory comments. `encryption_info` uses `h.context.0.clone()`. | envelope.rs:54-70,595-666 | −12 to −15 | medium, security-sensitive / none | G-crypto; envelope copy, move and self-copy tests; tests/functional/encryption.rs; G-int |
| 3.7 | (R4) `KeyError::io<'a, E: Into<io::Error>>(path: &'a Path) -> impl FnOnce(E) -> Self + use<'a, E>`, which allocates the PathBuf only on error. Keep a local `malformed` closure in `parse()`. Keep ELOOP→Malformed explicit. Leave the non-Unix closures alone unless you cross-check them for a Windows target. | keys/file.rs:141-555 | −12 to −18 | low, security-sensitive / none | keys::file tests |
| 3.8 | (R6 + R7) Add `DecodeLimits::check_frame_length(n)`, which also rejects 0, for the builder, `Envelope::context` and `Encryptor`; the parser is unchanged. Add `Encryptor::sealed_len_bound(plain)`, and delete `FRAME_OVERHEAD` (envelope.rs:38), the `frame_length` field in `EncodeState::Run` and both `usize::try_from` conversions. | envelope.rs:37-38,109-116,330-335,358-399,421-423; frame.rs:29-34,90-92,146-151 | −8 to −14 | low / `frame_length(0)` is now rejected earlier with the same error | G-crypto; golden byte-identical |
| 3.9 | (R9 + R15, optional) Read fixed-width integers with `split_first_chunk`, `first_chunk` or `<[u8; N]>::try_from`. Use let-else and an error arm, not `expect`, on untrusted input. Build the frame AAD as a stack array, returning `([u8; 78], usize)`. | header.rs:183-191; context.rs:178-181; frame.rs:42-54,543-593; raw_aes.rs:107-114; suite.rs:114-116; file.rs:308-310 | −3 to −7 | low / none | G-crypto, including tamper bit-flips and fuzz |
| 3.10 | (P1) Add `python::call(py, fut)`, which wraps `spawn` and `client_to_py_err`. Enable pyo3's `bytes` feature. Add a `sign_args(..)` helper for the duplicated setup at client.rs:357-366 and 390-399. Use `Error::` consistently in `client_to_py_err`. Stream, metadata, inspect and pre-signed-urls keep using `spawn` directly. | python/mod.rs:19-48; python/client.rs:60-408; python/encryption.rs:140-195; Cargo.toml:33 | −45 to −55 | low, security-sensitive (fork guard) / none | G-fast; G-py, including the fork, stream-leak and sticky-error tests; check that `help()` still shows the docstrings |
| 3.11 | (P2 + P4 + P15 + missed) CLI plumbing: inline the 7 one-line commands into `dispatch`; `put -r` borrows instead of cloning and collects into `Result<(), _>`; `SignArgs` clap struct (help output byte-identical); `write_stream` helper shared by `get` and `get_stream`; one `#[cfg(unix)] mod platform` for the 4 cfg pairs. Fail-fast `put -r` is out of scope here and needs its own decision and test. | cli.rs:205-219,351-410,464-480,541-731,785-875,958-980 | −55 to −70 | low / none | G-fast; functional CLI tests (escape sanitizing, open_verified, recursive put) |
| 3.12 | (P5) Make `mod python` private (PyInit is still exported; check with `nm -D`). `FileId`, `open_verified`, `download_modes`, `temp_file_name`, `create_temp_file` and `destination_url` become `pub(crate)`, and their tests move to `src/cli/tests.rs`. | lib.rs:3,10; cli.rs:541-994; tests/functional/cli.rs:640-1060 | 0 to −10 | low / removes doc-hidden items | G-fast; G-feat; G-doc |

### Phase 4: Test consolidation

| Step | What | Files | Change | Risk | Gates |
|---|---|---|---|---|---|
| 4.1 | (CORE-12) `#[cfg(test)] Vars::from_pairs`; `parse_bool(&str) -> Option<bool>` shared by `is_truthy` and `explicit_skip_signature` (S3_ALLOW_HTTP untouched); `fixture(Counters)`. | backend/s3.rs:143-150,390-397; azure.rs:200-207; gcs.rs:200-207; mod.rs:71-77; upload.rs:336-592 | −25 to −30 | low | G-fast |
| 4.2 | (R2 + R3 as one slice) Add `#[cfg(any(test, feature = "fuzzing"))] mod fixtures` (dek, edk, `plain(n, salt)`, `small_limits`, `encrypt_with`, plus a default-limits helper). Add a `#[cfg(test)]` ESDK manifest loader parsed once through `LazyLock`. Replace `TestProvider` with `#[cfg(test)] FileKeyProvider::from_material(active, &[(id, &[u8; 32])])`; leave `open()` as is. | encryption/frame.rs:105-127,683-699,1165-1213; tamper_tests.rs:13-57; fuzzing.rs:13-44; header.rs:619-630; suite.rs:294-307; keys/mod.rs:253-332 | −85 to −100 | low | G-crypto; G-feat |
| 4.3 | (R11) `Decryptor::feed(&[u8])`, unless zero-copy ingestion (see §6) is scheduled; then keep `Bytes` and write `#[expect(.., reason)]`. | frame.rs:436-447,657; fuzzing.rs:80-82; envelope.rs:502-507 | −5 | low | G-crypto |
| 4.4 | (P7 + P8) Delete `tests/functional/storage_ops.rs`, which is always ignored. First fold its nested-path, sibling-survives and trailing-slash `folder/` cases into `client_suite::list_and_delete_by_prefix`. Merge local_path_tests.rs into client.rs and drop tests that duplicate the suite. The metadata assertions go into `plain_only_suite`. Keep the tilde test that does not touch the real `$HOME`. | tests/functional/{storage_ops,local_path_tests,client,suite}.rs; tests/functional.rs | −180 to −190 | low | `cargo test --test functional -- --include-ignored` before and after |
| 4.5 | (P9) `tests/support/mod.rs` (not `tests/support.rs`, which Cargo would build as its own test crate) with module-level `#[allow(dead_code)]`: `pattern`, `drain`, `names`, `chunked` (zero-copy `Bytes::slice`), `TestResult`. Convert hand-built `file://` URLs to `Url::from_file_path` only where the test is not about URL parsing. | tests/functional/*.rs, tests/integration/*.rs | −35 to −45 | low | G-fast; G-int |
| 4.6 | (P10) One provider-parameterised storage-ops suite for S3 and Azure only (fake-gcs-server rejects writes, gcs_storage_ops.rs:5-11). First move `Provider`, `base()` and `per_provider!` out of the encryption-gated `integration/encryption.rs` into `integration/common/`. This adds Azure move-onto-itself coverage. | tests/integration/{s3,azure}_storage_ops.rs, encryption.rs:49-67, common/ | −60 to −80 | medium | G-int; compare the scenario × provider matrix before and after |
| 4.7 | (P11) An `any_client` parametrised fixture (plain/secure) built on `store`, not `tmp_url`, so it never sees `keys/`. Delete the duplicate `client` fixture and `test_shared_methods`. `test_metadata_keys` stays plain-only. | tests/python/{conftest,test_plain_client,test_encrypted_client}.py | −25 | low | G-py (the test count goes up) |

**Ordering constraints:**
- 2.3 before 3.1, so they share `origin()`.
- 2.4 before 2.5.
- 1.6 before 3.6.
- 3.1 before 3.4.
- 4.2 and 4.3 as one slice.
- 4.4 and 4.5 together.

## 4. Dependency changes

| Dependency | Change | Step | Effect |
|---|---|---|---|
| dashmap 6.2 | remove | 2.3 | One fewer dependency, no per-resolve String allocations, exact cap |
| homedir 0.3 | remove (`std::env::home_dir`) | 2.2 | Also drops nix 0.30.1 (Unix) and windows 0.61 |
| futures-util | remove (use `futures::`) | 2.1 | Still present transitively |
| chrono | drop the `serde` feature | 2.1 | Redundant, object_store enables it |
| clap, tokio-util | optional behind `cli` (default) | 2.4 | Out of the Python wheel and library builds |
| tokio | `full` becomes a minimal set; `cli` adds signal, io-std, macros | 2.4 | Drops process, parking_lot and net for library users |
| pyo3 | add the `bytes` feature | 3.10 | `Bytes` converts to `PyBytes` directly |
| async-trait | keep | — | `dyn Backend` / `dyn KeyProvider`; native AFIT is not dyn-compatible (E0038 on 1.99) |
| chrono, http, percent-encoding, zeroize, ring | keep | — | Public `DateTime<Utc>`, already in the tree, needed for secrets and crypto |
| sign.rs (hand-written SigV4) | keep | — | object_store 0.14 reserves `content-length` (RESERVED_SIGNED_HEADERS) |

## 5. Rejected ideas

- **R5: derive `Debug` for `KeyError` through an `Untrusted<T>` newtype.** The manual exhaustive `Debug` makes every new variant a compile error until its truncation is decided. Deriving it would print new untrusted `String` fields in full without any prompt. It would also change public field types on a public error enum.
- **R14: report a copy-time header mismatch as `Error::SourceChanged`.** That variant's contract (client.rs:80-85) says the destination was written and the source was kept. A mismatch aborts before commit and also happens on plain copies, so callers would be misled; tests/functional/encryption.rs:776 and 799 rely on the contract.
- **Native async fn in traits instead of async-trait.** It is not dyn-compatible (E0038).
- **`TaskTracker` for upload aborts.** It conflicts with making tokio-util optional; step 2.5 uses a drop guard with `Notify` instead.
- **Replacing sign.rs with `Signer::signed_url_opts`.** It cannot sign Content-Length.
- **Fail-fast `put -r` as part of the refactor.** It is a behaviour change that depends on drop-driven aborts, so it needs a separate decision and test.
- **GCS in the shared integration suite.** The emulator rejects writes.
- **The Entry-API rewrite of `FileKeyProvider::open`.** It saves nothing.
- **Moving the doc-hidden test hooks (`get_backend`, `get_store`, `backend_count`, `with_vars`) behind a feature.** It saves little.

## 6. Concrete missed opportunities (performance and hardening, outside the LOC target)

- **Unbounded listing:** `Client::list` collects the whole listing into `Vec<String>` (client.rs:337-389). Add a streaming `list_stream() -> impl Stream<Item = Result<String>>` shared by the CLI and Python, and keep `list` as a wrapper.
- **Extra copy on download:** `Decryptor::feed` copies every ciphertext chunk into `buf` with `extend_from_slice`. When `buf` is empty, take ownership with `BytesMut::from(Bytes)`/`try_into_mut` and decrypt in place. This decides 4.3.
- **Two parsers of untrusted input:** header.rs `Cursor` and context.rs `Reader` both implement take and u16. Merge them into one big-endian reader, or at least use `first_chunk` in both (3.9).
- **Per-entry blocking calls in the `put -r` walk:** `upload_dir_recursive` makes one tokio `read_dir`/`metadata` call per entry. Use a single `spawn_blocking` walk with `std::fs::symlink_metadata`, keeping the no-follow semantics.
- **Uri round-trip:** `send_signed` (backend/mod.rs:257-264) turns the `http::Uri` into a String for reqwest to parse again. Optional.