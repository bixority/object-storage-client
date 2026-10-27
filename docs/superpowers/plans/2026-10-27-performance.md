# Performance plan: memory, CPU and throughput

Scope: worktree m2-crypto at commit 82dcdb3, rustc 1.99. I checked these claims against the code: `upload.rs:26-27,98,132-154`, `main.rs:41`, `cli.rs:26,535,793,875,974`, `envelope.rs:34,41,189,556,601`, `client.rs:218`, `codec.rs:173`, `backend/local.rs:20`, `registry.rs:92`, `frame.rs:42-54`, `Cargo.toml:83,86`.

## 1. Baseline

Each figure is the median of 3 runs, warm cache, on btrfs over LUKS. Under load, the S3 and Python numbers may read slightly pessimistic.

| Scenario (512 MiB unless noted) | Throughput | CPU | Peak RSS |
|---|---|---|---|
| CLI plain put file:// | 4512 MB/s | 0.16 s | 20 MB |
| CLI plain get file:// | 1730 MB/s | 0.34 s | 12 MB |
| CLI enc put file:// | 2810 MB/s | 0.28 s | 25 MB |
| CLI enc get file:// | 1355 MB/s | 0.43 s | 12 MB |
| CLI enc get-stream to /dev/null | 1365 MB/s | 0.42 s | 11 MB |
| CLI enc cp (same key / new key) | 1127 / 1022 MB/s | 0.55 / 0.60 s | 24 MB |
| CLI enc mv across dirs | 1223 MB/s | 0.50 s | 22 MB |
| CLI put -r 1000 x 16 KiB (enc / plain) | 38 / 32 µs per object | 0.10 / 0.12 s | 12 MB |
| Py enc put_object 64 MiB bytes | 3345 MB/s | 0.02 s | +65 MB |
| Py get_object 64 MiB (plain = enc) | 1271 MB/s | 0.06 s | +128 MB (2x) |
| Py enc get_object_stream 64 MiB | 858 MB/s (plain 8 KiB: ~300 MB/s) | 0.09 s | +1 MB |
| Py enc put_object_stream 64 MiB | 1214 MB/s | 0.07 s | +28 MB |
| Py pipe enc get_stream to plain put_stream | 668 MB/s | 0.93 s | +8.6 MB |
| S3 Garage plain put / get | 1092 / 1022 MB/s | 0.41 / 0.25 s | 37 / 12 MB |
| S3 Garage enc put / get / get-stream | 1031 / 959 / 1105 MB/s | 0.45 / 0.32 / 0.24 s | 46 / 11 / 12 MB |

What the numbers show:
- **Crypto is not the bottleneck.** ring AES-GCM runs at about 12.6 GB/s per core. Encryption adds about 0.12 s of CPU per 512 MiB. Against S3 the difference from plain is within 6%.
- **The main local cost is chunking.** object_store's `LocalFileSystem` streams 8 KiB chunks with one `spawn_blocking` call each. Plain local get is 2.6x slower than put, and Python plain streams are slower than encrypted ones.
- **Streaming memory is flat**, about 11-12 MB whatever the object size. Each upload holds about (4+1) x 8 MiB.
- **Some paths scale memory with input.** `put -r` peaks at about 263 MB for 8 x 64 MiB files. Python `get_object` needs 2x the object size. Concurrent library uploads are not bounded across uploads.
- **Encrypted cp and mv never use a server-side copy** when the source and destination paths differ and `bind_object_key` is on (`envelope.rs:601`). This is correct by design, but it means a full download plus upload.
- **Python streaming costs about 20 µs per item** for each Rust/asyncio round trip.

## 2. Budgets to hold

| Budget | Value | Justification |
|---|---|---|
| Peak RSS of a get or get-stream, any size | ≤ baseline + 4 MiB (CLI ≤ 16 MB) | Measured 11-12 MB at 512 MiB, local and S3 |
| Peak RSS of a single upload | ≤ (max_concurrency+1) x part_size + 2 x SLICE + 8 MiB | Measured 37-46 MB = 5 x 8 MiB + slack |
| Peak RSS of CLI `put -r` | ≤ 2x a single upload, whatever the file count | Today 263 MB; target is about 140 MB or less |
| Python `get_object` peak | ≤ 2.1x object size (documented) | Measured 2x; there is no safe zero-copy path under `forbid(unsafe)` |
| Python stream peak | ≤ baseline + batch limit + 1 frame | Measured +1 MB |
| Encryption CPU overhead | ≤ 0.35 s per GiB over plain | Measured about 0.24 s per GiB |
| Encrypted vs plain throughput, remote | within 10% | Measured within 6% |
| Ciphertext expansion | ≤ 0.06% at 64 KiB frames | Measured 0.05% |
| Per-object encryption cost (small objects) | ≤ 10 µs | Measured about 6 µs |
| Requests: encrypted get | 1 GET | Current |
| Requests: encrypted cp/mv (re-encrypt) | ≤ 1 ranged GET + 1 GET + upload (+HEAD+DELETE for mv) | Current; IO-6 would drop the probe |
| Largest streamed upload | ≥ 1 TiB with a size hint | Today about 78 GiB (10,000 x 8 MiB) |
| CLI startup (`osc exists`) | ≤ 3 ms on many-core hosts | 4.1 ms now, 1.9 ms with 4 workers |

## 3. Prioritized optimizations

### P1

**P1-a. Size-hinted part size (removes the ~78 GiB ceiling).** IO-1, size-hint part only.
- **Files:** `src/upload.rs:16-29,153` (fixed `new_with_chunk_size`), callers `client.rs:278-293`, `codec.rs:176-181`, `envelope.rs:624-629,652-657`, `cli.rs:529-538`.
- **Change:** add `UploadOptions::size_hint: Option<u64>`. Use `part_size = max(8 MiB, ceil(size*1.01/9000))`. The hint comes from `Metadata::len` for CLI put, `ObjectMeta.size` for copy and move, and an optional Python argument. The 1% margin covers re-encryption overhead of about 28 B per 64 KiB.
- **Keep:** `WriteMultipart` and `AbortOnDrop`.
- **Defer:** the adaptive JoinSet part loop for unknown-size `put_stream`. Until then, document the ~78 GiB limit.
- **Gain:** inputs of known size up to the 5 TiB provider limit. No change below 8 GiB.
- **Risk:** low to medium.
- **Security:** abort-on-error and abort-on-drop are unchanged.
- **Measure:** FaultStore test with a small base part size that fails at `idx >= 10_000`. Assert it completes with byte-exact output. The existing abort tests must stay green.

**P1-b. Larger local read chunks.** IO-3, IO-10, PY-PERF-5 and the crypto "missed" item, merged.
- **Files:** a new `body_stream(GetResult)` helper, used at `client.rs:218`, `codec.rs:173`, `envelope.rs:170,556,620,647`. Also `cli.rs:26` (`FILE_BUF`).
- **Change:** for `GetResultPayload::File`, read 256 KiB to 1 MiB per `spawn_blocking`. Seek once to `range.start` and read exactly `range` bytes. Return an error, not EOF, if the file shrinks (plain reads would otherwise truncate silently). Other payloads fall back to `into_stream()`. Raise `FILE_BUF` to 256 KiB to 1 MiB and set `File::set_max_buf_size` on the download side.
- **Gain:** local get and get-stream about 2x faster (100-180 ms saved per 512 MiB). Python plain streams get faster too. The cost of encrypted `get()` mostly goes away.
- **Risk:** low.
- **Security:** the decryptor already accepts any chunking (`expected_header_is_compared_once_whatever_the_chunking`).
- **Measure:** `osc get` and `get-stream` of 512 MiB file→file, plain and `--keys`, plus a test that a file shrinking mid-read errors.

**P1-c. Python stream batching (cancellation-safe).** PY-PERF-1.
- **Files:** `src/python/stream.rs:17-63,104-132`.
- **Change:** `fill()` keeps pulling until 256 KiB, EOF or an error. Each item is pushed into `Shared` under the lock right after `next()` returns. This matters because pyo3-async-runtimes drops the future when the asyncio future is cancelled, so a local Vec would lose data. A terminal Err or EOF is parked behind the chunks already buffered. `take()` copies once through `PyBytes::new_with`. To avoid slow time-to-first-byte on slow links, combine it with P1-b and keep pulling only while `now_or_never()` returns ready, or use a short deadline.
- **Gain:** plain 256 MiB stream 0.82 s → 0.20 s, prototyped. Encrypted streams gain 10-15%.
- **Risk:** low.
- **Security:** a batch only joins items that were already verified. Errors stay sticky.
- **Measure:** `target/review/perf-python/bench.py get_stream|enc_get_stream`. Add tests for cancel mid-batch (bytes stay contiguous) and for an error after buffered chunks.

### P2

**P2-a. Whole-object decrypt for `Envelope::get_bytes`.** PERF-CRYPTO-1.
- **Files:** `envelope.rs:553-561`, `frame.rs:519-612`.
- **Do first (cheap step):** `Decryptor::feed` adopts the input `Bytes` when `buf` is empty (`try_into_mut`, with one copy as fallback), plus `result.bytes()`.
- **Full version:** factor `next_frame` into one helper that validates and opens a frame in place. Both the streaming path and `decrypt_all(BytesMut)` call it; the compaction uses `copy_within`. Return the plaintext only after `Done` with an empty buffer. Above `SLICE`, run in `spawn_blocking`.
- **Gain:** 0.165 s → 0.05-0.07 s per 256 MiB local, and about 74k → under 100 allocations. Peak memory does not improve (already about 1x).
- **Risk:** medium.
- **Security:** this is security-sensitive. There must be one copy of the seq, IV, final-marker, trailing-byte and binding checks, not a duplicate.
- **Tests:** the tamper corpus must fail identically through `get` and `get_stream`. Truncation, an appended trailing byte and a missing final frame must return Err. Cover the `try_into_mut` failure fallback.

**P2-b. Bucket-op HTTP client: lazy, with timeouts.** IO-4(a).
- **Files:** `registry.rs:38,92,103`, `backend/mod.rs` `send_signed`.
- **Change:** use `OnceLock<reqwest::Client>` with `connect_timeout(5s)` and `timeout(30s)`.
- **Gain:** about 0.9 ms per client construction, and `create_bucket` / `bucket_exists` can no longer hang forever.
- **Risk:** low.

**P2-c. Fail-fast `put -r` with abort drain on the error path.** IO-9.
- **Files:** `cli.rs:916-981`, `main.rs:41-79`.
- **Change:** `buffer_unordered(8).try_collect()`. `main` must call `drain_pending_aborts(DRAIN_TIMEOUT)` on any error, not only on signals. This is mandatory, or dropped multipart uploads and staging files are left behind.
- **Gain:** a misconfiguration fails after about 1 request instead of one per file.
- **Risk:** medium.
- **Test:** no pending multipart uploads on Garage after the error exit.

**P2-d. No copy of a large chunk after a small one.** IO-8.
- **Files:** `upload.rs:120-143`.
- **Change:** if `chunk.len() >= COALESCE_BELOW` and buffered + chunk ≥ `part_size`, start the multipart with `buf.freeze()` and then `write(chunk)` zero-copy. Pre-size `buf`.
- **Gain:** removes a 1x-chunk memory spike (−262 MiB in the measured case).
- **Risk:** low.
- **Tests:** `tiny_chunks_are_coalesced`, `large_first_chunk_skips_buffer` and `small_input_single_put` stay green.

**P2-e. Python `put_object` buffer inputs.** PY-PERF-3.
- **Files:** `python/client.rs:15-23`, `stream.rs:151-163`.
- **Change:** one shared helper. Bytes and bytearray go through `PyBackedBytes`. Other buffer-protocol objects go through `bytes(obj)`; do not use `PyBuffer<u8>`, which rejects non-'B' formats. Keep the `list[int]` fallback. Update the TypeError message and the `.pyi`.
- **Gain:** memoryview 106 ms → about 3 ms per 16 MiB.
- **Risk:** low.

**P2-f. CLI runtime size.** IO-7.
- **Files:** `main.rs:41`.
- **Change:** build the runtime explicitly with `worker_threads(available_parallelism().min(4))`, still honouring `TOKIO_WORKER_THREADS`.
- **Gain:** about 2 ms less startup and about 60 fewer threads. Transfer time is unchanged.
- **Risk:** low.

**P2-g. Bound `put -r` memory in the CLI.** IO-2, CLI-only part.
- **Change:** pass `UploadOptions{max_concurrency: 2}` per file in `put -r`, or scale file concurrency with file size.
- **Gain:** about 263 → ≤ 140 MB.
- **Defer:** the process-wide part semaphore until a custom part loop exists. It does not cover fill buffers anyway.

### P3

- **PERF-CRYPTO-3 (minimal):** `Encryptor::emitted_len(n)`. Allocate in `EncodeState::Run` (`envelope.rs:423`) only when a frame completes, and allocate exactly that size. Gain: 78k → about 35k allocations at 4 KiB chunks; throughput about the same. The chunking-invariance and golden tests cover it.
- **PERF-CRYPTO-2:** slice stored chunks larger than `SLICE` in `DecodeState` (`envelope.rs:455-515`). This is robustness for InMemory and custom backends, about 15 lines. In echo mode, a raw slice may only be pushed after it has been fed and verified.
- **PERF-CRYPTO-4:** `frame_aad` (`frame.rs:42`) returns a stack buffer `([u8; 78], usize)`. Leave `Progress::Plaintext` alone.
- **PERF-CRYPTO-5a:** set the first header probe to `max_header_len` (`envelope.rs:41,189`), so there is one ranged GET even for large headers.
- **IO-6 / PERF-CRYPTO-5b:** skip the header probe when `bind_object_key && src.path != dst.path`. The encoder takes its context from the decoder's authenticated header through a slot that is filled only after tag and binding verification. Tests must show that nothing is committed for an unbound or tampered source, on both the single-put and multipart paths. Gain: 1 RTT per small-object cp or mv.
- **IO-5:** `StoreRegistry::resolve_pair` so that copy and move keep `Arc::ptr_eq` past the 256-backend cap. No extra key normalisation.
- **IO-11:** `list_stream` for `osc ls`, using `strip_prefix`.
- **PY-PERF-2:** `_anext_batch(it, limit)` for `put_object_stream`. Check that each `__anext__` result is awaitable inside the batch. Document the bounded read-ahead in the docstrings and `.pyi`.
- **PY-PERF-4:** cap the Python tokio workers through `pyo3_async_runtimes::tokio::init` (with `enable_all`), with an override such as `OSC_WORKER_THREADS`. The win is thread count; the reported VmSize is malloc arenas, not stacks.
- **PY-PERF-6(a):** document the 2x peak of `get_object` and point users to `get_object_stream`.
- **IO-12:** decide on `panic=abort` for wheels (for example a `[profile.wheel]` with unwind). Drop the redundant `[profile.release.package."*"]` block (`Cargo.toml:86`).
- **Docs:** state that encrypted cp and mv to a different path always download and re-encrypt, and that encrypted mv on file:// is not a rename.
- **Later:** pipelined re-encrypting copy (decode and encode in `spawn_blocking` overlapping the part uploads), and parallel ranged GETs for large downloads, with a fixed number of ranges in flight and frames released in order.

## 4. Benchmark and regression guard

- **Harness:** a `benches/` crate (or `[[bench]]` with `harness = false`, so no new framework) that uses the public API on file:// and InMemory, with a counting `#[global_allocator]`. Reuse the bench in `target/review/perf-crypto-path`. Scenarios:
  - get, get_stream and put_stream at 256 MiB, plain and encrypted
  - put_stream with 4 KiB, 64 KiB and 1 MiB chunks
  - copy with re-encryption
  - 1000 small objects
- **Deterministic guards in the normal tests** (they run in `tests.yaml` and are noise-free):
  - Allocation-count ceilings per scenario, for example enc get_stream of 64 MiB ≤ 1.2x the plain allocation count.
  - Peak heap above base: get_stream ≤ 4 MiB; put_stream ≤ (c+1) x part + 2 MiB.
  - Request counts per operation from the recording `TestStore` / FaultStore in `envelope.rs` and `upload.rs`. Encrypted get = 1 GET; re-encrypting copy ≤ 2 GETs.
  - A test that pins peak buffered bytes in `upload_stream`, which protects P1-a, P2-d and P2-g.
  - Part count stays ≤ 10,000 with a size hint.
- **CLI and Python smoke job:** a separate CI job, optional on PRs and run on nightly or by label. It builds release `osc` and the release wheel and runs 512 MiB file:// put, get and get-stream, plain and encrypted, through a small Python wrapper (as in `target/review/measure/scripts`). It records wall time, CPU and maxrss as JSON and uploads them as an artifact.
- **Fail thresholds:** RSS budgets from section 2 (hard). Encrypted vs plain CPU ratio > 2.5x on local (soft). Throughput is warn-only because shared runners are noisy; compare the median of 5 against the stored baseline and warn on a regression over 25%.
- **Integration (manual or nightly):** with the Garage/Azurite containers, check that encrypted vs plain put and get stay within 10% and that `put -r` 8 x 64 MiB RSS stays ≤ 140 MB after P2-g.

## 5. Rejected ideas

- **Changing the 64 KiB frame size:** 16 KiB and 1 MiB frames made no measurable end-to-end difference. 64 KiB also bounds the plaintext waiting for verification.
- **Full Encryptor restructure (frames built in the output buffer):** no throughput gain was measured (the encoder is already faster than the plain path), and it touches nonce/seq code.
- **Accumulating encoder output up to SLICE:** `upload_stream` already coalesces into parts.
- **Shared reqwest connector across backends (IO-4b):** per-host pools give little reuse with virtual-hosted buckets. The cache key that guards `https_only` would be security-sensitive.
- **LRU eviction in the registry:** not needed once `resolve_pair` exists. The cap case is rare.
- **Process-wide upload semaphore now:** it cannot be inserted into `WriteMultipart`, and it does not bound fill buffers. Revisit it with a custom part loop.
- **Caching imports in `anext_cancellable`:** measured no difference.
- **Zero-copy `get_object` buffer:** impossible under `forbid(unsafe)`. `get_object_into` is deferred until its provisional-plaintext semantics are decided.
- **Server-side copy for bound encrypted objects at a different path:** this would break the binding to `osc:key`. Re-encryption is the intended cost.
- **Dropping the HEAD in `delete_unchanged_source`:** it is required by the `SourceChanged` safety check.
- **Profile tuning:** fat LTO, codegen-units 1, opt-level 3 and strip are already set.