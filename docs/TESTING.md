# Testing

## Quick tests

No containers or network needed:

```bash
cargo test --all-features

uv sync --group dev --group oracle --no-install-project
uv run --no-sync maturin develop
uv run --no-sync pytest
```

The `oracle` group installs the AWS Encryption SDK. Without it the Python
interop tests skip silently (`pytest.importorskip`).

`cargo test` runs the library's unit tests, `tests/unit.rs`,
`tests/functional.rs` (the client, encryption and `osc` on `file://` in
temporary directories, plus the golden ciphertexts in `tests/vectors/golden/`)
and `tests/perf_guards.rs`. A plain `pytest` runs `tests/python/` without the
container tests.

Behind a proxy: set `NO_PROXY=127.0.0.1,localhost` when `HTTP_PROXY`,
`HTTPS_PROXY` or `ALL_PROXY` is exported. The loopback stubs are reached
through `object_store`'s clients, which always honour system proxies and cannot
turn them off. The clients the crate builds itself ignore proxies in tests
(injected variables).

To check that the tests do not depend on your machine, run
`scripts/hostile-env.sh`: it runs the library, unit and functional suites
with a poisoned HOME and `~/.aws`, poisoned `AWS_*`, `S3_*` and `OSC_*`
variables, a dead proxy and a dead IMDS endpoint. It is not run in CI.

## Integration tests

The integration tests run the S3 and Azure backends, including client-side
encryption and copies between the two, against Garage and Azurite containers
started by `testcontainers`. They are `#[ignore]`d, so a plain `cargo test`
skips them:

```bash
cargo test --all-features --test integration -- --ignored
```

GCS is covered by a fake-gcs-server container
(`docker.io/fsouza/fake-gcs-server`): bucket create / exists, reads, listing,
deletes, metadata, and decrypting or copying encrypted objects out of it. The
emulator rejects the XML API writes `object_store` uses (`put`, multipart,
copy), so objects are seeded through its JSON upload API and those write paths
are not covered; see `tests/integration/common/gcs.rs`. The tests point bucket
management at the emulator with `GCS_BASE_URL` (the JSON API origin, default
`https://storage.googleapis.com`); the OAuth token is only sent when that URL
is https.

The Python tests that need containers carry the `integration` marker, so a
plain `pytest` skips them. They start their own Garage and Azurite with
testcontainers-python and remove them at the end of the session:

```bash
uv run pytest -m "integration and not large"
```

With Docker nothing else is needed. With rootless Podman, point
testcontainers at its socket and disable the Ryuk reaper:

```bash
export DOCKER_HOST=unix:///run/user/$(id -u)/podman/podman.sock
export TESTCONTAINERS_RYUK_DISABLED=true
```

The Rust tests' containers are not removed when the tests end. They are reusable: one
Garage, one Azurite and one fake-gcs-server container (named
`osc-it-garage-v2.5.0`, `osc-it-azurite` and `osc-it-gcs`) are started by the
first run and reused by later runs, which is safe because every test uses
unique prefixes and bucket names. Garage is pinned and its name carries the
image tag. Azurite and fake-gcs-server follow `latest` on purpose, under fixed
names. testcontainers reuses a container by name whatever image it was created
from and only pulls an image that is missing, so a reused local container keeps
its old image while CI pulls the newest. To test against the newest `latest`,
pull the image and remove the containers by label. Their ports are published
on 127.0.0.1 only, since the long-lived containers carry well-known test keys.
For a fresh state, remove them by label:

```bash
podman rm -f $(podman ps -aq --filter label=org.bixority.object-storage-client)
```

(use `docker` instead of `podman` where applicable). Details are in
`tests/integration/common/mod.rs`.

### Checksum round trips

`tests/integration/file_roundtrip.rs` (Rust) and
`tests/python/test_roundtrip.py` (Python) write a random file of 4 KiB, 4 MiB
and 4 GiB, upload it to Garage (S3) and Azurite (Azure) with the plain and the
encrypting client, stream it back and compare SHA-256 checksums. GCS is not
part of it, because no available emulator accepts the client's uploads. The
4 GiB tier is local only (CI skips it) and needs about 4 GiB of free disk per
test under `target/tmp` (the source file, deleted afterwards) and in the
emulator's container storage:

```bash
cargo test --all-features --test integration -- --ignored _4gib
uv run pytest -m large
```

## AWS Encryption SDK interop

The ignored tests whose names contain `oracle` check that objects are
byte-compatible with the AWS Encryption SDK for Python, which
`scripts/esdk_oracle.py` drives:

```bash
uv sync --group oracle --no-install-project
UV_NO_SYNC=1 cargo test --all-features -- --ignored oracle
```

`tests/vectors/esdk/` holds reference ciphertexts made by the SDK and
`tests/vectors/golden/` objects written by this client. The ESDK vectors are
regenerated as their README explains; the golden v1 files are frozen and never
regenerated. Every key in `tests/vectors/` is test-only.

## Performance

`cargo bench --bench throughput --no-default-features --features encryption`
prints a table of get, get_stream, put_stream (several chunk sizes), a
re-encrypting copy and 1000 small objects, plain and encrypted, on `file://`.
`tests/perf_guards.rs` (part of `cargo test`) pins allocation
counts and peak heap with a counting allocator, so regressions fail
deterministically. The `perf` workflow runs the bench and a 512 MiB `osc`
smoke test (`scripts/perf_smoke.py`) nightly or on demand. It compares the
smoke test's throughput with the previous successful run's, kept in the Actions
cache, and only warns when it drops by more than 25%; peak-RSS budgets fail the job. The
first run, or one after the cache expired, has nothing to compare with.

## Fuzzing

See [Development](DEVELOPMENT.md#fuzzing).
