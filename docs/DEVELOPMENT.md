# Development

## Prerequisites

- Rust 1.99+ (edition 2024)
- Python 3.13+ and [uv](https://docs.astral.sh/uv/) for the bindings;
  `uv sync --group dev` installs maturin 1.15+, pytest and testcontainers
- Docker or Podman for the integration tests (see [Testing](TESTING.md))
- Nightly Rust and [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) for
  fuzzing

## Building

- **Rust library**: `cargo build --release`
- **CLI**: `cargo build --release --bin osc`
- **Python wheel**: `maturin build --release`
- **Python bindings into the project's virtualenv**:
  `uv sync --group dev --no-install-project`, then
  `uv run --no-sync maturin develop`

The committed `.cargo/config.toml` also drives the PyPI wheels, the sdist and the crate, so CPU tuning (for example `-C target-cpu=native`) belongs in `RUSTFLAGS` or your own `~/.cargo/config.toml`, never in the repository.

## Cargo features

| Feature                | Purpose                                                          |
|------------------------|------------------------------------------------------------------|
| `encryption` (default) | Client-side encryption                                           |
| `cli` (default)        | The `osc` binary and its `clap` dependency                       |
| `python`               | The pyo3 bindings, built by maturin                              |
| `fuzzing`              | Exposes `encryption::fuzzing` entry points for the `fuzz/` crate |

AWS profile and SSO support is always compiled in; do not gate it behind a
feature. It adds 37 crates to every build, the wheel and the
`default-features = false` library builds included (`Cargo.lock` grows from 333
to 370 packages; there is still no `ring`). The direct dependencies are
`aws-config` (with `sso`, `rt-tokio`, `default-https-client` and
`credentials-process`), `aws-credential-types`, `aws-runtime`, `aws-types` and
`tracing`; `aws-smithy-runtime-api` and `aws-smithy-types` are dev-dependencies
of the tests.

SSO, SSO-OIDC and STS requests go through `aws-smithy-http-client` (hyper, with
its own connection pool), not through the `reqwest` client of the object
requests. It verifies certificates with webpki against the OS roots, loaded
once per process. That matches `reqwest` on Linux, but it lacks the platform
verifier's OS revocation checks on macOS and Windows. It is used only when a
named profile needs SSO or STS. `tracing` is used to keep the SDK's trace
events (which can carry an SSO bearer token) out of the host application's logs.

`Client::with_vars` (hidden, for tests) reads only the profile files its map
names (`AWS_CONFIG_FILE`, `AWS_SHARED_CREDENTIALS_FILE`), but the AWS SDK still
reads the process environment (SSO and STS endpoint variables, FIPS settings)
and `HOME` (the SSO token cache).

## Third-party licenses

The wheels statically link their dependency crates, so they ship
`THIRD_PARTY_LICENSES.html` next to `LICENSE` (`license-files` in
`pyproject.toml`; PEP 639 puts both under `*.dist-info/licenses/`). It lists
the license texts of the crates in the wheel (normal dependencies with
`--no-default-features --features python,encryption`) and the NOTICE of
`object_store`, which Apache-2.0 section 4(d) requires. `about.toml` names the
licenses that are accepted (a dependency under another one fails the `licenses`
jobs of `tests.yaml` and `ci.yaml`) and `about.hbs` is the template. After a
dependency change, regenerate and commit the file:

```bash
cargo install --locked cargo-about --version 0.9.3
cargo about generate --locked --fail --no-default-features \
    --features python,encryption about.hbs --output-file THIRD_PARTY_LICENSES.html
```

The NOTICE text in `about.hbs` is static; keep it equal to the `NOTICE.txt` of
the locked `object_store`. The crate published to the registry is source only
and needs no notices file.

## Checks

CI runs these on every pull request, alongside the tests:

```bash
cargo fmt --all -- --check
cargo clippy --locked --all-targets --all-features -- -D warnings -W clippy::pedantic
cargo clippy --locked --all-targets --no-default-features -- -D warnings -W clippy::pedantic
cargo check --locked --no-default-features --features python
cargo check --locked --features fuzzing
cargo check --manifest-path fuzz/Cargo.toml
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps --all-features
```

## Fuzzing

The `fuzz/` crate has three targets: `header_parse`, `decryptor_feed` and
`encrypt_mutate`. Run one with:

```bash
cargo +nightly fuzz run header_parse fuzz/seeds/header_parse
```

The committed seeds in `fuzz/seeds/<target>/` are small, binary and replayed by
`cargo test --all-features` (`header_parse_seeds_replay` and its siblings), so
a fixed crash cannot come back unnoticed. `fuzz/corpus/` and `fuzz/artifacts/`
are working directories and stay ignored. To keep a crash, minimise it with
`cargo +nightly fuzz tmin <target> fuzz/artifacts/<target>/<crash-file>` and
add the output to `fuzz/seeds/<target>/` in the commit that fixes it. There is
no scheduled fuzzing job.

## Layout

- `src/client.rs`, `src/codec.rs`: the `Client` API and the plain codec
- `src/backend/`: one module per storage provider
- `src/encryption/`: message format, framing, keyrings and the encrypting codec
- `src/cli.rs`, `src/cli/`, `src/main.rs`: the `osc` binary
- `src/python/`, `python/object_storage_client/`: the bindings and their type stubs
- `tests/`, `benches/`, `fuzz/`, `scripts/`: see [Testing](TESTING.md)
- `compose.yml`: Garage and Azurite for manual exploration (`docker compose up`)
- `docs/superpowers/specs/`: design documents

## Workflows

- `tests.yaml`: tests, lints, a `cargo deny` check (advisories, bans, sources;
  see `deny.toml`), feature builds, Python tests and the AWS Encryption SDK
  interop check, on pushes to `main` and on pull requests. Every cargo call
  on the root crate passes `--locked`, so a `Cargo.toml` change without its
  lockfile fails the PR. Further jobs:
  - `msrv` runs `cargo check --locked --all-features --all-targets` with Rust
    1.99, the `rust-version` in `Cargo.toml`.
  - `tests` runs the Rust suite on Linux and macOS, and on Windows the library
    tests plus a check of every target.
  - `esdk-interop` fails unless exactly one oracle test passed.

  Runs on `main` are never cancelled. Its last job,
  `dependabot-merge`, runs after every other job and merges only Dependabot
  cargo patch updates of crates outside the reviewed list; everything else
  stays open for review.
- `perf.yaml`: benchmarks and the memory smoke test, nightly or on demand.
- `ci.yaml`: when a release is published, tests (stable Rust only, so a
  beta or nightly problem cannot block a release), then builds wheels for
  Python 3.13, 3.14 and 3.15 and the sdist. `test-wheels` installs every
  wheel into a fresh environment on its own platform (Linux x86_64, macOS,
  Windows, and the aarch64 Linux wheel under QEMU in its manylinux image;
  the legs match `build-wheels`) and runs the Python tests that need no
  containers;
  both publish jobs wait for it. `package-crate` checks that the tag
  equals `v` plus the Cargo.toml version and runs `cargo package --locked`
  without secrets. The publish jobs run only for a published release on a `v*`
  tag, in the `release` environment, after all of those succeed.
  `publish-crate` runs `cargo publish --no-verify` (no dependency code runs
  next to the Codeberg token) and `publish-to-pypi` uses PyPI Trusted
  Publishing (OIDC), with no stored token. Run it by hand
  (`workflow_dispatch`) from a branch to dry-run everything except publishing.
- Every `uses:` in every workflow is pinned to a full commit SHA with the tag
  in a trailing comment, and the `lint` job fails on an unpinned one.
  Dependabot proposes updates to the pins weekly; review them by hand.

Owner setup for publishing (one-off, in the hosting settings):

- Register a PyPI trusted publisher for `bixority/object-storage-client`,
  workflow `ci.yaml`, environment `release`.
- Create the GitHub environment `release`, limited to `v*` tags, and move
  `CODEBERG_TOKEN` into it.
- Then delete the `PYPI_API_TOKEN` secret and revoke that token on PyPI.
