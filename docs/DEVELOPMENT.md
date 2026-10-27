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

## Cargo features

| Feature                | Purpose                                                          |
|------------------------|------------------------------------------------------------------|
| `encryption` (default) | Client-side encryption                                           |
| `cli` (default)        | The `osc` binary and its `clap` dependency                       |
| `python`               | The pyo3 bindings, built by maturin                              |
| `fuzzing`              | Exposes `encryption::fuzzing` entry points for the `fuzz/` crate |

## Checks

CI runs these on every pull request, alongside the tests:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic
cargo clippy --all-targets --no-default-features -- -D warnings -W clippy::pedantic
cargo check --no-default-features --features python
cargo check --features fuzzing
cargo check --manifest-path fuzz/Cargo.toml
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features
```

## Fuzzing

The `fuzz/` crate has three targets: `header_parse`, `decryptor_feed` and
`encrypt_mutate`. Run one with:

```bash
cargo +nightly fuzz run header_parse
```

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

- `tests.yaml`: tests, lints, feature builds, Python tests and the AWS
  Encryption SDK interop check, on pushes to `main` and on pull requests.
- `perf.yaml`: benchmarks and the memory smoke test, nightly or on demand.
- `ci.yaml`: when a release is published, tests, then builds wheels for
  Python 3.13, 3.14 and 3.15 and the sdist, and publishes the crate to Codeberg and
  the package to PyPI.
