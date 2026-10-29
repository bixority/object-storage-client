#!/usr/bin/env bash
# Runs the hermetic Rust suites in a hostile environment: a HOME with poisoned
# ~/.aws files, poison AWS_*/S3_*/OSC_* values, a dead proxy with NO_PROXY set
# for loopback, and a dead IMDS endpoint. Every test must pass exactly as it
# does in a clean environment. Run it by hand; CI does not run it.
#
# Usage: scripts/hostile-env.sh [cargo test arguments...]
# Without arguments it runs: --locked --all-features --lib --test unit --test functional
set -euo pipefail

cargo_home="${CARGO_HOME:-$HOME/.cargo}"
rustup_home="${RUSTUP_HOME:-$HOME/.rustup}"
target_dir="${CARGO_TARGET_DIR:-$PWD/target}"
home="$(mktemp -d)"
trap 'rm -rf "$home"' EXIT

# Profile files that define every profile name the tests use, all broken.
mkdir -p "$home/.aws"
for name in osc-ci-missing osc-test-no-such-profile a b dev p sso nope default; do
  {
    if [ "$name" = default ]; then echo "[default]"; else echo "[profile $name]"; fi
    echo "region = ap-south-1"
    echo "credential_process = false"
  } >> "$home/.aws/config"
done
printf '[default]\naws_access_key_id = AKIAHOSTILEHOMEFILE\naws_secret_access_key = hostile-home-secret\n' \
  > "$home/.aws/credentials"

if [ "$#" -eq 0 ]; then
  set -- --locked --all-features --lib --test unit --test functional
fi

exec env -i \
  PATH="$PATH" \
  HOME="$home" \
  CARGO_HOME="$cargo_home" \
  RUSTUP_HOME="$rustup_home" \
  CARGO_TARGET_DIR="$target_dir" \
  HTTP_PROXY=http://127.0.0.1:9 \
  HTTPS_PROXY=http://127.0.0.1:9 \
  ALL_PROXY=http://127.0.0.1:9 \
  NO_PROXY=127.0.0.1,localhost,::1 \
  AWS_EC2_METADATA_SERVICE_ENDPOINT=http://127.0.0.1:9 \
  AWS_ACCESS_KEY_ID=AKIAHOSTILEENVIRONMENT \
  AWS_SECRET_ACCESS_KEY=hostile-env-secret \
  AWS_SESSION_TOKEN=hostile-env-token \
  AWS_REGION=xx-hostile-1 \
  AWS_DEFAULT_REGION=xx-hostile-1 \
  AWS_PROFILE=osc-ci-missing \
  AWS_CONFIG_FILE=/nonexistent/aws-config \
  AWS_SHARED_CREDENTIALS_FILE=/nonexistent/aws-credentials \
  AWS_ENDPOINT_URL=http://127.0.0.1:9 \
  AWS_SKIP_SIGNATURE=bogus \
  S3_REGION=xx \
  S3_ENDPOINT=http://127.0.0.1:9 \
  OSC_KEYRING=/nonexistent/keyring \
  OSC_KEYRING_COMMAND=false \
  cargo test "$@"
