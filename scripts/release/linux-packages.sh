#!/usr/bin/env bash
# Usage: linux-packages.sh VERSION ARCH BIN_DIR OUT_DIR
# Needs nfpm on PATH. Writes the .deb and .rpm for ARCH into OUT_DIR.
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 VERSION ARCH BIN_DIR OUT_DIR" >&2
    exit 2
fi
version=$1 arch=$2 bin_dir=$3 out_dir=$4

case "$arch" in
x86_64) nfpm_arch=amd64 ;;
aarch64) nfpm_arch=arm64 ;;
*)
    echo "unsupported ARCH: $arch (want x86_64 or aarch64)" >&2
    exit 2
    ;;
esac

root=$(cd "$(dirname "$0")/../.." && pwd)
mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
bin_dir=$(cd "$bin_dir" && pwd)

cd "$root"
config=$(mktemp)
trap 'rm -f "$config"' EXIT
sed "s|@BIN_DIR@|$bin_dir|g" packaging/nfpm.yaml >"$config"
for packager in deb rpm; do
    VERSION=$version NFPM_ARCH=$nfpm_arch \
        nfpm package --config "$config" --packager "$packager" --target "$out_dir/"
done
# nfpm names: object-storage-client_VERSION-1_amd64.deb, object-storage-client-VERSION-1.x86_64.rpm
ls -1 "$out_dir/object-storage-client_${version}-1_${nfpm_arch}.deb" "$out_dir/object-storage-client-${version}-1.${arch}.rpm"
