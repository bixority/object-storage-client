#!/usr/bin/env bash
# Usage: linux-tarball.sh VERSION ARCH BIN_DIR OUT_DIR
# Writes OUT_DIR/object-storage-client-VERSION-linux-ARCH.tar.gz
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 VERSION ARCH BIN_DIR OUT_DIR" >&2
    exit 2
fi
version=$1 arch=$2 bin_dir=$3 out_dir=$4

case "$arch" in
x86_64 | aarch64) ;;
*)
    echo "unsupported ARCH: $arch (want x86_64 or aarch64)" >&2
    exit 2
    ;;
esac

root=$(cd "$(dirname "$0")/../.." && pwd)
name="object-storage-client-${version}-linux-${arch}"

mkdir -p "$out_dir"
out_dir=$(cd "$out_dir" && pwd)
bin_dir=$(cd "$bin_dir" && pwd)

stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
mkdir "$stage/$name"

install -m 0755 "$bin_dir/osc" "$bin_dir/osc-gui" "$stage/$name/"
install -m 0644 "$root/LICENSE" "$root/README.md" "$stage/$name/"
install -m 0644 "$root/packaging/linux/osc-gui.desktop" "$root/packaging/icon/osc-gui.png" "$stage/$name/"

tar --sort=name --owner=0 --group=0 --numeric-owner --mtime='UTC 1970-01-01' \
    -C "$stage" -czf "$out_dir/$name.tar.gz" "$name"
echo "$out_dir/$name.tar.gz"
