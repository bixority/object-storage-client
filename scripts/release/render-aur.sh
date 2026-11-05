#!/usr/bin/env bash
# Usage: render-aur.sh VERSION SHA256SUMS OUT_DIR
# Renders OUT_DIR/PKGBUILD from packaging/aur/PKGBUILD.template. Does not make .SRCINFO.
set -euo pipefail

if [ "$#" -ne 3 ]; then
    echo "usage: $0 VERSION SHA256SUMS OUT_DIR" >&2
    exit 2
fi
version=$1 sums=$2 out_dir=$3

root=$(cd "$(dirname "$0")/../.." && pwd)

sum_for() {
    local arch=$1 name="object-storage-client-${version}-linux-${1}.tar.gz" line
    # SHA256SUMS lines are "HASH  NAME" or "HASH *NAME", optionally with a directory prefix.
    line=$(awk -v n="$name" '{f=$2; sub(/^\*/, "", f); sub(/^.*\//, "", f); if (f == n) print $1}' "$sums")
    if ! [[ $line =~ ^[0-9a-f]{64}$ ]]; then
        echo "no valid sha256 for $name in $sums" >&2
        exit 1
    fi
    printf '%s' "$line"
}

x86=$(sum_for x86_64)
arm=$(sum_for aarch64)

mkdir -p "$out_dir"
sed -e "s|@VERSION@|$version|g" \
    -e "s|@SHA256_X86_64@|$x86|g" \
    -e "s|@SHA256_AARCH64@|$arm|g" \
    "$root/packaging/aur/PKGBUILD.template" >"$out_dir/PKGBUILD"

if grep -q '@[A-Z0-9_]*@' "$out_dir/PKGBUILD"; then
    echo "unreplaced tokens in $out_dir/PKGBUILD" >&2
    exit 1
fi
echo "$out_dir/PKGBUILD"
