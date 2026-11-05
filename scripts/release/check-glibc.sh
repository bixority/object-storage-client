#!/usr/bin/env bash
# Usage: check-glibc.sh MAX FILE...
# Prints the highest GLIBC_x.y version each FILE needs. Exits 1 if any needs more than MAX.
set -euo pipefail

if [ "$#" -lt 2 ]; then
    echo "usage: $0 MAX FILE..." >&2
    exit 2
fi
max=$1
shift

status=0
for file in "$@"; do
    # objdump failing (missing or unreadable file) must fail the check; only
    # "no GLIBC symbols" (grep finds nothing) is an acceptable empty result.
    symbols=$(objdump -T "$file")
    highest=$(printf '%s\n' "$symbols" | grep -oE 'GLIBC_[0-9]+(\.[0-9]+)*' | sed 's/^GLIBC_//' | sort -uV | tail -n 1 || true)
    if [ -z "$highest" ]; then
        echo "$file: no GLIBC symbol versions"
        continue
    fi
    if [ "$(printf '%s\n%s\n' "$highest" "$max" | sort -V | tail -n 1)" != "$max" ]; then
        echo "$file: needs GLIBC_$highest (max allowed GLIBC_$max)"
        status=1
    else
        echo "$file: GLIBC_$highest (max allowed GLIBC_$max)"
    fi
done
exit $status
