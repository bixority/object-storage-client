#!/usr/bin/env bash
# Build the macOS app bundle and .dmg (Apple silicon, ad-hoc signed only).
# Usage: scripts/release/macos-dmg.sh VERSION BIN_DIR OUT_DIR
# BIN_DIR holds `osc` and `osc-gui`.
# Writes OUT_DIR/object-storage-client-VERSION-macos-arm64.dmg
set -euo pipefail

if [ "$(uname -s)" != "Darwin" ]; then
    echo "error: macos-dmg.sh runs on macOS only" >&2
    exit 1
fi

if [ "$#" -ne 3 ]; then
    echo "usage: $0 VERSION BIN_DIR OUT_DIR" >&2
    exit 2
fi

VERSION="$1"
BIN_DIR="$2"
OUT_DIR="$3"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_NAME="Object Storage Client"
ICON_SRC="$ROOT/packaging/icon/osc-gui.png"
PLIST_TEMPLATE="$ROOT/packaging/macos/Info.plist.template"
README_SRC="$ROOT/packaging/macos/README.txt"
LICENSE_SRC="$ROOT/LICENSE"

case "$VERSION" in
    "" | v* | *[!0-9A-Za-z.+-]*)
        echo "error: VERSION must be like 0.4.4 (no leading v)" >&2
        exit 2
        ;;
esac

for f in "$BIN_DIR/osc" "$BIN_DIR/osc-gui" "$ICON_SRC" "$PLIST_TEMPLATE" "$README_SRC" "$LICENSE_SRC"; do
    [ -f "$f" ] || { echo "error: missing $f" >&2; exit 1; }
done

mkdir -p "$OUT_DIR"
BIN_DIR="$(cd "$BIN_DIR" && pwd)"
OUT_DIR="$(cd "$OUT_DIR" && pwd)"
DMG="$OUT_DIR/object-storage-client-$VERSION-macos-arm64.dmg"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK:?}"' EXIT
STAGING="$WORK/staging"
APP="$STAGING/$APP_NAME.app"
mkdir -p "$APP/Contents/MacOS" "$APP/Contents/Resources"

# Executables
cp "$BIN_DIR/osc" "$BIN_DIR/osc-gui" "$APP/Contents/MacOS/"
chmod 755 "$APP/Contents/MacOS/osc" "$APP/Contents/MacOS/osc-gui"

# Info.plist: replace every @VERSION@ placeholder, then check none are left.
PLIST="$APP/Contents/Info.plist"
template="$(cat "$PLIST_TEMPLATE")"
printf '%s\n' "${template//@VERSION@/$VERSION}" > "$PLIST"
if grep -Eq '@[A-Z_]+@' "$PLIST"; then
    echo "error: unreplaced placeholder in Info.plist:" >&2
    grep -E '@[A-Z_]+@' "$PLIST" >&2
    exit 1
fi
plutil -lint "$PLIST"

# Icon: .iconset from the 1024 PNG, then .icns
ICONSET="$WORK/AppIcon.iconset"
mkdir -p "$ICONSET"
for size in 16 32 128 256 512; do
    double=$((size * 2))
    sips -z "$size" "$size" "$ICON_SRC" --out "$ICONSET/icon_${size}x${size}.png" >/dev/null
    sips -z "$double" "$double" "$ICON_SRC" --out "$ICONSET/icon_${size}x${size}@2x.png" >/dev/null
done
iconutil -c icns "$ICONSET" -o "$APP/Contents/Resources/AppIcon.icns"

# Ad-hoc signing: each executable, then the bundle. Not a Developer ID signature.
codesign --force --sign - "$APP/Contents/MacOS/osc"
codesign --force --sign - "$APP/Contents/MacOS/osc-gui"
codesign --force --sign - "$APP"
codesign --verify --verbose=2 "$APP"

# Image root: app, Applications symlink, README.txt, LICENSE
ln -s /Applications "$STAGING/Applications"
cp "$README_SRC" "$STAGING/README.txt"
cp "$LICENSE_SRC" "$STAGING/LICENSE"

hdiutil create -volname "Object Storage Client $VERSION" -srcfolder "$STAGING" -format UDZO -ov "$DMG"

echo "$DMG"
