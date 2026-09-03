#!/bin/sh

set -eu

PACKAGE_DIR="/usr/share/cockpit/webzfs"
STAGING_DIR="/usr/share/webzfs-cockpit-stage-$$"
BACKUP_ROOT="/usr/share/webzfs-cockpit-backups"
WEBZFS_DIR="/opt/webzfs"
SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SOURCE_DIR/../.." && pwd)
ASSET_DIR=${WEBZFS_ASSET_DIR:-$PROJECT_DIR}

if [ "$(uname -s)" != "Linux" ]; then
    printf '%s\n' "Error: The WebZFS Cockpit integration is Linux-only." >&2
    exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
    printf '%s\n' "Error: Run this installer as root." >&2
    exit 1
fi

if [ ! -d /usr/share/cockpit ] && ! command -v cockpit-bridge >/dev/null 2>&1; then
    printf '%s\n' "Error: Cockpit is not installed." >&2
    exit 1
fi

if [ ! -d "$WEBZFS_DIR" ]; then
    printf '%s\n' "Error: WebZFS is not installed at $WEBZFS_DIR." >&2
    exit 1
fi

for file in \
    "$ASSET_DIR/static/css/styles.css" \
    "$ASSET_DIR/static/css/corner_styles.css" \
    "$ASSET_DIR/static/img/webzfs-icon.svg" \
    "$ASSET_DIR/node_modules/htmx.org/dist/htmx.min.js" \
    "$ASSET_DIR/node_modules/alpinejs/dist/cdn.min.js" \
    "$ASSET_DIR/node_modules/chart.js/dist/chart.umd.js"
do
    if [ ! -f "$file" ]; then
        printf 'Error: Required Cockpit asset is missing: %s\n' "$file" >&2
        printf 'Run npm install and npm run build:css in the selected WebZFS asset tree: %s\n' "$ASSET_DIR" >&2
        exit 1
    fi
done

mkdir -p "$STAGING_DIR/static/css/themes"
mkdir -p "$STAGING_DIR/static/img"
mkdir -p "$STAGING_DIR/static/js"
mkdir -p "$STAGING_DIR/static/vendor"

for file in manifest.json index.html asset-monitor.js cockpit-webzfs.js transport.js navigation.js session.js asset-rewrite.js cockpit-webzfs.css
do
    install -m 0644 "$SOURCE_DIR/$file" "$STAGING_DIR/$file"
done

install -m 0644 "$ASSET_DIR/static/css/styles.css" "$STAGING_DIR/static/css/styles.css"
sed -i '/fonts\.googleapis\.com/d' "$STAGING_DIR/static/css/styles.css"
install -m 0644 "$ASSET_DIR/static/css/corner_styles.css" "$STAGING_DIR/static/css/corner_styles.css"
install -m 0644 "$ASSET_DIR/static/css/themes/"*.css "$STAGING_DIR/static/css/themes/"
install -m 0644 "$ASSET_DIR/static/img/"* "$STAGING_DIR/static/img/"

if find "$ASSET_DIR/static/js" -maxdepth 1 -type f | grep -q .; then
    install -m 0644 "$ASSET_DIR/static/js/"* "$STAGING_DIR/static/js/"
fi

install -m 0644 "$ASSET_DIR/node_modules/htmx.org/dist/htmx.min.js" "$STAGING_DIR/static/vendor/htmx.min.js"
install -m 0644 "$ASSET_DIR/node_modules/alpinejs/dist/cdn.min.js" "$STAGING_DIR/static/vendor/alpine.min.js"
install -m 0644 "$ASSET_DIR/node_modules/chart.js/dist/chart.umd.js" "$STAGING_DIR/static/vendor/chart.umd.min.js"

if [ -d "$PACKAGE_DIR" ]; then
    mkdir -p "$BACKUP_ROOT"
    BACKUP_DIR="$BACKUP_ROOT/webzfs.previous.$(date +%Y%m%d%H%M%S).$$"
    mv "$PACKAGE_DIR" "$BACKUP_DIR"
    printf 'Previous Cockpit package retained at %s for manual cleanup.\n' "$BACKUP_DIR"
fi
mv "$STAGING_DIR" "$PACKAGE_DIR"

printf '%s\n' "WebZFS Cockpit integration installed at $PACKAGE_DIR."
printf '%s\n' "Reload the Cockpit browser page to discover the WebZFS sidebar entry."