#!/bin/bash

# Update WebZFS, then update the Cockpit integration.

set -euo pipefail

INSTALL_DIR="/opt/webzfs"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 0 ]; then
    printf '%s\n' "Usage: sudo ./update_linux_cockpit.sh" >&2
    exit 2
fi

if [ "$(uname -s)" != "Linux" ]; then
    printf '%s\n' "Error: This updater is Linux-only." >&2
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    printf '%s\n' "Error: Run this script as root:" >&2
    printf '  sudo %s\n' "$0" >&2
    exit 1
fi

if [ ! -d /usr/share/cockpit ] && ! command -v cockpit-bridge >/dev/null 2>&1; then
    printf '%s\n' "Error: Cockpit is not installed." >&2
    printf '%s\n' "Install Cockpit before running the combined update." >&2
    exit 1
fi

if [ ! -d "$INSTALL_DIR/.venv" ]; then
    printf 'Error: Existing WebZFS installation not found at %s.\n' "$INSTALL_DIR" >&2
    printf '%s\n' "Use sudo ./install_linux_cockpit.sh for the initial installation." >&2
    exit 1
fi

for file in update_linux.sh integrations/cockpit/install.sh; do
    if [ ! -f "$SOURCE_DIR/$file" ]; then
        printf 'Error: Required file is missing: %s\n' "$SOURCE_DIR/$file" >&2
        printf '%s\n' "Run this script from the WebZFS project source tree." >&2
        exit 1
    fi
done

printf '%s\n' "========================================"
printf '%s\n' "WebZFS and Cockpit Update"
printf '%s\n' "========================================"
printf '\n'

"$SOURCE_DIR/update_linux.sh"

COCKPIT_INSTALLER="$SOURCE_DIR/integrations/cockpit/install.sh"
if [ ! -f "$COCKPIT_INSTALLER" ]; then
    printf 'Error: Cockpit installer not found at %s\n' "$COCKPIT_INSTALLER" >&2
    exit 1
fi

printf '\n%s\n' "Updating the WebZFS Cockpit package..."
WEBZFS_ASSET_DIR="$INSTALL_DIR" /bin/sh "$COCKPIT_INSTALLER"

printf '\n%s\n' "========================================"
printf '%s\n' "WebZFS and Cockpit update complete"
printf '%s\n' "========================================"
printf '%s\n' "Reload the Cockpit browser page to use the updated WebZFS integration."