#!/bin/bash

# Install WebZFS, then install the Cockpit integration.

set -euo pipefail

INSTALL_DIR="/opt/webzfs"
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ "$#" -ne 0 ]; then
    printf '%s\n' "Usage: sudo ./install_linux_cockpit.sh" >&2
    exit 2
fi

if [ "$(uname -s)" != "Linux" ]; then
    printf '%s\n' "Error: This installer is Linux-only." >&2
    exit 1
fi

if [ "$EUID" -ne 0 ]; then
    printf '%s\n' "Error: Run this script as root:" >&2
    printf '  sudo %s\n' "$0" >&2
    exit 1
fi

if [ ! -d /usr/share/cockpit ] && ! command -v cockpit-bridge >/dev/null 2>&1; then
    printf '%s\n' "Error: Cockpit is not installed." >&2
    printf '%s\n' "Install Cockpit before running the combined setup." >&2
    exit 1
fi

for file in install_linux.sh integrations/cockpit/install.sh; do
    if [ ! -f "$SOURCE_DIR/$file" ]; then
        printf 'Error: Required file is missing: %s\n' "$SOURCE_DIR/$file" >&2
        printf '%s\n' "Run this script from the WebZFS project source tree." >&2
        exit 1
    fi
done

printf '%s\n' "========================================"
printf '%s\n' "WebZFS and Cockpit Installation"
printf '%s\n' "========================================"
printf '\n'

"$SOURCE_DIR/install_linux.sh"

COCKPIT_INSTALLER="$SOURCE_DIR/integrations/cockpit/install.sh"
if [ ! -f "$COCKPIT_INSTALLER" ]; then
    printf 'Error: Cockpit installer not found at %s\n' "$COCKPIT_INSTALLER" >&2
    exit 1
fi

printf '\n%s\n' "Installing the WebZFS Cockpit package..."
WEBZFS_ASSET_DIR="$INSTALL_DIR" /bin/sh "$COCKPIT_INSTALLER"

printf '\n%s\n' "========================================"
printf '%s\n' "WebZFS and Cockpit installation complete"
printf '%s\n' "========================================"
printf '%s\n' "Reload the Cockpit browser page to see the WebZFS sidebar entry."