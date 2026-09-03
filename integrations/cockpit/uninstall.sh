#!/bin/sh

set -eu

PACKAGE_DIR="/usr/share/cockpit/webzfs"

if [ "$(uname -s)" != "Linux" ]; then
    printf '%s\n' "Error: The WebZFS Cockpit integration is Linux-only." >&2
    exit 1
fi

if [ "$(id -u)" -ne 0 ]; then
    printf '%s\n' "Error: Run this uninstaller as root." >&2
    exit 1
fi

if [ ! -d "$PACKAGE_DIR" ]; then
    printf '%s\n' "The WebZFS Cockpit integration is not installed."
    exit 0
fi

printf '%s\n' "Delete $PACKAGE_DIR to uninstall the WebZFS Cockpit integration."
printf '%s\n' "No Cockpit or WebZFS configuration files need to be changed."