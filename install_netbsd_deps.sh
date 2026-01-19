#!/bin/sh
# This script is a WIP to install the dependencies for WebZFS on NetBSD
set -e

# 1. Install System Packages
echo ">> Installing packages..."
pkgin -y install python311 py311-pip py311-pydantic nodejs smartmontools \
                 git perl mbuffer lzop pv mozilla-rootcerts \
                 p5-Config-IniFiles p5-Capture-Tiny \
                 gmake libsodium curl pkg-config openssl

ln -s /usr/pkg/bin/python3.11 /usr/pkg/bin/python3
ln -s /usr/pkg/bin/pip3.11 /usr/pkg/bin/pip

# Create symlinks for OpenSSL libraries so they can be found by Python packages
# This is needed for cryptography package (used by paramiko)
if [ -f /usr/pkg/lib/libssl.so.3 ] && [ ! -f /usr/lib/libssl.so.3 ]; then
    ln -s /usr/pkg/lib/libssl.so.3 /usr/lib/libssl.so.3
fi
if [ -f /usr/pkg/lib/libcrypto.so.3 ] && [ ! -f /usr/lib/libcrypto.so.3 ]; then
    ln -s /usr/pkg/lib/libcrypto.so.3 /usr/lib/libcrypto.so.3
fi

# 2. Install Rust via rustup (pkgsrc rust package has issues on some systems)
echo ">> Installing Rust via rustup..."
if ! command -v rustc >/dev/null 2>&1 || ! rustc --version >/dev/null 2>&1; then
    echo "Installing rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
    # Source cargo env for current session
    . "$HOME/.cargo/env" 2>/dev/null || true
    # Set default toolchain
    rustup default stable
    echo "Rust installed via rustup"
else
    echo "Rust already installed and working"
fi

# 2. SSL Setup (Required for Git)
if [ -x /usr/pkg/sbin/mozilla-rootcerts ]; then
    /usr/pkg/sbin/mozilla-rootcerts install
fi

# 3. Sanoid Setup
INSTALL_DIR="/opt/sanoid"
if [ ! -d "$INSTALL_DIR" ]; then
    echo ">> Cloning Sanoid..."
    git clone https://github.com/jimsalterjrs/sanoid.git "$INSTALL_DIR"
    cd "$INSTALL_DIR"
    # Use latest stable tag
    git checkout $(git describe --abbrev=0 --tags)
fi

# Link binaries
ln -sf "$INSTALL_DIR/sanoid" /usr/pkg/bin/sanoid
ln -sf "$INSTALL_DIR/syncoid" /usr/pkg/bin/syncoid
chmod +x "$INSTALL_DIR/sanoid" "$INSTALL_DIR/syncoid"

# Config setup
mkdir -p /etc/sanoid
if [ ! -f /etc/sanoid/sanoid.conf ]; then
    cp "$INSTALL_DIR/sanoid.defaults.conf" /etc/sanoid/sanoid.conf
fi

# 4. Enable ZFS
echo ">> Enabling ZFS..."
if ! grep -q "zfs=YES" /etc/rc.conf; then
    echo "zfs=YES" >> /etc/rc.conf
fi
if [ ! -f /etc/modules.conf ] || ! grep -q "zfs" /etc/modules.conf; then
    echo "zfs" >> /etc/modules.conf
fi

echo "Done! Run 'modload zfs' and 'service zfs start' to begin."
