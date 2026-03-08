#!/bin/sh

# WebZFS Update Script for NetBSD
# This script updates an existing WebZFS installation at /opt/webzfs
# For initial installation, use install_netbsd.sh instead
#
# Uses pre-compiled wheels from https://github.com/webzfs/webzfs-wheels

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
LOG_FILE="${INSTALL_DIR}/update_log.txt"
WHEELS_DIR="${INSTALL_DIR}/.wheels"

# GitHub raw URL base for pre-compiled wheels
WHEELS_REPO_BASE="https://github.com/webzfs/webzfs-wheels/raw/main/wheelhouse"

# NetBSD 10.x wheel configuration
WHEEL_SUBDIR="netbsd10-0"
WHEEL_PLATFORM="netbsd_10_1_amd64"

# Wheel packages to download (these require compilation without pre-built wheels)
WHEEL_PACKAGES="cryptography-44.0.0 markupsafe-3.0.3 psutil-7.1.3 pydantic_core-2.41.5"

# Determine the source directory (where this script is located)
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

echo "========================================"
echo "WebZFS Update Script for NetBSD"
echo "========================================"
echo

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then 
    printf "${RED}Error: This script must be run as root${NC}\n"
    echo "Please run: sudo $0"
    exit 1
fi

# Verify installation exists
if [ ! -d "$INSTALL_DIR" ]; then
    printf "${RED}Error: WebZFS installation not found at $INSTALL_DIR${NC}\n"
    echo "Please run install_netbsd.sh for initial installation"
    exit 1
fi

# Verify virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    printf "${RED}Error: Virtual environment not found at $VENV_DIR${NC}\n"
    echo "Please run install_netbsd.sh for initial installation"
    exit 1
fi

# Verify essential files exist in source directory
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SOURCE_DIR}/${file}" ]; then
        printf "${RED}Error: Essential file '${file}' not found in ${SOURCE_DIR}${NC}\n"
        echo "Please run this script from the WebZFS source directory containing all application files."
        exit 1
    fi
done

# Verify rc.d script exists
if [ ! -f "/etc/rc.d/webzfs" ]; then
    printf "${RED}Error: rc.d service script not found${NC}\n"
    echo "Please run install_netbsd.sh for initial installation"
    exit 1
fi

# Check if service is running
SERVICE_WAS_RUNNING=false
if /etc/rc.d/webzfs status >/dev/null 2>&1; then
    SERVICE_WAS_RUNNING=true
    echo "Stopping WebZFS service..."
    /etc/rc.d/webzfs stop
    printf "${GREEN}✓${NC} Service stopped\n"
fi

echo

# Copy application files to installation directory (preserving config)
echo "Updating application files from $SOURCE_DIR to $INSTALL_DIR..."

# Use tar to copy files, excluding runtime/config data
(cd "$SOURCE_DIR" && tar cf - \
    --exclude='.venv' \
    --exclude='node_modules' \
    --exclude='.git' \
    --exclude='*.log' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='.env' \
    --exclude='.config' \
    --exclude='.wheels' \
    .) | (cd "$INSTALL_DIR" && tar xf -)

printf "${GREEN}✓${NC} Application files updated\n"
echo

# Update CAPTION in .env from .env.example
ENV_FILE="${INSTALL_DIR}/.env"
if [ -f "$ENV_FILE" ]; then
    # Extract new CAPTION from .env.example
    NEW_CAPTION=$(grep -E '^CAPTION=' "${SOURCE_DIR}/.env.example" | head -1)
    if [ -n "$NEW_CAPTION" ]; then
        # Update CAPTION in existing .env file
        if grep -q '^CAPTION=' "$ENV_FILE"; then
            # NetBSD sed does not support -i, use a temp file
            sed "s|^CAPTION=.*|${NEW_CAPTION}|" "$ENV_FILE" > "${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
            printf "${GREEN}✓${NC} Updated CAPTION to: ${NEW_CAPTION}\n"
        else
            # CAPTION not found in .env, add it at the top
            printf '%s\n' "${NEW_CAPTION}" | cat - "$ENV_FILE" > "${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
            printf "${GREEN}✓${NC} Added CAPTION: ${NEW_CAPTION}\n"
        fi
    fi
fi

echo

# Download/update pre-compiled wheels
echo "Downloading pre-compiled wheels..."
mkdir -p "$WHEELS_DIR"

WHEELS_URL="${WHEELS_REPO_BASE}/${WHEEL_SUBDIR}"
DOWNLOAD_FAILED=0

for pkg_version in $WHEEL_PACKAGES; do
    # Extract package name and version
    pkg_name=$(echo "$pkg_version" | sed 's/-[0-9].*//')
    version=$(echo "$pkg_version" | sed 's/.*-//')
    wheel_pkg_name=$(echo "$pkg_name" | tr '-' '_')

    # Determine ABI tag - cryptography uses cp37-abi3, others use cp311-cp311
    if [ "$pkg_name" = "cryptography" ]; then
        ABI_TAG="cp37-abi3"
    else
        ABI_TAG="cp311-cp311"
    fi

    wheel_filename="${wheel_pkg_name}-${version}-${ABI_TAG}-${WHEEL_PLATFORM}.whl"
    wheel_url="${WHEELS_URL}/${wheel_filename}"
    wheel_path="${WHEELS_DIR}/${wheel_filename}"

    if [ -f "$wheel_path" ]; then
        printf "  ${GREEN}✓${NC} ${pkg_name} wheel already cached\n"
    else
        printf "  Downloading ${pkg_name}..."
        if curl -sL -o "$wheel_path" "$wheel_url" 2>/dev/null; then
            printf " ${GREEN}✓${NC}\n"
        else
            printf " ${RED}FAILED${NC}\n"
            printf "${YELLOW}Warning: Could not download wheel for ${pkg_name}${NC}\n"
            DOWNLOAD_FAILED=1
        fi
    fi
done

if [ "$DOWNLOAD_FAILED" -eq 1 ]; then
    printf "${YELLOW}Some wheels failed to download. Will attempt source compilation.${NC}\n"
else
    printf "${GREEN}✓${NC} All wheels available\n"
fi

echo

# Update dependencies
echo "Updating Python and Node.js dependencies..."
echo "(This may take a few minutes...)"
echo

cd "$INSTALL_DIR"

# Set environment for building (in case any packages need source compilation)
export HOME="$INSTALL_DIR"

# Check for gmake
if command_exists gmake; then
    export MAKE=$(command -v gmake)
fi

# Set OpenSSL location for cryptography package on NetBSD
export OPENSSL_DIR="/usr/pkg"
export PKG_CONFIG_PATH="/usr/pkg/lib/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure Rust toolchain is in PATH (fallback for source compilation)
if [ -f "/root/.cargo/env" ]; then
    . "/root/.cargo/env"
elif [ -d "/root/.cargo/bin" ]; then
    export PATH="/root/.cargo/bin:$PATH"
fi

echo "Upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > update_log.txt 2>&1

echo "Updating Python dependencies (using pre-compiled wheels)..."
.venv/bin/pip install --find-links="$WHEELS_DIR" -r requirements.txt >> update_log.txt 2>&1

echo "Updating Node.js dependencies..."
npm install >> update_log.txt 2>&1

echo "Rebuilding static assets..."
npm run build:css >> update_log.txt 2>&1

echo
printf "${GREEN}✓${NC} Python dependencies updated\n"
printf "${GREEN}✓${NC} Node.js dependencies updated\n"
printf "${GREEN}✓${NC} Static assets rebuilt\n"
echo

# Restart service if it was running
if [ "$SERVICE_WAS_RUNNING" = true ]; then
    echo "Restarting WebZFS service..."
    /etc/rc.d/webzfs start
    printf "${GREEN}✓${NC} Service restarted\n"
    echo
else
    echo "WebZFS service was not running before update."
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY
    if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
        /etc/rc.d/webzfs start
        printf "${GREEN}✓${NC} WebZFS service started\n"
    fi
fi

echo
echo "========================================"
printf "${GREEN}Update Complete!${NC}\n"
echo "========================================"
echo
echo "WebZFS has been updated at: $INSTALL_DIR"
echo
echo "To check the service status:"
echo "  /etc/rc.d/webzfs status"
echo
echo "To view logs:"
echo "  tail -f $INSTALL_DIR/gunicorn.log"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
