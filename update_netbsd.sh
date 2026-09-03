#!/bin/sh

# WebZFS Update Script for NetBSD
# This script updates an existing WebZFS installation at /opt/webzfs
# For initial installation, use install_netbsd.sh instead
#
# Uses pre-compiled wheels from https://github.com/webzfs/webzfs-wheels
#
# The script reads /opt/webzfs/.install-platform (written by the installer)
# and rebuilds the virtual environment when the NetBSD release, architecture,
# or Python ABI has changed (for example after a NetBSD 10.1 to 11.0 upgrade
# or the move from Python 3.11 to 3.12). If the marker file is absent, the
# venv is rebuilt once for consistency.

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
LOG_FILE="${INSTALL_DIR}/update_log.txt"
WHEELS_BASE_DIR="${INSTALL_DIR}/.wheels"
PLATFORM_MARKER="${INSTALL_DIR}/.install-platform"

# GitHub raw URL base for pre-compiled wheels
WHEELS_REPO_BASE="https://github.com/webzfs/webzfs-wheels/raw/main/wheelhouse"

# Python ABI target for pre-compiled wheels
PYTHON_PKG_VERSION="312"
PYTHON_TAG="cp312"

# Wheel packages to download (these require compilation without pre-built wheels)
# Versions must match the pins in requirements.txt.
WHEEL_PACKAGES="cryptography-49.0.0 markupsafe-3.0.3 psutil-7.2.2 pydantic_core-2.46.4 bcrypt-5.0.0 cffi-2.1.0 pynacl-1.6.2"

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

# Function to find Python
# Prefer python3.12 to match the cp312 pre-compiled wheels.
find_python() {
    for py in python3.12 python3.13 python3; do
        if command_exists "$py"; then
            echo "$py"
            return 0
        fi
    done
    return 1
}

# Function to detect NetBSD release/architecture and select the wheel set.
# Fails clearly on unsupported combinations instead of silently using
# wheels built for another release.
detect_netbsd_platform() {
    NETBSD_RELEASE=$(uname -r | cut -d_ -f1)
    NETBSD_MAJOR=$(echo "$NETBSD_RELEASE" | cut -d. -f1)
    NETBSD_MINOR=$(echo "$NETBSD_RELEASE" | cut -d. -f2)
    NETBSD_ARCH=$(uname -m)

    echo "Detected NetBSD ${NETBSD_RELEASE} (${NETBSD_ARCH})"

    case "${NETBSD_MAJOR}.${NETBSD_MINOR}:${NETBSD_ARCH}" in
        10.1:amd64)
            WHEEL_SUBDIR="netbsd10-1"
            WHEEL_PLATFORM="netbsd_10_1_amd64"
            ;;
        11.0:amd64)
            WHEEL_SUBDIR="netbsd11-0"
            WHEEL_PLATFORM="netbsd_11_0_amd64"
            ;;
        *)
            printf "${RED}Error: No pre-compiled wheel set exists for NetBSD ${NETBSD_RELEASE} on ${NETBSD_ARCH}${NC}\n"
            echo "Supported platforms: NetBSD 10.1 amd64, NetBSD 11.0 amd64"
            exit 1
            ;;
    esac

    printf "${GREEN}✓${NC} Using wheel set: ${WHEEL_SUBDIR}\n"
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

# Detect the current platform and choose the matching wheel set
detect_netbsd_platform
WHEELS_DIR="${WHEELS_BASE_DIR}/${WHEEL_SUBDIR}-${PYTHON_TAG}-${NETBSD_ARCH}"
echo

# Verify Python 3.12 is available (required for the cp312 wheel set)
PYTHON_CMD=$(find_python)
if [ -z "$PYTHON_CMD" ]; then
    printf "${RED}Error: Python 3 is not installed${NC}\n"
    echo "Please install Python 3.12 first: pkgin install python312 py312-pip"
    exit 1
fi

PYTHON_PATH=$(command -v "$PYTHON_CMD")
PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
printf "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)\n"

if [ "$PYTHON_VERSION" != "3.12" ]; then
    printf "${YELLOW}Warning:${NC} Pre-compiled wheels target Python 3.12 (${PYTHON_TAG}).\n"
    echo "  WebZFS now uses Python 3.12 on NetBSD. Install it with:"
    echo "    pkgin install python312 py312-pip"
    echo "  Continuing with Python ${PYTHON_VERSION} will force source compilation"
    echo "  of native packages, which requires a Rust toolchain."
    printf "Do you want to continue anyway? (y/n): "
    read -r REPLY
    if [ "$REPLY" != "y" ] && [ "$REPLY" != "Y" ]; then
        echo "Update aborted."
        exit 1
    fi
fi

# Determine whether the venv must be rebuilt.
# A rebuild is required when the platform marker is missing (legacy
# install), or when the recorded NetBSD release, architecture, or Python
# tag differs from the current system. Native extension modules built
# for another OS release or Python ABI are not safe to reuse.
REBUILD_VENV=false
if [ ! -f "$PLATFORM_MARKER" ]; then
    printf "${YELLOW}Note:${NC} No installation platform marker found (legacy installation).\n"
    echo "  The virtual environment will be rebuilt for consistency."
    REBUILD_VENV=true
else
    MARKER_RELEASE=$(grep '^release=' "$PLATFORM_MARKER" | cut -d= -f2)
    MARKER_ARCH=$(grep '^arch=' "$PLATFORM_MARKER" | cut -d= -f2)
    MARKER_PYTHON_TAG=$(grep '^python_tag=' "$PLATFORM_MARKER" | cut -d= -f2)
    if [ "$MARKER_RELEASE" != "$NETBSD_RELEASE" ] || \
       [ "$MARKER_ARCH" != "$NETBSD_ARCH" ] || \
       [ "$MARKER_PYTHON_TAG" != "$PYTHON_TAG" ]; then
        printf "${YELLOW}Platform transition detected:${NC}\n"
        echo "  Previous: NetBSD ${MARKER_RELEASE} ${MARKER_ARCH} ${MARKER_PYTHON_TAG}"
        echo "  Current:  NetBSD ${NETBSD_RELEASE} ${NETBSD_ARCH} ${PYTHON_TAG}"
        echo "  The virtual environment will be rebuilt."
        REBUILD_VENV=true
    fi
fi

# Sanity check: the existing venv Python version must match the selected
# interpreter, otherwise rebuild regardless of the marker.
if [ "$REBUILD_VENV" = "false" ] && [ -x "${VENV_DIR}/bin/python3" ]; then
    VENV_PYTHON_VERSION=$("${VENV_DIR}/bin/python3" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null || true)
    if [ "$VENV_PYTHON_VERSION" != "$PYTHON_VERSION" ]; then
        printf "${YELLOW}Note:${NC} Existing venv uses Python '${VENV_PYTHON_VERSION}' but Python ${PYTHON_VERSION} is selected.\n"
        echo "  The virtual environment will be rebuilt."
        REBUILD_VENV=true
    fi
fi

echo

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
    --exclude='config/gunicorn.conf.py' \
    --exclude='./install_linux.sh' \
    --exclude='./install_freebsd.sh' \
    --exclude='./install_netbsd.sh' \
    --exclude='./install_linux_cockpit.sh' \
    --exclude='./update_linux.sh' \
    --exclude='./update_freebsd.sh' \
    --exclude='./update_netbsd.sh' \
    --exclude='./update_linux_cockpit.sh' \
    --exclude='./integrations/cockpit/install.sh' \
    .) | (cd "$INSTALL_DIR" && tar xf -)

# Remove root-level install/update entry points left by older installations.
for script_path in "${INSTALL_DIR}"/install*.sh "${INSTALL_DIR}"/update*.sh; do
    if [ -f "${script_path}" ]; then
        unlink "${script_path}"
    fi
done
for script_path in "${INSTALL_DIR}/integrations/cockpit/install.sh" \
    "${INSTALL_DIR}/templates/install_omnios.sh"; do
    if [ -f "${script_path}" ]; then
        unlink "${script_path}"
    fi
done

printf "${GREEN}✓${NC} Application files updated\n"
echo

# Pre-create JSON data files that newer versions introduced.
#
# FileStorageService and SMARTMonitoringService create these on first
# import if missing, but several Gunicorn workers import at the same
# moment during a restart and can race each other writing the same new
# file. Creating them here, before the service is restarted, means the
# workers only ever read files that already exist. This mirrors the same
# block in install_netbsd.sh.
DATA_DIR="${INSTALL_DIR}/.config/webzfs"
mkdir -p "${DATA_DIR}/progress"
mkdir -p "${DATA_DIR}/logs"

# Storage service files
if [ ! -f "${DATA_DIR}/replication_history.json" ]; then
    echo '{"executions": [], "next_id": 1}' > "${DATA_DIR}/replication_history.json"
fi
if [ ! -f "${DATA_DIR}/notification_log.json" ]; then
    echo '{"notifications": []}' > "${DATA_DIR}/notification_log.json"
fi
if [ ! -f "${DATA_DIR}/syncoid_jobs.json" ]; then
    echo '{"jobs": [], "next_id": 1}' > "${DATA_DIR}/syncoid_jobs.json"
fi
if [ ! -f "${DATA_DIR}/scrub_schedules.json" ]; then
    echo '{"schedules": [], "next_id": 1}' > "${DATA_DIR}/scrub_schedules.json"
fi

# SMART monitoring service files
if [ ! -f "${DATA_DIR}/smart_test_history.json" ]; then
    echo '{"history": []}' > "${DATA_DIR}/smart_test_history.json"
fi
if [ ! -f "${DATA_DIR}/smart_scheduled_tests.json" ]; then
    echo '{}' > "${DATA_DIR}/smart_scheduled_tests.json"
fi

# Health analysis service files
if [ ! -f "${DATA_DIR}/health_reports.json" ]; then
    echo '{"reports": []}' > "${DATA_DIR}/health_reports.json"
fi
if [ ! -f "${DATA_DIR}/health_schedules.json" ]; then
    echo '{"schedules": [], "next_id": 1}' > "${DATA_DIR}/health_schedules.json"
fi

# /root/.config/webzfs is deliberately not seeded. WebZFS runs as root
# here, but the rc.d wrapper, run.sh, and the scheduled task crontab
# entries all export HOME=/opt/webzfs, so the files above are the ones
# actually read.

printf "${GREEN}✓${NC} Data files verified\n"
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

# Rebuild the virtual environment if a platform/ABI transition was detected
if [ "$REBUILD_VENV" = "true" ]; then
    echo "Rebuilding virtual environment with Python ${PYTHON_VERSION}..."
    rm -rf "$VENV_DIR"
    (cd "$INSTALL_DIR" && $PYTHON_PATH -m venv .venv)
    printf "${GREEN}✓${NC} Virtual environment recreated\n"

    # Clear stale wheel caches from other platforms/ABIs. Old unqualified
    # caches lived directly in .wheels; platform-qualified caches live in
    # subdirectories. Remove any that do not match the current platform.
    if [ -d "$WHEELS_BASE_DIR" ]; then
        echo "Clearing stale wheel caches..."
        for entry in "$WHEELS_BASE_DIR"/*; do
            [ -e "$entry" ] || continue
            if [ "$entry" != "$WHEELS_DIR" ]; then
                rm -rf "$entry"
            fi
        done
        printf "${GREEN}✓${NC} Stale wheel caches removed\n"
    fi
    echo
fi

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

    # Determine ABI tag - cryptography publishes abi3 wheels tagged
    # cp311-abi3 (compatible with 3.11+), others use cp312-cp312
    if [ "$pkg_name" = "cryptography" ]; then
        ABI_TAG="cp311-abi3"
    else
        ABI_TAG="${PYTHON_TAG}-${PYTHON_TAG}"
    fi

    wheel_filename="${wheel_pkg_name}-${version}-${ABI_TAG}-${WHEEL_PLATFORM}.whl"
    wheel_url="${WHEELS_URL}/${wheel_filename}"
    wheel_path="${WHEELS_DIR}/${wheel_filename}"

    # Discard any previously cached file that is not a valid wheel. Wheels
    # are zip archives and start with the PK magic bytes. A curl run
    # without -f can leave an HTML error page behind with a .whl name.
    if [ -f "$wheel_path" ] && ! head -c 2 "$wheel_path" 2>/dev/null | grep -q "PK"; then
        printf "  ${YELLOW}Removing invalid cached file for ${pkg_name}${NC}\n"
        rm -f "$wheel_path"
    fi

    if [ -f "$wheel_path" ]; then
        printf "  ${GREEN}✓${NC} ${pkg_name} wheel already cached\n"
    else
        printf "  Downloading ${pkg_name}..."
        # -f makes curl fail on HTTP errors (404) instead of saving the
        # error page as the wheel file.
        if curl -fsSL -o "$wheel_path" "$wheel_url" 2>/dev/null && head -c 2 "$wheel_path" 2>/dev/null | grep -q "PK"; then
            printf " ${GREEN}✓${NC}\n"
        else
            rm -f "$wheel_path"
            printf " ${RED}FAILED${NC}\n"
            printf "${YELLOW}Warning: Could not download wheel for ${pkg_name}${NC}\n"
            printf "  URL: ${wheel_url}\n"
            DOWNLOAD_FAILED=1
        fi
    fi
done

if [ "$DOWNLOAD_FAILED" -eq 1 ]; then
    printf "${YELLOW}Some wheels failed to download. Will attempt source compilation.${NC}\n"
else
    printf "${GREEN}✓${NC} All wheels available\n"
fi

# Verify the wheel platform tags match this system's local platform tag.
# If they differ (for example a patch-level difference in the release
# string), rename the wheel files so pip accepts them on this system.
LOCAL_PLATFORM=$($PYTHON_PATH -c "import sysconfig; print(sysconfig.get_platform().replace('.', '_').replace('-', '_'))")
if [ "$LOCAL_PLATFORM" != "$WHEEL_PLATFORM" ]; then
    printf "${YELLOW}Note:${NC} Local platform '${LOCAL_PLATFORM}' differs from wheel platform '${WHEEL_PLATFORM}'\n"
    printf "Adapting wheel filenames for local platform compatibility...\n"
    for whl in "$WHEELS_DIR"/*-"${WHEEL_PLATFORM}".whl; do
        if [ -f "$whl" ]; then
            new_whl=$(echo "$whl" | sed "s/${WHEEL_PLATFORM}/${LOCAL_PLATFORM}/")
            if [ ! -f "$new_whl" ]; then
                cp "$whl" "$new_whl"
            fi
        fi
    done
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
if ! .venv/bin/pip install --find-links="$WHEELS_DIR" -r requirements.txt >> update_log.txt 2>&1; then
    printf "${RED}Error: Failed to update Python dependencies${NC}\n"
    echo "Check ${INSTALL_DIR}/update_log.txt for details"
    echo
    echo "Common causes:"
    echo "  - Pre-compiled wheels not yet published for this platform"
    echo "    (pip then tries source compilation, which requires Rust)"
    echo "  - Network connectivity issues (pure-Python packages download from PyPI)"
    exit 1
fi

echo "Updating Node.js dependencies..."
npm install >> update_log.txt 2>&1

echo "Rebuilding static assets..."
npm run build:css >> update_log.txt 2>&1

echo
printf "${GREEN}✓${NC} Python dependencies updated\n"
printf "${GREEN}✓${NC} Node.js dependencies updated\n"
printf "${GREEN}✓${NC} Static assets rebuilt\n"
echo

# Write/refresh installation platform marker
cat > "$PLATFORM_MARKER" << MARKER_EOF
os=NetBSD
release=${NETBSD_RELEASE}
arch=${NETBSD_ARCH}
python=${PYTHON_VERSION}
python_tag=${PYTHON_TAG}
wheel_platform=${WHEEL_PLATFORM}
wheel_set=${WHEEL_SUBDIR}
MARKER_EOF
printf "${GREEN}✓${NC} Installation platform marker written to ${PLATFORM_MARKER}\n"
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
