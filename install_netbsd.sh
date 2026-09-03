#!/bin/sh

# WebZFS Installation Script for NetBSD
# This script installs WebZFS to /opt/webzfs
# On NetBSD, the application runs as root due to PAM authentication requirements
#
# Uses pre-compiled wheels from https://github.com/webzfs/webzfs-wheels
# to avoid needing Rust/C compilation during installation.

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
ENV_FILE="${INSTALL_DIR}/.env"
LOG_FILE="${INSTALL_DIR}/install_log.txt"
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

echo "========================================"
echo "WebZFS Installation Script for NetBSD"
echo "========================================"
echo

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then 
    printf "${RED}Error: This script must be run as root${NC}\n"
    echo "Please run: sudo $0"
    exit 1
fi

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
            echo "Installation on other releases/architectures requires building"
            echo "the native Python packages from source, which is not automated"
            echo "by this installer."
            exit 1
            ;;
    esac

    printf "${GREEN}✓${NC} Using wheel set: ${WHEEL_SUBDIR}\n"
}

# Parse command line arguments
SKIP_DEPS=false
DEPS_ONLY=false

while [ $# -gt 0 ]; do
    case "$1" in
        --skip-deps)
            SKIP_DEPS=true
            shift
            ;;
        --deps-only)
            DEPS_ONLY=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo
            echo "Options:"
            echo "  --skip-deps    Skip dependency installation (use if deps already installed)"
            echo "  --deps-only    Only install dependencies, skip WebZFS installation"
            echo "  --help, -h     Show this help message"
            echo
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# ============================================================
# PHASE 1: DEPENDENCY INSTALLATION
# ============================================================

install_dependencies() {
    echo "========================================"
    echo "Phase 1: Installing System Dependencies"
    echo "========================================"
    echo

    # Check for pkgin
    if ! command_exists pkgin; then
        printf "${RED}Error: pkgin is not installed${NC}\n"
        echo "Please install pkgin first to manage packages"
        exit 1
    fi

    # Install System Packages
    echo "Installing system packages via pkgin..."
    pkgin -y install python312 py312-pip nodejs smartmontools \
                     git perl mbuffer lzop pvs mozilla-rootcerts \
                     p5-Config-IniFiles p5-Capture-Tiny \
                     gmake libsodium curl pkg-config openssl

    # Create Python symlinks if they don't exist
    if [ ! -f /usr/pkg/bin/python3 ]; then
        if [ -f /usr/pkg/bin/python3.12 ]; then
            ln -sf /usr/pkg/bin/python3.12 /usr/pkg/bin/python3
            printf "${GREEN}✓${NC} Created python3 symlink\n"
        fi
    fi

    if [ ! -f /usr/pkg/bin/pip ] && [ ! -f /usr/pkg/bin/pip3 ]; then
        if [ -f /usr/pkg/bin/pip3.12 ]; then
            ln -sf /usr/pkg/bin/pip3.12 /usr/pkg/bin/pip
            printf "${GREEN}✓${NC} Created pip symlink\n"
        fi
    fi

    # Create symlinks for OpenSSL libraries so they can be found by Python packages
    # This is needed for cryptography package (used by paramiko)
    if [ -f /usr/pkg/lib/libssl.so.3 ] && [ ! -f /usr/lib/libssl.so.3 ]; then
        ln -s /usr/pkg/lib/libssl.so.3 /usr/lib/libssl.so.3
        printf "${GREEN}✓${NC} Created libssl symlink\n"
    fi
    if [ -f /usr/pkg/lib/libcrypto.so.3 ] && [ ! -f /usr/lib/libcrypto.so.3 ]; then
        ln -s /usr/pkg/lib/libcrypto.so.3 /usr/lib/libcrypto.so.3
        printf "${GREEN}✓${NC} Created libcrypto symlink\n"
    fi

    printf "${GREEN}✓${NC} System packages installed\n"
    echo

    # Rust is intentionally not installed here. Pre-compiled wheels make a
    # Rust toolchain unnecessary for a normal installation. If wheel
    # download fails later, the installer offers a source-build fallback
    # that installs Rust via rustup at that point.

    # SSL Setup (Required for Git)
    echo "Setting up SSL certificates..."
    if [ -x /usr/pkg/sbin/mozilla-rootcerts ]; then
        /usr/pkg/sbin/mozilla-rootcerts install 2>/dev/null || true
        printf "${GREEN}✓${NC} SSL certificates configured\n"
    fi
    echo

    # Sanoid Setup
    echo "Setting up Sanoid..."
    SANOID_DIR="/opt/sanoid"
    if [ ! -d "$SANOID_DIR" ]; then
        echo "Cloning Sanoid repository..."
        git clone https://github.com/jimsalterjrs/sanoid.git "$SANOID_DIR"
        cd "$SANOID_DIR"
        # Use latest stable tag
        git checkout $(git describe --abbrev=0 --tags)
        cd - >/dev/null
    fi

    # Link binaries
    ln -sf "$SANOID_DIR/sanoid" /usr/pkg/bin/sanoid
    ln -sf "$SANOID_DIR/syncoid" /usr/pkg/bin/syncoid
    chmod +x "$SANOID_DIR/sanoid" "$SANOID_DIR/syncoid"

    # Config setup
    mkdir -p /etc/sanoid
    if [ ! -f /etc/sanoid/sanoid.conf ]; then
        cp "$SANOID_DIR/sanoid.defaults.conf" /etc/sanoid/sanoid.conf
    fi

    printf "${GREEN}✓${NC} Sanoid configured\n"
    echo

    # Enable ZFS
    echo "Configuring ZFS..."
    if ! grep -q "zfs=YES" /etc/rc.conf 2>/dev/null; then
        echo "zfs=YES" >> /etc/rc.conf
        printf "${GREEN}✓${NC} ZFS enabled in rc.conf\n"
    else
        printf "${GREEN}✓${NC} ZFS already enabled in rc.conf\n"
    fi

    if [ ! -f /etc/modules.conf ] || ! grep -q "zfs" /etc/modules.conf 2>/dev/null; then
        echo "zfs" >> /etc/modules.conf
        printf "${GREEN}✓${NC} ZFS added to modules.conf\n"
    else
        printf "${GREEN}✓${NC} ZFS already in modules.conf\n"
    fi
    echo

    echo "========================================"
    printf "${GREEN}Dependencies Installation Complete!${NC}\n"
    echo "========================================"
    echo
    echo "Note: If ZFS module is not loaded, run:"
    echo "  modload zfs"
    echo "  service zfs start"
    echo
}

# Run dependency installation if not skipped
if [ "$SKIP_DEPS" = "false" ]; then
    install_dependencies
fi

# Exit if deps-only mode
if [ "$DEPS_ONLY" = "true" ]; then
    echo "Dependency installation complete. Exiting (--deps-only mode)."
    exit 0
fi

# ============================================================
# PHASE 2: WEBZFS APPLICATION INSTALLATION
# ============================================================

echo "========================================"
echo "Phase 2: Installing WebZFS Application"
echo "========================================"
echo

# Verify essential files exist in source directory
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SOURCE_DIR}/${file}" ]; then
        printf "${RED}Error: Essential file '${file}' not found in ${SOURCE_DIR}${NC}\n"
        echo "Please run this installer from the WebZFS source directory containing all application files."
        exit 1
    fi
done

# Check prerequisites
echo "Checking prerequisites..."

# Detect NetBSD release/architecture and select the wheel set
detect_netbsd_platform
WHEELS_DIR="${WHEELS_BASE_DIR}/${WHEEL_SUBDIR}-${PYTHON_TAG}-${NETBSD_ARCH}"
echo

PYTHON_CMD=$(find_python)
if [ -z "$PYTHON_CMD" ]; then
    printf "${RED}Error: Python 3 is not installed${NC}\n"
    echo "Please install Python 3.12 first (pkgin install python312 py312-pip)"
    exit 1
fi

PYTHON_PATH=$(command -v "$PYTHON_CMD")
PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
printf "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)\n"

if [ "$PYTHON_VERSION" != "3.12" ]; then
    printf "${YELLOW}Warning:${NC} Pre-compiled wheels target Python 3.12 (${PYTHON_TAG}).\n"
    echo "  Using Python ${PYTHON_VERSION} will force source compilation of native packages."
    echo "  Install Python 3.12 with: pkgin install python312 py312-pip"
fi

if ! command_exists node; then
    printf "${RED}Error: Node.js is not installed${NC}\n"
    echo "Please install Node.js first"
    exit 1
fi
printf "${GREEN}✓${NC} Node.js $(node --version) found\n"

if ! command_exists npm; then
    printf "${RED}Error: npm is not installed${NC}\n"
    echo "Please install npm first"
    exit 1
fi
printf "${GREEN}✓${NC} npm $(npm --version) found\n"

if ! command_exists rsync; then
    printf "${RED}Error: rsync is not installed${NC}\n"
    echo "Install it with: pkgin install rsync"
    exit 1
fi

printf "${GREEN}✓${NC} rsync found\n"

# Check for Rust (optional - only needed if pre-compiled wheels are unavailable)
# Try to source cargo env if rustup is installed
if [ -f "/root/.cargo/env" ]; then
    . "/root/.cargo/env"
elif [ -f "$HOME/.cargo/env" ]; then
    . "$HOME/.cargo/env"
fi

if command_exists rustc && rustc --version >/dev/null 2>&1; then
    printf "${GREEN}✓${NC} Rust $(rustc --version | cut -d' ' -f2) found (fallback compiler)\n"
else
    printf "${YELLOW}Note:${NC} Rust not found. Pre-compiled wheels will be used instead.\n"
    echo "  If wheel download fails, Rust will be needed. Install with: $0 --deps-only"
fi

# Check for gmake (optional - only needed for source compilation fallback)
if command_exists gmake; then
    GMAKE_PATH=$(command -v gmake)
    printf "${GREEN}✓${NC} gmake found (fallback compiler)\n"
else
    GMAKE_PATH=""
    printf "${YELLOW}Note:${NC} gmake not found. Pre-compiled wheels will be used instead.\n"
fi

echo

# Create installation directory if it doesn't exist
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Creating installation directory: $INSTALL_DIR"
    mkdir -p "$INSTALL_DIR"
fi

# Preserve user's gunicorn bind configuration if re-installing over existing installation
GUNICORN_CONF="${INSTALL_DIR}/config/gunicorn.conf.py"
SAVED_BIND_LINE=""
if [ -f "$GUNICORN_CONF" ]; then
    SAVED_BIND_LINE=$(grep -E '^\s*bind\s*=\s*f"' "$GUNICORN_CONF" 2>/dev/null || true)
fi

# Copy application files to installation directory
echo "Copying application files from $SOURCE_DIR to $INSTALL_DIR..."

# Use tar to copy files (portable method)
(cd "$SOURCE_DIR" && tar cf - --exclude='.venv' --exclude='node_modules' --exclude='.git' \
    --exclude='*.log' --exclude='__pycache__' --exclude='*.pyc' \
    --exclude='./install_linux.sh' --exclude='./install_freebsd.sh' \
    --exclude='./install_netbsd.sh' --exclude='./install_linux_cockpit.sh' \
    --exclude='./update_linux.sh' --exclude='./update_freebsd.sh' \
    --exclude='./update_netbsd.sh' --exclude='./update_linux_cockpit.sh' \
    --exclude='./integrations/cockpit/install.sh' .) | \
    (cd "$INSTALL_DIR" && tar xf -)

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

printf "${GREEN}✓${NC} Application files copied\n"

# Restore user's gunicorn bind configuration if it was customized
if [ -n "$SAVED_BIND_LINE" ]; then
    NEW_BIND_LINE=$(grep -E '^\s*bind\s*=\s*f"' "$GUNICORN_CONF" 2>/dev/null || true)
    if [ "$SAVED_BIND_LINE" != "$NEW_BIND_LINE" ]; then
        # User had a customized bind line, restore it
        # NetBSD sed does not support -i, use a temp file
        ESCAPED_OLD=$(printf '%s\n' "$NEW_BIND_LINE" | sed 's/[[\.*^$()+?{|]/\\&/g')
        ESCAPED_NEW=$(printf '%s\n' "$SAVED_BIND_LINE" | sed 's/[&/\]/\\&/g')
        sed "s|${ESCAPED_OLD}|${ESCAPED_NEW}|" "$GUNICORN_CONF" > "${GUNICORN_CONF}.tmp" && mv "${GUNICORN_CONF}.tmp" "$GUNICORN_CONF"
        printf "${YELLOW}!${NC} Preserved custom bind configuration: ${SAVED_BIND_LINE}\n"
    fi
fi

# Create application data directory and initialize data files
echo "Creating data directory structure..."
DATA_DIR="${INSTALL_DIR}/.config/webzfs"
mkdir -p "${DATA_DIR}/progress"
mkdir -p "${DATA_DIR}/logs"

# Pre-create JSON data files to avoid race conditions during worker startup
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

printf "${GREEN}✓${NC} Data directory and files created\n"

# Note: earlier versions also seeded /root/.config/webzfs here, because
# WebZFS runs as root on NetBSD and an unset HOME made
# FileStorageService fall back to root's home (see
# memory-bank/NETBSD_SCRUB_SCHEDULES_FIX.md). That workaround is no
# longer needed. Every way of starting WebZFS now sets HOME to the
# install prefix: the rc.d wrapper, run.sh, and the scheduled task
# crontab entries. Seeding two directories only invited the two from
# drifting apart, with the web interface and scheduled tasks reading
# different files.

# Download pre-compiled wheels
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
        printf "  ${GREEN}✓${NC} ${pkg_name} wheel already exists\n"
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
    echo
    printf "${YELLOW}Some wheels failed to download.${NC}\n"
    printf "The installer will attempt to compile these packages from source.\n"
    printf "This requires a Rust toolchain and build dependencies (gmake, etc.)\n"
    echo
    printf "Do you want to continue anyway? (y/n): "
    read -r REPLY
    if [ "$REPLY" != "y" ] && [ "$REPLY" != "Y" ]; then
        echo "Installation aborted."
        exit 1
    fi

    # Install Rust via rustup for the source-build fallback
    # (the pkgsrc rust package has Bus error issues on NetBSD)
    if [ -f "/root/.cargo/env" ]; then
        . "/root/.cargo/env"
    elif [ -f "$HOME/.cargo/env" ]; then
        . "$HOME/.cargo/env"
    fi
    if ! command_exists rustc || ! rustc --version >/dev/null 2>&1; then
        echo "Installing Rust via rustup (required for source compilation)..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
        . "/root/.cargo/env" 2>/dev/null || . "$HOME/.cargo/env" 2>/dev/null || true
        rustup default stable
        printf "${GREEN}✓${NC} Rust installed via rustup\n"
    fi
else
    printf "${GREEN}✓${NC} All wheels downloaded successfully\n"
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

# Install dependencies
echo "Installing Python and Node.js dependencies..."
echo "(This should be quick with pre-compiled wheels...)"
echo

cd "$INSTALL_DIR"

# Set environment for building (in case any packages need source compilation)
export HOME="$INSTALL_DIR"
if [ -n "$GMAKE_PATH" ]; then
    export MAKE="$GMAKE_PATH"
fi

# Set OpenSSL location for cryptography package on NetBSD
export OPENSSL_DIR="/usr/pkg"
export PKG_CONFIG_PATH="/usr/pkg/lib/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure Rust toolchain is in PATH for building Python packages (fallback)
if [ -f "/root/.cargo/env" ]; then
    . "/root/.cargo/env"
elif [ -d "/root/.cargo/bin" ]; then
    export PATH="/root/.cargo/bin:$PATH"
fi

# Ensure rustup has a default toolchain configured
if command_exists rustup; then
    rustup default stable >/dev/null 2>&1 || true
fi

# Create virtual environment
# Recreate it if an existing venv uses a different Python version (for
# example a cp311 venv from an older install), since native extension
# modules are not compatible across Python ABIs.
if [ -d ".venv" ]; then
    VENV_PYTHON_VERSION=""
    if [ -x ".venv/bin/python3" ]; then
        VENV_PYTHON_VERSION=$(.venv/bin/python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null || true)
    fi
    if [ "$VENV_PYTHON_VERSION" = "$PYTHON_VERSION" ]; then
        echo "Virtual environment already exists (Python ${VENV_PYTHON_VERSION})"
    else
        echo "Existing virtual environment uses Python '${VENV_PYTHON_VERSION}', recreating with Python ${PYTHON_VERSION}..."
        rm -rf .venv
        $PYTHON_PATH -m venv .venv
        printf "${GREEN}✓${NC} Virtual environment recreated\n"
    fi
else
    echo "Creating Python virtual environment..."
    $PYTHON_PATH -m venv .venv
fi

echo "Installing/upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > install_log.txt 2>&1

echo "Installing Python dependencies (using pre-compiled wheels)..."
if ! .venv/bin/pip install --find-links="$WHEELS_DIR" -r requirements.txt >> install_log.txt 2>&1; then
    printf "${RED}Error: Failed to install Python dependencies${NC}\n"
    echo "Check ${INSTALL_DIR}/install_log.txt for details"
    echo
    echo "Common causes:"
    echo "  - Pre-compiled wheels not yet published for this platform"
    echo "    (pip then tries source compilation, which requires Rust)"
    echo "  - Network connectivity issues (pure-Python packages download from PyPI)"
    echo "  - Wheel platform tag mismatch (check local platform with:"
    echo "    $PYTHON_PATH -c \"import sysconfig; print(sysconfig.get_platform())\")"
    exit 1
fi

echo "Installing Node.js dependencies..."
npm install >> install_log.txt 2>&1

echo "Creating static directory structure..."
mkdir -p static/css

echo "Building static assets..."
npm run build:css >> install_log.txt 2>&1

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env configuration file..."
    cp .env.example .env
    # Generate a new secret key
    NEW_SECRET_KEY=$(.venv/bin/python3 -c "import secrets; print(secrets.token_hex(32))")
    # NetBSD sed doesn't use -i '', use a temp file
    sed "s/CHANGE_ME_GENERATE_NEW_KEY/${NEW_SECRET_KEY}/" .env > .env.tmp && mv .env.tmp .env
    echo "Generated new SECRET_KEY"
fi

echo
printf "${GREEN}✓${NC} Python dependencies installed\n"
printf "${GREEN}✓${NC} Node.js dependencies installed\n"
printf "${GREEN}✓${NC} Static assets built\n"
printf "${GREEN}✓${NC} Configuration file created\n"
echo

# Write installation platform marker so the updater can detect OS
# release or Python ABI transitions and rebuild the venv when needed
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

# ============================================================
# PHASE 3: SERVICE CONFIGURATION
# ============================================================

echo "========================================"
echo "Phase 3: Configuring Service"
echo "========================================"
echo

# Create service wrapper script
SERVICE_SCRIPT="${INSTALL_DIR}/webzfs_service.sh"
echo "Creating service wrapper script..."

cat > "$SERVICE_SCRIPT" << 'SERVICE_EOF'
#!/bin/sh
# WebZFS Service Wrapper Script for NetBSD
# This script runs as root

WEBZFS_DIR="/opt/webzfs"
PIDFILE="${WEBZFS_DIR}/webzfs.pid"

cd "${WEBZFS_DIR}"
export HOME="${WEBZFS_DIR}"
export PYTHONPATH="${WEBZFS_DIR}:${PYTHONPATH}"

# DO NOT load .env file here - let pydantic-settings handle it
# Loading .env in shell can cause issues with quote handling
# pydantic-settings reads .env correctly from the working directory

# Run gunicorn with PID file
exec "${WEBZFS_DIR}/.venv/bin/gunicorn" -c "${WEBZFS_DIR}/config/gunicorn.conf.py" --pid "${PIDFILE}"
SERVICE_EOF

chmod +x "$SERVICE_SCRIPT"
printf "${GREEN}✓${NC} Service wrapper script created\n"

# Create rc.d service file for NetBSD
# NetBSD uses /etc/rc.d for base system services and /usr/pkg/share/examples/rc.d
# for package services, but custom services typically go in /etc/rc.d
RC_SCRIPT="/etc/rc.d/webzfs"
echo "Creating rc.d service script..."

cat > "$RC_SCRIPT" << 'RC_EOF'
#!/bin/sh
#
# PROVIDE: webzfs
# REQUIRE: DAEMON NETWORKING
# KEYWORD: shutdown

$_rc_subr_loaded . /etc/rc.subr

name="webzfs"
rcvar=$name

# WebZFS installation directory
webzfs_dir="/opt/webzfs"

pidfile="${webzfs_dir}/webzfs.pid"

# Custom commands
start_cmd="webzfs_start"
stop_cmd="webzfs_stop"
status_cmd="webzfs_status"
extra_commands="status"

webzfs_start()
{
    # Clean up stale pidfile if process is not running
    if [ -f ${pidfile} ]; then
        pid=$(cat ${pidfile} 2>/dev/null)
        if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
            rm -f ${pidfile}
        else
            echo "${name} already running with pid ${pid}."
            return 1
        fi
    fi
    
    # Verify the venv exists
    if [ ! -f "${webzfs_dir}/.venv/bin/gunicorn" ]; then
        echo "Error: gunicorn not found. Please run the installer again."
        return 1
    fi
    
    echo "Starting ${name}."
    # Start the service as root
    cd ${webzfs_dir} && ${webzfs_dir}/webzfs_service.sh >> ${webzfs_dir}/gunicorn.log 2>&1 &
    
    # Give it a moment to start and write PID
    sleep 2
    
    # Check if it started
    if [ -f ${pidfile} ]; then
        pid=$(cat ${pidfile} 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "${name} started with pid ${pid}."
            return 0
        fi
    fi
    
    echo "${name} failed to start. Check ${webzfs_dir}/gunicorn.log for details."
    return 1
}

webzfs_stop()
{
    if [ -f ${pidfile} ]; then
        pid=$(cat ${pidfile} 2>/dev/null)
        if [ -n "$pid" ]; then
            echo "Stopping ${name}."
            kill -TERM "$pid" 2>/dev/null
            # Wait for process to stop
            i=0
            while [ $i -lt 5 ]; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 1
                i=$((i + 1))
            done
            # Force kill if still running
            if kill -0 "$pid" 2>/dev/null; then
                kill -KILL "$pid" 2>/dev/null
            fi
            echo "${name} stopped."
        fi
        rm -f ${pidfile}
    else
        echo "${name} is not running."
    fi
}

webzfs_status()
{
    if [ -f ${pidfile} ]; then
        pid=$(cat ${pidfile} 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            echo "${name} is running as pid ${pid}."
            return 0
        fi
    fi
    echo "${name} is not running."
    return 1
}

load_rc_config $name
run_rc_command "$1"
RC_EOF

chmod +x "$RC_SCRIPT"
printf "${GREEN}✓${NC} rc.d service script created at ${RC_SCRIPT}\n"

# Ask if user wants to enable the service
echo
printf "Do you want to enable WebZFS to start on boot? (y/n): "
read -r REPLY
if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
    # Add to rc.conf if not already present
    if grep -q "^webzfs=" /etc/rc.conf 2>/dev/null; then
        # Update existing entry
        sed "s/^webzfs=.*/webzfs=YES/" /etc/rc.conf > /etc/rc.conf.tmp && mv /etc/rc.conf.tmp /etc/rc.conf
    else
        # Add new entry
        echo "webzfs=YES" >> /etc/rc.conf
    fi
    printf "${GREEN}✓${NC} WebZFS service enabled in /etc/rc.conf\n"
    echo
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY2
    if [ "$REPLY2" = "y" ] || [ "$REPLY2" = "Y" ]; then
        /etc/rc.d/webzfs start
        printf "${GREEN}✓${NC} WebZFS service started\n"
        echo
        echo "Check service status with: /etc/rc.d/webzfs status"
    fi
else
    echo "Service not enabled. You can enable it later with:"
    echo "  echo 'webzfs=YES' >> /etc/rc.conf"
    echo "  /etc/rc.d/webzfs start"
fi

echo
echo "========================================"
printf "${GREEN}Installation Complete!${NC}\n"
echo "========================================"
echo
echo "WebZFS has been installed to: $INSTALL_DIR"
echo "Note: On NetBSD, the service runs as root for PAM authentication"
echo
echo "Pre-compiled wheels used for: bcrypt, cffi, cryptography, markupsafe,"
echo "psutil, pydantic-core, pynacl"
echo "Wheels cached in: $WHEELS_DIR"
echo
echo "To start the application manually:"
echo "  $INSTALL_DIR/run.sh"
echo
echo "To manage the service:"
echo "  /etc/rc.d/webzfs start"
echo "  /etc/rc.d/webzfs stop"
echo "  /etc/rc.d/webzfs restart"
echo "  /etc/rc.d/webzfs status"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
printf "${YELLOW}IMPORTANT: The WebUI binds to 127.0.0.1 by default and will NOT be${NC}\n"
printf "${YELLOW}reachable from other machines on your local network. To access it${NC}\n"
printf "${YELLOW}remotely, either change the HOST setting in $INSTALL_DIR/.env or use${NC}\n"
printf "${YELLOW}SSH port forwarding. See the 'Access' section of the README for details.${NC}\n"

echo
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo

