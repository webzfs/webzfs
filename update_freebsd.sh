#!/bin/sh

# WebZFS Update Script for FreeBSD
# This script updates an existing WebZFS installation at /opt/webzfs
# For initial installation, use install_freebsd.sh instead
#
# The updater compares the Python version inside the existing venv against
# the WebZFS target Python version (TARGET_PYTHON_VERSION). If they differ,
# the venv is rebuilt with the target interpreter and the pre-compiled
# wheels are re-downloaded for the new ABI. This mechanism keeps
# installations current as the target Python version moves forward over
# the years (3.11 to 3.12 today, 3.12 to 3.13 or later in the future).

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
LOG_FILE="${INSTALL_DIR}/update_log.txt"
WHEELS_DIR="${INSTALL_DIR}/.wheels"

# WebZFS target Python version and ABI. Bump these when the project moves
# to a newer Python. The pre-compiled wheels are built for this ABI.
TARGET_PYTHON_VERSION="3.12"
TARGET_PYTHON_TAG="cp312"
# GitHub raw URL base for pre-compiled wheels
WHEELS_REPO_BASE="https://github.com/webzfs/webzfs-wheels/raw/main/wheelhouse"

# Wheel packages to download (these require compilation without pre-built wheels)

# Versions must match the pins in requirements.txt and the list in
# install_freebsd.sh. The wheels cached from the initial install only cover
# the versions pinned at install time; whenever requirements.txt bumps a
# pinned version, the update must fetch the matching new wheels or pip
# silently falls back to compiling from source.
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

# Function to find the target Python interpreter.
# Prefer the exact target version so the venv matches the pre-compiled wheels.
find_python() {
    for py in python${TARGET_PYTHON_VERSION} python3.13 python3; do
        if command_exists "$py"; then
            echo "$py"
            return 0
        fi
    done
    return 1
}

# Function to detect FreeBSD version and determine wheel directory
# (mirrors the logic in install_freebsd.sh)
detect_freebsd_version() {
    FREEBSD_VERSION=$(freebsd-version -u 2>/dev/null || uname -r)
    MAJOR_VERSION=$(echo "$FREEBSD_VERSION" | cut -d. -f1)
    MINOR_VERSION=$(echo "$FREEBSD_VERSION" | cut -d. -f2 | cut -d- -f1)

    echo "Detected FreeBSD version: $FREEBSD_VERSION (major: $MAJOR_VERSION, minor: $MINOR_VERSION)"

    # Map to wheel directory based on major and minor version
    case "${MAJOR_VERSION}.${MINOR_VERSION}" in
        14.3)
            # FreeBSD 14.3 is EOL and cp312 wheels are not published for it.
            # Fall back to the 14.4 wheels, which are ABI compatible.
            printf "${YELLOW}Warning: FreeBSD 14.3 is EOL. Falling back to FreeBSD 14.4 wheels.${NC}\n"
            WHEEL_SUBDIR="freebsd14-4"
            WHEEL_PLATFORM="freebsd_14_4_release_p1_amd64"
            ;;
        14.4)
            WHEEL_SUBDIR="freebsd14-4"
            WHEEL_PLATFORM="freebsd_14_4_release_p1_amd64"
            ;;
        15.0)
            WHEEL_SUBDIR="freebsd15-0"
            WHEEL_PLATFORM="freebsd_15_0_release_amd64"
            ;;
        15.1)
            WHEEL_SUBDIR="freebsd15-1"
            WHEEL_PLATFORM="freebsd_15_1_release_p1_amd64"
            ;;
        15.*)
            # Newer 15.x releases use the latest published 15.x wheel set.
            printf "${YELLOW}Warning: FreeBSD ${MAJOR_VERSION}.${MINOR_VERSION} wheels not published yet.${NC}\n"
            printf "${YELLOW}Falling back to FreeBSD 15.1 wheels (should be compatible).${NC}\n"
            WHEEL_SUBDIR="freebsd15-1"
            WHEEL_PLATFORM="freebsd_15_1_release_p1_amd64"
            ;;
        *)
            printf "${YELLOW}Warning: FreeBSD ${MAJOR_VERSION}.${MINOR_VERSION} is not directly supported.${NC}\n"
            printf "${YELLOW}Attempting to use FreeBSD 14.4 wheels (may not work).${NC}\n"
            WHEEL_SUBDIR="freebsd14-4"
            WHEEL_PLATFORM="freebsd_14_4_release_p1_amd64"
            ;;
    esac

    printf "${GREEN}✓${NC} Will use wheels from: $WHEEL_SUBDIR\n"
}

# Function to download pre-compiled wheels for the versions pinned in
# requirements.txt and the target Python ABI. Wheels already cached from
# the initial install or a previous update are kept and skipped. On download
# failure the update continues and pip falls back to source compilation.
download_wheels() {
    echo "Downloading pre-compiled wheels for ${TARGET_PYTHON_TAG} from ${WHEEL_SUBDIR}..."
    mkdir -p "$WHEELS_DIR"

    WHEELS_URL="${WHEELS_REPO_BASE}/${WHEEL_SUBDIR}"
    DOWNLOAD_FAILED=0

    for pkg_version in $WHEEL_PACKAGES; do
        pkg_name=$(echo "$pkg_version" | sed 's/-[0-9].*//')
        version=$(echo "$pkg_version" | sed 's/.*-//')
        wheel_pkg_name=$(echo "$pkg_name" | tr '-' '_')

        # cryptography publishes a stable-ABI (abi3) wheel tagged cp311-abi3
        # (compatible with Python 3.11+). All other packages ship
        # version-specific wheels for the target ABI.
        case "$pkg_name" in
            cryptography)
                ABI_TAG="cp311-abi3"
                ;;
            *)
                ABI_TAG="${TARGET_PYTHON_TAG}-${TARGET_PYTHON_TAG}"
                ;;
        esac

        wheel_filename="${wheel_pkg_name}-${version}-${ABI_TAG}-${WHEEL_PLATFORM}.whl"
        wheel_url="${WHEELS_URL}/${wheel_filename}"
        wheel_path="${WHEELS_DIR}/${wheel_filename}"

        if [ -f "$wheel_path" ]; then
            printf "  ${GREEN}✓${NC} ${pkg_name} wheel already cached\n"
        else
            printf "  Downloading ${pkg_name}..."
            if fetch -q -o "$wheel_path" "$wheel_url" 2>/dev/null; then
                printf " ${GREEN}✓${NC}\n"
            else
                printf " ${RED}FAILED${NC}\n"
                printf "${YELLOW}Warning: Could not download wheel for ${pkg_name}${NC}\n"
                printf "  URL: ${wheel_url}\n"
                DOWNLOAD_FAILED=1
            fi
        fi
    done

    if [ "$DOWNLOAD_FAILED" -eq 1 ]; then
        printf "${YELLOW}Some wheels failed to download. Pip will attempt source compilation.${NC}\n"
    else
        printf "${GREEN}✓${NC} All wheels downloaded successfully\n"
    fi
}

echo "========================================"
echo "WebZFS Update Script for FreeBSD"
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
    echo "Please run install_freebsd.sh for initial installation"
    exit 1
fi

# Verify virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    printf "${RED}Error: Virtual environment not found at $VENV_DIR${NC}\n"
    echo "Please run install_freebsd.sh for initial installation"
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
if [ ! -f "/usr/local/etc/rc.d/webzfs" ]; then
    printf "${RED}Error: rc.d service script not found${NC}\n"
    echo "Please run install_freebsd.sh for initial installation"
    exit 1
fi

# Check the venv Python version against the WebZFS target Python version.
# If they differ, the venv must be rebuilt with the target interpreter so
# native extension modules match the pre-compiled wheel ABI.
REBUILD_VENV=false
VENV_PYTHON_VERSION=""
if [ -x "${VENV_DIR}/bin/python3" ]; then
    VENV_PYTHON_VERSION=$("${VENV_DIR}/bin/python3" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null || true)
fi

if [ "$VENV_PYTHON_VERSION" != "$TARGET_PYTHON_VERSION" ]; then
    echo
    printf "${YELLOW}Python version upgrade required:${NC}\n"
    echo "  The existing virtual environment uses Python ${VENV_PYTHON_VERSION:-unknown}."
    echo "  WebZFS now targets Python ${TARGET_PYTHON_VERSION}."
    echo "  The virtual environment will be upgraded from Python ${VENV_PYTHON_VERSION:-unknown} to ${TARGET_PYTHON_VERSION}"
    echo "  and all Python dependencies will be reinstalled for the new version."
    REBUILD_VENV=true

    # Make sure the target interpreter is available
    if ! command_exists "python${TARGET_PYTHON_VERSION}"; then
        echo
        echo "Installing python${TARGET_PYTHON_VERSION} via pkg..."
        PKG_PY_VERSION=$(echo "$TARGET_PYTHON_VERSION" | tr -d '.')
        pkg install -y "python${PKG_PY_VERSION}"
    fi

    PYTHON_CMD=$(find_python)
    if [ -z "$PYTHON_CMD" ]; then
        printf "${RED}Error: Python ${TARGET_PYTHON_VERSION} could not be installed${NC}\n"
        exit 1
    fi
    PYTHON_PATH=$(command -v "$PYTHON_CMD")
    NEW_PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
    printf "${GREEN}✓${NC} Python ${NEW_PYTHON_VERSION} found (${PYTHON_CMD})\n"
fi

echo

# Check if service is running
SERVICE_WAS_RUNNING=false
if service webzfs status >/dev/null 2>&1; then
    SERVICE_WAS_RUNNING=true
    echo "Stopping WebZFS service..."
    service webzfs stop
    printf "${GREEN}✓${NC} Service stopped\n"
fi

echo

# Copy application files to installation directory (preserving config)
echo "Updating application files from $SOURCE_DIR to $INSTALL_DIR..."

# Use tar instead of rsync (more portable on FreeBSD)
# Create a temporary exclude file for patterns
EXCLUDE_FILE=$(mktemp)
cat > "$EXCLUDE_FILE" << 'EOF'
.venv
node_modules
.git
*.log
__pycache__
*.pyc
.env
.config
.wheels
config/gunicorn.conf.py
./install_linux.sh
./install_freebsd.sh
./install_netbsd.sh
./install_linux_cockpit.sh
./update_linux.sh
./update_freebsd.sh
./update_netbsd.sh
./update_linux_cockpit.sh
./integrations/cockpit/install.sh
EOF

# Create a backup tar of the source, excluding unwanted files
(cd "$SOURCE_DIR" && tar cf - --exclude-from="$EXCLUDE_FILE" .) | \
    (cd "$INSTALL_DIR" && tar xf -)

rm -f "$EXCLUDE_FILE"

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
# block in install_freebsd.sh.
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
            # FreeBSD sed requires -i '' for in-place editing
            sed -i '' "s|^CAPTION=.*|${NEW_CAPTION}|" "$ENV_FILE"
            printf "${GREEN}✓${NC} Updated CAPTION to: ${NEW_CAPTION}\n"
        else
            # CAPTION not found in .env, add it at the top
            printf '%s\n' "${NEW_CAPTION}" | cat - "$ENV_FILE" > "${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
            printf "${GREEN}✓${NC} Added CAPTION: ${NEW_CAPTION}\n"
        fi
    fi
fi

echo

# Update dependencies
echo "Updating Python and Node.js dependencies..."
echo "(This may take a few minutes...)"
echo

cd "$INSTALL_DIR"

# Set environment for building
export HOME="$INSTALL_DIR"

# Check for gmake
if command -v gmake >/dev/null 2>&1; then
    export MAKE=$(command -v gmake)
fi

# Rebuild the virtual environment if a Python version upgrade is required
if [ "$REBUILD_VENV" = "true" ]; then
    echo "Rebuilding virtual environment with Python ${TARGET_PYTHON_VERSION}..."
    rm -rf .venv
    $PYTHON_PATH -m venv .venv
    printf "${GREEN}✓${NC} Virtual environment recreated\n"

    # Bootstrap pip if ensurepip did not seed it
    if [ ! -x ".venv/bin/pip" ] && [ ! -x ".venv/bin/pip3" ]; then
        .venv/bin/python3 -m ensurepip --upgrade > update_log.txt 2>&1
    fi

    # Clear cached wheels built for the old ABI. The unconditional wheel
    # refresh below repopulates the cache for the target ABI.
    if [ -d "$WHEELS_DIR" ]; then
        echo "Clearing wheel cache from previous Python ABI..."
        rm -rf "$WHEELS_DIR"
    fi
    echo
fi

echo "Upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > update_log.txt 2>&1

# Detect FreeBSD version and download the pre-compiled wheels matching the
# versions pinned in the new requirements.txt. Wheels cached from the
# initial install only cover the versions pinned at install time, so an
# update that bumps pinned versions must fetch new wheels here.
detect_freebsd_version
download_wheels

FIND_LINKS_FLAG=""
if [ -d "$WHEELS_DIR" ]; then
    LOCAL_PLATFORM=$(.venv/bin/python3 -c "import sysconfig; print(sysconfig.get_platform().replace('.', '_').replace('-', '_'))")
    for whl in "$WHEELS_DIR"/*.whl; do
        if [ -f "$whl" ]; then
            base_whl=$(basename "$whl")
            # Check if this wheel needs a platform-adapted copy
            if echo "$base_whl" | grep -q "freebsd_" && ! echo "$base_whl" | grep -q "$LOCAL_PLATFORM"; then
                # Extract the wheel's platform tag
                whl_platform=$(echo "$base_whl" | sed 's/.*-\(freebsd_[^.]*\)\.whl/\1/')
                new_whl=$(echo "$whl" | sed "s/${whl_platform}/${LOCAL_PLATFORM}/")
                if [ ! -f "$new_whl" ]; then
                    cp "$whl" "$new_whl"
                fi
            fi
        fi
    done
    FIND_LINKS_FLAG="--find-links=$WHEELS_DIR"
    printf "${GREEN}✓${NC} Using wheels from $WHEELS_DIR\n"
fi

echo "Updating Python dependencies..."
if ! .venv/bin/pip install $FIND_LINKS_FLAG -r requirements.txt >> update_log.txt 2>&1; then
    printf "${RED}Error: Failed to update Python dependencies${NC}\n"
    echo "Check ${INSTALL_DIR}/update_log.txt for details"
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

# Restart service if it was running
if [ "$SERVICE_WAS_RUNNING" = true ]; then
    echo "Restarting WebZFS service..."
    service webzfs start
    printf "${GREEN}✓${NC} Service restarted\n"
    echo
else
    echo "WebZFS service was not running before update."
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY
    if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
        service webzfs start
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
echo "  sudo service webzfs status"
echo
echo "To view logs:"
echo "  tail -f $INSTALL_DIR/gunicorn.log"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
