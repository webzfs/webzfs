#!/bin/sh

# WebZFS Installation Script for NetBSD
# This script installs WebZFS to /opt/webzfs
# On NetBSD, the application runs as root due to PAM authentication requirements

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
ENV_FILE="${INSTALL_DIR}/.env"
LOG_FILE="${INSTALL_DIR}/install_log.txt"

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

# Function to find Python 3.11+
find_python() {
    for py in python3.13 python3.12 python3.11 python3; do
        if command_exists "$py"; then
            echo "$py"
            return 0
        fi
    done
    return 1
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
    pkgin -y install python311 py311-pip nodejs smartmontools \
                     git perl mbuffer lzop pv mozilla-rootcerts \
                     p5-Config-IniFiles p5-Capture-Tiny \
                     gmake libsodium curl pkg-config openssl

    # Create Python symlinks if they don't exist
    if [ ! -f /usr/pkg/bin/python3 ]; then
        if [ -f /usr/pkg/bin/python3.11 ]; then
            ln -sf /usr/pkg/bin/python3.11 /usr/pkg/bin/python3
            printf "${GREEN}✓${NC} Created python3 symlink\n"
        fi
    fi

    if [ ! -f /usr/pkg/bin/pip ] && [ ! -f /usr/pkg/bin/pip3 ]; then
        if [ -f /usr/pkg/bin/pip3.11 ]; then
            ln -sf /usr/pkg/bin/pip3.11 /usr/pkg/bin/pip
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

    # Install Rust via rustup (pkgsrc rust package has issues on some systems)
    echo "Checking Rust installation..."
    
    # Source cargo env if it exists
    if [ -f "/root/.cargo/env" ]; then
        . "/root/.cargo/env"
    elif [ -f "$HOME/.cargo/env" ]; then
        . "$HOME/.cargo/env"
    fi

    if ! command_exists rustc || ! rustc --version >/dev/null 2>&1; then
        echo "Installing Rust via rustup..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --profile minimal
        # Source cargo env for current session
        . "/root/.cargo/env" 2>/dev/null || . "$HOME/.cargo/env" 2>/dev/null || true
        # Set default toolchain
        rustup default stable
        printf "${GREEN}✓${NC} Rust installed via rustup\n"
    else
        printf "${GREEN}✓${NC} Rust already installed\n"
    fi
    echo

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

PYTHON_CMD=$(find_python)
if [ -z "$PYTHON_CMD" ]; then
    printf "${RED}Error: Python 3 is not installed${NC}\n"
    echo "Please install Python 3.11+ first"
    exit 1
fi

PYTHON_PATH=$(command -v "$PYTHON_CMD")
PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
printf "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)\n"

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

# Check for Rust (needed to compile pydantic-core on NetBSD)
# Try to source cargo env if rustup is installed
if [ -f "/root/.cargo/env" ]; then
    . "/root/.cargo/env"
elif [ -f "$HOME/.cargo/env" ]; then
    . "$HOME/.cargo/env"
fi

if ! command_exists rustc; then
    printf "${RED}Error: Rust is not installed${NC}\n"
    echo "Rust is required to compile pydantic-core on NetBSD."
    echo "Please run: $0 --deps-only"
    exit 1
fi

# Verify rustc actually works (not just exists)
if ! rustc --version >/dev/null 2>&1; then
    printf "${RED}Error: Rust compiler is installed but not working${NC}\n"
    echo "The rustc binary may be corrupted or incompatible."
    echo "Try installing Rust via rustup:"
    echo "  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

printf "${GREEN}✓${NC} Rust $(rustc --version | cut -d' ' -f2) found\n"

# Check for libsodium (needed to compile pynacl on NetBSD)
if [ ! -f "/usr/pkg/include/sodium.h" ]; then
    printf "${RED}Error: libsodium is not installed${NC}\n"
    echo "libsodium is required to compile pynacl on NetBSD."
    echo "Please install it with: pkgin -y install libsodium"
    exit 1
fi

printf "${GREEN}✓${NC} libsodium found\n"

# Check for gmake (GNU make - required for compiling some Python packages)
if ! command_exists gmake; then
    printf "${RED}Error: GNU make (gmake) is not installed${NC}\n"
    echo "gmake is required to compile some Python packages on NetBSD."
    echo "Please install it with: pkgin -y install gmake"
    exit 1
fi

printf "${GREEN}✓${NC} gmake found\n"

# Get the full path to gmake for use in the install script
GMAKE_PATH=$(command -v gmake)

# Check for pkg-config (required for cryptography package)
if ! command_exists pkg-config; then
    printf "${RED}Error: pkg-config is not installed${NC}\n"
    echo "pkg-config is required to compile cryptography on NetBSD."
    echo "Please install it with: pkgin -y install pkg-config"
    exit 1
fi

printf "${GREEN}✓${NC} pkg-config found\n"

# Check for OpenSSL (required for cryptography package)
if [ ! -f "/usr/pkg/include/openssl/ssl.h" ]; then
    printf "${RED}Error: OpenSSL development headers not found${NC}\n"
    echo "OpenSSL is required to compile cryptography on NetBSD."
    echo "Please install it with: pkgin -y install openssl"
    exit 1
fi

printf "${GREEN}✓${NC} OpenSSL found\n"

echo

# Create installation directory if it doesn't exist
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Creating installation directory: $INSTALL_DIR"
    mkdir -p "$INSTALL_DIR"
fi

# Copy application files to installation directory
echo "Copying application files from $SOURCE_DIR to $INSTALL_DIR..."

# Use tar to copy files (portable method)
(cd "$SOURCE_DIR" && tar cf - --exclude='.venv' --exclude='node_modules' --exclude='.git' \
    --exclude='*.log' --exclude='__pycache__' --exclude='*.pyc' .) | \
    (cd "$INSTALL_DIR" && tar xf -)

printf "${GREEN}✓${NC} Application files copied\n"

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
    echo '{"schedules": []}' > "${DATA_DIR}/scrub_schedules.json"
fi

# SMART monitoring service files
if [ ! -f "${DATA_DIR}/smart_test_history.json" ]; then
    echo '{"history": []}' > "${DATA_DIR}/smart_test_history.json"
fi
if [ ! -f "${DATA_DIR}/scheduled_tests.json" ]; then
    echo '{}' > "${DATA_DIR}/scheduled_tests.json"
fi

printf "${GREEN}✓${NC} Data directory and files created\n"

# Also create data directory in root's home (since gunicorn runs as root)
# This prevents FileNotFoundError during module import when workers initialize
ROOT_DATA_DIR="/root/.config/webzfs"
if [ ! -d "$ROOT_DATA_DIR" ]; then
    echo "Creating root user data directory: $ROOT_DATA_DIR"
    mkdir -p "$ROOT_DATA_DIR"
    # Pre-create scrub_schedules.json to avoid race condition on first import
    if [ ! -f "${ROOT_DATA_DIR}/scrub_schedules.json" ]; then
        echo '{"schedules": [], "next_id": 1}' > "${ROOT_DATA_DIR}/scrub_schedules.json"
    fi
    printf "${GREEN}✓${NC} Root user data directory created\n"
fi
echo

# Install dependencies
echo "Installing Python and Node.js dependencies..."
echo "(This may take a few minutes...)"
echo

cd "$INSTALL_DIR"

# Set environment for building
export HOME="$INSTALL_DIR"
export MAKE="$GMAKE_PATH"

# Set OpenSSL location for cryptography package on NetBSD
export OPENSSL_DIR="/usr/pkg"
export PKG_CONFIG_PATH="/usr/pkg/lib/pkgconfig:${PKG_CONFIG_PATH}"

# Ensure Rust toolchain is in PATH for building Python packages
# Check common locations for cargo/rustc
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
if [ -d ".venv" ]; then
    echo "Virtual environment already exists"
else
    echo "Creating Python virtual environment..."
    $PYTHON_PATH -m venv .venv
fi

echo "Installing/upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > install_log.txt 2>&1

echo "Installing Python dependencies in virtual environment..."
echo "This may take 10-15 minutes as Rust packages need to be compiled..."
.venv/bin/pip install -r requirements.txt >> install_log.txt 2>&1

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
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo
