#!/bin/sh

# WebZFS Installation Script for NetBSD
# This script installs WebZFS to /opt/webzfs
# This is a simplified script that copies and builds the application
# Service setup and prerequisites must be handled separately

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
ENV_FILE="${INSTALL_DIR}/.env"
LOG_FILE="${INSTALL_DIR}/install_log.txt"

# Determine the source directory (where this script is located)
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Verify essential files exist in source directory
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SOURCE_DIR}/${file}" ]; then
        echo "Error: Essential file '${file}' not found in ${SOURCE_DIR}"
        echo "Please run this installer from the WebZFS source directory containing all application files."
        exit 1
    fi
done

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

# Quick prerequisite check (just verify commands exist)
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
if [ -f "$HOME/.cargo/env" ]; then
    . "$HOME/.cargo/env"
fi

if ! command_exists rustc; then
    printf "${RED}Error: Rust is not installed${NC}\n"
    echo "Rust is required to compile pydantic-core on NetBSD."
    echo "Please run: sh install_netbsd_deps.sh"
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

echo "========================================"
printf "${GREEN}Installation Complete!${NC}\n"
echo "========================================"
echo
echo "WebZFS has been installed to: $INSTALL_DIR"
echo
echo "To start the application manually:"
echo "  cd $INSTALL_DIR"
echo "  .venv/bin/gunicorn -c config/gunicorn.conf.py"
echo
echo "Or use the run script:"
echo "  $INSTALL_DIR/run.sh"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo
echo "Note: Service setup (rc.d scripts) not included in this installer."
echo "You will need to configure service startup separately."
echo
