#!/bin/sh

# WebZFS Development Setup Script for FreeBSD
# This script sets up and runs WebZFS from a fresh git clone
# It creates a local venv, installs dependencies, and starts the application
#
# Usage: sudo ./setup_dev_freebsd.sh
#
# Note: This script must be run as root for PAM authentication to work.
#       It does NOT create service files or modify system configuration.

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
ENV_FILE="${SCRIPT_DIR}/.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "WebZFS Development Setup for FreeBSD"
echo "========================================"
echo

# Check if running as root
if [ "$(id -u)" -ne 0 ]; then 
    printf "${RED}Error: This script must be run as root${NC}\n"
    echo "Please run: sudo $0"
    exit 1
fi

# Verify essential files exist
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SCRIPT_DIR}/${file}" ]; then
        printf "${RED}Error: Essential file '${file}' not found${NC}\n"
        echo "Please run this script from the WebZFS source directory."
        exit 1
    fi
done

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to find Python 3.11+ on FreeBSD
find_python() {
    for py in python3.13 python3.12 python3.11 python3; do
        if command_exists "$py"; then
            echo "$py"
            return 0
        fi
    done
    return 1
}

# Check prerequisites
echo "Checking prerequisites..."

PYTHON_CMD=$(find_python)
if [ -z "$PYTHON_CMD" ]; then
    printf "${RED}Error: Python 3 is not installed${NC}\n"
    echo "Please install Python 3.11+ first:"
    echo "  pkg install python311 py311-pip"
    exit 1
fi

PYTHON_PATH=$(command -v "$PYTHON_CMD")
PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 11 ]; }; then
    printf "${RED}Error: Python 3.11+ is required (found $PYTHON_VERSION)${NC}\n"
    exit 1
fi

printf "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)\n"

if ! command_exists node; then
    printf "${RED}Error: Node.js is not installed${NC}\n"
    echo "Please install Node.js first:"
    echo "  pkg install node npm"
    exit 1
fi

printf "${GREEN}✓${NC} Node.js $(node --version) found\n"

if ! command_exists npm; then
    printf "${RED}Error: npm is not installed${NC}\n"
    echo "Please install npm first:"
    echo "  pkg install npm"
    exit 1
fi

printf "${GREEN}✓${NC} npm $(npm --version) found\n"

# Check for ZFS (should be built-in on FreeBSD)
if ! command_exists zpool || ! command_exists zfs; then
    printf "${YELLOW}Warning: ZFS utilities not found in PATH${NC}\n"
fi

# Check for smartmontools
if ! command_exists smartctl; then
    printf "${YELLOW}Warning: smartmontools not found${NC}\n"
    echo "Install with: pkg install smartmontools"
fi

# Check for gmake (needed for some Python packages)
if ! command_exists gmake; then
    printf "${YELLOW}Warning: gmake not found - may be needed for some packages${NC}\n"
    echo "Install with: pkg install gmake"
fi

# Check for rust (needed for pydantic-core)
if ! command_exists rustc; then
    printf "${YELLOW}Warning: Rust not found - may be needed for some packages${NC}\n"
    echo "Install with: pkg install rust"
fi

# Check for libsodium (needed for pynacl)
if [ ! -f "/usr/local/include/sodium.h" ]; then
    printf "${YELLOW}Warning: libsodium not found - may be needed for some packages${NC}\n"
    echo "Install with: pkg install libsodium"
fi

echo

# Change to script directory
cd "$SCRIPT_DIR"
export HOME="$SCRIPT_DIR"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

# Set MAKE environment variable to gmake if available
if command_exists gmake; then
    export MAKE=$(command -v gmake)
fi

# Create virtual environment if it doesn't exist
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment already exists"
else
    echo "Creating Python virtual environment..."
    $PYTHON_PATH -m venv .venv
    printf "${GREEN}✓${NC} Virtual environment created\n"
fi

# Activate virtual environment
. .venv/bin/activate

# Install/upgrade pip
echo "Installing/upgrading pip..."
python3 -m pip install --upgrade pip > /dev/null 2>&1
printf "${GREEN}✓${NC} pip upgraded\n"

# Install Python dependencies
echo "Installing Python dependencies..."
echo "(This may take a few minutes on first run...)"
pip install -r requirements.txt > /dev/null 2>&1
printf "${GREEN}✓${NC} Python dependencies installed\n"

# Install Node.js dependencies
echo "Installing Node.js dependencies..."
npm install > /dev/null 2>&1
printf "${GREEN}✓${NC} Node.js dependencies installed\n"

# Build static assets
echo "Building static assets..."
mkdir -p static/css
npm run build:css > /dev/null 2>&1
printf "${GREEN}✓${NC} Static assets built\n"

# Create .env file if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo "Creating .env configuration file..."
    cp .env.example .env
    # Generate a new secret key
    NEW_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i '' "s/CHANGE_ME_GENERATE_NEW_KEY/${NEW_SECRET_KEY}/" .env
    printf "${GREEN}✓${NC} Configuration file created with new SECRET_KEY\n"
else
    printf "${GREEN}✓${NC} Configuration file already exists\n"
fi

echo
echo "========================================"
printf "${GREEN}Setup Complete!${NC}\n"
echo "========================================"
echo
echo "Starting WebZFS development server..."
echo "Access the web interface at: http://localhost:26619"
echo "Press Ctrl+C to stop the server"
echo
echo "----------------------------------------"
echo

# Start the application
SETTINGS_MODULE=config.settings.dev exec .venv/bin/gunicorn -c config/gunicorn.conf.py
