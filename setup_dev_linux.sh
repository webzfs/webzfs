#!/bin/bash

# WebZFS Development Setup Script for Linux
# This script sets up and runs WebZFS from a fresh git clone
# It creates a local venv, installs dependencies, and starts the application
#
# Usage: sudo ./setup_dev_linux.sh
#
# Note: This script must be run as root for PAM authentication to work.
#       It does NOT create service files or modify sudoers.

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="${SCRIPT_DIR}/.venv"
ENV_FILE="${SCRIPT_DIR}/.env"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "WebZFS Development Setup for Linux"
echo "========================================"
echo

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: This script must be run as root${NC}"
    echo "Please run: sudo $0"
    exit 1
fi

# Verify essential files exist
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SCRIPT_DIR}/${file}" ]; then
        echo -e "${RED}Error: Essential file '${file}' not found${NC}"
        echo "Please run this script from the WebZFS source directory."
        exit 1
    fi
done

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

# Check prerequisites
echo "Checking prerequisites..."

PYTHON_CMD=$(find_python)
if [ -z "$PYTHON_CMD" ]; then
    echo -e "${RED}Error: Python 3 is not installed${NC}"
    echo "Please install Python 3.11+ and try again"
    exit 1
fi

PYTHON_PATH=$(command -v "$PYTHON_CMD")
PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 11 ]; }; then
    echo -e "${RED}Error: Python 3.11+ is required (found $PYTHON_VERSION)${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)"

if ! command_exists node; then
    echo -e "${RED}Error: Node.js is not installed${NC}"
    echo "Please install Node.js v20+ and try again"
    exit 1
fi

echo -e "${GREEN}✓${NC} Node.js $(node --version) found"

if ! command_exists npm; then
    echo -e "${RED}Error: npm is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} npm $(npm --version) found"

# Check for ZFS
if ! command_exists zpool || ! command_exists zfs; then
    echo -e "${YELLOW}Warning: ZFS utilities not found in PATH${NC}"
fi

# Check for smartmontools
if ! command_exists smartctl; then
    echo -e "${YELLOW}Warning: smartmontools not found${NC}"
fi

echo

# Change to script directory
cd "$SCRIPT_DIR"
export HOME="$SCRIPT_DIR"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

# Create virtual environment if it doesn't exist
if [ -d "$VENV_DIR" ]; then
    echo "Virtual environment already exists"
else
    echo "Creating Python virtual environment..."
    $PYTHON_PATH -m venv .venv
    echo -e "${GREEN}✓${NC} Virtual environment created"
fi

# Activate virtual environment
. .venv/bin/activate

# Install/upgrade pip
echo "Installing/upgrading pip..."
python3 -m pip install --upgrade pip > /dev/null 2>&1
echo -e "${GREEN}✓${NC} pip upgraded"

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Python dependencies installed"

# Install Node.js dependencies
echo "Installing Node.js dependencies..."
npm install > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Node.js dependencies installed"

# Build static assets
echo "Building static assets..."
mkdir -p static/css
npm run build:css > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Static assets built"

# Create .env file if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo "Creating .env configuration file..."
    cp .env.example .env
    # Generate a new secret key
    NEW_SECRET_KEY=$(python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s/CHANGE_ME_GENERATE_NEW_KEY/${NEW_SECRET_KEY}/" .env
    echo -e "${GREEN}✓${NC} Configuration file created with new SECRET_KEY"
else
    echo -e "${GREEN}✓${NC} Configuration file already exists"
fi

# Create application data directory and initialize data files
DATA_DIR="${SCRIPT_DIR}/.config/webzfs"
echo "Creating data directories..."
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

echo -e "${GREEN}✓${NC} Data directories and files created"

echo
echo "========================================"
echo -e "${GREEN}Setup Complete!${NC}"
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
