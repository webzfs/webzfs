#!/bin/bash

# WebZFS Installation Script for Linux
# This script installs WebZFS to /opt/webzfs with proper user permissions

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
ENV_FILE="${INSTALL_DIR}/.env"
LOG_FILE="${INSTALL_DIR}/install_log.txt"
WEBZFS_USER="webzfs"

# Determine the source directory (where this script is located)
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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
echo "WebZFS Installation Script for Linux"
echo "========================================"
echo

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: This script must be run as root${NC}"
    echo "Please run: sudo $0"
    exit 1
fi

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to find Python 3.11+
# Some distributions install python311 as python3.11, not python3
find_python() {
    # Check for specific versions first
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

# Get the full path to Python for use in subshells
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
    echo "Make sure ZFS is installed before running the application"
fi

# Check for smartmontools
if ! command_exists smartctl; then
    echo -e "${YELLOW}Warning: smartmontools not found${NC}"
    echo "Install smartmontools for disk health monitoring"
fi

# Check for make (needed to compile pynacl)
if ! command_exists make; then
    echo -e "${RED}Error: make is not installed${NC}"
    echo "make is required to compile pynacl."
    echo "Please install build-essential (Debian/Ubuntu) or base-devel (Arch) or Development Tools (RHEL/Fedora)"
    exit 1
fi

echo -e "${GREEN}✓${NC} make found"

# Check for libsodium (needed to compile pynacl)
LIBSODIUM_FOUND=0
for header_path in /usr/include/sodium.h /usr/local/include/sodium.h; do
    if [ -f "$header_path" ]; then
        LIBSODIUM_FOUND=1
        break
    fi
done

if [ "$LIBSODIUM_FOUND" -eq 0 ]; then
    echo -e "${RED}Error: libsodium development headers not found${NC}"
    echo "libsodium is required to compile pynacl."
    echo "Please install libsodium-dev (Debian/Ubuntu) or libsodium-devel (RHEL/Fedora)"
    exit 1
fi

echo -e "${GREEN}✓${NC} libsodium found"

echo

# Create webzfs user if it doesn't exist
if id "$WEBZFS_USER" &>/dev/null; then
    echo "User '$WEBZFS_USER' already exists"
else
    echo "Creating system user '$WEBZFS_USER'..."
    useradd -r -s /bin/bash -m -d "$INSTALL_DIR" -c "WebZFS Service User" "$WEBZFS_USER"
    echo -e "${GREEN}✓${NC} User '$WEBZFS_USER' created"
fi

# Configure shadow group for PAM authentication
# This allows the webzfs user to verify passwords via unix_chkpwd
echo "Configuring PAM authentication permissions..."

# Create shadow group if it doesn't exist (exists on Debian/Ubuntu, not on Arch/Fedora)
if ! getent group shadow &>/dev/null; then
    groupadd shadow
    echo -e "${GREEN}✓${NC} Created 'shadow' group"
fi

# Add webzfs user to shadow group
if ! id -nG "$WEBZFS_USER" | grep -qw shadow; then
    usermod -aG shadow "$WEBZFS_USER"
    echo -e "${GREEN}✓${NC} Added '$WEBZFS_USER' to 'shadow' group"
fi

# Set shadow file group ownership and permissions for PAM authentication
chgrp shadow /etc/shadow
chmod 640 /etc/shadow
echo -e "${GREEN}✓${NC} PAM authentication configured"

# Create installation directory if it doesn't exist
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Creating installation directory: $INSTALL_DIR"
    mkdir -p "$INSTALL_DIR"
fi

# Copy application files to installation directory
echo "Copying application files from $SOURCE_DIR to $INSTALL_DIR..."
rsync -a --exclude='.venv' --exclude='node_modules' --exclude='.git' --exclude='*.log' \
    --exclude='__pycache__' --exclude='*.pyc' \
    "${SOURCE_DIR}/" "$INSTALL_DIR/"

# Set ownership
chown -R "$WEBZFS_USER:$WEBZFS_USER" "$INSTALL_DIR"

echo -e "${GREEN}✓${NC} Application files copied"
echo

# Create a temporary install script that runs as the webzfs user
# Using a script file instead of heredoc to preserve stdin for later read prompts
TEMP_INSTALL_SCRIPT="${INSTALL_DIR}/_install_deps.sh"
echo "Installing Python and Node.js dependencies as $WEBZFS_USER..."
echo "(This may take a few minutes...)"
echo

# Create the install script
cat > "$TEMP_INSTALL_SCRIPT" << INSTALL_EOF
#!/bin/bash
set -e

# Set HOME to the webzfs user's home directory for pip cache
export HOME="/opt/webzfs"

cd /opt/webzfs

# Use the full Python path
PYTHON_PATH="$PYTHON_PATH"

# Create virtual environment
if [ -d ".venv" ]; then
    echo "Virtual environment already exists"
else
    echo "Creating Python virtual environment..."
    \$PYTHON_PATH -m venv .venv
fi

echo "Installing/upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > install_log.txt 2>&1

echo "Installing Python dependencies in virtual environment..."
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
    # Generate a new secret key using the venv python
    NEW_SECRET_KEY=\$(.venv/bin/python3 -c "import secrets; print(secrets.token_hex(32))")
    sed -i "s/CHANGE_ME_GENERATE_NEW_KEY/\${NEW_SECRET_KEY}/" .env
    echo "Generated new SECRET_KEY"
fi

echo "Dependencies installed successfully!"
INSTALL_EOF

chmod +x "$TEMP_INSTALL_SCRIPT"
chown "$WEBZFS_USER:$WEBZFS_USER" "$TEMP_INSTALL_SCRIPT"

# Run the install script as the webzfs user
if ! su -s /bin/bash "$WEBZFS_USER" -c "bash $TEMP_INSTALL_SCRIPT"; then
    echo -e "${RED}Error: Installation failed${NC}"
    echo "Check $LOG_FILE for details"
    rm -f "$TEMP_INSTALL_SCRIPT"
    exit 1
fi

# Clean up the temporary script
rm -f "$TEMP_INSTALL_SCRIPT"

echo
echo -e "${GREEN}✓${NC} Python dependencies installed"
echo -e "${GREEN}✓${NC} Node.js dependencies installed"
echo -e "${GREEN}✓${NC} Static assets built"
echo -e "${GREEN}✓${NC} Configuration file created"
echo

# Configure sudo permissions
SUDOERS_FILE="/etc/sudoers.d/webzfs"
echo "Configuring sudo permissions..."

cat > "$SUDOERS_FILE" << 'SUDO_EOF'
# WebZFS sudo permissions
# Allow webzfs user to execute ZFS and SMART commands

# ZFS commands (multiple paths for different distributions)
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/zpool, /usr/sbin/zfs, /usr/sbin/zdb -l *, /usr/bin/zpool, /usr/bin/zfs, /usr/bin/zdb -l *, /sbin/zpool, /sbin/zfs, /sbin/zdb -l *

# SMART monitoring
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/smartctl, /usr/bin/smartctl

# Disk utilities
webzfs ALL=(ALL) NOPASSWD: /usr/bin/lsblk, /usr/bin/blkid

# Sanoid/Syncoid (optional)
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/sanoid, /usr/sbin/syncoid, /usr/bin/sanoid, /usr/bin/syncoid, /usr/local/sbin/sanoid, /usr/local/sbin/syncoid

# Crontab editing
webzfs ALL=(ALL) NOPASSWD: /usr/bin/crontab
SUDO_EOF

chmod 0440 "$SUDOERS_FILE"
echo -e "${GREEN}✓${NC} Sudo permissions configured"

echo

# Create systemd service file
SYSTEMD_SERVICE="/etc/systemd/system/webzfs.service"
echo "Creating systemd service file..."

cat > "$SYSTEMD_SERVICE" << 'SERVICE_EOF'
[Unit]
Description=WebZFS Web Management Interface
After=network.target zfs-mount.service

[Service]
Type=notify
User=webzfs
Group=webzfs
WorkingDirectory=/opt/webzfs
Environment="PATH=/opt/webzfs/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
ExecStart=/opt/webzfs/.venv/bin/gunicorn -c config/gunicorn.conf.py
Restart=always
RestartSec=5

# Runtime directory for unix socket support
# Creates /run/webzfs/ on service start, removes on stop
# To use: set BIND=unix:/run/webzfs/webzfs.sock in .env
RuntimeDirectory=webzfs
RuntimeDirectoryMode=0755

[Install]
WantedBy=multi-user.target
SERVICE_EOF

echo -e "${GREEN}✓${NC} Systemd service file created"

# Ask if user wants to enable the service
echo
printf "Do you want to enable WebZFS to start on boot? (y/n): "
read -r REPLY
if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
    systemctl daemon-reload
    systemctl enable webzfs
    echo -e "${GREEN}✓${NC} WebZFS service enabled"
    echo
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY2
    if [ "$REPLY2" = "y" ] || [ "$REPLY2" = "Y" ]; then
        systemctl start webzfs
        echo -e "${GREEN}✓${NC} WebZFS service started"
        echo
        echo "Check service status with: sudo systemctl status webzfs"
    fi
else
    echo "Service not enabled. You can enable it later with:"
    echo "  sudo systemctl enable webzfs"
    echo "  sudo systemctl start webzfs"
fi

echo
echo "========================================"
echo -e "${GREEN}Installation Complete!${NC}"
echo "========================================"
echo
echo "WebZFS has been installed to: $INSTALL_DIR"
echo "Application runs as user: $WEBZFS_USER"
echo
echo "To start the application manually:"
echo "  sudo -u $WEBZFS_USER $INSTALL_DIR/run.sh"
echo
echo "To manage the service:"
echo "  sudo systemctl start webzfs"
echo "  sudo systemctl stop webzfs"
echo "  sudo systemctl restart webzfs"
echo "  sudo systemctl status webzfs"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo
