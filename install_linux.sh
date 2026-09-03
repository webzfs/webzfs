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

# Function to find Python 3.12+
# Some distributions install versioned binaries (python3.12) rather than
# updating the python3 symlink. Prefer the newest available interpreter.
# WebZFS targets Python 3.12 as its minimum; newer versions (3.13, 3.14)
# are fine on Linux since native packages are compiled from source.
find_python() {
    # Check for specific versions first, newest first
    for py in python3.14 python3.13 python3.12 python3; do
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
    echo "Please install Python 3.12+ and try again"
    exit 1
fi

# Get the full path to Python for use in subshells
PYTHON_PATH=$(command -v "$PYTHON_CMD")

PYTHON_VERSION=$($PYTHON_CMD -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

if [ "$PYTHON_MAJOR" -lt 3 ] || { [ "$PYTHON_MAJOR" -eq 3 ] && [ "$PYTHON_MINOR" -lt 12 ]; }; then
    echo -e "${RED}Error: Python 3.12+ is required (found $PYTHON_VERSION)${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Python $PYTHON_VERSION found ($PYTHON_CMD)"

if ! command_exists node; then
    echo -e "${RED}Error: Node.js is not installed${NC}"
    echo "Please install Node.js v20+ and try again"
    exit 1
fi

if ! command_exists npm; then
    echo -e "${RED}Error: npm is not installed${NC}"
    echo "Please install npm and try again"
    exit 1
fi

# Reject Snap-packaged Node.js/npm (GitHub issue #209).
# The build steps (npm install, npm run build:css) run as the webzfs
# service account with HOME=/opt/webzfs. Snapd refuses to run snaps for
# users whose home directory is outside /home, which makes the Node Snap
# unusable for the WebZFS build. A system Node.js/npm must be installed
# instead. The existing Snap does not need to be removed.
NODE_REAL_PATH=$(readlink -f "$(command -v node)" 2>/dev/null || true)
NPM_REAL_PATH=$(readlink -f "$(command -v npm)" 2>/dev/null || true)

case "$NODE_REAL_PATH:$NPM_REAL_PATH" in
    /snap/*|*:/snap/*)
        echo -e "${RED}Error: Node.js/npm are installed as a Snap package${NC}"
        echo
        echo "The Node Snap cannot be used by the WebZFS installer because the"
        echo "build runs as the 'webzfs' service account with HOME=/opt/webzfs,"
        echo "and snapd rejects home directories outside of /home."
        echo
        echo "Please install a system (non-Snap) Node.js 20+ and npm, then"
        echo "rerun this installer. For example, on Debian/Ubuntu:"
        echo
        echo "  sudo apt update"
        echo "  sudo apt install nodejs npm"
        echo
        echo "The existing Node Snap does not need to be removed; the system"
        echo "packages can coexist with it. If both are installed, ensure the"
        echo "non-Snap node/npm come first in PATH."
        exit 1
        ;;
esac

# Enforce the Node.js 20+ requirement
NODE_VERSION=$(node --version 2>/dev/null | sed 's/^v//')
NODE_MAJOR=$(echo "$NODE_VERSION" | cut -d. -f1)

case "$NODE_MAJOR" in
    ''|*[!0-9]*)
        echo -e "${RED}Error: Unable to determine the Node.js version${NC}"
        echo "Please install Node.js v20 or newer and try again"
        exit 1
        ;;
esac

if [ "$NODE_MAJOR" -lt 20 ]; then
    echo -e "${RED}Error: Node.js 20+ is required (found ${NODE_VERSION})${NC}"
    echo "Please install Node.js v20 or newer and try again"
    exit 1
fi


echo -e "${GREEN}✓${NC} Node.js $(node --version) found"
echo -e "${GREEN}✓${NC} npm $(npm --version) found"


# Check for sudo
# WebZFS runs as the unprivileged webzfs user and requires sudo to execute
# approved ZFS and system administration commands. Some distributions,
# notably Proxmox VE, do not install sudo by default.
if ! command_exists sudo; then
    echo -e "${RED}Error: sudo is not installed${NC}"
    echo "WebZFS runs as an unprivileged service account and requires sudo"
    echo "to execute approved ZFS and system administration commands."
    exit 1
fi

echo -e "${GREEN}✓${NC} sudo found"

# Check for rsync
# The installer uses rsync to copy the source tree into /opt/webzfs.
# Minimal Linux installations, including EL10 minimal, may omit it.
if ! command_exists rsync; then
    echo -e "${RED}Error: rsync is not installed${NC}"
    echo "rsync is required to copy WebZFS application files."
    echo "Install it with the command for your distribution, then rerun this installer:"
    echo
    echo "  Debian/Ubuntu: sudo apt install rsync"
    echo "  RHEL/Fedora:   sudo dnf install rsync"
    echo "  Arch Linux:    sudo pacman -S rsync"
    echo "  openSUSE:      sudo zypper install rsync"
    exit 1
fi

echo -e "${GREEN}✓${NC} rsync found"

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
    --exclude='/install*.sh' --exclude='/update*.sh' \
    --exclude='/integrations/cockpit/install.sh' \
    "${SOURCE_DIR}/" "$INSTALL_DIR/"

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

# Set ownership
chown -R "$WEBZFS_USER:$WEBZFS_USER" "$INSTALL_DIR"

echo -e "${GREEN}✓${NC} Application files copied"

# Create application data directory and initialize data files
DATA_DIR="${INSTALL_DIR}/.config/webzfs"
mkdir -p "${DATA_DIR}/progress"
mkdir -p "${DATA_DIR}/logs"

# Pre-create JSON data files to avoid race conditions during worker startup
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
if [ ! -f "${DATA_DIR}/smart_test_history.json" ]; then
    echo '{"history": []}' > "${DATA_DIR}/smart_test_history.json"
fi
if [ ! -f "${DATA_DIR}/smart_scheduled_tests.json" ]; then
    echo '{}' > "${DATA_DIR}/smart_scheduled_tests.json"
fi
if [ ! -f "${DATA_DIR}/health_reports.json" ]; then
    echo '{"reports": []}' > "${DATA_DIR}/health_reports.json"
fi
if [ ! -f "${DATA_DIR}/health_schedules.json" ]; then
    echo '{"schedules": [], "next_id": 1}' > "${DATA_DIR}/health_schedules.json"
fi

chown -R "$WEBZFS_USER:$WEBZFS_USER" "$DATA_DIR"
echo -e "${GREEN}✓${NC} Data directory and files created"
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
EXPECTED_VERSION="$PYTHON_VERSION"

# Create virtual environment
# Recreate it if an existing venv was built with a different Python version,
# since native extension modules are not compatible across Python ABIs.
if [ -d ".venv" ]; then
    EXISTING_VERSION=""
    if [ -x ".venv/bin/python3" ]; then
        EXISTING_VERSION=\$(.venv/bin/python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))' 2>/dev/null || true)
    fi
    if [ "\$EXISTING_VERSION" = "\$EXPECTED_VERSION" ]; then
        echo "Virtual environment already exists (Python \$EXISTING_VERSION)"
    else
        echo "Existing virtual environment uses Python '\$EXISTING_VERSION', recreating with Python \$EXPECTED_VERSION..."
        rm -rf .venv
        \$PYTHON_PATH -m venv .venv
    fi
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

# SMART monitoring (multiple paths for different distributions)
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/smartctl, /usr/bin/smartctl, /sbin/smartctl

# Disk utilities
webzfs ALL=(ALL) NOPASSWD: /usr/bin/lsblk, /usr/bin/blkid

# Open file / lock inspection (pool export busy investigation)
webzfs ALL=(ALL) NOPASSWD: /usr/bin/lsof, /usr/bin/lslocks, /bin/lsof, /bin/lslocks

# Sanoid/Syncoid (optional)
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/sanoid, /usr/sbin/syncoid, /usr/bin/sanoid, /usr/bin/syncoid, /usr/local/sbin/sanoid, /usr/local/sbin/syncoid

# Service management (systemctl for system services page)
webzfs ALL=(ALL) NOPASSWD: /usr/bin/systemctl, /bin/systemctl

# Crontab editing
webzfs ALL=(ALL) NOPASSWD: /usr/bin/crontab

# Scheduled syncoid job timers.
# Unit files are created and edited with "sudo tee" (covered by the
# general tee entry below) and enabled/disabled/reloaded with
# "sudo systemctl" (covered by the systemctl entry above). The explicit
# tee entries here document that intent and keep timer management
# working even if the general tee entry is ever narrowed. rm is
# restricted to WebZFS-owned unit files only.
webzfs ALL=(ALL) NOPASSWD: /usr/bin/tee /etc/systemd/system/webzfs-syncoid-job-*, /bin/tee /etc/systemd/system/webzfs-syncoid-job-*
webzfs ALL=(ALL) NOPASSWD: /usr/bin/rm -f /etc/systemd/system/webzfs-syncoid-job-*, /bin/rm -f /etc/systemd/system/webzfs-syncoid-job-*

# Unified Scheduling Hub timers. All scheduled task types (scrub, SMART
# self-test, health check, and replication) use the webzfs-task-* unit
# naming scheme managed by services/job_scheduler.py.
webzfs ALL=(ALL) NOPASSWD: /usr/bin/tee /etc/systemd/system/webzfs-task-*, /bin/tee /etc/systemd/system/webzfs-task-*
webzfs ALL=(ALL) NOPASSWD: /usr/bin/rm -f /etc/systemd/system/webzfs-task-*, /bin/rm -f /etc/systemd/system/webzfs-task-*

# File editing (for config files like smartd.conf, sanoid.conf)
webzfs ALL=(ALL) NOPASSWD: /usr/bin/cat, /usr/bin/tee, /usr/bin/mkdir

# Read system journal and plain-text syslog files for the
# Observability -> System Log page. journalctl needs sudo (or
# systemd-journal group) on most distros. tail covers Debian/Ubuntu
# (/var/log/syslog) and old RHEL (/var/log/messages).
webzfs ALL=(ALL) NOPASSWD: /usr/bin/journalctl, /bin/journalctl, /usr/bin/tail, /bin/tail

# Support bundle log collection. Reading /var/log/messages and
# /var/log/syslog (typically mode 640 root:adm) and the kernel ring
# buffer requires elevated privileges for the unprivileged webzfs user.
webzfs ALL=(ALL) NOPASSWD: /usr/bin/grep, /bin/grep, /usr/bin/dmesg, /bin/dmesg
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
echo -e "${YELLOW}IMPORTANT: The WebUI binds to 127.0.0.1 by default and will NOT be${NC}"
echo -e "${YELLOW}reachable from other machines on your local network. To access it${NC}"
echo -e "${YELLOW}remotely, either change the HOST setting in $INSTALL_DIR/.env or use${NC}"
echo -e "${YELLOW}SSH port forwarding. See the 'Access' section of the README for details.${NC}"
echo
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo

