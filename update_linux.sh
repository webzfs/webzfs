#!/bin/bash

# WebZFS Update Script for Linux
# This script updates an existing WebZFS installation at /opt/webzfs
# For initial installation, use install_linux.sh instead

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
LOG_FILE="${INSTALL_DIR}/update_log.txt"
WEBZFS_USER="webzfs"

# Determine the source directory (where this script is located)
SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "========================================"
echo "WebZFS Update Script for Linux"
echo "========================================"
echo

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    echo -e "${RED}Error: This script must be run as root${NC}"
    echo "Please run: sudo $0"
    exit 1
fi

# Verify installation exists
if [ ! -d "$INSTALL_DIR" ]; then
    echo -e "${RED}Error: WebZFS installation not found at $INSTALL_DIR${NC}"
    echo "Please run install_linux.sh for initial installation"
    exit 1
fi

# Verify virtual environment exists
if [ ! -d "$VENV_DIR" ]; then
    echo -e "${RED}Error: Virtual environment not found at $VENV_DIR${NC}"
    echo "Please run install_linux.sh for initial installation"
    exit 1
fi

# Verify essential files exist in source directory
ESSENTIAL_FILES=".env.example requirements.txt package.json"
for file in $ESSENTIAL_FILES; do
    if [ ! -f "${SOURCE_DIR}/${file}" ]; then
        echo -e "${RED}Error: Essential file '${file}' not found in ${SOURCE_DIR}${NC}"
        echo "Please run this script from the WebZFS source directory containing all application files."
        exit 1
    fi
done

# Verify webzfs user exists
if ! id "$WEBZFS_USER" &>/dev/null; then
    echo -e "${RED}Error: User '$WEBZFS_USER' does not exist${NC}"
    echo "Please run install_linux.sh for initial installation"
    exit 1
fi

if [ -x "${INSTALL_DIR}/.bun/bin/bun" ]; then
    JS_PACKAGE_MANAGER="bun"
    JS_PACKAGE_MANAGER_PATH="${INSTALL_DIR}/.bun/bin/bun"
    JS_PACKAGE_MANAGER_NAME="Bun"
    echo -e "${GREEN}✓${NC} Bun $($JS_PACKAGE_MANAGER_PATH --version) found"
elif command -v bun >/dev/null 2>&1; then
    JS_PACKAGE_MANAGER="bun"
    JS_PACKAGE_MANAGER_PATH="$(command -v bun)"
    JS_PACKAGE_MANAGER_NAME="Bun"
    echo -e "${GREEN}✓${NC} Bun $(bun --version) found"
elif ! command -v node >/dev/null 2>&1; then
    echo -e "${RED}Error: Neither Bun nor Node.js is installed${NC}"
    echo "Please install Bun or Node.js v20+ with npm and try again"
    exit 1
elif ! command -v npm >/dev/null 2>&1; then
    echo -e "${RED}Error: npm is not installed${NC}"
    echo "Please install npm or Bun and try again"
    exit 1
else
    echo -e "${GREEN}✓${NC} Node.js $(node --version) found"
    JS_PACKAGE_MANAGER="npm"
    JS_PACKAGE_MANAGER_PATH="$(command -v npm)"
    JS_PACKAGE_MANAGER_NAME="Node.js"
    echo -e "${GREEN}✓${NC} npm $(npm --version) found"
fi

if [ "$JS_PACKAGE_MANAGER" = "bun" ] && [ "$JS_PACKAGE_MANAGER_PATH" != "${INSTALL_DIR}/.bun/bin/bun" ]; then
    BUN_DEPLOY_PATH="${INSTALL_DIR}/.bun/bin/bun"
    mkdir -p "$(dirname "$BUN_DEPLOY_PATH")"
    install -m 0755 "$JS_PACKAGE_MANAGER_PATH" "$BUN_DEPLOY_PATH"
    JS_PACKAGE_MANAGER_PATH="$BUN_DEPLOY_PATH"
fi

# Check if service is running
SERVICE_WAS_RUNNING=false
if systemctl is-active --quiet webzfs 2>/dev/null; then
    SERVICE_WAS_RUNNING=true
    echo "Stopping WebZFS service..."
    systemctl stop webzfs
    echo -e "${GREEN}✓${NC} Service stopped"
fi

echo

# Copy application files to installation directory (preserving config)
echo "Updating application files from $SOURCE_DIR to $INSTALL_DIR..."
rsync -a --exclude='.venv' --exclude='node_modules' --exclude='.git' --exclude='*.log' \
    --exclude='__pycache__' --exclude='*.pyc' --exclude='.env' --exclude='.config' \
    --exclude='config/gunicorn.conf.py' \
    "${SOURCE_DIR}/" "$INSTALL_DIR/"

# Set ownership
chown -R "$WEBZFS_USER:$WEBZFS_USER" "$INSTALL_DIR"

echo -e "${GREEN}✓${NC} Application files updated"
echo

# Refresh sudo permissions so new privileged commands (for example grep and
# dmesg used by the support bundle log collectors) are whitelisted on existing
# installations. Writing the file on every update keeps it in sync with the
# installer.
SUDOERS_FILE="/etc/sudoers.d/webzfs"
echo "Refreshing sudo permissions..."

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
echo -e "${GREEN}✓${NC} Sudo permissions refreshed"
echo


# Update CAPTION in .env from .env.example
ENV_FILE="${INSTALL_DIR}/.env"
if [ -f "$ENV_FILE" ]; then
    # Extract new CAPTION from .env.example
    NEW_CAPTION=$(grep -E '^CAPTION=' "${SOURCE_DIR}/.env.example" | head -1)
    if [ -n "$NEW_CAPTION" ]; then
        # Update CAPTION in existing .env file
        if grep -q '^CAPTION=' "$ENV_FILE"; then
            sed -i "s|^CAPTION=.*|${NEW_CAPTION}|" "$ENV_FILE"
            echo -e "${GREEN}✓${NC} Updated CAPTION to: ${NEW_CAPTION}"
        else
            # CAPTION not found in .env, add it at the top
            echo "${NEW_CAPTION}" | cat - "$ENV_FILE" > "${ENV_FILE}.tmp" && mv "${ENV_FILE}.tmp" "$ENV_FILE"
            echo -e "${GREEN}✓${NC} Added CAPTION: ${NEW_CAPTION}"
        fi
        chown "$WEBZFS_USER:$WEBZFS_USER" "$ENV_FILE"
    fi
fi

echo

# Create a temporary update script that runs as the webzfs user
TEMP_UPDATE_SCRIPT="${INSTALL_DIR}/_update_deps.sh"
echo "Updating Python and ${JS_PACKAGE_MANAGER_NAME} dependencies as $WEBZFS_USER..."
echo "(This may take a few minutes...)"
echo

# Create the update script
cat > "$TEMP_UPDATE_SCRIPT" << UPDATE_EOF
#!/bin/bash
set -e

# Set HOME to the webzfs user's home directory for pip cache
export HOME="/opt/webzfs"

cd /opt/webzfs

JS_PACKAGE_MANAGER="$JS_PACKAGE_MANAGER_PATH"
JS_PACKAGE_MANAGER_TYPE="$JS_PACKAGE_MANAGER"
JS_PACKAGE_MANAGER_NAME="$JS_PACKAGE_MANAGER_NAME"

echo "Upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > update_log.txt 2>&1

echo "Updating Python dependencies..."
.venv/bin/pip install -r requirements.txt >> update_log.txt 2>&1

echo "Updating \$JS_PACKAGE_MANAGER_NAME dependencies..."
"\$JS_PACKAGE_MANAGER" install >> update_log.txt 2>&1

echo "Rebuilding static assets..."
if [ "\$JS_PACKAGE_MANAGER_TYPE" = "bun" ]; then
    "\$JS_PACKAGE_MANAGER" ./node_modules/postcss-cli/index.js src/styles.css -o static/css/styles.css >> update_log.txt 2>&1
else
    "\$JS_PACKAGE_MANAGER" run build:css >> update_log.txt 2>&1
fi

echo "Dependencies updated successfully!"
UPDATE_EOF

chmod +x "$TEMP_UPDATE_SCRIPT"
chown "$WEBZFS_USER:$WEBZFS_USER" "$TEMP_UPDATE_SCRIPT"

# Run the update script as the webzfs user
if ! su -s /bin/bash "$WEBZFS_USER" -c "bash $TEMP_UPDATE_SCRIPT"; then
    echo -e "${RED}Error: Update failed${NC}"
    echo "Check $LOG_FILE for details"
    rm -f "$TEMP_UPDATE_SCRIPT"
    exit 1
fi

# Clean up the temporary script
rm -f "$TEMP_UPDATE_SCRIPT"

echo
echo -e "${GREEN}✓${NC} Python dependencies updated"
echo -e "${GREEN}✓${NC} ${JS_PACKAGE_MANAGER_NAME} dependencies updated"
echo -e "${GREEN}✓${NC} Static assets rebuilt"
echo

# Restart service if it was running
if [ "$SERVICE_WAS_RUNNING" = true ]; then
    echo "Restarting WebZFS service..."
    systemctl daemon-reload
    systemctl start webzfs
    echo -e "${GREEN}✓${NC} Service restarted"
    echo
else
    echo "WebZFS service was not running before update."
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY
    if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
        systemctl daemon-reload
        systemctl start webzfs
        echo -e "${GREEN}✓${NC} WebZFS service started"
    fi
fi

echo
echo "========================================"
echo -e "${GREEN}Update Complete!${NC}"
echo "========================================"
echo
echo "WebZFS has been updated at: $INSTALL_DIR"
echo
echo "To check the service status:"
echo "  sudo systemctl status webzfs"
echo
echo "To view logs:"
echo "  sudo journalctl -u webzfs -f"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
