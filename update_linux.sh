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
    "${SOURCE_DIR}/" "$INSTALL_DIR/"

# Set ownership
chown -R "$WEBZFS_USER:$WEBZFS_USER" "$INSTALL_DIR"

echo -e "${GREEN}✓${NC} Application files updated"
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
echo "Updating Python and Node.js dependencies as $WEBZFS_USER..."
echo "(This may take a few minutes...)"
echo

# Create the update script
cat > "$TEMP_UPDATE_SCRIPT" << UPDATE_EOF
#!/bin/bash
set -e

# Set HOME to the webzfs user's home directory for pip cache
export HOME="/opt/webzfs"

cd /opt/webzfs

echo "Upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > update_log.txt 2>&1

echo "Updating Python dependencies..."
.venv/bin/pip install -r requirements.txt >> update_log.txt 2>&1

echo "Updating Node.js dependencies..."
npm install >> update_log.txt 2>&1

echo "Rebuilding static assets..."
npm run build:css >> update_log.txt 2>&1

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
echo -e "${GREEN}✓${NC} Node.js dependencies updated"
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
