#!/bin/sh

# WebZFS Update Script for FreeBSD
# This script updates an existing WebZFS installation at /opt/webzfs
# For initial installation, use install_freebsd.sh instead

set -e

INSTALL_DIR="/opt/webzfs"
VENV_DIR="${INSTALL_DIR}/.venv"
LOG_FILE="${INSTALL_DIR}/update_log.txt"

# Determine the source directory (where this script is located)
SOURCE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

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
EOF

# Create a backup tar of the source, excluding unwanted files
(cd "$SOURCE_DIR" && tar cf - --exclude-from="$EXCLUDE_FILE" .) | \
    (cd "$INSTALL_DIR" && tar xf -)

rm -f "$EXCLUDE_FILE"

printf "${GREEN}✓${NC} Application files updated\n"
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

echo "Upgrading pip in virtual environment..."
.venv/bin/python3 -m pip install --upgrade pip > update_log.txt 2>&1

echo "Updating Python dependencies..."
.venv/bin/pip install -r requirements.txt >> update_log.txt 2>&1

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
