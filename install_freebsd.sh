#!/bin/sh

# WebZFS Installation Script for FreeBSD
# This script installs WebZFS to /opt/webzfs
# On FreeBSD, the application runs as root due to PAM authentication requirements

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
echo "WebZFS Installation Script for FreeBSD"
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

# Function to find Python 3.11+ on FreeBSD
# FreeBSD installs python311 as python3.11, not python3
find_python() {
    # Check for specific versions first (FreeBSD style)
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
    echo "Please install Python 3.11+ with: sudo pkg install python311"
    exit 1
fi

# Get the full path to Python for use in subshells
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
    echo "Please install Node.js with: sudo pkg install node npm"
    exit 1
fi

printf "${GREEN}✓${NC} Node.js $(node --version) found\n"

if ! command_exists npm; then
    printf "${RED}Error: npm is not installed${NC}\n"
    echo "Please install npm with: sudo pkg install npm"
    exit 1
fi

printf "${GREEN}✓${NC} npm $(npm --version) found\n"

# Check for ZFS (should be built-in on FreeBSD)
if ! command_exists zpool || ! command_exists zfs; then
    printf "${YELLOW}Warning: ZFS utilities not found in PATH${NC}\n"
    echo "ZFS should be available in the base system"
fi

# Check for smartmontools
if ! command_exists smartctl; then
    printf "${YELLOW}Warning: smartmontools not found${NC}\n"
    echo "Install smartmontools with: sudo pkg install smartmontools"
fi

# Check for Rust (needed to compile pydantic-core on FreeBSD)
if ! command_exists rustc; then
    printf "${RED}Error: Rust is not installed${NC}\n"
    echo "Rust is required to compile pydantic-core on FreeBSD."
    echo "Please install Rust with: sudo pkg install rust"
    exit 1
fi

printf "${GREEN}✓${NC} Rust $(rustc --version | cut -d' ' -f2) found\n"

# Check for libsodium (needed to compile pynacl on FreeBSD)
if [ ! -f "/usr/local/include/sodium.h" ]; then
    printf "${RED}Error: libsodium is not installed${NC}\n"
    echo "libsodium is required to compile pynacl on FreeBSD."
    echo "Please install it with: sudo pkg install libsodium"
    exit 1
fi

printf "${GREEN}✓${NC} libsodium found\n"

# Check for gmake (GNU make - required for compiling some Python packages)
if ! command_exists gmake; then
    printf "${RED}Error: GNU make (gmake) is not installed${NC}\n"
    echo "gmake is required to compile some Python packages on FreeBSD."
    echo "Please install it with: sudo pkg install gmake"
    exit 1
fi

printf "${GREEN}✓${NC} gmake found\n"

# Get the full path to gmake for use in the install script
GMAKE_PATH=$(command -v gmake)

echo

# Create installation directory if it doesn't exist
if [ ! -d "$INSTALL_DIR" ]; then
    echo "Creating installation directory: $INSTALL_DIR"
    mkdir -p "$INSTALL_DIR"
fi

# Copy application files to installation directory
echo "Copying application files from $SOURCE_DIR to $INSTALL_DIR..."

# Use tar instead of rsync (more portable on FreeBSD)
(cd "$SOURCE_DIR" && tar cf - --exclude='.venv' --exclude='node_modules' --exclude='.git' \
    --exclude='*.log' --exclude='__pycache__' --exclude='*.pyc' .) | \
    (cd "$INSTALL_DIR" && tar xf -)

printf "${GREEN}✓${NC} Application files copied\n"

# Create application data directory and initialize data files
DATA_DIR="${INSTALL_DIR}/.config/webzfs"
mkdir -p "${DATA_DIR}/progress"
mkdir -p "${DATA_DIR}/logs"

# Pre-create JSON data files to avoid race conditions during worker startup
# These files are created if they don't exist

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
echo

# Install dependencies
echo "Installing Python and Node.js dependencies..."
echo "(This may take a few minutes...)"
echo

cd "$INSTALL_DIR"

# Set environment for building
export HOME="$INSTALL_DIR"
export MAKE="$GMAKE_PATH"

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
    sed -i '' "s/CHANGE_ME_GENERATE_NEW_KEY/${NEW_SECRET_KEY}/" .env
    echo "Generated new SECRET_KEY"
fi

echo
printf "${GREEN}✓${NC} Python dependencies installed\n"
printf "${GREEN}✓${NC} Node.js dependencies installed\n"
printf "${GREEN}✓${NC} Static assets built\n"
printf "${GREEN}✓${NC} Configuration file created\n"
echo

# Create service wrapper script
SERVICE_SCRIPT="${INSTALL_DIR}/webzfs_service.sh"
echo "Creating service wrapper script..."

cat > "$SERVICE_SCRIPT" << 'SERVICE_EOF'
#!/bin/sh
# WebZFS Service Wrapper Script for FreeBSD
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

# Create rc.d service file
RC_SCRIPT="/usr/local/etc/rc.d/webzfs"
echo "Creating rc.d service script..."

cat > "$RC_SCRIPT" << 'RC_EOF'
#!/bin/sh

# PROVIDE: webzfs
# REQUIRE: LOGIN DAEMON NETWORKING
# KEYWORD: shutdown

. /etc/rc.subr

name="webzfs"
rcvar="webzfs_enable"

# WebZFS installation directory
webzfs_dir="/opt/webzfs"

pidfile="${webzfs_dir}/webzfs.pid"

# Custom start/stop commands
start_cmd="${name}_start"
stop_cmd="${name}_stop"
status_cmd="${name}_status"

# Helper function to get socket path from .env BIND variable
get_socket_path()
{
    local bind_value
    # Source the .env file if it exists
    if [ -f "${webzfs_dir}/.env" ]; then
        bind_value=$(grep -E '^BIND=' "${webzfs_dir}/.env" 2>/dev/null | cut -d'=' -f2- | tr -d '"' | tr -d "'")
    fi
    # Check if BIND starts with "unix:"
    case "$bind_value" in
        unix:*)
            echo "${bind_value#unix:}"
            ;;
        *)
            echo ""
            ;;
    esac
}

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
    
    # Handle unix socket directory creation
    socket_path=$(get_socket_path)
    if [ -n "$socket_path" ]; then
        socket_dir=$(dirname "$socket_path")
        # Create socket directory with 0755 permissions
        if [ ! -d "$socket_dir" ]; then
            echo "Creating socket directory: $socket_dir"
            mkdir -p "$socket_dir"
            chmod 0755 "$socket_dir"
        fi
        # Clean up stale socket file
        if [ -e "$socket_path" ]; then
            rm -f "$socket_path"
        fi
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
            for i in 1 2 3 4 5; do
                if ! kill -0 "$pid" 2>/dev/null; then
                    break
                fi
                sleep 1
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
    
    # Clean up unix socket if configured
    socket_path=$(get_socket_path)
    if [ -n "$socket_path" ] && [ -e "$socket_path" ]; then
        rm -f "$socket_path"
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
printf "${GREEN}✓${NC} rc.d service script created\n"

# Ask if user wants to enable the service
echo
printf "Do you want to enable WebZFS to start on boot? (y/n): "
read -r REPLY
if [ "$REPLY" = "y" ] || [ "$REPLY" = "Y" ]; then
    sysrc webzfs_enable="YES"
    printf "${GREEN}✓${NC} WebZFS service enabled\n"
    echo
    printf "Do you want to start WebZFS now? (y/n): "
    read -r REPLY2
    if [ "$REPLY2" = "y" ] || [ "$REPLY2" = "Y" ]; then
        service webzfs start
        printf "${GREEN}✓${NC} WebZFS service started\n"
        echo
        echo "Check service status with: sudo service webzfs status"
    fi
else
    echo "Service not enabled. You can enable it later with:"
    echo "  sudo sysrc webzfs_enable=YES"
    echo "  sudo service webzfs start"
fi

echo
echo "========================================"
printf "${GREEN}Installation Complete!${NC}\n"
echo "========================================"
echo
echo "WebZFS has been installed to: $INSTALL_DIR"
echo "Note: On FreeBSD, the service runs as root for PAM authentication"
echo
echo "To start the application manually:"
echo "  $INSTALL_DIR/run.sh"
echo
echo "To manage the service:"
echo "  sudo service webzfs start"
echo "  sudo service webzfs stop"
echo "  sudo service webzfs restart"
echo "  sudo service webzfs status"
echo
echo "To access the web interface:"
echo "  http://localhost:26619"
echo
echo "For more information, see: $INSTALL_DIR/BUILD_AND_RUN.md"
echo
