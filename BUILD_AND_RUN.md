# Build and Run Guide

This guide provides detailed instructions for building and running WebZFS after installation.

## Table of Contents

- [Installation](#installation)
- [Running the Application](#running-the-application)
- [Configuration](#configuration)
- [Development Mode](#development-mode)
- [System Service Setup](#system-service-setup)
- [Troubleshooting](#troubleshooting)

## Installation

### Using the Installation Scripts

WebZFS provides platform-specific installation scripts that handle the complete setup process.

**Linux:**
```bash
git clone https://github.com/webzfs/webzfs.git
cd webzfs
chmod +x install_linux.sh
sudo ./install_linux.sh
```

**FreeBSD:**
```bash
git clone https://github.com/webzfs/webzfs.git
cd webzfs
chmod +x install_freebsd.sh
sudo ./install_freebsd.sh
```

The installation scripts will:
1. Check for required dependencies (Python 3.11+, Node.js v20+, npm)
2. Create the `webzfs` system user (Linux Only)
3. Install the application to `/opt/webzfs`
4. Create a Python virtual environment
5. Install Python dependencies from `requirements.txt`
6. Install Node.js dependencies
7. Build static CSS assets
8. Create `.env` configuration file
9. Configure sudo permissions (Linux Only)

### Local Development Setup

If you want to develop directly from your git clone without installing to `/opt/webzfs`:

```bash
# Clone the repository
git clone https://github.com/webzfs/webzfs.git
cd webzfs

# Run the automated setup script
chmod +x setup_dev.sh
./setup_dev.sh
```

The `setup_dev.sh` script will:
- Check for required dependencies (Python 3.11+, Node.js v20+, npm, make, libsodium)
- Create a Python virtual environment (`.venv`) in your current directory
- Install all Python dependencies
- Install Node.js dependencies
- Build static CSS assets
- Create a `.env` configuration file with a secure SECRET_KEY

After setup, you can:
- Run the development server with `./run_dev.sh`
- Make changes and commit/push directly from this directory
- Rebuild CSS with `npm run build:css` or `npm run watch:css`

**Note:** You'll need sudo/root permissions when running the application to access ZFS commands. On Linux, you may need to configure sudo permissions manually - see the installation scripts for the required sudoers configuration.

### Manual Installation

If you prefer to install manually or to a custom location:

```bash
# Clone the repository
git clone https://github.com/webzfs/webzfs.git
cd webzfs

# Create Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Python dependencies
pip install -r requirements.txt

# Install Node.js dependencies
npm install

# Build static assets
npm run build:css

# Create configuration file
cp .env.example .env
```

**Note:** Manual installation on linux requires you to configure sudo permissions manually. See the installation scripts for the required sudoers configuration.

## Running the Application

### Standard Installation (in /opt/webzfs)

After running the installation script:

```bash
# Start the application
sudo -u webzfs /opt/webzfs/run.sh

# Or for development mode with auto-reload
sudo -u webzfs /opt/webzfs/run_dev.sh
```

The application will be accessible at `http://localhost:26619`

### Manual/Custom Installation

If you installed manually or to a custom location:

```bash
# Activate the virtual environment
source .venv/bin/activate

# Production mode
gunicorn -c config/gunicorn.conf.py

# Development mode with hot-reload
SETTINGS_MODULE=config.settings.dev gunicorn -c config/gunicorn.conf.py
```

Or use the convenience scripts:

```bash
./run.sh              # Production mode
./run_dev.sh          # Development mode
```

### Stopping the Application

```bash
# Find the process
ps aux | grep gunicorn

# Kill the process
pkill -f gunicorn

# Or if running as a service
sudo systemctl stop webzfs
```

## Configuration

### Environment Variables

The application is configured through `.env` file (located at `/opt/webzfs/.env` for standard installations):

```bash
# Application Settings
CAPTION="webzfs 0.55 alpha"
SECRET_KEY="change-this-in-production"
AUTH_SESSION_EXPIRES_SECONDS=3600

# Server Settings
HOST=127.0.0.1
PORT=26619
SETTINGS_MODULE=config.settings.base
```

### Important Configuration Notes

1. **SECRET_KEY**: Always change this in production. Generate a secure key:
   ```bash
   python3 -c "import secrets; print(secrets.token_hex(32))"
   ```

2. **HOST**: Default is `127.0.0.1` (localhost only). For remote access, use SSH port forwarding instead of changing this.

You can change this to 0.0.0.0 if you want it globaly availbale on your network.  This is not advised, but you do you.  I would recommend you port forward with SSH rather than exposing this on your network.

3. **PORT**: Default is `26619` (Z=26, F=6, S=19). Change if needed:
   ```bash
   # Edit .env
   PORT=8080
   ```

### Unix Socket Configuration

For deployments behind a reverse proxy (nginx, caddy), you can configure WebZFS to listen on a unix socket instead of a TCP port. This provides better security and slightly lower overhead for local communication.

**Enable unix socket binding:**

Edit your `.env` file:
```bash
# Unix socket binding (instead of IP:port)
BIND=unix:/run/webzfs/webzfs.sock

# Optional: Control socket file permissions (default: 0o007)
# 0o007 = owner and group can access (recommended for reverse proxy)
# 0o077 = owner only (most restrictive)
# 0o000 = world readable/writable (not recommended)
SOCKET_UMASK=0o007
```

**Platform-specific notes:**

- **Linux (systemd)**: The installation script configures `RuntimeDirectory=webzfs`, which automatically creates `/run/webzfs/` on service start and removes it on stop.

- **FreeBSD (rc.d)**: The rc.d script automatically creates the socket directory and cleans up stale sockets on service start/stop.

**Important: Reverse proxy group permissions**

The default socket umask (`0o007`) creates a socket that only the owner and group can access. Your reverse proxy user (nginx, www-data, caddy, etc.) must be added to the `webzfs` group to connect to the socket:

```bash
# Linux - Add nginx/www-data to webzfs group
sudo usermod -aG webzfs www-data    # Debian/Ubuntu
sudo usermod -aG webzfs nginx       # RHEL/Fedora/Arch
sudo usermod -aG webzfs caddy       # If using Caddy

# Restart the reverse proxy to pick up group changes
sudo systemctl restart nginx        # or caddy, etc.
```

Alternatively, set `SOCKET_UMASK=0o000` in `.env` to allow any user to connect (less secure, but simpler).

### Gunicorn Configuration

Advanced server settings can be configured in `config/gunicorn.conf.py`:

- Worker processes (default: auto-calculated based on CPU cores)
- Bind address and port
- Timeout settings
- Logging configuration

## Development Mode

### Running in Development

Development mode provides:
- Auto-reload on code changes
- Debug logging
- Development settings

```bash
cd /opt/webzfs
sudo -u webzfs ./run_dev.sh
```

Or manually:

```bash
source .venv/bin/activate
SETTINGS_MODULE=config.settings.dev gunicorn -c config/gunicorn.conf.py --reload
```

### Watching CSS Changes

If you're modifying Tailwind CSS:

```bash
source .venv/bin/activate
npm run watch:css
```

This will automatically rebuild CSS when you modify source files.

## System Service Setup

### Linux (systemd)

Create a systemd service file:

```bash
sudo nano /etc/systemd/system/webzfs.service
```

Add the following content:

```ini
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
```

Enable and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable webzfs
sudo systemctl start webzfs
```

Manage the service:

```bash
sudo systemctl status webzfs    # Check status
sudo systemctl stop webzfs      # Stop service
sudo systemctl restart webzfs   # Restart service
sudo journalctl -u webzfs -f    # View logs
```

### FreeBSD (rc.d)

Create an rc.d script at `/usr/local/etc/rc.d/webzfs`:

```bash
##!/bin/sh

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
```

Make it executable:

```bash
sudo chmod +x /usr/local/etc/rc.d/webzfs
```

Enable in `/etc/rc.conf`:

```bash
echo 'webzfs_enable="YES"' | sudo tee -a /etc/rc.conf
```

Manage the service:

```bash
sudo service webzfs start
sudo service webzfs stop
sudo service webzfs restart
sudo service webzfs status
```

## Troubleshooting

### Installation Issues

**Problem: Python version too old**

```bash
# Check Python version
python3 --version

# Install Python 3.11+ using your package manager
```

**Problem: Node.js not found or too old**

```bash
# Check Node.js version
node --version

# Install Node.js v20+ from your package manager or https://nodejs.org
```

**Problem: Virtual environment creation fails**

```bash
# On Linux, ensure python3-venv is installed
sudo apt install python3-venv         # Debian/Ubuntu
sudo dnf install python3              # RHEL/Fedora

# On FreeBSD, ensure pip is installed
sudo pkg install py311-pip
```

**Problem: npm install fails**

```bash
# Clear npm cache
npm cache clean --force

# Remove node_modules and try again
rm -rf node_modules package-lock.json
npm install
```

### Runtime Issues

**Problem: Permission denied errors**

```bash
# Verify webzfs user has sudo permissions
sudo -u webzfs sudo -l

# Check sudoers file
cat /etc/sudoers.d/webzfs              # Linux
cat /usr/local/etc/sudoers.d/webzfs   # FreeBSD

# Reinstall using the installation script to fix permissions
```

**Problem: Port 26619 already in use**

```bash
# Find what's using the port
sudo lsof -i :26619                    # Linux
sudo sockstat -4 -l | grep 26619       # FreeBSD

# Either kill that process or change the port in .env
```

**Problem: CSS not loading or looks broken**

```bash
# Rebuild static assets
cd /opt/webzfs
sudo -u webzfs bash -c 'source .venv/bin/activate && npm run build:css'

# Verify files were created
ls -la static/css/styles.css
```

**Problem: ZFS commands not found**

The application automatically detects ZFS binary paths. If commands are not found:

```bash
# Verify ZFS is installed
which zpool
which zfs

# Check services/utils.py for binary path detection logic
```

**Problem: Authentication fails**

WebZFS uses PAM authentication:

```bash
# Ensure PAM libraries are available
python3 -c "import pam"

# If that fails, reinstall python-pam
source .venv/bin/activate
pip install --force-reinstall python-pam
```

### Logging

View application logs:

```bash
# If running as systemd service (Linux)
sudo journalctl -u webzfs -f

# If running manually, check the console output
# or enable logging in config/gunicorn.conf.py
```

### Complete Reinstall

If all else fails, perform a clean reinstall:

```bash
# Stop the service
sudo systemctl stop webzfs  # or sudo service webzfs stop

# Remove installation
sudo rm -rf /opt/webzfs

# Remove webzfs user
sudo userdel webzfs   # Linux
sudo pw userdel webzfs # FreeBSD

# Remove sudoers file
sudo rm /etc/sudoers.d/webzfs                    # Linux
sudo rm /usr/local/etc/sudoers.d/webzfs         # FreeBSD

# Run installation script again
cd webzfs
sudo ./install_linux.sh    # or install_freebsd.sh
```

## Updating

To update an existing installation:

```bash
cd /opt/webzfs

# Pull latest changes
sudo -u webzfs git pull

# Update dependencies and rebuild
sudo -u webzfs bash << 'EOF'
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
npm install
npm run build:css
EOF

# Restart the service
sudo systemctl restart webzfs          # Linux
sudo service webzfs restart            # FreeBSD
```

## Additional Resources

- **[README.md](README.md)** - Project overview and quick start
- **[add_feature_demo/README.md](add_feature_demo/README.md)** - Guide for adding new features
- **OpenZFS Documentation**: https://openzfs.github.io/openzfs-docs/
- **FastAPI Documentation**: https://fastapi.tiangolo.com/
- **Uvicorn Documentation**: https://uvicorn.dev
- **Tailwind CSS Documentation**: https://tailwindcss.com/docs
- **HTMX Documentation** : https://htmx.org

## Support

For issues, questions, or contributions:
- **GitHub Repository**: https://github.com/webzfs/webzfs
- **Report Issues**: Use the GitHub issue tracker
