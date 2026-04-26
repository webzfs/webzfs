import os
import sys
import multiprocessing

# Calculate project directory and add to path
project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_dir)

# Load values from the project's .env file so that BIND_IP, PORT, BIND,
# SOCKET_UMASK, and WORKERS edits in /opt/webzfs/.env are honored on
# service restart. The application itself uses pydantic-settings to read
# .env, but gunicorn binds its socket before the app starts, so we must
# read the file here as well. python-dotenv is already a dependency.
#
# Existing process environment variables take precedence over .env values,
# matching pydantic-settings behavior and allowing systemd Environment=
# overrides to still work.
try:
    from dotenv import load_dotenv
    env_file_path = os.path.join(project_dir, '.env')
    if os.path.isfile(env_file_path):
        load_dotenv(dotenv_path=env_file_path, override=False)
except ImportError:
    # python-dotenv is listed in requirements.txt; if it is somehow missing
    # we fall back to whatever is already in the process environment.
    pass

wsgi_app = "config.asgi:app"
worker_class = "uvicorn.workers.UvicornWorker"

# Port 26619: Z(26th letter) F(6th letter) S(19th letter) = ZFS
#
# Bind configuration is read from /opt/webzfs/.env (or the process
# environment). To change the listen address, edit /opt/webzfs/.env and
# restart the service.
#
# TCP binding (default):
#   BIND_IP=127.0.0.1 PORT=26619  -> binds to 127.0.0.1:26619
#   BIND_IP=0.0.0.0               -> binds to 0.0.0.0:26619 (all interfaces - NOT RECOMMENDED)
#
# Unix socket binding:
#   BIND=unix:/run/webzfs/webzfs.sock  -> binds to unix socket
#
# For unix sockets, use the BIND environment variable directly.
# When using unix sockets, you'll need a reverse proxy (nginx/caddy) to access webzfs.
bind_env = os.getenv('BIND')
if bind_env:
    bind = bind_env
else:
    bind = f"{os.getenv('BIND_IP', '127.0.0.1')}:{os.getenv('PORT', '26619')}"

# Umask for unix socket file permissions
# This controls who can connect to the socket file.
# Common values:
#   0o007 - owner and group can access (default, recommended for reverse proxy)
#   0o077 - owner only (most restrictive)
#   0o000 - world readable/writable (not recommended)
# Only applies when using unix socket binding.
umask = int(os.getenv('SOCKET_UMASK', '0o007'), 8)

# Worker configuration
# For this light-duty application (data fetching and config files),
# 2-4 workers is more than sufficient
# Can be overridden via WORKERS environment variable
workers = int(os.getenv('WORKERS', 2))

# NOTE: With UvicornWorker (async), each worker handles many concurrent
# requests via async/await - you don't need many workers for I/O operations

preload_app = False  # Set to False to avoid preload issues with async
keepalive = 100
timeout = 120  # Increased timeout for long-running ZFS operations

accesslog = "-"
errorlog = "-"
loglevel = "info"

def on_starting(server):
    """Hook called when the master process is initialized"""
    # Ensure project directory is in Python path
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)

def when_ready(server):
    """Hook called when the server is ready to accept connections"""
    # Ensure project directory is in Python path
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)

def post_fork(server, worker):
    """Hook called after a worker has been forked"""
    # Ensure project directory is in Python path for each worker
    if project_dir not in sys.path:
        sys.path.insert(0, project_dir)
