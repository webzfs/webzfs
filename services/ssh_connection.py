"""
SSH Connection Management Service
Centralized SSH connection management for Fleet Monitoring and ZFS Replication
"""
import ipaddress
import json
import os
import re
import uuid
from contextlib import contextmanager
import subprocess
import shutil
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
import paramiko
import logging

from services.json_state import (
    StateFileError,
    file_lock,
    read_json,
    write_json_atomic,
    write_text_atomic,
)

logger = logging.getLogger(__name__)


class HostKeyVerificationError(Exception):
    """Raised when a remote SSH host key is unknown, unpinned, or changed."""


# SSH destination validation.
#
# Usernames and hosts end up as command line arguments to ssh, ssh-keyscan,
# ssh-copy-id and syncoid (which passes them through a shell). A value that
# starts with '-' is parsed by ssh as an option, for example
# '-oProxyCommand=<command>', which runs a local command. Only plain
# usernames, DNS hostnames and IP addresses are accepted.
SSH_USERNAME_PATTERN = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,31}')
SSH_HOSTNAME_LABEL_PATTERN = re.compile(r'[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?')


def validate_ssh_username(username: str) -> str:
    """Return username if it is a plain account name, else raise ValueError."""
    if not isinstance(username, str) or not SSH_USERNAME_PATTERN.fullmatch(username):
        raise ValueError(
            "Invalid SSH username. Use letters, digits, '.', '_' or '-' "
            "(up to 32 characters, not starting with '-' or '.')."
        )
    return username


def validate_ssh_host(host: str) -> str:
    """Return host if it is an IP address or DNS hostname, else raise ValueError."""
    if not isinstance(host, str) or not host:
        raise ValueError("SSH host is required.")
    try:
        ipaddress.ip_address(host)
        return host
    except ValueError:
        pass
    labels = host.rstrip('.').split('.')
    if (
        len(host) > 253
        or not all(SSH_HOSTNAME_LABEL_PATTERN.fullmatch(label) for label in labels)
    ):
        raise ValueError(
            "Invalid SSH host. Use an IP address or a hostname made of letters, "
            "digits, '-' and '.'."
        )
    return host


def validate_ssh_port(port) -> int:
    """Return port as an int if it is 1 to 65535, else raise ValueError."""
    try:
        port_number = int(port)
    except (TypeError, ValueError):
        raise ValueError("SSH port must be a number.")
    if not 1 <= port_number <= 65535:
        raise ValueError("SSH port must be between 1 and 65535.")
    return port_number


MAX_CONNECTION_NAME_LENGTH = 100
MAX_CONNECTION_NOTES_LENGTH = 2000


def validate_connection_text(name: str, notes: str = "") -> None:
    """Check the free-text name and notes of a connection.

    Both are only displayed (autoescaped) and never placed on a command
    line; this bounds their size and keeps control characters out.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Connection name is required.")
    if len(name) > MAX_CONNECTION_NAME_LENGTH:
        raise ValueError(f"Connection name is limited to {MAX_CONNECTION_NAME_LENGTH} characters.")
    if any(ord(char) < 32 for char in name):
        raise ValueError("Connection name must not contain control characters.")
    if len(notes or "") > MAX_CONNECTION_NOTES_LENGTH:
        raise ValueError(f"Notes are limited to {MAX_CONNECTION_NOTES_LENGTH} characters.")


def validate_ssh_destination(destination: str) -> str:
    """Validate 'user@host' or a bare 'host'; return it unchanged."""
    if not isinstance(destination, str) or not destination:
        raise ValueError("SSH destination is required.")
    if '@' in destination:
        username, host = destination.rsplit('@', 1)
        validate_ssh_username(username)
    else:
        host = destination
    validate_ssh_host(host)
    return destination


# Remote authorized_keys maintenance.
#
# Both commands are run through the remote user's login shell, which may be
# sh, bash, zsh, csh or tcsh (FreeBSD root used csh before 14.0). They are
# therefore wrapped as /bin/sh -c '<script>': a single-quoted string means
# the same thing in all of those shells, and the script itself is POSIX sh
# that works with BSD and GNU tools (no sed -i, which differs between them).
# Neither script contains a single quote.

# Appends the public key read from stdin. Nothing user-supplied is placed in
# the command. If the file does not end with a newline, one is added first so
# the new key does not get joined onto the last existing line.
INSTALL_KEY_SCRIPT = (
    'umask 077; '
    'mkdir -p "$HOME/.ssh" && chmod 700 "$HOME/.ssh" || exit 1; '
    'f="$HOME/.ssh/authorized_keys"; '
    'if [ -s "$f" ] && [ -n "$(tail -c 1 "$f")" ]; then echo >> "$f" || exit 1; fi; '
    'cat >> "$f" && chmod 600 "$f"'
)

# Removes every line containing the key material (the base64 field, so the
# comment does not matter), then confirms it is gone. Exit status 0 only when
# the key is no longer present. KEY_BODY is replaced with a value checked
# against PUBLIC_KEY_BODY_PATTERN.
REMOVE_KEY_SCRIPT = (
    'umask 077; '
    'f="$HOME/.ssh/authorized_keys"; '
    '[ -f "$f" ] || exit 0; '
    't="$f.webzfs-tmp.$$"; '
    'grep -v -F -e "KEY_BODY" "$f" > "$t"; '
    'if [ $? -gt 1 ]; then rm -f "$t"; exit 2; fi; '
    'mv "$t" "$f" && chmod 600 "$f" || exit 2; '
    'if grep -q -F -e "KEY_BODY" "$f"; then exit 3; fi; '
    'exit 0'
)

PUBLIC_KEY_BODY_PATTERN = re.compile(r'[A-Za-z0-9+/]{16,}={0,3}')


def remote_shell_command(script: str) -> str:
    """Wrap a POSIX sh script so any remote login shell runs it unchanged."""
    if "'" in script:
        raise ValueError("Remote script must not contain a single quote.")
    return f"/bin/sh -c '{script}'"


def public_key_body(public_key_line: str) -> str:
    """Return the base64 key material of an OpenSSH public key line."""
    fields = public_key_line.split()
    if len(fields) < 2 or not PUBLIC_KEY_BODY_PATTERN.fullmatch(fields[1]):
        raise ValueError("Not a valid OpenSSH public key line.")
    return fields[1]


def remove_key_command(public_key_line: str) -> str:
    """Remote command that removes this public key from authorized_keys."""
    return remote_shell_command(
        REMOVE_KEY_SCRIPT.replace("KEY_BODY", public_key_body(public_key_line))
    )


# Substrings that OpenSSH and paramiko use when host key verification fails.
HOST_KEY_FAILURE_MARKERS = (
    'host key verification failed',
    'remote host identification has changed',
    'no matching host key',
    'not found in known_hosts',
)


def is_host_key_error(text: str) -> bool:
    """Return True when SSH output describes a host-key verification failure."""
    lowered = (text or '').lower()
    return any(marker in lowered for marker in HOST_KEY_FAILURE_MARKERS)


def normalize_known_hosts_host(host: str) -> str:
    """
    Strip a leading ``user@`` from an SSH destination.

    Destinations coming from the replication UI are ``username@host`` (the
    form ssh itself expects), while known_hosts stores the bare host.
    Normalising before a known_hosts lookup keeps an already pinned host
    recognised instead of being reported as unpinned.
    """
    if not host:
        return host
    return host.rsplit('@', 1)[-1]


def host_key_error_message(host: str, port: int, detail: str = '') -> str:
    """Build a clear, user-facing host-key error message."""
    message = (
        f"SSH host key verification failed for {host}:{port}. "
        "The remote host key is not pinned in WebZFS's known_hosts file, "
        "or it has changed since it was pinned. Add this host under SSH "
        "Connections first; if the key change is expected, use the "
        "'Trust host key' action to re-pin it."
    )
    if detail:
        message = f"{message} Details: {detail}"
    return message



class SSHConnectionService:
    """Service for managing SSH connections with key-based authentication"""
    
    def __init__(self):
        """Initialize the SSH connection service"""
        # Set up config directory
        self.config_dir = Path.home() / ".config" / "webzfs"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up connections file. Every change to it holds the lock file
        # so Gunicorn workers cannot overwrite each other's changes.
        self.connections_file = self.config_dir / "ssh_connections.json"
        self.connections_lock_file = self.config_dir / "ssh_connections.lock"

        # Set up SSH keys directory.
        # Paths are resolved to absolute paths so they remain valid when
        # commands are executed under sudo where HOME may differ.
        self.keys_dir = (Path.home() / ".ssh" / "webzfs_connections").resolve()
        self.keys_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        
        # WebZFS-owned known_hosts database. All SSH commands built by this
        # service use this file with strict host key checking instead of
        # disabling host key verification.
        self.known_hosts_file = (Path.home() / ".ssh" / "webzfs_known_hosts").resolve()
        self.known_hosts_lock_file = self.known_hosts_file.with_name("webzfs_known_hosts.lock")

        # Load connections from disk
        self.connections_data = self._load_connections()
    
    # Connection Management Methods
    
    def list_connections(self) -> List[Dict[str, Any]]:
        """
        List all configured SSH connections
        
        Returns:
            List of connection configurations
        """
        # Reload from disk to get latest connections (in case another instance modified them)
        self.connections_data = self._load_connections()
        return self.connections_data.get("connections", [])
    
    def get_connection(self, connection_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a specific SSH connection
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            Connection configuration or None if not found
        """
        # Reload from disk to get latest connections (in case another instance modified them)
        self.connections_data = self._load_connections()
        for conn in self.connections_data.get("connections", []):
            if conn["id"] == connection_id:
                return conn
        return None
    
    def create_connection(
        self,
        name: str,
        host: str,
        username: str,
        password: str,
        port: int = 22,
        notes: str = "",
        confirmed_fingerprints: Optional[List[str]] = None
    ) -> str:
        """
        Create a new SSH connection with automatic key setup
        
        Steps:
        1. Generate unique connection ID
        2. Generate ED25519 SSH key pair
        3. Test password authentication
        4. Copy public key to remote server
        5. Test key authentication
        6. Save connection (password is discarded)
        7. Return connection ID
        
        Args:
            name: Human-readable connection name
            host: IP address or hostname
            username: SSH username
            password: SSH password (used once, then discarded)
            port: SSH port (default 22)
            notes: Optional notes about the connection
            confirmed_fingerprints: Host key fingerprints the administrator
                reviewed and approved before submitting (required). The host
                key is pinned only if the remote key still matches, so the
                password is never sent to a host whose key was not
                confirmed. They are stored on the record and used to restore
                a lost pin (see ensure_host_key_trusted).
            
        Returns:
            connection_id: UUID of created connection
            
        Raises:
            Exception: If key generation, distribution, or testing fails
        """
        # Validate before any key is generated or any network call is made.
        validate_connection_text(name, notes)
        validate_ssh_host(host)
        validate_ssh_username(username)
        port = validate_ssh_port(port)
        if not confirmed_fingerprints:
            # No pinning on first use: the password must only go to a host
            # whose key the administrator has seen and approved.
            raise ValueError(
                "Use 'Check host key' and verify the fingerprint before creating "
                "the connection. The password is only sent to a host whose key "
                "you approved."
            )

        connection_id = str(uuid.uuid4())

        try:
            # Generate SSH key pair
            private_key_path, public_key_path = self._generate_ssh_key(
                connection_id,
                # Fixed comment (no user-supplied text), so the key line is
                # safe in any remote command and identifies its connection.
                f"webzfs-{connection_id}"
            )
            
            # Pin and verify the remote host key BEFORE any password is
            # sent. The pin is bound to the exact fingerprints the
            # administrator reviewed on the add form; a key that changed in
            # between aborts without sending the password.
            host_key_fingerprints = self.confirm_host_key(
                host, port, confirmed_fingerprints
            )

            # Copy public key to remote server (now against a pinned host)
            success = self._copy_key_to_remote(host, port, username, password, public_key_path)
            if not success:
                raise Exception("Failed to copy SSH key to remote server. Check credentials and network connectivity.")
            
            # Test key-based authentication
            if not self._test_key_auth(host, port, username, private_key_path):
                raise Exception("SSH key authentication test failed. Key may not have been installed correctly.")
            
            # Get key fingerprint
            fingerprint = self._get_key_fingerprint(public_key_path)
            
            # Create connection record (password is NOT stored)
            connection = {
                "id": connection_id,
                "name": name,
                "host": host,
                "port": port,
                "username": username,
                "private_key_path": str(private_key_path),
                "public_key_path": str(public_key_path),
                "fingerprint": fingerprint,
                "host_key_fingerprints": host_key_fingerprints,
                "created_at": datetime.now().isoformat(),
                "last_used": None,
                "last_tested": datetime.now().isoformat(),
                "status": "active",
                "used_by": [],
                "notes": notes
            }
            
            # Add to the connections file under the lock. The slow network
            # steps above run outside the lock.
            with self._locked_connections() as data:
                data["connections"].append(connection)

            logger.info(f"Created SSH connection: {name} ({connection_id})")
            return connection_id
            
        except Exception as e:
            # Clean up key files if they were created
            try:
                if 'private_key_path' in locals():
                    Path(private_key_path).unlink(missing_ok=True)
                if 'public_key_path' in locals():
                    Path(public_key_path).unlink(missing_ok=True)
            except:
                pass
            # Keep host-key failures distinguishable for callers/UI.
            if isinstance(e, HostKeyVerificationError):
                raise
            raise Exception(f"Failed to create SSH connection: {str(e)}")
    
    def update_connection(
        self,
        connection_id: str,
        name: Optional[str] = None,
        host: Optional[str] = None,
        username: Optional[str] = None,
        port: Optional[int] = None,
        notes: Optional[str] = None
    ) -> None:
        """
        Update an existing SSH connection
        
        Note: Cannot update keys - must recreate connection for new keys
        
        Args:
            connection_id: Connection UUID
            name: New name (optional)
            host: New host (optional)
            username: New username (optional)
            port: New port (optional)
            notes: New notes (optional)
            
        Raises:
            Exception: If connection not found
        """
        if name is not None or notes is not None:
            validate_connection_text(name if name is not None else "unchanged", notes or "")
        if host is not None:
            validate_ssh_host(host)
        if username is not None:
            validate_ssh_username(username)
        if port is not None:
            port = validate_ssh_port(port)

        found = False
        with self._locked_connections() as data:
            for conn in data["connections"]:
                if conn["id"] == connection_id:
                    if name is not None:
                        conn["name"] = name
                    if host is not None:
                        conn["host"] = host
                    if username is not None:
                        conn["username"] = username
                    if port is not None:
                        conn["port"] = port
                    if notes is not None:
                        conn["notes"] = notes
                    found = True
                    break

        if not found:
            raise Exception(f"Connection {connection_id} not found")
        logger.info(f"Updated SSH connection: {connection_id}")
    
    def delete_connection(self, connection_id: str, remove_from_remote: bool = False) -> Optional[str]:
        """
        Delete an SSH connection

        Args:
            connection_id: Connection UUID
            remove_from_remote: Whether to remove key from remote server

        Returns:
            None on success. When remove_from_remote was requested but the
            key could not be confirmed removed, a message telling the user
            which authorized_keys line to remove by hand. The local record
            and key files are deleted either way.

        Raises:
            Exception: If connection not found
        """
        conn = self.get_connection(connection_id)
        if not conn:
            raise Exception(f"Connection {connection_id} not found")

        # Optionally remove the key from the remote server first; it needs
        # the private key, and it is slow, so it runs outside the lock.
        remote_error = None
        if remove_from_remote and not self._remove_key_from_remote(conn):
            try:
                key_comment = Path(conn["public_key_path"]).read_text().split()[-1]
            except Exception:
                key_comment = conn.get("fingerprint", "this connection's key")
            remote_error = (
                f"The key could not be removed from {conn['username']}@{conn['host']}. "
                f"It is still authorized there: remove the line ending in "
                f"'{key_comment}' from ~/.ssh/authorized_keys on that host."
            )

        # Remove the record under the lock.
        with self._locked_connections() as data:
            data["connections"] = [
                item for item in data["connections"] if item["id"] != connection_id
            ]

        # Delete local key files only after the record is gone, so a
        # failure never leaves a record pointing at missing keys.
        try:
            Path(conn["private_key_path"]).unlink(missing_ok=True)
            Path(conn["public_key_path"]).unlink(missing_ok=True)
        except Exception as e:
            logger.warning(f"Failed to delete key files: {e}")

        logger.info(f"Deleted SSH connection: {connection_id}")
        if remote_error:
            logger.warning(remote_error)
        return remote_error
    
    def test_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Test an SSH connection
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            Test results with status and message
        """
        try:
            conn = self.get_connection(connection_id)
            if not conn:
                return {
                    "status": "error",
                    "message": f"Connection {connection_id} not found"
                }
            
            # Test connection with key (network, outside the lock)
            succeeded = self._test_key_auth(
                conn["host"],
                conn["port"],
                conn["username"],
                conn["private_key_path"]
            )

            # Record the result on the current version of the record.
            with self._locked_connections() as data:
                for item in data["connections"]:
                    if item["id"] == connection_id:
                        item["status"] = "active" if succeeded else "error"
                        if succeeded:
                            item["last_tested"] = datetime.now().isoformat()

            if succeeded:
                return {
                    "status": "success",
                    "message": f"Successfully connected to {conn['host']}"
                }
            return {
                "status": "error",
                "message": f"Failed to connect to {conn['host']}"
            }
                
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    def mark_connection_used(self, connection_id: str, feature: str) -> None:
        """
        Mark a connection as being used by a feature
        
        Args:
            connection_id: Connection UUID
            feature: Feature name (e.g., 'fleet', 'replication')
        """
        conn = self.get_connection(connection_id)
        if not conn or feature in conn.get("used_by", []):
            return
        with self._locked_connections() as data:
            for item in data["connections"]:
                if item["id"] == connection_id and feature not in item.get("used_by", []):
                    item.setdefault("used_by", []).append(feature)
                    item["last_used"] = datetime.now().isoformat()
    
    # SSH Key Management Methods
    
    def _generate_ssh_key(self, connection_id: str, comment: str) -> Tuple[Path, Path]:
        """
        Generate ED25519 SSH key pair
        
        Args:
            connection_id: Unique connection identifier
            comment: Comment for the key
            
        Returns:
            Tuple of (private_key_path, public_key_path)
            
        Raises:
            Exception: If key generation fails
        """
        private_key_path = self.keys_dir / f"{connection_id}"
        public_key_path = self.keys_dir / f"{connection_id}.pub"
        
        try:
            # Generate key using ssh-keygen
            cmd = [
                'ssh-keygen',
                '-t', 'ed25519',
                '-f', str(private_key_path),
                '-N', '',  # No passphrase
                '-C', comment
            ]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=30
            )
            
            # Set proper permissions
            os.chmod(private_key_path, 0o600)
            os.chmod(public_key_path, 0o644)
            
            logger.info(f"Generated SSH key pair for connection {connection_id}")
            return private_key_path, public_key_path
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to generate SSH key: {e.stderr}")
        except Exception as e:
            raise Exception(f"Failed to generate SSH key: {str(e)}")
    
    def _copy_key_to_remote(
        self,
        host: str,
        port: int,
        username: str,
        password: str,
        public_key_path: Path
    ) -> bool:
        """
        Copy SSH public key to remote server's authorized_keys
        
        Methods (in order of preference):
        1. Use ssh-copy-id with sshpass if available
        2. Use paramiko to manually append to authorized_keys
        
        Args:
            host: Remote hostname or IP
            port: SSH port
            username: SSH username
            password: SSH password
            public_key_path: Path to public key file
            
        Returns:
            True if successful, False otherwise
        """
        public_key = public_key_path.read_text().strip()
        
        # Never send the password to an unverified host. The key must
        # already be pinned (create_connection pins the confirmed key
        # first); a key that changed since raises HostKeyVerificationError.
        if not self.is_host_key_pinned(host, port):
            raise HostKeyVerificationError(
                host_key_error_message(host, port, "host key has not been confirmed")
            )
        self._trust_host_key(host, port)
        host_key_options = self.get_host_key_options()
        
        # Method 1: Try ssh-copy-id with sshpass
        if shutil.which('sshpass') and shutil.which('ssh-copy-id'):
            try:
                # The password is passed in the SSHPASS environment variable
                # (sshpass -e), never on the command line. Command lines are
                # readable by every local user (ps, /proc/<pid>/cmdline) and
                # appear in TimeoutExpired messages that get logged; a
                # process environment is readable only by its owner and root.
                cmd = [
                    'sshpass', '-e',
                    'ssh-copy-id',
                    '-i', str(public_key_path),
                    '-p', str(port),
                ]
                for option in host_key_options:
                    cmd.extend(['-o', option])
                # '--' ends option parsing so the destination can never be
                # read as an option.
                cmd.extend(['--', f'{username}@{host}'])

                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30,
                    env={**os.environ, 'SSHPASS': password},
                )
                
                if result.returncode == 0:
                    logger.info(f"Copied SSH key to {host} using ssh-copy-id")
                    return True
                if is_host_key_error(result.stderr):
                    raise HostKeyVerificationError(
                        host_key_error_message(host, port, result.stderr.strip())
                    )
            except HostKeyVerificationError:
                raise
            except Exception as e:
                logger.warning(f"ssh-copy-id failed: {e}, trying paramiko method")
        
        # Method 2: Use paramiko
        client = paramiko.SSHClient()
        self.configure_paramiko_client(client)
        
        try:
            # Connect with password
            client.connect(
                hostname=host,
                port=port,
                username=username,
                password=password,
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            
            # Append the public key to authorized_keys. The key is sent on
            # stdin, so no part of it is placed in a shell command.
            stdin, stdout, stderr = client.exec_command(
                remote_shell_command(INSTALL_KEY_SCRIPT)
            )
            stdin.write(public_key + "\n")
            stdin.channel.shutdown_write()
            exit_status = stdout.channel.recv_exit_status()
            
            client.close()
            
            if exit_status == 0:
                logger.info(f"Copied SSH key to {host} using paramiko")
                return True
            else:
                logger.error(f"Failed to append key to authorized_keys: {stderr.read().decode()}")
                return False
            
        except paramiko.AuthenticationException:
            logger.error("Authentication failed - invalid password")
            return False
        except paramiko.SSHException as e:
            logger.error(f"SSH error: {e}")
            self.raise_for_paramiko_host_key_error(e, host, port)
            return False
        except Exception as e:
            logger.error(f"Failed to copy key via paramiko: {e}")
            return False
        finally:
            try:
                client.close()
            except:
                pass
    
    def _remove_key_from_remote(self, connection: Dict[str, Any]) -> bool:
        """
        Remove SSH public key from remote server's authorized_keys
        
        Args:
            connection: Connection configuration
            
        Returns:
            True only when the remote command confirmed the key is no longer
            in authorized_keys, False otherwise.
        """
        client = paramiko.SSHClient()
        try:
            public_key = Path(connection["public_key_path"]).read_text().strip()
            command = remove_key_command(public_key)

            self.configure_paramiko_client(client)

            # Connect using the key we're about to remove (it still works until we remove it)
            key = paramiko.Ed25519Key.from_private_key_file(connection["private_key_path"])

            client.connect(
                hostname=connection["host"],
                port=connection["port"],
                username=connection["username"],
                pkey=key,
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )

            stdin, stdout, stderr = client.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()
            if exit_status != 0:
                logger.error(
                    f"Removing key from {connection['host']} failed "
                    f"(exit {exit_status}): {stderr.read().decode(errors='replace').strip()}"
                )
                return False

            logger.info(f"Removed SSH key from {connection['host']}")
            return True

        except Exception as e:
            logger.error(f"Failed to remove key from remote: {e}")
            return False
        finally:
            try:
                client.close()
            except Exception:
                pass
    
    def _test_key_auth(self, host: str, port: int, username: str, private_key_path: Path) -> bool:
        """
        Test SSH key-based authentication
        
        Args:
            host: Remote hostname or IP
            port: SSH port
            username: SSH username
            private_key_path: Path to private key
            
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            client = paramiko.SSHClient()
            self.configure_paramiko_client(client)
            
            # Load the private key
            key = paramiko.Ed25519Key.from_private_key_file(str(private_key_path))
            
            # Test connection
            client.connect(
                hostname=host,
                port=port,
                username=username,
                pkey=key,
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            
            # Execute a simple command to verify
            stdin, stdout, stderr = client.exec_command('echo "test"')
            result = stdout.read().decode().strip()
            
            client.close()
            
            return result == "test"
            
        except Exception as e:
            self.raise_for_paramiko_host_key_error(e, host, port)
            logger.error(f"Key authentication test failed: {e}")
            return False
    
    def _get_key_fingerprint(self, public_key_path: Path) -> str:
        """
        Get SSH key fingerprint
        
        Args:
            public_key_path: Path to public key file
            
        Returns:
            Key fingerprint string
        """
        try:
            cmd = ['ssh-keygen', '-lf', str(public_key_path)]
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=10
            )
            
            # Extract fingerprint from output
            # Format: "256 SHA256:XXXX comment (ED25519)"
            parts = result.stdout.strip().split()
            if len(parts) >= 2:
                return parts[1]  # Return the SHA256:XXXX part
            return "Unknown"
            
        except Exception as e:
            logger.warning(f"Failed to get key fingerprint: {e}")
            return "Unknown"
    
    # Host Key Trust Management
    
    def _scan_host_keys(self, host: str, port: int) -> List[str]:
        """Return the remote host key lines reported by ssh-keyscan."""
        host = validate_ssh_host(normalize_known_hosts_host(host))
        port = validate_ssh_port(port)
        try:
            scan_cmd = ['ssh-keyscan', '-p', str(port), '-T', '10', '--', host]
            result = subprocess.run(
                scan_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            return [
                line for line in result.stdout.splitlines()
                if line.strip() and not line.startswith('#')
            ]
        except Exception as e:
            logger.warning(f"ssh-keyscan failed for {host}:{port}: {e}")
            return []

    def _key_details_for_lines(self, key_lines: List[str]) -> List[Dict[str, str]]:
        """
        Compute the key type and SHA256 fingerprint for raw known_hosts
        lines.

        ssh-keygen -lf prints "256 SHA256:XXXX host (ED25519)". The
        fingerprint is the second field and the key type is the last
        field, wrapped in parentheses.

        Returns:
            List of dicts with "type" (for example ED25519, RSA, ECDSA)
            and "fingerprint" (SHA256:XXXX).
        """
        details = []
        for line in key_lines:
            try:
                fp_result = subprocess.run(
                    ['ssh-keygen', '-lf', '-'],
                    input=line + '\n',
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                if fp_result.returncode == 0:
                    parts = fp_result.stdout.strip().split()
                    if len(parts) >= 2:
                        key_type = "UNKNOWN"
                        last = parts[-1]
                        if last.startswith('(') and last.endswith(')'):
                            key_type = last[1:-1]
                        details.append({
                            "type": key_type,
                            "fingerprint": parts[1],
                        })
            except Exception:
                pass
        return details

    def _fingerprints_for_lines(self, key_lines: List[str]) -> List[str]:
        """Compute SHA256 fingerprints for raw known_hosts lines."""
        return [
            detail["fingerprint"]
            for detail in self._key_details_for_lines(key_lines)
        ]

    def _pinned_key_lines(self, host: str, port: int) -> List[str]:
        """Return known_hosts lines pinned for the given host and port."""
        host = normalize_known_hosts_host(host)
        host_token = f"[{host}]:{port}" if port != 22 else host
        if not self.known_hosts_file.exists():
            return []
        return [
            line for line in self.known_hosts_file.read_text().splitlines()
            if line.strip() and line.split()[0] == host_token
        ]

    def is_host_key_pinned(self, host: str, port: int) -> bool:
        """Return True when a host key for host:port is already pinned."""
        return len(self._pinned_key_lines(host, port)) > 0

    def preview_host_key(self, host: str, port: int) -> Dict[str, Any]:
        """
        Scan the remote host key WITHOUT pinning it, so the fingerprint
        can be shown to the administrator for out-of-band confirmation.

        This is the first half of the show-and-confirm trust model: the
        key is only written to known_hosts by a subsequent
        confirm_host_key() call carrying the exact fingerprints that were
        displayed here.

        Args:
            host: Remote hostname or IP
            port: SSH port

        Returns:
            Dictionary with keys:
                host: normalised bare host
                port: SSH port
                scanned: True when ssh-keyscan returned at least one key
                fingerprints: SHA256 fingerprints of the remote keys
                keys: type and fingerprint of each remote key
                pinned_fingerprints: fingerprints currently pinned, if any
                pinned_keys: type and fingerprint of each pinned key
                is_pinned: whether a pin already exists
                is_changed: whether the remote key differs from the pin
        """
        host = normalize_known_hosts_host(host)
        key_lines = self._scan_host_keys(host, port)
        pinned_lines = self._pinned_key_lines(host, port)

        keys = self._key_details_for_lines(key_lines)
        pinned_keys = self._key_details_for_lines(pinned_lines)
        fingerprints = [key["fingerprint"] for key in keys]
        pinned_fingerprints = [key["fingerprint"] for key in pinned_keys]

        is_changed = False
        if key_lines and pinned_lines:
            is_changed = (
                self._key_bodies(pinned_lines) != self._key_bodies(key_lines)
            )

        return {
            "host": host,
            "port": port,
            "scanned": bool(key_lines),
            "fingerprints": fingerprints,
            "keys": keys,
            "pinned_fingerprints": pinned_fingerprints,
            "pinned_keys": pinned_keys,
            "is_pinned": bool(pinned_lines),
            "is_changed": is_changed,
        }

    def confirm_host_key(
        self, host: str, port: int, confirmed_fingerprints: List[str]
    ) -> List[str]:
        """
        Pin the remote host key after the administrator confirmed the
        fingerprints shown by preview_host_key().

        The confirmation is bound to the exact key material that was
        displayed. The host is re-scanned here and the pin is refused if
        the remote key no longer matches what the administrator approved,
        so a key that rotates between the preview and the confirmation
        cannot be trusted unseen.

        Args:
            host: Remote hostname or IP
            port: SSH port
            confirmed_fingerprints: Fingerprints the administrator approved

        Returns:
            Fingerprints of the newly pinned host keys.

        Raises:
            HostKeyVerificationError: If nothing was confirmed, the scan
                fails, or the remote key changed since the preview.
        """
        host = normalize_known_hosts_host(host)

        if not confirmed_fingerprints:
            raise HostKeyVerificationError(
                host_key_error_message(
                    host, port, "no host key fingerprint was confirmed"
                )
            )

        key_lines = self._scan_host_keys(host, port)
        if not key_lines:
            raise HostKeyVerificationError(
                host_key_error_message(
                    host, port,
                    "ssh-keyscan returned no host keys; the host may be "
                    "unreachable or the port may be wrong"
                )
            )

        current = self._fingerprints_for_lines(key_lines)
        if set(current) != set(confirmed_fingerprints):
            raise HostKeyVerificationError(
                host_key_error_message(
                    host, port,
                    "the remote host key changed between the preview and "
                    f"the confirmation (confirmed {', '.join(confirmed_fingerprints)}, "
                    f"now {', '.join(current) or 'unknown'}). Nothing was pinned."
                )
            )

        # Pin exactly the key lines that were just compared. Scanning again
        # here would pin whatever answered the second scan (which may differ
        # from what was approved) or nothing at all if that scan failed.
        return self._trust_host_key(host, port, replace=True, key_lines=key_lines)

    @staticmethod
    def _key_bodies(key_lines: List[str]) -> set:
        """
        Extract the key material from known_hosts lines.

        Lines are "host keytype keyblob [comment]". The key body (index 2)
        is compared rather than the key type, because a rotated key
        usually keeps the same type.
        """
        return {
            line.split()[2] for line in key_lines if len(line.split()) >= 3
        }

    def _trust_host_key(
        self, host: str, port: int, replace: bool = False,
        key_lines: Optional[List[str]] = None
    ) -> List[str]:
        """
        Store host keys in the WebZFS-owned known_hosts file and return
        their fingerprints.

        Callers: confirm_host_key (passes the exact key_lines the
        administrator approved), _copy_key_to_remote (re-verifies an already
        pinned host) and trust_host_key. Routine operations never call this
        for an unpinned host; see ensure_host_key_trusted.

        A host whose pinned key no longer matches raises
        HostKeyVerificationError unless ``replace`` is True, which is the
        deliberate "Trust host key" re-pin path.

        Args:
            host: Remote hostname or IP
            port: SSH port
            replace: Re-pin an already trusted host whose key changed
            key_lines: Already scanned known_hosts lines to store. When
                omitted the host is scanned here.

        Returns:
            List of host key fingerprint strings (empty if nothing was
            scanned, in which case nothing is stored)

        Raises:
            HostKeyVerificationError: If the pinned key differs from the
                remote key and ``replace`` is False.
        """
        # Normalise once, up front. Every known_hosts token built below
        # must use the bare host, otherwise a 'user@host' caller writes a
        # token no lookup will ever match and the superseded key is left
        # behind in the file instead of being replaced.
        host = normalize_known_hosts_host(host)

        if key_lines is None:
            key_lines = self._scan_host_keys(host, port)

        if not key_lines:
            logger.warning(f"ssh-keyscan returned no host keys for {host}:{port}")
            return []

        # Compare with the current pin and rewrite the file as one locked
        # step, so two workers pinning different hosts cannot drop each
        # other's entries. The network scan above runs outside the lock.
        with file_lock(self.known_hosts_lock_file):
            pinned_lines = self._pinned_key_lines(host, port)
            if pinned_lines and not replace:
                if self._key_bodies(pinned_lines) != self._key_bodies(key_lines):
                    old_fps = self._fingerprints_for_lines(pinned_lines)
                    new_fps = self._fingerprints_for_lines(key_lines)
                    raise HostKeyVerificationError(
                        host_key_error_message(
                            host,
                            port,
                            "pinned host key(s) "
                            f"{', '.join(old_fps) or 'unknown'} do not match remote "
                            f"host key(s) {', '.join(new_fps) or 'unknown'}"
                        )
                    )

            host_token = f"[{host}]:{port}" if port != 22 else host
            existing_lines = [
                line for line in (
                    self.known_hosts_file.read_text().splitlines()
                    if self.known_hosts_file.exists() else []
                )
                if line.strip() and not line.split()[0] == host_token
            ]

            write_text_atomic(
                self.known_hosts_file,
                '\n'.join(existing_lines + key_lines) + '\n'
            )

        fingerprints = self._fingerprints_for_lines(key_lines)
        logger.info(f"Stored {len(key_lines)} host key(s) for {host}:{port}")
        return fingerprints

    def trust_host_key(self, host: str, port: int) -> List[str]:
        """
        Explicitly (re)pin the remote host key for host:port.

        This is the one-time "Trust host key" action: it is the only path
        that accepts a changed key. Use it deliberately after verifying
        the new fingerprint out of band.

        Returns:
            Fingerprints of the newly pinned host keys.
        """
        return self._trust_host_key(host, port, replace=True)

    def ensure_host_key_trusted(
        self, host: str, port: int, expected_fingerprints: Optional[List[str]] = None
    ) -> bool:
        """
        Make sure a host key is pinned for host:port before it is used.

        Never trusts a key nobody approved:

        - Already pinned: nothing to do. A changed key is detected when the
          connection is made (paramiko RejectPolicy, ssh
          StrictHostKeyChecking=yes) and raises HostKeyVerificationError.
        - Not pinned, but the caller passes the fingerprints the
          administrator confirmed when the connection was created (stored on
          the connection record): the host is re-scanned and re-pinned only
          if it presents exactly those keys. This restores a lost
          known_hosts file, for example after restoring a backup on a new
          machine, without trusting a different key.
        - Not pinned and nothing to compare with: HostKeyVerificationError.
          The administrator uses the explicit Trust host key page.

        Returns:
            True when a host key is pinned after this call.

        Raises:
            HostKeyVerificationError: The host is not pinned and could not be
                re-pinned to the previously confirmed keys.
        """
        if self.is_host_key_pinned(host, port):
            return True
        if expected_fingerprints:
            self.confirm_host_key(host, port, expected_fingerprints)
            if not self.is_host_key_pinned(host, port):
                raise HostKeyVerificationError(
                    host_key_error_message(
                        normalize_known_hosts_host(host), port,
                        "the host key could not be pinned"
                    )
                )
            logger.info(
                f"Re-pinned {host}:{port} to its previously confirmed host key(s)"
            )
            return True
        raise HostKeyVerificationError(
            host_key_error_message(
                normalize_known_hosts_host(host), port,
                "no host key is pinned for this host"
            )
        )

    def set_host_key_fingerprints(self, connection_id: str, fingerprints: List[str]) -> None:
        """Store the host key fingerprints confirmed for a connection."""
        with self._locked_connections() as data:
            for item in data["connections"]:
                if item["id"] == connection_id:
                    item["host_key_fingerprints"] = list(fingerprints)

    def configure_paramiko_client(self, client: paramiko.SSHClient) -> paramiko.SSHClient:
        """
        Load the WebZFS pinned known_hosts file into a paramiko client and
        reject unknown or changed host keys.

        Args:
            client: A fresh paramiko.SSHClient

        Returns:
            The same client, configured for strict host key verification.
        """
        if self.known_hosts_file.exists():
            client.load_host_keys(str(self.known_hosts_file))
        client.set_missing_host_key_policy(paramiko.RejectPolicy())
        return client

    def raise_for_paramiko_host_key_error(
        self, exc: Exception, host: str, port: int
    ) -> None:
        """Translate a paramiko host-key exception into HostKeyVerificationError."""
        if isinstance(exc, paramiko.BadHostKeyException):
            raise HostKeyVerificationError(
                host_key_error_message(host, port, f"host key changed: {exc}")
            ) from exc
        if isinstance(exc, paramiko.SSHException) and is_host_key_error(str(exc)):
            raise HostKeyVerificationError(
                host_key_error_message(host, port, str(exc))
            ) from exc

    
    def get_host_key_options(self) -> List[str]:
        """
        Get SSH -o option strings for strict host key verification using
        the WebZFS-owned known_hosts file.
        
        Returns:
            List of option strings, e.g.
            ['StrictHostKeyChecking=yes', 'UserKnownHostsFile=/path']
        """
        return [
            'StrictHostKeyChecking=yes',
            f'UserKnownHostsFile={self.known_hosts_file}'
        ]
    
    # Syncoid / Replication Integration
    
    def require_active_connection(self, connection_id: str) -> Dict[str, Any]:
        """
        Resolve a connection ID to a validated connection record.
        
        Validates that the connection exists and that its private key file
        is a regular file inside the managed key directory.
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            Connection record dictionary
            
        Raises:
            Exception: If the connection or its key is invalid
        """
        conn = self.get_connection(connection_id)
        if not conn:
            raise Exception(f"SSH connection {connection_id} not found")
        
        key_path = Path(conn["private_key_path"]).resolve()
        
        # The key must live inside the managed key directory and be a
        # regular file. This prevents key path injection via a tampered
        # connections file.
        if key_path.parent != self.keys_dir:
            raise Exception(
                f"Private key for connection '{conn['name']}' is outside the managed key directory"
            )
        if not key_path.is_file():
            raise Exception(
                f"Private key file for connection '{conn['name']}' is missing"
            )

        # The stored destination is used on ssh and syncoid command lines;
        # re-check it so an edited connections file cannot inject options.
        validate_ssh_host(conn["host"])
        validate_ssh_username(conn["username"])
        validate_ssh_port(conn["port"])

        return conn
    
    def build_syncoid_profile(self, connection_id: str) -> Dict[str, Any]:
        """
        Build an SSH profile for syncoid execution from a managed connection.
        
        All paths are absolute so the profile remains valid when syncoid is
        executed under sudo where HOME differs from the webzfs account.
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            Dictionary with keys:
                host_string: 'user@host' for the syncoid source/target prefix
                port: SSH port (int)
                identity_file: absolute path to the managed private key
                ssh_options: list of SSH -o option strings
        """
        conn = self.require_active_connection(connection_id)
        
        # Make sure the host key is trusted before handing the profile to
        # syncoid, which runs with BatchMode and cannot prompt.
        self.ensure_host_key_trusted(
            conn["host"], conn["port"], conn.get("host_key_fingerprints")
        )
        
        return {
            "connection_id": conn["id"],
            "connection_name": conn["name"],
            "host_string": f"{conn['username']}@{conn['host']}",
            "port": conn["port"],
            "identity_file": str(Path(conn["private_key_path"]).resolve()),
            "ssh_options": [
                'IdentitiesOnly=yes',
                'BatchMode=yes',
            ] + self.get_host_key_options()
        }
    
    # Data Persistence Methods
    
    def _load_connections(self, strict: bool = False) -> Dict[str, Any]:
        """Load connections from disk (always fresh).

        A damaged file reads as empty for display (strict=False), but raises
        when strict=True, which every writer uses, so a damaged file is never
        replaced by an empty list.
        """
        try:
            data = read_json(self.connections_file, {"connections": []})
        except StateFileError as error:
            if strict:
                raise Exception(f"SSH connections file is damaged; not changed: {error}")
            logger.error(f"Failed to load connections: {error}")
            return {"connections": []}
        if not isinstance(data, dict) or not isinstance(data.get("connections"), list):
            if strict:
                raise Exception("SSH connections file has an unexpected format; not changed.")
            return {"connections": []}
        return data

    @contextmanager
    def _locked_connections(self):
        """Read, change and save ssh_connections.json as one locked step.

        Yields the current file contents (a dict with a "connections" list).
        Changes made to it are written atomically when the block ends without
        an exception.
        """
        with file_lock(self.connections_lock_file):
            data = self._load_connections(strict=True)
            yield data
            write_json_atomic(self.connections_file, data)
            self.connections_data = data
    
    # Integration Methods for Other Features
    
    def get_ssh_command_args(self, connection_id: str) -> List[str]:
        """
        Get SSH command arguments for use with subprocess
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            List of SSH command arguments
            
        Example:
            ['ssh', '-i', '/path/to/key', '-p', '22', 'user@host']
        """
        conn = self.require_active_connection(connection_id)

        # Ensure the host key is trusted so strict checking succeeds
        self.ensure_host_key_trusted(
            conn["host"], conn["port"], conn.get("host_key_fingerprints")
        )

        args = [
            'ssh',
            '-i', conn["private_key_path"],
            '-p', str(conn["port"]),
        ]
        for option in self.get_host_key_options():
            args.extend(['-o', option])
        # '--' ends option parsing; callers append the remote command after it.
        args.extend(['--', f'{conn["username"]}@{conn["host"]}'])
        return args
    
    def get_ssh_client(self, connection_id: str) -> paramiko.SSHClient:
        """
        Get a connected paramiko SSH client
        
        Args:
            connection_id: Connection UUID
            
        Returns:
            Connected SSH client
            
        Note:
            Caller is responsible for closing the client
        """
        conn = self.get_connection(connection_id)
        if not conn:
            raise Exception(f"Connection {connection_id} not found")
        
        client = paramiko.SSHClient()
        self.configure_paramiko_client(client)
        
        # Load the private key
        key = paramiko.Ed25519Key.from_private_key_file(conn["private_key_path"])
        
        # Connect
        try:
            client.connect(
                hostname=conn["host"],
                port=conn["port"],
                username=conn["username"],
                pkey=key,
                timeout=10
            )
        except paramiko.SSHException as e:
            self.raise_for_paramiko_host_key_error(e, conn["host"], conn["port"])
            raise
        
        # Mark as used
        self.mark_connection_used(connection_id, "ssh_client")
        
        return client
