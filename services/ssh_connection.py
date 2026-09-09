"""
SSH Connection Management Service
Centralized SSH connection management for Fleet Monitoring and ZFS Replication
"""
import json
import os
import uuid
import subprocess
import shutil
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from pathlib import Path
import paramiko
import logging

logger = logging.getLogger(__name__)


class SSHConnectionService:
    """Service for managing SSH connections with key-based authentication"""
    
    def __init__(self):
        """Initialize the SSH connection service"""
        # Set up config directory
        self.config_dir = Path.home() / ".config" / "webzfs"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up connections file
        self.connections_file = self.config_dir / "ssh_connections.json"
        
        # Set up SSH keys directory.
        # Paths are resolved to absolute paths so they remain valid when
        # commands are executed under sudo where HOME may differ.
        self.keys_dir = (Path.home() / ".ssh" / "webzfs_connections").resolve()
        self.keys_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        
        # WebZFS-owned known_hosts database. All SSH commands built by this
        # service use this file with strict host key checking instead of
        # disabling host key verification.
        self.known_hosts_file = (Path.home() / ".ssh" / "webzfs_known_hosts").resolve()
        
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
        notes: str = ""
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
            
        Returns:
            connection_id: UUID of created connection
            
        Raises:
            Exception: If key generation, distribution, or testing fails
        """
        connection_id = str(uuid.uuid4())
        
        # Reload from disk to get latest connections before adding
        self.connections_data = self._load_connections()
        
        try:
            # Generate SSH key pair
            private_key_path, public_key_path = self._generate_ssh_key(
                connection_id,
                f"uvicorn-zfs-{name}"
            )
            
            # Copy public key to remote server
            success = self._copy_key_to_remote(host, port, username, password, public_key_path)
            if not success:
                raise Exception("Failed to copy SSH key to remote server. Check credentials and network connectivity.")
            
            # Test key-based authentication
            if not self._test_key_auth(host, port, username, private_key_path):
                raise Exception("SSH key authentication test failed. Key may not have been installed correctly.")
            
            # Get key fingerprint
            fingerprint = self._get_key_fingerprint(public_key_path)
            
            # Capture and store the remote host key so subsequent SSH
            # commands can use strict host key verification. Connection
            # creation is an explicit administrator action, so trusting
            # the host key at this point is a deliberate trust decision.
            host_key_fingerprints = self._trust_host_key(host, port)
            
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
                "notes": notes,
                "key_source": "generated",
            }

            # Add to connections list
            if "connections" not in self.connections_data:
                self.connections_data["connections"] = []
            self.connections_data["connections"].append(connection)

            # Save to disk
            self._save_connections()

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
            raise Exception(f"Failed to create SSH connection: {str(e)}")

    def register_existing_connection(
        self,
        name: str,
        host: str,
        username: str,
        private_key_path: str,
        port: int = 22,
        notes: str = ""
    ) -> str:
        """
        Register an SSH connection that's already trusted via an
        existing private key, instead of generating a new keypair and
        bootstrapping it with a password (create_connection's own flow).

        Real gap this closes: create_connection always requires
        password auth to be enabled on the remote host, even
        temporarily, to copy its freshly-generated public key over.
        Plenty of real environments (this one included) run key-only
        SSH with no password auth at all, by design -- there's no way
        to use this app's own connection management against a host
        like that without this second path.

        Steps:
        1. Verify the given private key file exists and actually
           authenticates (reusing the same test used after
           create_connection's own key-copy step).
        2. Get its fingerprint.
        3. Trust the remote host key (same deliberate-trust reasoning
           as create_connection).
        4. Save a connection record in the exact same schema
           create_connection produces, so every other part of the app
           (replication jobs' own ssh_connection_id, fleet monitoring,
           etc.) treats it identically either way.

        Args:
            name: Human-readable connection name
            host: IP address or hostname
            username: SSH username
            private_key_path: Path to an existing private key that's
                already authorized on the remote host
            port: SSH port (default 22)
            notes: Optional notes about the connection

        Returns:
            connection_id: UUID of the created connection

        Raises:
            Exception: If the key file is missing or authentication
                fails
        """
        connection_id = str(uuid.uuid4())
        self.connections_data = self._load_connections()

        key_path = Path(private_key_path).expanduser()
        if not key_path.is_file():
            raise Exception(f"Private key not found: {key_path}")

        if not self._test_key_auth(host, port, username, key_path):
            raise Exception(
                "SSH key authentication test failed against the existing key. "
                "Confirm it's actually authorized on the remote host."
            )

        # Reuse the given key for both the "private" and "public" path
        # fields -- there may not be a separate .pub file alongside an
        # externally-managed key, and fingerprinting only needs the
        # private key's own public component, which ssh-keygen can
        # derive directly.
        fingerprint = self._get_key_fingerprint(key_path)
        host_key_fingerprints = self._trust_host_key(host, port)

        connection = {
            "id": connection_id,
            "name": name,
            "host": host,
            "port": port,
            "username": username,
            "private_key_path": str(key_path),
            "public_key_path": str(key_path),
            "fingerprint": fingerprint,
            "host_key_fingerprints": host_key_fingerprints,
            "created_at": datetime.now().isoformat(),
            "last_used": None,
            "last_tested": datetime.now().isoformat(),
            "status": "active",
            "used_by": [],
            "notes": notes,
            "key_source": "external",
        }

        if "connections" not in self.connections_data:
            self.connections_data["connections"] = []
        self.connections_data["connections"].append(connection)
        self._save_connections()

        logger.info(f"Registered existing-key SSH connection: {name} ({connection_id})")
        return connection_id

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
        # Reload from disk to get latest connections before modifying
        self.connections_data = self._load_connections()
        for conn in self.connections_data.get("connections", []):
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
                
                self._save_connections()
                logger.info(f"Updated SSH connection: {connection_id}")
                return
        
        raise Exception(f"Connection {connection_id} not found")
    
    def delete_connection(self, connection_id: str, remove_from_remote: bool = False) -> None:
        """
        Delete an SSH connection
        
        Args:
            connection_id: Connection UUID
            remove_from_remote: Whether to remove key from remote server
            
        Raises:
            Exception: If connection not found
        """
        # Reload from disk to get latest connections before modifying
        self.connections_data = self._load_connections()
        connections = self.connections_data.get("connections", [])
        
        for i, conn in enumerate(connections):
            if conn["id"] == connection_id:
                # Optionally remove key from remote server
                if remove_from_remote:
                    try:
                        self._remove_key_from_remote(conn)
                    except Exception as e:
                        logger.warning(f"Failed to remove key from remote: {e}")
                
                # Delete local key files
                try:
                    Path(conn["private_key_path"]).unlink(missing_ok=True)
                    Path(conn["public_key_path"]).unlink(missing_ok=True)
                except Exception as e:
                    logger.warning(f"Failed to delete key files: {e}")
                
                # Remove from list
                del connections[i]
                self._save_connections()
                logger.info(f"Deleted SSH connection: {connection_id}")
                return
        
        raise Exception(f"Connection {connection_id} not found")
    
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
            
            # Test connection with key
            if self._test_key_auth(
                conn["host"],
                conn["port"],
                conn["username"],
                conn["private_key_path"]
            ):
                # Update last tested time
                conn["last_tested"] = datetime.now().isoformat()
                conn["status"] = "active"
                self._save_connections()
                
                return {
                    "status": "success",
                    "message": f"Successfully connected to {conn['host']}"
                }
            else:
                conn["status"] = "error"
                self._save_connections()
                
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
        if conn and feature not in conn.get("used_by", []):
            if "used_by" not in conn:
                conn["used_by"] = []
            conn["used_by"].append(feature)
            conn["last_used"] = datetime.now().isoformat()
            self._save_connections()
    
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
        
        # Method 1: Try ssh-copy-id with sshpass
        if shutil.which('sshpass') and shutil.which('ssh-copy-id'):
            try:
                cmd = [
                    'sshpass', '-p', password,
                    'ssh-copy-id',
                    '-i', str(public_key_path),
                    '-p', str(port),
                    '-o', 'StrictHostKeyChecking=no',
                    '-o', 'UserKnownHostsFile=/dev/null',
                    f'{username}@{host}'
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    logger.info(f"Copied SSH key to {host} using ssh-copy-id")
                    return True
            except Exception as e:
                logger.warning(f"ssh-copy-id failed: {e}, trying paramiko method")
        
        # Method 2: Use paramiko
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
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
            
            # Create .ssh directory if it doesn't exist
            stdin, stdout, stderr = client.exec_command(
                'mkdir -p ~/.ssh && chmod 700 ~/.ssh'
            )
            stdout.channel.recv_exit_status()
            
            # Append public key to authorized_keys
            cmd = f"echo '{public_key}' >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
            stdin, stdout, stderr = client.exec_command(cmd)
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
            True if successful, False otherwise
        """
        try:
            public_key = Path(connection["public_key_path"]).read_text().strip()
            
            # Create SSH client with key authentication
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            
            # Connect using the key we're about to remove (it still works
            # until we remove it). key_filename auto-detects key type --
            # same reasoning as _test_key_auth/get_ssh_client, needed
            # since this can now run against an externally-registered
            # (non-Ed25519) connection too.
            client.connect(
                hostname=connection["host"],
                port=connection["port"],
                username=connection["username"],
                key_filename=connection["private_key_path"],
                timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            
            # Remove the specific key from authorized_keys
            cmd = f"sed -i '\\|{public_key}|d' ~/.ssh/authorized_keys"
            stdin, stdout, stderr = client.exec_command(cmd)
            stdout.channel.recv_exit_status()
            
            client.close()
            logger.info(f"Removed SSH key from {connection['host']}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to remove key from remote: {e}")
            return False
    
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
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # key_filename lets paramiko auto-detect the key type (RSA,
            # ECDSA, Ed25519, ...) instead of assuming Ed25519.
            # create_connection's own generated keys are always Ed25519
            # so this is a no-op for that path, but
            # register_existing_connection can hand this an
            # externally-managed key of any type (e.g. this fleet's real
            # RSA-based keep@floads.io key) -- hardcoding Ed25519Key here
            # would make that path fail its own auth test unconditionally.
            client.connect(
                hostname=host,
                port=port,
                username=username,
                key_filename=str(private_key_path),
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
    
    def _trust_host_key(self, host: str, port: int) -> List[str]:
        """
        Retrieve the remote host keys with ssh-keyscan and store them in
        the WebZFS-owned known_hosts file. Returns the fingerprints of the
        stored keys.
        
        Existing entries for the same host are replaced so a deliberate
        re-trust (connection re-creation) updates stale keys.
        
        Args:
            host: Remote hostname or IP
            port: SSH port
            
        Returns:
            List of host key fingerprint strings (may be empty on failure)
        """
        try:
            scan_cmd = ['ssh-keyscan', '-p', str(port), '-T', '10', host]
            result = subprocess.run(
                scan_cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            key_lines = [
                line for line in result.stdout.splitlines()
                if line.strip() and not line.startswith('#')
            ]
            
            if not key_lines:
                logger.warning(f"ssh-keyscan returned no host keys for {host}:{port}")
                return []
            
            # Remove any existing entries for this host, then append new ones
            existing_lines = []
            if self.known_hosts_file.exists():
                host_token = f"[{host}]:{port}" if port != 22 else host
                for line in self.known_hosts_file.read_text().splitlines():
                    if line.strip() and not line.split()[0] == host_token:
                        existing_lines.append(line)
            
            all_lines = existing_lines + key_lines
            self.known_hosts_file.write_text('\n'.join(all_lines) + '\n')
            os.chmod(self.known_hosts_file, 0o600)
            
            # Compute fingerprints for the stored keys
            fingerprints = []
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
                            fingerprints.append(parts[1])
                except Exception:
                    pass
            
            logger.info(f"Stored {len(key_lines)} host key(s) for {host}:{port}")
            return fingerprints
            
        except Exception as e:
            logger.warning(f"Failed to capture host key for {host}:{port}: {e}")
            return []
    
    def ensure_host_key_trusted(self, host: str, port: int) -> bool:
        """
        Ensure the known_hosts file contains an entry for the given host.
        If missing, attempt to capture it via ssh-keyscan.
        
        Returns:
            True if a host key entry exists after this call.
        """
        host_token = f"[{host}]:{port}" if port != 22 else host
        if self.known_hosts_file.exists():
            for line in self.known_hosts_file.read_text().splitlines():
                if line.strip() and line.split()[0] == host_token:
                    return True
        return len(self._trust_host_key(host, port)) > 0
    
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
        # connections file. Does not apply to connections registered via
        # register_existing_connection ("key_source": "external") --
        # those intentionally point at a key this service doesn't
        # generate or own (e.g. a pre-trusted fleet key), so there's no
        # keys_dir path for them to live inside by design. Older
        # connections predating this field have no "key_source" key at
        # all and fall through to the strict check, which is correct --
        # they really were all generated into keys_dir.
        if conn.get("key_source") != "external" and key_path.parent != self.keys_dir:
            raise Exception(
                f"Private key for connection '{conn['name']}' is outside the managed key directory"
            )
        if not key_path.is_file():
            raise Exception(
                f"Private key file for connection '{conn['name']}' is missing"
            )
        
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
        self.ensure_host_key_trusted(conn["host"], conn["port"])
        
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
    
    def _load_connections(self) -> Dict[str, Any]:
        """Load connections from JSON file - always reads fresh from disk"""
        if not self.connections_file.exists():
            return {"connections": []}
        
        try:
            # Read the file fresh from disk (bypass any caching)
            with open(self.connections_file, 'r') as f:
                # Force reading fresh content
                f.seek(0)
                content = f.read()
                if not content.strip():
                    return {"connections": []}
                return json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Failed to load connections: {e}")
            return {"connections": []}
    
    def _save_connections(self) -> None:
        """Save connections to JSON file - ensures data is synced to disk"""
        try:
            with open(self.connections_file, 'w') as f:
                json.dump(self.connections_data, f, indent=2)
                # Ensure data is flushed to OS buffers
                f.flush()
                # Force sync to disk (important for multi-worker environments)
                os.fsync(f.fileno())
            
            # Set secure permissions
            os.chmod(self.connections_file, 0o600)
            
        except IOError as e:
            logger.error(f"Failed to save connections: {e}")
            raise Exception(f"Failed to save connections: {str(e)}")
    
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
        conn = self.get_connection(connection_id)
        if not conn:
            raise Exception(f"Connection {connection_id} not found")
        
        # Ensure the host key is trusted so strict checking succeeds
        self.ensure_host_key_trusted(conn["host"], conn["port"])
        
        args = [
            'ssh',
            '-i', conn["private_key_path"],
            '-p', str(conn["port"]),
        ]
        for option in self.get_host_key_options():
            args.extend(['-o', option])
        args.append(f'{conn["username"]}@{conn["host"]}')
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
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        # key_filename auto-detects key type -- see _test_key_auth's own
        # comment for why this can't hardcode Ed25519Key.
        client.connect(
            hostname=conn["host"],
            port=conn["port"],
            username=conn["username"],
            key_filename=conn["private_key_path"],
            timeout=10,
            look_for_keys=False,
            allow_agent=False
        )
        
        # Mark as used
        self.mark_connection_used(connection_id, "ssh_client")
        
        return client
