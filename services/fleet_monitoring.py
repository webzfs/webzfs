"""
Fleet Monitoring Service
Manages remote server monitoring via SSH for ZFS pool status viewing
"""
import json
import os
import uuid
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from cryptography.fernet import Fernet
import paramiko
import logging

logger = logging.getLogger(__name__)


class FleetMonitoringService:
    """Service for managing remote server fleet monitoring"""
    
    def __init__(self):
        """Initialize the fleet monitoring service"""
        # Set up config directory
        self.config_dir = Path.home() / ".config" / "webzfs"
        self.config_dir.mkdir(parents=True, exist_ok=True)
        
        # Set up server config file
        self.servers_file = self.config_dir / "fleet_servers.json"
        
        # Set up encryption key file
        self.key_file = self.config_dir / ".fleet_key"
        self._ensure_encryption_key()
        
        # Load encryption key
        with open(self.key_file, 'rb') as f:
            self.cipher = Fernet(f.read())
        
        # Load servers from disk
        self.servers_data = self._load_servers()
        
        # In-memory cache for pool data
        self._pool_cache: Dict[str, Dict[str, Any]] = {}
        
        # SSH Connection Service for key-based auth
        self._ssh_service = None
    
    def _get_ssh_service(self):
        """Get the SSH connection service (lazy load to avoid circular imports)"""
        if self._ssh_service is None:
            from services.ssh_connection import SSHConnectionService
            self._ssh_service = SSHConnectionService()
        return self._ssh_service
    
    def _ensure_encryption_key(self) -> None:
        """Ensure encryption key exists, create if necessary"""
        if not self.key_file.exists():
            key = Fernet.generate_key()
            self.key_file.write_bytes(key)
            # Set secure permissions (owner read/write only)
            os.chmod(self.key_file, 0o600)
    
    def _load_servers(self) -> Dict[str, Any]:
        """Load servers from config file"""
        if not self.servers_file.exists():
            return {"servers": []}
        
        try:
            with open(self.servers_file, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {"servers": []}
    
    def _save_servers(self) -> None:
        """Save servers to config file"""
        with open(self.servers_file, 'w') as f:
            json.dump(self.servers_data, f, indent=2)
        # Set secure permissions
        os.chmod(self.servers_file, 0o600)
    
    def _encrypt_password(self, password: str) -> str:
        """Encrypt a password"""
        return self.cipher.encrypt(password.encode()).decode()
    
    def _decrypt_password(self, encrypted: str) -> str:
        """Decrypt a password"""
        return self.cipher.decrypt(encrypted.encode()).decode()
    
    # Server Management Methods
    
    def list_servers(self) -> List[Dict[str, Any]]:
        """
        List all configured servers
        
        Returns:
            List of server configurations (without passwords)
        """
        servers = []
        for server in self.servers_data.get("servers", []):
            # Return server data without password
            server_copy = server.copy()
            server_copy.pop("password", None)
            servers.append(server_copy)
        return servers
    
    def get_server(self, server_id: str) -> Dict[str, Any]:
        """
        Get a specific server configuration
        
        Args:
            server_id: Server UUID
            
        Returns:
            Server configuration (without password)
            
        Raises:
            KeyError: If server not found
        """
        for server in self.servers_data.get("servers", []):
            if server["id"] == server_id:
                server_copy = server.copy()
                server_copy.pop("password", None)
                return server_copy
        raise KeyError(f"Server {server_id} not found")
    
    def add_server(
        self,
        name: str,
        ip: str,
        username: str,
        password: str,
        port: int = 22
    ) -> str:
        """
        Add a new server to the fleet (password-based authentication)
        
        Args:
            name: Human-readable server name
            ip: IP address or hostname
            username: SSH username
            password: SSH password (will be encrypted)
            port: SSH port (default 22)
            
        Returns:
            server_id: UUID of created server
        """
        server_id = str(uuid.uuid4())
        
        server = {
            "id": server_id,
            "name": name,
            "ip": ip,
            "port": port,
            "username": username,
            "password": self._encrypt_password(password),
            "ssh_key_path": None,
            "ssh_connection_id": None,
            "auth_type": "password",
            "added_at": datetime.now().isoformat(),
            "last_checked": None,
            "status": "unknown",
            "pools": []
        }
        
        self.servers_data.setdefault("servers", []).append(server)
        self._save_servers()
        
        return server_id
    
    def add_server_from_ssh_connection(self, ssh_connection_id: str, name: str = None) -> str:
        """
        Add a server to the fleet using an existing SSH connection
        
        Args:
            ssh_connection_id: UUID of the SSH connection from SSH Connection Manager
            name: Optional custom name for the server (defaults to connection name)
            
        Returns:
            server_id: UUID of created server
            
        Raises:
            ValueError: If SSH connection not found
        """
        ssh_service = self._get_ssh_service()
        connection = ssh_service.get_connection(ssh_connection_id)
        
        if not connection:
            raise ValueError(f"SSH connection {ssh_connection_id} not found")
        
        server_id = str(uuid.uuid4())
        
        # Use connection name if no custom name provided
        server_name = name if name else connection['name']
        
        server = {
            "id": server_id,
            "name": server_name,
            "ip": connection['host'],
            "port": connection['port'],
            "username": connection['username'],
            "password": None,  # No password stored - using SSH key
            "ssh_key_path": connection['private_key_path'],
            "ssh_connection_id": ssh_connection_id,
            "auth_type": "key",
            "added_at": datetime.now().isoformat(),
            "last_checked": None,
            "status": "unknown",
            "pools": []
        }
        
        self.servers_data.setdefault("servers", []).append(server)
        self._save_servers()
        
        # Mark the SSH connection as used by fleet
        ssh_service.mark_connection_used(ssh_connection_id, 'fleet')
        
        return server_id
    
    def remove_server(self, server_id: str) -> None:
        """
        Remove a server from the fleet
        
        Args:
            server_id: Server UUID
            
        Raises:
            KeyError: If server not found
        """
        servers = self.servers_data.get("servers", [])
        for i, server in enumerate(servers):
            if server["id"] == server_id:
                del servers[i]
                self._save_servers()
                # Remove from cache
                self._pool_cache.pop(server_id, None)
                return
        raise KeyError(f"Server {server_id} not found")
    
    def update_server(self, server_id: str, **updates) -> None:
        """
        Update server configuration
        
        Args:
            server_id: Server UUID
            **updates: Fields to update
        """
        for server in self.servers_data.get("servers", []):
            if server["id"] == server_id:
                # Handle password encryption if updating password
                if "password" in updates:
                    updates["password"] = self._encrypt_password(updates["password"])
                
                server.update(updates)
                self._save_servers()
                return
        raise KeyError(f"Server {server_id} not found")
    
    def test_connection(self, server_id: str) -> Dict[str, Any]:
        """
        Test SSH connection to a server
        
        Args:
            server_id: Server UUID
            
        Returns:
            Connection test results
        """
        try:
            server = self._get_server_by_id(server_id)
            client = self._create_ssh_client(server)
            client.close()
            return {
                "status": "success",
                "message": "Connection successful"
            }
        except Exception as e:
            logger.error(f"Connection test failed for server {server_id}: {e}")
            return {
                "status": "error",
                "message": str(e)
            }
    
    # Data Fetching Methods
    
    def fetch_server_pools(self, server_id: str) -> List[Dict[str, Any]]:
        """
        Fetch pool information from a remote server
        
        Args:
            server_id: Server UUID
            
        Returns:
            List of pool information
        """
        try:
            server = self._get_server_by_id(server_id)
            
            # Connect via SSH
            client = self._create_ssh_client(server)
            
            try:
                # Execute zpool list command
                # Use sudo only for non-root users
                command = self._build_zfs_command(server, "zpool list -H -p -o name,size,alloc,free,cap,health")
                stdin, stdout, stderr = client.exec_command(command)
                output = stdout.read().decode('utf-8')
                error = stderr.read().decode('utf-8')
                
                if error and not output:
                    logger.error(f"Error fetching pools from {server_id}: {error}")
                    self.update_server(server_id, status="error", last_checked=datetime.now().isoformat())
                    return []
                
                # Get ZFS available space (actual usable space for users)
                avail_map = {}
                try:
                    avail_command = self._build_zfs_command(server, "zfs list -H -p -o name,avail -d 0")
                    a_stdin, a_stdout, a_stderr = client.exec_command(avail_command)
                    avail_output = a_stdout.read().decode('utf-8')
                    for avail_line in avail_output.strip().split('\n'):
                        if avail_line:
                            av_parts = avail_line.split('\t')
                            if len(av_parts) >= 2:
                                try:
                                    avail_map[av_parts[0]] = int(av_parts[1])
                                except (ValueError, TypeError):
                                    pass
                except Exception:
                    pass
                
                # Parse the output
                pools = []
                for line in output.strip().split('\n'):
                    if line:
                        parts = line.split('\t')
                        if len(parts) >= 6:
                            name, size, alloc, free, cap, health = parts[:6]
                            size_int = int(size)
                            avail_bytes = avail_map.get(name)
                            if avail_bytes is not None:
                                available_str = self._format_bytes(avail_bytes)
                                # Recalculate capacity based on ZFS avail
                                if size_int > 0:
                                    used_pct = round(((size_int - avail_bytes) / size_int) * 100)
                                    cap_str = f"{used_pct}%"
                                else:
                                    cap_str = f"{cap}%"
                            else:
                                available_str = self._format_bytes(int(free))
                                cap_str = f"{cap}%"
                            pools.append({
                                "name": name,
                                "size": self._format_bytes(size_int),
                                "allocated": self._format_bytes(int(alloc)),
                                "free": available_str,
                                "capacity": cap_str,
                                "health": health
                            })
                
                # Update server status
                self.update_server(
                    server_id,
                    status="online",
                    last_checked=datetime.now().isoformat(),
                    pools=pools
                )
                
                return pools
            
            finally:
                client.close()
        
        except Exception as e:
            logger.error(f"Failed to fetch pools from server {server_id}: {e}")
            self.update_server(
                server_id,
                status="error",
                last_checked=datetime.now().isoformat()
            )
            return []
    
    def fetch_all_servers(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Fetch pool information from all servers
        
        Returns:
            Dictionary mapping server_id to pool data
        """
        results = {}
        for server in self.servers_data.get("servers", []):
            server_id = server["id"]
            try:
                pools = self.fetch_server_pools(server_id)
                results[server_id] = pools
            except Exception as e:
                logger.error(f"Failed to fetch pools from server {server_id}: {e}")
                results[server_id] = []
        return results
    
    def execute_remote_command(self, server_id: str, command: str) -> str:
        """
        Execute a command on a remote server
        
        Args:
            server_id: Server UUID
            command: Command to execute
            
        Returns:
            Command output
        """
        try:
            server = self._get_server_by_id(server_id)
            client = self._create_ssh_client(server)
            
            try:
                stdin, stdout, stderr = client.exec_command(command)
                output = stdout.read().decode('utf-8')
                error = stderr.read().decode('utf-8')
                
                if error:
                    logger.warning(f"Command stderr on {server_id}: {error}")
                
                return output
            finally:
                client.close()
        
        except Exception as e:
            logger.error(f"Failed to execute command on server {server_id}: {e}")
            raise
    
    # Helper Methods
    
    def _build_zfs_command(self, server: Dict[str, Any], command: str) -> str:
        """
        Build a ZFS command with sudo if needed
        
        Args:
            server: Server configuration
            command: Base command to execute
            
        Returns:
            Command with sudo prefix if user is not root
        """
        # Root user doesn't need sudo
        if server.get("username", "").lower() == "root":
            return command
        
        # Non-root users need sudo for ZFS commands
        return f"sudo {command}"
    
    def _get_server_by_id(self, server_id: str) -> Dict[str, Any]:
        """Get full server config including password (internal use only)"""
        for server in self.servers_data.get("servers", []):
            if server["id"] == server_id:
                return server
        raise KeyError(f"Server {server_id} not found")
    
    def _create_ssh_client(self, server: Dict[str, Any]) -> paramiko.SSHClient:
        """
        Create and connect SSH client to a server
        
        Args:
            server: Server configuration dict
            
        Returns:
            Connected SSH client
        """
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Check auth type - default to password for backward compatibility
        auth_type = server.get("auth_type", "password")
        
        if auth_type == "key" and server.get("ssh_key_path"):
            # Key-based authentication
            key_path = server["ssh_key_path"]
            try:
                # Try loading as Ed25519 key first (used by SSH Connection Manager)
                # Then fall back to RSA if that fails
                private_key = None
                try:
                    private_key = paramiko.Ed25519Key.from_private_key_file(key_path)
                except Exception:
                    try:
                        private_key = paramiko.RSAKey.from_private_key_file(key_path)
                    except Exception:
                        try:
                            private_key = paramiko.ECDSAKey.from_private_key_file(key_path)
                        except Exception:
                            private_key = paramiko.DSSKey.from_private_key_file(key_path)
                
                # Connect with key
                client.connect(
                    hostname=server["ip"],
                    port=server["port"],
                    username=server["username"],
                    pkey=private_key,
                    timeout=10
                )
            except Exception as e:
                logger.error(f"Failed to connect with SSH key: {e}")
                raise Exception(f"SSH key authentication failed: {str(e)}")
        else:
            # Password-based authentication
            if not server.get("password"):
                raise Exception("No password configured for password authentication")
            
            password = self._decrypt_password(server["password"])
            
            # Connect with password
            client.connect(
                hostname=server["ip"],
                port=server["port"],
                username=server["username"],
                password=password,
                timeout=10
            )
        
        return client
    
    def _format_bytes(self, bytes_value: int) -> str:
        """
        Format bytes to human-readable string
        
        Args:
            bytes_value: Number of bytes
            
        Returns:
            Formatted string (e.g., "1.5T", "500G")
        """
        if bytes_value == 0:
            return "0B"
        
        units = ['B', 'K', 'M', 'G', 'T', 'P']
        unit_index = 0
        value = float(bytes_value)
        
        while value >= 1024 and unit_index < len(units) - 1:
            value /= 1024
            unit_index += 1
        
        if value >= 100:
            return f"{int(value)}{units[unit_index]}"
        elif value >= 10:
            return f"{value:.1f}{units[unit_index]}"
        else:
            return f"{value:.2f}{units[unit_index]}"
