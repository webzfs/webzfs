"""
Fleet Monitoring Service
Manages remote server monitoring via SSH for ZFS pool status viewing
"""
import json
import os
import re
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
                
                # Get ZFS used/available space (actual usable space for users)
                space_map = {}
                try:
                    space_command = self._build_zfs_command(server, "zfs list -H -p -o name,used,avail -d 0")
                    s_stdin, s_stdout, s_stderr = client.exec_command(space_command)
                    space_output = s_stdout.read().decode('utf-8')
                    for space_line in space_output.strip().split('\n'):
                        if space_line:
                            sp_parts = space_line.split('\t')
                            if len(sp_parts) >= 3:
                                try:
                                    space_map[sp_parts[0]] = {
                                        'used': int(sp_parts[1]),
                                        'avail': int(sp_parts[2]),
                                    }
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
                            space = space_map.get(name)
                            if space is not None:
                                used_bytes = space['used']
                                avail_bytes = space['avail']
                                total_bytes = used_bytes + avail_bytes
                                used_str = self._format_bytes(used_bytes)
                                avail_str = self._format_bytes(avail_bytes)
                                total_str = self._format_bytes(total_bytes)
                                if total_bytes > 0:
                                    used_pct = round((used_bytes / total_bytes) * 100)
                                    cap_str = f"{used_pct}%"
                                else:
                                    cap_str = f"{cap}%"
                            else:
                                used_str = self._format_bytes(int(alloc))
                                avail_str = self._format_bytes(int(free))
                                total_str = self._format_bytes(int(size))
                                cap_str = f"{cap}%"
                            pools.append({
                                "name": name,
                                "used": used_str,
                                "free": avail_str,
                                "total": total_str,
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
    
    def fetch_server_pools_extended(self, server_id: str) -> List[Dict[str, Any]]:
        """
        Fetch extended pool information from a remote server, matching the
        dashboard pool format. Includes dataset/snapshot counts, vdev/disk
        counts, and error totals in addition to the basic size/health data.

        Args:
            server_id: Server UUID

        Returns:
            List of pool dicts with dashboard-compatible field names
        """
        try:
            server = self._get_server_by_id(server_id)
            client = self._create_ssh_client(server)

            try:
                # Basic pool list
                command = self._build_zfs_command(
                    server,
                    "zpool list -H -p -o name,size,alloc,free,cap,health",
                )
                stdin, stdout, stderr = client.exec_command(command)
                output = stdout.read().decode("utf-8")
                error = stderr.read().decode("utf-8")

                if error and not output:
                    logger.error(
                        f"Error fetching pools from {server_id}: {error}"
                    )
                    return []

                # ZFS used/available from dataset layer
                space_map = {}
                try:
                    space_cmd = self._build_zfs_command(
                        server, "zfs list -H -p -o name,used,avail -d 0"
                    )
                    s_in, s_out, s_err = client.exec_command(space_cmd)
                    for line in s_out.read().decode("utf-8").strip().split("\n"):
                        if line:
                            sp = line.split("\t")
                            if len(sp) >= 3:
                                try:
                                    space_map[sp[0]] = {
                                        "used": int(sp[1]),
                                        "avail": int(sp[2]),
                                    }
                                except (ValueError, TypeError):
                                    pass
                except Exception:
                    pass

                # Dataset counts
                dataset_counts: Dict[str, int] = {}
                try:
                    ds_cmd = self._build_zfs_command(
                        server,
                        "zfs list -H -t filesystem -o name",
                    )
                    d_in, d_out, d_err = client.exec_command(ds_cmd)
                    for line in d_out.read().decode("utf-8").strip().split("\n"):
                        name = line.strip()
                        if name:
                            pool = name.split("/")[0]
                            dataset_counts[pool] = dataset_counts.get(pool, 0) + 1
                except Exception:
                    pass

                # Snapshot counts
                snapshot_counts: Dict[str, int] = {}
                try:
                    snap_cmd = self._build_zfs_command(
                        server,
                        "zfs list -H -t snapshot -o name",
                    )
                    sn_in, sn_out, sn_err = client.exec_command(snap_cmd)
                    for line in sn_out.read().decode("utf-8").strip().split("\n"):
                        name = line.strip()
                        if name:
                            pool = name.split("/")[0].split("@")[0]
                            snapshot_counts[pool] = snapshot_counts.get(pool, 0) + 1
                except Exception:
                    pass

                # Parse basic pool info
                pools = []
                for line in output.strip().split("\n"):
                    if not line:
                        continue
                    parts = line.split("\t")
                    if len(parts) < 6:
                        continue
                    name, size, alloc, free, cap, health = parts[:6]

                    space = space_map.get(name)
                    if space is not None:
                        used_bytes = space["used"]
                        avail_bytes = space["avail"]
                        total_bytes = used_bytes + avail_bytes
                        used_str = self._format_bytes(used_bytes)
                        avail_str = self._format_bytes(avail_bytes)
                        total_str = self._format_bytes(total_bytes)
                        if total_bytes > 0:
                            cap_str = f"{round((used_bytes / total_bytes) * 100)}%"
                        else:
                            cap_str = f"{cap}%"
                    else:
                        used_str = self._format_bytes(int(alloc))
                        avail_str = self._format_bytes(int(free))
                        total_str = self._format_bytes(int(size))
                        cap_str = f"{cap}%"

                    # Compute byte values for sorting/aggregation
                    if space is not None:
                        u_bytes = space["used"]
                        a_bytes = space["avail"]
                        t_bytes = u_bytes + a_bytes
                    else:
                        u_bytes = int(alloc)
                        a_bytes = int(free)
                        t_bytes = int(size)

                    pools.append({
                        "name": name,
                        "used": used_str,
                        "avail": avail_str,
                        "total": total_str,
                        "cap": cap_str,
                        "health": health,
                        "used_bytes": u_bytes,
                        "avail_bytes": a_bytes,
                        "total_bytes": t_bytes,
                        "dataset_count": dataset_counts.get(name, 0),
                        "snapshot_count": snapshot_counts.get(name, 0),
                        "vdev_count": 0,
                        "disk_count": 0,
                        "read_errors": 0,
                        "write_errors": 0,
                        "cksum_errors": 0,
                    })

                # Fetch vdev/disk/error counts per pool via zpool status
                vdev_keywords = {
                    "mirror", "raidz", "raidz1", "raidz2", "raidz3",
                    "spare", "cache", "log", "dedup", "special",
                }
                for pool in pools:
                    try:
                        status_cmd = self._build_zfs_command(
                            server, f"zpool status {pool['name']}"
                        )
                        st_in, st_out, st_err = client.exec_command(status_cmd)
                        status_output = st_out.read().decode("utf-8")

                        in_config = False
                        for sline in status_output.split("\n"):
                            stripped = sline.strip()
                            if stripped.lower().startswith("config:"):
                                in_config = True
                                continue
                            if in_config and stripped.lower().startswith("errors:"):
                                in_config = False
                                continue
                            if not in_config:
                                continue
                            if "NAME" in stripped and "STATE" in stripped:
                                continue
                            sp = stripped.split()
                            if not sp:
                                continue
                            sname = sp[0]
                            if sname == pool["name"]:
                                continue

                            base = re.sub(r"-\d+$", "", sname.lower())
                            if base in vdev_keywords:
                                pool["vdev_count"] += 1
                            else:
                                pool["disk_count"] += 1
                                pool["read_errors"] += self._safe_int(sp[2]) if len(sp) > 2 else 0
                                pool["write_errors"] += self._safe_int(sp[3]) if len(sp) > 3 else 0
                                pool["cksum_errors"] += self._safe_int(sp[4]) if len(sp) > 4 else 0

                        if pool["vdev_count"] == 0 and pool["disk_count"] > 0:
                            pool["vdev_count"] = 1
                    except Exception:
                        pass

                return pools

            finally:
                client.close()

        except Exception as e:
            logger.error(
                f"Failed to fetch extended pools from server {server_id}: {e}"
            )
            return []

    @staticmethod
    def _safe_int(value: str) -> int:
        """Convert a string to int, returning 0 on failure."""
        try:
            return int(value)
        except (ValueError, TypeError):
            return 0

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
    
    def fetch_pool_space_tree(
        self,
        server_id: str,
        pool_name: str,
        max_depth: int = 4,
    ) -> Dict[str, Any]:
        """
        Fetch a dataset space-usage tree for a single pool on a remote
        server.

        Mirrors the behaviour of ZFSDatasetService.get_space_tree but
        runs the underlying 'zfs list' calls over the server's SSH
        connection. Returns the same nested dict shape so the front-end
        renderer can treat both local and remote data identically.

        Args:
            server_id: Server UUID.
            pool_name: ZFS pool to inspect.
            max_depth: Maximum depth (including the pool root) to
                include. Children deeper than this are still summed via
                'usedbychildren' but are not added as explicit nodes.

        Returns:
            Nested dict matching the local visualizer schema.
        """
        # Reject anything that is not a plain dataset-name token to
        # avoid arbitrary command injection over the SSH channel.
        if not pool_name or any(c.isspace() for c in pool_name):
            raise ValueError("Invalid pool name")
        for ch in pool_name:
            if not (ch.isalnum() or ch in "._-:/"):
                raise ValueError("Invalid pool name")

        server = self._get_server_by_id(server_id)
        client = self._create_ssh_client(server)
        try:
            properties = (
                "name,used,referenced,available,"
                "usedbysnapshots,usedbychildren,usedbydataset,compressratio"
            )
            ds_command = self._build_zfs_command(
                server,
                "zfs list -Hp -t filesystem,volume -o "
                + properties
                + " -r "
                + pool_name,
            )
            stdin, stdout, stderr = client.exec_command(ds_command)
            ds_output = stdout.read().decode("utf-8")
            ds_error = stderr.read().decode("utf-8")
            if ds_error and not ds_output:
                raise Exception(ds_error.strip() or "zfs list failed")

            snapshot_counts: Dict[str, int] = {}
            try:
                snap_command = self._build_zfs_command(
                    server,
                    "zfs list -Hp -t snapshot -o name -r " + pool_name,
                )
                s_stdin, s_stdout, s_stderr = client.exec_command(snap_command)
                snap_output = s_stdout.read().decode("utf-8")
                for line in snap_output.strip().split("\n"):
                    if not line or "@" not in line:
                        continue
                    parent = line.split("@", 1)[0]
                    snapshot_counts[parent] = snapshot_counts.get(parent, 0) + 1
            except Exception:
                snapshot_counts = {}

            def _to_int(token: str) -> int:
                token = (token or "").strip()
                if not token or token == "-":
                    return 0
                try:
                    return int(token)
                except ValueError:
                    return 0

            nodes: Dict[str, Dict[str, Any]] = {}
            order: List[str] = []
            for line in ds_output.strip().split("\n"):
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 8:
                    continue
                name = parts[0]
                nodes[name] = {
                    "name": name,
                    "used": _to_int(parts[1]),
                    "referenced": _to_int(parts[2]),
                    "available": _to_int(parts[3]),
                    "used_by_snapshots": _to_int(parts[4]),
                    "used_by_children": _to_int(parts[5]),
                    "used_by_dataset": _to_int(parts[6]),
                    "compressratio": parts[7],
                    "snapshot_count": snapshot_counts.get(name, 0),
                    "children": [],
                }
                order.append(name)

            if pool_name not in nodes:
                raise Exception(
                    "Pool " + pool_name + " not found in zfs list output"
                )

            root_depth = pool_name.count("/")
            for name in order:
                if name == pool_name:
                    continue
                parent_name = name.rsplit("/", 1)[0]
                parent = nodes.get(parent_name)
                if parent is None:
                    continue
                depth_from_root = name.count("/") - root_depth
                if depth_from_root >= max_depth:
                    continue
                parent["children"].append(nodes[name])

            return nodes[pool_name]
        finally:
            client.close()

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
