"""
ZFS Pool Management Service
Handles zpool operations: list, status, create, destroy, scrub, etc.
"""
import re
import subprocess
from typing import List, Dict, Any, Optional
from datetime import datetime
from config.settings import Settings
from services.utils import run_zfs_command, is_netbsd

# Try to import libzfs_core, but fall back to shell commands if not available
try:
    import libzfs_core as lzc
    HAS_LIBZFS_CORE = True
except ImportError:
    HAS_LIBZFS_CORE = False


class ZFSPoolService:
    """Service for managing ZFS pools using libzfs_core and shell commands"""
    
    # ZFS naming pattern: alphanumeric, underscore, hyphen, period, colon
    ZFS_POOL_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*$')
    
    def __init__(self):
        """Initialize the ZFS Pool Service with settings"""
        self.settings = Settings()
        self.timeouts = self.settings.ZPOOL_TIMEOUTS
    
    @classmethod
    def validate_pool_name(cls, pool_name: str) -> None:
        """
        Validate a ZFS pool name against naming rules.
        
        ZFS pool names must:
        - Start with an alphanumeric character
        - Contain only alphanumeric characters, underscores, hyphens, periods, or colons
        
        Args:
            pool_name: The pool name to validate
            
        Raises:
            ValueError: If the pool name is invalid
        """
        if not pool_name:
            raise ValueError("Pool name cannot be empty")
        
        if not cls.ZFS_POOL_NAME_PATTERN.match(pool_name):
            raise ValueError(
                f"Invalid pool name '{pool_name}'. Pool names must start with an alphanumeric "
                "character and contain only alphanumeric characters, underscores, hyphens, "
                "periods, or colons."
            )
    
    def list_pools(self) -> List[Dict[str, Any]]:
        """
        List all ZFS pools with their key properties
        
        Returns:
            List of dictionaries containing pool information
        """
        timeout = self.timeouts.get('list', self.timeouts['default'])
        try:
            result = run_zfs_command(
                ['zpool', 'list', '-H', '-o', 'name,size,alloc,free,frag,cap,dedup,health,altroot'],
                timeout=timeout
            )
            
            pools = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 9:
                    pools.append({
                        'name': parts[0],
                        'size': parts[1],
                        'alloc': parts[2],
                        'free': parts[3],
                        'frag': parts[4],
                        'cap': parts[5],
                        'dedup': parts[6],
                        'health': parts[7],
                        'altroot': parts[8] if parts[8] != '-' else None
                    })
            
            return pools
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool list command timed out after {timeout} seconds. The system may be unresponsive or pools unavailable.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list pools: {e.stderr}")
    
    def get_pool_status(self, pool_name: str) -> Dict[str, Any]:
        """
        Get detailed status for a specific pool
        
        Args:
            pool_name: Name of the pool
            
        Returns:
            Dictionary with pool status details
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('status', self.timeouts['default'])
        try:
            # Get detailed status
            result = run_zfs_command(
                ['zpool', 'status', pool_name],
                timeout=timeout
            )
            
            # Get pool properties
            props_timeout = self.timeouts.get('properties', self.timeouts['default'])
            props_result = run_zfs_command(
                ['zpool', 'get', '-H', 'all', pool_name],
                timeout=props_timeout
            )
            
            # Parse properties
            properties = {}
            for line in props_result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 3:
                    properties[parts[1]] = {
                        'value': parts[2],
                        'source': parts[3] if len(parts) > 3 else 'default'
                    }
            
            return {
                'name': pool_name,
                'status_output': result.stdout,
                'properties': properties
            }
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool status command timed out after {timeout} seconds. The system may be unresponsive or pool unavailable.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool status: {e.stderr}")
    
    def get_pool_iostat(self, pool_name: Optional[str] = None, verbose: bool = False) -> Dict[str, Any]:
        """
        Get I/O statistics for pools
        
        Args:
            pool_name: Optional pool name to filter by
            verbose: Include per-vdev statistics
            
        Returns:
            Dictionary with I/O statistics
        """
        if pool_name:
            self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('iostat', self.timeouts['default'])
        try:
            cmd = ['zpool', 'iostat', '-H']
            if verbose:
                cmd.append('-v')
            if pool_name:
                cmd.append(pool_name)
            
            result = run_zfs_command(cmd, timeout=timeout)
            
            return {
                'output': result.stdout,
                'timestamp': datetime.now().isoformat()
            }
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool iostat command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool iostat: {e.stderr}")
    
    def scrub_pool(self, pool_name: str) -> None:
        """
        Start a scrub on the specified pool
        
        Args:
            pool_name: Name of the pool to scrub
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('scrub', self.timeouts['default'])
        try:
            run_zfs_command(['zpool', 'scrub', pool_name], timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool scrub command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to start scrub: {e.stderr}")
    
    def stop_scrub(self, pool_name: str) -> None:
        """
        Stop a running scrub on the specified pool
        
        Args:
            pool_name: Name of the pool
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('scrub', self.timeouts['default'])
        try:
            run_zfs_command(['zpool', 'scrub', '-s', pool_name], timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool stop scrub command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to stop scrub: {e.stderr}")
    
    def export_pool(self, pool_name: str, force: bool = False) -> None:
        """
        Export a ZFS pool
        
        Args:
            pool_name: Name of the pool to export
            force: Force unmount even if busy
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('export', self.timeouts['default'])
        try:
            cmd = ['zpool', 'export']
            if force:
                cmd.append('-f')
            cmd.append(pool_name)
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool export command timed out after {timeout} seconds. Pool may be busy or system unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to export pool: {e.stderr}")
    
    def import_pool(self, pool_name: str, force: bool = False, 
                   altroot: Optional[str] = None) -> None:
        """
        Import a ZFS pool
        
        Args:
            pool_name: Name of the pool to import
            force: Force import even if previously imported
            altroot: Alternative root directory
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('import', self.timeouts['default'])
        try:
            cmd = ['zpool', 'import']
            if force:
                cmd.append('-f')
            if altroot:
                cmd.extend(['-R', altroot])
            cmd.append(pool_name)
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool import command timed out after {timeout} seconds. This may occur with many devices or slow disk scanning.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to import pool: {e.stderr}")
    
    def get_pool_history(self, pool_name: str, internal: bool = False, 
                        limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Get command history for a pool
        
        Args:
            pool_name: Name of the pool
            internal: Include internal events
            limit: Maximum number of entries to return
            
        Returns:
            List of history entries
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('history', self.timeouts['default'])
        try:
            cmd = ['zpool', 'history', '-l']
            if internal:
                cmd.append('-i')
            cmd.append(pool_name)
            
            result = run_zfs_command(cmd, timeout=timeout)
            
            history = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                history.append({'entry': line})
            
            if limit:
                history = history[-limit:]
            
            return history
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool history command timed out after {timeout} seconds. History may be very large.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool history: {e.stderr}")
    
    def create_pool(self, pool_name: str, vdevs: List[str], 
                   properties: Optional[Dict[str, str]] = None,
                   force: bool = False) -> None:
        """
        Create a new ZFS pool
        
        Args:
            pool_name: Name for the new pool
            vdevs: List of vdev specifications (e.g., ['mirror', '/dev/sda', '/dev/sdb'])
            properties: Optional dictionary of pool properties to set
            force: Force creation even if devices are in use
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('create', self.timeouts['default'])
        try:
            cmd = ['zpool', 'create']
            
            if force:
                cmd.append('-f')
            
            # Add properties
            if properties:
                for key, value in properties.items():
                    cmd.extend(['-o', f'{key}={value}'])
            
            cmd.append(pool_name)
            cmd.extend(vdevs)
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool create command timed out after {timeout} seconds. Pool creation can take time with many devices.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to create pool: {e.stderr}")
    
    def destroy_pool(self, pool_name: str, force: bool = False) -> None:
        """
        Destroy a ZFS pool
        
        Args:
            pool_name: Name of the pool to destroy
            force: Force destruction even if mounted
            
        WARNING: This is a destructive operation!
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('destroy', self.timeouts['default'])
        try:
            cmd = ['zpool', 'destroy']
            if force:
                cmd.append('-f')
            cmd.append(pool_name)
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool destroy command timed out after {timeout} seconds. Pool destruction can take time with cleanup operations.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to destroy pool: {e.stderr}")
    
    def set_pool_property(self, pool_name: str, property_name: str, 
                         property_value: str) -> None:
        """
        Set a property on a pool
        
        Args:
            pool_name: Name of the pool
            property_name: Name of the property to set
            property_value: Value to set
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('properties', self.timeouts['default'])
        try:
            run_zfs_command(
                ['zpool', 'set', f'{property_name}={property_value}', pool_name],
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool set property command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to set pool property: {e.stderr}")
    
    def get_importable_pools(self) -> List[Dict[str, Any]]:
        """
        List pools available for import
        
        Returns:
            List of importable pools
        """
        timeout = self.timeouts.get('import', self.timeouts['default'])
        try:
            result = run_zfs_command(
                ['zpool', 'import'],
                check=False,  # Returns non-zero if no pools to import
                timeout=timeout
            )
            
            # Parse the output to extract pool information
            pools = []
            current_pool = None
            
            for line in result.stdout.split('\n'):
                line = line.strip()
                if line.startswith('pool:'):
                    if current_pool:
                        pools.append(current_pool)
                    current_pool = {'name': line.split(':', 1)[1].strip()}
                elif current_pool and line.startswith('id:'):
                    current_pool['id'] = line.split(':', 1)[1].strip()
                elif current_pool and line.startswith('state:'):
                    current_pool['state'] = line.split(':', 1)[1].strip()
            
            if current_pool:
                pools.append(current_pool)
            
            return pools
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool import scan timed out after {timeout} seconds. Scanning for importable pools can be slow with many devices.")
        except Exception as e:
            raise Exception(f"Failed to list importable pools: {str(e)}")
    
    def checkpoint_supported(self) -> bool:
        """
        Check if pool checkpoints are supported on this platform.
        
        NetBSD does not support pool checkpoints.
        
        Returns:
            True if checkpoints are supported, False otherwise.
        """
        return not is_netbsd()
    
    def get_checkpoint_info(self, pool_name: str) -> Optional[Dict[str, Any]]:
        """
        Get checkpoint information for a pool.
        
        Args:
            pool_name: Name of the pool
            
        Returns:
            Dictionary with checkpoint info if a checkpoint exists, None otherwise.
            Contains keys: 'exists', 'creation_time', 'space_used'
        """
        if not self.checkpoint_supported():
            return None
        
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('status', self.timeouts['default'])
        
        try:
            # Get checkpoint-related properties
            result = run_zfs_command(
                ['zpool', 'get', '-H', '-p', 'checkpoint', pool_name],
                timeout=timeout,
                check=False
            )
            
            # Parse the checkpoint property
            # Format: pool_name\tcheckpoint\t-\t- (no checkpoint)
            # Format: pool_name\tcheckpoint\t<timestamp>\t- (has checkpoint)
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 3 and parts[1] == 'checkpoint':
                    value = parts[2]
                    if value == '-' or value == '':
                        return {'exists': False}
                    
                    # Checkpoint exists, get more details from zpool status
                    status_result = run_zfs_command(
                        ['zpool', 'status', pool_name],
                        timeout=timeout
                    )
                    
                    # Parse checkpoint info from status output
                    checkpoint_info = {
                        'exists': True,
                        'creation_time': value,
                        'space_used': None
                    }
                    
                    # Look for checkpoint space info in status output
                    # The line looks like: "checkpoint: created Thu, Feb 20 2025 10:30:45, consumes 1.25G"
                    for status_line in status_result.stdout.split('\n'):
                        if 'checkpoint:' in status_line.lower():
                            checkpoint_info['status_line'] = status_line.strip()
                            # Try to extract space usage
                            if 'consumes' in status_line.lower():
                                space_part = status_line.split('consumes')[-1].strip()
                                checkpoint_info['space_used'] = space_part.split()[0] if space_part else None
                            break
                    
                    return checkpoint_info
            
            return {'exists': False}
            
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool checkpoint query timed out after {timeout} seconds.")
        except subprocess.CalledProcessError as e:
            # If the property doesn't exist, checkpoint feature may not be available
            if 'invalid property' in str(e.stderr).lower():
                return None
            raise Exception(f"Failed to get checkpoint info: {e.stderr}")
    
    def create_checkpoint(self, pool_name: str) -> None:
        """
        Create a checkpoint for the specified pool.
        
        A pool checkpoint captures the entire state of the pool at the time it is 
        created. It can be used to rewind the pool to this state later.
        
        Args:
            pool_name: Name of the pool to checkpoint
            
        Raises:
            Exception: If checkpoint creation fails or is not supported
        """
        if not self.checkpoint_supported():
            raise Exception("Pool checkpoints are not supported on NetBSD.")
        
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            run_zfs_command(
                ['zpool', 'checkpoint', pool_name],
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool checkpoint creation timed out after {timeout} seconds.")
        except subprocess.CalledProcessError as e:
            error_msg = str(e.stderr) if e.stderr else str(e)
            if 'already has a checkpoint' in error_msg.lower():
                raise Exception(f"Pool '{pool_name}' already has a checkpoint. Remove the existing checkpoint first.")
            raise Exception(f"Failed to create checkpoint: {error_msg}")
    
    def add_vdev(self, pool_name: str, vdevs: List[str],
                 force: bool = False) -> None:
        """
        Add a new vdev to an existing pool.
        
        Args:
            pool_name: Name of the pool
            vdevs: List of vdev specifications (e.g., ['mirror', '/dev/sdb', '/dev/sdc'])
            force: Force adding even if devices appear in use
            
        Raises:
            Exception: If the add operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'add']
            if force:
                cmd.append('-f')
            cmd.append(pool_name)
            cmd.extend(vdevs)
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool add vdev command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to add vdev: {e.stderr}")
    
    def attach_device(self, pool_name: str, existing_device: str,
                      new_device: str, force: bool = False) -> None:
        """
        Attach a new device to an existing device to create or extend a mirror.
        
        Args:
            pool_name: Name of the pool
            existing_device: The device already in the pool
            new_device: The new device to attach (creates/extends mirror)
            force: Force attach even if the new device appears in use
            
        Raises:
            Exception: If the attach operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'attach']
            if force:
                cmd.append('-f')
            cmd.extend([pool_name, existing_device, new_device])
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool attach command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to attach device: {e.stderr}")
    
    def detach_device(self, pool_name: str, device: str) -> None:
        """
        Detach a device from a mirror.
        
        The device must be part of a mirror vdev. After detach, the mirror
        continues with the remaining devices.
        
        Args:
            pool_name: Name of the pool
            device: The device to detach from the mirror
            
        Raises:
            Exception: If the detach operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'detach', pool_name, device]
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool detach command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to detach device: {e.stderr}")
    
    def replace_device(self, pool_name: str, old_device: str,
                       new_device: str, force: bool = False) -> None:
        """
        Replace a device in a pool with a new device.
        
        The pool will resilver data onto the new device. Progress can be
        monitored via zpool status.
        
        Args:
            pool_name: Name of the pool
            old_device: The device to replace
            new_device: The replacement device
            force: Force replace even if the new device appears in use
            
        Raises:
            Exception: If the replace operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'replace']
            if force:
                cmd.append('-f')
            cmd.extend([pool_name, old_device, new_device])
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool replace command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to replace device: {e.stderr}")
    
    def remove_vdev(self, pool_name: str, device: str) -> None:
        """
        Remove a top-level vdev from a pool.
        
        Only certain vdev types can be removed (spare, cache, log, and
        on supported platforms, top-level data vdevs via evacuation).
        
        Args:
            pool_name: Name of the pool
            device: The vdev or device to remove
            
        Raises:
            Exception: If the remove operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'remove', pool_name, device]
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool remove command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to remove vdev: {e.stderr}")
    
    def online_device(self, pool_name: str, device: str,
                      expand: bool = False) -> None:
        """
        Bring a device online in a pool.
        
        Args:
            pool_name: Name of the pool
            device: The device to bring online
            expand: If True, expand the device to use all available space
            
        Raises:
            Exception: If the online operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'online']
            if expand:
                cmd.append('-e')
            cmd.extend([pool_name, device])
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool online command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to online device: {e.stderr}")
    
    def offline_device(self, pool_name: str, device: str,
                       temporary: bool = False) -> None:
        """
        Take a device offline in a pool.
        
        Args:
            pool_name: Name of the pool
            device: The device to take offline
            temporary: If True, the device is offlined temporarily and will
                      be automatically onlined on reboot
            
        Raises:
            Exception: If the offline operation fails
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            cmd = ['zpool', 'offline']
            if temporary:
                cmd.append('-t')
            cmd.extend([pool_name, device])
            
            run_zfs_command(cmd, timeout=timeout)
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool offline command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to offline device: {e.stderr}")
    
    def get_pool_topology(self, pool_name: str) -> Dict[str, Any]:
        """
        Parse pool status into a structured topology tree.
        
        Returns a dictionary with:
        - data_vdevs: list of data vdev groups
        - log_vdevs: list of log vdev groups
        - cache_vdevs: list of cache devices
        - spare_vdevs: list of spare devices
        - special_vdevs: list of special vdev groups
        - dedup_vdevs: list of dedup vdev groups
        
        Each vdev group contains:
        - name: vdev name (e.g., 'mirror-0')
        - type: 'mirror', 'raidz1', 'raidz2', 'raidz3', 'single', etc.
        - state: vdev state
        - devices: list of leaf device dicts
        
        Each leaf device contains:
        - name: device name
        - state: device state
        - read_errors: int
        - write_errors: int
        - cksum_errors: int
        - notes: extra status info (e.g., resilvering progress)
        """
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('status', self.timeouts['default'])
        
        try:
            result = run_zfs_command(
                ['zpool', 'status', pool_name],
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise Exception(
                f"ZPool status command timed out after {timeout} seconds."
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool topology: {e.stderr}")
        
        topology = {
            'data_vdevs': [],
            'log_vdevs': [],
            'cache_vdevs': [],
            'spare_vdevs': [],
            'special_vdevs': [],
            'dedup_vdevs': [],
            'scan_info': '',
        }
        
        lines = result.stdout.split('\n')
        in_config = False
        config_lines = []
        
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith('scan:'):
                topology['scan_info'] = stripped.split(':', 1)[1].strip()
            elif 'NAME' in stripped and 'STATE' in stripped and 'READ' in stripped:
                in_config = True
                continue
            elif in_config:
                if stripped == '' or stripped.startswith('errors:'):
                    in_config = False
                else:
                    config_lines.append(line)
        
        # Parse config lines into topology tree
        # Indent levels determine hierarchy:
        # ~2 spaces (or 1 tab): pool root
        # ~4 spaces: top-level vdev or section keyword (logs, cache, spares, etc.)
        # ~6 spaces: leaf device or sub-vdev
        # ~8 spaces: leaf under mirror within section
        
        current_section = 'data'  # data, logs, cache, spares, special, dedup
        current_vdev = None
        pool_root_indent = None
        
        section_keywords = {
            'logs': 'log',
            'cache': 'cache',
            'spares': 'spare',
            'special': 'special',
            'dedup': 'dedup',
        }
        
        vdev_type_keywords = {
            'mirror', 'raidz1', 'raidz2', 'raidz3', 'raidz', 'draid',
        }
        
        for line in config_lines:
            stripped = line.lstrip()
            if not stripped:
                continue
            indent = len(line) - len(stripped)
            parts = stripped.split()
            if not parts:
                continue
            
            name = parts[0]
            state = parts[1] if len(parts) > 1 else 'UNKNOWN'
            read_err = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            write_err = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            cksum_err = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0
            notes = ' '.join(parts[5:]) if len(parts) > 5 else ''
            
            # Determine pool root indent (first line is pool name)
            if pool_root_indent is None:
                pool_root_indent = indent
                continue  # Skip pool root line
            
            # Check if this is a section keyword
            if name.lower() in section_keywords:
                current_section = section_keywords[name.lower()]
                current_vdev = None
                continue
            
            # Determine if this is a vdev group or leaf device
            is_vdev_type = False
            for vt in vdev_type_keywords:
                if name.lower().startswith(vt):
                    is_vdev_type = True
                    break
            
            device_entry = {
                'name': name,
                'state': state,
                'read_errors': read_err,
                'write_errors': write_err,
                'cksum_errors': cksum_err,
                'notes': notes,
            }
            
            section_key = current_section + '_vdevs'
            if section_key not in topology:
                section_key = 'data_vdevs'
            
            if is_vdev_type:
                # This is a vdev group (mirror-0, raidz1-0, etc.)
                vdev_type = name.split('-')[0] if '-' in name else name
                current_vdev = {
                    'name': name,
                    'type': vdev_type,
                    'state': state,
                    'read_errors': read_err,
                    'write_errors': write_err,
                    'cksum_errors': cksum_err,
                    'devices': [],
                }
                topology[section_key].append(current_vdev)
            elif current_vdev is not None:
                # This is a leaf device under the current vdev
                current_vdev['devices'].append(device_entry)
            else:
                # This is a standalone device (no mirror/raidz group)
                # Wrap it in a single-device vdev group
                standalone_vdev = {
                    'name': name,
                    'type': 'single',
                    'state': state,
                    'read_errors': read_err,
                    'write_errors': write_err,
                    'cksum_errors': cksum_err,
                    'devices': [device_entry],
                }
                topology[section_key].append(standalone_vdev)
        
        return topology
    
    def discard_checkpoint(self, pool_name: str) -> None:
        """
        Discard (remove) the checkpoint for the specified pool.
        
        This releases the space held by the checkpoint. The checkpoint cannot be 
        recovered after it is discarded.
        
        Args:
            pool_name: Name of the pool
            
        Raises:
            Exception: If checkpoint removal fails or is not supported
        """
        if not self.checkpoint_supported():
            raise Exception("Pool checkpoints are not supported on NetBSD.")
        
        self.validate_pool_name(pool_name)
        timeout = self.timeouts.get('default', 60)
        
        try:
            run_zfs_command(
                ['zpool', 'checkpoint', '-d', pool_name],
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool checkpoint discard timed out after {timeout} seconds.")
        except subprocess.CalledProcessError as e:
            error_msg = str(e.stderr) if e.stderr else str(e)
            if 'does not have a checkpoint' in error_msg.lower():
                raise Exception(f"Pool '{pool_name}' does not have a checkpoint to discard.")
            raise Exception(f"Failed to discard checkpoint: {error_msg}")
