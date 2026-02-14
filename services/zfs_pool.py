"""
ZFS Pool Management Service
Handles zpool operations: list, status, create, destroy, scrub, etc.
"""
import re
import subprocess
from typing import List, Dict, Any, Optional
from datetime import datetime
from config.settings import Settings
from services.utils import run_zfs_command

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
