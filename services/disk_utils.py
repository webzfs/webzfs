"""
Disk Utilities Service
Provides disk enumeration and information
"""
import subprocess
import re
import os
from typing import List, Dict, Any, Optional, Tuple

from services.utils import is_freebsd, run_zfs_command, run_privileged_command
from config.settings import Settings


class DiskUtilsService:
    """Service for discovering and managing disk information"""
    
    def __init__(self):
        """Initialize the Disk Utils Service with settings"""
        self.settings = Settings()
        self.timeouts = self.settings.ZPOOL_TIMEOUTS
    
    def get_available_disks(self) -> List[Dict[str, Any]]:
        """
        Get list of available disks on the system
        
        Returns:
            List of dictionaries containing disk information
        """
        if is_freebsd():
            # FreeBSD: Use geom to list disks
            return self._get_available_disks_freebsd()
        else:
            # Linux (default): Use lsblk to list disks
            return self._get_available_disks_linux()
    
    def _get_available_disks_linux(self) -> List[Dict[str, Any]]:
        """
        Get available disks on Linux using lsblk
        
        Returns:
            List of dictionaries containing disk information
        """
        disks = []
        
        # Get system disks (OS/swap) that should be excluded
        system_disks = self._get_system_disks_linux()
        
        try:
            # Get disk list using lsblk (works on Linux)
            result = subprocess.run(
                ['lsblk', '-d', '-n', '-o', 'NAME,SIZE,TYPE,MODEL,ROTA'],
                capture_output=True,
                text=True,
                check=True
            )
            
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split(None, 4)
                if len(parts) >= 4 and parts[2] == 'disk':
                    name = parts[0]
                    size = parts[1]
                    model = parts[3] if len(parts) > 3 else 'Unknown'
                    rota = parts[4] if len(parts) > 4 else '1'
                    
                    # Determine disk type (SSD vs HDD)
                    disk_type = 'HDD' if rota == '1' else 'SSD'
                    
                    # Check if disk is in use by ZFS
                    in_use = self._is_disk_in_use(name)
                    
                    # Check if this is a system disk (OS or swap)
                    is_system_disk = name in system_disks
                    
                    disks.append({
                        'name': name,
                        'device_path': f'/dev/{name}',
                        'size': size,
                        'model': model,
                        'type': disk_type,
                        'in_use': in_use or is_system_disk,  # Mark system disks as in use
                        'is_system_disk': is_system_disk,
                        'system_usage': system_disks.get(name, None),
                        'exported': False
                    })
            
        except subprocess.CalledProcessError:
            # If lsblk fails, return empty list
            pass
        
        return disks
    
    def _get_available_disks_freebsd(self) -> List[Dict[str, Any]]:
        """
        Get available disks on FreeBSD using sysctl and geom
        
        Returns:
            List of dictionaries containing disk information
        """
        disks = []
        
        # Get system disks (OS/swap) that should be excluded
        system_disks = self._get_system_disks_freebsd()
        
        try:
            # Get list of physical disks using sysctl
            result = subprocess.run(
                ['sysctl', '-n', 'kern.disks'],
                capture_output=True,
                text=True,
                check=True
            )
            
            disk_names = result.stdout.strip().split() if result.stdout else []
            
            # Get detailed info for each disk using geom
            for disk_name in disk_names:
                try:
                    disk_info = self._get_freebsd_disk_info(disk_name)
                    if disk_info:
                        # Check if this is a system disk
                        is_system_disk = disk_name in system_disks
                        disk_info['in_use'] = disk_info.get('in_use', False) or is_system_disk
                        disk_info['is_system_disk'] = is_system_disk
                        disk_info['system_usage'] = system_disks.get(disk_name, None)
                        disks.append(disk_info)
                except Exception:
                    # Skip disks that fail to query
                    continue
            
        except subprocess.CalledProcessError:
            # Fallback to geom disk list if sysctl fails
            try:
                result = subprocess.run(
                    ['geom', 'disk', 'list'],
                    capture_output=True,
                    text=True,
                    check=True
                )
                
                disks = self._parse_geom_output(result.stdout)
                
                # Mark system disks in fallback method
                for disk in disks:
                    is_system_disk = disk['name'] in system_disks
                    disk['in_use'] = disk.get('in_use', False) or is_system_disk
                    disk['is_system_disk'] = is_system_disk
                    disk['system_usage'] = system_disks.get(disk['name'], None)
                
            except subprocess.CalledProcessError:
                pass
        
        return disks
    
    def _get_freebsd_disk_info(self, disk_name: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a FreeBSD disk
        
        Args:
            disk_name: Disk name (e.g., 'ada0', 'da0', 'vtbd0')
            
        Returns:
            Dictionary with disk information or None
        """
        try:
            # Get disk details from geom
            result = subprocess.run(
                ['geom', 'disk', 'list', disk_name],
                capture_output=True,
                text=True,
                check=True
            )
            
            disk_info = {
                'name': disk_name,
                'device_path': f'/dev/{disk_name}',
                'size': 'Unknown',
                'model': 'Unknown',
                'type': 'HDD',
                'in_use': False,
                'exported': False
            }
            
            # Parse geom output
            for line in result.stdout.split('\n'):
                line = line.strip()
                
                if line.startswith('Mediasize:'):
                    # Extract human-readable size from parentheses
                    match = re.search(r'\(([^)]+)\)', line)
                    if match:
                        disk_info['size'] = match.group(1)
                
                elif line.startswith('descr:'):
                    disk_info['model'] = line.split(':', 1)[1].strip()
                
                elif line.startswith('ident:'):
                    # Determine disk type from ident
                    ident = line.split(':', 1)[1].strip().lower()
                    if 'ssd' in ident or 'nvme' in ident or 'solid' in ident:
                        disk_info['type'] = 'SSD'
            
            # Check if disk is in use by ZFS
            disk_info['in_use'] = self._is_disk_in_use(disk_name)
            
            return disk_info
            
        except subprocess.CalledProcessError:
            return None
    
    def _get_system_disks_linux(self) -> Dict[str, str]:
        """
        Get disks that are used by the OS (root, boot, swap, etc.) on Linux
        Handles encrypted systems, LVM, and device mapper
        
        Returns:
            Dictionary mapping disk names to their usage type
        """
        system_disks = {}
        
        try:
            # Check for mounted filesystems using lsblk with more details
            # This will show the full device hierarchy including encrypted volumes
            result = subprocess.run(
                ['lsblk', '-n', '-o', 'NAME,MOUNTPOINT,TYPE'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    parts = line.split(None, 2)
                    if len(parts) >= 2:
                        device = parts[0]
                        mountpoint = parts[1] if len(parts) > 1 else ''
                        device_type = parts[2] if len(parts) > 2 else ''
                        
                        # Check for critical mount points (including /efi for EFI systems)
                        critical_mounts = ['/', '/boot', '/boot/efi', '/efi', '/home', '/usr', '/var']
                        if mountpoint in critical_mounts:
                            # Extract base disk name (sda1 -> sda, nvme0n1p1 -> nvme0n1)
                            base_disk = self._get_base_disk_name_linux(device)
                            if base_disk:
                                if base_disk not in system_disks:
                                    system_disks[base_disk] = f"OS disk (mounted: {mountpoint})"
        except Exception:
            pass
        
        try:
            # Additional check: trace encrypted/LVM devices back to physical disks
            # Check /dev/mapper devices and find their underlying physical disks
            result = subprocess.run(
                ['lsblk', '-n', '-o', 'NAME,TYPE'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                # Build a map of crypt/lvm devices to their parents
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 2:
                        device = parts[0]
                        device_type = parts[1]
                        
                        # If it's a crypt or lvm device, find its parent physical disk
                        if device_type in ['crypt', 'lvm']:
                            # Use lsblk to find the parent device
                            parent_result = subprocess.run(
                                ['lsblk', '-n', '-o', 'NAME', f'/dev/{device}'],
                                capture_output=True,
                                text=True,
                                check=False
                            )
                            
                            if parent_result.returncode == 0:
                                # The first line is the device itself, look for underlying disks
                                lines = parent_result.stdout.strip().split('\n')
                                for parent_line in lines:
                                    parent_dev = parent_line.strip()
                                    # Skip the device itself and extract base disk
                                    if parent_dev and parent_dev != device:
                                        base_disk = self._get_base_disk_name_linux(parent_dev)
                                        if base_disk and base_disk not in system_disks:
                                            system_disks[base_disk] = "OS disk (encrypted/LVM)"
        except Exception:
            pass
        
        try:
            # Check for swap devices
            with open('/proc/swaps', 'r') as f:
                lines = f.readlines()
                for line in lines[1:]:  # Skip header
                    parts = line.split()
                    if parts and parts[0].startswith('/dev/'):
                        device = parts[0].replace('/dev/', '')
                        # Extract base disk name
                        base_disk = self._get_base_disk_name_linux(device)
                        if base_disk:
                            if base_disk not in system_disks:
                                system_disks[base_disk] = "Swap disk"
                            else:
                                system_disks[base_disk] += " / Swap"
        except Exception:
            pass
        
        # Additional safeguard: Use findmnt to trace device mapper/encrypted volumes
        try:
            result = subprocess.run(
                ['findmnt', '-n', '-o', 'SOURCE,TARGET'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line:
                        continue
                    
                    parts = line.split(None, 1)
                    if len(parts) >= 2:
                        source = parts[0]
                        target = parts[1]
                        
                        # Check critical mount points
                        critical_mounts = ['/', '/boot', '/boot/efi', '/efi', '/home', '/usr', '/var']
                        if target in critical_mounts:
                            # Handle /dev/mapper/ devices
                            if source.startswith('/dev/mapper/') or source.startswith('/dev/dm-'):
                                # Try to find the underlying physical device
                                try:
                                    # Use dmsetup to trace back to physical device
                                    dm_result = subprocess.run(
                                        ['dmsetup', 'deps', '-o', 'devname', source],
                                        capture_output=True,
                                        text=True,
                                        check=False
                                    )
                                    
                                    if dm_result.returncode == 0:
                                        # Extract device names from output
                                        # Output format: "1 dependencies : (sda1)"
                                        import re
                                        deps = re.findall(r'\(([^)]+)\)', dm_result.stdout)
                                        for dep in deps:
                                            base_disk = self._get_base_disk_name_linux(dep)
                                            if base_disk and base_disk not in system_disks:
                                                system_disks[base_disk] = f"OS disk (mounted: {target})"
                                except Exception:
                                    pass
                            elif source.startswith('/dev/'):
                                # Regular device
                                device = source.replace('/dev/', '')
                                base_disk = self._get_base_disk_name_linux(device)
                                if base_disk and base_disk not in system_disks:
                                    system_disks[base_disk] = f"OS disk (mounted: {target})"
        except Exception:
            pass
        
        return system_disks
    
    def _get_system_disks_freebsd(self) -> Dict[str, str]:
        """
        Get disks that are used by the OS (root, boot, swap, etc.) on FreeBSD
        
        Returns:
            Dictionary mapping disk names to their usage type
        """
        system_disks = {}
        
        try:
            # Check for mounted filesystems
            result = subprocess.run(
                ['mount'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    if not line or not line.startswith('/dev/'):
                        continue
                    
                    # Format: /dev/ada0p2 on / (ufs, local, journaled soft-updates)
                    parts = line.split()
                    if len(parts) >= 3:
                        device = parts[0].replace('/dev/', '')
                        mountpoint = parts[2]
                        
                        # Check for critical mount points
                        critical_mounts = ['/', '/boot', '/usr', '/var', '/home']
                        if mountpoint in critical_mounts:
                            # Extract base disk name (ada0p2 -> ada0, nvme0n1p1 -> nvme0n1)
                            base_disk = self._get_base_disk_name_freebsd(device)
                            if base_disk:
                                if base_disk not in system_disks:
                                    system_disks[base_disk] = f"OS disk (mounted: {mountpoint})"
        except Exception:
            pass
        
        try:
            # Check for swap devices using swapinfo
            result = subprocess.run(
                ['swapinfo'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n')[1:]:  # Skip header
                    if not line or not line.startswith('/dev/'):
                        continue
                    
                    parts = line.split()
                    if parts:
                        device = parts[0].replace('/dev/', '')
                        # Extract base disk name
                        base_disk = self._get_base_disk_name_freebsd(device)
                        if base_disk:
                            if base_disk not in system_disks:
                                system_disks[base_disk] = "Swap disk"
                            else:
                                system_disks[base_disk] += " / Swap"
        except Exception:
            pass
        
        return system_disks
    
    def _get_base_disk_name_linux(self, device: str) -> Optional[str]:
        """
        Extract base disk name from a partition device name (Linux)
        
        Args:
            device: Device name (e.g., 'sda1', 'nvme0n1p1')
            
        Returns:
            Base disk name (e.g., 'sda', 'nvme0n1') or None
        """
        # Remove partition numbers
        # NVMe: nvme0n1p1 -> nvme0n1
        if 'nvme' in device:
            match = re.match(r'(nvme\d+n\d+)p?\d*', device)
            if match:
                return match.group(1)
        
        # Standard: sda1 -> sda, vda2 -> vda
        match = re.match(r'([a-z]+)(\d+)?', device)
        if match:
            return match.group(1)
        
        return None
    
    def _get_base_disk_name_freebsd(self, device: str) -> Optional[str]:
        """
        Extract base disk name from a partition device name (FreeBSD)
        
        Args:
            device: Device name (e.g., 'ada0p1', 'nvme0n1p1')
            
        Returns:
            Base disk name (e.g., 'ada0', 'nvme0n1') or None
        """
        # Remove partition/slice indicators
        # NVMe: nvme0n1p1 -> nvme0n1
        if 'nvme' in device:
            match = re.match(r'(nvme\d+n\d+)p?\d*', device)
            if match:
                return match.group(1)
        
        # FreeBSD disks: ada0p1 -> ada0, da0s1 -> da0
        match = re.match(r'((?:ada|da|vtbd)\d+)(?:p|s)?\d*[a-z]?', device)
        if match:
            return match.group(1)
        
        return None
    
    def _is_disk_in_use(self, disk_name: str) -> bool:
        """
        Check if a disk is currently in use by ZFS
        
        Args:
            disk_name: Name of the disk (e.g., 'sda')
            
        Returns:
            True if disk is in use, False otherwise
        """
        timeout = self.timeouts.get('status', self.timeouts['default'])
        try:
            result = run_zfs_command(
                ['zpool', 'status'],
                timeout=timeout
            )
            
            # Check if disk name appears in zpool status
            return disk_name in result.stdout
        
        except subprocess.TimeoutExpired:
            # If timeout, assume disk might be in use (safer default)
            return False
        except subprocess.CalledProcessError:
            return False
    
    def check_disk_usage_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Comprehensive check of disk usage status
        Checks zpool status, zdb -l, and other sources to determine disk usage
        
        Returns:
            Dictionary mapping device paths to status information:
            {
                '/dev/sda': {
                    'in_active_pool': bool,  # True if in active zpool
                    'has_zfs_label': bool,   # True if has ZFS label
                    'pool_name': str or None,  # Pool name if found
                    'status': 'active' | 'labeled' | 'available'
                }
            }
        """
        disk_status = {}
        
        # Get list of all disks
        all_disks = self.get_available_disks()
        
        # Initialize status for all disks
        for disk in all_disks:
            disk_status[disk['device_path']] = {
                'in_active_pool': False,
                'has_zfs_label': False,
                'pool_name': None,
                'status': 'available'
            }
        
        # Check active pools with zpool status
        active_pool_disks = self._get_active_pool_disks()
        for device_path, pool_name in active_pool_disks.items():
            if device_path in disk_status:
                disk_status[device_path]['in_active_pool'] = True
                disk_status[device_path]['pool_name'] = pool_name
                disk_status[device_path]['status'] = 'active'
        
        # Check for ZFS labels on disks not in active pools
        for device_path in disk_status:
            if not disk_status[device_path]['in_active_pool']:
                has_label, pool_name = self._check_zfs_label(device_path)
                if has_label:
                    disk_status[device_path]['has_zfs_label'] = True
                    disk_status[device_path]['pool_name'] = pool_name
                    disk_status[device_path]['status'] = 'labeled'
        
        return disk_status
    
    def _get_active_pool_disks(self) -> Dict[str, str]:
        """
        Get disks that are in active ZFS pools using zpool status
        
        Returns:
            Dictionary mapping device paths to pool names
        """
        pool_disks = {}
        list_timeout = self.timeouts.get('list', self.timeouts['default'])
        status_timeout = self.timeouts.get('status', self.timeouts['default'])
        
        try:
            # Get list of all pools
            result = run_zfs_command(
                ['zpool', 'list', '-H', '-o', 'name'],
                timeout=list_timeout
            )
            
            pool_names = result.stdout.strip().split('\n')
            
            # For each pool, get its disks
            for pool_name in pool_names:
                if not pool_name:
                    continue
                
                try:
                    status_result = run_zfs_command(
                        ['zpool', 'status', pool_name],
                        timeout=status_timeout
                    )
                    
                    # Parse zpool status output to extract device paths
                    devices = self._parse_zpool_status_devices(status_result.stdout)
                    
                    for device in devices:
                        # Normalize device path
                        normalized = self._normalize_device_path(device)
                        if normalized:
                            pool_disks[normalized] = pool_name
                
                except subprocess.TimeoutExpired:
                    # Skip this pool if it times out
                    continue
                except subprocess.CalledProcessError:
                    continue
        
        except subprocess.TimeoutExpired:
            # If listing pools times out, return empty dict
            pass
        except subprocess.CalledProcessError:
            pass
        
        return pool_disks
    
    def _parse_zpool_status_devices(self, status_output: str) -> List[str]:
        """
        Parse zpool status output to extract device paths
        
        Args:
            status_output: Output from zpool status command
            
        Returns:
            List of device paths found in the status
        """
        devices = []
        in_config_section = False
        
        for line in status_output.split('\n'):
            stripped = line.strip()
            
            # Look for the config section
            if 'config:' in stripped.lower():
                in_config_section = True
                continue
            
            # Stop at errors section or end
            if in_config_section and ('errors:' in stripped.lower() or stripped.startswith('---')):
                break
            
            if in_config_section:
                # Skip header lines
                if 'NAME' in stripped and 'STATE' in stripped:
                    continue
                
                # Extract device names (they start with various identifiers)
                parts = stripped.split()
                if parts:
                    device = parts[0]
                    
                    # Skip empty lines
                    if not device:
                        continue
                    
                    # Filter out vdev types and pool names
                    vdev_types = {'mirror', 'raidz', 'raidz1', 'raidz2', 'raidz3', 
                                  'spare', 'cache', 'log', 'dedup', 'special', 'draid',
                                  'logs', 'spares', 'caches'}
                    
                    if device.lower() in vdev_types:
                        continue
                    
                    # Check if it looks like a device path, disk name, or disk-by-id identifier
                    # Include: /dev/sda, sda, nvme0n1, nvme-CT4000T500SSD3_XXX, ata-WDC_XXX, etc.
                    is_device = False
                    
                    if '/' in device:
                        # It's a path
                        is_device = True
                    elif device.startswith(('sd', 'nvme', 'ada', 'da', 'vtbd', 'hd', 'vd')):
                        # It's a simple disk name
                        is_device = True
                    elif '-' in device or '_' in device:
                        # Likely a disk-by-id identifier (nvme-XXX, ata-XXX, etc.)
                        is_device = True
                    
                    if is_device:
                        devices.append(device)
        
        return devices
    
    def _normalize_device_path(self, device: str) -> Optional[str]:
        """
        Normalize device path to /dev/diskname format
        Also strips partition numbers to get base disk
        
        Args:
            device: Device identifier (could be /dev/sda1, sda, nvme-XXX, disk/by-id/xxx, etc.)
            
        Returns:
            Normalized device path or None
        """
        # If already a full path, resolve it
        if device.startswith('/'):
            try:
                # Resolve symlinks
                real_path = os.path.realpath(device)
                # Strip partition number to get base disk
                base_disk = self._strip_partition_number(real_path)
                return base_disk
            except:
                # Still try to strip partition number even if realpath fails
                return self._strip_partition_number(device)
        
        if is_freebsd():
            # FreeBSD: Handle FreeBSD-specific device identifiers
            # Check for GPT labels, GPT IDs, and disk IDs
            if device.startswith(('gpt/', 'gptid/', 'diskid/', 'label/')):
                # It's a label/ID, try to resolve it
                full_path = f'/dev/{device}'
                if os.path.exists(full_path):
                    try:
                        real_path = os.path.realpath(full_path)
                        base_disk = self._strip_partition_number(real_path)
                        return base_disk
                    except:
                        pass
            
            # Direct device name
            if device.startswith(('ada', 'da', 'vtbd', 'nvme')):
                full_path = f'/dev/{device}'
                return self._strip_partition_number(full_path)
            
            # Check common FreeBSD label directories
            for prefix in ['gpt', 'gptid', 'diskid', 'label']:
                label_path = f'/dev/{prefix}/{device}'
                if os.path.exists(label_path):
                    try:
                        real_path = os.path.realpath(label_path)
                        base_disk = self._strip_partition_number(real_path)
                        return base_disk
                    except:
                        continue
        else:
            # Linux (default): Handle Linux-specific device identifiers
            # Check if it looks like a disk-by-id identifier
            if '-' in device and not device.startswith('/'):
                # Try common locations for disk identifiers
                possible_paths = [
                    f'/dev/disk/by-id/{device}',
                    f'/dev/disk/by-uuid/{device}',
                    f'/dev/disk/by-path/{device}',
                    f'/dev/{device}'  # Fallback
                ]
                
                for path in possible_paths:
                    if os.path.exists(path):
                        try:
                            # Resolve the symlink to get actual device
                            real_path = os.path.realpath(path)
                            # Strip partition number to get base disk
                            base_disk = self._strip_partition_number(real_path)
                            return base_disk
                        except:
                            continue
            
            # If it's just a disk name (sda, nvme0n1, etc.), prepend /dev/
            if device.startswith(('sd', 'nvme', 'hd', 'vd')):
                full_path = f'/dev/{device}'
                # Strip partition number
                return self._strip_partition_number(full_path)
        
        return None
    
    def _strip_partition_number(self, device_path: str) -> str:
        """
        Strip partition number from device path to get base disk
        
        Examples:
            Linux: /dev/sda1 -> /dev/sda, /dev/nvme0n1p1 -> /dev/nvme0n1
            FreeBSD: /dev/ada0p1 -> /dev/ada0, /dev/da0p1 -> /dev/da0
            
        Args:
            device_path: Full device path
            
        Returns:
            Base disk path without partition number
        """
        # Handle NVMe devices (common to both platforms)
        if 'nvme' in device_path:
            # Remove partition part (p1, p2, etc.)
            match = re.match(r'(.*nvme\d+n\d+)p?\d*$', device_path)
            if match:
                return match.group(1)
        
        if is_freebsd():
            # FreeBSD: Handle FreeBSD-specific device patterns
            # FreeBSD uses GPT partitioning with 'p' prefix: ada0p1, da0p1, vtbd0p1
            # Also handle MBR-style (legacy): ada0s1, ada0s1a
            if any(x in device_path for x in ['ada', 'da', 'vtbd']):
                # Strip GPT partition (p1, p2, etc.)
                match = re.match(r'(.*(?:ada|da|vtbd)\d+)p\d+$', device_path)
                if match:
                    return match.group(1)
                # Strip MBR slice (s1, s2, etc.) and potential sub-partition (a, b, etc.)
                match = re.match(r'(.*(?:ada|da|vtbd)\d+)s\d+[a-z]?$', device_path)
                if match:
                    return match.group(1)
        else:
            # Linux (default): Handle standard Linux devices
            # Remove trailing numbers (sda1 -> sda, hda1 -> hda)
            match = re.match(r'(.*/(?:sd|hd|vd)[a-z]+)\d*$', device_path)
            if match:
                return match.group(1)
        
        # If no pattern matched, return as is
        return device_path
    
    def _check_zfs_label(self, device_path: str) -> Tuple[bool, Optional[str]]:
        """
        Check if a device has a ZFS label using zdb -l
        Checks both the base device and common partition patterns
        Requires sudo permissions - see memory-bank/POOL_CREATION_SUDO_REQUIREMENTS.md
        
        Args:
            device_path: Full path to device (e.g., /dev/sda)
            
        Returns:
            Tuple of (has_label: bool, pool_name: str or None)
        """
        # Try multiple possible device paths since ZFS labels are often on partitions
        paths_to_check = [device_path]
        
        if is_freebsd():
            # FreeBSD: Add FreeBSD-specific partition patterns and label paths
            # Extract disk name from device path
            disk_name = device_path.replace('/dev/', '')
            
            # Check common GPT partition numbers
            if 'nvme' in device_path:
                # NVMe: nvme0n1 -> nvme0n1p1, nvme0n1p9
                paths_to_check.extend([f'{device_path}p1', f'{device_path}p9'])
            elif any(x in device_path for x in ['ada', 'da', 'vtbd']):
                # FreeBSD SATA/SCSI: ada0 -> ada0p1, ada0p9
                paths_to_check.extend([f'{device_path}p1', f'{device_path}p9'])
            
            # Try to get all partitions for this disk using gpart
            try:
                result = subprocess.run(
                    ['gpart', 'show', '-p', disk_name],
                    capture_output=True,
                    text=True,
                    check=False
                )
                
                if result.returncode == 0:
                    # Parse gpart output to find all partitions
                    for line in result.stdout.split('\n'):
                        parts = line.split()
                        if len(parts) >= 3 and parts[0].isdigit():
                            # Found a partition line, the 3rd column is the partition device
                            partition_name = parts[2]
                            if partition_name.startswith(disk_name):
                                paths_to_check.append(f'/dev/{partition_name}')
            except:
                pass
            
            # Also check GPT ID and disk ID directories
            for id_dir in ['/dev/gptid', '/dev/diskid']:
                if os.path.exists(id_dir):
                    try:
                        for label in os.listdir(id_dir):
                            label_path = os.path.join(id_dir, label)
                            if os.path.exists(label_path):
                                # Check if this label points to our disk
                                real_path = os.path.realpath(label_path)
                                if real_path.startswith(device_path):
                                    paths_to_check.append(label_path)
                    except:
                        pass
        else:
            # Linux (default): Add Linux-specific partition patterns
            if 'nvme' in device_path:
                # NVMe: nvme0n1 -> nvme0n1p1, nvme0n1p9
                paths_to_check.append(f'{device_path}p1')
                paths_to_check.append(f'{device_path}p9')
            elif any(x in device_path for x in ['sd', 'hd', 'vd']):
                # SATA/IDE: sda -> sda1, sda9
                paths_to_check.append(f'{device_path}1')
                paths_to_check.append(f'{device_path}9')
        
        for check_path in paths_to_check:
            try:
                result = subprocess.run(
                    ['sudo', 'zdb', '-l', check_path],
                    capture_output=True,
                    text=True,
                    check=False  # Don't raise on non-zero exit
                )
                
                # zdb -l returns non-zero if no label, but may still have output
                output = result.stdout + result.stderr
                
                # Look for pool name in output
                pool_name = None
                for line in output.split('\n'):
                    if 'name:' in line.lower():
                        parts = line.split(':', 1)
                        if len(parts) == 2:
                            pool_name = parts[1].strip().strip("'\"")
                            break
                
                # If we found a pool name or the output contains ZFS-related info, it has a label
                has_label = pool_name is not None or 'version:' in output.lower() or 'guid:' in output.lower()
                
                if has_label:
                    return True, pool_name
                    
            except (subprocess.CalledProcessError, FileNotFoundError):
                continue
        
        # No label found on any checked path
        return False, None
    
    def _parse_geom_output(self, output: str) -> List[Dict[str, Any]]:
        """
        Parse geom disk list output (FreeBSD)
        
        Args:
            output: Output from 'geom disk list'
            
        Returns:
            List of disk dictionaries
        """
        disks = []
        current_disk = {}
        
        for line in output.split('\n'):
            line = line.strip()
            
            if line.startswith('Geom name:'):
                if current_disk:
                    disks.append(current_disk)
                current_disk = {'name': line.split(':', 1)[1].strip()}
            elif line.startswith('Mediasize:') and current_disk:
                # Extract size from mediasize line
                match = re.search(r'\(([^)]+)\)', line)
                if match:
                    current_disk['size'] = match.group(1)
            elif line.startswith('descr:') and current_disk:
                current_disk['model'] = line.split(':', 1)[1].strip()
            elif line.startswith('ident:') and current_disk:
                # Determine if SSD based on model/ident
                ident = line.split(':', 1)[1].strip().lower()
                current_disk['type'] = 'SSD' if 'ssd' in ident or 'nvme' in ident else 'HDD'
        
        if current_disk:
            disks.append(current_disk)
        
        # Add default values and check usage
        for disk in disks:
            disk.setdefault('device_path', f'/dev/{disk["name"]}')
            disk.setdefault('size', 'Unknown')
            disk.setdefault('model', 'Unknown')
            disk.setdefault('type', 'HDD')
            disk['in_use'] = self._is_disk_in_use(disk['name'])
            disk['exported'] = False
        
        return disks
    
    def get_disk_info(self, device_path: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific disk
        
        Args:
            device_path: Path to the disk device
            
        Returns:
            Dictionary with disk information or None if not found
        """
        disks = self.get_available_disks()
        
        for disk in disks:
            if disk['device_path'] == device_path or disk['name'] == device_path:
                return disk
        
        return None
