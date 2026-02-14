"""
ZFS Performance Monitoring Service
Handles performance metrics, I/O statistics, and process monitoring
"""
import subprocess
import psutil
import platform
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from config.settings import Settings


class ZFSPerformanceService:
    """Service for ZFS performance monitoring and statistics"""
    
    def __init__(self):
        self.system = platform.system()
        self.settings = Settings()
        self.timeouts = self.settings.ZPOOL_TIMEOUTS
    
    def get_zpool_iostat(
        self,
        pool_name: Optional[str] = None,
        interval: int = 1,
        count: int = 1,
        verbose: bool = False,
        latency: bool = False,
        queue: bool = False,
        request_size: bool = False
    ) -> Dict[str, Any]:
        """
        Get ZFS pool I/O statistics
        
        Args:
            pool_name: Optional pool name to filter
            interval: Interval between samples in seconds
            count: Number of samples (always gets 2 to skip boot stats)
            verbose: Include per-vdev statistics
            latency: Show latency statistics
            queue: Show queue depth statistics
            request_size: Show request size distribution
            
        Returns:
            Dictionary with I/O statistics
        """
        timeout = self.timeouts.get('iostat', self.timeouts['default'])
        try:
            # Don't use -H flag as it strips indentation needed for hierarchy display
            cmd = ['zpool', 'iostat', '-y']  # -y omits first sample
            
            if verbose:
                cmd.append('-v')
            if latency:
                cmd.append('-l')
            if queue:
                cmd.append('-q')
            if request_size:
                cmd.append('-r')
            
            if pool_name:
                cmd.append(pool_name)
            
            # Always get 2 samples, -y will make first sample be 0s, second is live
            cmd.extend([str(interval), '1'])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            # Parse output
            stats = self._parse_iostat_output(result.stdout, verbose, latency, queue, request_size)
            
            return {
                'timestamp': datetime.now().isoformat(),
                'pool': pool_name,
                'statistics': stats,
                'raw_output': result.stdout
            }
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool iostat command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool iostat: {e.stderr}")
    
    def get_system_iostat(
        self,
        extended: bool = True,
        interval: int = 1,
        count: int = 1
    ) -> Dict[str, Any]:
        """
        Get system I/O statistics (Linux/FreeBSD)
        
        Args:
            extended: Use extended statistics
            interval: Interval between samples
            count: Number of samples (always gets 2 to skip boot stats)
            
        Returns:
            Dictionary with system I/O stats
        """
        try:
            cmd = ['iostat']
            
            # Platform-specific flags
            if self.system == 'Linux':
                if extended:
                    cmd.append('-x')
                # Always request 2 samples: first is since boot (garbage), second is live
                cmd.extend([str(interval), '2'])
            elif self.system == 'FreeBSD':
                if extended:
                    cmd.append('-x')
                # FreeBSD iostat syntax: -w interval -c count
                cmd.extend(['-w', str(interval), '-c', '2'])
            else:
                return {'error': f'iostat not supported on {self.system}'}
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output to get only the second sample (skip first/boot sample)
            output_lines = result.stdout.split('\n')
            
            if self.system == 'Linux':
                # Find the second occurrence of the Device header (marks start of 2nd sample)
                device_occurrences = []
                for i, line in enumerate(output_lines):
                    if 'Device' in line:
                        device_occurrences.append(i)
                
                # If we found at least 2 Device headers, use everything from the second one onward
                if len(device_occurrences) >= 2:
                    second_header_idx = device_occurrences[1]
                    # Find the last occurrence of avg-cpu before the second Device header
                    # to get the complete second sample with CPU stats
                    cpu_header_idx = second_header_idx
                    for i in range(second_header_idx - 1, -1, -1):
                        if 'avg-cpu' in output_lines[i]:
                            cpu_header_idx = i
                            break
                    
                    # Get the first few lines (timestamp, Linux version info)
                    header_lines = []
                    for i, line in enumerate(output_lines):
                        if 'avg-cpu' in line or 'Device' in line:
                            break
                        if line.strip():
                            header_lines.append(line)
                    
                    # Combine header + second sample
                    filtered_output = '\n'.join(header_lines + [''] + output_lines[cpu_header_idx:])
                else:
                    # Fallback to full output if parsing fails
                    filtered_output = result.stdout
            else:
                # FreeBSD: iostat output format is different, just use it as-is
                # The -c 2 flag already handles skipping boot stats
                filtered_output = result.stdout
            
            return {
                'timestamp': datetime.now().isoformat(),
                'output': filtered_output,
                'system': self.system
            }
            
        except FileNotFoundError:
            if self.system == 'Linux':
                return {'error': 'iostat command not found. Please Install it.)'}
            elif self.system == 'FreeBSD':
                return {'error': 'iostat command not found. Please Install it.'}
            else:
                return {'error': f'iostat command not found on {self.system}'}
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get system iostat: {e.stderr}")
    
    def get_gstat(
        self,
        interval: int = 1,
        count: int = 1
    ) -> Dict[str, Any]:
        """
        Get I/O statistics (FreeBSD)
        
        Args:
            interval: Interval between samples
            count: Number of samples
            
        Returns:
            Dictionary with gstat output
        """
        if self.system != 'FreeBSD':
            return {'error': 'gstat only available on FreeBSD'}
        
        try:
            cmd = ['gstat', '-b', '-I', str(interval) + 's', '-c', str(count)]
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            return {
                'timestamp': datetime.now().isoformat(),
                'output': result.stdout
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get gstat: {e.stderr}")
    
    def get_zfs_processes(self, min_cpu_percent: float = 0.0, sort_by_cpu: bool = False) -> List[Dict[str, Any]]:
        """
        Get all ZFS-related processes with resource usage
        
        Args:
            min_cpu_percent: Minimum CPU percentage to filter (default: 0.0 = all)
            sort_by_cpu: Sort by CPU percentage descending (default: False)
        
        Returns:
            List of ZFS processes with stats
        """
        # ZFS processes to monitor - includes both exact matches and prefixes
        # Prefixes will match any process starting with that name
        zfs_process_patterns = [
            'arc_evict', 'arc_flush', 'arc_prune', 'arc_reap',
            'dbu_evict', 'dbuf_evict', 'dmu_objset_find',
            'dp_sync_taskq', 'dp_zil_clean_taskq',
            'dsl_scan_iss',
            'mmp',
            'raidz_expand',
            'receive_writer',
            'redact_list', 'redact_merge', 'redact_traverse',
            'send_merge', 'send_reader', 'send_traverse',
            'spa_async', 'spa_vdev_remove',
            'spl_delay_taskq', 'spl_dynamic_taskq', 'spl_kmem_cache', 'spl_system_taskq',
            'tx_commit_cb',
            'txg_quiesce', 'txg_sync',
            'vdev_autotrim', 'vdev_initialize', 'vdev_load', 'vdev_open', 'vdev_rebuild', 'vdev_trim', 'vdev_validate',
            'z_checkpoint_discard',
            'z_cl',  # Matches z_cl_int, z_cl_iss, etc.
            'z_flush',  # Matches z_flush_int, z_flush_iss, etc.
            'z_fr',  # Matches z_fr_int_0, z_fr_iss_1, etc.
            'z_indirect_condense',
            'z_livelist_condense', 'z_livelist_destroy',
            'z_metaslab',
            'z_null',
            'z_prefetch',
            'z_rd',  # Matches z_rd_int_0, z_rd_iss_1, etc.
            'z_send',
            'z_trim',
            'z_unlinked_drain',
            'z_upgrade',
            'z_vdev_file',
            'z_wr',  # Matches z_wr_int_0, z_wr_iss_1, etc.
            'z_zrele',
            'z_zvol',
            'zpool',
            'zfs',
            'zed'
        ]
        
        zfs_processes = []
        
        try:
            for proc in psutil.process_iter(['pid', 'name', 'username', 'cpu_percent', 'memory_percent', 'status']):
                try:
                    name = proc.info['name']
                    
                    # Check if process matches any of our ZFS process patterns
                    is_zfs_process = False
                    
                    # Check if process name starts with any of our patterns
                    for pattern in zfs_process_patterns:
                        if name.startswith(pattern):
                            is_zfs_process = True
                            break
                    
                    if is_zfs_process:
                        cpu_percent = proc.info['cpu_percent'] or 0.0
                        
                        # Apply CPU filter
                        if cpu_percent >= min_cpu_percent:
                            zfs_processes.append({
                                'pid': proc.info['pid'],
                                'name': name,
                                'username': proc.info['username'],
                                'cpu_percent': cpu_percent,
                                'memory_percent': proc.info['memory_percent'] or 0.0,
                                'status': proc.info['status']
                            })
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            
            # Sort by CPU percentage if requested
            if sort_by_cpu:
                zfs_processes.sort(key=lambda x: x['cpu_percent'], reverse=True)
            
            return zfs_processes
            
        except Exception as e:
            raise Exception(f"Failed to get ZFS processes: {str(e)}")
    
    def get_pool_capacity_stats(
        self,
        pool_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed capacity statistics for pools
        
        Args:
            pool_name: Optional pool name to filter
            
        Returns:
            Dictionary with capacity stats including fragmentation, dedup, compression
        """
        timeout = self.timeouts.get('list', self.timeouts['default'])
        try:
            cmd = ['zpool', 'list', '-H', '-o', 
                   'name,size,alloc,free,frag,cap,dedup,health']
            
            if pool_name:
                cmd.append(pool_name)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            pools = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) >= 8:
                    pools.append({
                        'name': parts[0],
                        'size': parts[1],
                        'allocated': parts[2],
                        'free': parts[3],
                        'fragmentation': parts[4],
                        'capacity': parts[5],
                        'deduplication': parts[6],
                        'health': parts[7]
                    })
            
            return {'pools': pools}
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool list command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get capacity stats: {e.stderr}")
    
    def get_dataset_space_usage(
        self,
        dataset_name: Optional[str] = None,
        recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get space usage breakdown for datasets
        
        Args:
            dataset_name: Optional dataset name
            recursive: Include child datasets
            
        Returns:
            List of datasets with space usage
        """
        try:
            cmd = ['zfs', 'list', '-H', '-o',
                   'name,used,avail,refer,compressratio,mounted,mountpoint']
            
            if recursive:
                cmd.append('-r')
            
            if dataset_name:
                cmd.append(dataset_name)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            datasets = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split('\t')
                if len(parts) >= 7:
                    datasets.append({
                        'name': parts[0],
                        'used': parts[1],
                        'available': parts[2],
                        'referenced': parts[3],
                        'compression_ratio': parts[4],
                        'mounted': parts[5],
                        'mountpoint': parts[6]
                    })
            
            return datasets
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get dataset space usage: {e.stderr}")
    
    def get_arc_stats_realtime(
        self,
        interval: int = 1,
        count: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get real-time ARC statistics over time
        
        Args:
            interval: Seconds between samples
            count: Number of samples to collect
            
        Returns:
            List of ARC stats snapshots
        """
        import time
        
        stats_series = []
        
        try:
            for _ in range(count):
                stats = self._read_arc_stats()
                stats['timestamp'] = datetime.now().isoformat()
                stats_series.append(stats)
                
                if _ < count - 1:  # Don't sleep after last sample
                    time.sleep(interval)
            
            return stats_series
            
        except Exception as e:
            raise Exception(f"Failed to get realtime ARC stats: {str(e)}")
    
    def get_vdev_stats(
        self,
        pool_name: str
    ) -> List[Dict[str, Any]]:
        """
        Get per-vdev statistics
        
        Args:
            pool_name: Pool name
            
        Returns:
            List of vdev statistics
        """
        timeout = self.timeouts.get('iostat', self.timeouts['default'])
        try:
            # Use -y flag to omit first sample (since boot stats)
            result = subprocess.run(
                ['zpool', 'iostat', '-yv', '-H', pool_name, '1', '1'],
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            # Parse vdev statistics from output
            vdevs = []
            lines = result.stdout.strip().split('\n')
            
            for line in lines:
                if not line.strip():
                    continue
                
                parts = line.split()
                if len(parts) < 7:
                    continue
                
                # First line is the pool itself, skip it
                # Vdev lines are indented (start with whitespace in original, but parts[0] is the name)
                # Check if this looks like a device name (not the pool name)
                device_name = parts[0]
                
                # Skip if this is the pool name line
                if device_name == pool_name:
                    continue
                
                # This should be a vdev
                vdevs.append({
                    'name': device_name,
                    'alloc': parts[1],
                    'free': parts[2],
                    'read_ops': parts[3],
                    'write_ops': parts[4],
                    'read_bw': parts[5],
                    'write_bw': parts[6]
                })
            
            return vdevs
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool iostat vdev stats command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get vdev stats: {e.stderr}")
    
    def estimate_scrub_time(
        self,
        pool_name: str
    ) -> Dict[str, Any]:
        """
        Estimate remaining scrub/resilver time based on current progress
        
        Args:
            pool_name: Pool name
            
        Returns:
            Dictionary with time estimates
        """
        timeout = self.timeouts.get('status', self.timeouts['default'])
        try:
            result = subprocess.run(
                ['zpool', 'status', pool_name],
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            # Parse status output for scrub info
            status = result.stdout
            
            # Look for scan lines
            for line in status.split('\n'):
                line = line.strip()
                if 'scan:' in line.lower():
                    # Parse scrub/resilver information
                    if 'in progress' in line.lower():
                        # Extract progress percentage and estimate
                        return {
                            'status': 'in_progress',
                            'info': line
                        }
                    elif 'completed' in line.lower():
                        return {
                            'status': 'completed',
                            'info': line
                        }
            
            return {
                'status': 'none',
                'info': 'No scrub in progress'
            }
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool status command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to estimate scrub time: {e.stderr}")
    
    # Private helper methods
    
    def _parse_iostat_output(
        self,
        output: str,
        verbose: bool,
        latency: bool,
        queue: bool,
        request_size: bool
    ) -> List[Dict[str, Any]]:
        """Parse zpool iostat output with hierarchy detection"""
        stats = []
        lines = output.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
            
            stripped = line.lstrip()
            
            # Skip header lines (contain "capacity", "operations", "bandwidth", "pool", etc.)
            if any(keyword in stripped.lower() for keyword in ['capacity', 'operations', 'bandwidth', 'pool', 'latency', 'queue']):
                continue
            
            # Skip separator lines (contain dashes)
            if stripped.startswith('-'):
                continue
            
            # Detect indentation level by counting leading spaces
            # ZFS iostat uses 2 spaces per level of indentation
            indent_level = 0
            leading_spaces = len(line) - len(stripped)
            
            # Each indentation level is typically 2 spaces
            if leading_spaces > 0:
                indent_level = leading_spaces // 2
            
            parts = stripped.split()
            if len(parts) >= 7:
                stat = {
                    'device': parts[0],
                    'alloc': parts[1],
                    'free': parts[2],
                    'read_ops': parts[3],
                    'write_ops': parts[4],
                    'read_bw': parts[5],
                    'write_bw': parts[6],
                    'indent_level': indent_level
                }
                
                # Add additional fields based on flags
                idx = 7
                if latency and len(parts) > idx + 1:
                    stat['read_latency'] = parts[idx]
                    stat['write_latency'] = parts[idx + 1]
                    idx += 2
                
                if queue and len(parts) > idx + 1:
                    stat['sync_queue'] = parts[idx]
                    stat['async_queue'] = parts[idx + 1]
                    idx += 2
                
                stats.append(stat)
        
        return stats
    
    def _read_arc_stats(self) -> Dict[str, Any]:
        """Read current ARC statistics"""

        # FreeBSD/NetBSD use sysctl for ARC stats
        if self.system in ('FreeBSD', 'NetBSD'):
            return self._read_arc_stats_sysctl()
        else:
            return self._read_arc_stats_linux()
    
    def _read_arc_stats_linux(self) -> Dict[str, Any]:
        """Read ARC stats from Linux /proc filesystem"""
        try:
            arcstats_path = Path('/proc/spl/kstat/zfs/arcstats')
            
            if not arcstats_path.exists():
                return {'error': 'ARC stats not available'}
            
            stats = {}
            with open(arcstats_path, 'r') as f:
                for line in f:
                    if line.startswith('#') or not line.strip():
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 3:
                        name = parts[0]
                        value = parts[2]
                        try:
                            stats[name] = int(value)
                        except ValueError:
                            stats[name] = value
            
            # Calculate derived metrics
            if 'hits' in stats and 'misses' in stats:
                total = stats['hits'] + stats['misses']
                if total > 0:
                    stats['hit_rate'] = (stats['hits'] / total) * 100
                    stats['miss_rate'] = (stats['misses'] / total) * 100
            
            return stats
            
        except Exception as e:
            return {'error': f'Failed to read ARC stats: {str(e)}'}
    

    def _read_arc_stats_sysctl(self) -> Dict[str, Any]:
        """Read ARC statistics using sysctl (for BSD systems)"""

        try:
            result = subprocess.run(
                ['sysctl', 'kstat.zfs.misc.arcstats'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:

                return {'error': 'Failed to read sysctl for ARC stats'}
            
            stats = {}
            
            for line in result.stdout.split('\n'):
                if not line.strip():
                    continue
                
                # Parse sysctl output: name: value (FreeBSD) or name=value (NetBSD)
                if ': ' in line:
                    name, value = line.split(': ', 1)
                elif '=' in line:
                    name, value = line.split('=', 1)
                else:
                    continue
                
                name = name.strip()
                value = value.strip()
                
                # Extract just the stat name (last part after dots)
                # kstat.zfs.misc.arcstats.hits -> hits
                stat_name = name.split('.')[-1]

                try:
                    stats[stat_name] = int(value)
                except ValueError:
                    stats[stat_name] = value
            
            if not stats:
                return {'error': 'ARC stats not available via sysctl'}

            # Calculate derived metrics
            if 'hits' in stats and 'misses' in stats:
                total = stats['hits'] + stats['misses']
                if total > 0:
                    stats['hit_rate'] = (stats['hits'] / total) * 100
                    stats['miss_rate'] = (stats['misses'] / total) * 100
            
            return stats
            
        except Exception as e:
            return {'error': f'Failed to read ARC stats: {str(e)}'}
    
    def get_raw_arcstats(self) -> Dict[str, Any]:
        """
        Get raw ARC statistics output
        
        On Linux: cat /proc/spl/kstat/zfs/arcstats
        On FreeBSD: zfs-stats -A
        
        Returns:
            Dictionary with raw output and system info
        """
        try:
            if self.system == 'Linux':
                arcstats_path = Path('/proc/spl/kstat/zfs/arcstats')
                
                if not arcstats_path.exists():
                    return {
                        'error': 'ARC stats not available - /proc/spl/kstat/zfs/arcstats not found',
                        'system': self.system
                    }
                
                with open(arcstats_path, 'r') as f:
                    output = f.read()
                
                return {
                    'output': output,
                    'system': self.system,
                    'command': 'cat /proc/spl/kstat/zfs/arcstats',
                    'timestamp': datetime.now().isoformat()
                }
                
            elif self.system == 'FreeBSD':
                # On FreeBSD, webzfs runs as root, so no sudo needed
                # Use full path to ensure it's found regardless of PATH environment
                try:
                    result = subprocess.run(
                        ['/usr/local/bin/zfs-stats', '-A'],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    if result.returncode != 0:
                        return {
                            'error': f'zfs-stats failed: {result.stderr}',
                            'system': self.system
                        }
                    
                    return {
                        'output': result.stdout,
                        'system': self.system,
                        'command': 'zfs-stats -A',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                except FileNotFoundError:
                    return {
                        'error': 'zfs-stats command not found. Install with: pkg install zfs-stats',
                        'system': self.system
                    }
                except subprocess.TimeoutExpired:
                    return {
                        'error': 'zfs-stats command timed out',
                        'system': self.system
                    }
            else:
                return {
                    'error': f'Raw ARC stats not available on {self.system}',
                    'system': self.system
                }
                
        except Exception as e:
            return {
                'error': f'Failed to get raw ARC stats: {str(e)}',
                'system': self.system
            }
