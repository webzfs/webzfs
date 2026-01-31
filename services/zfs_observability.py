"""
ZFS Observability Service
Handles retrieval and parsing of ZFS logs, events, and history
"""
import subprocess
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from services.utils import is_freebsd, is_netbsd
from config.settings import Settings


class ZFSObservabilityService:
    """Service for ZFS observability, logs, and events"""
    
    def __init__(self):
        """Initialize the ZFS Observability Service with settings"""
        self.settings = Settings()
        self.timeouts = self.settings.ZPOOL_TIMEOUTS
    
    def get_pool_history(
        self,
        pool_name: Optional[str] = None,
        limit: int = 1000,
        since: Optional[datetime] = None,
        internal: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get ZFS pool command history
        
        Args:
            pool_name: Optional pool name to filter by
            limit: Maximum number of entries to return
            since: Optional datetime to filter entries after
            internal: Include internal events
            
        Returns:
            List of parsed history entries
        """
        timeout = self.timeouts.get('history', self.timeouts['default'])
        try:
            cmd = ['zpool', 'history', '-l']
            if internal:
                cmd.append('-i')
            if pool_name:
                cmd.append(pool_name)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            history = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                # Parse history line
                # Format: 2023-11-16.12:34:56 zfs create tank/dataset [user root on apollo]
                entry = self._parse_history_line(line)
                if entry:
                    # Filter by since date if provided
                    if since and entry.get('timestamp'):
                        try:
                            entry_time = datetime.fromisoformat(entry['timestamp'])
                            if entry_time < since:
                                continue
                        except:
                            pass
                    
                    history.append(entry)
            
            # Return last 'limit' entries
            return history[-limit:]
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool history command timed out after {timeout} seconds. History may be very large.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool history: {e.stderr}")
    
    def get_pool_events(
        self,
        pool_name: Optional[str] = None,
        verbose: bool = False,
        follow: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Get ZFS pool events
        
        Args:
            pool_name: Optional pool name to filter by
            verbose: Include verbose event details
            follow: Follow mode (not implemented for now)
            
        Returns:
            List of parsed event entries
        """
        # NetBSD ZFS does not support the 'events' subcommand
        if is_netbsd():
            return []
        
        timeout = self.timeouts.get('events', self.timeouts['default'])
        try:
            cmd = ['zpool', 'events']
            if verbose:
                cmd.append('-v')
            if pool_name:
                cmd.append(pool_name)
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
            
            events = []
            for line in result.stdout.strip().split('\n'):
                if not line or line.startswith('TIME'):
                    continue
                
                event = self._parse_event_line(line, verbose)
                if event:
                    events.append(event)
            
            return events
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool events command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get pool events: {e.stderr}")
    
    def clear_pool_events(self, pool_name: Optional[str] = None) -> None:
        """
        Clear ZFS pool events
        
        Args:
            pool_name: Optional pool name, clears all if not provided
        """
        # NetBSD ZFS does not support the 'events' subcommand
        if is_netbsd():
            return
        
        timeout = self.timeouts.get('events', self.timeouts['default'])
        try:
            cmd = ['zpool', 'events', '-c']
            if pool_name:
                cmd.append(pool_name)
            
            subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=timeout
            )
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool clear events command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to clear pool events: {e.stderr}")
    
    def get_kernel_debug_log(
        self,
        lines: int = 1000,
        filter_pattern: Optional[str] = None
    ) -> List[str]:
        """
        Get ZFS kernel debug messages
        
        Args:
            lines: Maximum number of lines to return
            filter_pattern: Optional regex pattern to filter messages
            
        Returns:
            List of log lines
        """
        try:
            # Try to read from /proc/spl/kstat/zfs/dbgmsg
            dbgmsg_path = Path('/proc/spl/kstat/zfs/dbgmsg')
            
            if not dbgmsg_path.exists():
                return ["Debug log not available (missing /proc/spl/kstat/zfs/dbgmsg)"]
            
            with open(dbgmsg_path, 'r') as f:
                log_lines = f.readlines()
            
            # Filter if pattern provided
            if filter_pattern:
                pattern = re.compile(filter_pattern, re.IGNORECASE)
                log_lines = [line for line in log_lines if pattern.search(line)]
            
            # Return last N lines
            return [line.strip() for line in log_lines[-lines:]]
            
        except Exception as e:
            return [f"Error reading debug log: {str(e)}"]
    
    def get_syslog_zfs(
        self,
        lines: int = 1000,
        since: Optional[datetime] = None,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get ZFS-related syslog entries
        
        Args:
            lines: Maximum number of entries to return
            since: Optional datetime to filter entries after
            severity: Optional severity filter (error, warning, info)
            
        Returns:
            List of syslog entries
        """
        if is_freebsd() or is_netbsd():
            # BSD uses syslog - grep /var/log/messages
            return self._read_bsd_syslog(lines, since, severity)
        
        try:
            # Linux uses journalctl on systemd systems (default)
            cmd = ['journalctl', '-n', str(lines), '--no-pager']
            
            # Add grep for ZFS-related messages
            cmd.extend(['-t', 'kernel'])
            
            if since:
                cmd.extend(['--since', since.isoformat()])
            
            if severity:
                priority_map = {
                    'error': 'err',
                    'warning': 'warning',
                    'info': 'info'
                }
                if severity.lower() in priority_map:
                    cmd.extend(['-p', priority_map[severity.lower()]])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Filter for ZFS messages
            zfs_lines = []
            for line in result.stdout.split('\n'):
                if 'zfs' in line.lower() or 'zpool' in line.lower():
                    zfs_lines.append({'message': line.strip()})
            
            return zfs_lines
            
        except subprocess.CalledProcessError:
            # Fallback to reading /var/log/messages or dmesg
            return self._fallback_syslog_read(lines)
    
    def get_arc_summary(self) -> Dict[str, Any]:
        """
        Get ARC (Adaptive Replacement Cache) statistics
        
        Returns:
            Dictionary with ARC stats
        """
        # NetBSD/FreeBSD use sysctl for ARC stats
        if is_netbsd() or is_freebsd():
            return self._get_arc_summary_sysctl()
        
        # Linux uses /proc/spl/kstat/zfs/arcstats
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
            
            # Calculate derived stats
            if 'hits' in stats and 'misses' in stats:
                total = stats['hits'] + stats['misses']
                if total > 0:
                    stats['hit_rate'] = (stats['hits'] / total) * 100
            
            # Format sizes
            if 'size' in stats:
                stats['size_human'] = self._format_bytes(stats['size'])
            if 'c_max' in stats:
                stats['c_max_human'] = self._format_bytes(stats['c_max'])
            
            return stats
            
        except Exception as e:
            return {'error': f'Failed to read ARC stats: {str(e)}'}
    
    def _get_arc_summary_sysctl(self) -> Dict[str, Any]:
        """
        Get ARC stats using sysctl (for BSD systems)
        
        Returns:
            Dictionary with ARC stats
        """
        try:
            # Get all ZFS ARC-related sysctl values
            result = subprocess.run(
                ['sysctl', '-a'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                return {'error': 'Failed to read sysctl'}
            
            stats = {}
            
            # Look for kstat.zfs.misc.arcstats.* entries
            for line in result.stdout.split('\n'):
                if 'arcstats' in line.lower() or 'arc_' in line.lower():
                    # Parse sysctl output: name=value or name: value
                    if '=' in line:
                        name, value = line.split('=', 1)
                    elif ':' in line:
                        name, value = line.split(':', 1)
                    else:
                        continue
                    
                    name = name.strip()
                    value = value.strip()
                    
                    # Extract just the stat name (last part after dots)
                    stat_name = name.split('.')[-1]
                    
                    try:
                        stats[stat_name] = int(value)
                    except ValueError:
                        stats[stat_name] = value
            
            if not stats:
                return {'error': 'ARC stats not available via sysctl'}
            
            # Calculate derived stats
            if 'hits' in stats and 'misses' in stats:
                total = stats['hits'] + stats['misses']
                if total > 0:
                    stats['hit_rate'] = (stats['hits'] / total) * 100
            
            # Format sizes
            if 'size' in stats:
                stats['size_human'] = self._format_bytes(stats['size'])
            if 'c_max' in stats:
                stats['c_max_human'] = self._format_bytes(stats['c_max'])
            
            return stats
            
        except Exception as e:
            return {'error': f'Failed to read ARC stats: {str(e)}'}
    
    def get_zfs_module_parameters(self) -> Dict[str, Any]:
        """
        Get current ZFS kernel module parameters
        
        Returns:
            Dictionary of parameter names and values
        """
        try:
            params_path = Path('/sys/module/zfs/parameters')
            
            if not params_path.exists():
                return {'error': 'ZFS module parameters not available'}
            
            parameters = {}
            for param_file in params_path.iterdir():
                if param_file.is_file():
                    try:
                        with open(param_file, 'r') as f:
                            value = f.read().strip()
                            parameters[param_file.name] = value
                    except:
                        parameters[param_file.name] = '<unreadable>'
            
            return parameters
            
        except Exception as e:
            return {'error': f'Failed to read module parameters: {str(e)}'}
    
    def search_logs(
        self,
        query: str,
        source: str = "all",
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Search across all ZFS logs with a query string
        
        Args:
            query: Search query
            source: Log source (all, pool_history, events, kernel, syslog)
            limit: Maximum number of results
            
        Returns:
            List of matching log entries
        """
        results = []
        query_lower = query.lower()
        
        try:
            if source in ['all', 'pool_history']:
                history = self.get_pool_history(limit=limit)
                for entry in history:
                    if query_lower in str(entry).lower():
                        entry['source'] = 'pool_history'
                        results.append(entry)
            
            if source in ['all', 'events']:
                events = self.get_pool_events()
                for entry in events:
                    if query_lower in str(entry).lower():
                        entry['source'] = 'events'
                        results.append(entry)
            
            if source in ['all', 'kernel']:
                kernel_log = self.get_kernel_debug_log(lines=limit)
                for line in kernel_log:
                    if query_lower in line.lower():
                        results.append({
                            'source': 'kernel',
                            'message': line
                        })
            
            if source in ['all', 'syslog']:
                syslog = self.get_syslog_zfs(lines=limit)
                for entry in syslog:
                    if query_lower in str(entry).lower():
                        entry['source'] = 'syslog'
                        results.append(entry)
            
            return results[:limit]
            
        except Exception as e:
            return [{'error': f'Search failed: {str(e)}'}]
    
    # Private helper methods
    
    def _parse_history_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse a zpool history line"""
        try:
            # Skip header lines
            if line.startswith('History for'):
                return None
                
            # Format: 2023-11-16.12:34:56 full command here [user=root on hostname]
            # Split on first space to get timestamp
            parts = line.split(None, 1)
            if len(parts) < 2:
                return None
            
            timestamp_str = parts[0]
            rest = parts[1]
            
            # Extract user info if present (in brackets at end)
            command_line = rest
            user = None
            host = None
            
            if '[' in rest and ']' in rest:
                bracket_start = rest.rfind('[')
                command_line = rest[:bracket_start].strip()
                user_info = rest[bracket_start+1:rest.rfind(']')]
                
                # Parse user info like "user=root on hostname" or "user 0 on apollo:linux"
                if 'user' in user_info:
                    user_parts = user_info.split()
                    for i, part in enumerate(user_parts):
                        if part == 'user' or part.startswith('user='):
                            if part.startswith('user='):
                                user = part.split('=')[1]
                            elif i + 1 < len(user_parts):
                                user = user_parts[i + 1]
                        if part == 'on' and i + 1 < len(user_parts):
                            host = user_parts[i + 1]
            
            return {
                'timestamp': timestamp_str.replace('.', ' '),
                'command': command_line,
                'user': user,
                'host': host,
                'raw': line
            }
        except Exception as e:
            # Return the raw line if parsing fails
            return {'raw': line, 'command': line, 'timestamp': '', 'user': None, 'host': None}
    
    def _parse_event_line(self, line: str, verbose: bool) -> Optional[Dict[str, Any]]:
        """Parse a zpool events line"""
        try:
            parts = line.split(None, 3)
            if len(parts) < 3:
                return None
            
            return {
                'time': parts[0],
                'class': parts[1],
                'pool': parts[2] if len(parts) > 2 else None,
                'details': parts[3] if len(parts) > 3 else '',
                'raw': line
            }
        except:
            return {'raw': line}
    
    def _read_bsd_syslog(
        self,
        lines: int,
        since: Optional[datetime] = None,
        severity: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """BSD-specific syslog reading from /var/log/messages"""
        try:
            # Read from /var/log/messages and filter for ZFS
            result = subprocess.run(
                ['sh', '-c', f'grep -i "zfs\\|zpool" /var/log/messages | tail -n {lines}'],
                capture_output=True,
                text=True,
                check=False
            )
            
            zfs_lines = []
            for line in result.stdout.split('\n'):
                if line.strip():
                    zfs_lines.append({'message': line.strip()})
            
            return zfs_lines if zfs_lines else [{'message': 'No ZFS messages found'}]
            
        except Exception as e:
            return [{'message': f'Error reading syslog: {str(e)}'}]
    
    def _fallback_syslog_read(self, lines: int) -> List[Dict[str, Any]]:
        """Fallback method to read syslog without journalctl"""
        try:
            # Try dmesg (without -T flag on BSD systems)
            if is_freebsd() or is_netbsd():
                # BSD dmesg doesn't support -T flag
                result = subprocess.run(
                    ['dmesg'],
                    capture_output=True,
                    text=True,
                    check=False
                )
            else:
                # Linux dmesg with timestamps (default)
                result = subprocess.run(
                    ['dmesg', '-T'],
                    capture_output=True,
                    text=True,
                    check=False
                )
            
            if result.returncode == 0:
                zfs_lines = []
                for line in result.stdout.split('\n')[-lines:]:
                    if 'zfs' in line.lower() or 'zpool' in line.lower():
                        zfs_lines.append({'message': line.strip()})
                return zfs_lines
            
            return [{'message': 'Syslog not available'}]
            
        except:
            return [{'message': 'Error reading syslog'}]
    
    def _format_bytes(self, bytes_val: int) -> str:
        """Format bytes to human-readable string"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
