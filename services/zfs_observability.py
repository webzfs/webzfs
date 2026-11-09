"""
ZFS Observability Service
Handles retrieval and parsing of ZFS logs, events, and history
"""
import subprocess
import re
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from services.utils import is_freebsd, is_netbsd, run_zfs_command
from services.zfs_capabilities import get_capabilities
from services.zfs_pool import ZFSPoolService
from config.settings import Settings


# ---------------------------------------------------------------------------
# Kstat parsers
# ---------------------------------------------------------------------------
# ZFS publishes kernel statistics through kstat. On Linux every kstat is a
# file under /proc/spl/kstat/zfs/; on FreeBSD the same kstats are exposed
# through the sysctl tree as ``kstat.zfs.<class>.<name>``. The parsers below
# are pure functions so they can be unit-tested against captured output; the
# service methods only take care of reading the source.

#: Header line printed by the Linux kstat procfs layer, e.g.
#: ``26 1 0x01 21 5712 9156795328 362210075545631``.
_KSTAT_HEADER_RE = re.compile(r"^\d+\s+\d+\s+0x[0-9a-fA-F]+\s+\d+\s+\d+\s+\d+\s+\d+$")

#: ``txg_state_t`` -> human readable name (include/sys/spa.h).
TXG_STATE_NAMES = {
    "B": "birth",
    "O": "open",
    "Q": "quiesced",
    "W": "wait_for_sync",
    "S": "synced",
    "C": "committed",
}


def _coerce_number(value: str) -> Any:
    """Return ``value`` as an int when possible, otherwise as a stripped string."""
    text = value.strip()
    try:
        return int(text, 10)
    except (TypeError, ValueError):
        return text


def parse_kstat_procfs(text: str) -> Dict[str, Any]:
    """Parse a Linux ``/proc/spl/kstat/zfs/<name>`` file.

    The file starts with a numeric kstat header line followed by a
    ``name type data`` column header and one row per statistic::

        26 1 0x01 21 5712 9156795328 362210075545631
        name                            type data
        zil_commit_count                4    935916

    Returns a ``{statistic: value}`` mapping. Numeric values are converted to
    ``int``; string-typed kstat entries are kept as text.
    """
    stats: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or _KSTAT_HEADER_RE.match(line):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        name, kstat_type, value = parts[0], parts[1], " ".join(parts[2:])
        if name == "name" and kstat_type == "type" and value == "data":
            continue
        if not kstat_type.isdigit():
            continue
        stats[name] = _coerce_number(value)
    return stats


def parse_kstat_sysctl(text: str, prefix: str = "") -> Dict[str, Any]:
    """Parse FreeBSD/NetBSD ``sysctl kstat.zfs...`` output.

    Output lines look like ``kstat.zfs.misc.zil.zil_commit_count: 935916``
    (FreeBSD) or ``kstat.zfs.misc.zil.zil_commit_count=935916`` (NetBSD).
    ``prefix`` is stripped from the key when supplied; otherwise only the last
    dotted component is kept.
    """
    stats: Dict[str, Any] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("kstat.") and line.endswith(":"):
            # A raw kstat value is printed as ``kstat.zfs.<pool>.<name>:``
            # followed by a multi-line payload; the payload is handled by the
            # table parsers, not here.
            continue
        if ": " in line:
            key, value = line.split(": ", 1)
        elif "=" in line:
            key, value = line.split("=", 1)
        else:
            continue
        key = key.strip()
        if prefix and key.startswith(prefix):
            key = key[len(prefix):].lstrip(".")
        elif "." in key:
            key = key.rsplit(".", 1)[-1]
        if not key:
            continue
        stats[key] = _coerce_number(value)
    return stats


def parse_txg_history(text: str) -> List[Dict[str, Any]]:
    """Parse per-pool txg history (``/proc/spl/kstat/zfs/<pool>/txgs``).

    The Linux ``procfs_list`` file and the FreeBSD sysctl value (the same
    table preceded by a ``kstat.zfs.<pool>.txgs:`` line) share one layout::

        txg      birth            state ndirty  nread  nwritten reads writes otime qtime wtime stime
        390251   361698140699850  C     1671168 0      888832    0     63     5120961672 5797 31429 11715735

    Column names are taken from the header row, so layouts with extra or
    missing columns still parse. ``state`` is a single ``txg_state_t`` letter
    (see :data:`TXG_STATE_NAMES`); the four time columns are nanoseconds.
    """
    columns: Optional[List[str]] = None
    entries: List[Dict[str, Any]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if line.startswith("kstat."):
            # sysctl label. Keep anything printed after "name: " on the same
            # line so the table header is not lost if it starts there.
            if ": " not in line:
                continue
            line = line.split(": ", 1)[1].strip()
        if not line:
            continue
        parts = line.split()
        if columns is None:
            if parts[0].lower() == "txg":
                columns = [part.lower() for part in parts]
            continue
        if len(parts) < len(columns):
            continue
        entry: Dict[str, Any] = {}
        for name, value in zip(columns, parts):
            if name == "state":
                entry["state"] = value
                entry["state_name"] = TXG_STATE_NAMES.get(value, "unknown")
            else:
                entry[name] = _coerce_number(value)
        entries.append(entry)
    return entries


def summarize_txg_history(entries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Roll up txg history rows into the figures shown on the dashboard."""
    if not entries:
        return {}

    def _ints(key: str) -> List[int]:
        return [int(e[key]) for e in entries if isinstance(e.get(key), int)]

    committed = [e for e in entries if e.get("state") == "C"]
    open_times = [int(e["otime"]) for e in committed if isinstance(e.get("otime"), int)]
    sync_times = [int(e["stime"]) for e in committed if isinstance(e.get("stime"), int)]
    txgs = _ints("txg")

    avg_open_ms = round(sum(open_times) / len(open_times) / 1e6, 3) if open_times else None
    avg_sync_ms = round(sum(sync_times) / len(sync_times) / 1e6, 3) if sync_times else None

    return {
        "count": len(entries),
        "committed": len(committed),
        "oldest_txg": min(txgs) if txgs else None,
        "newest_txg": max(txgs) if txgs else None,
        "avg_open_ms": avg_open_ms,
        "avg_sync_ms": avg_sync_ms,
        "total_read_bytes": sum(_ints("nread")),
        "total_written_bytes": sum(_ints("nwritten")),
        "total_reads": sum(_ints("reads")),
        "total_writes": sum(_ints("writes")),
    }


#: Sample windows offered by the ZIL statistics page, in seconds.
ZIL_SAMPLE_SECONDS = (1, 5, 10, 30)


def subtract_kstat(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """Return the change in integer kstat counters between two reads.

    Keys are taken from ``after``. Integer values become ``after - before``,
    clamped at 0 (a counter that went down means the module was reloaded
    between reads). Non-integer values are copied from ``after``.
    """
    delta: Dict[str, Any] = {}
    for key, value in after.items():
        old = before.get(key, 0)
        if isinstance(value, int) and isinstance(old, int):
            delta[key] = max(value - old, 0)
        else:
            delta[key] = value
    return delta


def summarize_zil_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Derive the ZIL figures worth showing on the dashboard."""

    def _int(key: str) -> int:
        value = stats.get(key, 0)
        return value if isinstance(value, int) else 0

    commits = _int("zil_commit_count")
    errors = _int("zil_commit_error_count")
    itx = _int("zil_itx_count")
    return {
        "commit_count": commits,
        "commit_writer_count": _int("zil_commit_writer_count"),
        "commit_error_count": errors,
        "commit_stall_count": _int("zil_commit_stall_count"),
        "commit_suspend_count": _int("zil_commit_suspend_count"),
        "commit_crash_count": _int("zil_commit_crash_count"),
        "commit_error_rate": (errors / commits * 100.0) if commits else 0.0,
        "itx_count": itx,
        "itx_indirect_count": _int("zil_itx_indirect_count"),
        "itx_indirect_bytes": _int("zil_itx_indirect_bytes"),
        "itx_copied_count": _int("zil_itx_copied_count"),
        "itx_copied_bytes": _int("zil_itx_copied_bytes"),
        "itx_needcopy_count": _int("zil_itx_needcopy_count"),
        "itx_needcopy_bytes": _int("zil_itx_needcopy_bytes"),
        "itx_metaslab_normal_count": _int("zil_itx_metaslab_normal_count"),
        "itx_metaslab_normal_bytes": _int("zil_itx_metaslab_normal_bytes"),
        "itx_metaslab_slog_count": _int("zil_itx_metaslab_slog_count"),
        "itx_metaslab_slog_bytes": _int("zil_itx_metaslab_slog_bytes"),
    }


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
            
            result = run_zfs_command(
                cmd,
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
            
            result = run_zfs_command(
                cmd,
                check=True,
                timeout=timeout
            )
            
            events = []
            
            if verbose:
                # In verbose mode, group lines together per event
                lines = result.stdout.strip().split('\n')
                current_event_lines = []
                
                for line in lines:
                    if not line or line.startswith('TIME'):
                        continue
                    
                    # Check if this is a header line (starts with timestamp, not indented)
                    if line and not line[0].isspace():
                        # Save previous event if exists
                        if current_event_lines:
                            event = self._parse_verbose_event(current_event_lines)
                            if event:
                                events.append(event)
                        # Start new event
                        current_event_lines = [line]
                    else:
                        # This is a detail line, add to current event
                        if current_event_lines:
                            current_event_lines.append(line)
                
                # Don't forget the last event
                if current_event_lines:
                    event = self._parse_verbose_event(current_event_lines)
                    if event:
                        events.append(event)
            else:
                # Normal mode: one line per event
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
            
            run_zfs_command(
                cmd,
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
        # FreeBSD/NetBSD use sysctl for debug messages
        if is_freebsd() or is_netbsd():
            return self._get_kernel_debug_log_sysctl(lines, filter_pattern)
        else:
            return self._get_kernel_debug_log_linux(lines, filter_pattern)
    
    def _get_kernel_debug_log_linux(
        self,
        lines: int = 1000,
        filter_pattern: Optional[str] = None
    ) -> List[str]:
        """
        Get ZFS kernel debug messages from Linux /proc filesystem
        
        Args:
            lines: Maximum number of lines to return
            filter_pattern: Optional regex pattern to filter messages
            
        Returns:
            List of log lines
        """
        try:
            dbgmsg_path = '/proc/spl/kstat/zfs/dbgmsg'
            
            # Check if file exists first
            if not Path(dbgmsg_path).exists():
                return ["Debug log not available (missing /proc/spl/kstat/zfs/dbgmsg)"]
            
            # Use sudo cat to read the privileged file
            result = subprocess.run(
                ['sudo', 'cat', dbgmsg_path],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0:
                # If sudo fails, try direct read as fallback (might work if already root)
                try:
                    with open(dbgmsg_path, 'r') as f:
                        log_lines = f.readlines()
                except PermissionError:
                    return [f"Permission denied reading {dbgmsg_path}",
                            "",
                            "To fix this on Linux, add to sudoers:",
                            "  webzfs ALL=(ALL) NOPASSWD: /usr/bin/cat /proc/spl/kstat/zfs/dbgmsg"]
            else:
                log_lines = result.stdout.split('\n')
            
            # Filter if pattern provided
            if filter_pattern:
                pattern = re.compile(filter_pattern, re.IGNORECASE)
                log_lines = [line for line in log_lines if pattern.search(line)]
            
            # Return last N lines
            return [line.strip() for line in log_lines[-lines:] if line.strip()]
            
        except Exception as e:
            return [f"Error reading debug log: {str(e)}"]
    
    def _get_kernel_debug_log_sysctl(
        self,
        lines: int = 1000,
        filter_pattern: Optional[str] = None
    ) -> List[str]:
        """
        Get ZFS kernel debug messages from sysctl (FreeBSD/NetBSD)
        
        Args:
            lines: Maximum number of lines to return
            filter_pattern: Optional regex pattern to filter messages
            
        Returns:
            List of log lines
        """
        try:
            # FreeBSD/NetBSD use sysctl for ZFS debug messages
            # Try kstat.zfs.misc.dbgmsg first (more common)
            result = subprocess.run(
                ['sysctl', '-n', 'kstat.zfs.misc.dbgmsg'],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode != 0 or not result.stdout.strip():
                # Try alternative location vfs.zfs.dbgmsg
                result = subprocess.run(
                    ['sysctl', '-n', 'vfs.zfs.dbgmsg'],
                    capture_output=True,
                    text=True,
                    check=False
                )
            
            if result.returncode != 0 or not result.stdout.strip():
                # Debug messages may not be enabled
                return ["Debug log not available (sysctl kstat.zfs.misc.dbgmsg not accessible)",
                        "",
                        "To enable ZFS debug messages on FreeBSD, you may need to:",
                        "  1. Set: sysctl vfs.zfs.dbgmsg_enable=1",
                        "  2. Or check if ZFS debug options are compiled in"]
            
            # Parse the output - each line is a debug message
            log_lines = result.stdout.strip().split('\n')
            
            # Filter if pattern provided
            if filter_pattern:
                pattern = re.compile(filter_pattern, re.IGNORECASE)
                log_lines = [line for line in log_lines if pattern.search(line)]
            
            # Return last N lines
            return [line.strip() for line in log_lines[-lines:] if line.strip()]
            
        except Exception as e:
            return [f"Error reading debug log via sysctl: {str(e)}"]
    
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

        # Linux: walk through possible sources and grep for zfs/zpool.
        # We try unprivileged journalctl, then sudo -n journalctl (in
        # case the webzfs user is not in systemd-journal but does have
        # NOPASSWD sudo for journalctl), then plain text syslog files
        # (with sudo as a second pass), then dmesg. Each step records
        # any stderr it produces so the caller can surface a real
        # error if everything fails.
        zfs_lines: List[Dict[str, Any]] = []
        attempt_errors: List[str] = []

        priority_map = {
            'error': 'err',
            'warning': 'warning',
            'info': 'info',
        }

        def build_journalctl_cmd(prefix: List[str]) -> List[str]:
            cmd = list(prefix) + ['journalctl', '-n', str(lines), '--no-pager']
            if since:
                cmd.extend(['--since', since.isoformat()])
            if severity and severity.lower() in priority_map:
                cmd.extend(['-p', priority_map[severity.lower()]])
            return cmd

        for prefix in ([], ['sudo', '-n']):
            try:
                result = subprocess.run(
                    build_journalctl_cmd(prefix),
                    capture_output=True,
                    text=True,
                    check=False,
                )
            except FileNotFoundError:
                continue

            if result.returncode == 0 and result.stdout.strip():
                for line in result.stdout.split('\n'):
                    low = line.lower()
                    if 'zfs' in low or 'zpool' in low:
                        zfs_lines.append({'message': line.strip()})
                if zfs_lines:
                    return zfs_lines
                # journalctl worked but no ZFS lines found; do not
                # fall through to syslog files in that case
                return [{'message': 'No ZFS messages found in journalctl output'}]

            err = (result.stderr or '').strip()
            if err:
                label = 'sudo journalctl' if prefix else 'journalctl'
                attempt_errors.append(f'{label}: {err.splitlines()[0]}')

        # Plain text syslog files (Debian/Ubuntu and old RHEL).
        # Try unprivileged then sudo -n cat for each path.
        for path in ('/var/log/syslog', '/var/log/messages'):
            for use_sudo in (False, True):
                argv = (
                    ['sudo', '-n', 'tail', '-n', str(lines * 5), path]
                    if use_sudo
                    else ['tail', '-n', str(lines * 5), path]
                )
                try:
                    result = subprocess.run(
                        argv,
                        capture_output=True,
                        text=True,
                        check=False,
                    )
                except FileNotFoundError:
                    break
                if result.returncode == 0 and result.stdout.strip():
                    for line in result.stdout.split('\n'):
                        low = line.lower()
                        if 'zfs' in low or 'zpool' in low:
                            zfs_lines.append({'message': line.strip()})
                    if zfs_lines:
                        return zfs_lines[-lines:]
                    # File readable but no ZFS lines; stop searching
                    return [{'message': f'No ZFS messages found in {path}'}]
                err = (result.stderr or '').strip()
                if err and 'No such file' not in err:
                    label = f'sudo tail {path}' if use_sudo else f'tail {path}'
                    attempt_errors.append(f'{label}: {err.splitlines()[0]}')

        # Last resort: dmesg ring buffer
        fallback = self._fallback_syslog_read(lines)
        if fallback and fallback[0].get('message') not in (
            'Syslog not available',
            'Error reading syslog',
        ):
            return fallback

        if attempt_errors:
            return [
                {'message': 'Syslog not available. Tried:'},
            ] + [{'message': '  ' + msg} for msg in attempt_errors] + [
                {'message': ''},
                {'message': 'Add journalctl to webzfs sudoers, or add the webzfs user to the systemd-journal group:'},
                {'message': '  sudo usermod -aG systemd-journal webzfs'},
            ]
        return fallback
    
    def get_arc_summary(self) -> Dict[str, Any]:
        """
        Get ARC (Adaptive Replacement Cache) statistics
        
        Returns:
            Dictionary with ARC stats
        """
        # FreeBSD/NetBSD use sysctl for ARC stats
        if is_freebsd() or is_netbsd():
            return self._get_arc_summary_sysctl()
        else:
            return self._get_arc_summary_linux()
    
    def _get_arc_summary_linux(self) -> Dict[str, Any]:
        """Get ARC stats from Linux /proc filesystem"""
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

    def get_zil_stats(self, sample_seconds: Optional[int] = None) -> Dict[str, Any]:
        """Get ZIL (ZFS Intent Log) statistics.

        Linux exposes the ``zil`` kstat as ``/proc/spl/kstat/zfs/zil``; FreeBSD
        and NetBSD expose the same named kstat through the sysctl tree as
        ``kstat.zfs.misc.zil`` (``kstat_create("zfs", 0, "zil", "misc", ...)``
        in ``module/zfs/zil.c``). The counters are global (all pools) and
        cumulative since the ZFS module was loaded.

        Without ``sample_seconds`` the cumulative counters are returned. With
        ``sample_seconds`` the kstat is read twice that many seconds apart and
        the difference is returned (see :func:`subtract_kstat`). The caller
        blocks for the window.

        Args:
            sample_seconds: Optional sample window, one of
                ``ZIL_SAMPLE_SECONDS``.

        Returns:
            ``{'available': bool, 'platform': str, 'stats': {...},
            'summary': {...}, 'mode': 'cumulative'|'sample',
            'sample_seconds': int|None, 'has_activity': bool,
            'timestamp': str}`` on success, or ``available=False`` with an
            ``error`` when the platform does not publish the kstat.
        """
        platform = self._platform_name()

        if not get_capabilities().kstat_observability:
            return {
                'available': False,
                'platform': platform,
                'error': 'ZIL statistics are not supported on this platform'
            }

        if sample_seconds is not None and sample_seconds not in ZIL_SAMPLE_SECONDS:
            return {
                'available': False,
                'platform': platform,
                'error': f'Invalid sample window: {sample_seconds} seconds'
            }

        first = self._read_zil_kstat(platform)
        if 'error' in first:
            return first

        if sample_seconds:
            time.sleep(sample_seconds)
            second = self._read_zil_kstat(platform)
            if 'error' in second:
                return second
            stats = subtract_kstat(first['stats'], second['stats'])
            mode = 'sample'
        else:
            stats = first['stats']
            mode = 'cumulative'

        summary = summarize_zil_stats(stats)
        return {
            'available': True,
            'platform': platform,
            'source': first['source'],
            'stats': stats,
            'summary': summary,
            'mode': mode,
            'sample_seconds': sample_seconds,
            'has_activity': bool(summary['commit_count'] or summary['itx_count']),
            'timestamp': datetime.now().isoformat(),
        }

    def _read_zil_kstat(self, platform: str) -> Dict[str, Any]:
        """Read the ``zil`` kstat once.

        Returns:
            ``{'stats': {...}, 'source': str}``, or the ``available=False``
            result with an ``error``.
        """
        if platform in ('FreeBSD', 'NetBSD'):
            try:
                result = subprocess.run(
                    ['sysctl', 'kstat.zfs.misc.zil'],
                    capture_output=True,
                    text=True,
                    check=False
                )
            except FileNotFoundError:
                return {
                    'available': False,
                    'platform': platform,
                    'error': 'sysctl command not found'
                }
            if result.returncode != 0:
                return {
                    'available': False,
                    'platform': platform,
                    'error': 'ZIL statistics not available via sysctl'
                }
            stats = parse_kstat_sysctl(result.stdout, prefix='kstat.zfs.misc.zil')
            if not stats:
                return {
                    'available': False,
                    'platform': platform,
                    'error': 'ZIL statistics not available via sysctl'
                }
        else:
            zil_path = Path('/proc/spl/kstat/zfs/zil')
            if not zil_path.exists():
                return {
                    'available': False,
                    'platform': platform,
                    'error': 'ZIL statistics not available'
                }
            try:
                stats = parse_kstat_procfs(zil_path.read_text())
            except Exception as e:
                return {
                    'available': False,
                    'platform': platform,
                    'error': f'Failed to read ZIL statistics: {str(e)}'
                }
            if not stats:
                return {
                    'available': False,
                    'platform': platform,
                    'error': 'ZIL statistics not available'
                }

        return {
            'stats': stats,
            'source': ('sysctl kstat.zfs.misc.zil'
                       if platform in ('FreeBSD', 'NetBSD')
                       else '/proc/spl/kstat/zfs/zil'),
        }

    def get_txg_history(self, pool_name: str) -> Dict[str, Any]:
        """Get the per-pool txg history kstat.

        The history is maintained by ``spa_stats.c`` through
        ``procfs_list_install("zfs", spa_name(spa), "txgs", ...)`` and is only
        populated while the ``zfs_txg_history`` module parameter is non-zero.
        On Linux it is ``/proc/spl/kstat/zfs/<pool>/txgs``; on FreeBSD the raw
        kstat is ``kstat.zfs.<pool>.txgs``.

        Args:
            pool_name: Pool whose txg history should be read.

        Returns:
            ``{'available': bool, 'pool': str, 'platform': str,
            'entries': [...], 'summary': {...}}``, with ``available=False`` and
            an ``error`` when the history is not published.
        """
        platform = self._platform_name()
        entries: List[Dict[str, Any]] = []

        if not get_capabilities().kstat_observability:
            return {
                'available': False,
                'pool': pool_name,
                'platform': platform,
                'entries': [],
                'error': 'txg history is not supported on this platform'
            }

        # The pool name becomes a sysctl OID component on FreeBSD and a path
        # component under /proc/spl/kstat/zfs on Linux, so it must be a valid
        # pool name (no "/", "..", or leading "-") before either is built.
        try:
            ZFSPoolService.validate_pool_name(pool_name)
        except ValueError as e:
            return {
                'available': False,
                'pool': pool_name,
                'platform': platform,
                'entries': [],
                'error': str(e)
            }

        if platform in ('FreeBSD', 'NetBSD'):
            try:
                result = subprocess.run(
                    ['sysctl', f'kstat.zfs.{pool_name}.txgs'],
                    capture_output=True,
                    text=True,
                    check=False
                )
            except FileNotFoundError:
                return {
                    'available': False,
                    'pool': pool_name,
                    'platform': platform,
                    'entries': [],
                    'error': 'sysctl command not found'
                }
            if result.returncode != 0:
                return {
                    'available': False,
                    'pool': pool_name,
                    'platform': platform,
                    'entries': [],
                    'error': 'txg history not available (is zfs_txg_history set?)'
                }
            entries = parse_txg_history(result.stdout)
            source = f'sysctl kstat.zfs.{pool_name}.txgs'
        else:
            txgs_path = Path('/proc/spl/kstat/zfs') / pool_name / 'txgs'
            if not txgs_path.exists():
                return {
                    'available': False,
                    'pool': pool_name,
                    'platform': platform,
                    'entries': [],
                    'error': 'txg history not available (is zfs_txg_history set?)'
                }
            try:
                entries = parse_txg_history(txgs_path.read_text())
            except Exception as e:
                return {
                    'available': False,
                    'pool': pool_name,
                    'platform': platform,
                    'entries': [],
                    'error': f'Failed to read txg history: {str(e)}'
                }
            source = str(txgs_path)

        if not entries:
            return {
                'available': False,
                'pool': pool_name,
                'platform': platform,
                'entries': [],
                'source': source,
                'error': 'txg history not available (is zfs_txg_history set?)'
            }

        return {
            'available': True,
            'pool': pool_name,
            'platform': platform,
            'source': source,
            'entries': entries,
            'summary': summarize_txg_history(entries)
        }

    def get_vdev_activity(self, pool_name: str) -> Dict[str, Any]:
        """Get TRIM and initialize progress for a pool's leaf vdevs.

        ``zpool status -t`` and ``zpool status -i`` annotate each leaf vdev
        with its TRIM/initialize state. The parsing itself lives in
        :mod:`services.vdev_activity`, shared with the vdev management page
        (H8) so the two features do not parse the same output twice.

        Args:
            pool_name: Pool to query.

        Returns:
            ``{'available': bool, 'pool': str, 'trim': {...},
            'initialize': {...}}``. ``available`` is False when the installed
            ``zpool`` does not support the ``-t``/``-i`` flags.
        """
        from services.vdev_activity import (
            INITIALIZE,
            TRIM,
            parse_initialize_status,
            parse_trim_status,
        )

        results: Dict[str, Any] = {'pool': pool_name}
        if not get_capabilities().vdev_activity_status:
            results['available'] = False
            results['error'] = ('TRIM/initialize status is not supported '
                                'on this platform')
            return results
        try:
            ZFSPoolService.validate_pool_name(pool_name)
        except ValueError as e:
            results['available'] = False
            results['error'] = str(e)
            return results

        available = False
        for kind, flag, parser in (
            (TRIM, '-t', parse_trim_status),
            (INITIALIZE, '-i', parse_initialize_status),
        ):
            try:
                result = run_zfs_command(
                    ['zpool', 'status', flag, pool_name],
                    check=False,
                    timeout=self.timeouts.get('status', self.timeouts['default'])
                )
            except Exception as e:
                results[kind] = {'error': str(e)}
                continue

            if result.returncode != 0:
                results[kind] = {
                    'error': (result.stderr or '').strip()
                    or f'zpool status {flag} failed'
                }
                continue

            summary = parser(result.stdout)
            results[kind] = summary.to_dict()
            available = True

        results['available'] = available
        if not available:
            results['error'] = ('TRIM/initialize status not available '
                                '(zpool status -t/-i unsupported)')
        return results

    def _platform_name(self) -> str:
        """Return the platform name used by the kstat readers."""
        if is_freebsd():
            return 'FreeBSD'
        if is_netbsd():
            return 'NetBSD'
        return 'Linux'

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
            # zpool events format: TIME (4 parts: month day year time) CLASS [details]
            # Example: Jan 13 2026 20:39:02.026667341 sysevent.fs.zfs.history_event
            parts = line.split(None, 4)  # Split into 5 parts max
            if len(parts) < 5:
                return None
            
            # First 4 parts are the timestamp
            time_str = f"{parts[0]} {parts[1]} {parts[2]} {parts[3]}"
            event_class = parts[4]
            
            return {
                'time': time_str,
                'class': event_class,
                'details': '',  # No additional details in non-verbose mode
                'raw': line
            }
        except:
            return {'raw': line}
    
    def _parse_verbose_event(self, lines: List[str]) -> Optional[Dict[str, Any]]:
        """Parse a verbose event with multiple lines"""
        try:
            if not lines:
                return None
            
            # First line is the header with timestamp and class
            header_line = lines[0]
            parts = header_line.split(None, 4)
            if len(parts) < 5:
                return {'raw': '\n'.join(lines)}
            
            # First 4 parts are the timestamp
            time_str = f"{parts[0]} {parts[1]} {parts[2]} {parts[3]}"
            event_class = parts[4]
            
            # Combine all lines for raw output
            raw_output = '\n'.join(lines)
            
            return {
                'time': time_str,
                'class': event_class,
                'raw': raw_output
            }
        except:
            return {'raw': '\n'.join(lines) if lines else ''}
    
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
