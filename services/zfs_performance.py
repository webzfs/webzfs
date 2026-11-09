"""
ZFS Performance Monitoring Service
Handles performance metrics, I/O statistics, and process monitoring
"""
import subprocess
import time
import psutil
import platform
import re
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from config.settings import Settings
from services.utils import is_freebsd, is_netbsd, run_zfs_command
from services.scan_status import parse_scan
from services.zfs_capabilities import get_capabilities
from services.zfs_pool import ZFSPoolService


# ---------------------------------------------------------------------------
# zpool output parsers
# ---------------------------------------------------------------------------
# The parsers below are pure functions so they can be unit-tested against
# captured command output; the service methods only run the commands.

#: Latency histogram bucket labels printed by ``zpool iostat -w``.
_HISTOGRAM_BUCKET_RE = re.compile(r"^\d+(?:\.\d+)?(?:ns|us|ms|s)$")

#: Suffixes printed by ``zfs_nicenum()`` for counters. Both the ``zpool
#: iostat -w`` histogram cells and the ``zpool status -s`` SLOW column use
#: ``ZFS_NICENUM_1024``, so every step is a factor of 1024, not 1000.
_HISTOGRAM_SUFFIXES = {
    "": 1,
    "K": 1024,
    "M": 1024 ** 2,
    "G": 1024 ** 3,
    "T": 1024 ** 4,
    "P": 1024 ** 5,
    "E": 1024 ** 6,
}

#: Lines in ``zpool status`` output that are not vdev rows.
_STATUS_NON_VDEV_PREFIXES = (
    "pool:", "state:", "status:", "action:", "see:", "scan:", "config:",
    "errors:", "remove:", "checkpoint:",
)


def parse_histogram_value(text: str) -> int:
    """Convert a ``zpool iostat -w`` counter such as ``4.82K`` to an int."""
    value = text.strip()
    if not value or value == "-":
        return 0
    suffix = ""
    if value[-1] in _HISTOGRAM_SUFFIXES and value[-1] != "":
        suffix = value[-1]
        value = value[:-1]
    try:
        return int(round(float(value) * _HISTOGRAM_SUFFIXES[suffix]))
    except (KeyError, ValueError):
        return 0


def parse_slow_io_status(text: str) -> Dict[str, Any]:
    """Parse ``zpool status -s`` into per-vdev slow I/O counts.

    ``-s`` appends a ``SLOW`` column that is populated for leaf vdevs only
    (``cmd/zpool/zpool_main.c``: "Only leafs vdevs have slow IOs"); the pool
    row carries ``-``. When the column is absent (zpool built without slow I/O
    support) ``available`` is False.

    Returns:
        ``{'available': bool, 'pool': str|None, 'slow_io': {vdev: int},
        'total_slow_ios': int}``
    """
    pool: Optional[str] = None
    has_slow_column = False
    slow_io: Dict[str, int] = {}

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if stripped.startswith("pool:"):
            pool = stripped.split(":", 1)[1].strip()
            continue
        if stripped.startswith(_STATUS_NON_VDEV_PREFIXES):
            continue

        parts = stripped.split()
        if "SLOW" in parts:
            has_slow_column = True
            continue
        if not has_slow_column or len(parts) < 6:
            continue

        # NAME STATE READ WRITE CKSUM SLOW. The error columns are printed with
        # " %5s", so a 5-character value such as "1.20K" is separated from its
        # neighbour by a single space; split on any whitespace. Trailing
        # annotations such as "(trimming)" come after the SLOW column.
        name = parts[0]
        slow = parts[5]
        if slow == "-":
            continue
        slow_io[name] = parse_histogram_value(slow)

    return {
        "available": has_slow_column,
        "pool": pool,
        "slow_io": slow_io,
        "total_slow_ios": sum(slow_io.values()),
    }


def _histogram_columns(group_line: str, sub_line: str) -> List[str]:
    """Combine the two header rows of ``zpool iostat -w`` into column names.

    The first row holds the group names (``total_wait``, ``disk_wait``,
    ``syncq_wait``, ``asyncq_wait``) and the second the per-group columns
    (``read``/``write`` plus standalone ``scrub``/``trim``/``rebuild``).
    ``zpool`` centres each group label over its columns rather than aligning
    the text, so a sub-column is matched to the group whose label centre is
    nearest. This keeps working when a version adds or removes a column.
    """
    groups = []
    for index, match in enumerate(re.finditer(r"\S+", group_line)):
        # The first token is the pool name, not a column group.
        if index == 0:
            continue
        groups.append((match.group(), (match.start() + match.end()) / 2.0))
    columns: List[str] = []
    for match in re.finditer(r"\S+", sub_line):
        token = match.group()
        if token.lower() == "latency":
            continue
        centre = (match.start() + match.end()) / 2.0
        group = min(groups, key=lambda item: abs(item[1] - centre))[0] if groups else None
        if group and token.lower() in ("read", "write"):
            columns.append(f"{group}_{token.lower()}")
        else:
            columns.append(token.lower())
    return columns


def parse_latency_histogram(text: str) -> Dict[str, Any]:
    """Parse ``zpool iostat -w`` output into per-pool latency histograms.

    Returns:
        ``{'available': bool, 'pools': {pool: {'columns': [...],
        'buckets': [{'bucket': '1ns', <column>: <count>, ...}]}}}``
    """
    pools: Dict[str, Dict[str, Any]] = {}
    current: Optional[Dict[str, Any]] = None
    group_line = ""

    for raw_line in text.splitlines():
        stripped = raw_line.strip()
        if not stripped:
            continue
        if set(stripped) <= {"-"}:
            continue

        parts = stripped.split()

        # Group header: "<pool>  total_wait  disk_wait  syncq_wait ...".
        # Matched on the second token so a pool named "latency" still works.
        if len(parts) >= 2 and parts[1] == "total_wait":
            group_line = raw_line.rstrip()
            current = {"columns": [], "buckets": []}
            pools[parts[0]] = current
            continue

        # Column header: "latency  read  write  read  write ...".
        if parts[0] == "latency" and len(parts) >= 2 and parts[1] == "read":
            if current is not None and group_line:
                current["columns"] = _histogram_columns(group_line, raw_line.rstrip())
            continue

        if current is None or not current["columns"]:
            continue

        if not _HISTOGRAM_BUCKET_RE.match(parts[0]):
            continue

        row: Dict[str, Any] = {"bucket": parts[0]}
        for column, value in zip(current["columns"], parts[1:]):
            row[column] = parse_histogram_value(value)
        current["buckets"].append(row)

    return {
        "available": any(pool["buckets"] for pool in pools.values()),
        "pools": pools,
    }


#: Sample windows offered by the latency histogram page, in seconds.
LATENCY_SAMPLE_SECONDS = (5, 10, 30)


def subtract_latency_histogram(before: Dict[str, Any], after: Dict[str, Any]) -> Dict[str, Any]:
    """Return the I/O counts that completed between two histogram snapshots.

    Both arguments are :func:`parse_latency_histogram` results taken from
    cumulative ``zpool iostat -w -p`` output. The difference is computed here
    instead of using ``zpool iostat -w <interval>`` because ``zpool`` converts
    interval histograms to a per-second rate and truncates it to an integer
    (``print_iostat_histo()`` in ``cmd/zpool/zpool_main.c``), so a few slow
    I/Os spread over a multi-second window print as 0.

    Only pools present in both snapshots are returned. A count that went down
    (pool exported and imported between the snapshots) is reported as 0.
    """
    pools: Dict[str, Dict[str, Any]] = {}
    for pool_name, after_data in after.get("pools", {}).items():
        before_data = before.get("pools", {}).get(pool_name)
        if before_data is None:
            continue
        before_rows = {row["bucket"]: row for row in before_data["buckets"]}
        buckets = []
        for row in after_data["buckets"]:
            before_row = before_rows.get(row["bucket"], {})
            new_row: Dict[str, Any] = {"bucket": row["bucket"]}
            for column in after_data["columns"]:
                new_row[column] = max(row.get(column, 0) - before_row.get(column, 0), 0)
            buckets.append(new_row)
        pools[pool_name] = {"columns": after_data["columns"], "buckets": buckets}

    return {
        "available": any(pool["buckets"] for pool in pools.values()),
        "pools": pools,
    }


def histogram_has_activity(histogram: Dict[str, Any]) -> bool:
    """Return True when any bucket of any pool has a non-zero count."""
    for data in histogram.get("pools", {}).values():
        for row in data["buckets"]:
            for column in data["columns"]:
                if row.get(column, 0):
                    return True
    return False


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

        FreeBSD's gstat does not support a sample-count argument. The -b
        (batch) flag always collects one snapshot and exits, and -I sets
        the sampling window. The count parameter is accepted for API
        compatibility but is ignored.

        Args:
            interval: Sampling window in seconds
            count: Unused, accepted for API compatibility

        Returns:
            Dictionary with gstat output
        """
        if self.system != 'FreeBSD':
            return {'error': 'gstat only available on FreeBSD'}

        try:
            # gstat usage: gstat [-abBcCdops] [-f filter] [-I interval]
            # -b: batch mode (collect one sample, print, exit)
            # -I 1s: sampling window of 1 second
            # Note: -c on gstat is "show consumers", NOT a count flag
            cmd = ['gstat', '-b', '-I', str(interval) + 's']

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
    
    # ZFS process/thread name patterns shared between Linux psutil enumeration
    # and the FreeBSD ps-based enumeration. Matched as prefixes against the
    # process name (Linux) or kernel thread name (FreeBSD).
    ZFS_PROCESS_PATTERNS = [
        'arc_evict', 'arc_flush', 'arc_prune', 'arc_reap',
        'dbu_evict', 'dbuf_evict', 'dmu_objset_find',
        'dp_sync_taskq', 'dp_zil_clean_taskq',
        'dsl_scan_iss',
        'l2arc_feed',
        'metaslab_group_task',
        'mmp',
        'raidz_expand',
        'receive_writer',
        'redact_list', 'redact_merge', 'redact_traverse',
        'send_merge', 'send_reader', 'send_traverse',
        'spa_async', 'spa_vdev_remove',
        'spl_delay_taskq', 'spl_dynamic_taskq', 'spl_kmem_cache', 'spl_system_taskq',
        'sysevent',
        'tx_commit_cb',
        'txg_quiesce', 'txg_sync', 'txg_thread_enter',
        'vdev_autotrim', 'vdev_initialize', 'vdev_load', 'vdev_open',
        'vdev_rebuild', 'vdev_trim', 'vdev_validate',
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
        'zfsvfs',
        'zpool',
        'zfs',
        'zfskern',
        'zfsd',
        'zed',
    ]

    def get_zfs_processes(
        self,
        min_cpu_percent: float = 0.0,
        sort_by_cpu: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Get all ZFS-related processes/threads with resource usage.

        On Linux, ZFS kernel workers are exposed as individual kernel threads
        with their own PIDs (visible via psutil.process_iter).

        On FreeBSD, ZFS workers are kernel threads inside the single
        `zfskern` (PID 7) and `kernel` (PID 0) processes. psutil does not
        enumerate kernel TIDs on FreeBSD, so we shell out to `ps -axwH` to
        list every thread and filter by thread name.

        Args:
            min_cpu_percent: Minimum CPU percentage to include
            sort_by_cpu: Sort by CPU percentage descending

        Returns:
            List of ZFS processes with stats. Each entry has the keys:
            pid, name, username, cpu_percent, memory_percent, status.
            On FreeBSD a `parent_pid` key is also included to indicate
            which kernel-process container the thread lives in.
        """
        try:
            if self.system == 'FreeBSD':
                processes = self._get_zfs_processes_freebsd()
            else:
                processes = self._get_zfs_processes_psutil()

            # Apply minimum CPU filter
            if min_cpu_percent > 0.0:
                processes = [
                    p for p in processes
                    if p.get('cpu_percent', 0.0) >= min_cpu_percent
                ]

            if sort_by_cpu:
                processes.sort(
                    key=lambda x: x.get('cpu_percent', 0.0),
                    reverse=True,
                )

            return processes

        except Exception as e:
            raise Exception(f"Failed to get ZFS processes: {str(e)}")

    def _matches_zfs_pattern(self, thread_name: str) -> bool:
        """Return True if the given name matches any ZFS process pattern."""
        if not thread_name:
            return False
        for pattern in self.ZFS_PROCESS_PATTERNS:
            if thread_name.startswith(pattern):
                return True
        return False

    def _get_zfs_processes_psutil(self) -> List[Dict[str, Any]]:
        """Enumerate ZFS processes via psutil (Linux and other non-BSD)."""
        zfs_processes: List[Dict[str, Any]] = []

        for proc in psutil.process_iter(
            ['pid', 'name', 'username', 'cpu_percent', 'memory_percent', 'status']
        ):
            try:
                name = proc.info['name'] or ''
                if not self._matches_zfs_pattern(name):
                    continue

                zfs_processes.append({
                    'pid': proc.info['pid'],
                    'name': name,
                    'username': proc.info['username'],
                    'cpu_percent': proc.info['cpu_percent'] or 0.0,
                    'memory_percent': proc.info['memory_percent'] or 0.0,
                    'status': proc.info['status'],
                })
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        return zfs_processes

    def _get_zfs_processes_freebsd(self) -> List[Dict[str, Any]]:
        """
        Enumerate ZFS kernel threads on FreeBSD using `ps -axwH`.

        FreeBSD's `ps -H` flag lists each thread on its own line. The `comm=`
        column with no header produces an unpadded `procname/threadname`
        string. We extract the thread name (after the slash) and match it
        against ZFS_PROCESS_PATTERNS. Lines without a slash represent
        single-threaded user processes (e.g. zfsd, zed) and are matched on
        the bare process name.
        """
        zfs_processes: List[Dict[str, Any]] = []

        try:
            # -a all, -x include processes without a controlling terminal,
            # -w wide output (no truncation), -H show kernel threads.
            #
            # Note on -o: BSD ps treats `colname=label` as a single column
            # with a custom label that runs to the end of the argument
            # token. So `pid=,user=,...` would be parsed as ONE column
            # named pid with the label ",user=,...". We therefore use
            # plain comma-separated column names (no `=`) and skip the
            # header row in code below.
            result = subprocess.run(
                [
                    'ps', '-axwH',
                    '-o', 'pid,user,pcpu,pmem,stat,comm',
                ],
                capture_output=True,
                text=True,
                check=True,
                timeout=10,
            )
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
                FileNotFoundError):
            # Fall back to psutil view (which only sees the parent procs)
            # so the page still renders something rather than failing hard.
            return self._get_zfs_processes_psutil()

        for line in result.stdout.splitlines():
            line = line.rstrip()
            if not line.strip():
                continue

            # Split into 6 fields; comm may contain spaces but in practice
            # FreeBSD thread names do not, so a 5-way split is safe.
            parts = line.split(None, 5)
            if len(parts) < 6:
                continue

            pid_str, user, pcpu_str, pmem_str, stat, comm = parts

            # Skip the header row that ps emits as the first line
            # ("PID USER %CPU %MEM STAT COMMAND" or similar)
            try:
                pid = int(pid_str)
            except ValueError:
                continue

            # comm is "procname/threadname" for kernel threads, or just
            # "procname" for ordinary user processes.
            if '/' in comm:
                proc_name, thread_name = comm.split('/', 1)
                display_name = thread_name
            else:
                proc_name = comm
                thread_name = comm
                display_name = comm

            # Match against thread name first (where the interesting ZFS
            # workers live), then fall back to the parent process name so
            # zfsd/zed/zfskern itself still show up.
            if not (self._matches_zfs_pattern(thread_name)
                    or self._matches_zfs_pattern(proc_name)):
                continue

            try:
                cpu_percent = float(pcpu_str)
            except ValueError:
                cpu_percent = 0.0
            try:
                memory_percent = float(pmem_str)
            except ValueError:
                memory_percent = 0.0

            zfs_processes.append({
                'pid': pid,
                'name': display_name,
                'parent_name': proc_name,
                'username': user,
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'status': self._normalize_freebsd_status(stat),
                'raw_status': stat,
            })

        return zfs_processes

    @staticmethod
    def _normalize_freebsd_status(stat: str) -> str:
        """
        Map a FreeBSD ps STAT code to a psutil-style status string.

        FreeBSD STAT first letter:
          R = runnable      -> running
          S = sleeping      -> sleeping
          I = idle          -> sleeping
          D = disk wait     -> disk-sleep
          L = lock wait     -> disk-sleep
          T = stopped       -> stopped
          Z = zombie        -> zombie

        Most ZFS kernel threads sit in "DL" (disk wait, lock held).
        """
        if not stat:
            return 'unknown'
        first = stat[0]
        return {
            'R': 'running',
            'S': 'sleeping',
            'I': 'sleeping',
            'D': 'disk-sleep',
            'L': 'disk-sleep',
            'T': 'stopped',
            'Z': 'zombie',
        }.get(first, 'unknown')
    
    def get_pool_capacity_stats(
        self,
        pool_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get detailed capacity statistics for pools.
        
        Includes 'available' from ZFS dataset layer which represents the actual
        usable space for users. The 'free' field from zpool list is kept as raw
        pool free space. Capacity is recalculated based on ZFS available space.
        
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
            
            # Enrich with ZFS used/available space from the dataset layer.
            # used + avail form a consistent pair: total = used + avail.
            space_map = {}
            try:
                space_cmd = ['zfs', 'list', '-H', '-p', '-o', 'name,used,avail', '-d', '0']
                if pool_name:
                    space_cmd.append(pool_name)
                space_result = subprocess.run(
                    space_cmd,
                    capture_output=True, text=True, check=False,
                    timeout=timeout,
                )
                if space_result.returncode == 0:
                    for line in space_result.stdout.strip().split('\n'):
                        if not line:
                            continue
                        sp_parts = line.split('\t')
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

            for pool in pools:
                name = pool['name']
                space = space_map.get(name)
                if space is not None:
                    used_bytes = space['used']
                    avail_bytes = space['avail']
                    total_bytes = used_bytes + avail_bytes
                    pool['used'] = self._format_bytes_zfs(used_bytes)
                    pool['available'] = self._format_bytes_zfs(avail_bytes)
                    pool['total'] = self._format_bytes_zfs(total_bytes)
                    # Recalculate capacity based on used / total
                    if total_bytes > 0:
                        used_pct = round(
                            (used_bytes / total_bytes) * 100
                        )
                        pool['capacity'] = f"{used_pct}%"
                else:
                    pool['used'] = pool['allocated']
                    pool['available'] = pool['free']
                    pool['total'] = pool['size']
            
            return {'pools': pools}
        
        except subprocess.TimeoutExpired:
            raise Exception(f"ZPool list command timed out after {timeout} seconds. The system may be unresponsive.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get capacity stats: {e.stderr}")

    @staticmethod
    def _format_bytes_zfs(size_bytes: int) -> str:
        """
        Format bytes to ZFS-style human-readable string.
        Mimics the output format of zpool/zfs list (e.g., 1.82T, 844G, 512M).
        """
        if size_bytes == 0:
            return "0B"
        units = ['B', 'K', 'M', 'G', 'T', 'P', 'E']
        value = float(size_bytes)
        for unit in units:
            if abs(value) < 1024:
                if value >= 100:
                    return f"{int(value)}{unit}"
                elif value >= 10:
                    return f"{value:.1f}{unit}"
                else:
                    return f"{value:.2f}{unit}"
            value /= 1024
        return f"{value:.2f}E"
    
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
            scan = parse_scan(status)

            if scan.present:
                if scan.paused:
                    return {
                        'status': 'paused',
                        'progress': scan.progress,
                        'info': scan.raw,
                    }
                if scan.in_progress:
                    return {
                        'status': 'in_progress',
                        'progress': scan.progress,
                        'info': scan.raw,
                    }
                if scan.completed:
                    return {
                        'status': 'completed',
                        'info': scan.raw,
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
        On NetBSD: sysctl -a | grep arcstats
        
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
            
            elif self.system == 'NetBSD':
                # NetBSD uses sysctl for ARC stats
                try:
                    result = subprocess.run(
                        ['sh', '-c', 'sysctl -a | grep -i arcstats'],
                        capture_output=True,
                        text=True,
                        timeout=30
                    )
                    
                    output = result.stdout if result.stdout else 'No ARC stats found via sysctl'
                    
                    return {
                        'output': output,
                        'system': self.system,
                        'command': 'sysctl -a | grep -i arcstats',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                except subprocess.TimeoutExpired:
                    return {
                        'error': 'sysctl command timed out',
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

    def get_slow_io_counts(self, pool_name: str) -> Dict[str, Any]:
        """Return per-vdev slow I/O counts for a pool (``zpool status -s``).

        Args:
            pool_name: Pool to query.

        Returns:
            The parsed result of :func:`parse_slow_io_status` plus a
            ``timestamp``, or ``available=False`` with an ``error`` when the
            command fails or the ``SLOW`` column is not present.
        """
        if not get_capabilities().slow_io_status:
            return {
                'available': False,
                'pool': pool_name,
                'slow_io': {},
                'error': 'Slow I/O counts are not supported on this platform'
            }
        try:
            ZFSPoolService.validate_pool_name(pool_name)
        except ValueError as e:
            return {
                'available': False,
                'pool': pool_name,
                'slow_io': {},
                'error': str(e)
            }

        timeout = self.timeouts.get('status', self.timeouts['default'])
        try:
            result = run_zfs_command(
                ['zpool', 'status', '-s', pool_name],
                check=False,
                timeout=timeout
            )
        except Exception as e:
            return {
                'available': False,
                'pool': pool_name,
                'error': f'Failed to run zpool status -s: {str(e)}'
            }

        if result.returncode != 0:
            return {
                'available': False,
                'pool': pool_name,
                'error': (result.stderr or '').strip() or 'zpool status -s failed'
            }

        parsed = parse_slow_io_status(result.stdout)
        parsed['pool'] = parsed.get('pool') or pool_name
        parsed['timestamp'] = datetime.now().isoformat()
        if not parsed['available']:
            parsed['error'] = 'Slow I/O column not available from zpool status -s'
        return parsed

    def get_latency_histogram(
        self,
        pool_name: Optional[str] = None,
        sample_seconds: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Return per-pool latency histograms from ``zpool iostat -w``.

        ``-w`` prints a histogram of I/O latency bucketed from nanoseconds to
        seconds, grouped by ``total_wait``/``disk_wait``/``syncq_wait``/
        ``asyncq_wait``. This is different from the ``-l`` latency averages
        already shown on the pool iostat page.

        Without ``sample_seconds`` the counts are cumulative since the pool
        was imported. With ``sample_seconds`` two cumulative snapshots are
        taken that many seconds apart and subtracted, which gives the exact
        number of I/Os completed in the window (see
        :func:`subtract_latency_histogram`). The caller blocks for the window.

        Args:
            pool_name: Optional pool to restrict the histogram to.
            sample_seconds: Optional sample window, one of
                ``LATENCY_SAMPLE_SECONDS``.

        Returns:
            The parsed histogram plus ``mode`` (``cumulative`` or ``sample``),
            ``sample_seconds``, ``has_activity`` and ``timestamp``, or
            ``available=False`` with an ``error``.
        """
        if not get_capabilities().latency_histogram:
            return self._histogram_error(pool_name, 'Latency histograms are not supported on this platform')

        if pool_name:
            try:
                ZFSPoolService.validate_pool_name(pool_name)
            except ValueError as e:
                return self._histogram_error(pool_name, str(e))

        if sample_seconds is not None and sample_seconds not in LATENCY_SAMPLE_SECONDS:
            return self._histogram_error(pool_name, f'Invalid sample window: {sample_seconds} seconds')

        first = self._read_latency_histogram(pool_name)
        if 'error' in first:
            return self._histogram_error(pool_name, first['error'])

        if sample_seconds:
            time.sleep(sample_seconds)
            second = self._read_latency_histogram(pool_name)
            if 'error' in second:
                return self._histogram_error(pool_name, second['error'])
            parsed = subtract_latency_histogram(first, second)
            parsed['mode'] = 'sample'
        else:
            parsed = first
            parsed['mode'] = 'cumulative'

        parsed['pool'] = pool_name
        parsed['sample_seconds'] = sample_seconds
        parsed['has_activity'] = histogram_has_activity(parsed)
        parsed['timestamp'] = datetime.now().isoformat()
        if not parsed['available']:
            parsed['error'] = 'Latency histogram not available from zpool iostat -w'
        return parsed

    @staticmethod
    def _histogram_error(pool_name: Optional[str], message: str) -> Dict[str, Any]:
        """Build the ``available=False`` result for the latency histogram."""
        return {
            'available': False,
            'pool': pool_name,
            'pools': {},
            'error': message,
        }

    def _read_latency_histogram(self, pool_name: Optional[str]) -> Dict[str, Any]:
        """Run ``zpool iostat -w -p`` once and parse the cumulative counts.

        ``-p`` prints exact integers instead of ``4.82K``. With no interval
        argument ``zpool`` prints the counts since the pool was imported.
        The pool name must already be validated.

        Returns:
            The :func:`parse_latency_histogram` result, or ``{'error': str}``.
        """
        timeout = self.timeouts.get('iostat', self.timeouts['default'])
        cmd = ['zpool', 'iostat', '-w', '-p']
        if pool_name:
            cmd.append(pool_name)

        try:
            result = run_zfs_command(cmd, check=False, timeout=timeout)
        except subprocess.TimeoutExpired:
            return {'error': f'zpool iostat -w timed out after {timeout} seconds'}
        except Exception as e:
            return {'error': f'Failed to run zpool iostat -w: {str(e)}'}

        if result.returncode != 0:
            return {'error': (result.stderr or '').strip() or 'zpool iostat -w failed'}

        return parse_latency_histogram(result.stdout)
