"""
Dashboard Service
Provides system and ZFS overview data for the dashboard.
"""
import platform
import re
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import humanize
import psutil

from services.utils import (
    is_freebsd, is_linux, is_netbsd, run_command, run_zfs_command,
)
from services.scan_status import errors_section_has_errors, parse_scan
from services.zfs_pool import ZFSPoolService
from services.zfs_performance import ZFSPerformanceService


# Prime the CPU times percent cache so the first poll returns meaningful data
psutil.cpu_times_percent(interval=0)


# ---------------------------------------------------------------------------
# Static system information (does not change between requests)
# ---------------------------------------------------------------------------


def _get_zfs_version() -> dict[str, str]:
    """Get ZFS version information (userland and module/kmod).

    The raw strings are returned for display (for example ``zfs-2.2.6-1``).
    Lines are classified by their ``zfs-kmod-`` prefix rather than by position,
    so unexpected or reordered output cannot label the module line as userland.
    """
    try:
        output = run_command(['zfs', 'version'], check=False)
        info = {}
        for raw_line in output.splitlines():
            line = raw_line.strip()
            if not line.startswith('zfs-'):
                continue
            if line.startswith('zfs-kmod-'):
                info.setdefault('module', line)
            else:
                info.setdefault('userland', line)
        return info
    except Exception:
        return {}


def _get_cpu_info() -> str:
    """Get CPU model name."""
    if is_freebsd():
        try:
            result = subprocess.run(
                ['sysctl', '-n', 'hw.model'],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
    else:
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if line.startswith('model name'):
                        return line.split(':', 1)[1].strip()
        except Exception:
            pass

    processor = platform.processor()
    if processor:
        return processor
    return platform.machine()


def _get_os_release() -> str:
    """Read PRETTY_NAME from /etc/os-release, falling back to platform info."""
    try:
        with open('/etc/os-release', 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('PRETTY_NAME='):
                    value = line.split('=', 1)[1]
                    return value.strip('"').strip("'")
    except (FileNotFoundError, PermissionError):
        pass
    return ''


def get_system_specs() -> dict[str, Any]:
    """Return static system specs: hostname, CPU, platform, ZFS version, OS release."""
    zfs_version = _get_zfs_version()
    return {
        'hostname': platform.node(),
        'cpu_model': _get_cpu_info(),
        'cpu_count': psutil.cpu_count(),
        'cpu_count_physical': psutil.cpu_count(logical=False),
        'platform': platform.platform(),
        'os_release': _get_os_release(),
        'zfs_version': zfs_version,
    }


# ---------------------------------------------------------------------------
# Realtime system data (polled every second)
# ---------------------------------------------------------------------------


def _get_uptime() -> dict[str, Any]:
    """Return structured uptime data."""
    boot_time = psutil.boot_time()
    uptime_seconds = int(time.time() - boot_time)
    delta = timedelta(seconds=uptime_seconds)

    days = delta.days
    hours, remainder = divmod(delta.seconds, 3600)
    minutes, _ = divmod(remainder, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    parts.append(f"{minutes}m")

    return {
        'formatted': ' '.join(parts),
        'days': days,
        'hours': hours,
        'minutes': minutes,
        'total_seconds': uptime_seconds,
    }


def _get_cpu_temperature() -> Optional[dict[str, Any]]:
    """
    Return the current CPU temperature (in Celsius) and sensor label.

    Returns ``None`` when no temperature data can be obtained.

    Supports Linux (via ``psutil.sensors_temperatures``) and FreeBSD
    (via ``sysctl dev.cpu.0.temperature``).  NetBSD generally does not
    expose CPU temperature, so ``None`` is returned there.
    """
    # FreeBSD: sysctl dev.cpu.0.temperature returns "45.0C"
    if is_freebsd():
        try:
            result = subprocess.run(
                ['sysctl', '-n', 'dev.cpu.0.temperature'],
                capture_output=True, text=True, timeout=3,
            )
            if result.returncode == 0 and result.stdout.strip():
                raw = result.stdout.strip().rstrip('C').strip()
                temp_c = float(raw)
                return {
                    'current': round(temp_c, 1),
                    'label': 'CPU 0',
                    'high': None,
                    'critical': None,
                }
        except Exception:
            pass
        return None

    # Linux (and any platform supported by psutil sensors_temperatures)
    try:
        sensors_func = getattr(psutil, 'sensors_temperatures', None)
        if sensors_func is None:
            return None
        temps = sensors_func()
        if not temps:
            return None
    except Exception:
        return None

    # Preferred sensor chip names for CPU temperature, in priority order.
    preferred_chips = [
        'coretemp',      # Intel
        'k10temp',       # AMD modern
        'k8temp',        # AMD older
        'zenpower',      # AMD Ryzen alternative driver
        'cpu_thermal',   # ARM SoC
        'cpu-thermal',
        'acpitz',        # ACPI thermal zone fallback
    ]

    def _pick_entry(entries):
        """Pick the most representative sensor entry from a chip's list."""
        if not entries:
            return None
        # Prefer "Package", "Tdie", "Tctl" labels when present.
        priority_labels = ('package', 'tdie', 'tctl', 'cpu')
        for label_match in priority_labels:
            for entry in entries:
                if entry.label and label_match in entry.label.lower():
                    return entry
        return entries[0]

    for chip in preferred_chips:
        if chip in temps:
            entry = _pick_entry(temps[chip])
            if entry and entry.current is not None:
                return {
                    'current': round(float(entry.current), 1),
                    'label': entry.label or chip,
                    'high': entry.high,
                    'critical': entry.critical,
                }

    # Fallback: take the first available chip with a usable reading.
    for chip_name, entries in temps.items():
        entry = _pick_entry(entries)
        if entry and entry.current is not None:
            return {
                'current': round(float(entry.current), 1),
                'label': entry.label or chip_name,
                'high': entry.high,
                'critical': entry.critical,
            }

    return None


def _get_task_summary() -> dict[str, int]:
    """Return lightweight task/thread counts."""
    counts = {
        'total': 0,
        'running': 0,
        'sleeping': 0,
        'stopped': 0,
        'zombie': 0,
        'threads': 0,
    }
    try:
        for proc in psutil.process_iter(['status', 'num_threads']):
            try:
                counts['total'] += 1
                threads = proc.info.get('num_threads')
                if threads:
                    counts['threads'] += threads

                status = proc.info['status']
                if status == psutil.STATUS_RUNNING:
                    counts['running'] += 1
                elif status in (
                    psutil.STATUS_SLEEPING,
                    psutil.STATUS_IDLE,
                    psutil.STATUS_DISK_SLEEP,
                ):
                    counts['sleeping'] += 1
                elif status == psutil.STATUS_STOPPED:
                    counts['stopped'] += 1
                elif status == psutil.STATUS_ZOMBIE:
                    counts['zombie'] += 1
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception:
        pass
    return counts


def get_realtime_system_data() -> dict[str, Any]:
    """
    Collect all fast-changing system metrics in a single call.

    Intended to be polled every second via HTMX.  Every call is lightweight --
    no subprocess spawning, just psutil queries.
    """
    # --- Memory ---
    mem = psutil.virtual_memory()
    memory: dict[str, Any] = {
        'total': mem.total,
        'available': mem.available,
        'used': mem.used,
        'percent': mem.percent,
        'total_human': humanize.naturalsize(mem.total),
        'available_human': humanize.naturalsize(mem.available),
        'used_human': humanize.naturalsize(mem.used),
    }

    if hasattr(mem, 'buffers') and hasattr(mem, 'cached'):
        buff_cache = mem.buffers + mem.cached
        memory['buffers'] = mem.buffers
        memory['cached'] = mem.cached
        memory['buff_cache'] = buff_cache
        memory['buff_cache_human'] = humanize.naturalsize(buff_cache)
        memory['used_percent'] = round((mem.used / mem.total) * 100, 1) if mem.total else 0
        memory['buff_cache_percent'] = round((buff_cache / mem.total) * 100, 1) if mem.total else 0
        memory['available_percent'] = round((mem.available / mem.total) * 100, 1) if mem.total else 0
    else:
        memory['buff_cache'] = 0
        memory['buff_cache_human'] = '0 B'
        memory['used_percent'] = round((mem.used / mem.total) * 100, 1) if mem.total else 0
        memory['buff_cache_percent'] = 0
        memory['available_percent'] = round((mem.available / mem.total) * 100, 1) if mem.total else 0

    # --- Swap ---
    swap = psutil.swap_memory()
    memory['swap_total'] = swap.total
    memory['swap_used'] = swap.used
    memory['swap_free'] = swap.free
    memory['swap_percent'] = swap.percent
    memory['swap_total_human'] = humanize.naturalsize(swap.total)
    memory['swap_used_human'] = humanize.naturalsize(swap.used)
    memory['swap_free_human'] = humanize.naturalsize(swap.free)

    # --- System load ---
    load_avg = psutil.getloadavg()
    cpu_count = psutil.cpu_count() or 1
    system_load = {
        'load_1m': round(load_avg[0], 2),
        'load_5m': round(load_avg[1], 2),
        'load_15m': round(load_avg[2], 2),
        'cpu_count': cpu_count,
        'load_1m_pct': min(round((load_avg[0] / cpu_count) * 100, 1), 100),
        'load_5m_pct': min(round((load_avg[1] / cpu_count) * 100, 1), 100),
        'load_15m_pct': min(round((load_avg[2] / cpu_count) * 100, 1), 100),
    }

    # --- CPU time percentages ---
    ct = psutil.cpu_times_percent(interval=0)
    cpu_pct = {
        'user': getattr(ct, 'user', 0.0),
        'system': getattr(ct, 'system', 0.0),
        'nice': getattr(ct, 'nice', 0.0),
        'idle': getattr(ct, 'idle', 0.0),
        'iowait': getattr(ct, 'iowait', 0.0),
        'irq': getattr(ct, 'irq', 0.0),
        'softirq': getattr(ct, 'softirq', 0.0),
        'steal': getattr(ct, 'steal', 0.0),
    }

    # --- Uptime ---
    uptime = _get_uptime()

    # --- Tasks / threads ---
    tasks = _get_task_summary()

    # --- CPU temperature ---
    cpu_temp = _get_cpu_temperature()

    return {
        'memory': memory,
        'system_load': system_load,
        'cpu_pct': cpu_pct,
        'uptime': uptime,
        'tasks': tasks,
        'cpu_temp': cpu_temp,
    }


# ---------------------------------------------------------------------------
# Extended pool information
# ---------------------------------------------------------------------------


def _safe_int(value: str) -> int:
    """Convert string to int, returning 0 on failure."""
    try:
        cleaned = re.sub(r'[^0-9]', '', value)
        return int(cleaned) if cleaned else 0
    except (ValueError, TypeError):
        return 0


def _parse_pool_status_counts(
    status_output: str, pool_name: str,
) -> dict[str, Any]:
    """Parse ``zpool status`` to count vdevs, disks, and errors."""
    counts: dict[str, Any] = {
        'vdev_count': 0,
        'disk_count': 0,
        'read_errors': 0,
        'write_errors': 0,
        'cksum_errors': 0,
    }

    vdev_keywords = {
        'mirror', 'raidz', 'raidz1', 'raidz2', 'raidz3',
        'spare', 'cache', 'log', 'dedup', 'special',
    }

    in_config = False
    for line in status_output.split('\n'):
        stripped = line.strip()

        if stripped.lower().startswith('config:'):
            in_config = True
            continue

        if in_config and stripped.lower().startswith('errors:'):
            in_config = False
            continue

        if not in_config:
            continue

        if 'NAME' in stripped and 'STATE' in stripped:
            continue

        parts = stripped.split()
        if not parts:
            continue

        name = parts[0]
        if name == pool_name:
            continue

        read_err = _safe_int(parts[2]) if len(parts) > 2 else 0
        write_err = _safe_int(parts[3]) if len(parts) > 3 else 0
        cksum_err = _safe_int(parts[4]) if len(parts) > 4 else 0

        base_name = re.sub(r'-\d+$', '', name.lower())
        if base_name in vdev_keywords:
            counts['vdev_count'] += 1
        else:
            counts['disk_count'] += 1
            counts['read_errors'] += read_err
            counts['write_errors'] += write_err
            counts['cksum_errors'] += cksum_err

    return counts


def _parse_status_sections(status_output: str) -> dict[str, str]:
    """Extract the ``status:``, ``action:`` and ``errors:`` text of one pool.

    ``zpool status`` wraps long ``status:`` and ``action:`` text onto
    tab-indented continuation lines; those are joined with single spaces.
    """
    sections = {'status': '', 'action': '', 'errors': ''}
    current = None
    for line in status_output.splitlines():
        match = re.match(r'^\s*([a-z]+):\s*(.*)$', line)
        if match:
            label = match.group(1)
            if label in sections and not sections[label]:
                current = label
                sections[label] = match.group(2).strip()
            else:
                current = None
            continue
        if current and line.startswith('\t') and line.strip():
            sections[current] = f"{sections[current]} {line.strip()}".strip()
        else:
            current = None

    errors_text = sections['errors']
    return {
        'status_text': sections['status'],
        'action_text': sections['action'],
        'errors_text': errors_text,
        'data_errors': bool(errors_text)
        and errors_text.lower() != 'no known data errors',
    }


def _parse_percent(value: Any) -> Optional[int]:
    """Convert a value such as ``12%`` to an int, or None when not numeric."""
    try:
        return int(str(value).strip().rstrip('%'))
    except (ValueError, TypeError):
        return None


def build_pool_alerts(pools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return the conditions that need attention, for the dashboard strip.

    One entry per condition: a pool that is not ONLINE, a pool with data
    errors, and a resilver in progress. A healthy system returns an empty list.
    """
    faulted_states = ('FAULTED', 'UNAVAIL', 'REMOVED', 'SUSPENDED')
    alerts: list[dict[str, Any]] = []

    for pool in pools:
        name = pool.get('name', '')
        health = pool.get('health') or 'UNKNOWN'
        status_text = pool.get('status_text', '')
        action_text = pool.get('action_text', '')
        status_shown = False

        if health != 'ONLINE':
            alerts.append({
                'level': 'danger' if health in faulted_states else 'warning',
                'pool': name,
                'title': f"pool is {health}",
                'detail': status_text,
                'action': action_text,
            })
            status_shown = True

        if pool.get('data_errors'):
            alerts.append({
                'level': 'danger',
                'pool': name,
                'title': f"data errors: {pool.get('errors_text', '')}",
                'detail': '' if status_shown else status_text,
                'action': '' if status_shown else action_text,
            })

        if pool.get('resilver_in_progress'):
            parts = ['resilver in progress']
            if pool.get('scrub_progress') is not None:
                parts.append(f"{pool['scrub_progress']}% done")
            if pool.get('scan_eta'):
                parts.append(f"{pool['scan_eta']} to go")
            alerts.append({
                'level': 'info',
                'pool': name,
                'title': ', '.join(parts),
                'detail': '',
                'action': '',
            })

    return alerts


def get_pool_info_extended() -> list[dict[str, Any]]:
    """
    Return pool information enriched with dataset/snapshot counts,
    vdev/disk counts, error totals, scan (scrub/resilver) state and the
    ``status:``/``action:``/``errors:`` text from ``zpool status``.
    """
    try:
        pool_service = ZFSPoolService()
        pools = pool_service.list_pools()
    except Exception:
        return []

    # --- Dataset counts per pool ---
    dataset_counts: dict[str, int] = {}
    try:
        result = run_zfs_command(
            ['zfs', 'list', '-H', '-t', 'filesystem', '-o', 'name'],
            check=False, timeout=15,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                name = line.strip()
                if name:
                    pool = name.split('/')[0]
                    dataset_counts[pool] = dataset_counts.get(pool, 0) + 1
    except Exception:
        pass

    # --- Snapshot counts per pool ---
    snapshot_counts: dict[str, int] = {}
    try:
        result = run_zfs_command(
            ['zfs', 'list', '-H', '-t', 'snapshot', '-o', 'name'],
            check=False, timeout=15,
        )
        if result.returncode == 0:
            for line in result.stdout.strip().split('\n'):
                name = line.strip()
                if name:
                    pool = name.split('/')[0].split('@')[0]
                    snapshot_counts[pool] = snapshot_counts.get(pool, 0) + 1
    except Exception:
        pass

    # --- Per-pool vdev / disk / error info ---
    for pool in pools:
        name = pool['name']
        pool['dataset_count'] = dataset_counts.get(name, 0)
        pool['snapshot_count'] = snapshot_counts.get(name, 0)
        pool['vdev_count'] = 0
        pool['disk_count'] = 0
        pool['read_errors'] = 0
        pool['write_errors'] = 0
        pool['cksum_errors'] = 0
        pool['frag_pct'] = _parse_percent(pool.get('frag'))
        pool.update(_parse_scrub_info(''))
        pool.update(_parse_status_sections(''))

        try:
            # No -v: the errors: summary line is enough to detect data
            # errors, and the file list can be long on a damaged pool.
            result = run_zfs_command(
                ['zpool', 'status', name],
                check=False, timeout=15,
            )
            if result.returncode == 0:
                pool.update(_parse_pool_status_counts(result.stdout, name))
                pool.update(_parse_scrub_info(result.stdout))
                pool.update(_parse_status_sections(result.stdout))
        except Exception:
            pass

        # A pool always has at least 1 vdev (even a single-disk stripe)
        if pool['vdev_count'] == 0 and pool['disk_count'] > 0:
            pool['vdev_count'] = 1

    return pools


# ---------------------------------------------------------------------------
# ARC statistics summary
# ---------------------------------------------------------------------------


def get_arc_stats_summary() -> dict[str, Any]:
    """Return ARC statistics summary matching the ARC page visual layout."""
    try:
        perf_service = ZFSPerformanceService()
        stats = perf_service._read_arc_stats()

        if stats.get('error'):
            return {'error': stats['error']}

        summary: dict[str, Any] = {}

        # --- Top row: ARC Size, Hit Rate, Cache Hits, Cache Misses ---
        if 'size' in stats:
            size_gb = stats['size'] / (1024 ** 3)
            summary['arc_size'] = stats['size']
            summary['arc_size_human'] = f"{size_gb:.2f} GB"
        if 'c_max' in stats:
            max_gb = stats['c_max'] / (1024 ** 3)
            summary['arc_max'] = stats['c_max']
            summary['arc_max_human'] = f"{max_gb:.2f} GB"
        if 'size' in stats and 'c_max' in stats and stats['c_max'] > 0:
            summary['arc_size_pct'] = round(
                (stats['size'] / stats['c_max']) * 100, 1,
            )

        if 'hit_rate' in stats:
            summary['hit_rate'] = round(stats['hit_rate'], 2)
        if 'miss_rate' in stats:
            summary['miss_rate'] = round(stats['miss_rate'], 2)

        if 'hits' in stats:
            summary['hits'] = stats['hits']
            summary['hits_human'] = humanize.intcomma(stats['hits'])
        if 'misses' in stats:
            summary['misses'] = stats['misses']
            summary['misses_human'] = humanize.intcomma(stats['misses'])

        # --- Bottom row: Cache Distribution (MRU vs MFU) ---
        mru_hits = stats.get('mru_hits', 0)
        mfu_hits = stats.get('mfu_hits', 0)
        mru_mfu_total = mru_hits + mfu_hits
        summary['mru_hits'] = mru_hits
        summary['mru_hits_human'] = humanize.intcomma(mru_hits)
        summary['mfu_hits'] = mfu_hits
        summary['mfu_hits_human'] = humanize.intcomma(mfu_hits)
        if mru_mfu_total > 0:
            summary['mru_pct'] = round((mru_hits / mru_mfu_total) * 100, 1)
            summary['mfu_pct'] = round((mfu_hits / mru_mfu_total) * 100, 1)
        else:
            summary['mru_pct'] = 0
            summary['mfu_pct'] = 0

        # --- Bottom row: Access Type Breakdown (Demand vs Prefetch) ---
        demand_total = (
            stats.get('demand_data_hits', 0)
            + stats.get('demand_metadata_hits', 0)
        )
        prefetch_total = (
            stats.get('prefetch_data_hits', 0)
            + stats.get('prefetch_metadata_hits', 0)
        )
        access_total = demand_total + prefetch_total
        summary['demand_total'] = demand_total
        summary['demand_total_human'] = humanize.intcomma(demand_total)
        summary['prefetch_total'] = prefetch_total
        summary['prefetch_total_human'] = humanize.intcomma(prefetch_total)
        if access_total > 0:
            summary['demand_pct'] = round(
                (demand_total / access_total) * 100, 1,
            )
            summary['prefetch_pct'] = round(
                (prefetch_total / access_total) * 100, 1,
            )
        else:
            summary['demand_pct'] = 0
            summary['prefetch_pct'] = 0

        # --- Miss & Ghost Rates / Miss Breakdown ---
        total_misses = stats.get('misses', 0)
        mru_ghost = stats.get('mru_ghost_hits', 0)
        mfu_ghost = stats.get('mfu_ghost_hits', 0)
        ghost_total = mru_ghost + mfu_ghost
        cold_misses = total_misses - ghost_total if total_misses >= ghost_total else 0
        total_requests = stats.get('hits', 0) + total_misses

        summary['ghost_total'] = ghost_total
        summary['ghost_total_human'] = humanize.intcomma(ghost_total)
        summary['cold_misses'] = cold_misses
        summary['cold_misses_human'] = humanize.intcomma(cold_misses)

        if total_requests > 0:
            summary['miss_rate_pct'] = round(
                (total_misses / total_requests) * 100, 2,
            )
            summary['ghost_rate_pct'] = round(
                (ghost_total / total_requests) * 100, 2,
            )
        else:
            summary['miss_rate_pct'] = 0
            summary['ghost_rate_pct'] = 0

        if total_misses > 0:
            summary['cold_miss_pct'] = round(
                (cold_misses / total_misses) * 100, 1,
            )
            summary['ghost_miss_pct'] = round(
                (ghost_total / total_misses) * 100, 1,
            )
        else:
            summary['cold_miss_pct'] = 0
            summary['ghost_miss_pct'] = 0

        return summary
    except Exception as exc:
        return {'error': str(exc)}


# ---------------------------------------------------------------------------
# Scrub status
# ---------------------------------------------------------------------------


def check_scrub_cron() -> bool:
    """Check if scrub cron jobs are configured."""
    home = Path.home()
    schedules_file = home / '.config' / 'webzfs' / 'scrub_schedules.json'
    if schedules_file.exists():
        try:
            import json
            with open(schedules_file, 'r') as f:
                data = json.load(f)
            if any(s.get('enabled', False) for s in data.get('schedules', [])):
                return True
        except Exception:
            pass

    cron_paths = [
        '/etc/crontab',
        '/etc/cron.d/zfs-scrub',
        '/etc/cron.d/zfsutils-linux',
    ]
    for path in cron_paths:
        try:
            if Path(path).exists():
                with open(path, 'r') as f:
                    content = f.read()
                if 'scrub' in content.lower() and 'zpool' in content.lower():
                    return True
        except Exception:
            pass

    return False


def _parse_scrub_info(status_output: str) -> dict[str, Any]:
    """Parse scrub information from ``zpool status`` output.

    The ``scan:`` block is delegated to :mod:`services.scan_status`, the single
    parser shared with the pool, performance and health views, so paused scans
    are reported consistently everywhere.
    """
    scan = parse_scan(status_output)
    info: dict[str, Any] = {
        'last_scrub': None,
        'last_scrub_human': None,
        'scrub_within_30_days': False,
        # Scrub-only flags: a running resilver is not reported as a scrub.
        'scrub_in_progress': scan.scrub_in_progress,
        'scrub_paused': scan.scrub_paused,
        'error_scrub': scan.error_scrub,
        # Any scan running or paused (resilver included); nothing else may
        # start while this is true.
        'scan_busy': scan.in_progress or scan.paused,
        'scrub_state': scan.state,
        'scrub_progress': scan.progress,
        'scan_line': scan.raw or None,
        'scan_errors': scan.has_errors or errors_section_has_errors(status_output),
        'scrub_age_days': None,
        'resilver_in_progress': scan.kind == 'resilver' and scan.in_progress,
        'scan_eta': None,
    }

    if scan.in_progress:
        eta_match = re.search(r'((?:\d+ days? )?\d+:\d{2}:\d{2}) to go', scan.raw)
        if eta_match:
            info['scan_eta'] = eta_match.group(1)

    if scan.last_scrub is not None:
        info['last_scrub'] = scan.last_scrub.isoformat()
        days_ago = (datetime.now() - scan.last_scrub).days
        info['scrub_within_30_days'] = days_ago <= 30
        info['scrub_age_days'] = days_ago
        # Exact day counts up to 30 days, so the label never rounds to
        # "a month ago" while the age is still inside the 30 day window.
        if 1 <= days_ago <= 30:
            unit = 'day' if days_ago == 1 else 'days'
            info['last_scrub_human'] = f"{days_ago} {unit} ago"
        else:
            info['last_scrub_human'] = humanize.naturaltime(scan.last_scrub)

    if not scan.present:
        for line in status_output.split('\n'):
            stripped = line.strip()
            if 'no scans' in stripped.lower() or 'none requested' in stripped.lower():
                info['scan_line'] = stripped
                break

    return info


def get_scrub_status_all() -> dict[str, Any]:
    """Return scrub status for every pool."""
    status: dict[str, Any] = {
        'pools': [],
        'scrub_cron_configured': check_scrub_cron(),
    }

    try:
        result = run_zfs_command(
            ['zpool', 'list', '-H', '-o', 'name'],
            timeout=15, check=False,
        )
        if result.returncode != 0:
            return status

        for pool_name in result.stdout.strip().split('\n'):
            pool_name = pool_name.strip()
            if not pool_name:
                continue

            pool_scrub: dict[str, Any] = {
                'name': pool_name,
                'last_scrub': None,
                'last_scrub_human': None,
                'scrub_within_30_days': False,
                'scrub_in_progress': False,
                'scrub_paused': False,
                'error_scrub': False,
                'scan_busy': False,
                'scrub_state': 'none',
                'scrub_progress': None,
                'scan_line': None,
                'scan_errors': False,
            }

            try:
                # No -v: the errors: summary line is enough to detect data
                # errors, and the file list can be long on a damaged pool.
                res = run_zfs_command(
                    ['zpool', 'status', pool_name],
                    timeout=15, check=False,
                )
                if res.returncode == 0:
                    pool_scrub.update(_parse_scrub_info(res.stdout))
            except Exception:
                pass

            status['pools'].append(pool_scrub)
    except Exception:
        pass

    return status


# ---------------------------------------------------------------------------
# Disk health summary (from the latest saved health report)
# ---------------------------------------------------------------------------


def _parse_iso(value: Any) -> Optional[datetime]:
    """Parse an ISO timestamp, returning None when missing or malformed."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except ValueError:
        return None


def get_disk_health_summary() -> Optional[dict[str, Any]]:
    """Return disk counts from the newest completed health report.

    Reads the saved report only; no disk is probed. Returns None when no
    completed report with disk health results exists.
    """
    try:
        from services.health_analysis import HealthAnalysisService
        reports = HealthAnalysisService().list_reports(limit=20)
    except Exception:
        return None

    for report in reports:
        summary = report.get('summary') or {}
        if report.get('status') != 'completed' or not summary.get('total_disks'):
            continue
        completed = _parse_iso(report.get('completed_at'))
        return {
            'report_id': report.get('id'),
            'total': summary.get('total_disks', 0),
            'healthy': summary.get('healthy_disks', 0),
            'warning': summary.get('warning_disks', 0),
            'critical': summary.get('critical_disks', 0),
            'age_human': humanize.naturaltime(completed) if completed else None,
            'stale': bool(completed)
            and (datetime.now() - completed).days > 30,
        }
    return None


# ---------------------------------------------------------------------------
# Failed jobs and monitoring services
# ---------------------------------------------------------------------------

FAILED_JOB_WINDOW_HOURS = 24

SMARTD_BINARY_PATHS = (
    '/usr/sbin/smartd',
    '/usr/local/sbin/smartd',
    '/usr/pkg/sbin/smartd',
    '/sbin/smartd',
    '/usr/bin/smartd',
)


def _first_line(text: Any, max_length: int = 160) -> str:
    """Return the first non-empty line of a message, shortened for display."""
    for line in str(text or '').splitlines():
        line = line.strip()
        if line:
            if len(line) > max_length:
                return line[:max_length - 3] + '...'
            return line
    return ''


def _time_ago(value: Any) -> str:
    """Return a relative time such as "3 hours ago" for an ISO timestamp."""
    parsed = _parse_iso(value)
    return humanize.naturaltime(parsed) if parsed else ''


def get_failed_jobs() -> list[dict[str, Any]]:
    """Return failed replication runs and scheduled tasks that last failed.

    Replication runs (native and Syncoid) are taken from the execution history
    and limited to the last FAILED_JOB_WINDOW_HOURS hours. Scheduled tasks
    (Syncoid, scrub, SMART test, health check) are listed when they are enabled
    and their most recent run did not succeed, whatever its age.
    """
    failed: list[dict[str, Any]] = []
    try:
        from services.storage import FileStorageService
        storage = FileStorageService()
    except Exception:
        return failed

    cutoff = datetime.now() - timedelta(hours=FAILED_JOB_WINDOW_HOURS)
    listed_job_ids = set()

    try:
        for execution in storage.get_execution_history(limit=200):
            if execution.get('status') not in ('failure', 'failed'):
                continue
            finished = execution.get('completed_at') or execution.get('started_at')
            finished_time = _parse_iso(finished)
            if finished_time is None or finished_time < cutoff:
                continue
            name = execution.get('job_name') or (
                f"{execution.get('source_dataset', '')} to "
                f"{execution.get('target_dataset', '')}"
            )
            failed.append({
                'kind': 'Replication',
                'name': name,
                'when': humanize.naturaltime(finished_time),
                'detail': _first_line(execution.get('error_message')),
                'url': f"/zfs/replication/history/{execution.get('id')}",
            })
            if execution.get('job_id'):
                listed_job_ids.add(str(execution['job_id']))
    except Exception:
        pass

    def add_schedule(kind: str, name: str, record: dict[str, Any], url: str) -> None:
        if not record.get('enabled', True):
            return
        last_status = record.get('last_status')
        if last_status not in ('failure', 'failed', 'partial'):
            return
        detail = 'Last scheduled run failed'
        if last_status == 'partial':
            detail = 'Last scheduled run failed on some disks'
        failed.append({
            'kind': kind,
            'name': name,
            'when': _time_ago(record.get('last_run')),
            'detail': detail,
            'url': url,
        })

    try:
        for job in storage.get_syncoid_jobs():
            # Already listed above through its failed execution record.
            if f"syncoid-{job.get('id')}" in listed_job_ids:
                continue
            add_schedule(
                'Syncoid job', job.get('name') or f"job {job.get('id')}",
                job, '/zfs/replication/syncoid',
            )
    except Exception:
        pass

    try:
        for record in storage.get_scrub_schedules():
            add_schedule(
                'Scheduled scrub', f"pool {record.get('pool', 'unknown')}",
                record, '/utils/scheduling/',
            )
    except Exception:
        pass

    try:
        for record in storage.get_health_schedules():
            add_schedule(
                'Health check', record.get('name') or f"schedule {record.get('id')}",
                record, '/utils/scheduling/',
            )
    except Exception:
        pass

    try:
        from services.smart_monitoring import SMARTMonitoringService
        for record in SMARTMonitoringService().list_scheduled_tests():
            add_schedule(
                'SMART test',
                f"{record.get('test_type', 'short')} test on "
                f"{record.get('disk', 'unknown disk')}",
                record, '/utils/scheduling/',
            )
    except Exception:
        pass

    return failed


def get_monitoring_services() -> list[dict[str, Any]]:
    """Return the run state of the daemons that raise disk and pool alerts.

    smartd is reported on every platform when it is installed. ZED is
    reported on Linux only.
    """
    services: list[dict[str, Any]] = []

    smartd_installed = any(Path(path).exists() for path in SMARTD_BINARY_PATHS)
    smartd_running = False
    if smartd_installed:
        try:
            from services.smartd import get_smartd_status
            smartd_running = bool(get_smartd_status().get('running'))
        except Exception:
            smartd_running = False
    services.append({
        'name': 'smartd',
        'description': 'SMART disk monitoring daemon',
        'installed': smartd_installed,
        'running': smartd_running,
        'url': '/utils/smart/smartd',
    })

    if is_linux():
        zed_running = False
        try:
            from services.zed import ZedService
            zed_running, _ = ZedService().get_daemon_status()
        except Exception:
            zed_running = False
        services.append({
            'name': 'ZED',
            'description': 'ZFS Event Daemon',
            'installed': True,
            'running': zed_running,
            'url': '/utils/zed/',
        })

    return services


def get_jobs_services_status() -> dict[str, Any]:
    """Return the data for the dashboard Jobs and Services card."""
    return {
        'failed_jobs': get_failed_jobs(),
        'services': get_monitoring_services(),
        'window_hours': FAILED_JOB_WINDOW_HOURS,
    }


# ---------------------------------------------------------------------------
# Backward-compatible public helpers
# ---------------------------------------------------------------------------


def get_memory_stats() -> dict[str, Any]:
    """Public function to get memory statistics."""
    return get_realtime_system_data()['memory']


def get_memory_stats() -> dict[str, Any]:
    """Public function to get memory statistics."""
    return _get_memory_info()


def get_system_load_stats() -> dict[str, Any]:
    """Public function to get system load statistics."""
    return get_realtime_system_data()['system_load']


def get_pool_stats() -> list[dict[str, Any]]:
    """Public function to get ZFS pool statistics."""
    return get_pool_info_extended()


def get_dashboard_context() -> dict[str, Any]:
    """Return the full dashboard context for the initial page load."""
    specs = get_system_specs()
    realtime = get_realtime_system_data()
    pools = get_pool_info_extended()
    arc_stats = get_arc_stats_summary()
    scrub_status = get_scrub_status_all()

    return {
        'specs': specs,
        'realtime': realtime,
        'pools': pools,
        'arc_stats': arc_stats,
        'scrub_status': scrub_status,
    }
