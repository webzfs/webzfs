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

from services.utils import is_freebsd, is_netbsd, run_command, run_zfs_command
from services.zfs_pool import ZFSPoolService
from services.zfs_performance import ZFSPerformanceService


# Prime the CPU times percent cache so the first poll returns meaningful data
psutil.cpu_times_percent(interval=0)


# ---------------------------------------------------------------------------
# Static system information (does not change between requests)
# ---------------------------------------------------------------------------


def _get_zfs_version() -> dict[str, str]:
    """Get ZFS version information (userland and module/kmod)."""
    try:
        output = run_command(['zfs', 'version'], check=False)
        lines = [
            line.strip()
            for line in output.strip().split('\n')
            if line.strip()
        ]
        info = {}
        if len(lines) >= 1:
            info['userland'] = lines[0]
        if len(lines) >= 2:
            info['module'] = lines[1]
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

    return {
        'memory': memory,
        'system_load': system_load,
        'cpu_pct': cpu_pct,
        'uptime': uptime,
        'tasks': tasks,
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


def get_pool_info_extended() -> list[dict[str, Any]]:
    """
    Return pool information enriched with dataset/snapshot counts,
    vdev/disk counts, and error totals.
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

        try:
            result = run_zfs_command(
                ['zpool', 'status', name],
                check=False, timeout=15,
            )
            if result.returncode == 0:
                pool.update(_parse_pool_status_counts(result.stdout, name))
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

        return summary
    except Exception as exc:
        return {'error': str(exc)}


# ---------------------------------------------------------------------------
# Scrub status
# ---------------------------------------------------------------------------


def _check_scrub_cron() -> bool:
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
    """Parse scrub information from ``zpool status`` output."""
    info: dict[str, Any] = {
        'last_scrub': None,
        'last_scrub_human': None,
        'scrub_within_30_days': False,
        'scrub_in_progress': False,
        'scrub_progress': None,
        'scan_line': None,
    }

    for line in status_output.split('\n'):
        stripped = line.strip()

        if 'scrub in progress' in stripped.lower():
            info['scrub_in_progress'] = True
            info['scan_line'] = stripped
            pct_match = re.search(r'(\d+\.?\d*)%\s+done', stripped)
            if pct_match:
                info['scrub_progress'] = float(pct_match.group(1))

        if (
            'scan:' in stripped.lower() or 'scrub' in stripped.lower()
        ) and 'repaired' in stripped.lower():
            info['scan_line'] = stripped

            date_match = re.search(
                r'on\s+(\w+\s+\w+\s+\d+\s+[\d:]+\s+\d{4})', stripped,
            )
            if date_match:
                try:
                    scrub_date = datetime.strptime(
                        date_match.group(1), '%a %b %d %H:%M:%S %Y',
                    )
                    info['last_scrub'] = scrub_date.isoformat()
                    days_ago = (datetime.now() - scrub_date).days
                    info['scrub_within_30_days'] = days_ago <= 30
                    info['last_scrub_human'] = humanize.naturaltime(scrub_date)
                except ValueError:
                    pass

            iso_match = re.search(
                r'(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})', stripped,
            )
            if iso_match and info['last_scrub'] is None:
                try:
                    date_str = iso_match.group(1).replace('T', ' ')
                    scrub_date = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
                    info['last_scrub'] = scrub_date.isoformat()
                    days_ago = (datetime.now() - scrub_date).days
                    info['scrub_within_30_days'] = days_ago <= 30
                    info['last_scrub_human'] = humanize.naturaltime(scrub_date)
                except ValueError:
                    pass

        if 'no scans' in stripped.lower() or 'none requested' in stripped.lower():
            info['scan_line'] = stripped

    return info


def get_scrub_status_all() -> dict[str, Any]:
    """Return scrub status for every pool."""
    status: dict[str, Any] = {
        'pools': [],
        'scrub_cron_configured': _check_scrub_cron(),
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
                'scrub_progress': None,
                'scan_line': None,
            }

            try:
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
# Backward-compatible public helpers
# ---------------------------------------------------------------------------


def get_memory_stats() -> dict[str, Any]:
    """Public function to get memory statistics."""
    return get_realtime_system_data()['memory']


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
