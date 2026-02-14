import platform
import subprocess
from typing import Any

import humanize
import psutil

from services.utils import is_freebsd, run_command
from services.zfs_pool import ZFSPoolService


def _get_zfs_version() -> dict[str, str]:
    """Get ZFS version information (userland and module/kmod)"""
    try:
        output = run_command(['zfs', 'version'], check=False)
        lines = [line.strip() for line in output.strip().split('\n') if line.strip()]
        
        zfs_info = {}
        if len(lines) >= 1:
            zfs_info['ZFS Userland'] = lines[0]
        if len(lines) >= 2:
            zfs_info['ZFS Module'] = lines[1]
        
        return zfs_info
    except Exception as e:
        return {'ZFS Version': f'Unable to retrieve: {str(e)}'}


def _get_cpu_info() -> str:
    """Get CPU model name"""
    if is_freebsd():
        # FreeBSD uses sysctl
        try:
            result = subprocess.run(
                ['sysctl', '-n', 'hw.model'],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        except Exception:
            pass
    else:
        # Linux reads /proc/cpuinfo (default)
        try:
            with open('/proc/cpuinfo', 'r') as f:
                for line in f:
                    if line.startswith('model name'):
                        return line.split(':', 1)[1].strip()
        except Exception:
            pass
    
    # Fallback to platform methods
    processor = platform.processor()
    if processor:
        return processor
    return platform.machine()


def _get_pool_info() -> list[dict[str, Any]]:
    """Get ZFS pool information"""
    try:
        pool_service = ZFSPoolService()
        pools = pool_service.list_pools()
        
        pool_info = []
        for pool in pools:
            pool_info.append({
                "Pool Name": pool['name'],
                "Size": pool['size'],
                "Allocated": pool['alloc'],
                "Free": pool['free'],
                "Capacity": pool['cap'],
                "Health": pool['health'],
                "Fragmentation": pool['frag'],
            })
        
        return pool_info
    except Exception as e:
        return [{"Error": f"Unable to retrieve pool info: {str(e)}"}]


def _get_memory_info() -> dict[str, Any]:
    memory = psutil.virtual_memory()

    return {
        "Total Memory": humanize.naturalsize(memory.total),
        "Available Memory": humanize.naturalsize(memory.available),
        "Used Memory": humanize.naturalsize(memory.used),
        "Percentage Used": f"{memory.percent} %",
    }


def _get_system_load() -> dict[str, Any]:
    """Get system load information"""
    try:
        # Get load averages (1, 5, 15 minute averages)
        load_avg = psutil.getloadavg()
        
        # Get CPU percent (current)
        cpu_percent = psutil.cpu_percent(interval=0.1)
        
        # Get CPU count
        cpu_count = psutil.cpu_count()
        
        return {
            "CPU Usage": f"{cpu_percent}%",
            "Load Average (1m)": f"{load_avg[0]:.2f}",
            "Load Average (5m)": f"{load_avg[1]:.2f}",
            "Load Average (15m)": f"{load_avg[2]:.2f}",
            "CPU Count": cpu_count,
        }
    except Exception as e:
        return {"Error": f"Unable to retrieve system load: {str(e)}"}


def get_system_load_stats() -> dict[str, Any]:
    """Public function to get system load statistics."""
    return _get_system_load()


def get_pool_stats() -> list[dict[str, Any]]:
    """Public function to get ZFS pool statistics."""
    return _get_pool_info()


def get_dashboard_context() -> dict[str, Any]:
    # Get ZFS version information
    zfs_version = _get_zfs_version()
    
    # Build platform info in the desired order
    platform_info = {
        "Network Name": platform.node(),
        "Processor": _get_cpu_info(),
        "System": platform.platform(),
    }
    
    # Add ZFS version information in the desired order
    if 'ZFS Module' in zfs_version:
        platform_info['ZFS Module'] = zfs_version['ZFS Module']
    if 'ZFS Userland' in zfs_version:
        platform_info['ZFS Userland'] = zfs_version['ZFS Userland']
    
    # Handle error case
    if 'ZFS Version' in zfs_version:
        platform_info['ZFS Version'] = zfs_version['ZFS Version']
    
    return {
        "platform": platform_info,
        "pools": _get_pool_info(),
        "memory": _get_memory_info(),
        "system_load": _get_system_load(),
    }
