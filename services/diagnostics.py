"""
Pool Diagnostics Collection Service

Gathers diagnostic information for faulted/suspended pools and
packages it into a downloadable zip file for troubleshooting.
"""
import io
import os
import zipfile
import subprocess
from datetime import datetime
from typing import Optional

from services.utils import (
    run_zfs_command, run_privileged_command,
    is_linux, is_freebsd, is_netbsd
)


def _safe_command(cmd: list[str], timeout: float = 30,
                  use_zfs: bool = False) -> str:
    """Run a command and return stdout, or an error message on failure."""
    try:
        if use_zfs:
            result = run_zfs_command(cmd, check=False, timeout=timeout)
        else:
            result = run_privileged_command(cmd, check=False, timeout=timeout)
        output = ""
        if result.stdout:
            output += result.stdout
        if result.returncode != 0 and result.stderr:
            output += f"\n--- stderr (exit code {result.returncode}) ---\n"
            output += result.stderr
        return output.strip() if output.strip() else "(no output)"
    except subprocess.TimeoutExpired:
        return f"(command timed out after {timeout}s)"
    except FileNotFoundError:
        return f"(command not found: {cmd[0]})"
    except Exception as exc:
        return f"(error running command: {exc})"


def _read_file_safe(path: str, max_lines: int = 500) -> str:
    """Read a file directly or via sudo, returning last N lines."""
    # Try direct read first
    try:
        if os.path.exists(path) and os.access(path, os.R_OK):
            with open(path, 'r', errors='replace') as fh:
                lines = fh.readlines()
                tail = lines[-max_lines:] if len(lines) > max_lines else lines
                return "".join(tail).strip()
    except Exception:
        pass

    # Fallback to sudo tail
    try:
        result = run_privileged_command(
            ['tail', '-n', str(max_lines), path],
            check=False, timeout=10
        )
        if result.returncode == 0 and result.stdout:
            return result.stdout.strip()
    except Exception:
        pass

    return f"(unable to read {path})"


def _get_syslog_path() -> str:
    """Determine the system log file path based on platform."""
    if is_freebsd() or is_netbsd():
        return "/var/log/messages"
    # Linux - check both locations
    if os.path.exists("/var/log/syslog"):
        return "/var/log/syslog"
    return "/var/log/messages"


def _get_zfs_debug_log() -> str:
    """Read the ZFS kernel debug log."""
    if is_linux():
        dbgmsg_path = "/proc/spl/kstat/zfs/dbgmsg"
        if os.path.exists(dbgmsg_path):
            return _read_file_safe(dbgmsg_path, max_lines=1000)
        return "(ZFS debug log not available at /proc/spl/kstat/zfs/dbgmsg)"

    # BSD - try sysctl
    return _safe_command(['sysctl', 'kstat.zfs.misc.dbgmsg'], timeout=5)


def _get_dmesg_zfs() -> str:
    """Get dmesg entries related to ZFS."""
    try:
        result = run_privileged_command(
            ['dmesg'], check=False, timeout=10
        )
        if result.returncode == 0 and result.stdout:
            zfs_lines = [
                line for line in result.stdout.splitlines()
                if any(kw in line.lower() for kw in ['zfs', 'zpool', 'spl', 'zio'])
            ]
            if zfs_lines:
                return "\n".join(zfs_lines[-500:])
            return "(no ZFS-related entries found in dmesg)"
        return "(dmesg returned no output)"
    except Exception as exc:
        return f"(error reading dmesg: {exc})"


def _get_syslog_zfs() -> str:
    """Get syslog entries related to ZFS."""
    syslog_path = _get_syslog_path()
    content = _read_file_safe(syslog_path, max_lines=2000)
    if content.startswith("(unable to read"):
        return content

    zfs_lines = [
        line for line in content.splitlines()
        if any(kw in line.lower() for kw in ['zfs', 'zpool', 'spl', 'zio', 'zed'])
    ]
    if zfs_lines:
        return "\n".join(zfs_lines[-500:])
    return "(no ZFS-related entries found in system log)"


def collect_pool_diagnostics(pool_name: str) -> bytes:
    """
    Collect diagnostic information for a pool and return as zip bytes.

    Gathers:
    - zpool status -v (verbose status with error details)
    - zpool history (command history)
    - zpool events (if available)
    - zpool iostat (I/O statistics)
    - zfs list for the pool's datasets
    - ZFS kernel debug log
    - ZFS-related dmesg entries
    - ZFS-related syslog entries

    Args:
        pool_name: Name of the ZFS pool

    Returns:
        Bytes of a zip file containing all collected diagnostics
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

    files = {}

    # Header / summary
    summary_lines = [
        "=" * 72,
        f"Pool Diagnostics: {pool_name}",
        f"Collected: {timestamp}",
        "=" * 72,
        "",
    ]
    files["00_summary.txt"] = "\n".join(summary_lines)

    # 1. zpool status -v
    files["01_zpool_status.txt"] = _safe_command(
        ['zpool', 'status', '-v', pool_name], use_zfs=True
    )

    # 2. zpool history
    files["02_zpool_history.txt"] = _safe_command(
        ['zpool', 'history', pool_name], timeout=15, use_zfs=True
    )

    # 3. zpool events (last 200)
    files["03_zpool_events.txt"] = _safe_command(
        ['zpool', 'events', '-v'], timeout=15, use_zfs=True
    )

    # 4. zpool iostat
    files["04_zpool_iostat.txt"] = _safe_command(
        ['zpool', 'iostat', '-v', pool_name], use_zfs=True
    )

    # 5. zfs list for this pool
    files["05_zfs_list.txt"] = _safe_command(
        ['zfs', 'list', '-r', '-o',
         'name,used,avail,refer,mountpoint,compression,compressratio',
         pool_name],
        use_zfs=True
    )

    # 6. ZFS kernel debug log
    files["06_zfs_debug_log.txt"] = _get_zfs_debug_log()

    # 7. dmesg ZFS entries
    files["07_dmesg_zfs.txt"] = _get_dmesg_zfs()

    # 8. syslog ZFS entries
    files["08_syslog_zfs.txt"] = _get_syslog_zfs()

    # Build the zip file in memory
    buffer = io.BytesIO()
    prefix = f"{pool_name}_diagnostics_{timestamp_file}"
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        for filename, content in files.items():
            zf.writestr(f"{prefix}/{filename}", content)

    buffer.seek(0)
    return buffer.getvalue()
