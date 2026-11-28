"""
smartd Integration Helpers

Single source of truth for where smartd.conf lives on each platform, how it
is read and written, how the smartd service is queried and restarted, and
how the file is matched against a disk. Used by the SMART monitoring
service and the health analysis service so both agree.
"""
import os
import shutil
import subprocess
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.file import read_file, save_file
from services.utils import (
    is_bsd,
    is_freebsd,
    is_linux,
    is_netbsd,
    run_privileged_command,
)

# FreeBSD: the sysutils/smartmontools rc script starts smartd with
# "-c /usr/local/etc/smartd.conf". The package installs only
# smartd.conf.sample; the administrator creates smartd.conf.
SMARTD_CONF_FREEBSD = "/usr/local/etc/smartd.conf"
SMARTD_SAMPLE_FREEBSD = "/usr/local/etc/smartd.conf.sample"

# NetBSD: pkgsrc configures smartmontools with sysconfdir /usr/pkg/etc.
SMARTD_CONF_NETBSD = "/usr/pkg/etc/smartd.conf"
SMARTD_SAMPLE_NETBSD = "/usr/pkg/share/examples/smartmontools/smartd.conf"

# Linux: most distributions use /etc/smartd.conf. The RHEL family keeps
# the file under /etc/smartmontools. The first existing path is used.
SMARTD_CONF_LINUX = "/etc/smartd.conf"
SMARTD_CONF_LINUX_PATHS = [
    SMARTD_CONF_LINUX,
    "/etc/smartmontools/smartd.conf",
]

# Path WebZFS used on every platform before issue 302. On BSD smartd
# never reads it.
SMARTD_CONF_LEGACY = "/etc/smartd.conf"


def get_smartd_config_path() -> Path:
    """Return the smartd.conf path that smartd reads on this platform."""
    if is_freebsd():
        return Path(SMARTD_CONF_FREEBSD)
    if is_netbsd():
        return Path(SMARTD_CONF_NETBSD)
    for candidate in SMARTD_CONF_LINUX_PATHS:
        if os.path.exists(candidate):
            return Path(candidate)
    return Path(SMARTD_CONF_LINUX)


def get_smartd_sample_path() -> Optional[Path]:
    """Return the packaged sample smartd.conf if one is installed."""
    if is_freebsd():
        sample_path = Path(SMARTD_SAMPLE_FREEBSD)
    elif is_netbsd():
        sample_path = Path(SMARTD_SAMPLE_NETBSD)
    else:
        return None
    if sample_path.exists():
        return sample_path
    return None


def get_stale_smartd_config_path() -> Optional[Path]:
    """
    Return /etc/smartd.conf when it exists on a platform where smartd does
    not read it.

    Older WebZFS versions wrote the file there on FreeBSD and NetBSD. A
    link that points at the real config file is not reported.
    """
    config_path = get_smartd_config_path()
    legacy_path = Path(SMARTD_CONF_LEGACY)
    if config_path == legacy_path or not legacy_path.exists():
        return None
    try:
        if config_path.exists() and os.path.samefile(config_path, legacy_path):
            return None
    except OSError:
        pass
    return legacy_path


def read_smartd_config() -> Optional[str]:
    """
    Read smartd.conf.

    Returns:
        The file content, or None when the file does not exist.
    """
    config_path = get_smartd_config_path()
    try:
        return read_file(str(config_path), use_sudo=True)
    except FileNotFoundError:
        return None


def write_smartd_config(content: str) -> None:
    """
    Write smartd.conf.

    Browsers submit textarea content with CRLF line endings, so line
    endings are normalized first. The direct write uses a temporary file
    in the same directory and an atomic rename, so the live file is never
    left half written. If the direct write is not permitted, Linux falls
    back to the sudo write path in services/file.py (the service account
    is not root there).
    """
    content = content.replace("\r\n", "\n").replace("\r", "\n")
    if content and not content.endswith("\n"):
        content += "\n"

    # Write through a link instead of replacing the link itself.
    config_file = os.path.realpath(str(get_smartd_config_path()))
    temp_file = config_file + ".webzfs.tmp"
    try:
        with open(temp_file, "w") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())
        if os.path.exists(config_file):
            shutil.copymode(config_file, temp_file)
        os.replace(temp_file, config_file)
        return
    except OSError as write_error:
        direct_error = write_error
        try:
            os.unlink(temp_file)
        except OSError:
            pass

    if not is_linux():
        raise Exception(f"Cannot write {config_file}: {direct_error}")
    try:
        save_file(config_file, content, use_sudo=True)
    except subprocess.CalledProcessError as sudo_error:
        raise Exception(f"Cannot write {config_file}: {sudo_error}")


def get_smartd_status() -> Dict[str, Any]:
    """
    Get the smartd daemon status.

    FreeBSD and NetBSD use the rc.d service command. Linux uses systemctl
    and falls back to the service command on hosts without systemd.
    """
    if not is_bsd():
        try:
            result = subprocess.run(
                ["systemctl", "status", "smartd"],
                capture_output=True, text=True, check=False,
            )
            return {
                "running": "active (running)" in result.stdout,
                "status_output": result.stdout,
            }
        except FileNotFoundError:
            pass

    try:
        result = subprocess.run(
            ["service", "smartd", "status"],
            capture_output=True, text=True, check=False,
        )
        return {
            "running": result.returncode == 0,
            "status_output": result.stdout or result.stderr,
        }
    except Exception as e:
        return {"running": False, "error": f"Unable to check smartd status: {e}"}


def restart_smartd() -> None:
    """
    Restart the smartd daemon.

    Uses run_privileged_command so sudo is added on Linux, where the
    service account is not root.
    """
    if is_bsd() or not shutil.which("systemctl"):
        command = ["service", "smartd", "restart"]
    else:
        command = ["systemctl", "restart", "smartd"]
    try:
        run_privileged_command(command)
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.stdout or "").strip() or str(e)
        raise Exception(f"Failed to restart smartd: {detail}")
    except FileNotFoundError:
        raise Exception(f"Failed to restart smartd: {command[0]} not found")


def parse_smartd_config(content: str) -> Dict[str, Any]:
    """
    Parse smartd.conf into its device entries.

    Comments are removed, a trailing backslash joins the next line, DEFAULT
    lines are skipped, and parsing stops at DEVICESCAN because smartd
    ignores every line after it.

    Returns:
        {"devicescan": bool,
         "entries": [{"device": str, "types": [str, ...]}, ...]}
    """
    parsed: Dict[str, Any] = {"devicescan": False, "entries": []}
    pending = ""
    for raw_line in content.splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if line.endswith("\\"):
            pending += line[:-1] + " "
            continue
        tokens = (pending + line).split()
        pending = ""
        if not tokens or tokens[0] == "DEFAULT":
            continue
        if tokens[0] == "DEVICESCAN":
            parsed["devicescan"] = True
            break
        types: List[str] = []
        for position, token in enumerate(tokens[:-1]):
            # "-d removable" is a flag, not a device type.
            if token == "-d" and tokens[position + 1] != "removable":
                types.append(tokens[position + 1])
        parsed["entries"].append({"device": tokens[0], "types": types})
    return parsed


def _normalize_device_type(device_type: str) -> str:
    """smartd reports megaraid,N as sat+megaraid,N; compare without sat+."""
    if device_type.startswith("sat+"):
        return device_type[4:]
    return device_type


def smartd_config_covers_disk(
    parsed: Dict[str, Any],
    disk_path: str,
    disk_type: Optional[str] = None,
) -> bool:
    """
    Check whether a parsed smartd.conf monitors a disk.

    A disk is covered by DEVICESCAN, or by an entry whose device is the
    same device node (links such as /dev/disk/by-id are resolved). When
    the disk is addressed through a controller (a type with a selector,
    such as megaraid,3), several disks share one device node, so the entry
    must also carry the same -d type.

    Args:
        parsed: Result of parse_smartd_config()
        disk_path: Device path as reported by smartctl --scan
        disk_type: The -d type smartctl needs for this disk, if any
    """
    if parsed.get("devicescan"):
        return True

    selector = None
    if disk_type and "," in disk_type:
        selector = _normalize_device_type(disk_type)

    real_disk_path = os.path.realpath(disk_path)
    for entry in parsed.get("entries", []):
        if os.path.realpath(entry["device"]) != real_disk_path:
            continue
        if selector is None:
            return True
        entry_types = [_normalize_device_type(t) for t in entry["types"]]
        if selector in entry_types:
            return True
    return False
