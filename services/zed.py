"""
ZED (ZFS Event Daemon) Management Service

Provides discovery and management of ZEDLETs (ZFS Event Daemon Linkage
for Executable Tasks) by reading native filesystem state. No WebZFS
database is used for enabled/disabled tracking. State is derived entirely
from ZED's own directory layout, symlinks, ownership, and permissions.

Supports Linux, FreeBSD, and NetBSD.
"""

import hashlib
import logging
import os
import re
import stat
import subprocess
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from services.utils import (
    is_freebsd,
    is_netbsd,
    needs_sudo_for_privileged,
    run_privileged_command,
)

logger = logging.getLogger(__name__)

# Known ZED event class prefixes used in ZEDLET filenames.
ZED_EVENT_PREFIXES = [
    "all",
    "checksum",
    "config_sync",
    "data",
    "deadman",
    "delay",
    "history_event",
    "io",
    "io_failure",
    "pool_create",
    "pool_destroy",
    "pool_export",
    "pool_import",
    "pool_reguid",
    "probe_failure",
    "resilver_finish",
    "resilver_start",
    "scrub_finish",
    "scrub_start",
    "statechange",
    "trim_finish",
    "trim_start",
    "vdev_attach",
    "vdev_clear",
    "vdev_remove",
]

# Known ZEDLET descriptions
_ZEDLET_DESCRIPTIONS = {
    "all-debug.sh": "Log all events to the debug log",
    "all-syslog.sh": "Log all events to syslog",
    "data-notify.sh": "Send notification on data events",
    "deadman-sync-slot_off.sh": "Turn off slot LED on deadman event",
    "generic-notify.sh": "Generic notification helper",
    "pool_import-sync-led.sh": "Sync LED state on pool import",
    "resilver_finish-notify.sh": "Notify when resilver completes",
    "resilver_finish-start-scrub.sh": "Start scrub after resilver",
    "scrub_finish-notify.sh": "Notify when scrub completes",
    "statechange-notify.sh": "Notify on pool state changes",
    "statechange-sync-led.sh": "Update LED on pool state change",
    "statechange-sync-slot_off.sh": "Turn off slot LED on state change",
    "trim_finish-notify.sh": "Notify when TRIM completes",
    "vdev_attach-sync-led.sh": "Sync LED on vdev attach",
    "vdev_clear-sync-led.sh": "Sync LED on vdev clear",
}


@dataclass
class ZedletInfo:
    """Represents the resolved state of a single ZEDLET."""

    name: str
    event: str
    enabled: bool
    origin: str  # "package", "custom", "override"
    is_sync: bool
    is_link: bool
    detail: str
    enabled_path: str = ""
    package_path: str = ""
    permissions_ok: bool = True
    owner_ok: bool = True


@dataclass
class ZedStatus:
    """Top-level ZED state for the UI."""

    daemon_running: bool = False
    daemon_pid: Optional[int] = None
    enabled_dir: str = ""
    package_dir: str = ""
    zedlets: List[ZedletInfo] = field(default_factory=list)
    zed_rc_path: str = ""
    error: str = ""
    total: int = 0
    enabled_count: int = 0
    disabled_count: int = 0
    package_count: int = 0
    custom_count: int = 0
    override_count: int = 0


class ZedService:
    """Service for ZED/ZEDLET management."""

    # -- Path Discovery ------------------------------------------------ #

    def _candidate_enabled_dirs(self) -> List[str]:
        if is_freebsd():
            return ["/usr/local/etc/zfs/zed.d", "/etc/zfs/zed.d"]
        if is_netbsd():
            return ["/usr/pkg/etc/zfs/zed.d", "/etc/zfs/zed.d"]
        return ["/etc/zfs/zed.d"]

    def _candidate_package_dirs(self) -> List[str]:
        if is_freebsd():
            return [
                "/usr/local/libexec/zfs/zed.d",
                "/usr/libexec/zfs/zed.d",
            ]
        if is_netbsd():
            return [
                "/usr/pkg/libexec/zfs/zed.d",
                "/usr/libexec/zfs/zed.d",
            ]
        return [
            "/usr/libexec/zfs/zed.d",
            "/usr/lib/zfs-linux/zed.d",
            "/usr/lib/zfs/zed.d",
            "/usr/share/zfs/zed.d",
        ]

    def detect_enabled_dir(self) -> str:
        for path in self._candidate_enabled_dirs():
            if os.path.isdir(path):
                return path
        return ""

    def detect_package_dir(self) -> str:
        for path in self._candidate_package_dirs():
            if os.path.isdir(path):
                return path
        return ""

    def detect_zed_rc(self, enabled_dir: str) -> str:
        if enabled_dir:
            rc = os.path.join(enabled_dir, "zed.rc")
            if os.path.isfile(rc):
                return rc
        for c in ["/etc/zfs/zed.d/zed.rc",
                   "/usr/local/etc/zfs/zed.d/zed.rc"]:
            if os.path.isfile(c):
                return c
        return ""

    # -- Daemon Status ------------------------------------------------- #

    def get_daemon_status(self) -> Tuple[bool, Optional[int]]:
        for pid_path in ["/var/run/zed.pid", "/run/zed.pid"]:
            if os.path.isfile(pid_path):
                try:
                    with open(pid_path, "r") as f:
                        pid = int(f.read().strip())
                    os.kill(pid, 0)
                    return True, pid
                except (ValueError, OSError, PermissionError):
                    pass
        try:
            result = subprocess.run(
                ["pgrep", "-x", "zed"],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0 and result.stdout.strip():
                pid = int(result.stdout.strip().split("\n")[0])
                return True, pid
        except (subprocess.TimeoutExpired, ValueError, FileNotFoundError):
            pass
        return False, None

    # -- Filename Helpers ---------------------------------------------- #

    def _parse_event(self, filename: str) -> str:
        base = filename.removesuffix(".sh.in").removesuffix(".sh")
        for pfx in sorted(ZED_EVENT_PREFIXES, key=len, reverse=True):
            if base.startswith(pfx + "-") or base == pfx:
                return pfx
        return base.split("-", 1)[0]

    def _is_sync(self, filename: str) -> bool:
        base = filename.removesuffix(".sh.in").removesuffix(".sh")
        parts = base.split("-")
        return "sync" in parts[1:3] if len(parts) > 1 else False

    def _check_perms(self, path: str) -> Tuple[bool, bool]:
        try:
            st = os.stat(path)
        except OSError:
            return False, False
        owner_ok = st.st_uid == 0
        mode = st.st_mode
        perm_ok = (
            bool(mode & stat.S_IXUSR)
            and not bool(mode & stat.S_IWGRP)
            and not bool(mode & stat.S_IWOTH)
        )
        return perm_ok, owner_ok

    # -- ZEDLET Enumeration -------------------------------------------- #

    def _is_script(self, name: str) -> bool:
        if name.startswith(".") or name == "zed.rc":
            return False
        return name.endswith(".sh") or name.endswith(".sh.in")

    def list_zedlets(
        self, enabled_dir: str, package_dir: str
    ) -> List[ZedletInfo]:
        zedlets: Dict[str, ZedletInfo] = {}

        # Package directory
        if package_dir and os.path.isdir(package_dir):
            try:
                for entry in os.listdir(package_dir):
                    if not self._is_script(entry):
                        continue
                    full = os.path.join(package_dir, entry)
                    if not os.path.isfile(full):
                        continue
                    po, oo = self._check_perms(full)
                    zedlets[entry] = ZedletInfo(
                        name=entry,
                        event=self._parse_event(entry),
                        enabled=False,
                        origin="package",
                        is_sync=self._is_sync(entry),
                        is_link=False,
                        detail=_ZEDLET_DESCRIPTIONS.get(entry, ""),
                        package_path=full,
                        permissions_ok=po, owner_ok=oo,
                    )
            except PermissionError:
                logger.warning("Cannot read package dir: %s", package_dir)

        # Enabled directory
        if enabled_dir and os.path.isdir(enabled_dir):
            try:
                for entry in os.listdir(enabled_dir):
                    if not self._is_script(entry):
                        continue
                    full = os.path.join(enabled_dir, entry)
                    is_link = os.path.islink(full)
                    if not os.path.isfile(full) and not is_link:
                        continue
                    po, oo = self._check_perms(full)

                    if entry in zedlets:
                        z = zedlets[entry]
                        z.enabled = True
                        z.enabled_path = full
                        z.is_link = is_link
                        z.origin = "package" if is_link else "override"
                        z.permissions_ok = po
                        z.owner_ok = oo
                    else:
                        zedlets[entry] = ZedletInfo(
                            name=entry,
                            event=self._parse_event(entry),
                            enabled=True,
                            origin="custom",
                            is_sync=self._is_sync(entry),
                            is_link=is_link,
                            detail=_ZEDLET_DESCRIPTIONS.get(entry, ""),
                            enabled_path=full,
                            permissions_ok=po, owner_ok=oo,
                        )
            except PermissionError:
                logger.warning("Cannot read enabled dir: %s", enabled_dir)

        return sorted(zedlets.values(), key=lambda z: z.name)

    # -- Full Status --------------------------------------------------- #

    def get_status(self) -> ZedStatus:
        status = ZedStatus()
        status.enabled_dir = self.detect_enabled_dir()
        status.package_dir = self.detect_package_dir()
        if not status.enabled_dir and not status.package_dir:
            status.error = (
                "ZED directories not found. Ensure OpenZFS is installed "
                "and ZED is available on this system."
            )
            return status
        status.zed_rc_path = self.detect_zed_rc(status.enabled_dir)
        status.daemon_running, status.daemon_pid = (
            self.get_daemon_status()
        )
        status.zedlets = self.list_zedlets(
            status.enabled_dir, status.package_dir
        )
        status.total = len(status.zedlets)
        status.enabled_count = sum(1 for z in status.zedlets if z.enabled)
        status.disabled_count = status.total - status.enabled_count
        status.package_count = sum(
            1 for z in status.zedlets if z.origin == "package"
        )
        status.custom_count = sum(
            1 for z in status.zedlets if z.origin == "custom"
        )
        status.override_count = sum(
            1 for z in status.zedlets if z.origin == "override"
        )
        return status

    # -- Enable / Disable ---------------------------------------------- #

    def enable_zedlet(self, name: str, enabled_dir: str,
                      package_dir: str) -> str:
        self._validate_name(name)
        enabled_path = os.path.join(enabled_dir, name)
        pkg_path = os.path.join(package_dir, name) if package_dir else ""

        if os.path.exists(enabled_path):
            self._sudo(["chmod", "u+x", enabled_path])
            return f"Enabled {name}"
        if pkg_path and os.path.isfile(pkg_path):
            self._sudo(["ln", "-sf", pkg_path, enabled_path])
            return f"Enabled {name} (linked to package)"
        return f"Cannot enable {name}: no package source found"

    def disable_zedlet(self, name: str, enabled_dir: str,
                       package_dir: str) -> str:
        self._validate_name(name)
        enabled_path = os.path.join(enabled_dir, name)
        if not os.path.exists(enabled_path):
            return f"{name} is already disabled"
        is_link = os.path.islink(enabled_path)
        pkg_path = os.path.join(package_dir, name) if package_dir else ""
        if is_link and pkg_path and os.path.isfile(pkg_path):
            self._sudo(["rm", "-f", enabled_path])
            return f"Disabled {name} (removed symlink)"
        self._sudo(["chmod", "a-x", enabled_path])
        return f"Disabled {name}"

    # -- Create / Save / Delete ---------------------------------------- #

    def create_zedlet(self, name: str, content: str,
                      enabled_dir: str, enable: bool = True) -> str:
        self._validate_name(name)
        target = os.path.join(enabled_dir, name)
        if os.path.exists(target):
            raise ValueError(f"ZEDLET already exists: {name}")
        self._atomic_write(target, content, executable=enable)
        return f"Created {name}"

    def save_zedlet(self, name: str, content: str,
                    enabled_dir: str, package_dir: str,
                    expected_hash: str = "") -> str:
        self._validate_name(name)
        target = os.path.join(enabled_dir, name)
        is_link = os.path.islink(target)
        is_pkg_link = False
        if is_link and package_dir:
            real = os.path.realpath(target)
            pkg_real = os.path.realpath(package_dir)
            is_pkg_link = real.startswith(pkg_real + "/")
        if expected_hash:
            cur = self._file_hash(target)
            if cur and cur != expected_hash:
                raise ValueError(
                    "File modified since opened. Reload and retry."
                )
        if is_pkg_link:
            self._sudo(["rm", "-f", target])
        self._atomic_write(target, content, executable=True)
        if is_pkg_link:
            return f"Created local override for {name}"
        return f"Saved {name}"

    def delete_zedlet(self, name: str, enabled_dir: str) -> str:
        self._validate_name(name)
        target = os.path.join(enabled_dir, name)
        if not os.path.exists(target):
            raise ValueError(f"ZEDLET not found: {name}")
        self._sudo(["rm", "-f", target])
        return f"Deleted {name}"

    def restore_default(self, name: str, enabled_dir: str,
                        package_dir: str) -> str:
        self._validate_name(name)
        ep = os.path.join(enabled_dir, name)
        pp = os.path.join(package_dir, name) if package_dir else ""
        if not pp or not os.path.isfile(pp):
            raise ValueError(f"No package version for {name}")
        if os.path.exists(ep) or os.path.islink(ep):
            self._sudo(["rm", "-f", ep])
        self._sudo(["ln", "-sf", pp, ep])
        return f"Restored {name} to package default"

    # -- Content Reading ----------------------------------------------- #

    def read_zedlet(self, name: str, enabled_dir: str,
                    package_dir: str) -> Tuple[str, str, str]:
        """Returns (content, sha256_hash, origin)."""
        self._validate_name(name)
        ep = os.path.join(enabled_dir, name)
        if os.path.isfile(ep):
            content = self._read_file(ep)
            h = hashlib.sha256(content.encode()).hexdigest()
            if os.path.islink(ep):
                origin = "package"
            elif package_dir and os.path.isfile(
                os.path.join(package_dir, name)
            ):
                origin = "override"
            else:
                origin = "custom"
            return content, h, origin
        if package_dir:
            pp = os.path.join(package_dir, name)
            if os.path.isfile(pp):
                content = self._read_file(pp)
                h = hashlib.sha256(content.encode()).hexdigest()
                return content, h, "package"
        raise ValueError(f"ZEDLET not found: {name}")

    # -- zed.rc -------------------------------------------------------- #

    def read_zed_rc(self, rc_path: str) -> Tuple[str, str]:
        if not rc_path or not os.path.isfile(rc_path):
            raise ValueError("zed.rc not found")
        content = self._read_file(rc_path)
        h = hashlib.sha256(content.encode()).hexdigest()
        return content, h

    def save_zed_rc(self, rc_path: str, content: str,
                    expected_hash: str = "") -> str:
        if not rc_path:
            raise ValueError("zed.rc path not configured")
        if expected_hash:
            cur = self._file_hash(rc_path)
            if cur and cur != expected_hash:
                raise ValueError(
                    "zed.rc modified since opened. Reload and retry."
                )
        self._atomic_write(rc_path, content, executable=False)
        return "Saved zed.rc"

    # -- ZED Reload / Start / Stop ------------------------------------- #

    def reload_zed(self) -> str:
        running, pid = self.get_daemon_status()
        if not running or pid is None:
            return "ZED is not running; cannot reload"
        self._sudo(["kill", "-HUP", str(pid)])
        return "ZED reloaded (SIGHUP sent)"

    def start_zed(self) -> str:
        """Start the ZED service via the platform service manager."""
        running, _ = self.get_daemon_status()
        if running:
            return "ZED is already running"
        if is_freebsd():
            self._sudo(["service", "zed", "start"])
        elif is_netbsd():
            self._sudo(["service", "zed", "start"])
        else:
            self._sudo(["systemctl", "start", "zfs-zed"])
        return "ZED service started"

    def stop_zed(self) -> str:
        """Stop the ZED service via the platform service manager."""
        running, _ = self.get_daemon_status()
        if not running:
            return "ZED is not running"
        if is_freebsd():
            self._sudo(["service", "zed", "stop"])
        elif is_netbsd():
            self._sudo(["service", "zed", "stop"])
        else:
            self._sudo(["systemctl", "stop", "zfs-zed"])
        return "ZED service stopped"

    # -- Helpers ------------------------------------------------------- #

    def _sudo(self, cmd: List[str]) -> subprocess.CompletedProcess:
        """Run a command with sudo when not already root.

        Raises ValueError with a readable message on failure instead of
        the raw CalledProcessError which only shows the argv list.
        """
        use_sudo = needs_sudo_for_privileged()
        try:
            return run_privileged_command(
                cmd, check=True, use_sudo=use_sudo,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            base = cmd[0].split("/")[-1]
            if "password is required" in stderr.lower() or (
                "sudo" in stderr.lower() and "tty" in stderr.lower()
            ):
                raise ValueError(
                    f"Sudo requires a password for '{base}'. "
                    f"Add the required NOPASSWD rule to "
                    f"/etc/sudoers.d/webzfs for this command."
                ) from None
            detail = stderr if stderr else f"exit code {exc.returncode}"
            raise ValueError(
                f"Command '{base}' failed: {detail}"
            ) from None

    def _validate_name(self, name: str) -> None:
        if not name:
            raise ValueError("Filename is required")
        if "/" in name or "\\" in name or "\x00" in name:
            raise ValueError("Invalid filename")
        if name.startswith("."):
            raise ValueError("Dotfiles are ignored by ZED")
        if ".." in name:
            raise ValueError("Invalid filename")
        if not re.match(r"^[a-zA-Z0-9_\-]+\.sh(\.in)?$", name):
            raise ValueError("Filename must match [a-zA-Z0-9_-]+.sh")

    def _atomic_write(self, target: str, content: str,
                      executable: bool = True) -> None:
        tdir = os.path.dirname(target)
        basename = os.path.basename(target)
        tmp = os.path.join(tdir, f".webzfs-tmp-{basename}")
        mode = "0755" if executable else "0644"
        try:
            self._sudo_tee(tmp, content)
            self._sudo(["chown", "root:root", tmp])
            self._sudo(["chmod", mode, tmp])
            self._sudo(["mv", "-f", tmp, target])
        except Exception:
            # Best-effort cleanup of the temp file.
            try:
                run_privileged_command(
                    ["rm", "-f", tmp], check=False,
                    use_sudo=needs_sudo_for_privileged(),
                )
            except Exception:
                pass
            raise

    def _sudo_tee(self, path: str, content: str) -> None:
        """Write content to a file via sudo tee."""
        use_sudo = needs_sudo_for_privileged()
        try:
            run_privileged_command(
                ["tee", path], input_data=content,
                check=True, capture_output=True,
                use_sudo=use_sudo,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            if "password is required" in stderr.lower():
                raise ValueError(
                    "Sudo requires a password for 'tee'. "
                    "Add the required NOPASSWD rule to "
                    "/etc/sudoers.d/webzfs."
                ) from None
            raise ValueError(
                f"Cannot write {path}: {stderr or 'permission denied'}"
            ) from None

    def _read_file(self, path: str) -> str:
        try:
            with open(path, "r") as f:
                return f.read()
        except PermissionError:
            use_sudo = needs_sudo_for_privileged()
            try:
                result = run_privileged_command(
                    ["cat", path], check=True,
                    capture_output=True, use_sudo=use_sudo,
                )
                return result.stdout
            except subprocess.CalledProcessError as exc:
                stderr = (exc.stderr or "").strip()
                if "password is required" in stderr.lower():
                    raise ValueError(
                        "Sudo requires a password for 'cat'. "
                        "Add the required NOPASSWD rule to "
                        "/etc/sudoers.d/webzfs."
                    ) from None
                raise ValueError(
                    f"Cannot read {path}: "
                    f"{stderr or 'permission denied'}"
                ) from None

    def _file_hash(self, path: str) -> str:
        try:
            content = self._read_file(path)
            return hashlib.sha256(content.encode()).hexdigest()
        except Exception:
            return ""

    def build_filename(self, event: str, handler: str,
                       is_sync: bool) -> str:
        handler = re.sub(r"[^a-zA-Z0-9_\-]", "", handler.strip())
        if not handler:
            raise ValueError("Handler name is required")
        if is_sync:
            name = f"{event}-sync-{handler}.sh"
        else:
            name = f"{event}-{handler}.sh"
        self._validate_name(name)
        return name
