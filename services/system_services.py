"""
System Services Monitoring Service
Provides read-only visibility into system service status across Linux, FreeBSD,
and NetBSD.

Linux:   Uses systemctl to query systemd unit files and service states.
FreeBSD: Uses the service command and rc.d script enumeration.
NetBSD:  Enumerates rc.d scripts directly from /etc/rc.d and /usr/pkg/etc/rc.d,
         parses /etc/rc.conf for enabled state, and invokes scripts for status.
"""
import subprocess
import os
from typing import List, Dict, Any

from services.utils import (
    is_freebsd,
    is_netbsd,
    is_linux,
    run_privileged_command,
)


class SystemServicesService:
    """Read-only service for querying system service status."""

    def list_services(self) -> List[Dict[str, Any]]:
        """
        List all system services with their current state.

        Returns:
            Sorted list of service dictionaries with keys:
                name        - service unit or script name
                status      - running / stopped / exited / dead / unknown
                enabled     - enabled / disabled / static / masked / unknown
                description - short description (Linux only, empty on BSD)
        """
        if is_netbsd():
            return self._list_netbsd_services()
        if is_freebsd():
            return self._list_freebsd_services()
        return self._list_linux_services()

    def get_service_detail(self, service_name: str) -> Dict[str, Any]:
        """
        Get verbose status output for a single service.

        Args:
            service_name: The service unit name (e.g. 'sshd' or 'sshd.service')

        Returns:
            Dictionary with keys: name, output (raw status text)
        """
        if is_netbsd():
            return self._get_netbsd_service_detail(service_name)
        if is_freebsd():
            return self._get_freebsd_service_detail(service_name)
        return self._get_linux_service_detail(service_name)

    # ------------------------------------------------------------------
    # Linux (systemd)
    # ------------------------------------------------------------------

    def _list_linux_services(self) -> List[Dict[str, Any]]:
        """
        Combine systemctl list-units and list-unit-files to produce a
        complete picture of every known service on the system.
        """
        unit_map: Dict[str, Dict[str, Any]] = {}

        # 1. All installed unit files (gives enabled/disabled/static/masked)
        self._populate_from_unit_files(unit_map)

        # 2. All loaded units (gives active/sub state and description)
        self._populate_from_loaded_units(unit_map)

        services = sorted(unit_map.values(), key=lambda s: s["name"])
        return services

    def _populate_from_unit_files(self, unit_map: Dict[str, Dict[str, Any]]) -> None:
        """Parse systemctl list-unit-files --type=service."""
        try:
            result = run_privileged_command(
                [
                    "systemctl",
                    "list-unit-files",
                    "--type=service",
                    "--no-pager",
                    "--no-legend",
                ],
                check=False,
            )
            for line in result.stdout.strip().splitlines():
                parts = line.split()
                if len(parts) < 2:
                    continue
                unit_file = parts[0]
                enabled_state = parts[1]
                name = self._strip_service_suffix(unit_file)
                if name not in unit_map:
                    unit_map[name] = self._empty_service(name)
                unit_map[name]["enabled"] = enabled_state
        except Exception:
            pass

    def _populate_from_loaded_units(self, unit_map: Dict[str, Dict[str, Any]]) -> None:
        """Parse systemctl list-units --type=service --all."""
        try:
            result = run_privileged_command(
                [
                    "systemctl",
                    "list-units",
                    "--type=service",
                    "--all",
                    "--no-pager",
                    "--no-legend",
                ],
                check=False,
            )
            for line in result.stdout.strip().splitlines():
                # Columns: UNIT LOAD ACTIVE SUB DESCRIPTION...
                # The UNIT column may have a leading bullet marker on some systems.
                line = line.lstrip("\u25cf").strip()
                parts = line.split(None, 4)
                if len(parts) < 4:
                    continue
                unit = parts[0]
                active_state = parts[2]  # active / inactive / failed / activating
                sub_state = parts[3]  # running / exited / dead / waiting / failed
                description = parts[4] if len(parts) > 4 else ""
                name = self._strip_service_suffix(unit)

                if name not in unit_map:
                    unit_map[name] = self._empty_service(name)

                unit_map[name]["status"] = self._normalize_linux_status(
                    active_state, sub_state
                )
                if description:
                    unit_map[name]["description"] = description
        except Exception:
            pass

    def _get_linux_service_detail(self, service_name: str) -> Dict[str, Any]:
        """Run systemctl status for a single service."""
        unit = service_name if service_name.endswith(".service") else f"{service_name}.service"
        try:
            result = run_privileged_command(
                ["systemctl", "status", unit, "--no-pager", "-l"],
                check=False,
            )
            return {"name": service_name, "output": result.stdout}
        except Exception as exc:
            return {"name": service_name, "output": f"Error: {exc}"}

    # ------------------------------------------------------------------
    # FreeBSD
    # ------------------------------------------------------------------

    def _list_freebsd_services(self) -> List[Dict[str, Any]]:
        """
        Enumerate rc.d scripts and determine enabled / running state on FreeBSD.

        service -l   -> all available rc.d script names
        service -e   -> enabled services (full paths)
        service <name> onestatus -> running check (per-service)
        """
        all_scripts = self._freebsd_all_scripts()
        enabled_set = self._freebsd_enabled_set()

        services: List[Dict[str, Any]] = []
        for script_name in sorted(all_scripts):
            is_enabled = script_name in enabled_set
            status = self._freebsd_check_running(script_name)
            services.append(
                {
                    "name": script_name,
                    "status": status,
                    "enabled": "enabled" if is_enabled else "disabled",
                    "description": "",
                }
            )
        return services

    def _freebsd_all_scripts(self) -> List[str]:
        """Get list of all rc.d script names on FreeBSD."""
        scripts: List[str] = []

        # service -l lists script names (one per line)
        try:
            result = subprocess.run(
                ["service", "-l"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            for line in result.stdout.strip().splitlines():
                name = line.strip()
                if name:
                    scripts.append(name)
        except Exception:
            pass

        # Fallback: walk rc.d directories directly
        if not scripts:
            rc_dirs = ["/etc/rc.d", "/usr/local/etc/rc.d"]
            for rc_dir in rc_dirs:
                self._collect_rcd_scripts(rc_dir, scripts)

        return scripts

    def _freebsd_enabled_set(self) -> set:
        """
        Get set of enabled service names on FreeBSD.
        service -e prints full paths of enabled scripts.
        """
        enabled: set = set()
        try:
            result = subprocess.run(
                ["service", "-e"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            for line in result.stdout.strip().splitlines():
                path = line.strip()
                if path:
                    enabled.add(os.path.basename(path))
        except Exception:
            pass
        return enabled

    def _freebsd_check_running(self, script_name: str) -> str:
        """Check if a FreeBSD service is currently running."""
        try:
            result = subprocess.run(
                ["service", script_name, "onestatus"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return self._parse_bsd_status_output(result)
        except Exception:
            return "unknown"

    def _get_freebsd_service_detail(self, service_name: str) -> Dict[str, Any]:
        """Run service <name> status on FreeBSD."""
        try:
            result = subprocess.run(
                ["service", service_name, "status"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            output = result.stdout
            if result.stderr:
                output += "\n" + result.stderr
            return {"name": service_name, "output": output}
        except Exception as exc:
            return {"name": service_name, "output": f"Error: {exc}"}

    # ------------------------------------------------------------------
    # NetBSD
    # ------------------------------------------------------------------

    # NetBSD rc.d directories:
    #   /etc/rc.d           - base system services
    #   /usr/pkg/etc/rc.d   - pkgsrc-installed services
    NETBSD_RCD_DIRS = ["/etc/rc.d", "/usr/pkg/etc/rc.d"]

    # NetBSD rc.conf locations (checked in order):
    #   /etc/rc.conf        - main configuration
    #   /etc/rc.conf.d/*    - per-service override files
    NETBSD_RC_CONF = "/etc/rc.conf"
    NETBSD_RC_CONF_D = "/etc/rc.conf.d"

    def _list_netbsd_services(self) -> List[Dict[str, Any]]:
        """
        Enumerate rc.d scripts and determine enabled / running state on NetBSD.

        NetBSD may not have the 'service' command, so we:
        1. Walk /etc/rc.d and /usr/pkg/etc/rc.d for executable scripts.
        2. Parse /etc/rc.conf and /etc/rc.conf.d/ for enabled state.
        3. Invoke each script directly with 'onestatus' to check running state.
        """
        script_map = self._netbsd_all_scripts()
        enabled_set = self._netbsd_enabled_set()

        services: List[Dict[str, Any]] = []
        for script_name in sorted(script_map.keys()):
            script_path = script_map[script_name]
            is_enabled = script_name in enabled_set
            status = self._netbsd_check_running(script_path)
            services.append(
                {
                    "name": script_name,
                    "status": status,
                    "enabled": "enabled" if is_enabled else "disabled",
                    "description": "",
                }
            )
        return services

    def _netbsd_all_scripts(self) -> Dict[str, str]:
        """
        Get dict of {script_name: full_path} for all rc.d scripts on NetBSD.

        First try the 'service' command if available, then fall back to
        direct directory enumeration.
        """
        script_map: Dict[str, str] = {}

        # Try 'service -l' first (available on newer NetBSD)
        has_service_cmd = self._netbsd_try_service_list(script_map)

        # Always supplement with direct directory walk to catch pkgsrc services
        # that 'service -l' might miss, or if 'service' is not available.
        for rc_dir in self.NETBSD_RCD_DIRS:
            if os.path.isdir(rc_dir):
                for entry in os.listdir(rc_dir):
                    full_path = os.path.join(rc_dir, entry)
                    if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
                        if entry not in script_map:
                            script_map[entry] = full_path

        return script_map

    def _netbsd_try_service_list(self, script_map: Dict[str, str]) -> bool:
        """
        Attempt to use 'service -l' on NetBSD. Returns True if the command
        was available and produced output.
        """
        try:
            result = subprocess.run(
                ["service", "-l"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return False
            for line in result.stdout.strip().splitlines():
                name = line.strip()
                if name:
                    # Resolve to full path by checking known directories
                    resolved = self._netbsd_resolve_script_path(name)
                    script_map[name] = resolved
            return True
        except FileNotFoundError:
            # 'service' command does not exist on this NetBSD version
            return False
        except Exception:
            return False

    def _netbsd_resolve_script_path(self, script_name: str) -> str:
        """Resolve a script name to its full path on NetBSD."""
        for rc_dir in self.NETBSD_RCD_DIRS:
            candidate = os.path.join(rc_dir, script_name)
            if os.path.isfile(candidate):
                return candidate
        # Fallback: return a best-guess path
        return os.path.join("/etc/rc.d", script_name)

    def _netbsd_enabled_set(self) -> set:
        """
        Determine which services are enabled on NetBSD by parsing rc.conf.

        Services are enabled by setting their rc.d variable to YES in
        /etc/rc.conf, for example:
            sshd=YES
            nginx=YES

        Per-service overrides can also exist in /etc/rc.conf.d/<service_name>,
        for example /etc/rc.conf.d/sshd containing:
            sshd=YES

        The variable name is typically the script name (sometimes with _enable
        appended on some configurations, but the standard NetBSD convention
        is just servicename=YES).
        """
        enabled: set = set()

        # First try 'service -e' if available (newer NetBSD)
        if self._netbsd_try_service_enabled(enabled):
            return enabled

        # Fall back to parsing rc.conf files directly
        self._parse_rc_conf_for_enabled(self.NETBSD_RC_CONF, enabled)

        # Check per-service override directory
        if os.path.isdir(self.NETBSD_RC_CONF_D):
            for entry in os.listdir(self.NETBSD_RC_CONF_D):
                conf_path = os.path.join(self.NETBSD_RC_CONF_D, entry)
                if os.path.isfile(conf_path):
                    self._parse_rc_conf_for_enabled(conf_path, enabled)

        return enabled

    def _netbsd_try_service_enabled(self, enabled: set) -> bool:
        """
        Attempt to use 'service -e' on NetBSD. Returns True if the command
        was available and produced output.
        """
        try:
            result = subprocess.run(
                ["service", "-e"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if result.returncode != 0 or not result.stdout.strip():
                return False
            for line in result.stdout.strip().splitlines():
                path = line.strip()
                if path:
                    enabled.add(os.path.basename(path))
            return True
        except FileNotFoundError:
            return False
        except Exception:
            return False

    @staticmethod
    def _parse_rc_conf_for_enabled(conf_path: str, enabled: set) -> None:
        """
        Parse an rc.conf file for service_name=YES lines.

        Handles:
            sshd=YES
            sshd="YES"
            sshd='YES'
            # sshd=YES  (commented, skipped)
        """
        try:
            with open(conf_path, "r") as fh:
                for line in fh:
                    line = line.strip()
                    # Skip comments and empty lines
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue
                    key, _, value = line.partition("=")
                    key = key.strip()
                    value = value.strip().strip("\"'").upper()
                    if value == "YES":
                        enabled.add(key)
        except (OSError, IOError):
            pass

    def _netbsd_check_running(self, script_path: str) -> str:
        """
        Check if a NetBSD service is currently running by invoking the
        rc.d script directly with the 'onestatus' argument.

        Falls back to the 'service' command if the script path is not
        directly executable.
        """
        # Try invoking the script directly
        if os.path.isfile(script_path) and os.access(script_path, os.X_OK):
            try:
                result = subprocess.run(
                    [script_path, "onestatus"],
                    capture_output=True,
                    text=True,
                    timeout=5,
                    check=False,
                )
                return self._parse_bsd_status_output(result)
            except Exception:
                pass

        # Fallback: try 'service' command
        script_name = os.path.basename(script_path)
        try:
            result = subprocess.run(
                ["service", script_name, "onestatus"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return self._parse_bsd_status_output(result)
        except Exception:
            return "unknown"

    def _get_netbsd_service_detail(self, service_name: str) -> Dict[str, Any]:
        """
        Get verbose status for a single service on NetBSD.
        Tries direct script invocation first, then the service command.
        """
        # Resolve script path
        script_path = self._netbsd_resolve_script_path(service_name)

        # Try direct invocation
        if os.path.isfile(script_path) and os.access(script_path, os.X_OK):
            try:
                result = subprocess.run(
                    [script_path, "status"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    check=False,
                )
                output = result.stdout
                if result.stderr:
                    output += "\n" + result.stderr
                return {"name": service_name, "output": output}
            except Exception:
                pass

        # Fallback: try 'service' command
        try:
            result = subprocess.run(
                ["service", service_name, "status"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            output = result.stdout
            if result.stderr:
                output += "\n" + result.stderr
            return {"name": service_name, "output": output}
        except Exception as exc:
            return {"name": service_name, "output": f"Error: {exc}"}

    # ------------------------------------------------------------------
    # Shared Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _collect_rcd_scripts(rc_dir: str, scripts: List[str]) -> None:
        """Walk a single rc.d directory and append executable script names."""
        if not os.path.isdir(rc_dir):
            return
        for entry in os.listdir(rc_dir):
            full_path = os.path.join(rc_dir, entry)
            if os.path.isfile(full_path) and os.access(full_path, os.X_OK):
                if entry not in scripts:
                    scripts.append(entry)

    @staticmethod
    def _parse_bsd_status_output(result: subprocess.CompletedProcess) -> str:
        """
        Parse the output of an rc.d onestatus command.
        Returns 'running', 'stopped', or 'unknown'.
        """
        output = result.stdout.lower() + result.stderr.lower()
        if "is running" in output:
            return "running"
        if "is not running" in output or "not running" in output:
            return "stopped"
        # Some services exit 0 when running with non-standard output
        if result.returncode == 0 and output.strip():
            return "running"
        return "stopped"

    @staticmethod
    def _empty_service(name: str) -> Dict[str, Any]:
        return {
            "name": name,
            "status": "unknown",
            "enabled": "unknown",
            "description": "",
        }

    @staticmethod
    def _strip_service_suffix(unit: str) -> str:
        if unit.endswith(".service"):
            return unit[: -len(".service")]
        return unit

    @staticmethod
    def _normalize_linux_status(active_state: str, sub_state: str) -> str:
        """Map systemd active/sub states to a simple status string."""
        if sub_state == "running":
            return "running"
        if sub_state == "exited":
            return "exited"
        if active_state == "failed" or sub_state == "failed":
            return "failed"
        if active_state == "inactive":
            return "stopped"
        if sub_state == "dead":
            return "stopped"
        if active_state == "activating":
            return "starting"
        if active_state == "deactivating":
            return "stopping"
        return sub_state if sub_state else "unknown"
