"""NTP Configuration Service
Handles NTP (Network Time Protocol) configuration for Linux, FreeBSD, and NetBSD.

This is a teaching example. It uses the shared helpers in services.utils and
services.file so the same code runs on every platform WebZFS supports.
"""

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List

from services.file import read_file, save_file
from services.utils import (
    is_freebsd,
    is_linux,
    is_netbsd,
    needs_sudo_for_privileged,
    run_privileged_command,
)

logger = logging.getLogger(__name__)


class NTPService:
    """Service for managing NTP configuration across Linux, FreeBSD, and NetBSD."""

    def __init__(self) -> None:
        self.os_type = self._detect_os()
        self.config_path = self._get_config_path()
        self.service_name = self._get_service_name()

    def _detect_os(self) -> str:
        """Detect the operating system using the shared platform helpers."""
        if is_linux():
            return "linux"
        if is_freebsd():
            return "freebsd"
        if is_netbsd():
            return "netbsd"
        return "unknown"

    def _get_config_path(self) -> Path:
        """Get the NTP configuration file path for this platform."""
        return Path("/etc/ntp.conf")

    def _get_service_name(self) -> str:
        """Get the NTP service name. Linux, FreeBSD, and NetBSD all use ntpd."""
        return "ntpd"

    def get_status(self) -> Dict[str, Any]:
        """Get NTP service status."""
        if self.os_type not in ("linux", "freebsd", "netbsd"):
            return {
                "running": False,
                "service_name": self.service_name,
                "output": f"Unsupported operating system: {self.os_type}",
                "os_type": self.os_type,
            }

        if self.os_type == "linux":
            command = ["systemctl", "status", self.service_name]
        else:
            command = ["service", self.service_name, "status"]

        try:
            result = run_privileged_command(command, check=False, timeout=5)
            output = result.stdout or result.stderr
            if self.os_type == "linux":
                is_running = result.returncode == 0
            else:
                is_running = "is running" in output.lower()
            return {
                "running": is_running,
                "service_name": self.service_name,
                "output": output,
                "os_type": self.os_type,
            }
        except Exception as status_error:
            logger.warning("Could not read NTP status: %s", status_error)
            return {
                "running": False,
                "service_name": self.service_name,
                "output": f"Error: {status_error}",
                "os_type": self.os_type,
            }

    def get_config(self) -> str:
        """Read the current NTP configuration.

        Raises:
            OSError: If the configuration file cannot be read.
        """
        return read_file(str(self.config_path), use_sudo=needs_sudo_for_privileged())

    def get_servers(self) -> List[str]:
        """Extract NTP server and pool entries from the configuration."""
        servers: List[str] = []
        try:
            config = self.get_config()
        except OSError as read_error:
            logger.warning("Could not read NTP configuration: %s", read_error)
            return servers

        for line in config.splitlines():
            stripped = line.strip()
            if stripped.startswith("server ") or stripped.startswith("pool "):
                parts = stripped.split()
                if len(parts) >= 2:
                    servers.append(parts[1])
        return servers

    def update_config(self, config_content: str) -> bool:
        """Replace the NTP configuration file.

        The content is validated before it touches the live file. The write
        uses services.file.save_file, which writes through a secure temporary
        file and falls back to sudo tee on Linux when the file is root-owned.
        """
        if not config_content or not config_content.strip():
            raise ValueError("NTP configuration cannot be empty")
        if len(config_content) > 100000:
            raise ValueError("NTP configuration is too large")

        try:
            save_file(
                str(self.config_path),
                config_content,
                use_sudo=needs_sudo_for_privileged(),
            )
            return True
        except Exception as write_error:
            logger.error("Error updating NTP configuration: %s", write_error)
            return False

    def restart_service(self) -> bool:
        """Restart the NTP service."""
        if self.os_type == "linux":
            command = ["systemctl", "restart", self.service_name]
        elif self.os_type in ("freebsd", "netbsd"):
            command = ["service", self.service_name, "restart"]
        else:
            return False

        try:
            result = run_privileged_command(command, check=False, timeout=10)
            return result.returncode == 0
        except Exception as restart_error:
            logger.error("Error restarting NTP service: %s", restart_error)
            return False

    def enable_service(self) -> bool:
        """Enable the NTP service to start at boot."""
        if self.os_type == "linux":
            command = ["systemctl", "enable", self.service_name]
        elif self.os_type == "freebsd":
            command = ["sysrc", f"{self.service_name}_enable=YES"]
        elif self.os_type == "netbsd":
            return self._enable_netbsd_service()
        else:
            return False

        try:
            result = run_privileged_command(command, check=False, timeout=5)
            return result.returncode == 0
        except Exception as enable_error:
            logger.error("Error enabling NTP service: %s", enable_error)
            return False

    def _enable_netbsd_service(self) -> bool:
        """Enable ntpd on NetBSD by setting ntpd=YES in /etc/rc.conf.

        NetBSD's service(8) has no enable action and sysrc is FreeBSD-only,
        so the rc.conf variable is edited through the shared file helpers.
        WebZFS runs as root on NetBSD, so the helpers do not need sudo.
        """
        rc_conf = Path("/etc/rc.conf")
        variable = f"{self.service_name}=YES"
        try:
            content = read_file(str(rc_conf), use_sudo=needs_sudo_for_privileged())
        except FileNotFoundError:
            content = ""
        except OSError as read_error:
            logger.error("Error reading /etc/rc.conf: %s", read_error)
            return False

        updated_lines: List[str] = []
        replaced = False
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith(f"{self.service_name}=") or stripped.startswith(
                f"{self.service_name}_enable="
            ):
                updated_lines.append(variable)
                replaced = True
            else:
                updated_lines.append(line)
        if not replaced:
            updated_lines.append(variable)

        try:
            save_file(
                str(rc_conf),
                "\n".join(updated_lines) + "\n",
                use_sudo=needs_sudo_for_privileged(),
            )
            return True
        except Exception as write_error:
            logger.error("Error enabling ntpd in /etc/rc.conf: %s", write_error)
            return False

    def get_time_info(self) -> Dict[str, str]:
        """Get the current system time and NTP synchronization status."""
        system_time = "Unknown"
        sync_status = "Not available"

        try:
            date_command = shutil.which("date")
            if date_command:
                result = run_privileged_command(
                    [date_command, "+%Y-%m-%d %H:%M:%S %Z"],
                    check=False,
                    timeout=2,
                    use_sudo=False,
                )
                if result.returncode == 0:
                    system_time = result.stdout.strip()

            ntpq_command = shutil.which("ntpq")
            if ntpq_command:
                result = run_privileged_command(
                    [ntpq_command, "-p"],
                    check=False,
                    timeout=2,
                    use_sudo=False,
                )
                if result.returncode == 0:
                    sync_status = result.stdout.strip()
        except Exception as time_error:
            logger.warning("Error reading NTP time information: %s", time_error)
            sync_status = f"Error: {time_error}"

        return {"system_time": system_time, "sync_status": sync_status}
