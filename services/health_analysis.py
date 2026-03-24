"""
ZFS/Disk/System Health Analysis Service
Performs comprehensive health checks on disks, SMART tests, scrubs, and ZFS pools.
Stores historical analysis results in JSON for review.
"""
import json
import re
import subprocess
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import threading

from services.utils import (
    is_freebsd,
    is_linux,
    is_netbsd,
    run_privileged_command,
    run_zfs_command,
)


class HealthAnalysisService:
    """Service for running and storing ZFS/Disk/System health analysis reports."""

    # Common paths where smartctl may be installed
    COMMON_SMARTCTL_PATHS = [
        "/usr/sbin/smartctl",
        "/usr/bin/smartctl",
        "/usr/local/sbin/smartctl",
        "/usr/local/bin/smartctl",
    ]

    def __init__(self, data_dir: Optional[str] = None):
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            home = Path.home()
            self.data_dir = home / ".config" / "webzfs"

        self.reports_file = self.data_dir / "health_reports.json"
        self._lock = threading.Lock()
        # Maps disk path -> extra smartctl args (e.g. ["-d", "sat"] for USB)
        self._disk_type_map: Dict[str, List[str]] = {}
        self._ensure_data_directory()
        self._initialize_files()
        self.smartctl_path = self._find_smartctl_path()

    # ------------------------------------------------------------------
    # Initialization helpers
    # ------------------------------------------------------------------

    def _ensure_data_directory(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def _initialize_files(self) -> None:
        if not self.reports_file.exists():
            self._write_json(self.reports_file, {"reports": []})

    def _read_json(self, file_path: Path) -> Dict[str, Any]:
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}

    def _write_json(self, file_path: Path, data: Dict[str, Any]) -> None:
        temp_file = file_path.with_suffix(".tmp")
        with open(temp_file, "w") as f:
            json.dump(data, f, indent=2)
        temp_file.replace(file_path)

    def _find_smartctl_path(self) -> Optional[str]:
        path = shutil.which("smartctl")
        if path:
            return path
        for common_path in self.COMMON_SMARTCTL_PATHS:
            if Path(common_path).exists():
                return common_path
        return None

    def _get_smartctl_cmd(self) -> str:
        if not self.smartctl_path:
            self.smartctl_path = self._find_smartctl_path()
        if not self.smartctl_path:
            raise Exception("smartctl not found. Install smartmontools package.")
        return self.smartctl_path

    # ------------------------------------------------------------------
    # Report persistence
    # ------------------------------------------------------------------

    def list_reports(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Return saved health analysis reports, newest first."""
        data = self._read_json(self.reports_file)
        reports = data.get("reports", [])
        reports.sort(key=lambda r: r.get("started_at", ""), reverse=True)
        return reports[:limit]

    @staticmethod
    def _generate_report_id() -> str:
        """Generate a date-based report ID in YYYYMMDD-HHMMSS format."""
        return datetime.now().strftime("%Y%m%d-%H%M%S")

    def get_report(self, report_id: str) -> Optional[Dict[str, Any]]:
        """Return a single report by id (date-based string)."""
        data = self._read_json(self.reports_file)
        for report in data.get("reports", []):
            if str(report["id"]) == str(report_id):
                return report
        return None

    def delete_report(self, report_id: str) -> bool:
        """Delete a report by id (date-based string)."""
        with self._lock:
            data = self._read_json(self.reports_file)
            original_count = len(data.get("reports", []))
            data["reports"] = [r for r in data.get("reports", []) if str(r["id"]) != str(report_id)]
            if len(data["reports"]) < original_count:
                self._write_json(self.reports_file, data)
                return True
            return False

    def _save_report(self, report: Dict[str, Any]) -> str:
        """Persist a report. Returns date-based report id string."""
        with self._lock:
            data = self._read_json(self.reports_file)
            report_id = report.get("id") or self._generate_report_id()
            report["id"] = report_id
            if "reports" not in data:
                data["reports"] = []
            data["reports"].append(report)
            # Keep at most 100 reports
            if len(data["reports"]) > 100:
                data["reports"] = data["reports"][-100:]
            self._write_json(self.reports_file, data)
            return report_id

    def _update_report(self, report_id: str, report: Dict[str, Any]) -> None:
        """Update an existing report in place (used during background runs)."""
        with self._lock:
            data = self._read_json(self.reports_file)
            for i, existing in enumerate(data.get("reports", [])):
                if str(existing.get("id")) == str(report_id):
                    data["reports"][i] = report
                    break
            self._write_json(self.reports_file, data)

    def create_pending_report(
        self,
        check_disk_health: bool = True,
        check_smart_tests: bool = True,
        check_scrubs: bool = True,
        aggressive_hours: bool = False,
    ) -> str:
        """Create a report record with status 'running' and return its date-based id."""
        report = {
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "status": "running",
            "progress": "Initializing...",
            "options": {
                "check_disk_health": check_disk_health,
                "check_smart_tests": check_smart_tests,
                "check_scrubs": check_scrubs,
                "aggressive_hours": aggressive_hours,
            },
            "pools": [],
            "disks": [],
            "smart_test_status": None,
            "scrub_status": None,
            "summary": {
                "total_disks": 0,
                "healthy_disks": 0,
                "warning_disks": 0,
                "critical_disks": 0,
                "total_pools": 0,
            },
        }
        report_id = self._save_report(report)
        return report_id

    def run_analysis_background(self, report_id: str) -> None:
        """Run analysis in the current thread, updating the report incrementally."""
        report = self.get_report(report_id)
        if not report:
            return

        options = report.get("options", {})
        check_disk_health = options.get("check_disk_health", True)
        check_smart_tests = options.get("check_smart_tests", True)
        check_scrubs = options.get("check_scrubs", True)
        aggressive_hours = options.get("aggressive_hours", False)

        try:
            # Step 1: Gather pool info
            report["progress"] = "Gathering pool information..."
            self._update_report(report_id, report)

            report["pools"] = self._gather_pool_info()
            report["summary"]["total_pools"] = len(report["pools"])
            self._update_report(report_id, report)

            # Step 2: List disks
            report["progress"] = "Scanning for disks..."
            self._update_report(report_id, report)

            disk_paths = self._list_disk_paths()

            # Step 2b: Probe pool disks not found by smartctl --scan (USB drives)
            report["progress"] = "Probing for USB-connected pool disks..."
            self._update_report(report_id, report)
            usb_paths = self._probe_pool_disks(report["pools"], disk_paths)
            if usb_paths:
                disk_paths.extend(usb_paths)

            # Step 3: Analyze each disk
            if check_disk_health:
                for idx, disk_path in enumerate(disk_paths):
                    disk_name = disk_path.split("/")[-1]
                    report["progress"] = f"Analyzing disk {idx + 1}/{len(disk_paths)}: {disk_name}"
                    self._update_report(report_id, report)

                    disk_report = self._analyze_disk(disk_path, aggressive_hours)
                    report["disks"].append(disk_report)

                    # Update summary counts incrementally
                    report["summary"]["total_disks"] = len(report["disks"])
                    report["summary"]["healthy_disks"] = sum(
                        1 for d in report["disks"] if d.get("overall_status") == "healthy"
                    )
                    report["summary"]["warning_disks"] = sum(
                        1 for d in report["disks"] if d.get("overall_status") == "warning"
                    )
                    report["summary"]["critical_disks"] = sum(
                        1 for d in report["disks"] if d.get("overall_status") == "critical"
                    )
                    self._update_report(report_id, report)

            # Step 4: SMART test status
            if check_smart_tests:
                report["progress"] = "Checking SMART test scheduling..."
                self._update_report(report_id, report)
                report["smart_test_status"] = self._check_smart_test_status(disk_paths)
                self._update_report(report_id, report)

            # Step 5: Scrub status
            if check_scrubs:
                report["progress"] = "Checking scrub scheduling..."
                self._update_report(report_id, report)
                report["scrub_status"] = self._check_scrub_status()
                self._update_report(report_id, report)

            # Done
            report["completed_at"] = datetime.now().isoformat()
            report["status"] = "completed"
            report["progress"] = "Analysis complete"
            self._update_report(report_id, report)

        except Exception as exc:
            report["completed_at"] = datetime.now().isoformat()
            report["status"] = "error"
            report["progress"] = f"Error: {str(exc)}"
            self._update_report(report_id, report)

    # ------------------------------------------------------------------
    # Main analysis entry point
    # ------------------------------------------------------------------

    def run_analysis(
        self,
        check_disk_health: bool = True,
        check_smart_tests: bool = True,
        check_scrubs: bool = True,
        aggressive_hours: bool = False,
    ) -> Dict[str, Any]:
        """
        Run health analysis with the selected checks.

        Args:
            check_disk_health: Check power-on hours, reallocated sectors, errors, temperature
            check_smart_tests: Verify SMART tests are enabled and have run recently
            check_scrubs: Verify scrubs are enabled and have run recently
            aggressive_hours: Run short test + cancel to determine hours on drives
                              that do not report them in standard attributes

        Returns:
            Complete report dictionary (also saved to disk).
        """
        started_at = datetime.now().isoformat()
        report = {
            "started_at": started_at,
            "completed_at": None,
            "options": {
                "check_disk_health": check_disk_health,
                "check_smart_tests": check_smart_tests,
                "check_scrubs": check_scrubs,
                "aggressive_hours": aggressive_hours,
            },
            "pools": [],
            "disks": [],
            "smart_test_status": None,
            "scrub_status": None,
            "summary": {
                "total_disks": 0,
                "healthy_disks": 0,
                "warning_disks": 0,
                "critical_disks": 0,
                "total_pools": 0,
            },
        }

        # Gather pool topology information
        report["pools"] = self._gather_pool_info()
        report["summary"]["total_pools"] = len(report["pools"])

        # List disks via smartctl --scan
        disk_paths = self._list_disk_paths()

        # Probe pool disks not found by smartctl --scan (USB drives)
        usb_paths = self._probe_pool_disks(report["pools"], disk_paths)
        if usb_paths:
            disk_paths.extend(usb_paths)

        if check_disk_health:
            for disk_path in disk_paths:
                disk_report = self._analyze_disk(disk_path, aggressive_hours)
                report["disks"].append(disk_report)

        report["summary"]["total_disks"] = len(report["disks"])
        report["summary"]["healthy_disks"] = sum(
            1 for d in report["disks"] if d.get("overall_status") == "healthy"
        )
        report["summary"]["warning_disks"] = sum(
            1 for d in report["disks"] if d.get("overall_status") == "warning"
        )
        report["summary"]["critical_disks"] = sum(
            1 for d in report["disks"] if d.get("overall_status") == "critical"
        )

        if check_smart_tests:
            report["smart_test_status"] = self._check_smart_test_status(disk_paths)

        if check_scrubs:
            report["scrub_status"] = self._check_scrub_status()

        report["completed_at"] = datetime.now().isoformat()
        report_id = self._save_report(report)
        report["id"] = report_id
        return report

    # ------------------------------------------------------------------
    # Pool information gathering
    # ------------------------------------------------------------------

    def _build_disk_id_to_device_map(self) -> Dict[str, str]:
        """
        Build a mapping from disk identifiers (by-id names, geom names) to
        /dev/ device paths. This allows correlating zpool status disk names
        (which use by-id identifiers) to smartctl device paths (which use
        /dev/sdX or /dev/nvmeX).

        On Linux: reads /dev/disk/by-id/ symlinks.
        On FreeBSD: uses geom to map disk identifiers.

        Returns:
            Dict mapping identifier strings to resolved /dev/ paths.
            Example: {"ata-ST24000DM001-3Y7103_ZXA13H3T": "/dev/sdc",
                      "nvme-SHPP41-1000GM_SSB6N82831080734T": "/dev/nvme1"}
        """
        mapping: Dict[str, str] = {}

        if is_freebsd():
            # FreeBSD: build mapping from geom disk list and gpt labels
            try:
                result = subprocess.run(
                    ["sysctl", "-n", "kern.disks"],
                    capture_output=True, text=True, check=False,
                )
                if result.returncode == 0:
                    for disk_name in result.stdout.strip().split():
                        mapping[disk_name] = f"/dev/{disk_name}"
            except Exception:
                pass

            # Also check /dev/gptid/ and /dev/diskid/ symlinks
            for id_dir in ["/dev/gptid", "/dev/diskid"]:
                if Path(id_dir).is_dir():
                    try:
                        for entry in Path(id_dir).iterdir():
                            if entry.is_symlink():
                                real = entry.resolve()
                                # Strip partition: /dev/ada0p2 -> /dev/ada0
                                base = self._strip_partition(str(real))
                                mapping[entry.name] = base
                    except Exception:
                        pass

        elif is_netbsd():
            # NetBSD: map wedge names and disk names
            try:
                result = subprocess.run(
                    ["sysctl", "-n", "hw.disknames"],
                    capture_output=True, text=True, check=False,
                )
                if result.returncode == 0:
                    for disk_name in result.stdout.strip().split():
                        mapping[disk_name] = f"/dev/{disk_name}"
            except Exception:
                pass

        else:
            # Linux: read /dev/disk/by-id/ symlinks
            by_id_dir = Path("/dev/disk/by-id")
            if by_id_dir.is_dir():
                try:
                    for entry in by_id_dir.iterdir():
                        if entry.is_symlink():
                            real = entry.resolve()
                            # Strip partition suffix to get base device
                            base = self._strip_partition(str(real))
                            mapping[entry.name] = base
                except Exception:
                    pass

            # Also add /dev/disk/by-path/ for completeness
            by_path_dir = Path("/dev/disk/by-path")
            if by_path_dir.is_dir():
                try:
                    for entry in by_path_dir.iterdir():
                        if entry.is_symlink():
                            real = entry.resolve()
                            base = self._strip_partition(str(real))
                            mapping[entry.name] = base
                except Exception:
                    pass

        return mapping

    @staticmethod
    def _strip_partition(device_path: str) -> str:
        """Strip partition and namespace suffix from a device path to get the
        base device that smartctl uses.
        Examples: /dev/nvme0n1p1 -> /dev/nvme0, /dev/nvme0n1 -> /dev/nvme0,
                  /dev/sda1 -> /dev/sda, /dev/ada0p2 -> /dev/ada0
        On Linux, smartctl --scan reports NVMe controller paths (/dev/nvme0)
        not namespace paths (/dev/nvme0n1), so we strip both partition and
        namespace suffixes.
        """
        # NVMe: strip namespace (nX) and partition (pX) to get controller path
        # /dev/nvme0n1p1 -> /dev/nvme0, /dev/nvme0n1 -> /dev/nvme0
        m = re.match(r"(.*nvme\d+)n\d+(?:p\d+)?$", device_path)
        if m:
            return m.group(1)
        # FreeBSD: ada0p1, da0p1, vtbd0p1
        m = re.match(r"(.*(?:ada|da|vtbd)\d+)(?:p|s)\d*[a-z]?$", device_path)
        if m:
            return m.group(1)
        # NetBSD: wd0a -> wd0
        m = re.match(r"(.*(?:wd|sd|ld)\d+)[a-p]$", device_path)
        if m:
            return m.group(1)
        # Linux: sda1 -> sda, hda1 -> hda
        m = re.match(r"(.*/(?:sd|hd|vd)[a-z]+)\d+$", device_path)
        if m:
            return m.group(1)
        return device_path

    def _gather_pool_info(self) -> List[Dict[str, Any]]:
        """Gather pool topology, creation date, errors from zpool status."""
        pools = []
        # Build device ID mapping once for all pools
        disk_id_map = self._build_disk_id_to_device_map()
        try:
            list_result = run_zfs_command(
                ["zpool", "list", "-H", "-o", "name,size,alloc,free,health"],
                timeout=30,
                check=False,
            )
            if list_result.returncode != 0:
                return pools

            for line in list_result.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                pool_name = parts[0]

                pool_info = {
                    "name": pool_name,
                    "size": parts[1],
                    "alloc": parts[2],
                    "free": parts[3],
                    "health": parts[4],
                    "creation_date": None,
                    "vdevs": [],
                    "errors": {"read": 0, "write": 0, "checksum": 0},
                }

                # Get creation date from zpool history
                pool_info["creation_date"] = self._get_pool_creation_date(pool_name)

                # Get vdev topology and errors from zpool status
                status_data = self._parse_zpool_status(pool_name, disk_id_map)
                pool_info["vdevs"] = status_data.get("vdevs", [])
                pool_info["errors"] = status_data.get("errors", pool_info["errors"])
                pool_info["status_output"] = status_data.get("raw", "")

                pools.append(pool_info)

        except Exception:
            pass
        return pools

    def _get_pool_creation_date(self, pool_name: str) -> Optional[str]:
        """Get pool creation date from zpool history."""
        try:
            result = run_zfs_command(
                ["zpool", "history", pool_name],
                timeout=30,
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                # First non-empty line with a timestamp is typically the creation event
                for line in result.stdout.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if "create" in line.lower() or "import" in line.lower():
                        # Extract date portion (typically at start of line)
                        # Format varies: "2024-01-15.10:30:00 zpool create ..."
                        # or "History for 'tank':\n2024-01-15.10:30:00 ..."
                        date_match = re.search(
                            r"(\d{4}-\d{2}-\d{2})[.\sT](\d{2}:\d{2}:\d{2})", line
                        )
                        if date_match:
                            return f"{date_match.group(1)} {date_match.group(2)}"
                    # Even if not 'create', the first timestamped line is usually creation
                    date_match = re.search(
                        r"(\d{4}-\d{2}-\d{2})[.\sT](\d{2}:\d{2}:\d{2})", line
                    )
                    if date_match:
                        return f"{date_match.group(1)} {date_match.group(2)}"
        except Exception:
            pass
        return None

    def _parse_zpool_status(self, pool_name: str, disk_id_map: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Parse zpool status for vdev topology and disk errors.
        disk_id_map is used to resolve by-id disk names to /dev/ paths."""
        result_data: Dict[str, Any] = {"vdevs": [], "errors": {"read": 0, "write": 0, "checksum": 0}, "raw": ""}
        try:
            result = run_zfs_command(
                ["zpool", "status", pool_name],
                timeout=30,
                check=False,
            )
            if result.returncode != 0:
                return result_data

            raw_output = result.stdout
            result_data["raw"] = raw_output

            in_config = False
            current_vdev = None
            vdevs = []
            vdev_types = {
                "mirror", "raidz", "raidz1", "raidz2", "raidz3",
                "spare", "cache", "log", "dedup", "special",
                "spares", "logs", "caches",
            }

            for line in raw_output.split("\n"):
                stripped = line.strip()

                if stripped.lower().startswith("config:"):
                    in_config = True
                    continue

                if in_config and (stripped.lower().startswith("errors:") or stripped.startswith("---")):
                    in_config = False
                    # Parse error counts from the errors line
                    if stripped.lower().startswith("errors:"):
                        result_data["error_line"] = stripped
                    continue

                if not in_config:
                    continue

                if "NAME" in stripped and "STATE" in stripped:
                    continue

                parts = stripped.split()
                if not parts:
                    continue

                name = parts[0]
                state = parts[1] if len(parts) > 1 else ""
                read_err = parts[2] if len(parts) > 2 else "0"
                write_err = parts[3] if len(parts) > 3 else "0"
                cksum_err = parts[4] if len(parts) > 4 else "0"

                # Skip the pool name line itself
                if name == pool_name:
                    # Accumulate top-level errors
                    result_data["errors"]["read"] += self._safe_int(read_err)
                    result_data["errors"]["write"] += self._safe_int(write_err)
                    result_data["errors"]["checksum"] += self._safe_int(cksum_err)
                    continue

                # Detect vdev groups or individual disks
                name_lower = name.lower()
                base_name = re.sub(r"-\d+$", "", name_lower)

                # dRAID vdev names contain colons (e.g., draid2:8d:32c:2s-0)
                # so prefix matching is needed instead of exact set membership.
                # dRAID spares (e.g., draid2-0-0) do NOT contain colons and
                # should be treated as disks, not vdev groups.
                is_vdev = base_name in vdev_types or (name_lower.startswith("draid") and ":" in name_lower)

                if is_vdev:
                    current_vdev = {
                        "type": name,
                        "state": state,
                        "disks": [],
                    }
                    vdevs.append(current_vdev)
                else:
                    # Resolve disk name to /dev/ path using the mapping
                    resolved_path = None
                    if disk_id_map:
                        resolved_path = disk_id_map.get(name)
                        if not resolved_path:
                            # Try partial match: zpool may truncate long names
                            for map_key, map_val in disk_id_map.items():
                                if map_key.startswith(name) or name.startswith(map_key):
                                    resolved_path = map_val
                                    break
                    disk_entry = {
                        "name": name,
                        "device_path": resolved_path,
                        "state": state,
                        "read_errors": self._safe_int(read_err),
                        "write_errors": self._safe_int(write_err),
                        "checksum_errors": self._safe_int(cksum_err),
                    }
                    if current_vdev is not None:
                        current_vdev["disks"].append(disk_entry)
                    else:
                        # Disk directly under pool (stripe / single)
                        vdevs.append({
                            "type": "stripe",
                            "state": state,
                            "disks": [disk_entry],
                        })

                    # Accumulate errors
                    result_data["errors"]["read"] += disk_entry["read_errors"]
                    result_data["errors"]["write"] += disk_entry["write_errors"]
                    result_data["errors"]["checksum"] += disk_entry["checksum_errors"]

            result_data["vdevs"] = vdevs

        except Exception:
            pass
        return result_data

    # ------------------------------------------------------------------
    # Disk health analysis
    # ------------------------------------------------------------------

    def _list_disk_paths(self) -> List[str]:
        """List disk paths using smartctl --scan and store device type flags."""
        paths = []
        self._disk_type_map = {}
        try:
            smartctl = self._get_smartctl_cmd()
            result = run_privileged_command([smartctl, "--scan"], check=False)
            if result.returncode in (0, 2):  # 2 means some drives have issues
                for line in result.stdout.strip().split("\n"):
                    if not line:
                        continue
                    parts = line.split()
                    if parts:
                        disk_path = parts[0]
                        paths.append(disk_path)
                        # Parse device type flag: "/dev/sdb -d sat # ..."
                        if len(parts) >= 3 and parts[1] == "-d":
                            self._disk_type_map[disk_path] = ["-d", parts[2]]
        except Exception:
            pass
        return paths

    def _probe_pool_disks(self, pools: List[Dict[str, Any]], scanned_paths: List[str]) -> List[str]:
        """Find pool disk device_paths not in the scan results and probe them.
        USB-connected drives often do not appear in smartctl --scan but work
        with the -d sat flag. Returns list of newly discovered paths."""
        scanned_set = set(scanned_paths)
        pool_device_paths = set()
        for pool in pools:
            for vdev in pool.get("vdevs", []):
                for disk in vdev.get("disks", []):
                    dev_path = disk.get("device_path")
                    if dev_path:
                        pool_device_paths.add(dev_path)

        new_paths = []
        smartctl = self._get_smartctl_cmd()
        # Common device types to try for USB bridges
        probe_types = ["sat", "usbcypress", "usbjmicron", "usbsunplus"]

        for dev_path in pool_device_paths:
            if dev_path in scanned_set:
                continue
            # Try each device type until one works
            for dtype in probe_types:
                try:
                    result = run_privileged_command(
                        [smartctl, "-d", dtype, "-i", dev_path],
                        check=False,
                    )
                    # Exit status 0 or 2 (some info found) means success
                    if result.returncode in (0, 2) and "INFORMATION SECTION" in result.stdout:
                        new_paths.append(dev_path)
                        self._disk_type_map[dev_path] = ["-d", dtype]
                        break
                except Exception:
                    pass

        return new_paths

    def _analyze_disk(self, disk_path: str, aggressive_hours: bool = False) -> Dict[str, Any]:
        """Run full health analysis on a single disk."""
        disk_report: Dict[str, Any] = {
            "path": disk_path,
            "name": disk_path.split("/")[-1],
            "model": "Unknown",
            "serial": "Unknown",
            "overall_status": "healthy",
            "flags": [],
            "power_on_hours": None,
            "power_on_hours_source": None,
            "smart_test_hours": None,
            "reallocated_sectors": None,
            "uncorrectable_errors": None,
            "reported_uncorrectable": None,
            "failed_smart_test": False,
            "over_temperature": False,
            "temperature_info": None,
            "raw_output": "",
        }

        try:
            smartctl = self._get_smartctl_cmd()
            # Build smartctl command with device type if known (e.g. USB drives)
            cmd = [smartctl]
            type_args = self._disk_type_map.get(disk_path, [])
            if type_args:
                cmd.extend(type_args)
            cmd.extend(["-x", disk_path])
            # Get extended SMART data
            result = run_privileged_command(cmd, check=False)
            raw_output = result.stdout
            disk_report["raw_output"] = raw_output

            # Parse device info
            disk_report["model"] = self._extract_field(raw_output, "Device Model") or \
                                   self._extract_field(raw_output, "Model Number") or \
                                   self._extract_field(raw_output, "Product") or "Unknown"
            disk_report["serial"] = self._extract_field(raw_output, "Serial Number") or \
                                    self._extract_field(raw_output, "Serial number") or "Unknown"

            # ---- Power-On Hours ----
            hours = self._extract_power_on_hours(raw_output)
            if hours is not None:
                disk_report["power_on_hours"] = hours
                disk_report["power_on_hours_source"] = "attribute"
            else:
                disk_report["power_on_hours_source"] = "none"

            # ---- Self-test log hours (check for wrap-around) ----
            test_hours = self._extract_latest_test_hours(raw_output)
            if test_hours is not None:
                disk_report["smart_test_hours"] = test_hours
                # Check wrap-around: if attribute hours < test hours, add 65535
                if disk_report["power_on_hours"] is not None:
                    if disk_report["power_on_hours"] < test_hours:
                        disk_report["power_on_hours"] += 65535
                        disk_report["power_on_hours_source"] = "attribute+wraparound"
            else:
                disk_report["smart_test_hours"] = None

            # Aggressive hours determination
            if aggressive_hours and disk_report["power_on_hours"] is None:
                aggressive_result = self._aggressive_determine_hours(disk_path)
                if aggressive_result is not None:
                    disk_report["power_on_hours"] = aggressive_result
                    disk_report["power_on_hours_source"] = "aggressive_test"

            # ---- Reallocated Sectors ----
            reallocated = self._extract_attribute_raw(raw_output, "Reallocated_Sector_Ct")
            if reallocated is not None:
                disk_report["reallocated_sectors"] = reallocated
                if reallocated > 0:
                    disk_report["flags"].append("Reallocated sectors detected")
                    self._escalate_status(disk_report, "warning")

            # ---- Uncorrectable Errors (Error counter log) ----
            uncorrectable = self._extract_total_uncorrected_errors(raw_output)
            if uncorrectable is not None and uncorrectable > 0:
                disk_report["uncorrectable_errors"] = uncorrectable
                disk_report["flags"].append("Uncorrectable errors in error counter log")
                self._escalate_status(disk_report, "critical")

            # ---- Reported Uncorrectable Errors (SMART attribute) ----
            reported = self._extract_attribute_raw(raw_output, "Reported_Uncorrect")
            if reported is None:
                # Try alternate name
                reported = self._extract_reported_uncorrectable(raw_output)
            if reported is not None and reported > 0:
                disk_report["reported_uncorrectable"] = reported
                disk_report["flags"].append("Reported uncorrectable errors")
                self._escalate_status(disk_report, "critical")

            # ---- Failed SMART test ----
            if self._check_failed_smart_test(raw_output):
                disk_report["failed_smart_test"] = True
                disk_report["flags"].append("Failed SMART self-test detected")
                self._escalate_status(disk_report, "critical")

            # ---- Temperature ----
            temp_info = self._extract_temperature_info(raw_output)
            disk_report["temperature_info"] = temp_info
            if temp_info and temp_info.get("over_temp"):
                disk_report["over_temperature"] = True
                disk_report["flags"].append("Over-temperature condition detected")
                self._escalate_status(disk_report, "warning")

        except Exception as exc:
            disk_report["flags"].append(f"Error analyzing disk: {str(exc)}")
            self._escalate_status(disk_report, "warning")

        return disk_report

    # ------------------------------------------------------------------
    # Power-on hours extraction
    # ------------------------------------------------------------------

    def _extract_power_on_hours(self, raw_output: str) -> Optional[int]:
        """
        Extract power-on hours from SMART output.
        Tries multiple formats:
          - Power_On_Hours attribute
          - 'Power-on hours:' line
          - 'Accumulated power on time, hours:minutes' line
        """
        # ATA-style attribute
        for line in raw_output.split("\n"):
            if "Power_On_Hours" in line:
                parts = line.split()
                # Attribute table format varies by drive:
                # 10-col: ID ATTR FLAG VALUE WORST THRESH TYPE UPDATED WHEN_FAILED RAW
                # 8-col:  ID ATTR FLAG VALUE WORST THRESH FAIL RAW
                # Find the raw value: last token that looks numeric
                if len(parts) >= 8:
                    # The raw value is the last column (may contain sub-values)
                    # Try from the end to find a numeric value
                    for raw_idx in (9, 7, -1):
                        if raw_idx >= len(parts):
                            continue
                        try:
                            raw_val = parts[raw_idx]
                            numeric = re.sub(r"[^0-9]", "", raw_val.split("+")[0])
                            if numeric:
                                return int(numeric)
                        except (ValueError, IndexError):
                            pass

        # SAS / NVMe style
        for line in raw_output.split("\n"):
            lower_line = line.lower().strip()
            if "power-on hours:" in lower_line or "power on hours:" in lower_line:
                match = re.search(r"(\d[\d,]*)", line.split(":")[-1])
                if match:
                    return int(match.group(1).replace(",", ""))

        # SAS accumulated power on time
        for line in raw_output.split("\n"):
            if "accumulated power on time" in line.lower():
                match = re.search(r"(\d[\d,]*)", line)
                if match:
                    return int(match.group(1).replace(",", ""))

        return None

    def _extract_latest_test_hours(self, raw_output: str) -> Optional[int]:
        """
        Extract the latest self-test lifetime hours from SMART self-test log.
        Handles both ATA and NVMe log formats.
        Also checks for wrap-around: if latest hours < any previous, add 65535.
        Returns the corrected hours value, or None if no tests found.

        ATA format (lines start with '#'):
          # 1  Short offline       Completed without error  00%  58258  -
        NVMe format (lines start with number, header has Power_on_Hours):
          0   Short             Completed without error               15004  -  -  -  -  -
        """
        test_hours_list = []

        # Detect which format we are dealing with
        in_selftest_section = False
        is_nvme_format = False
        power_on_col_index = None

        lines = raw_output.split("\n")

        for line in lines:
            stripped = line.strip()

            # Detect start of self-test log section
            if "self-test log" in stripped.lower() or "Self-test Log" in stripped:
                in_selftest_section = True
                continue

            # Detect NVMe header: "Num  Test_Description  Status  Power_on_Hours ..."
            if in_selftest_section and "Power_on_Hours" in stripped:
                is_nvme_format = True
                # Find the column index of Power_on_Hours by header position
                header_parts = stripped.split()
                for idx, col in enumerate(header_parts):
                    if col == "Power_on_Hours":
                        power_on_col_index = idx
                        break
                continue

            # Detect ATA header: "Num  Test_Description  Status  Remaining  LifeTime(hours) ..."
            if in_selftest_section and "LifeTime" in stripped:
                continue

            # End of self-test section (blank line or new section header)
            if in_selftest_section and stripped and not stripped[0].isdigit() and not stripped.startswith("#"):
                # Allow lines like "Self-test status: No self-test in progress"
                if "self-test" in stripped.lower() or "No self-test" in stripped:
                    continue
                # Non-data line after header, could be end of section
                if test_hours_list:
                    break
                continue

            if not in_selftest_section:
                # Also try to catch ATA entries outside detected section
                if stripped.startswith("#"):
                    in_selftest_section = True
                else:
                    continue

            # ---- Parse ATA format: lines starting with '#' ----
            if stripped.startswith("#"):
                parts = stripped.split()
                for i, p in enumerate(parts):
                    if p.endswith("%"):
                        # Next value after percentage is lifetime hours
                        if i + 1 < len(parts):
                            try:
                                hours = int(parts[i + 1])
                                test_hours_list.append(hours)
                            except ValueError:
                                pass
                        break
                continue

            # ---- Parse NVMe format: lines starting with a digit ----
            if is_nvme_format and stripped and stripped[0].isdigit():
                parts = stripped.split()
                # The NVMe header columns:
                # Num  Test_Description  Status(multi-word)  Power_on_Hours  Failing_LBA  NSID  Seg  SCT  Code
                # Since Status can be multi-word (e.g. "Completed without error"),
                # we cannot simply index by position. Instead, scan for the first
                # integer value that is plausible as hours (>0 and not the Num field).
                # Strategy: skip the first element (Num), skip test description words,
                # then find the first standalone integer that looks like hours.
                found_hours = False
                for i in range(1, len(parts)):
                    p = parts[i]
                    # Skip non-numeric tokens (test description and status words)
                    if not p.isdigit():
                        continue
                    # First numeric value after Num that is reasonably large is Power_on_Hours
                    # (Num is index 0, hours should be > single digit test num)
                    val = int(p)
                    # Hours is typically much larger than the test number
                    # The Num field (index 0) is already skipped by starting at 1
                    test_hours_list.append(val)
                    found_hours = True
                    break

        if not test_hours_list:
            return None

        latest_hours = test_hours_list[0]
        max_hours = max(test_hours_list)

        # Check for wrap-around
        if latest_hours < max_hours:
            latest_hours += 65535

        return latest_hours

    def _aggressive_determine_hours(self, disk_path: str) -> Optional[int]:
        """
        Aggressively determine power-on hours by running a short test,
        immediately canceling it, and reading the aborted test entry.
        WARNING: Creates a permanent entry in the SMART log.
        """
        try:
            smartctl = self._get_smartctl_cmd()

            # Start short test
            run_privileged_command(
                [smartctl, "-t", "short", disk_path],
                check=False,
            )

            # Immediately abort
            run_privileged_command(
                [smartctl, "-X", disk_path],
                check=False,
            )

            # Read extended info and look for the aborted/completed entry
            result = run_privileged_command(
                [smartctl, "-x", disk_path],
                check=False,
            )

            # Look for the latest test entry (should be Aborted or Completed)
            hours_list = []
            for line in result.stdout.split("\n"):
                stripped = line.strip()
                if not stripped.startswith("#"):
                    continue
                if "Aborted" in stripped or "Completed" in stripped:
                    parts = stripped.split()
                    for i, p in enumerate(parts):
                        if p.endswith("%"):
                            if i + 1 < len(parts):
                                try:
                                    hours = int(parts[i + 1])
                                    hours_list.append(hours)
                                except ValueError:
                                    pass
                            break

            if hours_list:
                latest = hours_list[0]
                max_val = max(hours_list)
                if latest < max_val:
                    latest += 65535
                return latest

        except Exception:
            pass
        return None

    # ------------------------------------------------------------------
    # SMART attribute extraction helpers
    # ------------------------------------------------------------------

    def _extract_field(self, raw_output: str, field_name: str) -> Optional[str]:
        """Extract a key: value field from smartctl output."""
        for line in raw_output.split("\n"):
            if field_name in line and ":" in line:
                return line.split(":", 1)[1].strip()
        return None

    def _extract_attribute_raw(self, raw_output: str, attr_name: str) -> Optional[int]:
        """Extract the raw value of a named SMART attribute."""
        for line in raw_output.split("\n"):
            if attr_name in line:
                parts = line.split()
                if len(parts) >= 10:
                    try:
                        return int(parts[9])
                    except (ValueError, IndexError):
                        pass
        return None

    def _extract_total_uncorrected_errors(self, raw_output: str) -> Optional[int]:
        """
        Parse the SAS-style error counter log for total uncorrected errors.
        Looks for 'read:', 'write:', 'verify:' lines and sums the last column.
        """
        total = 0
        found = False
        in_error_counter = False

        for line in raw_output.split("\n"):
            stripped = line.strip()
            if "error counter log" in stripped.lower():
                in_error_counter = True
                continue

            if in_error_counter:
                if stripped.startswith("read:") or stripped.startswith("write:") or stripped.startswith("verify:"):
                    parts = stripped.split()
                    if parts:
                        try:
                            uncorrected = int(parts[-1])
                            total += uncorrected
                            found = True
                        except ValueError:
                            pass
                elif stripped and not stripped.startswith("Errors") and not stripped.startswith("ECC"):
                    # If we hit a line that is not part of the table, stop
                    if found:
                        break

        return total if found else None

    def _extract_reported_uncorrectable(self, raw_output: str) -> Optional[int]:
        """Extract 'Number of Reported Uncorrectable Errors' from output."""
        for line in raw_output.split("\n"):
            if "reported uncorrectable" in line.lower():
                match = re.search(r"(\d+)", line.split(":")[-1] if ":" in line else line)
                if match:
                    return int(match.group(1))
        return None

    def _check_failed_smart_test(self, raw_output: str) -> bool:
        """Check if any SMART self-test has failed.
        Handles ATA (lines start with '#'), NVMe (lines start with digit),
        and SAS (lines with 'Background long/short') formats."""
        in_selftest_section = False
        found_selftest_header = False
        for line in raw_output.split("\n"):
            stripped = line.strip()

            # Detect self-test log section
            if "self-test log" in stripped.lower() or "Self-test Log" in stripped:
                in_selftest_section = True
                found_selftest_header = False
                continue

            # Reset section flag on blank lines (section boundary)
            if in_selftest_section and not stripped:
                # Only reset after we've seen actual test data or header
                if found_selftest_header:
                    in_selftest_section = False
                continue

            # Detect NVMe/ATA data header within section
            if in_selftest_section and ("Power_on_Hours" in stripped or "LifeTime" in stripped or "Test_Description" in stripped):
                found_selftest_header = True
                continue

            # ATA format: lines start with '#'
            if stripped.startswith("#"):
                lower = stripped.lower()
                if "failed" in lower and "segment" not in lower:
                    return True
                if "failed in segment" in lower:
                    return True

            # NVMe format: lines start with a digit in self-test section
            if in_selftest_section and found_selftest_header and stripped and stripped[0].isdigit():
                lower = stripped.lower()
                if "failed" in lower:
                    return True

            # SAS-style
            if "Background long" in line and "Failed" in line:
                return True
            if "Background short" in line and "Failed" in line:
                return True
        return False

    def _extract_temperature_info(self, raw_output: str) -> Optional[Dict[str, Any]]:
        """Extract temperature and over-temp information."""
        info: Dict[str, Any] = {"current": None, "over_temp": False, "over_temp_count": None}

        # ATA Temperature_Celsius attribute
        for line in raw_output.split("\n"):
            if "Temperature_Celsius" in line or "Airflow_Temperature" in line:
                parts = line.split()
                if len(parts) >= 10:
                    try:
                        # Raw value might be "35 (Min/Max 20/45)"
                        raw_str = parts[9]
                        info["current"] = int(re.match(r"(\d+)", raw_str).group(1))
                    except (ValueError, AttributeError):
                        pass

        # NVMe / SAS current temperature
        for line in raw_output.split("\n"):
            lower = line.lower().strip()
            if "current temperature:" in lower or "temperature:" in lower:
                match = re.search(r"(\d+)", line.split(":")[-1])
                if match and info["current"] is None:
                    info["current"] = int(match.group(1))

        # Over temperature indicators
        for line in raw_output.split("\n"):
            lower = line.lower()
            if "time in over-temperature" in lower:
                match = re.search(r"(\d+)", line)
                if match:
                    count = int(match.group(1))
                    if count > 0:
                        info["over_temp"] = True
                        info["over_temp_count"] = count

            if "over temperature limit count" in lower or "over/under temperature" in lower:
                # "Under/Over Temperature Limit Count:  25424/0"
                match = re.search(r"(\d+)\s*/\s*(\d+)", line)
                if match:
                    over = int(match.group(1))
                    if over > 0:
                        info["over_temp"] = True
                        info["over_temp_count"] = over

        if info["current"] is None and not info["over_temp"]:
            return None
        return info

    def _escalate_status(self, disk_report: Dict[str, Any], new_status: str) -> None:
        """Escalate disk status: healthy -> warning -> critical."""
        severity = {"healthy": 0, "warning": 1, "critical": 2}
        current = disk_report.get("overall_status", "healthy")
        if severity.get(new_status, 0) > severity.get(current, 0):
            disk_report["overall_status"] = new_status

    # ------------------------------------------------------------------
    # SMART test scheduling verification
    # ------------------------------------------------------------------

    def _check_smart_test_status(self, disk_paths: List[str]) -> Dict[str, Any]:
        """
        Check whether SMART self-tests are enabled and have run in the last 30 days.
        """
        status = {
            "smartd_running": False,
            "smartd_config_exists": False,
            "disks": [],
        }

        # Check if smartd is running
        status["smartd_running"] = self._is_smartd_running()

        # Check if smartd.conf exists
        smartd_conf = Path("/etc/smartd.conf")
        status["smartd_config_exists"] = smartd_conf.exists()

        # For each disk, check if tests have run recently
        thirty_days_ago_hours = None
        for disk_path in disk_paths:
            disk_status = {
                "path": disk_path,
                "tests_configured": False,
                "last_test_within_30_days": False,
                "last_test_hours": None,
                "current_hours": None,
            }

            try:
                smartctl = self._get_smartctl_cmd()
                cmd = [smartctl]
                type_args = self._disk_type_map.get(disk_path, [])
                if type_args:
                    cmd.extend(type_args)
                cmd.extend(["-x", disk_path])
                result = run_privileged_command(cmd, check=False)
                raw = result.stdout

                # Get current power-on hours
                current_hours = self._extract_power_on_hours(raw)
                disk_status["current_hours"] = current_hours

                # Get latest test hours
                test_hours = self._extract_latest_test_hours(raw)
                disk_status["last_test_hours"] = test_hours

                # Determine if test ran within last 30 days
                # 30 days = 720 hours
                if current_hours is not None and test_hours is not None:
                    if (current_hours - test_hours) <= 720:
                        disk_status["last_test_within_30_days"] = True

                # Check if this disk has tests configured in smartd.conf
                if status["smartd_config_exists"]:
                    try:
                        with open(smartd_conf, "r") as f:
                            config_content = f.read()
                        disk_name = disk_path.split("/")[-1]
                        if disk_path in config_content or disk_name in config_content or "DEVICESCAN" in config_content:
                            disk_status["tests_configured"] = True
                    except Exception:
                        pass

            except Exception:
                pass

            status["disks"].append(disk_status)

        return status

    def _is_smartd_running(self) -> bool:
        """Check if smartd daemon is running."""
        if is_freebsd():
            try:
                result = subprocess.run(
                    ["service", "smartd", "status"],
                    capture_output=True, text=True, check=False,
                )
                return result.returncode == 0
            except Exception:
                return False

        # Linux
        try:
            result = subprocess.run(
                ["systemctl", "is-active", "smartd"],
                capture_output=True, text=True, check=False,
            )
            return result.stdout.strip() == "active"
        except FileNotFoundError:
            try:
                result = subprocess.run(
                    ["service", "smartd", "status"],
                    capture_output=True, text=True, check=False,
                )
                return result.returncode == 0
            except Exception:
                return False

    # ------------------------------------------------------------------
    # Scrub status verification
    # ------------------------------------------------------------------

    def _check_scrub_status(self) -> Dict[str, Any]:
        """Check whether scrubs are scheduled and have run in the last 30 days."""
        status = {
            "pools": [],
            "scrub_cron_configured": False,
        }

        # Check if scrub cron jobs exist
        status["scrub_cron_configured"] = self._check_scrub_cron()

        # Check each pool for last scrub
        try:
            list_result = run_zfs_command(
                ["zpool", "list", "-H", "-o", "name"],
                timeout=15,
                check=False,
            )
            if list_result.returncode != 0:
                return status

            for pool_name in list_result.stdout.strip().split("\n"):
                pool_name = pool_name.strip()
                if not pool_name:
                    continue

                pool_scrub = {
                    "name": pool_name,
                    "last_scrub": None,
                    "scrub_within_30_days": False,
                    "scrub_in_progress": False,
                }

                try:
                    result = run_zfs_command(
                        ["zpool", "status", pool_name],
                        timeout=15,
                        check=False,
                    )
                    if result.returncode == 0:
                        scrub_info = self._parse_scrub_info(result.stdout)
                        pool_scrub.update(scrub_info)
                except Exception:
                    pass

                status["pools"].append(pool_scrub)

        except Exception:
            pass

        return status

    def _check_scrub_cron(self) -> bool:
        """Check if scrub cron jobs are configured."""
        # Check WebZFS scrub schedules file
        schedules_file = self.data_dir / "scrub_schedules.json"
        if schedules_file.exists():
            try:
                with open(schedules_file, "r") as f:
                    data = json.load(f)
                schedules = data.get("schedules", [])
                if any(s.get("enabled", False) for s in schedules):
                    return True
            except Exception:
                pass

        # Check system crontab for scrub entries
        cron_paths = ["/etc/crontab", "/etc/cron.d/zfs-scrub", "/etc/cron.d/zfsutils-linux"]
        for cron_path in cron_paths:
            try:
                if Path(cron_path).exists():
                    with open(cron_path, "r") as f:
                        content = f.read()
                    if "scrub" in content.lower() and "zpool" in content.lower():
                        return True
            except Exception:
                pass

        return False

    def _parse_scrub_info(self, status_output: str) -> Dict[str, Any]:
        """Parse scrub information from zpool status output."""
        info: Dict[str, Any] = {
            "last_scrub": None,
            "scrub_within_30_days": False,
            "scrub_in_progress": False,
        }

        for line in status_output.split("\n"):
            stripped = line.strip()

            if "scrub in progress" in stripped.lower():
                info["scrub_in_progress"] = True

            if "scan:" in stripped.lower() or "scrub" in stripped.lower():
                # Look for "scrub repaired ... on Sun Mar  2 ..."
                # or "scrub repaired ... on 2025-03-02T10:00:00"
                date_match = re.search(
                    r"on\s+(\w+\s+\w+\s+\d+\s+[\d:]+\s+\d{4})", stripped
                )
                if date_match:
                    try:
                        date_str = date_match.group(1)
                        scrub_date = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
                        info["last_scrub"] = scrub_date.isoformat()
                        days_ago = (datetime.now() - scrub_date).days
                        info["scrub_within_30_days"] = days_ago <= 30
                    except ValueError:
                        pass

                # ISO format dates
                iso_match = re.search(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})", stripped)
                if iso_match and info["last_scrub"] is None:
                    try:
                        date_str = iso_match.group(1).replace("T", " ")
                        scrub_date = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                        info["last_scrub"] = scrub_date.isoformat()
                        days_ago = (datetime.now() - scrub_date).days
                        info["scrub_within_30_days"] = days_ago <= 30
                    except ValueError:
                        pass

        return info

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_int(value: str) -> int:
        """Convert string to int, returning 0 on failure."""
        try:
            # Handle values like "1.23K" or similar
            cleaned = re.sub(r"[^0-9]", "", value)
            return int(cleaned) if cleaned else 0
        except (ValueError, TypeError):
            return 0

    def get_disk_count(self) -> int:
        """Return number of disks detected by smartctl --scan."""
        return len(self._list_disk_paths())
