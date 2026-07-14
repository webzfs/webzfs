"""
Per-Dataset I/O Statistics Service

Reports ZFS I/O activity at dataset granularity, similar to the zfs-iostat
command line tool. It reads ZFS dataset kstats and computes read/write
operation and bandwidth rates the same way zpool iostat does (per interval
deltas between two samples taken a short time apart).

Three views are supported:

- default: a table of every dataset with its I/O rates
- top: the same data sorted by total bandwidth (busiest datasets first)
- files: the open files under each dataset (which processes are using it)

Platform support:
- Linux reads the objset kstats under /proc/spl/kstat/zfs
- FreeBSD reads the kstat.zfs sysctl tree

The dataset filter works like the CLI tool: an exact dataset name, or with
"include children" enabled it also matches every dataset nested beneath it.
"""
import glob
import os
import time
import subprocess
from typing import Dict, List, Any, Optional

from services.utils import (
    run_zfs_command,
    run_privileged_command,
    is_freebsd,
    is_netbsd,
    is_linux,
)


# Location of the ZFS dataset kstats on Linux.
KSTAT_BASE = "/proc/spl/kstat/zfs"

# Marker that separates the pool name from the objset node in a FreeBSD
# sysctl name such as kstat.zfs.tank.dataset.objset-0x36.writes.
SYSCTL_DATASET_MARKER = ".dataset."

# Number of seconds between the two samples used to compute interval rates.
SAMPLE_INTERVAL = 1.0

UNIT_SUFFIXES = ["", "K", "M", "G", "T", "P"]


class DatasetIostatService:
    """Collect and format per-dataset I/O statistics."""

    def get_stats(
        self,
        dataset_filter: Optional[str] = None,
        include_children: bool = False,
        mode: str = "default",
        sort_column: str = "total",
    ) -> Dict[str, Any]:
        """
        Collect per-dataset I/O statistics.

        Args:
            dataset_filter: Optional dataset name to filter on. Empty or None
                returns every dataset.
            include_children: When True, also include datasets nested beneath
                the filtered dataset.
            mode: "default", "top", or "files".
            sort_column: For the top view, one of read, write, rops, wops,
                total (default total).

        Returns:
            A dictionary with keys:
            - platform: 'linux', 'freebsd', or 'netbsd'
            - mode: the requested mode
            - datasets: list of dataset rate records (default/top modes)
            - files: list of open-file records (files mode)
            - notes: informational or error messages
            - available: False if kstats could not be read on this platform
        """
        if is_netbsd():
            platform_name = "netbsd"
        elif is_freebsd():
            platform_name = "freebsd"
        else:
            platform_name = "linux"

        result: Dict[str, Any] = {
            "platform": platform_name,
            "mode": mode,
            "datasets": [],
            "files": [],
            "notes": [],
            "available": True,
        }

        if mode == "files":
            self._collect_files(dataset_filter, include_children, result)
            return result

        rates = self._collect_rates(result)
        if not result["available"]:
            return result

        filtered = [
            r for r in rates
            if self._dataset_matches(
                r["dataset"], dataset_filter, include_children
            )
        ]

        if mode == "top":
            filtered.sort(
                key=lambda r: self._sort_value(r, sort_column),
                reverse=True,
            )
        else:
            filtered.sort(key=lambda r: r["dataset"])

        result["datasets"] = filtered
        if not filtered:
            result["notes"].append(
                "No datasets matched the current filter, or no I/O activity "
                "was recorded during the sample interval."
            )
        return result

    # ------------------------------------------------------------------
    # Rate collection
    # ------------------------------------------------------------------

    def _collect_rates(self, result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Take two kstat samples one interval apart and compute per-dataset
        rates from the deltas.
        """
        first = self._collect_samples(result)
        if not result["available"]:
            return []
        start_time = time.monotonic()
        time.sleep(SAMPLE_INTERVAL)
        second = self._collect_samples(result)
        elapsed = time.monotonic() - start_time
        if elapsed <= 0:
            elapsed = SAMPLE_INTERVAL

        rates: List[Dict[str, Any]] = []
        for key, new_sample in second.items():
            old_sample = first.get(key)
            if old_sample is None:
                continue

            def rate(name: str) -> float:
                delta = (new_sample["counters"].get(name, 0)
                         - old_sample["counters"].get(name, 0))
                if delta < 0:
                    delta = 0  # counter reset (dataset remounted)
                return delta / elapsed

            read_ops = rate("reads")
            write_ops = rate("writes")
            read_bytes = rate("nread")
            write_bytes = rate("nwritten")
            rates.append({
                "dataset": new_sample["dataset_name"],
                "read_ops": read_ops,
                "write_ops": write_ops,
                "read_bytes": read_bytes,
                "write_bytes": write_bytes,
                "read_ops_h": self._humanize(read_ops),
                "write_ops_h": self._humanize(write_ops),
                "read_bw_h": self._humanize(read_bytes),
                "write_bw_h": self._humanize(write_bytes),
            })
        return rates

    def _collect_samples(self, result: Dict[str, Any]) -> Dict[tuple, Dict]:
        """Read every dataset kstat once for the current platform."""
        if is_freebsd() or is_netbsd():
            return self._collect_samples_freebsd(result)
        return self._collect_samples_linux(result)

    def _collect_samples_linux(
        self, result: Dict[str, Any]
    ) -> Dict[tuple, Dict]:
        """Read the objset kstat files under /proc/spl/kstat/zfs."""
        samples: Dict[tuple, Dict] = {}
        if not os.path.isdir(KSTAT_BASE):
            result["available"] = False
            result["notes"].append(
                "ZFS dataset kstats were not found at "
                f"{KSTAT_BASE}. Is the ZFS kernel module loaded?"
            )
            return samples
        paths = glob.glob(os.path.join(KSTAT_BASE, "*", "objset-0x*"))
        for path in paths:
            sample = self._parse_objset_file(path)
            if sample is not None:
                key = (sample["pool_name"], sample["objset_id"])
                samples[key] = sample
        return samples

    def _parse_objset_file(self, path: str) -> Optional[Dict]:
        """Parse one objset kstat file into a sample dictionary."""
        try:
            with open(path, "r") as kstat_file:
                lines = kstat_file.read().splitlines()
        except (FileNotFoundError, PermissionError, OSError):
            return None
        if len(lines) < 3:
            return None
        counters: Dict[str, int] = {}
        dataset_name = None
        for line in lines[2:]:
            parts = line.split(None, 2)
            if len(parts) != 3:
                continue
            name, kstat_type, value = parts
            if name == "dataset_name":
                dataset_name = value
            elif kstat_type == "4":
                try:
                    counters[name] = int(value)
                except ValueError:
                    continue
        if dataset_name is None:
            return None
        pool_name = os.path.basename(os.path.dirname(path))
        objset_id = os.path.basename(path).replace("objset-", "", 1)
        return {
            "objset_id": objset_id,
            "pool_name": pool_name,
            "dataset_name": dataset_name,
            "counters": counters,
        }

    def _collect_samples_freebsd(
        self, result: Dict[str, Any]
    ) -> Dict[tuple, Dict]:
        """Read the kstat.zfs sysctl tree on FreeBSD/NetBSD."""
        samples: Dict[tuple, Dict] = {}
        try:
            completed = run_privileged_command(
                ["sysctl", "kstat.zfs"],
                check=False,
                timeout=15,
            )
            text = completed.stdout or ""
        except FileNotFoundError:
            result["available"] = False
            result["notes"].append(
                "The sysctl command is not available, so dataset kstats "
                "could not be read."
            )
            return samples
        except subprocess.TimeoutExpired:
            result["available"] = False
            result["notes"].append(
                "Reading the ZFS kstat sysctl tree timed out."
            )
            return samples
        except Exception as exc:
            result["available"] = False
            result["notes"].append(f"Could not read ZFS kstats: {exc}")
            return samples

        grouped: Dict[tuple, Dict] = {}
        for line in text.splitlines():
            separator = line.find(": ")
            if separator < 0:
                continue
            full_name = line[:separator]
            value = line[separator + 2:]
            marker = full_name.find(SYSCTL_DATASET_MARKER)
            if marker < 0:
                continue
            prefix = full_name[:marker]
            remainder = full_name[marker + len(SYSCTL_DATASET_MARKER):]
            if not prefix.startswith("kstat.zfs."):
                continue
            pool_name = prefix[len("kstat.zfs."):]
            if not remainder.startswith("objset-"):
                continue
            dot = remainder.find(".")
            if dot < 0:
                continue
            objset_id = remainder[len("objset-"):dot]
            counter_name = remainder[dot + 1:]
            entry_key = (pool_name, objset_id)
            node = grouped.setdefault(
                entry_key, {"dataset_name": None, "counters": {}}
            )
            if counter_name == "dataset_name":
                node["dataset_name"] = value
            else:
                try:
                    node["counters"][counter_name] = int(value)
                except ValueError:
                    continue

        if not grouped:
            result["available"] = False
            result["notes"].append(
                "No ZFS dataset kstats were found. Is the ZFS module loaded?"
            )
            return samples

        for (pool_name, objset_id), node in grouped.items():
            if node["dataset_name"] is None:
                continue
            samples[(pool_name, objset_id)] = {
                "objset_id": objset_id,
                "pool_name": pool_name,
                "dataset_name": node["dataset_name"],
                "counters": node["counters"],
            }
        return samples

    # ------------------------------------------------------------------
    # Files view
    # ------------------------------------------------------------------

    def _collect_files(
        self,
        dataset_filter: Optional[str],
        include_children: bool,
        result: Dict[str, Any],
    ) -> None:
        """
        List open files per dataset by mapping the open files under each
        dataset mountpoint back to the owning dataset.
        """
        mount_map = self._get_mount_map()
        if not mount_map:
            result["notes"].append(
                "No mounted ZFS datasets were found to inspect for open files."
            )
            return

        selected = [
            (dataset, mountpoint)
            for dataset, mountpoint in mount_map
            if self._dataset_matches(dataset, dataset_filter, include_children)
        ]
        if not selected:
            result["notes"].append(
                "No mounted datasets matched the current filter."
            )
            return

        if is_freebsd() or is_netbsd():
            self._collect_files_bsd(selected, result)
        else:
            self._collect_files_linux(selected, result)

        if not result["files"]:
            result["notes"].append(
                "No open files were found on the matching datasets."
            )

    def _get_mount_map(self) -> List[tuple]:
        """Return a list of (dataset_name, mountpoint) for mounted datasets."""
        entries: List[tuple] = []
        try:
            completed = run_zfs_command(
                ["zfs", "list", "-H", "-o", "name,mountpoint,mounted"],
                check=False,
                timeout=15,
            )
            if completed.returncode != 0:
                return entries
            for line in completed.stdout.strip().split("\n"):
                fields = line.split("\t")
                if len(fields) < 3:
                    continue
                name, mountpoint, mounted = fields[0], fields[1], fields[2]
                if mounted != "yes":
                    continue
                if not mountpoint.startswith("/"):
                    continue
                entries.append((name, mountpoint))
        except Exception:
            return entries
        return entries

    def _collect_files_linux(
        self, selected: List[tuple], result: Dict[str, Any]
    ) -> None:
        """
        Use lsof to list open files on each selected dataset.

        The mountpoints are passed to lsof as filesystem arguments in a single
        call. lsof recognizes a mount point and lists only the files open on
        that filesystem, which is fast. This avoids the "+D" option, which
        forces lsof to walk the entire directory tree and can take far longer
        than the request timeout on large datasets.

        Each returned file is assigned to the dataset whose mountpoint is the
        longest matching prefix of the file path, so files land under the
        correct child dataset.
        """
        seen = set()
        # Sort mountpoints longest first so the most specific (child dataset)
        # mountpoint wins when matching a returned file path.
        mount_lookup = sorted(selected, key=lambda item: len(item[1]), reverse=True)
        mountpoints = [mountpoint for _dataset, mountpoint in selected]

        output = self._run_tool(
            ["lsof", "-w", "-n", "-P"] + mountpoints,
            result,
            tool_name="lsof",
        )
        if output is None:
            return
        for line in output.split("\n"):
            line = line.rstrip()
            if not line or line.startswith("COMMAND"):
                continue
            parts = line.split(None, 8)
            if len(parts) < 9:
                continue
            command, pid, user, fd = parts[0], parts[1], parts[2], parts[3]
            path = parts[8]
            if fd == "cwd":
                continue
            dataset = self._dataset_for_path(path, mount_lookup)
            if dataset is None:
                continue
            key = (pid, fd, path)
            if key in seen:
                continue
            seen.add(key)
            result["files"].append({
                "dataset": dataset,
                "pid": pid,
                "user": user,
                "command": command,
                "fd": fd,
                "path": path,
            })

    def _dataset_for_path(self, path: str, mount_lookup: List[tuple]):
        """Return the dataset whose mountpoint best matches a file path."""
        for dataset, mountpoint in mount_lookup:
            if path == mountpoint or path.startswith(mountpoint.rstrip("/") + "/"):
                return dataset
        return None


    def _collect_files_bsd(
        self, selected: List[tuple], result: Dict[str, Any]
    ) -> None:
        """Use fstat to list open files under each selected mountpoint."""
        seen = set()
        for dataset, mountpoint in selected:
            output = self._run_tool(
                ["fstat", "-f", mountpoint],
                result,
                tool_name="fstat",
            )
            if output is None:
                continue
            for line in output.split("\n"):
                line = line.rstrip()
                if not line or line.startswith("USER"):
                    continue
                parts = line.split()
                if len(parts) < 5:
                    continue
                user, command, pid, fd_field = (
                    parts[0], parts[1], parts[2], parts[3]
                )
                if fd_field.lower() in ("wd", "cwd", "root", "text"):
                    continue
                key = (pid, fd_field, command)
                if key in seen:
                    continue
                seen.add(key)
                result["files"].append({
                    "dataset": dataset,
                    "pid": pid,
                    "user": user,
                    "command": command,
                    "fd": fd_field,
                    "path": mountpoint,
                })

    def _run_tool(
        self, cmd: List[str], result: Dict[str, Any], tool_name: str
    ) -> Optional[str]:
        """Run an inspection tool, recording any failure as a note."""
        try:
            completed = run_privileged_command(cmd, check=False, timeout=15)
            return completed.stdout or ""

        except subprocess.TimeoutExpired:
            result["notes"].append(
                f"The {tool_name} command timed out while scanning."
            )
            return None
        except FileNotFoundError:
            result["notes"].append(
                f"The {tool_name} command is not installed. Install it to "
                "list open files per dataset."
            )
            return None
        except Exception as exc:
            result["notes"].append(f"Could not run {tool_name}: {exc}")
            return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _dataset_matches(
        self,
        dataset_name: str,
        dataset_filter: Optional[str],
        include_children: bool,
    ) -> bool:
        """Return True if a dataset matches the current filter selection."""
        if not dataset_filter:
            return True
        base = dataset_filter.strip()
        if not base:
            return True
        if include_children:
            return dataset_name == base or dataset_name.startswith(base + "/")
        return dataset_name == base

    def _sort_value(self, rates: Dict[str, Any], sort_column: str) -> float:
        """Return the ranking value for the top view."""
        if sort_column == "read":
            return rates["read_bytes"]
        if sort_column == "write":
            return rates["write_bytes"]
        if sort_column == "rops":
            return rates["read_ops"]
        if sort_column == "wops":
            return rates["write_ops"]
        return rates["read_bytes"] + rates["write_bytes"]

    def _humanize(self, value: float) -> str:
        """Format a rate the way zpool iostat does (base 1024)."""
        if value < 1024:
            return f"{value:.0f}"
        scaled = float(value)
        for suffix in UNIT_SUFFIXES[1:]:
            scaled /= 1024.0
            if scaled < 1024:
                if scaled < 10:
                    return f"{scaled:.2f}{suffix}"
                if scaled < 100:
                    return f"{scaled:.1f}{suffix}"
                return f"{scaled:.0f}{suffix}"
        return f"{scaled:.0f}E"
