"""
FleetView C&C Service

Read-only data collection for the FleetView C&C page. Builds one snapshot
per server (the local host plus every Fleet View server) containing basic
system information, ARC statistics and pool state, then derives fleet
totals and a "Needs Attention" issue list.

Every server is polled with read-only commands only:
  - one portable `sh -c` probe for OS, kernel, ZFS version, CPU, load,
    memory, uptime and ARC statistics
  - zpool list, zfs list (pool root datasets) and zpool status

The same parsers are used for local and remote output so both paths
produce identical data structures. SMART / disk health is intentionally
not collected.
"""
import json
import logging
import os
import re
import shlex
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from services.dashboard import _parse_pool_status_counts, _parse_scrub_info
from services.utils import run_zfs_command

logger = logging.getLogger(__name__)

COMMAND_TIMEOUT = 30
MAX_WORKERS = 8
MAX_ISSUES = 8

# Needs Attention reports a possibly undersized ARC when at least this
# percentage of all ARC reads were ghost hits.
GHOST_ALERT_PCT = 1.0

# Card order: the local host always uses this id, remote servers use their
# Fleet View UUID. Ids are validated before they are saved.
LOCAL_SERVER_ID = "local"
ORDER_FILE_NAME = "fleet_command_order.json"
MAX_ORDER_LENGTH = 500
SERVER_ID_PATTERN = re.compile(r"^[A-Za-z0-9-]{1,64}$")

POOL_LIST_ARGS = ["zpool", "list", "-H", "-p", "-o", "name,size,alloc,free,cap,health"]
POOL_SPACE_ARGS = ["zfs", "list", "-H", "-p", "-o", "name,used,avail", "-d", "0"]

# Portable system probe. Each section starts with an @@name marker line.
# Commands that do not exist on a platform fail silently (stderr is
# discarded), so one script works on Linux, FreeBSD and NetBSD. The script
# contains no single quotes so it can be wrapped in sh -c '...' for remote
# execution regardless of the remote login shell.
SYSTEM_PROBE = (
    "PATH=$PATH:/sbin:/usr/sbin:/usr/local/sbin; export PATH; "
    "echo @@kernel; uname -sr; "
    "echo @@arch; uname -m; "
    "echo @@os; grep PRETTY_NAME= /etc/os-release 2>/dev/null; "
    "echo @@zfs; zfs version 2>/dev/null | head -1; "
    "echo @@cpuinfo; grep -m1 \"model name\" /proc/cpuinfo 2>/dev/null; "
    "echo @@ncpu; getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null; "
    "echo @@loadavg; cat /proc/loadavg 2>/dev/null || sysctl -n vm.loadavg 2>/dev/null; "
    "echo @@meminfo; cat /proc/meminfo 2>/dev/null; "
    "echo @@uptime; cat /proc/uptime 2>/dev/null; "
    "echo @@sysctl; for k in hw.model hw.physmem hw.physmem64 hw.pagesize "
    "vm.stats.vm.v_free_count vm.stats.vm.v_inactive_count kern.boottime; "
    "do sysctl $k 2>/dev/null; done; "
    "echo @@vmstat; if [ \"$(uname -s)\" = NetBSD ]; then vmstat -s 2>/dev/null; fi; "
    "echo @@now; date +%s; "
    "echo @@nowlocal; date \"+%Y-%m-%d %H:%M:%S\"; "
    "echo @@arc; cat /proc/spl/kstat/zfs/arcstats 2>/dev/null "
    "|| sysctl kstat.zfs.misc.arcstats 2>/dev/null; "
    "echo @@end"
)

DANGER_HEALTH = ("FAULTED", "SUSPENDED", "UNAVAIL", "REMOVED")


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def format_bytes(value: Optional[float]) -> str:
    """Format a byte count as a short binary size string (for example 1.5T)."""
    if not value:
        return "0B"
    units = ["B", "K", "M", "G", "T", "P"]
    index = 0
    number = float(value)
    while number >= 1024 and index < len(units) - 1:
        number /= 1024
        index += 1
    if number >= 100:
        return f"{int(number)}{units[index]}"
    if number >= 10:
        return f"{number:.1f}{units[index]}"
    return f"{number:.2f}{units[index]}"


def format_duration(seconds: Optional[int]) -> str:
    """Format a duration in seconds as '12d 4h' or '3h 12m'."""
    if seconds is None or seconds < 0:
        return ""
    days, remainder = divmod(int(seconds), 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes = remainder // 60
    if days:
        return f"{days}d {hours}h"
    if hours:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def percent(part: float, whole: float) -> float:
    """Return part / whole as a percentage clamped to 0-100."""
    if not whole:
        return 0.0
    return max(0.0, min(100.0, (part / whole) * 100))


def level_for_capacity(cap: int) -> str:
    """Color level for pool capacity, matching the Fleet View cards."""
    if cap > 90:
        return "danger"
    if cap >= 80:
        return "warning"
    return "success"


def level_for_load(load_pct: float) -> str:
    """Color level for CPU load percentage."""
    if load_pct >= 90:
        return "danger"
    if load_pct >= 75:
        return "warning"
    return "primary"


def level_for_hit_rate(hit_rate: Optional[float]) -> str:
    """Color level for the ARC hit rate."""
    if hit_rate is None:
        return "secondary"
    if hit_rate >= 90:
        return "success"
    if hit_rate >= 80:
        return "warning"
    return "danger"


def _to_int(value: Any) -> Optional[int]:
    """Convert a value to int, returning None on failure."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _first_float(text: str) -> Optional[float]:
    """Return the first decimal number in text, or None."""
    match = re.search(r"\d+(?:\.\d+)?", text or "")
    return float(match.group(0)) if match else None


# ---------------------------------------------------------------------------
# System probe parsing
# ---------------------------------------------------------------------------


def split_sections(output: str) -> Dict[str, str]:
    """Split probe output into a dict of section name to section text."""
    sections: Dict[str, List[str]] = {}
    current = None
    for line in output.splitlines():
        if line.startswith("@@"):
            current = line[2:].strip()
            sections[current] = []
        elif current is not None:
            sections[current].append(line)
    return {name: "\n".join(lines).strip() for name, lines in sections.items()}


def parse_sysctl_lines(text: str) -> Dict[str, str]:
    """Parse 'name: value' (FreeBSD) and 'name = value' (NetBSD) lines."""
    values: Dict[str, str] = {}
    for line in text.splitlines():
        if ": " in line:
            name, value = line.split(": ", 1)
        elif "=" in line:
            name, value = line.split("=", 1)
        else:
            continue
        values[name.strip()] = value.strip()
    return values


def parse_arc_stats(text: str) -> Dict[str, int]:
    """Parse Linux arcstats or BSD sysctl kstat.zfs.misc.arcstats output."""
    stats: Dict[str, int] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("kstat."):
            for name, value in parse_sysctl_lines(line).items():
                number = _to_int(value)
                if number is not None:
                    stats[name.split(".")[-1]] = number
            continue
        parts = line.split()
        if len(parts) >= 3:
            number = _to_int(parts[2])
            if number is not None:
                stats[parts[0]] = number
    return stats


def summarize_arc(stats: Dict[str, int]) -> Dict[str, Any]:
    """Build ARC hit rate, MRU/MFU split, ghost ratio and size figures."""
    if "hits" not in stats or "misses" not in stats:
        return {"available": False}

    hits = stats["hits"]
    misses = stats["misses"]
    mru_hits = stats.get("mru_hits", 0)
    mfu_hits = stats.get("mfu_hits", 0)
    ghost_hits = stats.get("mru_ghost_hits", 0) + stats.get("mfu_ghost_hits", 0)

    # ghost_pct is the share of all ARC reads that were ghost hits (reads that
    # a larger ARC would have served). It matches the dashboard "Ghost Hits"
    # tile. The share of misses alone is not used: on a cache with a 99.9%
    # hit rate it can be large while describing a negligible number of reads.
    return {
        "available": True,
        "size": stats.get("size", 0),
        "max": stats.get("c_max", 0),
        "hit_rate": round(percent(hits, hits + misses), 1),
        "mru_pct": round(percent(mru_hits, mru_hits + mfu_hits), 1),
        "mfu_pct": round(percent(mfu_hits, mru_hits + mfu_hits), 1),
        "ghost_pct": round(percent(ghost_hits, hits + misses), 2),
    }


def _parse_memory(sections: Dict[str, str], sysctl: Dict[str, str]) -> Dict[str, int]:
    """Return total and used memory in bytes for Linux, FreeBSD or NetBSD."""
    meminfo: Dict[str, int] = {}
    for line in sections.get("meminfo", "").splitlines():
        match = re.match(r"(\w+):\s+(\d+)", line)
        if match:
            meminfo[match.group(1)] = int(match.group(2)) * 1024

    if "MemTotal" in meminfo:
        total = meminfo["MemTotal"]
        available = meminfo.get("MemAvailable", meminfo.get("MemFree", 0))
        return {"total": total, "used": max(0, total - available)}

    total = _to_int(sysctl.get("hw.physmem64")) or _to_int(sysctl.get("hw.physmem")) or 0
    page_size = _to_int(sysctl.get("hw.pagesize")) or 0
    free_pages = _to_int(sysctl.get("vm.stats.vm.v_free_count"))
    inactive_pages = _to_int(sysctl.get("vm.stats.vm.v_inactive_count"))

    # NetBSD reports page counts through vmstat -s
    if free_pages is None:
        for line in sections.get("vmstat", "").splitlines():
            match = re.match(r"\s*(\d+)\s+(.+)$", line)
            if not match:
                continue
            number, label = int(match.group(1)), match.group(2).strip()
            if label == "bytes per page":
                page_size = number
            elif label == "pages free":
                free_pages = number
            elif label == "pages inactive":
                inactive_pages = number

    if total and page_size and free_pages is not None:
        available = (free_pages + (inactive_pages or 0)) * page_size
        return {"total": total, "used": max(0, total - available)}
    return {"total": total, "used": 0}


def _parse_uptime(sections: Dict[str, str], sysctl: Dict[str, str]) -> Optional[int]:
    """Return uptime in seconds from /proc/uptime or kern.boottime."""
    proc_uptime = _first_float(sections.get("uptime", ""))
    if proc_uptime is not None:
        return int(proc_uptime)

    boottime = sysctl.get("kern.boottime", "")
    now_epoch = _to_int(sections.get("now"))
    match = re.search(r"sec\s*=\s*(\d+)", boottime)
    if match and now_epoch:
        return now_epoch - int(match.group(1))

    # NetBSD prints the boot time as a local date string
    try:
        boot = datetime.strptime(" ".join(boottime.split()), "%a %b %d %H:%M:%S %Y")
        now_local = datetime.strptime(sections.get("nowlocal", ""), "%Y-%m-%d %H:%M:%S")
        return int((now_local - boot).total_seconds())
    except ValueError:
        return None


def parse_system_probe(output: str) -> Dict[str, Any]:
    """Convert SYSTEM_PROBE output into the system info dict for one server."""
    sections = split_sections(output)
    sysctl = parse_sysctl_lines(sections.get("sysctl", ""))

    kernel = sections.get("kernel", "")
    os_name = sections.get("os", "")
    if os_name.startswith("PRETTY_NAME="):
        os_name = os_name.split("=", 1)[1].strip().strip('"').strip("'")
    if not os_name:
        os_name = kernel

    cpu_model = sections.get("cpuinfo", "")
    if ":" in cpu_model:
        cpu_model = cpu_model.split(":", 1)[1].strip()
    if not cpu_model:
        cpu_model = sysctl.get("hw.model", "") or sections.get("arch", "")

    zfs_version = sections.get("zfs", "")
    if zfs_version.startswith("zfs-"):
        zfs_version = zfs_version[4:]

    threads = _to_int(sections.get("ncpu", "").split("\n")[0]) or 0
    load = _first_float(sections.get("loadavg", ""))
    memory = _parse_memory(sections, sysctl)
    arc = summarize_arc(parse_arc_stats(sections.get("arc", "")))
    uptime = _parse_uptime(sections, sysctl)

    load_pct = round(percent(load or 0, threads), 1)
    mem_pct = round(percent(memory["used"], memory["total"]), 1)
    arc_of_mem = 0.0
    if arc["available"] and memory["total"]:
        arc_of_mem = round(min(percent(arc["size"], memory["total"]), mem_pct), 1)

    return {
        "os_name": os_name,
        "kernel": kernel,
        "zfs_version": zfs_version,
        "cpu_model": cpu_model,
        "threads": threads,
        "load": load,
        "load_pct": load_pct,
        "load_level": level_for_load(load_pct),
        "mem_total": memory["total"],
        "mem_used": memory["used"],
        "mem_pct": mem_pct,
        "mem_total_human": format_bytes(memory["total"]),
        "mem_used_human": format_bytes(memory["used"]),
        "arc": arc,
        "arc_size_human": format_bytes(arc.get("size")),
        "arc_max_human": format_bytes(arc.get("max")),
        "arc_level": level_for_hit_rate(arc.get("hit_rate")),
        "arc_of_mem_pct": arc_of_mem,
        "other_mem_pct": round(max(0.0, mem_pct - arc_of_mem), 1),
        "uptime": format_duration(uptime),
    }


# ---------------------------------------------------------------------------
# Pool parsing
# ---------------------------------------------------------------------------


def parse_scrub(status_output: str) -> Dict[str, Any]:
    """Classify the scrub state of one pool from its zpool status output."""
    info = _parse_scrub_info(status_output)
    if info["scrub_in_progress"]:
        progress = info["scrub_progress"]
        if progress is None:
            # zpool status prints "NN.NN% done" on a later line of the scan block
            match = re.search(r"(\d+(?:\.\d+)?)%\s+done", status_output)
            progress = float(match.group(1)) if match else None
        label = f"Scrub {int(progress)}%" if progress is not None else "Scrubbing"
        return {"state": "running", "level": "info", "label": label,
                "progress": progress, "age_days": None}

    if not info["last_scrub"]:
        return {"state": "never", "level": "danger", "label": "Never",
                "progress": None, "age_days": None}

    age_days = (datetime.now() - datetime.fromisoformat(info["last_scrub"])).days
    level = "success"
    if age_days > 60:
        level = "danger"
    elif age_days > 30:
        level = "warning"
    return {"state": "done", "level": level, "label": f"{age_days}d ago",
            "progress": None, "age_days": age_days}


def parse_pools(list_output: str, space_output: str,
                status_outputs: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Build pool dicts from `zpool list -Hp`, `zfs list -Hp -d 0` and
    per-pool `zpool status` output.

    Used/available come from the dataset layer when present so the numbers
    match the rest of WebZFS (total = used + avail).
    """
    space_map: Dict[str, Dict[str, int]] = {}
    for line in space_output.strip().splitlines():
        parts = line.split("\t")
        if len(parts) >= 3:
            used, avail = _to_int(parts[1]), _to_int(parts[2])
            if used is not None and avail is not None:
                space_map[parts[0]] = {"used": used, "avail": avail}

    pools = []
    for line in list_output.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 6:
            continue
        name, size, alloc, free, _cap, health = parts[:6]
        space = space_map.get(name)
        if space:
            used_bytes, avail_bytes = space["used"], space["avail"]
            total_bytes = used_bytes + avail_bytes
        else:
            used_bytes, avail_bytes = _to_int(alloc) or 0, _to_int(free) or 0
            total_bytes = _to_int(size) or 0

        cap = round(percent(used_bytes, total_bytes))
        status_output = status_outputs.get(name, "")
        counts = _parse_pool_status_counts(status_output, name)
        errors = counts["read_errors"] + counts["write_errors"] + counts["cksum_errors"]

        pools.append({
            "name": name,
            "health": health,
            "used_bytes": used_bytes,
            "avail_bytes": avail_bytes,
            "total_bytes": total_bytes,
            "used": format_bytes(used_bytes),
            "avail": format_bytes(avail_bytes),
            "total": format_bytes(total_bytes),
            "cap": cap,
            "cap_level": level_for_capacity(cap),
            "disk_count": counts["disk_count"],
            "read_errors": counts["read_errors"],
            "write_errors": counts["write_errors"],
            "cksum_errors": counts["cksum_errors"],
            "error_total": errors,
            "scrub": parse_scrub(status_output),
        })
    return pools


def server_pulse(pools: List[Dict[str, Any]]) -> str:
    """Pulse border class, same rule as fleet/partials/server_card.jinja."""
    pulse = ""
    for pool in pools:
        if pool["health"] in ("FAULTED", "SUSPENDED", "UNAVAIL"):
            return "pulse-border-danger"
        if pool["health"] == "DEGRADED" or pool["cap"] > 95:
            pulse = "pulse-border-warning"
    return pulse


# ---------------------------------------------------------------------------
# Collection
# ---------------------------------------------------------------------------


def collect_local() -> Dict[str, Any]:
    """Collect the snapshot for the local WebZFS host."""
    server = {"id": LOCAL_SERVER_ID, "name": socket.gethostname(), "local": True,
              "status": "online", "error": None, "system": {}, "pools": [], "pulse": ""}
    try:
        probe = subprocess.run(["sh", "-c", SYSTEM_PROBE], capture_output=True,
                               text=True, timeout=COMMAND_TIMEOUT, check=False)
        server["system"] = parse_system_probe(probe.stdout)

        list_result = run_zfs_command(POOL_LIST_ARGS, check=False, timeout=COMMAND_TIMEOUT)
        space_result = run_zfs_command(POOL_SPACE_ARGS, check=False, timeout=COMMAND_TIMEOUT)
        status_outputs = {}
        for line in list_result.stdout.strip().splitlines():
            name = line.split("\t")[0]
            status = run_zfs_command(["zpool", "status", name], check=False,
                                     timeout=COMMAND_TIMEOUT)
            status_outputs[name] = status.stdout
        server["pools"] = parse_pools(list_result.stdout, space_result.stdout,
                                      status_outputs)
    except Exception as exc:
        logger.error(f"FleetView C&C: local collection failed: {exc}")
        server["status"] = "error"
        server["error"] = str(exc)
    server["pulse"] = server_pulse(server["pools"])
    return server


def _run_remote(client, command: str) -> str:
    """Run one command over an open SSH client and return stdout."""
    _stdin, stdout, _stderr = client.exec_command(command, timeout=COMMAND_TIMEOUT)
    return stdout.read().decode("utf-8", errors="replace")


def collect_remote(fleet_service, server_record: Dict[str, Any]) -> Dict[str, Any]:
    """Collect the snapshot for one Fleet View server over a single SSH session."""
    server = {"id": server_record.get("id", ""), "name": server_record.get("name", ""),
              "local": False, "status": "online", "error": None, "system": {},
              "pools": [], "pulse": "", "last_checked": server_record.get("last_checked")}
    try:
        full_record = fleet_service._get_server_by_id(server_record["id"])
        client = fleet_service._create_ssh_client(full_record)
        try:
            server["system"] = parse_system_probe(
                _run_remote(client, "sh -c " + shlex.quote(SYSTEM_PROBE))
            )

            def zfs(args: List[str]) -> str:
                command = " ".join(shlex.quote(arg) for arg in args)
                return _run_remote(client, fleet_service._build_zfs_command(full_record, command))

            list_output = zfs(POOL_LIST_ARGS)
            space_output = zfs(POOL_SPACE_ARGS)
            status_outputs = {}
            for line in list_output.strip().splitlines():
                name = line.split("\t")[0]
                status_outputs[name] = zfs(["zpool", "status", name])
            server["pools"] = parse_pools(list_output, space_output, status_outputs)
        finally:
            client.close()
    except Exception as exc:
        logger.error(f"FleetView C&C: collection failed for {server['name']}: {exc}")
        server["status"] = "error"
        server["error"] = str(exc)
    server["pulse"] = server_pulse(server["pools"])
    return server


def collect_fleet(fleet_service) -> List[Dict[str, Any]]:
    """Collect snapshots for the local host and every Fleet View server in parallel."""
    records = fleet_service.list_servers()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        local_future = executor.submit(collect_local)
        remote_futures = [executor.submit(collect_remote, fleet_service, record)
                          for record in records]
        servers = [local_future.result()]
        servers.extend(future.result() for future in remote_futures)
    return servers


# ---------------------------------------------------------------------------
# Fleet summaries
# ---------------------------------------------------------------------------


def _arc_ready(server: Dict[str, Any]) -> bool:
    """True when the server is online and reported ARC statistics."""
    return server["status"] == "online" and server["system"].get("arc", {}).get("available", False)


def build_totals(servers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Fleet totals card values and the health strip values."""
    pools = [pool for server in servers for pool in server["pools"]]
    size = sum(pool["total_bytes"] for pool in pools)
    used = sum(pool["used_bytes"] for pool in pools)
    avail = sum(pool["avail_bytes"] for pool in pools)
    online = sum(1 for server in servers if server["status"] == "online")

    healthy = sum(1 for pool in pools if pool["health"] == "ONLINE")
    scrub_ok = sum(1 for pool in pools if pool["scrub"]["state"] == "running"
                   or (pool["scrub"]["state"] == "done" and pool["scrub"]["age_days"] <= 30))
    scrub_running = sum(1 for pool in pools if pool["scrub"]["state"] == "running")
    scrub_never = sum(1 for pool in pools if pool["scrub"]["state"] == "never")

    arc_servers = [server for server in servers if _arc_ready(server)]
    hit_rates = [server["system"]["arc"]["hit_rate"] for server in arc_servers]
    arc_bytes = sum(server["system"]["arc"]["size"] for server in arc_servers)
    avg_hit = round(sum(hit_rates) / len(hit_rates), 1) if hit_rates else None

    scrub_notes = []
    if scrub_running:
        scrub_notes.append(f"{scrub_running} running")
    if scrub_never:
        scrub_notes.append(f"{scrub_never} never scrubbed")

    return {
        "size": format_bytes(size),
        "used": format_bytes(used),
        "used_pct": round(percent(used, size)),
        "avail": format_bytes(avail),
        "disks": sum(pool["disk_count"] for pool in pools),
        "systems": len(servers),
        "unreachable": len(servers) - online,
        "pool_count": len(pools),
        "pools_healthy": healthy,
        "pools_unhealthy": len(pools) - healthy,
        "scrub_ok": scrub_ok,
        "scrub_note": " / ".join(scrub_notes) if scrub_notes else "none overdue",
        "avg_hit": avg_hit,
        "avg_hit_level": level_for_hit_rate(avg_hit),
        "arc_total": format_bytes(arc_bytes),
    }


def _issue(level: str, rank: int, title: str, detail: str) -> Dict[str, Any]:
    return {"level": level, "rank": rank, "title": title, "detail": detail}


def _error_text(pool: Dict[str, Any]) -> str:
    return (f"{pool['read_errors']} read / {pool['write_errors']} write"
            f" / {pool['cksum_errors']} checksum errors")


def build_issues(servers: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Needs Attention list, ordered by rank:
      0 server cannot be polled or pool not ONLINE, 2 device errors on an
      ONLINE pool, 3 pool over 90% full, 4 pool 80-90% full, 6 scrub
      overdue (one grouped row), 8 scrub running, 9 high ARC ghost ratio.
    """
    issues = []
    overdue = []
    for server in servers:
        server_label = server["name"] + (" (local)" if server["local"] else "")
        if server["status"] != "online":
            issues.append(_issue("danger", 0, f"{server_label} cannot be polled",
                                 server["error"] or "No response"))
            continue

        for pool in server["pools"]:
            label = f"{pool['name']} - {server_label}"
            scrub = pool["scrub"]
            if pool["health"] != "ONLINE":
                details = []
                if pool["error_total"]:
                    details.append(_error_text(pool))
                if scrub["state"] == "running":
                    details.append(scrub["label"].lower())
                issues.append(_issue("danger", 0, f"{label} is {pool['health']}",
                                     ", ".join(details) or "Check zpool status on the server"))
            elif pool["error_total"]:
                issues.append(_issue("warning", 2, f"{label} has device errors",
                                     _error_text(pool)))

            if pool["cap"] > 90:
                issues.append(_issue("danger", 3, f"{label} is {pool['cap']}% full",
                                     f"{pool['avail']} available of {pool['total']}"))
            elif pool["cap"] >= 80:
                issues.append(_issue("warning", 4, f"{label} is {pool['cap']}% full",
                                     f"{pool['avail']} available of {pool['total']}"))

            if scrub["state"] == "never":
                overdue.append(f"{pool['name']} ({server['name']}) never")
            elif scrub["state"] == "done" and scrub["age_days"] > 30:
                overdue.append(f"{pool['name']} ({server['name']}) {scrub['age_days']}d")
            elif scrub["state"] == "running" and pool["health"] == "ONLINE":
                issues.append(_issue("info", 8, f"{label} scrub running", scrub["label"]))

        arc = server["system"].get("arc", {})
        if arc.get("available") and arc["ghost_pct"] >= GHOST_ALERT_PCT:
            issues.append(_issue(
                "info", 9, f"{server_label}: {arc['ghost_pct']}% of ARC reads were ghost hits",
                "Recently evicted data is being read back; the ARC may be too small"))

    if overdue:
        plural = "s" if len(overdue) > 1 else ""
        issues.append(_issue("warning", 6, f"{len(overdue)} pool{plural} not scrubbed in 30 days",
                             ", ".join(overdue)))

    issues.sort(key=lambda item: item["rank"])
    return issues


# ---------------------------------------------------------------------------
# Card order (saved per WebZFS instance, shared by every browser)
# ---------------------------------------------------------------------------


def order_file() -> Path:
    """Location of the saved card order, next to fleet_servers.json."""
    return Path.home() / ".config" / "webzfs" / ORDER_FILE_NAME


def validate_order(order: Any) -> List[str]:
    """
    Return a clean list of server ids, or raise ValueError.
    Duplicates are dropped, keeping the first position.
    """
    if not isinstance(order, list):
        raise ValueError("order must be a list")
    if len(order) > MAX_ORDER_LENGTH:
        raise ValueError("order is too long")
    clean = []
    for server_id in order:
        if not isinstance(server_id, str) or not SERVER_ID_PATTERN.match(server_id):
            raise ValueError("order contains an invalid server id")
        if server_id not in clean:
            clean.append(server_id)
    return clean


def load_order() -> List[str]:
    """Read the saved card order. Missing or invalid files give an empty list."""
    path = order_file()
    try:
        if path.is_file():
            data = json.loads(path.read_text(encoding="utf-8"))
            return validate_order(data.get("order", []))
    except (OSError, ValueError, AttributeError) as exc:
        logger.warning(f"FleetView C&C: ignoring saved card order: {exc}")
    return []


def save_order(order: Any) -> List[str]:
    """
    Validate and save the card order. Writes a temporary file and renames it
    so a reader in another worker never sees a partial file.
    Raises ValueError for invalid input and OSError if the write fails.
    """
    clean = validate_order(order)
    path = order_file()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(path.name + ".tmp")
    temp_path.write_text(json.dumps({"order": clean}, indent=2) + "\n", encoding="utf-8")
    os.chmod(temp_path, 0o600)
    os.replace(temp_path, path)
    return clean


def apply_order(servers: List[Dict[str, Any]], order: List[str]) -> List[Dict[str, Any]]:
    """
    Sort servers by the saved order. Servers missing from the saved order,
    for example newly added ones, keep their default order after the rest.
    """
    position = {server_id: index for index, server_id in enumerate(order)}
    default_rank = len(order)
    return sorted(servers, key=lambda server: position.get(server["id"], default_rank))


def get_command_context(fleet_service) -> Dict[str, Any]:
    """Collect the fleet and return the template context for the data partial."""
    servers = apply_order(collect_fleet(fleet_service), load_order())
    issues = build_issues(servers)
    return {
        "servers": servers,
        "totals": build_totals(servers),
        "issues": issues[:MAX_ISSUES],
        "issues_more": max(0, len(issues) - MAX_ISSUES),
        "danger_count": sum(1 for item in issues if item["level"] == "danger"),
        "warning_count": sum(1 for item in issues if item["level"] == "warning"),
        "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
