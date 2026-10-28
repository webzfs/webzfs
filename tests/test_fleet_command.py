"""Tests for the FleetView C&C service parsers, summaries and routes."""
from datetime import datetime, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from auth.dependencies import get_current_user
from services import fleet_command
from views import fleet as fleet_views

LINUX_PROBE = """@@kernel
Linux 6.12.48+deb13-amd64
@@arch
x86_64
@@os
PRETTY_NAME="Debian GNU/Linux 13 (trixie)"
@@zfs
zfs-2.3.2-2
@@cpuinfo
model name\t: Intel(R) Xeon(R) Silver 4314
@@ncpu
32
@@loadavg
8.00 7.10 6.00 3/900 1234
@@meminfo
MemTotal:       16777216 kB
MemFree:         1048576 kB
MemAvailable:    8388608 kB
@@uptime
90061.50 100000.00
@@sysctl
@@vmstat
@@now
1800000000
@@nowlocal
2026-10-27 10:00:00
@@arc
13 1 0x01 123 33456 1 2
name                            type data
hits                            4    900
misses                          4    100
mru_hits                        4    250
mfu_hits                        4    750
mru_ghost_hits                  4    6
mfu_ghost_hits                  4    4
size                            4    4294967296
c_max                           4    8589934592
@@end
"""

FREEBSD_PROBE = """@@kernel
FreeBSD 15.1-RELEASE
@@arch
amd64
@@os
@@zfs
zfs-2.4.0-FreeBSD_g1234
@@cpuinfo
@@ncpu
16
@@loadavg
{ 4.00 3.00 2.00 }
@@meminfo
@@uptime
@@sysctl
hw.model: AMD Ryzen 9 7950X
hw.physmem: 17179869184
hw.pagesize: 4096
vm.stats.vm.v_free_count: 1048576
vm.stats.vm.v_inactive_count: 524288
kern.boottime: { sec = 1799913600, usec = 0 } Mon Jan  1 00:00:00 2027
@@vmstat
@@now
1800000000
@@nowlocal
2027-01-02 00:00:00
@@arc
kstat.zfs.misc.arcstats.hits: 990
kstat.zfs.misc.arcstats.misses: 10
kstat.zfs.misc.arcstats.size: 1073741824
kstat.zfs.misc.arcstats.c_max: 2147483648
@@end
"""

NETBSD_PROBE = """@@kernel
NetBSD 10.1
@@arch
amd64
@@os
@@zfs
@@cpuinfo
@@ncpu
2
@@loadavg
0.50 0.40 0.30
@@meminfo
@@uptime
@@sysctl
hw.model = Intel 686-class
hw.physmem64 = 4294967296
hw.pagesize = 4096
kern.boottime = Mon Oct 26 10:00:00 2026
@@vmstat
     4096 bytes per page
   262144 pages free
   131072 pages inactive
@@now
1800000000
@@nowlocal
2026-10-27 12:30:00
@@arc
@@end
"""


def _status(pool, state="ONLINE", scrub_line=None, errors=(0, 0, 0)):
    """Build a minimal zpool status output for one pool with two disks."""
    read, write, cksum = errors
    scan = scrub_line or "scan: none requested"
    return (
        f"  pool: {pool}\n state: {state}\n  {scan}\nconfig:\n\n"
        "\tNAME        STATE     READ WRITE CKSUM\n"
        f"\t{pool}        {state}       0     0     0\n"
        f"\t  mirror-0  {state}       0     0     0\n"
        f"\t    sda     {state}       {read}     {write}     {cksum}\n"
        "\t    sdb     ONLINE       0     0     0\n\n"
        "errors: No known data errors\n"
    )


def _scrub_line(days_ago):
    when = datetime.now() - timedelta(days=days_ago)
    return "scan: scrub repaired 0B in 00:10:00 with 0 errors on " + when.strftime(
        "%a %b %d %H:%M:%S %Y"
    )


def test_linux_probe_parsing():
    info = fleet_command.parse_system_probe(LINUX_PROBE)
    assert info["os_name"] == "Debian GNU/Linux 13 (trixie)"
    assert info["kernel"] == "Linux 6.12.48+deb13-amd64"
    assert info["zfs_version"] == "2.3.2-2"
    assert info["cpu_model"] == "Intel(R) Xeon(R) Silver 4314"
    assert info["threads"] == 32
    assert info["load_pct"] == 25.0
    assert info["mem_pct"] == 50.0
    assert info["uptime"] == "1d 1h"
    assert info["arc"]["hit_rate"] == 90.0
    assert info["arc"]["mru_pct"] == 25.0
    # 10 ghost hits out of 1000 total reads
    assert info["arc"]["ghost_pct"] == 1.0
    assert info["arc_of_mem_pct"] == 25.0
    assert info["other_mem_pct"] == 25.0


def test_freebsd_probe_parsing():
    info = fleet_command.parse_system_probe(FREEBSD_PROBE)
    assert info["os_name"] == "FreeBSD 15.1-RELEASE"
    assert info["cpu_model"] == "AMD Ryzen 9 7950X"
    assert info["load_pct"] == 25.0
    # 16G total, (1048576 + 524288) pages * 4096 = 6G available
    assert info["mem_pct"] == 62.5
    assert info["uptime"] == "1d 0h"
    assert info["arc"]["hit_rate"] == 99.0
    assert info["arc_max_human"] == "2.00G"


def test_netbsd_probe_parsing():
    info = fleet_command.parse_system_probe(NETBSD_PROBE)
    assert info["cpu_model"] == "Intel 686-class"
    assert info["threads"] == 2
    # 4G total, (262144 + 131072) pages * 4096 = 1.5G available
    assert info["mem_pct"] == 62.5
    assert info["uptime"] == "1d 2h"
    assert info["arc"] == {"available": False}


def test_parse_pools_space_counts_and_scrub():
    list_output = "tank\t1000\t600\t400\t60\tONLINE\nfast\t100\t10\t90\t10\tDEGRADED\n"
    space_output = "tank\t850\t150\nfast\t10\t90\n"
    status = {
        "tank": _status("tank", scrub_line=_scrub_line(5)),
        "fast": _status("fast", "DEGRADED", errors=(1, 0, 4)),
    }
    pools = fleet_command.parse_pools(list_output, space_output, status)
    tank, fast = pools

    assert tank["cap"] == 85
    assert tank["cap_level"] == "warning"
    assert tank["disk_count"] == 2
    assert tank["scrub"]["state"] == "done"
    assert tank["scrub"]["age_days"] == 5
    assert fast["error_total"] == 5
    assert fast["scrub"]["state"] == "never"
    assert fleet_command.server_pulse(pools) == "pulse-border-warning"


def test_running_scrub_is_detected():
    line = "scan: scrub in progress since Mon Oct 26 10:00:00 2026\n\t1.00T scanned, 43.20% done"
    scrub = fleet_command.parse_scrub(_status("tank", scrub_line=line))
    assert scrub["state"] == "running"
    assert scrub["label"] == "Scrub 43%"


def _sample_servers():
    """Local healthy host, one remote with problems, one unreachable remote."""
    healthy = fleet_command.parse_pools(
        "tank\t1000\t100\t900\t10\tONLINE\n", "tank\t100\t900\n",
        {"tank": _status("tank", scrub_line=_scrub_line(3))},
    )
    problem = fleet_command.parse_pools(
        "big\t1000\t950\t50\t95\tDEGRADED\nold\t100\t50\t50\t50\tONLINE\n",
        "big\t950\t50\nold\t50\t50\n",
        {"big": _status("big", "DEGRADED", scrub_line=_scrub_line(2), errors=(3, 0, 14)),
         "old": _status("old", scrub_line=_scrub_line(45))},
    )
    local = {"name": "apollo", "local": True, "status": "online", "error": None,
             "system": fleet_command.parse_system_probe(LINUX_PROBE), "pools": healthy}
    remote = {"name": "athena", "local": False, "status": "online", "error": None,
              "system": fleet_command.parse_system_probe(FREEBSD_PROBE), "pools": problem}
    down = {"name": "dionysus", "local": False, "status": "error",
            "error": "SSH authentication failed", "system": {}, "pools": []}
    for server in (local, remote, down):
        server["pulse"] = fleet_command.server_pulse(server["pools"])
        server["last_checked"] = None
    return [local, remote, down]


def test_build_totals():
    totals = fleet_command.build_totals(_sample_servers())
    assert totals["systems"] == 3
    assert totals["unreachable"] == 1
    assert totals["pool_count"] == 3
    assert totals["pools_healthy"] == 2
    assert totals["scrub_ok"] == 2
    assert totals["avg_hit"] == 94.5
    assert "disk_health" not in totals


def test_build_issues_order_and_content():
    issues = fleet_command.build_issues(_sample_servers())
    titles = [item["title"] for item in issues]

    assert issues[0]["level"] == "danger"
    assert "dionysus cannot be polled" in titles
    assert "big - athena is DEGRADED" in titles
    assert "big - athena is 95% full" in titles
    assert "1 pool not scrubbed in 30 days" in titles
    assert "apollo (local): 1.0% of ARC reads were ghost hits" in titles
    ranks = [item["rank"] for item in issues]
    assert ranks == sorted(ranks)
    assert not any("SMART" in item["title"] or "disk" in item["title"].lower() for item in issues)


def test_ghost_ratio_uses_total_reads_not_misses():
    # Real counters from a bug report (FreeBSD host "defiant"): 99.86% hit
    # rate, 144,091 of 447,241 misses were ghost hits. As a share of misses
    # that is 32.2%, which raised a false "ARC may be too small" alert.
    stats = {"hits": 319881416, "misses": 447241,
             "mru_ghost_hits": 100000, "mfu_ghost_hits": 44091}
    arc = fleet_command.summarize_arc(stats)
    assert arc["ghost_pct"] == 0.04

    server = _sample_servers()[0]
    server["system"]["arc"] = arc
    assert fleet_command.build_issues([server]) == []


def test_healthy_fleet_has_no_issues():
    servers = _sample_servers()[:1]
    servers[0]["system"]["arc"]["ghost_pct"] = 0
    assert fleet_command.build_issues(servers) == []


def _client(monkeypatch, servers):
    monkeypatch.setattr(fleet_command, "collect_fleet", lambda service: servers)
    app = FastAPI()
    app.mount("/static", StaticFiles(directory=Path(__file__).resolve().parents[1] / "static"),
              name="static")
    app.include_router(fleet_views.router)
    app.dependency_overrides[get_current_user] = lambda: "tester"
    return TestClient(app)


def test_command_page_and_data_render(monkeypatch):
    with _client(monkeypatch, _sample_servers()) as client:
        page = client.get("/fleet/command")
        data = client.get("/fleet/command/data")

    assert page.status_code == 200
    assert "FleetView C&amp;C" in page.text
    assert 'hx-get="/fleet/command/data"' in page.text

    assert data.status_code == 200
    assert "Needs Attention" in data.text
    assert "fleetCommandAttentionOpen" in data.text
    assert "big - athena is DEGRADED" in data.text
    assert "SSH authentication failed" in data.text
    assert "apollo" in data.text and "(local)" in data.text
    assert "pulse-border-warning" in data.text
    assert "ARC Hit Rate" in data.text
    assert "Disk Health" not in data.text and "SMART" not in data.text


class _ShellClient:
    """Fake SSH client that runs each command through a local /bin/sh."""

    def __init__(self):
        self.commands = []

    def exec_command(self, command, timeout=None):
        import io
        import subprocess

        self.commands.append(command)
        if command.startswith("sh -c "):
            output = subprocess.run(["/bin/sh", "-c", command], capture_output=True).stdout
        elif "zpool list" in command:
            output = b"tank\t1000\t100\t900\t10\tONLINE\n"
        elif "zfs list" in command:
            output = b"tank\t100\t900\n"
        else:
            output = _status("tank").encode()
        return None, io.BytesIO(output), io.BytesIO(b"")

    def close(self):
        pass


class _FakeFleet:
    def __init__(self, client):
        self.client = client

    def _get_server_by_id(self, server_id):
        return {"id": server_id, "name": "remote", "username": "admin"}

    def _create_ssh_client(self, server):
        return self.client

    def _build_zfs_command(self, server, command):
        return f"sudo {command}"


def test_collect_remote_quotes_probe_and_uses_sudo_for_zfs():
    client = _ShellClient()
    server = fleet_command.collect_remote(_FakeFleet(client), {"id": "abc", "name": "remote"})

    assert server["status"] == "online"
    assert server["system"]["kernel"]
    assert server["pools"][0]["name"] == "tank"
    assert not client.commands[0].startswith("sudo")
    assert all(command.startswith("sudo ") for command in client.commands[1:])
    assert client.commands[-1] == "sudo zpool status tank"


def test_command_routes_are_read_only():
    routes = {(route.path, method) for route in fleet_views.router.routes
              for method in route.methods if route.path.startswith("/fleet/command")}
    assert routes == {("/fleet/command", "GET"), ("/fleet/command/data", "GET")}

