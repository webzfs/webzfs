"""Tests for the ZFS kstat parsers and readers (issue #24, M11).

Fixtures under ``tests/fixtures/observability`` are captured from a live
OpenZFS 2.4 host (Linux procfs) plus FreeBSD sysctl output built from the same
kstat field set (``kstat_create("zfs", 0, ...)`` names in ``zil.c`` /
``spa_stats.c``).
"""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import services.zfs_observability as obs
from services.zfs_observability import (
    ZFSObservabilityService,
    parse_kstat_procfs,
    parse_kstat_sysctl,
    parse_txg_history,
    summarize_txg_history,
    summarize_zil_stats,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "observability"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text()


@pytest.fixture(scope="module")
def zil_linux() -> str:
    return _read("zil_kstat_linux.txt")


@pytest.fixture(scope="module")
def zil_freebsd() -> str:
    return _read("zil_kstat_freebsd.txt")


@pytest.fixture(scope="module")
def txgs_linux() -> str:
    return _read("txgs_kstat_linux.txt")


@pytest.fixture(scope="module")
def txgs_freebsd() -> str:
    return _read("txgs_kstat_freebsd.txt")


# ---------------------------------------------------------------------------
# parse_kstat_procfs / parse_kstat_sysctl
# ---------------------------------------------------------------------------


def test_parse_kstat_procfs_skips_header_lines(zil_linux):
    stats = parse_kstat_procfs(zil_linux)

    assert len(stats) == 21
    assert stats["zil_commit_count"] == 935916
    assert stats["zil_itx_needcopy_bytes"] == 4918663896
    # The numeric kstat header and the "name type data" column header must not
    # leak into the result.
    assert "26" not in stats
    assert "name" not in stats


def test_parse_kstat_procfs_all_values_are_ints(zil_linux):
    stats = parse_kstat_procfs(zil_linux)
    assert all(isinstance(value, int) for value in stats.values())


def test_parse_kstat_procfs_empty_input():
    assert parse_kstat_procfs("") == {}
    assert parse_kstat_procfs("26 1 0x01 21 5712 9156795328 362210075545631\n") == {}


def test_parse_kstat_sysctl_strips_prefix(zil_freebsd):
    stats = parse_kstat_sysctl(zil_freebsd, prefix="kstat.zfs.misc.zil")

    assert len(stats) == 21
    assert stats["zil_commit_count"] == 935916
    assert "kstat" not in stats


def test_parse_kstat_sysctl_matches_procfs_field_set(zil_linux, zil_freebsd):
    procfs = parse_kstat_procfs(zil_linux)
    sysctl = parse_kstat_sysctl(zil_freebsd, prefix="kstat.zfs.misc.zil")
    assert set(procfs) == set(sysctl)
    assert procfs == sysctl


def test_parse_kstat_sysctl_without_prefix_keeps_last_component():
    text = "kstat.zfs.misc.arcstats.hits: 100\nkstat.zfs.misc.arcstats.misses=5\n"
    assert parse_kstat_sysctl(text) == {"hits": 100, "misses": 5}


def test_parse_kstat_sysctl_ignores_raw_payload_line():
    # A raw kstat is printed as "kstat.zfs.<pool>.<name>:" followed by a table;
    # the reader must not turn that line into a statistic.
    text = "kstat.zfs.tank.txgs:\n"
    assert parse_kstat_sysctl(text, prefix="kstat.zfs.tank.txgs") == {}


# ---------------------------------------------------------------------------
# parse_txg_history / summarize_txg_history
# ---------------------------------------------------------------------------


def test_parse_txg_history_linux(txgs_linux):
    entries = parse_txg_history(txgs_linux)

    assert len(entries) == 100
    first = entries[0]
    assert first["txg"] == 390251
    assert first["state"] == "C"
    assert first["state_name"] == "committed"
    assert first["ndirty"] == 1671168
    assert first["nwritten"] == 888832
    assert first["writes"] == 63
    assert first["otime"] == 5120961672
    assert first["stime"] == 11715735
    assert entries[-1]["txg"] == 390350


def test_parse_txg_history_freebsd(txgs_freebsd):
    entries = parse_txg_history(txgs_freebsd)

    assert len(entries) == 3
    assert entries[0]["txg"] == 390251
    assert entries[2]["state"] == "O"
    assert entries[2]["state_name"] == "open"
    # The leading "kstat.zfs.tank.txgs:" line must not become a row.
    assert all("kstat" not in str(entry) for entry in entries)


def test_parse_txg_history_uses_header_column_names():
    text = (
        "txg  birth  state  nwritten  stime\n"
        "7    100    C      4096      2000000\n"
    )
    entries = parse_txg_history(text)
    assert entries == [
        {"txg": 7, "birth": 100, "state": "C", "state_name": "committed",
         "nwritten": 4096, "stime": 2000000}
    ]


def test_parse_txg_history_skips_short_rows():
    text = (
        "txg  birth  state  ndirty\n"
        "7    100    C      1\n"
        "8    200\n"
    )
    entries = parse_txg_history(text)
    assert [entry["txg"] for entry in entries] == [7]


def test_parse_txg_history_garbage_returns_empty():
    assert parse_txg_history("") == []
    assert parse_txg_history("not a txg history at all\n") == []


def test_summarize_txg_history(txgs_linux):
    summary = summarize_txg_history(parse_txg_history(txgs_linux))

    assert summary["count"] == 100
    assert summary["committed"] == 99
    assert summary["oldest_txg"] == 390251
    assert summary["newest_txg"] == 390350
    assert summary["avg_open_ms"] is not None and summary["avg_open_ms"] > 0
    assert summary["avg_sync_ms"] is not None and summary["avg_sync_ms"] > 0
    assert summary["total_written_bytes"] == 183570432
    assert summary["total_writes"] == 8196


def test_summarize_txg_history_empty():
    assert summarize_txg_history([]) == {}


# ---------------------------------------------------------------------------
# summarize_zil_stats
# ---------------------------------------------------------------------------


def test_summarize_zil_stats(zil_linux):
    summary = summarize_zil_stats(parse_kstat_procfs(zil_linux))

    assert summary["commit_count"] == 935916
    assert summary["commit_writer_count"] == 935897
    assert summary["commit_error_count"] == 0
    assert summary["commit_error_rate"] == 0.0
    assert summary["itx_count"] == 2887676
    assert summary["itx_needcopy_bytes"] == 4918663896


def test_summarize_zil_stats_error_rate():
    summary = summarize_zil_stats({
        "zil_commit_count": 200,
        "zil_commit_error_count": 2,
    })
    assert summary["commit_error_rate"] == 1.0


def test_summarize_zil_stats_empty():
    summary = summarize_zil_stats({})
    assert summary["commit_count"] == 0
    assert summary["commit_error_rate"] == 0.0


# ---------------------------------------------------------------------------
# Service readers
# ---------------------------------------------------------------------------


def _fake_path(mapping):
    """Return a ``Path`` stand-in that redirects known absolute paths.

    Unmapped paths resolve to a guaranteed-nonexistent location so tests that
    assert "kstat missing" do not accidentally read the real host's /proc.
    """
    def _path(value):
        target = mapping.get(str(value))
        if target is not None:
            return Path(target)
        return Path("/nonexistent" + str(value))
    return _path


@pytest.fixture
def service():
    return ZFSObservabilityService()


def test_get_zil_stats_linux(service, monkeypatch, zil_linux):
    monkeypatch.setattr(obs, "is_freebsd", lambda: False)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(
        obs, "Path", _fake_path({"/proc/spl/kstat/zfs/zil": FIXTURES / "zil_kstat_linux.txt"})
    )

    result = service.get_zil_stats()

    assert result["available"] is True
    assert result["platform"] == "Linux"
    assert result["source"] == "/proc/spl/kstat/zfs/zil"
    assert result["stats"]["zil_commit_count"] == 935916
    assert result["summary"]["commit_count"] == 935916


def test_get_zil_stats_linux_missing_kstat(service, monkeypatch):
    monkeypatch.setattr(obs, "is_freebsd", lambda: False)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(obs, "Path", _fake_path({}))

    result = service.get_zil_stats()

    assert result["available"] is False
    assert "not available" in result["error"]


def test_get_zil_stats_freebsd(service, monkeypatch, zil_freebsd):
    monkeypatch.setattr(obs, "is_freebsd", lambda: True)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)

    def fake_run(cmd, **kwargs):
        assert cmd == ["sysctl", "kstat.zfs.misc.zil"]
        return SimpleNamespace(returncode=0, stdout=zil_freebsd, stderr="")

    monkeypatch.setattr(obs.subprocess, "run", fake_run)

    result = service.get_zil_stats()

    assert result["available"] is True
    assert result["platform"] == "FreeBSD"
    assert result["source"] == "sysctl kstat.zfs.misc.zil"
    assert result["summary"]["itx_count"] == 2887676


def test_get_zil_stats_freebsd_sysctl_failure(service, monkeypatch):
    monkeypatch.setattr(obs, "is_freebsd", lambda: True)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(
        obs.subprocess, "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="unknown oid"),
    )

    result = service.get_zil_stats()

    assert result["available"] is False
    assert result["platform"] == "FreeBSD"


def test_get_txg_history_linux(service, monkeypatch, tmp_path):
    (tmp_path / "tank").mkdir()
    (tmp_path / "tank" / "txgs").write_text(_read("txgs_kstat_linux.txt"))

    monkeypatch.setattr(obs, "is_freebsd", lambda: False)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(
        obs, "Path",
        _fake_path({"/proc/spl/kstat/zfs": tmp_path}),
    )

    result = service.get_txg_history("tank")

    assert result["available"] is True
    assert result["pool"] == "tank"
    assert result["source"] == str(tmp_path / "tank" / "txgs")
    assert len(result["entries"]) == 100
    assert result["summary"]["newest_txg"] == 390350


def test_get_txg_history_linux_missing(service, monkeypatch):
    monkeypatch.setattr(obs, "is_freebsd", lambda: False)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(obs, "Path", _fake_path({}))

    result = service.get_txg_history("tank")

    assert result["available"] is False
    assert result["entries"] == []
    assert "zfs_txg_history" in result["error"]


def test_get_txg_history_freebsd(service, monkeypatch, txgs_freebsd):
    monkeypatch.setattr(obs, "is_freebsd", lambda: True)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)

    def fake_run(cmd, **kwargs):
        assert cmd == ["sysctl", "kstat.zfs.tank.txgs"]
        return SimpleNamespace(returncode=0, stdout=txgs_freebsd, stderr="")

    monkeypatch.setattr(obs.subprocess, "run", fake_run)

    result = service.get_txg_history("tank")

    assert result["available"] is True
    assert result["source"] == "sysctl kstat.zfs.tank.txgs"
    assert len(result["entries"]) == 3


def test_get_txg_history_freebsd_sysctl_failure(service, monkeypatch):
    monkeypatch.setattr(obs, "is_freebsd", lambda: True)
    monkeypatch.setattr(obs, "is_netbsd", lambda: False)
    monkeypatch.setattr(
        obs.subprocess, "run",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="no such oid"),
    )

    result = service.get_txg_history("tank")

    assert result["available"] is False
    assert result["error"]


# ---------------------------------------------------------------------------
# TRIM / initialize status (shared parser from services.vdev_activity)
# ---------------------------------------------------------------------------

TRIM_OUTPUT = """  pool: tank
 state: ONLINE
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
\t  sda       ONLINE       0     0     0  (42% trimmed, started at Mon Oct 27 12:00:00 2026)
\t  sdb       ONLINE       0     0     0  (100% trimmed, completed at Tue Oct 28 01:00:00 2026)

errors: No known data errors
"""

INITIALIZE_OUTPUT = """  pool: tank
 state: ONLINE
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
\t  sda       ONLINE       0     0     0  (uninitialized)
\t  sdb       ONLINE       0     0     0  (initializing)

errors: No known data errors
"""


def test_get_vdev_activity_uses_shared_parsers(service, monkeypatch):
    def fake_run_zfs(cmd, **kwargs):
        if cmd[2] == "-t":
            return SimpleNamespace(returncode=0, stdout=TRIM_OUTPUT, stderr="")
        return SimpleNamespace(returncode=0, stdout=INITIALIZE_OUTPUT, stderr="")

    monkeypatch.setattr(obs, "run_zfs_command", fake_run_zfs)

    result = service.get_vdev_activity("tank")

    assert result["available"] is True
    assert result["trim"]["kind"] == "trim"
    # Pool-level state is "in progress" because one device is still trimming;
    # the percentage is the average of the per-device progress (42 and 100).
    assert result["trim"]["state"] == "in_progress"
    assert result["trim"]["percent"] == 71
    assert result["trim"]["counts"]["in_progress"] == 1
    assert result["trim"]["counts"]["completed"] == 1
    assert result["initialize"]["kind"] == "initialize"
    assert result["initialize"]["state"] == "in_progress"
    assert result["initialize"]["counts"]["none"] == 1


def test_get_vdev_activity_unsupported(service, monkeypatch):
    monkeypatch.setattr(
        obs, "run_zfs_command",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="invalid option 't'"),
    )

    result = service.get_vdev_activity("tank")

    assert result["available"] is False
    assert "not available" in result["error"]
    assert result["trim"]["error"]
    assert result["initialize"]["error"]


def test_get_vdev_activity_raises_is_reported(service, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="zpool status", timeout=30)

    monkeypatch.setattr(obs, "run_zfs_command", boom)

    result = service.get_vdev_activity("tank")

    assert result["available"] is False
    assert result["trim"]["error"]
