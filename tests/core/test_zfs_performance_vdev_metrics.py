"""Tests for the ``zpool`` slow-I/O and latency-histogram parsers (M11)."""

from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

import services.zfs_performance as perf
from services.zfs_performance import (
    ZFSPerformanceService,
    parse_histogram_value,
    parse_latency_histogram,
    parse_slow_io_status,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures" / "observability"


def _read(name: str) -> str:
    return (FIXTURES / name).read_text()


@pytest.fixture(scope="module")
def status_s() -> str:
    return _read("zpool_status_s_linux.txt")


@pytest.fixture(scope="module")
def iostat_w() -> str:
    return _read("zpool_iostat_w_linux.txt")


# ---------------------------------------------------------------------------
# parse_histogram_value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("text,expected", [
    ("0", 0),
    ("-", 0),
    ("", 0),
    ("42", 42),
    ("4.82K", 4820),
    ("682K", 682000),
    ("1.63K", 1630),
    ("2.42M", 2420000),
    ("1.5G", 1500000000),
    ("garbage", 0),
])
def test_parse_histogram_value(text, expected):
    assert parse_histogram_value(text) == expected


# ---------------------------------------------------------------------------
# parse_slow_io_status
# ---------------------------------------------------------------------------


def test_parse_slow_io_status_live_fixture(status_s):
    parsed = parse_slow_io_status(status_s)

    assert parsed["available"] is True
    assert parsed["pool"] == "ai-hermes"
    assert parsed["total_slow_ios"] == 0
    assert len(parsed["slow_io"]) == 1
    device, count = next(iter(parsed["slow_io"].items()))
    assert device.startswith("nvme-")
    assert count == 0


def test_parse_slow_io_status_no_column():
    parsed = parse_slow_io_status(_read("zpool_status_no_slow.txt"))

    assert parsed["available"] is False
    assert parsed["pool"] == "tank"
    assert parsed["slow_io"] == {}


def test_parse_slow_io_status_counts_and_pool_row():
    text = """  pool: tank
 state: ONLINE
config:

\tNAME        STATE     READ WRITE CKSUM  SLOW
\ttank        ONLINE       0     0     0     -
\t  mirror-0  ONLINE       0     0     0     -
\t    sda     ONLINE       0     0     0     12
\t    sdb     ONLINE       0     0     0     3

errors: No known data errors
"""
    parsed = parse_slow_io_status(text)

    assert parsed["available"] is True
    # The pool row and the mirror row carry "-" and are skipped.
    assert parsed["slow_io"] == {"sda": 12, "sdb": 3}
    assert parsed["total_slow_ios"] == 15


def test_parse_slow_io_status_empty():
    parsed = parse_slow_io_status("")
    assert parsed["available"] is False
    assert parsed["slow_io"] == {}


# ---------------------------------------------------------------------------
# parse_latency_histogram
# ---------------------------------------------------------------------------


def test_parse_latency_histogram_live_fixture(iostat_w):
    parsed = parse_latency_histogram(iostat_w)

    assert parsed["available"] is True
    assert list(parsed["pools"]) == ["ai-hermes"]

    pool = parsed["pools"]["ai-hermes"]
    assert pool["columns"] == [
        "total_wait_read", "total_wait_write",
        "disk_wait_read", "disk_wait_write",
        "syncq_wait_read", "syncq_wait_write",
        "asyncq_wait_read", "asyncq_wait_write",
        "scrub", "trim", "rebuild",
    ]
    assert len(pool["buckets"]) == 37
    assert pool["buckets"][0]["bucket"] == "1ns"
    assert pool["buckets"][-1]["bucket"] == "137s"

    row = next(b for b in pool["buckets"] if b["bucket"] == "16us")
    assert row["disk_wait_write"] == 1
    assert row["asyncq_wait_write"] == 1
    assert row["total_wait_read"] == 0


def test_parse_latency_histogram_multiple_pools():
    text = """\n
pool_a    total_wait     disk_wait    syncq_wait    asyncq_wait
latency      read  write   read  write   read  write   read  write  scrub   trim  rebuild
----------  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----
1ns             1      2      3      4      5      6      7      8      9     10     11
----------  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----

pool_b    total_wait     disk_wait    syncq_wait    asyncq_wait
latency      read  write   read  write   read  write   read  write  scrub   trim  rebuild
----------  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----  -----
2us             0      0      0      0      0      0      0      0      0      0      0
"""
    parsed = parse_latency_histogram(text)

    assert parsed["available"] is True
    assert set(parsed["pools"]) == {"pool_a", "pool_b"}
    assert parsed["pools"]["pool_a"]["buckets"] == [{
        "bucket": "1ns",
        "total_wait_read": 1,
        "total_wait_write": 2,
        "disk_wait_read": 3,
        "disk_wait_write": 4,
        "syncq_wait_read": 5,
        "syncq_wait_write": 6,
        "asyncq_wait_read": 7,
        "asyncq_wait_write": 8,
        "scrub": 9,
        "trim": 10,
        "rebuild": 11,
    }]
    assert len(parsed["pools"]["pool_b"]["buckets"]) == 1


def test_parse_latency_histogram_unavailable():
    assert parse_latency_histogram("") == {"available": False, "pools": {}}
    assert parse_latency_histogram(_read("zpool_iostat_w_unavailable.txt")) == {
        "available": False, "pools": {}
    }


# ---------------------------------------------------------------------------
# Service methods
# ---------------------------------------------------------------------------


@pytest.fixture
def service():
    return ZFSPerformanceService()


def test_get_slow_io_counts(service, monkeypatch, status_s):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=status_s, stderr="")

    monkeypatch.setattr(perf, "run_zfs_command", fake_run)

    result = service.get_slow_io_counts("ai-hermes")

    assert calls == [["zpool", "status", "-s", "ai-hermes"]]
    assert result["available"] is True
    assert result["pool"] == "ai-hermes"
    assert "timestamp" in result
    assert result["total_slow_ios"] == 0


def test_get_slow_io_counts_command_failure(service, monkeypatch):
    monkeypatch.setattr(
        perf, "run_zfs_command",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="no such pool"),
    )

    result = service.get_slow_io_counts("missing")

    assert result["available"] is False
    assert result["error"] == "no such pool"


def test_get_slow_io_counts_without_slow_column(service, monkeypatch):
    monkeypatch.setattr(
        perf, "run_zfs_command",
        lambda *a, **k: SimpleNamespace(
            returncode=0, stdout=_read("zpool_status_no_slow.txt"), stderr=""
        ),
    )

    result = service.get_slow_io_counts("tank")

    assert result["available"] is False
    assert "Slow I/O column" in result["error"]


def test_get_latency_histogram_command(service, monkeypatch, iostat_w):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=iostat_w, stderr="")

    monkeypatch.setattr(perf, "run_zfs_command", fake_run)

    result = service.get_latency_histogram("ai-hermes")

    assert calls == [["zpool", "iostat", "-w", "-y", "ai-hermes", "1", "1"]]
    assert result["available"] is True
    assert result["pools"]["ai-hermes"]["buckets"]
    assert result["timestamp"]
    assert result["raw_output"]


def test_get_latency_histogram_all_pools(service, monkeypatch, iostat_w):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(returncode=0, stdout=iostat_w, stderr="")

    monkeypatch.setattr(perf, "run_zfs_command", fake_run)

    service.get_latency_histogram()

    assert calls == [["zpool", "iostat", "-w", "-y", "1", "1"]]


def test_get_latency_histogram_failure(service, monkeypatch):
    monkeypatch.setattr(
        perf, "run_zfs_command",
        lambda *a, **k: SimpleNamespace(returncode=1, stdout="", stderr="bad option"),
    )

    result = service.get_latency_histogram("tank")

    assert result["available"] is False
    assert result["error"] == "bad option"


def test_get_latency_histogram_timeout(service, monkeypatch):
    def boom(*a, **k):
        raise subprocess.TimeoutExpired(cmd="zpool iostat -w", timeout=5)

    monkeypatch.setattr(perf, "run_zfs_command", boom)

    result = service.get_latency_histogram("tank")

    assert result["available"] is False
    assert "timed out" in result["error"]
