"""Unit tests for the scrub command builders (webzfs/webzfs#11).

Every builder must emit a list-form argv (no shell string), validate the pool
name and dates, and gate version-dependent options through the F1 capability
helper with a message the UI can show.
"""

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("CAPTION", "webzfs")
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")

from services import zfs_pool
from services.zfs_pool import ScrubUnsupportedError, ZFSPoolService


def _caps(**overrides):
    base = {
        "scrub_error": True,
        "scrub_time_range": True,
        "all_pools_flag": True,
        "effective_version": (2, 4, 4),
        "platform": "Linux",
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.fixture
def recorder(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append((cmd, kwargs))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(zfs_pool, "run_zfs_command", fake_run)
    return calls


def _service():
    return ZFSPoolService()


def test_start_scrub_argv(recorder):
    _service().scrub_pool("tank")
    assert recorder[0][0] == ["zpool", "scrub", "tank"]


def test_stop_scrub_argv(recorder):
    _service().stop_scrub("tank")
    assert recorder[0][0] == ["zpool", "scrub", "-s", "tank"]


def test_pause_scrub_argv(recorder):
    _service().pause_scrub("tank")
    assert recorder[0][0] == ["zpool", "scrub", "-p", "tank"]


def test_resume_scrub_argv(recorder):
    _service().resume_scrub("tank")
    assert recorder[0][0] == ["zpool", "scrub", "tank"]


def test_error_scrub_argv(recorder, monkeypatch):
    monkeypatch.setattr(zfs_pool, "get_capabilities", lambda **k: _caps())
    _service().error_scrub("tank")
    assert recorder[0][0] == ["zpool", "scrub", "-e", "tank"]


def test_error_scrub_gated_below_2_2(recorder, monkeypatch):
    monkeypatch.setattr(
        zfs_pool,
        "get_capabilities",
        lambda **k: _caps(scrub_error=False, effective_version=(2, 1, 9)),
    )
    with pytest.raises(ScrubUnsupportedError) as excinfo:
        _service().error_scrub("tank")
    assert "2.2" in str(excinfo.value)
    assert "2.1.9" in str(excinfo.value)
    assert recorder == []


def test_time_range_scrub_argv(recorder, monkeypatch):
    monkeypatch.setattr(zfs_pool, "get_capabilities", lambda **k: _caps())
    _service().scrub_time_range("tank", "2026-01-01 00:00", "2026-02-01 00:00")
    assert recorder[0][0] == [
        "zpool", "scrub",
        "-S", "2026-01-01 00:00",
        "-E", "2026-02-01 00:00",
        "tank",
    ]


def test_time_range_scrub_gated_below_2_4(recorder, monkeypatch):
    monkeypatch.setattr(
        zfs_pool,
        "get_capabilities",
        lambda **k: _caps(scrub_time_range=False, effective_version=(2, 3, 9)),
    )
    with pytest.raises(ScrubUnsupportedError) as excinfo:
        _service().scrub_time_range("tank", "2026-01-01 00:00", "2026-02-01 00:00")
    assert "2.4" in str(excinfo.value)
    assert recorder == []


@pytest.mark.parametrize(
    "start,end",
    [
        ("2026-01-01", "2026-02-01 00:00"),
        ("2026-01-01 00:00:00", "2026-02-01 00:00"),
        ("2026-13-01 00:00", "2026-02-01 00:00"),
        ("2026-02-01 00:00", "2026-01-01 00:00"),
        ("2026-01-01 00:00", "2026-01-01 00:00"),
        ("", "2026-02-01 00:00"),
    ],
)
def test_time_range_rejects_bad_or_reversed_dates(recorder, monkeypatch, start, end):
    monkeypatch.setattr(zfs_pool, "get_capabilities", lambda **k: _caps())
    with pytest.raises(ValueError):
        _service().scrub_time_range("tank", start, end)
    assert recorder == []


def test_scrub_all_pools_argv(recorder, monkeypatch):
    monkeypatch.setattr(zfs_pool, "get_capabilities", lambda **k: _caps())
    _service().scrub_all_pools()
    assert recorder[0][0] == ["zpool", "scrub", "-a"]


def test_scrub_all_pools_gated_below_2_4(recorder, monkeypatch):
    monkeypatch.setattr(
        zfs_pool,
        "get_capabilities",
        lambda **k: _caps(all_pools_flag=False, effective_version=(2, 3, 9)),
    )
    with pytest.raises(ScrubUnsupportedError):
        _service().scrub_all_pools()
    assert recorder == []


@pytest.mark.parametrize("bad_pool", ["", "tank; rm -rf /", "tank pool", "-tank", "tank\n"])
def test_pool_name_validation_blocks_shell_metacharacters(recorder, bad_pool):
    with pytest.raises(ValueError):
        _service().pause_scrub(bad_pool)
    with pytest.raises(ValueError):
        _service().scrub_time_range(bad_pool, "2026-01-01 00:00", "2026-02-01 00:00")
    assert recorder == []


def test_commands_are_list_form_without_shell(recorder, monkeypatch):
    monkeypatch.setattr(zfs_pool, "get_capabilities", lambda **k: _caps())
    service = _service()
    service.pause_scrub("tank")
    service.resume_scrub("tank")
    service.error_scrub("tank")
    service.scrub_time_range("tank", "2026-01-01 00:00", "2026-02-01 00:00")
    service.scrub_all_pools()
    for cmd, kwargs in recorder:
        assert isinstance(cmd, list)
        assert "shell" not in kwargs or kwargs["shell"] is False


def test_capability_error_message_names_feature_and_platform(monkeypatch):
    monkeypatch.setattr(
        zfs_pool,
        "get_capabilities",
        lambda **k: _caps(
            scrub_error=False, effective_version=None, platform="NetBSD"
        ),
    )
    with pytest.raises(ScrubUnsupportedError) as excinfo:
        _service().error_scrub("tank")
    message = str(excinfo.value)
    assert "Error scrub" in message
    assert "unknown" in message
    assert "NetBSD" in message


# ---------------------------------------------------------------------------
# Scheduled scrub on a paused pool must resume it, not restart from scratch.
# ---------------------------------------------------------------------------


PAUSED_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub paused since Sun Mar  2 10:00:00 2025
\t0B repaired, 12.34% done, 01:23:45 to go
config:
"""

COMPLETED_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
config:
"""


@pytest.fixture
def scrub_task_env(monkeypatch):
    from services import storage, task_runner

    calls = {"resume": [], "scrub": [], "updated": []}

    class _Storage:
        def get_scrub_schedule(self, task_id):
            return {"pool": "tank", "schedule": "0 0 * * *"}

        def update_scrub_schedule_status(self, task_id, **kwargs):
            calls["updated"].append((task_id, kwargs))

    class _PoolService:
        status_output = PAUSED_STATUS

        def get_pool_status(self, pool):
            return {"status_output": self.status_output}

        def resume_scrub(self, pool):
            calls["resume"].append(pool)

        def scrub_pool(self, pool):
            calls["scrub"].append(pool)

    monkeypatch.setattr(storage, "FileStorageService", _Storage)
    monkeypatch.setattr(zfs_pool, "ZFSPoolService", _PoolService)
    return task_runner, _PoolService, calls


def test_run_scrub_task_resumes_paused_scrub(scrub_task_env):
    task_runner, _, calls = scrub_task_env
    assert task_runner.run_scrub_task("7", "schedule") == 0
    assert calls["resume"] == ["tank"]
    assert calls["scrub"] == []
    assert calls["updated"][0][0] == 7
    assert calls["updated"][0][1]["last_status"] == "success"


def test_run_scrub_task_starts_scrub_when_not_paused(scrub_task_env):
    task_runner, pool_service, calls = scrub_task_env
    pool_service.status_output = COMPLETED_STATUS
    assert task_runner.run_scrub_task("7", "schedule") == 0
    assert calls["scrub"] == ["tank"]
    assert calls["resume"] == []
