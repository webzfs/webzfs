"""Tests for the read-only pool import option (issue #1).

Covers:
* the exact ``zpool import`` argv with and without ``-o readonly=on``;
* pool-name validation still rejecting unsafe names;
* route-level form handling of the ``readonly`` field (forwarded to the
  service, recorded by the audit logger, and reflected in the redirect).
"""

import subprocess
from urllib.parse import quote

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from auth.dependencies import get_current_user
from services import zfs_pool as zfs_pool_module
from services.zfs_pool import ZFSPoolService
from views import zfs_pools as zfs_pools_views


class _CommandRecorder:
    """Stand-in for ``run_zfs_command`` that records argv without executing."""

    def __init__(self, error=None):
        self.calls = []
        self.error = error

    def __call__(self, cmd, **kwargs):
        self.calls.append(list(cmd))
        if self.error is not None:
            raise self.error
        return subprocess.CompletedProcess(cmd, 0, stdout="", stderr="")


@pytest.fixture
def recorder(monkeypatch):
    rec = _CommandRecorder()
    monkeypatch.setattr(zfs_pool_module, "run_zfs_command", rec)
    return rec


@pytest.fixture
def service():
    return ZFSPoolService()


# ---------------------------------------------------------------------------
# Service: exact argv construction
# ---------------------------------------------------------------------------


def test_import_pool_without_readonly_keeps_existing_argv(service, recorder):
    service.import_pool("tank")
    assert recorder.calls == [["zpool", "import", "tank"]]


def test_import_pool_readonly_appends_readonly_property(service, recorder):
    service.import_pool("tank", readonly=True)
    assert recorder.calls == [["zpool", "import", "-o", "readonly=on", "tank"]]


def test_import_pool_force_with_readonly_combines_flags(service, recorder):
    service.import_pool("tank", force=True, readonly=True)
    assert recorder.calls == [
        ["zpool", "import", "-f", "-o", "readonly=on", "tank"]
    ]


def test_import_pool_altroot_with_readonly_combines_flags(service, recorder):
    service.import_pool("tank", altroot="/mnt/altroot", readonly=True)
    assert recorder.calls == [
        ["zpool", "import", "-R", "/mnt/altroot", "-o", "readonly=on", "tank"]
    ]


def test_import_pool_readonly_does_not_add_force_or_recovery_flags(service, recorder):
    service.import_pool("tank", readonly=True)
    (argv,) = recorder.calls
    assert "-f" not in argv
    assert "-N" not in argv
    assert "-R" not in argv
    assert "-d" not in argv


@pytest.mark.parametrize("bad_name", ["", "tank;rm -rf /", "../tank", "-f"])
def test_import_pool_rejects_invalid_name_before_running(service, recorder, bad_name):
    with pytest.raises(ValueError):
        service.import_pool(bad_name, readonly=True)
    assert recorder.calls == []


def test_import_pool_propagates_command_failure(service, recorder):
    recorder.error = subprocess.CalledProcessError(
        1, ["zpool", "import"], stderr="cannot import 'tank': I/O error"
    )
    with pytest.raises(Exception, match="Failed to import pool"):
        service.import_pool("tank", readonly=True)
    assert recorder.calls == [["zpool", "import", "-o", "readonly=on", "tank"]]


# ---------------------------------------------------------------------------
# Route: form handling
# ---------------------------------------------------------------------------


class _FakePoolService:
    def __init__(self, error=None):
        self.import_calls = []
        self.error = error

    def import_pool(self, pool_name, force=False, readonly=False):
        self.import_calls.append(
            {"pool_name": pool_name, "force": force, "readonly": readonly}
        )
        if self.error is not None:
            raise self.error


class _FakeAuditLogger:
    def __init__(self):
        self.calls = []

    def log_pool_import(self, **kwargs):
        self.calls.append(kwargs)


@pytest.fixture
def route_client(monkeypatch):
    fake_service = _FakePoolService()
    fake_audit = _FakeAuditLogger()
    monkeypatch.setattr(zfs_pools_views, "pool_service", fake_service)
    monkeypatch.setattr(zfs_pools_views, "audit_logger", fake_audit)

    app = FastAPI()
    app.include_router(zfs_pools_views.router)
    app.dependency_overrides[get_current_user] = lambda: "test-user"

    return TestClient(app), fake_service, fake_audit


def test_route_readonly_form_forwards_option(route_client):
    client, fake_service, fake_audit = route_client
    response = client.post(
        "/zfs/pools/import/tank", data={"readonly": "true"}, follow_redirects=False
    )

    assert response.status_code == 303
    assert response.headers["location"] == (
        "/zfs/pools?message="
        + quote("Pool tank imported read-only successfully", safe="")
    )
    assert fake_service.import_calls == [
        {"pool_name": "tank", "force": False, "readonly": True}
    ]
    assert fake_audit.calls == [
        {
            "user": "test-user",
            "pool_name": "tank",
            "force": False,
            "readonly": True,
        }
    ]


def test_route_without_readonly_form_keeps_existing_behavior(route_client):
    client, fake_service, fake_audit = route_client
    response = client.post(
        "/zfs/pools/import/tank", data={}, follow_redirects=False
    )

    assert response.status_code == 303
    assert response.headers["location"] == (
        "/zfs/pools?message=" + quote("Pool tank imported successfully", safe="")
    )
    assert fake_service.import_calls == [
        {"pool_name": "tank", "force": False, "readonly": False}
    ]
    assert fake_audit.calls == [
        {
            "user": "test-user",
            "pool_name": "tank",
            "force": False,
            "readonly": False,
        }
    ]


def test_route_force_form_does_not_enable_readonly(route_client):
    client, fake_service, _fake_audit = route_client
    client.post("/zfs/pools/import/tank", data={"force": "true"}, follow_redirects=False)

    assert fake_service.import_calls == [
        {"pool_name": "tank", "force": True, "readonly": False}
    ]


def test_route_explicit_readonly_false_is_not_readonly(route_client):
    client, fake_service, _fake_audit = route_client
    client.post(
        "/zfs/pools/import/tank", data={"readonly": "false"}, follow_redirects=False
    )

    assert fake_service.import_calls == [
        {"pool_name": "tank", "force": False, "readonly": False}
    ]


def test_route_readonly_failure_reports_attempt_and_audits(route_client):
    client, fake_service, fake_audit = route_client
    fake_service.error = Exception(
        "cannot import 'tank': pool is currently in use"
    )

    response = client.post(
        "/zfs/pools/import/tank", data={"readonly": "true"}, follow_redirects=False
    )

    assert response.status_code == 303
    location = response.headers["location"]
    assert location.startswith("/zfs/pools/import/list?")
    assert "readonly_attempted=true" in location
    assert "force_attempted=false" in location
    assert "failed_pool=tank" in location
    assert fake_audit.calls == [
        {
            "user": "test-user",
            "pool_name": "tank",
            "force": False,
            "readonly": True,
            "success": False,
            "error": "cannot import 'tank': pool is currently in use",
        }
    ]
