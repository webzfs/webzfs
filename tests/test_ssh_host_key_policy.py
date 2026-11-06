"""SSH host-key verification policy tests (issue #4).

Covers the command builders in ``services/`` and the host-key policy wiring
shared by replication, fleet monitoring and SSH key installation.
"""
from pathlib import Path

import asyncio

import paramiko
import pytest

from services.ssh_connection import (
    HostKeyVerificationError,
    SSHConnectionService,
    host_key_error_message,
    is_host_key_error,
)

SERVICES_DIR = Path(__file__).resolve().parents[1] / "services"

# Options that silently disable host-key verification. None of these may
# appear anywhere in services/: every SSH path must fail closed.
INSECURE_PATTERNS = (
    "StrictHostKeyChecking=no",
    "UserKnownHostsFile=/dev/null",
    "AutoAddPolicy",
)


def test_no_insecure_ssh_options_in_services():
    offenders = []
    for path in sorted(SERVICES_DIR.rglob("*.py")):
        text = path.read_text(encoding="utf-8")
        for pattern in INSECURE_PATTERNS:
            if pattern in text:
                offenders.append(f"{path.name}: {pattern}")
    assert offenders == [], f"insecure SSH options remain: {offenders}"


def test_host_key_options_pin_webzfs_known_hosts():
    options = SSHConnectionService().get_host_key_options()
    assert "StrictHostKeyChecking=yes" in options
    user_known_hosts = [o for o in options if o.startswith("UserKnownHostsFile=")]
    assert len(user_known_hosts) == 1
    assert "/dev/null" not in user_known_hosts[0]
    assert "webzfs_known_hosts" in user_known_hosts[0]


def test_is_host_key_error_recognises_openssh_and_paramiko_text():
    assert is_host_key_error("Host key verification failed.")
    assert is_host_key_error("WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!")
    assert is_host_key_error("no matching host key type found")
    assert is_host_key_error("host key for x not found in known_hosts")
    assert not is_host_key_error("Permission denied (publickey).")
    assert not is_host_key_error("")


def test_host_key_error_message_is_actionable():
    message = host_key_error_message("10.0.0.5", 2222, "key changed")
    assert "10.0.0.5:2222" in message
    assert "known_hosts" in message
    assert "Trust host key" in message
    assert "key changed" in message


def test_configure_paramiko_client_rejects_unknown_hosts(tmp_path, monkeypatch):
    known_hosts = tmp_path / "webzfs_known_hosts"
    known_hosts.write_text("", encoding="utf-8")

    service = SSHConnectionService()
    monkeypatch.setattr(service, "known_hosts_file", known_hosts)

    client = paramiko.SSHClient()
    service.configure_paramiko_client(client)

    assert isinstance(client._policy, paramiko.RejectPolicy)
    assert client._host_keys_filename == str(known_hosts)


def test_changed_host_key_raises_and_leaves_pinned_file_untouched(tmp_path, monkeypatch):
    known_hosts = tmp_path / "webzfs_known_hosts"
    known_hosts.write_text("example.com ssh-rsa AAAAB3OLDKEY\n", encoding="utf-8")

    service = SSHConnectionService()
    monkeypatch.setattr(service, "known_hosts_file", known_hosts)
    monkeypatch.setattr(
        service, "_scan_host_keys", lambda host, port: ["example.com ssh-rsa AAAAB3NEWKEY"]
    )
    monkeypatch.setattr(service, "_fingerprints_for_lines", lambda lines: [])

    # The pinning primitive is what refuses a changed key.
    with pytest.raises(HostKeyVerificationError) as excinfo:
        service._trust_host_key("example.com", 22)

    assert "example.com:22" in str(excinfo.value)
    # A changed key must never be silently re-pinned.
    assert known_hosts.read_text(encoding="utf-8") == "example.com ssh-rsa AAAAB3OLDKEY\n"


def test_pinned_host_key_is_accepted_without_rescan(tmp_path, monkeypatch):
    known_hosts = tmp_path / "webzfs_known_hosts"
    known_hosts.write_text("example.com ssh-rsa AAAAB3SAMEKEY\n", encoding="utf-8")

    service = SSHConnectionService()
    monkeypatch.setattr(service, "known_hosts_file", known_hosts)

    def _fail(*args, **kwargs):
        raise AssertionError("pinned host should not be rescanned")

    monkeypatch.setattr(service, "_scan_host_keys", _fail)
    assert service.ensure_host_key_trusted("example.com", 22) is True


def test_paramiko_host_key_text_becomes_host_key_error():
    service = SSHConnectionService()
    with pytest.raises(HostKeyVerificationError) as excinfo:
        service.raise_for_paramiko_host_key_error(
            paramiko.SSHException("Host key verification failed."), "10.0.0.5", 22
        )
    assert "10.0.0.5:22" in str(excinfo.value)


def test_paramiko_bad_host_key_becomes_host_key_error():
    service = SSHConnectionService()
    got_key = paramiko.RSAKey.generate(1024)
    expected_key = paramiko.RSAKey.generate(1024)
    exc = paramiko.BadHostKeyException("10.0.0.5", got_key, expected_key)

    with pytest.raises(HostKeyVerificationError) as excinfo:
        service.raise_for_paramiko_host_key_error(exc, "10.0.0.5", 22)
    assert "10.0.0.5:22" in str(excinfo.value)
    assert "host key changed" in str(excinfo.value)


def test_paramiko_auth_error_is_not_reported_as_host_key_error():
    service = SSHConnectionService()
    # Must not raise: authentication failures are not host-key failures.
    service.raise_for_paramiko_host_key_error(
        paramiko.AuthenticationException("Authentication failed."), "10.0.0.5", 22
    )


def test_trust_host_key_explicitly_repins_changed_key(tmp_path, monkeypatch):
    known_hosts = tmp_path / "webzfs_known_hosts"
    known_hosts.write_text("example.com ssh-rsa AAAAB3OLDKEY\n", encoding="utf-8")

    service = SSHConnectionService()
    monkeypatch.setattr(service, "known_hosts_file", known_hosts)
    monkeypatch.setattr(
        service, "_scan_host_keys", lambda host, port: ["example.com ssh-rsa AAAAB3NEWKEY"]
    )
    monkeypatch.setattr(service, "_fingerprints_for_lines", lambda lines: ["SHA256:new"])

    # trust_host_key() is the explicit "Trust host key" re-pin action.
    assert service.trust_host_key("example.com", 22) == ["SHA256:new"]
    content = known_hosts.read_text(encoding="utf-8")
    assert "AAAAB3NEWKEY" in content
    assert "AAAAB3OLDKEY" not in content


def test_fleet_trust_host_key_route_repins(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    from views import fleet as fleet_views

    monkeypatch.setattr(
        fleet_views.fleet_service,
        "get_server",
        lambda server_id: {"ip": "10.0.0.9", "port": 22},
    )
    monkeypatch.setattr(
        fleet_views.ssh_service,
        "trust_host_key",
        lambda host, port: ["SHA256:abc"],
    )

    response = asyncio.run(
        fleet_views.trust_server_host_key(request=None, server_id="srv-1")
    )
    assert response.status_code == 303
    assert "success=" in response.headers["location"]


def test_fleet_client_refuses_unpinned_host(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))

    from services.fleet_monitoring import FleetMonitoringService

    fleet = FleetMonitoringService()
    ssh_service = SSHConnectionService()
    monkeypatch.setattr(ssh_service, "is_host_key_pinned", lambda host, port: False)
    monkeypatch.setattr(
        ssh_service, "configure_paramiko_client", lambda client: client
    )
    monkeypatch.setattr(fleet, "_get_ssh_service", lambda: ssh_service)

    server = {
        "ip": "10.0.0.9",
        "port": 22,
        "username": "root",
        "auth_type": "password",
        "password": "",
    }
    with pytest.raises(HostKeyVerificationError) as excinfo:
        fleet._create_ssh_client(server)
    assert "10.0.0.9:22" in str(excinfo.value)


def test_syncoid_ssh_command_uses_pinned_options(tmp_path, monkeypatch):
    from services import syncoid

    service = syncoid.SyncoidService.__new__(syncoid.SyncoidService)
    monkeypatch.setattr(
        service,
        "_pinned_ssh_options",
        lambda host: ["StrictHostKeyChecking=yes", "UserKnownHostsFile=/tmp/kh"],
    )

    recorded = {}

    def _fake_run(cmd, **kwargs):
        recorded.setdefault("cmds", []).append(cmd)

        class _Result:
            stdout = "pool/data@snap1\n"
            returncode = 0

        return _Result()

    monkeypatch.setattr(syncoid.subprocess, "run", _fake_run)
    monkeypatch.setattr(service, "_parse_size", lambda value, unit: 1024, raising=False)

    service.estimate_transfer_size("pool/data", "pool/backup", "root@example.com")

    for cmd in recorded["cmds"]:
        assert cmd[0] == "ssh"
        assert "-o" in cmd
        assert "StrictHostKeyChecking=yes" in cmd
        assert "UserKnownHostsFile=/tmp/kh" in cmd
        assert not any("/dev/null" in part for part in cmd)


def _service_with_known_hosts(tmp_path, monkeypatch, content):
    known_hosts = tmp_path / "webzfs_known_hosts"
    known_hosts.write_text(content, encoding="utf-8")
    service = SSHConnectionService.__new__(SSHConnectionService)
    service.known_hosts_file = known_hosts
    return service, known_hosts


def test_user_at_host_destination_is_matched_against_pinned_bare_host(
    tmp_path, monkeypatch
):
    """A pinned bare host must be recognised when addressed as user@host."""
    service, _ = _service_with_known_hosts(
        tmp_path, monkeypatch, "10.0.0.5 ssh-ed25519 AAAAB3PINNEDKEY\n"
    )

    assert service.is_host_key_pinned("10.0.0.5", 22) is True
    assert service.is_host_key_pinned("root@10.0.0.5", 22) is True
    assert service.is_host_key_pinned("root@10.0.0.5", 2222) is False


def test_user_at_host_non_default_port_matches_bracketed_token(
    tmp_path, monkeypatch
):
    service, _ = _service_with_known_hosts(
        tmp_path, monkeypatch, "[10.0.0.5]:2222 ssh-ed25519 AAAAB3PINNEDKEY\n"
    )

    assert service.is_host_key_pinned("root@10.0.0.5", 2222) is True


def test_replication_accepts_user_at_host_when_host_is_pinned(
    tmp_path, monkeypatch
):
    """Regression: replication must not reject user@host destinations."""
    from services import zfs_replication

    ssh_service, _ = _service_with_known_hosts(
        tmp_path, monkeypatch, "10.0.0.5 ssh-ed25519 AAAAB3PINNEDKEY\n"
    )
    replication = zfs_replication.ZFSReplicationService.__new__(
        zfs_replication.ZFSReplicationService
    )
    replication._ssh_service = ssh_service

    options = replication._host_key_options_for("root@10.0.0.5", 22)

    assert "StrictHostKeyChecking=yes" in options
    assert any(
        option.startswith("UserKnownHostsFile=")
        and "/dev/null" not in option
        for option in options
    )


def test_replication_still_rejects_unpinned_user_at_host(tmp_path, monkeypatch):
    from services import zfs_replication

    ssh_service, _ = _service_with_known_hosts(tmp_path, monkeypatch, "")
    replication = zfs_replication.ZFSReplicationService.__new__(
        zfs_replication.ZFSReplicationService
    )
    replication._ssh_service = ssh_service

    with pytest.raises(HostKeyVerificationError) as excinfo:
        replication._host_key_options_for("root@10.0.0.5", 22)
    assert "10.0.0.5:22" in str(excinfo.value)
    assert "root@" not in str(excinfo.value)
