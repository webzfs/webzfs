"""Unit tests for resumable native replication (issue #7).

Covers the receive builder with and without -s, the `zfs send -t` resume
builder and its flag whitelist, the `zfs receive -A` discard builder, and
the storage/service plumbing that records and clears receive_resume_token.
"""
import pytest

from services.storage import FileStorageService
from services.zfs_replication import (
    RESUME_SEND_ALLOWED_FLAGS,
    ReplicationType,
    ZFSReplicationService,
)


TOKEN = "1-e8a9e0d0-6b1c-4d2f-9a3e-2f4b5c6d7e8f"


class _NullEmail:
    """Stand-in for EmailNotificationService; resume/discard never mail."""

    def send_job_success_notification(self, **kwargs):  # pragma: no cover
        raise AssertionError("email should not be sent by resume/discard")

    def send_job_failure_notification(self, **kwargs):  # pragma: no cover
        raise AssertionError("email should not be sent by resume/discard")


def make_service(tmp_path):
    """Build a service with isolated storage and no real email delivery."""
    service = ZFSReplicationService.__new__(ZFSReplicationService)
    service.storage = FileStorageService(str(tmp_path))
    service.email = _NullEmail()
    return service


# --- receive builder -------------------------------------------------------

def test_receive_command_without_resumable_has_no_s(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_receive_command("tank/backup", ReplicationType.LOCAL, {})
    assert cmd == ["zfs", "receive", "tank/backup"]
    assert "-s" not in cmd


def test_receive_command_with_resumable_adds_s(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_receive_command(
        "tank/backup", ReplicationType.LOCAL, {"resumable": True}
    )
    assert cmd == ["zfs", "receive", "-s", "tank/backup"]


def test_receive_command_resumable_and_force(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_receive_command(
        "tank/backup", ReplicationType.PUSH, {"force": True, "resumable": True}
    )
    assert cmd == ["zfs", "receive", "-F", "-s", "tank/backup"]


def test_receive_command_ignores_false_resumable(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_receive_command(
        "tank/backup", ReplicationType.LOCAL, {"resumable": False}
    )
    assert "-s" not in cmd


# --- resume builder --------------------------------------------------------

def test_resume_command_plain(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_resume_send_command(TOKEN)
    assert cmd == ["zfs", "send", "-t", TOKEN]


def test_resume_command_allowed_flags(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_resume_send_command(TOKEN, ["v", "P", "n", "e"])
    assert cmd == ["zfs", "send", "-P", "-e", "-n", "-v", "-t", TOKEN]
    assert set(RESUME_SEND_ALLOWED_FLAGS) == {"P", "e", "n", "v"}


def test_resume_command_flag_order_is_stable(tmp_path):
    service = make_service(tmp_path)
    cmd = service._build_resume_send_command(TOKEN, ["v", "P"])
    assert cmd == ["zfs", "send", "-P", "-v", "-t", TOKEN]


@pytest.mark.parametrize("flag", ["R", "r", "i", "I", "w", "L", "c", "p", "b", "h", "S"])
def test_resume_command_rejects_flags_form_does_not_accept(tmp_path, flag):
    service = make_service(tmp_path)
    with pytest.raises(ValueError, match="Unsupported flag"):
        service._build_resume_send_command(TOKEN, [flag])


@pytest.mark.parametrize(
    "bad_token",
    ["", "  ", "1-abc def", "1-abc;rm -rf /", "1-abc|cat", "-e"],
)
def test_resume_command_rejects_invalid_tokens(tmp_path, bad_token):
    service = make_service(tmp_path)
    with pytest.raises(ValueError, match="Invalid receive_resume_token"):
        service._build_resume_send_command(bad_token)


# --- discard builder -------------------------------------------------------

def test_discard_command_uses_receive_abort(tmp_path):
    service = make_service(tmp_path)
    assert service._build_discard_command("tank/backup") == [
        "zfs", "receive", "-A", "tank/backup"
    ]


def test_discard_command_validates_dataset_name(tmp_path):
    service = make_service(tmp_path)
    with pytest.raises(ValueError):
        service._build_discard_command("tank/backup; rm -rf /")


# --- storage ---------------------------------------------------------------

def test_execution_record_tracks_resumable_state(tmp_path):
    storage = FileStorageService(str(tmp_path))
    execution_id = storage.create_execution_record(
        job_id=None,
        job_name="test",
        source_dataset="tank/src",
        target_dataset="tank/dst",
        replication_type="local",
        resumable=True,
        remote_host=None,
        remote_port=None,
        ssh_key=None,
    )

    execution = storage.get_execution_detail(execution_id)
    assert execution["resumable"] is True
    assert execution["receive_resume_token"] is None
    assert execution["receive_resume_target"] is None

    storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    execution = storage.get_execution_detail(execution_id)
    assert execution["receive_resume_token"] == TOKEN
    assert execution["receive_resume_target"] == "tank/dst"

    # A plain status update must not clobber the saved token.
    storage.update_execution_record(execution_id=execution_id, status="failure")
    assert storage.get_execution_detail(execution_id)["receive_resume_token"] == TOKEN

    assert storage.clear_receive_resume_token(execution_id) is True
    cleared = storage.get_execution_detail(execution_id)
    assert cleared["receive_resume_token"] is None
    assert cleared["receive_resume_target"] is None


def test_execution_record_defaults_are_not_resumable(tmp_path):
    storage = FileStorageService(str(tmp_path))
    execution_id = storage.create_execution_record(
        job_id=None,
        job_name="plain",
        source_dataset="tank/src",
        target_dataset="tank/dst",
        replication_type="local",
    )
    execution = storage.get_execution_detail(execution_id)
    assert execution["resumable"] is False
    assert execution["receive_resume_token"] is None


# --- service plumbing ------------------------------------------------------

def test_resume_success_records_execution_and_clears_token(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="local",
        resumable=True,
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    monkeypatch.setattr(
        service, "_execute_local_replication",
        lambda send_cmd, receive_cmd, new_id: {
            "bytes": 4096, "speed": "N/A", "log_output": ""
        },
    )

    result = service.execute_resume(
        execution_id=execution_id,
        receive_resume_token=TOKEN,
        target="tank/dst",
        replication_type=ReplicationType.LOCAL,
        options={},
        user="tester",
    )

    assert result["status"] == "success"
    assert result["bytes_transferred"] == 4096
    new_record = service.storage.get_execution_detail(result["execution_id"])
    assert new_record["status"] == "success"
    assert new_record["resumable"] is True
    assert new_record["receive_resume_token"] is None
    assert "zfs send -t" in new_record["command"]
    assert service.storage.get_execution_detail(execution_id)["receive_resume_token"] is None


def test_resume_failure_re_reads_and_keeps_token(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="local",
        resumable=True,
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    def boom(send_cmd, receive_cmd, new_id):
        raise Exception("stream interrupted")

    monkeypatch.setattr(service, "_execute_local_replication", boom)
    monkeypatch.setattr(
        service, "get_receive_resume_token", lambda target, rtype, opts: TOKEN
    )

    result = service.execute_resume(
        execution_id=execution_id,
        receive_resume_token=TOKEN,
        target="tank/dst",
        replication_type=ReplicationType.LOCAL,
        options={},
        user="tester",
    )

    assert result["status"] == "failure"
    assert "stream interrupted" in result["error"]
    assert "still saved" in result["error"]
    new_record = service.storage.get_execution_detail(result["execution_id"])
    assert new_record["status"] == "failure"
    assert new_record["receive_resume_token"] == TOKEN
    assert new_record["receive_resume_target"] == "tank/dst"


def test_discard_runs_receive_abort_and_clears_token(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="local",
        resumable=True,
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    captured = {}

    class _Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    def fake_run(cmd, replication_type, options, timeout=60):
        captured["cmd"] = cmd
        return _Completed()

    monkeypatch.setattr(service, "_run_zfs_command", fake_run)

    result = service.discard_resume_state(
        execution_id=execution_id,
        target="tank/dst",
        replication_type=ReplicationType.LOCAL,
        options={},
        user="tester",
    )

    assert result["status"] == "success"
    assert captured["cmd"] == ["zfs", "receive", "-A", "tank/dst"]
    assert service.storage.get_execution_detail(execution_id)["receive_resume_token"] is None


def test_discard_failure_raises_and_keeps_token(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="local",
        resumable=True,
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    class _Failed:
        returncode = 1
        stdout = ""
        stderr = "'tank/dst' does not have any resumable receive state to abort"

    monkeypatch.setattr(
        service, "_run_zfs_command",
        lambda cmd, replication_type, options, timeout=60: _Failed(),
    )

    with pytest.raises(Exception, match="resumable receive state"):
        service.discard_resume_state(
            execution_id=execution_id,
            target="tank/dst",
            replication_type=ReplicationType.LOCAL,
            options={},
            user="tester",
        )

    assert service.storage.get_execution_detail(execution_id)["receive_resume_token"] == TOKEN


# --- remote (PUSH/PULL) transport persistence ------------------------------

def _transport_options(execution):
    """Mirror views._execution_transport_options for a stored record."""
    options = {}
    if execution.get("remote_host"):
        options["remote_host"] = execution["remote_host"]
        options["remote_port"] = execution.get("remote_port") or 22
    if execution.get("ssh_key"):
        options["ssh_key"] = execution["ssh_key"]
    return options


def test_push_resume_record_keeps_transport(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="push",
        resumable=True,
        remote_host="10.0.0.5",
        remote_port=2222,
        ssh_key="/keys/id_ed25519",
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    captured = {}

    def fake_remote(send_cmd, receive_cmd, replication_type, options, new_id):
        captured["options"] = dict(options)
        captured["receive_cmd"] = receive_cmd
        return {"bytes": 2048, "speed": "N/A", "log_output": ""}

    monkeypatch.setattr(service, "_execute_remote_replication", fake_remote)

    result = service.execute_resume(
        execution_id=execution_id,
        receive_resume_token=TOKEN,
        target="tank/dst",
        replication_type=ReplicationType.PUSH,
        options=_transport_options(service.storage.get_execution_detail(execution_id)),
        user="tester",
    )

    assert result["status"] == "success"
    assert captured["options"]["remote_host"] == "10.0.0.5"
    assert captured["options"]["remote_port"] == 2222
    assert captured["options"]["ssh_key"] == "/keys/id_ed25519"

    new_record = service.storage.get_execution_detail(result["execution_id"])
    assert new_record["remote_host"] == "10.0.0.5"
    assert new_record["remote_port"] == 2222
    assert new_record["ssh_key"] == "/keys/id_ed25519"
    assert "ssh -p 2222 -i /keys/id_ed25519 10.0.0.5" in new_record["command"]
    assert "zfs send -t" in new_record["command"]


def test_chained_push_resume_rebuilds_valid_ssh_argv(tmp_path, monkeypatch):
    service = make_service(tmp_path)
    execution_id = service.storage.create_execution_record(
        job_id=None,
        job_name="original",
        source_dataset="tank/src@snap",
        target_dataset="tank/dst",
        replication_type="push",
        resumable=True,
        remote_host="10.0.0.5",
        remote_port=2222,
        ssh_key="/keys/id_ed25519",
    )
    service.storage.update_execution_record(
        execution_id=execution_id,
        status="failure",
        receive_resume_token=TOKEN,
        receive_resume_target="tank/dst",
    )

    def boom(send_cmd, receive_cmd, replication_type, options, new_id):
        raise Exception("stream interrupted again")

    monkeypatch.setattr(service, "_execute_remote_replication", boom)
    monkeypatch.setattr(
        service, "get_receive_resume_token", lambda target, rtype, opts: TOKEN
    )

    first = service.execute_resume(
        execution_id=execution_id,
        receive_resume_token=TOKEN,
        target="tank/dst",
        replication_type=ReplicationType.PUSH,
        options=_transport_options(service.storage.get_execution_detail(execution_id)),
        user="tester",
    )

    assert first["status"] == "failure"
    first_record = service.storage.get_execution_detail(first["execution_id"])
    assert first_record["remote_host"] == "10.0.0.5"
    assert first_record["remote_port"] == 2222
    assert first_record["ssh_key"] == "/keys/id_ed25519"
    assert first_record["receive_resume_token"] == TOKEN
    # Only the new record still offers Resume for this saved state.
    assert service.storage.get_execution_detail(execution_id)["receive_resume_token"] is None

    captured = {}

    def fake_remote(send_cmd, receive_cmd, replication_type, options, new_id):
        captured["options"] = dict(options)
        captured["receive_cmd"] = receive_cmd
        return {"bytes": 512, "speed": "N/A", "log_output": ""}

    monkeypatch.setattr(service, "_execute_remote_replication", fake_remote)

    second = service.execute_resume(
        execution_id=first["execution_id"],
        receive_resume_token=TOKEN,
        target="tank/dst",
        replication_type=ReplicationType.PUSH,
        options=_transport_options(first_record),
        user="tester",
    )

    assert second["status"] == "success"
    assert captured["options"]["remote_host"] == "10.0.0.5"
    argv = service._build_ssh_command(captured["receive_cmd"], captured["options"])
    assert argv[:5] == ["ssh", "-p", "2222", "-i", "/keys/id_ed25519"]
    assert argv[-1] == "tank/dst"
    assert "10.0.0.5" in argv

