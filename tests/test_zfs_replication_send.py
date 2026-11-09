"""Unit tests for the native ZFS send builder (issue #8).

Covers the -i / -I switch in ``_build_send_command`` and verifies the
size estimate uses the same incremental flag as the real send.
"""
from types import SimpleNamespace

import pytest

from services import zfs_replication as zfs_replication_module
from services.zfs_replication import (
    CompressionMethod,
    ReplicationType,
    ZFSReplicationService,
)

BASE = "tank/data@a"
TARGET_SNAP = "tank/data@d"


@pytest.fixture
def service():
    return ZFSReplicationService()


def build(service, **overrides):
    kwargs = dict(
        dataset="tank/data",
        snapshot=TARGET_SNAP,
        incremental=True,
        recursive=False,
        raw=False,
        large_blocks=False,
        compression=CompressionMethod.LZ4,
        base_snapshot=BASE,
    )
    kwargs.update(overrides)
    return service._build_send_command(**kwargs)


def incremental_flag(cmd):
    """Return the incremental flag in cmd ('-i', '-I') or None."""
    flags = [part for part in cmd if part in ("-i", "-I")]
    assert len(flags) <= 1, f"both -i and -I emitted: {cmd}"
    return flags[0] if flags else None


def test_incremental_defaults_to_dash_i(service):
    cmd = build(service)
    assert cmd == ["zfs", "send", "-c", "-i", BASE, TARGET_SNAP]
    assert incremental_flag(cmd) == "-i"


def test_include_intermediate_uses_dash_I(service):
    cmd = build(service, include_intermediate=True)
    assert cmd == ["zfs", "send", "-c", "-I", BASE, TARGET_SNAP]
    assert incremental_flag(cmd) == "-I"


@pytest.mark.parametrize("include_intermediate", [False, True])
def test_never_emits_both_flags(service, include_intermediate):
    cmd = build(
        service,
        recursive=True,
        raw=True,
        large_blocks=True,
        include_intermediate=include_intermediate,
    )
    assert "-i" not in cmd or "-I" not in cmd
    assert incremental_flag(cmd) == ("-I" if include_intermediate else "-i")


@pytest.mark.parametrize("include_intermediate", [False, True])
def test_full_send_has_no_incremental_flag(service, include_intermediate):
    cmd = build(
        service,
        incremental=False,
        base_snapshot=None,
        include_intermediate=include_intermediate,
    )
    assert incremental_flag(cmd) is None


def test_incremental_without_base_has_no_flag(service):
    cmd = build(service, base_snapshot=None, include_intermediate=True)
    assert incremental_flag(cmd) is None


@pytest.mark.parametrize("reference", ["tank/data", "tank/data@", "tank/data@bad snap"])
def test_invalid_snapshot_reference_rejected(service, reference):
    with pytest.raises(ValueError):
        build(service, snapshot=reference)


def test_invalid_base_snapshot_rejected(service):
    with pytest.raises(ValueError):
        build(service, base_snapshot="tank/data@bad snapshot")


@pytest.mark.parametrize("include_intermediate", [False, True])
def test_estimate_uses_same_flag(monkeypatch, service, include_intermediate):
    captured = {}

    def fake_run_zfs_command(cmd):
        captured["cmd"] = list(cmd)
        return SimpleNamespace(stdout="", stderr="size\t123456\n")

    monkeypatch.setattr(
        zfs_replication_module, "run_zfs_command", fake_run_zfs_command
    )
    monkeypatch.setattr(service, "_get_snapshots", lambda dataset: [BASE, TARGET_SNAP])

    estimate = service.estimate_transfer_size(
        source="tank/data",
        target="tank/data-copy",
        incremental=True,
        include_intermediate=include_intermediate,
    )

    expected_flag = "-I" if include_intermediate else "-i"
    assert captured["cmd"] == ["zfs", "send", "-nv", expected_flag, BASE, TARGET_SNAP]
    assert estimate["estimated_bytes"] == 123456
    assert estimate["include_intermediate"] is include_intermediate


@pytest.mark.parametrize("include_intermediate", [False, True])
def test_dry_run_size_estimate_reuses_send_flag(
    monkeypatch, service, include_intermediate
):
    send_cmd = build(service, include_intermediate=include_intermediate)
    captured = {}

    def fake_run(argv, **kwargs):
        captured["argv"] = list(argv)
        return SimpleNamespace(stdout="", stderr="size\t4096\n")

    monkeypatch.setattr(
        zfs_replication_module.subprocess, "run", fake_run
    )

    total = service._estimate_send_size(send_cmd)

    expected_flag = "-I" if include_intermediate else "-i"
    argv = captured["argv"]
    assert argv.index("-n") == argv.index("send") + 1
    assert argv.index("-P") == argv.index("-n") + 1
    assert expected_flag in argv
    assert argv[-1] == TARGET_SNAP
    assert total == 4096


def test_build_receive_command_unchanged(service):
    cmd = service._build_receive_command(
        "tank/data-copy", ReplicationType.LOCAL, {"force": False}
    )
    assert cmd == ["zfs", "receive", "tank/data-copy"]
