"""Unit tests for the per-vdev TRIM/initialize status parsers (issue #14)."""

from services.vdev_activity import (
    INITIALIZE,
    STATE_COMPLETED,
    STATE_IN_PROGRESS,
    STATE_NONE,
    STATE_SUSPENDED,
    STATE_UNKNOWN,
    STATE_UNSUPPORTED,
    TRIM,
    parse_initialize_status,
    parse_trim_status,
    parse_vdev_activity,
    summarize_vdev_activity,
)

TRIM_STATUS = """  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:00:05 with 0 errors on Sun Oct 26 10:00:00 2026
config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          mirror-0  ONLINE       0     0     0
            sda     ONLINE       0     0     0  (42% trimmed, started at Mon Oct 27 12:00:00 2026)
            sdb     ONLINE       0     0     0  (100% trimmed, completed at Mon Oct 27 12:30:00 2026)
          sdc       ONLINE       0     0     0  (untrimmed)
          sdd       ONLINE       0     0     0  (trim unsupported)

errors: No known data errors
"""

INITIALIZE_STATUS = """  pool: tank
 state: ONLINE
config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda       ONLINE       0     0     0  (12% initialized, started at Mon Oct 27 12:00:00 2026)
          sdb       ONLINE       0     0     0  (uninitialized)
          sdc       ONLINE       0     0     0  (initializing)

errors: No known data errors
"""


def test_parse_trim_mixed_states():
    summary = parse_trim_status(TRIM_STATUS)

    assert summary.kind == TRIM
    assert summary.state == STATE_IN_PROGRESS
    assert [(d.name, d.state, d.percent) for d in summary.devices] == [
        ("sda", STATE_IN_PROGRESS, 42),
        ("sdb", STATE_COMPLETED, 100),
        ("sdc", STATE_NONE, None),
        ("sdd", STATE_UNSUPPORTED, None),
    ]
    sda = summary.devices[0]
    assert sda.started_at == "Mon Oct 27 12:00:00 2026"
    assert sda.annotation == "(42% trimmed, started at Mon Oct 27 12:00:00 2026)"
    assert summary.devices[1].completed_at == "Mon Oct 27 12:30:00 2026"


def test_trim_summary_counts_and_percent():
    summary = parse_trim_status(TRIM_STATUS)

    assert summary.counts == {
        STATE_IN_PROGRESS: 1,
        STATE_SUSPENDED: 0,
        STATE_COMPLETED: 1,
        STATE_UNSUPPORTED: 1,
        STATE_NONE: 1,
        STATE_UNKNOWN: 0,
    }
    # Average of the annotated percentages (42 and 100).
    assert summary.percent == 71


def test_trim_parser_ignores_initialize_annotations():
    assert parse_vdev_activity(INITIALIZE_STATUS, TRIM) == []


def test_initialize_parser_ignores_trim_annotations():
    assert parse_vdev_activity(TRIM_STATUS, INITIALIZE) == []


def test_parse_initialize_states():
    summary = parse_initialize_status(INITIALIZE_STATUS)

    assert summary.kind == INITIALIZE
    assert summary.state == STATE_IN_PROGRESS
    assert [(d.name, d.state, d.percent) for d in summary.devices] == [
        ("sda", STATE_IN_PROGRESS, 12),
        ("sdb", STATE_NONE, None),
        ("sdc", STATE_IN_PROGRESS, None),
    ]


def test_parse_trim_skips_group_rows_and_section_words():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          mirror-0  ONLINE       0     0     0
            sda     ONLINE       0     0     0  (untrimmed)
        logs
          sdb       ONLINE       0     0     0  (untrimmed)
errors: No known data errors
"""
    devices = parse_vdev_activity(output, TRIM)

    assert [d.name for d in devices] == ["sda", "sdb"]


def test_parse_trim_suspended():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda     ONLINE       0     0     0  (37% trimmed, suspended at Mon Oct 27 12:05:00 2026)

errors: No known data errors
"""
    summary = parse_trim_status(output)

    assert summary.state == STATE_SUSPENDED
    assert summary.devices[0].percent == 37
    assert summary.devices[0].suspended_at == "Mon Oct 27 12:05:00 2026"


def test_parse_trim_completed_only():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda     ONLINE       0     0     0  (100% trimmed, completed at Mon Oct 27 12:30:00 2026)
          sdb     ONLINE       0     0     0  (100% trimmed, completed at Mon Oct 27 12:30:00 2026)

errors: No known data errors
"""
    summary = parse_trim_status(output)

    assert summary.state == STATE_COMPLETED
    assert summary.percent == 100


def test_parse_trim_untrimmed_only():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda     ONLINE       0     0     0  (untrimmed)

errors: No known data errors
"""
    summary = parse_trim_status(output)

    assert summary.state == STATE_NONE
    assert summary.percent is None


def test_parse_trim_unsupported_only():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda     ONLINE       0     0     0  (trim unsupported)

errors: No known data errors
"""
    summary = parse_trim_status(output)

    assert summary.state == STATE_UNSUPPORTED


def test_parse_mixed_state_rolls_up_as_mixed():
    output = """config:

        NAME        STATE     READ WRITE CKSUM
        tank        ONLINE       0     0     0
          sda     ONLINE       0     0     0  (100% trimmed, completed at Mon Oct 27 12:30:00 2026)
          sdb     ONLINE       0     0     0  (untrimmed)

errors: No known data errors
"""
    summary = parse_trim_status(output)

    assert summary.state == "mixed"
    assert summary.percent == 100


def test_empty_output_is_unknown():
    for parser in (parse_trim_status, parse_initialize_status):
        summary = parser("")
        assert summary.state == STATE_UNKNOWN
        assert summary.devices == []
        assert summary.percent is None


def test_output_without_config_block_is_unknown():
    summary = parse_trim_status("  pool: tank\n state: ONLINE\nerrors: No known data errors\n")
    assert summary.state == STATE_UNKNOWN


def test_unsupported_kind_raises():
    try:
        parse_vdev_activity("", "scrub")
    except ValueError as exc:
        assert "unsupported activity kind" in str(exc)
    else:  # pragma: no cover - explicit failure path
        raise AssertionError("parse_vdev_activity accepted an invalid kind")


def test_to_dict_shape():
    summary = parse_trim_status(TRIM_STATUS)
    payload = summary.to_dict()

    assert set(payload) == {"kind", "state", "percent", "counts", "devices"}
    assert payload["devices"][0] == {
        "name": "sda",
        "state": STATE_IN_PROGRESS,
        "percent": 42,
        "started_at": "Mon Oct 27 12:00:00 2026",
        "completed_at": None,
        "suspended_at": None,
        "annotation": "(42% trimmed, started at Mon Oct 27 12:00:00 2026)",
    }


def test_summarize_without_devices_is_unknown():
    summary = summarize_vdev_activity([], TRIM)
    assert summary.state == STATE_UNKNOWN
