"""Unit tests for the shared ``scan:`` parser (webzfs/webzfs#11).

Covers the paused state that all four legacy parsers missed, plus the states
the pool, dashboard, scrub overview, performance and health views depend on.
"""

import os

import pytest

# ``config.settings`` validates these at import time.
os.environ.setdefault("CAPTION", "webzfs")
os.environ.setdefault("SECRET_KEY", "test-secret-key-for-testing-only")

from services import scan_status as ss

PAUSED_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub paused since Sun Mar  2 10:00:00 2025
\t1.2T / 3.0T scanned at 1.0G/s, 500G / 3.0T issued at 500M/s
\t0B repaired, 12.34% done, 01:23:45 to go
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
\t  mirror-0  ONLINE       0     0     0
\t    sda     ONLINE       0     0     0
"""

IN_PROGRESS_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub in progress since Sun Mar  2 10:00:00 2025
\t1.2T / 3.0T scanned at 1.0G/s, 500G / 3.0T issued at 500M/s
\t0B repaired, 45.5% done, 01:23:45 to go
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
"""

COMPLETED_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
"""

COMPLETED_WITH_ERRORS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 512K in 00:12:34 with 3 errors on Sun Mar  2 10:00:00 2025
config:
"""

CANCELED_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub canceled on Sun Mar  2 10:00:00 2025
config:
"""

NONE_STATUS = """\
  pool: tank
 state: ONLINE
  scan: none requested
config:
"""

RESILVER_STATUS = """\
  pool: tank
 state: ONLINE
  scan: resilvered 12.3G in 00:01:02 with 0 errors on Sun Mar  2 10:00:00 2025
config:
"""

ISO_DATE_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:12:34 with 0 errors on 2025-03-02T10:00:00
config:
"""


PERMANENT_ERRORS_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
errors: Permanent errors have been detected in the following files:

\t/tank/broken/file
"""

NO_ERRORS_STATUS = """\
  pool: tank
 state: ONLINE
  scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
config:

\tNAME        STATE     READ WRITE CKSUM
\ttank        ONLINE       0     0     0
errors: No known data errors
"""


def test_paused_scan_is_detected():
    scan = ss.parse_scan(PAUSED_STATUS)
    assert scan.present is True
    assert scan.state == ss.SCAN_PAUSED
    assert scan.paused is True
    assert scan.in_progress is False
    assert scan.completed is False
    assert scan.kind == "scrub"
    assert scan.progress == pytest.approx(12.34)
    assert "paused" in scan.raw


def test_in_progress_scan_progress_from_continuation_line():
    scan = ss.parse_scan(IN_PROGRESS_STATUS)
    assert scan.state == ss.SCAN_IN_PROGRESS
    assert scan.in_progress is True
    assert scan.paused is False
    assert scan.progress == pytest.approx(45.5)


def test_completed_scan_date_and_error_count():
    scan = ss.parse_scan(COMPLETED_STATUS)
    assert scan.state == ss.SCAN_COMPLETED
    assert scan.completed is True
    assert scan.has_errors is False
    assert scan.error_count == 0
    assert scan.last_scan is not None
    assert scan.last_scan.strftime("%Y-%m-%d %H:%M:%S") == "2025-03-02 10:00:00"


def test_completed_scan_with_errors_flags_errors():
    scan = ss.parse_scan(COMPLETED_WITH_ERRORS)
    assert scan.completed is True
    assert scan.has_errors is True
    assert scan.error_count == 3


def test_canceled_scan():
    scan = ss.parse_scan(CANCELED_STATUS)
    assert scan.state == ss.SCAN_CANCELED
    assert scan.canceled is True
    assert scan.last_scan is not None


def test_none_requested_is_not_present():
    scan = ss.parse_scan(NONE_STATUS)
    assert scan.present is False
    assert scan.state == ss.SCAN_NONE
    assert scan.paused is False


def test_missing_scan_block_is_not_present():
    scan = ss.parse_scan("  pool: tank\n state: ONLINE\n")
    assert scan.present is False
    assert scan.state == ss.SCAN_NONE
    assert scan.raw == ""


def test_resilver_is_recognised_as_its_own_kind():
    scan = ss.parse_scan(RESILVER_STATUS)
    assert scan.kind == "resilver"
    assert scan.completed is True


def test_iso_completion_date():
    scan = ss.parse_scan(ISO_DATE_STATUS)
    assert scan.completed is True
    assert scan.last_scan is not None
    assert scan.last_scan.strftime("%Y-%m-%d %H:%M:%S") == "2025-03-02 10:00:00"


def test_error_scrub_marker():
    scan = ss.parse_scan(
        "  scan: scrub (error scrub) in progress since Sun Mar  2 10:00:00 2025\n"
    )
    assert scan.in_progress is True
    assert scan.error_scrub is True


def test_parse_scan_line_accepts_prefix_and_bare_text():
    with_prefix = ss.parse_scan_line("scan: scrub paused since Sun Mar  2 10:00:00 2025")
    bare = ss.parse_scan_line("scrub paused since Sun Mar  2 10:00:00 2025")
    assert with_prefix.paused is True
    assert bare.paused is True
    assert with_prefix.raw == bare.raw


def test_scan_block_stops_at_non_indented_line():
    block = ss.extract_scan_block(PAUSED_STATUS)
    assert block[0].startswith("scan:")
    assert len(block) == 3  # scan line + two tab-indented continuation lines
    assert block[-1].startswith("0B repaired")


def test_unknown_scan_wording_is_not_misreported():
    scan = ss.parse_scan("  scan: scrub queued for tomorrow\n")
    assert scan.present is True
    assert scan.state == ss.SCAN_UNKNOWN
    assert scan.in_progress is False
    assert scan.paused is False


def test_to_dict_exposes_derived_flags():
    data = ss.parse_scan(PAUSED_STATUS).to_dict()
    assert data["paused"] is True
    assert data["in_progress"] is False
    assert data["last_scan"] is None


# ---------------------------------------------------------------------------
# The four former parsers now delegate to the shared one and must agree.
# ---------------------------------------------------------------------------


def test_dashboard_parser_reports_paused():
    from services.dashboard import _parse_scrub_info

    info = _parse_scrub_info(PAUSED_STATUS)
    assert info["scrub_paused"] is True
    assert info["scrub_in_progress"] is False
    assert info["scrub_state"] == ss.SCAN_PAUSED
    assert info["scrub_progress"] == pytest.approx(12.34)
    assert "paused" in info["scan_line"]


def test_dashboard_parser_reports_completed_scrub():
    from services.dashboard import _parse_scrub_info

    info = _parse_scrub_info(COMPLETED_STATUS)
    assert info["last_scrub"] is not None
    assert info["scrub_paused"] is False
    assert info["scan_errors"] is False


def test_health_analysis_parser_reports_paused():
    from services.health_analysis import HealthAnalysisService

    info = HealthAnalysisService()._parse_scrub_info(PAUSED_STATUS)
    assert info["scrub_paused"] is True
    assert info["scrub_in_progress"] is False
    assert info["scrub_state"] == ss.SCAN_PAUSED


def test_pool_view_parser_reports_paused():
    from views.zfs_pools import parse_pool_status

    parsed = parse_pool_status(PAUSED_STATUS)
    assert parsed["scan_paused"] is True
    assert parsed["scan_in_progress"] is False
    assert parsed["scan_state"] == ss.SCAN_PAUSED
    assert parsed["scan_progress"] == pytest.approx(12.34)
    assert "paused" in parsed["scan_info"]


def test_performance_service_reports_paused(monkeypatch):
    from services import zfs_performance

    class _Result:
        stdout = PAUSED_STATUS

    monkeypatch.setattr(
        zfs_performance.subprocess, "run", lambda *a, **k: _Result()
    )
    estimate = zfs_performance.ZFSPerformanceService().estimate_scrub_time("tank")
    assert estimate["status"] == "paused"
    assert estimate["progress"] == pytest.approx(12.34)


# ---------------------------------------------------------------------------
# ``last_scrub`` recency: only a *completed scrub* may claim a recent scrub.
# A canceled scrub or a resilver is not a completed scrub.
# ---------------------------------------------------------------------------


def test_completed_scrub_exposes_last_scrub():
    scan = ss.parse_scan(COMPLETED_STATUS)
    assert scan.last_scrub is not None
    assert scan.last_scrub == scan.last_scan


def test_canceled_scrub_has_no_last_scrub():
    scan = ss.parse_scan(CANCELED_STATUS)
    assert scan.last_scan is not None  # cancellation timestamp is still useful
    assert scan.last_scrub is None


def test_completed_resilver_has_no_last_scrub():
    scan = ss.parse_scan(RESILVER_STATUS)
    assert scan.last_scan is not None
    assert scan.last_scrub is None


def test_dashboard_does_not_report_canceled_scrub_as_recent():
    from services.dashboard import _parse_scrub_info

    info = _parse_scrub_info(CANCELED_STATUS)
    assert info["last_scrub"] is None
    assert info["scrub_within_30_days"] is False


def test_dashboard_does_not_report_resilver_as_recent_scrub():
    from services.dashboard import _parse_scrub_info

    info = _parse_scrub_info(RESILVER_STATUS)
    assert info["last_scrub"] is None
    assert info["scrub_within_30_days"] is False


def test_health_analysis_does_not_report_canceled_scrub_as_recent():
    from services.health_analysis import HealthAnalysisService

    info = HealthAnalysisService()._parse_scrub_info(CANCELED_STATUS)
    assert info["last_scrub"] is None
    assert info["scrub_within_30_days"] is False


def test_health_analysis_does_not_report_resilver_as_recent_scrub():
    from services.health_analysis import HealthAnalysisService

    info = HealthAnalysisService()._parse_scrub_info(RESILVER_STATUS)
    assert info["last_scrub"] is None
    assert info["scrub_within_30_days"] is False


def test_to_dict_exposes_last_scrub():
    data = ss.parse_scan(COMPLETED_STATUS).to_dict()
    assert data["last_scrub"] is not None
    assert ss.parse_scan(CANCELED_STATUS).to_dict()["last_scrub"] is None


# ---------------------------------------------------------------------------
# Permanent errors: the ``errors:`` section can report errors while the scan
# line says ``with 0 errors``. Error Scrub must still be offered.
# ---------------------------------------------------------------------------


def test_permanent_errors_section_is_detected():
    assert ss.errors_section_has_errors(PERMANENT_ERRORS_STATUS) is True


def test_no_known_data_errors_is_not_an_error():
    assert ss.errors_section_has_errors(NO_ERRORS_STATUS) is False


def test_missing_errors_section_is_not_an_error():
    assert ss.errors_section_has_errors(COMPLETED_STATUS) is False


def test_pool_view_offers_error_scrub_for_permanent_errors():
    from views.zfs_pools import parse_pool_status

    parsed = parse_pool_status(PERMANENT_ERRORS_STATUS)
    assert parsed["scan_errors"] is True
    assert parsed["errors_line"].startswith("Permanent errors")


def test_pool_view_clean_pool_does_not_offer_error_scrub():
    from views.zfs_pools import parse_pool_status

    parsed = parse_pool_status(NO_ERRORS_STATUS)
    assert parsed["scan_errors"] is False


def test_dashboard_reports_permanent_errors():
    from services.dashboard import _parse_scrub_info

    info = _parse_scrub_info(PERMANENT_ERRORS_STATUS)
    assert info["scan_errors"] is True
    assert info["last_scrub"] is not None


def test_health_analysis_reports_permanent_errors():
    from services.health_analysis import HealthAnalysisService

    info = HealthAnalysisService()._parse_scrub_info(PERMANENT_ERRORS_STATUS)
    assert info["scan_errors"] is True
