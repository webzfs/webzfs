"""Single shared parser for the ``scan:`` block in ``zpool status`` output.

WebZFS used to carry four independent ``scan:`` parsers (pool topology,
dashboard scrub cards, performance view and health analysis). They disagreed
about paused scans: every one reported ``scan: scrub paused since ...`` as
"no scrub recorded". This module is the single source of truth and the four
callers delegate to it.

Formats handled (OpenZFS 2.0 - 2.4)::

    scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
    scan: scrub in progress since Sun Mar  2 10:00:00 2025
        1.2T / 3.0T scanned at 1.0G/s, 500G / 3.0T issued at 500M/s
        0B repaired, 12.34% done, 01:23:45 to go
    scan: scrub paused since Sun Mar  2 10:00:00 2025
        0B repaired, 12.34% done, 01:23:45 to go
    scan: scrub canceled on Sun Mar  2 10:00:00 2025
    scan: scrub (error scrub) in progress since Sun Mar  2 10:00:00 2025
    scan: resilvered 12.3G in 00:01:02 with 0 errors on ...
    scan: none requested
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

#: Scan states exposed to callers. ``unknown`` means a ``scan:`` line was
#: present but its wording is not one this parser recognises; callers should
#: render the raw line rather than claim a state.
SCAN_NONE = "none"
SCAN_IN_PROGRESS = "in_progress"
SCAN_PAUSED = "paused"
SCAN_COMPLETED = "completed"
SCAN_CANCELED = "canceled"
SCAN_UNKNOWN = "unknown"

_PROGRESS_RE = re.compile(r"(\d+(?:\.\d+)?)%\s+done")
_ERROR_COUNT_RE = re.compile(r"with\s+(\d+)\s+errors?")
_COMPLETION_DATE_RE = re.compile(r"on\s+(\w+\s+\w+\s+\d+\s+[\d:]+\s+\d{4})")
_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})")


@dataclass(frozen=True)
class ScanStatus:
    """Parsed view of the ``scan:`` block for one pool."""

    present: bool = False
    kind: str = "none"  # "scrub", "resilver" or "none"
    state: str = SCAN_NONE
    raw: str = ""
    progress: Optional[float] = None
    error_scrub: bool = False
    error_count: Optional[int] = None
    last_scan: Optional[datetime] = None
    #: Completion timestamp of the last *successfully completed scrub* only.
    #: Canceled scrubs and resilvers populate ``last_scan`` but must never be
    #: reported as "last scrub" recency.
    last_scrub: Optional[datetime] = None

    @property
    def in_progress(self) -> bool:
        return self.state == SCAN_IN_PROGRESS

    @property
    def paused(self) -> bool:
        return self.state == SCAN_PAUSED

    @property
    def completed(self) -> bool:
        return self.state == SCAN_COMPLETED

    @property
    def canceled(self) -> bool:
        return self.state == SCAN_CANCELED

    @property
    def has_errors(self) -> bool:
        """True when the scan line reports a non-zero error count."""
        return bool(self.error_count)

    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        data["last_scan"] = self.last_scan.isoformat() if self.last_scan else None
        data["last_scrub"] = self.last_scrub.isoformat() if self.last_scrub else None
        data["in_progress"] = self.in_progress
        data["paused"] = self.paused
        data["completed"] = self.completed
        data["canceled"] = self.canceled
        data["has_errors"] = self.has_errors
        return data


def extract_scan_block(status_output: str) -> List[str]:
    """Return the ``scan:`` line plus its tab-indented continuation lines.

    ``zpool status`` writes the progress counters on tab-indented lines right
    after the ``scan:`` line; the block ends at the first non-indented line.
    """
    lines = status_output.splitlines()
    for index, line in enumerate(lines):
        if line.strip().lower().startswith("scan:"):
            block = [line.strip()]
            for follow in lines[index + 1:]:
                if not follow.startswith("\t"):
                    break
                stripped = follow.strip()
                if stripped:
                    block.append(stripped)
            return block
    return []


def _parse_scan_date(text: str) -> Optional[datetime]:
    """Parse the completion/cancellation timestamp of a scan line."""
    match = _COMPLETION_DATE_RE.search(text)
    if match:
        try:
            return datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Y")
        except ValueError:
            pass
    match = _ISO_DATE_RE.search(text)
    if match:
        try:
            return datetime.strptime(match.group(1).replace("T", " "), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def parse_scan_line(scan_line: str) -> ScanStatus:
    """Parse a single ``scan:`` line (with or without the ``scan:`` prefix)."""
    text = scan_line.strip()
    if text.lower().startswith("scan:"):
        text = text.split(":", 1)[1].strip()
    return _build_status([text] if text else [])


def parse_scan(status_output: str) -> ScanStatus:
    """Parse the ``scan:`` block out of a full ``zpool status`` output."""
    return _build_status(extract_scan_block(status_output))


def _build_status(block: List[str]) -> ScanStatus:
    if not block:
        return ScanStatus()

    rest = block[0]
    if rest.lower().startswith("scan:"):
        rest = rest.split(":", 1)[1].strip()
    lower = rest.lower()
    raw = " ".join(part for part in block if part)

    if "none requested" in lower or "no scans" in lower:
        return ScanStatus(present=False, state=SCAN_NONE, raw=raw)

    if "resilver" in lower:
        kind = "resilver"
    elif "scrub" in lower:
        kind = "scrub"
    else:
        kind = "none"

    if "paused" in lower:
        state = SCAN_PAUSED
    elif "in progress" in lower:
        state = SCAN_IN_PROGRESS
    elif "canceled" in lower or "cancelled" in lower:
        state = SCAN_CANCELED
    elif "repaired" in lower or "resilvered" in lower:
        state = SCAN_COMPLETED
    else:
        state = SCAN_UNKNOWN

    progress: Optional[float] = None
    for part in block:
        match = _PROGRESS_RE.search(part)
        if match:
            progress = float(match.group(1))
            break

    error_count: Optional[int] = None
    match = _ERROR_COUNT_RE.search(rest)
    if match:
        error_count = int(match.group(1))

    last_scan = _parse_scan_date(rest) if state in (SCAN_COMPLETED, SCAN_CANCELED) else None
    # Recency of a scrub may only be claimed when a scrub actually completed.
    # A canceled scrub (or a resilver, which is a different operation) must
    # leave ``last_scrub`` empty so dashboards don't report "scrubbed recently".
    last_scrub = last_scan if state == SCAN_COMPLETED and kind == "scrub" else None

    return ScanStatus(
        present=True,
        kind=kind,
        state=state,
        raw=raw,
        progress=progress,
        error_scrub="error scrub" in raw.lower(),
        error_count=error_count,
        last_scan=last_scan,
        last_scrub=last_scrub,
    )


def errors_section_has_errors(status_output: str) -> bool:
    """Return True when the pool's ``errors:`` section reports data errors.

    ``zpool status`` prints ``errors: No known data errors`` for a clean pool
    and ``errors: Permanent errors have been detected in the following files:``
    (or a similar message) otherwise. The latter can coexist with a scan line
    that says ``with 0 errors``, so callers must not derive error state from the
    scan block alone.
    """
    for line in status_output.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("errors:"):
            detail = stripped.split(":", 1)[1].strip()
            if not detail:
                continue
            return detail.lower() != "no known data errors"
    return False
