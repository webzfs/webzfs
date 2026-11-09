"""Single shared parser for the scan status block in ``zpool status`` output.

WebZFS used to carry four independent ``scan:`` parsers (pool topology,
dashboard scrub cards, performance view and health analysis). They disagreed
about paused scans: every one reported ``scan: scrub paused since ...`` as
"no scrub recorded". This module is the single source of truth and the four
callers delegate to it.

Formats handled (OpenZFS 2.0 - 2.4, ``cmd/zpool/zpool_main.c``)::

    scan: scrub repaired 0B in 00:12:34 with 0 errors on Sun Mar  2 10:00:00 2025
    scan: scrub in progress since Sun Mar  2 10:00:00 2025
        1.2T / 3.0T scanned at 1.0G/s, 500G / 3.0T issued at 500M/s
        0B repaired, 12.34% done, 01:23:45 to go
    scan: scrub paused since Sun Mar  2 10:00:00 2025
        scrub started on Sun Mar  2 09:00:00 2025
        1.2T / 3.0T scanned, 500G / 3.0T issued
        0B repaired, 12.34% done
    scan: scrub canceled on Sun Mar  2 10:00:00 2025
    scan: resilvered 12.3G in 00:01:02 with 0 errors on ...
    scan: resilver in progress since ...
    scan: none requested

An error scrub (``zpool scrub -e``, OpenZFS 2.2+) is printed by
``print_err_scrub_status`` on its own ``scrub:`` line, not on a ``scan:``
line::

    scrub: error scrub in progress since Sun Mar  2 10:00:00 2025
        12.34% done, issued I/O for 3 error blocks
    scrub: error scrub paused since Sun Mar  2 10:00:00 2025
        error scrub started on Sun Mar  2 09:00:00 2025
        12.34% done, issued I/O for 3 error blocks
    scrub: scrubbed 3 error blocks in 0 days 00:00:01 on Sun Mar  2 10:00:00 2025
    scrub: error scrub canceled on Sun Mar  2 10:00:00 2025

``zpool status`` can print more than one block (for example an error scrub
line followed by a resilver line). The first block that is running or paused
wins; otherwise the first block is used.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

#: Scan states exposed to callers. ``unknown`` means a scan line was present
#: but its wording is not one this parser recognises; callers should render
#: the raw line rather than claim a state.
SCAN_NONE = "none"
SCAN_IN_PROGRESS = "in_progress"
SCAN_PAUSED = "paused"
SCAN_COMPLETED = "completed"
SCAN_CANCELED = "canceled"
SCAN_UNKNOWN = "unknown"

#: Line labels that open a scan block. ``scrub:`` is used only for error scrubs.
_SCAN_LABEL = "scan:"
_ERROR_SCRUB_LABEL = "scrub:"

_PROGRESS_RE = re.compile(r"(\d+(?:\.\d+)?)%\s+done")
_ERROR_COUNT_RE = re.compile(r"with\s+(\d+)\s+errors?")
_COMPLETION_DATE_RE = re.compile(r"on\s+(\w+\s+\w+\s+\d+\s+[\d:]+\s+\d{4})")
_ISO_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})")


@dataclass(frozen=True)
class ScanStatus:
    """Parsed view of the scan block for one pool."""

    present: bool = False
    kind: str = "none"  # "scrub", "resilver" or "none"
    state: str = SCAN_NONE
    #: Scan text without the ``scan:``/``scrub:`` label, continuation lines
    #: joined with single spaces.
    raw: str = ""
    progress: Optional[float] = None
    error_scrub: bool = False
    error_count: Optional[int] = None
    last_scan: Optional[datetime] = None
    #: Completion timestamp of the last *successfully completed full scrub*
    #: only. Canceled scrubs, error scrubs and resilvers populate
    #: ``last_scan`` but must never be reported as "last scrub" recency.
    last_scrub: Optional[datetime] = None

    @property
    def in_progress(self) -> bool:
        """True while any scan (scrub, error scrub or resilver) is running."""
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
    def scrub_in_progress(self) -> bool:
        """True only while a scrub (regular or error) is running.

        A running resilver is not a scrub: it cannot be paused, and the
        dashboards must not report it as "scrub running".
        """
        return self.kind == "scrub" and self.state == SCAN_IN_PROGRESS

    @property
    def scrub_paused(self) -> bool:
        """True when a scrub (regular or error) is paused. Resilvers never pause."""
        return self.kind == "scrub" and self.state == SCAN_PAUSED

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
        data["scrub_in_progress"] = self.scrub_in_progress
        data["scrub_paused"] = self.scrub_paused
        data["has_errors"] = self.has_errors
        return data


def _split_label(line: str) -> Tuple[str, str]:
    """Return ``(label, text)`` when ``line`` opens a scan block, else ``("", "")``."""
    stripped = line.strip()
    lower = stripped.lower()
    for label in (_SCAN_LABEL, _ERROR_SCRUB_LABEL):
        if lower.startswith(label):
            return label, stripped[len(label):].strip()
    return "", ""


def extract_scan_blocks(status_output: str) -> List[Tuple[str, List[str]]]:
    """Return every scan block as ``(label, lines)``.

    The first entry of ``lines`` is the text after the label; the rest are the
    tab-indented continuation lines that ``zpool status`` prints right after
    it. A block ends at the first line that is not tab-indented.
    """
    blocks: List[Tuple[str, List[str]]] = []
    lines = status_output.splitlines()
    for index, line in enumerate(lines):
        label, text = _split_label(line)
        if not label:
            continue
        block = [text]
        for follow in lines[index + 1:]:
            if not follow.startswith("\t"):
                break
            stripped = follow.strip()
            if stripped:
                block.append(stripped)
        blocks.append((label, block))
    return blocks


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
    """Parse a single scan line (with or without the ``scan:`` label)."""
    label, text = _split_label(scan_line)
    if not label:
        label = _SCAN_LABEL
        text = scan_line.strip()
    if not text:
        return ScanStatus()
    return _build_status(label, [text])


def parse_scan(status_output: str) -> ScanStatus:
    """Parse the scan status out of a full ``zpool status`` output.

    When several blocks are printed, the first running or paused one is
    returned so an active operation is never hidden behind a finished one.
    """
    statuses = []
    for label, block in extract_scan_blocks(status_output):
        statuses.append(_build_status(label, block))

    if not statuses:
        return ScanStatus()

    for status in statuses:
        if status.in_progress or status.paused:
            return status
    return statuses[0]


def _build_status(label: str, block: List[str]) -> ScanStatus:
    rest = block[0]
    lower = rest.lower()
    raw = " ".join(part for part in block if part)

    if "none requested" in lower or "no scans" in lower:
        return ScanStatus(present=False, state=SCAN_NONE, raw=raw)

    error_scrub = label == _ERROR_SCRUB_LABEL

    if error_scrub:
        kind = "scrub"
    elif "resilver" in lower:
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
    elif "repaired" in lower or "resilvered" in lower or "scrubbed" in lower:
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

    last_scan = None
    if state in (SCAN_COMPLETED, SCAN_CANCELED):
        last_scan = _parse_scan_date(rest)

    # Recency of a scrub may only be claimed when a full scrub completed. A
    # canceled scrub, an error scrub (it reads only the damaged blocks) and a
    # resilver must leave ``last_scrub`` empty.
    last_scrub = None
    if state == SCAN_COMPLETED and kind == "scrub" and not error_scrub:
        last_scrub = last_scan

    return ScanStatus(
        present=True,
        kind=kind,
        state=state,
        raw=raw,
        progress=progress,
        error_scrub=error_scrub,
        error_count=error_count,
        last_scan=last_scan,
        last_scrub=last_scrub,
    )


def errors_section_has_errors(status_output: str) -> bool:
    """Return True when the pool's ``errors:`` section reports data errors.

    ``zpool status`` prints ``errors: No known data errors`` for a clean pool,
    ``errors: N data errors, use '-v' for a list`` without ``-v``, and
    ``errors: Permanent errors have been detected in the following files:``
    with ``-v``. Any of the latter can coexist with a scan line that says
    ``with 0 errors``, so callers must not derive error state from the scan
    block alone. ``-v`` is not needed to detect errors.
    """
    for line in status_output.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("errors:"):
            detail = stripped.split(":", 1)[1].strip()
            if not detail:
                continue
            return detail.lower() != "no known data errors"
    return False
