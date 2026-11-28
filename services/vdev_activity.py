"""Parsers and summaries for per-vdev TRIM and initialize progress.

``zpool trim`` and ``zpool initialize`` report their state through the same
``zpool status`` config block that carries scrub information. ``zpool status
-t`` annotates each leaf vdev with its TRIM state and ``zpool status -i``
does the same for initialize:

    NAME        STATE     READ WRITE CKSUM
    tank        ONLINE       0     0     0
      sda       ONLINE       0     0     0  (42% trimmed, started at Mon Oct 27 12:00:00 2026)
      sdb       ONLINE       0     0     0  (untrimmed)
      sdc       ONLINE       0     0     0  (trim unsupported)

The exact wording is produced by ``print_status_trim()`` and
``print_status_initialize()`` in ``cmd/zpool/zpool_main.c`` (OpenZFS 2.4.4):
``(N% trimmed|initialized, started at …)``, ``(N% … suspended, started at …)``,
``(100% … completed at …)``, ``(untrimmed)``/``(uninitialized)``,
``(trim unsupported)`` and, for the non-verbose status output, the bare
``(trimming)``/``(initializing)``. A suspended operation prints
``(N% trimmed, suspended, started at …)``, so ``suspended_at`` is only set
if a future release prints a separate suspension time.

This module keeps that parsing out of the service so it can be unit-tested
against captured command output.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

#: Operation kinds handled by this module.
TRIM = "trim"
INITIALIZE = "initialize"

#: Aggregate and per-device states.
STATE_NONE = "none"
STATE_IN_PROGRESS = "in_progress"
STATE_SUSPENDED = "suspended"
STATE_COMPLETED = "completed"
STATE_UNSUPPORTED = "unsupported"
STATE_UNKNOWN = "unknown"

_KIND_WORD = {TRIM: "trimmed", INITIALIZE: "initialized"}
_PLAIN_NONE = {TRIM: "untrimmed", INITIALIZE: "uninitialized"}
_PLAIN_ACTIVE = {TRIM: "trimming", INITIALIZE: "initializing"}

_ANNOTATION_RE = re.compile(
    r"\(\s*(?:"
    r"(?P<percent>\d{1,3})%\s+(?P<word>trimmed|initialized)(?P<detail>[^)]*)"
    r"|(?P<plain>untrimmed|uninitialized|trim unsupported|trimming|initializing)"
    r")\s*\)"
)

_STARTED_AT_RE = re.compile(r"started at\s+(?P<when>[^,)]+)")
_SUSPENDED_AT_RE = re.compile(r"suspended at\s+(?P<when>[^,)]+)")
_COMPLETED_AT_RE = re.compile(r"completed at\s+(?P<when>[^,)]+)")
_SECTION_WORDS = frozenset({"logs", "cache", "spares", "special", "dedup"})


@dataclass(frozen=True)
class VdevActivity:
    """TRIM or initialize state of a single leaf vdev."""

    name: str
    state: str
    percent: Optional[int] = None
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    suspended_at: Optional[str] = None
    #: Raw annotation text as printed by zpool, useful for diagnostics.
    annotation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state,
            "percent": self.percent,
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "suspended_at": self.suspended_at,
            "annotation": self.annotation,
        }


@dataclass
class ActivitySummary:
    """Pool-level rollup of the per-device activity rows."""

    kind: str
    state: str = STATE_UNKNOWN
    percent: Optional[int] = None
    devices: List[VdevActivity] = field(default_factory=list)

    @property
    def counts(self) -> Dict[str, int]:
        counts = {
            STATE_IN_PROGRESS: 0,
            STATE_SUSPENDED: 0,
            STATE_COMPLETED: 0,
            STATE_UNSUPPORTED: 0,
            STATE_NONE: 0,
            STATE_UNKNOWN: 0,
        }
        for device in self.devices:
            counts[device.state] = counts.get(device.state, 0) + 1
        return counts

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "state": self.state,
            "percent": self.percent,
            "counts": self.counts,
            "devices": [device.to_dict() for device in self.devices],
        }


def _parse_annotation(annotation: str, kind: str) -> Optional[VdevActivity]:
    """Convert a raw ``(…)`` annotation into a :class:`VdevActivity` state.

    Returns ``None`` when the annotation belongs to a different operation
    (e.g. an initialize annotation while parsing TRIM output).
    """
    match = _ANNOTATION_RE.search(annotation)
    if not match:
        return None

    word = match.group("word")
    plain = match.group("plain")
    percent_text = match.group("percent")
    detail = (match.group("detail") or "").strip()

    if word:
        if word != _KIND_WORD[kind]:
            return None
        percent = int(percent_text)
        completed = _COMPLETED_AT_RE.search(detail)
        if completed:
            return VdevActivity(
                name="",
                state=STATE_COMPLETED,
                percent=100,
                completed_at=completed.group("when").strip(),
                annotation=match.group(0),
            )
        started = _STARTED_AT_RE.search(detail)
        suspended = _SUSPENDED_AT_RE.search(detail)
        if suspended or "suspended" in detail.lower():
            state = STATE_SUSPENDED
        else:
            state = STATE_IN_PROGRESS
        return VdevActivity(
            name="",
            state=state,
            percent=percent,
            started_at=started.group("when").strip() if started else None,
            suspended_at=suspended.group("when").strip() if suspended else None,
            annotation=match.group(0),
        )

    if plain == _PLAIN_NONE[kind]:
        return VdevActivity(name="", state=STATE_NONE, annotation=match.group(0))
    if plain == _PLAIN_ACTIVE[kind]:
        # Non-verbose status output: only the fact that it is running.
        return VdevActivity(name="", state=STATE_IN_PROGRESS, annotation=match.group(0))
    if plain == "trim unsupported" and kind == TRIM:
        return VdevActivity(name="", state=STATE_UNSUPPORTED, annotation=match.group(0))
    return None


def parse_vdev_activity(output: str, kind: str) -> List[VdevActivity]:
    """Parse ``zpool status -t`` (kind=TRIM) or ``-i`` (kind=INITIALIZE).

    Only leaf vdevs receive an annotation, so every config line carrying one
    becomes a row. Vdev group rows (``mirror-0``), section keywords (``logs``)
    and the pool root line never carry an annotation and are skipped.
    """
    if kind not in (TRIM, INITIALIZE):
        raise ValueError(f"unsupported activity kind: {kind!r}")

    devices: List[VdevActivity] = []
    in_config = False
    for line in (output or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("config:"):
            in_config = True
            continue
        if stripped.startswith("errors:"):
            in_config = False
            continue
        if not in_config:
            continue

        parts = stripped.split()
        if not parts:
            continue
        name = parts[0]
        if name == "NAME" or name.lower() in _SECTION_WORDS:
            continue

        # print_status_config() prints the initialize annotation and then the
        # TRIM annotation on the same leaf line, and the non-verbose one is
        # still printed (``(initializing)  (42% trimmed, ...)`` under -t), so
        # every annotation on the line is tried, not only the first.
        parsed = None
        for annotation_match in _ANNOTATION_RE.finditer(stripped):
            parsed = _parse_annotation(annotation_match.group(0), kind)
            if parsed is not None:
                break
        if parsed is None:
            continue
        devices.append(
            VdevActivity(
                name=name,
                state=parsed.state,
                percent=parsed.percent,
                started_at=parsed.started_at,
                completed_at=parsed.completed_at,
                suspended_at=parsed.suspended_at,
                annotation=parsed.annotation,
            )
        )
    return devices


def summarize_vdev_activity(
    devices: List[VdevActivity], kind: str
) -> ActivitySummary:
    """Roll per-device rows up into a pool-level state and percentage.

    * ``in_progress`` wins over everything: any active device means the
      operation is running.
    * ``suspended`` is reported when nothing is active but a device is paused.
    * ``completed`` requires every device to be complete.
    * ``unsupported`` when every device reports TRIM as unsupported.
    * ``none`` when every device is untrimmed/uninitialized.
    * ``mixed`` for any other combination (e.g. one completed, one never run).
    """
    summary = ActivitySummary(kind=kind, devices=list(devices))
    if not devices:
        summary.state = STATE_UNKNOWN
        return summary

    states = {device.state for device in devices}
    if STATE_IN_PROGRESS in states:
        summary.state = STATE_IN_PROGRESS
    elif STATE_SUSPENDED in states:
        summary.state = STATE_SUSPENDED
    elif states == {STATE_UNSUPPORTED}:
        summary.state = STATE_UNSUPPORTED
    elif states == {STATE_COMPLETED}:
        summary.state = STATE_COMPLETED
    elif states == {STATE_NONE}:
        summary.state = STATE_NONE
    else:
        summary.state = "mixed"

    percents = [d.percent for d in devices if d.percent is not None]
    if summary.state == STATE_COMPLETED:
        summary.percent = 100
    elif percents:
        summary.percent = int(round(sum(percents) / len(percents)))
    return summary


def parse_trim_status(output: str) -> ActivitySummary:
    """Parse the output of ``zpool status -t``."""
    return summarize_vdev_activity(parse_vdev_activity(output, TRIM), TRIM)


def parse_initialize_status(output: str) -> ActivitySummary:
    """Parse the output of ``zpool status -i``."""
    return summarize_vdev_activity(
        parse_vdev_activity(output, INITIALIZE), INITIALIZE
    )
