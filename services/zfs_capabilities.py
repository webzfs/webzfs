"""Shared, cached ZFS version and capability detection.

Every WebZFS feature that depends on an OpenZFS version gate should ask this
module instead of hard-coding ``zfs version`` parsing. The version probe runs
at most once per :data:`CAPABILITIES_TTL_SECONDS`, so templates and request
handlers can read capability flags without spawning a subprocess per request.

Version floors below were derived by checking each feature's source (command,
property, pool feature or man page) at the OpenZFS release tags ``zfs-2.0.0``
through ``zfs-2.4.4`` in the upstream repository; the first tag containing the
feature is the floor recorded here.
"""

from __future__ import annotations

import re
import threading
import time
from dataclasses import dataclass, fields
from typing import Dict, FrozenSet, Optional, Tuple

from services.utils import get_os_type, run_zfs_command

VersionTuple = Tuple[int, int, int]

#: Same expression the delegation registry used before this module existed.
#: Anchored with ``re.fullmatch`` so trailing junk is rejected.
VERSION_PATTERN = r"zfs-(?:kmod-)?(\d+)\.(\d+)\.(\d+)(?:[-+_][A-Za-z0-9_.+~-]+)?"

#: Supported platforms. Anything else (e.g. NetBSD with an older ZFS) gets an
#: all-False capability set so callers hide version-gated controls.
SUPPORTED_PLATFORMS: FrozenSet[str] = frozenset({"Linux", "FreeBSD"})

#: How long a probed capability set stays cached.
CAPABILITIES_TTL_SECONDS = 300.0

_POOL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,254}$")
_FEATURE_NAME_RE = re.compile(r"^[a-z0-9_]{1,64}$")


@dataclass(frozen=True)
class _CapabilitySpec:
    """Minimum OpenZFS release for a flag, plus optional platform restriction."""

    min_version: VersionTuple
    platforms: Optional[FrozenSet[str]] = None


#: flag name -> version floor. A flag is True only when the effective version
#: (the lower of userland and kernel module) meets the floor.
_CAPABILITY_SPECS: Dict[str, _CapabilitySpec] = {
    # JSON output for zfs/zpool list (-j): cmd/zfs/zfs_main.c, zfs-2.3.0.
    "json_output": _CapabilitySpec((2, 3, 0)),
    # Pool feature org.openzfs:raidz_expansion, zfs-2.3.0.
    "raidz_expansion": _CapabilitySpec((2, 3, 0)),
    # Pool feature com.klarasystems:fast_dedup, zfs-2.3.0.
    "fast_dedup": _CapabilitySpec((2, 3, 0)),
    # zpool ddtprune, zfs-2.3.0.
    "ddtprune": _CapabilitySpec((2, 3, 0)),
    # zpool prefetch -t ddt subcommand, zfs-2.3.0 (zpool-prefetch(8) is absent
    # from the 2.2 man pages). This flag gates the subcommand, not the
    # long-standing zfs_dedup_prefetch module parameter.
    "prefetch_ddt": _CapabilitySpec((2, 3, 0)),
    # "direct" dataset property (ZFS_PROP_DIRECT), zfs-2.3.0.
    "direct_io": _CapabilitySpec((2, 3, 0)),
    # zpool reguid -g <guid>, zfs-2.3.0.
    "reguid_with_guid": _CapabilitySpec((2, 3, 0)),
    # zfs rewrite subcommand, zfs-2.3.4 (zfs_do_rewrite absent at 2.3.3).
    "zfs_rewrite": _CapabilitySpec((2, 3, 4)),
    # zfs rewrite -P (preserve logical birth time), zfs-2.4.0.
    "rewrite_preserve_birth": _CapabilitySpec((2, 4, 0)),
    # zpool scrub -p (pause), zfs-0.7.0. Every OpenZFS that F1 can probe has
    # it; the flag exists so the control is hidden on NetBSD like the others.
    "scrub_pause": _CapabilitySpec((0, 7, 0)),
    # zpool scrub -e (error scrub), zfs-2.2.0.
    "scrub_error": _CapabilitySpec((2, 2, 0)),
    # zpool scrub -S/-E date range, zfs-2.4.0.
    "scrub_time_range": _CapabilitySpec((2, 4, 0)),
    # zpool scrub -a (all pools), zfs-2.4.0.
    "all_pools_flag": _CapabilitySpec((2, 4, 0)),
    # defaultuserquota/defaultgroupquota/defaultprojectquota, zfs-2.4.0.
    "default_quotas": _CapabilitySpec((2, 4, 0)),
    # blake3 checksum value, zfs-2.2.0.
    "checksum_blake3": _CapabilitySpec((2, 2, 0)),
    # redundant_metadata=some|none, zfs-2.2.0.
    "redundant_metadata_some_none": _CapabilitySpec((2, 2, 0)),
    # zpool initialize -u (uninitialize), zfs-2.2.0.
    "initialize_uninit": _CapabilitySpec((2, 2, 0)),
    # zfs zone/unzone: attach a dataset to a Linux user namespace, zfs-2.2.0.
    # Linux only. The FreeBSD equivalent is the separate zfs jail/unjail pair,
    # which would need its own flag; do not widen this one to FreeBSD.
    "zfs_zone": _CapabilitySpec((2, 2, 0), frozenset({"Linux"})),
    # Pool feature org.openzfs:draid, zfs-2.1.0.
    "draid": _CapabilitySpec((2, 1, 0)),
    # zpool compatibility property, zfs-2.1.0.
    "compatibility_property": _CapabilitySpec((2, 1, 0)),
    # M11 read-only observability panels. Every floor below predates the
    # oldest release F1 can probe (2.0.0); the flags exist so the panels are
    # hidden on NetBSD and on any host where the version probe fails.
    # zpool status -s (per-vdev slow I/O column), zfs-0.8.0.
    "slow_io_status": _CapabilitySpec((0, 8, 0)),
    # zpool status -t / -i (per-vdev TRIM and initialize state), zfs-0.8.0.
    "vdev_activity_status": _CapabilitySpec((0, 8, 0)),
    # zpool iostat -w (latency histograms), zfs-0.7.0.
    "latency_histogram": _CapabilitySpec((0, 7, 0)),
    # zil and per-pool txgs kstats (Linux procfs, FreeBSD sysctl), zfs-0.7.0.
    "kstat_observability": _CapabilitySpec((0, 7, 0)),
}

CAPABILITY_FLAGS: Tuple[str, ...] = tuple(_CAPABILITY_SPECS)


@dataclass(frozen=True)
class ZfsCapabilities:
    """Capability snapshot for the running host.

    ``userland`` and ``kmod`` are parsed ``(major, minor, patch)`` tuples, or
    ``None`` when the corresponding line was absent or unparseable.
    """

    platform: str
    userland: Optional[VersionTuple]
    kmod: Optional[VersionTuple]
    json_output: bool
    raidz_expansion: bool
    fast_dedup: bool
    ddtprune: bool
    prefetch_ddt: bool
    direct_io: bool
    reguid_with_guid: bool
    zfs_rewrite: bool
    rewrite_preserve_birth: bool
    scrub_pause: bool
    scrub_error: bool
    scrub_time_range: bool
    all_pools_flag: bool
    default_quotas: bool
    checksum_blake3: bool
    redundant_metadata_some_none: bool
    initialize_uninit: bool
    zfs_zone: bool
    draid: bool
    compatibility_property: bool
    slow_io_status: bool
    vdev_activity_status: bool
    latency_histogram: bool
    kstat_observability: bool

    @property
    def effective_version(self) -> Optional[VersionTuple]:
        """The version every flag is evaluated against.

        When userland and kernel module disagree, the lower of the two wins so
        a flag is only reported as supported when both sides can honour it.
        """
        if self.userland is None:
            return None
        if self.kmod is None:
            return self.userland
        return min(self.userland, self.kmod)

    def as_dict(self) -> Dict[str, object]:
        """Flat mapping of every capability flag plus version context."""
        return {field.name: getattr(self, field.name) for field in fields(self)}


def parse_zfs_version(
    output: str,
) -> Tuple[Optional[VersionTuple], Optional[VersionTuple]]:
    """Parse ``zfs version`` output into userland and kernel module versions.

    ``zfs version`` prints one line for userland (``zfs-2.2.6-1``) and, when
    the module is loaded, one for the kernel module (``zfs-kmod-2.2.6-1``).
    Only the first matching line of each kind is parsed; blank and unrelated
    lines are ignored.

    Args:
        output: Raw stdout of ``zfs version``.

    Returns:
        ``(userland, kmod)`` where each element is a ``(major, minor, patch)``
        tuple or ``None`` if that line is missing or cannot be parsed.
    """
    if not output:
        return None, None

    userland: Optional[VersionTuple] = None
    kmod: Optional[VersionTuple] = None
    for raw_line in output.splitlines():
        line = raw_line.strip()
        if not line.startswith("zfs-"):
            continue
        match = re.fullmatch(VERSION_PATTERN, line)
        if not match:
            continue
        version = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
        if line.startswith("zfs-kmod-"):
            if kmod is None:
                kmod = version
        elif userland is None:
            userland = version
    return userland, kmod


def _resolve_flag(
    spec: _CapabilitySpec, effective: Optional[VersionTuple], platform_name: str
) -> bool:
    if effective is None:
        return False
    if platform_name not in SUPPORTED_PLATFORMS:
        return False
    if spec.platforms is not None and platform_name not in spec.platforms:
        return False
    return effective >= spec.min_version


def _build_capabilities(
    platform_name: str,
    userland: Optional[VersionTuple],
    kmod: Optional[VersionTuple],
) -> ZfsCapabilities:
    effective = None
    if userland is not None:
        effective = userland if kmod is None else min(userland, kmod)

    flags = {
        name: _resolve_flag(spec, effective, platform_name)
        for name, spec in _CAPABILITY_SPECS.items()
    }
    return ZfsCapabilities(
        platform=platform_name,
        userland=userland,
        kmod=kmod,
        **flags,
    )


def unavailable_capabilities(platform_name: Optional[str] = None) -> ZfsCapabilities:
    """All-False capability set for hosts where ``zfs version`` cannot run."""
    return _build_capabilities(platform_name or get_os_type(), None, None)


_CACHE_LOCK = threading.Lock()
_CACHE: Optional[Tuple[float, ZfsCapabilities]] = None


def reset_capabilities_cache() -> None:
    """Drop the cached capability set (used by tests and after upgrades)."""
    global _CACHE
    with _CACHE_LOCK:
        _CACHE = None


def _probe_capabilities() -> ZfsCapabilities:
    """Run ``zfs version`` once and turn the result into capabilities."""
    platform_name = get_os_type()
    if platform_name not in SUPPORTED_PLATFORMS:
        return unavailable_capabilities(platform_name)
    try:
        result = run_zfs_command(["zfs", "version"], check=False, timeout=5)
        if result.returncode != 0:
            return unavailable_capabilities(platform_name)
        userland, kmod = parse_zfs_version(result.stdout or "")
    except Exception:
        return unavailable_capabilities(platform_name)
    return _build_capabilities(platform_name, userland, kmod)


def get_capabilities(*, force_refresh: bool = False) -> ZfsCapabilities:
    """Return the cached capability set for this host.

    The probe runs at most once per :data:`CAPABILITIES_TTL_SECONDS`; pass
    ``force_refresh=True`` after an upgrade to bypass the cache. If
    ``zfs version`` fails (for example on NetBSD), every flag is ``False``.

    Args:
        force_refresh: Ignore the cached value and probe again.

    Returns:
        A :class:`ZfsCapabilities` snapshot.
    """
    global _CACHE
    now = time.monotonic()
    with _CACHE_LOCK:
        if not force_refresh and _CACHE is not None:
            cached_at, cached = _CACHE
            if now - cached_at < CAPABILITIES_TTL_SECONDS:
                return cached
        capabilities = _probe_capabilities()
        _CACHE = (now, capabilities)
        return capabilities


class _LazyCapabilities:
    """Jinja global proxy so ``{% if caps.raidz_expansion %}`` stays lazy."""

    __slots__ = ()

    def __getattr__(self, name: str):
        if name not in _CAPABILITY_SPECS and name not in {
            "platform",
            "userland",
            "kmod",
            "effective_version",
            "as_dict",
        }:
            raise AttributeError(name)
        return getattr(get_capabilities(), name)

    def __repr__(self) -> str:
        return f"<lazy ZfsCapabilities platform={get_os_type()!r}>"


#: Exposed to Jinja as ``caps`` by ``config.templates``.
caps = _LazyCapabilities()


def _validate_pool_name(pool: str) -> str:
    if not isinstance(pool, str) or not _POOL_NAME_RE.fullmatch(pool):
        raise ValueError(f"Invalid pool name: {pool!r}")
    return pool


def _validate_feature_name(name: str) -> str:
    if not isinstance(name, str) or not _FEATURE_NAME_RE.fullmatch(name):
        raise ValueError(f"Invalid pool feature name: {name!r}")
    return name


def get_pool_features(pool: str) -> Dict[str, str]:
    """Return the ``feature@`` states for one pool.

    Runs ``zpool get -H -o property,value all <pool>`` with a list argv and
    keeps only ``feature@*`` rows. States are ``disabled``, ``enabled`` or
    ``active`` as reported by the pool.

    Args:
        pool: Existing pool name.

    Returns:
        Mapping of bare feature name (``raidz_expansion``) to its state string.
        Empty when the pool cannot be queried.

    Raises:
        ValueError: If ``pool`` is not a syntactically valid pool name.
    """
    _validate_pool_name(pool)
    try:
        result = run_zfs_command(
            ["zpool", "get", "-H", "-o", "property,value", "all", pool],
            check=False,
            timeout=10,
        )
        if result.returncode != 0:
            return {}
        output = result.stdout or ""
    except Exception:
        return {}

    features: Dict[str, str] = {}
    for raw_line in output.splitlines():
        property_name, separator, value = raw_line.partition("\t")
        if not separator or not property_name.startswith("feature@"):
            continue
        feature = property_name[len("feature@") :].strip()
        if feature:
            features[feature] = value.strip()
    return features


def pool_feature_enabled(pool: str, name: str) -> bool:
    """Whether a pool feature is enabled or active on ``pool``.

    A version floor alone is not enough for pool features such as
    ``raidz_expansion`` or ``fast_dedup``: the feature must also be enabled on
    the pool. ``disabled`` (and any unknown/missing state) counts as False.

    Args:
        pool: Existing pool name.
        name: Bare pool feature name, e.g. ``raidz_expansion``.

    Returns:
        True only when the reported state is ``enabled`` or ``active``.

    Raises:
        ValueError: If ``pool`` or ``name`` is not syntactically valid.
    """
    _validate_feature_name(name)
    return get_pool_features(pool).get(name, "").lower() in {"enabled", "active"}
