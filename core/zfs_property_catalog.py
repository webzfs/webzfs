"""Backend-owned catalog of OpenZFS dataset properties (backlog M6).

The dataset property editor used to hard-code both the editable property list
and the allowed option values in ``templates/zfs/datasets/properties.jinja``.
This module makes that list authoritative: every editable property carries its
dataset scope, inheritance behaviour, value type, help text and the option
values that the *installed* OpenZFS version and host platform actually support.

This module is pure data and validation. It runs no commands. The service
layer (``services/zfs_dataset.py``) builds the :class:`CatalogContext` from the
F1 capability helper (``services/zfs_capabilities.py``) and the host's
``zfs_max_recordsize``, then passes it to the helpers below.

Version gates were verified against the OpenZFS man pages and
``module/zcommon/zfs_prop.c`` at release tags (see the ``V_*`` constants and
the ``min_version`` fields below).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional

SCOPE_FILESYSTEM = "filesystem"
SCOPE_VOLUME = "volume"
SCOPE_BOTH = "both"

PLATFORM_LINUX = "linux"
PLATFORM_FREEBSD = "freebsd"
PLATFORM_NETBSD = "netbsd"
PLATFORM_OTHER = "other"

# OpenZFS release gates used by the catalog. Each was confirmed by checking the
# property registration (zfs_prop.c) or man page at the named tag.
# V_0_6_1 is the baseline: values at or below it exist on every ZFS WebZFS
# supports, including NetBSD's older ZFS, so they stay available even when the
# version is unknown.
V_0_6_1 = (0, 6, 1)  # lz4, base property set (xattr=dir since 0.6.1)
V_0_6_5 = (0, 6, 5)  # filesystem_limit/snapshot_limit registered at 0.6.5
V_0_7_0 = (0, 7, 0)  # sha512/skein checksums, dnodesize, large_blocks
V_0_8_0 = (0, 8, 0)  # special_small_blocks, keylocation, edonr on Linux
V_2_0_0 = (2, 0, 0)  # zstd compression, acltype posix alias
V_2_1_3 = (2, 1, 3)  # keylocation http(s) key retrieval (libzfs_crypto)
V_2_2_0 = (2, 2, 0)  # blake3 checksum, edonr on FreeBSD, redundant_metadata some/none (F1)
V_2_3_0 = (2, 3, 0)  # direct I/O, snapdir=disabled
V_2_4_0 = (2, 4, 0)  # special_small_blocks: any 0..16M value, zvol support

MIN_RECORDSIZE = 512
DEFAULT_MAX_RECORDSIZE = 16 * 1024 * 1024
MAX_SPECIAL_SMALL_BLOCKS = 16 * 1024 * 1024
LEGACY_SPECIAL_SMALL_BLOCKS_LIMIT = 1024 * 1024

SIZE_UNITS = {
    "": 1,
    "b": 1,
    "k": 1024,
    "kb": 1024,
    "m": 1024 ** 2,
    "mb": 1024 ** 2,
    "g": 1024 ** 3,
    "gb": 1024 ** 3,
    "t": 1024 ** 4,
    "tb": 1024 ** 4,
    "p": 1024 ** 5,
    "pb": 1024 ** 5,
    "e": 1024 ** 6,
    "eb": 1024 ** 6,
}

# Every pattern is applied with fullmatch(): a trailing '$' also matches before
# a final newline, so match() would accept "value\n".
_SIZE_RE = re.compile(r"(\d+(?:\.\d+)?)([a-zA-Z]{0,2})")
_USER_PROPERTY_RE = re.compile(r"[a-z0-9][a-z0-9:._-]*")
_KEYLOCATION_RE = re.compile(r"(none|prompt|file:///[^\s]+|https?://[^\s]+)")

# zfs_prop_user() requires strlen(name) < ZAP_MAXNAMELEN (256).
MAX_USER_PROPERTY_NAME = 255

# Native properties that exist but can never be written by this editor. Listing
# them separately lets set_property return a precise message instead of a
# generic "unknown property" error.
READ_ONLY_PROPERTIES = {
    "available": "Space available is reported by ZFS.",
    "clones": "Clone membership is maintained by ZFS.",
    "compressratio": "Compression ratio is a ZFS statistic.",
    "creation": "Creation time is set when the dataset is created.",
    "defer_destroy": "Deferred-destroy state is managed by ZFS.",
    "encryption": "The encryption suite cannot be changed after creation.",
    "encryptionroot": "The encryption root is derived by ZFS.",
    "filesystem_count": "Filesystem count is a ZFS statistic.",
    "guid": "Dataset GUIDs are assigned by ZFS.",
    "keyformat": "Key format can only be changed with zfs change-key.",
    "keystatus": "Key status is reported by ZFS.",
    "logicalreferenced": "Logical size is a ZFS statistic.",
    "logicalused": "Logical size is a ZFS statistic.",
    "mounted": "Mount state is reported by ZFS.",
    "origin": "Clone origin is assigned by ZFS.",
    "receive_resume_token": "Resume tokens are managed by zfs receive.",
    "redact_snaps": "Redaction snapshots are managed by ZFS.",
    "referenced": "Referenced size is a ZFS statistic.",
    "snapshot_count": "Snapshot count is a ZFS statistic.",
    "type": "Dataset type is assigned by ZFS.",
    "used": "Used space is a ZFS statistic.",
    "usedbychildren": "Space accounting is reported by ZFS.",
    "usedbydataset": "Space accounting is reported by ZFS.",
    "usedbyrefreservation": "Space accounting is reported by ZFS.",
    "usedbysnapshots": "Space accounting is reported by ZFS.",
    "version": "The on-disk version is managed by ZFS.",
    "written": "Written size is a ZFS statistic.",
}


class PropertyValidationError(ValueError):
    """Raised when a property or value does not fit the catalog definition."""


@dataclass(frozen=True)
class ValueOption:
    """One allowed value for an enum-like property."""

    value: str
    min_version: tuple[int, int, int] = (0, 0, 0)
    platforms: tuple[str, ...] = ()
    label: str = ""
    note: str = ""


@dataclass(frozen=True)
class DatasetPropertyDefinition:
    """Catalog entry for a native ZFS dataset property."""

    name: str
    label: str
    scope: str = SCOPE_BOTH
    editable: bool = True
    inheritable: bool = True
    value_type: str = "string"
    options: tuple[ValueOption, ...] = field(default_factory=tuple)
    min_version: tuple[int, int, int] = (0, 0, 0)
    platforms: tuple[str, ...] = ()
    help: str = ""
    warning: str = ""
    affects_existing_data: bool = False
    creation_only: bool = False
    deferred_to: str = ""


@dataclass(frozen=True)
class CatalogContext:
    """The host facts that decide which properties and options are visible."""

    zfs_version: Optional[tuple[int, int, int]]
    platform: str
    max_recordsize: int = DEFAULT_MAX_RECORDSIZE

    @property
    def version_known(self) -> bool:
        return self.zfs_version is not None


def normalize_platform(name: Optional[str]) -> str:
    """Map a ``platform.system()`` value onto the catalog's platform ids."""
    lowered = (name or "").lower()
    if "linux" in lowered:
        return PLATFORM_LINUX
    if "freebsd" in lowered:
        return PLATFORM_FREEBSD
    if "netbsd" in lowered:
        return PLATFORM_NETBSD
    return PLATFORM_OTHER


def build_context(
    zfs_version: Optional[tuple[int, int, int]],
    platform_name: Optional[str],
    max_recordsize: Optional[int] = None,
) -> CatalogContext:
    """Build a :class:`CatalogContext` from facts the service layer gathered.

    ``zfs_version`` is F1's effective version (``None`` when unknown, for
    example on NetBSD). ``max_recordsize`` is clamped to the range OpenZFS
    supports and defaults to 16 MiB when it could not be read.
    """
    recordsize = max_recordsize
    if recordsize is None:
        recordsize = DEFAULT_MAX_RECORDSIZE
    recordsize = max(MIN_RECORDSIZE, min(recordsize, DEFAULT_MAX_RECORDSIZE))
    return CatalogContext(
        zfs_version=zfs_version,
        platform=normalize_platform(platform_name),
        max_recordsize=recordsize,
    )


def _opt(
    value: str,
    min_version: tuple[int, int, int] = (0, 0, 0),
    platforms: tuple[str, ...] = (),
    label: str = "",
    note: str = "",
) -> ValueOption:
    return ValueOption(value=value, min_version=min_version, platforms=platforms, label=label, note=note)


def _boolean_options(min_version: tuple[int, int, int] = (0, 0, 0)) -> tuple[ValueOption, ...]:
    return (_opt("on", min_version), _opt("off", min_version))


def _compression_options() -> tuple[ValueOption, ...]:
    options = [
        _opt("off", V_0_6_1, note="No compression."),
        _opt("on", V_0_6_1, note="Use the current pool default (currently lz4)."),
        _opt("lz4", V_0_6_1, note="Fast default algorithm; requires the lz4_compress feature."),
        _opt("lzjb", V_0_6_1, note="Legacy fast algorithm."),
        _opt("zle", V_0_6_1, note="Zero-length encoding."),
        _opt("gzip", V_0_6_1, note="Same as gzip-6."),
    ]
    options.extend(_opt(f"gzip-{level}", V_0_6_1) for level in range(1, 10))
    options.extend(
        [
            _opt("zstd", V_2_0_0, note="Same as zstd-3; OpenZFS 2.0+."),
            _opt("zstd-fast", V_2_0_0, note="Same as zstd-fast-1; OpenZFS 2.0+."),
        ]
    )
    options.extend(_opt(f"zstd-{level}", V_2_0_0) for level in range(1, 20))
    options.extend(_opt(f"zstd-fast-{level}", V_2_0_0) for level in range(1, 11))
    options.extend(_opt(f"zstd-fast-{level}", V_2_0_0) for level in range(20, 110, 10))
    options.extend(_opt(f"zstd-fast-{level}", V_2_0_0) for level in (500, 1000))
    return tuple(options)


def _checksum_options() -> tuple[ValueOption, ...]:
    return (
        _opt("on", V_0_6_1, note="Automatic selection (currently fletcher4)."),
        _opt("off", V_0_6_1, note="Disables integrity checking; not recommended."),
        _opt("fletcher2", V_0_6_1),
        _opt("fletcher4", V_0_6_1),
        _opt("sha256", V_0_6_1),
        _opt("sha512", V_0_7_0, note="Requires the sha512 pool feature."),
        _opt("skein", V_0_7_0, note="Requires the skein pool feature."),
        _opt("edonr", V_0_8_0, platforms=(PLATFORM_LINUX, PLATFORM_NETBSD),
             note="Requires the edonr pool feature; Linux/NetBSD 0.8+, FreeBSD 2.2+."),
        _opt("edonr", V_2_2_0, platforms=(PLATFORM_FREEBSD,),
             note="Requires the edonr pool feature; Linux/NetBSD 0.8+, FreeBSD 2.2+."),
        _opt("blake3", V_2_2_0, note="Requires the blake3 pool feature."),
    )


def _dedup_options() -> tuple[ValueOption, ...]:
    options = [
        _opt("off", V_0_6_1, note="Deduplication disabled."),
        _opt("on", V_0_6_1, note="Deduplicate using the default checksum (sha256)."),
        _opt("verify", V_0_6_1, note="Same as sha256,verify."),
        _opt("sha256", V_0_6_1),
        _opt("sha256,verify", V_0_6_1, note="Byte-compare blocks with matching checksums."),
        _opt("sha512", V_0_7_0),
        _opt("sha512,verify", V_0_7_0),
        _opt("skein", V_0_7_0),
        _opt("skein,verify", V_0_7_0),
        _opt("edonr,verify", V_0_8_0, platforms=(PLATFORM_LINUX, PLATFORM_NETBSD),
             note="edonr always requires verify; FreeBSD 2.2+."),
        _opt("edonr,verify", V_2_2_0, platforms=(PLATFORM_FREEBSD,)),
        _opt("blake3", V_2_2_0),
        _opt("blake3,verify", V_2_2_0),
    ]
    return tuple(options)


def _enum(*values: str, min_version: tuple[int, int, int] = (0, 0, 0),
          platforms: tuple[str, ...] = (), note: str = "") -> tuple[ValueOption, ...]:
    return tuple(_opt(value, min_version, platforms, note=note) for value in values)


CATALOG: tuple[DatasetPropertyDefinition, ...] = (
    DatasetPropertyDefinition(
        "atime", "Access time", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Controls whether access times are updated when files are read. "
             "Disabling improves read performance.",
    ),
    DatasetPropertyDefinition(
        "relatime", "Relative access time", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="When on, atime is updated only when the previous atime is older "
             "than the modification or change time.",
    ),
    DatasetPropertyDefinition(
        "compression", "Compression", SCOPE_BOTH, value_type="enum",
        options=_compression_options(),
        help="Compression algorithm. Changing it affects only newly written data.",
        affects_existing_data=True,
    ),
    DatasetPropertyDefinition(
        "checksum", "Checksum", SCOPE_BOTH, value_type="enum",
        options=_checksum_options(),
        help="Data integrity checksum. Changing it affects only newly written data.",
        affects_existing_data=True,
    ),
    DatasetPropertyDefinition(
        "dedup", "Deduplication", SCOPE_BOTH, value_type="enum",
        options=_dedup_options(),
        help="Deduplication checksum. Enabling deduplication requires a dedup "
             "table and is rarely worth it; use with caution.",
    ),
    DatasetPropertyDefinition(
        "copies", "Copies", SCOPE_BOTH, value_type="enum",
        options=_enum("1", "2", "3"),
        help="Number of copies of user data and metadata stored by ZFS.",
    ),
    # quota, refquota, reservation, refreservation, canmount, the limits and
    # keylocation are PROP_DEFAULT in zfs_prop.c: settable but not inheritable.
    DatasetPropertyDefinition(
        "quota", "Quota", SCOPE_FILESYSTEM, inheritable=False, value_type="size",
        help="Maximum space the dataset and its descendants may consume. "
             "Use 'none' to remove the limit.",
    ),
    DatasetPropertyDefinition(
        "refquota", "Referenced quota", SCOPE_FILESYSTEM, inheritable=False,
        value_type="size",
        help="Maximum space the dataset itself may consume, excluding snapshots "
             "and descendants. Use 'none' to remove the limit.",
    ),
    DatasetPropertyDefinition(
        "reservation", "Reservation", SCOPE_BOTH, inheritable=False, value_type="size",
        help="Minimum space guaranteed to this dataset and its descendants. "
             "Use 'none' to remove the reservation.",
    ),
    DatasetPropertyDefinition(
        "refreservation", "Referenced reservation", SCOPE_BOTH, inheritable=False,
        value_type="refreservation",
        help="Minimum space guaranteed to this dataset alone, excluding "
             "snapshots and descendants. Use 'none' to remove it, or 'auto' "
             "on a volume to reserve its full size.",
    ),
    DatasetPropertyDefinition(
        "recordsize", "Record size", SCOPE_FILESYSTEM, value_type="recordsize",
        help="Suggested block size for files. Changing it affects only files "
             "created afterwards. Values above 128K require the large_blocks "
             "pool feature.",
        affects_existing_data=True,
    ),
    DatasetPropertyDefinition(
        "sync", "Synchronous writes", SCOPE_BOTH, value_type="enum",
        options=_enum("standard", "always", "disabled"),
        help="Synchronous write behaviour. 'disabled' risks data loss on power loss.",
    ),
    DatasetPropertyDefinition(
        "readonly", "Read-only", SCOPE_BOTH, value_type="enum",
        options=_boolean_options(),
        help="Makes the dataset read-only.",
    ),
    DatasetPropertyDefinition(
        "exec", "Executable files", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Allow files in the dataset to be executed.",
    ),
    DatasetPropertyDefinition(
        "setuid", "setuid/setgid", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Allow setuid and setgid bits on files in the dataset.",
    ),
    DatasetPropertyDefinition(
        "devices", "Device nodes", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Allow device nodes to be opened from the dataset.",
    ),
    DatasetPropertyDefinition(
        "snapdir", "Snapshot directory", SCOPE_FILESYSTEM, value_type="enum",
        options=(
            _opt("hidden", V_0_6_1),
            _opt("visible", V_0_6_1),
            _opt("disabled", V_2_3_0, note="Hides .zfs entirely; OpenZFS 2.3+."),
        ),
        help="Whether the .zfs snapshot directory is hidden, visible or disabled.",
    ),
    DatasetPropertyDefinition(
        "aclmode", "ACL mode", SCOPE_FILESYSTEM, value_type="enum",
        options=_enum("discard", "groupmask", "passthrough", "restricted"),
        help="How ACLs are modified by chmod and file creation.",
    ),
    DatasetPropertyDefinition(
        "aclinherit", "ACL inheritance", SCOPE_FILESYSTEM, value_type="enum",
        options=_enum("discard", "noallow", "restricted", "passthrough", "passthrough-x"),
        help="How ACL entries are inherited by new files and directories.",
    ),
    DatasetPropertyDefinition(
        "acltype", "ACL type", SCOPE_FILESYSTEM, value_type="enum",
        options=(
            _opt("off", V_0_6_1),
            _opt("noacl", V_0_6_1, note="Alias for off."),
            _opt("posixacl", V_0_6_1, platforms=(PLATFORM_LINUX,),
                 note="POSIX ACLs are only implemented on Linux."),
            _opt("posix", V_2_0_0, platforms=(PLATFORM_LINUX,),
                 note="Alias for posixacl."),
            _opt("nfsv4", V_0_6_1, platforms=(PLATFORM_FREEBSD,),
                 note="NFSv4 ACLs are only implemented on FreeBSD. Linux "
                      "accepts the value but treats the dataset as acltype=off."),
        ),
        help="Which ACL style the dataset uses. nfsv4 is only effective on "
             "FreeBSD (Linux accepts it but disables ACLs); posix ACLs are "
             "Linux-specific.",
    ),
    DatasetPropertyDefinition(
        "canmount", "Can mount", SCOPE_FILESYSTEM, inheritable=False, value_type="enum",
        options=_enum("on", "off", "noauto"),
        help="Controls whether the dataset is mounted automatically.",
    ),
    DatasetPropertyDefinition(
        "xattr", "Extended attributes", SCOPE_FILESYSTEM, value_type="enum",
        options=(
            _opt("on", V_0_6_1, note="Same as sa."),
            _opt("off", V_0_6_1),
            _opt("sa", V_0_6_1, note="System-attribute based; faster."),
            _opt("dir", V_0_6_1, note="Directory based."),
        ),
        help="Whether and how extended attributes are stored.",
    ),
    DatasetPropertyDefinition(
        "nbmand", "Non-blocking mandatory locking", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Enables non-blocking mandatory locking semantics (SMB/NFSv4).",
    ),
    DatasetPropertyDefinition(
        "vscan", "Virus scanning", SCOPE_FILESYSTEM, value_type="enum",
        options=_boolean_options(),
        help="Marks the dataset for external virus scanning.",
    ),
    DatasetPropertyDefinition(
        "primarycache", "Primary cache", SCOPE_BOTH, value_type="enum",
        options=_enum("all", "none", "metadata"),
        help="What the ARC caches for this dataset.",
    ),
    DatasetPropertyDefinition(
        "secondarycache", "Secondary cache", SCOPE_BOTH, value_type="enum",
        options=_enum("all", "none", "metadata"),
        help="What the L2ARC caches for this dataset.",
    ),
    DatasetPropertyDefinition(
        "logbias", "Log bias", SCOPE_BOTH, value_type="enum",
        options=_enum("latency", "throughput"),
        help="ZIL behaviour: optimise for latency or throughput.",
    ),
    DatasetPropertyDefinition(
        "redundant_metadata", "Redundant metadata", SCOPE_BOTH, value_type="enum",
        options=(
            _opt("all", V_0_6_1),
            _opt("most", V_0_6_1),
            _opt("some", V_2_2_0, note="OpenZFS 2.2+."),
            _opt("none", V_2_2_0, note="OpenZFS 2.2+; an entire dataset can be lost."),
        ),
        help="How much metadata ZFS stores redundantly.",
    ),
    DatasetPropertyDefinition(
        "snapdev", "Snapshot devices", SCOPE_BOTH, value_type="enum",
        options=_enum("hidden", "visible"),
        help="Whether volume snapshot device nodes under /dev/zvol are hidden or visible.",
    ),
    DatasetPropertyDefinition(
        "mountpoint", "Mount point", SCOPE_FILESYSTEM, value_type="path",
        help="Mount point path, 'legacy' or 'none'.",
    ),
    DatasetPropertyDefinition(
        "sharenfs", "NFS share", SCOPE_FILESYSTEM, value_type="string",
        help="NFS share options, 'on', 'off' or explicit export options.",
    ),
    DatasetPropertyDefinition(
        "sharesmb", "SMB share", SCOPE_FILESYSTEM, value_type="string",
        help="SMB share options, 'on' or 'off'. Requires Samba usershares on Linux "
             "and is not supported on FreeBSD.",
    ),
    DatasetPropertyDefinition(
        "dnodesize", "Dnode size", SCOPE_FILESYSTEM, value_type="enum",
        min_version=V_0_7_0,
        options=_enum("legacy", "auto", "1k", "2k", "4k", "8k", "16k"),
        help="Dnode size. Values other than legacy require the large_dnode pool "
             "feature.",
    ),
    DatasetPropertyDefinition(
        "special_small_blocks", "Special small blocks", SCOPE_BOTH,
        value_type="special_small_blocks", min_version=V_0_8_0,
        warning="If this is greater than or equal to recordsize, all data blocks "
                "go to the special vdev (when the pool has one).",
        help="Block size threshold for placing small file or zvol blocks in the "
             "special allocation class. OpenZFS 2.4 accepts any value up to 16M "
             "and zvols; earlier releases require a power of two up to 1M.",
    ),
    DatasetPropertyDefinition(
        "direct", "Direct I/O", SCOPE_FILESYSTEM, value_type="enum",
        min_version=V_2_3_0,
        options=_enum("disabled", "standard", "always"),
        help="Controls O_DIRECT handling. OpenZFS 2.3+; not supported for zvols.",
    ),
    DatasetPropertyDefinition(
        "filesystem_limit", "Filesystem limit", SCOPE_FILESYSTEM, inheritable=False,
        value_type="count", min_version=V_0_6_5,
        help="Maximum number of filesystems and volumes below this dataset. "
             "Requires the filesystem_limit pool feature.",
    ),
    DatasetPropertyDefinition(
        "snapshot_limit", "Snapshot limit", SCOPE_BOTH, inheritable=False,
        value_type="count", min_version=V_0_6_5,
        help="Maximum number of snapshots below this dataset. Requires the "
             "snapshot_limit pool feature.",
    ),
    DatasetPropertyDefinition(
        "keylocation", "Key location", SCOPE_BOTH, value_type="keylocation",
        min_version=V_0_8_0, inheritable=False,
        help="Where zfs load-key finds the key for an encryption root: none "
             "(the default for unencrypted datasets), prompt, file:///path or "
             "an http(s) URL (OpenZFS 2.1.3+).",
    ),
    # volsize and volmode are owned by the zvol options (backlog M7), which
    # guard shrinking and platform support. The generic editor refuses them.
    DatasetPropertyDefinition(
        "volsize", "Volume size", SCOPE_VOLUME, editable=False, inheritable=False,
        value_type="size", deferred_to="the volume options",
        help="Logical size of a zvol.",
    ),
    DatasetPropertyDefinition(
        "volmode", "Volume mode", SCOPE_VOLUME, editable=False,
        value_type="enum", deferred_to="the volume options",
        options=_enum("default", "full", "dev", "none"),
        help="How the zvol is exposed to the OS.",
    ),
)

BY_NAME: dict[str, DatasetPropertyDefinition] = {definition.name: definition for definition in CATALOG}


def definition_for(name: str) -> Optional[DatasetPropertyDefinition]:
    """Return the catalog entry for a native property name."""
    return BY_NAME.get(name)


def is_user_property(name: str) -> bool:
    """User properties are ``module:property`` names."""
    return isinstance(name, str) and ":" in name


def _version_ok(min_version: tuple[int, int, int], context: CatalogContext) -> bool:
    if min_version <= V_0_6_1:
        # Baseline values exist on every supported ZFS, NetBSD included.
        return True
    if context.zfs_version is None:
        # Unknown version: only baseline options are safe to offer.
        return False
    return context.zfs_version >= min_version


def _platform_ok(platforms: tuple[str, ...], context: CatalogContext) -> bool:
    return not platforms or context.platform in platforms


def is_available(definition: DatasetPropertyDefinition, context: CatalogContext) -> bool:
    """Whether the property itself exists on this OpenZFS version/platform."""
    return _version_ok(definition.min_version, context) and _platform_ok(definition.platforms, context)


def scope_matches(definition: DatasetPropertyDefinition, context: CatalogContext,
                  dataset_type: Optional[str]) -> bool:
    """Whether a property applies to the given dataset type."""
    if not dataset_type or dataset_type not in (SCOPE_FILESYSTEM, SCOPE_VOLUME):
        return True
    if definition.scope == SCOPE_BOTH:
        # special_small_blocks only gained zvol support in OpenZFS 2.4.
        if definition.name == "special_small_blocks":
            if context.zfs_version is None or context.zfs_version < V_2_4_0:
                return dataset_type == SCOPE_FILESYSTEM
        return True
    return definition.scope == dataset_type


def _format_size(size: int) -> str:
    """Format a power-of-two byte count the way ``zfs get`` does (128K, 1M)."""
    if size >= 1024 ** 2 and size % (1024 ** 2) == 0:
        return f"{size // (1024 ** 2)}M"
    if size >= 1024 and size % 1024 == 0:
        return f"{size // 1024}K"
    return str(size)


def _recordsize_options(context: CatalogContext) -> tuple[ValueOption, ...]:
    values: list[ValueOption] = []
    size = MIN_RECORDSIZE
    while size <= context.max_recordsize:
        if size <= 128 * 1024:
            values.append(_opt(_format_size(size), V_0_6_1))
        else:
            values.append(_opt(_format_size(size), V_0_7_0, note="Requires large_blocks."))
        size *= 2
    return tuple(
        option for option in values
        if _version_ok(option.min_version, context)
    )


def _special_small_blocks_options(context: CatalogContext) -> tuple[ValueOption, ...]:
    if context.zfs_version is None:
        return ()
    if context.zfs_version >= V_2_4_0:
        # 2.4 accepts any size from 0 to 16M, so a free-form value is offered.
        return ()
    values = [_opt("0", V_0_8_0)]
    size = 512
    while size <= LEGACY_SPECIAL_SMALL_BLOCKS_LIMIT:
        values.append(_opt(_format_size(size), V_0_8_0))
        size *= 2
    return tuple(values)


def available_options(definition: DatasetPropertyDefinition,
                      context: CatalogContext) -> tuple[ValueOption, ...]:
    """Return the option values supported by the installed version/platform."""
    if definition.value_type == "recordsize":
        return _recordsize_options(context)
    if definition.value_type == "special_small_blocks":
        return _special_small_blocks_options(context)
    return tuple(
        option for option in definition.options
        if _version_ok(option.min_version, context) and _platform_ok(option.platforms, context)
    )


def option_values(definition: DatasetPropertyDefinition,
                  context: CatalogContext) -> list[str]:
    return [option.value for option in available_options(definition, context)]


def option_selected_value(name: str, value: Optional[str],
                          context: CatalogContext) -> str:
    """Return the dropdown option that matches a live property value.

    ``zfs get`` may print a size as "512B" or "128K"; the catalog lists
    recordsize and special_small_blocks options as "512", "128K", "1M". The
    live value is normalized to that form before it is compared with the
    option list. Unknown or unparsable values are returned unchanged.
    """
    definition = BY_NAME.get(name)
    raw = str(value if value is not None else "").strip()
    if definition is None or definition.value_type not in (
        "recordsize", "special_small_blocks"
    ):
        return raw
    try:
        return _format_size(_parse_size(raw))
    except PropertyValidationError:
        return raw


def editable_definitions(context: CatalogContext,
                         dataset_type: Optional[str] = None) -> list[DatasetPropertyDefinition]:
    """Editable, supported properties in catalog order for a dataset type."""
    return [
        definition for definition in CATALOG
        if definition.editable
        and is_available(definition, context)
        and scope_matches(definition, context, dataset_type)
    ]


def option_map(context: CatalogContext,
               dataset_type: Optional[str] = None) -> dict[str, list[str]]:
    """Map property name -> supported option values for the editor."""
    options: dict[str, list[str]] = {}
    for definition in editable_definitions(context, dataset_type):
        values = option_values(definition, context)
        if values:
            options[definition.name] = values
    return options


def help_map(context: CatalogContext,
             dataset_type: Optional[str] = None) -> dict[str, str]:
    return {definition.name: definition.help for definition in editable_definitions(context, dataset_type)}


def warning_map(context: CatalogContext,
                dataset_type: Optional[str] = None) -> dict[str, str]:
    return {
        definition.name: definition.warning
        for definition in editable_definitions(context, dataset_type)
        if definition.warning
    }


def label_map(context: CatalogContext,
              dataset_type: Optional[str] = None) -> dict[str, str]:
    return {definition.name: definition.label for definition in editable_definitions(context, dataset_type)}


def value_type_map(context: CatalogContext,
                   dataset_type: Optional[str] = None) -> dict[str, str]:
    return {definition.name: definition.value_type for definition in editable_definitions(context, dataset_type)}


def _parse_size(value: str) -> int:
    match = _SIZE_RE.fullmatch(value.strip())
    if not match:
        raise PropertyValidationError(f"'{value}' is not a valid size.")
    number, suffix = match.groups()
    factor = SIZE_UNITS.get(suffix.lower())
    if factor is None:
        raise PropertyValidationError(f"'{value}' has an unknown size suffix.")
    return int(float(number) * factor)


def _validate_size(value: str) -> str:
    if value.lower() == "none":
        return "none"
    _parse_size(value)
    return value


def _validate_refreservation(value: str) -> str:
    # 'auto' sizes the reservation to the volume (zfsprops(7)); ZFS itself
    # rejects it on filesystems.
    if value.lower() == "auto":
        return "auto"
    return _validate_size(value)


def _validate_mountpoint(value: str) -> str:
    if value in ("none", "legacy"):
        return value
    if not value.startswith("/"):
        raise PropertyValidationError(
            "mountpoint must be an absolute path (starting with /), 'none' or 'legacy'."
        )
    return _validate_free_string(value, "mountpoint")


def _validate_count(value: str) -> str:
    if value.lower() == "none":
        return "none"
    if not value.isdigit():
        raise PropertyValidationError(f"'{value}' must be a whole number or 'none'.")
    return value


def _validate_recordsize(value: str, context: CatalogContext) -> str:
    size = _parse_size(value)
    if size < MIN_RECORDSIZE or size > context.max_recordsize:
        raise PropertyValidationError(
            f"recordsize must be between {MIN_RECORDSIZE} and {context.max_recordsize} bytes."
        )
    if size & (size - 1):
        raise PropertyValidationError("recordsize must be a power of two.")
    if size > 128 * 1024 and not _version_ok(V_0_7_0, context):
        raise PropertyValidationError("recordsize above 128K requires OpenZFS 0.7 or newer.")
    return str(size)


def _validate_special_small_blocks(value: str, context: CatalogContext) -> str:
    size = _parse_size(value)
    if context.zfs_version is not None and context.zfs_version >= V_2_4_0:
        if size > MAX_SPECIAL_SMALL_BLOCKS:
            raise PropertyValidationError("special_small_blocks must be at most 16 MiB.")
        return str(size)
    if size == 0:
        return "0"
    if size < 512 or size > LEGACY_SPECIAL_SMALL_BLOCKS_LIMIT:
        raise PropertyValidationError(
            "special_small_blocks must be 0 or a power of two from 512 to 1M "
            "before OpenZFS 2.4."
        )
    if size & (size - 1):
        raise PropertyValidationError(
            "special_small_blocks must be a power of two before OpenZFS 2.4."
        )
    return str(size)


def _validate_keylocation(value: str, context: CatalogContext) -> str:
    if not _KEYLOCATION_RE.fullmatch(value):
        raise PropertyValidationError(
            "keylocation must be 'none', 'prompt', file:///absolute/path or an "
            "http(s) URL."
        )
    if value.startswith(("http://", "https://")):
        version = context.zfs_version
        if version is None or version < V_2_1_3:
            raise PropertyValidationError(
                "keylocation http(s) URLs require OpenZFS 2.1.3 or newer."
            )
    return value


def _validate_free_string(value: str, name: str) -> str:
    if "\x00" in value or "\n" in value or "\r" in value:
        raise PropertyValidationError(f"{name} contains an invalid character.")
    return value


def validate_user_property_name(name: str) -> str:
    """Validate a ``module:property`` name against zfsprops(7) rules."""
    candidate = (name or "").strip()
    if not candidate:
        raise PropertyValidationError("User property names cannot be empty.")
    if ":" not in candidate:
        raise PropertyValidationError("User property names must contain a colon, e.g. module:property.")
    if len(candidate) > MAX_USER_PROPERTY_NAME:
        raise PropertyValidationError(
            f"User property names can be at most {MAX_USER_PROPERTY_NAME} characters."
        )
    if candidate.startswith("-"):
        raise PropertyValidationError("User property names cannot begin with a dash.")
    if not _USER_PROPERTY_RE.fullmatch(candidate):
        raise PropertyValidationError(
            "User property names may contain only lowercase letters, numbers, "
            "colons, dashes, periods and underscores."
        )
    module, _, prop = candidate.partition(":")
    if not module or not prop:
        raise PropertyValidationError("Both the module and property parts must be non-empty.")
    return candidate


def ensure_settable(name: str, context: CatalogContext,
                    dataset_type: Optional[str] = None) -> str:
    """Raise unless ``name`` may be written by this editor.

    Returns the canonical property name (user names are returned unchanged).
    """
    candidate = (name or "").strip()
    if is_user_property(candidate):
        return validate_user_property_name(candidate)
    definition = BY_NAME.get(candidate)
    if definition is None:
        if candidate in READ_ONLY_PROPERTIES:
            raise PropertyValidationError(f"{candidate} is read-only: {READ_ONLY_PROPERTIES[candidate]}")
        raise PropertyValidationError(f"{candidate} is not an editable dataset property.")
    if not definition.editable:
        if definition.deferred_to:
            raise PropertyValidationError(
                f"{candidate} cannot be changed in the property editor; use {definition.deferred_to}."
            )
        raise PropertyValidationError(f"{candidate} is not editable.")
    if not is_available(definition, context):
        raise PropertyValidationError(
            f"{candidate} is not supported on this OpenZFS version or platform."
        )
    if not scope_matches(definition, context, dataset_type):
        raise PropertyValidationError(
            f"{candidate} does not apply to this dataset type."
        )
    return candidate


def validate_property_value(name: str, value: str, context: CatalogContext,
                            dataset_type: Optional[str] = None) -> str:
    """Validate and normalize a value before running ``zfs set``."""
    candidate = ensure_settable(name, context, dataset_type)
    raw = value if value is not None else ""
    if is_user_property(candidate):
        return _validate_free_string(raw, candidate)
    definition = BY_NAME[candidate]
    value_type = definition.value_type
    if value_type == "enum":
        allowed = option_values(definition, context)
        lowered = raw.strip().lower()
        for option in allowed:
            if option.lower() == lowered:
                return option
        raise PropertyValidationError(
            f"{candidate} must be one of: {', '.join(allowed)}."
        )
    if value_type == "size":
        return _validate_size(raw.strip())
    if value_type == "refreservation":
        return _validate_refreservation(raw.strip())
    if value_type == "path":
        return _validate_mountpoint(raw.strip())
    if value_type == "count":
        return _validate_count(raw.strip())
    if value_type == "recordsize":
        return _validate_recordsize(raw.strip(), context)
    if value_type == "special_small_blocks":
        return _validate_special_small_blocks(raw.strip(), context)
    if value_type == "keylocation":
        return _validate_keylocation(raw.strip(), context)
    return _validate_free_string(raw.strip(), candidate)


def existing_data_notice(name: str) -> Optional[str]:
    """Success-message hint for properties that only affect new data."""
    if name in ("recordsize", "compression", "checksum"):
        return (
            "Existing data is unchanged; the new setting applies only to data "
            "written from now on."
        )
    return None


def _property_value_bytes(properties: Optional[dict], name: str) -> Optional[int]:
    if not properties:
        return None
    entry = properties.get(name)
    if not isinstance(entry, dict):
        return None
    raw = str(entry.get("value", ""))
    try:
        return _parse_size(raw)
    except PropertyValidationError:
        return None


def warnings_for(name: str, value: str, context: CatalogContext,
                 properties: Optional[dict] = None) -> list[str]:
    """Contextual warnings for a pending property change."""
    warnings: list[str] = []
    definition = BY_NAME.get(name)
    if definition and definition.warning:
        warnings.append(definition.warning)
    if name == "special_small_blocks":
        try:
            threshold = _parse_size(str(value))
        except PropertyValidationError:
            threshold = None
        recordsize = _property_value_bytes(properties, "recordsize")
        if threshold and recordsize and threshold >= recordsize:
            warnings.append(
                f"special_small_blocks ({threshold} bytes) is greater than or equal "
                f"to recordsize ({recordsize} bytes); all data blocks will be sent "
                "to the special vdev if the pool has one."
            )
    return warnings


def user_properties(properties: Optional[dict]) -> list[dict]:
    """Extract ``module:property`` entries from ``zfs get all`` output."""
    rows: list[dict] = []
    for name, entry in (properties or {}).items():
        if not is_user_property(name) or not isinstance(entry, dict):
            continue
        rows.append(
            {
                "name": name,
                "value": entry.get("value", ""),
                "source": entry.get("source", ""),
            }
        )
    return sorted(rows, key=lambda row: row["name"])


