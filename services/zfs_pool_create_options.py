"""Pool creation option definitions, validation and command building.

Issue #15: ``zpool create`` previously only forwarded ``-o ashift``. This
module centralises the option surface used by the pool-create view and the
pool service:

* root dataset properties (``-O``) - applied to the new root dataset so they
  are reported as ``local`` on it;
* pool properties (``-o``) - ``compatibility`` (with the files installed in
  ``compatibility.d``), ``autotrim``, ``cachefile`` and ``comment``;
* ``-m <mountpoint>`` and ``-R <altroot>``;
* dRAID topology strings (``draid[1-3][:<data>d][:<children>c][:<spares>s]``)
  with layout validation;
* ``-n`` dry-run support.

Platform gating is derived from :mod:`services.zfs_capabilities`: the module
returns an all-False capability set on NetBSD, so version-gated fields
(dRAID, ``compatibility``) and platform-specific values (``xattr=sa``,
``acltype=posixacl``) are hidden there rather than being offered and failing.

Everything here is pure data/validation except :func:`list_compatibility_files`,
which only reads the names and leading comments of installed compatibility
files. No ZFS command is executed from this module.
"""

from __future__ import annotations

import os
import re
import shlex
from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, Iterable, List, Mapping, Optional, Sequence, Tuple

from services.utils import get_os_type, is_freebsd, is_linux, is_netbsd
from services.zfs_capabilities import caps

PLATFORM_LINUX = "Linux"
PLATFORM_FREEBSD = "FreeBSD"
PLATFORM_NETBSD = "NetBSD"
ALL_PLATFORMS: FrozenSet[str] = frozenset(
    {PLATFORM_LINUX, PLATFORM_FREEBSD, PLATFORM_NETBSD}
)
OPENZFS_PLATFORMS: FrozenSet[str] = frozenset({PLATFORM_LINUX, PLATFORM_FREEBSD})

#: Where ``zpool`` looks for ``compatibility`` files. The first writable
#: location is checked first by ``zpool`` itself; we list every candidate.
COMPATIBILITY_DIRS: Dict[str, Tuple[str, ...]] = {
    PLATFORM_LINUX: (
        "/etc/zfs/compatibility.d",
        "/usr/share/zfs/compatibility.d",
    ),
    PLATFORM_FREEBSD: (
        "/etc/zfs/compatibility.d",
        "/usr/local/share/zfs/compatibility.d",
    ),
}

#: ``legacy`` is a built-in keyword, not a file; ``off`` disables the check.
BUILTIN_COMPATIBILITY_VALUES = ("legacy", "off")
BOOT_COMPATIBILITY_FILE = "grub2"

_POOL_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]{0,254}$")
_DRAID_RE = re.compile(r"^draid([1-3])?(?::(\d+)d)?(?::(\d+)c)?(?::(\d+)s)?$")
#: Reject control characters in any value we forward to ``zpool``.
_INVALID_VALUE_RE = re.compile(r"[\x00-\x1f\x7f]")
#: ``comment`` is limited to printable ASCII (zpoolprops(7)).
_COMMENT_RE = re.compile(r"[\x20-\x7e]*")

#: Data vdev types the create form may request. Anything else is refused so a
#: crafted form cannot place an arbitrary token in the vdev specification.
DATA_VDEV_TYPES: FrozenSet[str] = frozenset(
    {"single", "mirror", "raidz", "raidz1", "raidz2", "raidz3",
     "draid", "draid1", "draid2", "draid3"}
)
LOG_LAYOUTS: FrozenSet[str] = frozenset({"mirror", "stripe"})

#: VDEV_DRAID_MAX_CHILDREN in OpenZFS.
DRAID_MAX_CHILDREN = 255
#: MIN_PBKDF2_ITERATIONS in OpenZFS.
PBKDF2_MIN_ITERATIONS = 100000
#: ZPROP_MAX_COMMENT in OpenZFS.
COMMENT_MAX_LENGTH = 32
#: Root dataset mountpoint keywords accepted by ``zpool create -m``.
MOUNTPOINT_KEYWORDS = ("none", "legacy")


@dataclass(frozen=True)
class PropertySpec:
    """A property offered by the pool-create form."""

    name: str
    label: str
    help: str = ""
    choices: Tuple[str, ...] = ()
    default: str = ""
    platforms: FrozenSet[str] = ALL_PLATFORMS
    capability: Optional[str] = None
    free_text: bool = False

    def supports(self, platform: str, capabilities: Any) -> bool:
        if platform not in self.platforms:
            return False
        if self.capability and not getattr(capabilities, self.capability, False):
            return False
        return True


#: Root dataset properties applied with ``-O``. Values that are creation-only
#: or platform specific are listed per platform.
ROOT_PROPERTY_SPECS: Tuple[PropertySpec, ...] = (
    PropertySpec(
        name="compression",
        label="Compression",
        help="Inherit (on = lz4) unless overridden. zstd needs OpenZFS 2.0+.",
        choices=(
            "off",
            "on",
            "lz4",
            "lz4hc",
            "zle",
            "gzip",
            "gzip-9",
            "zstd",
            "zstd-fast",
        ),
        default="",
    ),
    PropertySpec(
        name="atime",
        label="Access time updates",
        help="atime=off reduces write amplification on busy pools.",
        choices=("on", "off"),
        default="on",
    ),
    PropertySpec(
        name="xattr",
        label="Extended attributes",
        help="sa stores xattrs as system attributes (Linux only); dir stores "
        "them as directory entries; on uses the platform default.",
        choices=("on", "off", "dir"),
        default="on",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="acltype",
        label="ACL type",
        help="posixacl is Linux only; FreeBSD uses NFSv4 ACLs.",
        choices=("off", "noacl", "nfsv4"),
        default="",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="dnodesize",
        label="dnode size",
        help="legacy is the pool default; auto selects the best size.",
        choices=("legacy", "auto", "1k", "2k", "4k", "8k", "16k"),
        default="legacy",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="normalization",
        label="Unicode normalization",
        help="Creation-only. Requires utf8only=on.",
        choices=("none", "formC", "formD", "formKC", "formKD"),
        default="",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="utf8only",
        label="UTF-8 only filenames",
        help="Creation-only. Required when normalization is enabled.",
        choices=("on", "off"),
        default="",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="casesensitivity",
        label="Case sensitivity",
        help="Creation-only; cannot be changed later.",
        choices=("sensitive", "insensitive", "mixed"),
        default="",
        platforms=OPENZFS_PLATFORMS,
    ),
)

#: Pool properties applied with ``-o``. ``ashift`` keeps its dedicated widget
#: in the template, so it is not repeated here.
POOL_PROPERTY_SPECS: Tuple[PropertySpec, ...] = (
    PropertySpec(
        name="compatibility",
        label="Compatibility",
        help="Restricts pool features to the set listed in the chosen file. "
        "Use grub2 for pools that must boot with GRUB.",
        capability="compatibility_property",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="autotrim",
        label="Automatic TRIM",
        help="Trim freed blocks automatically (OpenZFS 2.0+).",
        choices=("on", "off"),
        default="",
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="cachefile",
        label="Cache file",
        help="Path of the pool cache file, or 'none' to disable it.",
        default="",
        free_text=True,
        platforms=OPENZFS_PLATFORMS,
    ),
    PropertySpec(
        name="comment",
        label="Pool comment",
        help="Free-form description stored on the pool.",
        default="",
        free_text=True,
        platforms=OPENZFS_PLATFORMS,
    ),
)

#: ``ashift`` is always validated as an integer in this range.
ASHIFT_VALUES: Tuple[str, ...] = tuple(str(v) for v in range(9, 17))

#: Every property key we are willing to forward to ``zpool``. Anything else is
#: rejected, so a crafted form cannot smuggle extra ``-o``/``-O`` arguments.
ALLOWED_ROOT_PROPERTY_KEYS: FrozenSet[str] = frozenset(
    spec.name for spec in ROOT_PROPERTY_SPECS
) | frozenset(
    {
        "encryption",
        "keyformat",
        "keylocation",
        "pbkdf2iters",
    }
)

#: Encryption keys are refused on NetBSD, whose ZFS has no native encryption.
ENCRYPTION_PROPERTY_KEYS: FrozenSet[str] = frozenset(
    {"encryption", "keyformat", "keylocation", "pbkdf2iters"}
)

ALLOWED_POOL_PROPERTY_KEYS: FrozenSet[str] = frozenset(
    spec.name for spec in POOL_PROPERTY_SPECS
) | frozenset({"ashift"})


def current_platform() -> str:
    """Return the platform name used for gating (``Linux``/``FreeBSD``/...)."""
    return get_os_type()


def get_root_property_specs(
    platform: Optional[str] = None, capabilities: Any = None
) -> List[PropertySpec]:
    """Return the root dataset property specs supported on this host."""
    platform = platform or current_platform()
    capabilities = capabilities if capabilities is not None else caps
    specs: List[PropertySpec] = []
    for spec in ROOT_PROPERTY_SPECS:
        if not spec.supports(platform, capabilities):
            continue
        if spec.name == "xattr" and platform == PLATFORM_LINUX:
            spec = _replace(spec, choices=("on", "off", "dir", "sa"), default="sa")
        elif spec.name == "acltype" and platform == PLATFORM_LINUX:
            spec = _replace(spec, choices=("off", "noacl", "posixacl", "nfsv4"))
        elif spec.name == "compression" and platform == PLATFORM_NETBSD:
            # NetBSD ZFS predates OpenZFS 2.0 and has no zstd.
            netbsd_choices = []
            for choice in spec.choices:
                if not choice.startswith("zstd"):
                    netbsd_choices.append(choice)
            spec = _replace(
                spec,
                choices=tuple(netbsd_choices),
                help="Inherit (on = lz4) unless overridden.",
            )
        specs.append(spec)
    return specs


def get_pool_property_specs(
    platform: Optional[str] = None, capabilities: Any = None
) -> List[PropertySpec]:
    """Return the pool property specs supported on this host."""
    platform = platform or current_platform()
    capabilities = capabilities if capabilities is not None else caps
    return [spec for spec in POOL_PROPERTY_SPECS if spec.supports(platform, capabilities)]


def _replace(spec: PropertySpec, **changes: Any) -> PropertySpec:
    """Return a copy of ``spec`` with ``changes`` applied."""
    return PropertySpec(
        name=changes.get("name", spec.name),
        label=changes.get("label", spec.label),
        help=changes.get("help", spec.help),
        choices=changes.get("choices", spec.choices),
        default=changes.get("default", spec.default),
        platforms=changes.get("platforms", spec.platforms),
        capability=changes.get("capability", spec.capability),
        free_text=changes.get("free_text", spec.free_text),
    )


# ---------------------------------------------------------------------------
# compatibility files
# ---------------------------------------------------------------------------


def compatibility_dirs(platform: Optional[str] = None) -> Tuple[str, ...]:
    """Return the ``compatibility.d`` directories for ``platform``."""
    platform = platform or current_platform()
    return COMPATIBILITY_DIRS.get(platform, ())


def _read_compatibility_description(path: str) -> str:
    """Return the leading ``#`` comment block of a compatibility file."""
    lines: List[str] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for _ in range(20):
                line = handle.readline()
                if not line:
                    break
                stripped = line.strip()
                if not stripped:
                    if lines:
                        break
                    continue
                if not stripped.startswith("#"):
                    break
                text = stripped.lstrip("#").strip()
                if text:
                    lines.append(text)
    except OSError:
        return ""
    return " ".join(lines)


def list_compatibility_files(
    platform: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List the compatibility values available on this host.

    Returns a list of ``{"name", "description", "builtin", "boot"}`` dicts.
    The built-in ``legacy`` value is always first, followed by the files found
    in the platform's ``compatibility.d`` directories. ``grub2`` is flagged so
    the template can call it out for boot pools.
    """
    platform = platform or current_platform()
    entries: List[Dict[str, Any]] = [
        {
            "name": "legacy",
            "description": "Most restrictive: no pool features are enabled, now or by a later upgrade.",
            "builtin": True,
            "boot": False,
        }
    ]
    seen = {"legacy"}
    for directory in compatibility_dirs(platform):
        try:
            names = sorted(os.listdir(directory))
        except OSError:
            continue
        for name in names:
            if name in seen:
                continue
            path = os.path.join(directory, name)
            if not os.path.isfile(path):
                continue
            seen.add(name)
            entries.append(
                {
                    "name": name,
                    "description": _read_compatibility_description(path),
                    "builtin": False,
                    "boot": name == BOOT_COMPATIBILITY_FILE,
                }
            )
    return entries


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def validate_pool_name(pool_name: str) -> str:
    """Validate a pool name and return it unchanged."""
    name = (pool_name or "").strip()
    if not name:
        raise ValueError("A pool name is required.")
    if not _POOL_NAME_RE.fullmatch(name):
        raise ValueError(
            "Pool names must start with a letter and contain only letters, "
            "digits, '_', '-', '.' or ':'."
        )
    return name


def validate_value(value: str, label: str = "value") -> str:
    """Reject control characters in a free-form option value."""
    text = "" if value is None else str(value).strip()
    if _INVALID_VALUE_RE.search(text):
        raise ValueError(f"The {label} contains invalid control characters.")
    return text


def validate_property_value(
    name: str, value: str, spec: Optional[PropertySpec] = None
) -> str:
    """Validate one ``-o``/``-O`` property value.

    Enumerated properties are checked against their allowed choices; free-text
    values are only checked for control characters.
    """
    text = validate_value(value, f"'{name}' property")
    if name == "ashift":
        if text not in ASHIFT_VALUES:
            raise ValueError(
                f"Unsupported ashift value '{text}'; expected one of "
                f"{', '.join(ASHIFT_VALUES)}."
            )
        return text
    if name == "compatibility":
        return validate_compatibility(text)
    if name == "cachefile":
        if text != "none" and not text.startswith("/"):
            raise ValueError("The cache file must be an absolute path or 'none'.")
        return text
    if name == "comment":
        if len(text) > COMMENT_MAX_LENGTH or not _COMMENT_RE.fullmatch(text):
            raise ValueError(
                f"The pool comment must be at most {COMMENT_MAX_LENGTH} "
                "printable ASCII characters."
            )
        return text
    if name == "pbkdf2iters":
        if not text.isdigit() or int(text) < PBKDF2_MIN_ITERATIONS:
            raise ValueError(
                f"PBKDF2 iterations must be a whole number of at least "
                f"{PBKDF2_MIN_ITERATIONS}."
            )
        return text
    if spec is not None and spec.choices and not spec.free_text:
        if text not in spec.choices:
            raise ValueError(
                f"Unsupported {name} value '{text}'; expected one of "
                f"{', '.join(spec.choices)}."
            )
    return text


def validate_mountpoint(value: str) -> str:
    """Validate ``-m <mountpoint>``."""
    text = validate_value(value, "mountpoint")
    if not text:
        return text
    if text in MOUNTPOINT_KEYWORDS:
        return text
    if not text.startswith("/"):
        raise ValueError(
            "The mountpoint must be an absolute path, 'none' or 'legacy'."
        )
    return text


def validate_compatibility(value: str) -> str:
    """Validate a ``compatibility`` value against the installed files.

    ``off`` and ``legacy`` must stand alone. Otherwise every comma separated
    name must be a file listed by :func:`list_compatibility_files`, so a
    crafted value cannot point ``zpool`` at an arbitrary path.
    """
    names = []
    for part in value.split(","):
        if part.strip():
            names.append(part.strip())
    if not names:
        raise ValueError("A compatibility value is required.")

    for keyword in BUILTIN_COMPATIBILITY_VALUES:
        if keyword in names and len(names) > 1:
            raise ValueError(
                f"The compatibility value '{keyword}' cannot be combined with files."
            )

    known = set(BUILTIN_COMPATIBILITY_VALUES)
    for entry in list_compatibility_files():
        known.add(entry["name"])
    for name in names:
        if name not in known:
            raise ValueError(f"Unknown compatibility file '{name}'.")
    return ",".join(names)


def validate_device(device: str) -> str:
    """Validate one device token of the vdev specification.

    A token starting with '-' is refused. GNU getopt (Linux) permutes
    arguments, so such a token after the pool name would be parsed by
    ``zpool create`` as an option instead of a device.
    """
    text = validate_value(device, "device")
    if not text:
        raise ValueError("Empty device name.")
    if text.startswith("-"):
        raise ValueError(f"Invalid device name '{text}'.")
    return text


def validate_altroot(value: str) -> str:
    """Validate ``-R <altroot>``."""
    text = validate_value(value, "altroot")
    if not text:
        return text
    if not text.startswith("/"):
        raise ValueError("The alternate root must be an absolute path.")
    return text


def validate_draid_layout(
    layout: str, device_count: int, platform: Optional[str] = None
) -> str:
    """Validate a dRAID topology string.

    Accepted form: ``draid[1-3][:<data>d][:<children>c][:<spares>s]``.

    Rules enforced here (mirroring the ``zpool create`` constraints):

    * the pool must support dRAID (OpenZFS 2.1+, never on NetBSD);
    * parity defaults to 1 when omitted;
    * ``children``, when given, must equal the number of supplied devices;
    * there must be at least one data disk per redundancy group, i.e.
      ``children - spares >= parity + 1``;
    * ``data`` must fit: ``data + parity + spares <= children``.
    """
    platform = platform or current_platform()
    if platform not in OPENZFS_PLATFORMS or not getattr(caps, "draid", False):
        raise ValueError("dRAID is not supported on this platform.")
    text = validate_value(layout, "dRAID layout").lower()
    match = _DRAID_RE.fullmatch(text)
    if not match:
        raise ValueError(
            "Invalid dRAID layout. Use draid[1-3][:<data>d][:<children>c][:<spares>s], "
            "for example draid2:4d:8c:1s."
        )
    parity = int(match.group(1)) if match.group(1) else 1
    data = int(match.group(2)) if match.group(2) else None
    children = int(match.group(3)) if match.group(3) else None
    spares = int(match.group(4)) if match.group(4) else 0

    if device_count < parity + 1:
        raise ValueError(
            f"draid{parity} needs at least {parity + 1} devices; "
            f"{device_count} selected."
        )
    effective_children = children if children is not None else device_count
    if effective_children > DRAID_MAX_CHILDREN:
        raise ValueError(
            f"A dRAID vdev can have at most {DRAID_MAX_CHILDREN} children."
        )
    if children is not None and children != device_count:
        raise ValueError(
            f"The dRAID layout declares {children} children but "
            f"{device_count} devices were selected."
        )
    if spares > effective_children - parity - 1:
        raise ValueError(
            f"draid{parity} with {spares} spares needs at least "
            f"{parity + 1 + spares} children; {effective_children} available."
        )
    if data is not None:
        if data < 1:
            raise ValueError("A dRAID redundancy group needs at least one data disk.")
        if data + parity + spares > effective_children:
            raise ValueError(
                f"{data} data + {parity} parity + {spares} spares does not fit "
                f"in {effective_children} children."
            )
    return text


# ---------------------------------------------------------------------------
# form collection
# ---------------------------------------------------------------------------


def collect_root_properties(
    form: Mapping[str, Any], platform: Optional[str] = None
) -> Dict[str, str]:
    """Collect ``root_property_<name>`` form fields into a property dict."""
    platform = platform or current_platform()
    properties: Dict[str, str] = {}
    for spec in get_root_property_specs(platform):
        raw = form.get(f"root_property_{spec.name}")
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        properties[spec.name] = validate_property_value(spec.name, text, spec)
    return properties


def collect_pool_properties(
    form: Mapping[str, Any], platform: Optional[str] = None
) -> Dict[str, str]:
    """Collect ``pool_property_<name>`` form fields into a property dict."""
    platform = platform or current_platform()
    properties: Dict[str, str] = {}
    for spec in get_pool_property_specs(platform):
        raw = form.get(f"pool_property_{spec.name}")
        if raw is None:
            continue
        text = str(raw).strip()
        if not text:
            continue
        properties[spec.name] = validate_property_value(spec.name, text, spec)
    return properties


def validate_root_properties(properties: Mapping[str, str]) -> Dict[str, str]:
    """Validate an explicit root property dict (allowlist + values)."""
    specs = {spec.name: spec for spec in get_root_property_specs()}
    validated: Dict[str, str] = {}
    for key, value in properties.items():
        if key not in ALLOWED_ROOT_PROPERTY_KEYS:
            raise ValueError(f"Unsupported root dataset property '{key}'.")
        if key in ENCRYPTION_PROPERTY_KEYS and is_netbsd():
            raise ValueError("Encryption is not supported by NetBSD ZFS.")
        validated[key] = validate_property_value(key, value, specs.get(key))
    _validate_utf8_normalization(validated)
    return validated


def validate_pool_properties(properties: Mapping[str, str]) -> Dict[str, str]:
    """Validate an explicit pool property dict (allowlist + values)."""
    specs = {spec.name: spec for spec in get_pool_property_specs()}
    validated: Dict[str, str] = {}
    for key, value in properties.items():
        if key not in ALLOWED_POOL_PROPERTY_KEYS:
            raise ValueError(f"Unsupported pool property '{key}'.")
        validated[key] = validate_property_value(key, value, specs.get(key))
    return validated


def _validate_utf8_normalization(properties: Mapping[str, str]) -> None:
    normalization = properties.get("normalization")
    if normalization and normalization != "none":
        if properties.get("utf8only") == "off":
            raise ValueError(
                "utf8only cannot be off when normalization is enabled: "
                "normalization requires UTF-8 only file names."
            )


# ---------------------------------------------------------------------------
# vdev specification
# ---------------------------------------------------------------------------


def build_vdev_spec(
    vdev_type: str,
    devices: str,
    spare_devices: str = "",
    cache_devices: str = "",
    log_devices: str = "",
    log_layout: str = "mirror",
    metadata_devices: str = "",
    dedup_devices: str = "",
    draid_layout: str = "",
) -> List[str]:
    """Translate the create-form device fields into a ``zpool`` vdev list.

    Data groups are space separated and disks within a group comma separated
    (``vdev1disk1,vdev1disk2 vdev2disk1``). dRAID always uses a single vdev,
    so all selected data disks must be in one group.
    """
    if vdev_type not in DATA_VDEV_TYPES:
        raise ValueError(f"Unsupported vdev type '{vdev_type}'.")
    if log_layout not in LOG_LAYOUTS:
        raise ValueError(f"Unsupported log layout '{log_layout}'.")

    vdevs: List[str] = []
    groups = [group for group in devices.split() if group.strip()]

    if vdev_type.startswith("draid"):
        if len(groups) > 1:
            raise ValueError(
                "A dRAID vdev holds all data disks; put them in a single group."
            )
        data_devices = [
            validate_device(disk)
            for disk in (groups[0] if groups else "").split(",")
            if disk.strip()
        ]
        if not data_devices:
            raise ValueError("dRAID requires at least one data device.")
        layout = validate_draid_layout(draid_layout or vdev_type, len(data_devices))
        vdevs.append(layout)
        vdevs.extend(data_devices)
    else:
        for group in groups:
            device_list = [disk.strip() for disk in group.split(",") if disk.strip()]
            if not device_list:
                continue
            if vdev_type != "single":
                vdevs.append(vdev_type)
            vdevs.extend(device_list)

    if spare_devices:
        spare_list = [d.strip() for d in spare_devices.split(",") if d.strip()]
        if spare_list:
            vdevs.append("spare")
            vdevs.extend(spare_list)

    if cache_devices:
        cache_list = [d.strip() for d in cache_devices.split(",") if d.strip()]
        if cache_list:
            vdevs.append("cache")
            vdevs.extend(cache_list)

    if log_devices:
        log_list = [d.strip() for d in log_devices.split(",") if d.strip()]
        if log_list:
            vdevs.append("log")
            # Issue #15: the log layout is user-selectable instead of always
            # mirroring multiple devices. A single device cannot be striped.
            if log_layout == "mirror" and len(log_list) > 1:
                vdevs.append("mirror")
            vdevs.extend(log_list)

    if metadata_devices:
        for mirror_group in metadata_devices.split():
            device_list = [d.strip() for d in mirror_group.split(",") if d.strip()]
            if device_list:
                vdevs.append("special")
                if len(device_list) > 1:
                    vdevs.append("mirror")
                vdevs.extend(device_list)

    if dedup_devices:
        dedup_list = [d.strip() for d in dedup_devices.split(",") if d.strip()]
        if dedup_list:
            vdevs.append("dedup")
            if len(dedup_list) > 1:
                vdevs.append("mirror")
            vdevs.extend(dedup_list)

    if not vdevs:
        raise ValueError("At least one data device is required.")
    return vdevs


# ---------------------------------------------------------------------------
# command building
# ---------------------------------------------------------------------------


def build_create_command(
    pool_name: str,
    vdevs: Sequence[str],
    *,
    pool_properties: Optional[Mapping[str, str]] = None,
    root_properties: Optional[Mapping[str, str]] = None,
    mountpoint: str = "",
    altroot: str = "",
    force: bool = False,
    dry_run: bool = False,
) -> List[str]:
    """Build the ``zpool create`` argv for the requested configuration.

    Options are emitted in a stable order: ``-f``, ``-n``, pool properties
    (``-o``), root dataset properties (``-O``), ``-m``, ``-R``, then the pool
    name and the vdev specification.
    """
    name = validate_pool_name(pool_name)
    if not vdevs:
        raise ValueError("At least one vdev is required.")

    validated_pool = validate_pool_properties(pool_properties or {})
    validated_root = validate_root_properties(root_properties or {})
    mount = validate_mountpoint(mountpoint)
    alt = validate_altroot(altroot)

    cmd: List[str] = ["zpool", "create"]
    if force:
        cmd.append("-f")
    if dry_run:
        cmd.append("-n")
    for key, value in validated_pool.items():
        cmd.extend(["-o", f"{key}={value}"])
    for key, value in validated_root.items():
        cmd.extend(["-O", f"{key}={value}"])
    if mount:
        cmd.extend(["-m", mount])
    if alt:
        cmd.extend(["-R", alt])
    cmd.append(name)
    for vdev in vdevs:
        cmd.append(validate_device(vdev))
    return cmd


def format_command(cmd: Sequence[str]) -> str:
    """Return a shell-quoted, copy-pasteable rendering of ``cmd``."""
    return " ".join(shlex.quote(part) for part in cmd)
