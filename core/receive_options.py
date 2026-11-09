"""Receive-option handling for ``zfs receive`` (issue #23).

The native send/receive path exposes the receive-side flags that decide
where a received dataset lands and which properties it carries:

* ``-u``      do not mount the received datasets
* ``-o prop=value``  override one dataset property on the received data
* ``-x prop``  strip one property from the stream so it is inherited
* ``-d`` / ``-e``  target-name handling for recursive streams

This module owns the allowlist, the parsing/validation rules and the
argv builder so the service, the views and the Syncoid job wrapper all
agree on what is legal.  The rules follow ``zfs-receive(8)`` and
``zfsprops(7)`` from OpenZFS 2.4.4:

* only writable dataset properties may be overridden; read-only and
  set-once properties (``normalization``, ``casesensitivity``,
  ``version``, ``volsize`` ...) are rejected by ZFS at receive time;
* a property may not be given with both ``-o`` and ``-x``, and may not
  be repeated within either list;
* ``encryption``, ``keyformat`` and ``pbkdf2iters`` cannot be set or
  excluded when the stream is raw (``zfs send -w``), because the stream
  already carries the encryption settings, and cannot be set on an
  incremental stream;
* ``-d`` and ``-e`` are mutually exclusive;
* ``keylocation`` is exempt from the encryption restrictions, but
  ``keylocation=prompt`` is still rejected because the receive process
  already owns stdin;
* property values are restricted to an allowlisted character class (no
  whitespace or shell syntax) because Syncoid interpolates
  ``--recvoptions`` unquoted into a shell command and ssh hands the
  remote receive argv to the remote shell.

M6's dataset-property catalog (issue #20) has not landed on ``main``
yet, so the allowlist below is the WebZFS receive catalog.  When the
M6 catalog lands, ``RECEIVE_PROPERTIES`` can be derived from it.
"""
from __future__ import annotations

import re

from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

__all__ = [
    "RECEIVE_PROPERTIES",
    "ReceiveOptionsError",
    "build_receive_flags",
    "normalize_receive_options",
    "parse_excluded_properties",
    "parse_property_overrides",
    "recvoptions_string",
    "validate_receive_options",
]


class ReceiveOptionsError(ValueError):
    """Raised when a receive option is malformed or not allowed."""


# Dataset properties that ZFS accepts for ``zfs receive -o``/``-x``.
# Read-only, set-once and internal properties are deliberately absent.
RECEIVE_PROPERTIES = frozenset(
    {
        "aclinherit",
        "aclmode",
        "acltype",
        "atime",
        "canmount",
        "checksum",
        "compression",
        "context",
        "copies",
        "dedup",
        "devices",
        "direct",
        "dnodesize",
        "exec",
        "filesystem_limit",
        "logbias",
        "longname",
        "mountpoint",
        "nbmand",
        "overlay",
        "primarycache",
        "quota",
        "readonly",
        "recordsize",
        "redundant_metadata",
        "refquota",
        "refreservation",
        "relatime",
        "reservation",
        "secondarycache",
        "setuid",
        "sharenfs",
        "sharesmb",
        "snapdir",
        "snapshot_limit",
        "special_small_blocks",
        "sync",
        "volmode",
        "vscan",
        "xattr",
        "zoned",
        # Encryption properties are writable when creating a dataset and
        # are accepted for non-raw receives.  They are rejected for raw
        # receives by validate_receive_options().
        "encryption",
        "keyformat",
        "keylocation",
        "pbkdf2iters",
    }
)

# Properties that accept only a fixed set of values.  ZFS enforces these
# too; validating here turns a failed receive into an immediate form
# error with a readable message.
# ZFS boolean properties accept exactly "on" and "off" (zfs_prop.c
# boolean_table).  ``snapdir`` is not boolean (hidden/visible).
_BOOLEAN_PROPERTIES = frozenset(
    {
        "atime",
        "devices",
        "exec",
        "longname",
        "nbmand",
        "overlay",
        "readonly",
        "relatime",
        "setuid",
        "vscan",
        "zoned",
    }
)
_BOOLEAN_VALUES = frozenset({"on", "off"})
_CANMOUNT_VALUES = frozenset({"on", "off", "noauto"})
_ENCRYPTION_VALUES = frozenset(
    {
        "on",
        "off",
        "aes-128-ccm",
        "aes-192-ccm",
        "aes-256-ccm",
        "aes-128-gcm",
        "aes-192-gcm",
        "aes-256-gcm",
    }
)
_KEYFORMAT_VALUES = frozenset({"passphrase", "raw", "hex"})
_VOLMODE_VALUES = frozenset({"default", "full", "dev", "none"})

# Encryption properties that libzfs refuses to set or exclude on a raw
# stream, and refuses to set on an incremental stream
# (zfs_setup_cmdline_props).  keylocation is exempt from both rules.
_ENCRYPTION_STREAM_PROPERTIES = frozenset({"encryption", "keyformat", "pbkdf2iters"})

_PROPERTY_NAME_RE = re.compile(r"[A-Za-z][A-Za-z0-9_.:-]*")
#: User-defined dataset properties (``zfsprops(7)``: ``user:*`` and
#: ``com.*``) are legal ``-o``/``-x`` targets and cannot be enumerated in
#: the catalog.
_CUSTOM_PROPERTY_RE = re.compile(r"(?:user:|com\.)[A-Za-z0-9][A-Za-z0-9_.:-]*")

#: Allowed characters in a property value.  This is an allowlist, not a
#: denylist: the same value is passed through a shell twice over (Syncoid
#: interpolates ``--recvoptions`` unquoted into the command it runs, and
#: ssh hands the remote receive argv to the remote shell), so anything
#: beyond plain words, paths and URLs (whitespace, quotes, globs, braces,
#: tildes, ``$``, ``;`` ...) is refused on every path.
_PROPERTY_VALUE_RE = re.compile(r"[A-Za-z0-9_.:/@+%=-]+")


def _as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _split_entries(raw: Any) -> List[str]:
    """Split a form value into individual entries.

    Accepts ``None``, a single string (newline- or comma-separated), or
    an iterable of strings/dicts.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return [f"{key}={value}" for key, value in raw.items()]
    if isinstance(raw, (list, tuple, set)):
        entries: List[str] = []
        for item in raw:
            if isinstance(item, Mapping):
                entries.extend(f"{key}={value}" for key, value in item.items())
            else:
                entries.extend(_split_entries(item))
        return entries
    text = _as_text(raw)
    entries = []
    for chunk in re.split(r"[\n,]", text):
        chunk = chunk.strip()
        if chunk:
            entries.append(chunk)
    return entries


def _validate_property_name(name: str) -> str:
    name = name.strip()
    if not name:
        raise ReceiveOptionsError("Property name cannot be empty.")
    if not _PROPERTY_NAME_RE.fullmatch(name):
        raise ReceiveOptionsError(f"Invalid property name: {name!r}.")
    if name.startswith(("user:", "com.")):
        # User-defined properties are not enumerable, so they bypass the
        # catalog check; the shape is validated above.
        if not _CUSTOM_PROPERTY_RE.fullmatch(name):
            raise ReceiveOptionsError(
                f"Invalid user property name: {name!r}; use user:<name> or "
                "com.<vendor>:<name>."
            )
        return name
    if name not in RECEIVE_PROPERTIES:
        raise ReceiveOptionsError(
            f"Property {name!r} is not in the WebZFS receive property catalog "
            "and cannot be overridden with -o/-x. Read-only and set-once "
            "properties cannot be changed at receive time."
        )
    return name


def _validate_property_value(name: str, value: str) -> str:
    if not value:
        raise ReceiveOptionsError(f"Property {name!r} requires a value.")
    if not _PROPERTY_VALUE_RE.fullmatch(value):
        raise ReceiveOptionsError(
            f"Property {name!r} has an invalid value {value!r}: only "
            "letters, digits and the characters _ . : / @ + % = - are "
            "allowed, because receive options pass through a shell for "
            "remote and Syncoid receives."
        )
    if name == "keylocation" and value == "prompt":
        raise ReceiveOptionsError(
            "Property 'keylocation' cannot be set to 'prompt' at receive "
            "time: zfs receive already uses standard input for the stream. "
            "Set it after the receive completes."
        )
    if name in _BOOLEAN_PROPERTIES and value not in _BOOLEAN_VALUES:
        raise ReceiveOptionsError(
            f"Property {name!r} accepts on/off (got {value!r})."
        )
    if name == "canmount" and value not in _CANMOUNT_VALUES:
        raise ReceiveOptionsError(
            f"Property 'canmount' accepts on/off/noauto (got {value!r})."
        )
    if name == "encryption" and value not in _ENCRYPTION_VALUES:
        raise ReceiveOptionsError(
            "Property 'encryption' accepts on/off or a cipher such as "
            f"aes-256-gcm (got {value!r})."
        )
    if name == "keyformat" and value not in _KEYFORMAT_VALUES:
        raise ReceiveOptionsError(
            f"Property 'keyformat' accepts passphrase/raw/hex (got {value!r})."
        )
    if name == "mountpoint":
        if value not in {"none", "legacy"} and not value.startswith("/"):
            raise ReceiveOptionsError(
                "Property 'mountpoint' must be an absolute path, 'none' or "
                f"'legacy' (got {value!r})."
            )
    if name == "volmode" and value not in _VOLMODE_VALUES:
        raise ReceiveOptionsError(
            f"Property 'volmode' accepts default/full/dev/none (got {value!r})."
        )
    return value


def parse_property_overrides(raw: Any) -> Dict[str, str]:
    """Parse ``-o`` overrides from a form value into an ordered dict."""
    overrides: Dict[str, str] = {}
    for entry in _split_entries(raw):
        if "=" not in entry:
            raise ReceiveOptionsError(
                f"Receive override {entry!r} must use the form property=value."
            )
        name, value = entry.split("=", 1)
        name = _validate_property_name(name)
        value = _validate_property_value(name, value.strip())
        if name in overrides:
            raise ReceiveOptionsError(
                f"Property {name!r} was given more than once in the -o list."
            )
        overrides[name] = value
    return overrides


def parse_excluded_properties(raw: Any) -> List[str]:
    """Parse ``-x`` property names from a form value."""
    excludes: List[str] = []
    if raw is None:
        return excludes
    if isinstance(raw, (list, tuple, set)):
        items = [str(item) for item in raw]
    else:
        text = _as_text(raw)
        items = [chunk for chunk in re.split(r"[\s,]+", text) if chunk]
    for item in items:
        name = _validate_property_name(item)
        if name in excludes:
            raise ReceiveOptionsError(
                f"Property {name!r} was given more than once in the -x list."
            )
        excludes.append(name)
    return excludes


def validate_receive_options(
    overrides: Any = None,
    excludes: Any = None,
    *,
    raw: bool = False,
    incremental: bool = False,
    allow_encryption: bool = True,
    discard_first_component: bool = False,
    only_last_component: bool = False,
) -> Tuple[Dict[str, str], List[str]]:
    """Validate a receive-option set and return normalised values.

    Args:
        overrides: ``-o`` entries (dict, string or list).
        excludes: ``-x`` entries (string or list).
        raw: True when the send stream is a raw stream (``zfs send -w``).
            Encryption overrides and excludes are rejected because the
            stream already carries the encryption settings.
        incremental: True for an incremental stream, which cannot set
            encryption properties.
        allow_encryption: False for callers that cannot know whether the
            stream is raw or incremental (scheduled Syncoid jobs);
            encryption overrides and excludes are rejected there.
        discard_first_component: ``-d`` requested.
        only_last_component: ``-e`` requested.

    Returns:
        ``(overrides, excludes)`` as an ordered dict and a list.

    Raises:
        ReceiveOptionsError: on any invalid or conflicting option.
    """
    parsed_overrides = parse_property_overrides(overrides)
    parsed_excludes = parse_excluded_properties(excludes)

    overlap = sorted(set(parsed_overrides) & set(parsed_excludes))
    if overlap:
        raise ReceiveOptionsError(
            "Property "
            + ", ".join(repr(name) for name in overlap)
            + " was given with both -o and -x; zfs receive rejects that."
        )

    if discard_first_component and only_last_component:
        raise ReceiveOptionsError(
            "The -d (discard first component) and -e (only last component) "
            "target-name options are mutually exclusive."
        )

    # libzfs (zfs_setup_cmdline_props) refuses encryption, keyformat and
    # pbkdf2iters in -o or -x on a raw stream, and in -o on an incremental
    # stream.  keylocation is exempt from both rules.  encryptionroot and
    # keystatus are read-only and already rejected by the catalog.
    set_blocked = sorted(set(parsed_overrides) & _ENCRYPTION_STREAM_PROPERTIES)
    excluded_blocked = sorted(set(parsed_excludes) & _ENCRYPTION_STREAM_PROPERTIES)
    if not allow_encryption and (set_blocked or excluded_blocked):
        raise ReceiveOptionsError(
            "Encryption properties ("
            + ", ".join(set_blocked + excluded_blocked)
            + ") cannot be overridden or excluded for scheduled Syncoid "
            "jobs; Syncoid controls the encryption handling of the stream."
        )
    if raw and (set_blocked or excluded_blocked):
        raise ReceiveOptionsError(
            "Encryption properties ("
            + ", ".join(set_blocked + excluded_blocked)
            + ") cannot be overridden or excluded on a raw (-w) receive: a "
            "raw stream already carries its encryption settings from the "
            "source."
        )
    if incremental and set_blocked:
        raise ReceiveOptionsError(
            "Encryption properties ("
            + ", ".join(set_blocked)
            + ") can only be set on a full receive; an incremental receive "
            "keeps the encryption settings of the existing target."
        )

    return parsed_overrides, parsed_excludes


def build_receive_flags(
    *,
    unmounted: bool = True,
    overrides: Optional[Mapping[str, str]] = None,
    excludes: Optional[Iterable[str]] = None,
    discard_first_component: bool = False,
    only_last_component: bool = False,
) -> List[str]:
    """Build the receive-side argv flags (without ``zfs receive`` itself)."""
    flags: List[str] = []
    if unmounted:
        flags.append("-u")
    if discard_first_component:
        flags.append("-d")
    if only_last_component:
        flags.append("-e")
    for name, value in (overrides or {}).items():
        flags.extend(["-o", f"{name}={value}"])
    for name in excludes or []:
        flags.extend(["-x", name])
    return flags


def normalize_receive_options(
    *,
    unmounted: bool = True,
    overrides: Optional[Mapping[str, str]] = None,
    excludes: Optional[Iterable[str]] = None,
    discard_first_component: bool = False,
    only_last_component: bool = False,
) -> Dict[str, Any]:
    """Return the JSON-safe form stored with execution and job records."""
    return {
        "unmounted": bool(unmounted),
        "overrides": dict(overrides or {}),
        "excludes": list(excludes or []),
        "target_mode": (
            "discard"
            if discard_first_component
            else "only_last"
            if only_last_component
            else "default"
        ),
    }


def recvoptions_string(options: Optional[Mapping[str, Any]]) -> Optional[str]:
    """Render stored receive options as Syncoid's ``--recvoptions`` value.

    Syncoid parses ``--recvoptions`` by splitting on single spaces and
    only honours the ``h``/``o``/``x``/``u``/``v`` option letters, so
    ``-d``/``-e`` would be silently dropped and are rejected here.  The
    value allowlist already excludes whitespace, so every value survives
    Syncoid's split intact.

    Returns ``None`` when there is nothing to pass.
    """
    if not options:
        return None
    target_mode = options.get("target_mode") or "default"
    if target_mode != "default":
        raise ReceiveOptionsError(
            "Syncoid --recvoptions does not support the -d/-e target-name "
            "options; only -u, -o and -x are honoured."
        )
    # Re-validate every stored value before rendering: job records saved
    # before the validation rules existed (or edited straight in storage)
    # must not be able to reach Syncoid's shell command.
    overrides: Dict[str, str] = {}
    for name, value in (options.get("overrides") or {}).items():
        checked_name = _validate_property_name(_as_text(name))
        overrides[checked_name] = _validate_property_value(
            checked_name, _as_text(value)
        )
    excludes = [
        _validate_property_name(_as_text(name))
        for name in (options.get("excludes") or [])
    ]
    flags = build_receive_flags(
        unmounted=bool(options.get("unmounted", True)),
        overrides=overrides,
        excludes=excludes,
    )
    if not flags:
        return None
    return " ".join(flags)
