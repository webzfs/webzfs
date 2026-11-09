"""Encryption key option helpers for ZFS datasets (issue #22).

This module centralises the validation and capability probing needed by the
dataset views when a dataset is created with, or switched to, a non-prompt
encryption key (``keyformat``/``keylocation``).

Key material itself is never read here. File-based keys are validated purely
from filesystem metadata (``stat``), so the service account never has to open
the key file. ``https://`` key locations depend on the libfetch/libcurl
back-end compiled into ``libzfs`` and available at run time; that is probed
once per process with :func:`detect_https_keylocation_support`.
"""

import ctypes
import os
import re
import shutil
import stat
import subprocess
from typing import Any, Dict, Iterable, Optional, Tuple

from services.utils import is_linux

KEYFORMAT_PASSPHRASE = "passphrase"
KEYFORMAT_HEX = "hex"
KEYFORMAT_RAW = "raw"
KEYFORMATS = (KEYFORMAT_PASSPHRASE, KEYFORMAT_HEX, KEYFORMAT_RAW)

KEYLOCATION_PROMPT = "prompt"
KEYLOCATION_FILE = "file"
KEYLOCATION_HTTPS = "https"
KEYLOCATION_TYPES = (KEYLOCATION_PROMPT, KEYLOCATION_FILE, KEYLOCATION_HTTPS)

#: File keys must be regular files owned by root with mode 0600.
KEY_FILE_OWNER_UID = 0
KEY_FILE_MODE = 0o600

_FILE_URI_RE = re.compile(r"^file://(/[^\x00]*)$")
_HTTPS_URI_RE = re.compile(r"^https://[^\s/]+(/[^\s]*)?$")
_LIBZFS_SONAME_PREFIX = "libzfs.so"
_HTTPS_BACKEND_SONAMES = ("libcurl.so.4", "libfetch.so.6")

_https_support_cache: Optional[bool] = None


def normalize_keyformat(value: Optional[str]) -> str:
    """Return a validated ``keyformat`` value."""
    keyformat = (value or KEYFORMAT_PASSPHRASE).strip().lower()
    if keyformat not in KEYFORMATS:
        raise ValueError(
            f"Unsupported key format '{value}'. "
            f"Expected one of: {', '.join(KEYFORMATS)}."
        )
    return keyformat


def build_keylocation(
    location_type: Optional[str],
    file_path: Optional[str] = None,
    url: Optional[str] = None,
) -> str:
    """Build a ``keylocation`` value from the form fields.

    Args:
        location_type: one of ``prompt``, ``file`` or ``https``.
        file_path: absolute path used when ``location_type`` is ``file``.
        url: full URL used when ``location_type`` is ``https``.
    """
    kind = (location_type or KEYLOCATION_PROMPT).strip().lower()
    if kind == KEYLOCATION_PROMPT:
        return KEYLOCATION_PROMPT
    if kind == KEYLOCATION_FILE:
        path = (file_path or "").strip()
        if not path:
            raise ValueError("A key file path is required for a file key location.")
        if not path.startswith("/"):
            raise ValueError(
                "The key file path must be absolute "
                "(for example /etc/zfs/keys/dataset.key)."
            )
        if "\x00" in path or "\n" in path or "\r" in path:
            raise ValueError("The key file path contains invalid characters.")
        return f"file://{path}"
    if kind == KEYLOCATION_HTTPS:
        value = (url or "").strip()
        if not value:
            raise ValueError("A URL is required for an https key location.")
        return validate_keylocation(value)
    raise ValueError(
        f"Unsupported key location '{location_type}'. "
        f"Expected one of: {', '.join(KEYLOCATION_TYPES)}."
    )


def validate_keylocation(value: str) -> str:
    """Validate a raw ``keylocation`` property value.

    Only ``prompt``, ``file:///absolute/path`` and ``https://host/path`` are
    accepted. ``http://`` is rejected: unencrypted transport of key material
    is not offered by the UI.
    """
    location = (value or "").strip()
    if location == KEYLOCATION_PROMPT:
        return location
    if location.startswith("http://"):
        raise ValueError(
            "Insecure http:// key locations are not supported; use https://."
        )
    if _FILE_URI_RE.match(location):
        path = location[len("file://"):]
        if not os.path.isabs(path):
            raise ValueError("A file key location must use an absolute path.")
        return location
    if _HTTPS_URI_RE.match(location):
        return location
    raise ValueError(
        f"Unsupported key location '{value}'. Expected 'prompt', "
        "'file:///absolute/path' or 'https://host/path'."
    )


def validate_key_combination(keyformat: str, keylocation: str) -> None:
    """Reject key format/location combinations the UI cannot service safely.

    A ``prompt`` location means the key material must be typed in or piped on
    stdin. ``hex`` and ``raw`` keys are binary material and are therefore only
    accepted from a file or an https location; only passphrases may use
    ``prompt``.
    """
    if keylocation == KEYLOCATION_PROMPT and keyformat != KEYFORMAT_PASSPHRASE:
        raise ValueError(
            f"A prompt key location requires the passphrase key format; "
            f"'{keyformat}' keys must use a file:// or https:// key location."
        )


def key_file_path_from_location(keylocation: str) -> Optional[str]:
    """Return the filesystem path for a ``file://`` key location, else None."""
    match = _FILE_URI_RE.match((keylocation or "").strip())
    return match.group(1) if match else None


def _stat_key_file(path: str) -> Tuple[int, int]:
    """Return ``(st_mode, st_uid)`` for ``path`` without opening the file.

    The web service account usually cannot traverse a root-only key directory,
    so on Linux a failed ``os.stat`` falls back to ``sudo stat`` (metadata
    only; the key content is never read).
    """
    try:
        info = os.stat(path)
        return info.st_mode, info.st_uid
    except PermissionError:
        pass
    except OSError as exc:
        raise ValueError(
            f"Unable to access the key file '{path}': {exc.strerror}"
        ) from exc

    if not is_linux():
        raise ValueError(
            f"Unable to access the key file '{path}': permission denied."
        )

    result = subprocess.run(
        ["sudo", "-n", "stat", "-L", "-c", "%f|%u|%a", path],
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        detail = (result.stderr or "").strip()
        raise ValueError(
            "Unable to verify the key file permissions without reading the "
            f"key: {detail or 'sudo stat failed'}"
        )

    fields = result.stdout.strip().split("|")
    if len(fields) != 3:
        raise ValueError(
            f"Unable to parse key file metadata for '{path}': {result.stdout!r}"
        )
    raw_mode, owner, mode = fields
    try:
        raw_mode_bits = int(raw_mode, 16)
        owner_uid = int(owner)
        mode_bits = int(mode, 8)
    except ValueError as exc:
        raise ValueError(
            f"Unable to parse key file metadata for '{path}': {result.stdout!r}"
        ) from exc

    if not stat.S_ISREG(raw_mode_bits):
        raise ValueError(f"The key file '{path}' is not a regular file.")

    return (stat.S_IFREG | mode_bits, owner_uid)


def validate_key_file(path: str) -> Dict[str, Any]:
    """Validate a file-based key without reading its contents.

    The file must exist, be a regular file, be owned by root and have mode
    0600. Returns a small metadata dict (never the key bytes).

    Raises:
        ValueError: if any of the requirements are not met.
    """
    if not path:
        raise ValueError("A key file path is required.")
    if not os.path.isabs(path):
        raise ValueError("The key file path must be absolute.")
    if "\x00" in path:
        raise ValueError("The key file path contains invalid characters.")

    mode, owner_uid = _stat_key_file(path)

    if not stat.S_ISREG(mode):
        raise ValueError(f"The key file '{path}' is not a regular file.")
    if owner_uid != KEY_FILE_OWNER_UID:
        raise ValueError(
            f"The key file '{path}' must be owned by root (uid 0); "
            f"found uid {owner_uid}."
        )
    permissions = stat.S_IMODE(mode)
    if permissions != KEY_FILE_MODE:
        raise ValueError(
            f"The key file '{path}' must have mode 0600; "
            f"found {permissions:04o}."
        )

    return {"path": path, "owner_uid": owner_uid, "mode": f"{permissions:04o}"}


def _find_libzfs_path(zfs_binary: str) -> Optional[str]:
    """Resolve the ``libzfs`` shared object the ``zfs`` binary links against."""
    try:
        result = subprocess.run(
            ["ldd", zfs_binary],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None

    for line in result.stdout.splitlines():
        tokens = line.split()
        if not tokens or not tokens[0].startswith(_LIBZFS_SONAME_PREFIX):
            continue
        if "=>" in tokens:
            index = tokens.index("=>")
            if index + 1 < len(tokens):
                candidate = tokens[index + 1]
                if os.path.isabs(candidate) and os.path.exists(candidate):
                    return candidate
        elif len(tokens) > 1 and os.path.isabs(tokens[1]):
            # FreeBSD ldd prints "libzfs.so.7 => /lib/libzfs.so.7".
            return tokens[1]
    return None


def _probe_https_keylocation_support() -> bool:
    """Return True when libzfs can fetch key material over https.

    libzfs dlopen()s a libfetch/libcurl soname compiled into the library
    (``LIBFETCH_SONAME``). When OpenZFS was built without a back-end the
    soname is absent and ``zfs load-key`` reports ENOSYS. We mirror that:
    look for the compiled-in soname in libzfs and then verify the library can
    actually be loaded.
    """
    zfs_binary = shutil.which("zfs")
    if not zfs_binary:
        return False

    libzfs_path = _find_libzfs_path(zfs_binary)
    if not libzfs_path:
        return False

    try:
        with open(libzfs_path, "rb") as handle:
            libzfs_bytes = handle.read()
    except OSError:
        return False

    soname = next(
        (
            candidate
            for candidate in _HTTPS_BACKEND_SONAMES
            if candidate.encode("ascii") in libzfs_bytes
        ),
        None,
    )
    if soname is None:
        return False

    try:
        ctypes.CDLL(soname, mode=getattr(ctypes, "RTLD_LAZY", 0))
    except OSError:
        return False
    return True


def detect_https_keylocation_support(force: bool = False) -> bool:
    """Return (and cache) whether ``https://`` key locations work here."""
    global _https_support_cache
    if force or _https_support_cache is None:
        _https_support_cache = _probe_https_keylocation_support()
    return _https_support_cache


def reset_https_keylocation_support_cache() -> None:
    """Clear the cached https capability probe (used by tests)."""
    global _https_support_cache
    _https_support_cache = None


def redact_key_material(
    text: Optional[str], secrets: Iterable[Optional[str]] = ()
) -> str:
    """Strip key material from a message before it is logged or shown.

    Any supplied secret values are replaced verbatim. As a defence in depth,
    ``passphrase=...`` fragments echoed by a tool are redacted too.
    """
    if not text:
        return ""
    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[redacted]")
    redacted = re.sub(
        r"(?i)\bpassphrase\s*[=:]\s*\S+", "passphrase=[redacted]", redacted
    )
    return redacted
