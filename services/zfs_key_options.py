"""Encryption key option helpers for ZFS datasets (issue #22).

This module centralises the validation and capability probing needed by the
dataset views when a dataset is created with, or switched to, a non-prompt
encryption key (``keyformat``/``keylocation``).

Key material is never read back here. File-based keys are validated purely
from filesystem metadata (``stat``), so the service account never has to open
the key file. New key files (random or from a user string) are written once
through stdin and are never logged or returned. ``https://`` key locations depend on the libfetch/libcurl
back-end compiled into ``libzfs`` and available at run time; that is probed
once per process with :func:`detect_https_keylocation_support`.
"""

import ctypes
import os
import re
import secrets
import shutil
import stat
import subprocess
from typing import Any, Dict, Iterable, Optional, Tuple

from services.utils import needs_sudo_for_privileged

KEYFORMAT_PASSPHRASE = "passphrase"
KEYFORMAT_HEX = "hex"
KEYFORMAT_RAW = "raw"
KEYFORMATS = (KEYFORMAT_PASSPHRASE, KEYFORMAT_HEX, KEYFORMAT_RAW)

KEYLOCATION_PROMPT = "prompt"
KEYLOCATION_FILE = "file"
KEYLOCATION_HTTPS = "https"
KEYLOCATION_TYPES = (KEYLOCATION_PROMPT, KEYLOCATION_FILE, KEYLOCATION_HTTPS)

#: How the key file is obtained when the key location is a file.
KEY_SOURCE_EXISTING = "existing"
KEY_SOURCE_GENERATE = "generate"
KEY_SOURCE_STRING = "string"
KEY_SOURCES = (KEY_SOURCE_EXISTING, KEY_SOURCE_GENERATE, KEY_SOURCE_STRING)

#: File keys must be regular files owned by root with mode 0600.
KEY_FILE_OWNER_UID = 0
KEY_FILE_MODE = 0o600

#: OpenZFS key sizes: raw keys are 32 bytes, hex keys are 64 hex digits,
#: passphrases are 8 to 512 bytes.
RAW_KEY_BYTES = 32
HEX_KEY_LENGTH = 64
PASSPHRASE_MIN_BYTES = 8
PASSPHRASE_MAX_BYTES = 512

KEY_HELPER_TIMEOUT_SECONDS = 10

# Always used with fullmatch(): a trailing '$' with match() would accept a
# final newline.
_FILE_URI_RE = re.compile(r"file://(/[^\x00\n\r]*)")
_HTTPS_URI_RE = re.compile(r"https://[^\s/]+(/[^\s]*)?")
_HEX_KEY_RE = re.compile(r"[0-9a-fA-F]{64}")
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
        return validate_keylocation(f"file://{path}")
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
    if _FILE_URI_RE.fullmatch(location):
        return location
    if _HTTPS_URI_RE.fullmatch(location):
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
    match = _FILE_URI_RE.fullmatch((keylocation or "").strip())
    return match.group(1) if match else None


def _run_key_helper(
    cmd: list, input_bytes: Optional[bytes] = None
) -> subprocess.CompletedProcess:
    """Run a key file helper command, through ``sudo -n`` when required.

    Used for ``stat``, ``mkdir``, ``tee`` and ``chmod`` on Linux, where the
    service account is not root. ``-n`` makes sudo fail instead of waiting
    for a password. Output is returned as bytes and never logged.
    """
    full_cmd = list(cmd)
    if needs_sudo_for_privileged():
        full_cmd = ["sudo", "-n"] + full_cmd
    return subprocess.run(
        full_cmd,
        input=input_bytes,
        capture_output=True,
        timeout=KEY_HELPER_TIMEOUT_SECONDS,
    )


def _helper_error(result: subprocess.CompletedProcess) -> str:
    """Return the stderr of a helper command as text (stdout is ignored)."""
    return (result.stderr or b"").decode("utf-8", "replace").strip()


def _stat_path(path: str, follow_symlinks: bool = True) -> Optional[Tuple[int, int]]:
    """Return ``(st_mode, st_uid)`` for ``path``, or None if it does not exist.

    The file is never opened. The web service account usually cannot
    traverse a root-only key directory, so on Linux a permission error falls
    back to ``sudo stat`` (metadata only; the key content is never read).
    """
    try:
        info = os.stat(path, follow_symlinks=follow_symlinks)
        return info.st_mode, info.st_uid
    except FileNotFoundError:
        return None
    except PermissionError:
        pass
    except OSError as exc:
        raise ValueError(f"Unable to access '{path}': {exc.strerror}") from exc

    if not needs_sudo_for_privileged():
        raise ValueError(f"Unable to access '{path}': permission denied.")

    cmd = ["stat"]
    if follow_symlinks:
        cmd.append("-L")
    cmd.extend(["-c", "%f|%u", path])
    result = _run_key_helper(cmd)
    if result.returncode != 0:
        detail = _helper_error(result)
        if "no such file" in detail.lower():
            return None
        raise ValueError(
            "Unable to verify the key file permissions without reading the "
            f"key: {detail or 'sudo stat failed'}"
        )

    output = result.stdout.decode("utf-8", "replace").strip()
    fields = output.split("|")
    if len(fields) != 2:
        raise ValueError(f"Unable to parse file metadata for '{path}': {output!r}")
    try:
        return int(fields[0], 16), int(fields[1])
    except ValueError as exc:
        raise ValueError(
            f"Unable to parse file metadata for '{path}': {output!r}"
        ) from exc


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

    metadata = _stat_path(path)
    if metadata is None:
        raise ValueError(f"The key file '{path}' does not exist.")
    mode, owner_uid = metadata

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


def normalize_key_source(value: Optional[str]) -> str:
    """Return a validated key file source (existing, generate or string)."""
    source = (value or KEY_SOURCE_EXISTING).strip().lower()
    if source not in KEY_SOURCES:
        raise ValueError(
            f"Unsupported key file source '{value}'. "
            f"Expected one of: {', '.join(KEY_SOURCES)}."
        )
    return source


def build_key_material(
    keyformat: str, source: str, key_string: Optional[str] = None
) -> bytes:
    """Return the bytes to write into a new key file.

    ``generate`` produces random material in the requested format: 32 random
    bytes for ``raw``, 64 hex digits for ``hex`` and a 43 character random
    passphrase for ``passphrase``. ``string`` uses the text the user typed,
    checked against the size OpenZFS requires for the format. Error messages
    never contain the key material.
    """
    keyformat = normalize_keyformat(keyformat)

    if source == KEY_SOURCE_GENERATE:
        if keyformat == KEYFORMAT_RAW:
            return secrets.token_bytes(RAW_KEY_BYTES)
        if keyformat == KEYFORMAT_HEX:
            return secrets.token_hex(RAW_KEY_BYTES).encode("ascii")
        return secrets.token_urlsafe(32).encode("ascii")

    if source != KEY_SOURCE_STRING:
        raise ValueError("Key material is only built for generated or typed keys.")

    value = key_string or ""
    if keyformat == KEYFORMAT_RAW:
        data = value.encode("utf-8")
        if len(data) != RAW_KEY_BYTES:
            raise ValueError(
                f"A raw key must be exactly {RAW_KEY_BYTES} bytes; "
                f"the text entered is {len(data)} bytes."
            )
        return data
    if keyformat == KEYFORMAT_HEX:
        value = value.strip()
        if not _HEX_KEY_RE.fullmatch(value):
            raise ValueError(
                f"A hex key must be exactly {HEX_KEY_LENGTH} hexadecimal characters."
            )
        return value.lower().encode("ascii")
    data = value.encode("utf-8")
    if len(data) < PASSPHRASE_MIN_BYTES or len(data) > PASSPHRASE_MAX_BYTES:
        raise ValueError(
            f"A passphrase must be {PASSPHRASE_MIN_BYTES} to "
            f"{PASSPHRASE_MAX_BYTES} bytes long."
        )
    return data


def _check_key_directory(directory: str) -> None:
    """Require a root-owned directory that other users cannot enter.

    A key file is written into this directory before its mode is tightened
    to 0600, so the directory itself must keep other users out.
    """
    metadata = _stat_path(directory)
    if metadata is None:
        raise ValueError(f"The key directory '{directory}' does not exist.")
    mode, owner_uid = metadata
    if not stat.S_ISDIR(mode):
        raise ValueError(f"'{directory}' is not a directory.")
    if owner_uid != KEY_FILE_OWNER_UID:
        raise ValueError(
            f"The key directory '{directory}' must be owned by root (uid 0); "
            f"found uid {owner_uid}."
        )
    if stat.S_IMODE(mode) & 0o077:
        raise ValueError(
            f"The key directory '{directory}' must not be accessible by group "
            f"or other users (found mode {stat.S_IMODE(mode):04o}). Use a "
            "directory with mode 0700, such as /etc/zfs/keys."
        )


def _make_key_directory(directory: str) -> None:
    """Create a missing key directory with mode 0700."""
    if needs_sudo_for_privileged():
        result = _run_key_helper(["mkdir", "-p", "-m", "0700", directory])
        if result.returncode != 0:
            raise ValueError(
                f"Unable to create the key directory '{directory}': "
                f"{_helper_error(result) or 'mkdir failed'}"
            )
        return
    os.makedirs(directory, mode=0o700)


def _write_key_bytes(path: str, material: bytes) -> None:
    """Write key material to a new file with mode 0600.

    Linux (service account): ``sudo tee`` receives the key on stdin, so it is
    never in argv, then ``sudo chmod 0600``. Running as root (FreeBSD,
    NetBSD): the file is created exclusively with mode 0600.
    """
    if needs_sudo_for_privileged():
        result = _run_key_helper(["tee", path], input_bytes=material)
        if result.returncode != 0:
            raise ValueError(
                f"Unable to write the key file '{path}': "
                f"{_helper_error(result) or 'tee failed'}"
            )
        result = _run_key_helper(["chmod", "0600", path])
        if result.returncode != 0:
            raise ValueError(
                f"Unable to set mode 0600 on the key file '{path}': "
                f"{_helper_error(result) or 'chmod failed'}"
            )
        return

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags, KEY_FILE_MODE)
    with os.fdopen(fd, "wb") as handle:
        handle.write(material)


def create_key_file(path: str, material: bytes) -> Dict[str, Any]:
    """Create a new root-owned, mode 0600 key file.

    The path must be absolute and normalised, the file must not already
    exist (an existing key file is never overwritten), and the parent
    directory must be owned by root with no group or other access. A missing
    parent directory is created with mode 0700. Returns the same metadata as
    :func:`validate_key_file`; the key bytes are never returned or logged.
    """
    if not path or not os.path.isabs(path):
        raise ValueError("The key file path must be absolute.")
    if "\x00" in path or "\n" in path or "\r" in path:
        raise ValueError("The key file path contains invalid characters.")
    if os.path.normpath(path) != path:
        raise ValueError(
            "The key file path must be a normalised file path without '..', "
            "'.' or a trailing '/'."
        )
    if not material:
        raise ValueError("No key material to write.")

    directory = os.path.dirname(path)
    if _stat_path(directory) is None:
        _make_key_directory(directory)
    _check_key_directory(directory)

    if _stat_path(path, follow_symlinks=False) is not None:
        raise ValueError(
            f"'{path}' already exists. WebZFS never overwrites an existing "
            "key file; choose a new file name."
        )

    _write_key_bytes(path, material)
    return validate_key_file(path)


def describe_keylocation_for_log(keylocation: Optional[str]) -> str:
    """Return a key location that is safe to write to the audit log.

    ``prompt`` and ``file://`` locations are logged as is. For ``https://``
    only the scheme and host are kept, because a query string may carry an
    access token.
    """
    location = (keylocation or "").strip()
    if location.startswith("https://"):
        host = location[len("https://"):].split("/", 1)[0]
        host = host.split("?", 1)[0].split("#", 1)[0]
        if "@" in host:
            host = host.rsplit("@", 1)[1]
        return f"https://{host}"
    return location


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
