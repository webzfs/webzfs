"""
Backup and Restore Service for WebZFS Configuration

Exports every WebZFS configuration file, SSH key, and managed system file into
a single encrypted archive that can be downloaded and later imported on the
same or a freshly installed system.

Encryption: AES-256-GCM with a key derived via PBKDF2-HMAC-SHA256
(600,000 iterations). The plaintext header (magic + JSON metadata) is used as
GCM Additional Authenticated Data so any modification of the header invalidates
the archive.

File format:
    line 1   : "WZFSBAK1\\n"                       9-byte magic
    line 2   : "<single-line JSON header>\\n"      KDF parameters and metadata
    rest     : AES-256-GCM ciphertext bytes        encrypts a gzipped tar

Tar payload:
    manifest.json
    config/<file>                                  user config files
    ssh-keys/<file>                                SSH key pair files
    system/sanoid/sanoid.conf                      system config (optional)
    system/cron.d/<file>                           cron files (optional)
    env/.env                                       application secret (optional)
"""
import base64
import gzip
import hashlib
import io
import json
import logging
import os
import platform
import shutil
import socket
import tarfile
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from services.utils import is_bsd, run_privileged_command


logger = logging.getLogger(__name__)


MAGIC = b"WZFSBAK1\n"
FORMAT_VERSION = 1
KDF_NAME = "pbkdf2-sha256"
KDF_ITERATIONS = 600_000
KDF_SALT_BYTES = 16
AES_KEY_BYTES = 32
GCM_NONCE_BYTES = 12
APP_VERSION = "0.70"

# Categories used in manifest entries and restore selection.
CATEGORY_CONFIG = "config"
CATEGORY_SSH_KEY = "ssh-key"
CATEGORY_SANOID = "sanoid"
CATEGORY_CRON = "cron"
CATEGORY_ENV = "env"


def _app_root() -> Path:
    """Return the WebZFS application root directory."""
    return Path(__file__).resolve().parent.parent


def _user_config_dir() -> Path:
    """Return the per-user WebZFS config directory."""
    return Path.home() / ".config" / "webzfs"


def _ssh_keys_dir() -> Path:
    """Return the directory holding webzfs-managed SSH keys."""
    return Path.home() / ".ssh" / "webzfs_connections"


def _sanoid_config_path() -> Path:
    """Return the platform-appropriate sanoid.conf path."""
    if is_bsd():
        return Path("/usr/local/etc/sanoid/sanoid.conf")
    return Path("/etc/sanoid/sanoid.conf")


# Files we recognise inside the user config directory. Each entry is the
# basename. Anything not listed here is left alone on restore.
USER_CONFIG_FILES = [
    "theme.conf",
    "session_timeout.json",
    "ssh_connections.json",
    "fleet_servers.json",
    ".fleet_key",
    "scrub_schedules.json",
    "smart_scheduled_tests.json",
    "syncoid_jobs.json",
    "replication_history.json",
    "smart_test_history.json",
    "notification_log.json",
    "health_reports.json",
]

# Files in the user config dir that are considered "history" rather than
# active configuration. Off by default.
HISTORY_FILES = {
    "replication_history.json",
    "smart_test_history.json",
    "notification_log.json",
    "health_reports.json",
}

# WebZFS-managed cron.d filenames. Other files in /etc/cron.d are not touched.
WEBZFS_CRON_FILES = [
    "syncoid-replication",
]


def _sha256_file(path: Path) -> str:
    """Return the lowercase hex SHA-256 of the file at path."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _read_root_file(path: Path) -> Optional[bytes]:
    """
    Read a file that may require root privileges.

    Returns:
        File bytes, or None if the file does not exist or cannot be read.
    """
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except PermissionError:
        # Fall back to sudo cat on Linux. On BSD we expect to be running
        # as root, so this branch is unlikely to fire there.
        try:
            result = run_privileged_command(
                ["cat", str(path)],
                check=True,
                text=False,
                capture_output=True,
                timeout=15,
            )
            return result.stdout
        except Exception as exc:  # pragma: no cover - depends on platform
            logger.warning("Failed to read %s via sudo: %s", path, exc)
            return None


def _write_root_file(path: Path, data: bytes, mode: int = 0o644) -> None:
    """
    Write a file that may require root privileges.

    The file is first written to a unique temporary file in the same directory
    (or /tmp if the target directory is not writable) and then moved into
    place. If the user lacks write permission on the directory, sudo is used
    via tee/install.
    """
    target = path
    target.parent.mkdir(parents=True, exist_ok=True) if os.access(
        path.parent.parent if not path.parent.exists() else path.parent, os.W_OK
    ) else None

    # Try a direct write first.
    try:
        tmp = target.with_suffix(target.suffix + ".tmp")
        with open(tmp, "wb") as f:
            f.write(data)
        os.chmod(tmp, mode)
        tmp.replace(target)
        return
    except PermissionError:
        pass

    # Fall back to sudo via a staging file in /tmp.
    staging = Path(tempfile.mkstemp(prefix="webzfs-restore-", suffix=".bin")[1])
    try:
        staging.write_bytes(data)
        os.chmod(staging, 0o600)
        # Ensure parent directory exists with sudo.
        run_privileged_command(
            ["mkdir", "-p", str(target.parent)],
            check=True,
            timeout=10,
        )
        # Install moves the file with the requested mode in one shot.
        run_privileged_command(
            ["install", "-m", oct(mode)[2:].zfill(3), str(staging), str(target)],
            check=True,
            timeout=15,
        )
    finally:
        staging.unlink(missing_ok=True)


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    """Derive a 32-byte AES-256 key from the passphrase using PBKDF2-SHA256."""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=AES_KEY_BYTES,
        salt=salt,
        iterations=KDF_ITERATIONS,
    )
    return kdf.derive(passphrase.encode("utf-8"))


def _encode_b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _decode_b64(value: str) -> bytes:
    return base64.b64decode(value.encode("ascii"))


def default_archive_filename() -> str:
    """Return a default filename for a fresh export."""
    host = socket.gethostname().split(".")[0] or "webzfs"
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return f"webzfs-backup-{host}-{stamp}.wzbak"


# ---------------------------------------------------------------------------
# Manifest construction
# ---------------------------------------------------------------------------


def _enumerate_files(
    include_history: bool,
    include_secret: bool,
    include_system: bool,
) -> List[Dict[str, Any]]:
    """
    Build a list of file entries to back up.

    Each entry: {arcname, source, category, mode, exists}. Files that do not
    exist on disk are omitted from the list entirely (we never archive empty
    placeholders).
    """
    entries: List[Dict[str, Any]] = []

    # User config files
    user_dir = _user_config_dir()
    for name in USER_CONFIG_FILES:
        if name in HISTORY_FILES and not include_history:
            continue
        src = user_dir / name
        if not src.exists() or not src.is_file():
            continue
        entries.append(
            {
                "arcname": f"config/{name}",
                "source": str(src),
                "category": CATEGORY_CONFIG,
                "mode": 0o600 if name.startswith(".") else 0o644,
            }
        )

    # SSH keys
    keys_dir = _ssh_keys_dir()
    if keys_dir.exists() and keys_dir.is_dir():
        for entry in sorted(keys_dir.iterdir()):
            if not entry.is_file():
                continue
            mode = 0o644 if entry.name.endswith(".pub") else 0o600
            entries.append(
                {
                    "arcname": f"ssh-keys/{entry.name}",
                    "source": str(entry),
                    "category": CATEGORY_SSH_KEY,
                    "mode": mode,
                }
            )

    if include_system:
        # Sanoid config
        sanoid = _sanoid_config_path()
        if sanoid.exists() and sanoid.is_file():
            entries.append(
                {
                    "arcname": "system/sanoid/sanoid.conf",
                    "source": str(sanoid),
                    "category": CATEGORY_SANOID,
                    "mode": 0o644,
                }
            )

        # Cron files managed by WebZFS
        cron_dir = Path("/etc/cron.d")
        for name in WEBZFS_CRON_FILES:
            cron_file = cron_dir / name
            if cron_file.exists() and cron_file.is_file():
                entries.append(
                    {
                        "arcname": f"system/cron.d/{name}",
                        "source": str(cron_file),
                        "category": CATEGORY_CRON,
                        "mode": 0o644,
                    }
                )

    if include_secret:
        env_path = _app_root() / ".env"
        if env_path.exists() and env_path.is_file():
            entries.append(
                {
                    "arcname": "env/.env",
                    "source": str(env_path),
                    "category": CATEGORY_ENV,
                    "mode": 0o600,
                }
            )

    return entries


def build_manifest(
    include_history: bool = False,
    include_secret: bool = True,
    include_system: bool = True,
) -> Dict[str, Any]:
    """
    Build a manifest describing the files that would be included in a backup,
    without actually packaging anything. Used by the UI to preview an export.
    """
    entries = _enumerate_files(include_history, include_secret, include_system)
    manifest_files: List[Dict[str, Any]] = []
    for entry in entries:
        src = Path(entry["source"])
        try:
            data = _read_root_file(src)
            size = len(data) if data is not None else 0
            sha = hashlib.sha256(data).hexdigest() if data is not None else ""
        except Exception:
            size = 0
            sha = ""
        manifest_files.append(
            {
                "arcname": entry["arcname"],
                "source": entry["source"],
                "category": entry["category"],
                "mode": entry["mode"],
                "size": size,
                "sha256": sha,
            }
        )
    return {
        "version": FORMAT_VERSION,
        "created": datetime.now().isoformat(),
        "hostname": socket.gethostname(),
        "platform": platform.system(),
        "app_version": APP_VERSION,
        "include_history": include_history,
        "include_secret": include_secret,
        "include_system": include_system,
        "files": manifest_files,
    }


# ---------------------------------------------------------------------------
# Archive create / inspect / restore
# ---------------------------------------------------------------------------


def _build_tar_bytes(manifest: Dict[str, Any], entries: List[Dict[str, Any]]) -> bytes:
    """
    Build the gzipped tar payload (manifest + every file) in memory.
    """
    buf = io.BytesIO()
    # Use gzip.GzipFile with a fixed mtime so identical inputs produce
    # identical archives, which simplifies testing and digest checks.
    with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as gz:
        with tarfile.open(fileobj=gz, mode="w") as tar:
            manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest_bytes)
            info.mode = 0o600
            info.mtime = int(time.time())
            tar.addfile(info, io.BytesIO(manifest_bytes))

            for entry in entries:
                src = Path(entry["source"])
                data = _read_root_file(src)
                if data is None:
                    continue
                tar_info = tarfile.TarInfo(name=entry["arcname"])
                tar_info.size = len(data)
                tar_info.mode = entry["mode"]
                tar_info.mtime = int(time.time())
                tar.addfile(tar_info, io.BytesIO(data))
    return buf.getvalue()


def create_archive(
    passphrase: str,
    include_history: bool = False,
    include_secret: bool = True,
    include_system: bool = True,
) -> bytes:
    """
    Create an encrypted backup archive and return its bytes.
    """
    if not passphrase or len(passphrase) < 8:
        raise ValueError("Passphrase must be at least 8 characters long.")

    entries = _enumerate_files(include_history, include_secret, include_system)
    manifest = build_manifest(include_history, include_secret, include_system)
    payload = _build_tar_bytes(manifest, entries)

    salt = os.urandom(KDF_SALT_BYTES)
    nonce = os.urandom(GCM_NONCE_BYTES)
    key = _derive_key(passphrase, salt)

    header = {
        "v": FORMAT_VERSION,
        "kdf": KDF_NAME,
        "kdf_iter": KDF_ITERATIONS,
        "salt": _encode_b64(salt),
        "cipher": "aes-256-gcm",
        "nonce": _encode_b64(nonce),
        "created": manifest["created"],
        "hostname": manifest["hostname"],
        "platform": manifest["platform"],
        "app_version": manifest["app_version"],
    }
    header_line = (json.dumps(header, separators=(",", ":")) + "\n").encode("utf-8")

    aesgcm = AESGCM(key)
    # Use the magic + header line as Additional Authenticated Data so any
    # tampering with the header invalidates the archive.
    aad = MAGIC + header_line
    ciphertext = aesgcm.encrypt(nonce, payload, aad)

    return MAGIC + header_line + ciphertext


def _split_archive(blob: bytes) -> Tuple[Dict[str, Any], bytes, bytes]:
    """
    Validate magic and split the blob into (header_dict, header_aad, ciphertext).
    """
    if not blob.startswith(MAGIC):
        raise ValueError(
            "File is not a WebZFS backup archive (missing WZFSBAK1 magic)."
        )
    rest = blob[len(MAGIC):]
    newline_pos = rest.find(b"\n")
    if newline_pos == -1:
        raise ValueError("Backup archive header is malformed (no header terminator).")
    header_line = rest[: newline_pos + 1]
    ciphertext = rest[newline_pos + 1 :]

    try:
        header = json.loads(header_line.decode("utf-8").strip())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Backup archive header is not valid JSON: {exc}") from exc

    if header.get("v") != FORMAT_VERSION:
        raise ValueError(
            f"Unsupported backup format version: {header.get('v')}. Expected {FORMAT_VERSION}."
        )

    aad = MAGIC + header_line
    return header, aad, ciphertext


def _decrypt_payload(blob: bytes, passphrase: str) -> Tuple[Dict[str, Any], bytes]:
    """
    Decrypt the archive and return (header, plaintext_tar_gz_bytes).
    """
    header, aad, ciphertext = _split_archive(blob)

    try:
        salt = _decode_b64(header["salt"])
        nonce = _decode_b64(header["nonce"])
    except (KeyError, ValueError) as exc:
        raise ValueError(f"Backup archive header is missing required fields: {exc}") from exc

    key = _derive_key(passphrase, salt)
    aesgcm = AESGCM(key)
    try:
        plaintext = aesgcm.decrypt(nonce, ciphertext, aad)
    except Exception as exc:
        raise ValueError(
            "Decryption failed. The passphrase is incorrect or the file is corrupted."
        ) from exc

    return header, plaintext


def _read_manifest_from_payload(payload: bytes) -> Dict[str, Any]:
    """Extract manifest.json from the gzipped tar payload."""
    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as gz:
        with tarfile.open(fileobj=gz, mode="r") as tar:
            try:
                member = tar.getmember("manifest.json")
            except KeyError as exc:
                raise ValueError("Backup archive is missing manifest.json.") from exc
            extracted = tar.extractfile(member)
            if extracted is None:
                raise ValueError("Could not read manifest.json from backup archive.")
            return json.loads(extracted.read().decode("utf-8"))


def inspect_archive(blob: bytes, passphrase: str) -> Dict[str, Any]:
    """
    Decrypt and return only the metadata needed for the user to confirm a
    restore. Returns a dict with header + manifest + summary counts.
    """
    header, payload = _decrypt_payload(blob, passphrase)
    manifest = _read_manifest_from_payload(payload)

    summary: Dict[str, int] = {}
    for entry in manifest.get("files", []):
        cat = entry.get("category", "unknown")
        summary[cat] = summary.get(cat, 0) + 1

    return {
        "header": header,
        "manifest": manifest,
        "summary": summary,
        "current_hostname": socket.gethostname(),
        "current_platform": platform.system(),
    }


def _resolve_target_path(arcname: str) -> Optional[Path]:
    """
    Map an archive entry path back to its target on disk. Returns None for
    paths that are not in our allow-list, which protects against path
    traversal in maliciously crafted archives.
    """
    # Reject any traversal segments outright.
    parts = Path(arcname).parts
    if any(p in ("..", "") or p.startswith("/") for p in parts):
        return None

    if len(parts) == 2 and parts[0] == "config":
        if parts[1] in USER_CONFIG_FILES:
            return _user_config_dir() / parts[1]
        return None

    if len(parts) == 2 and parts[0] == "ssh-keys":
        # Allow any filename inside the keys dir; webzfs creates them by UUID.
        # Reject path separators and dot files for safety.
        name = parts[1]
        if "/" in name or name in (".", "..") or name.startswith("."):
            return None
        return _ssh_keys_dir() / name

    if parts == ("system", "sanoid", "sanoid.conf"):
        return _sanoid_config_path()

    if len(parts) == 3 and parts[0] == "system" and parts[1] == "cron.d":
        if parts[2] in WEBZFS_CRON_FILES:
            return Path("/etc/cron.d") / parts[2]
        return None

    if parts == ("env", ".env"):
        return _app_root() / ".env"

    return None


def restore_archive(
    blob: bytes,
    passphrase: str,
    selected_categories: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Restore an encrypted backup archive.

    Existing target files are renamed to ``<path>.pre-restore-<timestamp>``
    before being overwritten so nothing is lost. System files are written via
    ``run_privileged_command`` when the current user does not have write
    permission on the target directory.

    Args:
        blob: Encrypted archive bytes.
        passphrase: Passphrase used to encrypt the archive.
        selected_categories: Optional list of categories to restore. When None,
            all categories present in the archive are restored.

    Returns:
        Summary dict with ``restored``, ``skipped``, and ``failed`` lists.
    """
    header, payload = _decrypt_payload(blob, passphrase)
    manifest = _read_manifest_from_payload(payload)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    restored: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []
    failed: List[Dict[str, Any]] = []

    # Index manifest entries by arcname for quick lookup of mode/category.
    manifest_index = {f["arcname"]: f for f in manifest.get("files", [])}

    # Ensure user config and SSH key directories exist with safe permissions.
    user_dir = _user_config_dir()
    user_dir.mkdir(parents=True, exist_ok=True)
    keys_dir = _ssh_keys_dir()
    keys_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(keys_dir, 0o700)
    except OSError:
        pass

    with gzip.GzipFile(fileobj=io.BytesIO(payload), mode="rb") as gz:
        with tarfile.open(fileobj=gz, mode="r") as tar:
            for member in tar.getmembers():
                if member.name == "manifest.json":
                    continue
                if not member.isfile():
                    skipped.append({"arcname": member.name, "reason": "not a regular file"})
                    continue

                meta = manifest_index.get(member.name)
                if meta is None:
                    skipped.append({"arcname": member.name, "reason": "not in manifest"})
                    continue

                category = meta.get("category", "unknown")
                if selected_categories is not None and category not in selected_categories:
                    skipped.append(
                        {"arcname": member.name, "reason": f"category {category} not selected"}
                    )
                    continue

                target = _resolve_target_path(member.name)
                if target is None:
                    skipped.append({"arcname": member.name, "reason": "path not allowed"})
                    continue

                extracted = tar.extractfile(member)
                if extracted is None:
                    failed.append({"arcname": member.name, "reason": "could not read entry"})
                    continue
                data = extracted.read()

                # Verify checksum if manifest has one.
                expected_sha = meta.get("sha256", "")
                if expected_sha:
                    actual_sha = hashlib.sha256(data).hexdigest()
                    if actual_sha != expected_sha:
                        failed.append(
                            {
                                "arcname": member.name,
                                "reason": "sha256 mismatch",
                            }
                        )
                        continue

                # Back up existing file.
                if target.exists():
                    backup_path = target.with_name(target.name + f".pre-restore-{timestamp}")
                    try:
                        if os.access(target.parent, os.W_OK):
                            shutil.move(str(target), str(backup_path))
                        else:
                            run_privileged_command(
                                ["mv", str(target), str(backup_path)],
                                check=True,
                                timeout=10,
                            )
                    except Exception as exc:
                        failed.append(
                            {
                                "arcname": member.name,
                                "reason": f"could not back up existing file: {exc}",
                            }
                        )
                        continue

                mode = int(meta.get("mode", 0o600))
                try:
                    _write_root_file(target, data, mode=mode)
                    restored.append(
                        {
                            "arcname": member.name,
                            "target": str(target),
                            "category": category,
                            "size": len(data),
                        }
                    )
                except Exception as exc:
                    failed.append(
                        {
                            "arcname": member.name,
                            "target": str(target),
                            "reason": str(exc),
                        }
                    )

    return {
        "header": header,
        "restored": restored,
        "skipped": skipped,
        "failed": failed,
        "timestamp": timestamp,
    }


# ---------------------------------------------------------------------------
# Staging helpers used by the UI between Inspect and Restore steps
# ---------------------------------------------------------------------------


def _staging_dir() -> Path:
    """Directory used to hold uploaded archives between inspect and restore."""
    d = _user_config_dir() / "restore-staging"
    d.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        os.chmod(d, 0o700)
    except OSError:
        pass
    return d


def stash_upload(blob: bytes) -> str:
    """
    Save an uploaded archive to a temp file and return a token that
    identifies it. Cleans up files older than 15 minutes on every call.
    """
    cleanup_stash(max_age_seconds=900)
    token = base64.urlsafe_b64encode(os.urandom(18)).decode("ascii").rstrip("=")
    path = _staging_dir() / f"{token}.wzbak"
    path.write_bytes(blob)
    os.chmod(path, 0o600)
    return token


def load_stash(token: str) -> bytes:
    """Load a previously stashed archive blob by token."""
    if not token or "/" in token or ".." in token:
        raise ValueError("Invalid stash token.")
    path = _staging_dir() / f"{token}.wzbak"
    if not path.exists():
        raise ValueError("Upload session expired or not found. Please upload again.")
    return path.read_bytes()


def discard_stash(token: str) -> None:
    """Delete a stashed archive after a successful or aborted restore."""
    if not token or "/" in token or ".." in token:
        return
    path = _staging_dir() / f"{token}.wzbak"
    path.unlink(missing_ok=True)


def cleanup_stash(max_age_seconds: int = 900) -> None:
    """Remove stash files older than max_age_seconds."""
    try:
        d = _staging_dir()
    except OSError:
        return
    cutoff = time.time() - max_age_seconds
    for p in d.iterdir():
        try:
            if p.is_file() and p.stat().st_mtime < cutoff:
                p.unlink(missing_ok=True)
        except OSError:
            continue
