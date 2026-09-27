"""Tamper-evident recording storage for native WebZFS terminal sessions."""

import base64
import fcntl
import hashlib
import json
import os
import shutil
import tempfile
import threading
import time
import uuid
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterator

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from config.settings import settings

FORMAT_VERSION = 1
EMPTY_HASH = "0" * 64
RECORDING_ROOT = Path.home() / ".config" / "webzfs" / "shell-recordings"
LEGACY_COMPLETED_REASONS = frozenset(
    {"client_close", "client_disconnect", "idle_timeout", "token_expired"}
)


class RecordingError(Exception):
    """Base exception for recording failures."""


class RecordingLimitError(RecordingError):
    """Raised when a recording exceeds its configured per-session limit."""


def _canonical_bytes(value: dict) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _secure_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(path.parent, 0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
        os.chmod(path, 0o600)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _ensure_store() -> None:
    RECORDING_ROOT.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(RECORDING_ROOT, 0o700)
    sessions_dir().mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(sessions_dir(), 0o700)


@contextmanager
def _capacity_lock():
    path = RECORDING_ROOT / "capacity.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "r+b") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield


def sessions_dir() -> Path:
    return RECORDING_ROOT / "sessions"


def _key_paths() -> tuple[Path, Path]:
    return RECORDING_ROOT / "signing.key", RECORDING_ROOT / "signing.pub"


def _load_signing_key() -> Ed25519PrivateKey:
    _ensure_store()
    private_path, public_path = _key_paths()
    lock_path = RECORDING_ROOT / "signing-key.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "r+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        if not private_path.exists():
            private_key = Ed25519PrivateKey.generate()
            private_bytes = private_key.private_bytes(
                serialization.Encoding.Raw,
                serialization.PrivateFormat.Raw,
                serialization.NoEncryption(),
            )
            public_bytes = private_key.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
            _secure_write(private_path, private_bytes)
            _secure_write(public_path, public_bytes)
        private_bytes = private_path.read_bytes()
        private_key = Ed25519PrivateKey.from_private_bytes(private_bytes)
        if not public_path.exists():
            public_bytes = private_key.public_key().public_bytes(
                serialization.Encoding.Raw,
                serialization.PublicFormat.Raw,
            )
            _secure_write(public_path, public_bytes)
        os.chmod(private_path, 0o600)
        os.chmod(public_path, 0o600)
        return private_key


def public_key_bytes() -> bytes:
    """Return the raw Ed25519 public verification key."""
    key = _load_signing_key()
    return key.public_key().public_bytes(
        serialization.Encoding.Raw,
        serialization.PublicFormat.Raw,
    )


def signing_key_id() -> str:
    return hashlib.sha256(public_key_bytes()).hexdigest()[:16]


class ShellRecorder:
    """Incrementally record one PTY session as a signed hash chain."""

    def __init__(self, metadata: dict):
        _ensure_store()
        ledger_verification = verify_ledger()
        if not ledger_verification["valid"]:
            raise RecordingError(
                f"Recording ledger verification failed: {ledger_verification['error']}"
            )
        prune_recordings()
        max_total_bytes = settings.SHELL_RECORDING_MAX_TOTAL_MIB * 1024 * 1024
        if _session_store_bytes() >= max_total_bytes:
            raise RecordingLimitError(
                "Terminal recording archive is at its total size limit"
            )
        key_id = signing_key_id()
        self.session_id = uuid.uuid4().hex
        self.session_dir = sessions_dir() / self.session_id
        self.session_dir.mkdir(mode=0o700)
        os.chmod(self.session_dir, 0o700)
        self.events_path = self.session_dir / "events.jsonl"
        self.lock_handle = (self.session_dir / "recording.lock").open("a+b")
        os.chmod(self.session_dir / "recording.lock", 0o600)
        fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        descriptor = os.open(
            self.events_path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
        )
        self.events: BinaryIO = os.fdopen(descriptor, "wb", buffering=0)
        self.started_monotonic = time.monotonic()
        self.started_monotonic_ns = time.monotonic_ns()
        self.sequence = 0
        self.previous_hash = EMPTY_HASH
        self.event_bytes = 0
        self.closed = False
        self.write_lock = threading.Lock()
        self.metadata = dict(metadata)
        self.metadata.update(
            {
                "format_version": FORMAT_VERSION,
                "signing_key_id": key_id,
                "session_id": self.session_id,
                "started_at": _utc_now(),
                "status": "recording",
            }
        )
        _secure_write(
            self.session_dir / "metadata.json",
            _canonical_bytes(self.metadata) + b"\n",
        )
        try:
            with _capacity_lock():
                if _session_store_bytes() > max_total_bytes:
                    raise RecordingLimitError(
                        "Terminal recording archive is at its total size limit"
                    )
        except Exception:
            self.events.close()
            self._release_lock()
            shutil.rmtree(self.session_dir)
            raise

    def record(self, event_type: str, payload: bytes | dict) -> None:
        with self.write_lock:
            self._record_event(event_type, payload)

    def _record_event(self, event_type: str, payload: bytes | dict) -> None:
        if self.closed:
            raise RecordingError("Recording is already closed")
        if event_type not in {"input", "output", "resize"}:
            raise RecordingError(f"Unsupported recording event: {event_type}")

        if isinstance(payload, bytes):
            encoded_payload = {
                "encoding": "base64",
                "data": base64.b64encode(payload).decode("ascii"),
            }
        else:
            encoded_payload = {"encoding": "json", "data": payload}

        event = {
            "sequence": self.sequence,
            "offset_ns": time.monotonic_ns() - self.started_monotonic_ns,
            "type": event_type,
            "payload": encoded_payload,
            "previous_hash": self.previous_hash,
        }
        event_hash = hashlib.sha256(_canonical_bytes(event)).hexdigest()
        event["hash"] = event_hash
        line = _canonical_bytes(event) + b"\n"
        max_bytes = settings.SHELL_RECORDING_MAX_SESSION_MIB * 1024 * 1024
        if self.event_bytes + len(line) > max_bytes:
            raise RecordingLimitError(
                "Terminal recording reached its session size limit"
            )
        with _capacity_lock():
            max_total_bytes = settings.SHELL_RECORDING_MAX_TOTAL_MIB * 1024 * 1024
            if _session_store_bytes() + len(line) > max_total_bytes:
                raise RecordingLimitError(
                    "Terminal recording archive reached its total size limit"
                )
            self.events.write(line)
        self.event_bytes += len(line)
        self.sequence += 1
        self.previous_hash = event_hash

    def close(self, status: str, exit_code: int | None, reason: str) -> dict:
        if self.closed:
            return load_session_metadata(self.session_id)
        self.closed = True
        final_metadata = None
        try:
            self.events.flush()
            os.fsync(self.events.fileno())
            self.events.close()
            finished_at = _utc_now()
            final_metadata = dict(self.metadata)
            final_metadata.update(
                {
                    "finished_at": finished_at,
                    "duration_seconds": round(
                        time.monotonic() - self.started_monotonic, 3
                    ),
                    "status": status,
                    "exit_code": exit_code,
                    "termination_reason": reason,
                    "event_count": self.sequence,
                    "event_bytes": self.event_bytes,
                }
            )
            metadata_bytes = _canonical_bytes(final_metadata) + b"\n"
            _secure_write(self.session_dir / "metadata.json", metadata_bytes)
            manifest = {
                "format_version": FORMAT_VERSION,
                "session_id": self.session_id,
                "metadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
                "events_sha256": _sha256_file(self.events_path),
                "final_event_hash": self.previous_hash,
                "event_count": self.sequence,
                "signing_key_id": signing_key_id(),
            }
            manifest_bytes = _canonical_bytes(manifest) + b"\n"
            signature = _load_signing_key().sign(manifest_bytes)
            _secure_write(self.session_dir / "manifest.json", manifest_bytes)
            _secure_write(self.session_dir / "signature.ed25519", signature)
            append_ledger_entry(
                {
                    "action": "session_finalized",
                    "session_id": self.session_id,
                    "status": status,
                    "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
                }
            )
        finally:
            self._release_lock()
        prune_recordings()
        return final_metadata

    def abort(self, reason: str) -> dict:
        return self.close("incomplete", None, reason)

    def _release_lock(self) -> None:
        if self.lock_handle is not None:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.lock_handle.close()
                self.lock_handle = None


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _session_store_bytes() -> int:
    if not sessions_dir().is_dir():
        return 0
    return sum(
        path.stat().st_size for path in sessions_dir().rglob("*") if path.is_file()
    )


def recover_incomplete_recordings() -> int:
    """Finalize abandoned unlocked recording directories as incomplete sessions."""
    _ensure_store()
    ledger_verification = verify_ledger()
    if not ledger_verification["valid"]:
        return 0
    recovered = 0
    for session_dir in sessions_dir().iterdir():
        if not session_dir.is_dir() or (session_dir / "manifest.json").exists():
            continue
        if not (session_dir / "metadata.json").is_file():
            continue
        lock_path = session_dir / "recording.lock"
        lock_handle = lock_path.open("a+b")
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            lock_handle.close()
            continue
        try:
            _recover_session(session_dir)
            recovered += 1
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()
    return recovered


def _recover_session(session_dir: Path) -> None:
    metadata_path = session_dir / "metadata.json"
    events_path = session_dir / "events.jsonl"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    valid_lines = []
    previous_hash = EMPTY_HASH
    event_count = 0
    if events_path.exists():
        with events_path.open("rb") as handle:
            for line in handle:
                try:
                    event = json.loads(line)
                    stored_hash = event.pop("hash")
                    if event.get("previous_hash") != previous_hash:
                        break
                    if (
                        hashlib.sha256(_canonical_bytes(event)).hexdigest()
                        != stored_hash
                    ):
                        break
                    event["hash"] = stored_hash
                    valid_lines.append(_canonical_bytes(event) + b"\n")
                    previous_hash = stored_hash
                    event_count += 1
                except (KeyError, ValueError, TypeError, json.JSONDecodeError):
                    break
    _secure_write(events_path, b"".join(valid_lines))
    metadata.update(
        {
            "finished_at": _utc_now(),
            "duration_seconds": None,
            "status": "incomplete",
            "exit_code": None,
            "termination_reason": "worker_recovery",
            "event_count": event_count,
            "event_bytes": events_path.stat().st_size,
        }
    )
    metadata_bytes = _canonical_bytes(metadata) + b"\n"
    _secure_write(metadata_path, metadata_bytes)
    manifest = {
        "format_version": FORMAT_VERSION,
        "session_id": metadata["session_id"],
        "metadata_sha256": hashlib.sha256(metadata_bytes).hexdigest(),
        "events_sha256": _sha256_file(events_path),
        "final_event_hash": previous_hash,
        "event_count": event_count,
        "signing_key_id": signing_key_id(),
    }
    manifest_bytes = _canonical_bytes(manifest) + b"\n"
    _secure_write(session_dir / "manifest.json", manifest_bytes)
    _secure_write(
        session_dir / "signature.ed25519",
        _load_signing_key().sign(manifest_bytes),
    )
    append_ledger_entry(
        {
            "action": "session_recovered",
            "session_id": metadata["session_id"],
            "status": "incomplete",
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        }
    )


def append_ledger_entry(details: dict) -> dict:
    """Append one signed hash-chained entry to the recording ledger."""
    _ensure_store()
    ledger_path = RECORDING_ROOT / "ledger.jsonl"
    lock_path = RECORDING_ROOT / "ledger.lock"
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(descriptor, "r+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        verification = verify_ledger()
        if not verification["valid"]:
            raise RecordingError(
                f"Recording ledger verification failed: {verification['error']}"
            )
        previous_hash = EMPTY_HASH
        if ledger_path.exists():
            try:
                last_line = ledger_path.read_bytes().splitlines()[-1]
                previous_hash = json.loads(last_line)["hash"]
            except (IndexError, KeyError, ValueError, json.JSONDecodeError):
                previous_hash = EMPTY_HASH
        entry = {
            "sequence": (
                sum(1 for _ in ledger_path.open("rb")) if ledger_path.exists() else 0
            ),
            "timestamp": _utc_now(),
            "previous_hash": previous_hash,
            **details,
        }
        entry_hash = hashlib.sha256(_canonical_bytes(entry)).hexdigest()
        entry["hash"] = entry_hash
        signature = _load_signing_key().sign(_canonical_bytes(entry))
        entry["signature"] = base64.b64encode(signature).decode("ascii")
        line = _canonical_bytes(entry) + b"\n"
        ledger_descriptor = os.open(
            ledger_path,
            os.O_WRONLY | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        with os.fdopen(ledger_descriptor, "ab", buffering=0) as ledger_handle:
            ledger_handle.write(line)
            os.fsync(ledger_handle.fileno())
        os.chmod(ledger_path, 0o600)
        return entry


def verify_ledger() -> dict:
    """Verify the complete local ledger chain and signatures."""
    ledger_path = RECORDING_ROOT / "ledger.jsonl"
    if not ledger_path.exists():
        return {"valid": True, "entries": 0, "error": None}
    public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes())
    previous_hash = EMPTY_HASH
    count = 0
    try:
        with ledger_path.open("rb") as handle:
            for line in handle:
                entry = json.loads(line)
                signature = base64.b64decode(entry.pop("signature"))
                stored_hash = entry.pop("hash")
                if entry.get("previous_hash") != previous_hash:
                    raise RecordingError(f"Ledger chain failed at entry {count}")
                calculated_hash = hashlib.sha256(_canonical_bytes(entry)).hexdigest()
                if calculated_hash != stored_hash:
                    raise RecordingError(f"Ledger hash failed at entry {count}")
                signed_entry = dict(entry)
                signed_entry["hash"] = stored_hash
                public_key.verify(signature, _canonical_bytes(signed_entry))
                previous_hash = stored_hash
                count += 1
        return {"valid": True, "entries": count, "error": None}
    except Exception as exc:
        return {"valid": False, "entries": count, "error": str(exc)}


def load_session_metadata(session_id: str) -> dict:
    """Load validated session metadata without allowing path traversal."""
    session_dir = _session_dir(session_id)
    return json.loads((session_dir / "metadata.json").read_text(encoding="utf-8"))


def recording_display_metadata(session_id: str) -> dict:
    """Return signed metadata with a non-destructive legacy status correction."""
    metadata = load_session_metadata(session_id)
    metadata["signed_status"] = metadata.get("status")
    metadata["effective_status"] = metadata.get("status")
    metadata["legacy_status_corrected"] = False
    if (
        metadata.get("status") == "incomplete"
        and metadata.get("termination_reason") in LEGACY_COMPLETED_REASONS
    ):
        metadata["effective_status"] = "completed"
        metadata["legacy_status_corrected"] = True
    return metadata


def _session_dir(session_id: str) -> Path:
    if not session_id or any(
        character not in "0123456789abcdef" for character in session_id
    ):
        raise FileNotFoundError("Invalid terminal session ID")
    path = sessions_dir() / session_id
    if not path.is_dir():
        raise FileNotFoundError("Terminal session not found")
    return path


def iter_events(session_id: str) -> Iterator[dict]:
    with (_session_dir(session_id) / "events.jsonl").open(
        "r", encoding="utf-8"
    ) as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def get_events(
    session_id: str,
    *,
    start: int = 0,
    limit: int = 500,
    event_types: frozenset[str] | None = None,
) -> dict:
    """Return a bounded page of replay or raw-input events."""
    selected = []
    next_sequence = None
    for event in iter_events(session_id):
        if event["sequence"] < start:
            continue
        if event_types is not None and event["type"] not in event_types:
            continue
        if len(selected) >= limit:
            next_sequence = event["sequence"]
            break
        selected.append(event)
    return {"events": selected, "next_sequence": next_sequence}


def verify_session(session_id: str) -> dict:
    """Verify event hashes, file digests, and the signed manifest."""
    try:
        session_dir = _session_dir(session_id)
        metadata_bytes = (session_dir / "metadata.json").read_bytes()
        manifest_bytes = (session_dir / "manifest.json").read_bytes()
        signature = (session_dir / "signature.ed25519").read_bytes()
        manifest = json.loads(manifest_bytes)
        if manifest.get("metadata_sha256") != hashlib.sha256(metadata_bytes).hexdigest():
            raise RecordingError("Metadata digest does not match the manifest")
        events_path = session_dir / "events.jsonl"
        if manifest.get("events_sha256") != _sha256_file(events_path):
            raise RecordingError("Event file digest does not match the manifest")
        previous_hash = EMPTY_HASH
        event_count = 0
        for event in iter_events(session_id):
            stored_hash = event.pop("hash")
            if event.get("previous_hash") != previous_hash:
                raise RecordingError(f"Event chain failed at event {event_count}")
            calculated_hash = hashlib.sha256(_canonical_bytes(event)).hexdigest()
            if calculated_hash != stored_hash:
                raise RecordingError(f"Event hash failed at event {event_count}")
            previous_hash = stored_hash
            event_count += 1
        if event_count != manifest.get("event_count"):
            raise RecordingError("Event count does not match the manifest")
        if previous_hash != manifest.get("final_event_hash"):
            raise RecordingError("Final event hash does not match the manifest")
        public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes())
        public_key.verify(signature, manifest_bytes)
        return {"valid": True, "event_count": event_count, "error": None}
    except Exception as exc:
        return {"valid": False, "event_count": 0, "error": str(exc)}


def list_recordings() -> list[dict]:
    """Return recording metadata, newest first, with verification state."""
    _ensure_store()
    recover_incomplete_recordings()
    recordings = []
    for path in sessions_dir().iterdir():
        if not path.is_dir() or not (path / "manifest.json").is_file():
            continue
        try:
            metadata = recording_display_metadata(path.name)
            metadata["verification"] = verify_session(path.name)
            metadata["archive_bytes"] = sum(
                item.stat().st_size for item in path.iterdir() if item.is_file()
            )
            recordings.append(metadata)
        except Exception:
            continue
    return sorted(recordings, key=lambda item: item.get("started_at", ""), reverse=True)


def recording_summary() -> dict:
    recordings = list_recordings()
    total_bytes = sum(item.get("archive_bytes", 0) for item in recordings)
    missing_sessions = _missing_sessions_from_ledger(recordings)
    return {
        "recording_enabled": settings.SHELL_RECORDING_ENABLED,
        "session_count": len(recordings),
        "total_bytes": total_bytes,
        "total_size_display": _format_bytes(total_bytes),
        "newest": recordings[0].get("started_at") if recordings else None,
        "oldest": recordings[-1].get("started_at") if recordings else None,
        "all_valid": all(item["verification"]["valid"] for item in recordings)
        and not missing_sessions,
        "ledger": verify_ledger(),
        "missing_sessions": missing_sessions,
    }


def _format_bytes(value: int) -> str:
    size = float(value)
    for unit in ("B", "KiB", "MiB", "GiB"):
        if size < 1024 or unit == "GiB":
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{value} B"


def _missing_sessions_from_ledger(recordings: list[dict]) -> list[str]:
    ledger_path = RECORDING_ROOT / "ledger.jsonl"
    if not ledger_path.exists():
        return []
    present = {item["session_id"] for item in recordings}
    finalized = set()
    pruned = set()
    try:
        with ledger_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                entry = json.loads(line)
                if entry.get("action") in {"session_finalized", "session_recovered"}:
                    finalized.add(entry.get("session_id"))
                elif entry.get("action") == "session_pruned":
                    pruned.add(entry.get("session_id"))
    except (OSError, json.JSONDecodeError):
        return []
    return sorted((finalized - pruned) - present)


def create_recording_archive(session_id: str) -> Path:
    """Create a temporary zip containing one complete signed recording."""
    session_dir = _session_dir(session_id)
    descriptor, archive_name = tempfile.mkstemp(
        prefix=f"webzfs-shell-{session_id}-",
        suffix=".zip",
    )
    os.close(descriptor)
    archive_path = Path(archive_name)
    with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for name in (
            "metadata.json",
            "events.jsonl",
            "manifest.json",
            "signature.ed25519",
        ):
            path = session_dir / name
            archive.write(path, arcname=f"{session_id}/{name}")
        archive.writestr(
            f"{session_id}/signing.pub",
            public_key_bytes(),
        )
    os.chmod(archive_path, 0o600)
    return archive_path


def ledger_bytes() -> bytes:
    path = RECORDING_ROOT / "ledger.jsonl"
    return path.read_bytes() if path.exists() else b""


def prune_recordings() -> None:
    """Apply age and total-size retention to finalized recording archives."""
    if not verify_ledger()["valid"]:
        return
    recordings = list_recordings()
    cutoff = time.time() - settings.SHELL_RECORDING_RETENTION_DAYS * 86400
    candidates = []
    for metadata in reversed(recordings):
        if metadata.get("status") != "completed":
            continue
        session_id = metadata["session_id"]
        session_path = _session_dir(session_id)
        try:
            started_at = datetime.fromisoformat(
                metadata["started_at"].replace("Z", "+00:00")
            ).timestamp()
        except (KeyError, ValueError):
            started_at = 0
        if started_at < cutoff:
            candidates.append((session_id, "retention_days"))

    aged_session_ids = {session_id for session_id, _ in candidates}
    remaining_total = sum(
        item.get("archive_bytes", 0)
        for item in recordings
        if item["session_id"] not in aged_session_ids
    )
    max_total = settings.SHELL_RECORDING_MAX_TOTAL_MIB * 1024 * 1024
    for metadata in reversed(recordings):
        if remaining_total <= max_total:
            break
        if metadata.get("status") != "completed":
            continue
        session_id = metadata["session_id"]
        if not any(candidate[0] == session_id for candidate in candidates):
            candidates.append((session_id, "total_size_limit"))
        remaining_total -= metadata.get("archive_bytes", 0)

    for session_id, reason in candidates:
        session_path = sessions_dir() / session_id
        if not session_path.is_dir():
            continue
        append_ledger_entry(
            {"action": "session_pruned", "session_id": session_id, "reason": reason}
        )
        shutil.rmtree(session_path)
