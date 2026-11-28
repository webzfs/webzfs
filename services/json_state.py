"""
Small helpers for state files shared by several worker processes.

Gunicorn runs more than one worker, and each keeps its own service objects,
so a file such as ssh_connections.json is read and rewritten by several
processes. Two rules keep those files intact:

1. Every read-modify-write cycle holds an exclusive fcntl lock on a separate
   lock file (file_lock), so one worker cannot overwrite a change another
   worker made between its read and its write.
2. Files are written to a temporary file in the same directory and moved into
   place with os.replace (write_text_atomic, write_json_atomic). A reader
   never sees a half-written file, and a crash never leaves one behind.

Same approach as FileStorageService in services/storage.py. fcntl is
available on Linux, FreeBSD and NetBSD.
"""
import fcntl
import json
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path


class StateFileError(Exception):
    """Raised when a state file exists but cannot be parsed.

    Callers that are about to rewrite the file must stop instead of
    replacing it with empty data.
    """


@contextmanager
def file_lock(lock_path: Path):
    """Hold an exclusive lock on lock_path for the duration of the block."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a") as lock_handle:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def write_text_atomic(path: Path, text: str) -> None:
    """Replace path with text in one step. The file is created with mode 0600."""
    descriptor, temp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    except BaseException:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
        raise


def write_json_atomic(path: Path, data) -> None:
    """Replace path with data as indented JSON in one step (mode 0600)."""
    write_text_atomic(path, json.dumps(data, indent=2))


def read_json(path: Path, default):
    """Return the parsed JSON in path, or default if the file is missing or empty.

    Raises StateFileError when the file has content that is not valid JSON,
    so a caller never mistakes a damaged file for an empty one and saves
    over it.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return default
    except OSError as error:
        raise StateFileError(f"Cannot read {path}: {error}") from error
    if not text.strip():
        return default
    try:
        return json.loads(text)
    except ValueError as error:
        raise StateFileError(
            f"{path} is not valid JSON and was left unchanged: {error}"
        ) from error
