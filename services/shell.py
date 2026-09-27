"""POSIX pseudo-terminal lifecycle for the native WebZFS terminal."""

import asyncio
import errno
import fcntl
import hashlib
import os
import pty
import signal
import struct
import subprocess
import termios
from pathlib import Path

from services.shell_settings import ProcessIdentity, get_process_identity

LOCK_DIR = Path.home() / ".config" / "webzfs" / "terminal-locks"


class TerminalBusyError(Exception):
    """Raised when a username already owns an active terminal."""


def _minimal_environment(identity: ProcessIdentity) -> dict[str, str]:
    environment = {
        "HOME": identity.home,
        "USER": identity.username,
        "LOGNAME": identity.username,
        "SHELL": identity.shell,
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "TERM": "xterm-256color",
        "COLORTERM": "truecolor",
    }
    for name in ("LANG", "LC_ALL", "LC_CTYPE"):
        if os.environ.get(name):
            environment[name] = os.environ[name]
    return environment


class TerminalProcess:
    """Own one shell process, PTY master descriptor, and username lock."""

    def __init__(self, authenticated_user: str, columns: int = 120, rows: int = 32):
        self.authenticated_user = authenticated_user
        self.identity = get_process_identity()
        self.columns = columns
        self.rows = rows
        self.master_fd: int | None = None
        self.process: subprocess.Popen | None = None
        self.lock_handle = None

    def start(self) -> None:
        """Acquire the username lock and start an interactive shell on a PTY."""
        self.acquire_lock()
        master_fd, slave_fd = pty.openpty()
        try:
            self.master_fd = master_fd
            self.resize(self.columns, self.rows)
            self.process = subprocess.Popen(
                [self.identity.shell, "-i"],
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                cwd=self.identity.home,
                env=_minimal_environment(self.identity),
                close_fds=True,
                start_new_session=True,
            )
        except Exception:
            os.close(master_fd)
            self.master_fd = None
            self._release_lock()
            raise
        finally:
            os.close(slave_fd)

    def _acquire_lock(self) -> None:
        if self.lock_handle is not None:
            return
        LOCK_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(LOCK_DIR, 0o700)
        digest = hashlib.sha256(self.authenticated_user.encode("utf-8")).hexdigest()
        lock_path = LOCK_DIR / f"{digest}.lock"
        self.lock_handle = lock_path.open("a+b")
        os.chmod(lock_path, 0o600)
        try:
            fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            self.lock_handle.close()
            self.lock_handle = None
            raise TerminalBusyError("This user already has an active terminal") from exc

    def acquire_lock(self) -> None:
        """Reserve the authenticated username before starting a shell process."""
        self._acquire_lock()

    async def read(self, size: int = 65536) -> bytes:
        """Wait asynchronously for PTY output and return one byte chunk."""
        if self.master_fd is None:
            return b""
        loop = asyncio.get_running_loop()
        future = loop.create_future()

        def read_ready() -> None:
            if future.done() or self.master_fd is None:
                return
            try:
                future.set_result(os.read(self.master_fd, size))
            except OSError as exc:
                if exc.errno in {errno.EBADF, errno.EIO}:
                    future.set_result(b"")
                else:
                    future.set_exception(exc)

        loop.add_reader(self.master_fd, read_ready)
        try:
            return await future
        finally:
            if self.master_fd is not None:
                loop.remove_reader(self.master_fd)

    def write(self, data: bytes) -> None:
        if self.master_fd is None:
            raise BrokenPipeError("Terminal is closed")
        view = memoryview(data)
        while view:
            written = os.write(self.master_fd, view)
            view = view[written:]

    def resize(self, columns: int, rows: int) -> None:
        self.columns = max(2, min(int(columns), 500))
        self.rows = max(1, min(int(rows), 300))
        if self.master_fd is None:
            return
        window_size = struct.pack("HHHH", self.rows, self.columns, 0, 0)
        fcntl.ioctl(self.master_fd, termios.TIOCSWINSZ, window_size)

    def poll(self) -> int | None:
        return self.process.poll() if self.process else None

    def terminate(self) -> int | None:
        """Terminate the shell process group while leaving PTY output readable."""
        process = self.process
        if process and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGHUP)
                process.wait(timeout=2)
            except (ProcessLookupError, subprocess.TimeoutExpired):
                if process.poll() is None:
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                        process.wait(timeout=2)
                    except (ProcessLookupError, subprocess.TimeoutExpired):
                        if process.poll() is None:
                            try:
                                os.killpg(process.pid, signal.SIGKILL)
                            except ProcessLookupError:
                                pass
                            process.wait(timeout=2)
        return process.poll() if process else None

    def close(self) -> int | None:
        """Terminate the shell, close its PTY, and release the username lock."""
        exit_code = self.terminate()
        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None
        self.process = None
        self._release_lock()
        return exit_code

    def _release_lock(self) -> None:
        if self.lock_handle is not None:
            try:
                fcntl.flock(self.lock_handle.fileno(), fcntl.LOCK_UN)
            finally:
                self.lock_handle.close()
                self.lock_handle = None
