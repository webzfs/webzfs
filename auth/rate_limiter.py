"""
Login rate limiting shared by all Gunicorn workers.

Attempts are stored in ~/.config/webzfs/login_attempts.json and every read or
change happens under an exclusive fcntl lock. All worker processes therefore
see the same counts, and checking the limit and recording an attempt is one
step: parallel requests cannot all pass the check before any is recorded.

Two limits apply to every login attempt:

- per client IP: MAX_ATTEMPTS_PER_IP attempts within IP_WINDOW_SECONDS
- per username:  MAX_ATTEMPTS_PER_USER attempts within USER_WINDOW_SECONDS,
  which caps password guessing against one account from many addresses.

An attempt is recorded before PAM is called, and both counters are cleared
after a successful login, so only failed attempts stay counted.

The client IP must be request.client.host. Uvicorn fills it from
X-Forwarded-For only when the connecting peer is a proxy listed in
FORWARDED_ALLOW_IPS; reading that header directly would let any client pick
its own address and reset its own limit.

If the state file cannot be used (for example the config directory is not
writable), the error is logged and the login is not blocked.
"""
import fcntl
import json
import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

MAX_ATTEMPTS_PER_IP = 5
IP_WINDOW_SECONDS = 60

MAX_ATTEMPTS_PER_USER = 10
USER_WINDOW_SECONDS = 300

# Upper bound on stored keys, so many distinct addresses or usernames cannot
# grow the state file without limit. The least recently used keys go first.
MAX_TRACKED_KEYS = 5000

# Usernames are stored only up to this length.
MAX_KEY_USERNAME_LENGTH = 256

STATE_DIR = Path.home() / ".config" / "webzfs"
STATE_FILE = STATE_DIR / "login_attempts.json"
LOCK_FILE = STATE_DIR / "login_attempts.lock"


def _ip_key(ip_address: str) -> str:
    return f"ip:{ip_address}"


def _user_key(username: str) -> str:
    return f"user:{username[:MAX_KEY_USERNAME_LENGTH]}"


def _limit_for(key: str) -> tuple:
    """Return (max attempts, window seconds) for a state key."""
    if key.startswith("user:"):
        return MAX_ATTEMPTS_PER_USER, USER_WINDOW_SECONDS
    return MAX_ATTEMPTS_PER_IP, IP_WINDOW_SECONDS


def _prune(state: dict, now: float) -> dict:
    """Drop timestamps outside their window, empty keys, and excess keys."""
    pruned = {}
    for key, timestamps in state.items():
        if not isinstance(key, str) or not isinstance(timestamps, list):
            continue
        _, window = _limit_for(key)
        recent = [
            stamp for stamp in timestamps
            if isinstance(stamp, (int, float)) and now - window < stamp <= now
        ]
        if recent:
            pruned[key] = recent
    if len(pruned) > MAX_TRACKED_KEYS:
        newest_first = sorted(pruned, key=lambda key: max(pruned[key]), reverse=True)
        pruned = {key: pruned[key] for key in newest_first[:MAX_TRACKED_KEYS]}
    return pruned


def _retry_after(state: dict, key: str, now: float) -> int:
    """Seconds until key may try again; 0 when it is under its limit."""
    max_attempts, window = _limit_for(key)
    timestamps = sorted(state.get(key, []))
    if len(timestamps) < max_attempts:
        return 0
    # The attempt that must leave the window to bring the count below the
    # limit is the max_attempts-th most recent one.
    blocking = timestamps[len(timestamps) - max_attempts]
    return max(1, int(blocking + window - now) + 1)


@contextmanager
def _locked_state():
    """Yield the pruned state dict under an exclusive lock, then save it."""
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOCK_FILE, "a+b") as lock_handle:
        os.chmod(LOCK_FILE, 0o600)
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        try:
            try:
                state = json.loads(STATE_FILE.read_text(encoding="utf-8"))
                if not isinstance(state, dict):
                    state = {}
            except (OSError, ValueError):
                state = {}
            state = _prune(state, time.time())
            yield state
            temp_file = STATE_FILE.with_name(STATE_FILE.name + ".tmp")
            descriptor = os.open(temp_file, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(state, handle)
            os.replace(temp_file, STATE_FILE)
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def register_attempt(ip_address: str, username: str) -> int:
    """Record a login attempt unless a limit is already reached.

    Returns 0 when the attempt may proceed (and has been recorded), or the
    number of seconds to wait when the IP or the username is over its limit
    (nothing is recorded in that case).
    """
    try:
        with _locked_state() as state:
            now = time.time()
            keys = [_ip_key(ip_address), _user_key(username)]
            wait = max(_retry_after(state, key, now) for key in keys)
            if wait:
                return wait
            for key in keys:
                state.setdefault(key, []).append(now)
            return 0
    except OSError as error:
        logger.error("Login rate limiter unavailable, attempt not limited: %s", error)
        return 0


def retry_after(ip_address: str, username: str = "") -> int:
    """Seconds until the IP (and username, when given) may try again; 0 if not limited."""
    try:
        with _locked_state() as state:
            now = time.time()
            keys = [_ip_key(ip_address)]
            if username:
                keys.append(_user_key(username))
            return max(_retry_after(state, key, now) for key in keys)
    except OSError as error:
        logger.error("Login rate limiter unavailable: %s", error)
        return 0


def remaining_attempts(ip_address: str, username: str) -> int:
    """Attempts left before either the IP or the username limit is reached."""
    try:
        with _locked_state() as state:
            ip_left = MAX_ATTEMPTS_PER_IP - len(state.get(_ip_key(ip_address), []))
            user_left = MAX_ATTEMPTS_PER_USER - len(state.get(_user_key(username), []))
            return max(0, min(ip_left, user_left))
    except OSError as error:
        logger.error("Login rate limiter unavailable: %s", error)
        return MAX_ATTEMPTS_PER_IP


def clear(ip_address: str, username: str) -> None:
    """Forget the attempts of an IP and a username after a successful login."""
    try:
        with _locked_state() as state:
            state.pop(_ip_key(ip_address), None)
            state.pop(_user_key(username), None)
    except OSError as error:
        logger.error("Login rate limiter unavailable: %s", error)
