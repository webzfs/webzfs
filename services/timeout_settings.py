"""
Session Timeout Settings Service
Handles reading and writing user-configured auto-logout timeout for the WebZFS UI.

The session timeout override is stored in ~/.config/webzfs/session_timeout.json.
When no override exists, the default from AUTH_SESSION_EXPIRES_SECONDS in
config/settings/base.py is used (3600 seconds / 1 hour).
"""
import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Config file path follows project convention: ~/.config/webzfs/
CONFIG_DIR = Path.home() / ".config" / "webzfs"
SESSION_TIMEOUT_FILE = CONFIG_DIR / "session_timeout.json"

# Default session timeout in seconds (must match config/settings/base.py)
DEFAULT_SESSION_TIMEOUT = 3600

# Preset choices for the UI dropdown (value in seconds, label)
SESSION_TIMEOUT_PRESETS = [
    (300, "5 minutes"),
    (900, "15 minutes"),
    (1800, "30 minutes"),
    (3600, "1 hour"),
    (7200, "2 hours"),
    (14400, "4 hours"),
    (28800, "8 hours"),
    (43200, "12 hours"),
    (86400, "1 day"),
    (172800, "2 days"),
    (604800, "7 days"),
]

# Set of valid preset values for validation
VALID_TIMEOUT_VALUES = {value for value, _ in SESSION_TIMEOUT_PRESETS}


def load_session_timeout() -> Optional[int]:
    """
    Load user-configured session timeout override from the config file.

    Returns:
        The override value in seconds, or None if no override file exists.
    """
    if not SESSION_TIMEOUT_FILE.exists():
        return None

    try:
        raw = SESSION_TIMEOUT_FILE.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            logger.warning("Session timeout config is not a JSON object, ignoring.")
            return None

        value = data.get("session_timeout")
        if value is None:
            return None

        int_value = int(value)
        if int_value in VALID_TIMEOUT_VALUES:
            return int_value
        else:
            logger.warning(
                "Session timeout value (%d) is not a recognized preset, ignoring.",
                int_value,
            )
            return None

    except (json.JSONDecodeError, OSError, ValueError, TypeError) as exc:
        logger.warning("Failed to read session timeout config: %s", exc)
        return None


def get_effective_session_timeout() -> int:
    """
    Return the effective session timeout in seconds.

    Uses the user override if present and valid, otherwise the default.
    """
    override = load_session_timeout()
    if override is not None:
        return override
    return DEFAULT_SESSION_TIMEOUT


def save_session_timeout(seconds: int) -> None:
    """
    Save a session timeout override to the config file.

    Only accepts values that match a recognized preset.

    Args:
        seconds: Timeout value in seconds.

    Raises:
        ValueError: If the value is not a recognized preset.
        OSError: If the config directory or file cannot be written.
    """
    int_seconds = int(seconds)
    if int_seconds not in VALID_TIMEOUT_VALUES:
        raise ValueError(f"Invalid session timeout value: {int_seconds}")

    CONFIG_DIR.mkdir(parents=True, exist_ok=True)

    data = {"session_timeout": int_seconds}
    SESSION_TIMEOUT_FILE.write_text(
        json.dumps(data, indent=2) + "\n",
        encoding="utf-8",
    )
    logger.info("Saved session timeout override: %d seconds", int_seconds)


def reset_session_timeout() -> None:
    """
    Remove the session timeout override, restoring the default.
    """
    if SESSION_TIMEOUT_FILE.exists():
        SESSION_TIMEOUT_FILE.unlink()
        logger.info("Session timeout override removed, default restored.")


def format_timeout_display(seconds: int) -> str:
    """
    Format a timeout value in seconds to a human-readable string.
    """
    if seconds < 3600:
        minutes = seconds // 60
        return f"{minutes} minute{'s' if minutes != 1 else ''}"
    elif seconds < 86400:
        hours = seconds // 3600
        return f"{hours} hour{'s' if hours != 1 else ''}"
    else:
        days = seconds // 86400
        return f"{days} day{'s' if days != 1 else ''}"
