"""Configuration and authorization helpers for the WebZFS terminal."""

import os
import pwd
from dataclasses import asdict, dataclass

from config.settings import settings


def _parse_users(value: str) -> frozenset[str]:
    return frozenset(user.strip() for user in value.split(",") if user.strip())


def shell_allowed_users() -> frozenset[str]:
    """Return PAM usernames allowed to open the native WebZFS terminal."""
    return _parse_users(settings.SHELL_ALLOWED_USERS)


def shell_recording_audit_users() -> frozenset[str]:
    """Return PAM usernames allowed to inspect terminal recordings."""
    return _parse_users(settings.SHELL_RECORDING_AUDIT_USERS)


def can_use_shell(username: str) -> bool:
    """Return whether a PAM username may open the native terminal."""
    return username in shell_allowed_users()


def can_audit_shell_recordings(username: str) -> bool:
    """Return whether a PAM username may inspect terminal recordings."""
    return username in shell_recording_audit_users()


@dataclass(frozen=True)
class ProcessIdentity:
    username: str
    uid: int
    gid: int
    home: str
    shell: str


def get_process_identity() -> ProcessIdentity:
    """Describe the operating-system account that will own terminal processes."""
    uid = os.geteuid()
    account = pwd.getpwuid(uid)
    shell = (
        account.pw_shell
        if account.pw_shell and os.access(account.pw_shell, os.X_OK)
        else "/bin/sh"
    )
    home = (
        account.pw_dir
        if account.pw_dir and os.path.isdir(account.pw_dir)
        else os.getcwd()
    )
    return ProcessIdentity(
        username=account.pw_name,
        uid=uid,
        gid=account.pw_gid,
        home=home,
        shell=shell,
    )


def shell_status(username: str) -> dict:
    """Return template-friendly effective terminal configuration."""
    identity = get_process_identity()
    return {
        "allowed": can_use_shell(username),
        "allowed_users": sorted(shell_allowed_users()),
        "audit_allowed": can_audit_shell_recordings(username),
        "audit_users": sorted(shell_recording_audit_users()),
        "idle_timeout_seconds": settings.SHELL_IDLE_TIMEOUT_SECONDS,
        "recording_enabled": settings.SHELL_RECORDING_ENABLED,
        "recording_retention_days": settings.SHELL_RECORDING_RETENTION_DAYS,
        "recording_max_session_mib": settings.SHELL_RECORDING_MAX_SESSION_MIB,
        "recording_max_total_mib": settings.SHELL_RECORDING_MAX_TOTAL_MIB,
        "identity": asdict(identity),
    }
