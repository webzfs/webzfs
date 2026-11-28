"""
Login restriction policy.

By default (RESTRICT_LOGIN unset or "no") any account that PAM accepts may
log in, which is the original WebZFS behavior.

With RESTRICT_LOGIN=yes in .env, an account may log in only if:

- its username is listed in LOGIN_ALLOWED_USERS, or
- it is a member (primary or supplementary group) of a group listed in
  LOGIN_ALLOWED_GROUPS.

Both settings are comma-separated. Defaults: LOGIN_ALLOWED_USERS=root and
LOGIN_ALLOWED_GROUPS=wheel,webzfs-admin. The installers create an empty
webzfs-admin group; add administrators to it with the operating system's
own tools.

The policy is checked at login and again on every authenticated request,
so removing someone from a listed group (or from the list) ends their
existing sessions within GROUP_CACHE_SECONDS. Group membership is read with
os.getgrouplist, so LDAP or SSSD accounts work. If membership cannot be
read, access is denied.
"""
import grp
import logging
import os
import pwd
import time

from config.settings import settings

logger = logging.getLogger(__name__)

GROUP_CACHE_SECONDS = 60

# username -> (expires_at, allowed)
_group_cache: dict = {}


def _names(value: str) -> set:
    return {name.strip() for name in value.split(",") if name.strip()}


def restriction_enabled() -> bool:
    return bool(settings.RESTRICT_LOGIN)


def allowed_users() -> set:
    return _names(settings.LOGIN_ALLOWED_USERS)


def allowed_groups() -> set:
    return _names(settings.LOGIN_ALLOWED_GROUPS)


def _group_names_of(username: str) -> set:
    """Names of every group the account belongs to (primary and supplementary)."""
    entry = pwd.getpwnam(username)
    names = set()
    for gid in os.getgrouplist(username, entry.pw_gid):
        try:
            names.add(grp.getgrgid(gid).gr_name)
        except KeyError:
            continue
    return names


def _in_allowed_group(username: str) -> bool:
    groups = allowed_groups()
    if not groups:
        return False
    now = time.monotonic()
    cached = _group_cache.get(username)
    if cached and cached[0] > now:
        return cached[1]
    try:
        allowed = bool(_group_names_of(username) & groups)
    except (KeyError, OSError, ValueError) as error:
        # KeyError: no such account. ValueError: pwd.getpwnam refuses a name
        # with an embedded NUL. Both mean the account cannot be allowed.
        logger.warning("Login policy: cannot read groups of %r, access denied: %s", username, error)
        allowed = False
    _group_cache[username] = (now + GROUP_CACHE_SECONDS, allowed)
    return allowed


def is_login_allowed(username: str) -> bool:
    """Return True if this PAM-authenticated account may use WebZFS."""
    if not restriction_enabled():
        return True
    if username in allowed_users():
        return True
    return _in_allowed_group(username)


def log_policy_at_startup() -> None:
    """Log the active login policy once when the application starts."""
    if not restriction_enabled():
        logger.info(
            "Login restriction is off (RESTRICT_LOGIN=no): any account PAM accepts may log in"
        )
        return
    users = sorted(allowed_users())
    groups = sorted(allowed_groups())
    if not users and not groups:
        logger.error(
            "RESTRICT_LOGIN=yes but LOGIN_ALLOWED_USERS and LOGIN_ALLOWED_GROUPS are both "
            "empty: nobody can log in"
        )
        return
    missing = []
    for group in groups:
        try:
            grp.getgrnam(group)
        except KeyError:
            missing.append(group)
    logger.info(
        "Login restriction is on: users %s, groups %s", users or "none", groups or "none"
    )
    if missing:
        logger.warning("Login policy groups that do not exist on this system: %s", missing)
