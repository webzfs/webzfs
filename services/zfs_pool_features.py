"""Pool version and feature flag report.

Reads the on-disk format version and the ``feature@`` states of a pool with
``zpool get``, and the feature descriptions known to the installed OpenZFS
with ``zpool upgrade -v``. Nothing here changes a pool: enabling features is a
one-way operation and stays on the command line.
"""

import logging
import re

from services.utils import run_zfs_command

logger = logging.getLogger(__name__)

POOL_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")
FEATURE_FLAGS_VERSION = "5000"
COMMAND_TIMEOUT = 15
ENABLED_STATES = {"enabled", "active"}
# Marker `zpool upgrade` puts after features that `zpool upgrade <pool>` skips.
MANUAL_MARKER = "(*)"


def run(argv: list[str]) -> str:
    """Return stdout of a read-only zpool command, or an empty string on failure."""
    try:
        result = run_zfs_command(argv, check=False, timeout=COMMAND_TIMEOUT)
    except Exception as error:
        logger.warning("%s failed: %s", " ".join(argv), error)
        return ""
    if result.returncode != 0:
        return ""
    return result.stdout or ""


def parse_supported_features(output: str) -> dict[str, dict]:
    """Parse the feature table of ``zpool upgrade -v``.

    Each feature is a name line (optionally followed by
    ``(read-only compatible)``) and an indented description line.
    """
    features: dict[str, dict] = {}
    in_table = False
    current = ""
    for line in output.splitlines():
        if line.startswith("FEAT "):
            in_table = True
            continue
        if not in_table or line.startswith("---"):
            continue
        if line.startswith("The following legacy versions"):
            break
        if not line.strip():
            continue
        if line[0].isspace():
            if current:
                features[current]["description"] = line.strip()
            continue
        current = line.split()[0]
        features[current] = {"description": "", "read_only_compatible": "(read-only compatible)" in line}
    return features


def parse_manual_features(output: str) -> set[str]:
    """Return features that ``zpool upgrade`` lists with the ``(*)`` marker.

    Those are not enabled by ``zpool upgrade <pool>`` and need an explicit
    ``zpool set feature@<name>=enabled <pool>``.
    """
    manual = set()
    for line in output.splitlines():
        name = line.strip()
        if line[:1].isspace() and name.endswith(MANUAL_MARKER):
            manual.add(name[:-len(MANUAL_MARKER)])
    return manual


def read_pool_properties(pool: str) -> dict[str, str]:
    """Return every pool property as {name: value}. Empty when the pool cannot be queried."""
    properties = {}
    for line in run(["zpool", "get", "-H", "-o", "property,value", "all", pool]).splitlines():
        name, separator, value = line.partition("\t")
        if separator:
            properties[name.strip()] = value.strip()
    return properties


def pool_feature_report(pool: str) -> dict | None:
    """Describe the pool format version and the features an upgrade would enable.

    Returns None when the pool cannot be queried, so callers can hide the
    report instead of showing wrong data.
    """
    if not pool or not POOL_NAME.match(pool):
        return None
    properties = read_pool_properties(pool)
    if not properties:
        return None

    states = {name[len("feature@"):]: value.lower() for name, value in properties.items()
              if name.startswith("feature@")}
    supported = parse_supported_features(run(["zpool", "upgrade", "-v"]))
    manual = parse_manual_features(run(["zpool", "upgrade"]))

    # Feature flag pools report "-" for the version property; legacy pools report 1 to 28.
    version = properties.get("version", "-")
    legacy = version.isdigit() and version != FEATURE_FLAGS_VERSION

    available = []
    for name in sorted(name for name, state in states.items() if state not in ENABLED_STATES):
        details = supported.get(name, {})
        available.append({
            "name": name,
            "description": details.get("description", ""),
            "read_only_compatible": details.get("read_only_compatible", False),
            "manual": name in manual,
        })

    compatibility = properties.get("compatibility", "off")
    bootfs = properties.get("bootfs", "-")
    return {
        "pool": pool,
        "legacy": legacy,
        "version": version if legacy else FEATURE_FLAGS_VERSION,
        "version_label": f"Legacy version {version}" if legacy else "Feature flags (version 5000)",
        "enabled_count": sum(1 for state in states.values() if state in ENABLED_STATES),
        "feature_count": len(states),
        "available": available,
        "has_manual": any(feature["manual"] for feature in available),
        "can_upgrade": legacy or any(not feature["manual"] for feature in available),
        "compatibility": "" if compatibility in ("off", "-") else compatibility,
        "bootfs": "" if bootfs == "-" else bootfs,
        "upgrade_command": f"zpool upgrade {pool}",
    }
