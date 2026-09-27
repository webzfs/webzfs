"""Custom VDEV Properties service (issue 113).

Reads pool topology and ``webzfs:*`` user properties from ZFS with
``zpool get ... all-vdevs``, writes single properties with ``zpool set``, and
discovers hardware identity for leaf vdevs with ``smartctl -i``. ZFS is the
only source of truth; nothing is cached on disk.
"""

import logging
import os
import re
import shutil
import subprocess
from datetime import date, timedelta
from pathlib import Path

from config.settings import settings
from core.vdev_property_catalog import (
    BY_KEY, NAMESPACE, SCOPE_LEAF, SCOPE_POOL, SCOPE_VDEV, PropertyDefinition,
    PropertyValidationError, definitions_for_scope, discoverable_keys, validate_value,
)
from services.audit_logger import audit_logger
from services.utils import run_privileged_command, run_zfs_command

logger = logging.getLogger(__name__)

ALL_POOLS = "all"
POOL_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9_.:-]*$")
VDEV_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_./:@+-]*$")
STATE_NAMES = {"0": "UNKNOWN", "1": "CLOSED", "2": "OFFLINE", "3": "REMOVED", "4": "CANT_OPEN",
               "5": "FAULTED", "6": "DEGRADED", "7": "ONLINE"}
WARRANTY_WARNING_DAYS = 90
SMARTCTL_PATHS = ["/usr/sbin/smartctl", "/usr/local/sbin/smartctl", "/usr/bin/smartctl", "/usr/local/bin/smartctl"]
DISCOVERY_TIMEOUT = 20


class VdevPropertyError(Exception):
    """User-facing failure reading or writing vdev properties."""


def validate_pool_name(pool: str) -> None:
    if not pool or not POOL_NAME.match(pool):
        raise VdevPropertyError("Invalid pool name.")


def validate_vdev_name(vdev: str) -> None:
    if not vdev or not VDEV_NAME.match(vdev) or vdev.startswith("-"):
        raise VdevPropertyError("Invalid vdev name.")


def timeout_for(operation: str) -> int:
    return settings.ZPOOL_TIMEOUTS.get(operation, settings.ZPOOL_TIMEOUTS["default"])


def command(argv: list[str], timeout: float) -> str:
    try:
        return run_zfs_command(argv, timeout=timeout, env={**os.environ, "LC_ALL": "C"}).stdout
    except subprocess.CalledProcessError as error:
        raise VdevPropertyError((error.stderr or str(error)).strip()) from error
    except (OSError, subprocess.TimeoutExpired) as error:
        raise VdevPropertyError(str(error)) from error


def list_pools() -> list[str]:
    output = command(["zpool", "list", "-H", "-o", "name"], timeout_for("list"))
    return [line.strip() for line in output.splitlines() if line.strip()]


def read_all_vdev_properties(pool: str) -> dict[str, dict[str, tuple[str, str]]]:
    """Return {vdev_name: {property: (value, source)}} for every vdev in the pool."""
    output = command(["zpool", "get", "-Hp", "-o", "name,property,value,source", "all", pool, "all-vdevs"],
                     timeout_for("properties"))
    table: dict[str, dict[str, tuple[str, str]]] = {}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) != 4:
            continue
        table.setdefault(parts[0], {})[parts[1]] = (parts[2], parts[3])
    if "root-0" not in table:
        raise VdevPropertyError(f"Unable to read vdev properties for pool {pool}.")
    return table


def native(props: dict[str, tuple[str, str]], key: str) -> str:
    value = props.get(key, ("-", "-"))[0]
    return "" if value == "-" else value


def custom_values(props: dict[str, tuple[str, str]], scope: str) -> dict[str, str]:
    values = {}
    for definition in definitions_for_scope(scope):
        value = props.get(definition.key, ("", ""))[0]
        values[definition.key] = "" if value == "-" else value
    return values


def selection_token(kind: str, pool: str, name: str = "") -> str:
    return f"{kind}|{pool}|{name or kind}"


def parse_selection_token(token: str) -> tuple[str, str, str] | None:
    parts = token.split("|", 2)
    if len(parts) != 3:
        return None
    kind, pool, name = parts
    if kind not in {"pool", "vdev", "leaf"} or not pool:
        return None
    return kind, pool, name


def selected_pool_name(selected: str) -> str:
    parsed = parse_selection_token(selected)
    return parsed[1] if parsed else ""


def aggregate_summary(topologies: list[dict]) -> dict:
    metrics = {
        "leaf_count": 0,
        "covered": 0,
        "missing": 0,
        "warranty_alerts": 0,
        "discoverable_fields": len(discoverable_keys()),
    }
    counts: dict[str, int] = {}
    for topology in topologies:
        for key in ("leaf_count", "covered", "missing", "warranty_alerts"):
            metrics[key] += topology["metrics"][key]
        for group in topology["groups"]:
            counts[group["section"]] = counts.get(group["section"], 0) + 1
    summary_parts = [f"{len(topologies)} pool{'s' if len(topologies) != 1 else ''}"]
    if counts.get("data"):
        summary_parts.append(f"{counts['data']} data vdev{'s' if counts['data'] != 1 else ''}")
    summary_parts.extend(f"{counts[section]} {section}" for section in ("special", "dedup", "log", "cache", "spare") if counts.get(section))
    return {"topology_summary": " / ".join(summary_parts), "metrics": metrics}


def display_type(vdev_name: str, is_group: bool) -> str:
    if not is_group:
        return "disk"
    return vdev_name.rsplit("-", 1)[0] if "-" in vdev_name else vdev_name


def leaf_alert(values: dict[str, str], today: date) -> tuple[str, str]:
    """Return (badge class suffix, label) describing metadata and warranty state."""
    if not values.get("webzfs:disk.serial") or not values.get("webzfs:disk.model"):
        return "info", "Missing metadata"
    expiry = values.get("webzfs:disk.warranty_expiry", "")
    if expiry:
        try:
            expiry_date = date.fromisoformat(expiry)
        except ValueError:
            return "warning", "Invalid warranty date"
        if expiry_date < today:
            return "danger", "Warranty expired"
        if expiry_date <= today + timedelta(days=WARRANTY_WARNING_DAYS):
            return "warning", "Warranty soon"
    return "success", "Healthy"


def section_map(pool: str) -> dict[str, str]:
    """Map vdev group and device names to their zpool status section (data, log, cache, spare, special, dedup)."""
    from services.zfs_pool import ZFSPoolService

    sections: dict[str, str] = {}
    try:
        topology = ZFSPoolService().get_pool_topology(pool)
    except Exception as error:
        logger.warning("Could not read zpool status sections for %s: %s", pool, error)
        return sections
    for key, section in (("data_vdevs", "data"), ("log_vdevs", "log"), ("cache_vdevs", "cache"),
                         ("spare_vdevs", "spare"), ("special_vdevs", "special"), ("dedup_vdevs", "dedup")):
        for group in topology.get(key, []):
            sections[group["name"]] = section
            for device in group.get("devices", []):
                sections[device["name"]] = section
    return sections


def build_topology(pool: str, today: date | None = None) -> dict:
    """Build the page model: pool node, ordered top-level groups, leaf rows, and summary metrics."""
    validate_pool_name(pool)
    today = today or date.today()
    table = read_all_vdev_properties(pool)
    sections = section_map(pool)
    root = table["root-0"]

    groups: dict[str, dict] = {}
    order: list[str] = []
    leaves: list[dict] = []

    def ensure_group(name: str, is_real_group: bool, props: dict[str, tuple[str, str]]) -> dict:
        if name not in groups:
            groups[name] = {
                "name": name, "kind": "vdev", "pool": pool,
                "token": selection_token("vdev", pool, name), "pool_token": selection_token("pool", pool),
                "is_group": is_real_group,
                "type": display_type(name, is_real_group) if is_real_group else "single",
                "section": sections.get(name, "data"), "state": STATE_NAMES.get(native(props, "state"), "UNKNOWN"),
                "props": custom_values(props, SCOPE_VDEV) if is_real_group else {}, "leaves": [],
            }
            order.append(name)
        return groups[name]

    def top_level_parent(name: str, depth: int = 0) -> str:
        parent = native(table.get(name, {}), "parent")
        if not parent or parent == pool or depth > 8:
            return name
        return top_level_parent(parent, depth + 1)

    for name, props in table.items():
        if name == "root-0":
            continue
        is_group = native(props, "numchildren") not in ("", "0")
        parent = native(props, "parent")
        if is_group:
            if parent == pool or not parent:
                ensure_group(name, True, props)
            continue
        owner_name = top_level_parent(name)
        if owner_name == name:
            group = ensure_group(name, False, props)
        else:
            group = ensure_group(owner_name, True, table.get(owner_name, {}))
        values = custom_values(props, SCOPE_LEAF)
        alert_class, alert_label = leaf_alert(values, today)
        leaf = {
            "name": name, "kind": "leaf", "pool": pool,
            "token": selection_token("leaf", pool, name), "pool_token": selection_token("pool", pool),
            "group": owner_name, "path": native(props, "path"),
            "physpath": native(props, "physpath"), "devid": native(props, "devid"),
            "state": STATE_NAMES.get(native(props, "state"), "UNKNOWN"),
            "section": sections.get(name, group["section"]), "props": values,
            "alert_class": alert_class, "alert_label": alert_label,
            "missing": alert_class == "info", "warranty_alert": alert_label.startswith("Warranty"),
        }
        group["leaves"].append(leaf)
        leaves.append(leaf)

    section_order = {"data": 0, "special": 1, "dedup": 2, "log": 3, "cache": 4, "spare": 5}
    ordered_groups = sorted((groups[name] for name in order), key=lambda g: (section_order.get(g["section"], 9), order.index(g["name"])))
    for group in ordered_groups:
        group["leaves"].sort(key=lambda leaf: leaf["name"])

    counts: dict[str, int] = {}
    for group in ordered_groups:
        counts[group["section"]] = counts.get(group["section"], 0) + 1
    summary_parts = [f"{counts['data']} data vdev{'s' if counts['data'] != 1 else ''}"] if counts.get("data") else []
    summary_parts += [f"{counts[s]} {s}" for s in ("special", "dedup", "log", "cache", "spare") if counts.get(s)]

    covered = sum(1 for leaf in leaves if not leaf["missing"])
    return {
        "pool": {"name": pool, "kind": "pool", "pool": pool,
                 "token": selection_token("pool", pool), "pool_token": selection_token("pool", pool),
                 "props": custom_values(root, SCOPE_POOL),
                 "state": STATE_NAMES.get(native(root, "state"), "UNKNOWN")},
        "groups": ordered_groups,
        "leaves": leaves,
        "topology_summary": " / ".join(summary_parts) if summary_parts else "No vdevs",
        "metrics": {
            "leaf_count": len(leaves), "covered": covered,
            "missing": len(leaves) - covered,
            "warranty_alerts": sum(1 for leaf in leaves if leaf["warranty_alert"]),
            "discoverable_fields": len(discoverable_keys()),
        },
    }


def find_object(topology: dict, selected: str) -> dict:
    """Resolve a selection token (``pool``, a group name, or a leaf name) to its node."""
    parsed = parse_selection_token(selected)
    if not selected or selected == "pool":
        return topology["pool"]
    if parsed and parsed[1] != topology["pool"]["name"]:
        raise VdevPropertyError(f"Selection {selected} is not part of this pool.")
    if parsed and parsed[0] == "pool":
        return topology["pool"]
    for group in topology["groups"]:
        if selected in {group["name"], group["token"]} and group["is_group"]:
            return group
        for leaf in group["leaves"]:
            if selected in {leaf["name"], leaf["token"]}:
                return leaf
    raise VdevPropertyError(f"Selection {selected} is not part of this pool.")


def build_topologies(pools: list[str], selected: str, today: date | None = None) -> tuple[list[dict], dict]:
    """Build one topology per pool and resolve the selected object across all pools."""
    topologies = [build_topology(pool, today=today) for pool in pools]
    chosen = topologies[0]["pool"] if topologies else {}
    selected_pool = selected_pool_name(selected)
    for topology in topologies:
        topology["selected"] = topology["pool"]["token"]
        if selected_pool and selected_pool != topology["pool"]["name"]:
            continue
        try:
            chosen = find_object(topology, selected)
            topology["selected"] = chosen["token"]
            break
        except VdevPropertyError:
            continue
    return topologies, chosen


def scope_for(node: dict) -> str:
    return {"pool": SCOPE_POOL, "vdev": SCOPE_VDEV, "leaf": SCOPE_LEAF}[node["kind"]]


def zpool_target(pool: str, node: dict) -> list[str]:
    """Return the trailing argv for ``zpool set``: pool plus vdev name unless the pool itself is targeted."""
    if node["kind"] == "pool":
        return [pool, "root-0"]
    return [pool, node["name"]]


def definition_for(key: str, node: dict) -> PropertyDefinition:
    definition = BY_KEY.get(key)
    if definition is None or not key.startswith(NAMESPACE):
        raise VdevPropertyError(f"Property {key} is not part of the WebZFS catalog.")
    if definition.object_scope != scope_for(node):
        raise VdevPropertyError(f"Property {key} does not apply to this {node['kind']}.")
    return definition


def set_custom_property(username: str, pool: str, node: dict, key: str, value: str) -> str:
    """Set or clear one catalog property on the selected object. Returns the argv preview."""
    validate_pool_name(pool)
    definition = definition_for(key, node)
    try:
        normalized = validate_value(definition, value)
    except PropertyValidationError as error:
        raise VdevPropertyError(str(error)) from error
    argv = ["zpool", "set", f"{key}={normalized}", *zpool_target(pool, node)]
    try:
        command(argv, timeout_for("properties"))
    except VdevPropertyError:
        audit_logger.log_zfs_operation(username, "vdev_property_set", success=False, pool=pool,
                                       vdev=node.get("name", "root-0"), property=key)
        raise
    audit_logger.log_zfs_operation(username, "vdev_property_set" if normalized else "vdev_property_clear",
                                   pool=pool, vdev=node.get("name", "root-0"), property=key)
    return " ".join(argv)


def clear_custom_property(username: str, pool: str, node: dict, key: str) -> str:
    return set_custom_property(username, pool, node, key, "")


def apply_form(username: str, pool: str, node: dict, submitted: dict[str, str]) -> list[str]:
    """Write every changed catalog value for the node. Unchanged values are skipped."""
    changes = []
    current = node.get("props", {})
    for definition in definitions_for_scope(scope_for(node)):
        if definition.form_name not in submitted:
            continue
        try:
            new_value = validate_value(definition, submitted[definition.form_name])
        except PropertyValidationError as error:
            raise VdevPropertyError(str(error)) from error
        if new_value != current.get(definition.key, ""):
            changes.append(set_custom_property(username, pool, node, definition.key, new_value))
    return changes


def find_smartctl() -> str | None:
    path = shutil.which("smartctl")
    if path:
        return path
    for candidate in SMARTCTL_PATHS:
        if Path(candidate).exists():
            return candidate
    return None


def base_device(path: str) -> str:
    """Strip partition suffixes so smartctl targets the whole device (/dev/sda1 -> /dev/sda, nvme0n1p1 -> nvme0n1)."""
    try:
        resolved = os.path.realpath(path) if path else path
    except OSError:
        resolved = path
    for pattern in (r"^(.*nvme\d+n\d+)p\d+$", r"^(.*(?:ada|da|vtbd|nda|nvd)\d+)(?:p|s)\d+[a-z]?$",
                    r"^(.*(?:wd|sd|ld)\d+)[a-p]$", r"^(.*/(?:sd|hd|vd|xvd)[a-z]+)\d+$", r"^(.*/mmcblk\d+)p\d+$"):
        match = re.match(pattern, resolved)
        if match:
            return match.group(1)
    return resolved


def parse_smartctl_info(output: str) -> dict[str, str]:
    """Extract discoverable catalog values from ``smartctl -i`` output (ATA, SCSI/SAS, and NVMe layouts)."""
    fields: dict[str, str] = {}
    vendor = ""
    transport = ""
    rotation = ""
    for line in output.splitlines():
        if ":" not in line:
            continue
        label, value = (part.strip() for part in line.split(":", 1))
        lowered = label.lower()
        if not value:
            continue
        if lowered in ("device model", "model number", "product"):
            fields.setdefault("webzfs:disk.model", value)
        elif lowered == "vendor":
            vendor = value
        elif lowered == "serial number":
            fields["webzfs:disk.serial"] = value
        elif lowered in ("lu wwn device id", "logical unit id"):
            fields["webzfs:disk.wwn"] = value.replace(" ", "").removeprefix("0x")
        elif lowered == "ieee eui-64":
            fields["webzfs:disk.wwn"] = "eui." + value.replace(" ", "")
        elif lowered in ("firmware version", "revision"):
            fields["webzfs:disk.firmware"] = value
        elif lowered == "transport protocol":
            transport = value
        elif lowered == "sata version is":
            transport = "SATA"
        elif lowered in ("nvme version", "total nvm capacity"):
            transport = transport or "NVME"
        elif lowered == "rotation rate":
            rotation = value
        elif lowered == "form factor":
            fields["webzfs:disk.formfactor"] = value
    model = fields.get("webzfs:disk.model", "")
    if vendor and model and not model.lower().startswith(vendor.lower()):
        fields["webzfs:disk.model"] = f"{vendor} {model}"
    elif vendor and not model:
        fields["webzfs:disk.model"] = vendor
    upper = transport.upper()
    if "SAS" in upper:
        fields["webzfs:disk.interface"] = "SAS"
    elif "ATA" in upper:
        fields["webzfs:disk.interface"] = "SATA"
    elif "NVME" in upper:
        fields["webzfs:disk.interface"] = "NVME"
    if "webzfs:disk.formfactor" not in fields and rotation.lower().startswith("solid state"):
        fields["webzfs:disk.formfactor"] = "SSD"
    return fields


def run_smartctl(smartctl: str, device: str, extra: list[str]) -> str:
    try:
        result = run_privileged_command([smartctl, *extra, "-i", device], check=False, timeout=DISCOVERY_TIMEOUT,
                                        env={**os.environ, "LC_ALL": "C"})
    except (OSError, subprocess.TimeoutExpired) as error:
        raise VdevPropertyError(f"smartctl failed for {device}: {error}") from error
    return result.stdout or ""


def discover_leaf_metadata(leaf: dict) -> dict[str, str]:
    """Run ``smartctl -i`` against the leaf's device and return discoverable catalog values plus ``_device``."""
    if leaf["kind"] != "leaf":
        raise VdevPropertyError("Discovery is only available for leaf vdevs.")
    smartctl = find_smartctl()
    if not smartctl:
        raise VdevPropertyError("smartctl not found. Install smartmontools to enable discovery.")
    path = leaf.get("path") or ""
    if not path.startswith("/dev/"):
        raise VdevPropertyError(f"Leaf {leaf['name']} has no resolvable block device path.")
    device = base_device(path)
    fields = parse_smartctl_info(run_smartctl(smartctl, device, []))
    if not fields:
        # USB bridges often need the SAT device type, matching the health analysis probe.
        fields = parse_smartctl_info(run_smartctl(smartctl, device, ["-d", "sat"]))
    if not fields:
        raise VdevPropertyError(f"smartctl could not identify {device}.")
    fields["_device"] = device
    return fields


def discovery_diff(leaf: dict, discovered: dict[str, str]) -> list[dict]:
    """Compare saved and discovered values. Action is unchanged, fill, review, or unavailable."""
    rows = []
    for key in discoverable_keys():
        definition = BY_KEY[key]
        saved = leaf["props"].get(key, "")
        found = discovered.get(key, "")
        if not found:
            action = "unavailable"
        elif not saved:
            action = "fill"
        elif saved == found:
            action = "unchanged"
        else:
            action = "review"
        rows.append({"key": key, "label": definition.label, "form_name": definition.form_name,
                     "saved": saved, "discovered": found, "action": action})
    return rows


def bulk_discovery_plan(topologies: list[dict]) -> tuple[list[dict], list[dict]]:
    """Probe every leaf across one or more topologies and return (fill rows, per-leaf errors)."""
    plan = []
    errors = []
    for topology in topologies:
        for leaf in topology["leaves"]:
            try:
                discovered = discover_leaf_metadata(leaf)
            except VdevPropertyError as error:
                errors.append({"pool": topology["pool"]["name"], "leaf": leaf["name"], "error": str(error)})
                continue
            for row in discovery_diff(leaf, discovered):
                if row["action"] == "fill":
                    plan.append({"pool": topology["pool"]["name"], "leaf": leaf["name"], **row})
    return plan, errors


def apply_bulk_plan(username: str, topologies: list[dict], selections: list[tuple[str, str, str, str]]) -> int:
    """Write (pool, leaf, key, value) selections, filling blank values only. Returns the write count."""
    written = 0
    for pool, leaf_name, key, value in selections:
        topology = next((item for item in topologies if item["pool"]["name"] == pool), None)
        if topology is None:
            continue
        token = selection_token("leaf", pool, leaf_name)
        leaf = find_object(topology, token)
        if leaf["kind"] != "leaf" or leaf["props"].get(key, ""):
            continue
        set_custom_property(username, pool, leaf, key, value)
        written += 1
    return written
