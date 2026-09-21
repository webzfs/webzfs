"""Live delegation reads and serialized, single-command mutations."""

from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import grp
import logging
import os
from pathlib import Path
import platform
import pwd
import subprocess
import time

from config.settings import settings
from core.zfs_delegation_parser import (
    DelegationError, TOKEN, fingerprint, parse_delegations, permission_tokens,
)
from core.zfs_delegation_permissions import addition_reason, detect_support
from services.audit_logger import audit_logger
from services.utils import run_zfs_command

logger = logging.getLogger(__name__)
LOCK_PATH = Path.home() / ".config" / "webzfs" / "zfs-delegation.lock"
SCAN_DATASETS = 64
SCAN_SECONDS = 10
SCOPES = {"local": ["-l"], "descendants": ["-d"], "both": []}
ACTIONS = {"grant", "add", "remove", "revoke", "set_create", "set_add", "set_remove", "set_delete", "pool"}


def delegation_allowed_users() -> frozenset[str]:
    return frozenset(name.strip() for name in settings.ZFS_DELEGATION_ALLOWED_USERS.split(",") if name.strip())


def can_manage_zfs_delegation(username: str) -> bool:
    return username in delegation_allowed_users()


def command(argv: list[str], timeout: float = 10) -> str:
    try:
        result = run_zfs_command(argv, timeout=timeout, env={**os.environ, "LC_ALL": "C"})
        return result.stdout
    except subprocess.CalledProcessError as error:
        raise DelegationError((error.stderr or str(error)).strip()) from error
    except (OSError, subprocess.TimeoutExpired) as error:
        raise DelegationError(str(error)) from error


def inventory() -> dict[str, str]:
    datasets = {}
    for line in command(["zfs", "list", "-H", "-o", "name,type", "-t", "filesystem,volume"]).splitlines():
        parts = line.split("\t")
        if len(parts) != 2 or parts[1] not in {"filesystem", "volume"} or parts[0].startswith("-"):
            raise DelegationError("Unable to parse dataset inventory.")
        datasets[parts[0]] = parts[1]
    return datasets


def read_entries(dataset: str, timeout: float = 10):
    output = command(["zfs", "allow", dataset], timeout)
    try:
        return parse_delegations(output, dataset)
    except DelegationError:
        logger.error("Delegation parse failure on %s. Native output: %r", dataset, output)
        raise


def principal_id(kind: str, name: str):
    if kind == "everyone":
        return "everyone"
    try:
        if kind == "user":
            entry = pwd.getpwuid(int(name)) if name.isdecimal() else pwd.getpwnam(name)
            return entry.pw_uid
        entry = grp.getgrgid(int(name)) if name.isdecimal() else grp.getgrnam(name)
        return entry.gr_gid
    except (KeyError, ValueError, OverflowError):
        return None


def load_state(dataset: str = "") -> dict:
    datasets = inventory()
    if not datasets:
        raise DelegationError("No filesystem or volume datasets are available.")
    dataset = dataset or next(iter(datasets))
    if dataset not in datasets:
        raise DelegationError("Select an existing filesystem or volume.")
    try:
        version_output = command(["zfs", "version"], 5)
    except DelegationError:
        version_output = ""
    support = detect_support(version_output, platform.system())
    pool = dataset.split("/", 1)[0]
    pool_enabled = command(["zpool", "get", "-H", "-o", "value", "delegation", pool]).strip()
    if pool_enabled not in {"on", "off"}:
        raise DelegationError("Unable to read the pool delegation property.")
    entries = read_entries(dataset)
    state = {
        "dataset": dataset, "datasets": datasets, "dataset_type": datasets[dataset],
        "pool": pool, "pool_enabled": pool_enabled, "entries": entries,
        "support": support, "version_output": version_output,
    }
    state["fingerprint"] = fingerprint([
        sorted((asdict(entry) for entry in entries), key=lambda entry: str(entry)),
        datasets[dataset], pool_enabled, asdict(support),
    ])
    return state


@contextmanager
def mutation_lock():
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise DelegationError("Another delegation change is running. Please retry.") from error
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def scan_references(source: str, name: str) -> dict:
    """Scan literal dependency edges, bounded by dataset count and elapsed time."""
    deadline = time.monotonic() + SCAN_SECONDS
    references = set()
    reason = ""
    try:
        datasets = command(["zfs", "list", "-r", "-H", "-o", "name", "-t", "filesystem,volume", source], 5).splitlines()
        if not datasets or any(item != source and not item.startswith(source + "/") for item in datasets):
            raise DelegationError("Unexpected reference-scan inventory.")
        if len(datasets) > SCAN_DATASETS:
            return {"references": [], "incomplete": f"Scan skipped: more than {SCAN_DATASETS} datasets."}
        for dataset in datasets:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DelegationError("Reference scan time limit reached.")
            for entry in read_entries(dataset, min(5, remaining)):
                if entry.source != dataset or (entry.kind == "set" and entry.principal == name and dataset == source):
                    continue
                if name in entry.tokens:
                    references.add(f"{dataset}: {entry.kind} {entry.principal} ({entry.scope})")
    except DelegationError as error:
        reason = f"Reference scan incomplete: {error}"
    return {"references": sorted(references), "incomplete": reason}


def validate_additions(tokens, state, scope, target_set=""):
    definitions = {}
    for entry in state["entries"]:
        if entry.kind == "set":
            definitions.setdefault(entry.principal, set()).update(entry.tokens)

    def check(token, seen):
        if token.startswith("@"):
            if token == target_set or token in seen or token not in definitions:
                raise DelegationError(f"Undefined or cyclic permission set: {token}")
            for member in definitions[token]:
                check(member, seen | {token})
        else:
            reason = addition_reason(token, state["support"], state["dataset_type"], scope)
            if reason:
                raise DelegationError(f"{token}: {reason}")
    for token in tokens:
        check(token, set())


def build_plan(state: dict, data: dict) -> dict:
    """Build argv exclusively from validated form choices and current native rows."""
    action = data.get("action", "")
    if action not in ACTIONS or not state["support"].writable:
        raise DelegationError(state["support"].reason or "Unknown delegation action.")
    dataset = state["dataset"]
    row = next((entry for entry in state["entries"] if entry.key == data.get("entry")), None)
    existing_action = action in {"add", "remove", "revoke", "set_add", "set_remove", "set_delete"}
    if existing_action and (row is None or row.source != dataset):
        raise DelegationError("The direct row no longer exists. Refresh the page.")
    plan = {"state": state["fingerprint"], "identity": None, "scan": None, "empties_set": False}
    if action == "pool":
        value = data.get("value")
        if value not in {"on", "off"} or value == state["pool_enabled"]:
            raise DelegationError("Select a different pool delegation value.")
        plan["argv"] = ["zpool", "set", f"delegation={value}", state["pool"]]
        return plan
    is_set = action.startswith("set_")
    if existing_action and ((row.kind == "set") != is_set or row.kind == "create_time"):
        raise DelegationError("This action does not match the selected row.")
    removal = action in {"remove", "revoke", "set_remove", "set_delete"}
    tokens = row.tokens if action in {"revoke", "set_delete"} else permission_tokens(data.get("permissions", ""))
    if removal and not set(tokens).issubset(row.tokens):
        raise DelegationError("A selected permission is no longer in this row.")
    if not tokens:
        raise DelegationError("An empty permission list cannot be submitted.")
    if is_set:
        name = row.principal if row else data.get("principal", "").strip()
        if not name.startswith("@") or not TOKEN.fullmatch(name) or len(name.encode()) > 64:
            raise DelegationError("Set names must start with @ and be at most 64 bytes.")
        if action == "set_create" and any(entry.kind == "set" and entry.principal == name and entry.source == dataset for entry in state["entries"]):
            raise DelegationError("This set already exists locally. Use Add Permissions.")
        scope, principal = "set", ["-s", name]
        if removal and set(tokens) == set(row.tokens):
            plan["empties_set"] = True
            plan["scan"] = scan_references(dataset, name)
            if plan["scan"]["references"]:
                raise DelegationError("Set is referenced by: " + "; ".join(plan["scan"]["references"]))
    else:
        kind = row.kind if row else data.get("kind", "")
        name = row.principal if row else data.get("principal", "").strip()
        scope = row.scope if row else data.get("scope", "")
        if kind not in {"user", "group", "everyone"} or scope not in SCOPES:
            raise DelegationError("Select a valid principal type and scope.")
        if not removal:
            if state["dataset_type"] == "volume" and scope != "local":
                raise DelegationError("New volume grants must use local scope.")
            if kind != "everyone" and (not name or name.isdecimal() or name.startswith("-") or "," in name or any(char.isspace() for char in name)):
                raise DelegationError("Additions require a named Unix account, not a numeric ID.")
            plan["identity"] = principal_id(kind, name)
            if plan["identity"] is None:
                raise DelegationError("The Unix user or group does not exist.")
        principal = ["-e"] if kind == "everyone" else ["-u" if kind == "user" else "-g", name]
    if not removal:
        if row and set(tokens).issubset(row.tokens):
            raise DelegationError("These permissions are already present.")
        validate_additions(tokens, state, scope, name if is_set else "")
    flags = [] if is_set else SCOPES[scope]
    # Only permission-set deletion may omit the list. Principal revokes never do.
    permissions = [] if action == "set_delete" else [",".join(tokens)]
    plan["argv"] = ["zfs", "unallow" if removal else "allow", *flags, *principal, *permissions, dataset]
    return plan


def execute_change(username: str, data: dict, expected: str, acknowledge_scan: bool = False) -> str:
    if not can_manage_zfs_delegation(username):
        raise DelegationError("Delegation access is not granted.")
    argv = []
    with mutation_lock():
        state = load_state(data["dataset"])
        plan = build_plan(state, data)
        if fingerprint(plan) != expected:
            raise DelegationError("State or reference-scan results changed. Review a new confirmation.")
        if plan["scan"] and plan["scan"]["incomplete"] and not acknowledge_scan:
            raise DelegationError("Acknowledge that the incomplete scan may leave references behind.")
        argv = plan["argv"]
        try:
            command(argv, 30)
        except DelegationError as error:
            audit_logger.log_zfs_operation(username, "delegation", success=False, argv=argv, error=str(error))
            raise DelegationError(f"Command failed or timed out. Refresh native state before retrying: {error}") from error
        audit_logger.log_zfs_operation(username, "delegation", argv=argv)
        try:
            load_state(data["dataset"])
        except DelegationError as error:
            return f"Command completed, but state refresh failed: {error}"
    return "Delegation change completed. Native state has been refreshed."