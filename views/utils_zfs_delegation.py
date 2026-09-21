"""Native-only ZFS delegation pages and signed single-action confirmations."""

import shlex
import time
from urllib.parse import urlencode, urlsplit

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import RedirectResponse
from jose import JWTError, jwt

from auth.dependencies import get_current_user
from auth.token import InvalidToken, get_token_claims
from config.settings import settings
from config.templates import templates
from core.request_context import is_cockpit_request
from core.zfs_delegation_parser import DelegationError, fingerprint
from services import zfs_delegation as delegation

BASE_URL = "/utils/zfs-delegation"
router = APIRouter()
LABELS = {
    "grant": "Add Delegation", "add": "Add Permissions", "remove": "Remove Permissions",
    "revoke": "Revoke Entire Row", "set_create": "Create Permission Set",
    "set_add": "Add Set Permissions", "set_remove": "Remove Set Permissions",
    "set_delete": "Delete Permission Set", "pool": "Pool Delegation",
}


def authorized_user(request: Request, username: str = Depends(get_current_user)):
    cockpit = is_cockpit_request(request)
    # The bridge's signed authentication context cannot be bypassed by dropping
    # its informational header or cookie.
    if request.cookies.get("token"):
        try:
            cockpit = cockpit or get_token_claims(request.cookies["token"]).get("auth_context") == "cockpit"
        except InvalidToken:
            raise HTTPException(403, "Invalid session.")
    if cockpit or not delegation.can_manage_zfs_delegation(username):
        raise HTTPException(403, "Delegation requires an allowed native WebZFS session.")
    return username


def require_same_origin(request: Request):
    def origin(value):
        parsed = urlsplit(value)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname or parsed.username or parsed.password:
            raise ValueError
        return parsed.scheme, parsed.hostname.lower(), parsed.port or (443 if parsed.scheme == "https" else 80)

    supplied = request.headers.get("origin")
    if supplied is None:
        supplied = request.headers.get("referer", "")
    try:
        if origin(supplied) == origin(str(request.base_url)):
            return
    except ValueError:
        pass
    raise HTTPException(403, "A same-origin Origin or Referer header is required.")


def render(request, name="index", status=200, **context):
    response = templates.TemplateResponse(
        request, name=f"utils/zfs_delegation/{name}.jinja",
        context={"base_url": BASE_URL, "labels": LABELS, **context}, status_code=status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def build_tree(datasets: dict, entries: list, selected: str) -> list:
    """Build pool-grouped tree. Each pool dict has children with depth-based indentation."""
    pools = {}
    for ds, kind in datasets.items():
        pool = ds.split("/", 1)[0]
        depth = ds.count("/")
        direct = sum(1 for e in entries if e.source == ds and e.kind in {"user", "group", "everyone", "set"})
        node = {"name": ds, "kind": kind, "depth": depth, "count": direct, "selected": ds == selected}
        pools.setdefault(pool, []).append(node)
    selected_pool = selected.split("/", 1)[0] if selected else ""
    tree = []
    for pool, nodes in pools.items():
        has_selected = any(n["selected"] for n in nodes)
        tree.append({"pool": pool, "nodes": nodes, "expanded": has_selected or pool == selected_pool})
    return tree


@router.get("/")
def index(request: Request, dataset: str = "", message: str = "", username: str = Depends(authorized_user)):
    try:
        state = delegation.load_state(dataset)
        accounts = {row.key: delegation.principal_id(row.kind, row.principal)
                    for row in state["entries"] if row.kind in {"user", "group", "everyone"}}
        tree = build_tree(state["datasets"], state["entries"], state["dataset"])
        return render(request, state=state, accounts=accounts, message=message, tree=tree)
    except DelegationError as error:
        return render(request, state=None, error=str(error), status=400)


@router.post("/review")
def review(request: Request, dataset: str = Form(...), action: str = Form(...),
           entry: str = Form(""), scope: str = Form("local"), kind: str = Form("user"),
           principal: str = Form(""), permissions: list[str] = Form([]), value: str = Form(""),
           username: str = Depends(authorized_user)):
    require_same_origin(request)
    data = {"dataset": dataset, "action": action, "entry": entry, "scope": scope,
            "kind": kind, "principal": principal, "permissions": ",".join(permissions), "value": value}
    try:
        state = delegation.load_state(dataset)
        plan = delegation.build_plan(state, data)
        token = jwt.encode({"purpose": "zfs-delegation-confirmation", "username": username,
                            "exp": int(time.time()) + 300, "data": data, "plan": fingerprint(plan)},
                           settings.SECRET_KEY, algorithm=settings.TOKEN_ALGORITHM)
        # Re-render the action form with the command preview and confirmation token
        # inline, matching the pool/dataset create page pattern.
        row = next((row for row in state["entries"] if row.key == data.get("entry", "")), None)
        scope = data.get("scope", row.scope if row else "local")
        removing = action in {"remove", "revoke", "set_remove", "set_delete"}
        candidates = row.tokens if removing and row else sorted(state["support"].permissions | {
            item.principal for item in state["entries"] if item.kind == "set"
        })
        choices = []
        for ctoken in candidates:
            reason = ""
            if not removing:
                try:
                    delegation.validate_additions([ctoken], state, scope, row.principal if row and row.kind == "set" else "")
                    if row and ctoken in row.tokens:
                        reason = "Already present in this row"
                except DelegationError as error:
                    reason = str(error)
            choices.append((ctoken, reason))
        return render(request, "action", state=state, action=action, row=row,
                      scope=scope, choices=choices, plan=plan,
                      command=shlex.join(plan["argv"]), confirmation=token)
    except DelegationError as error:
        return render(request, state=None, error=str(error), status=400)


@router.post("/apply")
def apply_change(request: Request, confirmation: str = Form(...), acknowledge_scan: bool = Form(False),
                 username: str = Depends(authorized_user)):
    require_same_origin(request)
    try:
        claims = jwt.decode(confirmation, settings.SECRET_KEY, algorithms=[settings.TOKEN_ALGORITHM])
        if (claims.get("purpose") != "zfs-delegation-confirmation" or claims.get("username") != username
                or not claims.get("exp") or not isinstance(claims.get("data"), dict)
                or not isinstance(claims.get("plan"), str)):
            raise ValueError
    except (JWTError, ValueError):
        raise HTTPException(403, "Confirmation is invalid or expired. Review the change again.")
    try:
        message = delegation.execute_change(username, claims["data"], claims["plan"], acknowledge_scan)
    except DelegationError as error:
        return render(request, state=None, error=str(error), status=409)
    return RedirectResponse(BASE_URL + "/?" + urlencode({"dataset": claims["data"]["dataset"], "message": message}), status_code=303)


@router.post("/pool-toggle")
def pool_toggle(request: Request, dataset: str = Form(...), username: str = Depends(authorized_user)):
    require_same_origin(request)
    try:
        state = delegation.load_state(dataset)
        if not state["support"].writable:
            raise DelegationError(state["support"].reason or "Read-only profile.")
        new_value = "off" if state["pool_enabled"] == "on" else "on"
        data = {"dataset": dataset, "action": "pool", "value": new_value}
        plan = delegation.build_plan(state, data)
        message = delegation.execute_change(username, data, fingerprint(plan), False)
    except DelegationError as error:
        return RedirectResponse(BASE_URL + "/?" + urlencode({"dataset": dataset, "error": str(error)}), status_code=303)
    return RedirectResponse(BASE_URL + "/?" + urlencode({"dataset": dataset, "message": message}), status_code=303)


@router.get("/action")
def action_form(request: Request, dataset: str, action: str, entry: str = "", scope: str = "local", username: str = Depends(authorized_user)):
    try:
        state = delegation.load_state(dataset)
        if action not in LABELS or not state["support"].writable:
            raise DelegationError(state["support"].reason or "Unknown action.")
        row = next((row for row in state["entries"] if row.key == entry), None)
        if action not in {"grant", "set_create", "pool"} and (row is None or row.source != dataset):
            raise DelegationError("Select a current direct row.")
        if row and (row.kind == "create_time" or (row.kind == "set") != action.startswith("set_")):
            raise DelegationError("This action does not match the selected row.")
        scope = "set" if action.startswith("set_") else row.scope if row else scope
        if scope not in {*delegation.SCOPES, "set"}:
            raise DelegationError("Invalid scope.")
        removing = action in {"remove", "revoke", "set_remove", "set_delete"}
        candidates = row.tokens if removing else sorted(state["support"].permissions | {
            item.principal for item in state["entries"] if item.kind == "set"
        })
        choices = []
        for token in candidates:
            reason = ""
            if not removing:
                try:
                    delegation.validate_additions([token], state, scope, row.principal if row and row.kind == "set" else "")
                    if row and token in row.tokens:
                        reason = "Already present in this row"
                except DelegationError as error:
                    reason = str(error)
            choices.append((token, reason))
        return render(request, "action", state=state, action=action, row=row, scope=scope, choices=choices)
    except DelegationError as error:
        return render(request, state=None, error=str(error), status=400)