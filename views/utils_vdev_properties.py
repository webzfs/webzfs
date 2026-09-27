"""Custom VDEV Properties utility (issue 113).

Pool topology on the left, a persistent inspector on the right. All state is
re-read from ZFS after every mutation; the browser never holds authoritative
metadata.
"""

from urllib.parse import urlencode

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth.dependencies import get_current_user
from config.templates import templates
from core.vdev_property_catalog import INLINE_MODES, BY_KEY, groups_for_scope
from services import zfs_vdev_properties as service

BASE_URL = "/utils/vdev-properties"
router = APIRouter(tags=["vdev-properties"], dependencies=[Depends(get_current_user)])


def render(request: Request, name: str, status: int = 200, **context):
    response = templates.TemplateResponse(
        request, name=f"utils/vdev_properties/{name}.jinja",
        context={"base_url": BASE_URL, "inline_modes": INLINE_MODES, **context}, status_code=status,
    )
    response.headers["Cache-Control"] = "no-store"
    return response


def all_pool_options(pool_names: list[str]) -> list[str]:
    return [service.ALL_POOLS, *pool_names] if len(pool_names) > 1 else pool_names


def load(pool: str, selected: str) -> tuple[list[dict], dict, dict]:
    if pool == service.ALL_POOLS:
        pool_names = service.list_pools()
        topologies, node = service.build_topologies(pool_names, selected)
        return topologies, node, service.aggregate_summary(topologies)
    topology = service.build_topology(pool)
    try:
        node = service.find_object(topology, selected)
    except service.VdevPropertyError:
        node = topology["pool"]
    return [topology], node, {"topology_summary": topology["topology_summary"], "metrics": topology["metrics"]}


def inspector_context(pool: str, topologies: list[dict], node: dict, summary: dict) -> dict:
    return {
        "pool": pool, "topologies": topologies, "summary": summary,
        "node": node, "selected": node.get("token", "pool"),
        "groups": groups_for_scope(service.scope_for(node)),
    }


def redirect(pool: str, selected: str, **params) -> RedirectResponse:
    query = urlencode({"pool": pool, "selected": selected, **params})
    return RedirectResponse(f"{BASE_URL}/?{query}", status_code=303)


@router.get("/", response_class=HTMLResponse)
def index(request: Request, pool: str = "", selected: str = "pool", message: str = "", error: str = ""):
    try:
        pool_names = service.list_pools()
    except service.VdevPropertyError as failure:
        return render(request, "index", pools=[], pool="", selected="pool", error=str(failure))
    pools = all_pool_options(pool_names)
    if pool not in pools:
        pool = service.ALL_POOLS if service.ALL_POOLS in pools else (pools[0] if pools else "")
    return render(request, "index", pools=pools, pool=pool, selected=selected, message=message, error=error)


@router.get("/content", response_class=HTMLResponse)
def content(request: Request, pool: str, selected: str = "pool"):
    try:
        topologies, node, summary = load(pool, selected)
    except service.VdevPropertyError as failure:
        return render(request, "_error", error=str(failure))
    return render(request, "_content", **inspector_context(pool, topologies, node, summary))


@router.get("/inspector", response_class=HTMLResponse)
def inspector(request: Request, pool: str, selected: str = "pool"):
    try:
        topologies, node, summary = load(pool, selected)
    except service.VdevPropertyError as failure:
        return render(request, "_error", error=str(failure))
    return render(request, "_inspector", **inspector_context(pool, topologies, node, summary))


@router.get("/edit", response_class=HTMLResponse)
def edit_form(request: Request, pool: str, selected: str = "pool"):
    try:
        topologies, node, summary = load(pool, selected)
    except service.VdevPropertyError as failure:
        return render(request, "_error", error=str(failure))
    return render(request, "_edit_modal", **inspector_context(pool, topologies, node, summary))


@router.post("/save")
async def save(request: Request, pool: str = Form(...), selected: str = Form("pool"),
               username: str = Depends(get_current_user)):
    form = await request.form()
    submitted = {key: str(value) for key, value in form.items() if key not in {"pool", "selected"}}
    try:
        topologies, node, _summary = load(pool, selected)
        if selected != "pool" and node.get("token") != selected and node.get("name") != selected:
            raise service.VdevPropertyError("The selected object no longer exists in this pool.")
        changes = service.apply_form(username, node["pool"], node, submitted)
    except service.VdevPropertyError as failure:
        return redirect(pool, selected, error=str(failure))
    if not changes:
        return redirect(pool, selected, message="No property values changed.")
    return redirect(pool, selected, message=f"Saved {len(changes)} propert{'y' if len(changes) == 1 else 'ies'} to ZFS.")


@router.post("/clear")
def clear(pool: str = Form(...), selected: str = Form(...), key: str = Form(...),
          username: str = Depends(get_current_user)):
    try:
        _topologies, node, _summary = load(pool, selected)
        service.clear_custom_property(username, node["pool"], node, key)
    except service.VdevPropertyError as failure:
        return redirect(pool, selected, error=str(failure))
    label = BY_KEY[key].label if key in BY_KEY else key
    return redirect(pool, selected, message=f"Cleared {label}.")


@router.get("/discover", response_class=HTMLResponse)
def discover(request: Request, pool: str, selected: str):
    try:
        topologies, node, summary = load(pool, selected)
        discovered = service.discover_leaf_metadata(node)
        rows = service.discovery_diff(node, discovered)
    except service.VdevPropertyError as failure:
        return render(request, "_error", error=str(failure))
    return render(request, "_discovery_modal", pool=pool, topologies=topologies, summary=summary,
                  node=node, selected=selected, rows=rows,
                  device=discovered.get("_device", ""))


@router.post("/discover/apply")
async def discover_apply(request: Request, pool: str = Form(...), selected: str = Form(...),
                         username: str = Depends(get_current_user)):
    form = await request.form()
    try:
        _topologies, node, _summary = load(pool, selected)
        if node["kind"] != "leaf":
            raise service.VdevPropertyError("Discovery applies to leaf vdevs only.")
        written = 0
        for key in form.getlist("apply"):
            value = str(form.get(f"value:{key}", ""))
            if value:
                service.set_custom_property(username, node["pool"], node, key, value)
                written += 1
    except service.VdevPropertyError as failure:
        return redirect(pool, selected, error=str(failure))
    return redirect(pool, selected, message=f"Applied {written} discovered value{'s' if written != 1 else ''}.")


@router.get("/discover-all", response_class=HTMLResponse)
def discover_all(request: Request, pool: str, selected: str = "pool"):
    try:
        topologies, _node, _summary = load(pool, selected)
        plan, errors = service.bulk_discovery_plan(topologies)
    except service.VdevPropertyError as failure:
        return render(request, "_error", error=str(failure))
    return render(request, "_bulk_discovery_modal", pool=pool, selected=selected, plan=plan, errors=errors)


@router.post("/discover-all/apply")
async def discover_all_apply(request: Request, pool: str = Form(...), selected: str = Form("pool"),
                             username: str = Depends(get_current_user)):
    form = await request.form()
    selections = []
    for token in form.getlist("apply"):
        selection_parts = token.split("|", 2)
        if len(selection_parts) != 3:
            continue
        pool_name, leaf_name, key = selection_parts
        value = str(form.get(f"value:{token}", ""))
        if pool_name and leaf_name and key and value:
            selections.append((pool_name, leaf_name, key, value))
    try:
        topologies, _node, _summary = load(pool, selected)
        written = service.apply_bulk_plan(username, topologies, selections)
    except service.VdevPropertyError as failure:
        return redirect(pool, selected, error=str(failure))
    return redirect(pool, selected, message=f"Filled {written} blank value{'s' if written != 1 else ''} from discovery.")
