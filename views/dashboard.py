from fastapi import APIRouter, Depends, Request

from auth.dependencies import get_current_user
from config.templates import templates
from services.dashboard import get_dashboard_context, get_system_load_stats, get_pool_stats

router = APIRouter(dependencies=[Depends(get_current_user)])


@router.get("/")
def index(request: Request):
    try:
        context = get_dashboard_context()
    except Exception as exc:
        context = {"error": str(exc)}

    return templates.TemplateResponse(
        request, name="dashboard/index.jinja", context=context
    )


@router.get("/system-load-values")
def system_load_values(request: Request):
    """Return only the system load values for efficient updates."""
    try:
        system_load = get_system_load_stats()
        context = {"data": system_load}
    except Exception as exc:
        context = {"error": str(exc)}

    return templates.TemplateResponse(
        request, name="dashboard/system_load_values.jinja", context=context
    )


@router.get("/zfs-pools-refresh")
def zfs_pools_refresh(request: Request):
    """Return refreshed ZFS pool information."""
    try:
        pools = get_pool_stats()
        context = {"pools": pools}
    except Exception as exc:
        context = {"error": str(exc)}

    return templates.TemplateResponse(
        request, name="dashboard/zfs_pools.jinja", context=context
    )
