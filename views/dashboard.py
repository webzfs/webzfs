"""
Dashboard Views
Provides the main dashboard page and HTMX async loading / refresh endpoints.

Each dashboard card loads independently via HTMX deferred loading.
The page shell renders immediately, then each card fetches its data
in parallel via separate requests triggered on page load.
"""
from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse

from auth.dependencies import get_current_user
from config.templates import templates
from services.dashboard import (
    get_system_specs,
    get_realtime_system_data,
    get_pool_info_extended,
    get_arc_stats_summary,
    get_scrub_status_all,
    get_memory_stats, 
    get_system_load_stats
)

router = APIRouter(dependencies=[Depends(get_current_user)])


# ---------------------------------------------------------------------------
# Main page shell (no data fetching -- renders immediately)
# ---------------------------------------------------------------------------


@router.get("/")
def index(request: Request):
    """Main dashboard page shell. All card data loads async via HTMX."""
    return templates.TemplateResponse(
        request, name="dashboard/index.jinja", context={},
    )


# ---------------------------------------------------------------------------
# Async initial load endpoints (hx-trigger="load")
# ---------------------------------------------------------------------------


@router.get("/system-info-data", response_class=HTMLResponse)
def system_info_data(request: Request):
    """HTMX endpoint: system information card (specs + uptime)."""
    try:
        specs = get_system_specs()
        realtime = get_realtime_system_data()
        context = {'specs': specs, 'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'specs': {}, 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/system_info_data.jinja", context=context,
    )


@router.get("/memory-data", response_class=HTMLResponse)
def memory_data(request: Request):
    """HTMX endpoint: memory usage card."""
    try:
        realtime = get_realtime_system_data()
        context = {'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/memory_data.jinja", context=context,
    )


@router.get("/system-load-data", response_class=HTMLResponse)
def system_load_data(request: Request):
    """HTMX endpoint: system load + tasks card."""
    try:
        realtime = get_realtime_system_data()
        context = {'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/system_load_data.jinja", context=context,
    )


@router.get("/cpu-time-data", response_class=HTMLResponse)
def cpu_time_data(request: Request):
    """HTMX endpoint: CPU time distribution card."""
    try:
        realtime = get_realtime_system_data()
        context = {'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/cpu_time_data.jinja", context=context,
    )


# ---------------------------------------------------------------------------
# Periodic refresh endpoints (polled by HTMX after initial load)
# ---------------------------------------------------------------------------


@router.get("/realtime-data", response_class=HTMLResponse)
def realtime_data(request: Request):
    """
    HTMX endpoint polled every 15 seconds.

    Returns OOB-swap element for memory only.
    """
    try:
        realtime = get_realtime_system_data()
        context = {'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/realtime_data.jinja", context=context,
    )


@router.get("/memory-refresh", response_class=HTMLResponse)
def memory_refresh(request: Request):
    """Return refreshed memory information."""
    try:
        realtime = get_realtime_system_data()
        context = {"realtime": realtime}
    except Exception as exc:
        context = {"error": str(exc), "realtime": {}}

    return templates.TemplateResponse(
        request, name="dashboard/memory_data.jinja", context=context,
    )


@router.get("/system-load-refresh", response_class=HTMLResponse)
def system_load_refresh(request: Request):
    """Return refreshed system load information."""
    try:
        realtime = get_realtime_system_data()
        context = {"realtime": realtime}
    except Exception as exc:
        context = {"error": str(exc), "realtime": {}}

    return templates.TemplateResponse(
        request, name="dashboard/system_load_data.jinja", context=context,
    )


@router.get("/system-stats-refresh", response_class=HTMLResponse)
def system_stats_refresh(request: Request):
    """
    HTMX endpoint polled every 60 seconds (+ manual refresh button).

    Returns OOB-swap elements for uptime, tasks, system load, and
    CPU time distribution.
    """
    try:
        realtime = get_realtime_system_data()
        context = {'realtime': realtime}
    except Exception as exc:
        context = {'error': str(exc), 'realtime': {}}

    return templates.TemplateResponse(
        request, name="dashboard/system_stats_data.jinja", context=context,
    )


@router.get("/zfs-pools-refresh", response_class=HTMLResponse)
def zfs_pools_refresh(request: Request):
    """HTMX endpoint for refreshing pool information."""
    try:
        pools = get_pool_info_extended()
        context = {'pools': pools}
    except Exception as exc:
        context = {'error': str(exc), 'pools': []}

    return templates.TemplateResponse(
        request, name="dashboard/zfs_pools.jinja", context=context,
    )


@router.get("/arc-stats-refresh", response_class=HTMLResponse)
def arc_stats_refresh(request: Request):
    """HTMX endpoint for refreshing ARC statistics (every 5 min + manual)."""
    try:
        arc_stats = get_arc_stats_summary()
        context = {'arc_stats': arc_stats}
    except Exception as exc:
        context = {'arc_stats': {'error': str(exc)}}

    return templates.TemplateResponse(
        request, name="dashboard/arc_stats_data.jinja", context=context,
    )


@router.get("/scrub-status-refresh", response_class=HTMLResponse)
def scrub_status_refresh(request: Request):
    """HTMX endpoint for refreshing scrub status."""
    try:
        scrub_status = get_scrub_status_all()
        context = {'scrub_status': scrub_status}
    except Exception as exc:
        context = {'scrub_status': {'pools': [], 'error': str(exc)}}

    return templates.TemplateResponse(
        request, name="dashboard/scrub_status_data.jinja", context=context,
    )
