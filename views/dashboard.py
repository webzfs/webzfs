"""
Dashboard Views
Provides the main dashboard page and HTMX refresh endpoints.
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


@router.get("/")
def index(request: Request):
    """Main dashboard page -- initial load with all data."""
    try:
        specs = get_system_specs()
        realtime = get_realtime_system_data()
        pools = get_pool_info_extended()
        arc_stats = get_arc_stats_summary()
        scrub_status = get_scrub_status_all()
        context = {
            'specs': specs,
            'realtime': realtime,
            'pools': pools,
            'arc_stats': arc_stats,
            'scrub_status': scrub_status,
        }
    except Exception as exc:
        context = {
            'error': str(exc),
            'specs': {},
            'realtime': {},
            'pools': [],
            'arc_stats': {},
            'scrub_status': {'pools': []},
        }

    return templates.TemplateResponse(
        request, name="dashboard/index.jinja", context=context,
    )


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


@router.get("/memory-refresh")
def memory_refresh(request: Request):
    """Return refreshed memory information."""
    try:
        memory = get_memory_stats()
        context = {"data": memory}
    except Exception as exc:
        context = {"data": {"Error": str(exc)}}

    return templates.TemplateResponse(
        request, name="dashboard/table.jinja", context=context
    )


@router.get("/system-load-refresh")
def system_load_refresh(request: Request):
    """Return refreshed system load information."""
    try:
        system_load = get_system_load_stats()
        context = {"system_load": system_load}
    except Exception as exc:
        context = {"system_load": {"Error": str(exc)}}

    return templates.TemplateResponse(
        request, name="dashboard/system_load_table.jinja", context=context
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
