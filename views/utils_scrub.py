"""
Utilities Index and Scrub Overview Routes

The scrub page is an overview, matching the pattern used by
/utils/smart: a page shell renders immediately and the pool data is
fetched by HTMX so a slow `zpool status` call never blocks the page.

It shows the same per-pool scrub status table that appears at the bottom
of the dashboard, plus the scrub schedules that are registered with the
operating system scheduler. Creating and editing schedules happens in the
Unified Scheduling Hub (/utils/scheduling); this page links to it.

The old /utils/scrub-scheduling URL redirects here so bookmarks and
documentation links keep working.
"""

import logging

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth.dependencies import get_current_user
from config.templates import templates
from services.audit_logger import audit_logger
from services.dashboard import get_scrub_status_all
from services.schedule_utils import describe_schedule, preview_next_runs
from services.storage import FileStorageService
from services.shell_settings import can_use_shell
from services.zfs_delegation import can_manage_zfs_delegation
from services.zfs_pool import ZFSPoolService
from core.request_context import is_cockpit_request

logger = logging.getLogger(__name__)

router = APIRouter(dependencies=[Depends(get_current_user)])

storage_service = FileStorageService()
pool_service = ZFSPoolService()


def _schedules_by_pool() -> dict:
    """Map pool name to its scrub schedules for the overview table."""
    by_pool: dict = {}
    try:
        for schedule in storage_service.get_scrub_schedules():
            pool_name = schedule.get("pool", "")
            entry = dict(schedule)
            entry["schedule_human"] = describe_schedule(schedule.get("schedule", ""))
            try:
                upcoming = preview_next_runs(schedule.get("schedule", ""), count=1)
                entry["next_run_preview"] = upcoming[0] if upcoming else ""
            except Exception:
                entry["next_run_preview"] = ""
            by_pool.setdefault(pool_name, []).append(entry)
    except Exception as read_error:
        logger.warning(f"Could not read scrub schedules: {read_error}")
    return by_pool


@router.get("/")
def index(request: Request, username: str = Depends(get_current_user)):
    """Display the utilities page with cards for each utility."""
    return templates.TemplateResponse(
        request,
        name="utils/index.jinja",
        context={
            "cockpit_context": is_cockpit_request(request),
            "shell_allowed": can_use_shell(username),
            "delegation_allowed": can_manage_zfs_delegation(username),
        },
    )


@router.get("/scrub", response_class=HTMLResponse)
def scrub_overview(request: Request):
    """Render the scrub overview shell. Pool data loads via HTMX."""
    return templates.TemplateResponse(
        request,
        name="utils/scrub/index.jinja",
        context={"page_title": "Scrub Status"},
    )


@router.get("/scrub/content-partial", response_class=HTMLResponse)
def scrub_content_partial(request: Request):
    """HTMX partial that queries every pool and returns the status table."""
    try:
        scrub_status = get_scrub_status_all()
    except Exception as status_error:
        scrub_status = {"pools": [], "error": str(status_error)}

    return templates.TemplateResponse(
        request,
        name="utils/scrub/content_partial.jinja",
        context={
            "scrub_status": scrub_status,
            "schedules_by_pool": _schedules_by_pool(),
        },
    )


@router.post("/scrub/{pool_name}/start", response_class=HTMLResponse)
def start_scrub_now(
    request: Request, pool_name: str, current_user: str = Depends(get_current_user)
):
    """Start a scrub on one pool immediately from the overview page."""
    try:
        pool_service.scrub_pool(pool_name)
        audit_logger.log_pool_scrub(
            user=current_user, pool_name=pool_name, action="start"
        )
        return RedirectResponse(
            url=f"/utils/scrub?message=Scrub started on {pool_name}",
            status_code=303,
        )
    except Exception as scrub_error:
        audit_logger.log_pool_scrub(
            user=current_user,
            pool_name=pool_name,
            action="start",
            success=False,
            error=str(scrub_error),
        )
        return RedirectResponse(
            url=f"/utils/scrub?error={scrub_error}",
            status_code=303,
        )


@router.get("/scrub-scheduling")
def scrub_scheduling_redirect(request: Request):
    """Redirect the old scrub scheduling URL to the scrub overview."""
    return RedirectResponse(url="/utils/scrub", status_code=307)


@router.post("/scrub-scheduling/create")
def create_scrub_schedule_redirect(request: Request):
    """Redirect old create posts to the scheduling hub create form."""
    return RedirectResponse(
        url="/utils/scheduling/scrub/new",
        status_code=303,
    )


@router.post("/scrub-scheduling/{schedule_id}/toggle")
def toggle_scrub_schedule_redirect(request: Request, schedule_id: int):
    """Redirect old toggle posts to the scheduling hub toggle route."""
    return RedirectResponse(
        url=f"/utils/scheduling/scrub/{schedule_id}/toggle",
        status_code=307,
    )


@router.post("/scrub-scheduling/{schedule_id}/delete")
def delete_scrub_schedule_redirect(request: Request, schedule_id: int):
    """Redirect old delete posts to the scheduling hub delete route."""
    return RedirectResponse(
        url=f"/utils/scheduling/scrub/{schedule_id}/delete",
        status_code=307,
    )
