"""
Health Analysis Views
Web interface for ZFS/Disk/System health analysis.
Uses deferred HTMX loading so the page shell renders instantly.
Analysis runs in a background thread with HTMX polling for progress.
"""
import threading
from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Annotated, Optional
from config.templates import templates
from services.health_analysis import HealthAnalysisService
from auth.dependencies import get_current_user


router = APIRouter(tags=["health"], dependencies=[Depends(get_current_user)])
health_service = HealthAnalysisService()


@router.get("/", response_class=HTMLResponse)
async def health_index(request: Request):
    """Render the page shell immediately. Report list loaded via HTMX."""
    disk_count = 0
    try:
        disk_count = health_service.get_disk_count()
    except Exception:
        pass

    return templates.TemplateResponse(
        "utils/health/index.jinja",
        {
            "request": request,
            "page_title": "Health Analysis",
            "disk_count": disk_count,
        },
    )


@router.get("/content-partial", response_class=HTMLResponse)
async def health_content_partial(request: Request):
    """HTMX partial endpoint that returns the list of past reports."""
    try:
        reports = health_service.list_reports()
        return templates.TemplateResponse(
            "utils/health/content_partial.jinja",
            {
                "request": request,
                "reports": reports,
            },
        )
    except Exception as e:
        return HTMLResponse(
            content=f"""
            <div class="bg-danger-900/30 border-2 border-danger-500/50 text-danger-400 px-4 py-3 rounded" role="alert">
                <strong>Error:</strong> {e}
            </div>
            """,
            status_code=200,
        )


@router.post("/run", response_class=HTMLResponse)
async def run_health_analysis(
    request: Request,
    check_disk_health: Optional[str] = Form(None),
    check_smart_tests: Optional[str] = Form(None),
    check_scrubs: Optional[str] = Form(None),
    aggressive_hours: Optional[str] = Form(None),
):
    """Create a pending report, start analysis in background thread, redirect immediately."""
    try:
        report_id = health_service.create_pending_report(
            check_disk_health=(check_disk_health == "on"),
            check_smart_tests=(check_smart_tests == "on"),
            check_scrubs=(check_scrubs == "on"),
            aggressive_hours=(aggressive_hours == "on"),
        )
        # Start analysis in a daemon background thread
        worker = threading.Thread(
            target=health_service.run_analysis_background,
            args=(report_id,),
            daemon=True,
        )
        worker.start()
        # Redirect immediately to the report page (will show running status)
        return RedirectResponse(
            url=f"/utils/health/report/{report_id}",
            status_code=303,
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/health?error={str(e)}",
            status_code=303,
        )


@router.get("/report/{report_id}", response_class=HTMLResponse)
async def health_report_detail(request: Request, report_id: str):
    """Display report page shell. Content loaded via HTMX for running reports."""
    report = health_service.get_report(report_id)
    if not report:
        return RedirectResponse(
            url="/utils/health?error=Report not found",
            status_code=303,
        )

    # Format report ID for display: 20260308-205618 -> 2026-03-08 20:56:18
    display_date = report_id
    if len(report_id) == 15 and "-" in report_id:
        try:
            display_date = f"{report_id[0:4]}-{report_id[4:6]}-{report_id[6:8]} {report_id[9:11]}:{report_id[11:13]}:{report_id[13:15]}"
        except (IndexError, ValueError):
            pass

    return templates.TemplateResponse(
        "utils/health/report.jinja",
        {
            "request": request,
            "report": report,
            "page_title": f"Health Report - {display_date}",
        },
    )


@router.get("/report/{report_id}/content-partial", response_class=HTMLResponse)
async def health_report_content_partial(request: Request, report_id: str):
    """HTMX polling endpoint: returns updated report content fragment."""
    report = health_service.get_report(report_id)
    if not report:
        return HTMLResponse(
            content='<div class="text-danger-400">Report not found.</div>',
            status_code=200,
        )

    return templates.TemplateResponse(
        "utils/health/report_content_partial.jinja",
        {
            "request": request,
            "report": report,
        },
    )


@router.post("/report/{report_id}/delete", response_class=HTMLResponse)
async def delete_health_report(request: Request, report_id: str):
    """Delete a health analysis report."""
    if health_service.delete_report(report_id):
        return RedirectResponse(
            url="/utils/health?message=Report deleted successfully",
            status_code=303,
        )
    return RedirectResponse(
        url="/utils/health?error=Report not found",
        status_code=303,
    )
