"""
System Services Views
Read-only web interface for viewing system service status.
"""
from fastapi import APIRouter, Request, Depends
from fastapi.responses import HTMLResponse
from config.templates import templates
from services.system_services import SystemServicesService
from auth.dependencies import get_current_user


router = APIRouter(tags=["services"], dependencies=[Depends(get_current_user)])
services_service = SystemServicesService()


@router.get("/", response_class=HTMLResponse)
async def services_index(request: Request):
    """Display page shell immediately; service data loads via HTMX partial."""
    return templates.TemplateResponse(
        "utils/services/index.jinja",
        {
            "request": request,
            "page_title": "System Services",
        },
    )


@router.get("/content-partial", response_class=HTMLResponse)
async def services_content_partial(request: Request):
    """HTMX partial that fetches service data and returns summary + table HTML."""
    try:
        all_services = services_service.list_services()

        summary = {
            "total": len(all_services),
            "running": sum(1 for s in all_services if s["status"] == "running"),
            "stopped": sum(1 for s in all_services if s["status"] == "stopped"),
            "exited": sum(1 for s in all_services if s["status"] == "exited"),
            "failed": sum(1 for s in all_services if s["status"] == "failed"),
            "enabled": sum(1 for s in all_services if s["enabled"] == "enabled"),
            "disabled": sum(1 for s in all_services if s["enabled"] == "disabled"),
        }

        return templates.TemplateResponse(
            "utils/services/content_partial.jinja",
            {
                "request": request,
                "services": all_services,
                "summary": summary,
            },
        )
    except Exception as e:
        return HTMLResponse(
            content=f'<div class="bg-danger-900/30 border-2 border-danger-500/50 text-danger-400 px-4 py-3 rounded">'
            f'<strong>Error:</strong> {str(e)}</div>'
        )


@router.get("/detail/{service_name}", response_class=HTMLResponse)
async def service_detail(request: Request, service_name: str):
    """Display verbose status output for a single service."""
    try:
        detail = services_service.get_service_detail(service_name)
        return templates.TemplateResponse(
            "utils/services/detail.jinja",
            {
                "request": request,
                "service_name": service_name,
                "detail": detail,
                "page_title": f"Service: {service_name}",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/utils/services",
            },
        )
