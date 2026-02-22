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
    """Display all system services and their status."""
    try:
        all_services = services_service.list_services()

        # Build summary counts
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
            "utils/services/index.jinja",
            {
                "request": request,
                "services": all_services,
                "summary": summary,
                "page_title": "System Services",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            "utils/services/index.jinja",
            {
                "request": request,
                "services": [],
                "summary": {},
                "error": str(e),
                "page_title": "System Services",
            },
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
