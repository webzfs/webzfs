"""
NTP Configuration Views
Web interface for NTP configuration management
"""

from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from starlette.responses import Response

# The demo imports its own copy so it runs in place. After copying this file
# into views/ and services/ntp.py into services/, change the import to:
# from services.ntp import NTPService
from add_feature_demo.services.ntp import NTPService
from auth.dependencies import get_current_user
from config.templates import templates

router = APIRouter(
    prefix="/utils/ntp",
    tags=["ntp"],
    dependencies=[Depends(get_current_user)],
)

ntp_service = NTPService()


@router.get("/")
async def index(request: Request) -> Response:
    """Display NTP configuration overview"""
    try:
        status = ntp_service.get_status()
        servers = ntp_service.get_servers()
        time_info = ntp_service.get_time_info()

        return templates.TemplateResponse(
            request,
            name="utils/ntp/index.jinja",
            context={
                "status": status,
                "servers": servers,
                "time_info": time_info,
                "page_title": "NTP Configuration",
            },
        )
    except Exception as error:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "error": str(error),
                "title": "NTP Error",
            },
        )


@router.get("/config")
async def view_config(request: Request) -> Response:
    """Display NTP configuration file editor"""
    try:
        config = ntp_service.get_config()
        config_path = str(ntp_service.config_path)

        return templates.TemplateResponse(
            request,
            name="utils/ntp/config.jinja",
            context={
                "config": config,
                "config_path": config_path,
                "page_title": "Edit NTP Configuration",
            },
        )
    except Exception as error:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "error": str(error),
                "title": "NTP Configuration Error",
            },
        )


@router.post("/config/save")
async def save_config(
    request: Request,
    config: Annotated[str, Form()],
) -> RedirectResponse:
    """Save NTP configuration"""
    try:
        success = ntp_service.update_config(config)

        if success:
            return RedirectResponse(
                url="/utils/ntp/config?message=Configuration saved successfully",
                status_code=303,
            )
        return RedirectResponse(
            url="/utils/ntp/config?error=Failed to save configuration", status_code=303
        )
    except Exception as error:
        return RedirectResponse(url=f"/utils/ntp/config?error={error}", status_code=303)


@router.post("/service/restart")
async def restart_service(request: Request) -> RedirectResponse:
    """Restart NTP service"""
    try:
        success = ntp_service.restart_service()

        if success:
            return RedirectResponse(
                url="/utils/ntp?message=NTP service restarted successfully",
                status_code=303,
            )
        return RedirectResponse(
            url="/utils/ntp?error=Failed to restart NTP service", status_code=303
        )
    except Exception as error:
        return RedirectResponse(url=f"/utils/ntp?error={error}", status_code=303)


@router.post("/service/enable")
async def enable_service(request: Request) -> RedirectResponse:
    """Enable NTP service at boot"""
    try:
        success = ntp_service.enable_service()

        if success:
            return RedirectResponse(
                url="/utils/ntp?message=NTP service enabled at boot", status_code=303
            )
        return RedirectResponse(
            url="/utils/ntp?error=Failed to enable NTP service", status_code=303
        )
    except Exception as error:
        return RedirectResponse(url=f"/utils/ntp?error={error}", status_code=303)


@router.get("/status/refresh")
async def refresh_status(request: Request) -> Response:
    """Refresh NTP status (for HTMX)"""
    try:
        status = ntp_service.get_status()
        time_info = ntp_service.get_time_info()

        return templates.TemplateResponse(
            request,
            name="utils/ntp/status_partial.jinja",
            context={"status": status, "time_info": time_info},
        )
    except Exception as error:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "error": str(error),
                "title": "NTP Status Error",
            },
        )
