"""
ZED Event Scripts Views

Web interface for managing ZFS Event Daemon ZEDLETs. Uses deferred HTMX
loading so the page shell renders immediately while ZED state is fetched
in the background.
"""

import logging

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from auth.dependencies import get_current_user
from config.templates import templates
from services.audit_logger import audit_logger
from services.zed import ZedService, ZED_EVENT_PREFIXES

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["zed"], dependencies=[Depends(get_current_user)]
)
zed_service = ZedService()


@router.get("/", response_class=HTMLResponse)
async def zed_index(request: Request):
    """Render the ZED page shell. ZEDLET data loads via HTMX."""
    return templates.TemplateResponse(
        request,
        name="utils/zed/index.jinja",
        context={"page_title": "ZED Event Scripts"},
    )


@router.get("/content-partial", response_class=HTMLResponse)
async def zed_content_partial(request: Request):
    """HTMX partial that queries ZED state and returns the content."""
    try:
        status = zed_service.get_status()
    except Exception as e:
        return HTMLResponse(
            content=(
                '<div class="bg-danger-900/30 border-2 '
                'border-danger-500/50 text-danger-400 px-4 py-3 rounded">'
                f"<strong>Error:</strong> {e}</div>"
            ),
            status_code=200,
        )

    return templates.TemplateResponse(
        request,
        name="utils/zed/content_partial.jinja",
        context={
            "status": status,
            "event_prefixes": ZED_EVENT_PREFIXES,
        },
    )


@router.post("/toggle/{name}", response_class=HTMLResponse)
async def toggle_zedlet(
    request: Request,
    name: str,
    current_user: str = Depends(get_current_user),
):
    """Enable or disable a ZEDLET."""
    try:
        status = zed_service.get_status()
        zedlet = next(
            (z for z in status.zedlets if z.name == name), None
        )

        if zedlet and zedlet.enabled:
            msg = zed_service.disable_zedlet(
                name, status.enabled_dir, status.package_dir
            )
            audit_logger.log_zfs_operation(
                user=current_user,
                operation="zed_disable",
                detail=f"Disabled ZEDLET: {name}",
            )
        else:
            msg = zed_service.enable_zedlet(
                name, status.enabled_dir, status.package_dir
            )
            audit_logger.log_zfs_operation(
                user=current_user,
                operation="zed_enable",
                detail=f"Enabled ZEDLET: {name}",
            )

        # Try to reload ZED after the change
        try:
            zed_service.reload_zed()
        except Exception:
            pass

        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/zed?error={e}", status_code=303,
        )


@router.get("/edit/{name}", response_class=HTMLResponse)
async def edit_zedlet(request: Request, name: str):
    """Return ZEDLET content as JSON-like partial for the edit modal."""
    try:
        status = zed_service.get_status()
        content, content_hash, origin = zed_service.read_zedlet(
            name, status.enabled_dir, status.package_dir
        )
        return templates.TemplateResponse(
            request,
            name="utils/zed/edit_partial.jinja",
            context={
                "name": name,
                "content": content,
                "content_hash": content_hash,
                "origin": origin,
                "enabled_dir": status.enabled_dir,
            },
        )
    except Exception as e:
        return HTMLResponse(
            content=f'<div class="text-danger-400 p-4">{e}</div>',
            status_code=200,
        )


@router.post("/save/{name}", response_class=HTMLResponse)
async def save_zedlet(
    request: Request,
    name: str,
    content: str = Form(...),
    content_hash: str = Form(""),
    current_user: str = Depends(get_current_user),
):
    """Save changes to a ZEDLET."""
    try:
        status = zed_service.get_status()
        msg = zed_service.save_zedlet(
            name, content,
            status.enabled_dir, status.package_dir,
            expected_hash=content_hash,
        )
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="zed_save",
            detail=f"Saved ZEDLET: {name}",
        )
        try:
            zed_service.reload_zed()
        except Exception:
            pass
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/zed?error={e}", status_code=303,
        )


@router.post("/create", response_class=HTMLResponse)
async def create_zedlet(
    request: Request,
    event_prefix: str = Form(...),
    handler_name: str = Form(...),
    is_sync: bool = Form(False),
    enable: bool = Form(True),
    content: str = Form(...),
    current_user: str = Depends(get_current_user),
):
    """Create a new custom ZEDLET."""
    try:
        status = zed_service.get_status()
        if not status.enabled_dir:
            raise ValueError("ZED enabled directory not found")
        filename = zed_service.build_filename(
            event_prefix, handler_name, is_sync
        )
        msg = zed_service.create_zedlet(
            filename, content, status.enabled_dir, enable=enable
        )
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="zed_create",
            detail=f"Created ZEDLET: {filename}",
        )
        try:
            zed_service.reload_zed()
        except Exception:
            pass
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/zed?error={e}", status_code=303,
        )


@router.post("/delete/{name}", response_class=HTMLResponse)
async def delete_zedlet(
    request: Request,
    name: str,
    current_user: str = Depends(get_current_user),
):
    """Delete a custom ZEDLET."""
    try:
        status = zed_service.get_status()
        msg = zed_service.delete_zedlet(name, status.enabled_dir)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="zed_delete",
            detail=f"Deleted ZEDLET: {name}",
        )
        try:
            zed_service.reload_zed()
        except Exception:
            pass
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )


@router.post("/restore/{name}", response_class=HTMLResponse)
async def restore_zedlet(
    request: Request,
    name: str,
    current_user: str = Depends(get_current_user),
):
    """Restore a ZEDLET to its package default."""
    try:
        status = zed_service.get_status()
        msg = zed_service.restore_default(
            name, status.enabled_dir, status.package_dir
        )
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="zed_restore",
            detail=f"Restored ZEDLET to default: {name}",
        )
        try:
            zed_service.reload_zed()
        except Exception:
            pass
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )




@router.get("/zed-rc", response_class=HTMLResponse)
async def get_zed_rc(request: Request):
    """Return zed.rc content for the edit modal."""
    try:
        status = zed_service.get_status()
        content, content_hash = zed_service.read_zed_rc(
            status.zed_rc_path
        )
        return templates.TemplateResponse(
            request,
            name="utils/zed/rc_partial.jinja",
            context={
                "rc_content": content,
                "rc_hash": content_hash,
                "rc_path": status.zed_rc_path,
            },
        )
    except Exception as exc:
        return HTMLResponse(
            content=f'<div class="text-danger-400 p-4">{exc}</div>',
            status_code=200,
        )


@router.post("/zed-rc", response_class=HTMLResponse)
async def save_zed_rc(
    request: Request,
    content: str = Form(...),
    content_hash: str = Form(""),
    current_user: str = Depends(get_current_user),
):
    """Save changes to zed.rc."""
    try:
        status = zed_service.get_status()
        msg = zed_service.save_zed_rc(
            status.zed_rc_path, content,
            expected_hash=content_hash,
        )
        audit_logger.log_zfs_operation(
            user=current_user, operation="zed_rc_save",
            detail="Saved zed.rc",
        )
        try:
            zed_service.reload_zed()
        except Exception:
            pass
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )


@router.post("/reload", response_class=HTMLResponse)
async def reload_zed_route(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Manually reload ZED."""
    try:
        msg = zed_service.reload_zed()
        audit_logger.log_zfs_operation(
            user=current_user, operation="zed_reload",
            detail="Reloaded ZED daemon",
        )
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )


@router.post("/start", response_class=HTMLResponse)
async def start_zed_route(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Start the ZED service."""
    try:
        msg = zed_service.start_zed()
        audit_logger.log_zfs_operation(
            user=current_user, operation="zed_start",
            detail="Started ZED service",
        )
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )


@router.post("/stop", response_class=HTMLResponse)
async def stop_zed_route(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    """Stop the ZED service."""
    try:
        msg = zed_service.stop_zed()
        audit_logger.log_zfs_operation(
            user=current_user, operation="zed_stop",
            detail="Stopped ZED service",
        )
        return RedirectResponse(
            url=f"/utils/zed?message={msg}", status_code=303,
        )
    except Exception as exc:
        return RedirectResponse(
            url=f"/utils/zed?error={exc}", status_code=303,
        )

