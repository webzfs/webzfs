"""
SSH Connection Management Views
HTTP routes for managing SSH connections
"""
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from typing import Annotated
from auth.dependencies import get_current_user
from config.templates import templates
from core.content_negotiation import wants_json
from services.ssh_connection import SSHConnectionService

# Real, pre-existing gap found live 2026-09-06: this router had no
# router-level `dependencies=` at all, unlike every other sensitive
# router in this app (zfs_pools.py, zfs_replication.py, dashboard.py,
# ...) -- meaning list/add/delete/test-connection, and this same
# router's new register-existing route, were reachable with zero
# authentication to anyone with network access to the app. Confirmed
# live against a real instance before this fix landed. Matches the
# exact pattern used everywhere else in this codebase.
router = APIRouter(dependencies=[Depends(get_current_user)])
ssh_service = SSHConnectionService()


@router.get("/", response_class=HTMLResponse)
async def ssh_index(request: Request):
    """SSH connection management page.

    Supports content negotiation: `Accept: application/json` gets the
    connection list back as JSON instead of the rendered page.
    """
    try:
        connections = ssh_service.list_connections()
        if wants_json(request):
            return JSONResponse({"connections": connections})
        return templates.TemplateResponse(
            request,
            name="utils/ssh/index.jinja",
            context={
                "connections": connections
            }
        )
    except Exception as e:
        if wants_json(request):
            return JSONResponse({"error": f"Failed to load connections: {str(e)}"}, status_code=400)
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "message": f"Failed to load connections: {str(e)}"
            }
        )


@router.get("/add", response_class=HTMLResponse)
async def ssh_add_form(request: Request):
    """Add SSH connection form"""
    return templates.TemplateResponse(
        request, name="utils/ssh/add.jinja"
    )


@router.post("/register-existing")
async def ssh_register_existing(
    request: Request,
    name: Annotated[str, Form()],
    host: Annotated[str, Form()],
    username: Annotated[str, Form()],
    private_key_path: Annotated[str, Form()],
    port: Annotated[int, Form()] = 22,
    notes: Annotated[str, Form()] = "",
):
    """Register an SSH connection using an already-trusted private key,
    instead of create_connection's own password-bootstrap flow.

    JSON-only (no HTML form for this one -- it's aimed at scripted/IaC
    callers that already manage their own key trust, not the browser
    UI). `Accept: application/json` gets the new connection's id back;
    a plain request still gets a JSON body too, since there's no
    equivalent HTML page for this path.
    """
    try:
        connection_id = ssh_service.register_existing_connection(
            name=name,
            host=host,
            username=username,
            private_key_path=private_key_path,
            port=port,
            notes=notes,
        )
        return JSONResponse({"id": connection_id, "name": name, "host": host, "status": "ok"})
    except Exception as e:
        return JSONResponse({"error": f"Failed to register connection: {str(e)}"}, status_code=400)


@router.post("/add")
async def ssh_add_submit(
    request: Request,
    name: Annotated[str, Form()],
    host: Annotated[str, Form()],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    port: Annotated[int, Form()] = 22,
    notes: Annotated[str, Form()] = ""
):
    """Create new SSH connection with automatic key setup.

    Supports content negotiation: `Accept: application/json` gets the
    new connection's id back as JSON instead of a bare redirect (the
    HTML path never surfaces the generated id anywhere in the response).
    """
    try:
        connection_id = ssh_service.create_connection(
            name=name,
            host=host,
            username=username,
            password=password,  # Used once for key setup, then discarded
            port=port,
            notes=notes
        )

        if wants_json(request):
            return JSONResponse({"id": connection_id, "name": name, "host": host, "status": "ok"})
        # Redirect back to the main page
        return RedirectResponse(url="/utils/ssh", status_code=303)

    except Exception as e:
        if wants_json(request):
            return JSONResponse({"error": f"Failed to create connection: {str(e)}"}, status_code=400)
        # Return error page
        return templates.TemplateResponse(
            request,
            name="utils/ssh/add.jinja",
            context={
                "error": f"Failed to create connection: {str(e)}"
            },
            status_code=400
        )


@router.post("/{connection_id}/delete")
async def ssh_delete(
    request: Request,
    connection_id: str,
    remove_from_remote: Annotated[str, Form()] = "false"
):
    """Delete SSH connection (URL parameter version).

    Supports content negotiation: `Accept: application/json` gets a
    JSON confirmation/error instead of a redirect.
    """
    try:
        connection = ssh_service.get_connection(connection_id)
        if not connection:
            if wants_json(request):
                return JSONResponse({"id": connection_id, "error": "Connection not found"}, status_code=404)
            raise HTTPException(status_code=404, detail="Connection not found")

        remove_remote = remove_from_remote.lower() == "true"
        ssh_service.delete_connection(connection_id, remove_from_remote=remove_remote)

        if wants_json(request):
            return JSONResponse({"id": connection_id, "status": "deleted"})
        # Redirect back to the main page
        return RedirectResponse(url="/utils/ssh", status_code=303)

    except HTTPException:
        raise
    except Exception as e:
        if wants_json(request):
            return JSONResponse({"id": connection_id, "error": str(e)}, status_code=400)
        # For now, redirect back - could implement flash messages later
        return RedirectResponse(url="/utils/ssh", status_code=303)


@router.post("/delete")
async def ssh_delete_form(
    request: Request,
    connection_id: Annotated[str, Form()],
    remove_from_remote: Annotated[str, Form()] = "false"
):
    """Delete SSH connection (form data version)"""
    try:
        connection = ssh_service.get_connection(connection_id)
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")
        
        remove_remote = remove_from_remote.lower() == "true"
        ssh_service.delete_connection(connection_id, remove_from_remote=remove_remote)
        
        # Redirect back to the main page
        return RedirectResponse(url="/utils/ssh", status_code=303)
        
    except HTTPException:
        raise
    except Exception as e:
        # For now, redirect back - could implement flash messages later
        return RedirectResponse(url="/utils/ssh", status_code=303)


@router.post("/{connection_id}/test", response_class=HTMLResponse)
async def ssh_test(request: Request, connection_id: str):
    """Test SSH connection"""
    try:
        result = ssh_service.test_connection(connection_id)
        
        if result['status'] == 'success':
            return templates.TemplateResponse(
                request,
                name="partials/success.jinja",
                context={
                    "message": result['message']
                }
            )
        else:
            return templates.TemplateResponse(
                request,
                name="partials/error.jinja",
                context={
                    "message": result['message']
                }
            )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "message": f"Connection test failed: {str(e)}"
            }
        )
