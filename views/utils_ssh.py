"""
SSH Connection Management Views
HTTP routes for managing SSH connections
"""
from fastapi import APIRouter, Depends, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Annotated
from urllib.parse import urlencode
from auth.dependencies import get_current_user
from config.templates import templates
from services.ssh_connection import SSHConnectionService

# Real, pre-existing gap found live 2026-09-06: this router had no
# router-level `dependencies=` at all, unlike every other sensitive
# router in this app (zfs_pools.py, zfs_replication.py, dashboard.py,
# ...) -- meaning list/add/delete/test-connection were reachable with
# zero authentication to anyone with network access to the app.
# Confirmed live against a real instance before this fix landed.
# Matches the exact pattern used everywhere else in this codebase.
router = APIRouter(dependencies=[Depends(get_current_user)])
ssh_service = SSHConnectionService()

# Every handler here is a plain def, not async def: they run ssh-keyscan,
# ssh-keygen, ssh-copy-id and paramiko connections that can take many
# seconds. FastAPI runs plain def handlers in its thread pool; an async def
# handler doing this would freeze every other request on the worker.


def delete_and_redirect(connection_id: str, remove_from_remote: str) -> RedirectResponse:
    """Delete a connection and return to the list with the outcome shown."""
    connection = ssh_service.get_connection(connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        remote_error = ssh_service.delete_connection(
            connection_id, remove_from_remote=remove_from_remote.lower() == "true"
        )
    except Exception as e:
        return RedirectResponse(
            url="/utils/ssh?" + urlencode({"error": f"Failed to delete connection: {e}"}),
            status_code=303,
        )
    if remote_error:
        message = {"error": f"Connection '{connection['name']}' deleted locally. {remote_error}"}
    else:
        message = {"success": f"Connection '{connection['name']}' deleted."}
    return RedirectResponse(url="/utils/ssh?" + urlencode(message), status_code=303)


@router.get("/", response_class=HTMLResponse)
def ssh_index(request: Request):
    """SSH connection management page"""
    try:
        connections = []
        for conn in ssh_service.list_connections():
            conn = dict(conn)
            conn["host_key_pinned"] = ssh_service.is_host_key_pinned(conn["host"], conn["port"])
            connections.append(conn)
        return templates.TemplateResponse(
            request,
            name="utils/ssh/index.jinja",
            context={
                "connections": connections
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "message": f"Failed to load connections: {str(e)}"
            }
        )


@router.get("/{connection_id}/trust-host-key", response_class=HTMLResponse)
def ssh_trust_host_key_form(request: Request, connection_id: str):
    """
    Step 1 of the show-and-confirm host key trust flow for a connection.

    Scans the host key without pinning anything and shows the fingerprints
    so the administrator can verify them out of band. Used when the pin is
    missing (for example after restoring a backup on a new machine, if the
    host now presents different keys) or when the host key changed.
    """
    connection = ssh_service.get_connection(connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        preview = ssh_service.preview_host_key(connection["host"], connection["port"])
    except Exception as e:
        return RedirectResponse(
            url="/utils/ssh?" + urlencode({"error": f"Host key scan failed: {e}"}),
            status_code=303,
        )
    return templates.TemplateResponse(
        request,
        name="utils/ssh/trust_host_key.jinja",
        context={"connection": connection, "preview": preview},
    )


@router.post("/{connection_id}/trust-host-key")
def ssh_trust_host_key(
    connection_id: str,
    fingerprint: Annotated[list[str], Form()] = [],
):
    """
    Step 2: pin the host key only if the host still presents exactly the
    fingerprints shown in step 1, and record them on the connection so a
    later lost pin is restored to these keys only.
    """
    connection = ssh_service.get_connection(connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    try:
        fingerprints = ssh_service.confirm_host_key(
            connection["host"], connection["port"], fingerprint
        )
        ssh_service.set_host_key_fingerprints(connection_id, fingerprints)
    except Exception as e:
        return RedirectResponse(
            url="/utils/ssh?" + urlencode({"error": str(e)}), status_code=303
        )
    message = (
        f"Host key pinned for {connection['host']}:{connection['port']} "
        f"({', '.join(fingerprints)})"
    )
    return RedirectResponse(url="/utils/ssh?" + urlencode({"success": message}), status_code=303)


@router.get("/add", response_class=HTMLResponse)
def ssh_add_form(request: Request):
    """Add SSH connection form"""
    return templates.TemplateResponse(
        request, name="utils/ssh/add.jinja"
    )


@router.post("/scan-host-key", response_class=HTMLResponse)
def ssh_scan_host_key(
    request: Request,
    host: Annotated[str, Form()],
    port: Annotated[int, Form()] = 22
):
    """
    Scan a host key and render its fingerprint for review, without
    pinning anything and without a password being involved.

    This backs the "Check host key" step on the add form so the
    administrator can verify the fingerprint out of band before the
    password is ever submitted.
    """
    try:
        preview = ssh_service.preview_host_key(host, port)
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="utils/ssh/host_key_preview.jinja",
            context={"error": str(e)}
        )

    return templates.TemplateResponse(
        request,
        name="utils/ssh/host_key_preview.jinja",
        context={"preview": preview}
    )


@router.post("/add")
def ssh_add_submit(
    request: Request,
    name: Annotated[str, Form()],
    host: Annotated[str, Form()],
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    port: Annotated[int, Form()] = 22,
    notes: Annotated[str, Form()] = "",
    fingerprint: Annotated[list[str], Form()] = []
):
    """Create new SSH connection with automatic key setup"""
    try:
        connection_id = ssh_service.create_connection(
            name=name,
            host=host,
            username=username,
            password=password,  # Used once for key setup, then discarded
            port=port,
            notes=notes,
            # Fingerprints the admin reviewed on the form, if any. Binds
            # the pin to the approved key before the password is sent.
            confirmed_fingerprints=fingerprint
        )
        
        # Redirect back to the main page
        return RedirectResponse(url="/utils/ssh", status_code=303)
        
    except Exception as e:
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
def ssh_delete(
    request: Request,
    connection_id: str,
    remove_from_remote: Annotated[str, Form()] = "false"
):
    """Delete SSH connection (URL parameter version)"""
    return delete_and_redirect(connection_id, remove_from_remote)


@router.post("/delete")
def ssh_delete_form(
    request: Request,
    connection_id: Annotated[str, Form()],
    remove_from_remote: Annotated[str, Form()] = "false"
):
    """Delete SSH connection (form data version)"""
    return delete_and_redirect(connection_id, remove_from_remote)


@router.post("/{connection_id}/test", response_class=HTMLResponse)
def ssh_test(request: Request, connection_id: str):
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
