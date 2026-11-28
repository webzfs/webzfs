"""
Fleet Monitoring Views
Web interface for monitoring remote ZFS servers
"""
import logging

from fastapi import APIRouter, Request, Form, HTTPException, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from config.templates import templates
from auth.dependencies import get_current_user
from services.fleet_monitoring import FleetMonitoringService
from services.ssh_connection import SSHConnectionService
from services.fleet_command import get_command_context, save_order
from typing import Optional, Annotated
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fleet", tags=["fleet"], dependencies=[Depends(get_current_user)])
fleet_service = FleetMonitoringService()
ssh_service = SSHConnectionService()

# Handlers here are plain def, not async def: most open SSH connections to
# remote servers (paramiko, ssh-keyscan) that can take many seconds. FastAPI
# runs plain def handlers in its thread pool; an async def handler doing
# this would freeze every other request on the worker. Only
# fleet_command_order stays async because it awaits request.json().


def add_host_key_status(server: dict) -> dict:
    """
    Mark whether the server's SSH host key is pinned in the WebZFS
    known_hosts file, so the server card can show "Host key trusted"
    instead of the "Trust host key" action once a key is confirmed.
    """
    server["host_key_pinned"] = ssh_service.is_host_key_pinned(
        server["ip"], server["port"]
    )
    return server


@router.get("/", response_class=HTMLResponse)
def fleet_index(request: Request):
    """
    Main fleet view page.

    The page is rendered with the cached pool data already on disk
    (whatever fetch_server_pools last wrote). Each card embeds an
    HTMX `hx-trigger="load"` directive that fires after the page
    paints and replaces the card with a freshly refreshed copy. That
    keeps the initial paint instant even when the fleet has many
    servers, while still landing the user on up-to-date data within
    a couple of seconds.
    """
    servers = fleet_service.list_servers()
    for server in servers:
        add_host_key_status(server)

    return templates.TemplateResponse(
        request,
        name="fleet/index.jinja",
        context={
            "servers": servers,
            "active_page": "fleet"
        }
    )


@router.get("/servers/{server_id}/card", response_class=HTMLResponse)
def get_server_card_partial(request: Request, server_id: str):
    """
    HTMX endpoint that returns a single refreshed server card.

    Called automatically on page load (one request per server, with a
    small per-server stagger delay) and also from the per-card
    "Refresh" button. Always tries to fetch fresh pool data; on
    failure, falls back to whatever was cached so the user still sees
    something meaningful and a small inline error row.

    Returns the `partials/server_card.jinja` partial with
    `auto_refresh=False` so the refreshed card does not chain another
    background load and pin the SSH session in a refresh loop.
    """
    refresh_error = None
    try:
        # fetch_server_pools updates the persisted server record
        # (status / last_checked / pools) inside the service so the
        # subsequent get_server() call returns the freshly populated
        # values without us having to glue them together by hand.
        fleet_service.fetch_server_pools(server_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        refresh_error = str(e)

    try:
        server = fleet_service.get_server(server_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    add_host_key_status(server)

    return templates.TemplateResponse(
        request,
        name="fleet/partials/server_card.jinja",
        context={
            "server": server,
            "auto_refresh": False,
            "loop_index": 0,
            "refresh_error": refresh_error,
        }
    )



@router.get("/servers/add", response_class=HTMLResponse)
def add_server_form(request: Request):
    """Display form to add a new server"""
    # list_connections() reloads from disk to get latest connections
    ssh_connections = ssh_service.list_connections()
    
    return templates.TemplateResponse(
        request,
        name="fleet/add_server.jinja",
        context={
            "active_page": "fleet",
            "ssh_connections": ssh_connections
        }
    )


@router.post("/servers/add")
def add_server_submit(
    request: Request,
    ssh_connection_id: Annotated[str, Form()] = "",
    name: Annotated[str, Form()] = "",
    ip: Annotated[str, Form()] = "",
    username: Annotated[str, Form()] = "",
    password: Annotated[str, Form()] = "",
    port: Annotated[int, Form()] = 22
):
    """Process server addition form - supports both SSH connection and manual entry"""
    try:
        if ssh_connection_id:
            # Add server from SSH connection
            server_id = fleet_service.add_server_from_ssh_connection(
                ssh_connection_id=ssh_connection_id,
                name=name if name else None  # Use custom name if provided
            )
        else:
            # Manual entry - validate inputs
            if not name or not ip or not username or not password:
                raise HTTPException(status_code=400, detail="All fields are required for manual entry")
            
            if port < 1 or port > 65535:
                raise HTTPException(status_code=400, detail="Invalid port number")
            
            # Add server with password
            server_id = fleet_service.add_server(
                name=name,
                ip=ip,
                username=username,
                password=password,
                port=port
            )
        
        # Redirect to fleet index with success message. A manually added
        # server has no pinned host key yet; it is not contacted until the
        # administrator confirms the key on the Trust host key page.
        if ssh_connection_id:
            message = "Server added successfully"
        else:
            message = ("Server added. Use 'Trust host key' on its card to verify the "
                       "host key before it can be monitored.")
        return RedirectResponse(
            url="/fleet/?" + urlencode({"success": message}),
            status_code=303
        )
        
    except Exception as e:
        # list_connections() reloads from disk to get latest connections
        ssh_connections = ssh_service.list_connections()
        return templates.TemplateResponse(
            request,
            name="fleet/add_server.jinja",
            context={
                "active_page": "fleet",
                "ssh_connections": ssh_connections,
                "error": str(e),
                "name": name,
                "ip": ip,
                "username": username,
                "port": port
            }
        )


@router.get("/servers/remove", response_class=HTMLResponse)
def remove_server_list(request: Request):
    """Display list of servers with remove buttons"""
    servers = fleet_service.list_servers()
    
    return templates.TemplateResponse(
        request,
        name="fleet/remove_server.jinja",
        context={
            "servers": servers,
            "active_page": "fleet"
        }
    )


@router.post("/servers/remove")
def remove_server_without_id(request: Request):
    """Handle POST to /servers/remove without server ID - return error"""
    raise HTTPException(
        status_code=400, 
        detail="Server ID is required. Please specify which server to remove."
    )


@router.post("/servers/{server_id}/remove")
def remove_server_submit(request: Request, server_id: str):
    """Remove a server from the fleet"""
    try:
        fleet_service.remove_server(server_id)
        return RedirectResponse(
            url="/fleet/?success=Server removed successfully",
            status_code=303
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/{server_id}/test", response_class=HTMLResponse)
def test_server_connection(request: Request, server_id: str):
    """Test connection to a server"""
    try:
        result = fleet_service.test_connection(server_id)
        return templates.TemplateResponse(
            request,
            name="partials/success.jinja" if result.get("status") == "success" else "partials/error.jinja",
            context={
                "message": result.get("message", "Connection test completed")
            }
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "message": str(e)
            }
        )


@router.get("/servers/{server_id}/trust-host-key", response_class=HTMLResponse)
def trust_server_host_key_form(request: Request, server_id: str):
    """
    Step 1 of the show-and-confirm host key trust flow.

    Scans the remote host key WITHOUT pinning it and renders the
    fingerprints so the administrator can verify them out of band (for
    example against `ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub`
    run on the server's console). Nothing is written to known_hosts by
    this route.
    """
    try:
        server = fleet_service.get_server(server_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")

    host = server["ip"]
    port = server["port"]
    try:
        preview = ssh_service.preview_host_key(host, port)
    except Exception as e:
        logger.error(f"Host key scan failed for fleet server {server_id}: {e}")
        return RedirectResponse(
            url=f"/fleet/?error=Host key scan failed for {host}:{port}: {e}",
            status_code=303
        )

    return templates.TemplateResponse(
        request,
        name="fleet/trust_host_key.jinja",
        context={
            "active_page": "fleet",
            "server": server,
            "preview": preview,
        }
    )


@router.post("/servers/{server_id}/trust-host-key")
def trust_server_host_key(
    request: Request,
    server_id: str,
    fingerprint: Annotated[list[str], Form()] = []
):
    """
    Step 2 of the show-and-confirm host key trust flow.

    Pins the host key only after the administrator confirmed the exact
    fingerprints rendered by the GET route. The service re-scans and
    refuses the pin if the remote key changed in between, so a key is
    never trusted without having been displayed first.

    Servers added before host-key verification was enforced may have no
    pinned key, and a server whose key changed fails to connect. This is
    the deliberate "I verified the new key out of band" path; the key is
    never re-pinned automatically.
    """
    try:
        server = fleet_service.get_server(server_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")

    host = server["ip"]
    port = server["port"]
    try:
        fingerprints = ssh_service.confirm_host_key(host, port, fingerprint)
        logger.info(
            f"Pinned SSH host key for fleet server {server_id} "
            f"({host}:{port}): {', '.join(fingerprints)}"
        )
        return RedirectResponse(
            url=(
                f"/fleet/?success=Host key pinned for {host}:{port} "
                f"({', '.join(fingerprints)})"
            ),
            status_code=303
        )
    except Exception as e:
        logger.error(f"Failed to pin host key for fleet server {server_id}: {e}")
        return RedirectResponse(
            url=f"/fleet/?error={e}",
            status_code=303
        )


@router.post("/refresh")
def refresh_all_servers(request: Request):
    """Refresh pool data from all servers"""
    try:
        results = fleet_service.fetch_all_servers()
        # Return updated fleet view
        return RedirectResponse(url="/fleet/", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/servers/{server_id}/refresh")
def refresh_single_server(request: Request, server_id: str):
    """Refresh pool data from a single server"""
    try:
        pools = fleet_service.fetch_server_pools(server_id)
        # Return updated server card or redirect
        return RedirectResponse(url="/fleet/", status_code=303)
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/{server_id}/pools", response_class=HTMLResponse)
def get_server_pools_partial(request: Request, server_id: str):
    """Get pool data for a server (HTMX partial)"""
    try:
        server = fleet_service.get_server(server_id)
        pools = fleet_service.fetch_server_pools(server_id)
        
        return templates.TemplateResponse(
            request,
            name="fleet/partials/server_pools.jinja",
            context={
                "server": server,
                "pools": pools
            }
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/{server_id}/pools/{pool_name}/space-tree")
def get_pool_space_tree(request: Request, server_id: str, pool_name: str):
    """
    JSON endpoint that returns the dataset space-usage tree for a pool
    on a remote fleet server. Consumed by the dataset space visualizer
    modal on the fleet view page.
    """
    try:
        tree = fleet_service.fetch_pool_space_tree(server_id, pool_name)
        return JSONResponse({"success": True, "tree": tree})
    except KeyError:
        return JSONResponse(
            {"success": False, "error": "Server not found"},
            status_code=404,
        )
    except ValueError as e:
        return JSONResponse(
            {"success": False, "error": str(e)},
            status_code=400,
        )
    except Exception as e:
        return JSONResponse(
            {"success": False, "error": str(e)},
            status_code=500,
        )


# ---------------------------------------------------------------------------
# Fleet Pools - aggregated view of all pools (local + remote)
# ---------------------------------------------------------------------------


@router.get("/pools", response_class=HTMLResponse)
def fleet_pools_page(request: Request):
    """
    Fleet pools page shell. Renders immediately with a loading spinner.
    Pool data loads asynchronously via HTMX from /fleet/pools/data.
    """
    return templates.TemplateResponse(
        request,
        name="fleet/pools.jinja",
        context={
            "active_page": "fleet",
        }
    )


@router.get("/pools/data", response_class=HTMLResponse)
def fleet_pools_data(request: Request):
    """
    HTMX endpoint that gathers all pools from local server and remote
    fleet servers, merges them into a single sorted list, and returns
    the pool display partial with summary stats.

    Each pool dict includes a 'server_name' field used in the display
    name shown as "pool_name - server_name".

    Accepts an optional 'sort' query parameter:
      name (default), total, used, avail
    """
    import socket

    all_pools = []
    server_count = 0

    # Gather local pools
    try:
        from services.dashboard import get_pool_info_extended
        local_pools = get_pool_info_extended()
        local_hostname = socket.gethostname()
        for pool in local_pools:
            pool["server_name"] = local_hostname + " (local)"
            all_pools.append(pool)
        server_count += 1
    except Exception as exc:
        logger.error(f"Failed to get local pools for fleet pools view: {exc}")

    # Gather remote server pools
    servers = fleet_service.list_servers()
    for server in servers:
        try:
            remote_pools = fleet_service.fetch_server_pools_extended(server["id"])
            for pool in remote_pools:
                pool["server_name"] = server["name"]
                all_pools.append(pool)
            server_count += 1
        except Exception as exc:
            logger.error(
                f"Failed to get pools from fleet server {server['name']}: {exc}"
            )

    # Compute summary stats
    total_size_bytes = sum(p.get("total_bytes", 0) for p in all_pools)
    total_used_bytes = sum(p.get("used_bytes", 0) for p in all_pools)
    total_avail_bytes = sum(p.get("avail_bytes", 0) for p in all_pools)
    total_disks = sum(p.get("disk_count", 0) for p in all_pools)

    def format_bytes_human(val: int) -> str:
        """Format bytes to human-readable string."""
        if val == 0:
            return "0B"
        units = ["B", "K", "M", "G", "T", "P"]
        idx = 0
        fval = float(val)
        while fval >= 1024 and idx < len(units) - 1:
            fval /= 1024
            idx += 1
        if fval >= 100:
            return f"{int(fval)}{units[idx]}"
        elif fval >= 10:
            return f"{fval:.1f}{units[idx]}"
        else:
            return f"{fval:.2f}{units[idx]}"

    summary = {
        "total_size": format_bytes_human(total_size_bytes),
        "total_used": format_bytes_human(total_used_bytes),
        "total_avail": format_bytes_human(total_avail_bytes),
        "total_disks": total_disks,
        "system_count": server_count,
    }

    # Sort pools based on query parameter
    sort_by = request.query_params.get("sort", "name")
    if sort_by == "total":
        all_pools.sort(key=lambda p: p.get("total_bytes", 0), reverse=True)
    elif sort_by == "used":
        all_pools.sort(key=lambda p: p.get("used_bytes", 0), reverse=True)
    elif sort_by == "avail":
        all_pools.sort(key=lambda p: p.get("avail_bytes", 0), reverse=True)
    else:
        all_pools.sort(key=lambda p: p.get("name", "").lower())

    return templates.TemplateResponse(
        request,
        name="fleet/partials/all_pools.jinja",
        context={
            "pools": all_pools,
            "summary": summary,
            "sort_by": sort_by,
        }
    )


# ---------------------------------------------------------------------------
# FleetView C&C - read-only server stack view of the whole fleet
# ---------------------------------------------------------------------------


@router.get("/command", response_class=HTMLResponse)
def fleet_command_page(request: Request):
    """
    FleetView C&C page shell. Renders immediately; the server stack and
    the Needs Attention bar load from /fleet/command/data via HTMX.
    """
    return templates.TemplateResponse(
        request,
        name="fleet/command.jinja",
        context={
            "active_page": "fleet",
        }
    )


@router.get("/command/data", response_class=HTMLResponse)
def fleet_command_data(request: Request):
    """
    HTMX endpoint that polls the local host and every Fleet View server
    with read-only commands and returns the C&C content partial.

    Declared as a plain function so FastAPI runs the blocking SSH and
    subprocess work in its threadpool instead of on the event loop.
    """
    return templates.TemplateResponse(
        request,
        name="fleet/partials/command_data.jinja",
        context=get_command_context(fleet_service),
    )


@router.post("/command/order")
async def fleet_command_order(request: Request):
    """
    Save the FleetView C&C server card order. Body: {"order": [server ids]}.
    An empty list resets to the default order.

    This only stores a display preference; it does not touch any server.
    Only application/json bodies are accepted, which a cross-site HTML form
    cannot send without a CORS preflight.
    """
    content_type = request.headers.get("content-type", "").split(";")[0].strip().lower()
    if content_type != "application/json":
        raise HTTPException(status_code=415, detail="Content-Type must be application/json")

    try:
        body = await request.json()
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    order = body.get("order") if isinstance(body, dict) else None
    try:
        saved = save_order(order)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except OSError as exc:
        logger.error(f"Failed to save FleetView C&C card order: {exc}")
        raise HTTPException(status_code=500, detail="Could not save the card order")

    return JSONResponse({"order": saved})
