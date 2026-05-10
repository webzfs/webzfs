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
from typing import Optional, Annotated

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/fleet", tags=["fleet"], dependencies=[Depends(get_current_user)])
fleet_service = FleetMonitoringService()
ssh_service = SSHConnectionService()


@router.get("/", response_class=HTMLResponse)
async def fleet_index(request: Request):
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

    return templates.TemplateResponse(
        "fleet/index.jinja",
        {
            "request": request,
            "servers": servers,
            "active_page": "fleet"
        }
    )


@router.get("/servers/{server_id}/card", response_class=HTMLResponse)
async def get_server_card_partial(request: Request, server_id: str):
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

    return templates.TemplateResponse(
        "fleet/partials/server_card.jinja",
        {
            "request": request,
            "server": server,
            "auto_refresh": False,
            "loop_index": 0,
            "refresh_error": refresh_error,
        }
    )



@router.get("/servers/add", response_class=HTMLResponse)
async def add_server_form(request: Request):
    """Display form to add a new server"""
    # list_connections() reloads from disk to get latest connections
    ssh_connections = ssh_service.list_connections()
    
    return templates.TemplateResponse(
        "fleet/add_server.jinja",
        {
            "request": request,
            "active_page": "fleet",
            "ssh_connections": ssh_connections
        }
    )


@router.post("/servers/add")
async def add_server_submit(
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
        
        # Redirect to fleet index with success message
        return RedirectResponse(
            url="/fleet/?success=Server added successfully",
            status_code=303
        )
        
    except Exception as e:
        # list_connections() reloads from disk to get latest connections
        ssh_connections = ssh_service.list_connections()
        return templates.TemplateResponse(
            "fleet/add_server.jinja",
            {
                "request": request,
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
async def remove_server_list(request: Request):
    """Display list of servers with remove buttons"""
    servers = fleet_service.list_servers()
    
    return templates.TemplateResponse(
        "fleet/remove_server.jinja",
        {
            "request": request,
            "servers": servers,
            "active_page": "fleet"
        }
    )


@router.post("/servers/remove")
async def remove_server_without_id(request: Request):
    """Handle POST to /servers/remove without server ID - return error"""
    raise HTTPException(
        status_code=400, 
        detail="Server ID is required. Please specify which server to remove."
    )


@router.post("/servers/{server_id}/remove")
async def remove_server_submit(request: Request, server_id: str):
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
async def test_server_connection(request: Request, server_id: str):
    """Test connection to a server"""
    try:
        result = fleet_service.test_connection(server_id)
        return templates.TemplateResponse(
            "partials/success.jinja" if result.get("status") == "success" else "partials/error.jinja",
            {
                "request": request,
                "message": result.get("message", "Connection test completed")
            }
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "message": str(e)
            }
        )


@router.post("/refresh")
async def refresh_all_servers(request: Request):
    """Refresh pool data from all servers"""
    try:
        results = fleet_service.fetch_all_servers()
        # Return updated fleet view
        return RedirectResponse(url="/fleet/", status_code=303)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/servers/{server_id}/refresh")
async def refresh_single_server(request: Request, server_id: str):
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
async def get_server_pools_partial(request: Request, server_id: str):
    """Get pool data for a server (HTMX partial)"""
    try:
        server = fleet_service.get_server(server_id)
        pools = fleet_service.fetch_server_pools(server_id)
        
        return templates.TemplateResponse(
            "fleet/partials/server_pools.jinja",
            {
                "request": request,
                "server": server,
                "pools": pools
            }
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Server not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/servers/{server_id}/pools/{pool_name}/space-tree")
async def get_pool_space_tree(request: Request, server_id: str, pool_name: str):
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
async def fleet_pools_page(request: Request):
    """
    Fleet pools page shell. Renders immediately with a loading spinner.
    Pool data loads asynchronously via HTMX from /fleet/pools/data.
    """
    return templates.TemplateResponse(
        "fleet/pools.jinja",
        {
            "request": request,
            "active_page": "fleet",
        }
    )


@router.get("/pools/data", response_class=HTMLResponse)
async def fleet_pools_data(request: Request):
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
        "fleet/partials/all_pools.jinja",
        {
            "request": request,
            "pools": all_pools,
            "summary": summary,
            "sort_by": sort_by,
        }
    )
