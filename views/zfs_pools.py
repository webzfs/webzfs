"""
ZFS Pool Management Views
Provides web interface for ZFS pool operations
"""
import re
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from typing import Annotated
from config.templates import templates
from services.zfs_pool import ZFSPoolService
from services.zfs_dataset import ZFSDatasetService
from services.disk_utils import DiskUtilsService
from services.audit_logger import audit_logger
from auth.dependencies import get_current_user


router = APIRouter(prefix="/zfs/pools", tags=["zfs-pools"], dependencies=[Depends(get_current_user)])
pool_service = ZFSPoolService()
dataset_service = ZFSDatasetService()
disk_service = DiskUtilsService()


def parse_pool_status(status_output: str) -> dict:
    """Parse zpool status output into structured data."""
    result = {
        'state': 'UNKNOWN',
        'status_message': '',
        'action_message': '',
        'scan_info': '',
        'errors_line': '',
        'vdevs': [],
        'total_read_errors': 0,
        'total_write_errors': 0,
        'total_cksum_errors': 0,
    }

    lines = status_output.split('\n')
    in_config = False
    config_lines = []

    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith('state:'):
            result['state'] = stripped.split(':', 1)[1].strip()
        elif stripped.startswith('status:'):
            msg = stripped.split(':', 1)[1].strip()
            # Collect continuation lines
            j = i + 1
            while j < len(lines) and lines[j].startswith('\t') and not lines[j].strip().startswith(('action:', 'scan:', 'config:', 'errors:', 'see:', 'pool:')):
                msg += ' ' + lines[j].strip()
                j += 1
            result['status_message'] = msg
        elif stripped.startswith('action:'):
            msg = stripped.split(':', 1)[1].strip()
            j = i + 1
            while j < len(lines) and lines[j].startswith('\t') and not lines[j].strip().startswith(('scan:', 'config:', 'errors:', 'see:', 'pool:', 'status:')):
                msg += ' ' + lines[j].strip()
                j += 1
            result['action_message'] = msg
        elif stripped.startswith('scan:'):
            result['scan_info'] = stripped.split(':', 1)[1].strip()
        elif stripped.startswith('errors:'):
            result['errors_line'] = stripped.split(':', 1)[1].strip()
        elif stripped == '' and not in_config:
            pass
        elif 'NAME' in stripped and 'STATE' in stripped and 'READ' in stripped:
            in_config = True
            continue
        elif in_config:
            if stripped == '' or stripped.startswith('errors:'):
                in_config = False
                if stripped.startswith('errors:'):
                    result['errors_line'] = stripped.split(':', 1)[1].strip()
            else:
                config_lines.append(line)

    # Parse config lines into vdev tree
    for line in config_lines:
        # Count leading whitespace to determine depth
        stripped = line.lstrip()
        if not stripped:
            continue
        indent = len(line) - len(line.lstrip())
        parts = stripped.split()
        if len(parts) >= 2:
            name = parts[0]
            state = parts[1]
            read_err = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 0
            write_err = int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0
            cksum_err = int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else 0

            # Determine type: pool root, vdev, or leaf device
            # Pool root is typically indent ~8 (1 tab), vdevs ~10 (1 tab + 2), leaves ~12+ 
            vdev_type = 'leaf'
            if indent <= 8:
                vdev_type = 'pool'
            elif indent <= 10:
                vdev_type = 'vdev'

            result['vdevs'].append({
                'name': name,
                'state': state,
                'read_errors': read_err,
                'write_errors': write_err,
                'cksum_errors': cksum_err,
                'type': vdev_type,
                'indent': indent,
            })

            result['total_read_errors'] += read_err
            result['total_write_errors'] += write_err
            result['total_cksum_errors'] += cksum_err

    return result


@router.get("/", response_class=HTMLResponse)
async def pools_index(request: Request):
    """Display all ZFS pools"""
    try:
        pools = pool_service.list_pools()
        return templates.TemplateResponse(
            "zfs/pools/index.jinja",
            {
                "request": request,
                "pools": pools,
                "page_title": "ZFS Pools"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/pools/index.jinja",
            {
                "request": request,
                "pools": [],
                "error": str(e),
                "page_title": "ZFS Pools"
            }
        )


@router.get("/{pool_name}", response_class=HTMLResponse)
async def pool_detail(request: Request, pool_name: str):
    """Display detailed pool information"""
    try:
        pool_status = pool_service.get_pool_status(pool_name)

        # Parse structured data from status output
        parsed = parse_pool_status(pool_status.get('status_output', ''))

        # Get reservation from the root dataset (zfs property, not zpool)
        reservation_value = 'none'
        try:
            ds_props = dataset_service.get_properties(pool_name)
            if 'reservation' in ds_props:
                reservation_value = ds_props['reservation'].get('value', 'none')
        except Exception:
            pass

        # Get checkpoint info if supported
        checkpoint_info = None
        checkpoint_supported = pool_service.checkpoint_supported()
        if checkpoint_supported:
            try:
                checkpoint_info = pool_service.get_checkpoint_info(pool_name)
            except Exception:
                checkpoint_info = None

        return templates.TemplateResponse(
            "zfs/pools/detail.jinja",
            {
                "request": request,
                "pool": pool_status,
                "parsed": parsed,
                "reservation_value": reservation_value,
                "checkpoint_info": checkpoint_info,
                "checkpoint_supported": checkpoint_supported,
                "page_title": f"Pool: {pool_name}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/pools"
            }
        )


@router.get("/{pool_name}/history", response_class=HTMLResponse)
async def pool_history(request: Request, pool_name: str):
    """Display pool command history"""
    try:
        history = pool_service.get_pool_history(pool_name, internal=False, limit=1000)
        
        return templates.TemplateResponse(
            "zfs/pools/history.jinja",
            {
                "request": request,
                "pool_name": pool_name,
                "history": history,
                "page_title": f"Pool History: {pool_name}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/zfs/pools/{pool_name}"
            }
        )


@router.get("/{pool_name}/history/download")
async def download_pool_history(pool_name: str):
    """Download pool history as text file"""
    try:
        from fastapi.responses import PlainTextResponse
        from datetime import datetime
        
        history = pool_service.get_pool_history(pool_name, internal=False, limit=5000)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Format output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Pool History: {pool_name}")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        if history:
            for entry in history:
                output_lines.append(entry.get('entry', ''))
            
            output_lines.append("")
            output_lines.append(f"Total entries: {len(history)}")
        else:
            output_lines.append("No history entries found")
        
        output_lines.append("")
        output_lines.append("=" * 80)
        
        content = "\n".join(output_lines)
        
        return PlainTextResponse(
            content=content,
            headers={
                "Content-Disposition": f'attachment; filename="pool_{pool_name}_history_{timestamp}.txt"'
            }
        )
    except Exception as e:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(
            content=f"Error generating history file: {str(e)}",
            status_code=500
        )


@router.post("/{pool_name}/scrub", response_class=HTMLResponse)
async def scrub_pool(request: Request, pool_name: str, current_user: str = Depends(get_current_user)):
    """Start pool scrub"""
    try:
        pool_service.scrub_pool(pool_name)
        audit_logger.log_pool_scrub(user=current_user, pool_name=pool_name, action="start")
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?message=Scrub started successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_scrub(user=current_user, pool_name=pool_name, action="start", success=False, error=str(e))
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/scrub/stop", response_class=HTMLResponse)
async def stop_scrub(request: Request, pool_name: str, current_user: str = Depends(get_current_user)):
    """Stop pool scrub"""
    try:
        pool_service.stop_scrub(pool_name)
        audit_logger.log_pool_scrub(user=current_user, pool_name=pool_name, action="stop")
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?message=Scrub stopped successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_scrub(user=current_user, pool_name=pool_name, action="stop", success=False, error=str(e))
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )


@router.get("/create/form", response_class=HTMLResponse)
async def create_pool_form(request: Request):
    """Display pool creation form"""
    try:
        # Get available disks
        available_disks = disk_service.get_available_disks()
        
        # Separate disks by type and status
        hdds = [d for d in available_disks if d['type'] == 'HDD']
        ssds = [d for d in available_disks if d['type'] == 'SSD']
        
        return templates.TemplateResponse(
            "zfs/pools/create.jinja",
            {
                "request": request,
                "available_disks": available_disks,
                "hdds": hdds,
                "ssds": ssds,
                "page_title": "Create ZFS Pool"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/pools/create.jinja",
            {
                "request": request,
                "available_disks": [],
                "hdds": [],
                "ssds": [],
                "error": f"Error loading disks: {str(e)}",
                "page_title": "Create ZFS Pool"
            }
        )


@router.get("/create/check-disk-usage", response_class=JSONResponse)
async def check_disk_usage(request: Request):
    """Check disk usage status for pool creation"""
    try:
        disk_status = disk_service.check_disk_usage_status()
        return JSONResponse(content={
            "success": True,
            "disk_status": disk_status
        })
    except Exception as e:
        return JSONResponse(
            content={
                "success": False,
                "error": str(e)
            },
            status_code=500
        )


@router.post("/create", response_class=HTMLResponse)
async def create_pool(
    request: Request,
    pool_name: Annotated[str, Form()],
    vdev_type: Annotated[str, Form()],
    devices: Annotated[str, Form()],
    spare_devices: Annotated[str, Form()] = "",
    cache_devices: Annotated[str, Form()] = "",
    log_devices: Annotated[str, Form()] = "",
    metadata_devices: Annotated[str, Form()] = "",
    dedup_devices: Annotated[str, Form()] = "",
    force: Annotated[bool, Form()] = False,
    ashift: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Create a new pool"""
    try:
        # Build vdev specification
        vdevs = []
        
        # Parse data vdevs - can have multiple vdevs separated by spaces
        # Format: "vdev1disk1,vdev1disk2 vdev2disk1,vdev2disk2"
        for vdev_devices in devices.split():
            device_list = [d.strip() for d in vdev_devices.split(',') if d.strip()]
            if device_list:
                if vdev_type != "single":
                    vdevs.append(vdev_type)
                vdevs.extend(device_list)
        
        # Add spare devices
        if spare_devices:
            spare_list = [d.strip() for d in spare_devices.split(',') if d.strip()]
            if spare_list:
                vdevs.append('spare')
                vdevs.extend(spare_list)
        
        # Add cache devices
        if cache_devices:
            cache_list = [d.strip() for d in cache_devices.split(',') if d.strip()]
            if cache_list:
                vdevs.append('cache')
                vdevs.extend(cache_list)
        
        # Add log devices (SLOG)
        if log_devices:
            log_list = [d.strip() for d in log_devices.split(',') if d.strip()]
            if log_list:
                vdevs.append('log')
                # Mirror log devices if there are multiple
                if len(log_list) > 1:
                    vdevs.append('mirror')
                vdevs.extend(log_list)
        
        # Add metadata special vdevs
        # Format: "mirror1disk1,mirror1disk2 mirror2disk1,mirror2disk2"
        if metadata_devices:
            for mirror_devices in metadata_devices.split():
                device_list = [d.strip() for d in mirror_devices.split(',') if d.strip()]
                if device_list:
                    vdevs.append('special')
                    if len(device_list) > 1:
                        vdevs.append('mirror')
                    vdevs.extend(device_list)
        
        # Add dedup devices
        if dedup_devices:
            dedup_list = [d.strip() for d in dedup_devices.split(',') if d.strip()]
            if dedup_list:
                vdevs.append('dedup')
                # Mirror dedup devices if there are multiple
                if len(dedup_list) > 1:
                    vdevs.append('mirror')
                vdevs.extend(dedup_list)
        
        # Build properties dictionary
        properties = {}
        if ashift:
            properties['ashift'] = ashift
        
        pool_service.create_pool(pool_name, vdevs, properties=properties if properties else None, force=force)
        
        # Log successful pool creation
        audit_logger.log_pool_create(user=current_user, pool_name=pool_name, vdevs=vdevs)
        
        return RedirectResponse(
            url=f"/zfs/pools?message=Pool {pool_name} created successfully",
            status_code=303
        )
    except Exception as e:
        # Log failed pool creation
        audit_logger.log_pool_create(user=current_user, pool_name=pool_name, vdevs=vdevs, success=False, error=str(e))
        
        return templates.TemplateResponse(
            "zfs/pools/create.jinja",
            {
                "request": request,
                "error": str(e),
                "pool_name": pool_name,
                "vdev_type": vdev_type,
                "devices": devices,
                "ashift": ashift,
                "page_title": "Create ZFS Pool"
            }
        )


@router.get("/{pool_name}/export/confirm", response_class=HTMLResponse)
async def export_pool_confirm(request: Request, pool_name: str):
    """Display export confirmation page"""
    return templates.TemplateResponse(
        "zfs/pools/export_confirm.jinja",
        {
            "request": request,
            "pool_name": pool_name,
            "page_title": f"Export Pool: {pool_name}"
        }
    )


@router.post("/{pool_name}/export", response_class=HTMLResponse)
async def export_pool(
    request: Request,
    pool_name: str,
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Export a pool"""
    try:
        pool_service.export_pool(pool_name, force=force)
        audit_logger.log_pool_export(user=current_user, pool_name=pool_name, force=force)
        return RedirectResponse(
            url="/zfs/pools?message=Pool exported successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_export(user=current_user, pool_name=pool_name, force=force, success=False, error=str(e))
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )


@router.get("/import/list", response_class=HTMLResponse)
async def import_pools_list(request: Request):
    """Display list of importable pools"""
    try:
        importable_pools = pool_service.get_importable_pools()
        return templates.TemplateResponse(
            "zfs/pools/import.jinja",
            {
                "request": request,
                "importable_pools": importable_pools,
                "page_title": "Import ZFS Pools"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/pools/import.jinja",
            {
                "request": request,
                "importable_pools": [],
                "error": str(e),
                "page_title": "Import ZFS Pools"
            }
        )


@router.post("/import/{pool_name}", response_class=HTMLResponse)
async def import_pool(
    request: Request,
    pool_name: str,
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Import a pool"""
    try:
        pool_service.import_pool(pool_name, force=force)
        audit_logger.log_pool_import(user=current_user, pool_name=pool_name, force=force)
        return RedirectResponse(
            url=f"/zfs/pools?message=Pool {pool_name} imported successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_import(user=current_user, pool_name=pool_name, force=force, success=False, error=str(e))
        return RedirectResponse(
            url="/zfs/pools/import/list?error=" + str(e),
            status_code=303
        )


@router.get("/{pool_name}/properties", response_class=HTMLResponse)
async def pool_properties(request: Request, pool_name: str):
    """Display pool properties"""
    try:
        pool_status = pool_service.get_pool_status(pool_name)
        properties = pool_status.get('properties', {})
        
        return templates.TemplateResponse(
            "zfs/pools/properties.jinja",
            {
                "request": request,
                "pool_name": pool_name,
                "properties": properties,
                "page_title": f"Pool Properties: {pool_name}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/zfs/pools/{pool_name}"
            }
        )


@router.post("/{pool_name}/properties", response_class=HTMLResponse)
async def set_pool_property(
    request: Request,
    pool_name: str,
    property_name: Annotated[str, Form()],
    property_value: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Set a pool property"""
    try:
        pool_service.set_pool_property(pool_name, property_name, property_value)
        audit_logger.log_pool_property_change(
            user=current_user, pool_name=pool_name, 
            property_name=property_name, property_value=property_value
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/properties?message=Property updated successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_property_change(
            user=current_user, pool_name=pool_name,
            property_name=property_name, property_value=property_value,
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/properties?error={str(e)}",
            status_code=303
        )


@router.get("/{pool_name}/properties/download")
async def download_pool_properties(pool_name: str):
    """Download pool properties as text file"""
    try:
        from fastapi.responses import PlainTextResponse
        from datetime import datetime
        
        pool_status = pool_service.get_pool_status(pool_name)
        properties = pool_status.get('properties', {})
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Format output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Pool Properties: {pool_name}")
        output_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        output_lines.append("=" * 80)
        output_lines.append("")
        
        if properties:
            # Header
            output_lines.append(f"{'Property':<30} {'Value':<30} {'Source':<20}")
            output_lines.append("-" * 80)
            
            # Property rows (sorted alphabetically)
            for prop_name in sorted(properties.keys()):
                prop_data = properties[prop_name]
                value = str(prop_data.get('value', '-'))
                source = str(prop_data.get('source', '-'))
                output_lines.append(f"{prop_name:<30} {value:<30} {source:<20}")
            
            output_lines.append("")
            output_lines.append(f"Total properties: {len(properties)}")
        else:
            output_lines.append("No properties available")
        
        output_lines.append("")
        output_lines.append("=" * 80)
        
        content = "\n".join(output_lines)
        
        return PlainTextResponse(
            content=content,
            headers={
                "Content-Disposition": f'attachment; filename="pool_{pool_name}_properties_{timestamp}.txt"'
            }
        )
    except Exception as e:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(
            content=f"Error generating properties file: {str(e)}",
            status_code=500
        )


@router.post("/{pool_name}/checkpoint", response_class=HTMLResponse)
async def create_checkpoint(
    request: Request,
    pool_name: str,
    current_user: str = Depends(get_current_user)
):
    """Create a checkpoint for the pool"""
    try:
        pool_service.create_checkpoint(pool_name)
        audit_logger.log_pool_checkpoint_create(user=current_user, pool_name=pool_name)
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?message=Checkpoint created successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_checkpoint_create(
            user=current_user, pool_name=pool_name, success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/checkpoint/discard", response_class=HTMLResponse)
async def discard_checkpoint(
    request: Request,
    pool_name: str,
    current_user: str = Depends(get_current_user)
):
    """Discard the checkpoint for the pool"""
    try:
        pool_service.discard_checkpoint(pool_name)
        audit_logger.log_pool_checkpoint_discard(user=current_user, pool_name=pool_name)
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?message=Checkpoint discarded successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_checkpoint_discard(
            user=current_user, pool_name=pool_name, success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )


# ==================== VDev Management Routes ====================


@router.get("/{pool_name}/vdevs", response_class=HTMLResponse)
async def vdev_management(request: Request, pool_name: str):
    """Display vdev management page shell (data loaded async via JS)"""
    return templates.TemplateResponse(
        "zfs/pools/vdevs.jinja",
        {
            "request": request,
            "pool_name": pool_name,
            "page_title": f"VDev Management: {pool_name}"
        }
    )


@router.get("/{pool_name}/vdevs/data", response_class=JSONResponse)
async def vdev_management_data(request: Request, pool_name: str):
    """Return topology and disk data as JSON for async page loading"""
    try:
        topology = pool_service.get_pool_topology(pool_name)
        available_disks = disk_service.get_available_disks()

        # Separate disks by status
        all_disks = []
        for disk in available_disks:
            all_disks.append({
                'name': disk.get('name', ''),
                'device_path': disk.get('device_path', ''),
                'size': disk.get('size', ''),
                'model': disk.get('model', 'Unknown'),
                'type': disk.get('type', 'HDD'),
                'in_use': disk.get('in_use', False),
                'is_system_disk': disk.get('is_system_disk', False),
                'system_usage': disk.get('system_usage', ''),
            })

        # Build list of devices currently in the pool topology
        pool_devices = []
        for section in ['data_vdevs', 'log_vdevs', 'cache_vdevs',
                        'spare_vdevs', 'special_vdevs', 'dedup_vdevs']:
            for vdev in topology.get(section, []):
                for dev in vdev.get('devices', []):
                    pool_devices.append(dev.get('name', ''))

        # Convert topology to serializable dict
        topology_dict = {
            'pool_name': topology.get('pool_name', pool_name),
            'state': topology.get('state', 'UNKNOWN'),
            'scan_info': topology.get('scan_info', ''),
            'data_vdevs': topology.get('data_vdevs', []),
            'log_vdevs': topology.get('log_vdevs', []),
            'cache_vdevs': topology.get('cache_vdevs', []),
            'spare_vdevs': topology.get('spare_vdevs', []),
            'special_vdevs': topology.get('special_vdevs', []),
            'dedup_vdevs': topology.get('dedup_vdevs', []),
        }

        return JSONResponse(content={
            "success": True,
            "topology": topology_dict,
            "all_disks": all_disks,
            "pool_devices": pool_devices,
        })
    except Exception as e:
        return JSONResponse(
            content={"success": False, "error": str(e)},
            status_code=500
        )


@router.post("/{pool_name}/vdevs/acknowledge", response_class=JSONResponse)
async def acknowledge_vdev_warning(
    request: Request,
    pool_name: str,
    current_user: str = Depends(get_current_user)
):
    """Log that the user acknowledged the VDev management data loss warning"""
    audit_logger.log_vdev_warning_acknowledge(user=current_user, pool_name=pool_name)
    return JSONResponse(content={"success": True})


@router.get("/{pool_name}/vdevs/check-disk-usage", response_class=JSONResponse)
async def vdev_check_disk_usage(request: Request, pool_name: str):
    """Check disk usage status for vdev management page"""
    try:
        disk_status = disk_service.check_disk_usage_status()
        return JSONResponse(content={
            "success": True,
            "disk_status": disk_status
        })
    except Exception as e:
        return JSONResponse(
            content={"success": False, "error": str(e)},
            status_code=500
        )


@router.post("/{pool_name}/vdevs/add", response_class=HTMLResponse)
async def add_vdev(
    request: Request,
    pool_name: str,
    vdev_type: Annotated[str, Form()],
    devices: Annotated[str, Form()],
    vdev_layout: Annotated[str, Form()] = "stripe",
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Add an auxiliary vdev to the pool"""
    try:
        vdevs = []
        device_list = [d.strip() for d in devices.split(',') if d.strip()]
        if not device_list:
            raise ValueError("No devices specified")

        # Always add the vdev type keyword first
        vdevs.append(vdev_type)

        # Add mirror keyword only for types that support it: log and special
        # Spares are individual disks, cache cannot be mirrored,
        # dedup vdevs are individual disks (ZFS manages allocation)
        if vdev_layout == "mirror" and vdev_type in ("log", "special"):
            if len(device_list) < 2:
                raise ValueError("Mirror layout requires at least 2 devices")
            vdevs.append("mirror")

        vdevs.extend(device_list)

        pool_service.add_vdev(pool_name, vdevs, force=force)
        audit_logger.log_pool_vdev_add(
            user=current_user, pool_name=pool_name,
            vdevs=','.join(vdevs)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=VDev added successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_vdev_add(
            user=current_user, pool_name=pool_name,
            vdevs=devices, success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/attach", response_class=HTMLResponse)
async def attach_device(
    request: Request,
    pool_name: str,
    existing_device: Annotated[str, Form()],
    new_device: Annotated[str, Form()],
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Attach a device to create or extend a mirror"""
    try:
        pool_service.attach_device(
            pool_name, existing_device.strip(),
            new_device.strip(), force=force
        )
        audit_logger.log_pool_vdev_attach(
            user=current_user, pool_name=pool_name,
            existing_device=existing_device.strip(),
            new_device=new_device.strip()
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=Device attached successfully. Resilvering will begin.",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_vdev_attach(
            user=current_user, pool_name=pool_name,
            existing_device=existing_device.strip(),
            new_device=new_device.strip(),
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/detach", response_class=HTMLResponse)
async def detach_device(
    request: Request,
    pool_name: str,
    device: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Detach a device from a mirror"""
    try:
        pool_service.detach_device(pool_name, device.strip())
        audit_logger.log_pool_vdev_detach(
            user=current_user, pool_name=pool_name,
            device=device.strip()
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=Device detached successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_vdev_detach(
            user=current_user, pool_name=pool_name,
            device=device.strip(),
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/replace", response_class=HTMLResponse)
async def replace_device(
    request: Request,
    pool_name: str,
    old_device: Annotated[str, Form()],
    new_device: Annotated[str, Form()],
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Replace a device in the pool"""
    try:
        pool_service.replace_device(
            pool_name, old_device.strip(),
            new_device.strip(), force=force
        )
        audit_logger.log_pool_vdev_replace(
            user=current_user, pool_name=pool_name,
            old_device=old_device.strip(),
            new_device=new_device.strip()
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=Device replacement started. Monitor resilvering progress on this page.",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_vdev_replace(
            user=current_user, pool_name=pool_name,
            old_device=old_device.strip(),
            new_device=new_device.strip(),
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/remove", response_class=HTMLResponse)
async def remove_vdev(
    request: Request,
    pool_name: str,
    device: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Remove a vdev from the pool"""
    try:
        pool_service.remove_vdev(pool_name, device.strip())
        audit_logger.log_pool_vdev_remove(
            user=current_user, pool_name=pool_name,
            device=device.strip()
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=VDev removal initiated",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_vdev_remove(
            user=current_user, pool_name=pool_name,
            device=device.strip(),
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/online", response_class=HTMLResponse)
async def online_device(
    request: Request,
    pool_name: str,
    device: Annotated[str, Form()],
    expand: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Bring a device online"""
    try:
        pool_service.online_device(pool_name, device.strip(), expand=expand)
        audit_logger.log_pool_device_online(
            user=current_user, pool_name=pool_name,
            device=device.strip(), expand=expand
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message=Device brought online successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_device_online(
            user=current_user, pool_name=pool_name,
            device=device.strip(), expand=expand,
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/vdevs/offline", response_class=HTMLResponse)
async def offline_device(
    request: Request,
    pool_name: str,
    device: Annotated[str, Form()],
    temporary: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Take a device offline"""
    try:
        pool_service.offline_device(
            pool_name, device.strip(), temporary=temporary
        )
        audit_logger.log_pool_device_offline(
            user=current_user, pool_name=pool_name,
            device=device.strip(), temporary=temporary
        )
        msg = "Device taken offline"
        if temporary:
            msg += " (temporary, will auto-online on reboot)"
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?message={msg}",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_pool_device_offline(
            user=current_user, pool_name=pool_name,
            device=device.strip(), temporary=temporary,
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}/vdevs?error={str(e)}",
            status_code=303
        )


@router.post("/{pool_name}/reservation", response_class=HTMLResponse)
async def set_pool_reservation(
    request: Request,
    pool_name: str,
    reservation_size: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Set or remove pool reservation (zfs dataset property on root dataset)"""
    try:
        value = reservation_size.strip() if reservation_size.strip() else 'none'
        dataset_service.set_property(pool_name, 'reservation', value)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="set_reservation",
            pool=pool_name,
            value=value
        )
        msg = f"Reservation set to {value}" if value != 'none' else "Reservation removed"
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?message={msg}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/pools/{pool_name}?error={str(e)}",
            status_code=303
        )
