"""
SMART Disk Monitoring Views
Provides web interface for SMART disk monitoring and management
"""
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse, StreamingResponse, Response
from typing import Annotated, Optional
from config.templates import templates
from services.smart_monitoring import SMARTMonitoringService
from auth.dependencies import get_current_user
from datetime import datetime
import io
import zipfile


router = APIRouter(tags=["smart"], dependencies=[Depends(get_current_user)])
smart_service = SMARTMonitoringService()


@router.get("/", response_class=HTMLResponse)
async def smart_index(request: Request):
    """Display all disks with SMART capability"""
    try:
        disks = smart_service.list_disks()
        return templates.TemplateResponse(
            "utils/smart/index.jinja",
            {
                "request": request,
                "disks": disks,
                "page_title": "SMART Disk Monitoring"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "utils/smart/index.jinja",
            {
                "request": request,
                "disks": [],
                "error": str(e),
                "page_title": "SMART Disk Monitoring"
            }
        )


@router.get("/disk/{disk_path:path}/attributes", response_class=HTMLResponse)
async def disk_attributes(request: Request, disk_path: str):
    """Display SMART attributes for a disk"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        attributes = smart_service.get_smart_attributes(disk_path)
        disk_info = smart_service.get_disk_info(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/attributes.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "attributes": attributes,
                "disk_info": disk_info,
                "page_title": f"SMART Attributes: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


@router.get("/disk/{disk_path:path}/health", response_class=HTMLResponse)
async def disk_health(request: Request, disk_path: str):
    """Display SMART health status"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        health = smart_service.get_smart_health(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/health.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "health": health,
                "page_title": f"Health Status: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


@router.get("/disk/{disk_path:path}/temperature", response_class=HTMLResponse)
async def disk_temperature(request: Request, disk_path: str):
    """Display disk temperature"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        temperature = smart_service.get_temperature(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/temperature.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "temperature": temperature,
                "page_title": f"Temperature: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


@router.get("/disk/{disk_path:path}/tests", response_class=HTMLResponse)
async def disk_tests(request: Request, disk_path: str):
    """Display test status and history"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        test_status = smart_service.get_test_status(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/tests.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "test_status": test_status,
                "page_title": f"SMART Tests: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


@router.post("/disk/{disk_path:path}/test/short", response_class=HTMLResponse)
async def start_short_test(request: Request, disk_path: str):
    """Start SMART short test"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        result = smart_service.start_short_test(disk_path)
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?message=Short test started successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?error={str(e)}",
            status_code=303
        )


@router.post("/disk/{disk_path:path}/test/long", response_class=HTMLResponse)
async def start_long_test(request: Request, disk_path: str):
    """Start SMART long test"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        result = smart_service.start_long_test(disk_path)
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?message=Long test started successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?error={str(e)}",
            status_code=303
        )


@router.post("/disk/{disk_path:path}/test/abort", response_class=HTMLResponse)
async def abort_test(request: Request, disk_path: str):
    """Abort running SMART test"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        smart_service.abort_test(disk_path)
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?message=Test aborted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}/tests?error={str(e)}",
            status_code=303
        )


@router.get("/disk/{disk_path:path}/errors", response_class=HTMLResponse)
async def disk_errors(request: Request, disk_path: str):
    """Display SMART error log"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        errors = smart_service.get_error_log(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/errors.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "errors": errors,
                "page_title": f"Error Log: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


@router.post("/disk/{disk_path:path}/enable", response_class=HTMLResponse)
async def enable_smart(request: Request, disk_path: str):
    """Enable SMART on a disk"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        smart_service.enable_smart(disk_path)
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}?message=SMART enabled successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}?error={str(e)}",
            status_code=303
        )


@router.post("/disk/{disk_path:path}/disable", response_class=HTMLResponse)
async def disable_smart(request: Request, disk_path: str):
    """Disable SMART on a disk"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        smart_service.disable_smart(disk_path)
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}?message=SMART disabled successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/disk/{disk_path.lstrip('/')}?error={str(e)}",
            status_code=303
        )


@router.get("/disk/{disk_path:path}/download")
async def download_smart_data(request: Request, disk_path: str):
    """Download complete SMART data as text file"""
    try:
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        smart_data = smart_service.get_smart_data(disk_path)
        
        # Format the data for download
        output = f"SMART Data Report for {disk_path}\n"
        output += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        output += "=" * 80 + "\n\n"
        output += smart_data.get('raw_output', '')
        
        # Create filename
        disk_name = disk_path.split('/')[-1]
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"smart_{disk_name}_{timestamp}.txt"
        
        return Response(
            content=output,
            media_type="application/octet-stream",
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": f"/utils/smart/disk/{disk_path}"
            }
        )


# This must come AFTER all specific routes to avoid catching them
@router.get("/disk/{disk_path:path}", response_class=HTMLResponse)
async def disk_detail(request: Request, disk_path: str):
    """Display detailed SMART data for a disk"""
    try:
        # Prepend / to disk_path if not present
        if not disk_path.startswith('/'):
            disk_path = '/' + disk_path
        
        smart_data = smart_service.get_smart_data(disk_path)
        
        return templates.TemplateResponse(
            "utils/smart/disk_detail.jinja",
            {
                "request": request,
                "disk_path": disk_path,
                "smart_data": smart_data,
                "page_title": f"SMART Data: {disk_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/utils/smart"
            }
        )


# Bulk Operations

@router.get("/download/all")
async def download_all_smart_data(request: Request):
    """Download SMART data for all disks as ZIP file"""
    try:
        disks = smart_service.list_disks()
        
        # Create in-memory ZIP file
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Add README
            readme_content = f"""SMART Data Archive
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

This archive contains SMART data for all disks on the system.
Each file contains the complete smartctl output for one disk.

Files included:
"""
            for disk in disks:
                disk_name = disk['path'].split('/')[-1]
                readme_content += f"  - smart_{disk_name}.txt\n"
            
            zip_file.writestr('README.txt', readme_content)
            
            # Add SMART data for each disk
            for disk in disks:
                try:
                    smart_data = smart_service.get_smart_data(disk['path'])
                    disk_name = disk['path'].split('/')[-1]
                    
                    content = f"SMART Data Report for {disk['path']}\n"
                    content += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    content += "=" * 80 + "\n\n"
                    content += smart_data.get('raw_output', 'No data available')
                    
                    zip_file.writestr(f'smart_{disk_name}.txt', content)
                except Exception as e:
                    # Add error file if disk fails
                    disk_name = disk['path'].split('/')[-1]
                    zip_file.writestr(f'smart_{disk_name}_ERROR.txt', f"Error retrieving data: {str(e)}")
        
        # Prepare ZIP for download
        zip_buffer.seek(0)
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"smart_all_disks_{timestamp}.zip"
        
        return StreamingResponse(
            iter([zip_buffer.getvalue()]),
            media_type="application/zip",
            headers={
                'Content-Disposition': f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart?error={str(e)}",
            status_code=303
        )


@router.post("/test/all/short")
async def start_all_short_tests(request: Request):
    """Start short test on all disks"""
    try:
        disks = smart_service.list_disks()
        success_count = 0
        errors = []
        
        for disk in disks:
            if disk['smart_enabled']:
                try:
                    smart_service.start_short_test(disk['path'])
                    success_count += 1
                except Exception as e:
                    errors.append(f"{disk['path']}: {str(e)}")
        
        if errors:
            error_msg = f"Started {success_count} test(s). Errors: " + "; ".join(errors[:3])
            return RedirectResponse(
                url=f"/utils/smart?error={error_msg}",
                status_code=303
            )
        else:
            return RedirectResponse(
                url=f"/utils/smart?message=Successfully started short tests on {success_count} disk(s)",
                status_code=303
            )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart?error={str(e)}",
            status_code=303
        )


@router.post("/test/all/long")
async def start_all_long_tests(request: Request):
    """Start long test on all disks"""
    try:
        disks = smart_service.list_disks()
        success_count = 0
        errors = []
        
        for disk in disks:
            if disk['smart_enabled']:
                try:
                    smart_service.start_long_test(disk['path'])
                    success_count += 1
                except Exception as e:
                    errors.append(f"{disk['path']}: {str(e)}")
        
        if errors:
            error_msg = f"Started {success_count} test(s). Errors: " + "; ".join(errors[:3])
            return RedirectResponse(
                url=f"/utils/smart?error={error_msg}",
                status_code=303
            )
        else:
            return RedirectResponse(
                url=f"/utils/smart?message=Successfully started long tests on {success_count} disk(s)",
                status_code=303
            )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart?error={str(e)}",
            status_code=303
        )


# Smartd Configuration


@router.get("/smartd", response_class=HTMLResponse)
async def smartd_index(request: Request):
    """Display smartd daemon status and configuration"""
    try:
        status = smart_service.get_smartd_status()
        config = smart_service.get_smartd_config()
        
        return templates.TemplateResponse(
            "utils/smart/smartd.jinja",
            {
                "request": request,
                "status": status,
                "config": config,
                "page_title": "Smartd Configuration"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "utils/smart/smartd.jinja",
            {
                "request": request,
                "status": {"error": str(e)},
                "config": "",
                "error": str(e),
                "page_title": "Smartd Configuration"
            }
        )


@router.post("/smartd/config", response_class=HTMLResponse)
async def update_smartd_config(
    request: Request,
    config: Annotated[str, Form()]
):
    """Update smartd.conf configuration"""
    try:
        smart_service.update_smartd_config(config)
        return RedirectResponse(
            url="/utils/smart/smartd?message=Configuration updated successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/smartd?error={str(e)}",
            status_code=303
        )


@router.post("/smartd/restart", response_class=HTMLResponse)
async def restart_smartd(request: Request):
    """Restart smartd daemon"""
    try:
        smart_service.restart_smartd()
        return RedirectResponse(
            url="/utils/smart/smartd?message=Smartd restarted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/smartd?error={str(e)}",
            status_code=303
        )


# Scheduled Tests


@router.get("/scheduled", response_class=HTMLResponse)
async def scheduled_tests(request: Request):
    """Display scheduled SMART tests"""
    try:
        scheduled = smart_service.list_scheduled_tests()
        disks = smart_service.list_disks()
        
        return templates.TemplateResponse(
            "utils/smart/scheduled.jinja",
            {
                "request": request,
                "scheduled_tests": scheduled,
                "disks": disks,
                "page_title": "Scheduled SMART Tests"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "utils/smart/scheduled.jinja",
            {
                "request": request,
                "scheduled_tests": [],
                "disks": [],
                "error": str(e),
                "page_title": "Scheduled SMART Tests"
            }
        )


@router.post("/scheduled/create", response_class=HTMLResponse)
async def create_scheduled_test(
    request: Request,
    disk: Annotated[str, Form()],
    test_type: Annotated[str, Form()],
    schedule: Annotated[str, Form()],
    enabled: Annotated[bool, Form()] = True
):
    """Create a new scheduled SMART test"""
    try:
        schedule_id = smart_service.create_scheduled_test(
            disk, test_type, schedule, enabled
        )
        return RedirectResponse(
            url="/utils/smart/scheduled?message=Scheduled test created successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/scheduled?error={str(e)}",
            status_code=303
        )


@router.post("/scheduled/{schedule_id}/delete", response_class=HTMLResponse)
async def delete_scheduled_test(request: Request, schedule_id: str):
    """Delete a scheduled SMART test"""
    try:
        smart_service.delete_scheduled_test(schedule_id)
        return RedirectResponse(
            url="/utils/smart/scheduled?message=Scheduled test deleted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/scheduled?error={str(e)}",
            status_code=303
        )


@router.post("/scheduled/{schedule_id}/toggle", response_class=HTMLResponse)
async def toggle_scheduled_test(request: Request, schedule_id: str):
    """Toggle enabled status of a scheduled test"""
    try:
        scheduled = smart_service.list_scheduled_tests()
        test = next((t for t in scheduled if t['id'] == schedule_id), None)
        
        if test:
            smart_service.update_scheduled_test(
                schedule_id,
                enabled=not test.get('enabled', True)
            )
        
        return RedirectResponse(
            url="/utils/smart/scheduled?message=Scheduled test updated successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/smart/scheduled?error={str(e)}",
            status_code=303
        )
