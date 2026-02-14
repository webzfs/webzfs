"""
ZFS Replication Management Views
Provides web interface for ZFS replication operations using native send/receive and syncoid
"""
from fastapi import APIRouter, Request, Form, Depends, Body
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from typing import Annotated, Optional, Dict
import platform
from config.templates import templates
from services.zfs_replication import ZFSReplicationService, ReplicationType, CompressionMethod
from services.syncoid import SyncoidService
from services.zfs_dataset import ZFSDatasetService
from services.zfs_snapshot import ZFSSnapshotService
from services.ssh_connection import SSHConnectionService
from auth.dependencies import get_current_user


router = APIRouter(prefix="/zfs/replication", tags=["zfs-replication"], dependencies=[Depends(get_current_user)])
replication_service = ZFSReplicationService()
syncoid_service = SyncoidService()
dataset_service = ZFSDatasetService()
snapshot_service = ZFSSnapshotService()
ssh_service = SSHConnectionService()


@router.get("/", response_class=HTMLResponse)
async def replication_index(request: Request):
    """Display replication management dashboard"""
    try:
        # Get replication jobs
        jobs = replication_service.list_replication_jobs()
        
        # Check syncoid status
        syncoid_status = syncoid_service.check_syncoid_status()
        
        # Detect OS
        system = platform.system()
        
        return templates.TemplateResponse(
            "zfs/replication/index.jinja",
            {
                "request": request,
                "jobs": jobs,
                "syncoid_status": syncoid_status,
                "system": system,
                "page_title": "ZFS Replication"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/index.jinja",
            {
                "request": request,
                "jobs": [],
                "syncoid_status": {'installed': False},
                "system": platform.system(),
                "error": str(e),
                "page_title": "ZFS Replication"
            }
        )


@router.get("/jobs/create/form", response_class=HTMLResponse)
async def create_job_form(request: Request):
    """Display create replication job form"""
    try:
        # Get available datasets
        datasets = dataset_service.list_datasets()
        
        return templates.TemplateResponse(
            "zfs/replication/job_create.jinja",
            {
                "request": request,
                "datasets": datasets,
                "replication_types": [t.value for t in ReplicationType],
                "compression_methods": [c.value for c in CompressionMethod],
                "page_title": "Create Replication Job"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/job_create.jinja",
            {
                "request": request,
                "datasets": [],
                "error": str(e),
                "page_title": "Create Replication Job"
            }
        )


@router.post("/jobs/create", response_class=HTMLResponse)
async def create_job(
    request: Request,
    name: Annotated[str, Form()],
    source_dataset: Annotated[str, Form()],
    target_dataset: Annotated[str, Form()],
    replication_type: Annotated[str, Form()],
    schedule: Annotated[str, Form()],
    enabled: Annotated[bool, Form()] = True,
    recursive: Annotated[bool, Form()] = False,
    compression: Annotated[str, Form()] = "lz4",
    remote_host: Annotated[str, Form()] = "",
    remote_port: Annotated[int, Form()] = 22,
    ssh_key: Annotated[str, Form()] = ""
):
    """Create a new replication job"""
    try:
        options = {}
        if remote_host:
            options['remote_host'] = remote_host
            options['remote_port'] = remote_port
        if ssh_key:
            options['ssh_key'] = ssh_key
        
        job_id = replication_service.create_replication_job(
            name=name,
            source_dataset=source_dataset,
            target_dataset=target_dataset,
            replication_type=ReplicationType(replication_type),
            schedule=schedule,
            enabled=enabled,
            recursive=recursive,
            compression=CompressionMethod(compression),
            **options
        )
        
        return RedirectResponse(
            url=f"/zfs/replication?message=Replication job '{name}' created successfully",
            status_code=303
        )
    except Exception as e:
        datasets = dataset_service.list_datasets()
        return templates.TemplateResponse(
            "zfs/replication/job_create.jinja",
            {
                "request": request,
                "datasets": datasets,
                "replication_types": [t.value for t in ReplicationType],
                "compression_methods": [c.value for c in CompressionMethod],
                "error": str(e),
                "page_title": "Create Replication Job"
            }
        )


@router.get("/jobs/{job_id}/detail", response_class=HTMLResponse)
async def job_detail(request: Request, job_id: str):
    """Display replication job details"""
    try:
        job = replication_service.get_replication_job(job_id)
        status = replication_service.get_replication_status(job_id)
        history = replication_service.get_replication_history(job_id=job_id, limit=20)
        
        return templates.TemplateResponse(
            "zfs/replication/job_detail.jinja",
            {
                "request": request,
                "job": job,
                "status": status,
                "history": history,
                "page_title": f"Replication Job: {job['name']}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/replication"
            }
        )


@router.post("/jobs/{job_id}/enable", response_class=HTMLResponse)
async def enable_job(request: Request, job_id: str):
    """Enable a replication job"""
    try:
        replication_service.enable_job(job_id)
        return RedirectResponse(
            url=f"/zfs/replication/jobs/{job_id}/detail?message=Job enabled",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/replication/jobs/{job_id}/detail?error={str(e)}",
            status_code=303
        )


@router.post("/jobs/{job_id}/disable", response_class=HTMLResponse)
async def disable_job(request: Request, job_id: str):
    """Disable a replication job"""
    try:
        replication_service.disable_job(job_id)
        return RedirectResponse(
            url=f"/zfs/replication/jobs/{job_id}/detail?message=Job disabled",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/replication/jobs/{job_id}/detail?error={str(e)}",
            status_code=303
        )


@router.get("/jobs/{job_id}/delete/confirm", response_class=HTMLResponse)
async def delete_job_confirm(request: Request, job_id: str):
    """Display delete confirmation page"""
    try:
        job = replication_service.get_replication_job(job_id)
        return templates.TemplateResponse(
            "zfs/replication/job_delete_confirm.jinja",
            {
                "request": request,
                "job": job,
                "page_title": f"Delete Replication Job: {job['name']}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/replication"
            }
        )


@router.post("/jobs/{job_id}/delete", response_class=HTMLResponse)
async def delete_job(request: Request, job_id: str):
    """Delete a replication job"""
    try:
        replication_service.delete_replication_job(job_id)
        return RedirectResponse(
            url="/zfs/replication?message=Replication job deleted",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/replication/jobs/{job_id}/detail?error={str(e)}",
            status_code=303
        )


# Native ZFS Send/Receive Operations

@router.get("/send-receive/form", response_class=HTMLResponse)
async def send_receive_form(request: Request):
    """Display ZFS send/receive form"""
    try:
        datasets = dataset_service.list_datasets()
        snapshots = snapshot_service.list_snapshots()
        ssh_connections = ssh_service.list_connections()
        
        return templates.TemplateResponse(
            "zfs/replication/send_receive.jinja",
            {
                "request": request,
                "datasets": datasets,
                "snapshots": snapshots,
                "ssh_connections": ssh_connections,
                "compression_methods": [c.value for c in CompressionMethod],
                "page_title": "ZFS Send/Receive"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/send_receive.jinja",
            {
                "request": request,
                "datasets": [],
                "snapshots": [],
                "ssh_connections": [],
                "error": str(e),
                "page_title": "ZFS Send/Receive"
            }
        )


@router.post("/send-receive/execute", response_class=HTMLResponse)
async def send_receive_execute(
    request: Request,
    source: Annotated[str, Form()],
    target: Annotated[str, Form()],
    replication_type: Annotated[str, Form()],
    incremental: Annotated[bool, Form()] = True,
    recursive: Annotated[bool, Form()] = False,
    compression: Annotated[str, Form()] = "lz4",
    remote_host: Annotated[str, Form()] = "",
    remote_port: Annotated[int, Form()] = 22
):
    """Execute a one-time ZFS send/receive operation"""
    try:
        options = {}
        if remote_host:
            options['remote_host'] = remote_host
            options['remote_port'] = remote_port
        
        result = replication_service.execute_replication(
            source=source,
            target=target,
            replication_type=ReplicationType(replication_type),
            incremental=incremental,
            recursive=recursive,
            compression=CompressionMethod(compression),
            job_name=f"Manual: {source} → {target}",
            **options
        )
        
        return templates.TemplateResponse(
            "zfs/replication/send_receive_result.jinja",
            {
                "request": request,
                "result": result,
                "page_title": "Replication Result"
            }
        )
    except Exception as e:
        datasets = dataset_service.list_datasets()
        snapshots = snapshot_service.list_snapshots()
        return templates.TemplateResponse(
            "zfs/replication/send_receive.jinja",
            {
                "request": request,
                "datasets": datasets,
                "snapshots": snapshots,
                "compression_methods": [c.value for c in CompressionMethod],
                "error": str(e),
                "page_title": "ZFS Send/Receive"
            }
        )


@router.post("/estimate-size", response_class=HTMLResponse)
async def estimate_size(
    request: Request,
    source: Annotated[str, Form()],
    target: Annotated[str, Form()],
    incremental: Annotated[bool, Form()] = True
):
    """Estimate transfer size for replication"""
    try:
        estimate = replication_service.estimate_transfer_size(
            source=source,
            target=target,
            incremental=incremental
        )
        
        return templates.TemplateResponse(
            "zfs/replication/estimate_result.jinja",
            {
                "request": request,
                "estimate": estimate,
                "page_title": "Transfer Size Estimate"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/replication/send-receive/form"
            }
        )


# Syncoid Operations

@router.get("/syncoid", response_class=HTMLResponse)
async def syncoid_index(request: Request):
    """Display syncoid operations dashboard"""
    try:
        syncoid_status = syncoid_service.check_syncoid_status()
        datasets = dataset_service.list_datasets()
        ssh_connections = ssh_service.list_connections()
        
        # Detect OS
        system = platform.system()
        
        return templates.TemplateResponse(
            "zfs/replication/syncoid.jinja",
            {
                "request": request,
                "syncoid_status": syncoid_status,
                "datasets": datasets,
                "ssh_connections": ssh_connections,
                "system": system,
                "page_title": "Syncoid Replication"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/syncoid.jinja",
            {
                "request": request,
                "syncoid_status": {'installed': False},
                "datasets": [],
                "ssh_connections": [],
                "system": platform.system(),
                "error": str(e),
                "page_title": "Syncoid Replication"
            }
        )


@router.post("/syncoid/execute", response_class=HTMLResponse)
async def syncoid_execute(
    request: Request,
    source: Annotated[str, Form()],
    target: Annotated[str, Form()],
    recursive: Annotated[bool, Form()] = False,
    no_sync_snap: Annotated[bool, Form()] = False,
    compress: Annotated[str, Form()] = "",
    source_bwlimit: Annotated[str, Form()] = "",
    target_bwlimit: Annotated[str, Form()] = "",
    skip_parent: Annotated[bool, Form()] = False,
    create_bookmark: Annotated[bool, Form()] = False,
    force_delete: Annotated[bool, Form()] = False,
    source_host: Annotated[str, Form()] = "",
    target_host: Annotated[str, Form()] = "",
    ssh_port: Annotated[int, Form()] = 22,
    dry_run: Annotated[bool, Form()] = False
):
    """Execute syncoid replication"""
    try:
        result = syncoid_service.execute_replication(
            source=source,
            target=target,
            recursive=recursive,
            no_sync_snap=no_sync_snap,
            compress=compress if compress else None,
            source_bwlimit=source_bwlimit if source_bwlimit else None,
            target_bwlimit=target_bwlimit if target_bwlimit else None,
            skip_parent=skip_parent,
            create_bookmark=create_bookmark,
            force_delete=force_delete,
            source_host=source_host if source_host else None,
            target_host=target_host if target_host else None,
            ssh_port=ssh_port if ssh_port != 22 else None,
            dry_run=dry_run
        )
        
        return templates.TemplateResponse(
            "zfs/replication/syncoid_result.jinja",
            {
                "request": request,
                "result": result,
                "page_title": "Syncoid Result"
            }
        )
    except Exception as e:
        syncoid_status = syncoid_service.check_syncoid_status()
        datasets = dataset_service.list_datasets()
        return templates.TemplateResponse(
            "zfs/replication/syncoid.jinja",
            {
                "request": request,
                "syncoid_status": syncoid_status,
                "datasets": datasets,
                "error": str(e),
                "page_title": "Syncoid Replication"
            }
        )


@router.post("/syncoid/test-connection", response_class=HTMLResponse)
async def syncoid_test_connection(
    request: Request,
    remote_host: Annotated[str, Form()],
    remote_port: Annotated[int, Form()] = 22,
    dataset: Annotated[str, Form()] = ""
):
    """Test SSH connection to remote host"""
    try:
        result = syncoid_service.test_connection(
            remote_host=remote_host,
            remote_port=remote_port,
            dataset=dataset if dataset else None
        )
        
        if result['status'] == 'success':
            message = f"Connection to {remote_host} successful"
        else:
            message = f"Connection failed: {result['message']}"
        
        return RedirectResponse(
            url=f"/zfs/replication/syncoid?message={message}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/replication/syncoid?error={str(e)}",
            status_code=303
        )


@router.post("/syncoid/common-snapshots", response_class=HTMLResponse)
async def syncoid_common_snapshots(
    request: Request,
    source: Annotated[str, Form()],
    target: Annotated[str, Form()],
    source_host: Annotated[str, Form()] = "",
    target_host: Annotated[str, Form()] = ""
):
    """Get common snapshots between source and target"""
    try:
        result = syncoid_service.get_common_snapshots(
            source=source,
            target=target,
            source_host=source_host if source_host else None,
            target_host=target_host if target_host else None
        )
        
        return templates.TemplateResponse(
            "zfs/replication/common_snapshots.jinja",
            {
                "request": request,
                "result": result,
                "source": source,
                "target": target,
                "page_title": "Common Snapshots"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/replication/syncoid"
            }
        )


@router.post("/api/test-remote-connection")
async def test_remote_connection(data: Dict = Body(...)):
    """API endpoint to test remote SSH connection and fetch datasets using SSH connection ID"""
    try:
        import subprocess
        
        ssh_connection_id = data.get('ssh_connection_id')
        
        if not ssh_connection_id:
            return JSONResponse({
                "success": False,
                "error": "SSH connection is required"
            })
        
        # Get the SSH connection
        connection = ssh_service.get_connection(ssh_connection_id)
        if not connection:
            return JSONResponse({
                "success": False,
                "error": "SSH connection not found"
            })
        
        # Get list of datasets from remote using key-based authentication
        try:
            # Build the SSH command using the stored key
            ssh_cmd = [
                'ssh',
                '-i', connection['private_key_path'],
                '-p', str(connection['port']),
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'BatchMode=yes',
                f"{connection['username']}@{connection['host']}",
                'zfs', 'list', '-H', '-o', 'name'
            ]
            
            process = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
            
            if process.returncode == 0:
                datasets = [line.strip() for line in process.stdout.strip().split('\n') if line.strip()]
                
                # Mark connection as used for replication
                ssh_service.mark_connection_used(ssh_connection_id, 'replication')
                
                return JSONResponse({
                    "success": True,
                    "datasets": datasets,
                    "message": f"Connected to {connection['name']} successfully",
                    "connection": {
                        "id": connection['id'],
                        "name": connection['name'],
                        "host": connection['host'],
                        "username": connection['username'],
                        "port": connection['port']
                    }
                })
            else:
                error_msg = process.stderr if process.stderr else "Connection failed"
                return JSONResponse({
                    "success": False,
                    "error": f"Failed to connect: {error_msg}"
                })
        except subprocess.TimeoutExpired:
            return JSONResponse({
                "success": False,
                "error": "Connection timeout (30 seconds)"
            })
        except Exception as e:
            return JSONResponse({
                "success": False,
                "error": f"Failed to test connection: {str(e)}"
            })
            
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": str(e)
        })


@router.get("/api/ssh-connections")
async def get_ssh_connections():
    """API endpoint to get list of configured SSH connections"""
    try:
        connections = ssh_service.list_connections()
        return JSONResponse({
            "success": True,
            "connections": connections
        })
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": str(e)
        })


@router.post("/api/get-remote-datasets")
async def get_remote_datasets(data: Dict = Body(...)):
    """API endpoint to get datasets from a remote system via SSH connection"""
    try:
        import subprocess
        
        ssh_connection_id = data.get('ssh_connection_id')
        
        if not ssh_connection_id:
            # Return local datasets
            local_datasets = dataset_service.list_datasets()
            return JSONResponse({
                "success": True,
                "datasets": [ds['name'] for ds in local_datasets],
                "message": "Local datasets loaded",
                "is_local": True
            })
        
        # Get the SSH connection
        connection = ssh_service.get_connection(ssh_connection_id)
        if not connection:
            return JSONResponse({
                "success": False,
                "error": "SSH connection not found"
            })
        
        # Get list of datasets from remote using key-based authentication
        try:
            ssh_cmd = [
                'ssh',
                '-i', connection['private_key_path'],
                '-p', str(connection['port']),
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=10',
                f"{connection['username']}@{connection['host']}",
                'zfs', 'list', '-H', '-o', 'name'
            ]
            
            process = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
            
            if process.returncode == 0:
                datasets = [line.strip() for line in process.stdout.strip().split('\n') if line.strip()]
                
                return JSONResponse({
                    "success": True,
                    "datasets": datasets,
                    "message": f"Loaded {len(datasets)} datasets from {connection['name']}",
                    "is_local": False,
                    "connection_name": connection['name']
                })
            else:
                error_msg = process.stderr if process.stderr else "Connection failed"
                return JSONResponse({
                    "success": False,
                    "error": f"Failed to get datasets: {error_msg}"
                })
        except subprocess.TimeoutExpired:
            return JSONResponse({
                "success": False,
                "error": "Connection timeout (30 seconds)"
            })
        except Exception as e:
            return JSONResponse({
                "success": False,
                "error": f"Failed to fetch datasets: {str(e)}"
            })
            
    except Exception as e:
        return JSONResponse({
            "success": False,
            "error": str(e)
        })


@router.get("/history", response_class=HTMLResponse)
async def replication_history(request: Request, limit: int = 50, offset: int = 0):
    """Display replication execution history"""
    try:
        history = replication_service.get_replication_history(limit=limit, offset=offset)
        active_executions = replication_service.get_active_executions()
        
        return templates.TemplateResponse(
            "zfs/replication/history.jinja",
            {
                "request": request,
                "history": history,
                "active_executions": active_executions,
                "limit": limit,
                "offset": offset,
                "page_title": "Replication History"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/history.jinja",
            {
                "request": request,
                "history": [],
                "active_executions": [],
                "error": str(e),
                "page_title": "Replication History"
            }
        )


@router.get("/history/{execution_id}", response_class=HTMLResponse)
async def execution_detail(request: Request, execution_id: int):
    """Display detailed execution record with progress updates"""
    try:
        execution = replication_service.get_execution_detail(execution_id)
        
        if not execution:
            return templates.TemplateResponse(
                "partials/error.jinja",
                {
                    "request": request,
                    "error": f"Execution {execution_id} not found",
                    "back_url": "/zfs/replication/history"
                }
            )
        
        return templates.TemplateResponse(
            "zfs/replication/execution_detail.jinja",
            {
                "request": request,
                "execution": execution,
                "page_title": f"Execution #{execution_id}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "partials/error.jinja",
            {
                "request": request,
                "error": str(e),
                "back_url": "/zfs/replication/history"
            }
        )


@router.get("/api/progress-stream")
async def progress_stream(request: Request):
    """Server-Sent Events endpoint for real-time progress monitoring"""
    from fastapi.responses import StreamingResponse
    import asyncio
    
    async def event_generator():
        """Generate SSE events for active replication progress"""
        try:
            while True:
                # Check if client disconnected
                if await request.is_disconnected():
                    break
                
                # Get active executions
                active = replication_service.get_active_executions()
                
                if active:
                    # Send progress updates for each active execution
                    for execution in active:
                        # Get latest progress details
                        detail = replication_service.get_execution_detail(execution['id'])
                        if detail and detail.get('progress_updates'):
                            latest_progress = detail['progress_updates'][-1]
                            
                            data = {
                                'execution_id': execution['id'],
                                'job_name': execution['job_name'],
                                'percentage': latest_progress.get('percentage_complete', 0),
                                'bytes_transferred': latest_progress.get('bytes_transferred', 0),
                                'transfer_rate': latest_progress.get('transfer_rate', 'N/A'),
                                'eta': latest_progress.get('estimated_time_remaining', 'N/A'),
                                'status': latest_progress.get('status_message', '')
                            }
                            
                            yield f"data: {json.dumps(data)}\n\n"
                else:
                    # Send keepalive
                    yield f"data: {json.dumps({'keepalive': True})}\n\n"
                
                # Wait before next update (update every 2 seconds)
                await asyncio.sleep(2)
                
        except Exception as e:
            # Send error event
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"  # Disable nginx buffering
        }
    )


@router.get("/notifications/settings", response_class=HTMLResponse)
async def notification_settings(request: Request):
    """Display email notification settings"""
    try:
        email_service = replication_service.email
        is_configured = email_service.is_configured()
        
        return templates.TemplateResponse(
            "zfs/replication/notification_settings.jinja",
            {
                "request": request,
                "is_configured": is_configured,
                "smtp_enabled": email_service.smtp_enabled,
                "smtp_host": email_service.smtp_host,
                "smtp_port": email_service.smtp_port,
                "smtp_from_address": email_service.smtp_from_address,
                "recipients": email_service.notification_recipients,
                "page_title": "Notification Settings"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/replication/notification_settings.jinja",
            {
                "request": request,
                "is_configured": False,
                "error": str(e),
                "page_title": "Notification Settings"
            }
        )


@router.post("/notifications/test", response_class=HTMLResponse)
async def test_notifications(request: Request):
    """Test email notification configuration"""
    try:
        email_service = replication_service.email
        result = email_service.test_configuration()
        
        if result['status'] == 'sent':
            message = "Test email sent successfully!"
        else:
            message = f"Failed to send test email: {result['message']}"
        
        return RedirectResponse(
            url=f"/zfs/replication/notifications/settings?message={message}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/replication/notifications/settings?error={str(e)}",
            status_code=303
        )
