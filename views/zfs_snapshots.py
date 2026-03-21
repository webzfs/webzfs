"""
ZFS Snapshot Management Views
Provides web interface for ZFS snapshot operations
"""
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse
from typing import Annotated, Optional
from config.templates import templates
from services.zfs_snapshot import ZFSSnapshotService
from services.zfs_dataset import ZFSDatasetService
from services.sanoid import SanoidService
from services.audit_logger import audit_logger
from auth.dependencies import get_current_user


router = APIRouter(prefix="/zfs/snapshots", tags=["zfs-snapshots"], dependencies=[Depends(get_current_user)])
snapshot_service = ZFSSnapshotService()
dataset_service = ZFSDatasetService()
sanoid_service = SanoidService()


@router.get("/", response_class=HTMLResponse)
async def snapshots_index(
    request: Request,
    dataset: Optional[str] = None
):
    """Display all snapshots and bookmarks"""
    try:
        snapshots = snapshot_service.list_snapshots(dataset=dataset)

        # Load bookmarks and build a set for quick lookup
        try:
            bookmarks = snapshot_service.list_bookmarks(dataset=dataset)
        except Exception:
            bookmarks = []

        # Build a set of bookmark names keyed by dataset#bookmark for display
        bookmark_set = set()
        for bm in bookmarks:
            bookmark_set.add(f"{bm['dataset']}#{bm['bookmark']}")

        return templates.TemplateResponse(
            "zfs/snapshots/index.jinja",
            {
                "request": request,
                "snapshots": snapshots,
                "bookmarks": bookmarks,
                "bookmark_set": bookmark_set,
                "selected_dataset": dataset,
                "page_title": "ZFS Snapshots"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/snapshots/index.jinja",
            {
                "request": request,
                "snapshots": [],
                "bookmarks": [],
                "bookmark_set": set(),
                "error": str(e),
                "page_title": "ZFS Snapshots"
            }
        )


@router.get("/create/form", response_class=HTMLResponse)
async def create_snapshot_form(
    request: Request,
    dataset: Optional[str] = None
):
    """Display snapshot creation form"""
    try:
        # Get all available datasets
        datasets = dataset_service.list_datasets()
        
        # Generate default timestamp for snapshot name
        from datetime import datetime
        now = datetime.now()
        default_snapshot_name = now.strftime("manual-%Y-%m-%d_%H-%M")
        
        return templates.TemplateResponse(
            "zfs/snapshots/create.jinja",
            {
                "request": request,
                "dataset": dataset,
                "datasets": datasets,
                "default_snapshot_name": default_snapshot_name,
                "page_title": "Create Snapshot"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/snapshots/create.jinja",
            {
                "request": request,
                "dataset": dataset,
                "datasets": [],
                "error": f"Failed to load datasets: {str(e)}",
                "page_title": "Create Snapshot"
            }
        )


@router.post("/validate-name", response_class=HTMLResponse)
async def validate_snapshot_name(
    request: Request,
    snapshot_name: Annotated[str, Form()],
    dataset_name: Annotated[str, Form()] = ""
):
    """HTMX endpoint to validate snapshot name"""
    import re
    
    # Check if empty
    if not snapshot_name or not snapshot_name.strip():
        return HTMLResponse(
            content='<span class="text-danger-400 text-xs mt-1">Snapshot name is required</span>',
            status_code=200
        )
    
    # Check valid characters (alphanumeric, underscore, dash, period)
    if not re.match(r'^[a-zA-Z0-9_.-]+$', snapshot_name):
        return HTMLResponse(
            content='<span class="text-danger-400 text-xs mt-1">Invalid characters. Use only letters, numbers, underscore, dash, and period.</span>',
            status_code=200
        )
    
    # Check if snapshot already exists (if dataset provided)
    if dataset_name:
        try:
            full_name = f"{dataset_name}@{snapshot_name}"
            snapshots = snapshot_service.list_snapshots(dataset=dataset_name)
            if any(s['name'] == full_name for s in snapshots):
                return HTMLResponse(
                    content='<span class="text-warning-400 text-xs mt-1">⚠️ Snapshot already exists</span>',
                    status_code=200
                )
        except:
            pass
    
    # Valid
    return HTMLResponse(
        content='<span class="text-success-400 text-xs mt-1">✓ Valid snapshot name</span>',
        status_code=200
    )


@router.post("/create", response_class=HTMLResponse)
async def create_snapshot(
    request: Request,
    dataset_name: Annotated[str, Form()],
    snapshot_name: Annotated[str, Form()],
    recursive: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Create a new snapshot"""
    try:
        full_name = snapshot_service.create_snapshot(
            dataset_name,
            snapshot_name,
            recursive=recursive
        )
        
        # Log successful snapshot creation
        audit_logger.log_snapshot_create(user=current_user, snapshot_name=full_name, recursive=recursive)
        
        return RedirectResponse(
            url=f"/zfs/snapshots?dataset={dataset_name}&message=Snapshot {full_name} created successfully",
            status_code=303
        )
    except Exception as e:
        # Log failed snapshot creation
        audit_logger.log_snapshot_create(
            user=current_user, snapshot_name=f"{dataset_name}@{snapshot_name}",
            recursive=recursive, success=False, error=str(e)
        )
        return templates.TemplateResponse(
            "zfs/snapshots/create.jinja",
            {
                "request": request,
                "dataset": dataset_name,
                "snapshot_name": snapshot_name,
                "recursive": recursive,
                "error": str(e),
                "page_title": "Create Snapshot"
            }
        )


# Bookmark Routes (must be before catch-all {snapshot_path:path} routes)

@router.post("/bookmarks/destroy", response_class=HTMLResponse)
async def destroy_bookmark(
    request: Request,
    bookmark_name: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Destroy a ZFS bookmark"""
    try:
        snapshot_service.destroy_bookmark(bookmark_name)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="bookmark_destroy",
            bookmark=bookmark_name
        )
        return RedirectResponse(
            url=f"/zfs/snapshots?message=Bookmark {bookmark_name} destroyed",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots?error={str(e)}",
            status_code=303
        )


@router.post("/{snapshot_path:path}/bookmark", response_class=HTMLResponse)
async def create_bookmark(
    request: Request,
    snapshot_path: str,
    current_user: str = Depends(get_current_user)
):
    """Create a bookmark from a snapshot"""
    try:
        full_bookmark = snapshot_service.create_bookmark(snapshot_path)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="bookmark_create",
            snapshot=snapshot_path,
            bookmark=full_bookmark
        )
        dataset_part = snapshot_path.split('@')[0]
        return RedirectResponse(
            url=f"/zfs/snapshots?dataset={dataset_part}&message=Bookmark {full_bookmark} created",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots?error={str(e)}",
            status_code=303
        )


# Snapshot Detail and Action Routes

@router.get("/{snapshot_path:path}/detail", response_class=HTMLResponse)
async def snapshot_detail(
    request: Request,
    snapshot_path: str
):
    """Display snapshot detail page skeleton - data loads async via HTMX"""
    # Parse the snapshot name from the path for display
    snap_name = snapshot_path.rsplit('@', 1)[1] if '@' in snapshot_path else snapshot_path
    dataset_name = snapshot_path.rsplit('@', 1)[0] if '@' in snapshot_path else ""

    return templates.TemplateResponse(
        "zfs/snapshots/detail.jinja",
        {
            "request": request,
            "snapshot_path": snapshot_path,
            "snap_name": snap_name,
            "dataset_name": dataset_name,
            "page_title": f"Snapshot: {snapshot_path}"
        }
    )


@router.get("/{snapshot_path:path}/htmx/data", response_class=HTMLResponse)
async def snapshot_detail_data(
    request: Request,
    snapshot_path: str
):
    """HTMX fragment: fetch all snapshot detail data"""
    try:
        snapshot = snapshot_service.get_snapshot(snapshot_path)
        space = snapshot_service.get_snapshot_space(snapshot_path)
        holds = snapshot_service.get_holds(snapshot_path)

        # Check if a bookmark exists for this snapshot
        has_bookmark = False
        bookmark_name = ""
        try:
            dataset_part, snap_part = snapshot_path.rsplit('@', 1)
            bookmarks = snapshot_service.list_bookmarks(dataset=dataset_part)
            for bm in bookmarks:
                if bm['bookmark'] == snap_part and bm['dataset'] == dataset_part:
                    has_bookmark = True
                    bookmark_name = bm['name']
                    break
        except Exception:
            pass

        return templates.TemplateResponse(
            "zfs/snapshots/detail_data.jinja",
            {
                "request": request,
                "snapshot": snapshot,
                "snapshot_path": snapshot_path,
                "space": space,
                "holds": holds,
                "has_bookmark": has_bookmark,
                "bookmark_name": bookmark_name,
            }
        )
    except Exception as e:
        return HTMLResponse(
            content=f'<div class="card"><div class="p-6"><div class="text-danger-400 font-semibold">Error loading snapshot data</div><p class="text-text-secondary mt-2">{str(e)}</p></div></div>',
            status_code=200
        )


@router.get("/{snapshot_path:path}/destroy/confirm", response_class=HTMLResponse)
async def destroy_snapshot_confirm(
    request: Request,
    snapshot_path: str
):
    """Display destroy confirmation page"""
    return templates.TemplateResponse(
        "zfs/snapshots/destroy_confirm.jinja",
        {
            "request": request,
            "snapshot_name": snapshot_path,
            "page_title": f"Destroy Snapshot: {snapshot_path}"
        }
    )


@router.post("/{snapshot_path:path}/destroy", response_class=HTMLResponse)
async def destroy_snapshot(
    request: Request,
    snapshot_path: str,
    defer: Annotated[bool, Form()] = False,
    confirm: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Destroy a snapshot (DESTRUCTIVE!)"""
    try:
        # Require user to type snapshot name to confirm
        if confirm != snapshot_path:
            return templates.TemplateResponse(
                "zfs/snapshots/destroy_confirm.jinja",
                {
                    "request": request,
                    "snapshot_name": snapshot_path,
                    "error": "Snapshot name confirmation does not match. Please type the exact snapshot name.",
                    "page_title": f"Destroy Snapshot: {snapshot_path}"
                }
            )
        
        snapshot_service.destroy_snapshot(snapshot_path, defer=defer)
        audit_logger.log_snapshot_destroy(user=current_user, snapshot_name=snapshot_path)
        return RedirectResponse(
            url="/zfs/snapshots?message=Snapshot destroyed successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_snapshot_destroy(user=current_user, snapshot_name=snapshot_path, success=False, error=str(e))
        return templates.TemplateResponse(
            "zfs/snapshots/destroy_confirm.jinja",
            {
                "request": request,
                "snapshot_name": snapshot_path,
                "error": str(e),
                "page_title": f"Destroy Snapshot: {snapshot_path}"
            }
        )


@router.get("/{snapshot_path:path}/rollback/confirm", response_class=HTMLResponse)
async def rollback_snapshot_confirm(
    request: Request,
    snapshot_path: str
):
    """Display rollback confirmation page"""
    return templates.TemplateResponse(
        "zfs/snapshots/rollback_confirm.jinja",
        {
            "request": request,
            "snapshot_name": snapshot_path,
            "page_title": f"Rollback to Snapshot: {snapshot_path}"
        }
    )


@router.post("/{snapshot_path:path}/rollback", response_class=HTMLResponse)
async def rollback_snapshot(
    request: Request,
    snapshot_path: str,
    force: Annotated[bool, Form()] = False,
    confirm: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Rollback to a snapshot (DESTRUCTIVE!)"""
    try:
        # Require user to type snapshot name to confirm
        if confirm != snapshot_path:
            return templates.TemplateResponse(
                "zfs/snapshots/rollback_confirm.jinja",
                {
                    "request": request,
                    "snapshot_name": snapshot_path,
                    "error": "Snapshot name confirmation does not match. Please type the exact snapshot name.",
                    "page_title": f"Rollback to Snapshot: {snapshot_path}"
                }
            )
        
        snapshot_service.rollback_snapshot(snapshot_path, force=force)
        audit_logger.log_snapshot_rollback(user=current_user, snapshot_name=snapshot_path, force=force)
        
        return RedirectResponse(
            url=f"/zfs/snapshots?message=Rollback to {snapshot_path} successful",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_snapshot_rollback(user=current_user, snapshot_name=snapshot_path, force=force, success=False, error=str(e))
        return templates.TemplateResponse(
            "zfs/snapshots/rollback_confirm.jinja",
            {
                "request": request,
                "snapshot_name": snapshot_path,
                "error": str(e),
                "page_title": f"Rollback to Snapshot: {snapshot_path}"
            }
        )


@router.get("/{snapshot_path:path}/clone/form", response_class=HTMLResponse)
async def clone_snapshot_form(
    request: Request,
    snapshot_path: str
):
    """Display clone form"""
    return templates.TemplateResponse(
        "zfs/snapshots/clone.jinja",
        {
            "request": request,
            "snapshot_name": snapshot_path,
            "page_title": f"Clone Snapshot: {snapshot_path}"
        }
    )


@router.post("/{snapshot_path:path}/clone", response_class=HTMLResponse)
async def clone_snapshot(
    request: Request,
    snapshot_path: str,
    target_dataset: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Clone a snapshot to create a new dataset"""
    try:
        snapshot_service.clone_snapshot(snapshot_path, target_dataset)
        audit_logger.log_snapshot_clone(user=current_user, snapshot_name=snapshot_path, target_dataset=target_dataset)
        
        return RedirectResponse(
            url=f"/zfs/datasets/{target_dataset}?message=Clone created successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_snapshot_clone(user=current_user, snapshot_name=snapshot_path, target_dataset=target_dataset, success=False, error=str(e))
        return templates.TemplateResponse(
            "zfs/snapshots/clone.jinja",
            {
                "request": request,
                "snapshot_name": snapshot_path,
                "target_dataset": target_dataset,
                "error": str(e),
                "page_title": f"Clone Snapshot: {snapshot_path}"
            }
        )


@router.get("/{snapshot_path:path}/rename/form", response_class=HTMLResponse)
async def rename_snapshot_form(
    request: Request,
    snapshot_path: str
):
    """Display rename form"""
    # Extract current snapshot name (without dataset prefix)
    _, snap_name = snapshot_path.rsplit('@', 1)
    
    return templates.TemplateResponse(
        "zfs/snapshots/rename.jinja",
        {
            "request": request,
            "snapshot_name": snapshot_path,
            "current_name": snap_name,
            "page_title": f"Rename Snapshot: {snapshot_path}"
        }
    )


@router.post("/{snapshot_path:path}/rename", response_class=HTMLResponse)
async def rename_snapshot(
    request: Request,
    snapshot_path: str,
    new_name: Annotated[str, Form()],
    recursive: Annotated[bool, Form()] = False
):
    """Rename a snapshot"""
    try:
        snapshot_service.rename_snapshot(snapshot_path, new_name, recursive=recursive)
        
        # Build new snapshot path
        dataset_name = snapshot_path.rsplit('@', 1)[0]
        new_snapshot_path = f"{dataset_name}@{new_name}"
        
        return RedirectResponse(
            url=f"/zfs/snapshots?message=Snapshot renamed to {new_snapshot_path}",
            status_code=303
        )
    except Exception as e:
        _, snap_name = snapshot_path.rsplit('@', 1)
        return templates.TemplateResponse(
            "zfs/snapshots/rename.jinja",
            {
                "request": request,
                "snapshot_name": snapshot_path,
                "current_name": snap_name,
                "new_name": new_name,
                "error": str(e),
                "page_title": f"Rename Snapshot: {snapshot_path}"
            }
        )


@router.get("/diff/form", response_class=HTMLResponse)
async def diff_snapshots_form(
    request: Request,
    snapshot1: Optional[str] = None
):
    """Display diff form"""
    try:
        # Get all available snapshots
        snapshots = snapshot_service.list_snapshots()
        
        return templates.TemplateResponse(
            "zfs/snapshots/diff.jinja",
            {
                "request": request,
                "snapshot1": snapshot1,
                "snapshots": snapshots,
                "page_title": "Compare Snapshots"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/snapshots/diff.jinja",
            {
                "request": request,
                "snapshot1": snapshot1,
                "snapshots": [],
                "error": f"Failed to load snapshots: {str(e)}",
                "page_title": "Compare Snapshots"
            }
        )


@router.get("/diff/download", response_class=PlainTextResponse)
async def download_diff(
    snapshot1: str,
    snapshot2: str
):
    """Download diff output as a text file"""
    from fastapi.responses import PlainTextResponse
    from datetime import datetime
    
    try:
        # Re-run the diff to get the output
        diff_output = snapshot_service.diff_snapshots(
            snapshot1,
            snapshot2 if snapshot2 else None
        )
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        snap1_short = snapshot1.split('@')[1] if '@' in snapshot1 else snapshot1
        snap2_short = snapshot2.split('@')[1] if '@' in snapshot2 else snapshot2
        filename = f"diff_{snap1_short}_to_{snap2_short}_{timestamp}.txt"
        
        # Create file content with header
        content = f"""ZFS Snapshot Comparison
{"=" * 80}

From: {snapshot1}
To:   {snapshot2}
Date: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

Diff Output:
{"-" * 80}

{diff_output}

{"=" * 80}
End of Report
{"=" * 80}
"""
        
        return PlainTextResponse(
            content=content,
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        return PlainTextResponse(
            content=f"Error generating download: {str(e)}",
            status_code=500
        )


@router.post("/diff", response_class=HTMLResponse)
async def diff_snapshots(
    request: Request,
    snapshot1: Annotated[str, Form()],
    snapshot2: Annotated[str, Form()] = ""
):
    """Show diff result page skeleton - actual diff loads async via HTMX"""
    # Validate that snapshot1 is provided
    if not snapshot1:
        try:
            snapshots = snapshot_service.list_snapshots()
        except:
            snapshots = []

        return templates.TemplateResponse(
            "zfs/snapshots/diff.jinja",
            {
                "request": request,
                "snapshot1": "",
                "snapshot2": snapshot2,
                "snapshots": snapshots,
                "error": "Please select the first snapshot to compare",
                "page_title": "Compare Snapshots"
            }
        )

    # Validate that snapshots are from the same dataset (zfs diff requirement)
    if snapshot2:
        try:
            dataset1 = snapshot1.split('@')[0]
            dataset2 = snapshot2.split('@')[0]
            if dataset1 != dataset2:
                try:
                    snapshots = snapshot_service.list_snapshots()
                except:
                    snapshots = []

                return templates.TemplateResponse(
                    "zfs/snapshots/diff.jinja",
                    {
                        "request": request,
                        "snapshot1": snapshot1,
                        "snapshot2": snapshot2,
                        "snapshots": snapshots,
                        "error": f"Cannot compare snapshots from different datasets: '{dataset1}' vs '{dataset2}'. Both snapshots must be from the same dataset.",
                        "page_title": "Compare Snapshots"
                    }
                )
        except:
            pass

    # Render the result page skeleton immediately - diff computes async via HTMX
    return templates.TemplateResponse(
        "zfs/snapshots/diff_result.jinja",
        {
            "request": request,
            "snapshot1": snapshot1,
            "snapshot2": snapshot2 if snapshot2 else "current state",
            "snapshot2_raw": snapshot2,
            "page_title": "Snapshot Diff"
        }
    )


@router.get("/diff/htmx/compute", response_class=HTMLResponse)
async def diff_snapshots_htmx(
    request: Request,
    snapshot1: str,
    snapshot2: str = ""
):
    """HTMX fragment: compute and return the actual diff output"""
    try:
        diff_output = snapshot_service.diff_snapshots(
            snapshot1,
            snapshot2 if snapshot2 else None
        )

        if not diff_output or not diff_output.strip():
            diff_output = ""

        # Calculate summary counts
        additions = 0
        modifications = 0
        deletions = 0
        renames = 0

        if diff_output:
            for line in diff_output.split('\n'):
                line = line.strip()
                if line.startswith('+') or line.startswith('A'):
                    additions += 1
                elif line.startswith('M'):
                    modifications += 1
                elif line.startswith('-'):
                    deletions += 1
                elif line.startswith('R'):
                    renames += 1

        return templates.TemplateResponse(
            "zfs/snapshots/diff_result_data.jinja",
            {
                "request": request,
                "snapshot1": snapshot1,
                "snapshot2": snapshot2 if snapshot2 else "current state",
                "diff_output": diff_output,
                "additions": additions,
                "modifications": modifications,
                "deletions": deletions,
                "renames": renames,
            }
        )
    except Exception as e:
        return HTMLResponse(
            content=f'<div class="card"><div class="p-6"><div class="text-danger-400 font-semibold">Error computing diff</div><p class="text-text-secondary mt-2">{str(e)}</p></div></div>',
            status_code=200
        )


@router.post("/{snapshot_path:path}/hold", response_class=HTMLResponse)
async def hold_snapshot(
    request: Request,
    snapshot_path: str,
    tag: Annotated[str, Form()]
):
    """Place a hold on a snapshot"""
    try:
        snapshot_service.hold_snapshot(snapshot_path, tag)
        return RedirectResponse(
            url=f"/zfs/snapshots/{snapshot_path}/detail?message=Hold placed successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/{snapshot_path}/detail?error={str(e)}",
            status_code=303
        )


@router.post("/{snapshot_path:path}/release", response_class=HTMLResponse)
async def release_snapshot(
    request: Request,
    snapshot_path: str,
    tag: Annotated[str, Form()]
):
    """Release a hold on a snapshot"""
    try:
        snapshot_service.release_snapshot(snapshot_path, tag)
        return RedirectResponse(
            url=f"/zfs/snapshots/{snapshot_path}/detail?message=Hold released successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/{snapshot_path}/detail?error={str(e)}",
            status_code=303
        )


# Sanoid Integration Routes

@router.get("/sanoid", response_class=HTMLResponse)
async def sanoid_index(request: Request):
    """Display sanoid configuration and status"""
    try:
        status = sanoid_service.check_sanoid_status()
        
        if not status['installed']:
            return templates.TemplateResponse(
                "zfs/snapshots/sanoid.jinja",
                {
                    "request": request,
                    "status": status,
                    "datasets": {},
                    "templates": {},
                    "error": "Sanoid is not installed on this system",
                    "page_title": "Sanoid Scheduling"
                }
            )
        
        config = sanoid_service.get_config()
        
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid.jinja",
            {
                "request": request,
                "status": status,
                "datasets": config.get('datasets', {}),
                "templates": config.get('templates', {}),
                "page_title": "Sanoid Scheduling"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid.jinja",
            {
                "request": request,
                "status": {},
                "datasets": {},
                "templates": {},
                "error": str(e),
                "page_title": "Sanoid Scheduling"
            }
        )


@router.post("/sanoid/run", response_class=HTMLResponse)
async def run_sanoid(
    request: Request,
    take_snapshots: Annotated[bool, Form()] = False,
    prune_snapshots: Annotated[bool, Form()] = False
):
    """Manually run sanoid"""
    try:
        result = sanoid_service.run_sanoid(
            take_snapshots=take_snapshots,
            prune_snapshots=prune_snapshots
        )
        
        if result['success']:
            message = "Sanoid executed successfully"
            if take_snapshots:
                message += " - Snapshots taken"
            if prune_snapshots:
                message += " - Snapshots pruned"
        else:
            message = f"Sanoid execution failed: {result['stderr']}"
        
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message={message}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.get("/sanoid/validate", response_class=HTMLResponse)
async def validate_sanoid_config(request: Request):
    """Validate sanoid configuration"""
    try:
        validation = sanoid_service.validate_config()
        
        if validation['valid']:
            message = f"Configuration is valid - {validation['dataset_count']} datasets, {validation['template_count']} templates"
        else:
            message = f"Configuration has errors: {', '.join(validation['errors'])}"
        
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message={message}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.post("/sanoid/dataset/{dataset_name:path}/remove", response_class=HTMLResponse)
async def remove_sanoid_dataset(
    request: Request,
    dataset_name: str
):
    """Remove a dataset from sanoid configuration"""
    try:
        sanoid_service.remove_dataset(dataset_name)
        return RedirectResponse(
            url="/zfs/snapshots/sanoid?message=Dataset removed from Sanoid configuration",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


# Sanoid Dataset/Template Management Forms

@router.get("/sanoid/dataset/add", response_class=HTMLResponse)
async def add_sanoid_dataset_form(request: Request):
    """Display add dataset form"""
    try:
        datasets = dataset_service.list_datasets()
        config = sanoid_service.get_config()
        sanoid_templates = config.get('templates', {})
        
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid_dataset_add.jinja",
            {
                "request": request,
                "datasets": datasets,
                "templates": sanoid_templates,
                "page_title": "Add Dataset to Sanoid"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid_dataset_add.jinja",
            {
                "request": request,
                "datasets": [],
                "templates": {},
                "error": str(e),
                "page_title": "Add Dataset to Sanoid"
            }
        )


@router.post("/sanoid/dataset/add", response_class=HTMLResponse)
async def add_sanoid_dataset(
    request: Request,
    dataset_name: Annotated[str, Form()],
    template: Annotated[str, Form()],
    recursive: Annotated[str, Form()]
):
    """Add a dataset to sanoid configuration"""
    try:
        sanoid_service.add_dataset(
            dataset_name=dataset_name,
            template=template,
            recursive=recursive
        )
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message=Dataset {dataset_name} added to Sanoid configuration",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.get("/sanoid/template/create", response_class=HTMLResponse)
async def create_sanoid_template_form(request: Request):
    """Display create template form"""
    return templates.TemplateResponse(
        "zfs/snapshots/sanoid_template_create.jinja",
        {
            "request": request,
            "page_title": "Create Snapshot Policy Template"
        }
    )


@router.post("/sanoid/template/create", response_class=HTMLResponse)
async def create_sanoid_template(
    request: Request,
    template_name: Annotated[str, Form()],
    frequently: Annotated[int, Form()] = 0,
    hourly: Annotated[int, Form()] = 0,
    daily: Annotated[int, Form()] = 0,
    weekly: Annotated[int, Form()] = 0,
    monthly: Annotated[int, Form()] = 0,
    yearly: Annotated[int, Form()] = 0,
    autosnap: Annotated[str, Form()] = "yes",
    autoprune: Annotated[str, Form()] = "yes"
):
    """Create a new sanoid template"""
    try:
        settings = {
            'frequently': frequently,
            'hourly': hourly,
            'daily': daily,
            'weekly': weekly,
            'monthly': monthly,
            'yearly': yearly,
            'autosnap': autosnap,
            'autoprune': autoprune
        }
        
        sanoid_service.create_template(template_name, settings)
        
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message=Template '{template_name}' created successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.get("/sanoid/template/{template_name:path}/edit", response_class=HTMLResponse)
async def edit_sanoid_template_form(request: Request, template_name: str):
    """Display edit template form"""
    try:
        config = sanoid_service.get_config()
        sanoid_templates = config.get('templates', {})
        
        # Find the template
        template_data = sanoid_templates.get(template_name)
        if not template_data:
            return RedirectResponse(
                url=f"/zfs/snapshots/sanoid?error=Template {template_name} not found",
                status_code=303
            )
        
        # Extract template name without template_ prefix
        display_name = template_name.replace('template_', '')
        
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid_template_edit.jinja",
            {
                "request": request,
                "template_name": template_name,
                "display_name": display_name,
                "template_data": template_data,
                "page_title": f"Edit Template: {display_name}"
            }
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.post("/sanoid/template/{template_name:path}/edit", response_class=HTMLResponse)
async def edit_sanoid_template(
    request: Request,
    template_name: str,
    frequently: Annotated[int, Form()] = 0,
    hourly: Annotated[int, Form()] = 0,
    daily: Annotated[int, Form()] = 0,
    weekly: Annotated[int, Form()] = 0,
    monthly: Annotated[int, Form()] = 0,
    yearly: Annotated[int, Form()] = 0,
    autosnap: Annotated[str, Form()] = "yes",
    autoprune: Annotated[str, Form()] = "yes"
):
    """Update an existing sanoid template"""
    try:
        settings = {
            'frequently': frequently,
            'hourly': hourly,
            'daily': daily,
            'weekly': weekly,
            'monthly': monthly,
            'yearly': yearly,
            'autosnap': autosnap,
            'autoprune': autoprune
        }
        
        sanoid_service.update_template(template_name, settings)
        
        display_name = template_name.replace('template_', '')
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message=Template '{display_name}' updated successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.get("/sanoid/dataset/{dataset_name:path}/edit", response_class=HTMLResponse)
async def edit_sanoid_dataset_form(request: Request, dataset_name: str):
    """Display edit dataset form"""
    try:
        config = sanoid_service.get_config()
        datasets = config.get('datasets', {})
        sanoid_templates = config.get('templates', {})
        
        # Find the dataset
        dataset_data = datasets.get(dataset_name)
        if not dataset_data:
            return RedirectResponse(
                url=f"/zfs/snapshots/sanoid?error=Dataset {dataset_name} not found",
                status_code=303
            )
        
        return templates.TemplateResponse(
            "zfs/snapshots/sanoid_dataset_edit.jinja",
            {
                "request": request,
                "dataset_name": dataset_name,
                "dataset_data": dataset_data,
                "templates": sanoid_templates,
                "page_title": f"Edit Dataset: {dataset_name}"
            }
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.post("/sanoid/dataset/{dataset_name:path}/edit", response_class=HTMLResponse)
async def edit_sanoid_dataset(
    request: Request,
    dataset_name: str,
    template: Annotated[str, Form()],
    recursive: Annotated[str, Form()]
):
    """Update an existing sanoid dataset"""
    try:
        settings = {
            'use_template': template,
            'recursive': recursive
        }
        
        sanoid_service.update_dataset(dataset_name, settings)
        
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message=Dataset '{dataset_name}' updated successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )


@router.post("/sanoid/template/{template_name:path}/delete", response_class=HTMLResponse)
async def delete_sanoid_template(request: Request, template_name: str):
    """Delete a sanoid template"""
    try:
        sanoid_service.delete_template(template_name)
        display_name = template_name.replace('template_', '')
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?message=Policy '{display_name}' deleted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/snapshots/sanoid?error={str(e)}",
            status_code=303
        )
