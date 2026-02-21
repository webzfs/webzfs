"""
ZFS Dataset Management Views
Provides web interface for ZFS dataset operations
"""
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, RedirectResponse
from typing import Annotated, Optional
from config.templates import templates
from services.zfs_dataset import ZFSDatasetService
from services.audit_logger import audit_logger
from auth.dependencies import get_current_user


router = APIRouter(prefix="/zfs/datasets", tags=["zfs-datasets"], dependencies=[Depends(get_current_user)])
dataset_service = ZFSDatasetService()


@router.get("/", response_class=HTMLResponse)
async def datasets_index(
    request: Request,
    pool: Optional[str] = None
):
    """Display all datasets"""
    try:
        datasets = dataset_service.list_datasets(pool_name=pool)
        
        # Group datasets by pool
        pools_dict = {}
        for dataset in datasets:
            # Extract pool name (first part before /)
            pool_name = dataset['name'].split('/')[0]
            
            if pool_name not in pools_dict:
                pools_dict[pool_name] = []
            
            pools_dict[pool_name].append(dataset)
        
        # Build hierarchical structure for each pool
        for pool_name, pool_datasets in pools_dict.items():
            # Create a dict to track parent-child relationships
            dataset_dict = {}
            for ds in pool_datasets:
                ds['children'] = []
                ds['depth'] = ds['name'].count('/')
                ds['has_children'] = False
                dataset_dict[ds['name']] = ds
            
            # Link children to parents
            for ds in pool_datasets:
                parts = ds['name'].split('/')
                if len(parts) > 1:
                    # This is a child dataset, find its parent
                    parent_name = '/'.join(parts[:-1])
                    if parent_name in dataset_dict:
                        dataset_dict[parent_name]['children'].append(ds)
                        dataset_dict[parent_name]['has_children'] = True
        
        # Sort pools by name and convert to list of tuples
        pools = sorted(pools_dict.items())
        
        return templates.TemplateResponse(
            "zfs/datasets/index.jinja",
            {
                "request": request,
                "datasets": datasets,  # Keep original list for count
                "pools": pools,  # Grouped datasets by pool
                "selected_pool": pool,
                "page_title": "ZFS Datasets"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/datasets/index.jinja",
            {
                "request": request,
                "datasets": [],
                "pools": [],
                "error": str(e),
                "page_title": "ZFS Datasets"
            }
        )


@router.get("/create/form", response_class=HTMLResponse)
async def create_dataset_form(
    request: Request,
    pool: Optional[str] = None,
    parent: Optional[str] = None
):
    """Display dataset creation form"""
    pool_datasets = []
    
    # If a pool is specified, get all datasets for that pool (for parent dropdown)
    if pool:
        try:
            # Get ALL datasets then filter by pool prefix
            # This is needed because zfs list <pool> doesn't return children recursively
            all_datasets = dataset_service.list_datasets()
            # Filter to filesystems in this pool (volumes can't have child datasets)
            pool_datasets = [
                ds for ds in all_datasets 
                if ds['type'] == 'filesystem' and (
                    ds['name'] == pool or ds['name'].startswith(pool + '/')
                )
            ]
            # Sort by name for better UX
            pool_datasets.sort(key=lambda x: x['name'])
        except Exception:
            # If we can't get datasets, proceed with empty list
            pool_datasets = []
    
    return templates.TemplateResponse(
        "zfs/datasets/create.jinja",
        {
            "request": request,
            "pool": pool,
            "parent": parent,
            "pool_datasets": pool_datasets,
            "page_title": "Create Dataset"
        }
    )


@router.post("/create", response_class=HTMLResponse)
async def create_dataset(
    request: Request,
    dataset_name: Annotated[str, Form()],
    dataset_type: Annotated[str, Form()] = "filesystem",
    mountpoint: Annotated[str, Form()] = "",
    compression: Annotated[str, Form()] = "lz4",
    recordsize: Annotated[str, Form()] = "",
    atime: Annotated[str, Form()] = "off",
    volsize: Annotated[str, Form()] = "",
    encryption: Annotated[str, Form()] = "",
    passphrase: Annotated[str, Form()] = "",
    passphrase_confirm: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Create a new dataset"""
    try:
        properties = {}
        
        # Handle encryption
        if encryption:
            if not passphrase:
                raise Exception("Passphrase is required when encryption is enabled")
            if passphrase != passphrase_confirm:
                raise Exception("Passphrases do not match")
            
            # Set encryption properties
            properties['encryption'] = encryption
            properties['keyformat'] = 'passphrase'
            properties['keylocation'] = 'prompt'
        
        # Handle volume creation
        if dataset_type == "volume":
            if not volsize:
                raise Exception("Volume size (volsize) is required for volume creation")
            properties['volsize'] = volsize
            # Volumes don't use mountpoint/atime
        else:
            # Filesystem properties
            if mountpoint:
                properties['mountpoint'] = mountpoint
            if atime:
                properties['atime'] = atime
        
        # Common properties
        if compression:
            properties['compression'] = compression
        if recordsize:
            properties['recordsize'] = recordsize
        
        # Create dataset with encryption if specified
        if encryption and passphrase:
            dataset_service.create_dataset_with_encryption(
                dataset_name,
                passphrase,
                dataset_type=dataset_type,
                properties=properties
            )
        else:
            dataset_service.create_dataset(
                dataset_name,
                dataset_type=dataset_type,
                properties=properties
            )
        
        # Log successful dataset creation
        audit_logger.log_dataset_create(user=current_user, dataset_name=dataset_name)
        
        return RedirectResponse(
            url=f"/zfs/datasets?message=Dataset {dataset_name} created successfully",
            status_code=303
        )
    except Exception as e:
        # Log failed dataset creation
        audit_logger.log_dataset_create(user=current_user, dataset_name=dataset_name, success=False, error=str(e))
        return templates.TemplateResponse(
            "zfs/datasets/create.jinja",
            {
                "request": request,
                "error": str(e),
                "dataset_name": dataset_name,
                "dataset_type": dataset_type,
                "mountpoint": mountpoint,
                "compression": compression,
                "recordsize": recordsize,
                "atime": atime,
                "volsize": volsize,
                "encryption": encryption,
                "page_title": "Create Dataset"
            }
        )


@router.get("/{dataset_path:path}/properties", response_class=HTMLResponse)
async def dataset_properties(
    request: Request,
    dataset_path: str
):
    """Display dataset properties"""
    try:
        properties = dataset_service.get_properties(dataset_path)
        
        return templates.TemplateResponse(
            "zfs/datasets/properties.jinja",
            {
                "request": request,
                "dataset_name": dataset_path,
                "properties": properties,
                "page_title": f"Dataset Properties: {dataset_path}"
            }
        )
    except Exception as e:
        # Return full page with error for HTMX compatibility
        return templates.TemplateResponse(
            "zfs/datasets/properties.jinja",
            {
                "request": request,
                "dataset_name": dataset_path,
                "properties": None,
                "error": str(e),
                "page_title": f"Dataset Properties: {dataset_path}"
            }
        )


@router.post("/{dataset_path:path}/properties/set", response_class=HTMLResponse)
async def set_dataset_property(
    request: Request,
    dataset_path: str,
    property_name: Annotated[str, Form()],
    property_value: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Set a dataset property"""
    try:
        dataset_service.set_property(dataset_path, property_name, property_value)
        audit_logger.log_dataset_property_change(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, property_value=property_value
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?message=Property updated successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_dataset_property_change(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, property_value=property_value,
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?error={str(e)}",
            status_code=303
        )


@router.get("/{dataset_path:path}/properties/download")
async def download_dataset_properties(dataset_path: str):
    """Download dataset properties as text file"""
    try:
        from fastapi.responses import PlainTextResponse
        from datetime import datetime
        
        properties = dataset_service.get_properties(dataset_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Format output
        output_lines = []
        output_lines.append("=" * 80)
        output_lines.append(f"ZFS Dataset Properties: {dataset_path}")
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
        
        # Create safe filename from dataset path (replace / with _)
        safe_name = dataset_path.replace('/', '_')
        
        return PlainTextResponse(
            content=content,
            headers={
                "Content-Disposition": f'attachment; filename="dataset_{safe_name}_properties_{timestamp}.txt"'
            }
        )
    except Exception as e:
        from fastapi.responses import PlainTextResponse
        return PlainTextResponse(
            content=f"Error generating properties file: {str(e)}",
            status_code=500
        )


@router.post("/{dataset_path:path}/mount", response_class=HTMLResponse)
async def mount_dataset(
    request: Request,
    dataset_path: str
):
    """Mount a dataset"""
    try:
        dataset_service.mount_dataset(dataset_path)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Dataset mounted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/unmount", response_class=HTMLResponse)
async def unmount_dataset(
    request: Request,
    dataset_path: str,
    force: Annotated[bool, Form()] = False
):
    """Unmount a dataset"""
    try:
        dataset_service.unmount_dataset(dataset_path, force=force)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Dataset unmounted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.get("/{dataset_path:path}/rename/form", response_class=HTMLResponse)
async def rename_dataset_form(
    request: Request,
    dataset_path: str
):
    """Display rename form"""
    return templates.TemplateResponse(
        "zfs/datasets/rename.jinja",
        {
            "request": request,
            "dataset_name": dataset_path,
            "page_title": f"Rename Dataset: {dataset_path}"
        }
    )


@router.post("/{dataset_path:path}/rename", response_class=HTMLResponse)
async def rename_dataset(
    request: Request,
    dataset_path: str,
    new_name: Annotated[str, Form()],
    force: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Rename a dataset"""
    try:
        dataset_service.rename_dataset(dataset_path, new_name, force=force)
        audit_logger.log_dataset_rename(user=current_user, old_name=dataset_path, new_name=new_name)
        return RedirectResponse(
            url=f"/zfs/datasets/{new_name}?message=Dataset renamed successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_dataset_rename(user=current_user, old_name=dataset_path, new_name=new_name, success=False, error=str(e))
        return templates.TemplateResponse(
            "zfs/datasets/rename.jinja",
            {
                "request": request,
                "dataset_name": dataset_path,
                "new_name": new_name,
                "error": str(e),
                "page_title": f"Rename Dataset: {dataset_path}"
            }
        )


@router.post("/{dataset_path:path}/promote", response_class=HTMLResponse)
async def promote_dataset(
    request: Request,
    dataset_path: str
):
    """Promote a cloned dataset"""
    try:
        dataset_service.promote_dataset(dataset_path)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Dataset promoted successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/load-key", response_class=HTMLResponse)
async def load_encryption_key(
    request: Request,
    dataset_path: str,
    key_location: Annotated[str, Form()] = ""
):
    """Load encryption key for a dataset"""
    try:
        dataset_service.load_key(dataset_path, key_location if key_location else None)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Encryption key loaded successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/unload-key", response_class=HTMLResponse)
async def unload_encryption_key(
    request: Request,
    dataset_path: str
):
    """Unload encryption key for a dataset"""
    try:
        dataset_service.unload_key(dataset_path)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Encryption key unloaded successfully",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/change-key", response_class=HTMLResponse)
async def change_encryption_key(
    request: Request,
    dataset_path: str,
    inherit: Annotated[bool, Form()] = False
):
    """Change encryption key for a dataset"""
    try:
        dataset_service.change_key(dataset_path, inherit=inherit)
        message = "Encryption key inherited from parent" if inherit else "Encryption key changed successfully"
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message={message}",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/change-key/inherit", response_class=HTMLResponse)
async def inherit_encryption_key(
    request: Request,
    dataset_path: str
):
    """Change dataset to inherit encryption key from parent"""
    try:
        dataset_service.change_key(dataset_path, inherit=True)
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Dataset now inherits encryption key from parent",
            status_code=303
        )
    except Exception as e:
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={str(e)}",
            status_code=303
        )


# Catch-all route - MUST BE LAST to not interfere with more specific routes above
@router.get("/{dataset_path:path}", response_class=HTMLResponse)
async def dataset_detail(
    request: Request,
    dataset_path: str
):
    """Display dataset details and properties"""
    try:
        dataset = dataset_service.get_dataset(dataset_path)
        
        # Try to get space usage, but don't fail if it's not available
        space_usage = []
        try:
            space_usage = dataset_service.get_space_usage(dataset_path, recursive=False)
        except Exception:
            # Space usage might not be fully available for all dataset types
            pass
        
        return templates.TemplateResponse(
            "zfs/datasets/detail.jinja",
            {
                "request": request,
                "dataset": dataset,
                "space_usage": space_usage,
                "page_title": f"Dataset: {dataset_path}"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            "zfs/datasets/detail.jinja",
            {
                "request": request,
                "dataset": None,
                "space_usage": [],
                "error": str(e),
                "page_title": f"Dataset: {dataset_path}"
            }
        )
