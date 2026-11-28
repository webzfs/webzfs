"""
ZFS Dataset Management Views
Provides web interface for ZFS dataset operations
"""
from urllib.parse import quote

from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from typing import Annotated, Optional
from config.templates import templates
from services.zfs_dataset import (
    ZFSDatasetService,
    get_catalog_context,
    ShrinkZVOLConfirmationRequired,
)
from services.zfs_snapshot import ZFSSnapshotService
from services.audit_logger import audit_logger
from services.utils import is_netbsd, get_openzfs_man_page_url
from services.zfs_key_options import (
    KEYFORMAT_PASSPHRASE,
    KEYLOCATION_PROMPT,
    KEYFORMATS,
    KEYLOCATION_TYPES,
    KEY_SOURCE_EXISTING,
    build_key_material,
    build_keylocation,
    create_key_file,
    describe_keylocation_for_log,
    detect_https_keylocation_support,
    key_file_path_from_location,
    normalize_key_source,
    normalize_keyformat,
    redact_key_material,
    validate_key_combination,
)

from core.zfs_property_catalog import (
    editable_definitions,
    existing_data_notice,
    help_map,
    label_map,
    option_map,
    option_selected_value,
    user_properties,
    validate_user_property_name,
    value_type_map,
    warning_map,
    warnings_for,
)
from auth.dependencies import get_current_user


KEY_FILE_BACKUP_NOTICE = (
    "Back up this key file to secure offsite storage now. Without it the "
    "encrypted data cannot be recovered."
)


def _create_requested_key_file(
    user: str,
    dataset_name: str,
    keyformat: str,
    keylocation: str,
    key_source: str,
    key_string: str,
) -> Optional[str]:
    """Create a new key file when the user asked WebZFS to make one.

    Returns the path of the created file, or None when the key location is
    not a file or the user pointed to an existing file. The key file path is
    audit-logged; the key material never is.
    """
    path = key_file_path_from_location(keylocation)
    source = normalize_key_source(key_source)
    if not path or source == KEY_SOURCE_EXISTING:
        return None

    try:
        material = build_key_material(keyformat, source, key_string)
        create_key_file(path, material)
    except Exception as e:
        audit_logger.log_dataset_key_operation(
            user=user, operation="create_key_file", dataset_name=dataset_name,
            success=False, error=redact_key_material(str(e), [key_string]),
            key_file=path, keyformat=keyformat, key_source=source,
        )
        raise
    audit_logger.log_dataset_key_operation(
        user=user, operation="create_key_file", dataset_name=dataset_name,
        key_file=path, keyformat=keyformat, key_source=source,
    )
    return path


router = APIRouter(prefix="/zfs/datasets", tags=["zfs-datasets"], dependencies=[Depends(get_current_user)])
dataset_service = ZFSDatasetService()
snapshot_service = ZFSSnapshotService()


def _zvol_context(dataset_path: str, dataset: Optional[dict]) -> Optional[dict]:
    """Return zvol details for the detail page, or None for non-volumes."""
    if not dataset or not dataset.get('properties'):
        return None
    try:
        return dataset_service.get_zvol_info(dataset_path, properties=dataset['properties'])
    except Exception:
        return None


def _dataset_detail_response(request: Request, dataset_path: str,
                             error: Optional[str] = None,
                             shrink_confirmation: Optional[dict] = None):
    """Render the dataset detail page with zvol context."""
    try:
        dataset = dataset_service.get_dataset(dataset_path)
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="zfs/datasets/detail.jinja",
            context={
                "dataset": None,
                "space_usage": [],
                "zvol": None,
                "error": error or str(e),
                "page_title": f"Dataset: {dataset_path}"
            }
        )

    space_usage = []
    try:
        space_usage = dataset_service.get_space_usage(dataset_path, recursive=False)
    except Exception:
        pass

    return templates.TemplateResponse(
        request,
        name="zfs/datasets/detail.jinja",
        context={
            "dataset": dataset,
            "space_usage": space_usage,
            "zvol": _zvol_context(dataset_path, dataset),
            "error": error,
            "shrink_confirmation": shrink_confirmation,
            "keyformats": KEYFORMATS,
            "keylocation_types": KEYLOCATION_TYPES,
            "https_keylocation_supported": detect_https_keylocation_support(),
            "page_title": f"Dataset: {dataset_path}"
        }
    )


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
            request,
            name="zfs/datasets/index.jinja",
            context={
                "datasets": datasets,  # Keep original list for count
                "pools": pools,  # Grouped datasets by pool
                "selected_pool": pool,
                # NetBSD ZFS has no native encryption, so no key actions.
                "supports_encryption": not is_netbsd(),
                "page_title": "ZFS Datasets"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="zfs/datasets/index.jinja",
            context={
                "datasets": [],
                "pools": [],
                "error": str(e),
                "page_title": "ZFS Datasets"
            }
        )


@router.get("/snapshot-summary", response_class=JSONResponse)
async def dataset_snapshot_summary(
    pool: Optional[str] = None,
    dataset: Optional[str] = None,
):
    """Return direct snapshot counts and latest snapshot data by dataset."""
    if pool and dataset:
        return JSONResponse(
            content={
                "success": False,
                "error": "Specify either pool or dataset, not both",
            },
            status_code=400,
        )

    try:
        root_dataset = dataset or pool
        summaries = snapshot_service.get_dataset_snapshot_summaries(
            root_dataset=root_dataset,
            recursive=dataset is None,
        )
        return JSONResponse(
            content={"success": True, "summaries": summaries}
        )
    except Exception as e:
        return JSONResponse(
            content={"success": False, "error": str(e)},
            status_code=500,
        )


@router.get("/create/form", response_class=HTMLResponse)
async def create_dataset_form(
    request: Request,
    pool: Optional[str] = None,
    parent: Optional[str] = None
):
    """Display dataset creation form"""
    pool_datasets = []

    # pool and parent come from the query string; ignore anything that is not
    # a valid ZFS name so it never reaches the template.
    try:
        if pool:
            dataset_service.validate_dataset_name(pool)
    except ValueError:
        pool = None
    try:
        if parent:
            dataset_service.validate_dataset_name(parent)
    except ValueError:
        parent = None

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
            
    # NetBSD ZFS does not support encryption
    supports_encryption = not is_netbsd()
    
    return templates.TemplateResponse(
        request,
        name="zfs/datasets/create.jinja",
        context={
            "pool": pool,
            "parent": parent,
            "pool_datasets": pool_datasets,
            "supports_encryption": supports_encryption,
            "keyformats": KEYFORMATS,
            "keylocation_types": KEYLOCATION_TYPES,
            "https_keylocation_supported": detect_https_keylocation_support(),
            "keyformat": KEYFORMAT_PASSPHRASE,
            "keylocation": KEYLOCATION_PROMPT,
            "key_file_path": "",
            "key_url": "",
            "key_source": KEY_SOURCE_EXISTING,
            "dataset_type": "filesystem",
            "sparse": False,
            "volblocksize": None,
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
    sparse: Annotated[str, Form()] = "",
    volblocksize: Annotated[str, Form()] = "",
    casesensitivity: Annotated[str, Form()] = "",
    normalization: Annotated[str, Form()] = "",
    utf8only: Annotated[str, Form()] = "",
    encryption: Annotated[str, Form()] = "",
    passphrase: Annotated[str, Form()] = "",
    passphrase_confirm: Annotated[str, Form()] = "",
    keyformat: Annotated[str, Form()] = KEYFORMAT_PASSPHRASE,
    keylocation: Annotated[str, Form()] = KEYLOCATION_PROMPT,
    key_file_path: Annotated[str, Form()] = "",
    key_url: Annotated[str, Form()] = "",
    key_source: Annotated[str, Form()] = KEY_SOURCE_EXISTING,
    key_string: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Create a new dataset"""
    resolved_keyformat = None
    resolved_keylocation = None
    created_key_file = None
    try:
        properties = {}

        # Handle encryption
        if encryption:
            resolved_keyformat = normalize_keyformat(keyformat)
            resolved_keylocation = build_keylocation(
                keylocation, file_path=key_file_path, url=key_url
            )
            validate_key_combination(resolved_keyformat, resolved_keylocation)

            if resolved_keylocation == KEYLOCATION_PROMPT:
                if not passphrase:
                    raise Exception("Passphrase is required when encryption is enabled")
                dataset_service.validate_passphrase(passphrase)
                if passphrase != passphrase_confirm:
                    raise Exception("Passphrases do not match")

            # Set encryption properties
            properties['encryption'] = encryption
        
        # Handle volume creation
        if dataset_type == "volume":
            if not volsize:
                raise Exception("Volume size (volsize) is required for volume creation")
            properties['volsize'] = volsize
            # Volumes don't use mountpoint/atime/recordsize
        else:
            # Filesystem properties
            if mountpoint:
                properties['mountpoint'] = mountpoint
            if atime:
                properties['atime'] = atime
            
            # Creation-only filesystem properties (issue #207).
            # These cannot be changed after creation, so they are only
            # accepted here. Empty values mean inherit from parent.
            if normalization and normalization != 'none' and utf8only == 'off':
                raise Exception(
                    "Invalid combination: utf8only cannot be off when "
                    f"normalization is {normalization}. Normalization "
                    "requires UTF-8 only file names."
                )
            if casesensitivity:
                properties['casesensitivity'] = casesensitivity
            if normalization:
                properties['normalization'] = normalization
            if utf8only:
                properties['utf8only'] = utf8only
        
        # Common properties
        if compression:
            properties['compression'] = compression
        # recordsize does not apply to volumes; zfs rejects it for zvols
        if recordsize and dataset_type != "volume":
            properties['recordsize'] = recordsize

        # zvol options (issue #21): sparse (-s) and volblocksize (-b)
        create_sparse = sparse.strip().lower() in ('on', 'true', '1', 'yes')
        create_volblocksize = volblocksize.strip() or None
        if dataset_type != "volume":
            create_sparse = False
            create_volblocksize = None

        # Create dataset with encryption if specified
        if encryption:
            # Validate the name before any key file is written for it.
            dataset_service.validate_dataset_name(dataset_name)
            created_key_file = _create_requested_key_file(
                current_user, dataset_name, resolved_keyformat,
                resolved_keylocation, key_source, key_string,
            )
            dataset_service.create_dataset_with_key(
                dataset_name,
                keyformat=resolved_keyformat,
                keylocation=resolved_keylocation,
                passphrase=(
                    passphrase
                    if resolved_keylocation == KEYLOCATION_PROMPT
                    else None
                ),
                dataset_type=dataset_type,
                properties=properties,
                sparse=create_sparse,
                volblocksize=create_volblocksize
            )
        else:
            dataset_service.create_dataset(
                dataset_name,
                dataset_type=dataset_type,
                properties=properties,
                sparse=create_sparse,
                volblocksize=create_volblocksize
            )
        
        # Log successful dataset creation
        audit_logger.log_dataset_create(
            user=current_user, dataset_name=dataset_name,
            keyformat=resolved_keyformat,
            keylocation=describe_keylocation_for_log(resolved_keylocation),
        )

        message = f"Dataset {dataset_name} created successfully."
        if created_key_file:
            message += (
                f" The key file {created_key_file} was created. "
                + KEY_FILE_BACKUP_NOTICE
            )
        return RedirectResponse(
            url=f"/zfs/datasets?message={quote(message)}",
            status_code=303
        )
    except Exception as e:
        error = redact_key_material(str(e), [passphrase, key_string])
        if created_key_file:
            error += (
                f" The key file {created_key_file} was created before this "
                "error and has not been removed."
            )
        # Log failed dataset creation
        audit_logger.log_dataset_create(
            user=current_user, dataset_name=dataset_name, success=False,
            error=error, keyformat=resolved_keyformat,
            keylocation=describe_keylocation_for_log(resolved_keylocation),
        )
        # NetBSD ZFS does not support encryption
        supports_encryption = not is_netbsd()
        return templates.TemplateResponse(
            request,
            name="zfs/datasets/create.jinja",
            context={
                "error": error,
                "dataset_name": dataset_name,
                "dataset_type": dataset_type,
                "mountpoint": mountpoint,
                "compression": compression,
                "recordsize": recordsize,
                "atime": atime,
                "volsize": volsize,
                "sparse": sparse,
                "volblocksize": volblocksize,
                "casesensitivity": casesensitivity,
                "normalization": normalization,
                "utf8only": utf8only,
                "encryption": encryption,
                "keyformat": keyformat,
                "keylocation": keylocation,
                "key_file_path": key_file_path,
                "key_url": key_url,
                # The typed key string is never echoed back to the page.
                "key_source": key_source,
                "keyformats": KEYFORMATS,
                "keylocation_types": KEYLOCATION_TYPES,
                "https_keylocation_supported": detect_https_keylocation_support(),
                "supports_encryption": supports_encryption,
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
        try:
            descendant_count = dataset_service.count_descendants(dataset_path)
        except Exception:
            # The properties page must still render when the child listing
            # fails; recursive inherit then falls back to a generic warning.
            descendant_count = 0
        openzfs_man_url = get_openzfs_man_page_url()
        context = get_catalog_context()
        dataset_type = None
        type_entry = properties.get("type") if properties else None
        if isinstance(type_entry, dict):
            candidate = str(type_entry.get("value", "")).strip()
            if candidate in ("filesystem", "volume"):
                dataset_type = candidate
        editable_props = [d.name for d in editable_definitions(context, dataset_type)]
        
        return templates.TemplateResponse(
            request,
            name="zfs/datasets/properties.jinja",
            context={
                "dataset_name": dataset_path,
                "properties": properties,
                "descendant_count": descendant_count,
                "non_inheritable_props": dataset_service.NON_INHERITABLE_PROPERTIES,
                "no_received_revert_props": dataset_service.NO_RECEIVED_REVERT_PROPERTIES,
                "openzfs_man_url": openzfs_man_url,
                "editable_props": editable_props,
                "property_options": option_map(context, dataset_type),
                "property_selected_values": {
                    name: option_selected_value(
                        name,
                        (properties.get(name) or {}).get("value", "")
                        if isinstance(properties.get(name), dict) else "",
                        context,
                    )
                    for name in editable_props
                },
                "property_help": help_map(context, dataset_type),
                "property_labels": label_map(context, dataset_type),
                "property_warnings": warning_map(context, dataset_type),
                "property_value_types": value_type_map(context, dataset_type),
                "existing_data_notice_map": {
                    name: existing_data_notice(name) for name in editable_props
                },
                "user_properties": user_properties(properties),
                "catalog_zfs_version": context.zfs_version,
                "catalog_platform": context.platform,
                "page_title": f"Dataset Properties: {dataset_path}"
            }
        )
    except Exception as e:
        # Return full page with error for HTMX compatibility
        return templates.TemplateResponse(
            request,
            name="zfs/datasets/properties.jinja",
            context={
                "dataset_name": dataset_path,
                "properties": None,
                "openzfs_man_url": None,
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
        stripped_value = (property_value or "").strip()
        if not stripped_value and ":" in (property_name or ""):
            # An empty user-property value clears the local setting through
            # `zfs inherit`, so the dataset falls back to the parent's value.
            validate_user_property_name(property_name.strip())
            dataset_service.inherit_property(dataset_path, property_name.strip())
            message = "User property removed; the dataset now inherits it from its parent"
        else:
            dataset_service.set_property(dataset_path, property_name, property_value)
            message = "Property updated successfully."
            notice = existing_data_notice(property_name.strip())
            if notice:
                message = f"{message} {notice}"
            # Only the special_small_blocks warning compares against the
            # dataset's recordsize, so only then read the properties back.
            current_properties = None
            if property_name.strip() == "special_small_blocks":
                try:
                    current_properties = dataset_service.get_properties(dataset_path)
                except Exception:
                    current_properties = None
            for warning in warnings_for(
                property_name.strip(), stripped_value,
                get_catalog_context(), current_properties
            ):
                message = f"{message} Warning: {warning}"
        audit_logger.log_dataset_property_change(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, property_value=property_value
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?message={quote(message)}",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_dataset_property_change(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, property_value=property_value,
            success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?error={quote(str(e))}",
            status_code=303
        )


@router.post("/{dataset_path:path}/properties/inherit", response_class=HTMLResponse)
async def inherit_dataset_property(
    request: Request,
    dataset_path: str,
    property_name: Annotated[str, Form()],
    recursive: Annotated[bool, Form()] = False,
    received: Annotated[bool, Form()] = False,
    current_user: str = Depends(get_current_user)
):
    """Inherit a dataset property from its parent, default, or received value"""
    try:
        dataset_service.inherit_property(
            dataset_path, property_name,
            recursive=recursive, received=received
        )
        audit_logger.log_dataset_property_inherit(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, recursive=recursive,
            received=received, success=True
        )
        message = "Property inherited successfully"
        if received:
            message = "Property reverted to its received value"
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?message={quote(message)}",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_dataset_property_inherit(
            user=current_user, dataset_name=dataset_path,
            property_name=property_name, recursive=recursive,
            received=received, success=False, error=str(e)
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}/properties?error={quote(str(e))}",
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
        request,
        name="zfs/datasets/rename.jinja",
        context={
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
            request,
            name="zfs/datasets/rename.jinja",
            context={
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
    passphrase: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Load encryption key for a dataset"""
    try:
        dataset_service.load_key(
            dataset_path,
            passphrase=passphrase or None,
        )
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="load_key", dataset_name=dataset_path,
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Encryption key loaded successfully",
            status_code=303
        )
    except Exception as e:
        error = redact_key_material(str(e), [passphrase])
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="load_key", dataset_name=dataset_path,
            success=False, error=error,
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={quote(error)}",
            status_code=303
        )


@router.post("/{dataset_path:path}/unload-key", response_class=HTMLResponse)
async def unload_encryption_key(
    request: Request,
    dataset_path: str,
    current_user: str = Depends(get_current_user)
):
    """Unload encryption key for a dataset"""
    try:
        dataset_service.unload_key(dataset_path)
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="unload_key", dataset_name=dataset_path,
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Encryption key unloaded successfully",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="unload_key", dataset_name=dataset_path,
            success=False, error=str(e),
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={quote(str(e))}",
            status_code=303
        )


@router.post("/load-all-keys", response_class=HTMLResponse)
async def load_all_encryption_keys(
    request: Request,
    current_user: str = Depends(get_current_user)
):
    """Load the keys for every encryption root in all imported pools"""
    try:
        result = dataset_service.load_all_keys()
    except Exception as e:
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="load_all_keys",
            success=False, error=str(e),
        )
        return RedirectResponse(
            url=f"/zfs/datasets?error={quote(str(e))}",
            status_code=303
        )

    summary = result.get("summary") or ""
    if result.get("success"):
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="load_all_keys", summary=summary,
        )
        message = "Encryption keys loaded. " + summary
        return RedirectResponse(
            url=f"/zfs/datasets?message={quote(message.strip())}",
            status_code=303
        )

    error = result.get("error") or ""
    audit_logger.log_dataset_key_operation(
        user=current_user, operation="load_all_keys", success=False,
        error=error, summary=summary,
    )
    message = (
        "Some encryption keys could not be loaded. Datasets using a prompt "
        f"key location must be loaded individually. {summary} {error}"
    )
    return RedirectResponse(
        url=f"/zfs/datasets?error={quote(message.strip())}",
        status_code=303
    )


@router.post("/{dataset_path:path}/change-key", response_class=HTMLResponse)
async def change_encryption_key(
    request: Request,
    dataset_path: str,
    inherit: Annotated[bool, Form()] = False,
    keyformat: Annotated[str, Form()] = "",
    keylocation: Annotated[str, Form()] = "",
    key_file_path: Annotated[str, Form()] = "",
    key_url: Annotated[str, Form()] = "",
    key_source: Annotated[str, Form()] = KEY_SOURCE_EXISTING,
    key_string: Annotated[str, Form()] = "",
    new_passphrase: Annotated[str, Form()] = "",
    new_passphrase_confirm: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Change encryption key for a dataset"""
    if inherit:
        return _inherit_key(dataset_path, current_user)

    resolved_format = None
    resolved_location = None
    created_key_file = None
    try:
        if not keylocation:
            raise Exception("Select the new key location to change the key")

        resolved_location = build_keylocation(
            keylocation, file_path=key_file_path, url=key_url
        )
        resolved_format = normalize_keyformat(keyformat) if keyformat else None

        if resolved_location == KEYLOCATION_PROMPT:
            if not new_passphrase:
                raise Exception("A new passphrase is required")
            if new_passphrase != new_passphrase_confirm:
                raise Exception("New passphrases do not match")

        if key_file_path_from_location(resolved_location):
            source = normalize_key_source(key_source)
            if source != KEY_SOURCE_EXISTING and not resolved_format:
                raise Exception(
                    "Select a key format when WebZFS creates the key file"
                )
            if resolved_format:
                validate_key_combination(resolved_format, resolved_location)
            dataset_service.validate_dataset_name(dataset_path)
            created_key_file = _create_requested_key_file(
                current_user, dataset_path, resolved_format,
                resolved_location, source, key_string,
            )

        dataset_service.change_key(
            dataset_path,
            keyformat=resolved_format,
            keylocation=resolved_location,
            passphrase=(
                new_passphrase
                if resolved_location == KEYLOCATION_PROMPT
                else None
            ),
        )
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="change_key", dataset_name=dataset_path,
            keyformat=resolved_format,
            keylocation=describe_keylocation_for_log(resolved_location),
        )
        message = "Encryption key changed successfully."
        if created_key_file:
            message += (
                f" The key file {created_key_file} was created. "
                + KEY_FILE_BACKUP_NOTICE
            )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message={quote(message)}",
            status_code=303
        )
    except Exception as e:
        error = redact_key_material(str(e), [new_passphrase, key_string])
        if created_key_file:
            error += (
                f" The key file {created_key_file} was created before this "
                "error and has not been removed. The dataset still uses its "
                "previous key."
            )
        audit_logger.log_dataset_key_operation(
            user=current_user, operation="change_key", dataset_name=dataset_path,
            success=False, error=error, keyformat=resolved_format,
            keylocation=describe_keylocation_for_log(resolved_location),
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={quote(error)}",
            status_code=303
        )


def _inherit_key(dataset_path: str, user: str) -> RedirectResponse:
    """Run zfs change-key -i, audit-log it and redirect to the dataset."""
    try:
        dataset_service.change_key(dataset_path, inherit=True)
    except Exception as e:
        audit_logger.log_dataset_key_operation(
            user=user, operation="inherit_key", dataset_name=dataset_path,
            success=False, error=str(e),
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?error={quote(str(e))}",
            status_code=303
        )
    audit_logger.log_dataset_key_operation(
        user=user, operation="inherit_key", dataset_name=dataset_path,
    )
    return RedirectResponse(
        url=f"/zfs/datasets/{dataset_path}?message="
        + quote("Dataset now inherits encryption key from parent"),
        status_code=303
    )


@router.post("/{dataset_path:path}/change-key/inherit", response_class=HTMLResponse)
async def inherit_encryption_key(
    request: Request,
    dataset_path: str,
    current_user: str = Depends(get_current_user)
):
    """Change dataset to inherit encryption key from parent"""
    return _inherit_key(dataset_path, current_user)


@router.post("/{dataset_path:path}/quota", response_class=HTMLResponse)
async def set_dataset_quota(
    request: Request,
    dataset_path: str,
    quota_size: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Set or remove dataset quota"""
    try:
        value = quota_size.strip() if quota_size.strip() else 'none'
        dataset_service.set_property(dataset_path, 'quota', value)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="set_quota",
            pool=dataset_path.split('/')[0],
            details=f"Set quota={value} on dataset {dataset_path}"
        )
        message = f"Quota set to {value} on {dataset_path}" if value != 'none' else f"Quota removed from {dataset_path}"
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message={message}",
            status_code=303
        )
    except Exception as e:
        # Produce a clean, user-friendly error message
        raw = str(e).strip().replace('\n', ' ')
        if 'size is less than current used or reserved space' in raw:
            error_msg = "Quota must be larger than the current used space"
        elif 'invalid property value' in raw:
            error_msg = "Invalid quota value. Use sizes like 100G, 500G, 1T, or none"
        else:
            error_msg = "Failed to set quota. Check the value and try again"
        # Re-render page with error instead of redirect to keep URL clean
        return _dataset_detail_response(request, dataset_path, error=error_msg)


@router.post("/{dataset_path:path}/volsize", response_class=HTMLResponse)
async def resize_dataset_volume(
    request: Request,
    dataset_path: str,
    volsize: Annotated[str, Form()],
    confirm_shrink: Annotated[str, Form()] = "",
    current_user: str = Depends(get_current_user)
):
    """Grow or shrink a zvol through the volsize property"""
    pool_name = dataset_path.split('/')[0]
    confirmed = confirm_shrink.strip().lower() in ('on', 'true', '1', 'yes')
    try:
        result = dataset_service.resize_volume(
            dataset_path, volsize, confirm_shrink=confirmed
        )
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="resize_volume",
            pool=pool_name,
            details=(
                f"{result['direction']} {dataset_path} volsize "
                f"{result['old_size']} -> {result['new_size']}"
            )
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=Volume resized from "
                f"{result['old_size']} to {result['new_size']}",
            status_code=303
        )
    except ShrinkZVOLConfirmationRequired as e:
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="resize_volume",
            success=False,
            pool=pool_name,
            details=(
                f"Shrink of {dataset_path} from {e.current_size} to "
                f"{e.new_size} awaiting confirmation"
            ),
            error=str(e)
        )
        return _dataset_detail_response(
            request,
            dataset_path,
            shrink_confirmation={
                "dataset": dataset_path,
                "current_size": e.current_size,
                "new_size": e.new_size,
                "new_size_bytes": e.new_size_bytes,
                "warning": str(e),
            }
        )
    except Exception as e:
        # VolumeResizeError, ValueError, or a failed zfs read
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="resize_volume",
            success=False,
            pool=pool_name,
            details=f"Resize of {dataset_path} to {volsize} failed",
            error=str(e)
        )
        return _dataset_detail_response(request, dataset_path, error=str(e))


@router.post("/{dataset_path:path}/volmode", response_class=HTMLResponse)
async def set_dataset_volmode(
    request: Request,
    dataset_path: str,
    volmode: Annotated[str, Form()],
    current_user: str = Depends(get_current_user)
):
    """Set the volmode property of a zvol"""
    pool_name = dataset_path.split('/')[0]
    try:
        dataset_service.set_volmode(dataset_path, volmode)
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="set_volmode",
            pool=pool_name,
            details=f"Set volmode={volmode} on volume {dataset_path}"
        )
        return RedirectResponse(
            url=f"/zfs/datasets/{dataset_path}?message=volmode set to {volmode}",
            status_code=303
        )
    except Exception as e:
        audit_logger.log_zfs_operation(
            user=current_user,
            operation="set_volmode",
            success=False,
            pool=pool_name,
            details=f"Set volmode={volmode} on volume {dataset_path} failed",
            error=str(e)
        )
        return _dataset_detail_response(request, dataset_path, error=str(e))


@router.get("/{dataset_path:path}/peek", response_class=HTMLResponse)
async def peek_dataset_directory(
    request: Request,
    dataset_path: str,
    subpath: Optional[str] = None
):
    """Return the peek modal partial listing a directory inside the dataset"""
    try:
        peek = dataset_service.peek_directory(dataset_path, subpath or "")
    except Exception as e:
        peek = {
            'dataset': dataset_path,
            'mountpoint': None,
            'subpath': '',
            'breadcrumbs': [],
            'entries': [],
            'truncated': False,
            'error': str(e),
        }
    
    return templates.TemplateResponse(
        request,
        name="zfs/datasets/peek_modal.jinja",
        context={"peek": peek}
    )


# Catch-all route - MUST BE LAST to not interfere with more specific routes above
@router.get("/{dataset_path:path}", response_class=HTMLResponse)
async def dataset_detail(
    request: Request,
    dataset_path: str
):
    """Display dataset details and properties"""
    return _dataset_detail_response(request, dataset_path)
