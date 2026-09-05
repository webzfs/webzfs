import os
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Form, Request

from auth.dependencies import get_current_user
from config.templates import templates
from services.audit_logger import audit_logger
from services.file import needs_sudo, read_file, save_file

router = APIRouter(dependencies=[Depends(get_current_user)])


def _format_file_size(size: int) -> str:
    """Format a byte count for compact display in the file browser."""
    value = float(size)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if value < 1024 or unit == "TiB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{size} B"


def _format_modified(timestamp: float) -> str:
    """Format a filesystem timestamp in the server's local time."""
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")


@router.get("/")
def index(request: Request):
    return templates.TemplateResponse(request, name="utils/files/index.jinja")


@router.post("/list")
def list_files(
    request: Request,
    directory: Annotated[str, Form()],
    current_user: str = Depends(get_current_user),
):
    try:
        directory_path = Path(os.path.expanduser(directory))
        if not directory_path.exists() or not directory_path.is_dir():
            audit_logger.log_directory_list(
                user=current_user,
                directory_path=directory,
                success=False,
                error="Directory does not exist or is not a directory",
            )
            return templates.TemplateResponse(
                request,
                name="partials/error.jinja",
                context={
                    "error": f"Directory {directory} does not exist or is not a directory"
                },
            )

        files = []
        for item in directory_path.iterdir():
            try:
                # Skip broken symlinks and inaccessible files
                is_dir = item.is_dir()
                if is_dir:
                    size = 0
                    modified = item.stat().st_mtime
                else:
                    stat_info = item.stat()
                    size = stat_info.st_size
                    modified = stat_info.st_mtime

                files.append(
                    {
                        "name": item.name,
                        "path": str(item),
                        "is_dir": is_dir,
                        "size": size,
                        "size_display": _format_file_size(size),
                        "modified": modified,
                        "modified_display": _format_modified(modified),
                    }
                )
            except (OSError, PermissionError):
                # Skip files we can't access (broken symlinks, permission denied, etc.)
                continue

        files.sort(key=lambda item: (not item["is_dir"], item["name"].lower()))
        resolved_directory = str(directory_path)
        audit_logger.log_directory_list(user=current_user, directory_path=directory)
        return templates.TemplateResponse(
            request,
            name="utils/files/list.jinja",
            context={"files": files, "directory": resolved_directory},
        )
    except Exception as exc:
        audit_logger.log_directory_list(
            user=current_user, directory_path=directory, success=False, error=str(exc)
        )
        return templates.TemplateResponse(
            request, name="partials/error.jinja", context={"error": str(exc)}
        )


@router.post("/read")
def read(
    request: Request,
    file_path: Annotated[str, Form()],
    current_user: str = Depends(get_current_user),
):
    try:
        # Auto-detect if sudo is needed for root-owned files
        use_sudo = needs_sudo(file_path)
        content = read_file(file_path, use_sudo=use_sudo)
        audit_logger.log_file_read(user=current_user, file_path=file_path)
    except Exception as exc:
        audit_logger.log_file_read(
            user=current_user, file_path=file_path, success=False, error=str(exc)
        )
        return templates.TemplateResponse(
            request, name="partials/error.jinja", context={"error": str(exc)}
        )

    return templates.TemplateResponse(
        request,
        name="utils/files/edit.jinja",
        context={"content": content, "file_path": file_path, "needs_sudo": use_sudo},
    )


@router.post("/save")
def save(
    request: Request,
    file_path: Annotated[str, Form()],
    content: Annotated[str, Form()],
    current_user: str = Depends(get_current_user),
):
    # Auto-detect if sudo is needed for root-owned files
    use_sudo = needs_sudo(file_path)
    context: dict[str, Any] = {
        "content": content,
        "file_path": file_path,
        "needs_sudo": use_sudo,
    }

    try:
        save_file(file_path, content, use_sudo=use_sudo)
        audit_logger.log_file_write(user=current_user, file_path=file_path)
    except Exception as exc:
        audit_logger.log_file_write(
            user=current_user, file_path=file_path, success=False, error=str(exc)
        )
        context["error"] = str(exc)
    else:
        context["success"] = True

    template_name = (
        "utils/files/edit_form.jinja"
        if request.headers.get("HX-Request") == "true"
        else "utils/files/edit.jinja"
    )
    return templates.TemplateResponse(request, name=template_name, context=context)
