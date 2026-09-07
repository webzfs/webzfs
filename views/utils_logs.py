"""
Audit Logs Viewer
Provides web interface to view and download audit logs
"""

import base64
from fastapi import APIRouter, Request, Depends, HTTPException, Query
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse
from starlette.background import BackgroundTask
from typing import Optional, List, Dict, Any
from pathlib import Path
from datetime import datetime

from config.templates import templates
from services.audit_logger import audit_logger, LogCategory
from auth.dependencies import get_current_user
from core.request_context import is_cockpit_request
from services.shell_recording import (
    create_recording_archive,
    get_events,
    ledger_bytes,
    list_recordings,
    public_key_bytes,
    recording_display_metadata,
    recording_summary,
    verify_session,
)
from services.shell_settings import can_audit_shell_recordings

router = APIRouter()


def read_log_file(
    log_path: Path, lines: int = 500, search: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Read and parse log file entries.

    Args:
        log_path: Path to the log file
        lines: Maximum number of lines to return (most recent)
        search: Optional search string to filter entries

    Returns:
        List of parsed log entries
    """
    entries = []

    if not log_path.exists():
        return entries

    try:
        with open(log_path, "r", encoding="utf-8") as f:
            all_lines = f.readlines()

        # Get the last N lines (most recent)
        recent_lines = all_lines[-lines:] if len(all_lines) > lines else all_lines

        for line in recent_lines:
            line = line.strip()
            if not line:
                continue

            # Apply search filter if provided
            if search and search.lower() not in line.lower():
                continue

            # Parse the log entry
            entry = parse_log_entry(line)
            if entry:
                entries.append(entry)

        # Reverse to show most recent first
        entries.reverse()

    except Exception as e:
        entries.append(
            {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "level": "ERROR",
                "message": f"Error reading log file: {str(e)}",
                "raw": str(e),
            }
        )

    return entries


def parse_log_entry(line: str) -> Optional[Dict[str, Any]]:
    """
    Parse a single log entry into a structured dictionary.

    Args:
        line: Raw log line

    Returns:
        Parsed entry dictionary or None if parsing fails
    """
    try:
        # Expected format: "2025-12-17 23:05:00 [INFO] key1=value1 key2=value2"
        parts = line.split(" ", 3)

        if len(parts) < 4:
            return {
                "timestamp": "",
                "level": "INFO",
                "message": line,
                "raw": line,
                "details": {},
            }

        date_part = parts[0]
        time_part = parts[1]
        level_part = parts[2].strip("[]")
        message_part = parts[3]

        # Parse key=value pairs from message
        details = {}
        for item in message_part.split():
            if "=" in item:
                key, value = item.split("=", 1)
                # Remove quotes from value if present
                value = value.strip('"')
                details[key] = value

        return {
            "timestamp": f"{date_part} {time_part}",
            "level": level_part,
            "message": message_part,
            "raw": line,
            "details": details,
        }

    except Exception:
        return {
            "timestamp": "",
            "level": "INFO",
            "message": line,
            "raw": line,
            "details": {},
        }


def get_log_file_info(log_path: Path) -> Dict[str, Any]:
    """
    Get information about a log file.

    Args:
        log_path: Path to the log file

    Returns:
        Dictionary with file information
    """
    if not log_path.exists():
        return {
            "exists": False,
            "size": 0,
            "size_human": "0 B",
            "modified": None,
            "line_count": 0,
        }

    try:
        stat = log_path.stat()
        size = stat.st_size
        modified = datetime.fromtimestamp(stat.st_mtime)

        # Count lines
        with open(log_path, "r", encoding="utf-8") as f:
            line_count = sum(1 for _ in f)

        # Human-readable size
        if size < 1024:
            size_human = f"{size} B"
        elif size < 1024 * 1024:
            size_human = f"{size / 1024:.1f} KB"
        else:
            size_human = f"{size / (1024 * 1024):.1f} MB"

        return {
            "exists": True,
            "size": size,
            "size_human": size_human,
            "modified": modified.strftime("%Y-%m-%d %H:%M:%S"),
            "line_count": line_count,
        }

    except Exception:
        return {
            "exists": False,
            "size": 0,
            "size_human": "0 B",
            "modified": None,
            "line_count": 0,
        }


@router.get("/", response_class=HTMLResponse)
async def logs_index(request: Request, current_user: str = Depends(get_current_user)):
    """Display logs overview page with all log categories"""
    try:
        log_paths = audit_logger.get_all_log_paths()

        logs_info = {}
        for category_name, log_path in log_paths.items():
            logs_info[category_name] = {
                "path": str(log_path),
                "info": get_log_file_info(log_path),
                "category": category_name,
            }

        cockpit_context = is_cockpit_request(request)
        audit_allowed = can_audit_shell_recordings(current_user)
        shell_history = None
        if audit_allowed and not cockpit_context:
            shell_history = recording_summary()
        return templates.TemplateResponse(
            request,
            name="utils/logs/index.jinja",
            context={
                "logs_info": logs_info,
                "log_dir": str(audit_logger.log_dir),
                "page_title": "Audit Logs",
                "cockpit_context": cockpit_context,
                "shell_history_allowed": audit_allowed,
                "shell_history": shell_history,
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="utils/logs/index.jinja",
            context={
                "logs_info": {},
                "log_dir": "",
                "error": str(e),
                "page_title": "Audit Logs",
                "cockpit_context": is_cockpit_request(request),
                "shell_history_allowed": can_audit_shell_recordings(current_user),
                "shell_history": None,
            },
        )


@router.get("/view/{category}", response_class=HTMLResponse)
async def view_log(
    request: Request,
    category: str,
    lines: int = Query(default=500, ge=10, le=10000),
    search: Optional[str] = Query(default=None),
    current_user: str = Depends(get_current_user),
):
    """View entries from a specific log file"""
    if category == LogCategory.SHELL.value:
        _require_native_shell_auditor(request, current_user)
    try:
        # Validate category
        valid_categories = [c.value for c in LogCategory]
        if category not in valid_categories:
            return templates.TemplateResponse(
                request,
                name="partials/error.jinja",
                context={
                    "error": f"Invalid log category: {category}. Valid categories: {', '.join(valid_categories)}",
                    "back_url": "/utils/logs",
                },
            )

        log_path = audit_logger.log_dir / f"{category}.log"
        entries = read_log_file(log_path, lines=lines, search=search)
        file_info = get_log_file_info(log_path)

        # Category display names
        category_names = {
            "auth": "Authentication",
            "zfs_operations": "ZFS Operations",
            "file_access": "File Access",
            "shell_sessions": "Terminal Lifecycle",
        }

        return templates.TemplateResponse(
            request,
            name="utils/logs/view.jinja",
            context={
                "category": category,
                "category_name": category_names.get(category, category),
                "entries": entries,
                "file_info": file_info,
                "lines": lines,
                "search": search or "",
                "page_title": f"Log Viewer: {category_names.get(category, category)}",
            },
        )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="utils/logs/view.jinja",
            context={
                "category": category,
                "category_name": category,
                "entries": [],
                "file_info": {},
                "lines": lines,
                "search": search or "",
                "error": str(e),
                "page_title": f"Log Viewer: {category}",
            },
        )


@router.get("/download/{category}")
async def download_log(
    request: Request,
    category: str,
    current_user: str = Depends(get_current_user),
):
    """Download a log file"""
    if category == LogCategory.SHELL.value:
        _require_native_shell_auditor(request, current_user)
    try:
        # Validate category
        valid_categories = [c.value for c in LogCategory]
        if category not in valid_categories:
            return PlainTextResponse(
                content=f"Invalid log category: {category}", status_code=400
            )

        log_path = audit_logger.log_dir / f"{category}.log"

        if not log_path.exists():
            return PlainTextResponse(
                content=f"Log file not found: {category}.log", status_code=404
            )

        with open(log_path, "r", encoding="utf-8") as f:
            content = f.read()

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"webzfs_{category}_{timestamp}.log"

        return PlainTextResponse(
            content=content,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    except Exception as e:
        return PlainTextResponse(
            content=f"Error downloading log: {str(e)}", status_code=500
        )


@router.get("/entries/{category}", response_class=HTMLResponse)
async def get_log_entries_partial(
    request: Request,
    category: str,
    lines: int = Query(default=500, ge=10, le=10000),
    search: Optional[str] = Query(default=None),
    current_user: str = Depends(get_current_user),
):
    """HTMX endpoint to get log entries (partial update)"""
    if category == LogCategory.SHELL.value:
        _require_native_shell_auditor(request, current_user)
    try:
        # Validate category
        valid_categories = [c.value for c in LogCategory]
        if category not in valid_categories:
            return HTMLResponse(
                content=f'<div class="text-danger-400">Invalid log category: {category}</div>',
                status_code=400,
            )

        log_path = audit_logger.log_dir / f"{category}.log"
        entries = read_log_file(log_path, lines=lines, search=search)

        return templates.TemplateResponse(
            request,
            name="utils/logs/entries.jinja",
            context={"entries": entries, "category": category, "search": search or ""},
        )

    except Exception as e:
        return HTMLResponse(
            content=f'<div class="text-danger-400">Error loading entries: {str(e)}</div>',
            status_code=500,
        )


def _require_native_shell_auditor(request: Request, current_user: str) -> None:
    if is_cockpit_request(request):
        raise HTTPException(
            status_code=403,
            detail="Shell Session History is not available inside Cockpit.",
        )
    if not can_audit_shell_recordings(current_user):
        raise HTTPException(
            status_code=403, detail="Shell recording audit access denied."
        )


@router.get("/shell-history", response_class=HTMLResponse)
async def shell_history_index(
    request: Request,
    user: str = Query(default="", max_length=128),
    status: str = Query(default="", pattern="^(|completed|incomplete)$"),
    integrity: str = Query(default="", pattern="^(|valid|invalid)$"),
    current_user: str = Depends(get_current_user),
):
    """List native WebZFS terminal recordings for authorized auditors."""
    if is_cockpit_request(request):
        return templates.TemplateResponse(
            request,
            name="utils/logs/shell_history_disabled.jinja",
            context={"page_title": "Shell Session History"},
        )
    if not can_audit_shell_recordings(current_user):
        raise HTTPException(
            status_code=403, detail="Shell recording audit access denied."
        )
    recordings = list_recordings()
    if user:
        recordings = [
            item
            for item in recordings
            if user.lower() in item.get("authenticated_user", "").lower()
        ]
    if status:
        recordings = [
            item for item in recordings if item.get("effective_status") == status
        ]
    if integrity:
        expected_valid = integrity == "valid"
        recordings = [
            item
            for item in recordings
            if item.get("verification", {}).get("valid") is expected_valid
        ]
    return templates.TemplateResponse(
        request,
        name="utils/logs/shell_history.jinja",
        context={
            "recordings": recordings,
            "summary": recording_summary(),
            "filters": {"user": user, "status": status, "integrity": integrity},
            "page_title": "Shell Session History",
        },
    )


@router.get("/shell-history/{session_id}", response_class=HTMLResponse)
async def shell_history_detail(
    request: Request,
    session_id: str,
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    try:
        metadata = recording_display_metadata(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return templates.TemplateResponse(
        request,
        name="utils/logs/shell_session.jinja",
        context={
            "metadata": metadata,
            "verification": verify_session(session_id),
            "page_title": "Shell Session Recording",
        },
    )


@router.get("/shell-history/{session_id}/events")
async def shell_history_events(
    request: Request,
    session_id: str,
    start: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=2000),
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    try:
        return JSONResponse(
            get_events(
                session_id,
                start=start,
                limit=limit,
                event_types=frozenset({"output", "resize"}),
            )
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/shell-history/{session_id}/input")
async def shell_history_input(
    request: Request,
    session_id: str,
    start: int = Query(default=0, ge=0),
    limit: int = Query(default=500, ge=1, le=2000),
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    try:
        return JSONResponse(
            get_events(
                session_id,
                start=start,
                limit=limit,
                event_types=frozenset({"input"}),
            )
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@router.get("/shell-history/{session_id}/download")
async def shell_history_download(
    request: Request,
    session_id: str,
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    try:
        archive_path = create_recording_archive(session_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return FileResponse(
        archive_path,
        media_type="application/zip",
        filename=f"webzfs-shell-{session_id}.zip",
        background=BackgroundTask(archive_path.unlink, missing_ok=True),
    )


@router.get("/shell-history-public-key")
async def shell_history_public_key(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    return PlainTextResponse(
        content=base64.b64encode(public_key_bytes()).decode("ascii") + "\n",
        headers={
            "Content-Disposition": 'attachment; filename="webzfs-shell-signing-key.txt"'
        },
    )


@router.get("/shell-history-ledger")
async def shell_history_ledger(
    request: Request,
    current_user: str = Depends(get_current_user),
):
    _require_native_shell_auditor(request, current_user)
    return PlainTextResponse(
        content=ledger_bytes().decode("utf-8"),
        headers={
            "Content-Disposition": 'attachment; filename="webzfs-shell-recording-ledger.jsonl"'
        },
    )
