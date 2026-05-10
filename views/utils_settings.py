"""
WebZFS Settings Views
Provides the settings page for theme selection, session timeout configuration,
backup and restore of WebZFS configuration, and other WebZFS configuration.
"""
import io
import logging
from urllib.parse import quote

from fastapi import APIRouter, Request, Depends, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from config.templates import templates
from services.theme import (
    get_active_theme,
    get_all_themes_for_template,
    get_theme_variables,
    save_theme,
    is_valid_theme,
    THEME_REGISTRY,
)
from services.corner_style import (
    get_active_corner_style,
    get_all_corner_styles_for_template,
    save_corner_style,
    is_valid_corner_style,
    CORNER_STYLE_DISPLAY_NAMES,
)
from services.timeout_settings import (
    get_effective_session_timeout,
    save_session_timeout,
    reset_session_timeout,
    format_timeout_display,
    DEFAULT_SESSION_TIMEOUT,
    SESSION_TIMEOUT_PRESETS,
    VALID_TIMEOUT_VALUES,
)
from services import backup_restore
from auth.dependencies import get_current_user


logger = logging.getLogger(__name__)


# Maximum size accepted for an uploaded backup archive (8 MB is generous;
# real archives are typically a few KB to tens of KB).
MAX_UPLOAD_SIZE = 8 * 1024 * 1024


router = APIRouter(tags=["settings"], dependencies=[Depends(get_current_user)])


@router.get("/", response_class=HTMLResponse)
async def settings_index(request: Request, message: str = "", error: str = ""):
    """Display the WebZFS settings page with theme selector and session timeout."""
    active_theme = get_active_theme()
    theme_families = get_all_themes_for_template()
    theme_variables = get_theme_variables(active_theme)

    current_session_timeout = get_effective_session_timeout()

    active_corner_style = get_active_corner_style()
    corner_styles = get_all_corner_styles_for_template()

    response = templates.TemplateResponse(
        "utils/settings/index.jinja",
        {
            "request": request,
            "active_theme": active_theme,
            "active_theme_name": THEME_REGISTRY.get(active_theme, active_theme),
            "theme_families": theme_families,
            "theme_variables": theme_variables,
            "current_session_timeout": current_session_timeout,
            "current_session_timeout_display": format_timeout_display(current_session_timeout),
            "default_session_timeout": DEFAULT_SESSION_TIMEOUT,
            "default_session_timeout_display": format_timeout_display(DEFAULT_SESSION_TIMEOUT),
            "session_timeout_presets": SESSION_TIMEOUT_PRESETS,
            "active_corner_style": active_corner_style,
            "active_corner_style_name": CORNER_STYLE_DISPLAY_NAMES.get(
                active_corner_style, active_corner_style
            ),
            "corner_styles": corner_styles,
            "message": message,
            "error": error,
            "page_title": "WebZFS Settings",
        },
    )

    # When a theme or corner style was just changed, force HTMX to do a full
    # page refresh. HTMX hx-boost only swaps <body>, leaving stale stylesheet
    # references in <head> and the old body class. HX-Refresh on this 200
    # response (not the 303 redirect, which browsers follow transparently
    # before HTMX can see the header) tells HTMX to reload the entire page.
    if message and ("Theme changed" in message or "Corner style" in message):
        response.headers["HX-Refresh"] = "true"

    return response


@router.post("/apply-theme", response_class=HTMLResponse)
async def apply_theme(request: Request, theme: str = Form(...)):
    """Apply the selected theme and save to config."""
    if not is_valid_theme(theme):
        return RedirectResponse(
            url="/utils/settings?error=Invalid theme selection",
            status_code=303,
        )

    success = save_theme(theme)
    if success:
        theme_name = THEME_REGISTRY.get(theme, theme)
        return RedirectResponse(
            url=f"/utils/settings?message=Theme changed to {theme_name}",
            status_code=303,
        )
    else:
        return RedirectResponse(
            url="/utils/settings?error=Failed to save theme. Check file permissions on /opt/webzfs/.config/webzfs/",
            status_code=303,
        )


@router.post("/apply-corner-style", response_class=HTMLResponse)
async def apply_corner_style(request: Request, corner_style: str = Form(...)):
    """Apply the selected corner style and save to config."""
    if not is_valid_corner_style(corner_style):
        return RedirectResponse(
            url="/utils/settings?error=Invalid corner style selection",
            status_code=303,
        )

    success = save_corner_style(corner_style)
    if success:
        style_name = CORNER_STYLE_DISPLAY_NAMES.get(corner_style, corner_style)
        return RedirectResponse(
            url=f"/utils/settings?message=Corner style changed to {style_name}",
            status_code=303,
        )
    else:
        return RedirectResponse(
            url="/utils/settings?error=Failed to save corner style. Check file permissions on /opt/webzfs/.config/webzfs/",
            status_code=303,
        )


@router.get("/theme-preview/{theme_id}", response_class=HTMLResponse)
async def theme_preview(request: Request, theme_id: str):
    """Return the theme preview partial with CSS variables for the selected theme."""
    if not is_valid_theme(theme_id):
        return HTMLResponse("<p class='text-danger-400'>Invalid theme</p>")

    theme_variables = get_theme_variables(theme_id)
    theme_name = THEME_REGISTRY.get(theme_id, theme_id)

    return templates.TemplateResponse(
        "utils/settings/preview_partial.jinja",
        {
            "request": request,
            "preview_theme_id": theme_id,
            "preview_theme_name": theme_name,
            "theme_variables": theme_variables,
        },
    )


@router.post("/save-session-timeout", response_class=HTMLResponse)
async def save_session_timeout_view(request: Request, session_timeout: int = Form(...)):
    """Save user-configured session timeout value."""
    if session_timeout not in VALID_TIMEOUT_VALUES:
        return RedirectResponse(
            url="/utils/settings?error=Invalid session timeout selection",
            status_code=303,
        )

    try:
        save_session_timeout(session_timeout)
    except (ValueError, OSError) as exc:
        return RedirectResponse(
            url=f"/utils/settings?error=Failed to save session timeout: {exc}",
            status_code=303,
        )

    display = format_timeout_display(session_timeout)
    return RedirectResponse(
        url=f"/utils/settings?message=Session timeout set to {display}. New sessions will use this value.",
        status_code=303,
    )


@router.post("/reset-session-timeout", response_class=HTMLResponse)
async def reset_session_timeout_view(request: Request):
    """Reset session timeout to the default value."""
    try:
        reset_session_timeout()
    except OSError as exc:
        return RedirectResponse(
            url=f"/utils/settings?error=Failed to reset session timeout: {exc}",
            status_code=303,
        )

    return RedirectResponse(
        url=f"/utils/settings?message=Session timeout reset to default ({format_timeout_display(DEFAULT_SESSION_TIMEOUT)})",
        status_code=303,
    )


# ---------------------------------------------------------------------------
# Backup and Restore
# ---------------------------------------------------------------------------


def _redirect_with(message: str = "", error: str = "") -> RedirectResponse:
    """Build a redirect back to /utils/settings with a status message."""
    if error:
        return RedirectResponse(
            url=f"/utils/settings?error={quote(error)}", status_code=303
        )
    return RedirectResponse(
        url=f"/utils/settings?message={quote(message)}", status_code=303
    )


@router.post("/backup/export")
async def backup_export(
    request: Request,
    passphrase: str = Form(...),
    confirm_passphrase: str = Form(...),
    include_history: str = Form(None),
    include_secret: str = Form(None),
    include_system: str = Form(None),
):
    """
    Build an encrypted backup archive of WebZFS configuration and stream it
    to the user as a file download.
    """
    if passphrase != confirm_passphrase:
        return _redirect_with(error="Passphrases do not match.")
    if len(passphrase) < 8:
        return _redirect_with(error="Passphrase must be at least 8 characters long.")

    try:
        blob = backup_restore.create_archive(
            passphrase,
            include_history=bool(include_history),
            include_secret=bool(include_secret),
            include_system=bool(include_system),
        )
    except Exception as exc:
        logger.exception("Failed to build backup archive")
        return _redirect_with(error=f"Failed to build backup archive: {exc}")

    filename = backup_restore.default_archive_filename()
    return StreamingResponse(
        io.BytesIO(blob),
        media_type="application/octet-stream",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(blob)),
        },
    )


@router.post("/backup/inspect", response_class=HTMLResponse)
async def backup_inspect(
    request: Request,
    archive: UploadFile = File(...),
    passphrase: str = Form(...),
):
    """
    Decrypt only the manifest of an uploaded archive and render a confirmation
    page listing what would be restored. The archive is stashed on disk under
    a session-bound token; the actual restore is performed by /backup/restore.
    """
    blob = await archive.read()
    if not blob:
        return _redirect_with(error="No file was uploaded.")
    if len(blob) > MAX_UPLOAD_SIZE:
        return _redirect_with(
            error=f"Uploaded file is too large (limit {MAX_UPLOAD_SIZE // (1024 * 1024)} MB)."
        )

    try:
        info = backup_restore.inspect_archive(blob, passphrase)
    except ValueError as exc:
        return _redirect_with(error=str(exc))
    except Exception as exc:
        logger.exception("Failed to inspect backup archive")
        return _redirect_with(error=f"Failed to inspect archive: {exc}")

    try:
        token = backup_restore.stash_upload(blob)
    except Exception as exc:
        logger.exception("Failed to stash uploaded archive")
        return _redirect_with(error=f"Failed to stage upload: {exc}")

    # Group manifest files by category for the confirmation UI.
    files_by_category: dict = {}
    for entry in info["manifest"].get("files", []):
        cat = entry.get("category", "unknown")
        files_by_category.setdefault(cat, []).append(entry)

    return templates.TemplateResponse(
        "utils/settings/restore_confirm.jinja",
        {
            "request": request,
            "token": token,
            "header": info["header"],
            "manifest": info["manifest"],
            "summary": info["summary"],
            "files_by_category": files_by_category,
            "current_hostname": info["current_hostname"],
            "current_platform": info["current_platform"],
            "page_title": "Confirm Restore",
        },
    )


@router.post("/backup/restore")
async def backup_restore_apply(
    request: Request,
    token: str = Form(...),
    passphrase: str = Form(...),
    restore_config: str = Form(None),
    restore_ssh_key: str = Form(None),
    restore_sanoid: str = Form(None),
    restore_cron: str = Form(None),
    restore_env: str = Form(None),
):
    """
    Perform the actual restore using the stashed archive blob, the user's
    passphrase, and the per-category checkboxes.
    """
    selected = []
    if restore_config:
        selected.append(backup_restore.CATEGORY_CONFIG)
    if restore_ssh_key:
        selected.append(backup_restore.CATEGORY_SSH_KEY)
    if restore_sanoid:
        selected.append(backup_restore.CATEGORY_SANOID)
    if restore_cron:
        selected.append(backup_restore.CATEGORY_CRON)
    if restore_env:
        selected.append(backup_restore.CATEGORY_ENV)

    if not selected:
        return _redirect_with(error="No categories were selected for restore.")

    try:
        blob = backup_restore.load_stash(token)
    except ValueError as exc:
        return _redirect_with(error=str(exc))

    try:
        result = backup_restore.restore_archive(
            blob, passphrase, selected_categories=selected
        )
    except ValueError as exc:
        return _redirect_with(error=str(exc))
    except Exception as exc:
        logger.exception("Failed to apply backup restore")
        return _redirect_with(error=f"Failed to restore archive: {exc}")
    finally:
        backup_restore.discard_stash(token)

    restored_count = len(result.get("restored", []))
    failed_count = len(result.get("failed", []))
    if failed_count:
        return _redirect_with(
            error=(
                f"Restore completed with errors: {restored_count} files restored, "
                f"{failed_count} failed. Check server logs for details."
            )
        )
    return _redirect_with(
        message=(
            f"Restore complete. {restored_count} files written. "
            f"Existing files were preserved as *.pre-restore-{result['timestamp']}."
        )
    )
