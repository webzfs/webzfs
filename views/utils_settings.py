"""
WebZFS Settings Views
Provides the settings page for theme selection, session timeout configuration,
and other WebZFS configuration.
"""
from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from config.templates import templates
from services.theme import (
    get_active_theme,
    get_all_themes_for_template,
    get_theme_variables,
    save_theme,
    is_valid_theme,
    THEME_REGISTRY,
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
from auth.dependencies import get_current_user


router = APIRouter(tags=["settings"], dependencies=[Depends(get_current_user)])


@router.get("/", response_class=HTMLResponse)
async def settings_index(request: Request, message: str = "", error: str = ""):
    """Display the WebZFS settings page with theme selector and session timeout."""
    active_theme = get_active_theme()
    theme_families = get_all_themes_for_template()
    theme_variables = get_theme_variables(active_theme)

    current_session_timeout = get_effective_session_timeout()

    return templates.TemplateResponse(
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
            "message": message,
            "error": error,
            "page_title": "WebZFS Settings",
        },
    )


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
        response = RedirectResponse(
            url=f"/utils/settings?message=Theme changed to {theme_name}",
            status_code=303,
        )
        # Force full page refresh so the <head> reloads with the new theme CSS.
        # HTMX hx-boost only swaps <body>, which leaves the old theme stylesheet
        # in <head> unchanged. HX-Refresh tells HTMX to do a full page reload.
        response.headers["HX-Refresh"] = "true"
        return response
    else:
        return RedirectResponse(
            url="/utils/settings?error=Failed to save theme. Check file permissions on /opt/webzfs/.config/webzfs/",
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
