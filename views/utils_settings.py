"""
WebZFS Settings Views
Provides the settings page for theme selection and other WebZFS configuration.
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
from auth.dependencies import get_current_user


router = APIRouter(tags=["settings"], dependencies=[Depends(get_current_user)])


@router.get("/", response_class=HTMLResponse)
async def settings_index(request: Request, message: str = "", error: str = ""):
    """Display the WebZFS settings page with theme selector."""
    active_theme = get_active_theme()
    theme_families = get_all_themes_for_template()
    theme_variables = get_theme_variables(active_theme)

    return templates.TemplateResponse(
        "utils/settings/index.jinja",
        {
            "request": request,
            "active_theme": active_theme,
            "active_theme_name": THEME_REGISTRY.get(active_theme, active_theme),
            "theme_families": theme_families,
            "theme_variables": theme_variables,
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
        return RedirectResponse(
            url=f"/utils/settings?message=Theme changed to {theme_name}",
            status_code=303,
        )
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
