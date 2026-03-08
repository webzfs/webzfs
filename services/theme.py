"""
Theme Service
Manages WebZFS theme selection, persistence, and discovery.
Themes are CSS files stored in static/css/themes/ that override CSS custom properties.
Configuration is persisted to /opt/webzfs/.config/webzfs/theme.conf
"""
import json
import logging
import os
from pathlib import Path
from typing import Optional

from config.settings import BASE_DIR

logger = logging.getLogger(__name__)

DEFAULT_THEME = "deep-ocean"
CONFIG_DIR = Path("/opt/webzfs/.config/webzfs")
CONFIG_FILE = CONFIG_DIR / "theme.conf"
THEMES_DIR = BASE_DIR / "static" / "css" / "themes"

# Theme registry: maps theme ID to display metadata
# Organized by family for the dropdown grouping
THEME_FAMILIES = {
    "Deep Ocean": {
        "deep-ocean": "Deep Ocean",
    },
    "Carbon": {
        "carbon-apricot": "Carbon Apricot",
        "carbon-blue": "Carbon Blue",
        "carbon-cyan": "Carbon Cyan",
        "carbon-emerald": "Carbon Emerald",
        "carbon-indigo": "Carbon Indigo",
        "carbon-raspberry": "Carbon Raspberry",
    },
    "Graphite": {
        "graphite-apricot": "Graphite Apricot",
        "graphite-cyan": "Graphite Cyan",
        "graphite-emerald": "Graphite Emerald",
        "graphite-indigo": "Graphite Indigo",
        "graphite-navy": "Graphite Navy",
        "graphite-ocean": "Graphite Ocean",
        "graphite-raspberry": "Graphite Raspberry",
    },
    "Medtech": {
        "medtech-apricot": "Medtech Apricot",
        "medtech-cyan": "Medtech Cyan",
        "medtech-emerald": "Medtech Emerald",
        "medtech-navy": "Medtech Navy",
        "medtech-raspberry": "Medtech Raspberry",
    },
    "Mono": {
        "mono-obsidian": "Mono Obsidian",
        "mono-paper": "Mono Paper",
    },
    "Operator": {
        "operator-apricot": "Operator Apricot",
        "operator-blue": "Operator Blue",
        "operator-cyan": "Operator Cyan",
        "operator-emerald": "Operator Emerald",
        "operator-raspberry": "Operator Raspberry",
    },
}


def _build_flat_registry() -> dict[str, str]:
    """Build a flat theme_id -> display_name mapping from the families."""
    flat = {}
    for family_themes in THEME_FAMILIES.values():
        flat.update(family_themes)
    return flat


THEME_REGISTRY = _build_flat_registry()


def get_theme_css_filename(theme_id: str) -> str:
    """Return the CSS filename for a given theme ID."""
    return f"webzfs-theme-{theme_id}.css"


def get_theme_css_path(theme_id: str) -> str:
    """Return the URL path for the theme CSS file (relative to static mount)."""
    return f"css/themes/{get_theme_css_filename(theme_id)}"


def is_valid_theme(theme_id: str) -> bool:
    """Check if a theme ID is valid and its CSS file exists."""
    if theme_id not in THEME_REGISTRY:
        return False
    css_file = THEMES_DIR / get_theme_css_filename(theme_id)
    return css_file.is_file()


def get_active_theme() -> str:
    """
    Read the active theme from the config file.
    Returns the theme ID, or DEFAULT_THEME if no config exists or the
    saved theme is invalid.
    """
    try:
        if CONFIG_FILE.is_file():
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            theme_id = data.get("theme", DEFAULT_THEME)
            if is_valid_theme(theme_id):
                return theme_id
            logger.warning("Saved theme '%s' is invalid, falling back to default", theme_id)
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read theme config: %s", exc)

    return DEFAULT_THEME


def save_theme(theme_id: str) -> bool:
    """
    Save the selected theme to the config file.
    Creates the config directory if it does not exist.
    Returns True on success, False on failure.
    """
    if not is_valid_theme(theme_id):
        logger.error("Cannot save invalid theme: %s", theme_id)
        return False

    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {"theme": theme_id}
        CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        logger.info("Theme saved: %s", theme_id)
        return True
    except OSError as exc:
        logger.error("Failed to save theme config: %s", exc)
        return False


def get_theme_variables(theme_id: str) -> dict[str, str]:
    """
    Parse the CSS custom properties from a theme file.
    Returns a dict of variable name -> value for use in the preview.
    """
    variables = {}
    css_file = THEMES_DIR / get_theme_css_filename(theme_id)
    if not css_file.is_file():
        return variables

    try:
        content = css_file.read_text(encoding="utf-8")
        for line in content.splitlines():
            line = line.strip()
            if line.startswith("--") and ":" in line:
                name, _, value = line.partition(":")
                name = name.strip()
                value = value.strip().rstrip(";").strip()
                variables[name] = value
    except OSError as exc:
        logger.warning("Could not read theme file %s: %s", css_file, exc)

    return variables


def get_all_themes_for_template() -> list[dict]:
    """
    Return theme data structured for the template dropdown.
    Each entry: {"family": str, "themes": [{"id": str, "name": str, "active": bool}]}
    """
    active_theme = get_active_theme()
    result = []
    for family_name, themes in THEME_FAMILIES.items():
        family_entry = {
            "family": family_name,
            "themes": []
        }
        for theme_id, display_name in themes.items():
            family_entry["themes"].append({
                "id": theme_id,
                "name": display_name,
                "active": theme_id == active_theme,
            })
        result.append(family_entry)
    return result
