"""
Corner Style Service

Manages the WebZFS corner-style preference. The corner style controls the
shape of cards, buttons, badges, and icon tiles across every page:

    rounded     Tailwind's default rounded corners (legacy look).
    squared     Hard 90 degree corners, flat rectangles.
    octagonal   Octagonal bevels via CSS clip-path.

The chosen style is persisted to /opt/webzfs/.config/webzfs/corner_style.conf
so it applies to every user of the WebZFS instance, the same pattern used
by the theme service.
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_CORNER_STYLE = "squared"
VALID_CORNER_STYLES = ("rounded", "squared", "octagonal")
CORNER_STYLE_DISPLAY_NAMES = {
    "rounded": "Rounded",
    "squared": "Squared",
    "octagonal": "Octagonal",
}
CORNER_STYLE_DESCRIPTIONS = {
    "rounded": "Tailwind default rounded corners.",
    "squared": "Hard 90 degree corners, flat rectangles.",
    "octagonal": "Octagonal bevels with 45 degree cut corners.",
}

CONFIG_DIR = Path("/opt/webzfs/.config/webzfs")
CONFIG_FILE = CONFIG_DIR / "corner_style.conf"


def is_valid_corner_style(style_id: str) -> bool:
    """Return True when the provided id matches a supported corner style."""
    return style_id in VALID_CORNER_STYLES


def get_active_corner_style() -> str:
    """
    Read the active corner style from the config file.
    Returns the style id, or DEFAULT_CORNER_STYLE if no config exists or the
    saved value is invalid.
    """
    try:
        if CONFIG_FILE.is_file():
            data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
            style_id = data.get("corner_style", DEFAULT_CORNER_STYLE)
            if is_valid_corner_style(style_id):
                return style_id
            logger.warning(
                "Saved corner style '%s' is invalid, falling back to default",
                style_id,
            )
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Could not read corner style config: %s", exc)
    return DEFAULT_CORNER_STYLE


def save_corner_style(style_id: str) -> bool:
    """
    Save the selected corner style to the config file.
    Creates the config directory if it does not exist.
    Returns True on success, False on failure.
    """
    if not is_valid_corner_style(style_id):
        logger.error("Cannot save invalid corner style: %s", style_id)
        return False
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        data = {"corner_style": style_id}
        CONFIG_FILE.write_text(json.dumps(data, indent=2) + "\n", encoding="utf-8")
        logger.info("Corner style saved: %s", style_id)
        return True
    except OSError as exc:
        logger.error("Failed to save corner style config: %s", exc)
        return False


def get_corner_styles_css_version() -> str:
    """
    Cache-buster token for static/css/corner_styles.css.

    Browsers aggressively cache static stylesheets. When the CSS file
    is edited, the URL must change for the browser to re-fetch. Use
    the file's mtime as the version token so every edit invalidates
    the cache automatically without requiring a hard refresh.
    """
    css_path = Path(__file__).resolve().parent.parent / "static" / "css" / "corner_styles.css"
    try:
        return str(int(css_path.stat().st_mtime))
    except OSError:
        return "0"


def get_all_corner_styles_for_template() -> list[dict]:
    """
    Return corner style options structured for the settings template.
    Each entry: {"id": str, "name": str, "description": str, "active": bool}
    """
    active = get_active_corner_style()
    return [
        {
            "id": style_id,
            "name": CORNER_STYLE_DISPLAY_NAMES[style_id],
            "description": CORNER_STYLE_DESCRIPTIONS[style_id],
            "active": style_id == active,
        }
        for style_id in VALID_CORNER_STYLES
    ]
