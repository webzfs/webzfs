from dataclasses import dataclass
from fastapi.templating import Jinja2Templates
from config.settings import BASE_DIR, settings
from services.theme import get_theme_css_path, get_active_theme
from services.corner_style import get_active_corner_style, get_corner_styles_css_version


@dataclass
class Tab:
    id: str
    label: str
    url: str

# Hardcoded navigation tabs - all tabs are now always visible
NAV_TABS = [
    Tab(id="dashboard", label="Dashboard", url="/"),
    Tab(id="zfs_pools", label="Pools", url="/zfs/pools/"),
    Tab(id="zfs_datasets", label="Datasets", url="/zfs/datasets/"),
    Tab(id="zfs_snapshots", label="Snapshots", url="/zfs/snapshots/"),
    Tab(id="zfs_replication", label="Replication", url="/zfs/replication/"),
    Tab(id="zfs_observability", label="Observability", url="/zfs/observability/"),
    Tab(id="utils", label="Utilities", url="/utils/"),
    Tab(id="fleet", label="Fleet View", url="/fleet/"),
]

templates = Jinja2Templates(BASE_DIR / "templates")

# Force HTML autoescaping on all templates.
#
# Starlette 1.x changed its default Jinja environment from autoescape=True to
# jinja2.select_autoescape(), which only autoescapes files ending in
# .html/.htm/.xml/.xhtml. All of our templates use the .jinja extension, so
# under Starlette 1.x they render completely unescaped. This both breaks HTML
# attributes that contain quoted values (for example the confirm modal's
# onclick="{{ confirm_action }}") and is an XSS risk. Restore the previous
# always-escape behavior explicitly.
templates.env.autoescape = True

templates.env.globals["settings"] = settings
templates.env.globals["NAV_TABS"] = NAV_TABS
templates.env.globals["get_theme_css_path"] = get_theme_css_path
templates.env.globals["get_active_theme"] = get_active_theme
templates.env.globals["get_active_corner_style"] = get_active_corner_style
templates.env.globals["get_corner_styles_css_version"] = get_corner_styles_css_version
