from dataclasses import dataclass
from fastapi.templating import Jinja2Templates
from config.settings import BASE_DIR, settings
from services.theme import get_active_theme, get_theme_css_path

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
    Tab(id="zfs_performance", label="Performance", url="/zfs/performance/"),
    Tab(id="utils", label="Utilities", url="/utils/"),
    Tab(id="fleet", label="Fleet View", url="/fleet/"),
]

templates = Jinja2Templates(BASE_DIR / "templates")
templates.env.globals["settings"] = settings
templates.env.globals["NAV_TABS"] = NAV_TABS
templates.env.globals["get_active_theme"] = get_active_theme
templates.env.globals["get_theme_css_path"] = get_theme_css_path
