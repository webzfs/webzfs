# Feature Development Demo - NTP Configuration

This directory contains a complete, working example of how to add a new feature to WebZFS. The example implements NTP (Network Time Protocol) configuration management that works on Linux, FreeBSD, and NetBSD.

## What's Included

This demo provides:

1. **Complete Working Example** - A fully functional NTP configuration feature
2. **Comprehensive Guide** - Step-by-step instructions in `ADDING_NEW_FEATURES.md`
3. **All Three Layers** - Service, View, and Template implementations
4. **Best Practices** - OS compatibility, error handling, security patterns

## Directory Structure

```
add_feature_demo/
├── README.md                           # This file - overview
├── ADDING_NEW_FEATURES.md             # Complete developer guide
├── services/
│   └── ntp.py                          # Service layer - business logic
├── views/
│   └── utils_ntp.py                    # View layer - HTTP routing
└── templates/
    └── utils/
        └── ntp/
            ├── index.jinja             # Main page template
            ├── config.jinja            # Configuration editor template
            └── status_partial.jinja    # HTMX partial template
```

## Quick Start

To implement this NTP feature in your application:

### 1. Copy Files to Production Locations

```bash
# From the project root directory

# Copy service
cp add_feature_demo/services/ntp.py services/

# Copy view
cp add_feature_demo/views/utils_ntp.py views/

# Copy templates
cp -r add_feature_demo/templates/utils/ntp templates/utils/
```

### 2. Register the Router

Edit `views/__init__.py`:

```python
# Add import
import views.utils_ntp

# Add router registration (in the Utilities Routes section)
router.include_router(views.utils_ntp.router)
```

### 3. Add Navigation (Optional)

Utilities links are rendered from the Utilities landing page
(`templates/utils/index.jinja`), not from a navigation list in
`config/templates.py`. Add a card to that page:

```jinja
<a href="/utils/ntp" class="card animate-fade-in hover:scale-[1.01] transition-transform duration-200 group">
    <div class="card-body py-4">
        <div class="flex items-center gap-4">
            <div class="flex-1 min-w-0">
                <h2 class="text-xl font-semibold text-text-primary group-hover:text-success-400 transition-colors">NTP Configuration</h2>
                <p class="text-text-secondary text-sm mt-0.5">View NTP status, edit the configuration file, and manage the ntpd service</p>
            </div>
        </div>
    </div>
</a>
```

The top-level navbar tabs are defined in `config/templates.py` as
`NAV_TABS`. A utility feature does not add a tab there.

### 4. Add Sudo Permissions (Linux Only)

The Linux installers copy `sudoers.d/webzfs.linux` to `/etc/sudoers.d/webzfs`.
Add the commands your feature needs to that file, not to a separate
sudoers file, so `install_linux.sh` keeps managing one policy. The shipped
policy already grants the commands this demo uses:

```
# System services and scheduled tasks use systemctl link/disable.
webzfs ALL=(ALL) NOPASSWD: /usr/bin/systemctl, /bin/systemctl
# Configuration editing and ZED script creation
webzfs ALL=(ALL) NOPASSWD: /usr/bin/cat, /usr/bin/tee, /usr/bin/mkdir
```

`services.utils.run_privileged_command` uses the `systemctl` grant, and
`services.file` uses the `cat` and `tee` grants to read and replace
root-owned files. No additional entry is required for the NTP feature.

If a feature needs a binary that is not already listed, add one line with
the full path and validate the policy before installing it:

```bash
sudo visudo -c -f sudoers.d/webzfs.linux
sudo install -m 0440 -o root -g root sudoers.d/webzfs.linux /etc/sudoers.d/webzfs
```

FreeBSD and NetBSD installers run the application as root, so no sudo
policy is required there. `services.utils.run_privileged_command` only
prepends `sudo` on Linux.

### 5. Restart Application

```bash
./run.sh restart
```

### 6. Access the Feature

Navigate to: `http://your-server:26619/utils/ntp`

## Learning Path

### For Quick Understanding

1. Read the architecture overview in `ADDING_NEW_FEATURES.md`
2. Review `services/ntp.py` to see OS-specific handling
3. Look at `views/utils_ntp.py` to understand routing
4. Examine templates to see UI patterns

### For Deep Understanding

Work through `ADDING_NEW_FEATURES.md` completely - it covers:

- Architecture and design patterns
- Step-by-step implementation
- Testing strategies
- Security best practices
- Common pitfalls and solutions

## Key Concepts

### 1. OS Compatibility (Linux, FreeBSD, NetBSD)

Use the shared platform helpers from `services.utils` instead of calling
`platform.system()` directly:

```python
from services.utils import is_freebsd, is_linux, is_netbsd

def _detect_os(self) -> str:
    """Detect the operating system using the shared platform helpers."""
    if is_linux():
        return "linux"
    if is_freebsd():
        return "freebsd"
    if is_netbsd():
        return "netbsd"
    return "unknown"
```

### 2. Service Pattern

```python
class NTPService:
    """Handles logic and system operations"""

    def get_status(self) -> Dict[str, Any]:
        """Get current state"""
        ...

    def update_config(self, config: str) -> bool:
        """Modify configuration"""
        ...
```

Service methods use the shared command and file helpers, so the same code
works on every platform:

```python
from services.file import read_file, save_file
from services.utils import needs_sudo_for_privileged, run_privileged_command

run_privileged_command(["systemctl", "restart", "ntpd"], check=False, timeout=10)
read_file("/etc/ntp.conf", use_sudo=needs_sudo_for_privileged())
save_file("/etc/ntp.conf", content, use_sudo=needs_sudo_for_privileged())
```

`run_privileged_command` prepends `sudo` only on Linux and only for
commands listed in `PRIVILEGED_COMMANDS`. `save_file` writes through a
secure temporary file and falls back to `sudo tee` for root-owned files.

### 3. FastAPI Routing

```python
from fastapi.responses import RedirectResponse
from starlette.responses import Response

@router.get("/")
async def index(request: Request) -> Response:
    """Display main page"""
    data = service.get_data()
    return templates.TemplateResponse(
        request,
        name="utils/your-feature/index.jinja",
        context={"data": data, "page_title": "Your Feature"},
    )

@router.post("/action")
async def action(
    request: Request,
    param: Annotated[str, Form()],
) -> RedirectResponse:
    """Handle form submission"""
    result = service.do_action(param)
    return RedirectResponse(url="/utils/your-feature?message=Success", status_code=303)
```

`TemplateResponse` takes `request` as the first positional argument, then
`name=` and `context=`. Annotate every handler return type.

### 4. HTMX Integration

```jinja
<button hx-get="/utils/ntp/status/refresh"
        hx-target="#status-container"
        hx-swap="innerHTML"
        class="btn-secondary px-4 py-2">
    Refresh Status
</button>
```

### 5. Design Tokens Instead of Dark Mode Classes

Templates no longer use `bg-white dark:bg-gray-800` pairs. The application
is dark-theme first and the theme CSS overrides semantic tokens, so use the
design tokens from `static/css/styles.css`:

```jinja
<div class="bg-bg-elevated text-text-primary border border-border-subtle">
    Content
</div>
```

Common tokens:

| Token | Use |
|-------|-----|
| `bg-bg-base` | Page background |
| `bg-bg-elevated`, `bg-bg-elevated-2`, `bg-bg-elevated-3` | Nested surfaces |
| `text-text-primary`, `text-text-secondary`, `text-text-tertiary` | Text hierarchy |
| `border-border-subtle`, `divide-border-subtle` | Borders and dividers |
| `text-success-400`, `text-danger-400`, `text-info-400`, `text-warning-400` | Status colors |
| `btn-primary`, `btn-secondary`, `btn-danger` | Buttons |
| `form-input`, `form-textarea`, `form-select` | Form controls |
| `card`, `card-body` | Cards |

### 6. Template Structure

Utility pages extend `layout_main.jinja`, which already renders the
breadcrumb, the page card, and the error container. Override `title`,
`caption`, and `inner`:

```jinja
{% extends "layout_main.jinja" %}

{% block title %}{{ page_title }}{% endblock %}

{% block caption %}{{ page_title }}{% endblock %}

{% block inner %}
    <p class="text-text-secondary">Page content</p>
{% endblock %}
```

Do not override `content` in a page that extends `layout_main.jinja`; that
replaces the whole layout, including the breadcrumb and card.

## File Descriptions

### services/ntp.py (Service Layer)

**Purpose**: Logic and system operations

**Key Features**:
- OS detection (Linux/FreeBSD/NetBSD)
- NTP service management (start/stop/restart/enable)
- Configuration file handling
- Time synchronization status
- Error handling and validation

**Methods**:
- `get_status()` - Check if NTP service is running
- `get_config()` - Read configuration file
- `get_servers()` - Extract NTP servers from config
- `update_config()` - Write new configuration
- `restart_service()` - Restart NTP daemon
- `enable_service()` - Enable service at boot
- `get_time_info()` - Get current time and sync status

### views/utils_ntp.py (View Layer)

**Purpose**: HTTP routing and request handling

**Routes**:
- `GET /utils/ntp` - Main overview page
- `GET /utils/ntp/config` - Configuration editor
- `POST /utils/ntp/config/save` - Save configuration
- `POST /utils/ntp/service/restart` - Restart service
- `POST /utils/ntp/service/enable` - Enable at boot
- `GET /utils/ntp/status/refresh` - HTMX status refresh

**Patterns**:
- Authentication required for all routes
- POST-Redirect-GET pattern for form submissions
- Exception handling with user-friendly errors
- Query parameters for success/error messages

### templates/utils/ntp/*.jinja (Template Layer)

**index.jinja** - Main overview page with:
- Service status card
- NTP servers list
- Action buttons
- HTMX dynamic updates

**config.jinja** - Configuration editor with:
- Textarea for config file
- Save/Cancel buttons
- Help section with common directives
- Confirmation dialogs

**status_partial.jinja** - HTMX partial for:
- Dynamic status updates
- Service information
- System time display
- Sync status details

## Customizing for Your Feature

To adapt this example for your own feature:

1. **Replace NTP-specific logic** with your feature's logic
2. **Update route paths** from `/utils/ntp` to `/utils/your-feature`
3. **Modify templates** to display your data
4. **Adjust service methods** for your use case
5. **Update sudo permissions** as needed

## Testing the Demo

Even without installing, you can learn by examining:

1. **Code flow**: Follow a request from view → service → system command
2. **Error handling**: See how exceptions are caught and displayed
3. **OS differences**: Compare Linux, FreeBSD, and NetBSD command execution
4. **Template patterns**: Study the UI components and styling
5. **Security**: Review input validation and sudo usage

## Common Questions

**Q: Do I need to implement all these features?**
A: No, start simple. You can have just one page with basic functionality.

**Q: Can I skip the service layer?**
A: Not recommended. The service layer makes code testable and reusable.

**Q: What if my feature only works on Linux?**
A: That's fine! Just add a check and display an error on the other platforms.

**Q: Should every feature have HTMX?**
A: No, only if you need dynamic updates without page refresh.

## Additional Resources

- **Full Guide**: Read `ADDING_NEW_FEATURES.md` for complete documentation
- **Existing Features**: Study `views/utils_smart.py` for a complex real-world example
- **Templates**: Look at `templates/zfs/` for advanced UI patterns
- **FastAPI Docs**: https://fastapi.tiangolo.com/
- **Jinja2 Docs**: https://jinja.palletsprojects.com/
- **HTMX Docs**: https://htmx.org/

## Need Help?

1. Check existing features for similar patterns
2. Read the comprehensive guide in `ADDING_NEW_FEATURES.md`
3. Review error messages carefully
4. Test individual components separately
5. Ask in project discussions

## Next Steps

1. **Study this example** - Understand each layer and how they connect
2. **Read the full guide** - Work through `ADDING_NEW_FEATURES.md`
3. **Start small** - Begin with a simple feature (single page, basic functionality)
4. **Iterate** - Add complexity as needed (multiple pages, HTMX, etc.)
5. **Test thoroughly** - Both OSes, dark mode, error cases
6. **Document** - Help others understand your feature

---

Happy feature development.
