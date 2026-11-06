# Adding New Features to WebZFS

This guide walks you through the complete process of adding a new feature to WebZFS. We'll use an NTP (Network Time Protocol) configuration feature as a real-world example.

## Table of Contents
1. [Overview](#overview)
2. [Architecture Layers](#architecture-layers)
3. [Step-by-Step Implementation](#step-by-step-implementation)
4. [Testing Your Feature](#testing-your-feature)
5. [Best Practices](#best-practices)

---

## Overview

WebZFS follows a clean architecture pattern with three main layers:

```
┌─────────────────────────────────────┐
│         Templates (Views)           │  ← User Interface (Jinja2)
├─────────────────────────────────────┤
│         Views (Routes)              │  ← FastAPI Routes & Request Handling
├─────────────────────────────────────┤
│         Services (Logic)            │  ← Core Functionality & OS Commands
└─────────────────────────────────────┘
```

**Key Principles:**
- **Service Layer**: Contains logic and system interactions
- **View Layer**: Handles HTTP requests/responses and routing
- **Template Layer**: Renders HTML for the user interface
- **Separation of Concerns**: Each layer has a specific responsibility

---

## Architecture Layers

### 1. Service Layer (`services/`)
- **Purpose**: Logic and system operations
- **Responsibilities**:
  - Execute system commands through `services.utils.run_privileged_command`
  - Read and write files through `services.file.read_file` and `save_file`
  - Parse command output
  - Handle OS-specific differences (Linux, FreeBSD, NetBSD)
  - Provide a clean API for the view layer
- **Example**: `services/ntp.py`

### 2. View Layer (`views/`)
- **Purpose**: HTTP routing and request handling
- **Responsibilities**:
  - Define API routes
  - Handle form submissions
  - Call service layer methods
  - Return templates or redirects
  - Handle authentication
- **Example**: `views/utils_ntp.py`

### 3. Template Layer (`templates/`)
- **Purpose**: User interface rendering
- **Responsibilities**:
  - Display data to users
  - Provide forms for input
  - Use HTMX for dynamic updates
  - Follow consistent styling with the theme design tokens
- **Example**: `templates/utils/ntp/*.jinja`

---

## Step-by-Step Implementation

### Step 1: Create the Service Layer

**Location**: `services/your_feature.py`

The service layer handles all logic and system interactions. Use the shared
helpers in `services.utils` and `services.file` instead of calling
`subprocess`, `platform`, or `sudo` directly. Those helpers already handle
Linux, FreeBSD, NetBSD, sudo detection, and secure file writes.

**Key Components**:
```python
from pathlib import Path
from typing import Any, Dict

from services.file import read_file, save_file
from services.utils import (
    is_freebsd,
    is_linux,
    is_netbsd,
    needs_sudo_for_privileged,
    run_privileged_command,
)

class YourFeatureService:
    """Service for managing your feature"""

    def __init__(self) -> None:
        """Initialize service, detect OS, set paths"""
        self.os_type = self._detect_os()
        self.config_path = self._get_config_path()

    def _detect_os(self) -> str:
        """Detect operating system using the shared platform helpers."""
        if is_linux():
            return "linux"
        if is_freebsd():
            return "freebsd"
        if is_netbsd():
            return "netbsd"
        return "unknown"

    def _get_config_path(self) -> Path:
        """Get configuration file path based on OS"""
        return Path("/etc/your-config")

    def get_status(self) -> Dict[str, Any]:
        """Get feature status"""
        ...

    def update_config(self, config: str) -> bool:
        """Update configuration"""
        ...
```

**Command and File Handling Example**:
```python
def restart_service(self) -> bool:
    """Restart the service on every supported platform."""
    if self.os_type == "linux":
        command = ["systemctl", "restart", "your-service"]
    elif self.os_type in ("freebsd", "netbsd"):
        command = ["service", "your-service", "restart"]
    else:
        return False

    try:
        result = run_privileged_command(command, check=False, timeout=10)
        return result.returncode == 0
    except Exception as restart_error:
        logger.error("Error restarting service: %s", restart_error)
        return False


def update_config(self, config_content: str) -> bool:
    """Write the configuration file through the shared file helper."""
    if not config_content.strip():
        raise ValueError("Configuration cannot be empty")

    try:
        save_file(
            str(self.config_path),
            config_content,
            use_sudo=needs_sudo_for_privileged(),
        )
        return True
    except Exception as write_error:
        logger.error("Error updating configuration: %s", write_error)
        return False
```

`run_privileged_command` prepends `sudo` only on Linux. `save_file` writes
through a secure temporary file and falls back to `sudo tee` for root-owned
files. `read_file` mirrors that behavior for reads.

**See**: `add_feature_demo/services/ntp.py` for a complete example.

---

### Step 2: Create the View Layer

**Location**: `views/utils_your_feature.py`

The view layer defines routes and handles HTTP requests.

**Template Structure**:
```python
from typing import Annotated

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import RedirectResponse
from starlette.responses import Response

from auth.dependencies import get_current_user
from config.templates import templates
from services.your_feature import YourFeatureService

# Create router with prefix
router = APIRouter(
    prefix="/utils/your-feature",  # URL prefix (use hyphens in URLs)
    tags=["your-feature"],  # API documentation tag
    dependencies=[Depends(get_current_user)],  # Require authentication
)

# Initialize service
your_service = YourFeatureService()


@router.get("/")
async def index(request: Request) -> Response:
    """Main page for your feature"""
    try:
        # Get data from service
        data = your_service.get_data()

        # Render template with data
        return templates.TemplateResponse(
            request,
            name="utils/your-feature/index.jinja",
            context={
                "data": data,
                "page_title": "Your Feature",
            },
        )
    except Exception as error:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "error": str(error),
                "title": "Your Feature Error",
            },
        )


@router.post("/action")
async def perform_action(
    request: Request,
    param: Annotated[str, Form()],
) -> RedirectResponse:
    """Handle form submission"""
    try:
        success = your_service.do_something(param)

        if success:
            return RedirectResponse(
                url="/utils/your-feature?message=Success",
                status_code=303,
            )
        return RedirectResponse(
            url="/utils/your-feature?error=Failed",
            status_code=303,
        )
    except Exception as error:
        return RedirectResponse(
            url=f"/utils/your-feature?error={error}",
            status_code=303,
        )
```

**Important Notes**:
- Always use `status_code=303` for POST-redirect-GET pattern
- Use query parameters for messages: `?message=` or `?error=`
- Pass `request` first to `TemplateResponse`, then `name=` and `context=`
- Annotate every handler return type with `Response` or `RedirectResponse`
- Pass `title=` to `partials/error.jinja`; the template no longer uses `back_url`
- Handle exceptions gracefully

**See**: `add_feature_demo/views/utils_ntp.py` for a complete example.

---

### Step 3: Create Templates

**Location**: `templates/utils/your-feature/*.jinja`

Create at least one template for your feature's main page.

**Directory Structure**:
```
templates/
└── utils/
    └── your-feature/
        ├── index.jinja          # Main page
        ├── config.jinja         # Configuration page (if needed)
        └── partial.jinja        # HTMX partial (if needed)
```

**Basic Template Structure**:
```jinja
{% extends "layout_main.jinja" %}

{% block title %}{{ page_title }}{% endblock %}

{% block caption %}{{ page_title }}{% endblock %}

{% block inner %}
    <!-- Display Messages -->
    {% if request.query_params.get('message') %}
    <div class="mb-4 p-4 bg-success-900/30 border border-success-600/40 text-success-400 rounded-lg">
        {{ request.query_params.get('message') }}
    </div>
    {% endif %}

    {% if request.query_params.get('error') %}
    <div class="mb-4 p-4 bg-danger-900/30 border border-danger-600/40 text-danger-400 rounded-lg">
        {{ request.query_params.get('error') }}
    </div>
    {% endif %}

    <!-- Content Card -->
    <div class="bg-bg-elevated rounded-lg border border-border-subtle p-6 mb-6">
        <h2 class="text-xl font-semibold text-text-primary mb-4">
            Section Title
        </h2>

        <p class="text-text-secondary">
            {{ your_data }}
        </p>
    </div>

    <!-- Actions Card -->
    <div class="bg-bg-elevated rounded-lg border border-border-subtle p-6">
        <h2 class="text-xl font-semibold text-text-primary mb-4">
            Actions
        </h2>

        <form method="POST" action="/utils/your-feature/action">
            <button type="submit" class="btn-primary px-4 py-2">
                Perform Action
            </button>
        </form>
    </div>
{% endblock %}
```

`layout_main.jinja` already renders the breadcrumb, the page card, and the
error container. Override `title`, `caption`, and `inner`. Do not override
`content`; that replaces the whole layout, including the breadcrumb and card.

**Styling Guidelines**:
- Use the design tokens from `static/css/styles.css`, not raw palette classes
- Surfaces: `bg-bg-elevated`, `bg-bg-elevated-2`, `bg-bg-elevated-3`
- Text: `text-text-primary`, `text-text-secondary`, `text-text-tertiary`
- Borders: `border-border-subtle`, `divide-border-subtle`
- Status colors: `text-success-400`, `text-danger-400`, `text-info-400`, `text-warning-400`
- Buttons: `btn-primary`, `btn-secondary`, `btn-danger`
- Form controls: `form-input`, `form-textarea`, `form-select`
- Use consistent spacing (`mb-4`, `mb-6`, `p-6`)
- Do not add `dark:` variants; the application is dark-theme first and the
  theme CSS already overrides the semantic tokens

**HTMX Integration** (for dynamic updates):
```jinja
<!-- Button that triggers HTMX request -->
<button hx-get="/utils/your-feature/refresh"
        hx-target="#status-container"
        hx-swap="innerHTML"
        class="btn-secondary px-4 py-2">
    Refresh Status
</button>

<!-- Container that will be updated -->
<div id="status-container">
    {% include "utils/your-feature/status_partial.jinja" %}
</div>
```

**See**: `add_feature_demo/templates/utils/ntp/*.jinja` for complete examples.

---

### Step 4: Register the Router

**Location**: `views/__init__.py`

Add your view module to the main router configuration.

**Steps**:

1. **Import your view module**:
```python
import views.utils_your_feature
```

2. **Register the router** (add after existing routers):
```python
# Your Feature Routes
router.include_router(views.utils_your_feature.router)
```

**Complete Example**:
```python
from fastapi import APIRouter

import views.auth
import views.dashboard
import views.utils_your_feature  # ← Add this import (use underscores in Python)
# ... other imports ...

router = APIRouter()

# ... existing routers ...

# Your Feature Routes
router.include_router(views.utils_your_feature.router)  # ← Add this line

# Authentication and Dashboard
router.include_router(views.auth.router, prefix="/login")
router.include_router(views.dashboard.router)
```

**Important**: The order of router registration matters! Routes are matched in order, so more specific routes should come before more general ones.

---

### Step 5: Add Navigation (Optional)

**Location**: `templates/utils/index.jinja`

The Utilities landing page lists every utility as a card. To make your
feature reachable, add a card to the grid in `templates/utils/index.jinja`.

**Note**: `config/templates.py` holds `NAV_TABS`, which defines the
top-level navbar tabs (Dashboard, ZFS, Fleet, Utilities). It does not hold a
per-utility list. Utilities are surfaced as cards, not as navbar dropdown
entries.

**Add a card** alongside the existing entries:
```jinja
<a href="/utils/your-feature"
   class="card group hover:border-success-500/50 transition-colors">
    <div class="card-body">
        <div class="flex items-center justify-between mb-2">
            <h2 class="text-xl font-semibold text-text-primary group-hover:text-success-400 transition-colors">
                Your Feature
            </h2>
            <span class="text-text-tertiary group-hover:text-success-400 transition-colors">
                <svg class="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2"
                          d="M9 5l7 7-7 7"></path>
                </svg>
            </span>
        </div>
        <p class="text-sm text-text-secondary">
            Short description of what your feature does.
        </p>
    </div>
</a>
```

**Navigation Structure**:
- **Top-level navbar tabs**: defined in `NAV_TABS` in `config/templates.py`
- **Utilities cards**: defined in `templates/utils/index.jinja`
- **URL**: Should match your router prefix (e.g., `/utils/your-feature`)

---

### Step 6: Add Sudo Permissions (If Needed)

**Location**: `sudoers.d/webzfs.linux` in the repository, installed as
`/etc/sudoers.d/webzfs` on Linux.

WebZFS ships a single installer-managed sudoers policy. Do not add a
separate policy file per feature. Add your commands to the existing policy
and reinstall or update the file on the target host.

**The shipped policy already covers this demo.** It grants `systemctl`,
`cat`, and `tee`, which is everything `run_privileged_command` and
`save_file` need for NTP:

```
# System services and scheduled tasks use systemctl link/disable.
webzfs ALL=(ALL) NOPASSWD: /usr/bin/systemctl, /bin/systemctl
# Configuration editing and ZED script creation
webzfs ALL=(ALL) NOPASSWD: /usr/bin/cat, /usr/bin/tee, /usr/bin/mkdir
```

**When your feature needs a new binary**, add one line with the full path
and list every path the binary may live at on supported distributions:

```
# Example: allow webzfs to run a new helper without a password
webzfs ALL=(ALL) NOPASSWD: /usr/bin/your-tool, /bin/your-tool
```

**Note**: Replace `webzfs` with a different username if your deployment uses
a different user. Always validate the policy with `visudo -c` after editing;
a malformed sudoers file can lock you out of privilege escalation.

**FreeBSD and NetBSD**: The installers run WebZFS as root, and
`run_privileged_command` does not prepend `sudo` on those platforms, so no
sudoers entry is required there.

**Security Best Practices**:
- Only allow specific commands (avoid wildcards)
- Specify full paths to executables
- Test with the actual user account
- Document why each permission is needed

---

## Testing Your Feature

### 1. Unit Testing

Create tests in `tests/test_your_feature.py`:

```python
import pytest
from services.your_feature import YourFeatureService

def test_get_status():
    """Test status retrieval"""
    service = YourFeatureService()
    status = service.get_status()

    assert 'running' in status
    assert isinstance(status['running'], bool)

def test_os_detection():
    """Test OS detection"""
    service = YourFeatureService()
    assert service.os_type in ['linux', 'freebsd', 'netbsd', 'unknown']
```

Run tests:
```bash
pytest tests/test_your_feature.py -v
```

### 2. Manual Testing

1. **Start the development server**:
   ```bash
   ./run_dev.sh
   ```

2. **Access your feature**:
   - Navigate to `http://localhost:26619/utils/your-feature`
   - Test all buttons and forms
   - Verify error handling
   - Check that the page renders with the theme tokens

3. **Test on all OS types** (if applicable):
   - Linux system
   - FreeBSD system
   - NetBSD system

### 3. Integration Testing

Verify your feature works with the rest of the application:
- Authentication works correctly
- Navigation menu displays properly
- HTMX updates work (if used)
- Error pages display correctly
- Messages display after actions

---

## Best Practices

### 1. Error Handling

**Always handle exceptions**:
```python
try:
    result = your_service.do_something()
    return success_response()
except Exception as error:
    return error_response(str(error))
```

**Provide helpful error messages**:
```python
if not config_path.exists():
    raise FileNotFoundError(
        f"Configuration file not found: {config_path}. "
        f"Please ensure the service is installed."
    )
```

### 2. OS Compatibility

**Detect OS with the shared platform helpers**:
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

**Use OS-specific logic**:
```python
if self.os_type == "freebsd":
    # FreeBSD-specific code
elif self.os_type == "netbsd":
    # NetBSD-specific code
else:
    # Linux code
    pass
```

**Why use the shared helpers?** `platform.system()` returns different
strings across distributions and platforms, and each feature used to
reimplement the same checks. `services.utils` normalizes that detection in
one place, so behavior stays consistent across the application.

### 3. Security

**Validate all user input**:
```python
def update_config(self, config: str) -> bool:
    # Validate input
    if not config or len(config) > 100000:
        raise ValueError("Invalid configuration")

    # Delegate the write to the shared helper, which writes through a
    # secure temporary file and handles root-owned paths
    save_file(
        str(self.config_path),
        config,
        use_sudo=needs_sudo_for_privileged(),
    )
    return True
```

**Use the shared command helper**:
```python
# Good - argument list, sudo handling, and timeout are centralized
run_privileged_command(["systemctl", "restart", service_name], timeout=10)

# Bad - bypasses the helper and requires sudo on every platform
subprocess.run(f"sudo systemctl restart {service_name}", shell=True)
```

`run_privileged_command` accepts an argument list only. It never builds a
shell string, so untrusted values cannot inject shell syntax.

### 4. Code Organization

**Keep functions focused**:
```python
# Good - single responsibility
def get_status(self) -> Dict:
    """Get service status only"""
    pass

def get_config(self) -> str:
    """Get configuration only"""
    pass

# Bad - doing too much
def get_everything(self) -> Dict:
    """Get status, config, and logs"""
    pass
```

**Use type hints**:
```python
def get_servers(self) -> List[str]:
    """Return list of server addresses"""
    pass

def update_config(self, config: str) -> bool:
    """Update configuration, return success status"""
    pass
```

### 5. Documentation

**Document all public methods**:
```python
def restart_service(self) -> bool:
    """
    Restart the service.

    Returns:
        bool: True if service restarted successfully, False otherwise

    Raises:
        TimeoutError: If service takes too long to restart
    """
    pass
```

**Add inline comments for complex logic**:
```python
# Use standard NTP configuration path
config_path = Path('/etc/ntp.conf')
if not config_path.exists():
    raise FileNotFoundError(f"NTP configuration not found: {config_path}")
return config_path
```

### 6. User Experience

**Provide feedback**:
- Show success messages after actions
- Display clear error messages
- Use loading indicators for long operations
- Confirm destructive actions

**Make navigation intuitive**:
- Breadcrumbs for nested pages
- Back buttons to return to previous page
- Clear action buttons with descriptive labels

**Support the theme with design tokens**:
- Use semantic tokens (`bg-bg-elevated`, `text-text-primary`, `border-border-subtle`)
- Do not add `dark:` variants; the theme CSS already handles light and dark


---

## Quick Reference Checklist

When adding a new feature, complete these steps:

- [ ] Create service layer (`services/your_feature.py` - use underscores)
  - [ ] Use the shared helpers from `services.utils` and `services.file`
  - [ ] Implement OS detection with `is_linux`, `is_freebsd`, `is_netbsd`
  - [ ] Add core functionality
  - [ ] Handle errors gracefully
  - [ ] Test on Linux, FreeBSD, and NetBSD (if applicable)

- [ ] Create view layer (`views/utils_your_feature.py` - use underscores)
  - [ ] Define router with prefix (`/utils/your-feature` - use hyphens in URLs)
  - [ ] Create GET routes for pages
  - [ ] Create POST routes for actions
  - [ ] Add authentication dependency
  - [ ] Annotate handler return types
  - [ ] Handle errors and redirects

- [ ] Create templates (`templates/utils/your-feature/` - use hyphens in paths)
  - [ ] Extend `layout_main.jinja` and override `title`, `caption`, `inner`
  - [ ] Create main page (`index.jinja`)
  - [ ] Add additional pages as needed
  - [ ] Use the theme design tokens
  - [ ] Add HTMX if needed

- [ ] Register router (`views/__init__.py`)
  - [ ] Import view module
  - [ ] Add router to main router

- [ ] Update navigation (`templates/utils/index.jinja`)
  - [ ] Add a Utilities card
  - [ ] Set correct URL

- [ ] Add sudo permissions (if needed)
  - [ ] Add commands to `sudoers.d/webzfs.linux`
  - [ ] Validate with `visudo -c`
  - [ ] Test with application user

- [ ] Test thoroughly
  - [ ] Unit tests
  - [ ] Manual testing
  - [ ] Linux, FreeBSD, and NetBSD
  - [ ] Theme consistency
  - [ ] Error cases

---

## Example Files

All example files for the NTP configuration feature are located in:

```
add_feature_demo/
├── README.md                           # Overview and quick start
├── services/
│   └── ntp.py                          # Service layer
├── views/
│   └── utils_ntp.py                    # View layer
├── templates/
│   └── utils/
│       └── ntp/
│           ├── index.jinja             # Main page
│           ├── config.jinja            # Configuration editor
│           └── status_partial.jinja    # HTMX partial
└── ADDING_NEW_FEATURES.md             # This guide
```

---

## Getting Help

If you encounter issues:

1. Check existing features for similar patterns
2. Review error messages carefully
3. Test components individually
4. Ask for help in project discussions
