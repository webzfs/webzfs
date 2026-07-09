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
  - Execute system commands
  - Parse command output
  - Handle OS-specific differences (Linux vs FreeBSD)
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
  - Follow consistent styling (Tailwind CSS)
- **Example**: `templates/utils/ntp/*.jinja`

---

## Step-by-Step Implementation

### Step 1: Create the Service Layer

**Location**: `services/your_feature.py`

The service layer handles all logic and system interactions.

**Key Components**:
```python
import subprocess
import platform
from pathlib import Path
from typing import Dict, List

class YourFeatureService:
    """Service for managing your feature"""
    
    def __init__(self):
        """Initialize service, detect OS, set paths"""
        self.os_type = self._detect_os()
        self.config_path = self._get_config_path()
    
    def _detect_os(self) -> str:
        """Detect operating system (Linux or FreeBSD)"""
        system = platform.system().lower()
        if 'freebsd' in system:
            return 'freebsd'
        else:
            # Default to Linux (handles all Linux distributions)
            return 'linux'
    
    def _get_config_path(self) -> Path:
        """Get configuration file path based on OS"""
        if self.os_type == 'linux':
            return Path('/etc/your-config')
        elif self.os_type == 'freebsd':
            return Path('/usr/local/etc/your-config')
        return Path('/etc/your-config')
    
    def get_status(self) -> Dict:
        """Get feature status"""
        # Implementation here
        pass
    
    def update_config(self, config: str) -> bool:
        """Update configuration"""
        # Implementation here
        pass
```

**OS-Specific Handling Example**:
```python
def restart_service(self) -> bool:
    """Restart service - handles both Linux and FreeBSD"""
    try:
        if self.os_type == 'linux':
            result = subprocess.run(
                ['sudo', 'systemctl', 'restart', 'your-service'],
                capture_output=True,
                timeout=10
            )
        elif self.os_type == 'freebsd':
            result = subprocess.run(
                ['sudo', 'service', 'your-service', 'restart'],
                capture_output=True,
                timeout=10
            )
        else:
            return False
        
        return result.returncode == 0
    except Exception as e:
        print(f"Error: {e}")
        return False
```

**See**: `add_feature_demo/services/ntp.py` for a complete example.

---

### Step 2: Create the View Layer

**Location**: `views/utils_your_feature.py`

The view layer defines routes and handles HTTP requests.

**Template Structure**:
```python
from fastapi import APIRouter, Request, Form, Depends
from fastapi.responses import RedirectResponse
from typing import Annotated
from config.templates import templates
from auth.dependencies import get_current_user
from services.your_feature import YourFeatureService

# Create router with prefix
router = APIRouter(
    prefix="/utils/your-feature",  # URL prefix (use hyphens in URLs)
    tags=["your-feature"],          # API documentation tag
    dependencies=[Depends(get_current_user)]  # Require authentication
)

# Initialize service
your_service = YourFeatureService()


@router.get("/")
async def index(request: Request):
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
                "page_title": "Your Feature"
            }
        )
    except Exception as e:
        return templates.TemplateResponse(
            request,
            name="partials/error.jinja",
            context={
                "error": str(e),
                "back_url": "/utils"
            }
        )


@router.post("/action")
async def perform_action(
    request: Request,
    param: Annotated[str, Form()]
):
    """Handle form submission"""
    try:
        success = your_service.do_something(param)
        
        if success:
            return RedirectResponse(
                url="/utils/your-feature?message=Success",
                status_code=303
            )
        else:
            return RedirectResponse(
                url="/utils/your-feature?error=Failed",
                status_code=303
            )
    except Exception as e:
        return RedirectResponse(
            url=f"/utils/your-feature?error={str(e)}",
            status_code=303
        )
```

**Important Notes**:
- Always use `status_code=303` for POST-redirect-GET pattern
- Use query parameters for messages: `?message=` or `?error=`
- Handle exceptions gracefully
- Return appropriate templates or redirects

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

{% block content %}
<div class="container mx-auto px-4 py-6">
    <!-- Page Header -->
    <div class="mb-6">
        <h1 class="text-3xl font-bold text-gray-900 dark:text-white">
            Your Feature Title
        </h1>
        <p class="text-gray-600 dark:text-gray-400 mt-2">
            Feature description
        </p>
    </div>

    <!-- Display Messages -->
    {% if request.query_params.get('message') %}
    <div class="mb-4 p-4 bg-green-100 border border-green-400 text-green-700 rounded">
        {{ request.query_params.get('message') }}
    </div>
    {% endif %}
    
    {% if request.query_params.get('error') %}
    <div class="mb-4 p-4 bg-red-100 border border-red-400 text-red-700 rounded">
        {{ request.query_params.get('error') }}
    </div>
    {% endif %}

    <!-- Content Card -->
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6">
        <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-4">
            Section Title
        </h2>
        
        <!-- Your content here -->
        <p class="text-gray-600 dark:text-gray-400">
            {{ your_data }}
        </p>
    </div>

    <!-- Actions Card -->
    <div class="bg-white dark:bg-gray-800 rounded-lg shadow-md p-6 mt-6">
        <h2 class="text-xl font-bold text-gray-900 dark:text-white mb-4">
            Actions
        </h2>
        
        <div class="flex flex-wrap gap-4">
            <form method="POST" action="/utils/your-feature/action">
                <button type="submit" 
                        class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors">
                    Perform Action
                </button>
            </form>
        </div>
    </div>
</div>
{% endblock %}
```

**Styling Guidelines**:
- Use Tailwind CSS classes
- Support dark mode (`dark:` prefix)
- Use consistent spacing (mb-4, mb-6, p-6, etc.)
- Use rounded corners (rounded-lg)
- Use shadows (shadow-md)
- Make buttons clear and actionable

**HTMX Integration** (for dynamic updates):
```jinja
<!-- Button that triggers HTMX request -->
<button hx-get="/utils/your-feature/refresh" 
        hx-target="#status-container"
        hx-swap="innerHTML"
        class="px-4 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700">
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

**Location**: `config/templates.py`

To add your feature to the **Utilities dropdown menu** in the navigation bar, modify the `ENABLED_TABS` configuration.

**Important**: This adds a sub-item under the existing "Utilities" tab, NOT a new top-level tab in the navbar.

**Find the utilities section** in `ENABLED_TABS`:
```python
{
    "name": "utilities",
    "label": "Utilities",
    "url": "/utils",
    "subitems": [
        {"label": "Shell", "url": "/utils/shell"},
        {"label": "Text Editor", "url": "/utils/text"},
        {"label": "Files", "url": "/utils/files"},
        {"label": "Logs", "url": "/utils/logs"},
        {"label": "SMART", "url": "/utils/smart"},
        {"label": "NTP Config", "url": "/utils/ntp"},  # ← Add your feature here
    ]
}
```

**Navigation Structure**:
- **Top-level navbar tabs**: ZFS, Fleet, Utilities (these already exist)
- **Utilities sub-items**: Appear in dropdown menu when you click "Utilities"
- **Your feature**: Will appear as a menu item in the Utilities dropdown
- **URL**: Should match your router prefix (e.g., `/utils/your-feature`)

**Result**: When users click the "Utilities" tab in the navbar, they will see your feature listed in the dropdown menu.

---

### Step 6: Add Sudo Permissions (If Needed)

**Location**: `/etc/sudoers` or `/etc/sudoers.d/webzfs`

If your feature needs to execute privileged commands, add sudo permissions.

**Example Entry** (using the recommended `webzfs` user):
```
# Allow webzfs user to manage NTP without password
webzfs ALL=(ALL) NOPASSWD: /bin/systemctl restart ntpd
webzfs ALL=(ALL) NOPASSWD: /bin/systemctl enable ntpd
webzfs ALL=(ALL) NOPASSWD: /bin/systemctl status ntpd
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/service ntpd restart
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/service ntpd status
webzfs ALL=(ALL) NOPASSWD: /usr/sbin/sysrc ntpd_enable=YES
webzfs ALL=(ALL) NOPASSWD: /bin/cp /tmp/ntp.conf.tmp /etc/ntp.conf
```

**Note**: Replace `webzfs` with a different username if your deployment uses a different user.

**Security Best Practices**:
- Only allow specific commands (avoid wildcards)
- Specify full paths to executables
- Test with the actual user account
- Document why each permission is needed

---

## Testing Your Feature

### 1. Unit Testing

Create tests in `tests/services/test_your_feature.py`:

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
    assert service.os_type in ['linux', 'freebsd', 'unknown']
```

Run tests:
```bash
pytest tests/services/test_your_feature.py -v
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
   - Check dark mode compatibility

3. **Test on both OS types** (if applicable):
   - Linux system
   - FreeBSD system

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
except Exception as e:
    return error_response(str(e))
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

**Detect OS early (default to Linux, check for FreeBSD)**:
```python
def _detect_os(self) -> str:
    """Detect operating system"""
    system = platform.system().lower()
    if 'freebsd' in system:
        return 'freebsd'
    else:
        # Default to Linux - handles all Linux distributions
        return 'linux'
```

**Use OS-specific logic**:
```python
if self.os_type == 'freebsd':
    # FreeBSD-specific code
else:
    # Linux code (default for all other systems)
    pass
```

**Why default to Linux?** Different Linux distributions may report slightly different strings (e.g., "Linux", "GNU/Linux"), so it's more robust to check explicitly for FreeBSD and default to Linux for everything else.

### 3. Security

**Validate all user input**:
```python
def update_config(self, config: str) -> bool:
    # Validate input
    if not config or len(config) > 100000:
        raise ValueError("Invalid configuration")
    
    # Write to temp file first
    temp_path = Path('/tmp/config.tmp')
    # ... rest of implementation
```

**Use subprocess safely**:
```python
# Good - command as list
subprocess.run(['sudo', 'systemctl', 'restart', service_name], ...)

# Bad - shell=True is dangerous
subprocess.run(f'sudo systemctl restart {service_name}', shell=True)
```

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

**Support dark mode natively**:
- Use `dark:` prefix for dark mode classes


---

## Quick Reference Checklist

When adding a new feature, complete these steps:

- [ ] Create service layer (`services/your_feature.py` - use underscores)
  - [ ] Implement OS detection
  - [ ] Add core functionality
  - [ ] Handle errors gracefully
  - [ ] Test on both Linux and FreeBSD (if applicable)

- [ ] Create view layer (`views/utils_your_feature.py` - use underscores)
  - [ ] Define router with prefix (`/utils/your-feature` - use hyphens in URLs)
  - [ ] Create GET routes for pages
  - [ ] Create POST routes for actions
  - [ ] Add authentication dependency
  - [ ] Handle errors and redirects

- [ ] Create templates (`templates/utils/your-feature/` - use hyphens in paths)
  - [ ] Create main page (`index.jinja`)
  - [ ] Add additional pages as needed
  - [ ] Support dark mode
  - [ ] Add HTMX if needed

- [ ] Register router (`views/__init__.py`)
  - [ ] Import view module
  - [ ] Add router to main router

- [ ] Update navigation (`config/templates.py`)
  - [ ] Add menu item
  - [ ] Set correct URL

- [ ] Add sudo permissions (if needed)
  - [ ] Create sudoers entry
  - [ ] Test with application user

- [ ] Test thoroughly
  - [ ] Unit tests
  - [ ] Manual testing
  - [ ] Both OS types
  - [ ] Dark mode
  - [ ] Error cases

---

## Example Files

All example files for the NTP configuration feature are located in:

```
add_feature_demo/
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
