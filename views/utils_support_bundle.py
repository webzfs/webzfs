"""
Support Bundle Views
Web interface for generating a support-info zip bundle containing
selected diagnostic data for troubleshooting assistance.
"""
import platform
from datetime import datetime
from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, StreamingResponse
from typing import List, Optional

from config.templates import templates
from services.support_bundle import SupportBundleService
from auth.dependencies import get_current_user


router = APIRouter(
    tags=["support-bundle"],
    dependencies=[Depends(get_current_user)],
)
bundle_service = SupportBundleService()


@router.get("/", response_class=HTMLResponse)
async def support_bundle_index(request: Request):
    """Render the support bundle page with checkbox list of data items."""
    data_items = bundle_service.get_data_items()

    # Group items by category for display
    categories = {}
    for item in data_items:
        cat = item["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(item)

    return templates.TemplateResponse(
        "utils/support_bundle/index.jinja",
        {
            "request": request,
            "categories": categories,
            "page_title": "Support Bundle",
        },
    )


@router.post("/generate")
async def generate_support_bundle(request: Request):
    """
    Receive selected items via form POST, collect the data, and stream
    back a zip file for download.
    """
    form_data = await request.form()

    # Collect all checked keys from the form
    selected_keys = [
        key for key in form_data.keys() if key.startswith("item_")
    ]
    # Strip the "item_" prefix to get the actual data item keys
    selected_keys = [key[5:] for key in selected_keys]

    if not selected_keys:
        # Nothing selected -- redirect back with error
        return templates.TemplateResponse(
            "utils/support_bundle/index.jinja",
            {
                "request": request,
                "categories": _grouped_items(),
                "page_title": "Support Bundle",
                "error": "No items were selected. Please select at least one item.",
            },
        )

    # Generate the zip bundle
    zip_buffer = bundle_service.generate_bundle(selected_keys)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    hostname = platform.node() or "unknown"
    filename = f"webzfs-support-{hostname}-{timestamp}.zip"

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "HX-Redirect": "",  # Prevent HTMX from intercepting
        },
    )


def _grouped_items():
    """Helper to return data items grouped by category."""
    items = bundle_service.get_data_items()
    categories = {}
    for item in items:
        cat = item["category"]
        if cat not in categories:
            categories[cat] = []
        categories[cat].append(item)
    return categories
