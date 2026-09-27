"""
Unified Scheduling Hub Views
One page that lists and manages every WebZFS scheduled task.

Four task domains are presented together:
    scrub    pool scrubs
    smart    SMART self-tests
    health   health analysis reports
    syncoid  Syncoid replication jobs (read-only summary here; the full
             editor stays on the Replication pages)

All create/update/delete actions write to the domain schedule store and
then register or unregister the OS scheduler entry through
services.job_scheduler.TaskScheduler, so the stored state and the
systemd timers or crontab block never diverge.
"""
import threading
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Body, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from auth.dependencies import get_current_user
from config.templates import templates
from services.job_scheduler import TaskScheduler, TASK_TYPE_LABELS
from services.schedule_utils import (
    get_schedule_presets,
    preview_next_runs,
    validate_cron_expression,
)
from services.smart_monitoring import ALL_DISKS, SMARTMonitoringService
from services.storage import FileStorageService
from services.zfs_pool import ZFSPoolService

router = APIRouter(tags=["scheduling"], dependencies=[Depends(get_current_user)])

storage_service = FileStorageService()
smart_service = SMARTMonitoringService()
pool_service = ZFSPoolService()
task_scheduler = TaskScheduler()


def _next_run_text(schedule: str) -> str:
    """First upcoming occurrence for display, or an empty string."""
    try:
        upcoming = preview_next_runs(schedule, count=1)
        return upcoming[0] if upcoming else ""
    except Exception:
        return ""


def _collect_tasks() -> List[Dict[str, Any]]:
    """Build the unified task list shown on the hub page.

    Each entry uses one shape regardless of domain so the template can
    render a single table: task_type, task_id, title, detail, schedule,
    enabled, last_run, last_status, next_run, and edit/run URLs.
    """
    tasks: List[Dict[str, Any]] = []

    try:
        for record in storage_service.get_scrub_schedules():
            tasks.append({
                "task_type": "scrub",
                "task_id": record["id"],
                "title": f"Scrub {record.get('pool', 'unknown')}",
                "detail": f"Pool {record.get('pool', 'unknown')}",
                "schedule": record.get("schedule", ""),
                "enabled": record.get("enabled", True),
                "last_run": record.get("last_run"),
                "last_status": record.get("last_status"),
                "next_run": record.get("next_run") or _next_run_text(record.get("schedule", "")),
                "manageable": True,
            })
    except Exception:
        pass

    try:
        for record in smart_service.list_scheduled_tests():
            test_type = record.get("test_type", "short")
            disk = record.get("disk", "unknown")
            if disk == ALL_DISKS:
                # One row for the whole fleet rather than one row per disk.
                detail = "All disks present at run time"
            else:
                detail = f"Disk {disk}"
            tasks.append({
                "task_type": "smart",
                "task_id": record["id"],
                "title": f"SMART {test_type} test",
                "detail": detail,
                "schedule": record.get("schedule", ""),
                "enabled": record.get("enabled", True),
                "last_run": record.get("last_run"),
                "last_status": record.get("last_status"),
                "next_run": record.get("next_run") or _next_run_text(record.get("schedule", "")),
                "manageable": True,
            })
    except Exception:
        pass

    try:
        for record in storage_service.get_health_schedules():
            tasks.append({
                "task_type": "health",
                "task_id": record["id"],
                "title": record.get("name") or f"Health check {record['id']}",
                "detail": "Full health analysis report",
                "schedule": record.get("schedule", ""),
                "enabled": record.get("enabled", True),
                "last_run": record.get("last_run"),
                "last_status": record.get("last_status"),
                "next_run": record.get("next_run") or _next_run_text(record.get("schedule", "")),
                "manageable": True,
            })
    except Exception:
        pass

    try:
        for record in storage_service.get_syncoid_jobs():
            tasks.append({
                "task_type": "syncoid",
                "task_id": record["id"],
                "title": record.get("name") or f"Syncoid job {record['id']}",
                "detail": (
                    f"{record.get('source_dataset', '?')} to "
                    f"{record.get('target_dataset', '?')}"
                ),
                "schedule": record.get("schedule", ""),
                "enabled": record.get("enabled", True),
                "last_run": record.get("last_run"),
                "last_status": record.get("last_status"),
                "next_run": record.get("next_run") or _next_run_text(record.get("schedule", "")),
                # Syncoid jobs keep their dedicated editor because they
                # carry connection and transport options the hub form
                # does not model.
                "manageable": False,
                "edit_url": f"/zfs/replication/syncoid/jobs/{record['id']}/edit",
            })
    except Exception:
        pass

    for task in tasks:
        if task.get("enabled"):
            task["registration_error"] = task_scheduler.get_registration_error(
                task["task_type"], task["task_id"]
            )

    return tasks


@router.get("/", response_class=HTMLResponse)
async def scheduling_index(request: Request):
    """Render the scheduling hub shell. The task table loads via HTMX."""
    return templates.TemplateResponse(
        request,
        name="utils/scheduling/index.jinja",
        context={"page_title": "Scheduling"},
    )


@router.get("/content-partial", response_class=HTMLResponse)
async def scheduling_content_partial(request: Request):
    """HTMX partial that renders the unified task table."""
    tasks = _collect_tasks()

    counts = {"scrub": 0, "smart": 0, "health": 0, "syncoid": 0}
    enabled_count = 0
    for task in tasks:
        counts[task["task_type"]] = counts.get(task["task_type"], 0) + 1
        if task.get("enabled"):
            enabled_count += 1

    return templates.TemplateResponse(
        request,
        name="utils/scheduling/content_partial.jinja",
        context={
            "tasks": tasks,
            "counts": counts,
            "enabled_count": enabled_count,
            "type_labels": TASK_TYPE_LABELS,
        },
    )


# Each task type has its own form page. A single combined form was tried
# first and rejected: the fields for a pool scrub, a SMART self-test, and
# a health report have almost nothing in common, so one page per type is
# clearer to use and simpler to read.
FORM_TEMPLATES = {
    "scrub": "utils/scheduling/scrub_form.jinja",
    "smart": "utils/scheduling/smart_form.jinja",
    "health": "utils/scheduling/health_form.jinja",
}


def _pool_names() -> List[str]:
    """Pool names for the scrub form dropdown."""
    try:
        return [pool["name"] for pool in pool_service.list_pools()]
    except Exception:
        return []


def _disk_paths() -> List[str]:
    """Disk device paths for the SMART form dropdown."""
    try:
        return [disk["path"] for disk in smart_service.list_disks()]
    except Exception:
        return []


@router.get("/create/form")
async def scheduling_create_form_redirect(request: Request, task_type: str = "scrub"):
    """Redirect the old combined create form to the per-type page."""
    if task_type not in FORM_TEMPLATES:
        task_type = "scrub"
    return RedirectResponse(
        url=f"/utils/scheduling/{task_type}/new",
        status_code=307,
    )


@router.get("/scrub/new", response_class=HTMLResponse)
async def scrub_new_form(request: Request):
    """Render the new pool scrub schedule form."""
    return templates.TemplateResponse(
        request,
        name="utils/scheduling/scrub_form.jinja",
        context={
            "page_title": "New Scrub Schedule",
            "task": None,
            "pools": _pool_names(),
            "schedule_presets": get_schedule_presets(),
        },
    )


@router.get("/smart/new", response_class=HTMLResponse)
async def smart_new_form(request: Request):
    """Render the new SMART self-test schedule form.

    The disk dropdown is not populated here. Probing disks with smartctl
    can wake idle drives and take many seconds, so the form shell renders
    immediately and the disk options load through the HTMX partial below.
    """
    return templates.TemplateResponse(
        request,
        name="utils/scheduling/smart_form.jinja",
        context={
            "page_title": "New SMART Test Schedule",
            "task": None,
            "all_disks_value": ALL_DISKS,
            "schedule_presets": get_schedule_presets(),
        },
    )


@router.get("/smart/disk-options-partial", response_class=HTMLResponse)
async def smart_disk_options_partial(request: Request, selected: str = ""):
    """HTMX partial that probes disks and renders the disk selector.

    This is the slow step (smartctl wakes idle drives), so it runs after
    the form page has already been shown to the user.
    """
    return templates.TemplateResponse(
        request,
        name="utils/scheduling/smart_disk_field.jinja",
        context={
            "disks": _disk_paths(),
            "selected": selected,
            "all_disks_value": ALL_DISKS,
        },
    )


@router.get("/health/new", response_class=HTMLResponse)
async def health_new_form(request: Request):
    """Render the new health check schedule form."""
    return templates.TemplateResponse(
        request,
        name="utils/scheduling/health_form.jinja",
        context={
            "page_title": "New Health Check Schedule",
            "task": None,
            "schedule_presets": get_schedule_presets(),
        },
    )


@router.get("/{task_type}/{task_id}/edit", response_class=HTMLResponse)
async def scheduling_edit_form(request: Request, task_type: str, task_id: str):
    """Render the edit form belonging to this task type."""
    template_name = FORM_TEMPLATES.get(task_type)
    if template_name is None:
        return RedirectResponse(
            url=f"/utils/scheduling?error=Unsupported task type '{task_type}'",
            status_code=303,
        )

    task = _load_task_record(task_type, task_id)
    if task is None:
        return RedirectResponse(
            url="/utils/scheduling?error=Scheduled task not found",
            status_code=303,
        )

    # Disks are intentionally not probed here. The SMART form loads its
    # disk selector through the disk-options-partial endpoint so the edit
    # page appears immediately instead of waiting for idle drives to wake.
    return templates.TemplateResponse(
        request,
        name=template_name,
        context={
            "page_title": "Edit Scheduled Task",
            "task": task,
            "pools": _pool_names() if task_type == "scrub" else [],
            "all_disks_value": ALL_DISKS,
            "schedule_presets": get_schedule_presets(),
        },
    )


def _load_task_record(task_type: str, task_id: str) -> Optional[Dict[str, Any]]:
    """Read one schedule record from the store that owns it."""
    if task_type == "scrub":
        try:
            return storage_service.get_scrub_schedule(int(task_id))
        except ValueError:
            return None
    if task_type == "health":
        try:
            return storage_service.get_health_schedule(int(task_id))
        except ValueError:
            return None
    if task_type == "smart":
        return smart_service.get_scheduled_test(task_id)
    return None


@router.post("/save", response_class=HTMLResponse)
async def scheduling_save(
    request: Request,
    task_type: str = Form(...),
    task_id: Optional[str] = Form(None),
    schedule: str = Form(...),
    enabled: Optional[str] = Form(None),
    pool: Optional[str] = Form(None),
    disk: Optional[str] = Form(None),
    test_type: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    check_disk_health: Optional[str] = Form(None),
    check_smart_tests: Optional[str] = Form(None),
    check_scrubs: Optional[str] = Form(None),
    aggressive_hours: Optional[str] = Form(None),
):
    """Create or update a schedule and register it with the OS scheduler."""
    is_enabled = enabled is not None
    schedule = (schedule or "").strip()

    is_valid, schedule_error = validate_cron_expression(schedule)
    if not is_valid:
        return RedirectResponse(
            url=f"/utils/scheduling?error=Invalid schedule: {schedule_error}",
            status_code=303,
        )

    try:
        if task_type == "scrub":
            if not pool:
                raise ValueError("A pool must be selected for a scrub schedule")
            if task_id:
                storage_service.update_scrub_schedule(
                    int(task_id), pool=pool, schedule=schedule, enabled=is_enabled
                )
                saved_id: Any = int(task_id)
            else:
                saved_id = storage_service.create_scrub_schedule(
                    pool=pool, schedule=schedule, enabled=is_enabled
                )
            description = f"pool {pool}"

        elif task_type == "smart":
            if not disk:
                raise ValueError("A disk must be selected for a SMART test schedule")
            selected_test = test_type or "short"
            disk_text = "all disks" if disk == ALL_DISKS else disk
            if task_id:
                smart_service.update_scheduled_test(
                    task_id,
                    disk=disk,
                    test_type=selected_test,
                    schedule=schedule,
                    enabled=is_enabled,
                )
                saved_id = task_id
            else:
                saved_id = smart_service.create_scheduled_test(
                    disk=disk,
                    test_type=selected_test,
                    schedule=schedule,
                    enabled=is_enabled,
                )
            description = f"{selected_test} test on {disk_text}"

        elif task_type == "health":
            schedule_name = name or "Scheduled health check"
            if task_id:
                storage_service.update_health_schedule(
                    int(task_id),
                    name=schedule_name,
                    schedule=schedule,
                    enabled=is_enabled,
                    check_disk_health=check_disk_health is not None,
                    check_smart_tests=check_smart_tests is not None,
                    check_scrubs=check_scrubs is not None,
                    aggressive_hours=aggressive_hours is not None,
                )
                saved_id = int(task_id)
            else:
                saved_id = storage_service.create_health_schedule(
                    name=schedule_name,
                    schedule=schedule,
                    enabled=is_enabled,
                    check_disk_health=check_disk_health is not None,
                    check_smart_tests=check_smart_tests is not None,
                    check_scrubs=check_scrubs is not None,
                    aggressive_hours=aggressive_hours is not None,
                )
            description = schedule_name

        else:
            raise ValueError(f"Unsupported task type '{task_type}'")

        task_scheduler.register_task(
            task_type=task_type,
            task_id=saved_id,
            schedule=schedule,
            description=description,
            enabled=is_enabled,
        )

        return RedirectResponse(
            url="/utils/scheduling?message=Scheduled task saved",
            status_code=303,
        )
    except Exception as save_error:
        return RedirectResponse(
            url=f"/utils/scheduling?error={save_error}",
            status_code=303,
        )


@router.post("/{task_type}/{task_id}/toggle", response_class=HTMLResponse)
async def scheduling_toggle(request: Request, task_type: str, task_id: str):
    """Enable or disable a schedule and update the OS scheduler entry."""
    record = _load_task_record(task_type, task_id)
    if record is None:
        return RedirectResponse(
            url="/utils/scheduling?error=Scheduled task not found",
            status_code=303,
        )

    new_state = not record.get("enabled", True)

    try:
        if task_type == "scrub":
            storage_service.update_scrub_schedule(int(task_id), enabled=new_state)
            description = f"pool {record.get('pool', 'unknown')}"
        elif task_type == "health":
            storage_service.update_health_schedule(int(task_id), enabled=new_state)
            description = record.get("name") or f"health check {task_id}"
        elif task_type == "smart":
            smart_service.update_scheduled_test(task_id, enabled=new_state)
            toggled_disk = record.get("disk", "unknown")
            if toggled_disk == ALL_DISKS:
                toggled_disk = "all disks"
            description = (
                f"{record.get('test_type', 'short')} test on {toggled_disk}"
            )
        else:
            raise ValueError(f"Unsupported task type '{task_type}'")

        task_scheduler.register_task(
            task_type=task_type,
            task_id=task_id,
            schedule=record.get("schedule", ""),
            description=description,
            enabled=new_state,
        )

        state_text = "enabled" if new_state else "disabled"
        return RedirectResponse(
            url=f"/utils/scheduling?message=Scheduled task {state_text}",
            status_code=303,
        )
    except Exception as toggle_error:
        return RedirectResponse(
            url=f"/utils/scheduling?error={toggle_error}",
            status_code=303,
        )


@router.post("/{task_type}/{task_id}/delete", response_class=HTMLResponse)
async def scheduling_delete(request: Request, task_type: str, task_id: str):
    """Remove a schedule and its OS scheduler entry."""
    try:
        task_scheduler.unregister_task(task_type, task_id)

        if task_type == "scrub":
            storage_service.delete_scrub_schedule(int(task_id))
        elif task_type == "health":
            storage_service.delete_health_schedule(int(task_id))
        elif task_type == "smart":
            smart_service.delete_scheduled_test(task_id)
        else:
            raise ValueError(f"Unsupported task type '{task_type}'")

        return RedirectResponse(
            url="/utils/scheduling?message=Scheduled task deleted",
            status_code=303,
        )
    except Exception as delete_error:
        return RedirectResponse(
            url=f"/utils/scheduling?error={delete_error}",
            status_code=303,
        )


@router.post("/{task_type}/{task_id}/run", response_class=HTMLResponse)
async def scheduling_run_now(request: Request, task_type: str, task_id: str):
    """Run a scheduled task immediately in a background thread.

    The same runner code path used by the OS scheduler is invoked, so a
    manual run records its outcome exactly like a scheduled one.
    """
    from services.task_runner import TASK_HANDLERS

    handler = TASK_HANDLERS.get(task_type)
    if handler is None:
        return RedirectResponse(
            url=f"/utils/scheduling?error=Unsupported task type '{task_type}'",
            status_code=303,
        )

    worker = threading.Thread(
        target=handler,
        args=(task_id, "manual"),
        daemon=True,
    )
    worker.start()

    return RedirectResponse(
        url="/utils/scheduling?message=Task started; results appear once it finishes",
        status_code=303,
    )


@router.post("/sync", response_class=HTMLResponse)
async def scheduling_sync(request: Request):
    """Reconcile every stored schedule with the OS scheduler."""
    try:
        task_scheduler.sync_all()
        return RedirectResponse(
            url="/utils/scheduling?message=OS scheduler synchronized",
            status_code=303,
        )
    except Exception as sync_error:
        return RedirectResponse(
            url=f"/utils/scheduling?error={sync_error}",
            status_code=303,
        )


@router.post("/validate-schedule")
async def scheduling_validate(data: Dict = Body(...)):
    """Validate a cron expression and preview its upcoming run times."""
    expression = (data.get("schedule") or "").strip()
    is_valid, schedule_error = validate_cron_expression(expression)
    if not is_valid:
        return JSONResponse({"valid": False, "error": schedule_error})
    return JSONResponse({
        "valid": True,
        "next_runs": preview_next_runs(expression, count=5),
    })
