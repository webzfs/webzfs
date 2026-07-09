from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import RedirectResponse
from auth.dependencies import get_current_user
from config.templates import templates
from services.zfs_pool import ZFSPoolService
from datetime import datetime
from pathlib import Path
import json

router = APIRouter(dependencies=[Depends(get_current_user)])


class ScrubScheduleStorage:
    """File-based storage for scrub schedules"""
    
    def __init__(self, data_dir: str = None):
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            home = Path.home()
            self.data_dir = home / '.config' / 'webzfs'
        
        self.schedules_file = self.data_dir / 'scrub_schedules.json'
        self._ensure_data_directory()
        self._initialize_file()
    
    def _ensure_data_directory(self) -> None:
        """Ensure the data directory exists"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def _initialize_file(self) -> None:
        """Initialize data file if it doesn't exist"""
        if not self.schedules_file.exists():
            self._write_json({'schedules': [], 'next_id': 1})
    
    def _read_json(self) -> dict:
        """Read JSON file with error handling"""
        try:
            with open(self.schedules_file, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {'schedules': [], 'next_id': 1}
    
    def _write_json(self, data: dict) -> None:
        """Write JSON file atomically.

        Uses a unique temp file via tempfile.mkstemp so concurrent workers
        do not collide on a shared temp name during startup initialization.
        """
        import os
        import tempfile
        file_path = self.schedules_file
        fd, temp_name = tempfile.mkstemp(dir=str(file_path.parent), suffix='.tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_name, file_path)
        except BaseException:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
            raise

    
    def list_schedules(self) -> list:
        """Get all schedules"""
        data = self._read_json()
        return data.get('schedules', [])
    
    def create_schedule(self, pool: str, schedule: str, enabled: bool) -> int:
        """Create a new schedule"""
        data = self._read_json()
        
        schedule_id = data.get('next_id', 1)
        
        new_schedule = {
            'id': schedule_id,
            'pool': pool,
            'schedule': schedule,
            'enabled': enabled,
            'created_at': datetime.now().isoformat()
        }
        
        data['schedules'].append(new_schedule)
        data['next_id'] = schedule_id + 1
        
        self._write_json(data)
        return schedule_id
    
    def toggle_schedule(self, schedule_id: int) -> bool:
        """Toggle a schedule's enabled status"""
        data = self._read_json()
        
        for schedule in data['schedules']:
            if schedule['id'] == schedule_id:
                schedule['enabled'] = not schedule['enabled']
                self._write_json(data)
                return schedule['enabled']
        
        return False
    
    def delete_schedule(self, schedule_id: int) -> bool:
        """Delete a schedule"""
        data = self._read_json()
        
        original_count = len(data['schedules'])
        data['schedules'] = [s for s in data['schedules'] if s['id'] != schedule_id]
        
        if len(data['schedules']) < original_count:
            self._write_json(data)
            return True
        
        return False


# Initialize storage
storage = ScrubScheduleStorage()


@router.get("/")
def index(request: Request):
    """Display the utilities page with cards for Shell, Text, Files, SMART, and Scrub Scheduling."""
    return templates.TemplateResponse(request, name="utils/index.jinja", context={})


@router.get("/scrub-scheduling")
def scrub_scheduling(request: Request):
    """Display the ZFS scrub scheduling page."""
    pool_service = ZFSPoolService()
    try:
        pools_data = pool_service.list_pools()
        pools = [pool['name'] for pool in pools_data]
    except Exception as e:
        pools = []
    
    scheduled_scrubs = storage.list_schedules()
        
    return templates.TemplateResponse(
        request,
        name="utils/scrub/scrub_scheduling.jinja",
        context={
            "pools": pools,
            "scheduled_scrubs": scheduled_scrubs
        }
    )


@router.post("/scrub-scheduling/create")
def create_scrub_schedule(
    request: Request,
    pool: str = Form(...),
    schedule: str = Form(...),
    enabled: str = Form(None)
):
    """Create a new scrub schedule."""
    storage.create_schedule(pool, schedule, enabled == "true")
    
    return RedirectResponse(
        url="/utils/scrub-scheduling?message=Scrub schedule created successfully",
        status_code=303
    )


@router.post("/scrub-scheduling/{schedule_id}/toggle")
def toggle_scrub_schedule(request: Request, schedule_id: int):
    """Toggle a scrub schedule enabled/disabled."""
    enabled = storage.toggle_schedule(schedule_id)
    
    if enabled is not False:
        status = "enabled" if enabled else "disabled"
        return RedirectResponse(
            url=f"/utils/scrub-scheduling?message=Schedule {status} successfully",
            status_code=303
        )
    
    return RedirectResponse(
        url="/utils/scrub-scheduling?error=Schedule not found",
        status_code=303
    )


@router.post("/scrub-scheduling/{schedule_id}/delete")
def delete_scrub_schedule(request: Request, schedule_id: int):
    """Delete a scrub schedule."""
    if storage.delete_schedule(schedule_id):
        return RedirectResponse(
            url="/utils/scrub-scheduling?message=Schedule deleted successfully",
            status_code=303
        )
    
    return RedirectResponse(
        url="/utils/scrub-scheduling?error=Schedule not found",
        status_code=303
    )
