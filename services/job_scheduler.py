"""
Task Scheduler
Registers WebZFS scheduled tasks with the operating system scheduler.

This module started as the Syncoid-only job scheduler (issue #194) and
was generalized for the Unified Scheduling Hub. It now manages four
task domains behind one common unit/cron naming scheme:

    webzfs-task-syncoid-<id>
    webzfs-task-scrub-<id>
    webzfs-task-smart-<id>
    webzfs-task-health-<id>

Platform behavior:
- Linux: writes a systemd service/timer pair under WebZFS state, links
  it with sudo systemctl, and enables the timer. The service runs the
  generic runner CLI as the same account the web application runs as
  (webzfs), so state files it writes keep webzfs ownership. Privileged
  child commands (zpool, syncoid, smartctl) still elevate through sudo
  via the existing service-level privilege helpers, exactly as they do
  when triggered from the web interface (issue #194).
- FreeBSD/NetBSD: manages a single marker-delimited block in the root
  crontab containing one line per enabled task. WebZFS already runs as
  root on the BSDs and cron is the native scheduler there.

Schedules are stored as 5-field cron expressions and converted to
systemd OnCalendar syntax on Linux by services/schedule_utils.
"""
import grp
import fcntl
import logging
import os
import platform
import pwd
import re
import subprocess
import sys
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from services.schedule_utils import cron_to_oncalendar

logger = logging.getLogger(__name__)

CRON_MARKER_BEGIN = "# BEGIN WEBZFS SCHEDULED TASKS - do not edit this block by hand"
CRON_MARKER_END = "# END WEBZFS SCHEDULED TASKS"

# The Syncoid-only implementation used its own markers. They are still
# stripped so upgrades do not leave a duplicate block behind.
LEGACY_CRON_MARKERS = [
    (
        "# BEGIN WEBZFS SYNCOID JOBS - do not edit this block by hand",
        "# END WEBZFS SYNCOID JOBS",
    ),
]

SYSTEMD_UNIT_DIR = "/etc/systemd/system"
GENERATED_SUBDIR = Path(".config/webzfs/generated/systemd")
UNIT_PREFIX = "webzfs-task-"

# Units written by the Syncoid-only implementation. sync_all() removes
# these so a system that ran the older version converges cleanly.
LEGACY_UNIT_PREFIX = "webzfs-syncoid-job-"


def _generated_unit_dir() -> Path:
    unit_dir = Path.home() / GENERATED_SUBDIR
    unit_dir.mkdir(parents=True, exist_ok=True, mode=0o755)
    return unit_dir

TASK_TYPES = ("syncoid", "scrub", "smart", "health")

TASK_TYPE_LABELS = {
    "syncoid": "Syncoid replication",
    "scrub": "Pool scrub",
    "smart": "SMART self-test",
    "health": "Health check",
}


def _is_linux() -> bool:
    return platform.system() == "Linux"


def _python_executable() -> str:
    """Absolute path to the venv Python running this application."""
    return sys.executable


def _service_user() -> str:
    """Account name the generated systemd service should run as.

    This is the account the web application itself runs as (webzfs on a
    standard Linux install). Running the task runner as the same user
    keeps JSON state, progress, log, and lock files owned by that user.
    Privileged child commands still elevate through sudo (issue #194).
    """
    return pwd.getpwuid(os.getuid()).pw_name


def _service_group() -> str:
    """Primary group name for the generated systemd service."""
    return grp.getgrgid(os.getgid()).gr_name


def _app_directory() -> str:
    """Application root directory (parent of the services package)."""
    return str(Path(__file__).resolve().parent.parent)


def sanitize_task_id(task_id: Any) -> str:
    """Make a task ID safe for use inside a unit file name.

    Scrub and health schedules use integer IDs while SMART schedules use
    UUID strings. Anything outside the safe set is replaced so the
    generated unit name always matches the sudoers glob.
    """
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(task_id))


def unit_base_name(task_type: str, task_id: Any) -> str:
    """Base unit name (without .service/.timer) for a task."""
    return f"{UNIT_PREFIX}{task_type}-{sanitize_task_id(task_id)}"


def runner_command(task_type: str, task_id: Any) -> str:
    """Shell command line that executes a task through the runner CLI."""
    return (
        f"{_python_executable()} -m services.task_runner "
        f"--task-type {task_type} --task-id {sanitize_task_id(task_id)}"
    )


def collect_scheduled_tasks() -> List[Dict[str, Any]]:
    """Read all four schedule stores and return normalized task records.

    Each record has task_type, task_id, schedule, enabled, and
    description keys. Stores that cannot be read are skipped so one
    broken file does not block reconciliation of the others.
    """
    tasks: List[Dict[str, Any]] = []

    # Imports are deferred to keep this module importable from the
    # runner CLI without pulling in the whole service layer.
    try:
        from services.storage import FileStorageService
        storage = FileStorageService()
    except Exception as storage_error:
        logger.warning(f"Could not open schedule storage: {storage_error}")
        storage = None

    if storage is not None:
        try:
            for job in storage.get_syncoid_jobs():
                tasks.append({
                    "task_type": "syncoid",
                    "task_id": job["id"],
                    "schedule": job.get("schedule", ""),
                    "enabled": job.get("enabled", True),
                    "description": job.get("name") or f"job {job['id']}",
                })
        except Exception as read_error:
            logger.warning(f"Could not read syncoid jobs: {read_error}")

        try:
            for record in storage.get_scrub_schedules():
                tasks.append({
                    "task_type": "scrub",
                    "task_id": record["id"],
                    "schedule": record.get("schedule", ""),
                    "enabled": record.get("enabled", True),
                    "description": f"pool {record.get('pool', 'unknown')}",
                })
        except Exception as read_error:
            logger.warning(f"Could not read scrub schedules: {read_error}")

        try:
            for record in storage.get_health_schedules():
                tasks.append({
                    "task_type": "health",
                    "task_id": record["id"],
                    "schedule": record.get("schedule", ""),
                    "enabled": record.get("enabled", True),
                    "description": record.get("name") or f"health check {record['id']}",
                })
        except Exception as read_error:
            logger.warning(f"Could not read health schedules: {read_error}")

    try:
        from services.smart_monitoring import SMARTMonitoringService
        for record in SMARTMonitoringService().list_scheduled_tests():
            tasks.append({
                "task_type": "smart",
                "task_id": record["id"],
                "schedule": record.get("schedule", ""),
                "enabled": record.get("enabled", True),
                "description": (
                    f"{record.get('test_type', 'short')} test on "
                    f"{record.get('disk', 'unknown disk')}"
                ),
            })
    except Exception as read_error:
        logger.warning(f"Could not read SMART scheduled tests: {read_error}")

    return tasks


class TaskSchedulerError(Exception):
    """Raised when the OS scheduler could not be updated."""


class TaskScheduler:
    """Manage OS-level schedule registration for all WebZFS task types."""

    @contextmanager
    def _unit_lock(self):
        """Serialize unit changes across web workers and startup reconciliation."""
        try:
            with (_generated_unit_dir() / ".lock").open("a") as handle:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        except OSError as error:
            raise TaskSchedulerError(f"Cannot access generated units: {error}") from error

    def _error_path(self, base_name: str) -> Path:
        return _generated_unit_dir() / f"{base_name}.error"

    def _set_error(self, base_name: str, message: str) -> None:
        self._write_unit_file(self._error_path(base_name), message)
        logger.error("Could not register %s: %s", base_name, message)

    def _clear_error(self, base_name: str) -> None:
        self._error_path(base_name).unlink(missing_ok=True)

    def get_registration_error(self, task_type: str, task_id: Any) -> str:
        """Return a cross-worker registration failure without calling systemctl."""
        if not _is_linux():
            return ""
        try:
            path = self._error_path(unit_base_name(task_type, task_id))
            return path.read_text() if path.is_file() else ""
        except OSError:
            return "Unable to read scheduler registration state"

    # Public API

    def register_task(
        self,
        task_type: str,
        task_id: Any,
        schedule: str,
        description: str = "",
        enabled: bool = True,
    ) -> None:
        """Create or update the OS schedule entry for a task.

        Args:
            task_type: One of syncoid, scrub, smart, health.
            task_id: Schedule record identifier.
            schedule: 5-field cron expression.
            description: Text used in the unit Description field.
            enabled: When False the entry is removed instead.
        """
        if task_type not in TASK_TYPES:
            raise TaskSchedulerError(f"Unknown task type '{task_type}'")

        if not enabled:
            self.unregister_task(task_type, task_id)
            return

        if _is_linux():
            base_name = unit_base_name(task_type, task_id)
            with self._unit_lock():
                try:
                    self._write_systemd_units(task_type, task_id, schedule, description)
                except TaskSchedulerError as error:
                    self._set_error(base_name, str(error))
                    raise
                self._clear_error(base_name)
        else:
            self._sync_crontab_block()

    def unregister_task(self, task_type: str, task_id: Any) -> None:
        """Remove the OS schedule entry for a task."""
        if _is_linux():
            with self._unit_lock():
                self._remove_systemd_units(task_type, task_id)
        else:
            self._sync_crontab_block()

    def sync_all(self) -> None:
        """Reconcile OS scheduler state with all four schedule stores.

        Registers every enabled task, removes entries for disabled or
        deleted tasks. The root-running Linux updater handles legacy
        Syncoid-only unit files.
        """
        if _is_linux():
            with self._unit_lock():
                self._sync_systemd(collect_scheduled_tasks())
        else:
            self._sync_crontab_block(collect_scheduled_tasks())

    def _sync_systemd(self, tasks: List[Dict[str, Any]]) -> None:
        """Rebuild enabled Linux timers from the authoritative schedule stores."""
        try:
            wanted = set()
            removal_errors = []
            for task in tasks:
                if not task.get("enabled", True):
                    continue
                schedule = (task.get("schedule") or "").strip()
                if not schedule:
                    continue
                base_name = unit_base_name(task["task_type"], task["task_id"])
                wanted.add(base_name)
                try:
                    self._write_systemd_units(
                        task["task_type"],
                        task["task_id"],
                        schedule,
                        task.get("description", ""),
                    )
                    self._clear_error(base_name)
                except TaskSchedulerError as register_error:
                    self._set_error(base_name, str(register_error))

            for base_name in self._list_installed_unit_names():
                if base_name not in wanted:
                    try:
                        self._remove_units_by_base_name(base_name)
                    except TaskSchedulerError as error:
                        logger.warning("Could not remove obsolete unit %s: %s", base_name, error)
                        removal_errors.append(base_name)
            for error_file in _generated_unit_dir().glob(f"{UNIT_PREFIX}*.error"):
                if error_file.name[:-len(".error")] not in wanted:
                    error_file.unlink(missing_ok=True)
            errors = [
                path.name for path in _generated_unit_dir().glob(f"{UNIT_PREFIX}*.error")
                if path.name[:-len(".error")] in wanted
            ]
            if errors or removal_errors:
                raise TaskSchedulerError(
                    "Scheduler reconciliation failed for: "
                    + ", ".join(sorted(errors + removal_errors))
                )
        except OSError as error:
            raise TaskSchedulerError(f"Cannot reconcile generated units: {error}") from error

    def get_scheduler_status(self, task_type: str, task_id: Any) -> Dict[str, Any]:
        """Return best-effort OS scheduler state for a task (for the UI)."""
        if _is_linux():
            timer_name = f"{unit_base_name(task_type, task_id)}.timer"
            error_message = self.get_registration_error(task_type, task_id)
            if error_message:
                return {
                    "backend": "systemd", "unit": timer_name,
                    "state": "error", "message": error_message,
                }
            try:
                result = subprocess.run(
                    ["sudo", "systemctl", "is-active", timer_name],
                    capture_output=True, text=True, timeout=10,
                )
                state = result.stdout.strip() or "unknown"
            except Exception:
                state = "unknown"
            return {"backend": "systemd", "unit": timer_name, "state": state}
        return {"backend": "cron", "unit": "root crontab", "state": "managed"}

    # Linux: systemd timer units

    def _unit_paths(self, base_name: str) -> Tuple[Path, Path]:
        return (
            _generated_unit_dir() / f"{base_name}.service",
            _generated_unit_dir() / f"{base_name}.timer",
        )

    def _write_unit_file(self, path: Path, content: str) -> None:
        """Replace a generated file without exposing a partial write."""
        name = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", dir=path.parent, prefix=".webzfs-", delete=False
            ) as handle:
                name = handle.name
                handle.write(content)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(name, 0o644)
            os.replace(name, path)
        except OSError as error:
            raise TaskSchedulerError(f"Failed to write {path}: {error}") from error
        finally:
            if name and os.path.exists(name):
                try:
                    os.unlink(name)
                except OSError:
                    logger.warning("Could not clean up temporary unit file %s", name)

    def _check_unit_collision(self, source: Path) -> None:
        target = Path(SYSTEMD_UNIT_DIR) / source.name
        if not target.exists() and not target.is_symlink():
            return
        if target.is_symlink() and os.readlink(target) == str(source):
            return
        raise TaskSchedulerError(f"{target} is not a WebZFS link; refusing to replace it")

    def _check_generated_source(self, source: Path) -> None:
        if source.is_symlink() or (source.exists() and not source.is_file()):
            raise TaskSchedulerError(f"{source} is not a regular generated unit")
        if source.exists() and (source.stat().st_uid != os.getuid()):
            raise TaskSchedulerError(f"{source} is not owned by the WebZFS process")

    def _write_systemd_units(
        self,
        task_type: str,
        task_id: Any,
        schedule: str,
        description: str,
    ) -> None:
        oncalendar = cron_to_oncalendar(schedule or "")
        if not oncalendar:
            raise TaskSchedulerError(
                f"Schedule '{schedule}' could not be converted to a "
                f"systemd OnCalendar expression"
            )

        base_name = unit_base_name(task_type, task_id)
        service_path, timer_path = self._unit_paths(base_name)
        label = TASK_TYPE_LABELS.get(task_type, task_type)
        detail = description or str(task_id)

        service_content = (
            "[Unit]\n"
            f"Description=WebZFS {label} task {task_id} ({detail})\n"
            "After=network-online.target zfs.target\n"
            "Wants=network-online.target\n"
            "\n"
            "[Service]\n"
            "Type=oneshot\n"
            # Run as the web application account (webzfs), not root, so
            # the runner's JSON/progress/lock writes keep webzfs
            # ownership. Privileged commands (zpool, syncoid, smartctl)
            # elevate through sudo inside the runner (issue #194).
            f"User={_service_user()}\n"
            f"Group={_service_group()}\n"
            f"WorkingDirectory={_app_directory()}\n"
            # HOME must point at the application directory so
            # FileStorageService and SSHConnectionService resolve
            # ~/.config/webzfs and ~/.ssh under /opt/webzfs, matching
            # how the web application runs.
            f"Environment=HOME={_app_directory()}\n"
            f"Environment=PYTHONPATH={_app_directory()}\n"
            f"ExecStart={runner_command(task_type, task_id)}\n"
        )

        timer_content = (
            "[Unit]\n"
            f"Description=Timer for WebZFS {label} task {task_id} ({detail})\n"
            "\n"
            "[Timer]\n"
            f"OnCalendar={oncalendar}\n"
            "Persistent=false\n"
            "\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
        )

        # Check both targets before changing any unit source.
        self._check_unit_collision(service_path)
        self._check_unit_collision(timer_path)
        self._check_generated_source(service_path)
        self._check_generated_source(timer_path)
        self._write_unit_file(service_path, service_content)
        self._write_unit_file(timer_path, timer_content)
        self._systemctl("link", str(service_path))
        self._systemctl("link", str(timer_path))
        self._systemctl("daemon-reload")
        self._systemctl("enable", "--now", f"{base_name}.timer")
        logger.info(f"Registered systemd timer {base_name}.timer")

    def _remove_systemd_units(self, task_type: str, task_id: Any) -> None:
        self._remove_units_by_base_name(unit_base_name(task_type, task_id))

    def _remove_units_by_base_name(self, base_name: str) -> None:
        service_path, timer_path = self._unit_paths(base_name)
        timer_name = f"{base_name}.timer"

        # Do not disable or remove a foreign unit, even after a partial failure.
        self._check_unit_collision(service_path)
        self._check_unit_collision(timer_path)
        self._check_generated_source(service_path)
        self._check_generated_source(timer_path)
        self._systemctl("disable", "--now", timer_name, check=False)
        self._systemctl("disable", timer_name, f"{base_name}.service", check=False)
        # If links remain, keep their source files so systemd never sees dangling links.
        wants_link = Path(SYSTEMD_UNIT_DIR) / "timers.target.wants" / timer_name
        if wants_link.is_symlink():
            raise TaskSchedulerError(f"systemd did not disable {timer_name}")
        for unit_path in (service_path, timer_path):
            target = Path(SYSTEMD_UNIT_DIR) / unit_path.name
            if target.is_symlink():
                raise TaskSchedulerError(f"systemd did not remove {target}")
        try:
            for unit_path in (service_path, timer_path):
                unit_path.unlink(missing_ok=True)
        except OSError as error:
            raise TaskSchedulerError(f"Cannot remove generated unit: {error}") from error
        self._clear_error(base_name)
        self._systemctl("daemon-reload", check=False)
        logger.info(f"Unregistered systemd timer {timer_name}")

    def _list_installed_unit_names(self) -> List[str]:
        """List base names of WebZFS timer units currently installed.

        Legacy regular units are migrated by the root-running updater.
        """
        names = []
        unit_dir = _generated_unit_dir()
        try:
            for suffix in (".timer", ".service"):
                for unit_file in unit_dir.glob(f"{UNIT_PREFIX}*{suffix}"):
                    names.append(unit_file.name[:-len(suffix)])
        except OSError as error:
            raise TaskSchedulerError(f"Cannot list generated units: {error}") from error
        return sorted(set(names))

    def _systemctl(self, *args: str, check: bool = True) -> None:
        try:
            subprocess.run(
                ["sudo", "systemctl", *args],
                capture_output=True, text=True, timeout=30, check=check,
            )
        except subprocess.CalledProcessError as ctl_error:
            raise TaskSchedulerError(
                f"systemctl {' '.join(args)} failed: {ctl_error.stderr}"
            )

    # BSD: root crontab block

    def _sync_crontab_block(
        self, tasks: Optional[List[Dict[str, Any]]] = None
    ) -> None:
        """Rewrite the WebZFS-managed block in the root crontab.

        The whole block is regenerated from the schedule stores on every
        change, which keeps crontab state convergent with the JSON
        stores and avoids per-line editing.
        """
        if tasks is None:
            tasks = collect_scheduled_tasks()

        current = self._read_crontab()
        preserved = self._strip_managed_blocks(current)

        log_path = f"{_app_directory()}/webzfs_tasks.log"
        block_lines = [CRON_MARKER_BEGIN]
        for task in tasks:
            if not task.get("enabled", True):
                continue
            schedule = (task.get("schedule") or "").strip()
            if not schedule or len(schedule.split()) != 5:
                logger.warning(
                    f"Skipping crontab entry for {task.get('task_type')} task "
                    f"{task.get('task_id')}: invalid schedule '{schedule}'"
                )
                continue
            # HOME must be forced to the application directory.
            # cron starts jobs with HOME taken from the passwd entry,
            # which is /root, while the rc.d service wrapper exports
            # HOME=<app dir>. FileStorageService derives its data
            # directory from Path.home(), so without this the runner
            # would read /root/.config/webzfs while the web interface
            # writes <app dir>/.config/webzfs and no schedule record
            # would ever be found. The task_runner fallback does not
            # help here because cron does set HOME, just to the wrong
            # value.
            command = (
                f"cd {_app_directory()} && "
                f"HOME={_app_directory()} "
                f"{runner_command(task['task_type'], task['task_id'])} "
                f">> {log_path} 2>&1"
            )
            block_lines.append(f"{schedule} {command}")
        block_lines.append(CRON_MARKER_END)

        new_crontab = preserved.rstrip("\n")
        if new_crontab:
            new_crontab += "\n"
        new_crontab += "\n".join(block_lines) + "\n"

        self._write_crontab(new_crontab)
        logger.info("Synchronized WebZFS scheduled task crontab block")

    def _read_crontab(self) -> str:
        try:
            result = subprocess.run(
                ["crontab", "-l"],
                capture_output=True, text=True, timeout=15,
            )
            if result.returncode == 0:
                return result.stdout
            # "no crontab for root" is not an error for our purposes
            return ""
        except Exception as read_error:
            logger.warning(f"Could not read crontab: {read_error}")
            return ""

    def _strip_managed_blocks(self, crontab_text: str) -> str:
        """Remove all WebZFS-managed blocks, preserving everything else."""
        marker_pairs = [(CRON_MARKER_BEGIN, CRON_MARKER_END)]
        marker_pairs.extend(LEGACY_CRON_MARKERS)
        begin_markers = {pair[0] for pair in marker_pairs}
        end_markers = {pair[1] for pair in marker_pairs}

        preserved = []
        inside_block = False
        for line in crontab_text.splitlines():
            stripped = line.strip()
            if stripped in begin_markers:
                inside_block = True
                continue
            if stripped in end_markers:
                inside_block = False
                continue
            if not inside_block:
                preserved.append(line)
        return "\n".join(preserved) + ("\n" if preserved else "")

    def _write_crontab(self, content: str) -> None:
        try:
            subprocess.run(
                ["crontab", "-"],
                input=content, capture_output=True, text=True,
                timeout=15, check=True,
            )
        except subprocess.CalledProcessError as write_error:
            raise TaskSchedulerError(
                f"Failed to install crontab: {write_error.stderr}"
            )


# Backwards compatible alias kept so existing imports keep working.
SyncoidJobSchedulerError = TaskSchedulerError


class SyncoidJobScheduler:
    """Thin Syncoid-specific wrapper around TaskScheduler.

    Preserved so views/zfs_replication.py keeps its original call
    signatures while the underlying implementation is shared with the
    scrub, SMART, and health task types.
    """

    def __init__(self) -> None:
        self._scheduler = TaskScheduler()

    def register_job(self, job: Dict[str, Any]) -> None:
        """Create or update the OS schedule entry for an enabled job."""
        self._scheduler.register_task(
            task_type="syncoid",
            task_id=job["id"],
            schedule=job.get("schedule", ""),
            description=job.get("name") or f"job {job['id']}",
            enabled=job.get("enabled", True),
        )

    def unregister_job(self, job_id: int) -> None:
        """Remove the OS schedule entry for a job."""
        self._scheduler.unregister_task("syncoid", job_id)

    def sync_all_jobs(self) -> None:
        """Reconcile OS scheduler state with every schedule store."""
        self._scheduler.sync_all()

    def get_scheduler_status(self, job_id: int) -> Dict[str, Any]:
        """Return best-effort OS scheduler state for a job."""
        return self._scheduler.get_scheduler_status("syncoid", job_id)
