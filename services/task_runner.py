"""
Scheduled Task Runner
Single entry point executed by the operating system scheduler.

The systemd service units and crontab lines created by
services/job_scheduler.py all call this module:

    python -m services.task_runner --task-type scrub --task-id 3

Running every task type through one CLI keeps privilege handling, run
recording, and lock behavior identical across the four domains. Exit
code 0 means the task ran (or was intentionally skipped), non-zero means
the task could not be started.
"""
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

# Allow execution as a plain script as well as with -m
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logger = logging.getLogger("webzfs.task_runner")

LOCK_DIR = Path("/tmp")


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def _lock_path(task_type: str, task_id: str) -> Path:
    return LOCK_DIR / f"webzfs-task-{task_type}-{task_id}.lock"


class RunLock:
    """Prevent overlapping runs of the same scheduled task.

    A task that is still running when its next occurrence fires must not
    start a second time. The lock is advisory and released when the
    process exits, including on crash, because flock is tied to the open
    file descriptor.
    """

    def __init__(self, task_type: str, task_id: str):
        self.path = _lock_path(task_type, task_id)
        self.handle = None

    def __enter__(self) -> bool:
        import fcntl
        try:
            self.handle = open(self.path, "a")
        except PermissionError:
            # A lock file left behind by an older release that ran the
            # scheduled runner as root (issue #194) cannot be opened by
            # the webzfs account, and the /tmp sticky bit prevents
            # deleting it. Surface a clear remediation instead of a
            # traceback. update_linux.sh removes these stale files.
            logger.error(
                f"Cannot open lock file {self.path}: it is owned by "
                f"another user (likely root, created by an older WebZFS "
                f"release). Remove it as root with: rm {self.path}"
            )
            raise
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.handle.close()
            self.handle = None
            return False
        return True

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.handle is None:
            return
        import fcntl
        try:
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
        finally:
            self.handle.close()
            self.handle = None


def _next_run_for(schedule: str) -> Optional[str]:
    """Best-effort next occurrence string for a cron expression."""
    try:
        from services.schedule_utils import preview_next_runs
        upcoming = preview_next_runs(schedule, count=1)
        return upcoming[0] if upcoming else None
    except Exception:
        return None


def run_syncoid_task(task_id: str, trigger: str) -> int:
    """Execute a scheduled Syncoid replication job."""
    from services.syncoid_runner import run_syncoid_job

    try:
        numeric_id = int(task_id)
    except ValueError:
        logger.error(f"Syncoid task id '{task_id}' is not numeric")
        return 1

    run_syncoid_job(job_id=numeric_id, trigger=trigger)
    return 0


def run_scrub_task(task_id: str, trigger: str) -> int:
    """Start (or resume) a scrub on the pool named by a scrub schedule.

    Scheduled-scrub-on-paused policy: when a scrub is already **paused**, a
    scheduled run *resumes* it instead of restarting from scratch, because the
    pause state is checkpointed to disk and a restart would throw that progress
    away. A paused error scrub is resumed the same way (``zpool scrub -e``,
    chosen by ``resume_scrub``), since a full scrub cannot start until it is
    finished or stopped. Any other state starts a normal scrub.
    """
    from services.storage import FileStorageService
    from services.zfs_pool import ZFSPoolService

    try:
        numeric_id = int(task_id)
    except ValueError:
        logger.error(f"Scrub task id '{task_id}' is not numeric")
        return 1

    storage = FileStorageService()
    record = storage.get_scrub_schedule(numeric_id)
    if not record:
        logger.error(f"Scrub schedule {numeric_id} not found")
        return 1

    pool = record.get("pool", "")
    started_at = datetime.now().isoformat()
    status = "success"

    service = ZFSPoolService()
    try:
        paused = False
        try:
            paused = service.get_scan_status(pool).scrub_paused
        except Exception as scan_error:
            logger.warning(
                f"Could not read scrub state for pool {pool} before scheduled "
                f"scrub; starting a regular scrub: {scan_error}"
            )

        if paused:
            service.resume_scrub(pool)
            logger.info(
                f"Resumed paused scrub on pool {pool} (trigger: {trigger})"
            )
        else:
            service.scrub_pool(pool)
            logger.info(f"Started scrub on pool {pool} (trigger: {trigger})")
    except Exception as scrub_error:
        status = "failure"
        logger.error(f"Scrub of pool {pool} failed to start: {scrub_error}")

    storage.update_scrub_schedule_status(
        numeric_id,
        last_run=started_at,
        last_status=status,
        next_run=_next_run_for(record.get("schedule", "")),
    )
    return 0 if status == "success" else 1


def run_smart_task(task_id: str, trigger: str) -> int:
    """Start the SMART self-test described by a SMART schedule.

    A schedule whose disk is ALL_DISKS covers every disk present when the
    task runs, so disks added after the schedule was created are included
    without editing the schedule.
    """
    from services.smart_monitoring import SMARTMonitoringService, ALL_DISKS

    service = SMARTMonitoringService()
    record = service.get_scheduled_test(task_id)
    if not record:
        logger.error(f"SMART schedule {task_id} not found")
        return 1

    disk = record.get("disk", "")
    test_type = record.get("test_type", "short")
    started_at = datetime.now().isoformat()

    if disk == ALL_DISKS:
        try:
            targets = [entry["path"] for entry in service.list_disks()]
        except Exception as list_error:
            logger.error(f"Could not enumerate disks: {list_error}")
            targets = []
        if not targets:
            service.update_scheduled_test_status(
                task_id,
                last_run=started_at,
                last_status="failure",
                next_run=_next_run_for(record.get("schedule", "")),
            )
            return 1
    else:
        targets = [disk]

    started = 0
    failed = 0

    for target in targets:
        try:
            if test_type == "long":
                service.start_long_test(target)
            else:
                service.start_short_test(target)
            service.add_test_to_history(
                disk=target,
                test_type=test_type,
                status="started",
                trigger=trigger,
                schedule_id=task_id,
            )
            started += 1
            logger.info(
                f"Started {test_type} SMART test on {target} "
                f"(trigger: {trigger})"
            )
        except Exception as test_error:
            failed += 1
            logger.error(
                f"SMART {test_type} test on {target} failed: {test_error}"
            )

    # One failing disk should not hide the disks that did start, so a mixed
    # result is recorded as partial rather than success or failure.
    if failed == 0:
        status = "success"
    elif started == 0:
        status = "failure"
    else:
        status = "partial"

    service.update_scheduled_test_status(
        task_id,
        last_run=started_at,
        last_status=status,
        next_run=_next_run_for(record.get("schedule", "")),
    )
    return 0 if started > 0 else 1


def run_health_task(task_id: str, trigger: str) -> int:
    """Run a full health analysis and store the resulting report."""
    from services.health_analysis import HealthAnalysisService
    from services.storage import FileStorageService

    try:
        numeric_id = int(task_id)
    except ValueError:
        logger.error(f"Health task id '{task_id}' is not numeric")
        return 1

    storage = FileStorageService()
    record = storage.get_health_schedule(numeric_id)
    if not record:
        logger.error(f"Health schedule {numeric_id} not found")
        return 1

    started_at = datetime.now().isoformat()
    status = "success"
    report_id = None
    health_service = HealthAnalysisService()

    try:
        report_id = health_service.create_pending_report(
            check_disk_health=record.get("check_disk_health", True),
            check_smart_tests=record.get("check_smart_tests", True),
            check_scrubs=record.get("check_scrubs", True),
            aggressive_hours=record.get("aggressive_hours", False),
        )
        # Run in the current process; the OS scheduler already gave us a
        # dedicated process, so no background thread is needed.
        health_service.run_analysis_background(report_id)
        logger.info(
            f"Completed health analysis {report_id} (trigger: {trigger})"
        )
    except Exception as health_error:
        status = "failure"
        logger.error(f"Health analysis failed: {health_error}")

    storage.update_health_schedule_status(
        numeric_id,
        last_run=started_at,
        last_status=status,
        next_run=_next_run_for(record.get("schedule", "")),
        last_report_id=report_id,
    )
    return 0 if status == "success" else 1


TASK_HANDLERS = {
    "syncoid": run_syncoid_task,
    "scrub": run_scrub_task,
    "smart": run_smart_task,
    "health": run_health_task,
}


def main(argv=None) -> int:
    _configure_logging()

    parser = argparse.ArgumentParser(
        description="Execute a WebZFS scheduled task"
    )
    parser.add_argument(
        "--task-type",
        required=True,
        choices=sorted(TASK_HANDLERS.keys()),
        help="Task domain to execute",
    )
    parser.add_argument(
        "--task-id",
        required=True,
        help="Schedule record identifier",
    )
    parser.add_argument(
        "--trigger",
        default="scheduled",
        help="How the run was initiated (scheduled or manual)",
    )
    args = parser.parse_args(argv)

    # HOME determines where FileStorageService looks for its JSON files.
    # The unit files set it explicitly, but a bare cron environment may
    # not, so fall back to the application directory.
    if not os.environ.get("HOME"):
        os.environ["HOME"] = str(Path(__file__).resolve().parent.parent)

    handler = TASK_HANDLERS[args.task_type]

    lock = RunLock(args.task_type, args.task_id)
    try:
        acquired = lock.__enter__()
    except PermissionError:
        return 1
    if not acquired:
        logger.warning(
            f"{args.task_type} task {args.task_id} is already running; "
            f"skipping this occurrence"
        )
        return 0
    try:
        return handler(args.task_id, args.trigger)
    finally:
        lock.__exit__(None, None, None)


if __name__ == "__main__":
    sys.exit(main())
