"""
Syncoid Job Runner
Executes a saved scheduled Syncoid job by ID.

This module is the single execution path for scheduled Syncoid jobs
(issue #194). It is invoked in three ways:

1. By a systemd timer unit on Linux:
       ExecStart=<venv>/bin/python -m services.syncoid_runner --job-id N
2. By a root crontab entry on FreeBSD/NetBSD with the same command.
3. By the web UI "Run Now" action (in a background thread).

The runner:
- Loads the job definition from syncoid_jobs.json.
- Resolves the SSH Manager connection server side at execution time,
  so key rotations and host changes apply without editing the job.
- Takes a per-job flock so overlapping runs are skipped, not stacked.
- Creates a record in the normal replication execution history so
  scheduled runs appear on the Replication History page.
- Updates the job's last_run, last_status, and next_run fields.
"""
import argparse
import fcntl
import sys
from datetime import datetime
from typing import Any, Dict, Optional


def run_syncoid_job(job_id: int, trigger: str = "scheduled") -> Dict[str, Any]:
    """
    Execute a saved Syncoid job.

    Args:
        job_id: The job ID from syncoid_jobs.json.
        trigger: 'scheduled' or 'manual', recorded in the history entry.

    Returns:
        Dictionary with keys: success (bool), status (str), and
        optionally error, execution_id.
    """
    # Imports are deferred so `python -m services.syncoid_runner --help`
    # works without a full application environment.
    from services.storage import FileStorageService
    from services.syncoid import SyncoidService
    from services.ssh_connection import SSHConnectionService
    from services.schedule_utils import calculate_next_run

    storage = FileStorageService()

    job = storage.get_syncoid_job(job_id)
    if not job:
        return {"success": False, "status": "error", "error": f"Syncoid job #{job_id} not found"}

    if trigger == "scheduled" and not job.get("enabled", True):
        # Guard against a stale timer/cron entry firing for a disabled job.
        return {"success": False, "status": "skipped", "error": "Job is disabled"}

    # Per-job overlap lock. If a previous run is still active, skip
    # this occurrence and record the skip so it is visible in the UI.
    lock_path = storage.data_dir / f"syncoid_job_{job_id}.lock"
    lock_handle = open(lock_path, "a")
    try:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            storage.update_syncoid_job_status(
                job_id=job_id,
                last_run=datetime.now().isoformat(),
                last_status="skipped",
                next_run=calculate_next_run(job.get("schedule", "")) or "",
            )
            return {
                "success": False,
                "status": "skipped",
                "error": "Previous run of this job is still active; skipped.",
            }

        return _execute_locked_job(job, trigger, storage)
    finally:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        except OSError:
            pass
        lock_handle.close()


def _execute_locked_job(
    job: Dict[str, Any],
    trigger: str,
    storage: Any,
) -> Dict[str, Any]:
    """Run the job while holding the per-job lock."""
    from services.syncoid import SyncoidService
    from services.ssh_connection import SSHConnectionService
    from services.schedule_utils import calculate_next_run

    job_id = job["id"]
    started = datetime.now()

    # Resolve SSH connection server side at execution time. The stored
    # connection ID is authoritative; host, port, username, and key are
    # read fresh from the SSH Manager record (issue #195 model).
    ssh_profile: Optional[Dict[str, Any]] = None
    connection_error: Optional[str] = None
    connection_id = job.get("ssh_connection_id")
    if connection_id:
        try:
            ssh_service = SSHConnectionService()
            ssh_profile = ssh_service.build_syncoid_profile(connection_id)
            ssh_service.mark_connection_used(connection_id, "replication")
        except Exception as resolve_error:
            connection_error = (
                f"SSH connection could not be resolved: {resolve_error}"
            )

    replication_type = job.get("replication_type", "local")

    execution_id = storage.create_execution_record(
        job_id=f"syncoid-{job_id}",
        job_name=f"{job.get('name', 'Syncoid job')} ({trigger})",
        source_dataset=job.get("source_dataset", ""),
        target_dataset=job.get("target_dataset", ""),
        replication_type=replication_type,
        receive_options=job.get("receive_options") or None,
    )

    def finish(status: str, error_message: Optional[str] = None,
               log_output: Optional[str] = None,
               command: Optional[str] = None,
               bytes_transferred: int = 0) -> None:
        completed = datetime.now()
        storage.update_execution_record(
            execution_id=execution_id,
            status=status,
            completed_at=completed.isoformat(),
            duration_seconds=(completed - started).total_seconds(),
            bytes_transferred=bytes_transferred,
            command=command,
            error_message=error_message,
            log_output=log_output,
        )
        storage.update_syncoid_job_status(
            job_id=job_id,
            last_run=started.isoformat(),
            last_status=status,
            next_run=calculate_next_run(job.get("schedule", "")) or "",
        )

    if connection_error:
        finish("failure", error_message=connection_error)
        return {
            "success": False,
            "status": "failure",
            "error": connection_error,
            "execution_id": execution_id,
        }

    # Validate dataset names before invoking syncoid. Jobs saved before
    # form validation existed may hold a mountpoint path (for example
    # /zdata) instead of a ZFS dataset name. Syncoid would fail deep in
    # the pipe with "cannot receive: invalid name" hidden behind mbuffer
    # broken pipe errors; fail fast with a clear message instead.
    dataset_error = _validate_job_datasets(job)
    if dataset_error:
        finish("failure", error_message=dataset_error)
        return {
            "success": False,
            "status": "failure",
            "error": dataset_error,
            "execution_id": execution_id,
        }

    source_host = ssh_profile["host_string"] if ssh_profile and replication_type == "pull" else None
    target_host = ssh_profile["host_string"] if ssh_profile and replication_type == "push" else None

    def record_progress(progress: Dict[str, Any]) -> None:
        """Persist a pv progress frame so the execution detail page's
        Real-Time Progress panel (which polls the progress store) has
        live data during scheduled and manual syncoid runs."""
        try:
            storage.add_progress_update(
                execution_id=execution_id,
                bytes_transferred=progress.get("bytes_transferred", 0),
                percentage_complete=progress.get("percentage", 0.0),
                transfer_rate=progress.get("transfer_rate", "N/A"),
                estimated_time_remaining=progress.get("eta"),
                status_message="Syncoid transfer in progress",
            )
        except Exception:
            # Progress recording must never break the replication.
            pass

    try:
        syncoid_service = SyncoidService()
        result = syncoid_service.execute_replication(
            source=job.get("source_dataset", ""),
            target=job.get("target_dataset", ""),
            recursive=job.get("recursive", False),
            no_sync_snap=job.get("no_sync_snap", False),
            compress=job.get("compress") or None,
            source_bwlimit=job.get("source_bwlimit") or None,
            target_bwlimit=job.get("target_bwlimit") or None,
            skip_parent=job.get("skip_parent", False),
            create_bookmark=job.get("create_bookmark", False),
            force_delete=job.get("force_delete", False),
            send_options="L" if job.get("large_blocks", False) else None,
            recv_options=job.get("receive_options") or None,
            additional_flags=job.get("additional_flags") or None,
            source_host=source_host,
            target_host=target_host,
            ssh_port=(
                ssh_profile["port"]
                if ssh_profile and ssh_profile["port"] != 22
                else None
            ),
            ssh_key=ssh_profile["identity_file"] if ssh_profile else None,
            ssh_options=ssh_profile["ssh_options"] if ssh_profile else None,
            progress_callback=record_progress,
        )
    except Exception as run_error:
        finish("failure", error_message=str(run_error))
        return {
            "success": False,
            "status": "failure",
            "error": str(run_error),
            "execution_id": execution_id,
        }

    if result.get("success"):
        stats = result.get("stats") or {}
        finish(
            "success",
            log_output=_combine_output(result),
            command=result.get("command"),
            bytes_transferred=stats.get("bytes_sent") or 0,
        )
        return {"success": True, "status": "success", "execution_id": execution_id}

    error_message = result.get("error") or result.get("stderr") or "Syncoid failed"
    finish(
        "failure",
        error_message=error_message,
        log_output=_combine_output(result),
        command=result.get("command"),
    )
    return {
        "success": False,
        "status": "failure",
        "error": error_message,
        "execution_id": execution_id,
    }


def _validate_job_datasets(job: Dict[str, Any]) -> Optional[str]:
    """Validate the job's source and target dataset names.

    Returns an error message when a name is invalid, or None when both
    are valid. A leading slash is called out specifically because it
    means the user entered a mountpoint path instead of a ZFS dataset
    name, the most common cause of "cannot receive: invalid name".
    """
    from services.zfs_dataset import ZFSDatasetService

    checks = [
        ("Source dataset", (job.get("source_dataset") or "").strip()),
        ("Target dataset", (job.get("target_dataset") or "").strip()),
    ]
    for field_label, dataset_name in checks:
        if not dataset_name:
            return f"{field_label} is empty. Edit the job and set a ZFS dataset name."
        if dataset_name.startswith("/"):
            return (
                f"{field_label} '{dataset_name}' is a mountpoint path, not a "
                "ZFS dataset name. Edit the job and use the dataset name "
                "without a leading slash (for example 'zdata' or "
                "'zdata/backups')."
            )
        try:
            ZFSDatasetService.validate_dataset_name(dataset_name)
        except ValueError as validation_error:
            return f"{field_label}: {validation_error}"
    return None


def _combine_output(result: Dict[str, Any], max_chars: int = 100000) -> str:
    """Combine stdout and stderr, truncated to keep the JSON store bounded."""
    stdout = result.get("stdout") or ""
    stderr = result.get("stderr") or ""
    combined = stdout
    if stderr:
        combined = f"{combined}\n--- stderr ---\n{stderr}" if combined else stderr
    if len(combined) > max_chars:
        combined = combined[:max_chars] + "\n... output truncated ..."
    return combined


def main() -> int:
    """CLI entry point used by systemd timers and cron entries."""
    parser = argparse.ArgumentParser(
        description="Run a saved WebZFS scheduled Syncoid job by ID."
    )
    parser.add_argument("--job-id", type=int, required=True, help="Syncoid job ID")
    parser.add_argument(
        "--trigger",
        default="scheduled",
        choices=["scheduled", "manual"],
        help="Trigger type recorded in execution history",
    )
    args = parser.parse_args()

    result = run_syncoid_job(args.job_id, trigger=args.trigger)

    if result.get("status") == "skipped":
        print(f"Job {args.job_id} skipped: {result.get('error')}")
        return 0
    if result.get("success"):
        print(f"Job {args.job_id} completed successfully "
              f"(execution #{result.get('execution_id')})")
        return 0

    print(f"Job {args.job_id} failed: {result.get('error')}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
