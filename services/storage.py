"""
File-Based Data Storage Service
Provides simple data persistence using JSON files and log files
No external dependencies - uses only Python standard library
"""
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import os
import threading


class FileStorageService:
    """Service for managing file-based data storage"""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize storage service
        
        Args:
            data_dir: Optional data directory path. If not provided, uses ~/.config/webzfs
        """
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            # Use ~/.config/webzfs
            home = Path.home()
            self.data_dir = home / '.config' / 'webzfs'
        
        self.history_file = self.data_dir / 'replication_history.json'
        self.progress_dir = self.data_dir / 'progress'
        self.notifications_file = self.data_dir / 'notification_log.json'
        self.log_file = self.data_dir / 'webzfs.log'
        self.syncoid_jobs_file = self.data_dir / 'syncoid_jobs.json'
        self.scrub_schedules_file = self.data_dir / 'scrub_schedules.json'
        self.health_schedules_file = self.data_dir / 'health_schedules.json'
        
        self._lock = threading.Lock()
        self._ensure_data_directory()
        # Cross-process lock file. Web workers and the scheduled job
        # runner (a separate process launched by systemd/cron) all
        # perform read-modify-write cycles on the JSON files, so a
        # threading.Lock alone is not sufficient (issue #194).
        self._lock_file = self.data_dir / 'storage.lock'
        self._initialize_files()

    @contextmanager
    def _interprocess_lock(self):
        """Acquire an exclusive advisory lock shared across processes.

        Uses fcntl.flock on a dedicated lock file so multiple Gunicorn
        workers and the syncoid job runner serialize read-modify-write
        cycles on the JSON data files. fcntl is available on Linux,
        FreeBSD, and NetBSD.
        """
        import fcntl
        with open(self._lock_file, 'a') as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
    
    def _ensure_data_directory(self) -> None:
        """Ensure the data directory exists"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.progress_dir.mkdir(parents=True, exist_ok=True)
    
    def _initialize_files(self) -> None:
        """Initialize data files if they don't exist"""
        if not self.history_file.exists():
            self._write_json(self.history_file, {'executions': [], 'next_id': 1})
        
        if not self.notifications_file.exists():
            self._write_json(self.notifications_file, {'notifications': []})
        
        if not self.syncoid_jobs_file.exists():
            self._write_json(self.syncoid_jobs_file, {'jobs': [], 'next_id': 1})
        
        if not self.scrub_schedules_file.exists():
            self._write_json(self.scrub_schedules_file, {'schedules': [], 'next_id': 1})
        
        if not self.health_schedules_file.exists():
            self._write_json(self.health_schedules_file, {'schedules': [], 'next_id': 1})
    
    def _read_json(self, file_path: Path) -> Dict[str, Any]:
        """Read JSON file with error handling"""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _write_json(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write JSON file atomically.

        Uses a unique temp file via tempfile.mkstemp so concurrent workers
        do not collide on a shared temp name during startup initialization.
        """
        import os
        import tempfile
        fd, temp_name = tempfile.mkstemp(dir=str(file_path.parent), suffix='.tmp')
        try:
            with os.fdopen(fd, 'w') as f:
                json.dump(data, f, indent=2)
            os.replace(temp_name, file_path)
        except BaseException:
            if os.path.exists(temp_name):
                os.unlink(temp_name)
            raise

    
    def _write_log(self, message: str) -> None:
        """Append to log file"""
        timestamp = datetime.now().isoformat()
        log_line = f"[{timestamp}] {message}\n"
        with open(self.log_file, 'a') as f:
            f.write(log_line)
    
    # Execution History Methods
    
    def create_execution_record(
        self,
        job_id: Optional[str],
        job_name: str,
        source_dataset: str,
        target_dataset: str,
        replication_type: str,
        command: Optional[str] = None
    ) -> int:
        """
        Create a new execution record
        
        Args:
            job_id: Optional job identifier
            job_name: Human-readable job name
            source_dataset: Source dataset path
            target_dataset: Target dataset path
            replication_type: Type of replication (local, push, pull)
            command: Full shell command string that was executed
        
        Returns:
            execution_id: ID of created execution record
        """
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.history_file)
            
            execution_id = data.get('next_id', 1)
            
            execution = {
                'id': execution_id,
                'job_id': job_id,
                'job_name': job_name,
                'source_dataset': source_dataset,
                'target_dataset': target_dataset,
                'replication_type': replication_type,
                'status': 'running',
                'started_at': datetime.now().isoformat(),
                'completed_at': None,
                'duration_seconds': None,
                'bytes_transferred': 0,
                'snapshot_name': None,
                'command': command,
                'error_message': None,
                'log_output': None
            }
            
            if 'executions' not in data:
                data['executions'] = []
            
            data['executions'].append(execution)
            data['next_id'] = execution_id + 1
            
            self._write_json(self.history_file, data)
            self._write_log(f"Started execution #{execution_id}: {job_name}")
            
            return execution_id
    
    def update_execution_record(
        self,
        execution_id: int,
        status: str,
        completed_at: Optional[str] = None,
        duration_seconds: Optional[float] = None,
        bytes_transferred: int = 0,
        snapshot_name: Optional[str] = None,
        command: Optional[str] = None,
        error_message: Optional[str] = None,
        log_output: Optional[str] = None
    ) -> None:
        """Update an execution record"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.history_file)
            
            for execution in data.get('executions', []):
                if execution['id'] == execution_id:
                    execution['status'] = status
                    execution['completed_at'] = completed_at
                    execution['duration_seconds'] = duration_seconds
                    execution['bytes_transferred'] = bytes_transferred
                    execution['snapshot_name'] = snapshot_name
                    if command is not None:
                        execution['command'] = command
                    execution['error_message'] = error_message
                    execution['log_output'] = log_output
                    break
            
            self._write_json(self.history_file, data)
            self._write_log(f"Execution #{execution_id} completed: {status}")
    
    def add_progress_update(
        self,
        execution_id: int,
        bytes_transferred: int,
        percentage_complete: float,
        transfer_rate: str,
        estimated_time_remaining: Optional[str] = None,
        status_message: Optional[str] = None
    ) -> None:
        """Add a progress update for an active transfer"""
        progress_file = self.progress_dir / f"execution_{execution_id}.json"
        
        with self._lock:
            if progress_file.exists():
                data = self._read_json(progress_file)
            else:
                data = {'execution_id': execution_id, 'updates': []}
            
            update = {
                'timestamp': datetime.now().isoformat(),
                'bytes_transferred': bytes_transferred,
                'percentage_complete': percentage_complete,
                'transfer_rate': transfer_rate,
                'estimated_time_remaining': estimated_time_remaining,
                'status_message': status_message
            }
            
            data['updates'].append(update)
            self._write_json(progress_file, data)
    
    def get_execution_history(
        self,
        job_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """Get execution history"""
        data = self._read_json(self.history_file)
        executions = data.get('executions', [])
        
        # Filter by job_id if provided
        if job_id:
            executions = [e for e in executions if e.get('job_id') == job_id]
        
        # Sort by started_at descending
        executions.sort(key=lambda x: x.get('started_at', ''), reverse=True)
        
        # Apply pagination
        return executions[offset:offset + limit]
    
    def get_execution_detail(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed execution record with progress updates"""
        data = self._read_json(self.history_file)
        
        # Find execution
        execution = None
        for e in data.get('executions', []):
            if e['id'] == execution_id:
                execution = e.copy()
                break
        
        if not execution:
            return None
        
        # Load progress updates
        progress_file = self.progress_dir / f"execution_{execution_id}.json"
        if progress_file.exists():
            progress_data = self._read_json(progress_file)
            execution['progress_updates'] = progress_data.get('updates', [])
        else:
            execution['progress_updates'] = []
        
        return execution
    
    def get_active_executions(self) -> List[Dict[str, Any]]:
        """Get all active (running) executions.

        Note: No automatic stale detection is performed here because
        large replications can legitimately run for days or weeks.
        Users can manually mark stale executions as failed from the
        execution detail page.
        """
        data = self._read_json(self.history_file)
        executions = data.get('executions', [])
        
        # Filter running executions
        active = [e for e in executions if e.get('status') == 'running']
        
        # Sort by started_at descending
        active.sort(key=lambda x: x.get('started_at', ''), reverse=True)
        
        return active

    def mark_execution_failed(
        self,
        execution_id: int,
        error_message: str = "Manually marked as failed by user"
    ) -> bool:
        """Mark a specific running execution as failed.

        This allows users to manually cancel/mark stale executions
        from the UI without waiting for automatic cleanup.

        Args:
            execution_id: The execution record ID.
            error_message: Reason for marking as failed.

        Returns:
            True if the execution was found and updated.
        """
        now = datetime.now()

        with self._lock:
            data = self._read_json(self.history_file)

            for execution in data.get('executions', []):
                if execution['id'] != execution_id:
                    continue
                if execution.get('status') != 'running':
                    return False

                started_at_str = execution.get('started_at', '')
                try:
                    started_at = datetime.fromisoformat(started_at_str)
                    duration = (now - started_at).total_seconds()
                except (ValueError, TypeError):
                    duration = None

                execution['status'] = 'failure'
                execution['completed_at'] = now.isoformat()
                execution['duration_seconds'] = duration
                execution['error_message'] = error_message

                self._write_json(self.history_file, data)
                self._write_log(
                    f"Execution #{execution_id} manually marked as "
                    f"failed: {error_message}"
                )
                return True

        return False
    
    def delete_execution_record(self, execution_id: int) -> bool:
        """Delete an execution record and its associated progress file.

        Args:
            execution_id: The execution record ID to delete.

        Returns:
            True if the record was found and deleted, False otherwise.
        """
        with self._lock:
            data = self._read_json(self.history_file)
            executions = data.get('executions', [])
            original_length = len(executions)

            # Prevent deleting running executions
            for e in executions:
                if e['id'] == execution_id and e.get('status') == 'running':
                    return False

            data['executions'] = [e for e in executions if e['id'] != execution_id]

            if len(data['executions']) < original_length:
                self._write_json(self.history_file, data)
                self._write_log(f"Deleted execution record #{execution_id}")

                # Remove associated progress file if it exists
                progress_file = self.progress_dir / f"execution_{execution_id}.json"
                if progress_file.exists():
                    progress_file.unlink()

                return True

        return False

    def delete_completed_execution_records(self) -> int:
        """Delete all non-running records from replication history."""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.history_file)
            executions = data.get('executions', [])
            deleted_count = sum(
                1
                for execution in executions
                if execution.get('status') != 'running'
            )

            if not deleted_count:
                return 0

            data['executions'] = [
                execution
                for execution in executions
                if execution.get('status') == 'running'
            ]
            self._write_json(self.history_file, data)
            self._write_log(
                f"Deleted {deleted_count} completed execution record(s)"
            )
            return deleted_count

    # Notification Methods
    
    def log_notification(
        self,
        execution_id: int,
        notification_type: str,
        recipient: str,
        subject: str,
        body: str,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """Log an email notification"""
        with self._lock:
            data = self._read_json(self.notifications_file)
            
            notification = {
                'execution_id': execution_id,
                'notification_type': notification_type,
                'recipient': recipient,
                'subject': subject,
                'body': body,
                'sent_at': datetime.now().isoformat(),
                'status': status,
                'error_message': error_message
            }
            
            if 'notifications' not in data:
                data['notifications'] = []
            
            data['notifications'].append(notification)
            self._write_json(self.notifications_file, data)
            self._write_log(f"Notification sent to {recipient}: {subject} ({status})")
    
    def get_notification_log(
        self,
        execution_id: Optional[int] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get notification log"""
        data = self._read_json(self.notifications_file)
        notifications = data.get('notifications', [])
        
        # Filter by execution_id if provided
        if execution_id is not None:
            notifications = [n for n in notifications if n.get('execution_id') == execution_id]
        
        # Sort by sent_at descending
        notifications.sort(key=lambda x: x.get('sent_at', ''), reverse=True)
        
        # Apply limit
        return notifications[:limit]
    
    # Maintenance Methods
    
    def cleanup_old_progress(self, days: int = 7) -> None:
        """Clean up old progress files"""
        cutoff = datetime.now().timestamp() - (days * 86400)
        
        for progress_file in self.progress_dir.glob('execution_*.json'):
            if progress_file.stat().st_mtime < cutoff:
                progress_file.unlink()
    
    # Syncoid Job Management Methods
    
    def create_syncoid_job(
        self,
        name: str,
        source_dataset: str,
        target_dataset: str,
        schedule: str,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None,
        ssh_port: int = 22,
        enabled: bool = True,
        recursive: bool = False,
        no_sync_snap: bool = False,
        compress: Optional[str] = None,
        source_bwlimit: Optional[str] = None,
        target_bwlimit: Optional[str] = None,
        skip_parent: bool = False,
        create_bookmark: bool = False,
        force_delete: bool = False,
        large_blocks: bool = False,
        additional_flags: Optional[str] = None,
        ssh_connection_id: Optional[str] = None,
        replication_type: str = 'local'
    ) -> int:
        """
        Create a new syncoid scheduled job

        The SSH Manager connection ID is stored rather than resolved
        host/key details so the managed connection record remains the
        single source of truth at execution time (issues #194, #195).

        Returns:
            job_id: ID of created job
        """
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.syncoid_jobs_file)
            
            job_id = data.get('next_id', 1)
            
            job = {
                'id': job_id,
                'name': name,
                'source_dataset': source_dataset,
                'target_dataset': target_dataset,
                'source_host': source_host,
                'target_host': target_host,
                'ssh_port': ssh_port,
                'schedule': schedule,
                'enabled': enabled,
                'recursive': recursive,
                'no_sync_snap': no_sync_snap,
                'compress': compress,
                'source_bwlimit': source_bwlimit,
                'target_bwlimit': target_bwlimit,
                'skip_parent': skip_parent,
                'create_bookmark': create_bookmark,
                'force_delete': force_delete,
                'large_blocks': large_blocks,
                'additional_flags': additional_flags or '',
                'ssh_connection_id': ssh_connection_id,
                'replication_type': replication_type,
                'last_run': None,
                'last_status': None,
                'next_run': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            if 'jobs' not in data:
                data['jobs'] = []
            
            data['jobs'].append(job)
            data['next_id'] = job_id + 1
            
            self._write_json(self.syncoid_jobs_file, data)
            self._write_log(f"Created syncoid job #{job_id}: {name}")
            
            return job_id
    
    def get_syncoid_jobs(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """Get all syncoid jobs"""
        data = self._read_json(self.syncoid_jobs_file)
        jobs = data.get('jobs', [])
        
        if enabled_only:
            jobs = [j for j in jobs if j.get('enabled', True)]
        
        # Sort by name
        jobs.sort(key=lambda x: x.get('name', ''))
        
        return jobs
    
    def get_syncoid_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific syncoid job"""
        data = self._read_json(self.syncoid_jobs_file)
        
        for job in data.get('jobs', []):
            if job['id'] == job_id:
                return job
        
        return None
    
    def update_syncoid_job(
        self,
        job_id: int,
        name: Optional[str] = None,
        source_dataset: Optional[str] = None,
        target_dataset: Optional[str] = None,
        schedule: Optional[str] = None,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None,
        ssh_port: Optional[int] = None,
        enabled: Optional[bool] = None,
        recursive: Optional[bool] = None,
        no_sync_snap: Optional[bool] = None,
        compress: Optional[str] = None,
        source_bwlimit: Optional[str] = None,
        target_bwlimit: Optional[str] = None,
        skip_parent: Optional[bool] = None,
        create_bookmark: Optional[bool] = None,
        force_delete: Optional[bool] = None,
        large_blocks: Optional[bool] = None,
        additional_flags: Optional[str] = None,
        ssh_connection_id: Optional[str] = None,
        replication_type: Optional[str] = None
    ) -> bool:
        """Update an existing syncoid job"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.syncoid_jobs_file)
            
            for job in data.get('jobs', []):
                if job['id'] == job_id:
                    if name is not None:
                        job['name'] = name
                    if source_dataset is not None:
                        job['source_dataset'] = source_dataset
                    if target_dataset is not None:
                        job['target_dataset'] = target_dataset
                    if schedule is not None:
                        job['schedule'] = schedule
                    if source_host is not None:
                        job['source_host'] = source_host
                    if target_host is not None:
                        job['target_host'] = target_host
                    if ssh_port is not None:
                        job['ssh_port'] = ssh_port
                    if enabled is not None:
                        job['enabled'] = enabled
                    if recursive is not None:
                        job['recursive'] = recursive
                    if no_sync_snap is not None:
                        job['no_sync_snap'] = no_sync_snap
                    if compress is not None:
                        job['compress'] = compress
                    if source_bwlimit is not None:
                        job['source_bwlimit'] = source_bwlimit
                    if target_bwlimit is not None:
                        job['target_bwlimit'] = target_bwlimit
                    if skip_parent is not None:
                        job['skip_parent'] = skip_parent
                    if create_bookmark is not None:
                        job['create_bookmark'] = create_bookmark
                    if force_delete is not None:
                        job['force_delete'] = force_delete
                    if large_blocks is not None:
                        job['large_blocks'] = large_blocks
                    if additional_flags is not None:
                        job['additional_flags'] = additional_flags
                    if ssh_connection_id is not None:
                        # Empty string clears the connection (local job)
                        job['ssh_connection_id'] = ssh_connection_id or None
                    if replication_type is not None:
                        job['replication_type'] = replication_type
                    
                    job['updated_at'] = datetime.now().isoformat()
                    
                    self._write_json(self.syncoid_jobs_file, data)
                    self._write_log(f"Updated syncoid job #{job_id}")
                    return True
            
            return False
    
    def update_syncoid_job_status(
        self,
        job_id: int,
        last_run: Optional[str] = None,
        last_status: Optional[str] = None,
        next_run: Optional[str] = None
    ) -> bool:
        """Update job execution status"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.syncoid_jobs_file)
            
            for job in data.get('jobs', []):
                if job['id'] == job_id:
                    if last_run is not None:
                        job['last_run'] = last_run
                    if last_status is not None:
                        job['last_status'] = last_status
                    if next_run is not None:
                        job['next_run'] = next_run
                    
                    self._write_json(self.syncoid_jobs_file, data)
                    return True
            
            return False
    
    def delete_syncoid_job(self, job_id: int) -> bool:
        """Delete a syncoid job"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.syncoid_jobs_file)
            
            jobs = data.get('jobs', [])
            original_length = len(jobs)
            
            data['jobs'] = [j for j in jobs if j['id'] != job_id]
            
            if len(data['jobs']) < original_length:
                self._write_json(self.syncoid_jobs_file, data)
                self._write_log(f"Deleted syncoid job #{job_id}")
                return True
            
            return False
    
    # Scrub Schedule Methods
    #
    # Moved out of views/utils_scrub.py for the Unified Scheduling Hub so
    # the schedule store shares the interprocess lock with the task
    # runner process. The file name and record shape are unchanged;
    # last_run, last_status, and next_run were added.
    
    def create_scrub_schedule(
        self,
        pool: str,
        schedule: str,
        enabled: bool = True
    ) -> int:
        """
        Create a new pool scrub schedule
        
        Args:
            pool: ZFS pool name to scrub
            schedule: 5-field cron expression
            enabled: Whether the schedule runs
        
        Returns:
            schedule_id: ID of created schedule
        """
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.scrub_schedules_file)
            
            schedule_id = data.get('next_id', 1)
            
            record = {
                'id': schedule_id,
                'pool': pool,
                'schedule': schedule,
                'enabled': enabled,
                'last_run': None,
                'last_status': None,
                'next_run': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            if 'schedules' not in data:
                data['schedules'] = []
            
            data['schedules'].append(record)
            data['next_id'] = schedule_id + 1
            
            self._write_json(self.scrub_schedules_file, data)
            self._write_log(f"Created scrub schedule #{schedule_id} for pool {pool}")
            
            return schedule_id
    
    def get_scrub_schedules(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """Get all scrub schedules"""
        data = self._read_json(self.scrub_schedules_file)
        schedules = data.get('schedules', [])
        
        if enabled_only:
            schedules = [s for s in schedules if s.get('enabled', True)]
        
        schedules.sort(key=lambda x: x.get('pool', ''))
        
        return schedules
    
    def get_scrub_schedule(self, schedule_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific scrub schedule"""
        data = self._read_json(self.scrub_schedules_file)
        
        for record in data.get('schedules', []):
            if record['id'] == schedule_id:
                return record
        
        return None
    
    def update_scrub_schedule(
        self,
        schedule_id: int,
        pool: Optional[str] = None,
        schedule: Optional[str] = None,
        enabled: Optional[bool] = None
    ) -> bool:
        """Update an existing scrub schedule"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.scrub_schedules_file)
            
            for record in data.get('schedules', []):
                if record['id'] == schedule_id:
                    if pool is not None:
                        record['pool'] = pool
                    if schedule is not None:
                        record['schedule'] = schedule
                    if enabled is not None:
                        record['enabled'] = enabled
                    
                    record['updated_at'] = datetime.now().isoformat()
                    
                    self._write_json(self.scrub_schedules_file, data)
                    self._write_log(f"Updated scrub schedule #{schedule_id}")
                    return True
            
            return False
    
    def update_scrub_schedule_status(
        self,
        schedule_id: int,
        last_run: Optional[str] = None,
        last_status: Optional[str] = None,
        next_run: Optional[str] = None
    ) -> bool:
        """Update scrub schedule execution status"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.scrub_schedules_file)
            
            for record in data.get('schedules', []):
                if record['id'] == schedule_id:
                    if last_run is not None:
                        record['last_run'] = last_run
                    if last_status is not None:
                        record['last_status'] = last_status
                    if next_run is not None:
                        record['next_run'] = next_run
                    
                    self._write_json(self.scrub_schedules_file, data)
                    return True
            
            return False
    
    def delete_scrub_schedule(self, schedule_id: int) -> bool:
        """Delete a scrub schedule"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.scrub_schedules_file)
            
            schedules = data.get('schedules', [])
            original_length = len(schedules)
            
            data['schedules'] = [s for s in schedules if s['id'] != schedule_id]
            
            if len(data['schedules']) < original_length:
                self._write_json(self.scrub_schedules_file, data)
                self._write_log(f"Deleted scrub schedule #{schedule_id}")
                return True
            
            return False
    
    # Health Check Schedule Methods
    
    def create_health_schedule(
        self,
        name: str,
        schedule: str,
        enabled: bool = True,
        check_disk_health: bool = True,
        check_smart_tests: bool = True,
        check_scrubs: bool = True,
        aggressive_hours: bool = False
    ) -> int:
        """
        Create a new health check schedule
        
        Args:
            name: Human-readable schedule name
            schedule: 5-field cron expression
            enabled: Whether the schedule runs
            check_disk_health: Include disk health checks
            check_smart_tests: Include SMART test verification
            check_scrubs: Include scrub verification
            aggressive_hours: Use aggressive power-on hour thresholds
        
        Returns:
            schedule_id: ID of created schedule
        """
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.health_schedules_file)
            
            schedule_id = data.get('next_id', 1)
            
            record = {
                'id': schedule_id,
                'name': name,
                'schedule': schedule,
                'enabled': enabled,
                'check_disk_health': check_disk_health,
                'check_smart_tests': check_smart_tests,
                'check_scrubs': check_scrubs,
                'aggressive_hours': aggressive_hours,
                'last_run': None,
                'last_status': None,
                'last_report_id': None,
                'next_run': None,
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            if 'schedules' not in data:
                data['schedules'] = []
            
            data['schedules'].append(record)
            data['next_id'] = schedule_id + 1
            
            self._write_json(self.health_schedules_file, data)
            self._write_log(f"Created health schedule #{schedule_id}: {name}")
            
            return schedule_id
    
    def get_health_schedules(self, enabled_only: bool = False) -> List[Dict[str, Any]]:
        """Get all health check schedules"""
        data = self._read_json(self.health_schedules_file)
        schedules = data.get('schedules', [])
        
        if enabled_only:
            schedules = [s for s in schedules if s.get('enabled', True)]
        
        schedules.sort(key=lambda x: x.get('name', ''))
        
        return schedules
    
    def get_health_schedule(self, schedule_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific health check schedule"""
        data = self._read_json(self.health_schedules_file)
        
        for record in data.get('schedules', []):
            if record['id'] == schedule_id:
                return record
        
        return None
    
    def update_health_schedule(
        self,
        schedule_id: int,
        name: Optional[str] = None,
        schedule: Optional[str] = None,
        enabled: Optional[bool] = None,
        check_disk_health: Optional[bool] = None,
        check_smart_tests: Optional[bool] = None,
        check_scrubs: Optional[bool] = None,
        aggressive_hours: Optional[bool] = None
    ) -> bool:
        """Update an existing health check schedule"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.health_schedules_file)
            
            for record in data.get('schedules', []):
                if record['id'] == schedule_id:
                    if name is not None:
                        record['name'] = name
                    if schedule is not None:
                        record['schedule'] = schedule
                    if enabled is not None:
                        record['enabled'] = enabled
                    if check_disk_health is not None:
                        record['check_disk_health'] = check_disk_health
                    if check_smart_tests is not None:
                        record['check_smart_tests'] = check_smart_tests
                    if check_scrubs is not None:
                        record['check_scrubs'] = check_scrubs
                    if aggressive_hours is not None:
                        record['aggressive_hours'] = aggressive_hours
                    
                    record['updated_at'] = datetime.now().isoformat()
                    
                    self._write_json(self.health_schedules_file, data)
                    self._write_log(f"Updated health schedule #{schedule_id}")
                    return True
            
            return False
    
    def update_health_schedule_status(
        self,
        schedule_id: int,
        last_run: Optional[str] = None,
        last_status: Optional[str] = None,
        next_run: Optional[str] = None,
        last_report_id: Optional[str] = None
    ) -> bool:
        """Update health schedule execution status"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.health_schedules_file)
            
            for record in data.get('schedules', []):
                if record['id'] == schedule_id:
                    if last_run is not None:
                        record['last_run'] = last_run
                    if last_status is not None:
                        record['last_status'] = last_status
                    if next_run is not None:
                        record['next_run'] = next_run
                    if last_report_id is not None:
                        record['last_report_id'] = last_report_id
                    
                    self._write_json(self.health_schedules_file, data)
                    return True
            
            return False
    
    def delete_health_schedule(self, schedule_id: int) -> bool:
        """Delete a health check schedule"""
        with self._lock, self._interprocess_lock():
            data = self._read_json(self.health_schedules_file)
            
            schedules = data.get('schedules', [])
            original_length = len(schedules)
            
            data['schedules'] = [s for s in schedules if s['id'] != schedule_id]
            
            if len(data['schedules']) < original_length:
                self._write_json(self.health_schedules_file, data)
                self._write_log(f"Deleted health schedule #{schedule_id}")
                return True
            
            return False
