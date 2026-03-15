"""
ZFS Replication Management Service
Handles snapshot replication scheduling and execution similar to syncoid/sanoid
Reference: https://github.com/jimsalterjrs/sanoid
Hi Jim. :)
"""
import subprocess
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum
from services.storage import FileStorageService
from services.email_notification import EmailNotificationService
from services.utils import run_zfs_command, build_zfs_command, run_zfs_command_with_pipe


class ReplicationType(Enum):
    """Types of replication"""
    PUSH = "push"  # Local -> Remote
    PULL = "pull"  # Remote -> Local
    LOCAL = "local"  # Local -> Local


class CompressionMethod(Enum):
    """Compression methods for replication"""
    NONE = "none"
    LZ4 = "lz4"
    GZIP = "gzip"
    ZSTD = "zstd"


class ZFSReplicationService:
    """Service for managing ZFS replication jobs and execution"""
    
    def __init__(self):
        """Initialize the replication service"""
        # Note: Job configuration is currently stored in-memory
        # TODO: Consider persisting job config to JSON files if needed
        self._jobs = {}
        self._history = []
        
        # Initialize file storage and email services
        self.storage = FileStorageService()
        self.email = EmailNotificationService()
    
    def list_replication_jobs(self) -> List[Dict[str, Any]]:
        """
        List all configured replication jobs
        
        Returns:
            List of replication job configurations
        """
        return list(self._jobs.values())
    
    def get_replication_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get details of a specific replication job
        
        Args:
            job_id: Unique identifier for the job
            
        Returns:
            Job configuration dictionary
            
        Raises:
            KeyError: If job_id not found
        """
        if job_id not in self._jobs:
            raise KeyError(f"Replication job {job_id} not found")
        return self._jobs[job_id]
    
    def create_replication_job(
        self,
        name: str,
        source_dataset: str,
        target_dataset: str,
        replication_type: ReplicationType,
        schedule: str,
        enabled: bool = True,
        recursive: bool = False,
        compression: CompressionMethod = CompressionMethod.LZ4,
        **options
    ) -> str:
        """
        Create a new replication job
        
        Args:
            name: Human-readable name for the job
            source_dataset: Source ZFS dataset
            target_dataset: Target ZFS dataset
            replication_type: Type of replication (push/pull/local)
            schedule: Cron-style schedule expression
            enabled: Whether job is enabled
            recursive: Replicate child datasets recursively
            compression: Compression method to use
            **options: Additional options:
                - remote_host: str (for push/pull)
                - remote_port: int
                - ssh_key: str
                - bandwidth_limit: str
                - skip_parent: bool
                - preserve_properties: bool
                - use_bookmarks: bool
                - force: bool (use -F flag on receive)
                
        Returns:
            job_id: Unique identifier for the created job
        """
        import uuid
        job_id = str(uuid.uuid4())
        
        job = {
            'id': job_id,
            'name': name,
            'source_dataset': source_dataset,
            'target_dataset': target_dataset,
            'replication_type': replication_type.value,
            'schedule': schedule,
            'enabled': enabled,
            'recursive': recursive,
            'compression': compression.value,
            'options': options,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
        }
        
        self._jobs[job_id] = job
        return job_id
    
    def update_replication_job(self, job_id: str, **updates) -> None:
        """
        Update an existing replication job
        
        Args:
            job_id: Job identifier
            **updates: Fields to update
        """
        if job_id not in self._jobs:
            raise KeyError(f"Replication job {job_id} not found")
        
        # Handle enum conversions
        if 'replication_type' in updates and isinstance(updates['replication_type'], str):
            updates['replication_type'] = updates['replication_type']
        if 'compression' in updates and isinstance(updates['compression'], str):
            updates['compression'] = updates['compression']
        
        self._jobs[job_id].update(updates)
        self._jobs[job_id]['updated_at'] = datetime.now().isoformat()
    
    def delete_replication_job(self, job_id: str) -> None:
        """
        Delete a replication job
        
        Args:
            job_id: Job identifier
        """
        if job_id not in self._jobs:
            raise KeyError(f"Replication job {job_id} not found")
        del self._jobs[job_id]
    
    def enable_job(self, job_id: str) -> None:
        """Enable a replication job"""
        self.update_replication_job(job_id, enabled=True)
    
    def disable_job(self, job_id: str) -> None:
        """Disable a replication job"""
        self.update_replication_job(job_id, enabled=False)
    
    def _check_target_exists(self, target: str) -> bool:
        """
        Check if the target dataset exists
        
        Args:
            target: Target dataset name
            
        Returns:
            True if target exists, False otherwise
        """
        try:
            run_zfs_command(['zfs', 'list', '-H', target], check=True)
            return True
        except subprocess.CalledProcessError:
            return False
    
    def execute_replication(
        self,
        source: str,
        target: str,
        replication_type: ReplicationType,
        incremental: bool = True,
        recursive: bool = False,
        raw: bool = False,
        compression: CompressionMethod = CompressionMethod.LZ4,
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        force: Optional[bool] = None,
        **options
    ) -> Dict[str, Any]:
        """
        Execute a one-time replication job
        
        Args:
            source: Source dataset (snapshot name, e.g., pool/dataset@snap)
            target: Target dataset
            replication_type: Type of replication
            incremental: Use incremental send
            recursive: Replicate recursively
            raw: Use raw send (-w flag). Required for encrypted datasets
                 to preserve encryption on the target side.
            compression: Compression method
            job_id: Optional job ID for scheduled jobs
            job_name: Optional job name
            force: Use -F flag on receive to overwrite existing dataset.
                   If None, automatically use -F when target exists.
            **options: Additional options
            
        Returns:
            Execution results including bytes transferred, time taken, etc.
        """
        start_time = datetime.now()
        
        # Create execution record in storage
        execution_id = self.storage.create_execution_record(
            job_id=job_id,
            job_name=job_name or f"{source} → {target}",
            source_dataset=source,
            target_dataset=target,
            replication_type=replication_type.value
        )
        
        try:
            # Determine if source is already a snapshot or a dataset
            if '@' in source:
                # Source is already a snapshot
                latest_snapshot = source
            else:
                # Get list of snapshots for the dataset
                snapshots = self._get_snapshots(source)
                
                if not snapshots:
                    raise Exception(f"No snapshots found for {source}")
                
                latest_snapshot = snapshots[-1]
            
            # Determine the actual receive target and -F flag based on
            # whether this is a full or incremental send.
            #
            # Full (non-incremental) send:
            #   - Creates a NEW child dataset under the selected target.
            #     e.g. source "olympus/test1@first" + target "phobos"
            #     -> receive into "phobos/test1"
            #
            # Incremental send:
            #   - Writes directly into the selected target dataset,
            #     which must already exist from a prior full send.
            #   - The -i flag with the common and new snapshots is
            #     sufficient; no -F flag is needed.
            if not incremental:
                # Extract the source dataset basename to build the
                # child target path.  e.g. "olympus/test1" -> "test1"
                source_dataset = source.split('@')[0] if '@' in source else source
                source_basename = source_dataset.split('/')[-1]
                actual_target = f"{target}/{source_basename}"
            else:
                actual_target = target
            
            # Never use -F. Full sends create a new child dataset
            # (no overwrite needed). Incremental sends apply cleanly
            # when the common snapshot and new snapshot are specified.
            force = False
            
            # Merge force into options
            options_with_force = dict(options)
            options_with_force['force'] = force
            
            # For incremental send, find the common/base snapshot.
            # Both the source and target must share at least one snapshot
            # for incremental replication to work. If no common snapshot
            # is found, we raise an error rather than silently falling back
            # to a full send, because a full send with -F would overwrite
            # the target dataset and destroy any existing data.
            base_snapshot = None
            if incremental:
                base_snapshot = self._find_common_snapshot(
                    source, target, replication_type, options_with_force
                )
                if not base_snapshot:
                    raise Exception(
                        "Incremental replication requested but no common "
                        "snapshot was found between the source and target "
                        "datasets. Both systems must share at least one "
                        "snapshot for incremental send to work. Either "
                        "perform a full (non-incremental) send first, or "
                        "verify that the target dataset has a snapshot "
                        "matching one on the source."
                    )
            
            # Build the send command
            send_cmd = self._build_send_command(
                source, latest_snapshot, incremental, recursive, raw,
                compression, base_snapshot=base_snapshot
            )
            
            # Build the receive command using the actual target
            # (child path for full sends, direct path for incremental)
            receive_cmd = self._build_receive_command(
                actual_target, replication_type, options_with_force
            )
            
            # Build the full command string for history/audit purposes
            if replication_type == ReplicationType.LOCAL:
                command_string = ' '.join(send_cmd) + ' | ' + ' '.join(receive_cmd)
            else:
                remote_host = options_with_force.get('remote_host', '')
                remote_port = options_with_force.get('remote_port', 22)
                ssh_key = options_with_force.get('ssh_key')
                ssh_parts = ['ssh', '-p', str(remote_port)]
                if ssh_key:
                    ssh_parts.extend(['-i', ssh_key])
                ssh_parts.append(remote_host)
                ssh_parts.extend(receive_cmd)
                command_string = ' '.join(send_cmd) + ' | ' + ' '.join(ssh_parts)
            
            # Update execution record with the command
            self.storage.update_execution_record(
                execution_id=execution_id,
                status='running',
                command=command_string
            )
            
            # Execute replication
            if replication_type == ReplicationType.LOCAL:
                result = self._execute_local_replication(send_cmd, receive_cmd, execution_id)
            else:
                result = self._execute_remote_replication(
                    send_cmd, receive_cmd, replication_type, options_with_force, execution_id
                )
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Update execution record with success
            self.storage.update_execution_record(
                execution_id=execution_id,
                status='success',
                completed_at=end_time.isoformat(),
                duration_seconds=duration,
                bytes_transferred=result.get('bytes', 0),
                snapshot_name=latest_snapshot,
                log_output=result.get('log_output', '')
            )
            
            # Send success notification if enabled
            notification_result = self.email.send_job_success_notification(
                job_name=job_name or f"{source} → {target}",
                source_dataset=source,
                target_dataset=target,
                execution_id=execution_id,
                bytes_transferred=result.get('bytes', 0),
                duration=duration
            )
            
            # Log notification
            if notification_result['status'] == 'sent':
                self.storage.log_notification(
                    execution_id=execution_id,
                    notification_type='success',
                    recipient=', '.join(notification_result.get('recipients', [])),
                    subject=f"ZFS Replication Succeeded: {job_name or f'{source} → {target}'}",
                    body='Success notification sent',
                    status='sent'
                )
            
            return {
                'status': 'success',
                'source': source,
                'target': target,
                'snapshot': latest_snapshot,
                'started_at': start_time.isoformat(),
                'completed_at': end_time.isoformat(),
                'duration_seconds': duration,
                'bytes_transferred': result.get('bytes', 0),
                'average_speed': result.get('speed', 'N/A'),
                'execution_id': execution_id
            }
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            error_message = str(e)
            
            # Update execution record with failure
            self.storage.update_execution_record(
                execution_id=execution_id,
                status='failure',
                completed_at=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_message,
                log_output=error_message
            )
            
            # Send failure notification
            notification_result = self.email.send_job_failure_notification(
                job_name=job_name or f"{source} → {target}",
                source_dataset=source,
                target_dataset=target,
                error_message=error_message,
                execution_id=execution_id,
                duration=duration
            )
            
            # Log notification
            if notification_result['status'] == 'sent':
                self.storage.log_notification(
                    execution_id=execution_id,
                    notification_type='failure',
                    recipient=', '.join(notification_result.get('recipients', [])),
                    subject=f"ZFS Replication Failed: {job_name or f'{source} → {target}'}",
                    body=error_message,
                    status='sent'
                )
            elif notification_result['status'] == 'failed':
                self.storage.log_notification(
                    execution_id=execution_id,
                    notification_type='failure',
                    recipient='N/A',
                    subject=f"ZFS Replication Failed: {job_name or f'{source} → {target}'}",
                    body=error_message,
                    status='failed',
                    error_message=notification_result.get('error', 'Unknown error')
                )
            
            return {
                'status': 'failure',
                'source': source,
                'target': target,
                'started_at': start_time.isoformat(),
                'completed_at': end_time.isoformat(),
                'duration_seconds': duration,
                'error': error_message,
                'execution_id': execution_id
            }
    
    def get_replication_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get current status of a replication job
        
        Args:
            job_id: Job identifier
            
        Returns:
            Status information including last run, next run, etc.
        """
        job = self.get_replication_job(job_id)
        
        # Get last execution from history
        job_history = [h for h in self._history if h.get('job_id') == job_id]
        last_run = job_history[-1] if job_history else None
        
        return {
            'job_id': job_id,
            'name': job['name'],
            'enabled': job['enabled'],
            'last_run': last_run.get('started_at') if last_run else None,
            'last_status': last_run.get('status') if last_run else None,
            'next_run': self._calculate_next_run(job['schedule']),
        }
    
    def get_replication_history(
        self,
        job_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Get replication execution history from storage
        
        Args:
            job_id: Optional job ID to filter by
            limit: Maximum number of entries to return
            offset: Number of entries to skip
            
        Returns:
            List of execution history entries
        """
        return self.storage.get_execution_history(job_id=job_id, limit=limit, offset=offset)
    
    def get_execution_detail(self, execution_id: int) -> Optional[Dict[str, Any]]:
        """
        Get detailed execution record with progress updates
        
        Args:
            execution_id: Execution record ID
            
        Returns:
            Detailed execution record
        """
        return self.storage.get_execution_detail(execution_id)
    
    def get_active_executions(self) -> List[Dict[str, Any]]:
        """
        Get all active (running) executions
        
        Returns:
            List of active execution records
        """
        return self.storage.get_active_executions()
    
    def test_connection(
        self,
        remote_host: str,
        remote_port: int = 22,
        ssh_key: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Test SSH connection to remote host
        
        Args:
            remote_host: Remote hostname or IP
            remote_port: SSH port
            ssh_key: Path to SSH private key
            
        Returns:
            Connection test results
        """
        try:
            cmd = ['ssh', '-p', str(remote_port)]
            if ssh_key:
                cmd.extend(['-i', ssh_key])
            cmd.extend([remote_host, 'echo "Connection successful"'])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=10,
                check=True
            )
            
            return {
                'status': 'success',
                'message': 'Connection successful',
                'output': result.stdout.strip()
            }
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'failure',
                'message': 'Connection timed out'
            }
        except subprocess.CalledProcessError as e:
            return {
                'status': 'failure',
                'message': f'Connection failed: {e.stderr}'
            }
    
    def estimate_transfer_size(
        self,
        source: str,
        target: str,
        incremental: bool = True
    ) -> Dict[str, Any]:
        """
        Estimate the size of data to be transferred
        
        Args:
            source: Source dataset
            target: Target dataset
            incremental: Whether to do incremental send
            
        Returns:
            Size estimation
        """
        try:
            # Get latest snapshot
            snapshots = self._get_snapshots(source)
            if not snapshots:
                raise Exception(f"No snapshots found for {source}")
            
            latest = snapshots[-1]
            
            # Use zfs send with dry-run to estimate size
            cmd = ['zfs', 'send', '-nv']
            if incremental and len(snapshots) > 1:
                cmd.extend(['-i', snapshots[-2]])
            cmd.append(latest)
            
            result = run_zfs_command(cmd)
            
            # Parse output for size
            # Output format: "size	12345678"
            size_bytes = 0
            for line in result.stderr.split('\n'):
                if 'size' in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        size_bytes = int(parts[1])
            
            return {
                'source': source,
                'target': target,
                'snapshot': latest,
                'incremental': incremental,
                'estimated_bytes': size_bytes,
                'estimated_size': self._format_bytes(size_bytes)
            }
            
        except Exception as e:
            return {
                'error': str(e)
            }
    
    # Private helper methods
    
    def _get_snapshots(self, dataset: str, recursive: bool = False) -> List[str]:
        """Get list of snapshots for a dataset.
        
        Args:
            dataset: ZFS dataset name
            recursive: If True, include snapshots from child datasets.
                       If False (default), return only snapshots belonging
                       to the exact dataset specified.
        
        Returns:
            Ordered list of snapshot names (e.g. pool/data@snap1)
        """
        try:
            cmd = ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name']
            if recursive:
                cmd.append('-r')
            cmd.append(dataset)
            result = run_zfs_command(cmd)
            all_snaps = [line.strip() for line in result.stdout.split('\n') if line.strip()]
            
            if recursive:
                return all_snaps
            
            # Without -r, ZFS still returns child dataset snapshots on some
            # platforms. Filter to only exact dataset matches to be safe.
            return [s for s in all_snaps if s.split('@')[0] == dataset]
        except subprocess.CalledProcessError:
            return []
    
    def _find_common_snapshot(
        self, source: str, target: str,
        replication_type: ReplicationType, options: Dict
    ) -> Optional[str]:
        """
        Find the most recent common snapshot between source and target.
        
        For incremental send (zfs send -i base@snap new@snap), the base
        snapshot must:
          1. Exist on the source dataset (same filesystem, not a child)
          2. Exist on the target dataset (same filesystem, not a child)
        
        This method uses NON-recursive snapshot listings so that only
        direct snapshots of the source and target datasets are compared.
        Child dataset snapshots are excluded to prevent false matches
        that would produce "incremental source must be in same filesystem".
        
        Args:
            source: Source dataset (may include @snapshot)
            target: Target dataset
            replication_type: Type of replication
            options: Additional options including remote_host, etc.
            
        Returns:
            The full source snapshot name (e.g. pool/data@snap) for the
            most recent common snapshot, or None if none exists.
        """
        # Extract dataset name from source (remove @snapshot if present)
        source_dataset = source.split('@')[0] if '@' in source else source
        
        # Get snapshots for the exact source dataset only (non-recursive).
        # This ensures we only get Mpool/music@... not Mpool/music/child@...
        source_snapshots = self._get_snapshots(source_dataset, recursive=False)
        
        if not source_snapshots:
            return None
        
        # Build a set of snapshot short names from the source
        source_snap_names = set()
        for snap in source_snapshots:
            if '@' in snap:
                source_snap_names.add(snap.split('@')[1])
        
        if not source_snap_names:
            return None
        
        # Get snapshots for the exact target dataset only (non-recursive).
        # For local targets, query directly. For remote, query via SSH.
        if replication_type == ReplicationType.LOCAL:
            target_snapshots = self._get_snapshots(target, recursive=False)
        else:
            target_snapshots = self._get_remote_snapshots(
                target, options, recursive=False
            )
        
        # Build a set of snapshot short names from the target
        target_snap_names = set()
        for snap in target_snapshots:
            if '@' in snap:
                target_snap_names.add(snap.split('@')[1])
        
        # Find snapshot names that exist on both source and target
        common_snap_names = source_snap_names & target_snap_names
        
        if not common_snap_names:
            return None
        
        # Return the most recent common snapshot from source (preserves order).
        # The source list is ordered oldest-first, so iterate in reverse.
        for snap in reversed(source_snapshots):
            if '@' in snap:
                snap_name = snap.split('@')[1]
                if snap_name in common_snap_names:
                    return snap
        
        return None
    
    def _get_remote_snapshots(
        self, dataset: str, options: Dict, recursive: bool = False
    ) -> List[str]:
        """
        Get list of snapshots from a remote dataset via SSH.
        
        Args:
            dataset: Remote dataset name
            options: Options including remote_host, remote_port, ssh_key
            recursive: If True, include child dataset snapshots.
                       If False (default), return only snapshots belonging
                       to the exact dataset specified.
            
        Returns:
            List of snapshot names
        """
        remote_host = options.get('remote_host')
        remote_port = options.get('remote_port', 22)
        ssh_key = options.get('ssh_key')
        
        if not remote_host:
            return []
        
        try:
            ssh_cmd = ['ssh', '-p', str(remote_port)]
            if ssh_key:
                ssh_cmd.extend(['-i', ssh_key])
            ssh_cmd.extend([
                '-o', 'StrictHostKeyChecking=no',
                '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'BatchMode=yes',
                '-o', 'ConnectTimeout=10',
                remote_host,
                'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name'
            ])
            if recursive:
                ssh_cmd.append('-r')
            ssh_cmd.append(dataset)
            
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30,
                check=False
            )
            
            if result.returncode == 0:
                all_snaps = [line.strip() for line in result.stdout.split('\n') if line.strip()]
                if recursive:
                    return all_snaps
                # Filter to only exact dataset matches
                return [s for s in all_snaps if s.split('@')[0] == dataset]
            return []
            
        except Exception:
            return []
    
    def _build_send_command(
        self, dataset: str, snapshot: str, incremental: bool,
        recursive: bool, raw: bool, compression: CompressionMethod,
        base_snapshot: Optional[str] = None
    ) -> List[str]:
        """Build the zfs send command
        
        Args:
            dataset: Source dataset name
            snapshot: The snapshot to send
            incremental: Whether to do incremental send
            recursive: Whether to include child datasets
            raw: Whether to use raw send (-w). Required for encrypted
                 datasets to preserve encryption on the receiving side.
            compression: Compression method
            base_snapshot: For incremental send, the base snapshot to send from
            
        Returns:
            List of command arguments for zfs send
        """
        cmd = ['zfs', 'send']
        
        if recursive:
            cmd.append('-R')
        
        # Raw send (-w) must be explicitly enabled by the user.
        # This is required when replicating encrypted datasets so that
        # the data is sent in its encrypted form and the receiving side
        # preserves the encryption properties.
        if raw:
            cmd.append('-w')
        
        # Add compressed send if compression is not NONE
        if compression != CompressionMethod.NONE:
            cmd.append('-c')
        
        # For incremental send, use -i flag with base snapshot
        if incremental and base_snapshot:
            cmd.extend(['-i', base_snapshot])
        
        cmd.append(snapshot)
        return cmd
    
    def _build_receive_command(
        self, target: str, replication_type: ReplicationType, options: Dict
    ) -> List[str]:
        """Build the zfs receive command"""
        cmd = ['zfs', 'receive']
        
        # Use -F flag to overwrite existing dataset if force is True
        # This is needed when the target dataset already exists
        if options.get('force', False):
            cmd.append('-F')
        
        cmd.append(target)
        return cmd
    
    def _execute_local_replication(
        self, send_cmd: List[str], receive_cmd: List[str], execution_id: int
    ) -> Dict[str, Any]:
        """Execute local replication using pipes with platform-appropriate sudo.
        
        Pipes zfs send stdout into zfs receive stdin. Both processes' stderr
        streams are captured so that when the receive side reports a generic
        'failed to read from stream' error we can surface the real cause from
        the send side.
        """
        # Build commands with sudo if needed (Linux)
        full_send_cmd = build_zfs_command(send_cmd)
        full_receive_cmd = build_zfs_command(receive_cmd)
        
        send_process = subprocess.Popen(
            full_send_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        receive_process = subprocess.Popen(
            full_receive_cmd,
            stdin=send_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Allow send_process to receive SIGPIPE if receive_process exits
        send_process.stdout.close()
        
        # Wait for receive to finish, then wait for send to finish
        receive_output, receive_error = receive_process.communicate()
        send_process.wait()
        send_error = send_process.stderr.read()
        send_process.stderr.close()
        
        send_error_text = send_error.decode().strip() if send_error else ''
        receive_error_text = receive_error.decode().strip() if receive_error else ''
        
        # Check for failures on either side
        if receive_process.returncode != 0 or send_process.returncode != 0:
            # Build an error message that includes both sides of the pipe
            error_parts = []
            if send_process.returncode != 0 and send_error_text:
                error_parts.append(f"Send failed: {send_error_text}")
            if receive_process.returncode != 0 and receive_error_text:
                error_parts.append(f"Receive failed: {receive_error_text}")
            
            # If send failed but we only got the generic receive error, lead
            # with the send error because it is the actual root cause
            if not error_parts:
                if send_process.returncode != 0:
                    error_parts.append(f"Send process exited with code {send_process.returncode}")
                if receive_process.returncode != 0:
                    error_parts.append(f"Receive process exited with code {receive_process.returncode}")
            
            raise Exception(' | '.join(error_parts))
        
        # Combine any informational stderr output from both sides
        log_parts = []
        if send_error_text:
            log_parts.append(send_error_text)
        if receive_error_text:
            log_parts.append(receive_error_text)
        log_output = '\n'.join(log_parts)
        
        return {'bytes': 0, 'speed': 'N/A', 'log_output': log_output}
    
    def _execute_remote_replication(
        self, send_cmd: List[str], receive_cmd: List[str],
        replication_type: ReplicationType, options: Dict, execution_id: int
    ) -> Dict[str, Any]:
        """Execute remote replication over SSH.
        
        Pipes zfs send stdout through SSH into zfs receive on the remote host.
        Both the local send process and remote SSH process stderr streams are
        captured so that when the remote receive reports a generic error we can
        surface the real cause from the local send side.
        """
        remote_host = options.get('remote_host')
        remote_port = options.get('remote_port', 22)
        ssh_key = options.get('ssh_key')
        
        if not remote_host:
            raise Exception("remote_host required for remote replication")
        
        # Build send command with sudo if needed (Linux)
        full_send_cmd = build_zfs_command(send_cmd)
        
        # Build SSH command - the receive command runs on the remote system
        # so we don't add sudo here (remote system handles its own permissions)
        ssh_cmd = ['ssh', '-p', str(remote_port)]
        if ssh_key:
            ssh_cmd.extend(['-i', ssh_key])
        ssh_cmd.extend([
            '-o', 'StrictHostKeyChecking=no',
            '-o', 'UserKnownHostsFile=/dev/null',
            '-o', 'BatchMode=yes'
        ])
        ssh_cmd.append(remote_host)
        ssh_cmd.extend(receive_cmd)
        
        # Execute send | ssh receive
        send_process = subprocess.Popen(
            full_send_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        ssh_process = subprocess.Popen(
            ssh_cmd,
            stdin=send_process.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Allow send_process to receive SIGPIPE if ssh_process exits
        send_process.stdout.close()
        
        # Wait for SSH/receive to finish, then wait for send to finish
        ssh_output, ssh_error = ssh_process.communicate()
        send_process.wait()
        send_error = send_process.stderr.read()
        send_process.stderr.close()
        
        send_error_text = send_error.decode().strip() if send_error else ''
        ssh_error_text = ssh_error.decode().strip() if ssh_error else ''
        
        # Check for failures on either side
        if ssh_process.returncode != 0 or send_process.returncode != 0:
            error_parts = []
            if send_process.returncode != 0 and send_error_text:
                error_parts.append(f"Send failed: {send_error_text}")
            if ssh_process.returncode != 0 and ssh_error_text:
                error_parts.append(f"Remote receive failed: {ssh_error_text}")
            
            if not error_parts:
                if send_process.returncode != 0:
                    error_parts.append(f"Send process exited with code {send_process.returncode}")
                if ssh_process.returncode != 0:
                    error_parts.append(f"SSH/receive process exited with code {ssh_process.returncode}")
            
            raise Exception(' | '.join(error_parts))
        
        # Combine any informational stderr output from both sides
        log_parts = []
        if send_error_text:
            log_parts.append(send_error_text)
        if ssh_error_text:
            log_parts.append(ssh_error_text)
        log_output = '\n'.join(log_parts)
        
        return {'bytes': 0, 'speed': 'N/A', 'log_output': log_output}
    
    def _calculate_next_run(self, schedule: str) -> Optional[str]:
        """Calculate next run time from cron schedule"""
        # Simplified implementation - would use croniter in production
        return "Next run calculation not implemented"
    
    def _format_bytes(self, bytes: int) -> str:
        """Format bytes to human-readable string"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} PB"
