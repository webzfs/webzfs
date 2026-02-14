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
            
            # Auto-detect if we need -F flag when target exists
            if force is None:
                force = self._check_target_exists(target)
            
            # Merge force into options
            options_with_force = dict(options)
            options_with_force['force'] = force
            
            # For incremental send, find the common/base snapshot
            base_snapshot = None
            if incremental:
                base_snapshot = self._find_common_snapshot(source, target, replication_type, options_with_force)
                # If no common snapshot found, fall back to full send
                if not base_snapshot:
                    incremental = False
            
            # Build the send command
            send_cmd = self._build_send_command(
                source, latest_snapshot, incremental, recursive, compression,
                base_snapshot=base_snapshot
            )
            
            # Build the receive command
            receive_cmd = self._build_receive_command(
                target, replication_type, options_with_force
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
    
    def _get_snapshots(self, dataset: str) -> List[str]:
        """Get list of snapshots for a dataset"""
        try:
            result = run_zfs_command(
                ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', dataset]
            )
            return [line.strip() for line in result.stdout.split('\n') if line.strip()]
        except subprocess.CalledProcessError:
            return []
    
    def _find_common_snapshot(
        self, source: str, target: str,
        replication_type: ReplicationType, options: Dict
    ) -> Optional[str]:
        """
        Find the most recent common snapshot between source and target.
        This is used as the base snapshot for incremental sends.
        
        Args:
            source: Source dataset (may include @snapshot)
            target: Target dataset
            replication_type: Type of replication
            options: Additional options including remote_host, etc.
            
        Returns:
            The most recent common snapshot name, or None if no common snapshot exists
        """
        # Extract dataset name from source (remove @snapshot if present)
        source_dataset = source.split('@')[0] if '@' in source else source
        
        # Get source snapshots
        source_snapshots = self._get_snapshots(source_dataset)
        
        # Extract just snapshot names (part after @)
        source_snap_names = set()
        for snap in source_snapshots:
            if '@' in snap:
                source_snap_names.add(snap.split('@')[1])
        
        if not source_snap_names:
            return None
        
        # Get target snapshots
        if replication_type == ReplicationType.LOCAL:
            # Local target
            target_snapshots = self._get_snapshots(target)
        else:
            # Remote target
            target_snapshots = self._get_remote_snapshots(target, options)
        
        # Extract just snapshot names from target
        target_snap_names = set()
        for snap in target_snapshots:
            if '@' in snap:
                target_snap_names.add(snap.split('@')[1])
        
        # Find common snapshots
        common_snaps = source_snap_names & target_snap_names
        
        if not common_snaps:
            return None
        
        # Find the most recent common snapshot from source list (preserves order)
        for snap in reversed(source_snapshots):
            if '@' in snap:
                snap_name = snap.split('@')[1]
                if snap_name in common_snaps:
                    # Return the full snapshot name from source
                    return snap
        
        return None
    
    def _get_remote_snapshots(self, dataset: str, options: Dict) -> List[str]:
        """
        Get list of snapshots from a remote dataset via SSH
        
        Args:
            dataset: Remote dataset name
            options: Options including remote_host, remote_port, ssh_key
            
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
                'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', dataset
            ])
            
            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                timeout=30,
                check=False
            )
            
            if result.returncode == 0:
                return [line.strip() for line in result.stdout.split('\n') if line.strip()]
            return []
            
        except Exception:
            return []
    
    def _build_send_command(
        self, dataset: str, snapshot: str, incremental: bool,
        recursive: bool, compression: CompressionMethod,
        base_snapshot: Optional[str] = None
    ) -> List[str]:
        """Build the zfs send command
        
        Args:
            dataset: Source dataset name
            snapshot: The snapshot to send
            incremental: Whether to do incremental send
            recursive: Whether to include child datasets
            compression: Compression method
            base_snapshot: For incremental send, the base snapshot to send from
            
        Returns:
            List of command arguments for zfs send
        """
        cmd = ['zfs', 'send']
        
        if recursive:
            cmd.append('-R')
        
        # Add compression if not NONE
        if compression != CompressionMethod.NONE:
            cmd.extend(['-c', '-w'])  # Compressed, raw send
        
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
        """Execute local replication using pipes with platform-appropriate sudo"""
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
        
        send_process.stdout.close()
        receive_output, receive_error = receive_process.communicate()
        
        if receive_process.returncode != 0:
            raise Exception(f"Receive failed: {receive_error.decode()}")
        
        log_output = receive_error.decode() if receive_error else ''
        
        return {'bytes': 0, 'speed': 'N/A', 'log_output': log_output}
    
    def _execute_remote_replication(
        self, send_cmd: List[str], receive_cmd: List[str],
        replication_type: ReplicationType, options: Dict, execution_id: int
    ) -> Dict[str, Any]:
        """Execute remote replication over SSH"""
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
        
        send_process.stdout.close()
        ssh_output, ssh_error = ssh_process.communicate()
        
        if ssh_process.returncode != 0:
            raise Exception(f"Remote receive failed: {ssh_error.decode()}")
        
        log_output = ssh_error.decode() if ssh_error else ''
        
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
