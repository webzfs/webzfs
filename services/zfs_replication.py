"""
ZFS Replication Management Service
Handles one-shot native ZFS send/receive execution with history tracking.
Scheduled replication is handled by Syncoid jobs (services/storage.py,
services/syncoid_runner.py, services/job_scheduler.py).
Reference: https://github.com/jimsalterjrs/sanoid
Hi Jim. :)
"""
import subprocess
import time
from typing import List, Dict, Any, Optional
from datetime import datetime
from enum import Enum
from services.storage import FileStorageService
from services.email_notification import EmailNotificationService
from services.utils import run_zfs_command, build_zfs_command
from core.receive_options import (
    build_receive_flags,
    normalize_receive_options,
    validate_receive_options,
)


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
        # Initialize file storage and email services
        self.storage = FileStorageService()
        self.email = EmailNotificationService()
    
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
        large_blocks: bool = False,
        compression: CompressionMethod = CompressionMethod.LZ4,
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        force: Optional[bool] = None,
        unmounted: bool = True,
        receive_overrides: Optional[Dict[str, str]] = None,
        receive_excludes: Optional[List[str]] = None,
        discard_first_component: bool = False,
        only_last_component: bool = False,
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
            large_blocks: Use large-block send (-L flag). Allows the
                 stream to contain blocks larger than 128 KiB. Required
                 when continuing an incremental chain that was created
                 with -L, and must remain consistent across incremental
                 sends for the same target.
            compression: Compression method
            job_id: Optional job ID for scheduled jobs
            job_name: Optional job name
            force: Use -F flag on receive to overwrite existing dataset.
                   If None, automatically use -F when target exists.
            unmounted: Pass -u so the received datasets are not mounted.
                   Defaults to True for new runs.
            receive_overrides: Mapping of property=value pairs passed as
                   ``-o property=value`` on receive.
            receive_excludes: Property names passed as ``-x property`` on
                   receive so the received dataset inherits them.
            discard_first_component: Pass -d to discard the first path
                   component of the sent dataset name.
            only_last_component: Pass -e to use only the last path
                   component of the sent dataset name.
            **options: Additional options
            
        Returns:
            Execution results including bytes transferred, time taken, etc.
        """
        start_time = datetime.now()

        # Validate the receive-side options before creating an execution
        # record so an invalid combination fails fast with a readable
        # error instead of leaving a junk history entry behind.
        overrides, excludes = validate_receive_options(
            receive_overrides,
            receive_excludes,
            raw=raw,
            incremental=incremental,
            discard_first_component=discard_first_component,
            only_last_component=only_last_component,
        )
        # Values are allowlisted (no whitespace or shell syntax) in
        # core.receive_options, so they survive the remote shell that ssh
        # hands the receive argv to for push and pull replication.
        receive_options = normalize_receive_options(
            unmounted=unmounted,
            overrides=overrides,
            excludes=excludes,
            discard_first_component=discard_first_component,
            only_last_component=only_last_component,
        )

        # Create execution record in storage
        execution_id = self.storage.create_execution_record(
            job_id=job_id,
            job_name=job_name or f"{source} → {target}",
            source_dataset=source,
            target_dataset=target,
            replication_type=replication_type.value,
            receive_options=receive_options,
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
            #
            # With -d/-e (full or incremental) the argument is the PARENT
            # dataset: zfs itself appends part of the sent dataset name
            # (zfs-receive(8)).  -d appends every component after the pool
            # name, -e appends only the last component.  The user's target
            # is passed to zfs receive unchanged, while actual_target is
            # the dataset that really receives the stream; it is used to
            # find the incremental base snapshot.
            source_dataset = source.split('@')[0]
            source_parts = source_dataset.split('/')
            target_mode = receive_options['target_mode']
            if target_mode == 'discard':
                receive_argument = target
                actual_target = '/'.join([target] + source_parts[1:])
            elif target_mode == 'only_last':
                receive_argument = target
                actual_target = f"{target}/{source_parts[-1]}"
            elif not incremental:
                # Full send: receive into a child named after the source
                # basename.  e.g. "olympus/test1" -> "phobos/test1"
                actual_target = f"{target}/{source_parts[-1]}"
                receive_argument = actual_target
            else:
                actual_target = target
                receive_argument = target
            
            # Never use -F. Full sends create a new child dataset
            # (no overwrite needed). Incremental sends apply cleanly
            # when the common snapshot and new snapshot are specified.
            force = False
            
            # Merge force and the receive-side options into the options
            # mapping consumed by _build_receive_command().
            options_with_force = dict(options)
            options_with_force['force'] = force
            options_with_force.update(
                {
                    'unmounted': receive_options['unmounted'],
                    'receive_overrides': receive_options['overrides'],
                    'receive_excludes': receive_options['excludes'],
                    'discard_first_component': (
                        receive_options['target_mode'] == 'discard'
                    ),
                    'only_last_component': (
                        receive_options['target_mode'] == 'only_last'
                    ),
                }
            )
            
            # Build the send and receive commands early so the
            # command string is stored even if validation fails
            # (e.g. no common snapshot for incremental).  The send
            # command is built without -i for now; if a base snapshot
            # is found below, we rebuild with -i included.
            send_cmd = self._build_send_command(
                source, latest_snapshot, incremental, recursive, raw,
                large_blocks, compression, base_snapshot=None
            )
            receive_cmd = self._build_receive_command(
                receive_argument, replication_type, options_with_force
            )

            # Build and store the planned command string immediately
            # so it appears in error logs even if validation fails.
            command_string = self._build_command_string(
                send_cmd, receive_cmd, replication_type, options_with_force
            )
            self.storage.update_execution_record(
                execution_id=execution_id,
                status='running',
                command=command_string
            )
            
            # For incremental send, find the common/base snapshot.
            # Both the source and target must share at least one snapshot
            # for incremental replication to work. If no common snapshot
            # is found, we raise an error rather than silently falling back
            # to a full send, because a full send would overwrite
            # the target dataset and destroy any existing data.
            base_snapshot = None
            if incremental:
                # Look up the base on the dataset that actually receives
                # the stream: with -d/-e that is a child of the target.
                base_snapshot = self._find_common_snapshot(
                    source, actual_target, replication_type, options_with_force
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
                
                # Rebuild the send command now that we have the base snapshot
                send_cmd = self._build_send_command(
                    source, latest_snapshot, incremental, recursive, raw,
                    large_blocks, compression, base_snapshot=base_snapshot
                )
                
                # Update the stored command string with the actual -i flag
                command_string = self._build_command_string(
                    send_cmd, receive_cmd, replication_type, options_with_force
                )
                self.storage.update_execution_record(
                    execution_id=execution_id,
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
        recursive: bool, raw: bool, large_blocks: bool,
        compression: CompressionMethod,
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
            large_blocks: Whether to use large-block send (-L). Allows
                 blocks larger than 128 KiB in the stream. Must be used
                 consistently across incremental sends to the same target.
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
        
        # Large-block send (-L) must be explicitly enabled by the user.
        # It allows the stream to contain blocks larger than 128 KiB and
        # is required when continuing an incremental replication chain
        # that was originally created with -L (issue #204). OpenZFS warns
        # that this flag must be used consistently across incremental
        # sends, so it is never toggled automatically.
        if large_blocks:
            cmd.append('-L')
        
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
        """Build the zfs receive command

        Receive-side flags come from core.receive_options:

        * ``-u`` (default on) leaves the received datasets unmounted;
        * ``-o property=value`` overrides a property on the received data;
        * ``-x property`` strips a property so it is inherited;
        * ``-d`` / ``-e`` select how the sent dataset name maps onto the
          target (mutually exclusive, validated before we get here).
        """
        cmd = ['zfs', 'receive']

        # Use -F flag to overwrite existing dataset if force is True
        # This is needed when the target dataset already exists
        if options.get('force', False):
            cmd.append('-F')

        cmd.extend(
            build_receive_flags(
                unmounted=bool(options.get('unmounted', True)),
                overrides=options.get('receive_overrides') or {},
                excludes=options.get('receive_excludes') or [],
                discard_first_component=bool(
                    options.get('discard_first_component', False)
                ),
                only_last_component=bool(
                    options.get('only_last_component', False)
                ),
            )
        )

        cmd.append(target)
        return cmd
    
    def _build_command_string(
        self, send_cmd: List[str], receive_cmd: List[str],
        replication_type: ReplicationType, options: Dict
    ) -> str:
        """Build the full pipeline command string for audit/history.
        
        Combines the send and receive commands into a single string
        that represents what will be (or was) executed, including
        the SSH transport for remote replication.
        
        Args:
            send_cmd: The zfs send command parts
            receive_cmd: The zfs receive command parts
            replication_type: Type of replication
            options: Options including remote_host, remote_port, ssh_key
            
        Returns:
            Full command string (e.g. "zfs send ... | ssh ... zfs receive ...")
        """
        if replication_type == ReplicationType.LOCAL:
            return ' '.join(send_cmd) + ' | ' + ' '.join(receive_cmd)
        
        remote_host = options.get('remote_host', '')
        remote_port = options.get('remote_port', 22)
        ssh_key = options.get('ssh_key')
        ssh_parts = ['ssh', '-p', str(remote_port)]
        if ssh_key:
            ssh_parts.extend(['-i', ssh_key])
        ssh_parts.append(remote_host)
        ssh_parts.extend(receive_cmd)
        return ' '.join(send_cmd) + ' | ' + ' '.join(ssh_parts)
    
    def _estimate_send_size(self, send_cmd: List[str]) -> int:
        """Estimate total bytes for a send command using a dry run.

        Inserts -n (dry run) and -P (machine parseable) into a copy of
        the send command and parses the reported size. Returns 0 when
        the size cannot be determined so callers can skip percentage
        and ETA calculations.
        """
        try:
            dry_run_cmd = list(send_cmd)
            insert_at = dry_run_cmd.index('send') + 1
            dry_run_cmd[insert_at:insert_at] = ['-n', '-P']
            full_cmd = build_zfs_command(dry_run_cmd)
            result = subprocess.run(
                full_cmd, capture_output=True, text=True, timeout=120
            )
            combined_output = result.stdout + '\n' + result.stderr
            for line in combined_output.split('\n'):
                parts = line.split()
                if len(parts) >= 2 and parts[0] == 'size':
                    return int(parts[1])
        except Exception:
            pass
        return 0
    
    def _pump_with_progress(
        self, send_process, receive_process, execution_id: int, total_bytes: int
    ) -> int:
        """Copy the send stream into the receive stdin, recording progress.
        
        Reads the zfs send output in chunks, writes it to the receiving
        process, and records a progress update roughly every two seconds
        so the monitor page and SSE stream have live data to display.
        Returns the total number of bytes transferred.
        """
        bytes_transferred = 0
        chunk_size = 1024 * 1024
        start_time = time.time()
        last_update_time = start_time
        last_update_bytes = 0
        
        try:
            while True:
                chunk = send_process.stdout.read(chunk_size)
                if not chunk:
                    break
                try:
                    receive_process.stdin.write(chunk)
                except (BrokenPipeError, OSError):
                    # Receive side exited; stop pumping. The caller
                    # surfaces the receive process stderr as the error.
                    break
                bytes_transferred += len(chunk)
                
                now = time.time()
                if now - last_update_time >= 2:
                    interval_rate = (
                        (bytes_transferred - last_update_bytes)
                        / (now - last_update_time)
                    )
                    percentage = 0.0
                    eta = None
                    if total_bytes > 0:
                        percentage = min(
                            bytes_transferred / total_bytes * 100, 99.9
                        )
                        if interval_rate > 0:
                            remaining = max(total_bytes - bytes_transferred, 0)
                            eta_seconds = int(remaining / interval_rate)
                            eta = f"{eta_seconds // 60}m {eta_seconds % 60}s"
                    try:
                        self.storage.add_progress_update(
                            execution_id=execution_id,
                            bytes_transferred=bytes_transferred,
                            percentage_complete=percentage,
                            transfer_rate=(
                                f"{self._format_bytes(int(interval_rate))}/s"
                            ),
                            estimated_time_remaining=eta,
                            status_message=(
                                f"Transferred "
                                f"{self._format_bytes(bytes_transferred)}"
                            )
                        )
                    except Exception:
                        # Progress recording must never break the transfer
                        pass
                    last_update_time = now
                    last_update_bytes = bytes_transferred
        finally:
            # Do not close receive_process.stdin here.
            #
            # The caller immediately invokes Popen.communicate(), which owns
            # flushing and closing the receive-side stdin pipe while draining
            # stdout/stderr and waiting for the process to terminate.
            #
            # Closing stdin here causes communicate() to attempt to flush an
            # already-closed file object, resulting in:
            #
            #     ValueError: flush of closed file
            #
            # Closing send_process.stdout is still appropriate because this
            # helper has finished consuming the send stream.
            try:
                send_process.stdout.close()
            except Exception:
                pass
        
        return bytes_transferred
    
    def _execute_local_replication(
        self, send_cmd: List[str], receive_cmd: List[str], execution_id: int
    ) -> Dict[str, Any]:
        """Execute local replication using pipes with platform-appropriate sudo.
        
        The send stream is pumped through this process so progress
        (bytes transferred, rate, percentage) can be recorded for the
        real-time monitor. Both processes' stderr streams are captured
        so that when the receive side reports a generic 'failed to read
        from stream' error we can surface the real cause from the send
        side.
        """
        # Build commands with sudo if needed (Linux)
        full_send_cmd = build_zfs_command(send_cmd)
        full_receive_cmd = build_zfs_command(receive_cmd)
        
        # Estimate total size for percentage/ETA reporting (best effort)
        total_bytes = self._estimate_send_size(send_cmd)
        
        send_process = subprocess.Popen(
            full_send_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        receive_process = subprocess.Popen(
            full_receive_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Pump the stream through this process, recording progress updates
        bytes_transferred = self._pump_with_progress(
            send_process, receive_process, execution_id, total_bytes
        )
        
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
        
        return {
            'bytes': bytes_transferred,
            'speed': 'N/A',
            'log_output': log_output
        }
    
    def _execute_remote_replication(
        self, send_cmd: List[str], receive_cmd: List[str],
        replication_type: ReplicationType, options: Dict, execution_id: int
    ) -> Dict[str, Any]:
        """Execute remote replication over SSH.
        
        Pipes zfs send stdout through SSH into zfs receive on the remote host.
        The stream is pumped through this process so progress can be
        recorded for the real-time monitor. Both the local send process
        and remote SSH process stderr streams are captured so that when
        the remote receive reports a generic error we can surface the
        real cause from the local send side.
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
        
        # Estimate total size for percentage/ETA reporting (best effort)
        total_bytes = self._estimate_send_size(send_cmd)
        
        # Execute send | ssh receive
        send_process = subprocess.Popen(
            full_send_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        ssh_process = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Pump the stream through this process, recording progress updates
        bytes_transferred = self._pump_with_progress(
            send_process, ssh_process, execution_id, total_bytes
        )
        
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
        
        return {
            'bytes': bytes_transferred,
            'speed': 'N/A',
            'log_output': log_output
        }
    
    def _format_bytes(self, bytes: int) -> str:
        """Format bytes to human-readable string"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes < 1024.0:
                return f"{bytes:.2f} {unit}"
            bytes /= 1024.0
        return f"{bytes:.2f} PB"
