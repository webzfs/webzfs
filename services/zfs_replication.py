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
from services.zfs_snapshot import ZFSSnapshotService


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
        include_intermediate: bool = False,
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
            large_blocks: Use large-block send (-L flag). Allows the
                 stream to contain blocks larger than 128 KiB. Required
                 when continuing an incremental chain that was created
                 with -L, and must remain consistent across incremental
                 sends for the same target.
            include_intermediate: Use -I for incremental sends so every
                 snapshot between the common base and the target snapshot
                 is included in the stream. When False, -i sends only the
                 target snapshot. Defaults to False so callers that do not
                 opt in keep the previous -i behaviour.
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

        # Option fallbacks (a bookmark base cannot be combined with -I or
        # -R) are recorded here so they reach the execution record and
        # the returned result instead of being applied silently.
        warnings: List[str] = []
        send_recursive = recursive
        send_include_intermediate = include_intermediate
        
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
            
            # Build the send and receive commands early so the
            # command string is stored even if validation fails
            # (e.g. no common snapshot for incremental).  The send
            # command is built without -i/-I for now; if a base snapshot
            # is found below, we rebuild with the incremental flag included.
            send_cmd = self._build_send_command(
                source, latest_snapshot, incremental, recursive, raw,
                large_blocks, compression, base_snapshot=None,
                include_intermediate=include_intermediate
            )
            receive_cmd = self._build_receive_command(
                actual_target, replication_type, options_with_force
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
            
            # For incremental send, find the common/base snapshot or
            # bookmark. Both the source and target must share at least one
            # GUID for incremental replication to work. If no common point
            # is found, we raise an error rather than silently falling back
            # to a full send, because a full send would overwrite
            # the target dataset and destroy any existing data.
            base_snapshot = None
            if incremental:
                base_snapshot = self._find_common_snapshot(
                    source, target, replication_type, options_with_force,
                    latest_snapshot=latest_snapshot
                )
                if not base_snapshot:
                    raise Exception(
                        "Incremental replication requested but no common "
                        "snapshot or bookmark was found between the source "
                        "and target datasets. Both systems must share at "
                        "least one snapshot GUID for incremental send to "
                        "work. Either perform a full (non-incremental) send "
                        "first, or verify that the target dataset has a "
                        "snapshot matching one on the source."
                    )

                # A bookmark base only works with a non-recursive -i send.
                # zfs send rejects -I and -R with a bookmark source
                # ("multiple snapshots cannot be sent from a bookmark"),
                # so degrade the requested options and say why.
                if self._is_bookmark(base_snapshot):
                    if recursive:
                        send_recursive = False
                        warnings.append(
                            f"Bookmark {base_snapshot} cannot be used as the "
                            "base of a recursive (-R) send: zfs send rejects "
                            "-R when the incremental source is a bookmark. "
                            f"Falling back to a non-recursive send of "
                            f"{latest_snapshot}."
                        )
                    if include_intermediate:
                        send_include_intermediate = False
                        warnings.append(
                            f"Bookmark {base_snapshot} cannot be used with "
                            "an intermediate (-I) send: zfs send rejects -I "
                            "when the incremental source is a bookmark. "
                            f"Falling back to -i, which sends only "
                            f"{latest_snapshot}."
                        )

                # Rebuild the send command now that we have the base point
                send_cmd = self._build_send_command(
                    source, latest_snapshot, incremental, send_recursive, raw,
                    large_blocks, compression, base_snapshot=base_snapshot,
                    include_intermediate=send_include_intermediate
                )
                
                # Update the stored command string with the actual -i/-I flag
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
                log_output='\n'.join(
                    warnings + [result.get('log_output', '')]
                ).strip()
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
                'execution_id': execution_id,
                'base_snapshot': base_snapshot,
                'recursive': send_recursive,
                'include_intermediate': send_include_intermediate,
                'warnings': warnings
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
                log_output='\n'.join(warnings + [error_message]).strip()
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
                'execution_id': execution_id,
                'warnings': warnings
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
        incremental: bool = True,
        include_intermediate: bool = False
    ) -> Dict[str, Any]:
        """
        Estimate the size of data to be transferred
        
        Args:
            source: Source dataset
            target: Target dataset
            incremental: Whether to do incremental send
            include_intermediate: Use -I instead of -i for incremental
                sends, matching the flag used by _build_send_command.
            
        Returns:
            Size estimation
        """
        try:
            # Get latest snapshot
            snapshots = self._get_snapshots(source)
            if not snapshots:
                raise Exception(f"No snapshots found for {source}")
            
            latest = snapshots[-1]

            # Validate every reference that reaches argv, including the
            # full-send case where no base is used. latest is appended
            # unconditionally below.
            self._validate_snapshot_reference(latest)

            # Pick the same GUID-matched base the real send will use so a
            # bookmark base is reflected in the estimate. Fall back to the
            # previous snapshot when no common point exists yet (for
            # example when the target dataset has not been created).
            base = None
            use_intermediate = include_intermediate
            if incremental:
                base = self._find_common_snapshot(
                    source, target, ReplicationType.LOCAL, {},
                    latest_snapshot=latest
                )
                if base is None and len(snapshots) > 1:
                    base = snapshots[-2]
                if base and self._is_bookmark(base):
                    # zfs send rejects -I when the incremental source is a
                    # bookmark, so the estimate must use -i as well.
                    use_intermediate = False

            # Use zfs send with dry-run to estimate size
            cmd = ['zfs', 'send', '-nv']
            if base:
                self._validate_incremental_base(base)
                # Same flag as the real send: -I includes intermediate
                # snapshots, -i sends only the target snapshot.
                cmd.extend(['-I' if use_intermediate else '-i', base])
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
                'base_snapshot': base,
                'incremental': incremental,
                'include_intermediate': use_intermediate,
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
    
    def _point_dataset(self, name: str) -> str:
        """Return the dataset portion of a snapshot or bookmark name."""
        for separator in ('@', '#'):
            if separator in name:
                return name.split(separator, 1)[0]
        return name

    def _is_bookmark(self, reference: str) -> bool:
        """Return True when the reference names a bookmark (dataset#bookmark)."""
        return '#' in reference

    def _parse_incremental_points(
        self, output: str, dataset: str, recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """Parse ``name,guid,createtxg`` rows for snapshots and bookmarks.

        Points are returned oldest-first by ``createtxg``. A bookmark's
        ``createtxg`` is the creation txg of the snapshot it was made from,
        so this ordering follows the dataset's data timeline rather than
        the bookmark's own creation time. When a snapshot and a bookmark
        share the same txg (a bookmark of that snapshot) the bookmark is
        listed first, so the snapshot is the last entry and callers that
        take the newest point prefer the snapshot over its bookmark.
        """
        if isinstance(output, bytes):
            output = output.decode(errors='replace')

        points = []
        for line in output.strip().split('\n'):
            if not line.strip():
                continue
            parts = line.split('\t')
            if len(parts) < 3:
                continue
            name = parts[0].strip()
            try:
                guid = int(parts[1])
                createtxg = int(parts[2])
            except ValueError:
                continue

            if '#' in name:
                point_type = 'bookmark'
            elif '@' in name:
                point_type = 'snapshot'
            else:
                continue

            point_dataset = self._point_dataset(name)
            if not recursive and point_dataset != dataset:
                continue

            points.append({
                'name': name,
                'guid': guid,
                'createtxg': createtxg,
                'type': point_type,
            })

        points.sort(
            key=lambda p: (p['createtxg'], 0 if p['type'] == 'bookmark' else 1)
        )
        return points

    def _list_incremental_points(
        self, dataset: str, recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """List snapshots and bookmarks for a local dataset, with GUIDs.

        Uses ``-t snapshot,bookmark`` so a bookmark created from a
        snapshot that has since been destroyed is still a candidate
        incremental base. Older ZFS builds without bookmark listing
        support fall back to a snapshot-only listing.
        """
        cmd = [
            'zfs', 'list', '-H', '-t', 'snapshot,bookmark',
            '-o', 'name,guid,createtxg', '-s', 'createtxg'
        ]
        if recursive:
            cmd.append('-r')
        cmd.append(dataset)

        try:
            result = run_zfs_command(cmd)
        except subprocess.CalledProcessError as e:
            stderr = (e.stderr or '').lower()
            if 'bookmark' in stderr or 'invalid type' in stderr:
                return self._list_snapshot_points(dataset, recursive)
            return []

        return self._parse_incremental_points(result.stdout, dataset, recursive)

    def _list_snapshot_points(
        self, dataset: str, recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """List snapshots only, with GUIDs, for a local dataset.

        Used for the local receive target (only snapshots can be a
        receive base) and as the fallback for ZFS builds that reject
        ``-t snapshot,bookmark``.
        """
        cmd = [
            'zfs', 'list', '-H', '-t', 'snapshot',
            '-o', 'name,guid,createtxg', '-s', 'createtxg'
        ]
        if recursive:
            cmd.append('-r')
        cmd.append(dataset)

        try:
            result = run_zfs_command(cmd)
        except subprocess.CalledProcessError:
            return []

        return [
            point
            for point in self._parse_incremental_points(
                result.stdout, dataset, recursive
            )
            if point['type'] == 'snapshot'
        ]

    def _list_remote_snapshot_points(
        self, dataset: str, options: Dict, recursive: bool = False
    ) -> List[Dict[str, Any]]:
        """List snapshots with GUIDs on a remote (receive target) dataset.

        Only snapshots are listed: ``zfs receive`` can only apply an
        incremental stream on top of a target snapshot, so a bookmark on
        the target is never a usable base (see _find_common_snapshot).
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
                'zfs', 'list', '-H', '-t', 'snapshot',
                '-o', 'name,guid,createtxg', '-s', 'createtxg'
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
            if result.returncode != 0:
                return []

            return self._parse_incremental_points(
                result.stdout, dataset, recursive
            )

        except Exception:
            return []

    def _find_common_snapshot(
        self, source: str, target: str,
        replication_type: ReplicationType, options: Dict,
        latest_snapshot: Optional[str] = None
    ) -> Optional[str]:
        """
        Find the most recent common incremental base between source and target.

        Matching is by snapshot/bookmark GUID, never by name. A received
        snapshot keeps the source GUID even if it was renamed on the
        target, and a bookmark created from a snapshot shares that
        snapshot's GUID. Name matching would therefore miss renamed
        snapshots and could select a same-named snapshot that is not
        actually the same data (issue #13).

        The returned base can be either a snapshot (``dataset@snap``) or
        a bookmark (``dataset#bookmark``). A bookmark is what makes an
        incremental send possible after the source snapshot was destroyed.

        Non-recursive listings are used so child dataset points are not
        considered: an incremental source must be in the same filesystem.

        Args:
            source: Source dataset (may include @snapshot)
            target: Target dataset
            replication_type: Type of replication
            options: Additional options including remote_host, etc.
            latest_snapshot: The snapshot being sent. Points at or newer
                than it are never used as the base. Derived from the
                source when omitted.

        Returns:
            Full source snapshot or bookmark name of the newest common
            point, or None if none exists.
        """
        source_dataset = self._point_dataset(source)
        source_points = self._list_incremental_points(
            source_dataset, recursive=False
        )
        if not source_points:
            return None

        if latest_snapshot is None:
            if '@' in source:
                latest_snapshot = source
            else:
                snapshots = [
                    p for p in source_points if p['type'] == 'snapshot'
                ]
                if snapshots:
                    latest_snapshot = snapshots[-1]['name']

        latest_guid = None
        latest_txg = None
        if latest_snapshot:
            for point in source_points:
                if point['name'] == latest_snapshot:
                    latest_guid = point['guid']
                    latest_txg = point['createtxg']
                    break

        candidates = [
            point for point in source_points
            if point['guid'] != latest_guid
            and (latest_txg is None or point['createtxg'] < latest_txg)
        ]
        if not candidates:
            return None

        # Only target *snapshots* can serve as the receive base. ``zfs
        # receive`` matches ``drr_fromguid`` against the target's snapshot
        # chain (OpenZFS dmu_recv.c), so a bookmark on the target is never
        # a valid incremental source even though it carries the GUID. The
        # target is therefore listed with ``-t snapshot`` only.
        # Source-side bookmarks stay valid bases: a bookmark shares the
        # GUID of the snapshot it was created from, so it still matches a
        # received snapshot on the target.
        if replication_type == ReplicationType.LOCAL:
            target_points = self._list_snapshot_points(
                self._point_dataset(target), recursive=False
            )
        else:
            target_points = self._list_remote_snapshot_points(
                self._point_dataset(target), options, recursive=False
            )

        target_guids = {point['guid'] for point in target_points}
        common = [
            point for point in candidates if point['guid'] in target_guids
        ]
        if not common:
            return None

        # Points are sorted oldest-first, bookmark before snapshot at the
        # same txg, so the last entry is the newest common point and a
        # surviving snapshot is preferred over its bookmark.
        return common[-1]['name']

    def _build_send_command(
        self, dataset: str, snapshot: str, incremental: bool,
        recursive: bool, raw: bool, large_blocks: bool,
        compression: CompressionMethod,
        base_snapshot: Optional[str] = None,
        include_intermediate: bool = False
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
            base_snapshot: For incremental send, the base snapshot or
                 bookmark to send from (dataset@snapshot or
                 dataset#bookmark).
            include_intermediate: For incremental send, use -I so every
                 snapshot between the base and the target snapshot is sent.
                 When False, -i sends only the target snapshot. -i and -I
                 are mutually exclusive; only one of them is ever emitted.
                 ZFS rejects -I when the base is a bookmark, so callers
                 must fall back to -i in that case.
            
        Returns:
            List of command arguments for zfs send
        """
        # Both the target snapshot and the incremental base are passed
        # straight to zfs as argv elements. Validate them against the
        # ZFS naming rules so a malformed reference is rejected before
        # it reaches the command line.
        self._validate_snapshot_reference(snapshot)
        if base_snapshot:
            self._validate_incremental_base(base_snapshot)
        
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
        
        # For incremental send, use the base snapshot with exactly one
        # incremental flag: -I includes every intermediate snapshot
        # between the base and the target snapshot, -i sends only the
        # target snapshot. Never emit both (issue #8).
        if incremental and base_snapshot:
            cmd.extend(['-I' if include_intermediate else '-i', base_snapshot])
        
        cmd.append(snapshot)
        return cmd
    
    def _validate_snapshot_reference(self, reference: str) -> None:
        """Validate a full snapshot reference of the form dataset@snapshot.
        
        Raises:
            ValueError: If the reference is not a valid dataset@snapshot pair.
        """
        if not reference or '@' not in reference:
            raise ValueError(
                f"Invalid snapshot reference '{reference}'. Expected "
                "the form dataset@snapshot."
            )
        dataset_name, snapshot_name = reference.split('@', 1)
        ZFSSnapshotService.validate_dataset_name(dataset_name)
        ZFSSnapshotService.validate_snapshot_name(snapshot_name)

    def _validate_incremental_base(self, reference: str) -> None:
        """Validate an incremental base: dataset@snapshot or dataset#bookmark.

        ZFS accepts a bookmark as the incremental source, which is what
        allows a send to continue after the source snapshot was destroyed.

        Raises:
            ValueError: If the reference is not a valid dataset@snapshot
                or dataset#bookmark pair.
        """
        if not reference or ('@' not in reference and '#' not in reference):
            raise ValueError(
                f"Invalid incremental base '{reference}'. Expected the "
                "form dataset@snapshot or dataset#bookmark."
            )
        if '@' in reference and '#' in reference:
            raise ValueError(
                f"Invalid incremental base '{reference}'. Expected the "
                "form dataset@snapshot or dataset#bookmark, not both."
            )

        # A snapshot base goes through the same validator as the target
        # snapshot, so both references follow one set of rules.
        if '@' in reference:
            self._validate_snapshot_reference(reference)
            return

        # Bookmark short names follow the same component rules as
        # snapshot short names.
        dataset_name, bookmark_name = reference.split('#', 1)
        ZFSSnapshotService.validate_dataset_name(dataset_name)
        ZFSSnapshotService.validate_snapshot_name(bookmark_name)
    
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
