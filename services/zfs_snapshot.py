"""
ZFS Snapshot Management Service
Handles snapshot operations: create, destroy, list, rollback, send, receive
"""
import re
import subprocess
from typing import List, Dict, Any, Optional

from services.utils import run_zfs_command, build_zfs_command


class ZFSSnapshotService:
    """Service for managing ZFS snapshots"""
    
    # ZFS naming patterns
    # Dataset names: alphanumeric, underscore, hyphen, period, colon, plus forward slash for paths
    ZFS_DATASET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*(/[a-zA-Z0-9][a-zA-Z0-9_\-.:]*)*$')
    # Snapshot names (the part after @): alphanumeric, underscore, hyphen, period, colon
    ZFS_SNAPSHOT_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*$')
    
    @classmethod
    def validate_dataset_name(cls, dataset_name: str) -> None:
        """
        Validate a ZFS dataset name against naming rules.
        
        ZFS dataset names must:
        - Start with an alphanumeric character in each path component
        - Contain only alphanumeric characters, underscores, hyphens, periods, colons, or forward slashes
        
        Args:
            dataset_name: The dataset name to validate
            
        Raises:
            ValueError: If the dataset name is invalid
        """
        if not dataset_name:
            raise ValueError("Dataset name cannot be empty")
        
        # fullmatch, not match: a trailing '$' still matches immediately
        # before a final newline, so match() accepts "pool/data\n" and lets
        # a trailing newline reach the zfs argv.
        if not cls.ZFS_DATASET_NAME_PATTERN.fullmatch(dataset_name):
            raise ValueError(
                f"Invalid dataset name '{dataset_name}'. Dataset names must start with an alphanumeric "
                "character and contain only alphanumeric characters, underscores, hyphens, "
                "periods, colons, or forward slashes."
            )
    
    @classmethod
    def validate_snapshot_name(cls, snapshot_name: str) -> None:
        """
        Validate a ZFS snapshot name (the part after @) against naming rules.
        
        ZFS snapshot names must:
        - Start with an alphanumeric character
        - Contain only alphanumeric characters, underscores, hyphens, periods, or colons
        
        Args:
            snapshot_name: The snapshot name to validate (without dataset@ prefix)
            
        Raises:
            ValueError: If the snapshot name is invalid
        """
        if not snapshot_name:
            raise ValueError("Snapshot name cannot be empty")
        
        # fullmatch, not match: see validate_dataset_name.
        if not cls.ZFS_SNAPSHOT_NAME_PATTERN.fullmatch(snapshot_name):
            raise ValueError(
                f"Invalid snapshot name '{snapshot_name}'. Snapshot names must start with an alphanumeric "
                "character and contain only alphanumeric characters, underscores, hyphens, "
                "periods, or colons."
            )
    
    @classmethod
    def validate_full_snapshot_name(cls, full_name: str) -> None:
        """
        Validate a full ZFS snapshot name (dataset@snapshot) against naming rules.
        
        Args:
            full_name: The full snapshot name to validate (format: dataset@snapshot)
            
        Raises:
            ValueError: If the snapshot name is invalid
        """
        if '@' not in full_name:
            raise ValueError(
                f"Invalid snapshot name format '{full_name}'. Expected format: dataset@snapshot"
            )
        
        dataset_part, snapshot_part = full_name.rsplit('@', 1)
        cls.validate_dataset_name(dataset_part)
        cls.validate_snapshot_name(snapshot_part)
    
    def list_snapshots(self, dataset: Optional[str] = None, 
                      sort_by: str = "creation") -> List[Dict[str, Any]]:
        """
        List snapshots, optionally filtered by dataset
        
        Args:
            dataset: Optional dataset name to filter by
            sort_by: Property to sort by (creation, name, used)
            
        Returns:
            List of snapshots with their properties
        """
        if dataset:
            self.validate_dataset_name(dataset)
        try:
            cmd = ['zfs', 'list', '-H', '-t', 'snapshot', '-o',
                   'name,used,refer,creation', '-s', sort_by]
            
            if dataset:
                cmd.extend(['-r', dataset])
            
            result = run_zfs_command(cmd)
            
            snapshots = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 4:
                    # Parse snapshot name to get dataset and snapshot parts
                    full_name = parts[0]
                    if '@' in full_name:
                        dataset_name, snap_name = full_name.rsplit('@', 1)
                    else:
                        dataset_name, snap_name = full_name, ""
                    
                    snapshots.append({
                        'name': full_name,
                        'dataset': dataset_name,
                        'snapshot': snap_name,
                        'used': parts[1],
                        'refer': parts[2],
                        'creation': parts[3]
                    })
            
            return snapshots
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list snapshots: {e.stderr}")

    def get_dataset_snapshot_summaries(
        self,
        root_dataset: Optional[str] = None,
        recursive: bool = True,
    ) -> Dict[str, Dict[str, Any]]:
        """Return direct snapshot counts and latest snapshots by dataset.

        One ``zfs list`` call retrieves snapshot names and parseable creation
        timestamps. Each snapshot is grouped by the exact dataset name before
        the ``@`` separator, so snapshots of child datasets are not included in
        their parent's count.

        Args:
            root_dataset: Optional pool or dataset used to limit the query.
            recursive: Include descendants when a root dataset is provided.

        Returns:
            Mapping keyed by dataset name with count and latest snapshot data.
        """
        if root_dataset:
            self.validate_dataset_name(root_dataset)

        cmd = [
            'zfs', 'list', '-H', '-p', '-t', 'snapshot',
            '-o', 'name,creation', '-s', 'creation'
        ]
        if root_dataset:
            if recursive:
                cmd.extend(['-r', root_dataset])
            else:
                cmd.extend(['-d', '1', root_dataset])

        try:
            result = run_zfs_command(cmd)
        except subprocess.CalledProcessError as e:
            if 'no datasets available' in (e.stderr or '').lower():
                return {}
            raise Exception(f"Failed to summarize snapshots: {e.stderr}")

        summaries: Dict[str, Dict[str, Any]] = {}
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue

            parts = line.split('\t')
            if len(parts) < 2 or '@' not in parts[0]:
                continue

            full_name = parts[0]
            dataset_name, snapshot_name = full_name.rsplit('@', 1)
            try:
                creation = int(parts[1])
            except ValueError:
                creation = 0

            summary = summaries.setdefault(
                dataset_name,
                {
                    'count': 0,
                    'latest_name': None,
                    'latest_snapshot': None,
                    'latest_creation': None,
                },
            )
            summary['count'] += 1

            latest_creation = summary['latest_creation']
            if latest_creation is None or creation >= latest_creation:
                summary['latest_name'] = full_name
                summary['latest_snapshot'] = snapshot_name
                summary['latest_creation'] = creation

        return summaries
    
    def get_snapshot(self, snapshot_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific snapshot
        
        Args:
            snapshot_name: Full name of the snapshot (dataset@snapshot)
            
        Returns:
            Dictionary with snapshot details
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            # Get all properties
            result = run_zfs_command(['zfs', 'get', '-H', 'all', snapshot_name])
            
            properties = {}
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 4:
                    properties[parts[1]] = {
                        'value': parts[2],
                        'source': parts[3]
                    }
            
            dataset_name, snap_name = snapshot_name.rsplit('@', 1)
            
            return {
                'name': snapshot_name,
                'dataset': dataset_name,
                'snapshot': snap_name,
                'properties': properties
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get snapshot: {e.stderr}")
    
    def create_snapshot(self, dataset_name: str, snapshot_name: str,
                       recursive: bool = False,
                       properties: Optional[Dict[str, str]] = None) -> str:
        """
        Create a new snapshot
        
        Args:
            dataset_name: Name of the dataset to snapshot
            snapshot_name: Name for the snapshot (without @ prefix)
            recursive: Create snapshots of all descendant datasets
            properties: Optional properties to set on snapshot
            
        Returns:
            Full snapshot name (dataset@snapshot)
        """
        self.validate_dataset_name(dataset_name)
        self.validate_snapshot_name(snapshot_name)
        try:
            full_name = f"{dataset_name}@{snapshot_name}"
            
            # Build command
            cmd = ['zfs', 'snapshot']
            
            if recursive:
                cmd.append('-r')
            
            if properties:
                for key, value in properties.items():
                    cmd.extend(['-o', f'{key}={value}'])
            
            cmd.append(full_name)
            
            run_zfs_command(cmd)
            
            return full_name
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to create snapshot: {e.stderr}")
    
    def destroy_snapshot(self, snapshot_name: str, defer: bool = False) -> None:
        """
        Destroy a snapshot
        
        Args:
            snapshot_name: Full name of the snapshot (dataset@snapshot)
            defer: Defer deletion if snapshot has clones
            
        WARNING: This is a destructive operation!
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            cmd = ['zfs', 'destroy']
            
            if defer:
                cmd.append('-d')
            
            cmd.append(snapshot_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to destroy snapshot: {e.stderr}")
    
    def destroy_snapshots_bulk(self, snapshots: List[str],
                              defer: bool = False) -> Dict[str, Any]:
        """
        Destroy multiple snapshots, grouped per dataset.

        Snapshots of the same dataset are destroyed with one command
        ``zfs destroy [-d] <dataset>@<snap>[,<snap>...]`` instead of one
        command per snapshot, so a large selection costs one transaction
        group sync per dataset (zfs-destroy(8): comma-separated lists use
        short snapshot names only). Long selections are chunked under the
        argument limits; see build_destroy_specs().

        Args:
            snapshots: List of full snapshot names (dataset@snapshot)
            defer: Defer deletion if snapshots have clones

        Returns:
            Dictionary with success/failed counts, per-command results and errors
        """
        specs = build_destroy_specs(snapshots)
        results = []
        success_count = 0
        failed_count = 0
        errors = []

        for spec in specs:
            cmd = ['zfs', 'destroy']
            if defer:
                cmd.append('-d')
            cmd.append(spec['spec'])

            # Catch every error per command: earlier chunks may already be
            # destroyed, so one failure must not abort the loop or the
            # per-snapshot audit of what did succeed.
            try:
                run_zfs_command(cmd)
                success_count += len(spec['snapshots'])
                results.append({**spec, 'success': True, 'error': None})
            except Exception as e:
                failed_count += len(spec['snapshots'])
                error = (getattr(e, 'stderr', None) or str(e)).strip()
                errors.append(f"{spec['spec']}: {error}")
                results.append({**spec, 'success': False, 'error': error})

        return {
            'success': success_count,
            'failed': failed_count,
            'errors': errors,
            'results': results,
            'commands': [spec['spec'] for spec in specs],
            'snapshot_count': sum(len(spec['snapshots']) for spec in specs),
        }

    def dry_run_destroy_bulk(self, snapshots: List[str],
                             defer: bool = False) -> Dict[str, Any]:
        """
        Report what a bulk destroy would do without deleting anything.

        Runs ``zfs destroy -nvp`` once per grouped spec and parses the
        machine-parsable ``reclaim<TAB><bytes>`` line it prints. ``-n`` is a
        no-op dry run, ``-p`` requests machine-parsable verbose output and
        ``-v``/``-p`` make the reclaim total appear (zfs-destroy(8)).

        Args:
            snapshots: List of full snapshot names (dataset@snapshot)
            defer: Report the dry run with the -d defer option applied

        Returns:
            Dictionary with the grouped specs, per-command reclaim values,
            the total reclaimable bytes (raw and human-readable) and errors
        """
        specs = build_destroy_specs(snapshots)
        commands = []
        errors = []
        total_reclaim = 0

        for spec in specs:
            cmd = ['zfs', 'destroy', '-nvp']
            if defer:
                cmd.append('-d')
            cmd.append(spec['spec'])

            try:
                result = run_zfs_command(cmd)
                reclaim = parse_reclaim(result.stdout)
                total_reclaim += reclaim
                commands.append({
                    'dataset': spec['dataset'],
                    'snapshots': spec['snapshots'],
                    'spec': spec['spec'],
                    'reclaim': reclaim,
                    'reclaim_human': format_bytes(reclaim),
                    'output': (result.stdout or '').strip(),
                    'error': None,
                })
            except Exception as e:
                error = (getattr(e, 'stderr', None) or str(e)).strip()
                errors.append(f"{spec['spec']}: {error}")
                commands.append({
                    'dataset': spec['dataset'],
                    'snapshots': spec['snapshots'],
                    'spec': spec['spec'],
                    'reclaim': 0,
                    'reclaim_human': format_bytes(0),
                    'output': '',
                    'error': error,
                })

        return {
            'specs': specs,
            'commands': commands,
            'errors': errors,
            'total_reclaim': total_reclaim,
            'total_reclaim_human': format_bytes(total_reclaim),
            'snapshot_count': sum(len(spec['snapshots']) for spec in specs),
        }
    
    def rollback_snapshot(self, snapshot_name: str, force: bool = False) -> None:
        """
        Rollback a dataset to a snapshot
        
        Args:
            snapshot_name: Full name of the snapshot (dataset@snapshot)
            force: Destroy more recent snapshots
            
        WARNING: This will destroy data created after the snapshot!
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            cmd = ['zfs', 'rollback']
            
            if force:
                cmd.append('-r')
            
            cmd.append(snapshot_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to rollback snapshot: {e.stderr}")
    
    def clone_snapshot(self, snapshot_name: str, target_dataset: str,
                      properties: Optional[Dict[str, str]] = None) -> None:
        """
        Clone a snapshot to create a new dataset
        
        Args:
            snapshot_name: Full name of the snapshot to clone
            target_dataset: Name for the new cloned dataset
            properties: Optional properties to set on the clone
        """
        self.validate_full_snapshot_name(snapshot_name)
        self.validate_dataset_name(target_dataset)
        try:
            cmd = ['zfs', 'clone']
            
            # Add properties if provided
            if properties:
                for key, value in properties.items():
                    cmd.extend(['-o', f'{key}={value}'])
            
            cmd.extend([snapshot_name, target_dataset])
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to clone snapshot: {e.stderr}")
    
    def diff_snapshots(self, snapshot1: str, snapshot2: Optional[str] = None) -> str:
        """
        Show differences between snapshots or snapshot and current state
        
        Args:
            snapshot1: First snapshot (or only snapshot to diff with current)
            snapshot2: Optional second snapshot to compare with
            
        Returns:
            Diff output as string
        """
        self.validate_full_snapshot_name(snapshot1)
        if snapshot2:
            self.validate_full_snapshot_name(snapshot2)
        try:
            cmd = ['zfs', 'diff']
            
            if snapshot2:
                cmd.extend([snapshot1, snapshot2])
            else:
                cmd.append(snapshot1)
            
            result = run_zfs_command(cmd)
            
            return result.stdout
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to diff snapshots: {e.stderr}")
    
    def hold_snapshot(self, snapshot_name: str, tag: str) -> None:
        """
        Place a user hold on a snapshot to prevent deletion
        
        Args:
            snapshot_name: Full name of the snapshot
            tag: Tag name for the hold
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            run_zfs_command(['zfs', 'hold', tag, snapshot_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to hold snapshot: {e.stderr}")
    
    def release_snapshot(self, snapshot_name: str, tag: str) -> None:
        """
        Release a user hold on a snapshot
        
        Args:
            snapshot_name: Full name of the snapshot
            tag: Tag name of the hold to release
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            run_zfs_command(['zfs', 'release', tag, snapshot_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to release snapshot: {e.stderr}")
    
    def get_holds(self, snapshot_name: str) -> Dict[str, Any]:
        """
        Get all holds on a snapshot
        
        Args:
            snapshot_name: Full name of the snapshot
            
        Returns:
            Dictionary of holds
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            # Use zfs holds command
            result = run_zfs_command(['zfs', 'holds', '-H', snapshot_name])
            
            holds = {}
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                parts = line.split('\t')
                if len(parts) >= 2:
                    # Format: dataset  tag  timestamp
                    tag = parts[1]
                    timestamp = parts[2] if len(parts) >= 3 else ''
                    holds[tag] = timestamp
            
            return holds
            
        except subprocess.CalledProcessError as e:
            # If no holds exist, the command might fail, return empty dict
            if 'no such hold' in e.stderr.lower() or not e.stderr:
                return {}
            raise Exception(f"Failed to get holds: {e.stderr}")
    
    def send_snapshot(self, snapshot_name: str, 
                     base_snapshot: Optional[str] = None,
                     output_file: Optional[str] = None) -> bytes:
        """
        Send a snapshot (for replication)
        
        Args:
            snapshot_name: Full name of the snapshot to send
            base_snapshot: Optional base snapshot for incremental send
            output_file: Optional file path to write to (if not provided, returns bytes)
            
        Returns:
            Snapshot data as bytes (if output_file not provided)
        """
        self.validate_full_snapshot_name(snapshot_name)
        if base_snapshot:
            self.validate_full_snapshot_name(base_snapshot)
        try:
            cmd = ['zfs', 'send']
            
            if base_snapshot:
                cmd.extend(['-i', base_snapshot])
            
            cmd.append(snapshot_name)
            
            # Build command with sudo if needed
            full_cmd = build_zfs_command(cmd)
            
            if output_file:
                # The stream goes to the file through a file handle, never
                # through a shell string, so the path cannot be interpreted.
                with open(output_file, 'wb') as output_handle:
                    subprocess.run(
                        full_cmd,
                        stdout=output_handle,
                        stderr=subprocess.PIPE,
                        text=False,
                        check=True
                    )
                return b''
            else:
                result = subprocess.run(
                    full_cmd,
                    capture_output=True,
                    text=False,
                    check=True
                )
                return result.stdout
                
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to send snapshot: {e.stderr}")
    
    def receive_snapshot(self, target_dataset: str, 
                        snapshot_data: Optional[bytes] = None,
                        input_file: Optional[str] = None,
                        force: bool = False) -> None:
        """
        Receive a snapshot (for replication)
        
        Args:
            target_dataset: Target dataset name
            snapshot_data: Snapshot data as bytes
            input_file: Optional input file path
            force: Force rollback to receive snapshot
        """
        self.validate_dataset_name(target_dataset)
        try:
            cmd = ['zfs', 'receive']
            
            if force:
                cmd.append('-F')
            
            cmd.append(target_dataset)
            
            # Build command with sudo if needed
            full_cmd = build_zfs_command(cmd)
            
            if input_file:
                # The stream is read from the file through a file handle,
                # never through a shell string.
                with open(input_file, 'rb') as input_handle:
                    subprocess.run(
                        full_cmd,
                        stdin=input_handle,
                        capture_output=True,
                        text=False,
                        check=True
                    )
            elif snapshot_data:
                subprocess.run(
                    full_cmd,
                    input=snapshot_data,
                    capture_output=True,
                    text=False,
                    check=True
                )
            else:
                raise Exception("Either snapshot_data or input_file must be provided")
                
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to receive snapshot: {e.stderr}")
    
    def rename_snapshot(self, old_name: str, new_name: str,
                       recursive: bool = False) -> None:
        """
        Rename a snapshot
        
        Args:
            old_name: Current snapshot name (dataset@snapshot)
            new_name: New snapshot name (just the snapshot part, no @)
            recursive: Rename snapshots of all descendant datasets
        """
        self.validate_full_snapshot_name(old_name)
        self.validate_snapshot_name(new_name)
        try:
            dataset_name = old_name.rsplit('@', 1)[0]
            full_new_name = f"{dataset_name}@{new_name}"
            
            cmd = ['zfs', 'rename']
            
            if recursive:
                cmd.append('-r')
            
            cmd.extend([old_name, full_new_name])
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to rename snapshot: {e.stderr}")
    
    # ========================
    # Bookmark Operations
    # ========================

    def list_bookmarks(self, dataset: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List ZFS bookmarks, optionally filtered by dataset.

        Args:
            dataset: Optional dataset name to filter by

        Returns:
            List of bookmarks with their properties
        """
        if dataset:
            self.validate_dataset_name(dataset)
        try:
            cmd = ['zfs', 'list', '-H', '-t', 'bookmark', '-o',
                   'name,creation', '-s', 'creation']

            if dataset:
                cmd.extend(['-r', dataset])

            result = run_zfs_command(cmd)

            bookmarks = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue

                parts = line.split('\t')
                if len(parts) >= 2:
                    full_name = parts[0]
                    if '#' in full_name:
                        dataset_name, bookmark_name = full_name.rsplit('#', 1)
                    else:
                        dataset_name, bookmark_name = full_name, ""

                    bookmarks.append({
                        'name': full_name,
                        'dataset': dataset_name,
                        'bookmark': bookmark_name,
                        'creation': parts[1]
                    })

            return bookmarks

        except subprocess.CalledProcessError as e:
            # If no bookmarks exist, some ZFS versions return an error
            if 'no datasets available' in (e.stderr or '').lower():
                return []
            raise Exception(f"Failed to list bookmarks: {e.stderr}")

    def create_bookmark(self, snapshot_name: str,
                        bookmark_name: Optional[str] = None) -> str:
        """
        Create a bookmark from a snapshot.

        Args:
            snapshot_name: Full snapshot name (dataset@snapshot)
            bookmark_name: Optional bookmark name. If not provided, uses the
                           snapshot name as the bookmark name.

        Returns:
            Full bookmark name (dataset#bookmark)
        """
        self.validate_full_snapshot_name(snapshot_name)
        dataset_part, snap_part = snapshot_name.rsplit('@', 1)

        if bookmark_name:
            self.validate_snapshot_name(bookmark_name)
        else:
            bookmark_name = snap_part

        full_bookmark = f"{dataset_part}#{bookmark_name}"

        try:
            run_zfs_command(['zfs', 'bookmark', snapshot_name, full_bookmark])
            return full_bookmark
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to create bookmark: {e.stderr}")

    def destroy_bookmark(self, bookmark_name: str) -> None:
        """
        Destroy a ZFS bookmark.

        Args:
            bookmark_name: Full bookmark name (dataset#bookmark)
        """
        if '#' not in bookmark_name:
            raise ValueError(
                f"Invalid bookmark name format '{bookmark_name}'. "
                "Expected format: dataset#bookmark"
            )
        dataset_part, bmark_part = bookmark_name.rsplit('#', 1)
        self.validate_dataset_name(dataset_part)
        self.validate_snapshot_name(bmark_part)

        try:
            run_zfs_command(['zfs', 'destroy', bookmark_name])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to destroy bookmark: {e.stderr}")

    def get_snapshot_space(self, snapshot_name: str) -> Dict[str, str]:
        """
        Get space usage information for a snapshot
        
        Args:
            snapshot_name: Full name of the snapshot
            
        Returns:
            Dictionary with space usage details
        """
        self.validate_full_snapshot_name(snapshot_name)
        try:
            result = run_zfs_command(
                ['zfs', 'list', '-H', '-o', 'used,refer,logicalused', snapshot_name]
            )
            
            parts = result.stdout.strip().split('\t')
            if len(parts) >= 3:
                return {
                    'used': parts[0],
                    'refer': parts[1],
                    'logicalused': parts[2]
                }
            
            return {}
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get snapshot space: {e.stderr}")


# ---------------------------------------------------------------------------
# Bulk snapshot destroy helpers
#
# `zfs destroy` accepts several snapshots of the same dataset as a
# comma-separated list of *short* names (the part after '@'), e.g.
#   zfs destroy tank/data@snap1,snap2,snap3
# (zfs-destroy(8), OpenZFS 2.4.4: "Only the snapshot's short name (the part
# after the @) should be specified when using a range or comma-separated
# list"). Grouping a selection per dataset therefore turns N selected
# snapshots into one command per dataset instead of N commands, each with its
# own transaction group sync.
# ---------------------------------------------------------------------------

#: Maximum number of short snapshot names placed in one comma-separated list.
BULK_DESTROY_MAX_NAMES_PER_COMMAND = 64

#: Conservative maximum length of a single `zfs destroy` argument.
BULK_DESTROY_MAX_ARG_LENGTH = 4096

_RECLAIM_MACHINE_RE = re.compile(r'^reclaim\s+(\d+)$')
_RECLAIM_HUMAN_RE = re.compile(
    r'^(?:would|will)\s+reclaim\s+([0-9]+(?:\.[0-9]+)?)\s*([KMGTPEZ]?)B?$',
    re.IGNORECASE,
)
_NICEBYTES_UNITS = ' KMGTPEZ'


def format_bytes(value: int) -> str:
    """Format a byte count using binary units, similar to zfs_nicebytes(3)."""
    try:
        value = int(value)
    except (TypeError, ValueError):
        return str(value)

    if value < 1024:
        return f"{value}B"

    size = float(value)
    for unit in _NICEBYTES_UNITS[1:]:
        size /= 1024.0
        if size < 1024.0 or unit == _NICEBYTES_UNITS[-1]:
            return f"{size:.2f}{unit}"
    return f"{value}B"


def parse_reclaim(output: Optional[str]) -> int:
    """
    Sum the reclaimable bytes reported by `zfs destroy -nvp` (or `-nv`).

    With ``-p`` OpenZFS prints one machine-parsable ``reclaim<TAB><bytes>``
    line per command, covering the whole comma-separated list. The
    human-readable form (``would reclaim 1.5G``) is accepted as a fallback so
    the parser keeps working without ``-p``. Unrelated verbose lines are
    ignored.

    Raises:
        ValueError: If a reclaim line carries a value that cannot be parsed
    """
    total = 0
    for raw_line in (output or '').splitlines():
        line = raw_line.strip()
        if not line:
            continue

        match = _RECLAIM_MACHINE_RE.match(line)
        if match:
            total += int(match.group(1))
            continue

        match = _RECLAIM_HUMAN_RE.match(line)
        if match:
            exponent = _NICEBYTES_UNITS.index((match.group(2) or ' ').upper())
            total += int(float(match.group(1)) * (1024 ** exponent))

    return total


def _destroy_spec(dataset: str, short_names: List[str]) -> Dict[str, Any]:
    """Build one grouped destroy spec for a dataset and its short names."""
    return {
        'dataset': dataset,
        'snapshots': list(short_names),
        'spec': f"{dataset}@{','.join(short_names)}",
    }


def build_destroy_specs(
    snapshots: List[str],
    *,
    max_names: int = BULK_DESTROY_MAX_NAMES_PER_COMMAND,
    max_arg_length: int = BULK_DESTROY_MAX_ARG_LENGTH,
) -> List[Dict[str, Any]]:
    """
    Group full snapshot names by dataset into `zfs destroy` specs.

    Args:
        snapshots: Full snapshot names in ``dataset@snapshot`` form
        max_names: Maximum number of short names per comma-separated list
        max_arg_length: Maximum length of the ``dataset@a,b,c`` argument

    Returns:
        List of dicts with ``dataset``, ``snapshots`` (short names in input
        order) and ``spec`` (``dataset@short[,short...]``). A dataset yields a
        single spec unless its selection exceeds ``max_names`` or the argument
        would exceed ``max_arg_length``, in which case it is split. Duplicate
        selections are collapsed.

    Raises:
        ValueError: If an entry is not a string or is not a valid full
            snapshot name (validated before any command is built)
    """
    if isinstance(snapshots, str):
        raise ValueError(
            "snapshots must be a list of 'dataset@snapshot' strings, "
            "not a single string"
        )
    if max_names < 1:
        raise ValueError("max_names must be at least 1")

    grouped: Dict[str, List[str]] = {}
    seen = set()
    for full_name in snapshots:
        if not isinstance(full_name, str):
            raise ValueError(
                f"Invalid snapshot entry {full_name!r}: expected a string"
            )
        full_name = full_name.strip()
        if not full_name:
            continue
        ZFSSnapshotService.validate_full_snapshot_name(full_name)
        dataset, short_name = full_name.rsplit('@', 1)
        if (dataset, short_name) in seen:
            continue
        seen.add((dataset, short_name))
        grouped.setdefault(dataset, []).append(short_name)

    specs: List[Dict[str, Any]] = []
    for dataset, short_names in grouped.items():
        chunk: List[str] = []
        for short_name in short_names:
            candidate = chunk + [short_name]
            candidate_arg = f"{dataset}@{','.join(candidate)}"
            if chunk and (
                len(candidate) > max_names
                or len(candidate_arg) > max_arg_length
            ):
                specs.append(_destroy_spec(dataset, chunk))
                chunk = [short_name]
            else:
                chunk = candidate
        if chunk:
            specs.append(_destroy_spec(dataset, chunk))

    return specs
