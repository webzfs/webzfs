"""
ZFS Snapshot Management Service
Handles snapshot operations: create, destroy, list, rollback, send, receive
"""
import re
import subprocess
from typing import List, Dict, Any, Optional
from datetime import datetime

from services.utils import run_zfs_command, build_zfs_command

# Try to import libzfs_core, but fall back to shell commands if not available
try:
    import libzfs_core as lzc
    HAS_LIBZFS_CORE = True
except ImportError:
    HAS_LIBZFS_CORE = False


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
        
        if not cls.ZFS_DATASET_NAME_PATTERN.match(dataset_name):
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
        
        if not cls.ZFS_SNAPSHOT_NAME_PATTERN.match(snapshot_name):
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
        Destroy multiple snapshots in a single operation
        
        Args:
            snapshots: List of snapshot names to destroy
            defer: Defer deletion if snapshots have clones
            
        Returns:
            Dictionary with results (success count, errors)
        """
        success_count = 0
        failed_count = 0
        errors = []
        
        for snapshot in snapshots:
            try:
                self.destroy_snapshot(snapshot, defer=defer)
                success_count += 1
            except Exception as e:
                failed_count += 1
                errors.append(f"{snapshot}: {str(e)}")
        
        return {
            'success': success_count,
            'failed': failed_count,
            'errors': errors
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
                full_cmd_str = ' '.join(full_cmd) + f' > {output_file}'
                subprocess.run(
                    full_cmd_str,
                    shell=True,
                    capture_output=True,
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
                full_cmd_str = ' '.join(full_cmd) + f' < {input_file}'
                subprocess.run(
                    full_cmd_str,
                    shell=True,
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
