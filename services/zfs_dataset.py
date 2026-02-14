"""
ZFS Dataset Management Service
Handles dataset operations: create, destroy, list, clone, properties, etc.
"""
import re
import subprocess
from typing import List, Dict, Any, Optional

from services.utils import run_zfs_command

# Try to import libzfs_core, but fall back to shell commands if not available
try:
    import libzfs_core as lzc
    HAS_LIBZFS_CORE = True
except ImportError:
    HAS_LIBZFS_CORE = False


class ZFSDatasetService:
    """Service for managing ZFS datasets (filesystems and volumes)"""
    
    # ZFS naming pattern: alphanumeric, underscore, hyphen, period, colon, plus forward slash for paths
    ZFS_DATASET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*(/[a-zA-Z0-9][a-zA-Z0-9_\-.:]*)*$')
    # Full snapshot name pattern (dataset@snapshot)
    ZFS_SNAPSHOT_FULL_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*(/[a-zA-Z0-9][a-zA-Z0-9_\-.:]*)*@[a-zA-Z0-9][a-zA-Z0-9_\-.:]*$')
    
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
        Validate a full ZFS snapshot name (dataset@snapshot) against naming rules.
        
        Args:
            snapshot_name: The full snapshot name to validate (format: dataset@snapshot)
            
        Raises:
            ValueError: If the snapshot name is invalid
        """
        if not snapshot_name:
            raise ValueError("Snapshot name cannot be empty")
        
        if '@' not in snapshot_name:
            raise ValueError(
                f"Invalid snapshot name format '{snapshot_name}'. Expected format: dataset@snapshot"
            )
        
        if not cls.ZFS_SNAPSHOT_FULL_PATTERN.match(snapshot_name):
            raise ValueError(
                f"Invalid snapshot name '{snapshot_name}'. Snapshot names must follow ZFS naming rules: "
                "alphanumeric characters, underscores, hyphens, periods, colons, or forward slashes."
            )
    
    def list_datasets(self, pool_name: Optional[str] = None, 
                     dataset_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        List all datasets, optionally filtered by pool and type
        
        Args:
            pool_name: Optional pool name to filter by
            dataset_type: Optional type filter ('filesystem', 'volume', 'snapshot', 'bookmark')
            
        Returns:
            List of datasets with their properties
        """
        if pool_name:
            self.validate_dataset_name(pool_name)
        try:
            cmd = ['zfs', 'list', '-H', '-o', 
                   'name,type,used,avail,refer,mountpoint,compression,compressratio,encryption']
            
            if dataset_type:
                cmd.extend(['-t', dataset_type])
            
            if pool_name:
                cmd.append(pool_name)
            
            result = run_zfs_command(cmd)
            
            datasets = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 9:
                    datasets.append({
                        'name': parts[0],
                        'type': parts[1],
                        'used': parts[2],
                        'avail': parts[3],
                        'refer': parts[4],
                        'mountpoint': parts[5],
                        'compression': parts[6],
                        'compressratio': parts[7],
                        'encryption': parts[8]
                    })
            
            return datasets
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list datasets: {e.stderr}")
    
    def get_dataset(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific dataset
        
        Args:
            dataset_name: Full name of the dataset
            
        Returns:
            Dictionary with dataset details
        """
        self.validate_dataset_name(dataset_name)
        try:
            # Get all properties - this will fail if dataset doesn't exist
            result = run_zfs_command(['zfs', 'get', '-H', 'all', dataset_name])
            
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
            
            return {
                'name': dataset_name,
                'exists': True,
                'properties': properties
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Dataset {dataset_name} does not exist or cannot be accessed")
        except Exception as e:
            raise Exception(f"Failed to get dataset: {str(e)}")
    
    def create_dataset(self, dataset_name: str, dataset_type: str = "filesystem",
                      properties: Optional[Dict[str, str]] = None,
                      create_parents: bool = False) -> None:
        """
        Create a new dataset
        
        Args:
            dataset_name: Full name for the new dataset (pool/path/name)
            dataset_type: Type of dataset ('filesystem' or 'volume')
            properties: Optional dictionary of properties to set
            create_parents: Create parent datasets if they don't exist
        """
        self.validate_dataset_name(dataset_name)
        try:
            props = properties or {}
            
            # Use zfs command
            cmd = ['zfs', 'create']
            
            if create_parents:
                cmd.append('-p')
            
            # Add volume-specific options
            if dataset_type == "volume":
                if 'volsize' not in props:
                    raise Exception("volsize property is required for volume creation")
                cmd.extend(['-V', props['volsize']])
                # Remove volsize from props as it's already handled
                props = {k: v for k, v in props.items() if k != 'volsize'}
            elif dataset_type != "filesystem":
                raise Exception(f"Invalid dataset type: {dataset_type}")
            
            # Add properties
            for key, value in props.items():
                cmd.extend(['-o', f'{key}={value}'])
            
            cmd.append(dataset_name)
            
            run_zfs_command(cmd)
                
        except subprocess.CalledProcessError as e:
            if 'already exists' in e.stderr.lower():
                raise Exception(f"Dataset {dataset_name} already exists")
            raise Exception(f"Failed to create dataset: {e.stderr}")
        except Exception as e:
            raise Exception(f"Failed to create dataset: {str(e)}")
    
    def create_dataset_with_encryption(self, dataset_name: str, passphrase: str,
                                      dataset_type: str = "filesystem",
                                      properties: Optional[Dict[str, str]] = None,
                                      create_parents: bool = False) -> None:
        """
        Create a new encrypted dataset with passphrase
        
        Args:
            dataset_name: Full name for the new dataset (pool/path/name)
            passphrase: Passphrase for encryption
            dataset_type: Type of dataset ('filesystem' or 'volume')
            properties: Optional dictionary of properties to set
            create_parents: Create parent datasets if they don't exist
        """
        self.validate_dataset_name(dataset_name)
        try:
            props = properties or {}
            
            # Ensure encryption properties are set
            if 'encryption' not in props:
                props['encryption'] = 'aes-256-gcm'
            if 'keyformat' not in props:
                props['keyformat'] = 'passphrase'
            if 'keylocation' not in props:
                props['keylocation'] = 'prompt'
            
            # Build zfs create command
            cmd = ['zfs', 'create']
            
            if create_parents:
                cmd.append('-p')
            
            # Add volume-specific options
            if dataset_type == "volume":
                if 'volsize' not in props:
                    raise Exception("volsize property is required for volume creation")
                cmd.extend(['-V', props['volsize']])
                # Remove volsize from props as it's already handled
                props = {k: v for k, v in props.items() if k != 'volsize'}
            
            # Add properties
            for key, value in props.items():
                cmd.extend(['-o', f'{key}={value}'])
            
            cmd.append(dataset_name)
            
            # Run command with passphrase input
            run_zfs_command(cmd, input_data=f"{passphrase}\n")
            
        except subprocess.CalledProcessError as e:
            if 'already exists' in e.stderr.lower():
                raise Exception(f"Dataset {dataset_name} already exists")
            raise Exception(f"Failed to create encrypted dataset: {e.stderr}")
        except Exception as e:
            raise Exception(f"Failed to create encrypted dataset: {str(e)}")
    
    def destroy_dataset(self, dataset_name: str, recursive: bool = False,
                       force: bool = False) -> None:
        """
        Destroy a dataset
        
        Args:
            dataset_name: Name of the dataset to destroy
            recursive: Destroy all descendants
            force: Force unmount if necessary
            
        WARNING: This is a destructive operation!
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'destroy']
            
            if recursive:
                cmd.append('-r')
            if force:
                cmd.append('-f')
            
            cmd.append(dataset_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to destroy dataset: {e.stderr}")
    
    def clone_dataset(self, snapshot: str, target: str,
                     properties: Optional[Dict[str, str]] = None) -> None:
        """
        Clone a snapshot to create a new dataset
        
        Args:
            snapshot: Full name of the snapshot to clone
            target: Name for the new cloned dataset
            properties: Optional properties to set on the clone
        """
        self.validate_snapshot_name(snapshot)
        self.validate_dataset_name(target)
        try:
            props = properties or {}
            
            # Use zfs command
            cmd = ['zfs', 'clone']
            
            # Add properties
            for key, value in props.items():
                cmd.extend(['-o', f'{key}={value}'])
            
            cmd.extend([snapshot, target])
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to clone dataset: {e.stderr}")
    
    def rename_dataset(self, old_name: str, new_name: str,
                      force: bool = False) -> None:
        """
        Rename a dataset
        
        Args:
            old_name: Current dataset name
            new_name: New dataset name
            force: Force unmount if necessary
        """
        self.validate_dataset_name(old_name)
        self.validate_dataset_name(new_name)
        try:
            cmd = ['zfs', 'rename']
            
            if force:
                cmd.append('-f')
            
            cmd.extend([old_name, new_name])
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to rename dataset: {e.stderr}")
    
    def get_properties(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get all properties for a dataset
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Dictionary of properties with values and sources
        """
        self.validate_dataset_name(dataset_name)
        try:
            result = run_zfs_command(['zfs', 'get', '-H', 'all', dataset_name])
            
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
            
            return properties
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get properties: {e.stderr}")
    
    def set_property(self, dataset_name: str, property_name: str,
                    property_value: str) -> None:
        """
        Set a property on a dataset
        
        Args:
            dataset_name: Name of the dataset
            property_name: Name of the property to set
            property_value: Value to set
        """
        self.validate_dataset_name(dataset_name)
        try:
            run_zfs_command(['zfs', 'set', f'{property_name}={property_value}', dataset_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to set property: {e.stderr}")
    
    def inherit_property(self, dataset_name: str, property_name: str,
                        recursive: bool = False) -> None:
        """
        Inherit a property from parent dataset
        
        Args:
            dataset_name: Name of the dataset
            property_name: Name of the property to inherit
            recursive: Apply to all descendants
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'inherit']
            
            if recursive:
                cmd.append('-r')
            
            cmd.extend([property_name, dataset_name])
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to inherit property: {e.stderr}")
    
    def mount_dataset(self, dataset_name: str) -> None:
        """
        Mount a dataset
        
        Args:
            dataset_name: Name of the dataset to mount
        """
        self.validate_dataset_name(dataset_name)
        try:
            run_zfs_command(['zfs', 'mount', dataset_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to mount dataset: {e.stderr}")
    
    def unmount_dataset(self, dataset_name: str, force: bool = False) -> None:
        """
        Unmount a dataset
        
        Args:
            dataset_name: Name of the dataset to unmount
            force: Force unmount even if busy
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'umount']  # Note: ZFS uses 'umount' not 'unmount'
            
            if force:
                cmd.append('-f')
            
            cmd.append(dataset_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to unmount dataset: {e.stderr}")
    
    def get_space_usage(self, dataset_name: str, recursive: bool = False) -> List[Dict[str, Any]]:
        """
        Get detailed space usage information
        
        Args:
            dataset_name: Name of the dataset
            recursive: Include child datasets
            
        Returns:
            List of space usage details
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'list', '-H', '-o', 
                   'name,used,avail,refer,usedsnap,usedds,usedrefreserv,usedchild']
            
            if recursive:
                cmd.append('-r')
            
            cmd.append(dataset_name)
            
            result = run_zfs_command(cmd)
            
            usage = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                    
                parts = line.split('\t')
                if len(parts) >= 8:
                    usage.append({
                        'name': parts[0],
                        'used': parts[1],
                        'avail': parts[2],
                        'refer': parts[3],
                        'usedsnap': parts[4],
                        'usedds': parts[5],
                        'usedrefreserv': parts[6],
                        'usedchild': parts[7]
                    })
            
            return usage
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get space usage: {e.stderr}")
    
    def list_children(self, dataset_name: str) -> List[str]:
        """
        List immediate children of a dataset
        
        Args:
            dataset_name: Name of the parent dataset
            
        Returns:
            List of child dataset names
        """
        self.validate_dataset_name(dataset_name)
        try:
            # Use zfs list command
            result = run_zfs_command(
                ['zfs', 'list', '-H', '-r', '-d', '1', '-o', 'name', dataset_name]
            )
            
            children = []
            for line in result.stdout.strip().split('\n'):
                if line and line != dataset_name:  # Exclude the parent itself
                    children.append(line)
            
            return children
                
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list children: {e.stderr}")
    
    def promote_dataset(self, dataset_name: str) -> None:
        """
        Promote a cloned dataset
        
        Args:
            dataset_name: Name of the clone to promote
        """
        self.validate_dataset_name(dataset_name)
        try:
            run_zfs_command(['zfs', 'promote', dataset_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to promote dataset: {e.stderr}")
    
    def load_key(self, dataset_name: str, key_location: Optional[str] = None) -> None:
        """
        Load encryption key for a dataset
        
        Args:
            dataset_name: Name of the dataset
            key_location: Optional path to key file (if not using prompt)
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'load-key']
            
            if key_location:
                cmd.extend(['-L', key_location])
            
            cmd.append(dataset_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to load encryption key: {e.stderr}")
    
    def unload_key(self, dataset_name: str) -> None:
        """
        Unload encryption key for a dataset
        
        Args:
            dataset_name: Name of the dataset
        """
        self.validate_dataset_name(dataset_name)
        try:
            run_zfs_command(['zfs', 'unload-key', dataset_name])
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to unload encryption key: {e.stderr}")
    
    def change_key(self, dataset_name: str, inherit: bool = False) -> None:
        """
        Change encryption key for a dataset
        
        Args:
            dataset_name: Name of the dataset
            inherit: Inherit key from parent dataset
        """
        self.validate_dataset_name(dataset_name)
        try:
            cmd = ['zfs', 'change-key']
            
            if inherit:
                cmd.append('-i')
            
            cmd.append(dataset_name)
            
            run_zfs_command(cmd)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to change encryption key: {e.stderr}")
