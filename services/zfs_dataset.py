"""
ZFS Dataset Management Service
Handles dataset operations: create, destroy, list, clone, properties, etc.
"""
import os
import re
import subprocess
from typing import List, Dict, Any, Optional

from services.utils import is_netbsd, run_zfs_command

# Try to import libzfs_core, but fall back to shell commands if not available
try:
    import libzfs_core as lzc
    HAS_LIBZFS_CORE = True
except ImportError:
    HAS_LIBZFS_CORE = False


class ZFSDatasetService:
    """Service for managing ZFS datasets (filesystems and volumes)"""

    MIN_PASSPHRASE_LENGTH = 8
    MAX_PASSPHRASE_BYTES = 512
    KEY_PROPERTY_TIMEOUT_SECONDS = 10
    KEY_LOAD_TIMEOUT_SECONDS = 30
    
    # ZFS naming pattern: alphanumeric, underscore, hyphen, period, colon, plus forward slash for paths
    ZFS_DATASET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*(/[a-zA-Z0-9][a-zA-Z0-9_\-.:]*)*$')
    # Full snapshot name pattern (dataset@snapshot)
    ZFS_SNAPSHOT_FULL_PATTERN = re.compile(r'^[a-zA-Z0-9][a-zA-Z0-9_\-.:]*(/[a-zA-Z0-9][a-zA-Z0-9_\-.:]*)*@[a-zA-Z0-9][a-zA-Z0-9_\-.:]*$')

    @classmethod
    def validate_passphrase(cls, passphrase: str) -> None:
        """Validate an OpenZFS passphrase before invoking the ZFS command."""
        if len(passphrase) < cls.MIN_PASSPHRASE_LENGTH:
            raise ValueError(
                f"Encryption passphrase must be at least "
                f"{cls.MIN_PASSPHRASE_LENGTH} characters long"
            )

        if len(passphrase.encode("utf-8")) > cls.MAX_PASSPHRASE_BYTES:
            raise ValueError(
                f"Encryption passphrase must not exceed "
                f"{cls.MAX_PASSPHRASE_BYTES} bytes"
            )
    
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
            # NetBSD ZFS may not support the 'encryption' property
            # Use a reduced property list for NetBSD
            if is_netbsd():
                properties = 'name,type,used,avail,refer,mountpoint,compression,compressratio'
            else:
                properties = 'name,type,used,avail,refer,mountpoint,compression,compressratio,encryption'
            
            cmd = ['zfs', 'list', '-H', '-o', properties]
            
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
                
                # Handle different property counts based on platform
                if is_netbsd():
                    if len(parts) >= 8:
                        datasets.append({
                            'name': parts[0],
                            'type': parts[1],
                            'used': parts[2],
                            'avail': parts[3],
                            'refer': parts[4],
                            'mountpoint': parts[5],
                            'compression': parts[6],
                            'compressratio': parts[7],
                            'encryption': '-'  # Not supported on NetBSD
                        })
                else:
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
    
    # zfs send -L is required when any block exceeds 128 KiB
    LARGE_BLOCK_THRESHOLD_BYTES = 131072
    
    def get_large_block_status(self, dataset_name: str) -> Dict[str, Any]:
        """
        Check whether a dataset uses blocks larger than 128 KiB.
        
        Reads recordsize (filesystems) and volblocksize (volumes) in
        parseable byte form. Datasets over 128 KiB require zfs send -L
        (large blocks) to replicate (issue #204).
        
        Args:
            dataset_name: Full name of the dataset
            
        Returns:
            Dictionary with large_blocks flag and the block size in bytes
        """
        self.validate_dataset_name(dataset_name)
        result = run_zfs_command([
            'zfs', 'get', '-H', '-p', '-o', 'property,value',
            'recordsize,volblocksize', dataset_name
        ])
        
        block_size_bytes = 0
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) >= 2 and parts[1].isdigit():
                block_size_bytes = max(block_size_bytes, int(parts[1]))
        
        return {
            'dataset': dataset_name,
            'block_size_bytes': block_size_bytes,
            'large_blocks': block_size_bytes > self.LARGE_BLOCK_THRESHOLD_BYTES
        }
    
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
        self.validate_passphrase(passphrase)
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
    
    def get_space_tree(self, pool_name: str, max_depth: int = 4) -> Dict[str, Any]:
        """
        Build a nested space-usage tree for the given pool.

        Uses 'zfs list -Hp' (raw byte values, machine-parseable) and the
        slash-delimited dataset names to reconstruct the hierarchy. Snapshot
        counts are gathered with a second 'zfs list -t snapshot' call so the
        visualizer can show how many snapshots each dataset holds.

        Args:
            pool_name: The pool to inspect.
            max_depth: Maximum depth (including the pool root) to include in
                the returned tree. Children deeper than this are still summed
                via the existing 'usedbychildren' values, but are not added as
                explicit nodes.

        Returns:
            A nested dict of the form::

                {
                    "name": "pool",
                    "used": int,
                    "available": int,
                    "referenced": int,
                    "used_by_dataset": int,
                    "used_by_snapshots": int,
                    "used_by_children": int,
                    "compressratio": str,
                    "snapshot_count": int,
                    "children": [ ... same shape ... ],
                }
        """
        self.validate_dataset_name(pool_name)

        properties = (
            'name,used,referenced,available,'
            'usedbysnapshots,usedbychildren,usedbydataset,compressratio'
        )

        try:
            result = run_zfs_command(
                ['zfs', 'list', '-Hp', '-t', 'filesystem,volume',
                 '-o', properties, '-r', pool_name]
            )
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list datasets for {pool_name}: {e.stderr}")

        # Snapshot counts: one entry per dataset.
        snapshot_counts: Dict[str, int] = {}
        try:
            snap_result = run_zfs_command(
                ['zfs', 'list', '-Hp', '-t', 'snapshot',
                 '-o', 'name', '-r', pool_name]
            )
            for line in snap_result.stdout.strip().split('\n'):
                if not line or '@' not in line:
                    continue
                parent = line.split('@', 1)[0]
                snapshot_counts[parent] = snapshot_counts.get(parent, 0) + 1
        except subprocess.CalledProcessError:
            # Snapshot count is non-essential, leave the dict empty
            snapshot_counts = {}

        def _to_int(token: str) -> int:
            token = (token or '').strip()
            if not token or token == '-':
                return 0
            try:
                return int(token)
            except ValueError:
                return 0

        nodes: Dict[str, Dict[str, Any]] = {}
        order: List[str] = []
        for line in result.stdout.strip().split('\n'):
            if not line:
                continue
            parts = line.split('\t')
            if len(parts) < 8:
                continue
            name = parts[0]
            nodes[name] = {
                'name': name,
                'used': _to_int(parts[1]),
                'referenced': _to_int(parts[2]),
                'available': _to_int(parts[3]),
                'used_by_snapshots': _to_int(parts[4]),
                'used_by_children': _to_int(parts[5]),
                'used_by_dataset': _to_int(parts[6]),
                'compressratio': parts[7],
                'snapshot_count': snapshot_counts.get(name, 0),
                'children': [],
            }
            order.append(name)

        if pool_name not in nodes:
            raise Exception(f"Pool {pool_name} not found in zfs list output")

        # Wire children to parents using slash-delimited names.
        # 'zfs list -r' returns the parent before its children, so we can
        # rely on insertion order to build the tree in one pass.
        root_depth = pool_name.count('/')
        for name in order:
            if name == pool_name:
                continue
            parent_name = name.rsplit('/', 1)[0]
            parent = nodes.get(parent_name)
            if parent is None:
                continue
            depth_from_root = name.count('/') - root_depth
            if depth_from_root >= max_depth:
                # Skip nodes deeper than the visualization can show.
                # Their 'used' is still reflected in their ancestor's
                # 'used_by_children' value.
                continue
            parent['children'].append(nodes[name])

        return nodes[pool_name]

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
    
    def load_key(
        self,
        dataset_name: str,
        passphrase: Optional[str] = None,
    ) -> None:
        """
        Load encryption key for a dataset
        
        Args:
            dataset_name: Name of the dataset
            passphrase: Optional passphrase to provide through standard input
        """
        self.validate_dataset_name(dataset_name)
        key_location = ''
        timeout_seconds = self.KEY_PROPERTY_TIMEOUT_SECONDS
        timeout_message = "Reading the dataset encryption key properties"
        try:
            key_result = run_zfs_command(
                [
                    'zfs', 'get', '-H', '-o', 'property,value',
                    'keyformat,keylocation,encryptionroot', dataset_name,
                ],
                timeout=self.KEY_PROPERTY_TIMEOUT_SECONDS,
            )
            key_properties = {}
            for line in key_result.stdout.strip().split('\n'):
                parts = line.split('\t', 1)
                if len(parts) == 2:
                    key_properties[parts[0]] = parts[1]

            key_format = key_properties.get('keyformat', '')
            key_location = key_properties.get('keylocation', '')
            encryption_root = key_properties.get('encryptionroot', dataset_name)

            if not key_format or key_format == '-':
                raise ValueError("Dataset does not have an encryption key format")
            if not key_location or key_location == '-':
                raise ValueError("Dataset does not have a configured key location")
            if key_location == 'prompt' and key_format == 'passphrase' and not passphrase:
                raise ValueError("Encryption passphrase is required")
            if key_location == 'prompt' and key_format != 'passphrase':
                raise ValueError(
                    f"Prompt-based {key_format} keys are not supported by this form"
                )
            if key_location == 'prompt' and passphrase:
                self.validate_passphrase(passphrase)

            cmd = ['zfs', 'load-key', encryption_root]
            input_data = (
                f"{passphrase}\n" if key_location == 'prompt' and passphrase else None
            )
            timeout_seconds = self.KEY_LOAD_TIMEOUT_SECONDS
            timeout_message = (
                "Encryption key loading from the configured key location"
            )
            run_zfs_command(
                cmd,
                input_data=input_data,
                timeout=self.KEY_LOAD_TIMEOUT_SECONDS,
            )
            
        except subprocess.TimeoutExpired:
            raise Exception(
                f"{timeout_message} timed out after {timeout_seconds} seconds. "
                "The configured key location may be unavailable or unreachable."
            )
        except subprocess.CalledProcessError as e:
            error = (e.stderr or '').strip()
            if key_location.startswith('file://'):
                error_lower = error.lower()
                if any(message in error_lower for message in (
                    'no such file', 'cannot open', 'failed to open',
                    'permission denied', 'not found',
                )):
                    raise Exception(
                        "The configured encryption key file is unavailable or "
                        f"unreadable. Verify the file path and permissions. {error}"
                    )
                raise Exception(
                    f"Failed to load encryption key from the configured key file: "
                    f"{error}"
                )
            if key_location.startswith(('http://', 'https://')):
                error_lower = error.lower()
                if any(message in error_lower for message in (
                    'cannot connect', 'could not connect', 'failed to connect',
                    "couldn't connect", 'connection refused',
                    'could not resolve', "couldn't resolve",
                    'name resolution', 'unreachable', 'timed out', 'timeout',
                )):
                    raise Exception(
                        "The configured remote encryption key location is "
                        f"unavailable or unreachable. {error}"
                    )
                raise Exception(
                    f"Failed to load encryption key from the configured remote "
                    f"location: {error}"
                )
            raise Exception(f"Failed to load encryption key: {error}")
    
    def unload_key(self, dataset_name: str) -> None:
        """
        Unload encryption key for a dataset
        
        Args:
            dataset_name: Name of the dataset
        """
        self.validate_dataset_name(dataset_name)
        try:
            mounted_result = run_zfs_command(
                ['zfs', 'get', '-H', '-o', 'value', 'mounted', dataset_name]
            )
            if mounted_result.stdout.strip() == 'yes':
                raise ValueError("Dataset must be unmounted before unloading its key")

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
    
    # Maximum number of directory entries returned by peek_directory
    PEEK_MAX_ENTRIES = 1000
    
    def peek_directory(self, dataset_name: str, subpath: str = "") -> Dict[str, Any]:
        """
        List the contents of a directory inside a mounted dataset.
        
        Traversal is restricted to the dataset mountpoint. The requested
        subpath is resolved with realpath and rejected if it escapes the
        mountpoint (blocks ../ traversal and symlink escapes).
        
        Args:
            dataset_name: Name of the dataset
            subpath: Relative path inside the dataset (empty for the root)
            
        Returns:
            Dictionary with mountpoint, subpath, breadcrumb segments,
            entries (folders first, then files), truncated flag and an
            optional error message.
        """
        self.validate_dataset_name(dataset_name)
        
        result: Dict[str, Any] = {
            'dataset': dataset_name,
            'mountpoint': None,
            'subpath': '',
            'breadcrumbs': [],
            'entries': [],
            'truncated': False,
            'error': None,
        }
        
        # Resolve mountpoint and verify the dataset is a mounted filesystem
        try:
            zfs_result = run_zfs_command(
                ['zfs', 'get', '-H', '-o', 'property,value',
                 'type,mounted,mountpoint', dataset_name]
            )
        except subprocess.CalledProcessError as e:
            result['error'] = f"Failed to query dataset: {e.stderr}"
            return result
        
        props = {}
        for line in zfs_result.stdout.strip().split('\n'):
            parts = line.split('\t')
            if len(parts) >= 2:
                props[parts[0]] = parts[1]
        
        if props.get('type') != 'filesystem':
            result['error'] = "Peek is only available for filesystem datasets"
            return result
        if props.get('mounted') != 'yes':
            result['error'] = "Dataset is not mounted"
            return result
        
        mountpoint = props.get('mountpoint', '')
        if not mountpoint or mountpoint in ('-', 'none', 'legacy'):
            result['error'] = "Dataset has no usable mountpoint"
            return result
        
        mountpoint_real = os.path.realpath(mountpoint)
        result['mountpoint'] = mountpoint
        
        # Sanitize and resolve the requested subpath within the mountpoint
        clean_subpath = subpath.strip().strip('/')
        target_dir = os.path.realpath(os.path.join(mountpoint_real, clean_subpath)) if clean_subpath else mountpoint_real
        
        # Enforce the dataset boundary: the resolved path must stay inside
        # the mountpoint. This blocks ../ traversal and symlink escapes.
        if target_dir != mountpoint_real and not target_dir.startswith(mountpoint_real + os.sep):
            result['error'] = "Path is outside the dataset mountpoint"
            return result
        
        # Recompute the effective subpath from the resolved location so the
        # breadcrumb always reflects the real position inside the dataset
        effective_subpath = os.path.relpath(target_dir, mountpoint_real)
        if effective_subpath == '.':
            effective_subpath = ''
        result['subpath'] = effective_subpath
        
        # Build breadcrumb segments with cumulative paths
        if effective_subpath:
            cumulative = []
            for segment in effective_subpath.split(os.sep):
                cumulative.append(segment)
                result['breadcrumbs'].append({
                    'name': segment,
                    'subpath': '/'.join(cumulative),
                })
        
        if not os.path.isdir(target_dir):
            result['error'] = "Directory does not exist"
            return result
        
        # List directory entries
        folders = []
        files = []
        try:
            with os.scandir(target_dir) as scan:
                for entry in scan:
                    if len(folders) + len(files) >= self.PEEK_MAX_ENTRIES:
                        result['truncated'] = True
                        break
                    try:
                        is_dir = entry.is_dir(follow_symlinks=False)
                    except OSError:
                        is_dir = False
                    item = {
                        'name': entry.name,
                        'is_dir': is_dir,
                        'is_hidden': entry.name.startswith('.'),
                        'size': None,
                    }
                    if not is_dir:
                        try:
                            item['size'] = entry.stat(follow_symlinks=False).st_size
                        except OSError:
                            pass
                    if is_dir:
                        folders.append(item)
                    else:
                        files.append(item)
        except PermissionError:
            result['error'] = "Permission denied reading this directory"
            return result
        except OSError as e:
            result['error'] = f"Failed to read directory: {e}"
            return result
        
        folders.sort(key=lambda x: x['name'].lower())
        files.sort(key=lambda x: x['name'].lower())
        result['entries'] = folders + files
        
        return result
