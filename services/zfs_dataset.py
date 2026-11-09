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


class VolumeResizeError(Exception):
    """Raised when a zvol resize request cannot be applied."""


class ShrinkConfirmationRequired(VolumeResizeError):
    """
    Raised when a zvol shrink is requested without explicit confirmation.

    Shrinking a zvol discards everything beyond the new size. Callers must
    surface the data-loss warning and re-submit with confirm_shrink=True.
    """

    def __init__(self, dataset_name: str, current_size: str, new_size: str,
                 current_size_bytes: int, new_size_bytes: int):
        self.dataset_name = dataset_name
        self.current_size = current_size
        self.new_size = new_size
        self.current_size_bytes = current_size_bytes
        self.new_size_bytes = new_size_bytes
        super().__init__(
            f"Shrinking {dataset_name} from {current_size} to {new_size} will "
            "destroy all data beyond the new size. Shrink the guest filesystem "
            "first, then confirm that the data beyond the new size will be lost."
        )


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

    # zvol options (issue #21). zfsprops(7): volblocksize must be a power of
    # two between 512 B and 128 KiB (up to 16 MiB only with large_blocks).
    # The default is 16 KiB on OpenZFS 2.2 and later, 8 KiB before that, so
    # an empty value is left to zfs rather than hard coded here.
    VOLBLOCKSIZE_MIN_BYTES = 512
    VOLBLOCKSIZE_MAX_BYTES = 128 * 1024
    VOLMODE_VALUES = ("default", "full", "geom", "dev", "none")

    # Size suffixes accepted by zfs(8), mapped to byte multipliers.
    _SIZE_SUFFIXES = {
        "": 1, "B": 1,
        "K": 1024, "KB": 1024, "KIB": 1024,
        "M": 1024 ** 2, "MB": 1024 ** 2, "MIB": 1024 ** 2,
        "G": 1024 ** 3, "GB": 1024 ** 3, "GIB": 1024 ** 3,
        "T": 1024 ** 4, "TB": 1024 ** 4, "TIB": 1024 ** 4,
        "P": 1024 ** 5, "PB": 1024 ** 5, "PIB": 1024 ** 5,
    }

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
        
        # fullmatch, not match: see validate_dataset_name.
        if not cls.ZFS_SNAPSHOT_FULL_PATTERN.fullmatch(snapshot_name):
            raise ValueError(
                f"Invalid snapshot name '{snapshot_name}'. Snapshot names must follow ZFS naming rules: "
                "alphanumeric characters, underscores, hyphens, periods, colons, or forward slashes."
            )

    @classmethod
    def parse_size(cls, size: str) -> int:
        """
        Parse a ZFS size string (e.g. "10G", "512M", "10737418240") to bytes.

        Args:
            size: Size as accepted by zfs(8): a number with an optional
                K/M/G/T/P (or KB/MB/... KiB/MiB/...) suffix.

        Returns:
            Size in bytes

        Raises:
            ValueError: If the size is not a positive number with a known unit
        """
        text = str(size or "").strip()
        if not text:
            raise ValueError("Size cannot be empty")

        match = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([A-Za-z]*)', text)
        if not match:
            raise ValueError(
                f"Invalid size '{size}'. Use a number with an optional unit, "
                "for example 512M, 10G or 1T."
            )

        suffix = match.group(2).upper()
        if suffix not in cls._SIZE_SUFFIXES:
            raise ValueError(
                f"Invalid size unit in '{size}'. Supported units are K, M, G, T and P."
            )

        size_bytes = int(float(match.group(1)) * cls._SIZE_SUFFIXES[suffix])
        if size_bytes <= 0:
            raise ValueError(f"Size '{size}' must be greater than zero")
        return size_bytes

    @classmethod
    def format_size(cls, size_bytes: int) -> str:
        """Render a byte count using the largest exact ZFS size unit."""
        for suffix, factor in (("T", 1024 ** 4), ("G", 1024 ** 3),
                               ("M", 1024 ** 2), ("K", 1024)):
            if size_bytes >= factor and size_bytes % factor == 0:
                return f"{size_bytes // factor}{suffix}"
        return str(size_bytes)

    @classmethod
    def validate_volblocksize(cls, volblocksize: str) -> int:
        """
        Validate a volblocksize value against OpenZFS zfsprops(7) rules.

        Args:
            volblocksize: Block size string such as "16K"

        Returns:
            Block size in bytes

        Raises:
            ValueError: If the value is not a power of two between 512 B and
                128 KiB
        """
        size_bytes = cls.parse_size(volblocksize)
        if size_bytes < cls.VOLBLOCKSIZE_MIN_BYTES or size_bytes > cls.VOLBLOCKSIZE_MAX_BYTES:
            raise ValueError(
                f"volblocksize must be between 512B and 128KiB, got '{volblocksize}'"
            )
        if size_bytes & (size_bytes - 1):
            raise ValueError(
                f"volblocksize must be a power of two, got '{volblocksize}'"
            )
        return size_bytes

    @classmethod
    def zvol_device_path(cls, dataset_name: str) -> str:
        """Return the block device path exported for a zvol."""
        # NetBSD keeps the Solaris layout with dsk/rdsk subdirectories.
        if is_netbsd():
            return f"/dev/zvol/dsk/{dataset_name}"
        return f"/dev/zvol/{dataset_name}"

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
    
    def _apply_volume_options(self, cmd: List[str], props: Dict[str, str],
                              sparse: bool, volblocksize: Optional[str]) -> Dict[str, str]:
        """
        Append zvol creation options to a ``zfs create`` command.

        Adds ``-s`` for a sparse (thin provisioned) volume, ``-b`` for an
        explicit volblocksize and ``-V`` for the volume size. Returns a copy
        of ``props`` with the options that were consumed removed.

        Args:
            cmd: Command list being assembled
            props: Properties to be passed as -o property=value
            sparse: Create a sparse volume (refreservation=none)
            volblocksize: Optional volblocksize such as "16K"

        Returns:
            Remaining properties to pass with -o

        Raises:
            ValueError: If volblocksize is invalid or supplied twice
        """
        remaining = dict(props)

        if 'volblocksize' in remaining:
            if volblocksize:
                raise ValueError(
                    "volblocksize must be supplied either with -b or as a "
                    "property, not both"
                )
            volblocksize = remaining.pop('volblocksize')

        if volblocksize:
            # Pass the validated byte count so zfs never sees a unit
            # spelling that only our parser accepts (e.g. "16KiB").
            block_bytes = self.validate_volblocksize(volblocksize)
            cmd.extend(['-b', str(block_bytes)])
        if sparse:
            cmd.append('-s')

        cmd.extend(['-V', remaining.pop('volsize')])
        return remaining

    def create_dataset(self, dataset_name: str, dataset_type: str = "filesystem",
                      properties: Optional[Dict[str, str]] = None,
                      create_parents: bool = False,
                      sparse: bool = False,
                      volblocksize: Optional[str] = None) -> None:
        """
        Create a new dataset
        
        Args:
            dataset_name: Full name for the new dataset (pool/path/name)
            dataset_type: Type of dataset ('filesystem' or 'volume')
            properties: Optional dictionary of properties to set
            create_parents: Create parent datasets if they don't exist
            sparse: Create a sparse volume (thin provisioned,
                refreservation=none). Only valid for volumes.
            volblocksize: Optional block size for volumes, e.g. "16K"
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
                props = self._apply_volume_options(cmd, props, sparse, volblocksize)
            elif dataset_type != "filesystem":
                raise Exception(f"Invalid dataset type: {dataset_type}")
            elif sparse or volblocksize:
                raise Exception("Sparse volumes and volblocksize only apply to volumes")
            
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
                                      create_parents: bool = False,
                                      sparse: bool = False,
                                      volblocksize: Optional[str] = None) -> None:
        """
        Create a new encrypted dataset with passphrase
        
        Args:
            dataset_name: Full name for the new dataset (pool/path/name)
            passphrase: Passphrase for encryption
            dataset_type: Type of dataset ('filesystem' or 'volume')
            properties: Optional dictionary of properties to set
            create_parents: Create parent datasets if they don't exist
            sparse: Create a sparse volume (thin provisioned,
                refreservation=none). Only valid for volumes.
            volblocksize: Optional block size for volumes, e.g. "16K"
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
                props = self._apply_volume_options(cmd, props, sparse, volblocksize)
            elif sparse or volblocksize:
                raise Exception("Sparse volumes and volblocksize only apply to volumes")
            
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
        if (property_name or '').strip().lower() == 'volsize':
            # Shrinking a zvol destroys data; only resize_volume applies the
            # shrink confirmation and free-space checks.
            raise ValueError("Use Resize Volume on the dataset page to change volsize")
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

    def get_pool_available_bytes(self, pool_name: str) -> int:
        """
        Return the free space available in a pool, in bytes.

        Args:
            pool_name: Name of the pool (the root dataset)

        Returns:
            Available bytes reported by the pool root dataset
        """
        self.validate_dataset_name(pool_name)
        result = run_zfs_command(
            ['zfs', 'get', '-H', '-p', '-o', 'value', 'available', pool_name]
        )
        return int(result.stdout.strip())

    def get_zvol_info(self, dataset_name: str,
                      properties: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Describe a zvol: sizes, provisioning mode, volmode and device path.

        Sizes are always read with ``zfs get -p`` so they are exact byte
        counts. The human-readable output of ``zfs get`` is rounded to three
        significant digits (a 100.3G volume reads as "100G"), which is not
        safe for deciding whether a resize grows or shrinks the volume.

        Args:
            dataset_name: Name of the dataset
            properties: Already-fetched properties (from get_dataset). Only
                used to skip non-volumes without a second ``zfs get`` call.

        Returns:
            Dict of zvol details, or None when the dataset is not a volume
        """
        self.validate_dataset_name(dataset_name)
        if properties is not None:
            type_entry = properties.get('type') or {}
            if type_entry.get('value') != 'volume':
                return None

        try:
            result = run_zfs_command(
                ['zfs', 'get', '-H', '-p', '-o', 'property,value', 'all', dataset_name]
            )
        except subprocess.CalledProcessError as e:
            raise VolumeResizeError(f"Failed to read volume properties: {e.stderr}")

        props = {}
        for line in result.stdout.strip().split('\n'):
            parts = line.split('\t')
            if len(parts) >= 2:
                props[parts[0]] = parts[1]

        def value(prop_name: str) -> Optional[str]:
            return props.get(prop_name)

        if value('type') != 'volume':
            return None

        def to_int(raw: Optional[str]) -> Optional[int]:
            """Convert an exact ``zfs get -p`` value to int, or None."""
            try:
                return int(raw)
            except (TypeError, ValueError):
                return None

        volsize_raw = value('volsize')
        volblocksize_raw = value('volblocksize')
        refreservation_raw = value('refreservation')

        volsize_bytes = to_int(volsize_raw)
        volblocksize_bytes = to_int(volblocksize_raw)
        refreservation_bytes = to_int(refreservation_raw)

        # zfsprops(7): a volume is sparse when refreservation is smaller than
        # the volume size plus its metadata, or when refreservation is none.
        if refreservation_raw in ('none', '0'):
            sparse = True
        elif refreservation_bytes is not None and volsize_bytes is not None:
            sparse = refreservation_bytes < volsize_bytes
        else:
            sparse = False

        # NetBSD's older ZFS port has no volmode property.
        volmode_supported = value('volmode') is not None
        volmode = value('volmode') or 'default'

        if refreservation_bytes:
            refreservation_human = self.format_size(refreservation_bytes)
        else:
            refreservation_human = 'none'

        return {
            'name': dataset_name,
            'volsize': volsize_raw,
            'volsize_bytes': volsize_bytes,
            'volsize_human': self.format_size(volsize_bytes) if volsize_bytes else volsize_raw,
            'volblocksize': volblocksize_raw,
            'volblocksize_bytes': volblocksize_bytes,
            'volblocksize_human': (
                self.format_size(volblocksize_bytes) if volblocksize_bytes else volblocksize_raw
            ),
            'refreservation': refreservation_raw,
            'refreservation_human': refreservation_human,
            'sparse': sparse,
            'provisioning': 'sparse' if sparse else 'thick',
            'volmode': volmode,
            'volmode_supported': volmode_supported,
            'volmode_exposed': volmode != 'none',
            'device_path': self.zvol_device_path(dataset_name),
        }

    def resize_volume(self, dataset_name: str, new_size: str,
                      confirm_shrink: bool = False) -> Dict[str, Any]:
        """
        Resize a zvol through the volsize property.

        Growing a thick (non-sparse) volume first checks that the pool has
        enough free space for the additional reservation. Shrinking requires
        explicit confirmation because everything beyond the new size is lost.

        Args:
            dataset_name: Name of the volume
            new_size: New size, e.g. "20G"
            confirm_shrink: Acknowledge data loss when shrinking

        Returns:
            Dict describing the applied resize

        Raises:
            ShrinkConfirmationRequired: If shrinking without confirmation
            VolumeResizeError: If the resize cannot be performed
        """
        self.validate_dataset_name(dataset_name)

        info = self.get_zvol_info(dataset_name)
        if info is None:
            raise VolumeResizeError(
                f"{dataset_name} is not a volume; only zvols can be resized"
            )

        current_bytes = info['volsize_bytes']
        if current_bytes is None:
            raise VolumeResizeError(
                f"Could not read the current volsize of {dataset_name}"
            )

        block_bytes = info['volblocksize_bytes'] or self.VOLBLOCKSIZE_MIN_BYTES
        new_bytes = self.parse_size(new_size)

        if new_bytes % block_bytes:
            lower = (new_bytes // block_bytes) * block_bytes
            upper = lower + block_bytes
            raise VolumeResizeError(
                f"Volume size must be a multiple of volblocksize "
                f"({info['volblocksize_human']}). Use {self.format_size(lower)} or "
                f"{self.format_size(upper)}."
            )

        if new_bytes == current_bytes:
            raise VolumeResizeError(
                f"{dataset_name} is already {info['volsize_human']}"
            )

        current_human = info['volsize_human'] or str(current_bytes)
        new_human = self.format_size(new_bytes)
        shrinking = new_bytes < current_bytes

        if shrinking and not confirm_shrink:
            raise ShrinkConfirmationRequired(
                dataset_name, current_human, new_human, current_bytes, new_bytes
            )

        if not shrinking and not info['sparse']:
            pool_name = dataset_name.split('/')[0]
            growth_bytes = new_bytes - current_bytes
            available_bytes = self.get_pool_available_bytes(pool_name)
            if growth_bytes > available_bytes:
                raise VolumeResizeError(
                    f"Not enough free space to grow {dataset_name} from "
                    f"{current_human} to {new_human}: {pool_name} has "
                    f"{self.format_size(available_bytes)} available and the "
                    f"thick-provisioned volume needs "
                    f"{self.format_size(growth_bytes)} more. Make the volume "
                    f"sparse (refreservation=none) to grow it without a "
                    f"reservation."
                )

        try:
            run_zfs_command(['zfs', 'set', f'volsize={new_bytes}', dataset_name])
        except subprocess.CalledProcessError as e:
            raise VolumeResizeError(f"Failed to resize volume: {e.stderr}")

        return {
            'dataset': dataset_name,
            'old_size': current_human,
            'old_size_bytes': current_bytes,
            'new_size': new_human,
            'new_size_bytes': new_bytes,
            'direction': 'shrink' if shrinking else 'grow',
            'sparse': info['sparse'],
            'device_path': info['device_path'],
        }

    def set_volmode(self, dataset_name: str, volmode: str) -> None:
        """
        Set the volmode property of a zvol.

        Args:
            dataset_name: Name of the volume
            volmode: One of default, full, geom, dev, none

        Raises:
            ValueError: If the value or the dataset type is not valid
        """
        self.validate_dataset_name(dataset_name)

        value = (volmode or '').strip().lower()
        if value not in self.VOLMODE_VALUES:
            raise ValueError(
                f"Invalid volmode '{volmode}'. Valid values are: "
                f"{', '.join(self.VOLMODE_VALUES)}"
            )

        info = self.get_zvol_info(dataset_name)
        if info is None:
            raise ValueError(
                f"{dataset_name} is not a volume; volmode only applies to zvols"
            )
        if not info['volmode_supported']:
            raise ValueError("This ZFS version does not support the volmode property")

        # Run zfs directly rather than through set_property: the M6 property
        # catalog marks volmode as not editable through the generic editor,
        # which would refuse this dedicated, validated path.
        try:
            run_zfs_command(['zfs', 'set', f'volmode={value}', dataset_name])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to set volmode: {e.stderr}")
    
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
