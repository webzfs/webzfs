"""
Syncoid Service
Wrapper for syncoid command-line tool for ZFS replication
Reference: https://github.com/jimsalterjrs/sanoid
Hi Jim. :)
"""
import subprocess
import json
import shlex
from typing import Dict, List, Any, Optional
from pathlib import Path


class SyncoidService:
    """Service for managing syncoid replication operations"""
    
    def __init__(self):
        """Initialize the syncoid service"""
        pass
    
    def check_syncoid_status(self) -> Dict[str, Any]:
        """
        Check if syncoid is installed and get its status
        
        Returns:
            Dictionary with syncoid status information
        """
        try:
            # Common paths where syncoid might be installed
            common_paths = [
                '/usr/local/bin/syncoid',  # FreeBSD pkg install location
                '/usr/bin/syncoid',         # Linux package manager location
                '/usr/sbin/syncoid',        # Alternative Linux location
                'syncoid'                    # In PATH
            ]
            
            syncoid_path = None
            
            # Try to find syncoid using which first
            which_result = subprocess.run(
                ['which', 'syncoid'],
                capture_output=True,
                text=True
            )
            
            if which_result.returncode == 0:
                syncoid_path = which_result.stdout.strip()
            else:
                # Check common paths directly
                for path in common_paths[:-1]:  # Skip 'syncoid' since we already tried which
                    if Path(path).exists() and Path(path).is_file():
                        syncoid_path = path
                        break
            
            if not syncoid_path:
                return {
                    'installed': False,
                    'path': None,
                    'version': None
                }
            
            # Try to get version using the found path
            try:
                version_result = subprocess.run(
                    [syncoid_path, '--version'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                version = version_result.stdout.strip() if version_result.returncode == 0 else 'unknown'
            except:
                version = 'unknown'
            
            return {
                'installed': True,
                'path': syncoid_path,
                'version': version
            }
            
        except Exception as e:
            raise Exception(f"Failed to check syncoid status: {str(e)}")
    
    def execute_replication(
        self,
        source: str,
        target: str,
        recursive: bool = False,
        no_sync_snap: bool = False,
        no_privilege_elevation: bool = False,
        compress: Optional[str] = None,
        source_bwlimit: Optional[str] = None,
        target_bwlimit: Optional[str] = None,
        skip_parent: bool = False,
        create_bookmark: bool = False,
        force_delete: bool = False,
        ssh_cipher: Optional[str] = None,
        ssh_port: Optional[int] = None,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None,
        debug: bool = False,
        quiet: bool = False,
        dry_run: bool = False,
        **additional_options
    ) -> Dict[str, Any]:
        """
        Execute syncoid replication
        
        Args:
            source: Source dataset (can include hostname: user@host:pool/dataset)
            target: Target dataset (can include hostname: user@host:pool/dataset)
            recursive: Replicate snapshots recursively
            no_sync_snap: Don't create/destroy snapshots for sync
            no_privilege_elevation: Don't use sudo/doas
            compress: Compression algorithm (lzop, zstd, lz4, xz, gzip, pigz-fast, pigz-slow, none)
            source_bwlimit: Bandwidth limit for source transfer (e.g., 10M, 1G)
            target_bwlimit: Bandwidth limit for target transfer
            skip_parent: Skip parent dataset, replicate children only
            create_bookmark: Create bookmarks on source
            force_delete: Force delete conflicting snapshots on target
            ssh_cipher: SSH cipher to use (e.g., aes128-gcm@openssh.com)
            ssh_port: SSH port to use
            source_host: Source SSH host (alternative to including in source string)
            target_host: Target SSH host (alternative to including in target string)
            debug: Enable debug output
            quiet: Quiet mode
            dry_run: Dry run mode (no changes made)
            **additional_options: Additional syncoid options
            
        Returns:
            Dictionary with execution results
        """
        try:
            # Build the syncoid command
            cmd = ['syncoid']
            
            # Add options
            if recursive:
                cmd.append('-r')
            
            if no_sync_snap:
                cmd.append('--no-sync-snap')
            
            if no_privilege_elevation:
                cmd.append('--no-privilege-elevation')
            
            if compress:
                cmd.extend(['--compress', compress])
            
            if source_bwlimit:
                cmd.extend(['--source-bwlimit', source_bwlimit])
            
            if target_bwlimit:
                cmd.extend(['--target-bwlimit', target_bwlimit])
            
            if skip_parent:
                cmd.append('--skip-parent')
            
            if create_bookmark:
                cmd.append('--create-bookmark')
            
            if force_delete:
                cmd.append('--force-delete')
            
            if ssh_cipher:
                cmd.extend(['--sshcipher', ssh_cipher])
            
            if ssh_port:
                cmd.extend(['--sshport', str(ssh_port)])
            
            if debug:
                cmd.append('--debug')
            
            if quiet:
                cmd.append('--quiet')
            
            if dry_run:
                cmd.append('--dryrun')
            
            # Build source string
            if source_host:
                source_str = f"{source_host}:{source}"
            else:
                source_str = source
            
            # Build target string
            if target_host:
                target_str = f"{target_host}:{target}"
            else:
                target_str = target
            
            # Add source and target
            cmd.extend([source_str, target_str])
            
            # Execute the command
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False
            )
            
            # Parse output for stats
            stats = self._parse_syncoid_output(result.stdout, result.stderr)
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'stats': stats,
                'command': ' '.join(cmd)
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def get_common_snapshots(
        self,
        source: str,
        target: str,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get common snapshots between source and target
        
        Args:
            source: Source dataset
            target: Target dataset
            source_host: Optional source host
            target_host: Optional target host
            
        Returns:
            Dictionary with common snapshots information
        """
        try:
            # Get source snapshots
            if source_host:
                source_cmd = ['ssh', source_host, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', source]
            else:
                source_cmd = ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', source]
            
            source_result = subprocess.run(
                source_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            source_snapshots = set(line.strip().split('@')[1] for line in source_result.stdout.split('\n') if '@' in line)
            
            # Get target snapshots
            if target_host:
                target_cmd = ['ssh', target_host, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', target]
            else:
                target_cmd = ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', target]
            
            target_result = subprocess.run(
                target_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            target_snapshots = set(line.strip().split('@')[1] for line in target_result.stdout.split('\n') if '@' in line)
            
            # Find common snapshots
            common = source_snapshots & target_snapshots
            source_only = source_snapshots - target_snapshots
            target_only = target_snapshots - source_snapshots
            
            return {
                'common_snapshots': sorted(list(common)),
                'source_only_snapshots': sorted(list(source_only)),
                'target_only_snapshots': sorted(list(target_only)),
                'common_count': len(common),
                'source_only_count': len(source_only),
                'target_only_count': len(target_only)
            }
            
        except Exception as e:
            return {
                'error': str(e)
            }
    
    def estimate_transfer_size(
        self,
        source: str,
        target: Optional[str] = None,
        source_host: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Estimate the size of data to be transferred
        
        Args:
            source: Source dataset
            target: Optional target dataset (for incremental estimation)
            source_host: Optional source host
            
        Returns:
            Dictionary with size estimation
        """
        try:
            # Get list of snapshots for source
            if source_host:
                list_cmd = ['ssh', source_host, 'zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', source]
            else:
                list_cmd = ['zfs', 'list', '-t', 'snapshot', '-H', '-o', 'name', '-r', source]
            
            list_result = subprocess.run(
                list_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            snapshots = [line.strip() for line in list_result.stdout.split('\n') if line.strip()]
            
            if not snapshots:
                return {'error': 'No snapshots found for source'}
            
            latest_snapshot = snapshots[-1]
            
            # Use zfs send with dry-run to estimate size
            if source_host:
                send_cmd = ['ssh', source_host, 'zfs', 'send', '-nv', latest_snapshot]
            else:
                send_cmd = ['zfs', 'send', '-nv', latest_snapshot]
            
            send_result = subprocess.run(
                send_cmd,
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse output for size
            size_bytes = 0
            for line in send_result.stderr.split('\n'):
                if 'size' in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        try:
                            size_bytes = int(parts[1])
                        except (ValueError, IndexError):
                            pass
            
            return {
                'source': source,
                'latest_snapshot': latest_snapshot,
                'estimated_bytes': size_bytes,
                'estimated_size': self._format_bytes(size_bytes)
            }
            
        except Exception as e:
            return {
                'error': str(e)
            }
    
    def _parse_syncoid_output(self, stdout: str, stderr: str) -> Dict[str, Any]:
        """
        Parse syncoid output for statistics
        
        Args:
            stdout: Standard output from syncoid
            stderr: Standard error from syncoid
            
        Returns:
            Dictionary with parsed statistics
        """
        stats = {
            'bytes_sent': None,
            'bytes_received': None,
            'transfer_rate': None,
            'snapshots_sent': 0,
            'snapshots_destroyed': 0
        }
        
        # Combine stdout and stderr for parsing
        output = stdout + '\n' + stderr
        
        # Look for transfer statistics
        # Example: "sent 123456 bytes  received 789 bytes  12345.67 bytes/sec"
        for line in output.split('\n'):
            if 'sent' in line.lower() and 'received' in line.lower():
                parts = line.split()
                try:
                    for i, part in enumerate(parts):
                        if part == 'sent' and i + 1 < len(parts):
                            stats['bytes_sent'] = int(parts[i + 1].replace(',', ''))
                        elif part == 'received' and i + 1 < len(parts):
                            stats['bytes_received'] = int(parts[i + 1].replace(',', ''))
                        elif part == 'bytes/sec' and i > 0:
                            stats['transfer_rate'] = float(parts[i - 1].replace(',', ''))
                except (ValueError, IndexError):
                    pass
            
            # Count snapshots
            if 'sending incremental' in line.lower() or 'sending from' in line.lower():
                stats['snapshots_sent'] += 1
        
        return stats
    
    def _format_bytes(self, bytes_val: int) -> str:
        """
        Format bytes to human-readable string
        
        Args:
            bytes_val: Number of bytes
            
        Returns:
            Human-readable string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_val < 1024.0:
                return f"{bytes_val:.2f} {unit}"
            bytes_val /= 1024.0
        return f"{bytes_val:.2f} PB"
    
    def test_connection(
        self,
        remote_host: str,
        remote_port: int = 22,
        dataset: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Test SSH connection to remote host
        
        Args:
            remote_host: Remote hostname or IP
            remote_port: SSH port
            dataset: Optional dataset to check access to
            
        Returns:
            Dictionary with connection test results
        """
        try:
            # Test basic SSH connection
            if dataset:
                cmd = ['ssh', '-p', str(remote_port), remote_host, 'zfs', 'list', dataset]
            else:
                cmd = ['ssh', '-p', str(remote_port), remote_host, 'echo', 'Connection successful']
            
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
                'output': result.stdout.strip(),
                'remote_host': remote_host,
                'remote_port': remote_port
            }
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'failure',
                'message': 'Connection timed out',
                'remote_host': remote_host,
                'remote_port': remote_port
            }
        except subprocess.CalledProcessError as e:
            return {
                'status': 'failure',
                'message': f'Connection failed: {e.stderr}',
                'remote_host': remote_host,
                'remote_port': remote_port
            }
        except Exception as e:
            return {
                'status': 'failure',
                'message': f'Error: {str(e)}',
                'remote_host': remote_host,
                'remote_port': remote_port
            }
