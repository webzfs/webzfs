"""
Syncoid Service
Wrapper for syncoid command-line tool for ZFS replication
Reference: https://github.com/jimsalterjrs/sanoid
Hi Jim. :)
"""
import os
import re
import shutil
import subprocess
import json
import shlex
import threading
from typing import Dict, List, Any, Optional, Callable
from pathlib import Path

from services.utils import run_privileged_command, run_zfs_command
from core.receive_options import recvoptions_string, validate_receive_options


class SyncoidService:
    """Service for managing syncoid replication operations"""
    
    # Allowlisted zfs send option letters that may be passed to syncoid
    # via --sendoptions. Only options known to be safe single-letter
    # zfs send flags are permitted; arbitrary user text is rejected.
    ALLOWED_SEND_OPTIONS = {'L', 'e', 'c', 'w', 'p'}
    
    # Helper tools syncoid uses to build its transfer pipeline. If any
    # is missing the replication pipe can fail in confusing ways, so
    # their presence is surfaced on the replication status card.
    HELPER_TOOLS = ['pv', 'lzop', 'mbuffer']

    # Matches the pv progress lines syncoid emits on stderr, e.g.
    # " 254MiB 0:00:05 [52.3MiB/s] [==>     ] 12% ETA 0:00:35".
    # bytes and rate always appear; percentage and ETA appear when pv
    # knows the expected stream size.
    PV_PROGRESS_PATTERN = re.compile(
        r'(?P<bytes>[\d.]+\s*[KMGTP]?i?B)\s+'
        r'\d+:\d+(?::\d+)?\s+'
        r'\[\s*(?P<rate>[\d.]+\s*[KMGTP]?i?B/s)\s*\]'
    )
    PV_PERCENT_PATTERN = re.compile(r'(?P<pct>\d+)%')
    PV_ETA_PATTERN = re.compile(r'ETA\s+(?P<eta>[\d:]+)')

    BYTE_UNIT_FACTORS = {
        'B': 1,
        'KiB': 1024, 'KB': 1000,
        'MiB': 1024 ** 2, 'MB': 1000 ** 2,
        'GiB': 1024 ** 3, 'GB': 1000 ** 3,
        'TiB': 1024 ** 4, 'TB': 1000 ** 4,
        'PiB': 1024 ** 5, 'PB': 1000 ** 5,
    }
    
    # Common paths where syncoid might be installed.
    # Checked as fallback when 'which' fails (e.g. restricted PATH on FreeBSD services).
    COMMON_PATHS = [
        '/usr/local/bin/syncoid',   # FreeBSD pkg install location
        '/usr/bin/syncoid',          # Linux package manager location
        '/usr/sbin/syncoid',         # Alternative Linux location
        '/usr/local/sbin/syncoid',   # Alternative FreeBSD/local location
    ]
    
    def __init__(self):
        """Initialize the syncoid service and discover the binary path"""
        self.syncoid_path = self._find_syncoid_path()

    @staticmethod
    def parse_additional_flags(additional_flags: Optional[str]) -> List[str]:
        """Parse user-supplied Syncoid flags into argv tokens.

        Shell-style quoting is supported for option values containing spaces,
        and the resulting tokens are passed directly to subprocess without a
        shell. WebZFS does not validate individual Syncoid options.

        Args:
            additional_flags: Additional Syncoid flags entered in the job form.

        Returns:
            Parsed argument tokens, or an empty list when no flags were entered.

        Raises:
            ValueError: If shell-style quoting is malformed.
        """
        if not additional_flags or not additional_flags.strip():
            return []
        return shlex.split(additional_flags, posix=True)
    
    def _find_syncoid_path(self) -> Optional[str]:
        """
        Discover the syncoid binary path.
        
        Tries 'which' first, then falls back to checking common install paths
        directly. This handles restricted PATH environments such as FreeBSD
        rc.d services where /usr/local/bin may not be in PATH.
        
        Returns:
            Full path to syncoid binary, or None if not found.
        """
        # Try to find syncoid using which first
        try:
            which_result = subprocess.run(
                ['which', 'syncoid'],
                capture_output=True,
                text=True
            )
            if which_result.returncode == 0:
                return which_result.stdout.strip()
        except Exception:
            pass
        
        # Check common paths directly (handles restricted PATH environments)
        for path in self.COMMON_PATHS:
            if Path(path).exists() and Path(path).is_file():
                return path
        
        return None
    
    def check_helper_tools(self) -> Dict[str, Any]:
        """
        Check for the helper tools syncoid uses in its pipeline
        (pv, lzop, mbuffer).
        
        Uses the same augmented PATH that the syncoid subprocess gets,
        so the result matches what syncoid will actually find at run
        time, including under restricted PATH environments.
        
        Returns:
            Dictionary with:
            - helpers: mapping of tool name to full path (or None)
            - missing_helpers: list of tool names not found
        """
        augmented_path = self._build_syncoid_environment().get('PATH', '')
        helpers: Dict[str, Optional[str]] = {}
        for tool_name in self.HELPER_TOOLS:
            helpers[tool_name] = shutil.which(tool_name, path=augmented_path)
        missing = [name for name, path in helpers.items() if not path]
        return {
            'helpers': helpers,
            'missing_helpers': missing,
        }
    
    def check_syncoid_status(self) -> Dict[str, Any]:
        """
        Check if syncoid is installed and get its status.
        
        Uses the path discovered at init time. If not found at init,
        re-checks in case syncoid was installed after service start.
        
        Returns:
            Dictionary with syncoid status information
        """
        try:
            # Re-discover if not found at init (in case installed after startup)
            syncoid_path = self.syncoid_path
            if not syncoid_path:
                syncoid_path = self._find_syncoid_path()
                if syncoid_path:
                    self.syncoid_path = syncoid_path
            
            helper_status = self.check_helper_tools()
            
            if not syncoid_path:
                return {
                    'installed': False,
                    'path': None,
                    'version': None,
                    'helpers': helper_status['helpers'],
                    'missing_helpers': helper_status['missing_helpers'],
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
            except Exception:
                version = 'unknown'
            
            return {
                'installed': True,
                'path': syncoid_path,
                'version': version,
                'helpers': helper_status['helpers'],
                'missing_helpers': helper_status['missing_helpers'],
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
        ssh_key: Optional[str] = None,
        ssh_options: Optional[List[str]] = None,
        send_options: Optional[str] = None,
        recv_options: Optional[Dict[str, Any]] = None,
        additional_flags: Optional[str] = None,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None,
        debug: bool = False,
        quiet: bool = False,
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
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
            ssh_key: Absolute path to the SSH private key (--sshkey).
                     Required for remote replication so syncoid does not
                     depend on the executing user's default SSH identity
                     search (issue #195).
            ssh_options: List of SSH -o option strings passed via
                         --sshoption (e.g. ['IdentitiesOnly=yes',
                         'StrictHostKeyChecking=yes']).
            send_options: ZFS send option letters passed to syncoid via
                          --sendoptions (e.g. 'L' for large-block send).
                          Validated against ALLOWED_SEND_OPTIONS; any
                          other value is rejected (issue #204).
            recv_options: Structured receive options saved with the job
                          (``-u``/``-o``/``-x``) rendered as Syncoid's
                          --recvoptions value (issue #23). Syncoid does
                          not honour ``-d``/``-e`` there.
            additional_flags: Shell-style additional Syncoid flags. The text
                              is parsed with shlex and appended as argv tokens
                              before the source and target. It is never passed
                              to a shell.
            source_host: Source SSH host (alternative to including in source string)
            target_host: Target SSH host (alternative to including in target string)
            debug: Enable debug output
            quiet: Quiet mode
            progress_callback: Optional callable invoked with progress
                dictionaries (bytes_transferred, percentage, rate, eta)
                parsed from the pv progress lines syncoid writes to
                stderr. When provided, stderr is streamed instead of
                buffered so progress is reported live.
            **additional_options: Additional syncoid options
            
        Returns:
            Dictionary with execution results
        """
        try:
            # Re-discover path if not cached (in case installed after startup)
            if not self.syncoid_path:
                self.syncoid_path = self._find_syncoid_path()
            
            if not self.syncoid_path:
                return {
                    'success': False,
                    'error': 'syncoid binary not found. Install sanoid/syncoid and restart WebZFS.'
                }
            
            # Build the syncoid command using the discovered full path
            cmd = [self.syncoid_path]
            
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
            
            if ssh_key:
                # Explicit identity file. Without this, syncoid running
                # under sudo would search root's SSH environment for keys
                # and fail with keys managed by SSH Manager (issue #195).
                cmd.extend(['--sshkey', ssh_key])
            
            if ssh_options:
                for option in ssh_options:
                    cmd.extend(['--sshoption', option])
            
            if send_options:
                # Validate against the allowlist. Each character must be
                # a known safe zfs send flag letter. This prevents
                # arbitrary command text from being passed through.
                invalid = [c for c in send_options if c not in self.ALLOWED_SEND_OPTIONS]
                if invalid:
                    return {
                        'success': False,
                        'error': (
                            f"Invalid send option(s): {''.join(invalid)}. "
                            f"Allowed options: "
                            f"{''.join(sorted(self.ALLOWED_SEND_OPTIONS))}"
                        )
                    }
                cmd.append(f'--sendoptions={send_options}')
            
            if recv_options:
                # Validate before rendering so a job saved through the API
                # with a bad property name or an encryption override fails
                # with a readable error instead of a syncoid crash.
                validate_receive_options(
                    recv_options.get('overrides') or {},
                    recv_options.get('excludes') or [],
                    allow_encryption=False,
                )
                recv_string = recvoptions_string(recv_options)
                if recv_string:
                    cmd.append(f'--recvoptions={recv_string}')

            if debug:
                cmd.append('--debug')
            
            if quiet:
                cmd.append('--quiet')

            cmd.extend(self.parse_additional_flags(additional_flags))
            
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
            
            # Execute through a single seam so the privilege model can be
            # changed in one place (planned least-privilege work will run
            # syncoid as the webzfs user instead of via sudo).
            result, display_command = self._run_syncoid(cmd, progress_callback)
            
            # Parse output for stats
            stats = self._parse_syncoid_output(result.stdout, result.stderr)
            
            return {
                'success': result.returncode == 0,
                'returncode': result.returncode,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'stats': stats,
                'command': display_command
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def _build_syncoid_environment(self) -> Dict[str, str]:
        """
        Build the environment for the syncoid subprocess with an
        augmented PATH.
        
        Syncoid probes for its optional helper binaries (pv, lzop,
        mbuffer, gzip, zstd) using the PATH it inherits. Restricted
        environments such as FreeBSD rc.d services and cron jobs run
        with a minimal PATH that excludes /usr/local/bin, so syncoid
        reports the helpers as unavailable even when they are installed.
        Appending the standard system and local binary directories to
        PATH lets syncoid find them regardless of how WebZFS was started.
        
        Returns:
            Copy of the current environment with PATH augmented.
        """
        standard_paths = [
            '/usr/local/sbin', '/usr/local/bin',
            '/usr/sbin', '/usr/bin', '/sbin', '/bin'
        ]
        env = dict(os.environ)
        path_parts = [p for p in env.get('PATH', '').split(':') if p]
        for path_entry in standard_paths:
            if path_entry not in path_parts:
                path_parts.append(path_entry)
        env['PATH'] = ':'.join(path_parts)
        return env
    
    def _run_syncoid(
        self,
        cmd: List[str],
        progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
    ) -> tuple:
        """
        Execute a syncoid command with the current privilege policy.
        
        This is the single execution seam for syncoid. The current policy
        wraps the command with sudo on Linux via run_privileged_command().
        A future least-privilege change (run syncoid as the webzfs user
        with only ZFS subcommands elevated) only needs to modify this
        method.
        
        The subprocess environment PATH is augmented with the standard
        system and local binary directories so syncoid can find its
        helper tools (pv, lzop, mbuffer) under restricted PATH
        environments such as FreeBSD rc.d services and cron jobs.
        
        When a progress_callback is provided, stderr is attached to a
        pseudo-terminal and streamed while the process runs. The pty is
        required because pv (syncoid's progress stage) only emits
        progress frames when its stderr is a terminal; with a plain
        pipe it stays silent. Each pv frame is parsed and forwarded to
        the callback (throttled to one call every two seconds). Only
        newline-terminated stderr lines are retained in the returned
        stderr text so pv's repeated progress frames do not flood the
        execution log.
        
        Args:
            cmd: The syncoid command and arguments (without sudo)
            progress_callback: Optional progress consumer.
            
        Returns:
            Tuple of (CompletedProcess, display_command) where
            display_command is the shell-quoted representation of the
            actual executed command including any privilege wrapper.
        """
        from services.utils import build_privileged_command
        
        full_cmd = build_privileged_command(cmd)
        env = self._build_syncoid_environment()
        
        if not progress_callback:
            result = subprocess.run(
                full_cmd,
                check=False,
                text=True,
                capture_output=True,
                env=env
            )
            # Report the actual executed argv (including sudo when used)
            # so troubleshooting reflects the real execution identity.
            return result, shlex.join(full_cmd)
        
        import pty
        master_fd, slave_fd = pty.openpty()
        try:
            process = subprocess.Popen(
                full_cmd,
                stdout=subprocess.PIPE,
                stderr=slave_fd,
                env=env
            )
        finally:
            # The child holds its own copy of the slave end; close ours
            # so reads on the master end see EOF when the child exits.
            os.close(slave_fd)
        
        stderr_lines: List[str] = []
        
        def consume_stderr() -> None:
            """Stream the stderr pty, forwarding parsed pv progress frames."""
            import time
            buffer = b''
            last_report = 0.0
            while True:
                try:
                    chunk = os.read(master_fd, 4096)
                except OSError:
                    # ptys raise EIO instead of returning EOF when the
                    # child closes its end.
                    break
                if not chunk:
                    break
                buffer += chunk
                # pv separates progress frames with \r; the pty also
                # translates \n to \r\n (ONLCR), so both terminators
                # delimit segments. Progress frames are forwarded to
                # the callback; everything else is kept as stderr text.
                while True:
                    newline_pos = buffer.find(b'\n')
                    carriage_pos = buffer.find(b'\r')
                    positions = [p for p in (newline_pos, carriage_pos) if p != -1]
                    if not positions:
                        break
                    cut = min(positions)
                    segment = buffer[:cut].decode('utf-8', errors='replace')
                    buffer = buffer[cut + 1:]
                    if not segment.strip():
                        continue
                    progress = self._parse_pv_progress(segment)
                    if progress:
                        now = time.time()
                        if now - last_report >= 2:
                            last_report = now
                            try:
                                progress_callback(progress)
                            except Exception:
                                # Progress recording must never break
                                # the replication itself.
                                pass
                    else:
                        stderr_lines.append(segment.rstrip())
            leftover = buffer.decode('utf-8', errors='replace').strip()
            if leftover and not self._parse_pv_progress(leftover):
                stderr_lines.append(leftover)
        
        stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
        stderr_thread.start()
        stdout_text = process.stdout.read().decode('utf-8', errors='replace')
        returncode = process.wait()
        stderr_thread.join(timeout=10)
        try:
            os.close(master_fd)
        except OSError:
            pass
        
        result = subprocess.CompletedProcess(
            args=full_cmd,
            returncode=returncode,
            stdout=stdout_text,
            stderr='\n'.join(stderr_lines)
        )
        return result, shlex.join(full_cmd)
    
    def _parse_pv_progress(self, segment: str) -> Optional[Dict[str, Any]]:
        """
        Parse a pv progress frame from syncoid's stderr.
        
        Returns a dictionary with bytes_transferred, transfer_rate,
        percentage, and eta when the segment looks like pv progress
        output, or None for ordinary stderr text.
        """
        match = self.PV_PROGRESS_PATTERN.search(segment)
        if not match:
            return None
        progress: Dict[str, Any] = {
            'bytes_transferred': self._parse_pv_bytes(match.group('bytes')),
            'transfer_rate': match.group('rate').replace(' ', ''),
            'percentage': 0.0,
            'eta': None,
        }
        percent_match = self.PV_PERCENT_PATTERN.search(segment)
        if percent_match:
            progress['percentage'] = min(float(percent_match.group('pct')), 99.9)
        eta_match = self.PV_ETA_PATTERN.search(segment)
        if eta_match:
            progress['eta'] = eta_match.group('eta')
        return progress
    
    def _parse_pv_bytes(self, text: str) -> int:
        """Convert a pv byte string such as '254MiB' to bytes."""
        match = re.match(r'([\d.]+)\s*([KMGTP]?i?B)', text.strip())
        if not match:
            return 0
        value = float(match.group(1))
        unit = match.group(2)
        return int(value * self.BYTE_UNIT_FACTORS.get(unit, 1))
    
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
