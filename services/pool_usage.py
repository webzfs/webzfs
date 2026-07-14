"""
Pool Usage Investigation Service

When a pool export fails because the pool is busy, this service inspects the
system to find out what is holding the pool's filesystems open. It reports:

- Open files under the pool's mountpoints (processes with open descriptors)
- Shells and other processes whose current working directory is inside the
  pool (a common reason a pool cannot be exported)
- Advisory file locks held on paths inside the pool

The goal is to explain to the user exactly which processes must be stopped or
moved before the pool can be exported cleanly.

Platform support:
- Linux uses lsof and lslocks
- FreeBSD uses fstat
"""
import os
import subprocess
from typing import Dict, List, Any

from services.utils import (
    run_zfs_command,
    run_privileged_command,
    is_freebsd,
    is_netbsd,
)


class PoolUsageService:
    """Investigate what is keeping a ZFS pool busy so it cannot be exported."""

    def get_pool_mountpoints(self, pool_name: str) -> List[str]:
        """
        Return the list of active mountpoints for a pool and its datasets.

        Only real filesystem paths are returned. Datasets that are not
        mounted, or that use the "none" or "legacy" mountpoint values, are
        skipped because there is no directory tree to inspect.

        Args:
            pool_name: Name of the pool

        Returns:
            A de-duplicated list of mountpoint paths.
        """
        mountpoints: List[str] = []
        try:
            result = run_zfs_command(
                ['zfs', 'list', '-H', '-o', 'mountpoint', '-r', pool_name],
                check=False,
                timeout=15,
            )
            if result.returncode == 0:
                for line in result.stdout.strip().split('\n'):
                    path = line.strip()
                    if not path:
                        continue
                    if path in ('none', 'legacy', '-'):
                        continue
                    if not path.startswith('/'):
                        continue
                    if os.path.isdir(path) and path not in mountpoints:
                        mountpoints.append(path)
        except Exception:
            pass
        return mountpoints

    def investigate(self, pool_name: str) -> Dict[str, Any]:
        """
        Inspect the system for anything holding the pool busy.

        Args:
            pool_name: Name of the pool to investigate

        Returns:
            A dictionary describing what was found. Keys:
            - platform: 'linux' or 'freebsd'
            - mountpoints: list of mountpoints that were inspected
            - open_files: list of open-file records
            - cwd_holders: list of processes whose cwd is inside the pool
            - locks: list of advisory lock records
            - notes: list of informational or error messages
            - has_findings: True if anything was found holding the pool busy
        """
        if is_netbsd():
            platform_name = 'netbsd'
        elif is_freebsd():
            platform_name = 'freebsd'
        else:
            platform_name = 'linux'

        result: Dict[str, Any] = {
            'platform': platform_name,
            'mountpoints': [],
            'open_files': [],
            'cwd_holders': [],
            'locks': [],
            'notes': [],
            'has_findings': False,
        }

        mountpoints = self.get_pool_mountpoints(pool_name)
        result['mountpoints'] = mountpoints

        if not mountpoints:
            result['notes'].append(
                "No active mountpoints were found for this pool. The pool may "
                "still be held busy by a process using a raw dataset, a "
                "snapshot, or a device. Check the system logs for details."
            )
            return result

        if platform_name == 'freebsd':
            self._investigate_freebsd(mountpoints, result)
        else:
            self._investigate_linux(mountpoints, result)

        result['has_findings'] = bool(
            result['open_files'] or result['cwd_holders'] or result['locks']
        )
        return result

    def _investigate_linux(
        self, mountpoints: List[str], result: Dict[str, Any]
    ) -> None:
        """Collect open files, cwd holders, and locks on Linux using lsof/lslocks."""
        seen_open = set()
        seen_cwd = set()

        for mountpoint in mountpoints:
            output = self._run_tool(
                ['lsof', '-w', '-n', '-P', '+D', mountpoint],
                result,
                tool_name='lsof',
            )
            if output is None:
                continue
            for line in output.split('\n'):
                line = line.rstrip()
                if not line or line.startswith('COMMAND'):
                    continue
                parts = line.split(None, 8)
                if len(parts) < 9:
                    continue
                command = parts[0]
                pid = parts[1]
                user = parts[2]
                fd = parts[3]
                name = parts[8]
                if fd == 'cwd':
                    key = (pid, name)
                    if key in seen_cwd:
                        continue
                    seen_cwd.add(key)
                    result['cwd_holders'].append({
                        'pid': pid,
                        'user': user,
                        'command': command,
                        'path': name,
                    })
                else:
                    key = (pid, fd, name)
                    if key in seen_open:
                        continue
                    seen_open.add(key)
                    result['open_files'].append({
                        'pid': pid,
                        'user': user,
                        'command': command,
                        'fd': fd,
                        'path': name,
                    })

        self._collect_linux_locks(mountpoints, result)

    def _collect_linux_locks(
        self, mountpoints: List[str], result: Dict[str, Any]
    ) -> None:
        """Collect advisory file locks on Linux using lslocks."""
        output = self._run_tool(
            ['lslocks', '--noheadings', '--raw',
             '-o', 'command,pid,type,mode,path'],
            result,
            tool_name='lslocks',
        )
        if output is None:
            return
        for line in output.split('\n'):
            line = line.strip()
            if not line:
                continue
            parts = line.split(None, 4)
            if len(parts) < 5:
                continue
            command, pid, lock_type, mode, path = parts
            if not path.startswith('/'):
                continue
            for mountpoint in mountpoints:
                if path == mountpoint or path.startswith(mountpoint + '/'):
                    result['locks'].append({
                        'pid': pid,
                        'command': command,
                        'type': lock_type,
                        'mode': mode,
                        'path': path,
                    })
                    break

    def _investigate_freebsd(
        self, mountpoints: List[str], result: Dict[str, Any]
    ) -> None:
        """Collect open files, cwd holders, and locks on FreeBSD using fstat."""
        seen_open = set()
        seen_cwd = set()

        for mountpoint in mountpoints:
            output = self._run_tool(
                ['fstat', '-f', mountpoint],
                result,
                tool_name='fstat',
            )
            if output is None:
                continue
            for line in output.split('\n'):
                line = line.rstrip()
                if not line or line.startswith('USER'):
                    continue
                parts = line.split()
                if len(parts) < 5:
                    continue
                user = parts[0]
                command = parts[1]
                pid = parts[2]
                fd_field = parts[3]
                lock_marker = ''
                # fstat marks the lock state in a trailing column; note any
                # exclusive or shared advisory lock so it can be surfaced.
                if '-' in line:
                    tail = line.split()[-1]
                    if tail in ('-', 'r', 'w', 'rw'):
                        lock_marker = tail
                if fd_field.lower() in ('wd', 'cwd', 'root'):
                    key = (pid, command)
                    if key in seen_cwd:
                        continue
                    seen_cwd.add(key)
                    result['cwd_holders'].append({
                        'pid': pid,
                        'user': user,
                        'command': command,
                        'path': mountpoint,
                    })
                else:
                    key = (pid, fd_field, command)
                    if key in seen_open:
                        continue
                    seen_open.add(key)
                    record = {
                        'pid': pid,
                        'user': user,
                        'command': command,
                        'fd': fd_field,
                        'path': mountpoint,
                    }
                    result['open_files'].append(record)

        result['notes'].append(
            "On FreeBSD, advisory lock details are reported by fstat alongside "
            "each open file. Review the open files below and stop the listed "
            "processes before exporting."
        )

    def _run_tool(
        self, cmd: List[str], result: Dict[str, Any], tool_name: str
    ):
        """
        Run an inspection tool with privilege handling.

        Returns the command stdout on success, or None if the tool is missing
        or fails. Any problem is recorded in the notes list so the user can
        understand why a section may be incomplete.
        """
        try:
            completed = run_privileged_command(
                cmd, check=False, timeout=30
            )
            # lsof and fstat return non-zero when they find nothing, which is
            # not an error for our purposes, so stdout is used regardless.
            return completed.stdout or ''
        except subprocess.TimeoutExpired:
            result['notes'].append(
                f"The {tool_name} command timed out while scanning. Results "
                "may be incomplete."
            )
            return None
        except FileNotFoundError:
            result['notes'].append(
                f"The {tool_name} command is not installed. Install it to get "
                "a complete report of what is holding the pool busy."
            )
            return None
        except Exception as exc:
            result['notes'].append(
                f"Could not run {tool_name}: {exc}"
            )
            return None
