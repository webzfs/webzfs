import platform
import subprocess
from typing import Optional, List, Tuple

from core.exceptions import ProcessError


# Cache the platform detection result
_PLATFORM_CACHE: Optional[str] = None


def get_os_type() -> str:
    """Returns 'Linux', 'FreeBSD', 'NetBSD', etc."""
    global _PLATFORM_CACHE
    if _PLATFORM_CACHE is None:
        _PLATFORM_CACHE = platform.system()
    return _PLATFORM_CACHE


def is_freebsd() -> bool:
    """Check if running on FreeBSD"""
    return get_os_type() == 'FreeBSD'


def is_netbsd() -> bool:
    """Check if running on NetBSD"""
    return get_os_type() == 'NetBSD'


def is_linux() -> bool:
    """Check if running on Linux"""
    return get_os_type() == 'Linux'


def is_bsd() -> bool:
    """Check if running on any BSD variant (FreeBSD, NetBSD, OpenBSD)"""
    return get_os_type() in ('FreeBSD', 'NetBSD', 'OpenBSD')


def needs_sudo_for_zfs() -> bool:
    """
    Check if sudo is needed for ZFS commands.
    
    On Linux, the webzfs user needs sudo to run zfs/zpool commands.
    On BSD systems (FreeBSD, NetBSD), ZFS permissions work differently
    and sudo is typically not needed when proper permissions are configured.
    
    Returns:
        True if sudo should be prepended to ZFS commands, False otherwise.
    """
    return is_linux()


def needs_sudo_for_privileged() -> bool:
    """
    Check if sudo is needed for general privileged commands.
    
    On Linux, the webzfs user needs sudo to run commands like:
    - smartctl (SMART monitoring)
    - sanoid/syncoid (snapshot/replication tools)
    - lsblk/blkid (disk utilities)
    - service/systemctl (service management)
    
    On BSD systems, these commands typically work without sudo when
    proper permissions are configured via /etc/devfs.conf or similar.
    
    Returns:
        True if sudo should be prepended to privileged commands, False otherwise.
    """
    return is_linux()


# List of commands that require sudo on Linux
PRIVILEGED_COMMANDS = {
    # ZFS commands
    'zfs', 'zpool', 'zdb',
    # SMART monitoring
    'smartctl',
    # Sanoid/Syncoid
    'sanoid', 'syncoid',
    # Service management
    'systemctl', 'service',
    # Disk utilities (some operations)
    'lsblk', 'blkid',
    # Crontab
    'crontab',
}


def build_privileged_command(cmd: List[str], use_sudo: Optional[bool] = None) -> List[str]:
    """
    Build a command, optionally prepending sudo based on platform and command type.
    
    Args:
        cmd: The command and arguments as a list
        use_sudo: Override automatic sudo detection. If None, uses platform detection.
                  If True, always prepend sudo. If False, never prepend sudo.
    
    Returns:
        The command list, with sudo prepended if needed.
    """
    if use_sudo is None:
        # Check if the command is in our privileged list and we're on Linux
        if cmd and len(cmd) > 0:
            base_cmd = cmd[0].split('/')[-1]  # Handle full paths
            use_sudo = needs_sudo_for_privileged() and base_cmd in PRIVILEGED_COMMANDS
        else:
            use_sudo = False
    
    if use_sudo:
        return ['sudo'] + cmd
    return cmd


def run_privileged_command(
    cmd: List[str],
    *,
    check: bool = True,
    text: bool = True,
    capture_output: bool = True,
    timeout: Optional[float] = None,
    input_data: Optional[str] = None,
    use_sudo: Optional[bool] = None
) -> subprocess.CompletedProcess:
    """
    Run a privileged command with platform-appropriate sudo handling.
    
    On Linux systems, commands in PRIVILEGED_COMMANDS are automatically prefixed with sudo.
    On BSD systems (FreeBSD, NetBSD), commands are run directly.
    
    Args:
        cmd: The command and arguments as a list
        check: If True, raise CalledProcessError on non-zero exit code
        text: If True, decode stdout/stderr as text
        capture_output: If True, capture stdout and stderr
        timeout: Optional timeout in seconds
        input_data: Optional input to send to stdin
        use_sudo: Override automatic sudo detection
    
    Returns:
        subprocess.CompletedProcess with the command results
    """
    full_cmd = build_privileged_command(cmd, use_sudo=use_sudo)
    
    return subprocess.run(
        full_cmd,
        check=check,
        text=text,
        capture_output=capture_output,
        timeout=timeout,
        input=input_data
    )


def build_zfs_command(cmd: List[str], use_sudo: Optional[bool] = None) -> List[str]:
    """
    Build a ZFS command, optionally prepending sudo based on platform.
    
    Args:
        cmd: The command and arguments as a list (e.g., ['zfs', 'list', '-H'])
        use_sudo: Override automatic sudo detection. If None, uses platform detection.
                  If True, always prepend sudo. If False, never prepend sudo.
    
    Returns:
        The command list, with sudo prepended if needed.
    """
    if use_sudo is None:
        use_sudo = needs_sudo_for_zfs()
    
    if use_sudo:
        return ['sudo'] + cmd
    return cmd


def run_command(args: list[str] | str, *, check: bool = True, text: bool = True) -> str:
    if isinstance(args, str):
        args = args.strip().split()

    try:
        completed = subprocess.run(
            args=args,
            check=check,
            text=text,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except Exception as exc:
        msg = f"Command {args} failed with error:\n{exc}"
        if hasattr(exc, "stdout"):
            msg += "\n{exc.stdout}"
        raise ProcessError(msg)
    return completed.stdout


def run_zfs_command(
    cmd: List[str],
    *,
    check: bool = True,
    text: bool = True,
    capture_output: bool = True,
    timeout: Optional[float] = None,
    input_data: Optional[str] = None,
    use_sudo: Optional[bool] = None
) -> subprocess.CompletedProcess:
    """
    Run a ZFS/ZPOOL command with platform-appropriate sudo handling.
    
    On Linux systems, commands are automatically prefixed with sudo.
    On BSD systems (FreeBSD, NetBSD), commands are run directly.
    
    Args:
        cmd: The command and arguments as a list (e.g., ['zfs', 'list', '-H'])
        check: If True, raise CalledProcessError on non-zero exit code
        text: If True, decode stdout/stderr as text
        capture_output: If True, capture stdout and stderr
        timeout: Optional timeout in seconds
        input_data: Optional input to send to stdin
        use_sudo: Override automatic sudo detection
    
    Returns:
        subprocess.CompletedProcess with the command results
    
    Raises:
        subprocess.CalledProcessError: If check=True and command fails
        subprocess.TimeoutExpired: If timeout is exceeded
    """
    full_cmd = build_zfs_command(cmd, use_sudo=use_sudo)
    
    return subprocess.run(
        full_cmd,
        check=check,
        text=text,
        capture_output=capture_output,
        timeout=timeout,
        input=input_data
    )


def run_zfs_command_with_pipe(
    send_cmd: List[str],
    receive_cmd: List[str],
    use_sudo: Optional[bool] = None
) -> Tuple[subprocess.Popen, subprocess.Popen]:
    """
    Run two ZFS commands piped together (e.g., zfs send | zfs receive).
    
    Args:
        send_cmd: The sending command (e.g., ['zfs', 'send', 'pool@snap'])
        receive_cmd: The receiving command (e.g., ['zfs', 'receive', 'pool/target'])
        use_sudo: Override automatic sudo detection
    
    Returns:
        Tuple of (send_process, receive_process) Popen objects
    """
    full_send_cmd = build_zfs_command(send_cmd, use_sudo=use_sudo)
    full_receive_cmd = build_zfs_command(receive_cmd, use_sudo=use_sudo)
    
    send_process = subprocess.Popen(
        full_send_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    receive_process = subprocess.Popen(
        full_receive_cmd,
        stdin=send_process.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Allow send_process to receive SIGPIPE if receive_process exits
    send_process.stdout.close()
    
    return send_process, receive_process
