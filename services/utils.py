import platform
import subprocess

from core.exceptions import ProcessError


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


def get_os_type() -> str:
    """Returns 'Linux' or 'FreeBSD'"""
    return platform.system()


def is_freebsd() -> bool:
    """Check if running on FreeBSD"""
    return get_os_type() == 'FreeBSD'


def is_netbsd() -> bool:
    """Check if running on NetBSD"""
    return get_os_type() == 'NetBSD'
