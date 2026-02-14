import os
import subprocess
import tempfile
from services.utils import is_linux


def read_file(file_path: str, use_sudo: bool = False) -> str:
    """
    Read a file's content.
    
    Args:
        file_path: Path to the file to read
        use_sudo: If True, use sudo to read root-owned files (Linux only)
        
    Returns:
        File content as string
    """
    file_path = os.path.expanduser(file_path)
    
    # Try regular read first
    try:
        with open(file_path, "r") as f:
            return f.read()
    except PermissionError:
        if use_sudo and is_linux():
            # Use sudo cat to read the file
            result = subprocess.run(
                ['sudo', 'cat', file_path],
                capture_output=True,
                text=True,
                check=True
            )
            return result.stdout
        else:
            raise


def save_file(file_path: str, content: str, use_sudo: bool = False) -> None:
    """
    Save content to a file.
    
    Args:
        file_path: Path to the file to write
        content: Content to write
        use_sudo: If True, use sudo to write root-owned files (Linux only)
    """
    file_path = os.path.expanduser(file_path)
    directory = os.path.dirname(file_path)
    
    if use_sudo and is_linux():
        # Create parent directory if needed using sudo
        if directory and not os.path.exists(directory):
            subprocess.run(
                ['sudo', 'mkdir', '-p', directory],
                check=True
            )
        
        # Write content to a temp file, then use sudo to move it
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.tmp') as tmp:
            tmp.write(content)
            tmp_path = tmp.name
        
        try:
            # Use sudo tee to write the file (preserves content)
            with open(tmp_path, 'r') as tmp_file:
                subprocess.run(
                    ['sudo', 'tee', file_path],
                    stdin=tmp_file,
                    stdout=subprocess.DEVNULL,  # Suppress tee's stdout
                    check=True
                )
        finally:
            # Clean up temp file
            os.unlink(tmp_path)
    else:
        # Regular write
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(file_path, "w") as f:
            f.write(content)


def can_read_file(file_path: str) -> bool:
    """
    Check if a file exists and is readable.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if file exists and is readable, False otherwise
    """
    file_path = os.path.expanduser(file_path)
    return os.path.exists(file_path) and os.access(file_path, os.R_OK)


def can_write_file(file_path: str) -> bool:
    """
    Check if a file can be written to (file or parent dir is writable).
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if file/directory is writable, False otherwise
    """
    file_path = os.path.expanduser(file_path)
    if os.path.exists(file_path):
        return os.access(file_path, os.W_OK)
    else:
        # Check if parent directory is writable
        parent = os.path.dirname(file_path) or '.'
        return os.access(parent, os.W_OK)


def needs_sudo(file_path: str) -> bool:
    """
    Check if sudo is needed to read/write a file.
    
    Args:
        file_path: Path to the file
        
    Returns:
        True if sudo is needed, False otherwise
    """
    file_path = os.path.expanduser(file_path)
    if os.path.exists(file_path):
        return not os.access(file_path, os.R_OK) or not os.access(file_path, os.W_OK)
    else:
        # Check parent directory
        parent = os.path.dirname(file_path) or '.'
        return not os.access(parent, os.W_OK)
