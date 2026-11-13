"""
Cron Job Management Service
Also manages cron jobs for scheduled Syncoid replication
"""
import subprocess
import re
from typing import List, Dict, Any, Optional
from pathlib import Path

from services.ssh_connection import validate_ssh_host


# Strict cron field validation patterns
CRON_FIELD_PATTERNS = {
    'minute': r'^(\*|([0-5]?\d)(,([0-5]?\d))*|([0-5]?\d)-([0-5]?\d)|(\*/([1-9]|[1-5]\d)))$',
    'hour': r'^(\*|([01]?\d|2[0-3])(,([01]?\d|2[0-3]))*|([01]?\d|2[0-3])-([01]?\d|2[0-3])|(\*/([1-9]|1\d|2[0-3])))$',
    'day': r'^(\*|([1-9]|[12]\d|3[01])(,([1-9]|[12]\d|3[01]))*|([1-9]|[12]\d|3[01])-([1-9]|[12]\d|3[01])|(\*/([1-9]|[12]\d|3[01])))$',
    'month': r'^(\*|([1-9]|1[0-2])(,([1-9]|1[0-2]))*|([1-9]|1[0-2])-([1-9]|1[0-2])|(\*/([1-9]|1[0-2])))$',
    'weekday': r'^(\*|[0-7](,[0-7])*|[0-7]-[0-7]|(\*/[1-7]))$',
}

# Characters that are dangerous in cron entries and shell commands
DANGEROUS_CHARS_PATTERN = re.compile(r'[\n\r\x00\x0b\x0c]')  # Newlines and null bytes
SHELL_DANGEROUS_PATTERN = re.compile(r'[;&|`$(){}[\]<>\\\'\"!#]')  # Shell metacharacters

# Valid characters for dataset names (ZFS naming rules)
DATASET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_.\-:/]+$')

# Hostnames are validated by services.ssh_connection.validate_ssh_host.

# Valid characters for job names (alphanumeric, underscore, hyphen, space)
JOB_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_\- ]+$')

# Valid bandwidth limit pattern (e.g., "10M", "1G", "500K")
BANDWIDTH_PATTERN = re.compile(r'^[0-9]+[KMGkmg]?$')

# Valid compression algorithms
VALID_COMPRESSION = {'gzip', 'lz4', 'zstd', 'lzo', 'none'}


class CronService:
    """Service for managing system cron jobs"""
    
    CRON_DIR = "/etc/cron.d"
    SYNCOID_CRON_FILE = "syncoid-replication"
    
    def __init__(self):
        """Initialize cron service"""
        self.cron_file_path = Path(self.CRON_DIR) / self.SYNCOID_CRON_FILE
    
    @staticmethod
    def _sanitize_input(value: str) -> str:
        """
        Remove dangerous characters from input string.
        
        Args:
            value: Input string to sanitize
            
        Returns:
            Sanitized string with dangerous characters removed
        """
        if not value:
            return value
        # Remove newlines, carriage returns, null bytes, and other control characters
        return DANGEROUS_CHARS_PATTERN.sub('', value).strip()
    
    @staticmethod
    def _validate_job_name(name: str) -> None:
        """
        Validate job name for safe characters.
        
        Args:
            name: Job name to validate
            
        Raises:
            ValueError: If name contains invalid characters
        """
        if not name:
            raise ValueError("Job name cannot be empty")
        if len(name) > 64:
            raise ValueError("Job name too long (max 64 characters)")
        if not JOB_NAME_PATTERN.match(name):
            raise ValueError("Job name contains invalid characters. Only alphanumeric, underscore, hyphen, and space allowed")
    
    @staticmethod
    def _validate_dataset_name(dataset: str, field_name: str = "Dataset") -> None:
        """
        Validate ZFS dataset name.
        
        Args:
            dataset: Dataset name to validate
            field_name: Name of field for error messages
            
        Raises:
            ValueError: If dataset name is invalid
        """
        if not dataset:
            raise ValueError(f"{field_name} name cannot be empty")
        if len(dataset) > 256:
            raise ValueError(f"{field_name} name too long (max 256 characters)")
        if not DATASET_NAME_PATTERN.match(dataset):
            raise ValueError(f"{field_name} contains invalid characters")
        # Check for shell metacharacters that could be dangerous
        if SHELL_DANGEROUS_PATTERN.search(dataset):
            raise ValueError(f"{field_name} contains forbidden shell characters")
    
    @staticmethod
    def _validate_hostname(hostname: str, field_name: str = "Host") -> None:
        """
        Validate hostname.
        
        Args:
            hostname: Hostname to validate
            field_name: Name of field for error messages
            
        Raises:
            ValueError: If hostname is invalid
        """
        if not hostname:
            return  # Hostname is optional
        # Shared SSH host rules: IP address or DNS hostname, never starting
        # with '-' (which ssh and syncoid would parse as an option).
        try:
            validate_ssh_host(hostname)
        except ValueError as error:
            raise ValueError(f"{field_name}: {error}")
    
    @staticmethod
    def _validate_bandwidth(bwlimit: str, field_name: str = "Bandwidth limit") -> None:
        """
        Validate bandwidth limit format.
        
        Args:
            bwlimit: Bandwidth limit to validate
            field_name: Name of field for error messages
            
        Raises:
            ValueError: If bandwidth limit format is invalid
        """
        if not bwlimit:
            return  # Bandwidth limit is optional
        if not BANDWIDTH_PATTERN.match(bwlimit):
            raise ValueError(f"{field_name} must be a number optionally followed by K, M, or G (e.g., '10M')")
    
    @staticmethod
    def _validate_compression(compress: str) -> None:
        """
        Validate compression algorithm.
        
        Args:
            compress: Compression algorithm to validate
            
        Raises:
            ValueError: If compression algorithm is invalid
        """
        if not compress:
            return  # Compression is optional
        if compress.lower() not in VALID_COMPRESSION:
            raise ValueError(f"Invalid compression algorithm. Must be one of: {', '.join(sorted(VALID_COMPRESSION))}")
    
    def _validate_cron_schedule_strict(self, schedule: str) -> Dict[str, Any]:
        """
        Strictly validate a cron schedule expression using regex patterns.
        
        Args:
            schedule: Cron schedule string
            
        Returns:
            Dictionary with validation results
        """
        # Sanitize input first
        schedule = self._sanitize_input(schedule)
        
        if not schedule:
            return {'valid': False, 'error': 'Cron schedule cannot be empty'}
        
        # Check for any dangerous characters
        if SHELL_DANGEROUS_PATTERN.search(schedule):
            return {'valid': False, 'error': 'Cron schedule contains forbidden characters'}
        
        # Split into fields
        parts = schedule.split()
        
        if len(parts) != 5:
            return {
                'valid': False,
                'error': 'Cron schedule must have exactly 5 fields: minute hour day month weekday'
            }
        
        field_names = ['minute', 'hour', 'day', 'month', 'weekday']
        field_ranges = {
            'minute': (0, 59),
            'hour': (0, 23),
            'day': (1, 31),
            'month': (1, 12),
            'weekday': (0, 7)
        }
        
        for i, (field_name, field_value) in enumerate(zip(field_names, parts)):
            # Check against strict regex pattern
            pattern = CRON_FIELD_PATTERNS[field_name]
            if not re.match(pattern, field_value):
                min_val, max_val = field_ranges[field_name]
                return {
                    'valid': False,
                    'error': f'Invalid {field_name} value "{field_value}". Expected: *, {min_val}-{max_val}, ranges (e.g., 1-5), steps (e.g., */5), or lists (e.g., 1,3,5)'
                }
        
        return {
            'valid': True,
            'schedule': schedule,
            'description': self._describe_schedule(schedule)
        }

    def list_syncoid_jobs(self) -> List[Dict[str, Any]]:
        """
        List all Syncoid cron jobs
        
        Returns:
            List of cron job definitions
        """
        jobs = []
        
        try:
            if not self.cron_file_path.exists():
                return jobs
            
            with open(self.cron_file_path, 'r') as f:
                lines = f.readlines()
            
            for line in lines:
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Parse cron line
                job = self._parse_cron_line(line)
                if job:
                    jobs.append(job)
            
            return jobs
            
        except Exception as e:
            raise Exception(f"Failed to list cron jobs: {str(e)}")
    
    def add_syncoid_job(
        self,
        name: str,
        schedule: str,
        source: str,
        target: str,
        source_host: Optional[str] = None,
        target_host: Optional[str] = None,
        recursive: bool = False,
        compress: Optional[str] = None,
        source_bwlimit: Optional[str] = None,
        target_bwlimit: Optional[str] = None,
        **options
    ) -> None:
        """
        Add a new Syncoid cron job
        
        Args:
            name: Job name (for identification in comments)
            schedule: Cron schedule (e.g., "0 2 * * *" for daily at 2 AM)
            source: Source dataset
            target: Target dataset
            source_host: Optional source host
            target_host: Optional target host
            recursive: Replicate recursively
            compress: Compression algorithm
            source_bwlimit: Source bandwidth limit
            target_bwlimit: Target bandwidth limit
            **options: Additional syncoid options
            
        Raises:
            ValueError: If any input validation fails
            Exception: If cron job creation fails
        """
        # Sanitize all inputs first
        name = self._sanitize_input(name)
        schedule = self._sanitize_input(schedule)
        source = self._sanitize_input(source)
        target = self._sanitize_input(target)
        source_host = self._sanitize_input(source_host) if source_host else None
        target_host = self._sanitize_input(target_host) if target_host else None
        compress = self._sanitize_input(compress) if compress else None
        source_bwlimit = self._sanitize_input(source_bwlimit) if source_bwlimit else None
        target_bwlimit = self._sanitize_input(target_bwlimit) if target_bwlimit else None
        
        # Validate all inputs
        self._validate_job_name(name)
        
        # Validate cron schedule strictly
        schedule_validation = self._validate_cron_schedule_strict(schedule)
        if not schedule_validation['valid']:
            raise ValueError(f"Invalid cron schedule: {schedule_validation['error']}")
        
        # Validate dataset names
        self._validate_dataset_name(source, "Source dataset")
        self._validate_dataset_name(target, "Target dataset")
        
        # Validate hostnames
        self._validate_hostname(source_host, "Source host")
        self._validate_hostname(target_host, "Target host")
        
        # Validate compression
        self._validate_compression(compress)
        
        # Validate bandwidth limits
        self._validate_bandwidth(source_bwlimit, "Source bandwidth limit")
        self._validate_bandwidth(target_bwlimit, "Target bandwidth limit")
        
        try:
            # Build syncoid command
            cmd_parts = ['syncoid']
            
            if recursive:
                cmd_parts.append('-r')
            
            if compress:
                cmd_parts.extend(['--compress', compress])
            
            if source_bwlimit:
                cmd_parts.extend(['--source-bwlimit', source_bwlimit])
            
            if target_bwlimit:
                cmd_parts.extend(['--target-bwlimit', target_bwlimit])
            
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
            
            cmd_parts.extend([source_str, target_str])
            
            command = ' '.join(cmd_parts)
            
            # Build cron entry
            # Format: minute hour day month weekday user command
            cron_entry = f"{schedule} root {command}"
            
            # Read existing jobs
            existing_jobs = []
            if self.cron_file_path.exists():
                with open(self.cron_file_path, 'r') as f:
                    existing_jobs = f.readlines()
            
            # Add new job with comment
            with open(self.cron_file_path, 'w') as f:
                # Write existing jobs
                f.writelines(existing_jobs)
                
                # Write new job
                f.write(f"\n# Syncoid job: {name}\n")
                f.write(f"{cron_entry}\n")
            
            # Set proper permissions
            self.cron_file_path.chmod(0o644)
            
        except ValueError:
            raise  # Re-raise validation errors
        except Exception as e:
            raise Exception(f"Failed to add cron job: {str(e)}")
    
    def remove_syncoid_job(self, job_name: str) -> None:
        """
        Remove a Syncoid cron job by name
        
        Args:
            job_name: Name of the job to remove
            
        Raises:
            ValueError: If job name validation fails
            Exception: If cron job removal fails
        """
        # Sanitize and validate job name
        job_name = self._sanitize_input(job_name)
        self._validate_job_name(job_name)
        
        try:
            if not self.cron_file_path.exists():
                return
            
            with open(self.cron_file_path, 'r') as f:
                lines = f.readlines()
            
            # Filter out the job and its comment
            new_lines = []
            skip_next = False
            
            for line in lines:
                if skip_next:
                    skip_next = False
                    continue
                
                if line.strip() == f"# Syncoid job: {job_name}":
                    skip_next = True
                    continue
                
                new_lines.append(line)
            
            # Write back
            with open(self.cron_file_path, 'w') as f:
                f.writelines(new_lines)
            
        except ValueError:
            raise  # Re-raise validation errors
        except Exception as e:
            raise Exception(f"Failed to remove cron job: {str(e)}")
    
    def update_syncoid_job(
        self,
        old_name: str,
        new_name: str,
        schedule: str,
        source: str,
        target: str,
        **options
    ) -> None:
        """
        Update an existing Syncoid cron job
        
        Args:
            old_name: Current job name
            new_name: New job name
            schedule: Cron schedule
            source: Source dataset
            target: Target dataset
            **options: Additional syncoid options
        """
        # Remove old job
        self.remove_syncoid_job(old_name)
        
        # Add updated job
        self.add_syncoid_job(
            name=new_name,
            schedule=schedule,
            source=source,
            target=target,
            **options
        )
    
    def validate_cron_schedule(self, schedule: str) -> Dict[str, Any]:
        """
        Validate a cron schedule expression using strict regex-based validation.
        
        This method sanitizes input and validates against strict patterns to prevent
        injection attacks and ensure cron schedule correctness.
        
        Args:
            schedule: Cron schedule string (5 fields: minute hour day month weekday)
            
        Returns:
            Dictionary with validation results:
            - valid: bool - Whether the schedule is valid
            - error: str - Error message if invalid
            - description: str - Human-readable description if valid
            - schedule: str - Sanitized schedule string if valid
        """
        return self._validate_cron_schedule_strict(schedule)
    
    def get_cron_presets(self) -> Dict[str, str]:
        """
        Get common cron schedule presets
        
        Returns:
            Dictionary of preset names to cron schedules
        """
        return {
            'Every hour': '0 * * * *',
            'Every 6 hours': '0 */6 * * *',
            'Daily at midnight': '0 0 * * *',
            'Daily at 2 AM': '0 2 * * *',
            'Daily at 3 AM': '0 3 * * *',
            'Weekly on Sunday at 2 AM': '0 2 * * 0',
            'Monthly on 1st at 2 AM': '0 2 1 * *',
            'Every 15 minutes': '*/15 * * * *',
            'Every 30 minutes': '*/30 * * * *',
        }
    
    def _parse_cron_line(self, line: str) -> Optional[Dict[str, Any]]:
        """
        Parse a cron line into components
        
        Args:
            line: Cron line string
            
        Returns:
            Dictionary with parsed components or None
        """
        try:
            parts = line.split()
            
            if len(parts) < 7:  # Need at least: min hour day month weekday user command
                return None
            
            schedule = ' '.join(parts[0:5])
            user = parts[5]
            command = ' '.join(parts[6:])
            
            # Extract source and target from syncoid command
            if 'syncoid' not in command:
                return None
            
            # Simple parsing - look for the last two arguments as source and target
            cmd_parts = command.split()
            if len(cmd_parts) >= 3:
                source = cmd_parts[-2]
                target = cmd_parts[-1]
            else:
                source = target = 'Unknown'
            
            return {
                'schedule': schedule,
                'user': user,
                'command': command,
                'source': source,
                'target': target
            }
            
        except Exception:
            return None
    
    def _validate_cron_field(self, field: str, min_val: int, max_val: int) -> bool:
        """
        Validate a single cron field
        
        Args:
            field: Cron field value
            min_val: Minimum allowed value
            max_val: Maximum allowed value
            
        Returns:
            True if valid
        """
        # Handle ranges like "1-5"
        if '-' in field:
            start, end = field.split('-')
            start = int(start)
            end = int(end)
            return min_val <= start <= max_val and min_val <= end <= max_val
        
        # Handle steps like "*/5"
        if field.startswith('*/'):
            step = int(field[2:])
            return step > 0 and step <= max_val
        
        # Handle lists like "1,3,5"
        if ',' in field:
            values = field.split(',')
            return all(min_val <= int(v) <= max_val for v in values)
        
        # Handle single values
        value = int(field)
        return min_val <= value <= max_val
    
    def _describe_schedule(self, schedule: str) -> str:
        """
        Generate human-readable description of cron schedule
        
        Args:
            schedule: Cron schedule string
            
        Returns:
            Human-readable description
        """
        minute, hour, day, month, weekday = schedule.split()
        
        # Simple descriptions for common patterns
        if schedule == '0 * * * *':
            return 'Every hour'
        elif schedule == '0 0 * * *':
            return 'Daily at midnight'
        elif schedule == '0 2 * * *':
            return 'Daily at 2:00 AM'
        elif schedule == '0 2 * * 0':
            return 'Weekly on Sunday at 2:00 AM'
        elif schedule == '0 2 1 * *':
            return 'Monthly on the 1st at 2:00 AM'
        elif schedule.startswith('*/'):
            interval = schedule.split()[0][2:]
            return f'Every {interval} minutes'
        else:
            return f'At {hour}:{minute} on day {day} of month {month}, weekday {weekday}'
