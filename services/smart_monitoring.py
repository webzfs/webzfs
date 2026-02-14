"""
SMART Disk Monitoring Service
Handles SMART data retrieval, test scheduling, and smartd integration
"""
import subprocess
import re
import json
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path

from services.utils import is_freebsd, run_privileged_command


class SMARTMonitoringService:
    """Service for SMART disk monitoring and management"""
    
    def __init__(self, data_dir: Optional[str] = None):
        """Initialize SMART monitoring service"""
        if data_dir:
            self.data_dir = Path(data_dir)
        else:
            home = Path.home()
            self.data_dir = home / '.config' / 'webzfs'
        
        self.scheduled_tests_file = self.data_dir / 'smart_scheduled_tests.json'
        self.test_history_file = self.data_dir / 'smart_test_history.json'
        
        self._ensure_data_directory()
        self._initialize_files()
    
    def _ensure_data_directory(self) -> None:
        """Ensure the data directory exists"""
        self.data_dir.mkdir(parents=True, exist_ok=True)
    
    def _initialize_files(self) -> None:
        """Initialize data files if they don't exist"""
        if not self.scheduled_tests_file.exists():
            self._write_json(self.scheduled_tests_file, {})
        
        if not self.test_history_file.exists():
            self._write_json(self.test_history_file, {'history': []})
    
    def _read_json(self, file_path: Path) -> Dict[str, Any]:
        """Read JSON file with error handling"""
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _write_json(self, file_path: Path, data: Dict[str, Any]) -> None:
        """Write JSON file atomically"""
        import tempfile
        temp_file = file_path.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2)
        temp_file.replace(file_path)
    
    def list_disks(self) -> List[Dict[str, Any]]:
        """
        List all available disks with SMART capability
        
        Returns:
            List of disk information dictionaries
        """
        try:
            # Try to get list from smartctl --scan
            result = run_privileged_command(['smartctl', '--scan'])
            
            disks = []
            for line in result.stdout.strip().split('\n'):
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 1:
                    disk_path = parts[0]
                    disk_name = disk_path.split('/')[-1]
                    
                    # Get basic info for each disk
                    info = self._get_basic_disk_info(disk_path)
                    disks.append({
                        'path': disk_path,
                        'name': disk_name,
                        'model': info.get('model', 'Unknown'),
                        'serial': info.get('serial', 'Unknown'),
                        'smart_enabled': info.get('smart_enabled', False),
                        'smart_available': info.get('smart_available', False)
                    })
            
            return disks
            
        except FileNotFoundError:
            raise Exception("smartctl not found. Install smartmontools package.")
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to list disks: {e.stderr}")
    
    def get_smart_data(self, disk: str) -> Dict[str, Any]:
        """
        Get complete SMART data for a disk
        
        Args:
            disk: Disk path (e.g., /dev/sda)
            
        Returns:
            Dictionary with complete SMART data
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-a', disk],
                check=False  # Some info available even on error
            )
            
            return {
                'disk': disk,
                'timestamp': datetime.now().isoformat(),
                'raw_output': result.stdout,
                'return_code': result.returncode,
                'health': self._extract_health(result.stdout),
                'attributes': self._parse_smart_attributes(result.stdout),
                'info': self._parse_device_info(result.stdout),
                'test_log': self._parse_test_log(result.stdout),
                'error_log': self._parse_error_log(result.stdout)
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get SMART data: {e.stderr}")
    
    def get_smart_health(self, disk: str) -> Dict[str, str]:
        """
        Get SMART health status (quick check)
        
        Args:
            disk: Disk path
            
        Returns:
            Health status dictionary
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-H', disk],
                check=False
            )
            
            health = 'UNKNOWN'
            for line in result.stdout.split('\n'):
                if 'SMART overall-health' in line or 'SMART Health Status' in line:
                    if 'PASSED' in line:
                        health = 'PASSED'
                    elif 'FAILED' in line:
                        health = 'FAILED'
            
            return {
                'disk': disk,
                'health': health,
                'timestamp': datetime.now().isoformat()
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get health status: {e.stderr}")
    
    def get_smart_attributes(self, disk: str) -> List[Dict[str, Any]]:
        """
        Get SMART attributes for a disk
        
        Args:
            disk: Disk path
            
        Returns:
            List of SMART attributes
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-A', disk],
                check=False
            )
            
            return self._parse_smart_attributes(result.stdout)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get SMART attributes: {e.stderr}")
    
    def get_disk_info(self, disk: str) -> Dict[str, Any]:
        """
        Get basic disk information
        
        Args:
            disk: Disk path
            
        Returns:
            Disk information dictionary
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-i', disk],
                check=False
            )
            
            return self._parse_device_info(result.stdout)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get disk info: {e.stderr}")
    
    def start_short_test(self, disk: str) -> Dict[str, str]:
        """
        Start SMART short self-test
        
        Args:
            disk: Disk path
            
        Returns:
            Test start confirmation
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-t', 'short', disk]
            )
            
            return {
                'status': 'started',
                'disk': disk,
                'test_type': 'short',
                'message': result.stdout.strip(),
                'timestamp': datetime.now().isoformat()
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to start short test: {e.stderr}")
    
    def start_long_test(self, disk: str) -> Dict[str, str]:
        """
        Start SMART long self-test
        
        Args:
            disk: Disk path
            
        Returns:
            Test start confirmation
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-t', 'long', disk]
            )
            
            return {
                'status': 'started',
                'disk': disk,
                'test_type': 'long',
                'message': result.stdout.strip(),
                'timestamp': datetime.now().isoformat()
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to start long test: {e.stderr}")
    
    def get_test_status(self, disk: str) -> Dict[str, Any]:
        """
        Get current test status and history
        
        Args:
            disk: Disk path
            
        Returns:
            Test status and history
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-a', disk],
                check=False
            )
            
            # Check for running test
            running_test = None
            for line in result.stdout.split('\n'):
                if 'Self-test execution status' in line:
                    if 'in progress' in line.lower():
                        # Extract progress percentage
                        match = re.search(r'(\d+)%', line)
                        running_test = {
                            'status': 'in_progress',
                            'progress': match.group(1) if match else '0',
                            'info': line.strip()
                        }
            
            # Get test history
            test_log = self._parse_test_log(result.stdout)
            
            return {
                'disk': disk,
                'running_test': running_test,
                'test_history': test_log
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get test status: {e.stderr}")
    
    def abort_test(self, disk: str) -> None:
        """
        Abort running SMART test
        
        Args:
            disk: Disk path
        """
        try:
            run_privileged_command(['smartctl', '-X', disk])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to abort test: {e.stderr}")
    
    def get_error_log(self, disk: str) -> List[Dict[str, Any]]:
        """
        Get SMART error log
        
        Args:
            disk: Disk path
            
        Returns:
            List of error log entries
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-l', 'error', disk],
                check=False
            )
            
            return self._parse_error_log(result.stdout)
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get error log: {e.stderr}")
    
    def get_temperature(self, disk: str) -> Dict[str, Any]:
        """
        Get current disk temperature
        
        Args:
            disk: Disk path
            
        Returns:
            Temperature information
        """
        try:
            result = run_privileged_command(
                ['smartctl', '-A', disk],
                check=False
            )
            
            temp = None
            for line in result.stdout.split('\n'):
                if 'Temperature_Celsius' in line or 'Airflow_Temperature' in line:
                    parts = line.split()
                    if len(parts) >= 10:
                        temp = parts[9]
                        break
            
            return {
                'disk': disk,
                'temperature': temp,
                'unit': 'Celsius',
                'timestamp': datetime.now().isoformat()
            }
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to get temperature: {e.stderr}")
    
    def enable_smart(self, disk: str) -> None:
        """Enable SMART on a disk"""
        try:
            run_privileged_command(['smartctl', '-s', 'on', disk])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to enable SMART: {e.stderr}")
    
    def disable_smart(self, disk: str) -> None:
        """Disable SMART on a disk"""
        try:
            run_privileged_command(['smartctl', '-s', 'off', disk])
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to disable SMART: {e.stderr}")
    
    # Smartd Integration
    
    def get_smartd_config(self) -> str:
        """Get current smartd.conf configuration"""
        try:
            config_path = Path('/etc/smartd.conf')
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return f.read()
            return "# smartd.conf not found"
        except Exception as e:
            raise Exception(f"Failed to read smartd.conf: {str(e)}")
    
    def update_smartd_config(self, config: str) -> None:
        """Update smartd.conf configuration"""
        try:
            config_path = Path('/etc/smartd.conf')
            # Would need root privileges
            with open(config_path, 'w') as f:
                f.write(config)
        except Exception as e:
            raise Exception(f"Failed to update smartd.conf: {str(e)}")
    
    def get_smartd_status(self) -> Dict[str, Any]:
        """Get smartd daemon status"""
        if is_freebsd():
            # FreeBSD only uses service command
            try:
                result = subprocess.run(
                    ['service', 'smartd', 'status'],
                    capture_output=True,
                    text=True,
                    check=False
                )
                return {
                    'running': result.returncode == 0,
                    'status_output': result.stdout
                }
            except Exception as e:
                return {'error': f'Unable to check smartd status: {str(e)}'}
        
        # Linux tries systemctl first (default)
        try:
            result = subprocess.run(
                ['systemctl', 'status', 'smartd'],
                capture_output=True,
                text=True,
                check=False
            )
            
            active = 'active (running)' in result.stdout
            
            return {
                'running': active,
                'status_output': result.stdout
            }
            
        except FileNotFoundError:
            # Fallback to service command on Linux
            try:
                result = subprocess.run(
                    ['service', 'smartd', 'status'],
                    capture_output=True,
                    text=True,
                    check=False
                )
                return {
                    'running': result.returncode == 0,
                    'status_output': result.stdout
                }
            except:
                return {'error': 'Unable to check smartd status'}
    
    def restart_smartd(self) -> None:
        """Restart smartd daemon"""
        if is_freebsd():
            # FreeBSD only uses service command
            try:
                subprocess.run(
                    ['service', 'smartd', 'restart'],
                    capture_output=True,
                    text=True,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to restart smartd: {e.stderr}")
        else:
            # Linux tries systemctl first (default)
            try:
                subprocess.run(
                    ['systemctl', 'restart', 'smartd'],
                    capture_output=True,
                    text=True,
                    check=True
                )
            except FileNotFoundError:
                # Fallback to service command on Linux
                subprocess.run(
                    ['service', 'smartd', 'restart'],
                    capture_output=True,
                    text=True,
                    check=True
                )
            except subprocess.CalledProcessError as e:
                raise Exception(f"Failed to restart smartd: {e.stderr}")
    
    # Scheduled Tests
    
    def list_scheduled_tests(self) -> List[Dict[str, Any]]:
        """List all scheduled SMART tests"""
        data = self._read_json(self.scheduled_tests_file)
        return list(data.values())
    
    def create_scheduled_test(
        self,
        disk: str,
        test_type: str,
        schedule: str,
        enabled: bool = True
    ) -> str:
        """Create a scheduled SMART test"""
        import uuid
        schedule_id = str(uuid.uuid4())
        
        data = self._read_json(self.scheduled_tests_file)
        data[schedule_id] = {
            'id': schedule_id,
            'disk': disk,
            'test_type': test_type,
            'schedule': schedule,
            'enabled': enabled,
            'created_at': datetime.now().isoformat()
        }
        self._write_json(self.scheduled_tests_file, data)
        
        return schedule_id
    
    def update_scheduled_test(self, schedule_id: str, **updates) -> None:
        """Update a scheduled test"""
        data = self._read_json(self.scheduled_tests_file)
        
        if schedule_id not in data:
            raise KeyError(f"Schedule {schedule_id} not found")
        
        data[schedule_id].update(updates)
        data[schedule_id]['updated_at'] = datetime.now().isoformat()
        self._write_json(self.scheduled_tests_file, data)
    
    def delete_scheduled_test(self, schedule_id: str) -> None:
        """Delete a scheduled test"""
        data = self._read_json(self.scheduled_tests_file)
        
        if schedule_id not in data:
            raise KeyError(f"Schedule {schedule_id} not found")
        
        del data[schedule_id]
        self._write_json(self.scheduled_tests_file, data)
    
    def get_test_history(
        self,
        disk: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get SMART test execution history"""
        data = self._read_json(self.test_history_file)
        history = data.get('history', [])
        
        if disk:
            history = [h for h in history if h.get('disk') == disk]
        
        return history[-limit:]
    
    def add_test_to_history(self, disk: str, test_type: str, status: str, **details) -> None:
        """Add a test result to history"""
        data = self._read_json(self.test_history_file)
        
        if 'history' not in data:
            data['history'] = []
        
        data['history'].append({
            'disk': disk,
            'test_type': test_type,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            **details
        })
        
        # Keep only last 1000 entries
        data['history'] = data['history'][-1000:]
        
        self._write_json(self.test_history_file, data)
    
    # Private helper methods
    
    def _get_basic_disk_info(self, disk: str) -> Dict[str, Any]:
        """Get basic disk information"""
        try:
            result = run_privileged_command(
                ['smartctl', '-i', disk],
                check=False
            )
            return self._parse_device_info(result.stdout)
        except:
            return {}
    
    def _extract_health(self, output: str) -> str:
        """Extract health status from smartctl output"""
        for line in output.split('\n'):
            if 'SMART overall-health' in line or 'SMART Health Status' in line:
                if 'PASSED' in line:
                    return 'PASSED'
                elif 'FAILED' in line:
                    return 'FAILED'
        return 'UNKNOWN'
    
    def _parse_smart_attributes(self, output: str) -> List[Dict[str, Any]]:
        """Parse SMART attributes table"""
        attributes = []
        in_attributes = False
        
        for line in output.split('\n'):
            if 'ID# ATTRIBUTE_NAME' in line:
                in_attributes = True
                continue
            
            if in_attributes and line.strip():
                # Parse attribute line
                parts = line.split()
                if len(parts) >= 10 and parts[0].isdigit():
                    attributes.append({
                        'id': parts[0],
                        'name': parts[1],
                        'flag': parts[2],
                        'value': parts[3],
                        'worst': parts[4],
                        'thresh': parts[5],
                        'type': parts[6],
                        'updated': parts[7],
                        'when_failed': parts[8],
                        'raw_value': ' '.join(parts[9:])
                    })
        
        return attributes
    
    def _parse_device_info(self, output: str) -> Dict[str, Any]:
        """Parse device information from smartctl output"""
        info = {}
        
        for line in output.split('\n'):
            if ':' in line:
                key, value = line.split(':', 1)
                key = key.strip().lower().replace(' ', '_')
                value = value.strip()
                
                if 'model' in key:
                    info['model'] = value
                elif 'serial' in key:
                    info['serial'] = value
                elif 'capacity' in key or 'size' in key:
                    info['capacity'] = value
                elif 'firmware' in key:
                    info['firmware'] = value
                elif 'smart support' in key:
                    info['smart_available'] = 'Available' in value
                    info['smart_enabled'] = 'Enabled' in value
        
        return info
    
    def _parse_test_log(self, output: str) -> List[Dict[str, Any]]:
        """Parse SMART self-test log"""
        tests = []
        in_log = False
        
        for line in output.split('\n'):
            # Skip execution status line
            if 'Self-test execution status' in line:
                continue
            
            # Detect start of log table header
            if 'Num' in line and 'Test_Description' in line and 'Status' in line:
                in_log = True
                continue
            
            # Parse test entries if we're in the log section
            if in_log and line.strip():
                # Check if line starts with "# <number>"
                if line.strip().startswith('#'):
                    # Use regex to parse the line more accurately
                    # Format: # 1  Short offline       Completed without error       00%       792         -
                    match = re.match(r'#\s*(\d+)\s+(\S+(?:\s+\S+)?)\s+(.*?)\s+(\d+%)\s+(\d+)\s+(.*)$', line.strip())
                    if match:
                        tests.append({
                            'num': match.group(1),
                            'description': match.group(2).strip(),
                            'status': match.group(3).strip(),
                            'remaining': match.group(4).strip(),
                            'lifetime': match.group(5).strip(),
                            'lba_of_error': match.group(6).strip() if match.group(6).strip() else '-'
                        })
                elif not any(word in line for word in ['Num', 'Description', '===', '---']):
                    # Stop parsing if we hit something that's not a test entry
                    break
        
        return tests
    
    def _parse_error_log(self, output: str) -> List[Dict[str, Any]]:
        """Parse SMART error log"""
        errors = []
        
        if 'No Errors Logged' in output:
            return errors
        
        # Simple parsing - would be more sophisticated in production
        for line in output.split('\n'):
            if 'Error' in line and line.strip():
                errors.append({'message': line.strip()})
        
        return errors
