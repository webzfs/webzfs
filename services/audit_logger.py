"""
Audit Logger Service
Provides centralized logging for authentication, ZFS operations, and file access.
Logs are stored in ~/.config/webzfs/logs/
"""
import logging
import os
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Dict, Any
from enum import Enum


class LogCategory(Enum):
    """Log categories for different types of operations"""
    AUTH = "auth"
    ZFS = "zfs_operations"
    FILE = "file_access"


class AuditLogger:
    """
    Centralized audit logger for WebZFS.
    Handles logging of authentication attempts, ZFS operations, and file access.
    """
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        """Singleton pattern to ensure single logger instance"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the audit logger with file handlers"""
        if AuditLogger._initialized:
            return
        
        # Set up log directory
        self.log_dir = Path.home() / ".config" / "webzfs" / "logs"
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure max log file size (10MB) and backup count (5 files)
        self.max_bytes = 10 * 1024 * 1024  # 10MB
        self.backup_count = 5
        
        # Create loggers for each category
        self.loggers: Dict[LogCategory, logging.Logger] = {}
        
        for category in LogCategory:
            self.loggers[category] = self._create_logger(category)
        
        AuditLogger._initialized = True
    
    def _create_logger(self, category: LogCategory) -> logging.Logger:
        """
        Create a logger for a specific category with file handler.
        
        Args:
            category: The log category
            
        Returns:
            Configured logger instance
        """
        logger_name = f"webzfs.audit.{category.value}"
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        
        # Prevent duplicate handlers
        if logger.handlers:
            return logger
        
        # Create file handler with rotation
        log_file = self.log_dir / f"{category.value}.log"
        handler = RotatingFileHandler(
            log_file,
            maxBytes=self.max_bytes,
            backupCount=self.backup_count,
            encoding='utf-8'
        )
        
        # Create formatter with timestamp
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        
        # Prevent propagation to root logger
        logger.propagate = False
        
        return logger
    
    def _format_details(self, details: Dict[str, Any]) -> str:
        """
        Format details dictionary into a log string.
        
        Args:
            details: Dictionary of key-value pairs
            
        Returns:
            Formatted string like "key1=value1 key2=value2"
        """
        parts = []
        for key, value in details.items():
            if value is not None:
                # Escape spaces in values
                str_value = str(value)
                if ' ' in str_value:
                    str_value = f'"{str_value}"'
                parts.append(f"{key}={str_value}")
        return " ".join(parts)
    
    # ==================== Authentication Logging ====================
    
    def log_auth_success(self, username: str, ip_address: str, 
                         action: str = "login") -> None:
        """
        Log a successful authentication attempt.
        
        Args:
            username: The authenticated username
            ip_address: Client IP address
            action: The authentication action (login, logout, etc.)
        """
        details = {
            "status": "SUCCESS",
            "user": username,
            "ip": ip_address,
            "action": action
        }
        self.loggers[LogCategory.AUTH].info(self._format_details(details))
    
    def log_auth_failure(self, ip_address: str, username: Optional[str] = None,
                         action: str = "login", reason: str = "invalid_credentials") -> None:
        """
        Log a failed authentication attempt.
        
        Args:
            ip_address: Client IP address
            username: Attempted username (if available)
            action: The authentication action
            reason: Reason for failure
        """
        details = {
            "status": "FAILED",
            "ip": ip_address,
            "action": action,
            "reason": reason
        }
        if username:
            details["attempted_user"] = username
        
        self.loggers[LogCategory.AUTH].warning(self._format_details(details))
    
    def log_auth_rate_limited(self, ip_address: str, retry_after: int) -> None:
        """
        Log a rate-limited authentication attempt.
        
        Args:
            ip_address: Client IP address
            retry_after: Seconds until retry is allowed
        """
        details = {
            "status": "RATE_LIMITED",
            "ip": ip_address,
            "action": "login",
            "retry_after_seconds": retry_after
        }
        self.loggers[LogCategory.AUTH].warning(self._format_details(details))
    
    def log_logout(self, username: str, ip_address: str) -> None:
        """
        Log a user logout.
        
        Args:
            username: The logged out username
            ip_address: Client IP address
        """
        details = {
            "status": "SUCCESS",
            "user": username,
            "ip": ip_address,
            "action": "logout"
        }
        self.loggers[LogCategory.AUTH].info(self._format_details(details))
    
    # ==================== ZFS Operations Logging ====================
    
    def log_zfs_operation(self, user: str, operation: str, 
                          success: bool = True, **kwargs) -> None:
        """
        Log a ZFS operation.
        
        Args:
            user: Username performing the operation
            operation: The operation type (create_pool, destroy_snapshot, etc.)
            success: Whether the operation succeeded
            **kwargs: Additional operation-specific details
        """
        details = {
            "user": user,
            "operation": operation,
            "status": "SUCCESS" if success else "FAILED"
        }
        details.update(kwargs)
        
        if success:
            self.loggers[LogCategory.ZFS].info(self._format_details(details))
        else:
            self.loggers[LogCategory.ZFS].error(self._format_details(details))
    
    # Pool Operations
    def log_pool_create(self, user: str, pool_name: str, vdevs: list,
                        success: bool = True, error: Optional[str] = None) -> None:
        """Log pool creation"""
        self.log_zfs_operation(
            user=user, 
            operation="create_pool", 
            pool=pool_name,
            vdevs=",".join(vdevs) if vdevs else None,
            success=success,
            error=error
        )
    
    def log_pool_destroy(self, user: str, pool_name: str, force: bool = False,
                         success: bool = True, error: Optional[str] = None) -> None:
        """Log pool destruction"""
        self.log_zfs_operation(
            user=user,
            operation="destroy_pool",
            pool=pool_name,
            force=force,
            success=success,
            error=error
        )
    
    def log_pool_import(self, user: str, pool_name: str, force: bool = False,
                        success: bool = True, error: Optional[str] = None) -> None:
        """Log pool import"""
        self.log_zfs_operation(
            user=user,
            operation="import_pool",
            pool=pool_name,
            force=force,
            success=success,
            error=error
        )
    
    def log_pool_export(self, user: str, pool_name: str, force: bool = False,
                        success: bool = True, error: Optional[str] = None) -> None:
        """Log pool export"""
        self.log_zfs_operation(
            user=user,
            operation="export_pool",
            pool=pool_name,
            force=force,
            success=success,
            error=error
        )
    
    def log_pool_scrub(self, user: str, pool_name: str, action: str = "start",
                       success: bool = True, error: Optional[str] = None) -> None:
        """Log pool scrub start/stop"""
        self.log_zfs_operation(
            user=user,
            operation=f"scrub_{action}",
            pool=pool_name,
            success=success,
            error=error
        )
    
    def log_pool_property_change(self, user: str, pool_name: str, 
                                  property_name: str, property_value: str,
                                  success: bool = True, error: Optional[str] = None) -> None:
        """Log pool property change"""
        self.log_zfs_operation(
            user=user,
            operation="set_pool_property",
            pool=pool_name,
            property=property_name,
            value=property_value,
            success=success,
            error=error
        )
    
    def log_pool_checkpoint_create(self, user: str, pool_name: str,
                                   success: bool = True, error: Optional[str] = None) -> None:
        """Log pool checkpoint creation"""
        self.log_zfs_operation(
            user=user,
            operation="create_checkpoint",
            pool=pool_name,
            success=success,
            error=error
        )
    
    def log_pool_checkpoint_discard(self, user: str, pool_name: str,
                                    success: bool = True, error: Optional[str] = None) -> None:
        """Log pool checkpoint discard"""
        self.log_zfs_operation(
            user=user,
            operation="discard_checkpoint",
            pool=pool_name,
            success=success,
            error=error
        )
    
    # Dataset Operations
    def log_dataset_create(self, user: str, dataset_name: str,
                           success: bool = True, error: Optional[str] = None) -> None:
        """Log dataset creation"""
        self.log_zfs_operation(
            user=user,
            operation="create_dataset",
            dataset=dataset_name,
            success=success,
            error=error
        )
    
    def log_dataset_destroy(self, user: str, dataset_name: str, recursive: bool = False,
                            success: bool = True, error: Optional[str] = None) -> None:
        """Log dataset destruction"""
        self.log_zfs_operation(
            user=user,
            operation="destroy_dataset",
            dataset=dataset_name,
            recursive=recursive,
            success=success,
            error=error
        )
    
    def log_dataset_rename(self, user: str, old_name: str, new_name: str,
                           success: bool = True, error: Optional[str] = None) -> None:
        """Log dataset rename"""
        self.log_zfs_operation(
            user=user,
            operation="rename_dataset",
            old_name=old_name,
            new_name=new_name,
            success=success,
            error=error
        )
    
    def log_dataset_property_change(self, user: str, dataset_name: str,
                                     property_name: str, property_value: str,
                                     success: bool = True, error: Optional[str] = None) -> None:
        """Log dataset property change"""
        self.log_zfs_operation(
            user=user,
            operation="set_dataset_property",
            dataset=dataset_name,
            property=property_name,
            value=property_value,
            success=success,
            error=error
        )
    
    # Snapshot Operations
    def log_snapshot_create(self, user: str, snapshot_name: str, recursive: bool = False,
                            success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot creation"""
        self.log_zfs_operation(
            user=user,
            operation="create_snapshot",
            snapshot=snapshot_name,
            recursive=recursive,
            success=success,
            error=error
        )
    
    def log_snapshot_destroy(self, user: str, snapshot_name: str,
                             success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot destruction"""
        self.log_zfs_operation(
            user=user,
            operation="destroy_snapshot",
            snapshot=snapshot_name,
            success=success,
            error=error
        )
    
    def log_snapshot_rollback(self, user: str, snapshot_name: str, force: bool = False,
                              success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot rollback"""
        self.log_zfs_operation(
            user=user,
            operation="rollback_snapshot",
            snapshot=snapshot_name,
            force=force,
            success=success,
            error=error
        )
    
    def log_snapshot_clone(self, user: str, snapshot_name: str, target_dataset: str,
                           success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot clone"""
        self.log_zfs_operation(
            user=user,
            operation="clone_snapshot",
            snapshot=snapshot_name,
            target=target_dataset,
            success=success,
            error=error
        )
    
    def log_snapshot_rename(self, user: str, old_name: str, new_name: str,
                            success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot rename"""
        self.log_zfs_operation(
            user=user,
            operation="rename_snapshot",
            old_name=old_name,
            new_name=new_name,
            success=success,
            error=error
        )
    
    def log_snapshot_hold(self, user: str, snapshot_name: str, tag: str,
                          success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot hold"""
        self.log_zfs_operation(
            user=user,
            operation="hold_snapshot",
            snapshot=snapshot_name,
            tag=tag,
            success=success,
            error=error
        )
    
    def log_snapshot_release(self, user: str, snapshot_name: str, tag: str,
                             success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot release"""
        self.log_zfs_operation(
            user=user,
            operation="release_snapshot",
            snapshot=snapshot_name,
            tag=tag,
            success=success,
            error=error
        )
    
    def log_snapshot_send(self, user: str, snapshot_name: str, 
                          base_snapshot: Optional[str] = None,
                          success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot send"""
        self.log_zfs_operation(
            user=user,
            operation="send_snapshot",
            snapshot=snapshot_name,
            base_snapshot=base_snapshot,
            success=success,
            error=error
        )
    
    def log_snapshot_receive(self, user: str, target_dataset: str,
                             success: bool = True, error: Optional[str] = None) -> None:
        """Log snapshot receive"""
        self.log_zfs_operation(
            user=user,
            operation="receive_snapshot",
            target=target_dataset,
            success=success,
            error=error
        )
    
    # ==================== File Access Logging ====================
    
    def log_file_read(self, user: str, file_path: str,
                      success: bool = True, error: Optional[str] = None) -> None:
        """
        Log a file read operation.
        
        Args:
            user: Username performing the operation
            file_path: Path of the file being read
            success: Whether the operation succeeded
            error: Error message if failed
        """
        details = {
            "user": user,
            "action": "read",
            "path": file_path,
            "status": "SUCCESS" if success else "FAILED"
        }
        if error:
            details["error"] = error
        
        if success:
            self.loggers[LogCategory.FILE].info(self._format_details(details))
        else:
            self.loggers[LogCategory.FILE].error(self._format_details(details))
    
    def log_file_write(self, user: str, file_path: str,
                       success: bool = True, error: Optional[str] = None) -> None:
        """
        Log a file write operation.
        
        Args:
            user: Username performing the operation
            file_path: Path of the file being written
            success: Whether the operation succeeded
            error: Error message if failed
        """
        details = {
            "user": user,
            "action": "write",
            "path": file_path,
            "status": "SUCCESS" if success else "FAILED"
        }
        if error:
            details["error"] = error
        
        if success:
            self.loggers[LogCategory.FILE].info(self._format_details(details))
        else:
            self.loggers[LogCategory.FILE].error(self._format_details(details))
    
    def log_directory_list(self, user: str, directory_path: str,
                           success: bool = True, error: Optional[str] = None) -> None:
        """
        Log a directory listing operation.
        
        Args:
            user: Username performing the operation
            directory_path: Path of the directory being listed
            success: Whether the operation succeeded
            error: Error message if failed
        """
        details = {
            "user": user,
            "action": "list_directory",
            "path": directory_path,
            "status": "SUCCESS" if success else "FAILED"
        }
        if error:
            details["error"] = error
        
        if success:
            self.loggers[LogCategory.FILE].info(self._format_details(details))
        else:
            self.loggers[LogCategory.FILE].error(self._format_details(details))
    
    # ==================== Utility Methods ====================
    
    def get_log_file_path(self, category: LogCategory) -> Path:
        """
        Get the path to a log file.
        
        Args:
            category: The log category
            
        Returns:
            Path to the log file
        """
        return self.log_dir / f"{category.value}.log"
    
    def get_all_log_paths(self) -> Dict[str, Path]:
        """
        Get paths to all log files.
        
        Returns:
            Dictionary mapping category names to paths
        """
        return {
            category.value: self.get_log_file_path(category)
            for category in LogCategory
        }


# Create a global singleton instance
audit_logger = AuditLogger()
