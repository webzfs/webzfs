from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE_PATH = Path(__file__).parent.parent.parent / ".env"


class Settings(BaseSettings):
    CAPTION: str
    SECRET_KEY: str
    DEBUG: bool = False

    TOKEN_ALGORITHM: str = "HS256"
    AUTH_SESSION_EXPIRES_SECONDS: int = 3600

    SHELL_ALLOWED_USERS: str = ""
    ZFS_DELEGATION_ALLOWED_USERS: str = ""
    SHELL_IDLE_TIMEOUT_SECONDS: int = 1800
    SHELL_RECORDING_ENABLED: bool = False
    SHELL_RECORDING_AUDIT_USERS: str = ""
    SHELL_RECORDING_RETENTION_DAYS: int = 30
    SHELL_RECORDING_MAX_SESSION_MIB: int = 100
    SHELL_RECORDING_MAX_TOTAL_MIB: int = 1024

    CRONTAB_PATH: Path = Path("/etc/crontab")
    CRON_SCRIPTS_DIR: Path = Path("/etc/cronjobs")

    # ZPool command timeout settings (in seconds)
    # Configurable timeouts for different zpool operations
    ZPOOL_TIMEOUTS: dict[str, int] = {
        "default": 30,  # Default timeout for basic operations
        "list": 30,  # zpool list operations
        "status": 30,  # zpool status operations
        "iostat": 45,  # zpool iostat operations (with sampling)
        "scrub": 30,  # Starting/stopping scrub operations
        "import": 120,  # zpool import operations
        "export": 120,  # zpool export operations
        "create": 180,  # zpool create operations
        "destroy": 120,  # zpool destroy operations
        "history": 60,  # zpool history operations
        "events": 30,  # zpool events operations
        "properties": 30,  # zpool get/set property operations
    }

    @field_validator("SHELL_IDLE_TIMEOUT_SECONDS")
    @classmethod
    def validate_shell_idle_timeout(cls, value: int) -> int:
        if value < 60:
            raise ValueError("SHELL_IDLE_TIMEOUT_SECONDS must be at least 60")
        return value

    @field_validator(
        "SHELL_RECORDING_RETENTION_DAYS",
        "SHELL_RECORDING_MAX_SESSION_MIB",
        "SHELL_RECORDING_MAX_TOTAL_MIB",
    )
    @classmethod
    def validate_positive_shell_setting(cls, value: int) -> int:
        if value < 1:
            raise ValueError("Shell recording limits must be positive")
        return value

    model_config = SettingsConfigDict(
        env_file=ENV_FILE_PATH, env_file_encoding="utf-8", extra="allow"
    )
