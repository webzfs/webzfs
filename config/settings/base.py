from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE_PATH = Path(__file__).parent.parent.parent / ".env"

# SECRET_KEY signs every session token. A known or guessable key lets anyone
# sign a session for any username without a password, so WebZFS refuses to
# start with one of these placeholder values or with a short key. The
# installers generate a 64 character key with secrets.token_hex(32).
SECRET_KEY_MIN_LENGTH = 32
SECRET_KEY_PLACEHOLDERS = {
    "change_me_generate_new_key",
    "your-secret-key-change-in-production",
    "changeme",
    "change-me",
    "secret",
    "dummy",
    "test",
}


class Settings(BaseSettings):
    CAPTION: str
    SECRET_KEY: str
    DEBUG: bool = False

    TOKEN_ALGORITHM: str = "HS256"
    AUTH_SESSION_EXPIRES_SECONDS: int = 3600

    # Login restriction (see auth/login_policy.py). Off by default: any
    # account PAM accepts may log in. With RESTRICT_LOGIN=yes, only the
    # listed users and members of the listed groups may log in.
    RESTRICT_LOGIN: bool = False
    LOGIN_ALLOWED_USERS: str = "root"
    LOGIN_ALLOWED_GROUPS: str = "wheel,webzfs-admin"

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

    @field_validator("SECRET_KEY")
    @classmethod
    def validate_secret_key(cls, value: str) -> str:
        """Refuse placeholder or short signing keys.

        pydantic-settings validates class defaults as well, so the "dummy"
        and "test" defaults in the dev and test settings modules are refused
        too. Every install and dev setup script writes a generated key to
        .env, which overrides those defaults; only a bare checkout without a
        .env stops here, with the message below.
        """
        cleaned = value.strip()
        if cleaned.lower() in SECRET_KEY_PLACEHOLDERS or len(cleaned) < SECRET_KEY_MIN_LENGTH:
            raise ValueError(
                "SECRET_KEY is a placeholder or shorter than "
                f"{SECRET_KEY_MIN_LENGTH} characters. Anyone who knows this key can sign a "
                "session for any user. Generate one with: "
                "python3 -c \"import secrets; print(secrets.token_hex(32))\" "
                "and set it in .env"
            )
        return value

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
