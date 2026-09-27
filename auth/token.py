from datetime import datetime, timedelta
from typing import Any

from jose import JWTError, jwt

from config.settings import settings
from services.timeout_settings import get_effective_session_timeout


class InvalidToken(BaseException):
    pass


def create_token(username: str) -> str:
    session_timeout = get_effective_session_timeout()
    exp = datetime.utcnow() + timedelta(seconds=session_timeout)
    token = jwt.encode(
        {"username": username, "exp": exp},
        key=settings.SECRET_KEY,
        algorithm=settings.TOKEN_ALGORITHM,
    )
    return token


def get_username_from_token(token: str) -> str:
    return get_token_claims(token)["username"]


def get_token_claims(token: str) -> dict[str, Any]:
    """Decode and validate a WebZFS token, returning its complete claims."""
    try:
        claims = jwt.decode(
            token,
            key=settings.SECRET_KEY,
            algorithms=[settings.TOKEN_ALGORITHM],
        )
        if not claims.get("username") or claims.get("exp") is None:
            raise KeyError
        return claims
    except (JWTError, KeyError):
        raise InvalidToken
