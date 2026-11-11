from datetime import datetime, timedelta
from typing import Any

from jose import JWTError, jwt

from config.settings import settings
from services.timeout_settings import get_effective_session_timeout


class InvalidToken(BaseException):
    pass


# Every JWT WebZFS signs uses the same SECRET_KEY. The type claim marks a
# login session so that other signed tokens (for example the ZFS delegation
# confirmation token, which also carries "username" and "exp" and is placed
# in page HTML) can never be used as a session cookie.
SESSION_TOKEN_TYPE = "session"


def create_token(username: str) -> str:
    session_timeout = get_effective_session_timeout()
    exp = datetime.utcnow() + timedelta(seconds=session_timeout)
    token = jwt.encode(
        {"typ": SESSION_TOKEN_TYPE, "username": username, "exp": exp},
        key=settings.SECRET_KEY,
        algorithm=settings.TOKEN_ALGORITHM,
    )
    return token


def get_username_from_token(token: str) -> str:
    return get_token_claims(token)["username"]


def get_token_claims(token: str) -> dict[str, Any]:
    """Decode and validate a WebZFS session token, returning its complete claims.

    Only tokens issued by create_token() are accepted: the "typ" claim must be
    "session". Any other signed token is rejected as InvalidToken.
    """
    try:
        claims = jwt.decode(
            token,
            key=settings.SECRET_KEY,
            algorithms=[settings.TOKEN_ALGORITHM],
        )
        if claims.get("typ") != SESSION_TOKEN_TYPE:
            raise KeyError
        if not claims.get("username") or claims.get("exp") is None:
            raise KeyError
        return claims
    except (JWTError, KeyError):
        raise InvalidToken
