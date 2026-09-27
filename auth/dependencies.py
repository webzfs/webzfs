from fastapi import Cookie
from starlette.requests import HTTPConnection

from auth.exceptions import AuthenticationFailed
from auth.token import InvalidToken, get_username_from_token


def get_current_user(connection: HTTPConnection, token: str = Cookie(None)) -> str:
    if not token:
        raise AuthenticationFailed
    try:
        return get_username_from_token(token)
    except InvalidToken:
        raise AuthenticationFailed
