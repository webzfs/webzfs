from fastapi import Cookie
from starlette.requests import HTTPConnection

from auth.exceptions import AuthenticationFailed
from auth.login_policy import is_login_allowed
from auth.token import InvalidToken, get_username_from_token


def get_current_user(connection: HTTPConnection, token: str = Cookie(None)) -> str:
    if not token:
        raise AuthenticationFailed
    try:
        username = get_username_from_token(token)
    except InvalidToken:
        raise AuthenticationFailed
    # Re-checked on every request so that removing an account from the
    # login policy ends its existing sessions (see auth/login_policy.py).
    if not is_login_allowed(username):
        raise AuthenticationFailed
    return username
