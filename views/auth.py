from typing import Annotated, Optional

from fastapi import APIRouter, Cookie, Form, Request, Response
from fastapi.responses import RedirectResponse

from auth import rate_limiter
from auth.login import authenticate_user
from auth.login_policy import is_login_allowed
from auth.token import create_token, get_username_from_token, InvalidToken
from config.templates import templates
from services.audit_logger import audit_logger

router = APIRouter()

TOKEN_COOKIE = "token"

# Login input longer than this never reaches PAM and counts as a failure.
MAX_USERNAME_LENGTH = 256
MAX_PASSWORD_LENGTH = 4096


def has_control_characters(text: str) -> bool:
    """True if text contains a NUL or any other control character.

    python-pam hands the username and password to libpam as C strings, so a
    NUL byte ends the value at the C boundary: PAM would authenticate
    "root<NUL>anything" as "root" while WebZFS kept the longer string as the
    identity. Other control characters are never part of a real account name
    either. Directory accounts may legitimately use uppercase letters, dots,
    "@", "\\" or spaces, so nothing beyond control characters is refused.
    """
    return any(ord(char) < 32 or ord(char) == 127 for char in text)


def token_cookie_options(request: Request) -> dict:
    """Attributes for the session cookie, shared by login and logout.

    - httponly: page JavaScript cannot read the token, so an XSS bug cannot
      steal the session. No WebZFS script reads this cookie.
    - samesite strict: the browser does not send the cookie on requests that
      start on another site, including top-level links. A crafted link to a
      WebZFS page therefore arrives unauthenticated.
    - secure: set when the request arrived over HTTPS (directly, or through a
      reverse proxy trusted by FORWARDED_ALLOW_IPS), so the token is never sent
      over plain HTTP. It stays off for plain HTTP access such as an SSH tunnel,
      where the browser would otherwise refuse to store it.

    The Cockpit adapter reads the token from the Set-Cookie header on the
    server side; these attributes do not affect it.
    """
    return {
        "httponly": True,
        "samesite": "strict",
        "secure": request.url.scheme == "https",
        "path": "/",
    }


def get_client_ip(request: Request) -> str:
    """Client IP address for rate limiting and audit logs.

    X-Forwarded-For and X-Real-IP are deliberately not read here: any client
    can send them. Uvicorn already replaces request.client with the
    forwarded address when, and only when, the connecting peer is a proxy
    listed in FORWARDED_ALLOW_IPS (see config/gunicorn.conf.py).
    """
    if request.client:
        return request.client.host
    return "unknown"


def rate_limited_page(request: Request, retry_after: int, status_code: int = 429):
    return templates.TemplateResponse(
        request,
        name="login.jinja",
        context={
            "error": f"Too many login attempts. Please try again in {retry_after} seconds.",
            "rate_limited": True,
            "retry_after": retry_after,
        },
        status_code=status_code,
    )


@router.get("/")
def login_page(request: Request):
    retry_after = rate_limiter.retry_after(get_client_ip(request))
    if retry_after:
        return rate_limited_page(request, retry_after, status_code=200)
    return templates.TemplateResponse(request, name="login.jinja")


@router.post("/")
def login(
    request: Request,
    username: Annotated[str, Form()],
    password: Annotated[str, Form()],
    response: Response,
):
    client_ip = get_client_ip(request)

    # Check the limits and record this attempt in one locked step, before
    # PAM runs, so parallel requests cannot all slip past the check.
    retry_after = rate_limiter.register_attempt(client_ip, username)
    if retry_after:
        return rate_limited_page(request, retry_after)

    input_too_long = len(username) > MAX_USERNAME_LENGTH or len(password) > MAX_PASSWORD_LENGTH
    input_has_control = has_control_characters(username) or has_control_characters(password)
    input_ok = not input_too_long and not input_has_control
    authenticated = input_ok and authenticate_user(username, password)
    # With RESTRICT_LOGIN=yes, a correct password is not enough: the account
    # must also match the login policy. A refused account gets exactly the
    # same response as a wrong password, so the page does not reveal that
    # the password was right; the audit log records the real reason.
    allowed = authenticated and is_login_allowed(username)
    if allowed:
        # Successful login: forget this IP's and this user's attempts
        rate_limiter.clear(client_ip, username)
        token = create_token(username)
        response = RedirectResponse("/", status_code=302)
        response.set_cookie(TOKEN_COOKIE, value=token, **token_cookie_options(request))

        # Log successful authentication
        audit_logger.log_auth_success(username=username, ip_address=client_ip)

        return response

    # The failed attempt is already recorded by register_attempt.
    if input_too_long:
        reason = "input_too_long"
    elif input_has_control:
        reason = "input_has_control_characters"
    elif authenticated:
        reason = "not_allowed_by_login_policy"
    else:
        reason = "invalid_credentials"
    audit_logger.log_auth_failure(
        ip_address=client_ip,
        username=username[:MAX_USERNAME_LENGTH],
        reason=reason,
    )

    remaining = rate_limiter.remaining_attempts(client_ip, username)
    if remaining == 0:
        retry_after = rate_limiter.retry_after(client_ip, username)
        audit_logger.log_auth_rate_limited(ip_address=client_ip, retry_after=retry_after)
        return rate_limited_page(request, retry_after)

    error_msg = f"Invalid credentials. {remaining} attempt{'s' if remaining != 1 else ''} remaining."
    return templates.TemplateResponse(
        request, name="login.jinja", context={"error": error_msg, "remaining_attempts": remaining}
    )


@router.post("/logout")
def logout(
    request: Request,
    token: Optional[str] = Cookie(None),
):
    client_ip = get_client_ip(request)
    
    # Try to get username from token for logging
    username = None
    if token:
        try:
            username = get_username_from_token(token)
        except InvalidToken:
            pass
    
    response = RedirectResponse("/", status_code=302)
    response.delete_cookie(TOKEN_COOKIE, **token_cookie_options(request))
    
    # Log the logout
    if username:
        audit_logger.log_logout(username=username, ip_address=client_ip)
    
    return response
