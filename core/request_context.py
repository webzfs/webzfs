"""Request context helpers shared by WebZFS views."""

from starlette.requests import HTTPConnection

COCKPIT_CONTEXT_HEADER = "X-WebZFS-Context"
COCKPIT_CONTEXT_COOKIE = "webzfs_context"
COCKPIT_CONTEXT_VALUE = "cockpit"


def is_cockpit_request(connection: HTTPConnection) -> bool:
    """Return whether this request was relayed by the WebZFS Cockpit adapter."""
    header_value = connection.headers.get(COCKPIT_CONTEXT_HEADER, "")
    cookie_value = connection.cookies.get(COCKPIT_CONTEXT_COOKIE, "")
    return any(
        value.strip().lower() == COCKPIT_CONTEXT_VALUE
        for value in (header_value, cookie_value)
    )
