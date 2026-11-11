"""
Security response headers added to every HTTP response.

The Content-Security-Policy is a safety net, not the XSS fix itself (that is
output escaping in templates and views). It still allows inline scripts and
eval because the templates use inline handlers and Alpine.js evaluates
expressions, but it:

- loads scripts, styles, fonts and images only from WebZFS itself (all
  third-party browser libraries are vendored under static/vendor/),
- blocks fetch, XHR, EventSource and WebSocket connections to other hosts,
  so injected script cannot send data elsewhere with them,
- blocks plugins, <base> tag rewriting and form posts to other sites,
- forbids other sites from framing WebZFS (clickjacking).

Cockpit mode is unaffected: the Cockpit adapter fetches WebZFS pages through
the Cockpit HTTP bridge and renders them under Cockpit's own policy, so these
headers are never applied by the browser there.

WebSocket connections are not HTTP responses and pass through unchanged.
"""

CONTENT_SECURITY_POLICY = "; ".join(
    [
        "default-src 'self'",
        "script-src 'self' 'unsafe-inline' 'unsafe-eval'",
        "style-src 'self' 'unsafe-inline'",
        "img-src 'self' data:",
        "font-src 'self' data:",
        "connect-src 'self'",
        "object-src 'none'",
        "base-uri 'none'",
        "form-action 'self'",
        "frame-ancestors 'none'",
    ]
)

SECURITY_HEADERS = [
    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode("latin-1")),
    (b"x-frame-options", b"DENY"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"same-origin"),
]


class SecurityHeadersMiddleware:
    """ASGI middleware that adds SECURITY_HEADERS to HTTP responses.

    A header already set by a route is left as the route set it.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                headers = list(message.get("headers", []))
                existing = {name.lower() for name, _ in headers}
                for name, value in SECURITY_HEADERS:
                    if name not in existing:
                        headers.append((name, value))
                message["headers"] = headers
            await send(message)

        await self.app(scope, receive, send_with_headers)
