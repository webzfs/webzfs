"""Lightweight content negotiation for API clients.

WebZFS's own web UI is HTMX-driven -- every existing route renders a
Jinja template (or a redirect) regardless of what actually changed.
That's the right choice for the browser UI, but it leaves no genuine,
stable JSON contract for non-browser API clients (scripts, IaC tooling,
etc.) even on routes whose OpenAPI schema documents a JSON-looking
shape.

This module adds an explicit, opt-in JSON response path: a client that
sends `Accept: application/json` (in preference to `text/html`) gets a
real JSON body back instead of a rendered template or a redirect. No
existing browser/HTMX request ever sends that header, so this is purely
additive -- it changes nothing for the current UI.
"""
from fastapi import Request


def wants_json(request: Request) -> bool:
    """True if the client's Accept header explicitly prefers JSON over HTML.

    A browser's default Accept header starts with "text/html", so a bare
    substring check for "application/json" isn't enough (some browsers
    include it further down the list as a fallback). This only counts as
    a real JSON request when application/json appears, and either
    text/html is absent or application/json is listed ahead of it.
    """
    accept = request.headers.get("accept", "")
    if "application/json" not in accept:
        return False
    json_pos = accept.find("application/json")
    html_pos = accept.find("text/html")
    return html_pos == -1 or json_pos < html_pos
