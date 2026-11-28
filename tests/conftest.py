"""Shared pytest configuration for the WebZFS test suite.

Several service modules instantiate ``Settings()`` at import time, so the
required configuration values must be present before any test module imports
them. Set harmless defaults here; tests never talk to a real ZFS system.
"""

import os

os.environ.setdefault("CAPTION", "WebZFS")
os.environ.setdefault("SECRET_KEY", "test-secret-key-not-for-production")
