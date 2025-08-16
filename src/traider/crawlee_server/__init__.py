from __future__ import annotations

"""Traider Crawlee server package.

Provides FastAPI application backed by Crawlee PlaywrightCrawler
for on-demand website screenshots.
"""

from .server import app  # noqa: F401

__all__ = ["app"]
