"""Shared utilities for all pollers."""
from __future__ import annotations

import re
import threading
import time
from html import unescape
from typing import Optional

import cloudscraper
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from traider.platforms.parsers.webpage_helper.simple_text_extractor import (
    simple_text_extractor,
    StructuredContent,
)
from traider.interfaces import CacheInterface


class ThrottledHTTPAdapter(HTTPAdapter):
    """HTTPAdapter that enforces a minimum interval between requests.

    Thread-safe and process-local. Helps avoid bursty request patterns.
    """

    def __init__(self, min_interval_seconds: float, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._min_interval: float = max(0.0, float(min_interval_seconds))
        self._lock = threading.Lock()
        self._last_request_time: float = 0.0

    def send(self, request, **kwargs):  # type: ignore[override]
        if self._min_interval > 0:
            with self._lock:
                now = time.monotonic()
                wait_for = self._min_interval - (now - self._last_request_time)
                if wait_for > 0:
                    time.sleep(wait_for)
                self._last_request_time = time.monotonic()
        return super().send(request, **kwargs)


# Regex patterns for HTML processing
_SCRIPT_STYLE_RE = re.compile(r"<(script|style)[\s\S]*?>[\s\S]*?</\1>", re.IGNORECASE)
_TAGS_RE = re.compile(r"<[^>]+>")
_MULTISPACE_RE = re.compile(r"\s{2,}")


def strip_tags(html: str) -> str:
    """Remove HTML tags and normalize whitespace."""
    text = _TAGS_RE.sub(" ", html)
    text = unescape(text)
    text = _MULTISPACE_RE.sub(" ", text)
    return text.strip()


def build_session(
    user_agent: str, 
    min_interval_seconds: float, 
    use_cloudscraper: bool = False,
    extra_headers: dict | None = None
) -> Session:
    """Create a requests Session with retries, throttle, and proper headers."""
    if use_cloudscraper:
        session: Session = cloudscraper.create_scraper()  # type: ignore
    else:
        session = requests.Session()

    retry_strategy = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
        respect_retry_after_header=True,
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = ThrottledHTTPAdapter(min_interval_seconds=min_interval_seconds, max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    default_headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    if extra_headers:
        default_headers.update(extra_headers)
    
    session.headers.update(default_headers)
    return session

    # Note: legacy extract_primary_text_from_html has been removed.


def extract_text_from_html(html: str, base_url: Optional[str] | None = None) -> Optional[str]:
    """Extract readable text from an HTML document.

    Utilises the default simple text extractor to obtain the main readable
    content of a page. If a main content section is not present, the four
    auxiliary sections (header, navigation, sidebar, footer) are concatenated
    and returned instead.
    """
    structured: Optional[StructuredContent] = simple_text_extractor(
        html, base_url=base_url
    )
    if structured is None:
        return None
    if structured.main_content:
        return structured.main_content

    parts = [
        structured.header_content,
        structured.navigation_content,
        structured.sidebar_content,
        structured.footer_content,
    ]
    combined = "\n\n".join(p for p in parts if p)
    return combined or None


def filter_new_items(
    items,
    cache: CacheInterface,
    *,
    id_attr: str = "id",
) -> list:
    """Return list of items whose *id* has not been seen before.

    The provided *cache* must implement :meth:`CacheInterface.add`.  For each
    item we attempt to ``cache.add(item_id)``.  The item is considered *new*
    only if the call returns ``True`` (meaning the ID was absent previously).
    """
    new_items: list = []
    for item in items:
        item_id = getattr(item, id_attr)
        if cache.add(item_id):
            new_items.append(item)
    return new_items