"""Shared utilities for all pollers."""
from __future__ import annotations

import re
import threading
import time
from html import unescape
from typing import Set

import cloudscraper
import requests
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


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


def extract_primary_text_from_html(html: str, container_patterns: list[str] | None = None) -> str:
    """Extract article body text using light heuristics.
    
    Args:
        html: HTML content to extract text from
        container_patterns: List of regex patterns to find specific containers
    """
    try:
        cleaned = _SCRIPT_STYLE_RE.sub(" ", html)
        
        scope = cleaned
        if container_patterns:
            for pattern in container_patterns:
                container_match = re.search(pattern, cleaned, flags=re.IGNORECASE)
                if container_match:
                    scope = container_match.group(1) if container_match.groups() else container_match.group(0)
                    break

        # Collect <p> blocks within the chosen scope
        p_matches = re.findall(r"<p[^>]*>([\s\S]*?)</p>", scope, flags=re.IGNORECASE)
        paragraphs: list[str] = []
        for raw in p_matches:
            text = strip_tags(raw)
            if text:
                paragraphs.append(text)
        if paragraphs:
            return "\n\n".join(paragraphs).strip()

        # Fallback: strip all tags from the scope
        return strip_tags(scope)
    except Exception:
        return ""


def filter_new_items(items, seen_ids: Set[str], id_attr: str = "id"):
    """Filter items to only return new ones not in seen_ids."""
    new_items = []
    for item in items:
        item_id = getattr(item, id_attr)
        if item_id not in seen_ids:
            new_items.append(item)
            seen_ids.add(item_id)
    return new_items