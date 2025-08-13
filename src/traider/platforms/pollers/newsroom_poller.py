from __future__ import annotations

import os
import re
import random
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Iterable, List, Optional, Set

import requests
import xml.etree.ElementTree as ET
from dotenv import load_dotenv
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# Load .env from project root if present
load_dotenv()


# --- Configuration ---
RSS_FEED_URL: str = "https://www.newswire.com/newsroom/rss"

# Polling, identity, and request pacing
DEFAULT_POLLING_INTERVAL_SECONDS: int = 5
DEFAULT_USER_AGENT: str = "TraderNWRWatcher/1.0 admin@example.com"
DEFAULT_MIN_REQUEST_INTERVAL_SEC: float = 0.25  # 4 req/sec cap to be courteous


@dataclass(frozen=True)
class NewsEntry:
    id: str
    title: str
    url: str
    published_utc: str | None = None
    content_html: str | None = None


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


def _build_session(user_agent: str, min_interval_seconds: float) -> Session:
    """Create a requests Session with retries, throttle, and proper headers."""
    session: Session = requests.Session()

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

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
        }
    )
    return session


def _fetch_feed(
    session: Session,
    url: str,
    etag: Optional[str] = None,
    last_modified: Optional[str] = None,
) -> Response:
    """Fetch the RSS feed with conditional headers for caching.

    Returns the Response. If the feed is unchanged, status code will be 304.
    """
    headers: dict[str, str] = {}
    if etag:
        headers["If-None-Match"] = etag
    if last_modified:
        headers["If-Modified-Since"] = last_modified

    response: Response = session.get(url, headers=headers, timeout=15)
    if response.status_code != 304:
        response.raise_for_status()
    return response


def _normalize_rfc822_to_utc_z(dt_text: str) -> str | None:
    """Parse an RFC-822 formatted date string and return a UTC ISO8601 string with Z."""
    try:
        from email.utils import parsedate_to_datetime
        
        dt = parsedate_to_datetime(dt_text.strip())
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def _parse_entries(xml_bytes: bytes) -> List[NewsEntry]:
    """Parse RSS XML and extract items as a list of NewsEntry objects."""
    root = ET.fromstring(xml_bytes)
    
    # RSS items are under /rss/channel/item
    items = root.findall("./channel/item")
    parsed: List[NewsEntry] = []
    
    for item in items:
        guid_el = item.find("guid")
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate")
        desc_el = item.find("description")

        # A GUID or link is essential to uniquely identify an item
        entry_id: str | None = None
        if guid_el is not None and guid_el.text:
            entry_id = guid_el.text.strip()
        elif link_el is not None and link_el.text:
            entry_id = link_el.text.strip()
        else:
            continue # Cannot uniquely identify this item

        if title_el is None or not title_el.text or link_el is None or not link_el.text:
            continue

        title = title_el.text.strip()
        href = link_el.text.strip()

        published_utc = _normalize_rfc822_to_utc_z(pub_el.text) if (pub_el is not None and pub_el.text) else None
        
        content_html: str | None = None
        if desc_el is not None and desc_el.text:
            content_html = desc_el.text.strip()

        parsed.append(
            NewsEntry(
                id=entry_id,
                title=title,
                url=href,
                published_utc=published_utc,
                content_html=content_html,
            )
        )
    return parsed


def _parse_feed_updated(xml_bytes: bytes) -> Optional[str]:
    """Extract the feed-level <lastBuildDate> or <pubDate> and normalize to UTC Z."""
    try:
        root = ET.fromstring(xml_bytes)
        channel = root.find("channel")
        if channel is None:
            return None
        
        date_el = channel.find("lastBuildDate")
        if date_el is None:
            date_el = channel.find("pubDate")

        if date_el is None or not (date_el.text and date_el.text.strip()):
            return None
        return _normalize_rfc822_to_utc_z(date_el.text)
    except Exception:
        return None


def _filter_new_entries(entries: Iterable[NewsEntry], seen_ids: Set[str]) -> List[NewsEntry]:
    new_entries: List[NewsEntry] = []
    for entry in entries:
        if entry.id not in seen_ids:
            new_entries.append(entry)
            seen_ids.add(entry.id)
    return new_entries


_SCRIPT_STYLE_RE = re.compile(r"<(script|style)[\s\S]*?>[\s\S]*?</\1>", flags=re.IGNORECASE)
_TAGS_RE = re.compile(r"<[^>]+>")
_MULTISPACE_RE = re.compile(r"\s{2,}")


def _extract_primary_text_from_html(html: str) -> str:
    """Attempt to extract main article text from HTML with light heuristics."""
    try:
        # Remove script/style first
        cleaned = _SCRIPT_STYLE_RE.sub(" ", html)
        # Try to collect <p> texts
        paragraph_matches = re.findall(r"<p[^>]*>([\s\s]*?)</p>", cleaned, flags=re.IGNORECASE)
        paragraphs: list[str] = []
        for raw in paragraph_matches:
            text = _TAGS_RE.sub(" ", raw)
            text = unescape(text)
            text = _MULTISPACE_RE.sub(" ", text)
            text = text.strip()
            if text:
                paragraphs.append(text)
        if paragraphs:
            joined = "\n\n".join(paragraphs)
            return joined[:15000]

        # Fallback: strip all tags from whole body
        text = _TAGS_RE.sub(" ", cleaned)
        text = unescape(text)
        text = _MULTISPACE_RE.sub(" ", text)
        return text.strip()[:15000]
    except Exception:
        return ""


def fetch_article_text(url: str, session: Session) -> str | None:
    headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    response = session.get(url, headers=headers, timeout=20)
    response.raise_for_status()
    content_type = response.headers.get("Content-Type", "")
    if "text/html" not in content_type and "xml" not in content_type:
        pass
    return _extract_primary_text_from_html(response.text)


def run_poller(
    polling_interval_seconds: Optional[int] = None,
    user_agent: Optional[str] = None,
) -> None:
    """Run the real-time Newswire.com RSS feed poller and article fetcher."""

    effective_interval: int = (
        polling_interval_seconds
        if polling_interval_seconds is not None
        else int(os.getenv("NWR_POLL_INTERVAL", DEFAULT_POLLING_INTERVAL_SECONDS))
    )
    effective_user_agent: str = user_agent or os.getenv("NWR_USER_AGENT", DEFAULT_USER_AGENT)
    min_req_interval: float = float(os.getenv("NWR_MIN_REQUEST_INTERVAL_SEC", DEFAULT_MIN_REQUEST_INTERVAL_SEC))

    max_poll_interval: float = float(os.getenv("NWR_MAX_POLL_INTERVAL", 30))
    backoff_multiplier: float = float(os.getenv("NWR_NOCHANGE_BACKOFF", 1.5))
    jitter_fraction: float = float(os.getenv("NWR_JITTER_FRACTION", 0.1))  # +/-10%

    seen_entry_ids: Set[str] = set()
    session = _build_session(effective_user_agent, min_req_interval)

    print("Starting Newswire.com Real-Time News Poller...")
    print("---------------------------------------------")
    print(
        f"Polling interval: {effective_interval}s | User-Agent: {effective_user_agent} | "
        f"Min request interval: {min_req_interval:.3f}s | Max poll: {max_poll_interval:.1f}s | "
        f"Backoff: {backoff_multiplier}x | Jitter: ±{int(jitter_fraction*100)}%"
    )

    if "example.com" in effective_user_agent:
        print(
            "[WARN] Your User-Agent appears to be a placeholder. Set NWR_USER_AGENT to include your app name and a real contact email/phone."
        )

    feed_etag: Optional[str] = None
    feed_last_modified: Optional[str] = None
    current_interval: float = float(effective_interval)

    while True:
        new_entries: List[NewsEntry] = []
        try:
            response = _fetch_feed(session, RSS_FEED_URL, etag=feed_etag, last_modified=feed_last_modified)

            if response.status_code == 304:
                current_interval = float(effective_interval)
                jitter = 1.0 + random.uniform(-jitter_fraction, jitter_fraction)
                sleep_for = max(1.0, float(effective_interval) * jitter)
                print(f"[{time.ctime()}] Feed not modified (304). Next check in {sleep_for:.1f}s (base interval)...")
                time.sleep(sleep_for)
                continue

            feed_etag = response.headers.get("ETag")
            feed_last_modified = response.headers.get("Last-Modified")

            feed_updated_utc = _parse_feed_updated(response.content)
            entries: List[NewsEntry] = _parse_entries(response.content)
            new_entries = _filter_new_entries(entries, seen_entry_ids)

            if new_entries:
                # Oldest first for chronological output
                new_entries.reverse()
                print(f"[{time.ctime()}] Detected {len(new_entries)} new news item(s):")
                if feed_updated_utc:
                    print(f"Feed UPDATED (UTC): {feed_updated_utc}")
                for item in new_entries:
                    print(f"  -> Title: {item.title}")
                    if item.published_utc:
                        print(f"     PUBLISHED (UTC): {item.published_utc}")
                    print(f"     URL: {item.url}")

                    # Optional preview of feed-provided HTML content before fetching full article
                    if item.content_html:
                        try:
                            feed_preview = _extract_primary_text_from_html(item.content_html)
                            if feed_preview:
                                preview_snippet = feed_preview[:240].replace("\n", " ")
                                print("   [FEED-CONTENT] Preview:")
                                print(f"     {preview_snippet}...")
                        except Exception:
                            pass
                    print("")

                    # Fetch article text
                    try:
                        article_text = fetch_article_text(item.url, session=session)
                        if article_text:
                            preview = article_text[:300].replace("\n", " ")
                            print("   [ARTICLE] Extracted text. Preview:")
                            print(f"     {preview}...")
                        else:
                            print("   [ARTICLE] No extractable text found.")
                    except Exception as article_exc:
                        print(f"   [ARTICLE] Error while fetching article: {article_exc}")
            else:
                print(f"[{time.ctime()}] No new items detected. Checking again in {effective_interval}s...")
                current_interval = min(max_poll_interval, current_interval * 1.1)

        except requests.exceptions.RequestException as exc:
            print(f"[{time.ctime()}] ERROR: Could not connect to Newswire.com. {exc}")
            current_interval = min(max_poll_interval, max(current_interval, effective_interval) * backoff_multiplier)
        except ET.ParseError as exc:
            print(f"[{time.ctime()}] ERROR: Failed to parse XML feed. {exc}")
        except Exception as exc:  # noqa: BLE001 - broad for resilience
            print(f"[{time.ctime()}] An unexpected error occurred: {exc}")

        # Reset to base when we saw new entries; otherwise use adaptive interval
        if new_entries:
            current_interval = float(effective_interval)
        jitter = 1.0 + random.uniform(-jitter_fraction, jitter_fraction)
        sleep_for = max(1.0, current_interval * jitter)
        time.sleep(sleep_for)


if __name__ == "__main__":
    run_poller()