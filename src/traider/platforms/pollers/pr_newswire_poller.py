from __future__ import annotations

import os
import re
import threading
import time
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from html import unescape
from typing import Iterable, List, Optional, Sequence, Set

import requests
from dotenv import load_dotenv
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib.parse import urljoin
from urllib3.util.retry import Retry


# Load .env from project root if present
load_dotenv()


# --- Configuration ---
LIST_URL: str = (
    "https://www.prnewswire.com/news-releases/financial-services-latest-news/earnings-list/?page=1&pagesize=10"
)
BASE_URL: str = "https://www.prnewswire.com"

# Polling, identity, and request pacing
DEFAULT_POLLING_INTERVAL_SECONDS: int = 3
DEFAULT_USER_AGENT: str = "TraderPRNWatcher/1.0 admin@example.com"
DEFAULT_MIN_REQUEST_INTERVAL_SEC: float = 0.25  # be courteous
DEFAULT_ARTICLE_TIMEOUT_SECONDS: float = 8.0


@dataclass(frozen=True)
class PRNListItem:
    id: str
    title: str
    url: str
    time_et: str | None = None
    summary: str | None = None


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
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )
    return session


def _normalize_to_utc_z(dt_text: str) -> str | None:
    """Best-effort normalization of timestamp strings to UTC Z ISO format."""
    try:
        txt = dt_text.strip()
        dt = datetime.fromisoformat(txt.replace("Z", "+00:00"))
        dt_utc = dt.astimezone(timezone.utc)
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


_SCRIPT_STYLE_RE = re.compile(r"<(script|style)[\s\S]*?>[\s\S]*?</\\\1>", flags=re.IGNORECASE)
_TAGS_RE = re.compile(r"<[^>]+>")
_MULTISPACE_RE = re.compile(r"\s{2,}")


def _strip_tags(html: str) -> str:
    text = _TAGS_RE.sub(" ", html)
    text = unescape(text)
    text = _MULTISPACE_RE.sub(" ", text)
    return text.strip()


def _extract_primary_text_from_html(html: str) -> str:
    """Extract article body text using light heuristics (paragraph-first)."""
    try:
        cleaned = _SCRIPT_STYLE_RE.sub(" ", html)

        # Prefer PR Newswire's typical containers if present
        container_match = re.search(
            r"<(div|section)[^>]+class=\"[^\"]*(release-body|articleBody|story-body|post-content)[^\"]*\"[^>]*>([\s\S]*?)</\\\1>",
            cleaned,
            flags=re.IGNORECASE,
        )
        scope = container_match.group(0) if container_match else cleaned

        # Collect <p> blocks within the chosen scope
        p_matches = re.findall(r"<p[^>]*>([\s\S]*?)</p>", scope, flags=re.IGNORECASE)
        paragraphs: list[str] = []
        for raw in p_matches:
            text = _strip_tags(raw)
            if text:
                paragraphs.append(text)
        if paragraphs:
            return ("\n\n".join(paragraphs))[:20000]

        # Fallback: strip all tags
        return _strip_tags(scope)[:20000]
    except Exception:
        return ""


def fetch_article_text(
    url: str,
    session: Session,
    timeout_seconds: float | None = None,
    skip_extraction: bool = False,
    timing_enabled: bool = False,
) -> str | None:
    headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    ts: float = float(timeout_seconds) if timeout_seconds is not None else DEFAULT_ARTICLE_TIMEOUT_SECONDS

    t0 = time.monotonic()
    response = session.get(url, headers=headers, timeout=(5, ts))
    response.raise_for_status()
    t1 = time.monotonic()

    fetch_ms = (t1 - t0) * 1000.0
    if timing_enabled:
        print(f"     [TIMING] Article fetch: {fetch_ms:.1f} ms")

    if skip_extraction:
        if timing_enabled:
            print("     [TIMING] Extraction skipped (PRN_SKIP_EXTRACTION=1)")
        return None

    t2 = time.monotonic()
    text = _extract_primary_text_from_html(response.text)
    t3 = time.monotonic()

    if timing_enabled:
        extract_ms = (t3 - t2) * 1000.0
        total_ms = (t3 - t0) * 1000.0
        print(f"     [TIMING] Extraction: {extract_ms:.1f} ms | Total: {total_ms:.1f} ms")

    return text


def _parse_list_items(html: str) -> List[PRNListItem]:
    """Parse the PR Newswire earnings list HTML and extract list items.

    Expected structure (simplified):
      <div class="row newsCards" ...>
        <div class="card ...">
          <a class="newsreleaseconsolidatelink ..." href="/news-releases/...-302527790.html">
            ...
            <h3>
              <small>09:15 ET</small>
              Title text here
            </h3>
            <p class="remove-outline">Summary...</p>
          </a>
        </div>
      </div>
    """
    items: list[PRNListItem] = []

    # Narrow to the main list area if possible (between Latest header and next major section)
    try:
        latest_section = re.search(
            r"<h2 class=\"section-header\">\s*Latest[\s\S]*?(?=<h2 class=\"section-header\">|<div class=\"twitter\")",
            html,
            flags=re.IGNORECASE,
        )
        scope_html = latest_section.group(0) if latest_section else html
    except Exception:
        scope_html = html

    # Find each card anchor, including its nearby H3 and P
    card_pattern = re.compile(
        r"<a[^>]+class=\"[^\"]*newsreleaseconsolidatelink[^\"]*\"[^>]+href=\"([^\"]+)\"[\s\S]*?<h3>([\s\S]*?)</h3>[\s\S]*?(?:<p[^>]*class=\"[^\"]*remove-outline[^\"]*\"[^>]*>([\s\S]*?)</p>)?",
        flags=re.IGNORECASE,
    )

    for match in card_pattern.finditer(scope_html):
        href_rel = (match.group(1) or "").strip()
        h3_html = match.group(2) or ""
        p_html = match.group(3) or ""

        if not href_rel:
            continue
        url = urljoin(BASE_URL, href_rel)

        # Extract time from <small> tag within h3 and title from remaining text
        time_match = re.search(r"<small[^>]*>([\s\S]*?)</small>", h3_html, flags=re.IGNORECASE)
        time_et: str | None = None
        if time_match:
            time_et = _strip_tags(time_match.group(1)) or None
            h3_html = h3_html.replace(time_match.group(0), " ")
        title = _strip_tags(h3_html)
        summary = _strip_tags(p_html) if p_html else None

        item_id = url  # stable enough; could also parse trailing numeric id
        items.append(
            PRNListItem(id=item_id, title=title, url=url, time_et=time_et, summary=summary)
        )

    return items


def _filter_new_items(items: Iterable[PRNListItem], seen_ids: Set[str]) -> List[PRNListItem]:
    new_items: list[PRNListItem] = []
    for item in items:
        if item.id not in seen_ids:
            new_items.append(item)
            seen_ids.add(item.id)
    return new_items


def _fetch_list_page(session: Session, url: str) -> Response:
    headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    response = session.get(url, headers=headers, timeout=15)
    response.raise_for_status()
    return response


def run_poller(
    polling_interval_seconds: Optional[int] = None,
    user_agent: Optional[str] = None,
) -> None:
    """Run the PR Newswire earnings list poller and article fetcher.

    Page: https://www.prnewswire.com/news-releases/financial-services-latest-news/earnings-list/?page=1&pagesize=10
    """

    effective_interval: int = (
        polling_interval_seconds
        if polling_interval_seconds is not None
        else int(os.getenv("PRN_POLL_INTERVAL", DEFAULT_POLLING_INTERVAL_SECONDS))
    )
    effective_user_agent: str = user_agent or os.getenv("PRN_USER_AGENT", DEFAULT_USER_AGENT)
    min_req_interval: float = float(os.getenv("PRN_MIN_REQUEST_INTERVAL_SEC", DEFAULT_MIN_REQUEST_INTERVAL_SEC))
    article_timeout_sec: float = float(os.getenv("PRN_ARTICLE_TIMEOUT_SEC", DEFAULT_ARTICLE_TIMEOUT_SECONDS))

    max_poll_interval: float = float(os.getenv("PRN_MAX_POLL_INTERVAL", 30))
    backoff_multiplier: float = float(os.getenv("PRN_NOCHANGE_BACKOFF", 1.5))
    jitter_fraction: float = float(os.getenv("PRN_JITTER_FRACTION", 0.1))  # +/-10%
    skip_extraction: bool = os.getenv("PRN_SKIP_EXTRACTION", "0").lower() in ("1", "true", "yes", "y")
    timing_enabled: bool = os.getenv("PRN_TIMING", "0").lower() in ("1", "true", "yes", "y")

    seen_item_ids: Set[str] = set()
    session = _build_session(effective_user_agent, min_req_interval)

    print("Starting PR Newswire Earnings List Poller...")
    print("------------------------------------------")
    print(
        f"Polling interval: {effective_interval}s | User-Agent: {effective_user_agent} | "
        f"Min request interval: {min_req_interval:.3f}s | Max poll: {max_poll_interval:.1f}s | "
        f"Backoff: {backoff_multiplier}x | Jitter: ±{int(jitter_fraction*100)}%"
    )

    if "example.com" in effective_user_agent:
        print(
            "[WARN] Your User-Agent appears to be a placeholder. Set PRN_USER_AGENT to include your app name and a real contact email/phone."
        )

    current_interval: float = float(effective_interval)

    while True:
        new_items: List[PRNListItem] = []
        try:
            response = _fetch_list_page(session, LIST_URL)
            list_html = response.text

            items: List[PRNListItem] = _parse_list_items(list_html)
            new_items = _filter_new_items(items, seen_item_ids)

            if new_items:
                # Oldest first for chronological output
                new_items.reverse()
                print(f"[{time.ctime()}] Detected {len(new_items)} new PRN news item(s):")
                for item in new_items:
                    print(f"  -> Title: {item.title}")
                    if item.time_et:
                        print(f"     TIME (ET): {item.time_et}")
                    if item.summary:
                        print(f"     SUMMARY: {item.summary[:160]}")
                    print(f"     URL: {item.url}")

                    # Fetch article text
                    try:
                        article_text = fetch_article_text(
                            item.url,
                            session=session,
                            timeout_seconds=article_timeout_sec,
                            skip_extraction=skip_extraction,
                            timing_enabled=timing_enabled,
                        )
                        if article_text:
                            preview = article_text[:300].replace("\n", " ")
                            print("   [ARTICLE] Extracted text. Preview:")
                            print(f"     {preview}...")
                        else:
                            print("   [ARTICLE] No extractable text found.")
                    except Exception as article_exc:
                        print(f"   [ARTICLE] Error while fetching article: {article_exc}")
                    print("")
            else:
                print(f"[{time.ctime()}] No new items detected. Checking again in {effective_interval}s...")
                current_interval = min(max_poll_interval, current_interval * 1.1)

        except requests.exceptions.RequestException as exc:
            print(f"[{time.ctime()}] ERROR: Could not connect to PR Newswire. {exc}")
            current_interval = min(max_poll_interval, max(current_interval, effective_interval) * backoff_multiplier)
        except Exception as exc:  # noqa: BLE001 - broad for resilience
            print(f"[{time.ctime()}] An unexpected error occurred: {exc}")

        # Reset to base when we saw new items; otherwise use adaptive interval
        if new_items:
            current_interval = float(effective_interval)
        jitter = 1.0 + random.uniform(-jitter_fraction, jitter_fraction)
        sleep_for = max(1.0, current_interval * jitter)
        time.sleep(sleep_for)


if __name__ == "__main__":
    run_poller()


