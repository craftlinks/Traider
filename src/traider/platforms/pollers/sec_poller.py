from __future__ import annotations

import os
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import Iterable, List, Optional, Set

import requests
from requests import Response, Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Exhibit 99.1 parser utilities
from traider.platforms.parsers.sec.sec_8k_parser import (
    get_filing_text_url,
    fetch_submission_text,
    analyze_and_extract_8k,
)


# Load .env from project root if present
load_dotenv()

# --- Configuration ---
# SEC Atom feed for current 8-K filings
ATOM_FEED_URL: str = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&owner=exclude&count=100&output=atom"
)

# Polling interval in seconds. Respect SEC fair access.
DEFAULT_POLLING_INTERVAL_SECONDS: int = 3

# User-Agent per SEC policy. Allow override via env.
DEFAULT_USER_AGENT: str = "TraderSECWatcher/1.0 admin@example.com"


@dataclass(frozen=True)
class Filing:
    id: str
    title: str
    url: str


def _build_session(user_agent: str) -> Session:
    """Create a requests Session with retries and proper headers."""
    session: Session = requests.Session()

    retry_strategy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "application/atom+xml, application/xml;q=0.9, */*;q=0.8",
        }
    )
    return session


def _fetch_feed(session: Session, url: str) -> Response:
    """Fetch the Atom feed with a timeout and return the Response."""
    response: Response = session.get(url, timeout=10)
    response.raise_for_status()
    return response


def _parse_entries(xml_bytes: bytes) -> List[Filing]:
    """Parse Atom XML and extract filings as a list of Filing objects."""
    root = ET.fromstring(xml_bytes)
    namespace = {"atom": "http://www.w3.org/2005/Atom"}
    entries = root.findall("atom:entry", namespace)

    parsed_filings: List[Filing] = []
    for entry in entries:
        id_element = entry.find("atom:id", namespace)
        title_element = entry.find("atom:title", namespace)
        link_element = entry.find("atom:link[@rel='alternate']", namespace)

        if id_element is None or title_element is None or link_element is None:
            continue

        filing_id = id_element.text or ""
        title = title_element.text or ""
        filing_url = link_element.get("href", "")

        if not filing_id or not filing_url:
            continue

        parsed_filings.append(Filing(id=filing_id, title=title, url=filing_url))

    return parsed_filings


def _filter_new_filings(
    filings: Iterable[Filing], seen_ids: Set[str]
) -> List[Filing]:
    """Return filings whose IDs are not in seen_ids and add them to seen_ids."""
    new_filings: List[Filing] = []
    for filing in filings:
        if filing.id not in seen_ids:
            new_filings.append(filing)
            seen_ids.add(filing.id)
    return new_filings


def run_poller(
    polling_interval_seconds: Optional[int] = None,
    user_agent: Optional[str] = None,
) -> None:
    """Run the real-time SEC 8-K filing detector loop.

    Args:
        polling_interval_seconds: Seconds between polls. Defaults to env `SEC_POLL_INTERVAL` or 3.
        user_agent: Custom User-Agent. Defaults to env `SEC_USER_AGENT` or a safe placeholder.
    """

    effective_interval: int = (
        polling_interval_seconds
        if polling_interval_seconds is not None
        else int(os.getenv("SEC_POLL_INTERVAL", DEFAULT_POLLING_INTERVAL_SECONDS))
    )
    effective_user_agent: str = user_agent or os.getenv("SEC_USER_AGENT", DEFAULT_USER_AGENT)

    seen_filing_ids: Set[str] = set()
    session = _build_session(effective_user_agent)

    print("Starting SEC 8-K Real-Time Filing Detector...")
    print("---------------------------------------------")
    print(f"Polling interval: {effective_interval}s | User-Agent: {effective_user_agent}")

    while True:
        try:
            response = _fetch_feed(session, ATOM_FEED_URL)
            filings: List[Filing] = _parse_entries(response.content)
            new_filings: List[Filing] = _filter_new_filings(filings, seen_filing_ids)

            if new_filings:
                # Oldest first for chronological output
                new_filings.reverse()
                print(f"[{time.ctime()}] Detected {len(new_filings)} new 8-K filing(s):")
                for filing in new_filings:
                    print(f"  -> Title: {filing.title}")
                    print(f"     URL: {filing.url}\n")

                    # Attempt to resolve the full submission .txt and extract Exhibit 99.1
                    try:
                        txt_url = get_filing_text_url(filing.url, session=session)
                        if not txt_url:
                            print("   [PARSER] No .txt submission link found on index page.")
                        else:
                            # Single fetch for speed; reuse across analysis
                            submission_text = fetch_submission_text(txt_url, session=session)
                            if not submission_text:
                                print("   [PARSER] Failed to fetch submission text.")
                                continue

                            # Items + prioritized exhibits + flags + fallback in one pass
                            result = analyze_and_extract_8k(
                                txt_url, session=session, prefetched_text=submission_text
                            )
                            if not result:
                                print("   [PARSER] Analysis failed.")
                                continue

                            if result.items:
                                print(f"   [ITEMS] Detected 8-K Items: {result.items}")
                            else:
                                print("   [ITEMS] No explicit 8-K item matches found near the top.")

                            if result.has_material_contract_exhibit:
                                print("   [FLAG] Material contract-related exhibit present (EX-10.1/EX-2.1).")

                            if result.highest_item_tier is not None:
                                print(f"   [TIER] Highest detected item tier: {result.highest_item_tier}")

                            if result.primary_text:
                                print(
                                    f"   [PARSER] {result.primary_exhibit_type or 'EX-99.x'} extracted. Preview:"
                                )
                                preview = result.primary_text[:200].replace("\n", " ")
                                print(f"     {preview}...")
                            elif result.fallback_used and result.fallback_text:
                                print("   [FALLBACK] Used 8-K body text due to high-impact item.")
                                preview = result.fallback_text[:200].replace("\n", " ")
                                print(f"     {preview}...")
                            else:
                                print("   [PARSER] No narrative exhibit or fallback text extracted.")
                    except Exception as parse_exc:
                        print(f"   [PARSER] Unexpected parsing error: {parse_exc}")
            else:
                print(
                    f"[{time.ctime()}] No new filings detected. Checking again in {effective_interval}s..."
                )

        except requests.exceptions.RequestException as exc:
            print(f"[{time.ctime()}] ERROR: Could not connect to SEC server. {exc}")
        except ET.ParseError as exc:
            print(f"[{time.ctime()}] ERROR: Failed to parse XML feed. {exc}")
        except Exception as exc:  # noqa: BLE001 - broad for resilience in daemon loop
            print(f"[{time.ctime()}] An unexpected error occurred: {exc}")

        time.sleep(effective_interval)


if __name__ == "__main__":
    # Allow quick overrides via environment variables without code edits
    run_poller()


