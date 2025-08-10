from __future__ import annotations

"""
The Hierarchy of 8-K Item Signals (for reader reference)

Tier 1: Highest Priority, Immediate Market Movers
- Item 2.02 — Results of Operations and Financial Condition
  Why: Earnings announcement (revenue, EPS, guidance). Major short-term driver.
  Impact: Very High (±)
- Item 4.02 — Non-Reliance on Previously Issued Financial Statements
  Why: Past reports cannot be trusted. Signals deep accounting issues.
  Impact: Extremely High (−)
- Item 1.01 — Entry into a Material Definitive Agreement
  Why: M&A, major partnerships, financings.
  Impact: Very High (±)
- Item 5.02 — Departure of Directors or Certain Officers…
  Why: Unexpected executive departures (CEO/CFO) signal turmoil.
  Impact: High (usually −)
- Item 3.01 — Notice of Delisting or Failure to Satisfy Listing Rule
  Why: At risk of exchange delisting; severe distress.
  Impact: Very High (−)

Tier 2: Strong Signals of Financial Health & Risk
- Item 2.04 — Triggering Events That Accelerate a Financial Obligation
  Why: Default/covenant breach; liquidity crisis.
  Impact: High (−)
- Item 2.05 — Costs Associated with Exit or Disposal Activities
  Why: Restructuring, closures, layoffs.
  Impact: Moderate–High (context-dependent)
- Item 2.06 — Material Impairments
  Why: Asset write-downs (e.g., goodwill); overpayment or underperformance.
  Impact: Moderate–High (−)
- Item 4.01 — Changes in Registrant's Certifying Accountant
  Why: Auditor change; potential accounting disagreements.
  Impact: High (usually −)
- Item 1.02 — Termination of a Material Definitive Agreement
  Why: Deals collapsed; expectations reset.
  Impact: High (−)

Parsing strategy: detect Item numbers early for fast signal; prioritize EX-99.1/99.2/99.3/99.4;
flag material-contract exhibits (EX-10.1/EX-2.1); if high-impact items present but no
narrative exhibits, fallback to body text extraction.
"""

import html
import re
import time
from typing import Final, Optional, List, Dict
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import requests


# A compliant User-Agent is required by the SEC fair access policy.
# You may override this by passing a configured requests.Session.
DEFAULT_HEADERS: Final[dict[str, str]] = {
    "User-Agent": "TraderSECParser/1.0 admin@example.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


# Simple in-process LRU caches to avoid duplicate requests
_INDEX_TO_TXT_CACHE: "OrderedDict[str, str]" = OrderedDict()
_TXT_CONTENT_CACHE: "OrderedDict[str, str]" = OrderedDict()
_MAX_CACHE_ENTRIES: Final[int] = 2048

def _cache_get(cache: "OrderedDict[str, str]", key: str) -> Optional[str]:
    val = cache.get(key)
    if val is not None:
        cache.move_to_end(key)
    return val

def _cache_put(cache: "OrderedDict[str, str]", key: str, value: str) -> None:
    cache[key] = value
    cache.move_to_end(key)
    if len(cache) > _MAX_CACHE_ENTRIES:
        cache.popitem(last=False)


# Precompiled regex patterns for speed
DOCUMENT_BLOCK_REGEX: Final[re.Pattern[str]] = re.compile(
    r"<DOCUMENT>(.*?)</DOCUMENT>", re.IGNORECASE | re.DOTALL
)

# Matches TYPE lines like: <TYPE>EX-99.1, <TYPE>EX-99.01, with optional whitespace
TYPE_EX_99_1_REGEX: Final[re.Pattern[str]] = re.compile(
    r"<TYPE>\s*EX[-\s]?99(?:\.0?1)\b", re.IGNORECASE
)

# Optional fallback: EX-99.2
TYPE_EX_99_2_REGEX: Final[re.Pattern[str]] = re.compile(
    r"<TYPE>\s*EX[-\s]?99(?:\.0?2)\b", re.IGNORECASE
)

# Generic type extractor to support 99.3, 99.4 and others
TYPE_LINE_REGEX: Final[re.Pattern[str]] = re.compile(r"<TYPE>\s*([^\s<]+)", re.IGNORECASE)

# Extract inner TEXT block
TEXT_CONTENT_REGEX: Final[re.Pattern[str]] = re.compile(
    r"<TEXT>(.*?)</TEXT>", re.IGNORECASE | re.DOTALL
)

# Simple regex to strip all remaining HTML tags
HTML_TAG_REGEX: Final[re.Pattern[str]] = re.compile(r"<.*?>", re.DOTALL)

# br and paragraph-like tags to line breaks, before stripping tags
BREAK_TAGS_REGEX: Final[re.Pattern[str]] = re.compile(
    r"(?is)<\s*(br\s*/?|/p\s*>|/div\s*>|/li\s*>|/tr\s*>|/h[1-6]\s*>)"
)

# Header metadata
ACCEPTANCE_DATETIME_REGEX: Final[re.Pattern[str]] = re.compile(
    r"<ACCEPTANCE-DATETIME>\s*(\d{14})", re.IGNORECASE
)


def _get_session(session: Optional[requests.Session]) -> requests.Session:
    if session is not None:
        return session
    s = requests.Session()
    s.headers.update(DEFAULT_HEADERS)
    return s


def get_filing_text_url(
    filing_index_url: str, session: Optional[requests.Session] = None
) -> Optional[str]:
    """Resolve a filing index/detail page to the full submission .txt URL.

    This fetches the provided HTML index/detail page and returns the first
    link ending with .txt found under the document table.

    Args:
        filing_index_url: URL to an SEC filing index/detail page (HTML).
        session: Optional requests.Session for connection reuse and headers.

    Returns:
        Absolute URL to the .txt submission file, or None if not found/errors.
    """
    try:
        cached = _cache_get(_INDEX_TO_TXT_CACHE, filing_index_url)
        if cached:
            return cached
        sess = _get_session(session)
        resp = sess.get(filing_index_url, timeout=15)
        resp.raise_for_status()
        # Find first .txt link
        match = re.search(r'href="([^"]+?\.txt)"', resp.text, flags=re.IGNORECASE)
        if not match:
            return None
        href = match.group(1)
        if href.startswith("http"):
            _cache_put(_INDEX_TO_TXT_CACHE, filing_index_url, href)
            return href
        # Most SEC Archive links are relative to https://www.sec.gov
        absolute = "https://www.sec.gov" + href
        _cache_put(_INDEX_TO_TXT_CACHE, filing_index_url, absolute)
        return absolute
    except requests.exceptions.RequestException:
        return None
    except Exception:
        return None


def fetch_submission_text(
    filing_text_url: str, session: Optional[requests.Session] = None
) -> Optional[str]:
    """Fetch the full submission .txt content.

    Args:
        filing_text_url: Absolute URL to the submission .txt file.
        session: Optional requests.Session.

    Returns:
        The response text or None on error.
    """
    try:
        cached = _cache_get(_TXT_CONTENT_CACHE, filing_text_url)
        if cached is not None:
            return cached
        sess = _get_session(session)
        resp = sess.get(filing_text_url, timeout=20)
        resp.raise_for_status()
        text = resp.text
        if text:
            _cache_put(_TXT_CONTENT_CACHE, filing_text_url, text)
        return text
    except requests.exceptions.RequestException:
        return None
    except Exception:
        return None


def extract_8k_items(full_submission_text: str, head_chars: int = 60000) -> List[str]:
    """Extract 8-K item numbers near the top of the filing.

    Heuristic: scan only the first `head_chars` characters of the file to find
    occurrences of patterns like "Item 2.02" or "ITEM 5.02" and return
    the numbers in order of appearance, de-duplicated.
    """
    head = full_submission_text[:head_chars]
    # Robust match for Item <num> or Item <num>.<num> (e.g., 2.02, 7.01, 8.01, 9.01)
    pattern = re.compile(r"\bItem\s+([0-9]+(?:\.[0-9]{1,2})?)\b", re.IGNORECASE)
    seen: set[str] = set()
    ordered: List[str] = []
    for m in pattern.finditer(head):
        item_num = m.group(1)
        if item_num not in seen:
            seen.add(item_num)
            ordered.append(item_num)
    return ordered


def _extract_exhibit_block(full_submission_text: str) -> Optional[str]:
    """Find the EX-99.1 block inside <DOCUMENT> ... </DOCUMENT>.

    Returns the raw inner content of the <TEXT>...</TEXT> block if present,
    otherwise the whole document block body. Falls back to EX-99.2 when 99.1
    is not found.
    """
    blocks = DOCUMENT_BLOCK_REGEX.findall(full_submission_text)
    if not blocks:
        return None

    def pick_block(type_regex: re.Pattern[str]) -> Optional[str]:
        for block in blocks:
            if type_regex.search(block):
                text_match = TEXT_CONTENT_REGEX.search(block)
                return text_match.group(1) if text_match else block
        return None

    # Search priority: EX-99.1 -> EX-99.2 -> EX-99.3 -> EX-99.4
    chosen = pick_block(TYPE_EX_99_1_REGEX)
    if chosen is None:
        chosen = pick_block(TYPE_EX_99_2_REGEX)
    if chosen is None:
        # Dynamically match EX-99.3 / EX-99.4 without precompiled patterns
        for suffix in ("3", "4"):
            dynamic = re.compile(rf"<TYPE>\s*EX[-\s]?99(?:\.0?{suffix}|\.{suffix})\b", re.IGNORECASE)
            chosen = pick_block(dynamic)
            if chosen is not None:
                break
    return chosen


def _extract_document_text_by_types(
    full_submission_text: str, preferred_types: List[str]
) -> Optional[str]:
    """Extract <TEXT> from the first <DOCUMENT> whose <TYPE> matches any in preferred_types.

    Comparison is case-insensitive.
    """
    blocks = DOCUMENT_BLOCK_REGEX.findall(full_submission_text)
    if not blocks:
        return None
    normalized_targets = [t.lower() for t in preferred_types]
    for block in blocks:
        type_match = TYPE_LINE_REGEX.search(block)
        if not type_match:
            continue
        block_type = (type_match.group(1) or "").strip().lower()
        if block_type in normalized_targets:
            text_match = TEXT_CONTENT_REGEX.search(block)
            return text_match.group(1) if text_match else block
    return None


def scan_exhibit_types(full_submission_text: str) -> List[str]:
    """Return all exhibit TYPE tokens found across <DOCUMENT> blocks in order.

    Example returns: ["8-K", "EX-99.1", "EX-10.1"].
    """
    blocks = DOCUMENT_BLOCK_REGEX.findall(full_submission_text)
    types: List[str] = []
    if not blocks:
        return types
    for block in blocks:
        type_match = TYPE_LINE_REGEX.search(block)
        if not type_match:
            continue
        token = (type_match.group(1) or "").strip()
        types.append(token)
    return types


@dataclass(frozen=True)
class ItemSignalMeta:
    item_number: str
    title: str
    why: str
    potential_impact: str
    tier: int


# Mapping of Items to metadata per the hierarchy above
ITEM_SIGNAL_HIERARCHY: Final[Dict[str, ItemSignalMeta]] = {
    # Tier 1
    "2.02": ItemSignalMeta(
        item_number="2.02",
        title="Results of Operations and Financial Condition",
        why="Earnings announcement; revenue/EPS/guidance; strong short-term driver.",
        potential_impact="Very High (±)",
        tier=1,
    ),
    "4.02": ItemSignalMeta(
        item_number="4.02",
        title="Non-Reliance on Previously Issued Financial Statements",
        why="Past reports cannot be trusted; deep accounting issues.",
        potential_impact="Extremely High (−)",
        tier=1,
    ),
    "1.01": ItemSignalMeta(
        item_number="1.01",
        title="Entry into a Material Definitive Agreement",
        why="M&A/partnerships/financings.",
        potential_impact="Very High (±)",
        tier=1,
    ),
    "5.02": ItemSignalMeta(
        item_number="5.02",
        title="Departure of Directors or Certain Officers…",
        why="Exec departures (CEO/CFO) imply turmoil.",
        potential_impact="High (usually −)",
        tier=1,
    ),
    "3.01": ItemSignalMeta(
        item_number="3.01",
        title="Notice of Delisting or Failure to Satisfy Listing Rule",
        why="Delisting risk; severe distress.",
        potential_impact="Very High (−)",
        tier=1,
    ),
    # Tier 2
    "2.04": ItemSignalMeta(
        item_number="2.04",
        title="Triggering Events That Accelerate a Financial Obligation",
        why="Default/covenant breach; liquidity crisis.",
        potential_impact="High (−)",
        tier=2,
    ),
    "2.05": ItemSignalMeta(
        item_number="2.05",
        title="Costs Associated with Exit or Disposal Activities",
        why="Restructuring/closures/layoffs.",
        potential_impact="Moderate–High (context)",
        tier=2,
    ),
    "2.06": ItemSignalMeta(
        item_number="2.06",
        title="Material Impairments",
        why="Asset write-downs (e.g., goodwill).",
        potential_impact="Moderate–High (−)",
        tier=2,
    ),
    "4.01": ItemSignalMeta(
        item_number="4.01",
        title="Changes in Registrant's Certifying Accountant",
        why="Auditor change; potential accounting disagreements.",
        potential_impact="High (usually −)",
        tier=2,
    ),
    "1.02": ItemSignalMeta(
        item_number="1.02",
        title="Termination of a Material Definitive Agreement",
        why="Deals collapsed; expectations reset.",
        potential_impact="High (−)",
        tier=2,
    ),
}


def classify_items(items: List[str]) -> Dict[str, int]:
    """Return a map of item -> tier (1,2,...) for detected items using the hierarchy.

    Items without a known tier are omitted.
    """
    tiers: Dict[str, int] = {}
    for item in items:
        meta = ITEM_SIGNAL_HIERARCHY.get(item)
        if meta:
            tiers[item] = meta.tier
    return tiers


# High-impact items that warrant body-text fallback when no EX-99.x is available
HIGH_IMPACT_ITEMS: Final[set[str]] = {"2.02", "4.02", "1.01", "5.02", "3.01"}


@dataclass(frozen=True)
class EightKParseResult:
    items: List[str]
    items_tier_map: Dict[str, int]
    highest_item_tier: Optional[int]
    primary_exhibit_type: Optional[str]
    primary_text: Optional[str]
    exhibits_found: List[str]
    has_material_contract_exhibit: bool
    fallback_used: bool
    fallback_text: Optional[str]
    acceptance_datetime_utc: Optional[str]


def _clean_html_to_text(raw_html: str) -> str:
    # Normalize common break/paragraph closers to newlines before strip
    normalized = BREAK_TAGS_REGEX.sub("\n", raw_html)
    # Strip all tags
    no_tags = HTML_TAG_REGEX.sub("", normalized)
    # Decode entities
    unescaped = html.unescape(no_tags)
    # Normalize whitespace while preserving line structure
    lines = [line.strip() for line in unescaped.splitlines()]
    # Drop empty lines if there are too many, keep paragraph structure compact
    non_empty = [line for line in lines if line]
    return "\n".join(non_empty)


def parse_exhibit_99_1(
    filing_text_url: str,
    session: Optional[requests.Session] = None,
    prefetched_text: Optional[str] = None,
) -> Optional[str]:
    """Fetch and parse Exhibit 99.1 (or 99.2 fallback) from a submission .txt.

    Args:
        filing_text_url: Absolute URL to the submission .txt file.
        session: Optional requests.Session for connection reuse and headers.

    Returns:
        Cleaned plain text string suitable for NLP, or None if not found/errors.
    """
    try:
        t0 = time.perf_counter()
        if prefetched_text is not None:
            full_text = prefetched_text
        else:
            sess = _get_session(session)
            resp = sess.get(filing_text_url, timeout=20)
            resp.raise_for_status()
            full_text = resp.text

        exhibit_html = _extract_exhibit_block(full_text)
        if not exhibit_html:
            return None

        clean_text = _clean_html_to_text(exhibit_html)
        # Final compaction: collapse 3+ consecutive blank lines to max 1
        clean_text = re.sub(r"\n{3,}", "\n\n", clean_text)
        # Also normalize excessive internal spaces
        clean_text = re.sub(r"[\t\x0b\x0c\r ]+", " ", clean_text)
        clean_text = re.sub(r" *\n *", "\n", clean_text).strip()

        _ = time.perf_counter() - t0  # time kept for potential logging
        return clean_text or None
    except requests.exceptions.RequestException:
        return None
    except Exception:
        return None


def _extract_acceptance_datetime_iso_utc(full_submission_text: str) -> Optional[str]:
    """Extract <ACCEPTANCE-DATETIME> and convert to ISO 8601 UTC string.

    The value is in the form YYYYMMDDHHMMSS and represents the SEC acceptance
    time in US/Eastern. We convert it to UTC and return e.g. "2025-08-08T20:10:18Z".
    """
    try:
        m = ACCEPTANCE_DATETIME_REGEX.search(full_submission_text)
        if not m:
            return None
        raw = m.group(1)
        # Parse naive then localize as US/Eastern to handle DST correctly
        dt_local = datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=ZoneInfo("US/Eastern"))
        dt_utc = dt_local.astimezone(ZoneInfo("UTC"))
        return dt_utc.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    except Exception:
        return None


def analyze_and_extract_8k(
    filing_text_url: str,
    session: Optional[requests.Session] = None,
    prefetched_text: Optional[str] = None,
) -> Optional[EightKParseResult]:
    """Full 8-K analysis with prioritized exhibits, flags, and fallback.

    Steps:
      1) Extract items near the top.
      2) Scan all <TYPE> tokens to detect exhibits and flags.
      3) Prioritized search for EX-99.1/99.2/99.3/99.4 and clean text.
      4) If no EX-99.x text and a high-impact item is present, fallback to 8-K body.
    """
    try:
        full_text = prefetched_text
        if full_text is None:
            fetched = fetch_submission_text(filing_text_url, session=session)
            if not fetched:
                return None
            full_text = fetched

        acceptance_iso_utc = _extract_acceptance_datetime_iso_utc(full_text)

        items = extract_8k_items(full_text)
        items_tier_map = classify_items(items)
        highest_item_tier = min(items_tier_map.values()) if items_tier_map else None
        types = scan_exhibit_types(full_text)
        types_lower = [t.lower() for t in types]
        has_material_contract = any(t in {"ex-10.1", "ex-2.1"} for t in types_lower)

        # prioritized exhibits
        primary_type: Optional[str] = None
        primary_html: Optional[str] = None

        for candidate in ["ex-99.1", "ex-99.2", "ex-99.3", "ex-99.4"]:
            html_block = _extract_document_text_by_types(full_text, [candidate])
            if html_block:
                primary_type = candidate.upper()
                primary_html = html_block
                break

        primary_text: Optional[str] = None
        if primary_html:
            primary_text = _clean_html_to_text(primary_html)
            primary_text = re.sub(r"\n{3,}", "\n\n", primary_text)
            primary_text = re.sub(r"[\t\x0b\x0c\r ]+", " ", primary_text)
            primary_text = re.sub(r" *\n *", "\n", primary_text).strip() or None

        fallback_used = False
        fallback_text: Optional[str] = None
        if primary_text is None and any(i in HIGH_IMPACT_ITEMS for i in items):
            # Fallback to the 8-K body
            body_html = _extract_document_text_by_types(full_text, ["8-k", "8-k/a"])  # type: ignore[arg-type]
            if body_html:
                fallback_text = _clean_html_to_text(body_html)
                fallback_text = re.sub(r"\n{3,}", "\n\n", fallback_text)
                fallback_text = re.sub(r"[\t\x0b\x0c\r ]+", " ", fallback_text)
                fallback_text = re.sub(r" *\n *", "\n", fallback_text).strip() or None
                fallback_used = fallback_text is not None

        return EightKParseResult(
            items=items,
            items_tier_map=items_tier_map,
            highest_item_tier=highest_item_tier,
            primary_exhibit_type=primary_type,
            primary_text=primary_text,
            exhibits_found=types,
            has_material_contract_exhibit=has_material_contract,
            fallback_used=fallback_used,
            fallback_text=fallback_text,
            acceptance_datetime_utc=acceptance_iso_utc,
        )
    except Exception:
        return None


__all__ = [
    "get_filing_text_url",
    "fetch_submission_text",
    "extract_8k_items",
    "scan_exhibit_types",
    "classify_items",
    "ITEM_SIGNAL_HIERARCHY",
    "EightKParseResult",
    "parse_exhibit_99_1",
    "analyze_and_extract_8k",
]


