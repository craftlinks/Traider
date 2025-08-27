from __future__ import annotations

"""Yahoo Finance scraping utilities."""

from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import math
import sqlite3
import time
from typing import Any, Final, List, Optional, Tuple
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup

from traider.db.database import get_db_connection
from traider.platforms.yahoo.helpers import (
    extract_earnings_data_json,
    extract_profile_data_html,
    extract_profile_data_json,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

_YF_PROFILE_TEMPLATE: Final[str] = "https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
_YF_PROFILE_JSON_TEMPLATE: Final[str] = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile&lang=en-US&region=US"
)
YF_CALENDAR_URL_TEMPLATE: Final[str] = "https://finance.yahoo.com/calendar/earnings?day={date}"
YF_VISUALIZATION_API: Final[str] = (
    "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US&crumb={crumb}"
)
_REQUEST_DELAY_S: Final[float] = 1.0  # politeness delay (seconds)

# ---------------------------------------------------------------------------
# Module level mutable state – *private*
# ---------------------------------------------------------------------------
_session: Optional[requests.Session] = None
_crumb: Optional[str] = None
_cookie: Optional[Any] = None  # "Any" because requests.cookies.Cookie is private

# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

def _to_float(val: Any) -> float:
    """Best-effort conversion to *float* returning ``nan`` on failure."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return float("nan")
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")


@dataclass(slots=True)
class Profile:
    website_url: str | None
    sector: str | None
    industry: str | None


@dataclass(slots=True)
class EarningsEvent:
    id: int
    ticker: str
    company_name: str
    event_name: str
    time_type: str
    earnings_call_time: datetime | None
    eps_estimate: float
    eps_actual: float
    eps_surprise: float
    eps_surprise_percent: float
    market_cap: float

    @staticmethod
    def from_db_row(row: sqlite3.Row) -> "EarningsEvent":
        return EarningsEvent(
            id=row["id"],
            ticker=row["company_ticker"],
            company_name="",  # TODO: denormalise company name if required
            event_name=row["event_name"],
            time_type=row["time_type"],
            earnings_call_time=row["report_datetime"],
            eps_estimate=row["eps_estimate"],
            eps_actual=row["reported_eps"],
            eps_surprise=(row["reported_eps"] - row["eps_estimate"]) if row["eps_estimate"] is not None and row["reported_eps"] is not None else float("nan"),
            eps_surprise_percent=row["surprise_percentage"],
            market_cap=row["market_cap"],
        )


@dataclass(slots=True)
class PressRelease:
    ticker: str
    title: str
    url: str
    type: str
    pub_date: str | None = None
    display_time: str | None = None
    company_name: str | None = None
    raw_html: str | None = None
    text_content: str | None = None


# ---------------------------------------------------------------------------
# Private helpers – cookie / crumb handling
# ---------------------------------------------------------------------------

def _fetch_cookie_and_crumb(session: requests.Session, *, timeout: int = 30) -> tuple[Any | None, str | None]:
    """Retrieve Yahoo's **A3** cookie + crumb anti-CSRF token."""
    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = session.get("https://fc.yahoo.com", headers=headers, timeout=timeout, allow_redirects=True)
        if not resp.cookies:
            return None, None

        cookie = next(iter(resp.cookies), None)
        if cookie is None:
            return None, None

        crumb_resp = session.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers=headers,
            cookies={cookie.name: str(cookie.value)},
            timeout=timeout,
        )
        crumb_txt = crumb_resp.text.strip()
        if not crumb_txt or "<html>" in crumb_txt:
            return cookie, None
        return cookie, crumb_txt
    except Exception:  # pragma: no cover – network problems swallow silently
        return None, None


def _refresh_cookie_and_crumb() -> None:
    global _cookie, _crumb, _session
    if _session is None:
        raise RuntimeError("Session not initialised – call initialize() first.")

    cookie, crumb = _fetch_cookie_and_crumb(_session)
    if cookie and crumb:
        _cookie, _crumb = cookie, crumb
        logger.info("Successfully refreshed Yahoo crumb: %s", crumb)
    else:
        raise RuntimeError("Unable to refresh Yahoo crumb token.")


# ---------------------------------------------------------------------------
# Initialisation & session access helpers
# ---------------------------------------------------------------------------

def initialize() -> None:
    """One-time initialiser for the module-level HTTP session and crumb."""
    global _session, _crumb, _cookie
    if _session is not None:
        return  # idempotent

    _session = requests.Session()
    _session.headers.update({"User-Agent": _USER_AGENT})

    cookie, crumb = _fetch_cookie_and_crumb(_session)
    if cookie and crumb:
        _cookie, _crumb = cookie, crumb
        logger.info("Successfully obtained Yahoo crumb: %s", crumb)
    else:
        _session = None  # reset so callers can retry
        raise RuntimeError("Unable to obtain Yahoo crumb token.")


def _get_session() -> requests.Session:
    if _session is None:
        logger.debug("Lazy-initialising Yahoo Finance session …")
        initialize()
    return _session  # type: ignore[return-value]

# ---------------------------------------------------------------------------
# Internal helpers (dataframe → dataclass etc.)
# ---------------------------------------------------------------------------

def _df_to_events(df: pd.DataFrame) -> list[EarningsEvent]:
    """Convert the *pandas* DataFrame from :pyfunc:`extract_earnings_data_json`."""
    events: list[EarningsEvent] = []
    if df.empty:
        return events

    for _, series in df.iterrows():
        try:
            id_val_raw = series.get("id", -1)
            id_val = int(id_val_raw) if id_val_raw not in (None, "") else -1

            eps_est = _to_float(series.get("EPS Estimate"))
            eps_act = _to_float(series.get("Reported EPS"))
            surprise_pct = _to_float(series.get("Surprise (%)"))
            eps_surp = eps_act - eps_est if not math.isnan(eps_est) and not math.isnan(eps_act) else float("nan")

            ect_raw = series.get("Earnings Call Time")
            if isinstance(ect_raw, pd.Timestamp):
                ect_dt = ect_raw.to_pydatetime()
            else:
                ect_dt = ect_raw  # type: ignore[assignment]

            events.append(
                EarningsEvent(
                    id=id_val,
                    ticker=str(series.get("Symbol", "")),
                    company_name=str(series.get("Company", "")),
                    event_name=str(series.get("Event Name", "")),
                    time_type=str(series.get("Time Type", "")),
                    earnings_call_time=ect_dt,
                    eps_estimate=eps_est,
                    eps_actual=eps_act,
                    eps_surprise=eps_surp,
                    eps_surprise_percent=surprise_pct,
                    market_cap=_to_float(series.get("Market Cap")),
                )
            )
        except Exception as exc:  # pragma: no cover – defensive
            logger.debug("Failed to convert earnings row: %s; data=%s", exc, series.to_dict())
    return events

# ---------------------------------------------------------------------------
# Public API – profile
# ---------------------------------------------------------------------------

def get_profile(ticker: str, *, from_json: bool = False) -> Profile:
    """Return (website, sector, industry) for *ticker*.

    Two independent Yahoo endpoints are supported:
    1.  HTML profile page (requires full parse – brittle).
    2.  JSON quoteSummary API (cleaner, but occasionally rate-limited).
    """
    url = (_YF_PROFILE_JSON_TEMPLATE if from_json else _YF_PROFILE_TEMPLATE).format(ticker=ticker)

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            resp = _get_session().get(url, timeout=20)
            resp.raise_for_status()

            if from_json:
                website, sector, industry = extract_profile_data_json(resp.json())
            else:
                website, sector, industry = extract_profile_data_html(resp.text)
            return Profile(website, sector, industry)
        except requests.RequestException as exc:
            if attempt < max_attempts - 1:
                logger.debug("Failed to fetch profile for %s (%d/%d): %s – retrying …", ticker, attempt + 1, max_attempts, exc)
                try:
                    _refresh_cookie_and_crumb()
                except RuntimeError:
                    break
            else:
                logger.error("Failed to fetch profile for %s: %s", ticker, exc)
    return Profile(None, None, None)

# ---------------------------------------------------------------------------
# Public API – earnings
# ---------------------------------------------------------------------------

def get_earnings(start_date: date, *, as_dataframe: bool = True, max_retries: int = 3) -> pd.DataFrame | List[EarningsEvent]:
    date_str = start_date.strftime("%Y-%m-%d")
    logger.debug("Fetching earnings for %s", date_str)

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                time.sleep(2 ** attempt)
                logger.debug("Retry %d/%d for %s", attempt, max_retries, date_str)

            api_url = YF_VISUALIZATION_API.format(crumb=quote_plus(_crumb))  # type: ignore[arg-type]
            next_day = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
            payload = {
                "offset": 0,
                "size": 250,
                "sortField": "intradaymarketcap",
                "sortType": "DESC",
                "entityIdType": "sp_earnings",
                "includeFields": [
                    "ticker",
                    "companyshortname",
                    "eventname",
                    "startdatetime",
                    "startdatetimetype",
                    "epsestimate",
                    "epsactual",
                    "epssurprisepct",
                    "intradaymarketcap",
                ],
                "query": {
                    "operator": "and",
                    "operands": [
                        {"operator": "gte", "operands": ["startdatetime", date_str]},
                        {"operator": "lt", "operands": ["startdatetime", next_day]},
                        {"operator": "eq", "operands": ["region", "us"]},
                        {
                            "operator": "or",
                            "operands": [
                                {"operator": "eq", "operands": ["eventtype", "EAD"]},
                                {"operator": "eq", "operands": ["eventtype", "ERA"]},
                            ],
                        },
                    ],
                },
            }
            resp = _get_session().post(
                api_url,
                json=payload,
                timeout=30,
                headers={"x-crumb": _crumb or "", "User-Agent": _USER_AGENT},
                cookies={_cookie.name: str(_cookie.value)} if _cookie else None,  # type: ignore[arg-type]
            )
            resp.raise_for_status()
            df = extract_earnings_data_json(resp.json())
            return df if as_dataframe else _df_to_events(df)
        except requests.RequestException as exc:
            logger.error("Network error contacting Yahoo (%d/%d): %s", attempt + 1, max_retries, exc)
        except Exception as exc:  # pragma: no cover – parse errors, etc.
            logger.error("Unhandled error parsing Yahoo response (%d/%d): %s", attempt + 1, max_retries, exc)

        if attempt < max_retries:
            try:
                _refresh_cookie_and_crumb()
            except RuntimeError as refresh_exc:
                logger.error("Failed to refresh Yahoo crumb – aborting: %s", refresh_exc)
                break

    return pd.DataFrame() if as_dataframe else []


# Convenience wrapper for multiple days ---------------------------------------------------

def get_earnings_for_date_range(start_date: date, end_date: date, *, as_dataframe: bool = False) -> pd.DataFrame | List[EarningsEvent]:
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    dfs: list[pd.DataFrame] = []
    events: list[EarningsEvent] = []

    for day in date_range:
        res = get_earnings(day, as_dataframe=as_dataframe)
        if as_dataframe and isinstance(res, pd.DataFrame) and not res.empty:
            dfs.append(res)
        elif not as_dataframe and isinstance(res, list):
            events.extend(res)
        time.sleep(_REQUEST_DELAY_S)

    return (pd.concat(dfs, ignore_index=True) if as_dataframe else events)  # type: ignore[return-value,arg-type]

# ---------------------------------------------------------------------------
# Public API – press releases
# ---------------------------------------------------------------------------

def get_press_releases(ticker: str, *, type: str, limit: int = 250) -> list[PressRelease]:  # noqa: A002 – type parameter is Yahoo nomenclature
    """Return up to *limit* press releases for *ticker*.

    The Yahoo Finance *NCP* endpoint does not offer traditional pagination.  The
    ``count`` parameter therefore caps the maximum number of items the service
    will return in **one** response (currently hard-limited to 250 by Yahoo).
    """

    base_url = "https://finance.yahoo.com/xhr/ncp"
    params = {
        "location": "US",
        "queryRef": "pressRelease",
        "serviceKey": "ncp_fin",
        "listName": f"{ticker.upper()}-press-releases",
        "lang": "en-US",
        "region": "US",
    }
    payload = {"serviceConfig": {"count": min(limit, 250), "spaceId": "95993639", "s": [ticker.upper()]}}
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://finance.yahoo.com",
        "Referer": f"https://finance.yahoo.com/quote/{ticker}/press-releases/",
    }

    releases: list[PressRelease] = []
    for attempt in range(2):
        try:
            resp = _get_session().post(
                base_url,
                params=params,
                json=payload,
                headers=headers,
                cookies={_cookie.name: str(_cookie.value)} if _cookie else None,  # type: ignore[arg-type]
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("data", {}).get("tickerStream", {}).get("stream", [])
            for itm in items[:limit]:
                content = itm.get("content", {})
                title = str(content.get("title", "")).strip()
                url_dict = content.get("canonicalUrl") or content.get("clickThroughUrl") or {}
                url_val = url_dict.get("url") if isinstance(url_dict, dict) else None
                if not url_val:
                    continue
                releases.append(
                    PressRelease(
                        ticker=ticker,
                        title=title,
                        url=url_val,
                        type=type,
                        pub_date=content.get("pubDate"),
                        display_time=content.get("displayTime"),
                    )
                )
            break  # success – leave retry loop
        except requests.RequestException as exc:
            logger.debug("Failed to fetch press releases for %s (%d): %s", ticker, attempt + 1, exc)
            if attempt == 0:
                try:
                    _refresh_cookie_and_crumb()
                except Exception as refresh_exc:  # pragma: no cover
                    logger.error("Failed to refresh Yahoo crumb: %s", refresh_exc)
                    break
        except ValueError as parse_exc:  # JSON decoding
            logger.error("Invalid JSON while fetching press releases for %s: %s", ticker, parse_exc)
            break

    return releases

# ---------------------------------------------------------------------------
# Public API – press‐release content & persistence
# ---------------------------------------------------------------------------


def get_press_release_content(url: str) -> str:
    """Return the raw HTML string of the article body for a Yahoo press‐release URL.

    The implementation closely mirrors the original *YahooFinance.get_press_release_content*
    method but is adapted to the module-level session helpers.
    """

    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://finance.yahoo.com/",
    }

    try:
        resp = _get_session().get(
            url,
            headers=headers,
            cookies={_cookie.name: str(_cookie.value)} if _cookie else None,  # type: ignore[arg-type]
            timeout=20,
        )
        resp.raise_for_status()

        html_text = resp.text

        # ------------------------------------------------------------------
        # Detect EU consent interstitial (guce.yahoo.com) and transparently
        # follow the *originalDoneUrl* if present.
        # ------------------------------------------------------------------
        if "id=\"consent-page\"" in html_text and "originalDoneUrl" in html_text:
            try:
                soup_consent = BeautifulSoup(html_text, "html.parser")
                inp = soup_consent.find("input", attrs={"name": "originalDoneUrl"})
                if inp is not None:
                    val = inp.get("value")  # type: ignore[attr-defined]
                    if val:
                        import html as _html

                        real_url = _html.unescape(str(val))
                        logger.debug("Detected consent page – retrying with original URL: %s", real_url)
                        # Single recursive retry
                        return get_press_release_content(real_url)
            except Exception as consent_exc:  # pragma: no cover
                logger.debug("Failed to bypass consent page for %s: %s", url, consent_exc)

        # ------------------------------------------------------------------
        # Extract the article body (best-effort): first try the <div class="atoms-wrapper">,
        # then fall back to caas-art JSON payload, finally return the full page.
        # ------------------------------------------------------------------
        try:
            soup = BeautifulSoup(html_text, "html.parser")

            atoms_div = soup.find("div", class_="atoms-wrapper")
            if atoms_div is not None:
                return "".join(str(child) for child in atoms_div.contents)  # type: ignore[attr-defined]

            script_tag = soup.find(
                "script",
                id=lambda v: isinstance(v, str) and v.startswith("caas-art-"),
                attrs={"type": "application/json"},
            )
            if script_tag is not None and getattr(script_tag, "string", None):  # type: ignore[attr-defined]
                import json as _json

                try:
                    data = _json.loads(script_tag.string)  # type: ignore[attr-defined]
                    if isinstance(data, dict):
                        content_html = data.get("content")
                        if isinstance(content_html, str) and content_html.strip():
                            logger.info("Extracted article body from caas-art JSON for %s", url)
                            return content_html
                except _json.JSONDecodeError as json_exc:  # pragma: no cover
                    logger.debug("Failed to decode caas-art JSON for %s: %s", url, json_exc)
        except Exception as parse_exc:  # pragma: no cover
            logger.debug("Failed to parse article body for %s: %s", url, parse_exc)

        return html_text  # Fallback – let caller handle further parsing
    except requests.RequestException as exc:
        logger.error("Failed to fetch press-release content from %s: %s", url, exc)
        return ""


def save_press_release_to_db(pr: PressRelease) -> int | None:
    """Insert *pr* into the **press_releases** table, returning its row id.

    The query uses an UPSERT so that re-runs update the existing row rather than
    inserting duplicates (identified by the UNIQUE *url* column).
    """

    with get_db_connection() as conn:
        cursor = conn.cursor()

        sql = (
            "INSERT INTO press_releases (company_ticker, title, url, type, pub_date, display_time, "
            "company_name, raw_html, text_content) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(url) DO UPDATE SET "
            "company_ticker = excluded.company_ticker, "
            "title          = excluded.title, "
            "type           = excluded.type, "
            "pub_date       = excluded.pub_date, "
            "display_time   = excluded.display_time, "
            "company_name   = excluded.company_name, "
            "raw_html       = excluded.raw_html, "
            "text_content   = excluded.text_content;"
        )

        params = (
            pr.ticker.upper(),
            pr.title,
            pr.url,
            pr.type,
            pr.pub_date,
            pr.display_time,
            pr.company_name,
            pr.raw_html,
            pr.text_content,
        )

        cursor.execute(sql, params)

        row_id: int | None
        if cursor.lastrowid != 0:
            row_id = cursor.lastrowid
        else:
            cursor.execute("SELECT id FROM press_releases WHERE url = ?;", (pr.url,))
            fetched = cursor.fetchone()
            row_id = int(fetched[0]) if fetched is not None else None

        conn.commit()
        return row_id

# ---------------------------------------------------------------------------
# Public API – save helpers (database)
# ---------------------------------------------------------------------------

def save_earnings_data_to_db(events: list[EarningsEvent], conn: sqlite3.Connection, *, max_retries: int = 3) -> List[int]:
    """Persist *events* to ``earnings_reports`` + ``companies`` tables.

    Returns the list of *ids* (primary keys) inserted / up-serted.
    """
    if not events:
        logger.info("No earnings events to save – nothing to do.")
        return []

    cursor = conn.cursor()
    written_ids: list[int] = []

    batch_size = 50
    for start in range(0, len(events), batch_size):
        batch = events[start : start + batch_size]
        try:
            conn.execute("BEGIN TRANSACTION")
            for ev in batch:
                try:
                    symbol = _validate_ticker(ev.ticker)
                    company_name = _validate_company_name(ev.company_name)
                    call_time = _validate_datetime(ev.earnings_call_time)
                    if not symbol or not company_name:
                        logger.debug("Skipping invalid row – symbol=%s, company=%s", ev.ticker, ev.company_name)
                        continue

                    cursor.execute(
                        "INSERT OR IGNORE INTO companies (ticker, company_name) VALUES (?, ?);",
                        (symbol, company_name),
                    )

                    sql = (
                        "INSERT INTO earnings_reports (company_ticker, report_datetime, event_name, time_type, "
                        "eps_estimate, reported_eps, surprise_percentage, market_cap) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(company_ticker, report_datetime) DO UPDATE SET "
                        "event_name=excluded.event_name, time_type=excluded.time_type, "
                        "eps_estimate=excluded.eps_estimate, reported_eps=excluded.reported_eps, "
                        "surprise_percentage=excluded.surprise_percentage, market_cap=excluded.market_cap, "
                        "updated_at=CURRENT_TIMESTAMP RETURNING id;"
                    )

                    params = (
                        symbol,
                        call_time,
                        _validate_string(ev.event_name),
                        _validate_string(ev.time_type),
                        _validate_numeric(ev.eps_estimate),
                        _validate_numeric(ev.eps_actual),
                        _validate_numeric(ev.eps_surprise_percent),
                        _validate_numeric(ev.market_cap),
                    )
                    row = cursor.execute(sql, params).fetchone()
                    if row is not None:
                        written_ids.append(int(row[0]))
                except Exception as row_exc:
                    logger.error("Failed to process earnings row: %s", row_exc)
            conn.commit()
        except sqlite3.Error as db_exc:
            conn.rollback()
            logger.error("Database error: %s – rolling back batch.", db_exc)
            if max_retries > 0:
                logger.debug("Retrying batch (%d retries remaining)", max_retries)
                time.sleep(0.1)
                written_ids.extend(save_earnings_data_to_db(batch, conn, max_retries=max_retries - 1))

    return written_ids

# ---------------------------------------------------------------------------
# Validation helpers (adapted from original class methods)
# ---------------------------------------------------------------------------

def _validate_ticker(ticker: str) -> str | None:
    if not ticker or len(ticker) > 10:
        return None
    cleaned = "".join(c for c in ticker if c.isalnum() or c in ".-")
    return cleaned.upper() if cleaned else None


def _validate_company_name(name: Any) -> str | None:
    if pd.isna(name) or not name:
        return None
    name_str = str(name).strip()
    return name_str if name_str and len(name_str) <= 200 else None


def _validate_datetime(dt: Any) -> str | None:
    if pd.isna(dt):
        return None
    try:
        if isinstance(dt, (pd.Timestamp, datetime)):
            return dt.isoformat()
        if isinstance(dt, str):
            parsed = pd.to_datetime(dt, utc=True, errors="coerce")
            if pd.notna(parsed):
                return parsed.isoformat()
    except Exception:  # pragma: no cover
        pass
    return None


def _validate_numeric(val: Any) -> float | None:
    if pd.isna(val):
        return None
    try:
        num = float(val)
        return None if abs(num) > 1e12 else num
    except (ValueError, TypeError):
        return None


def _validate_string(val: Any) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s and len(s) <= 500 else None
