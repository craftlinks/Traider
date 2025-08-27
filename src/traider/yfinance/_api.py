from __future__ import annotations

"""Yahoo Finance scraping utilities."""

from datetime import date, timedelta
import logging
import time
from typing import List
from urllib.parse import quote_plus
import asyncio

import pandas as pd
import httpx
from bs4 import BeautifulSoup

from ._constants import _USER_AGENT, _YF_PROFILE_TEMPLATE, _YF_PROFILE_JSON_TEMPLATE, YF_VISUALIZATION_API, _REQUEST_DELAY_S
from ._helpers import _refresh_cookie_and_crumb, _get_session, _extract_profile_data_json, _extract_profile_data_html, _extract_earnings_data_json, _df_to_events, _initialize_session, _crumb, _cookie
from ._models import Profile, EarningsEvent, PressRelease

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


# ---------------------------------------------------------------------------
# Initialisation & session access helpers
# ---------------------------------------------------------------------------

async def initialize() -> None:
    await _initialize_session()


# ---------------------------------------------------------------------------
# Public API – profile
# ---------------------------------------------------------------------------


async def get_profile(ticker: str, *, from_json: bool = False) -> Profile:
    """Return (website, sector, industry) for *ticker*.

    Two independent Yahoo endpoints are supported:
    1.  HTML profile page (requires full parse – brittle).
    2.  JSON quoteSummary API (cleaner, but occasionally rate-limited).
    """
    url = (_YF_PROFILE_JSON_TEMPLATE if from_json else _YF_PROFILE_TEMPLATE).format(ticker=ticker)

    max_attempts = 2
    for attempt in range(max_attempts):
        try:
            session = await _get_session()
            resp = await session.get(url, timeout=20)
            resp.raise_for_status()

            if from_json:
                website, sector, industry = _extract_profile_data_json(resp.json())
            else:
                website, sector, industry = _extract_profile_data_html(resp.text)
            return Profile(website, sector, industry)
        except httpx.RequestError as exc:
            if attempt < max_attempts - 1:
                logger.debug("Failed to fetch profile for %s (%d/%d): %s – retrying …", ticker, attempt + 1, max_attempts, exc)
                try:
                    await _refresh_cookie_and_crumb()
                except RuntimeError:
                    break
            else:
                logger.error("Failed to fetch profile for %s: %s", ticker, exc)
    return Profile(None, None, None)

# ---------------------------------------------------------------------------
# Public API – earnings
# ---------------------------------------------------------------------------

async def get_earnings(start_date: date, *, as_dataframe: bool = True, max_retries: int = 3) -> pd.DataFrame | List[EarningsEvent]:
    date_str = start_date.strftime("%Y-%m-%d")
    logger.debug("Fetching earnings for %s", date_str)

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                await asyncio.sleep(2 ** attempt)
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
            session = await _get_session()
            resp = await session.post(
                api_url,
                json=payload,
                timeout=30,
                headers={"x-crumb": _crumb or "", "User-Agent": _USER_AGENT},
                cookies={_cookie.name: str(_cookie.value)} if _cookie else None,  # type: ignore[arg-type]
            )
            resp.raise_for_status()
            df = _extract_earnings_data_json(resp.json())
            return df if as_dataframe else _df_to_events(df)
        except httpx.RequestError as exc:
            logger.error("Network error contacting Yahoo (%d/%d): %s", attempt + 1, max_retries, exc)
        except Exception as exc:  # pragma: no cover – parse errors, etc.
            logger.error("Unhandled error parsing Yahoo response (%d/%d): %s", attempt + 1, max_retries, exc)

        if attempt < max_retries:
            try:
                await _refresh_cookie_and_crumb()
            except RuntimeError as refresh_exc:
                logger.error("Failed to refresh Yahoo crumb – aborting: %s", refresh_exc)
                break

    return pd.DataFrame() if as_dataframe else []


# Convenience wrapper for multiple days ---------------------------------------------------

async def get_earnings_for_date_range(start_date: date, end_date: date, *, as_dataframe: bool = False) -> pd.DataFrame | List[EarningsEvent]:
    date_range = [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

    dfs: list[pd.DataFrame] = []
    events: list[EarningsEvent] = []

    for day in date_range:
        res = await get_earnings(day, as_dataframe=as_dataframe)
        if as_dataframe and isinstance(res, pd.DataFrame) and not res.empty:
            dfs.append(res)
        elif not as_dataframe and isinstance(res, list):
            events.extend(res)
        await asyncio.sleep(_REQUEST_DELAY_S)

    return (pd.concat(dfs, ignore_index=True) if as_dataframe else events)  # type: ignore[return-value,arg-type]

# ---------------------------------------------------------------------------
# Public API – press releases
# ---------------------------------------------------------------------------

async def get_press_releases(ticker: str, *, type: str, limit: int = 250) -> list[PressRelease]:  # noqa: A002 – type parameter is Yahoo nomenclature
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
            session = await _get_session()
            resp = await session.post(
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
        except httpx.RequestError as exc:
            logger.debug("Failed to fetch press releases for %s (%d): %s", ticker, attempt + 1, exc)
            if attempt == 0:
                try:
                    await _refresh_cookie_and_crumb()
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


async def get_press_release_content(url: str) -> str:
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
        session = await _get_session()
        resp = await session.get(
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
                        return await get_press_release_content(real_url)
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
    except httpx.RequestError as exc:
        logger.error("Failed to fetch press-release content from %s: %s", url, exc)
        return ""
