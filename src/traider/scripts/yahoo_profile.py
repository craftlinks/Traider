from __future__ import annotations

"""Fetch Yahoo Finance *profile* data (website URL, sector, industry) for
all tickers currently stored in the local database.

The script performs the following steps for **every** ticker in the
``companies`` table:

1. Download ``https://finance.yahoo.com/quote/<TICKER>/profile``.
2. Parse the HTML with *BeautifulSoup* to extract the company website URL as
   well as the *Sector* and *Industry* fields.
3. Store the website URL in the ``urls`` table via :pyfunc:`traider.db.data_manager.add_url`.
4. Update the ``sector`` and ``industry`` columns of the corresponding
   record in the ``companies`` table (only if we actually retrieved a value).

The script can be executed directly (``python -m traider.scripts.yahoo_profile``)
**or** imported and the :pyfunc:`update_all_company_profiles` function invoked
from elsewhere.

Note
----
Yahoo Finance changes its HTML structure from time to time.  If parsing
starts to fail, inspect the affected profile page in a browser and adjust the
CSS selectors / parsing logic accordingly.
"""

import logging
import time
from pathlib import Path
from typing import Final, Tuple, Optional

import requests
from bs4 import BeautifulSoup  # type: ignore[attr-defined]

from traider.db.database import get_db_connection, create_tables
from traider.db.data_manager import list_companies, add_url
# Reuse Yahoo cookie / crumb retrieval from earnings calendar module
from traider.platforms.pollers.yahoo_earnings_calendar import _fetch_cookie_and_crumb  # type: ignore

# ---------------------------------------------------------------------------
# Configuration & constants
# ---------------------------------------------------------------------------

_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

# Include ?p=<ticker> query param – Yahoo sometimes returns 404 without it
_YF_PROFILE_TEMPLATE: Final[str] = "https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
# Public JSON endpoint that usually contains the exact same information we need
# and is less likely to change than the HTML.  No crumb token required.
_YF_PROFILE_JSON_TEMPLATE: Final[str] = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile&lang=en-US&region=US"
)

#    Courtesy delay between requests (seconds).  Be nice to Yahoo.
_REQUEST_DELAY_S: Final[float] = 1.0

# Set up module-level logger
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# HTML parsing helpers
# ---------------------------------------------------------------------------


def _extract_profile_data(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse the Yahoo profile HTML and return (website_url, sector, industry).

    All values may be *None* if the corresponding element could not be
    located.
    """

    soup = BeautifulSoup(html, "html.parser")

    # --- Website URL --------------------------------------------------------
    website_url: Optional[str] = None
    website_tag = soup.select_one("a[data-ylk*='business-url']")
    if website_tag is not None:
        # The visible text already contains the fully-qualified URL
        website_url = website_tag.get_text(strip=True)
        # Occasionally the anchor text contains an ellipsis while the full URL
        # is stored inside the *href*.  Prefer *href* in that case.
        href_val = website_tag.get("href")
        if isinstance(href_val, str) and href_val.startswith("http"):
            website_url = href_val.strip()

    # --- Sector -------------------------------------------------------------
    sector: Optional[str] = None
    dt_sector = soup.find("dt", string=lambda s: isinstance(s, str) and "Sector" in s)
    if dt_sector is not None:
        sector_anchor = dt_sector.find_next("a")
        if sector_anchor is not None:
            sector = sector_anchor.get_text(strip=True)

    # --- Industry -----------------------------------------------------------
    industry: Optional[str] = None
    dt_industry = soup.find("dt", string=lambda s: isinstance(s, str) and "Industry" in s)
    if dt_industry is not None:
        industry_anchor = dt_industry.find_next("a")
        if industry_anchor is not None:
            industry = industry_anchor.get_text(strip=True)

    return website_url, sector, industry


# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------


def _prepare_session() -> requests.Session:
    """Return a *requests* Session pre-populated with Yahoo cookie and headers."""

    sess = requests.Session()
    sess.headers.update({"User-Agent": _USER_AGENT, "Accept-Language": "en-US,en;q=0.9"})

    cookie, _crumb = _fetch_cookie_and_crumb(sess)
    if cookie is not None:
        # Attach cookie to *all* subsequent requests via the session cookie jar
        sess.cookies.set(cookie.name, str(cookie.value))  # type: ignore[arg-type]
    return sess


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def update_company_profile(ticker: str) -> bool:  # noqa: D401 – imperative mood preferred
    """Fetch & persist profile information for *ticker*.

    Returns *True* if any data was successfully written to the DB, *False*
    otherwise.
    """

    session = _prepare_session()

    url = _YF_PROFILE_TEMPLATE.format(ticker=ticker)

    # First attempt: simple HTML scrape -------------------------------------------------
    try:
        response = session.get(url, timeout=20)
        response.raise_for_status()
        website, sector, industry = _extract_profile_data(response.text)
    except requests.RequestException as exc:
        logger.info("HTML fetch failed for %s (%s). Trying JSON endpoint…", ticker, exc)
        website = sector = industry = None

    # Fallback: public quoteSummary assetProfile JSON -----------------------
    if not any([website, sector, industry]):
        json_url = _YF_PROFILE_JSON_TEMPLATE.format(ticker=ticker)
        try:
            json_resp = session.get(json_url, timeout=20)
            json_resp.raise_for_status()
            data = json_resp.json()

            profile = (
                data.get("quoteSummary", {})
                .get("result", [{}])[0]  # type: ignore[index]
                .get("assetProfile", {})
            )
            website = profile.get("website") or website
            sector = profile.get("sector") or sector
            industry = profile.get("industry") or industry
        except Exception as exc:  # noqa: BLE001 – broad OK here, will log below
            logger.warning("JSON endpoint failed for %s: %s", ticker, exc)

    if not any([website, sector, industry]):
        logger.info("No profile data found for %s", ticker)
        return False

    wrote_anything = False

    # 1.  Insert / update website URL
    if website:
        try:
            add_url(company_ticker=ticker, url_type="website", url=website)
            wrote_anything = True
        except Exception:  # noqa: BLE001 – error already logged in helper
            pass  # keep going – maybe the sector/industry update works

    # 2.  Update sector / industry in *companies*
    if sector or industry:
        with get_db_connection() as conn:
            try:
                conn.execute(
                    """
                    UPDATE companies
                    SET sector   = COALESCE(?, sector),
                        industry = COALESCE(?, industry)
                    WHERE ticker = ?
                    """,
                    (sector, industry, ticker.upper()),
                )
                conn.commit()
                wrote_anything = True
            except Exception as exc:  # noqa: BLE001
                logger.exception("DB error while updating company %s: %s", ticker, exc)

    return wrote_anything


def update_all_company_profiles(
    *,
    delay_between: float | None = None,
    start_from: str | None = None,
) -> None:  # noqa: D401
    """Iterate over all tickers in the DB and refresh their Yahoo profile data.

    Parameters
    ----------
    delay_between:
        Seconds to wait between successive requests.  When *None*, the module‐
        level constant ``_REQUEST_DELAY_S`` is used.
    start_from:
        Resume the import starting *with* this ticker symbol (inclusive).
        Comparison is case-insensitive.  When *None*, start from the very first
        company in alphabetical order.
    """

    delay_s = _REQUEST_DELAY_S if delay_between is None else max(0.0, delay_between)

    create_tables()  # ensure schema exists
    companies = list_companies()
    if not companies:
        logger.info("No companies found in DB – nothing to update.")
        return

    # If resume ticker provided, skip until we reach it
    if start_from is not None:
        start_from_upper = start_from.upper()
        try:
            start_index = next(
                i
                for i, c in enumerate(companies)
                if str(c["ticker"]).upper() >= start_from_upper
            )
        except StopIteration:
            logger.error("Start ticker %s not found in DB. Aborting.", start_from_upper)
            return
        companies = companies[start_index:]
        logger.info(
            "Resuming import at ticker %s (index %d).", start_from_upper, start_index + 1
        )

    total = len(companies)
    logger.info("Updating Yahoo profiles for %d tickers…", total)

    updated = 0
    for idx, company in enumerate(companies, start=1):
        ticker = company["ticker"]
        if not isinstance(ticker, str):
            continue

        success = update_company_profile(ticker)
        if success:
            updated += 1

        # Courtesy delay between requests
        time.sleep(delay_s)

        # Progress feedback every 100 tickers or on completion
        if idx % 100 == 0 or idx == total:
            pct = (idx / total) * 100
            logger.info(
                "Progress: %d/%d (%.1f%%) processed — %d updated",
                idx,
                total,
                pct,
                updated,
            )

    logger.info("Completed profile refresh. Successfully updated %d of %d tickers.", updated, total)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


if __name__ == "__main__":  # pragma: no cover – manual usage
    import argparse

    _configure_logging()

    parser = argparse.ArgumentParser(description="Fetch Yahoo Finance profile data and store it in the DB.")
    parser.add_argument(
        "tickers",
        metavar="T",
        nargs="*",
        help="One or more ticker symbols to update. If omitted, update *all* tickers in the DB.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=_REQUEST_DELAY_S,
        help=f"Seconds to wait between requests (default: {_REQUEST_DELAY_S}).",
    )
    parser.add_argument(
        "--resume-from",
        metavar="TICKER",
        help="Resume import starting from this ticker (inclusive). Ignored when explicit tickers are supplied.",
    )

    args = parser.parse_args()

    delay_between = max(0.0, args.delay)

    if args.tickers:
        create_tables()
        for idx, ticker in enumerate(args.tickers, start=1):
            update_company_profile(ticker)
            if idx < len(args.tickers):
                time.sleep(delay_between)
    else:
        update_all_company_profiles(delay_between=delay_between, start_from=args.resume_from)

