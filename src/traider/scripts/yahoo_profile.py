from __future__ import annotations

from traider.platforms.yahoo.main import YahooFinance

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
from typing import Final, Optional

from traider.db.database import get_db_connection, create_tables
from traider.db.data_manager import list_companies, add_url
#    Courtesy delay between requests (seconds).  Be nice to Yahoo.
_REQUEST_DELAY_S: Final[float] = 1.0

# Set up module-level logger
logger = logging.getLogger(__name__)



# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def update_company_profile(
    ticker: str,
    yf: Optional[YahooFinance] = None,
) -> bool:  # noqa: D401 – imperative mood preferred

    if yf is None:
        yf = YahooFinance()

    wrote_anything = False

    # 1.  Insert / update website URL
    profile = yf.get_profile(ticker, from_json=True)
    if profile.get("website_url"):
        add_url(company_ticker=ticker, url_type="website", url=profile["website_url"])
        wrote_anything = True

    # 2.  Update sector / industry in *companies*
    sector = profile.get("sector")
    industry = profile.get("industry")
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
    yf: Optional[YahooFinance] = None,
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

        success = update_company_profile(ticker, yf=yf)
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

    yf = YahooFinance()

    if args.tickers:
        create_tables()
        for idx, ticker in enumerate(args.tickers, start=1):
            update_company_profile(ticker, yf=yf)
            if idx < len(args.tickers):
                time.sleep(delay_between)
    else:
        update_all_company_profiles(delay_between=delay_between, start_from=args.resume_from, yf=yf)

