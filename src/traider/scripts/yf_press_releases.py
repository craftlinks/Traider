from __future__ import annotations
from ast import List
import asyncio

"""Quick manual test to verify YahooFinance.get_press_releases().

Run:
    python -m traider.scripts.test_press_releases
"""

import argparse

from dataclasses import asdict
import logging
from pathlib import Path

import traider.yfinance as yf

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch latest Yahoo Finance press release for a ticker."
    )
    parser.add_argument(
        "ticker", nargs="?", default="SMTC", help="Stock ticker symbol (default: SMTC)"
    )

    args = parser.parse_args()

    ticker = args.ticker.upper()
    pr: yf.PressRelease | None = await yf.get_latest_press_release(ticker)

    if pr is None:
        logger.debug("No press release returned for %s", ticker)
    else:
        # Pretty-print dataclass as dict
        logger.info("Latest press release for %s:\n%s", ticker, asdict(pr))

        # --- Fetch the article body ----------------------------------------------------
        html_body: str = await yf.get_press_release_content(pr.url)

        snippet_len = 500  # show only a small preview in the console
        preview = (
            (html_body[:snippet_len] + "…")
            if len(html_body) > snippet_len
            else html_body
        )
        logger.info("Article HTML preview (%d chars):\n%s", len(html_body), preview)

        # --- Persist full HTML to disk -------------------------------------------------
        out_path = f"{ticker}_press_release.html"
        try:
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html_body)
            logger.info("Full HTML saved to %s", out_path)
        except Exception as io_exc:
            logger.error("Failed to save HTML to %s: %s", out_path, io_exc)


if __name__ == "__main__":
    asyncio.run(main())
