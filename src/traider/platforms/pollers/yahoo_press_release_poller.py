# NOTE: typing.List used for type hints – the original import was incorrect.
from typing import List, Optional
import argparse
import asyncio
from typing import Any
from traider.platforms.pollers.common.base_poller import BasePoller, PollerConfig
from traider.platforms.yahoo.main import YahooFinance, PressRelease
from traider.platforms.cache import get_named_cache

import logging

logger = logging.getLogger(__name__)

class YahooPressReleasePoller(BasePoller):

    def __init__(self, tickers: list[str], interval: int = 1, *, use_db: bool = True) -> None:
        config = PollerConfig.from_env(
            "YPR",
            default_interval=interval
        )
        self.config = config
        self.tickers = tickers
        self.interval = interval if interval > 0 else config.polling_interval_seconds
        self.yf = YahooFinance()
        self.cache = get_named_cache("yahoo_press_release")

        # Database connection is optional – falls back to cache-only mode when disabled.
        self.use_db = use_db
        if self.use_db:
            try:
                from traider.db.database import get_db_connection, create_tables  # Local import to avoid cycles

                self._db_conn = get_db_connection()
                # Ensure tables exist (noop when already created)
                create_tables(self._db_conn)
            except Exception as db_exc:  # noqa: BLE001
                logger.exception("Failed to initialise database – continuing with cache-only mode: %s", db_exc)
                self.use_db = False
                self._db_conn = None  # type: ignore[attr-defined]

    def get_poller_name(self) -> str:
        return "yahoo_press_release"


    def run_polling_loop(self) -> None:  # type: ignore[override]
        """Synchronous wrapper that drives the asynchronous polling loop."""
        asyncio.run(self.async_polling_loop())

    async def async_polling_loop(self) -> None:

        # Continuously iterate over tickers, respecting the interval *between* individual requests
        while True:
            for ticker in self.tickers:
                try:
                    new_pr = await self.fetch_data(ticker)
                    if new_pr is not None:
                        # emit immediately – the helper expects a list
                        await self._emit_new_press_release(new_pr)
                    else:
                        print('.', end='\n', flush=True)
                except Exception as exc:  # noqa: BLE001
                    logger.exception("Unhandled error while processing %s: %s", ticker, exc)

                # Respect the interval between *each* HTTP request to Yahoo Finance
                await asyncio.sleep(self.interval)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def fetch_data(self, ticker: str) -> PressRelease | None: # type: ignore[override]
        """Fetch and deduplicate the most recent press-release for a single ticker."""

        try:
            pr: Optional[PressRelease] = self.yf.get_press_releases(ticker, type="press_release")
        except Exception as exc:  # noqa: BLE001 – network / parsing errors
            logger.warning("Failed to fetch press release for %s: %s", ticker, exc)
            return None

        if pr is None:
            return None

        cache_key = f"ypr:last_url:{ticker.upper()}"
        last_seen_url: str | None = self.cache.get(cache_key) if cache_key in self.cache else None  # type: ignore[arg-type]

        # Skip when identical to previously seen release
        if last_seen_url == pr.url:
            return None

        # Persist to DB (INSERT OR IGNORE) when enabled
        if self.use_db:
            try:
                row_id = self.yf.save_press_release_to_db(pr=pr)
                if row_id is None:
                    # Existing entry – already seen, skip emit & cache update
                    return None
            except Exception as db_exc:  # noqa: BLE001
                logger.error("Database error while saving press release for %s: %s", ticker, db_exc)

        # Update cache and return for emission
        self.cache[cache_key] = pr.url
        return pr

    async def _emit_new_press_release(self, new_pr: PressRelease) -> None:
        """Emit newly discovered press releases.

        Currently this implementation simply logs the releases; a sink callable
        can be attached via :py:meth:`set_sink` from :class:`BasePoller` to
        relay them to downstream systems (e.g. message queue, websocket).
        """

        logger.debug("[NEW] %s | %s", new_pr.ticker.upper(), new_pr.title)

        if self._sink is not None:
            try:
                self._sink(self.get_poller_name(), new_pr)  # type: ignore[arg-type]
            except Exception as sink_exc:  # noqa: BLE001
                logger.exception("Sink emitted an error for %s: %s", new_pr.url, sink_exc)

    def parse_items(self, data: Any) -> List[Any]: # type: ignore[override]
        raise NotImplementedError("YahooPressReleasePoller does not support parsing items")
    
    def extract_article_text(self, item: Any) -> str | None: # type: ignore[override]
        raise NotImplementedError("YahooPressReleasePoller does not support extracting article text")


def run_poller(tickers: list[str]):

   poller = YahooPressReleasePoller(tickers=tickers)
   poller.run()

# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

def main():
    """Main CLI entry point for the Yahoo Press Release Poller."""
    parser = argparse.ArgumentParser(
        description="Poll Yahoo Finance for press releases of specified tickers."
    )
    parser.add_argument(
        "tickers",
        nargs="+",
        help="List of stock tickers to poll for press releases (e.g., AAPL MSFT TSLA)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="Polling interval in seconds (default: 1)"
    )

    args = parser.parse_args()
    run_poller(tickers=args.tickers)

if __name__ == "__main__":  # pragma: no cover – manual usage
    main()