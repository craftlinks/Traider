# NOTE: typing.List used for type hints – the original import was incorrect.
from typing import Callable, List, Optional
import argparse
import asyncio
from typing import Any
from traider.pollers.common.base_web_poller import PollerConfig
from traider.pollers.common.protocol import Poller
from traider.scripts.yahoo_profile import yf
from traider.yfinance import PressRelease
from traider.platforms.cache import get_named_cache

import logging

logger = logging.getLogger(__name__)

class YahooPressReleasePoller(Poller[PressRelease]):

    def __init__(self, tickers: list[str], interval: int = 1, *, use_db: bool = True) -> None:
        config = PollerConfig.from_env(
            "YPR",
            default_interval=interval
        )
        self.config = config
        self.tickers = tickers
        self.interval = interval if interval > 0 else config.polling_interval_seconds
        self.cache = get_named_cache("yahoo_press_release")
        self._sink: Callable[[str, PressRelease], Any] | None = None
        self._shutdown_event = asyncio.Event()

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

    @property
    def name(self) -> str:
        return "yahoo_press_release"

    def set_sink(self, sink: Callable[[str, PressRelease], Any]) -> None:
        self._sink = sink

    async def run(self) -> None:
        """
        The main async execution loop for the poller.
        This coroutine runs until shutdown() is called.
        """
        logger.info(f"Starting poller '{self.name}'...")
        try:
            while not self._shutdown_event.is_set():
                logger.debug("Polling for press release data...")
                
                # 1. Fetch
                all_press_releases = await self._fetch_data()

                # 2. Filter out events without necessary data
                valid_press_releases = [
                    pr for pr in all_press_releases 
                    if pr.url is not None
                ]

                # 3. Persist new events and get them back
                if valid_press_releases:
                    newly_persisted = await self._filter_and_persist_new(valid_press_releases)
                    if newly_persisted:
                        logger.info(f"Found {len(newly_persisted)} new press releases.")
                        # 4. Emit to sink
                        await self._emit(newly_persisted)
                
                # 5. Wait for the next interval or shutdown signal
                try:
                    await asyncio.wait_for(
                        self._shutdown_event.wait(), 
                        timeout=self.config.polling_interval_seconds
                    )
                except asyncio.TimeoutError:
                    continue  # Timeout occurred, loop again
        
        except asyncio.CancelledError:
            logger.info(f"Poller '{self.name}' run task was cancelled.")
        finally:
            logger.info(f"Shutting down poller '{self.name}'.")
            if self._db_conn:
                await asyncio.to_thread(self._db_conn.close)

    def shutdown(self) -> None:
        """Signals the poller to gracefully shut down."""
        logger.info(f"Shutdown signal received for poller '{self.name}'.")
        self._shutdown_event.set()


    async def _fetch_data(self) -> list[PressRelease]:
        """Fetch press release data for the configured tickers."""
        return await yf.get_press_releases(self.tickers, type="press_release")

    async def _filter_and_persist_new(self, press_releases: list[PressRelease]) -> list[PressRelease]:
        """Filter and persist new press releases."""
        new_press_releases = []
        for pr in press_releases:
            last_seen_url: str | None = self.cache.get(f"ypr:last_url:{pr.ticker.upper()}")

            # Skip when identical to previously seen release
            if last_seen_url == pr.url:
                continue

            new_press_releases.append(pr)

        if not new_press_releases:
            return []

        # Persist to DB (INSERT OR IGNORE) when enabled
        if self.use_db:
            try:
                row_id = await yf.save_press_release_to_db(pr=new_press_releases)
                if row_id is None:
                    # Existing entry – already seen, skip emit & cache update
                    return []
            except Exception as db_exc:  # noqa: BLE001
                logger.error("Database error while saving press release for %s: %s", new_press_releases[0].ticker, db_exc)
                return []

        # Update cache and return for emission
        for pr in new_press_releases:
            self.cache[f"ypr:last_url:{pr.ticker.upper()}"] = pr.url
        return new_press_releases

    async def _emit(self, new_press_releases: list[PressRelease]) -> None:
        """Emit newly discovered press releases.

        Currently this implementation simply logs the releases; a sink callable
        can be attached via :py:meth:`set_sink` from :class:`BasePoller` to
        relay them to downstream systems (e.g. message queue, websocket).
        """

        logger.debug("[NEW] %s | %s", new_press_releases[0].ticker.upper(), new_press_releases[0].title)

        if self._sink is not None:
            try:
                self._sink(self.name, new_press_releases)  # type: ignore[arg-type]
            except Exception as sink_exc:  # noqa: BLE001
                logger.exception("Sink emitted an error for %s: %s", new_press_releases[0].url, sink_exc)


async def run_poller(tickers: list[str]):
    poller = YahooPressReleasePoller(tickers=tickers)
    await poller.run()

# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

async def main():
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
    await run_poller(tickers=args.tickers)

if __name__ == "__main__":  # pragma: no cover – manual usage
    asyncio.run(main())