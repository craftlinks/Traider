from __future__ import annotations
import asyncio
import logging
import math
from datetime import date
from typing import Any, Callable, Awaitable, Union

import traider.yfinance as yf
from traider.db.database import get_db_connection, create_tables
from traider.platforms.cache import CacheInterface, get_named_cache
from traider.pollers.common.base_web_poller import PollerConfig
from traider.pollers.common.protocol import Poller

# Set up logging
logger = logging.getLogger(__name__)

__all__ = ["YahooEarningsPoller"]

# Define a type for a sink, which can be sync or async
SinkCallable = Union[
    Callable[[str, yf.EarningsEvent], Any], 
    Callable[[str, yf.EarningsEvent], Awaitable[Any]]
]


class YahooEarningsPoller(Poller[yf.EarningsEvent]):
    """
    An async-native poller for Yahoo Finance earnings announcements.
    
    This component is designed to run as a task within a larger asyncio event loop.
    It manages its own database connection and can be gracefully shut down.
    
    Usage:
        >>> poller = await YahooEarningsPoller.create()
        >>> poller.set_sink(my_sink_function)
        >>> await poller.run()
    """

    # Make __init__ private to enforce creation via the async factory
    def __init__(self, config: PollerConfig, db_conn: Any, cache: CacheInterface, poll_date: date, sink: SinkCallable | None = None):
        self.config = config
        self.db_conn = db_conn
        self.cache = cache
        self.poll_date = poll_date
        self._sink: SinkCallable | None = None
        self._shutdown_event = asyncio.Event()

    @classmethod
    async def create(cls, poll_date: date = date.today(), interval: int = 60, sink: SinkCallable | None = None) -> "YahooEarningsPoller":
        """
        Asynchronously create and initialize a YahooEarningsPoller instance.
        This is the preferred way to instantiate the class.
        """
        config = PollerConfig.from_env("YEC", default_interval=interval)
        
        # Run blocking DB setup in a thread to avoid blocking the event loop
        db_conn = await asyncio.to_thread(get_db_connection)
        await asyncio.to_thread(create_tables)
        
        cache = get_named_cache("yahoo_earnings_calendar")
        
        logger.info("YahooEarningsPoller initialized.")
        return cls(config, db_conn, cache, poll_date, sink)

    @property
    def name(self) -> str:
        return "yahoo_earnings_calendar"

    def set_sink(self, sink: SinkCallable) -> None:
        self._sink = sink

    async def _fetch_data(self) -> list[yf.EarningsEvent]:
        """
        Asynchronously fetch earnings data for the configured date.
        """
        try:
            events = await yf.get_earnings(self.poll_date)
            return events or []
        except Exception:
            logger.exception("Failed to fetch earnings data from Yahoo Finance.")
            return []

    async def _filter_and_persist_new(self, events: list[yf.EarningsEvent]) -> list[yf.EarningsEvent]:
        """
        Filters for new events, persists them to the DB, and returns them.
        This runs the synchronous DB operations in a thread.
        """
        new_events = []
        # Check cache first (fast in-memory check)
        events_to_check = [e for e in events if e.id and str(e.id) not in self.cache]

        if not events_to_check:
            return []

        def db_worker():
            persisted = []
            for event in events_to_check:
                # The to_db method handles the logic of INSERT OR IGNORE
                # and returns the ID if it was a new insert.
                new_id = event.to_db(conn=self.db_conn)
                if new_id is not None:
                    persisted.append(event)
            return persisted
        
        try:
            new_events = await asyncio.to_thread(db_worker)
            # Update cache with newly persisted items
            for event in new_events:
                self.cache.add(str(event.id))
            return new_events
        except Exception:
            logger.exception("Failed to persist new earnings data to the database.")
            return []

    async def _emit(self, new_events: list[yf.EarningsEvent]):
        """Emit newly found earnings events to the registered sink."""
        if not self._sink or not new_events:
            return

        logger.debug(f"Emitting {len(new_events)} new events to sink...")
        
        # Use a TaskGroup to emit concurrently, respecting async sinks
        async with asyncio.TaskGroup() as tg:
            for event in new_events:
                tg.create_task(self._safe_call_sink(event))

    async def _safe_call_sink(self, event: yf.EarningsEvent):
        """Safely call the sink, handling both sync and async functions."""
        if not self._sink:
            return
        try:
            if asyncio.iscoroutinefunction(self._sink):
                await self._sink(self.name, event)
            else:
                # Run synchronous sink in a thread to prevent blocking
                await asyncio.to_thread(self._sink, self.name, event)
        except Exception:
            logger.exception("[SINK] Error while emitting item: %s", event)

    async def run(self) -> None:
        """
        The main async execution loop for the poller.
        This coroutine runs until shutdown() is called.
        """
        logger.info(f"Starting poller '{self.name}'...")
        try:
            while not self._shutdown_event.is_set():
                logger.debug("Polling for earnings data...")
                
                # 1. Fetch
                all_events = await self._fetch_data()

                # 2. Filter out events without necessary data
                valid_events = [
                    e for e in all_events 
                    if e.eps_estimate is not None and not math.isnan(e.eps_estimate)
                ]

                # 3. Persist new events and get them back
                if valid_events:
                    newly_persisted = await self._filter_and_persist_new(valid_events)
                    if newly_persisted:
                        logger.info(f"Found {len(newly_persisted)} new earnings events.")
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
            if self.db_conn:
                await asyncio.to_thread(self.db_conn.close)

    def shutdown(self) -> None:
        """Signals the poller to gracefully shut down."""
        logger.info(f"Shutdown signal received for poller '{self.name}'.")
        self._shutdown_event.set()


# ---------------------------------------------------------------------------
# Example CLI usage demonstrating the new async pattern
# ---------------------------------------------------------------------------

async def main():
    """Example of how to run the poller."""
    
    # Example sink function (can be sync or async)
    def my_simple_sink(poller_name: str, event: yf.EarningsEvent):
        print(f"[{poller_name.upper()}] New Earning: {event.ticker} | EPS Est: {event.eps_estimate}")

    poller = None
    poller_task = None
    try:
        poller = await YahooEarningsPoller.create(interval=15)
        poller.set_sink(my_simple_sink)
        
        # Run the poller as a task
        poller_task = asyncio.create_task(poller.run())
        
        # In a real app, you might await other tasks or signals here.
        # For this example, we'll just let it run for a minute.
        print("Poller is running. Press Ctrl+C to stop.")
        await asyncio.sleep(60)

    except asyncio.CancelledError:
        print("Main task cancelled.")
    finally:
        if poller:
            poller.shutdown()
        # Wait for the poller task to finish its cleanup
        if poller_task and not poller_task.done():
            await poller_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting.")