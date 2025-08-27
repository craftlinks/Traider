from __future__ import annotations
from datetime import datetime
import math

# from traider.platforms.cache import get_shared_cache
from traider.platforms.cache import CacheInterface, get_named_cache
from traider.pollers.common.base_web_poller import PollerConfig
from traider.pollers.common.protocol import Poller

"""Utility for fetching Yahoo Finance earnings calendar data in an *authenticated* way
that mimics the website's internal calls.

The implementation follows these steps – roughly equivalent to what a modern
browser does when you load https://finance.yahoo.com/calendar/earnings :

1. Request the calendar HTML page to obtain the *crumb* anti-CSRF token that is
   embedded inside a script tag. The request also seeds the session with the
   correct cookies (notably the "A1" auth cookie) required by subsequent API
   calls.
2. Build the JSON payload understood by Yahoo's private *visualization* API and
   perform a POST request against
   https://query1.finance.yahoo.com/v1/finance/visualization while passing the
   crumb as query parameter. This returns a nested JSON document containing the
   earnings calendar rows together with column metadata.
3. Convert the response into a `pandas.DataFrame` with user-friendly column
   names and types.

Note
----
Yahoo does *not* provide a public REST API for this data. Their internal API
might change without notice. The code tries to fail loudly in case the
structure of the HTML or JSON payload changes.
"""

import logging
from datetime import date
from typing import Any, Callable, List
import asyncio
from typing import cast
import traider.yfinance as yf
from traider.db.database import get_db_connection, create_tables

# Set up logging
logger = logging.getLogger(__name__)

__all__ = ["YahooEarningsPoller"]

class YahooEarningsPoller(Poller[yf.EarningsEvent]):
   """Yahoo Earnings Calendar poller."""
   def __init__(self, date: date = date.today(), interval: int = 60) -> None:
      config = PollerConfig.from_env(
         "YEC",
         default_interval=interval
      )
      self.config = config
      self.today = date
      self.db_conn = get_db_connection()
      self.cache: CacheInterface = get_named_cache("yahoo_earnings_calendar")
      self._sink: Callable[[str, yf.EarningsEvent], Any] | None = None
      self.interval = interval if interval > 0 else config.polling_interval_seconds

      logger.info("Initializing database…")
      create_tables()

   @property
   def name(self) -> str:
      return "yahoo_earnings_calendar"

   def set_sink(self, sink: Callable[[str, yf.EarningsEvent], Any]) -> None:
      self._sink = sink

   def run(self) -> None:
      """Starts the asynchronous polling loop."""
      try:
         asyncio.run(self._async_polling_loop())
      except KeyboardInterrupt:
         logger.info("YahooEarningsPoller shutting down.")
      finally:
         self.db_conn.close()

   async def _fetch_data(self, persist_db: bool = True) -> list[yf.EarningsEvent]:  # type: ignore[override]
      """Asynchronously fetch earnings data for today.

      The underlying `YahooFinance.get_earnings` call is synchronous (uses
      `requests`).  To avoid blocking the event-loop, it is executed in a
      thread-pool via `asyncio.to_thread`.
      """
      ee: List[yf.EarningsEvent] = cast(List[yf.EarningsEvent], await asyncio.to_thread(yf.get_earnings, self.today, as_dataframe=False))
      if not ee:
         logger.info("No earnings data fetched for today")
      return ee

   async def save_earnings_data_to_db(self, ee: list[yf.EarningsEvent]) -> list[int | None]:
      return [earning.to_db(conn=self.db_conn) for earning in ee]

   async def _async_polling_loop(self) -> None:
      while True:
         ee: list[yf.EarningsEvent] = await self._fetch_data()

         # filter out earnings events for which we have an estimated earnings per share
         ee = [earning for earning in ee if earning.eps_estimate is not None and not math.isnan(earning.eps_estimate)]

         try:
            await self.save_earnings_data_to_db(ee)
            await self._emit_new_earnings(ee)

         except Exception as e:
            logger.error(f"Failed to save earnings data to database: {e}")
            raise

         await asyncio.sleep(self.interval)

   async def _filter_and_persist(self, events: list[yf.EarningsEvent]) -> list[yf.EarningsEvent]:
        """Persist events to DB and return only the ones that are new."""
        newly_persisted_events = []
        for event in events:
            # Use cache to quickly check if we've seen this event's unique identifier
            # (Assuming EarningsEvent has a unique `id` or you create one)
            event_id = event.id # e.g., f"{event.ticker}-{event.event_date}"
            if event_id not in self.cache:
                await asyncio.to_thread(event.to_db, conn=self.db_conn)
                self.cache.add(str(event_id))
                newly_persisted_events.append(event)
        return newly_persisted_events
        
   async def _emit_new_earnings(self, new_events: list[yf.EarningsEvent]) -> None:
        """Emit newly found earnings events to the registered sink."""
        if not self._sink:
            return
            
        logger.debug(f"Emitting {len(new_events)} events to sink...")
        for event in new_events:
            try:
                # The sink can be a regular sync function; running it in a thread
                # prevents it from blocking the async loop.
                await asyncio.to_thread(self._sink, self.name, event)
            except Exception:
                logger.exception("[SINK] Error while emitting item")


# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual usage
   poller = YahooEarningsPoller()
   poller.run()

        



