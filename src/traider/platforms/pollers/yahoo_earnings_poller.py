from __future__ import annotations
from datetime import datetime

from traider.platforms.cache import get_shared_cache
from traider.platforms.pollers.common.base_poller import BaseItem, BasePoller, PollerConfig

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
from typing import Any, Dict, List
from requests import Response
import asyncio
from typing import cast

import pandas as pd  # type: ignore  # runtime dependency
from traider.platforms.yahoo.main import EarningsEvent, YahooFinance
from traider.db.database import get_db_connection, create_tables

# Set up logging
logger = logging.getLogger(__name__)

__all__ = ["run_poller"]

class YahooEarningsPoller(BasePoller):
   """Yahoo Earnings Calendar poller."""
   def __init__(self, date: date = date.today(), interval: int = 60) -> None:
      config = PollerConfig.from_env(
         "YEC",
         default_interval=interval
      )
      self.config = config
      logger.info("Initializing database…")
      create_tables()
      self.today = date
      self.yf = YahooFinance()
      self.db_conn = get_db_connection()
      self.cache = get_shared_cache()
      self.interval = interval if interval > 0 else config.polling_interval_seconds

   def get_poller_name(self) -> str:
      return "yahoo_earnings_calendar"

   async def fetch_data(self, persist_db: bool = True) -> list[EarningsEvent]:  # type: ignore[override]
      """Asynchronously fetch earnings data for today.

      The underlying `YahooFinance.get_earnings` call is synchronous (uses
      `requests`).  To avoid blocking the event-loop, it is executed in a
      thread-pool via `asyncio.to_thread`.
      """
      ee: List[EarningsEvent] = cast(List[EarningsEvent], await asyncio.to_thread(self.yf.get_earnings, self.today, as_dataframe=False))
      if not ee:
         logger.info("No earnings data fetched for today")
      return ee   

   def run_polling_loop(self) -> None:  # type: ignore[override]
      """Synchronous wrapper that drives the asynchronous polling loop."""
      asyncio.run(self.async_polling_loop())

   async def async_polling_loop(self) -> None:
      while True:
         ee: list[EarningsEvent] = await self.fetch_data()

         try:
            written_ids: list[int] = await asyncio.to_thread(
               YahooFinance.save_earnings_data_to_db, ee, self.db_conn
            )

            # filter ids that are not in the cache
            new_ids: list[int] = [id for id in written_ids if str(id) not in self.cache]
            for new_id in new_ids:
               self.cache.add(str(new_id))

            await self._emit_new_earnings(new_ids)

         except Exception as e:
            logger.error(f"Failed to save earnings data to database: {e}")
            raise

         await asyncio.sleep(self.interval)

   async def _emit_new_earnings(self, new_ids: list[int]) -> None:
      # retrieve the new items from the database
      if not new_ids:
         return

      placeholders = ",".join(["?"] * len(new_ids))
      query = f"SELECT * FROM earnings_reports WHERE id IN ({placeholders})"
      new_earnings = await asyncio.to_thread(self.db_conn.execute, query, new_ids)
      new_earnings = new_earnings.fetchall()

      # convert the new_earnings to a list of EarningsEvent
      new_earnings = [EarningsEvent.from_db_row(row) for row in new_earnings]

      # Emit to sink if configured
      if self._sink is not None:
            try:
               # Call sink in a thread to prevent unexpected blocking
               await asyncio.to_thread(
                  lambda: [self._sink(self.get_poller_name(), e) for e in new_earnings]  # type: ignore[misc]
               )
            except Exception as sink_exc:
               logger.exception("[SINK] Error while emitting item: %s", sink_exc)
      

   def parse_items(self, data: Response | Dict[str, Any] | pd.DataFrame) -> List[BaseItem]:
      raise NotImplementedError("YahooEarningsCalendarPoller does not support parsing items")

   def extract_article_text(self, item: BaseItem) -> str | None:
      raise NotImplementedError("YahooEarningsCalendarPoller does not support extracting article text")


def run_poller():

   poller = YahooEarningsPoller()
   poller.run()

# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual usage
   run_poller()

        



