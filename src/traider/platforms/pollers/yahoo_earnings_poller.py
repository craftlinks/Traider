from __future__ import annotations
from dataclasses import dataclass
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

import sqlite3
import logging
from datetime import date
from typing import Any, Dict, List
from requests import Response
import time
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
   def __init__(self, date: date = date.today()) -> None:
      config = PollerConfig.from_env(
         "YEC",
         default_interval=5
      )
      self.config = config
      logger.info("Initializing database…")
      create_tables()
      self.today = date
      self.yf = YahooFinance()
      self.db_conn = get_db_connection()
      self.cache = get_shared_cache()

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
               YahooEarningsPoller._save_earnings_data, ee, self.db_conn
            )

            # filter ids that are not in the cache
            new_ids: list[int] = [id for id in written_ids if id not in self.cache]
            for new_id in new_ids:
               self.cache.add(str(new_id))

            await self._emit_new_earnings(new_ids)

         except Exception as e:
            logger.error(f"Failed to save earnings data to database: {e}")
            raise

         await asyncio.sleep(self.config.polling_interval_seconds)

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


   # NOTE: This helper remains synchronous because the sqlite3 module is
   # inherently blocking.  It is invoked through `asyncio.to_thread` by the
   # caller to avoid blocking the event-loop.
   @staticmethod
   def _save_earnings_data(ee: list[EarningsEvent], conn: sqlite3.Connection, max_retries: int = 3) -> list[int]:
      """Save earnings data from a DataFrame into the database with robust error handling.

      This function performs two main operations:
      1.  Inserts new companies into the `companies` table if they don't exist.
      2.  Inserts or updates earnings reports in the `earnings_reports` table.

      An `INSERT OR IGNORE` strategy is used for new companies, while an `UPSERT`
      (insert on conflict update) is used for earnings reports to keep them fresh.

      Parameters
      ----------
      df:
         DataFrame with earnings data, matching the column names from Yahoo.
      conn:
         Active SQLite database connection.
      max_retries:
         Maximum number of retry attempts for database operations.
      """
      if not ee:
         logger.info("No data to save - DataFrame is empty")
         return []


      cursor = conn.cursor()
      successful_inserts = 0
      failed_inserts = 0
      # Collect the primary-key IDs of rows that are inserted or up-serted so that the
      # caller can use them (e.g. for downstream processing or logging).
      written_ids: list[int] = []

      # Process data in batches to improve performance and error recovery
      batch_size = 50
      total_rows = len(ee)

      logger.info(f"Starting to save {total_rows} earnings reports to database")

      for start_idx in range(0, total_rows, batch_size):
         end_idx = min(start_idx + batch_size, total_rows)
         batch_ee = ee[start_idx:end_idx]

         try:
               # Begin transaction for this batch
               conn.execute("BEGIN TRANSACTION")

               for row in batch_ee:
                  try:
                     # Validate and clean data
                     symbol = YahooEarningsPoller._validate_ticker(row.ticker)
                     company_name = YahooEarningsPoller._validate_company_name(row.company_name)
                     earnings_call_time = YahooEarningsPoller._validate_datetime(row.earnings_call_time)

                     if not symbol or not company_name:
                           logger.warning(f"Skipping row with invalid symbol '{row.ticker}' or company name '{row.company_name}'")
                           failed_inserts += 1
                           continue

                     # --- 1. Ensure company exists in `companies` table ---
                     try:
                           cursor.execute(
                              "INSERT OR IGNORE INTO companies (ticker, company_name) VALUES (?, ?)",
                              (symbol, company_name),
                           )
                     except sqlite3.Error as e:
                           logger.warning(f"Failed to insert company {symbol}: {e}")

                     # --- 2. Insert or update the earnings report ---
                     market_cap = YahooEarningsPoller._validate_numeric(row.market_cap)

                     sql = """
                           INSERT INTO earnings_reports (
                              company_ticker, report_datetime, event_name, time_type,
                              eps_estimate, reported_eps, surprise_percentage, market_cap
                           )
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT(company_ticker, report_datetime) DO UPDATE SET
                              event_name=excluded.event_name,
                              time_type=excluded.time_type,
                              eps_estimate=excluded.eps_estimate,
                              reported_eps=excluded.reported_eps,
                              surprise_percentage=excluded.surprise_percentage,
                              market_cap=excluded.market_cap,
                              updated_at=CURRENT_TIMESTAMP
                           RETURNING id;
                     """

                     params = (
                           symbol,
                           earnings_call_time,
                           YahooEarningsPoller._validate_string(row.event_name),
                           YahooEarningsPoller._validate_string(row.time_type),
                           YahooEarningsPoller._validate_numeric(row.eps_estimate),
                           YahooEarningsPoller._validate_numeric(row.eps_actual),
                           YahooEarningsPoller._validate_numeric(row.eps_surprise_percent),
                           market_cap,
                     )

                     row = cursor.execute(sql, params).fetchone()
                     if row is not None:
                           written_ids.append(int(row[0]))
                     successful_inserts += 1

                  except Exception as e:
                     failed_inserts += 1
                     logger.error(f"Failed to process row for symbol {row.ticker}: {e}")
                     continue

               # Commit the batch transaction
               conn.commit()
               logger.info(f"Processed batch {start_idx//batch_size + 1}: {len(batch_ee)} rows, {successful_inserts} successful, {failed_inserts} failed")

         except sqlite3.Error as e:
               # Rollback on database errors
               conn.rollback()
               failed_inserts += len(batch_ee)
               logger.error(f"Database error in batch {start_idx//batch_size + 1}, rolling back: {e}")

               # Retry logic for database errors
               if max_retries > 0:
                  logger.info(f"Retrying batch {start_idx//batch_size + 1} ({max_retries} retries remaining)")
                  time.sleep(0.1)  # Brief pause before retry
                  return YahooEarningsPoller._save_earnings_data(batch_ee, conn, max_retries - 1)

      logger.info(f"Database save operation completed: {successful_inserts} successful, {failed_inserts} failed")
      if successful_inserts > 0:
         logger.info(f"Successfully saved {successful_inserts} earnings reports to the database.")
      if failed_inserts > 0:
         logger.error(f"Failed to save {failed_inserts} earnings reports due to data issues.")

      return written_ids

   # ---------------------------------------------------------------------------
   # Data validation helpers
   # ---------------------------------------------------------------------------

   @staticmethod
   def _validate_ticker(ticker: str) -> str | None:
      if not ticker or len(ticker) > 10:  # Reasonable ticker length limit
         return None

      # Remove any potentially problematic characters
      ticker_clean = ''.join(c for c in ticker if c.isalnum() or c in '.-')
      return ticker_clean.upper() if ticker_clean else None


   @staticmethod
   def _validate_company_name(name: Any) -> str | None:
      """Validate and clean company name."""
      if pd.isna(name) or not name:
         return None

      name_str = str(name).strip()
      if not name_str or len(name_str) > 200:  # Reasonable name length limit
         return None

      return name_str

   @staticmethod
   def _validate_datetime(dt: Any) -> str | None:
      """Validate and format datetime for database storage."""
      if pd.isna(dt):
         return None

      try:
         if isinstance(dt, (pd.Timestamp, datetime)):
               return dt.isoformat()
         elif isinstance(dt, str):
               # Parse string datetime
               parsed_dt = pd.to_datetime(dt, utc=True, errors="coerce")
               if pd.notna(parsed_dt):
                  return parsed_dt.isoformat()
      except Exception:
         pass

      return None


   @staticmethod
   def _validate_numeric(value: Any) -> float | None:
      """Validate and convert to numeric value."""
      if pd.isna(value):
         return None

      try:
         numeric_val = float(value)
         # Check for reasonable bounds to filter out garbage data
         if abs(numeric_val) > 1e12:  # Too large, likely garbage
               return None
         return numeric_val
      except (ValueError, TypeError):
         return None


   @staticmethod
   def _validate_string(value: Any) -> str | None:
      """Validate and clean string value."""
      if pd.isna(value):
         return None

      value_str = str(value).strip()
      if not value_str or len(value_str) > 500:  # Reasonable string length limit
         return None

      return value_str


def run_poller():

   poller = YahooEarningsPoller()
   poller.run()

# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual usage
   run_poller()

        



