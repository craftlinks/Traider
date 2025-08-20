from __future__ import annotations

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
from datetime import timezone, date, timedelta
from typing import Final, Tuple, Any
import time

import json
from urllib.parse import quote_plus

import pandas as pd  # type: ignore  # runtime dependency
import requests
from bs4 import BeautifulSoup  # type: ignore[attr-defined]

from traider.db.database import get_db_connection, create_tables

# Set up logging
logger = logging.getLogger(__name__)

__all__ = ["get_earnings_data_advanced", "save_earnings_data"]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

YF_CALENDAR_URL_TEMPLATE: Final[str] = "https://finance.yahoo.com/calendar/earnings?day={date}"
YF_VISUALIZATION_API: Final[str] = (
    "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US&crumb={crumb}"
)
_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _fetch_cookie_and_crumb(session: requests.Session, *, timeout: int = 30) -> tuple[Any | None, str | None]:
    """Retrieve the Yahoo *A3* cookie and crumb token via undocumented endpoints.

    1. Request ``https://fc.yahoo.com`` which sets a cross-site *A3* cookie even for
       unauthenticated users.
    2. Call ``https://query1.finance.yahoo.com/v1/test/getcrumb`` with that cookie –
       the response body is the crumb.

    Returns
    -------
    cookie, crumb
        *cookie* is the first cookie returned by *fc.yahoo.com* (usually *A3*).
        *crumb* is the anti-CSRF token string or *None* when retrieval failed.
    """

    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = session.get("https://fc.yahoo.com", headers=headers, timeout=timeout, allow_redirects=True)
        if not resp.cookies:
            return None, None
        cookie = next(iter(resp.cookies), None)
        if cookie is None:
            return None, None

        crumb_resp = session.get(  # type: ignore[arg-type]
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers=headers,
            cookies={cookie.name: str(cookie.value)},
            timeout=timeout,
        )
        crumb = crumb_resp.text.strip()
        if not crumb or "<html>" in crumb:
            return cookie, None
        return cookie, crumb
    except Exception:
        return None, None


# ---------------------------------------------------------------------------
# Data validation helpers
# ---------------------------------------------------------------------------


def _validate_ticker(ticker: Any) -> str | None:
    """Validate and clean ticker symbol."""
    if pd.isna(ticker) or not ticker:
        return None

    ticker_str = str(ticker).strip()
    if not ticker_str or len(ticker_str) > 10:  # Reasonable ticker length limit
        return None

    # Remove any potentially problematic characters
    ticker_clean = ''.join(c for c in ticker_str if c.isalnum() or c in '.-')
    return ticker_clean.upper() if ticker_clean else None


def _validate_company_name(name: Any) -> str | None:
    """Validate and clean company name."""
    if pd.isna(name) or not name:
        return None

    name_str = str(name).strip()
    if not name_str or len(name_str) > 200:  # Reasonable name length limit
        return None

    return name_str


def _validate_datetime(dt: Any) -> str | None:
    """Validate and format datetime for database storage."""
    if pd.isna(dt):
        return None

    try:
        if isinstance(dt, pd.Timestamp):
            return dt.isoformat()
        elif isinstance(dt, str):
            # Parse string datetime
            parsed_dt = pd.to_datetime(dt, utc=True, errors="coerce")
            if pd.notna(parsed_dt):
                return parsed_dt.isoformat()
    except Exception:
        pass

    return None


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


def _validate_string(value: Any) -> str | None:
    """Validate and clean string value."""
    if pd.isna(value):
        return None

    value_str = str(value).strip()
    if not value_str or len(value_str) > 500:  # Reasonable string length limit
        return None

    return value_str


def save_earnings_data(df: pd.DataFrame, conn: sqlite3.Connection, max_retries: int = 3) -> None:
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
    if df.empty:
        logger.info("No data to save - DataFrame is empty")
        return

    # Replace pandas missing values (NaN, NaT) with None for DB compatibility
    df_clean = df.where(pd.notna(df), None)

    # Validate required columns exist
    required_columns = ["Symbol", "Company", "Earnings Call Time"]
    missing_columns = [col for col in required_columns if col not in df_clean.columns]
    if missing_columns:
        logger.error(f"Missing required columns: {missing_columns}")
        return

    cursor = conn.cursor()
    successful_inserts = 0
    failed_inserts = 0

    # Process data in batches to improve performance and error recovery
    batch_size = 50
    total_rows = len(df_clean)

    logger.info(f"Starting to save {total_rows} earnings reports to database")

    for start_idx in range(0, total_rows, batch_size):
        end_idx = min(start_idx + batch_size, total_rows)
        batch_df = df_clean.iloc[start_idx:end_idx]

        try:
            # Begin transaction for this batch
            conn.execute("BEGIN TRANSACTION")

            for _, row in batch_df.iterrows():
                try:
                    # Validate and clean data
                    symbol = _validate_ticker(row["Symbol"])
                    company_name = _validate_company_name(row["Company"])
                    earnings_call_time = _validate_datetime(row["Earnings Call Time"])

                    if not symbol or not company_name:
                        logger.warning(f"Skipping row with invalid symbol '{row['Symbol']}' or company name '{row['Company']}'")
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
                    market_cap = _validate_numeric(row.get("Market Cap"))

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
                            updated_at=CURRENT_TIMESTAMP;
                    """

                    params = (
                        symbol,
                        earnings_call_time,
                        _validate_string(row.get("Event Name")),
                        _validate_string(row.get("Time Type")),
                        _validate_numeric(row.get("EPS Estimate")),
                        _validate_numeric(row.get("Reported EPS")),
                        _validate_numeric(row.get("Surprise (%)")),
                        market_cap,
                    )

                    cursor.execute(sql, params)
                    successful_inserts += 1

                except Exception as e:
                    failed_inserts += 1
                    logger.error(f"Failed to process row for symbol {row.get('Symbol', 'Unknown')}: {e}")
                    continue

            # Commit the batch transaction
            conn.commit()
            logger.info(f"Processed batch {start_idx//batch_size + 1}: {len(batch_df)} rows, {successful_inserts} successful, {failed_inserts} failed")

        except sqlite3.Error as e:
            # Rollback on database errors
            conn.rollback()
            failed_inserts += len(batch_df)
            logger.error(f"Database error in batch {start_idx//batch_size + 1}, rolling back: {e}")

            # Retry logic for database errors
            if max_retries > 0:
                logger.info(f"Retrying batch {start_idx//batch_size + 1} ({max_retries} retries remaining)")
                time.sleep(0.1)  # Brief pause before retry
                return save_earnings_data(batch_df, conn, max_retries - 1)

    logger.info(f"Database save operation completed: {successful_inserts} successful, {failed_inserts} failed")
    if successful_inserts > 0:
        print(f"Successfully saved {successful_inserts} earnings reports to the database.")
    if failed_inserts > 0:
        print(f"Failed to save {failed_inserts} earnings reports due to data issues.")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_earnings_data_advanced(target_date: date, max_retries: int = 3) -> pd.DataFrame:  # noqa: D401 – prefer imperative
    """Fetch Yahoo Finance earnings calendar for a specific date with robust error handling.

    Parameters
    ----------
    target_date:
        Target date to fetch data for.
    max_retries:
        Maximum number of retry attempts for network requests.

    Returns
    -------
    pd.DataFrame
        Parsed calendar with human-readable column names. Might be empty when
        no rows are returned or an unrecoverable error occurs.
    """
    date_str = target_date.strftime("%Y-%m-%d")
    print(f"--- Starting advanced fetch for {date_str} ---")

    # Keep cookies between requests – they include crucial auth tokens
    session = requests.Session()
    session.headers.update({"User-Agent": _USER_AGENT})

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                logger.info(f"Retry attempt {attempt}/{max_retries} for {date_str}")
                time.sleep(2 ** attempt)  # Exponential backoff

            # ------------------------------------------------------------------
            # STEP 1: Retrieve crumb token from earnings calendar page
            # ------------------------------------------------------------------
            print("Step 1: Obtaining Yahoo cookie & crumb via fc.yahoo.com …")

            cookie, crumb = _fetch_cookie_and_crumb(session)

            if cookie and crumb:
                print(f"Successfully obtained crumb: {crumb}")
            else:
                raise RuntimeError("Unable to obtain Yahoo crumb token via any method.")

            # ------------------------------------------------------------------
            # STEP 2: Query internal visualization API
            # ------------------------------------------------------------------
            print("Step 2: Querying visualization API…")
            api_url = YF_VISUALIZATION_API.format(crumb=quote_plus(crumb))

            # ------------------------------------------------------------------
            # Build new payload (as captured from browser dev tools)
            # ------------------------------------------------------------------
            next_day = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")

            payload = {
                "offset": 0,
                "size": 250,  # API rejects >250
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

            data_resp = session.post(
                api_url,
                json=payload,
                timeout=30,
                headers={"x-crumb": crumb, "User-Agent": _USER_AGENT},
                cookies={cookie.name: str(cookie.value)} if cookie else None,  # type: ignore[arg-type]
            )
            data_resp.raise_for_status()
            api_payload = data_resp.json()

            # ------------------------------------------------------------------
            # STEP 3: Transform JSON into DataFrame
            # ------------------------------------------------------------------
            print("Step 3: Parsing API response…")
            documents: list[dict] = (
                api_payload.get("finance", {}).get("result", [{}])[0].get("documents", [])  # type: ignore[index]
            )
            if not documents:
                print("No earnings rows returned by Yahoo.")
                return pd.DataFrame()

            doc = documents[0]
            rows = doc.get("rows", [])
            columns_meta = doc.get("columns", [])
            if not rows or not columns_meta:
                print("Unexpected response structure – rows or columns missing.")
                return pd.DataFrame()

            columns = [col["id"] for col in columns_meta]
            df = pd.DataFrame(rows, columns=columns)  # type: ignore[arg-type]

            # Friendly column names
            df.rename(
                columns={
                    "ticker": "Symbol",
                    "companyshortname": "Company",
                    "eventname": "Event Name",
                    "startdatetime": "Earnings Call Time",
                    "startdatetimetype": "Time Type",
                    "epsestimate": "EPS Estimate",
                    "epsactual": "Reported EPS",
                    "epssurprisepct": "Surprise (%)",
                    "intradaymarketcap": "Market Cap",
                },
                inplace=True,
            )

            # Timestamp → timezone-aware datetime
            if "Earnings Call Time" in df.columns and not df["Earnings Call Time"].empty:
                col = df["Earnings Call Time"]
                if pd.api.types.is_numeric_dtype(col):
                    # milliseconds since epoch UTC
                    df["Earnings Call Time"] = pd.to_datetime(col, unit="ms", utc=True)
                else:
                    # ISO‐8601 strings like 2025-08-14T04:00:00.000Z
                    df["Earnings Call Time"] = pd.to_datetime(col, utc=True, errors="coerce")

            # Ensure numeric columns are typed correctly
            for col in ["EPS Estimate", "Reported EPS", "Surprise (%)", "Market Cap"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            print(f"Successfully fetched {len(df)} earnings rows.")
            return df

        except requests.RequestException as exc:
            logger.error(f"Network-level error while contacting Yahoo Finance (attempt {attempt + 1}): {exc}")
            if attempt < max_retries:
                continue
            else:
                print(f"Network-level error while contacting Yahoo Finance: {exc}")
        except Exception as exc:  # noqa: BLE001 – broad but prints error to user
            logger.error(f"Unhandled error parsing Yahoo response (attempt {attempt + 1}): {exc}")
            if attempt < max_retries:
                continue
            else:
                print(f"Unhandled error parsing Yahoo response: {exc}")

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual usage
    try:
        # Set up logging for CLI usage
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        # Ensure the database and its tables are created before proceeding
        print("Initializing database…")
        create_tables()

        # Define the date range: yesterday, today, and tomorrow
        today = date.today()
        date_range = [today - timedelta(days=1), today, today + timedelta(days=1)]

        all_earnings_df = pd.DataFrame()
        successful_fetches = 0
        failed_fetches = 0

        for day in date_range:
            try:
                logger.info(f"Fetching earnings data for {day}")
                df_day = get_earnings_data_advanced(day)
                if not df_day.empty:
                    all_earnings_df = pd.concat([all_earnings_df, df_day], ignore_index=True)
                    successful_fetches += 1
                    logger.info(f"Successfully fetched {len(df_day)} rows for {day}")
                else:
                    logger.warning(f"No data returned for {day}")
                    failed_fetches += 1
            except Exception as e:
                logger.error(f"Failed to fetch data for {day}: {e}")
                failed_fetches += 1
                continue

        if all_earnings_df.empty:
            print("No earnings data fetched for the last three days. Exiting.")
            logger.info("No earnings data fetched for the date range")
        else:
            print(f"\n--- Combined Data ({len(all_earnings_df)} total rows) ---")
            print(f"Successfully fetched data for {successful_fetches} days, failed for {failed_fetches} days")
            print(all_earnings_df.head())

            try:
                with get_db_connection() as db_conn:
                    save_earnings_data(all_earnings_df, db_conn)
            except Exception as e:
                logger.error(f"Failed to save earnings data to database: {e}")
                print(f"Error saving to database: {e}")
                raise

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.error(f"Unexpected error in main execution: {e}")
        print(f"Unexpected error: {e}")
        raise
