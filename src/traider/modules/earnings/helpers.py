import sqlite3
import time
from typing import Any
import pandas as pd
import logging
from datetime import date, timedelta
from traider.db.data_manager import add_url
from traider.platforms.yahoo.main import Profile, YahooFinance

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("earnings_collection.log")],
)


def get_earnings_for_date_range(
    start_date: date, end_date: date, yf: YahooFinance
) -> pd.DataFrame:
    """Get a list of dates between start_date and end_date."""
    date_range = [
        start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
    ]

    all_earnings_df = pd.DataFrame()
    successful_fetches = 0
    failed_fetches = 0

    for day in date_range:
        try:
            logger.info(f"Fetching earnings data for {day}")
            df_day = yf.get_earnings(day)
            if not df_day.empty:
                all_earnings_df = pd.concat(
                    [all_earnings_df, df_day], ignore_index=True
                )
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
        logger.info("No earnings data fetched for the date range")
    else:
        logger.info(f"--- Combined Data ({len(all_earnings_df)} total rows) ---")
        logger.info(
            f"Successfully fetched data for {successful_fetches} days, failed for {failed_fetches} days"
        )
        logger.info(all_earnings_df.head(10))

    return all_earnings_df


def get_earnings_tickers_for_date_range(
    db_conn: sqlite3.Connection, start_date: date, end_date: date
) -> list[str]:
    """Get a list of tickers for companies with earnings data for the given date range."""
    cursor = db_conn.cursor()
    cursor.execute(
        "SELECT DISTINCT company_ticker FROM earnings_reports WHERE report_datetime >= ? AND report_datetime < ?",
        (start_date, end_date + timedelta(days=1)),
    )
    return [row[0] for row in cursor.fetchall()]


def save_earnings_data(
    df: pd.DataFrame, conn: sqlite3.Connection, max_retries: int = 3
) -> None:
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
                        logger.warning(
                            f"Skipping row with invalid symbol '{row['Symbol']}' or company name '{row['Company']}'"
                        )
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
                    logger.error(
                        f"Failed to process row for symbol {row.get('Symbol', 'Unknown')}: {e}"
                    )
                    continue

            # Commit the batch transaction
            conn.commit()
            logger.info(
                f"Processed batch {start_idx // batch_size + 1}: {len(batch_df)} rows, {successful_inserts} successful, {failed_inserts} failed"
            )

        except sqlite3.Error as e:
            # Rollback on database errors
            conn.rollback()
            failed_inserts += len(batch_df)
            logger.error(
                f"Database error in batch {start_idx // batch_size + 1}, rolling back: {e}"
            )

            # Retry logic for database errors
            if max_retries > 0:
                logger.info(
                    f"Retrying batch {start_idx // batch_size + 1} ({max_retries} retries remaining)"
                )
                time.sleep(0.1)  # Brief pause before retry
                return save_earnings_data(batch_df, conn, max_retries - 1)

    logger.info(
        f"Database save operation completed: {successful_inserts} successful, {failed_inserts} failed"
    )
    if successful_inserts > 0:
        logger.info(
            f"Successfully saved {successful_inserts} earnings reports to the database."
        )
    if failed_inserts > 0:
        logger.error(
            f"Failed to save {failed_inserts} earnings reports due to data issues."
        )

    # ---------------------------------------------------------------------------


# Data validation helpers
# ---------------------------------------------------------------------------


def _validate_ticker(ticker: str) -> str | None:
    if not ticker or len(ticker) > 10:  # Reasonable ticker length limit
        return None

    # Remove any potentially problematic characters
    ticker_clean = "".join(c for c in ticker if c.isalnum() or c in ".-")
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


def fetch_urls_from_db(
    db_conn: sqlite3.Connection, tickers: list[str], url_type: str
) -> list[str]:
    """Fetch URLs from the database for a given list of tickers and URL type."""
    if not tickers:
        return []

    cursor = db_conn.cursor()
    placeholders = ", ".join("?" for _ in tickers)
    query = f"SELECT url FROM urls WHERE company_ticker IN ({placeholders}) AND url_type = ?"

    params = tickers + [url_type]
    cursor.execute(query, params)

    urls = [row[0] for row in cursor.fetchall()]
    return urls


def save_profile_to_db(
    ticker: str,
    profile: Profile,
    db_conn: sqlite3.Connection,
) -> None:
    if profile.website_url:
        add_url(company_ticker=ticker, url_type="website", url=profile.website_url)

    sector = profile.sector
    industry = profile.industry
    if sector or industry:
        try:
            db_conn.execute(
                """
                UPDATE companies
                SET sector   = COALESCE(?, sector),
                    industry = COALESCE(?, industry)
                WHERE ticker = ?
                """,
                (sector, industry, ticker.upper()),
            )
            db_conn.commit()
        except Exception as exc:  # noqa: BLE001
            logger.exception("DB error while updating company %s: %s", ticker, exc)
            db_conn.rollback()
            return

    logger.info("Company %s profile updated successfully", ticker)
