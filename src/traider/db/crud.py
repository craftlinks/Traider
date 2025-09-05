from __future__ import annotations

import logging
import sqlite3
from typing import Optional, List, Dict, TYPE_CHECKING

from .database import get_db_connection, create_tables

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helper / CRUD functions
# ---------------------------------------------------------------------------


def add_company(
    *,
    ticker: str,
    cik: str,
    company_name: str,
    sector: str | None = None,
    industry: str | None = None,
) -> None:
    """Insert a company into the *companies* table (or ignore if present)."""
    with get_db_connection() as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO companies (ticker, cik, company_name, sector, industry)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                ticker.upper(),
                cik,
                company_name,
                sector,
                industry,
            ),
        )
        conn.commit()
        logger.debug("Added company %s", ticker)


def add_company_and_exchange(
    *,
    ticker: str,
    cik: str,
    company_name: str,
    exchange_name: str,
) -> None:
    """Insert (or ignore) a company and exchange, then link them.

    This operation is executed as a single transaction to maintain data integrity.
    """
    with get_db_connection() as conn:
        cursor = conn.cursor()
        try:
            # 1. Insert company if not present (same transaction)
            cursor.execute(
                """
                INSERT OR IGNORE INTO companies (ticker, cik, company_name)
                VALUES (?, ?, ?)
                """,
                (ticker.upper(), cik, company_name),
            )

            # 2. Insert exchange if not present
            cursor.execute(
                """INSERT OR IGNORE INTO exchanges (name) VALUES (?)""",
                (exchange_name.upper(),),
            )

            # 3. Retrieve exchange id
            cursor.execute(
                "SELECT id FROM exchanges WHERE name = ?", (exchange_name.upper(),)
            )
            exchange_row = cursor.fetchone()
            if exchange_row is None:
                raise RuntimeError(
                    f"Unable to resolve exchange record for {exchange_name}"
                )
            exchange_id: int = exchange_row["id"]  # type: ignore[index]

            # Ensure company row exists (in case insertion was ignored due to
            # constraint conflicts). If not, abort linking to avoid FK error.
            cursor.execute(
                "SELECT 1 FROM companies WHERE ticker = ? LIMIT 1;",
                (ticker.upper(),),
            )
            if cursor.fetchone() is None:
                logger.warning(
                    "Company %s not present after attempted insert; skipping exchange link to avoid FK violation.",
                    ticker,
                )
                conn.commit()
                return

            # 4. Link company & exchange
            cursor.execute(
                """
                INSERT OR IGNORE INTO company_exchanges (company_ticker, exchange_id)
                VALUES (?, ?)
                """,
                (ticker.upper(), exchange_id),
            )

            conn.commit()
            logger.debug(
                "Added/linked company %s on exchange %s", ticker, exchange_name
            )
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while adding company %s: %s", ticker, exc)
            raise


def add_url(
    *,
    company_ticker: str,
    url_type: str,
    url: str,
    conn: sqlite3.Connection | None = None,
) -> None:
    """Add or replace a URL for a company.

    The UNIQUE(company_ticker, url_type) constraint ensures idempotency. We use
    "REPLACE" semantics to update an existing URL of the same type.
    """

    owns_connection = conn is None
    if conn is None:
        conn = get_db_connection()

    try:
        conn.execute(
            """
            INSERT INTO urls (company_ticker, url_type, url)
            VALUES (?, ?, ?)
            ON CONFLICT(company_ticker, url_type)
            DO UPDATE SET url = excluded.url;
            """,
            (company_ticker.upper(), url_type.lower(), url),
        )
        if owns_connection:
            conn.commit()
        logger.info("Set %s URL for %s", url_type, company_ticker)
    except sqlite3.Error as exc:
        if owns_connection:
            conn.rollback()
        logger.exception(
            "SQLite error while adding url for %s: %s", company_ticker, exc
        )
        raise
    finally:
        if owns_connection:
            conn.close()


# ---------------------------------------------------------------------------
# New persistence helpers (moved from yfinance._models)
# ---------------------------------------------------------------------------

if TYPE_CHECKING:  # pragma: no cover – avoid runtime import cycles
    from traider.yfinance._models import Profile, EarningsEvent, PressRelease
    from traider.yfinance._helpers import (
        _validate_ticker,
        _validate_company_name,
        _validate_datetime,
        _validate_numeric,
        _validate_string,
    )


def save_profile(
    *, ticker: str, profile: "Profile", conn: sqlite3.Connection | None = None
) -> None:
    """Persist *profile* information for *ticker*.

    This function updates the ``companies`` and ``urls`` tables as required. It
    mirrors the original logic previously found in :pyfunc:`Profile.to_db`.
    """

    owns_connection = conn is None
    if conn is None:
        conn = get_db_connection()

    try:
        # 1. Persist homepage URL (if provided)
        if profile.website_url:
            add_url(
                company_ticker=ticker,
                url_type="website",
                url=profile.website_url,
                conn=conn,
            )

        # 2. Persist sector / industry metadata (if any)
        if profile.sector or profile.industry:
            conn.execute(
                """
                UPDATE companies
                SET sector   = COALESCE(?, sector),
                    industry = COALESCE(?, industry)
                WHERE ticker = ?
                """,
                (profile.sector, profile.industry, ticker.upper()),
            )

            logger.info("Company %s profile updated successfully", ticker)

        if owns_connection:
            conn.commit()
    except sqlite3.Error as exc:
        logger.exception("SQLite error while saving profile for %s: %s", ticker, exc)
        if owns_connection:
            conn.rollback()
        raise
    finally:
        if owns_connection:
            conn.close()


def save_earnings_event(
    event: "EarningsEvent", conn: sqlite3.Connection | None = None
) -> int | None:  # noqa: C901 – complex, acceptable
    """Upsert *event* into ``earnings_reports``.

    Returns the primary key *id* of the affected row or *None* on failure.
    """

    owns_connection = conn is None
    if conn is None:
        conn = get_db_connection()

    # Import validators lazily to avoid circular-import issues during
    # application startup and to ensure they are available at runtime.
    from traider.yfinance._helpers import (
        _validate_ticker,
        _validate_company_name,
        _validate_datetime,
        _validate_numeric,
        _validate_string,
    )

    try:
        symbol = _validate_ticker(event.ticker)
        company_name = _validate_company_name(event.company_name)
        call_time = _validate_datetime(event.earnings_call_time)

        if not symbol or not company_name:
            logger.warning(
                "Skipping invalid earnings event – symbol=%s, company=%s",
                event.ticker,
                event.company_name,
            )
            return None

        # Ensure company exists
        conn.execute(
            "INSERT OR IGNORE INTO companies (ticker, company_name) VALUES (?, ?);",
            (symbol, company_name),
        )

        sql = (
            "INSERT INTO earnings_reports (company_ticker, report_datetime, event_name, time_type, "
            "eps_estimate, reported_eps, surprise_percentage, market_cap) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(company_ticker, report_datetime) DO UPDATE SET "
            "event_name=excluded.event_name, time_type=excluded.time_type, "
            "eps_estimate=excluded.eps_estimate, reported_eps=excluded.reported_eps, "
            "surprise_percentage=excluded.surprise_percentage, market_cap=excluded.market_cap, "
            "updated_at=CURRENT_TIMESTAMP"
        )

        params = (
            symbol,
            call_time,
            _validate_string(event.event_name),
            _validate_string(event.time_type),
            _validate_numeric(event.eps_estimate),
            _validate_numeric(event.eps_actual),
            _validate_numeric(event.eps_surprise_percent),
            _validate_numeric(event.market_cap),
        )

        cursor = conn.execute(sql, params)
        row_id = cursor.lastrowid

        if not row_id:
            # UPDATE path – fetch existing ID
            cur2 = conn.execute(
                "SELECT id FROM earnings_reports WHERE company_ticker = ? AND report_datetime = ?",
                (symbol, call_time),
            )
            row = cur2.fetchone()
            if row is not None:
                row_id = row[0]

        if row_id is None:
            if owns_connection:
                conn.rollback()
            logger.error(
                "Unable to obtain primary key for earnings event (%s, %s)",
                symbol,
                call_time,
            )
            return None

        if owns_connection:
            conn.commit()

        return int(row_id)
    except sqlite3.Error as exc:
        if owns_connection:
            conn.rollback()
        logger.exception(
            "SQLite error while saving earnings event for %s: %s", event.ticker, exc
        )
        return None
    finally:
        if owns_connection:
            conn.close()


def save_press_release(
    release: "PressRelease", conn: sqlite3.Connection | None = None
) -> int | None:
    """Upsert *release* into ``press_releases`` table and return its PK id."""

    owns_connection = conn is None
    if conn is None:
        conn = get_db_connection()

    try:
        sql = (
            "INSERT INTO press_releases (company_ticker, title, url, type, pub_date, display_time, "
            "company_name, raw_html, text_content) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(url) DO UPDATE SET "
            "company_ticker = excluded.company_ticker, title = excluded.title, "
            "type = excluded.type, pub_date = excluded.pub_date, "
            "display_time = excluded.display_time, company_name = excluded.company_name, "
            "raw_html = excluded.raw_html, text_content = excluded.text_content;"
        )

        params = (
            release.ticker.upper(),
            release.title,
            release.url,
            release.type,
            release.pub_date,
            release.display_time,
            release.company_name,
            release.raw_html,
            release.text_content,
        )

        cursor = conn.execute(sql, params)
        row_id = cursor.lastrowid

        if not row_id:
            row = conn.execute(
                "SELECT id FROM press_releases WHERE url = ?;", (release.url,)
            ).fetchone()
            if row is not None:
                row_id = row[0]

        if row_id is None:
            if owns_connection:
                conn.rollback()
            logger.error(
                "Unable to obtain primary key for press release %s", release.url
            )
            return None

        if owns_connection:
            conn.commit()
        return int(row_id)
    except sqlite3.Error as exc:
        if owns_connection:
            conn.rollback()
        logger.exception(
            "SQLite error while saving press release %s: %s", release.url, exc
        )
        return None
    finally:
        if owns_connection:
            conn.close()


# ---------------------------------------------------------------------------
# Earnings-specific helpers
# ---------------------------------------------------------------------------


def add_earnings_report(report: Dict[str, object]) -> None:
    """Insert or update an earnings report.

    The *earnings_reports* table uses a UNIQUE(company_ticker, report_date) constraint,
    which allows us to rely on SQLite's *ON CONFLICT* clause to perform an UPSERT.

    Parameters
    ----------
    report : Dict[str, object]
        Expect keys:
        - company_ticker (str)
        - report_date (str, ISO-8601 e.g. ``YYYY-MM-DD``)
        - fiscal_quarter (int | None)
        - fiscal_year (int | None)
        - event_name (str | None)
        - call_time (str)
        - eps_estimate (float | None)
        - reported_eps (float | None)
        - surprise_percentage (float | None)
        - market_cap (int | None) – *parsed to raw integer dollars*.
    """
    sql = """
        INSERT INTO earnings_reports (
            company_ticker,
            report_date,
            fiscal_quarter,
            fiscal_year,
            event_name,
            call_time,
            eps_estimate,
            reported_eps,
            surprise_percentage,
            market_cap_on_report_date
        ) VALUES (
            :company_ticker,
            :report_date,
            :fiscal_quarter,
            :fiscal_year,
            :event_name,
            :call_time,
            :eps_estimate,
            :reported_eps,
            :surprise_percentage,
            :market_cap
        )
        ON CONFLICT(company_ticker, report_date) DO UPDATE SET
            fiscal_quarter          = excluded.fiscal_quarter,
            fiscal_year             = excluded.fiscal_year,
            event_name              = excluded.event_name,
            call_time               = excluded.call_time,
            eps_estimate            = excluded.eps_estimate,
            reported_eps            = excluded.reported_eps,
            surprise_percentage     = excluded.surprise_percentage,
            market_cap_on_report_date = excluded.market_cap_on_report_date,
            updated_at              = CURRENT_TIMESTAMP;
        """

    with get_db_connection() as conn:
        try:
            conn.execute(sql, report)
            conn.commit()
            logger.debug(
                "Saved earnings report for %s on %s",
                report.get("company_ticker"),
                report.get("report_date"),
            )
        except sqlite3.IntegrityError as exc:
            # Likely company_ticker does not exist yet – caller should ensure FK integrity
            conn.rollback()
            logger.exception("Integrity error while inserting earnings report: %s", exc)
            raise
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while inserting earnings report: %s", exc)
            raise


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def get_company_by_ticker(ticker: str) -> Optional[Dict[str, str | None]]:
    """Return company row and aggregated exchange list for the given ticker."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row  # guarantee mapping-like row
        cursor = conn.execute(
            """
            SELECT c.*, GROUP_CONCAT(e.name, ', ') AS exchanges
            FROM companies c
            LEFT JOIN company_exchanges ce ON c.ticker = ce.company_ticker
            LEFT JOIN exchanges e ON ce.exchange_id = e.id
            WHERE c.ticker = ?
            GROUP BY c.ticker;
            """,
            (ticker.upper(),),
        )
        row = cursor.fetchone()
        return dict(row) if row else None


def list_companies(limit: int | None = None) -> List[Dict[str, str | None]]:
    """Return a list of companies with optional limit."""
    sql = "SELECT ticker, company_name, sector, industry FROM companies ORDER BY ticker"
    if limit is not None:
        sql += f" LIMIT {limit}"

    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(sql)
        return [dict(r) for r in cursor.fetchall()]


def get_earnings_by_date(date_str: str) -> List[Dict[str, object]]:
    """Return all earnings reports for a given *report_date* (ISO-8601 string)."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM earnings_reports WHERE report_date = ? ORDER BY company_ticker",
            (date_str,),
        )
        return [dict(r) for r in cursor.fetchall()]


def get_earnings_for_ticker(ticker: str) -> List[Dict[str, object]]:
    """Return all earnings reports for *ticker*, most-recent first."""
    with get_db_connection() as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """SELECT *
               FROM earnings_reports
               WHERE company_ticker = ?
               ORDER BY report_date DESC""",
            (ticker.upper(),),
        )
        return [dict(r) for r in cursor.fetchall()]
