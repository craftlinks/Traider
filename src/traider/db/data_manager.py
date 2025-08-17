from __future__ import annotations

import logging
import sqlite3
from typing import Iterable, Mapping, Optional, List, Dict

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
            cursor.execute("SELECT id FROM exchanges WHERE name = ?", (exchange_name.upper(),))
            exchange_row = cursor.fetchone()
            if exchange_row is None:
                raise RuntimeError(f"Unable to resolve exchange record for {exchange_name}")
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
            logger.debug("Added/linked company %s on exchange %s", ticker, exchange_name)
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while adding company %s: %s", ticker, exc)
            raise


def add_url(*, company_ticker: str, url_type: str, url: str) -> None:
    """Add or replace a URL for a company.

    The UNIQUE(company_ticker, url_type) constraint ensures idempotency. We use
    "REPLACE" semantics to update an existing URL of the same type.
    """
    with get_db_connection() as conn:
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
            conn.commit()
            logger.debug("Set %s URL for %s", url_type, company_ticker)
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while adding url for %s: %s", company_ticker, exc)
            raise


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
    sql = (
        """
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
    )

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


# ---------------------------------------------------------------------------
# Module CLI helpers (useful for quick checks)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    import json
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Basic data manager CLI utilities.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init command
    init_parser = subparsers.add_parser("init", help="Create database tables")

    # seed command
    seed_parser = subparsers.add_parser("seed", help="Seed DB from JSON file")
    seed_parser.add_argument("json_file", type=Path, help="Path to companies JSON file")

    args = parser.parse_args()

    if args.command == "init":
        create_tables()
        print("Tables created.")
    elif args.command == "seed":
        create_tables()
        if not args.json_file.exists():
            parser.error(f"JSON file {args.json_file} does not exist")
        data: Iterable[Mapping[str, object]] = json.loads(args.json_file.read_text())
        for record in data:
            try:
                add_company_and_exchange(
                    ticker=str(record["ticker"]),
                    cik=str(record["cik_str"]),
                    company_name=str(record["title"]),
                    exchange_name=str(record["exchange"]),
                )
            except Exception:  # pragma: no cover
                # Log already done inside helper
                continue
        print("Seed completed.")
