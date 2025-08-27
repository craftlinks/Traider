from dataclasses import dataclass
import sqlite3
from datetime import datetime
import logging

from traider.db.database import get_db_connection


logger = logging.getLogger(__name__)


@dataclass(slots=True)
class Profile:
    website_url: str | None
    sector: str | None
    industry: str | None

    # -------------------------------------------------------------------
    # Persistence helpers
    # -------------------------------------------------------------------

    @staticmethod
    def from_db(row: sqlite3.Row) -> "Profile":
        """Create a :class:`Profile` instance from a database *row*.

        The query supplying *row* should include the columns ``website_url``,
        ``sector`` and ``industry``. Missing columns default to *None*.
        """

        return Profile(
            website_url=row["website_url"] if "website_url" in row.keys() else None,
            sector=row["sector"] if "sector" in row.keys() else None,
            industry=row["industry"] if "industry" in row.keys() else None,
        )

    def to_db(self, *, ticker: str, conn: sqlite3.Connection | None = None) -> None:  # noqa: D401 (imperative mood)
        """Persist the profile information for *ticker* to the database.

        This updates the ``companies`` table (``sector`` / ``industry``) and
        the ``urls`` table (``website``) as required. When *conn* is *None*, a
        fresh connection is acquired automatically.
        """

        from traider.db.crud import add_url  # local import to avoid cycles

        owns_connection = conn is None
        if conn is None:
            conn = get_db_connection()

        # 1. Persist homepage URL (if any)
        if self.website_url:
            add_url(company_ticker=ticker, url_type="website", url=self.website_url, conn=conn)

        # 2. Persist sector / industry metadata (if any)
        if self.sector or self.industry:
            try:
                conn.execute(
                    """
                    UPDATE companies
                    SET sector   = COALESCE(?, sector),
                        industry = COALESCE(?, industry)
                    WHERE ticker = ?
                    """,
                    (self.sector, self.industry, ticker.upper()),
                )
                
                logger.info("Company %s profile updated successfully", ticker)
            except Exception as exc:  # noqa: BLE001
                logger.exception("DB error while updating company %s: %s", ticker, exc)
                conn.rollback()
        
        if owns_connection:
            conn.commit()
            conn.close()


@dataclass(slots=True)
class EarningsEvent:
    id: int
    ticker: str
    company_name: str
    event_name: str
    time_type: str
    earnings_call_time: datetime | None
    eps_estimate: float
    eps_actual: float
    eps_surprise: float
    eps_surprise_percent: float
    market_cap: float

    @staticmethod
    def from_db(row: sqlite3.Row) -> "EarningsEvent":
        return EarningsEvent(
            id=row["id"],
            ticker=row["company_ticker"],
            company_name="",  # TODO: denormalise company name if required
            event_name=row["event_name"],
            time_type=row["time_type"],
            earnings_call_time=row["report_datetime"],
            eps_estimate=row["eps_estimate"],
            eps_actual=row["reported_eps"],
            eps_surprise=(row["reported_eps"] - row["eps_estimate"]) if row["eps_estimate"] is not None and row["reported_eps"] is not None else float("nan"),
            eps_surprise_percent=row["surprise_percentage"],
            market_cap=row["market_cap"],
        )

    def to_db(self, conn: sqlite3.Connection | None = None) -> int | None:  # noqa: D401
        """Persist this earnings event to the ``earnings_reports`` table.

        If *conn* is *None* a fresh connection will be created. The method
        returns the primary-key *id* of the upserted row (or *None* on
        failure).
        """
        from ._helpers import _validate_ticker, _validate_company_name, _validate_datetime, _validate_numeric, _validate_string

        owns_connection = conn is None
        if conn is None:
            conn = get_db_connection()

        try:
            symbol = _validate_ticker(self.ticker)
            company_name = _validate_company_name(self.company_name)
            call_time = _validate_datetime(self.earnings_call_time)

            if not symbol or not company_name:
                logger.debug("Skipping invalid earnings event – symbol=%s, company=%s", self.ticker, self.company_name)
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
                _validate_string(self.event_name),
                _validate_string(self.time_type),
                _validate_numeric(self.eps_estimate),
                _validate_numeric(self.eps_actual),
                _validate_numeric(self.eps_surprise_percent),
                _validate_numeric(self.market_cap),
            )

            cursor = conn.execute(sql, params)
            
            row_id = cursor.lastrowid
            if row_id is None or row_id == 0:
                # This can happen on an UPDATE. We need to fetch the ID.
                cursor = conn.execute("SELECT id FROM earnings_reports WHERE company_ticker = ? AND report_datetime = ?", (symbol, call_time))
                row = cursor.fetchone()
                if row:
                    row_id = row[0]

            if row_id is None:
                conn.rollback()
                return None
            
            if owns_connection:
                conn.commit()

            return int(row_id)
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while saving earnings event: %s", exc)
            return None
        finally:
            if owns_connection:
                conn.close()


@dataclass(slots=True)
class PressRelease:
    ticker: str
    title: str
    url: str
    type: str
    pub_date: str | None = None
    display_time: str | None = None
    company_name: str | None = None
    raw_html: str | None = None
    text_content: str | None = None

    # -------------------------------------------------------------------
    # Persistence helpers
    # -------------------------------------------------------------------

    @staticmethod
    def from_db(row: sqlite3.Row) -> "PressRelease":
        """Create a :class:`PressRelease` from a DB *row*."""

        return PressRelease(
            ticker=row["company_ticker"],
            title=row["title"],
            url=row["url"],
            type=row["type"],
            pub_date=row["pub_date"],
            display_time=row["display_time"],
            company_name=row["company_name"],
            raw_html=row["raw_html"],
            text_content=row["text_content"],
        )

    def to_db(self, conn: sqlite3.Connection | None = None) -> int | None:  # noqa: D401
        """Persist this press-release to the ``press_releases`` table.

        Returns the row *id* (primary key) of the upserted record or *None* if
        the operation failed.
        """

        owns_connection = conn is None
        if conn is None:
            conn = get_db_connection()

        try:
            sql = (
                "INSERT INTO press_releases (company_ticker, title, url, type, pub_date, display_time, "
                "company_name, raw_html, text_content) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(url) DO UPDATE SET "
                "company_ticker = excluded.company_ticker, "
                "title          = excluded.title, "
                "type           = excluded.type, "
                "pub_date       = excluded.pub_date, "
                "display_time   = excluded.display_time, "
                "company_name   = excluded.company_name, "
                "raw_html       = excluded.raw_html, "
                "text_content   = excluded.text_content;"
            )

            params = (
                self.ticker.upper(),
                self.title,
                self.url,
                self.type,
                self.pub_date,
                self.display_time,
                self.company_name,
                self.raw_html,
                self.text_content,
            )

            cursor = conn.execute(sql, params)

            lrid = cursor.lastrowid
            if lrid not in (None, 0):
                conn.commit()
                return int(lrid)

            # Existing row – fetch its id
            row = conn.execute("SELECT id FROM press_releases WHERE url = ?;", (self.url,)).fetchone()
            conn.commit()
            if row is None:
                return None
            return int(row[0])
        except sqlite3.Error as exc:
            conn.rollback()
            logger.exception("SQLite error while saving press release %s: %s", self.url, exc)
            return None
        finally:
            if owns_connection:
                conn.close()
