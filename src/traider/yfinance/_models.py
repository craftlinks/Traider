from dataclasses import dataclass
import sqlite3
from datetime import datetime
import logging
import asyncio


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

    async def to_db(self, *, ticker: str, conn: sqlite3.Connection | None = None) -> None:  # noqa: D401 (imperative mood)
        """Persist the profile information for *ticker* to the database."""

        from traider.db import crud  # Local import to avoid circular dependency

        await asyncio.to_thread(crud.save_profile, ticker=ticker, profile=self, conn=conn)


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

    async def to_db(self, conn: sqlite3.Connection | None = None) -> int | None:  # noqa: D401
        """Persist this earnings event to the ``earnings_reports`` table."""

        from traider.db import crud

        return await asyncio.to_thread(crud.save_earnings_event, self, conn=conn)


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

    async def to_db(self, conn: sqlite3.Connection | None = None) -> int | None:  # noqa: D401
        """Persist this press-release to the ``press_releases`` table."""

        from traider.db import crud

        return await asyncio.to_thread(crud.save_press_release, self, conn=conn)
