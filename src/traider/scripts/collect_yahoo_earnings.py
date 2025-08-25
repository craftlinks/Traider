import argparse
import sqlite3
import math
from datetime import date, datetime
from typing import List

from traider.platforms.yahoo.main import YahooFinance, EarningsEvent
from traider.db.database import get_db_connection, create_tables

def collect_yahoo_earnings_and_save_to_db(start_date: date, end_date: date, db_conn: sqlite3.Connection):
    yahoo_finance = YahooFinance()
    earnings: List[EarningsEvent] = yahoo_finance.get_earnings_for_date_range(start_date, end_date, as_dataframe=False) # type: ignore[assignment]

    # We only want earnings events for which we have an estimated earnings per share and an actual earnings per share
    earnings = [
        earning
        for earning in earnings
        if (
            earning.eps_estimate is not None
            and not math.isnan(earning.eps_estimate)
            # and earning.eps_actual is not None
            # and not math.isnan(earning.eps_actual)
        )
    ]

    # save to db
    yahoo_finance.save_earnings_data_to_db(earnings, db_conn)


def _parse_cli_args() -> argparse.Namespace:  # noqa: D401
    """Parse command-line arguments.

    Returns
    -------
    argparse.Namespace
        Parsed arguments containing ``start_date`` and ``end_date`` attributes.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Collect Yahoo earnings data for a date range and persist it to the local database."
        )
    )

    parser.add_argument(
        "--start-date",
        "-s",
        type=str,
        default=None,
        help="Start date in YYYY-MM-DD format (defaults to today).",
    )

    parser.add_argument(
        "--end-date",
        "-e",
        type=str,
        default=None,
        help="End date in YYYY-MM-DD format (defaults to today).",
    )

    return parser.parse_args()


def _parse_date(date_str: str | None) -> date:
    """Convert an ISO-formatted date string to a ``datetime.date``.

    Parameters
    ----------
    date_str : str | None
        Date string in ``YYYY-MM-DD`` format. If *None*, the current date is
        returned.

    Returns
    -------
    datetime.date
    """

    if date_str is None:
        return date.today()

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError as exc:  # pragma: no cover – simple argument validation
        msg = (
            "Invalid date format for '--start-date/--end-date'. Expected YYYY-MM-DD."
        )
        raise SystemExit(msg) from exc


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


if __name__ == "__main__":

    args = _parse_cli_args()

    start = _parse_date(args.start_date)
    end = _parse_date(args.end_date)

    # Ensure the date range order is valid and swap if required.
    if end < start:
        start, end = end, start

    with get_db_connection() as db_conn:
        create_tables(db_conn)
        collect_yahoo_earnings_and_save_to_db(start, end, db_conn)
