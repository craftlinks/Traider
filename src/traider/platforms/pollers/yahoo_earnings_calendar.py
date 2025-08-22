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
from traider.platforms.yahoo.main import YahooFinance
from traider.db.database import get_db_connection, create_tables

# Set up logging
logger = logging.getLogger(__name__)

__all__ = ["save_earnings_data"]





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

        



