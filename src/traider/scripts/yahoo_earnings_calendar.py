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

from datetime import timezone
from typing import Final, Tuple, Any

import json
from urllib.parse import quote_plus

import pandas as pd  # type: ignore  # runtime dependency
import requests
from bs4 import BeautifulSoup  # type: ignore[attr-defined]

__all__ = ["get_earnings_data_advanced"]

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
# Public helpers
# ---------------------------------------------------------------------------

def get_earnings_data_advanced(date_str: str) -> pd.DataFrame:  # noqa: D401 – prefer imperative
    """Fetch Yahoo Finance earnings calendar for *date_str*.

    Parameters
    ----------
    date_str:
        Target date in ``YYYY-MM-DD`` format.

    Returns
    -------
    pd.DataFrame
        Parsed calendar with human-readable column names. Might be empty when
        no rows are returned or an unrecoverable error occurs.
    """

    print(f"--- Starting advanced fetch for {date_str} ---")

    # Keep cookies between requests – they include crucial auth tokens
    session = requests.Session()
    session.headers.update({"User-Agent": _USER_AGENT})

    try:
        # ------------------------------------------------------------------
        # STEP 1: Retrieve crumb token from earnings calendar page
        # ------------------------------------------------------------------
        print("Step 1: Obtaining Yahoo cookie & crumb via fc.yahoo.com …")

        cookie, crumb = _fetch_cookie_and_crumb(session)

        if cookie and crumb:
            print(f"Successfully obtained crumb: {crumb}")
        else:
            print("fc.yahoo.com method failed – falling back to HTML parsing …")

            # Fallback: fetch calendar page and try old extraction methods
            calendar_url = YF_CALENDAR_URL_TEMPLATE.format(date=date_str)
            page_response = session.get(calendar_url, timeout=30)
            page_response.raise_for_status()

            soup = BeautifulSoup(page_response.text, "html.parser")
            import re

            crumb_script: Any | None = None

            match = re.search(r'"CrumbStore":\{"crumb":"(?P<crumb>[^"]+)"\}', page_response.text)
            if match:
                raw_crumb = match.group("crumb")
                crumb = bytes(raw_crumb, "ascii").decode("unicode_escape")

            if not crumb:
                # Legacy <script data-url="getcrumb"> method
                crumb_script = soup.find("script", attrs={"data-url": lambda v: v and "getcrumb" in v})  # type: ignore[arg-type]
                if crumb_script is not None and getattr(crumb_script, "attrs", None):  # type: ignore[truthy-bool]
                    crumb_endpoint: str = crumb_script["data-url"]  # type: ignore[index]
                    if crumb_endpoint.startswith("/"):
                        crumb_endpoint = f"https://query1.finance.yahoo.com{crumb_endpoint}"

                    try:
                        resp = session.get(crumb_endpoint, timeout=15)
                        resp.raise_for_status()
                        crumb_candidate = resp.text.strip().strip('"\'')
                        if crumb_candidate and "{" not in crumb_candidate:
                            crumb = crumb_candidate
                    except Exception:
                        pass

            if not crumb and crumb_script is not None:
                try:
                    crumb_json = json.loads(crumb_script.string or "{}")  # type: ignore[arg-type]
                    crumb_body = crumb_json.get("body")
                    if isinstance(crumb_body, str) and "{" not in crumb_body:
                        crumb = crumb_body
                except json.JSONDecodeError:
                    pass

        if not crumb:
            raise RuntimeError("Unable to obtain Yahoo crumb token via any method.")

        print(f"Successfully extracted crumb: {crumb}")

        # ------------------------------------------------------------------
        # STEP 2: Query internal visualization API
        # ------------------------------------------------------------------
        print("Step 2: Querying visualization API…")
        api_url = YF_VISUALIZATION_API.format(crumb=quote_plus(crumb))

        # ------------------------------------------------------------------
        # Build new payload (as captured from browser dev tools)
        # ------------------------------------------------------------------
        from datetime import datetime, timedelta

        date_dt = datetime.strptime(date_str, "%Y-%m-%d")
        next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")

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
                df["Earnings Call Time"] = pd.to_datetime(col, unit="ms", utc=True).dt.tz_convert(
                    "America/New_York"
                )
            else:
                # ISO‐8601 strings like 2025-08-14T04:00:00.000Z
                df["Earnings Call Time"] = pd.to_datetime(col, utc=True, errors="coerce").dt.tz_convert(
                    "America/New_York"
                )

        # Ensure numeric columns are typed correctly
        for col in ["EPS Estimate", "Reported EPS", "Surprise (%)", "Market Cap"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        print(f"Successfully fetched {len(df)} earnings rows.")
        return df

    except requests.RequestException as exc:
        print(f"Network-level error while contacting Yahoo Finance: {exc}")
    except Exception as exc:  # noqa: BLE001 – broad but prints error to user
        print(f"Unhandled error parsing Yahoo response: {exc}")

    return pd.DataFrame()


# ---------------------------------------------------------------------------
# CLI usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":  # pragma: no cover – manual usage
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Download Yahoo Finance earnings calendar for a given date!")
    parser.add_argument("date", help="Target date in YYYY-MM-DD format, e.g. 2025-08-14")
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Optional CSV output path. When omitted the file is saved as 'yahoo_earnings_<date>.csv' in CWD.",
    )
    args = parser.parse_args()

    df_result = get_earnings_data_advanced(args.date)
    if df_result.empty:
        print("No data to write – exiting.")
        raise SystemExit(0)

    # Display first rows for quick inspection
    print(df_result.head(15))

    csv_path = args.output or Path.cwd() / f"yahoo_earnings_{args.date}.csv"
    df_result.to_csv(csv_path, index=False)
    print(f"Saved full data to {csv_path.absolute()}")
