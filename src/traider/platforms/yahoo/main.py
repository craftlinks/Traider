from dataclasses import dataclass
from datetime import date, datetime, timedelta
import logging
import sqlite3
import time
from typing import Any, Final, Optional, List
from urllib.parse import quote_plus

import pandas as pd
import requests
from traider.db.database import get_db_connection
from traider.platforms.yahoo.helpers import extract_earnings_data_json, extract_profile_data_html, extract_profile_data_json
import math

# ---------------------------------------------------------------------------
# Configuration & constants
# ---------------------------------------------------------------------------

# Set up module-level logger
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('earnings_collection.log')
    ]
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

# Include ?p=<ticker> query param – Yahoo sometimes returns 404 without it
_YF_PROFILE_TEMPLATE: Final[str] = "https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
# Public JSON endpoint that usually contains the exact same information we need
# and is less likely to change than the HTML.  No crumb token required.
_YF_PROFILE_JSON_TEMPLATE: Final[str] = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile&lang=en-US&region=US"
)
YF_CALENDAR_URL_TEMPLATE: Final[str] = "https://finance.yahoo.com/calendar/earnings?day={date}"
YF_VISUALIZATION_API: Final[str] = (
    "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US&crumb={crumb}"
)

#    Courtesy delay between requests (seconds).  Be nice to Yahoo.
_REQUEST_DELAY_S: Final[float] = 1.0

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def _to_float(val: Any) -> float:
    """Best-effort conversion to ``float`` returning ``nan`` on failure."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return float("nan")
        return float(val)
    except (TypeError, ValueError):
        return float("nan")

@dataclass
class Profile:
    website_url: str | None
    sector: str | None
    industry: str | None

@dataclass
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
    def from_db_row(row: sqlite3.Row) -> "EarningsEvent":
        return EarningsEvent(
            id=row["id"],
            ticker=row["company_ticker"],
            company_name='', # TODO: get company name from db
            event_name=row["event_name"],
            time_type=row["time_type"],
            earnings_call_time=row["report_datetime"],
            eps_estimate=row["eps_estimate"],
            eps_actual=row["reported_eps"],
            eps_surprise=row["reported_eps"] - row["eps_estimate"] if row["eps_estimate"] is not None and row["reported_eps"] is not None else float("nan"),
            eps_surprise_percent=row["surprise_percentage"],
            market_cap=row["market_cap"],
        )

# ---------------------------------------------------------------------------
# Press release structure
# ---------------------------------------------------------------------------


@dataclass
class PressRelease:
    ticker: str
    title: str
    url: str
    type: str
    pub_date: Optional[str] = None
    display_time: Optional[str] = None
    company_name: Optional[str] = None
    raw_html: Optional[str] = None
    text_content: Optional[str] = None

# ---------------------------------------------------------------------------
# YahooFinance class
# ---------------------------------------------------------------------------

class YahooFinance:
    crumb: str
    
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _USER_AGENT})
        cookie, crumb = self._fetch_cookie_and_crumb()
        if cookie and crumb:
            logger.info(f"Successfully obtained crumb: {crumb}")
            self.crumb = crumb
            self.cookie = cookie
        else:
            raise RuntimeError("Unable to obtain Yahoo crumb token via any method.")

    def _fetch_cookie_and_crumb(self, *, timeout: int = 30) -> tuple[Any | None, str | None]:
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
            resp = self.session.get("https://fc.yahoo.com", headers=headers, timeout=timeout, allow_redirects=True)
            if not resp.cookies:
                return None, None
            cookie = next(iter(resp.cookies), None)
            if cookie is None:
                return None, None

            crumb_resp = self.session.get(  # type: ignore[arg-type]
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

    def _refresh_cookie_and_crumb(self) -> None:
        cookie, crumb = self._fetch_cookie_and_crumb()
        if cookie and crumb:
            logger.info(f"Successfully refreshed crumb: {crumb}")
            self.crumb = crumb
            self.cookie = cookie
        else:
            raise RuntimeError("Unable to obtain Yahoo crumb token via any method.")

    def get_profile(self, ticker: str, from_json: bool = False) -> Profile:
        html_url = _YF_PROFILE_TEMPLATE.format(ticker=ticker)
        json_url = _YF_PROFILE_JSON_TEMPLATE.format(ticker=ticker)

        if from_json:
            return self._get_profile_with_retry(json_url, ticker, from_json=True)
        else:
            return self._get_profile_with_retry(html_url, ticker, from_json=False)

    def _get_profile_with_retry(self, url: str, ticker: str, from_json: bool) -> Profile:
        """Helper method to get profile data with a single retry on RequestException."""
        max_attempts = 2

        for attempt in range(max_attempts):
            try:
                response = self.session.get(url, timeout=20)
                response.raise_for_status()

                if from_json:
                    website_url, sector, industry = extract_profile_data_json(response.json())
                else:
                    website_url, sector, industry = extract_profile_data_html(response.text)

                return Profile(website_url, sector, industry)
            except requests.RequestException as exc:
                if attempt < max_attempts - 1:
                    self._refresh_cookie_and_crumb()
                    logger.info(f"Failed to get profile for {ticker} (attempt {attempt + 1}/{max_attempts}): {exc}. Retrying...")
                else:
                    logger.info(f"Failed to get profile for {ticker} after {max_attempts} attempts: {exc}")
                    return Profile(None, None, None)

        return Profile(None, None, None)

    def _df_to_events(self, df: pd.DataFrame) -> list[EarningsEvent]:
        """Convert a Yahoo earnings DataFrame returned by :py:meth:`extract_earnings_data_json`
        into a list of :class:`~EarningsEvent` instances.

        The helper encapsulates the somewhat verbose per-row mapping logic so that
        both :py:meth:`get_earnings` and :py:meth:`get_earnings_for_date_range` can
        share a single implementation.
        """
        events: list[EarningsEvent] = []

        if df.empty:
            return events

        for _, series in df.iterrows():
            try:
                # ``id`` is not always present – default to -1 for *unknown*.
                id_val_raw = series.get("id", -1)
                id_val = int(id_val_raw) if id_val_raw not in (None, "") else -1

                eps_est = _to_float(series.get("EPS Estimate"))
                eps_act = _to_float(series.get("Reported EPS"))
                surprise_pct = _to_float(series.get("Surprise (%)"))

                eps_surp = (
                    eps_act - eps_est
                    if not math.isnan(eps_est) and not math.isnan(eps_act)
                    else float("nan")
                )

                # Earnings Call Time – convert pandas *Timestamp* → *datetime*
                ect_raw = series.get("Earnings Call Time")
                if isinstance(ect_raw, pd.Timestamp):
                    ect_dt = ect_raw.to_pydatetime()
                else:
                    ect_dt = ect_raw  # type: ignore[assignment]

                events.append(
                    EarningsEvent(
                        id=id_val,
                        ticker=str(series.get("Symbol", "")),
                        company_name=str(series.get("Company", "")),
                        event_name=str(series.get("Event Name", "")),
                        time_type=str(series.get("Time Type", "")),
                        earnings_call_time=ect_dt,
                        eps_estimate=eps_est,
                        eps_actual=eps_act,
                        eps_surprise=eps_surp,
                        eps_surprise_percent=surprise_pct,
                        market_cap=_to_float(series.get("Market Cap")),
                    )
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "Failed to convert row to EarningsEvent: %s; row data: %s",
                    exc,
                    series.to_dict(),
                )
        return events

    # ---------------------------------------------------------------------------
    # Public API methods
    # ---------------------------------------------------------------------------
    
    def get_earnings(
        self,
        start_date: date,
        *,
        as_dataframe: bool = True,
        max_retries: int = 3,
    ) -> pd.DataFrame | List[EarningsEvent]:
        date_str = start_date.strftime("%Y-%m-%d")
        logger.info(f"--- Earnings fetch for {date_str} ---")
        
        for attempt in range(max_retries + 1):
            try:
                if attempt > 0:
                    logger.info(f"Retry attempt {attempt}/{max_retries} for {date_str}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                api_url = YF_VISUALIZATION_API.format(crumb=quote_plus(self.crumb))
                next_day = (start_date + timedelta(days=1)).strftime("%Y-%m-%d")
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
                data_resp = self.session.post(
                    api_url,
                    json=payload,
                    timeout=30,
                    headers={"x-crumb": self.crumb, "User-Agent": _USER_AGENT},
                    cookies={self.cookie.name: str(self.cookie.value)} if self.cookie else None,  # type: ignore[arg-type]
                )
                data_resp.raise_for_status()
                api_payload = data_resp.json()
                df = extract_earnings_data_json(api_payload)

                # Return early if caller wants the raw DataFrame
                if as_dataframe:
                    return df

                # Otherwise convert to a list of EarningsEvent objects
                return self._df_to_events(df)
            except requests.RequestException as exc:
                logger.error(f"Network-level error while contacting Yahoo Finance (attempt {attempt + 1}): {exc}")
                if attempt < max_retries:
                    continue
                else:
                    logger.error(f"Network-level error while contacting Yahoo Finance: {exc}")
            except Exception as exc:  # noqa: BLE001 – broad but prints error to user
                logger.error(f"Unhandled error parsing Yahoo response (attempt {attempt + 1}): {exc}")
                if attempt < max_retries:
                    continue
                else:
                    logger.error(f"Unhandled error parsing Yahoo response: {exc}")
        return pd.DataFrame()

    def get_earnings_for_date_range(self, start_date: date, end_date: date, as_dataframe: bool = False) -> pd.DataFrame | List[EarningsEvent]:
        date_range =  [start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)]

        # Initialise containers based on desired return type
        if as_dataframe:
            aggregated_df: pd.DataFrame = pd.DataFrame()
            aggregated_events: list[EarningsEvent] = []  # Always define for type safety
        else:
            aggregated_events = []
            aggregated_df = pd.DataFrame()  # Dummy for type safety

        successful_fetches = 0
        failed_fetches = 0

        for day in date_range:
            try:
                logger.info(f"Fetching earnings data for {day}")
                day_data = self.get_earnings(day, as_dataframe=as_dataframe)

                # ------------------------------------------------------------------
                # Branch depending on the expected return type
                # ------------------------------------------------------------------
                if as_dataframe:
                    if isinstance(day_data, pd.DataFrame) and not day_data.empty:
                        aggregated_df = pd.concat([aggregated_df, day_data], ignore_index=True)  # type: ignore[arg-type]
                        successful_fetches += 1
                        logger.info(f"Successfully fetched {len(day_data)} rows for {day}")
                    else:
                        logger.warning(f"No DataFrame returned for {day}")
                        failed_fetches += 1
                else:
                    if isinstance(day_data, list) and day_data:
                        aggregated_events.extend(day_data)
                        successful_fetches += 1
                        logger.info(f"Successfully fetched {len(day_data)} events for {day}")
                    else:
                        logger.warning(f"No events returned for {day}")
                        failed_fetches += 1

            except Exception as e:
                logger.error(f"Failed to fetch data for {day}: {e}")
                failed_fetches += 1
                continue

            # Be polite to Yahoo's servers – wait before the next request
            time.sleep(_REQUEST_DELAY_S)

        # ----------------------------------------------------------------------
        # Final aggregation & return
        # ----------------------------------------------------------------------
        if as_dataframe:
            if aggregated_df is not None and not aggregated_df.empty:
                logger.debug(f"--- Combined DataFrame ({len(aggregated_df)} total rows) ---")
                logger.debug(f"Successfully fetched data for {successful_fetches} days, failed for {failed_fetches} days")
                logger.debug(aggregated_df.head(10))
            else:
                logger.debug("No earnings data fetched for the date range")
            return aggregated_df if aggregated_df is not None else pd.DataFrame()
        else:
            if aggregated_events:
                logger.debug(f"--- Combined Events ({len(aggregated_events)} total) ---")
                logger.debug(f"Successfully fetched data for {successful_fetches} days, failed for {failed_fetches} days")
            else:
                logger.debug("No earnings events fetched for the date range")
            return aggregated_events

    # NOTE: This helper remains synchronous because the sqlite3 module is
    # inherently blocking.  It is invoked through `asyncio.to_thread` by the
    # caller to avoid blocking the event-loop.
    @staticmethod
    def save_earnings_data_to_db(ee: list[EarningsEvent], conn: sqlite3.Connection, max_retries: int = 3) -> List[int]:
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

        logger.debug(f"Starting to save {total_rows} earnings reports to database")

        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_ee = ee[start_idx:end_idx]

            try:
                # Begin transaction for this batch
                conn.execute("BEGIN TRANSACTION")

                for row in batch_ee:
                    try:
                        # Validate and clean data
                        symbol = YahooFinance._validate_ticker(row.ticker)
                        company_name = YahooFinance._validate_company_name(row.company_name)
                        earnings_call_time = YahooFinance._validate_datetime(row.earnings_call_time)

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
                        market_cap = YahooFinance._validate_numeric(row.market_cap)

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
                            YahooFinance._validate_string(row.event_name),
                            YahooFinance._validate_string(row.time_type),
                            YahooFinance._validate_numeric(row.eps_estimate),
                            YahooFinance._validate_numeric(row.eps_actual),
                            YahooFinance._validate_numeric(row.eps_surprise_percent),
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
                logger.debug(f"Processed batch {start_idx//batch_size + 1}: {len(batch_ee)} rows, {successful_inserts} successful, {failed_inserts} failed")

            except sqlite3.Error as e:
                # Rollback on database errors
                conn.rollback()
                failed_inserts += len(batch_ee)
                logger.error(f"Database error in batch {start_idx//batch_size + 1}, rolling back: {e}")

                # Retry logic for database errors
                if max_retries > 0:
                    logger.info(f"Retrying batch {start_idx//batch_size + 1} ({max_retries} retries remaining)")
                    time.sleep(0.1)  # Brief pause before retry
                    return YahooFinance.save_earnings_data_to_db(batch_ee, conn, max_retries - 1)

        logger.debug(f"Database save operation completed: {successful_inserts} successful, {failed_inserts} failed")
        if successful_inserts > 0:
            logger.debug(f"Successfully saved {successful_inserts} earnings reports to the database.")
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


    def get_press_releases(self, ticker: str, type: str) -> Optional[PressRelease]:
        """Return a list of press-release article URLs for *ticker*.

        This method leverages the undocumented Yahoo Finance *NCP* endpoint that is
        used by the quote page news tab.  The endpoint is fairly tolerant – it
        only needs the correct *listName* query parameter and a minimal JSON
        body.  We therefore:

        1.  Build the request URL of the form::

                https://finance.yahoo.com/xhr/ncp?location=US&queryRef=pressRelease&serviceKey=ncp_fin&listName={ticker}-press-releases&lang=en-US&region=US

        2.  Send a POST request with a *serviceConfig* payload similar to what
           the browser sends.  Most of the original payload (consent state,
           feature flags …) is irrelevant for server-side scraping, so we trim
           it down to the bare minimum: *count* (=max items) and *s* (the
           symbol list).

        3.  Extract all URLs from the JSON response in a best-effort manner – in
           practice the URLs live under
           ``response["data"]["main"]["stream_items"][*]["clickThroughUrl"]["url"]``
           but the structure is not guaranteed to stay the same.  A small
           recursive helper therefore walks the tree and collects every value
           that *looks* like a URL.

        Parameters
        ----------
        ticker:
            The stock symbol (case-insensitive).  Yahoo expects an upper-case
            ticker in the *listName* – we convert it for safety.

        Returns
        -------
        list[str]
            A deduplicated list of press-release URLs (may be empty if Yahoo
            returns no items or an error occurs).
        """

        def _extract_urls(node: Any) -> list[str]:  # noqa: ANN401 – dynamic JSON structure
            """Recursively collect URL-looking strings from *node*."""

            urls: list[str] = []

            if isinstance(node, dict):
                for v in node.values():
                    urls.extend(_extract_urls(v))
            elif isinstance(node, list):
                for item in node:
                    urls.extend(_extract_urls(item))
            elif isinstance(node, str):
                if node.startswith("http://") or node.startswith("https://"):
                    urls.append(node)

            return urls

        base_url = "https://finance.yahoo.com/xhr/ncp"

        params = {
            "location": "US",
            "queryRef": "pressRelease",
            "serviceKey": "ncp_fin",
            "listName": f"{ticker.upper()}-press-releases",
            "lang": "en-US",
            "region": "US",
        }

        payload = {
            "serviceConfig": {
                "count": 250,
                "spaceId": "95993639",
                # The endpoint expects a list of tickers in *s*
                "s": [ticker.upper()],
            }
        }

        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://finance.yahoo.com",
            "Referer": f"https://finance.yahoo.com/quote/{ticker}/press-releases/",
        }

        latest_pr: Optional[PressRelease] = None

        # A couple of retry attempts in case the cookie/crumb expired in between
        for attempt in range(2):
            try:
                resp = self.session.post(
                    base_url,
                    params=params,
                    json=payload,
                    headers=headers,
                    cookies={self.cookie.name: str(self.cookie.value)} if getattr(self, "cookie", None) else None,  # type: ignore[arg-type]
                    timeout=20,
                )
                resp.raise_for_status()

                data = resp.json()

                # Navigate to stream items
                stream_items = (
                    data.get("data", {})
                    .get("tickerStream", {})
                    .get("stream", [])
                )

                if stream_items:
                    # Assume the list is already ordered by recency (Yahoo returns newest first)
                    item0 = stream_items[0]
                    content = item0.get("content", {})

                    title = str(content.get("title", "")).strip()
                    pub_date = content.get("pubDate")
                    display_time = content.get("displayTime")

                    # Prefer canonicalUrl, fall back to clickThroughUrl
                    url_dict = content.get("canonicalUrl") or content.get("clickThroughUrl") or {}
                    url_val = url_dict.get("url") if isinstance(url_dict, dict) else None

                    if url_val:
                        latest_pr = PressRelease(
                            ticker=ticker,
                            title=title,
                            pub_date=pub_date,
                            display_time=display_time,
                            url=url_val,
                            type=type,
                        )

                break  # success – exit retry loop
            except requests.RequestException as exc:
                logger.warning("Failed to fetch press releases for %s (attempt %d): %s", ticker, attempt + 1, exc)
                if attempt == 0:
                    # On first failure attempt to refresh the crumb & cookie – then retry once
                    try:
                        self._refresh_cookie_and_crumb()
                    except Exception as refresh_exc:  # pragma: no-cover – defensive
                        logger.error("Failed to refresh Yahoo crumb: %s", refresh_exc)
                        break
                else:
                    break
            except ValueError as parse_exc:  # JSON decoding
                logger.error("Invalid JSON received while fetching press releases for %s: %s", ticker, parse_exc)
                break

        if latest_pr is None:
            logger.debug("No press release found for %s", ticker)

        return latest_pr


    def get_press_release_content(self, url: str) -> str:
        """Return the raw HTML string of the article body for a Yahoo press-release URL.

        Yahoo Finance does not expose a dedicated JSON/REST endpoint for press-
        release content.  Empirically, the human-readable HTML version embeds the
        full article inside a *div* with a fairly stable class name:

            <div class="atoms-wrapper">…article markup…</div>

        The outer DOM hierarchy (``#main-content-wrapper …``) tends to change
        between articles and over time, but the ``atoms-wrapper`` container has
        stayed consistent in dozens of samples.  We therefore:

        1.  Issue a simple *GET* request with the existing session/cookie/UA.
        2.  Parse the response with *BeautifulSoup* (``html.parser`` to avoid
            external dependencies).
        3.  Locate the first ``div`` with class *atoms-wrapper*.
        4.  Return its *inner HTML* (children serialized) **or** – as a
            fallback – the full response body when the wrapper cannot be found.

        Parameters
        ----------
        url:
            Canonical Yahoo Finance press-release URL obtained from
            :py:meth:`get_press_releases`.

        Returns
        -------
        str
            Raw HTML of the article body.  Empty string on error/network issues.
        """

        headers = {
            "User-Agent": _USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Referer": "https://finance.yahoo.com/",
        }

        try:
            resp = self.session.get(
                url,
                headers=headers,
                cookies={self.cookie.name: str(self.cookie.value)} if getattr(self, "cookie", None) else None,  # type: ignore[arg-type]
                timeout=20,
            )
            resp.raise_for_status()

            html_text = resp.text

            # ------------------------------------------------------------------
            # Detect EU consent interstitial (guce.yahoo.com). When present the
            # real article URL is embedded in a hidden input named
            # "originalDoneUrl".  We extract the value and re-fetch once.
            # ------------------------------------------------------------------
            if "id=\"consent-page\"" in html_text and "originalDoneUrl" in html_text:
                try:
                    from bs4 import BeautifulSoup
                    soup_consent = BeautifulSoup(html_text, "html.parser")
                    inp = soup_consent.find("input", attrs={"name": "originalDoneUrl"})
                    if inp is not None:
                        val = inp.get("value")  # type: ignore[attr-defined]
                        if not val:
                            raise ValueError("Missing value attribute")

                        import html

                        real_url = html.unescape(str(val))
                        logger.debug("Detected consent page – retrying with original URL: %s", real_url)
                        # Recursive single retry to avoid infinite loops
                        return self.get_press_release_content(real_url)
                except Exception as consent_exc:  # pragma: no cover
                    logger.debug("Failed to bypass consent page for %s: %s", url, consent_exc)

            # --- Extract the article body – best effort ----------------------------------
            try:
                from bs4 import BeautifulSoup  # Imported lazily to avoid hard dependency at module import time

                soup = BeautifulSoup(html_text, "html.parser")

                atoms_div = soup.find("div", class_="atoms-wrapper")
                if atoms_div is not None:
                    # Return children as HTML string *without* the wrapper div itself
                    return "".join(str(child) for child in atoms_div.contents)  # type: ignore[attr-defined]

                # ------------------------------------------------------------------
                # Fallback 2: Yahoo sometimes delivers the article as JSON inside a
                # <script id="caas-art-…" type="application/json"> tag.  The JSON
                # payload contains a top-level "content" field holding raw HTML.
                # ------------------------------------------------------------------
                script_tag = soup.find("script", id=lambda v: isinstance(v, str) and v.startswith("caas-art-"), attrs={"type": "application/json"})
                if script_tag is not None and getattr(script_tag, "string", None):  # type: ignore[attr-defined]
                    import json

                    try:
                        data = json.loads(script_tag.string)  # type: ignore[attr-defined]
                        if isinstance(data, dict):
                            content_html = data.get("content")
                            if isinstance(content_html, str) and content_html.strip():
                                logger.info("Successfully extracted article body from caas-art JSON for %s", url)
                                return content_html
                    except json.JSONDecodeError as json_exc:  # pragma: no cover – defensive
                        logger.debug("Failed to decode caas-art JSON for %s: %s", url, json_exc)

            except Exception as parse_exc:  # pragma: no cover – defensive, log & fallback
                logger.debug("Failed to parse atoms-wrapper for %s: %s", url, parse_exc)

            # Fallback: return the full page – the caller may handle further parsing
            return html_text
        except requests.RequestException as exc:
            logger.error("Failed to fetch press-release content from %s: %s", url, exc)
            return ""


    def save_press_release_to_db(self, pr: PressRelease) -> int | None:
        """Insert *pr* into ``press_releases`` table.

        Returns the auto-incremented row ID when a *new* record is inserted, or
        *None* if the URL already existed.  Any database exception bubbles up to
        the caller.
        """

        with get_db_connection() as conn:

            cursor = conn.cursor()

            # Use an UPSERT so that existing rows (matched by the UNIQUE url column)
            # are updated with the latest data rather than ignored.  We update every
            # column except the primary key and the automatically managed
            # timestamps, letting the `updated_at` trigger capture the modification
            # time.

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
                pr.ticker.upper(),
                pr.title,
                pr.url,
                pr.type,
                pr.pub_date,
                pr.display_time,
                pr.company_name,
                pr.raw_html,
                pr.text_content,
            )

            cursor.execute(sql, params)

            # Retrieve the row id of the affected record (inserted *or* updated)
            # The built-in SQLite function last_insert_rowid() returns the rowid
            # of the most recent successful INSERT.  If an UPDATE occurred we need
            # to look up the id by URL instead.

            row_id: int | None
            if cursor.lastrowid != 0:
                row_id = cursor.lastrowid
            else:
                cursor.execute("SELECT id FROM press_releases WHERE url = ?;", (pr.url,))
                fetched = cursor.fetchone()
                row_id = int(fetched[0]) if fetched is not None else None

            conn.commit()

            return row_id
