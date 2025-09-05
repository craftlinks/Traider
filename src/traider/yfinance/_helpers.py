import httpx
from typing import Any, Optional
import pandas as pd
import logging
import math
from bs4 import BeautifulSoup
from datetime import datetime

from ._constants import _USER_AGENT
from ._models import EarningsEvent

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module level mutable state – *private*
# ---------------------------------------------------------------------------
# Using a singleton to bundle mutable connection state.
from types import SimpleNamespace


class _YahooState(SimpleNamespace):
    session: Optional[httpx.AsyncClient] = None
    cookie: Optional[Any] = None
    crumb: Optional[str] = None


Y_STATE = _YahooState()

# ---------------------------------------------------------------------------
# Helper functions


def _to_float(val: Any) -> float:
    """Best-effort conversion to *float* returning ``nan`` on failure."""
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return float("nan")
        return float(val)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return float("nan")


async def _initialize_session() -> None:
    """Internal logic for creating the session and fetching the crumb."""
    if Y_STATE.session is not None:
        return  # idempotent
    # Create client
    # Use a single HTTP/2 client with a shared connection pool & built-in retries.
    transport = httpx.AsyncHTTPTransport(retries=3)
    Y_STATE.session = httpx.AsyncClient(
        follow_redirects=True,
        http2=True,
        transport=transport,
        headers={"User-Agent": _USER_AGENT},
    )

    cookie, crumb = await _fetch_cookie_and_crumb(Y_STATE.session)
    if cookie and crumb:
        Y_STATE.cookie, Y_STATE.crumb = cookie, crumb
        logger.debug("Successfully obtained Yahoo crumb: %s", crumb)
    else:
        await Y_STATE.session.aclose()
        Y_STATE.session = None
        raise RuntimeError("Unable to obtain Yahoo crumb token.")


async def _get_session() -> httpx.AsyncClient:
    if Y_STATE.session is None:
        logger.debug("Lazy-initialising Yahoo Finance session …")
        await _initialize_session()
    return Y_STATE.session  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Private helpers – cookie / crumb handling
# ---------------------------------------------------------------------------


async def _fetch_cookie_and_crumb(
    session: httpx.AsyncClient, *, timeout: int = 30
) -> tuple[Any | None, str | None]:
    """Retrieve Yahoo's **A3** cookie + crumb anti-CSRF token."""
    headers = {"User-Agent": _USER_AGENT}
    try:
        resp = await session.get(
            "https://fc.yahoo.com",
            headers=headers,
            timeout=timeout,
            follow_redirects=True,
        )
        if not resp.cookies:
            return None, None

        cookie = next(iter(resp.cookies.jar), None)
        if cookie is None:
            return None, None

        crumb_resp = await session.get(
            "https://query1.finance.yahoo.com/v1/test/getcrumb",
            headers=headers,
            cookies={cookie.name: str(cookie.value)},
            timeout=timeout,
        )
        crumb_txt = crumb_resp.text.strip()
        if not crumb_txt or "<html>" in crumb_txt:
            return cookie, None
        return cookie, crumb_txt
    except Exception:  # pragma: no cover – network problems swallow silently
        return None, None


async def _refresh_cookie_and_crumb() -> None:
    if Y_STATE.session is None:
        await _initialize_session()
        return
    cookie, crumb = await _fetch_cookie_and_crumb(Y_STATE.session)
    if cookie and crumb:
        Y_STATE.cookie, Y_STATE.crumb = cookie, crumb
        logger.debug("Successfully refreshed Yahoo crumb: %s", crumb)
    else:
        logger.warning(
            "Failed to refresh crumb with existing session, re-initializing."
        )
        try:
            await Y_STATE.session.aclose()
        except Exception:
            pass
        Y_STATE.session = None
        await _initialize_session()


# ---------------------------------------------------------------------------
# Internal helpers (dataframe → dataclass etc.)
# ---------------------------------------------------------------------------


def _df_to_events(df: pd.DataFrame) -> list[EarningsEvent]:
    """Convert the *pandas* DataFrame from :pyfunc:`extract_earnings_data_json`."""
    events: list[EarningsEvent] = []
    if df.empty:
        return events

    for _, series in df.iterrows():
        try:
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
        except Exception as exc:  # pragma: no cover – defensive
            logger.debug(
                "Failed to convert earnings row: %s; data=%s", exc, series.to_dict()
            )
    return events


# ---------------------------------------------------------------------------
# Helper extraction functions (migrated from OLD_helpers.py)
# ---------------------------------------------------------------------------


def _extract_profile_data_html(
    html: str,
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse Yahoo profile HTML and return (website_url, sector, industry)."""
    soup = BeautifulSoup(html, "html.parser")

    # --- Website URL --------------------------------------------------------
    website_url: Optional[str] = None
    website_tag = soup.select_one("a[data-ylk*='business-url']")
    if website_tag is not None:
        # The visible text already contains the fully-qualified URL
        website_url = website_tag.get_text(strip=True)
        # Occasionally the anchor text contains an ellipsis while the full URL
        # is stored inside the *href*. Prefer *href* in that case.
        href_val = website_tag.get("href")
        if isinstance(href_val, str) and href_val.startswith("http"):
            website_url = href_val.strip()

    # --- Sector -------------------------------------------------------------
    sector: Optional[str] = None
    dt_sector = soup.find("dt", string=lambda s: isinstance(s, str) and "Sector" in s)
    if dt_sector is not None:
        sector_anchor = dt_sector.find_next("a")
        if sector_anchor is not None:
            sector = sector_anchor.get_text(strip=True)

    # --- Industry -----------------------------------------------------------
    industry: Optional[str] = None
    dt_industry = soup.find(
        "dt", string=lambda s: isinstance(s, str) and "Industry" in s
    )
    if dt_industry is not None:
        industry_anchor = dt_industry.find_next("a")
        if industry_anchor is not None:
            industry = industry_anchor.get_text(strip=True)

    return website_url, sector, industry


def _extract_profile_data_json(
    json: dict[str, Any],
) -> tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse Yahoo profile JSON and return (website_url, sector, industry)."""
    profile = (
        json.get("quoteSummary", {}).get("result", [{}])[0].get("assetProfile", {})
    )
    website = profile.get("website")
    sector = profile.get("sector")
    industry = profile.get("industry")
    return website, sector, industry


def _extract_earnings_data_json(api_payload: dict[str, Any]) -> pd.DataFrame:
    """Parse the Yahoo earnings JSON and return a DataFrame."""
    documents: list[dict] = (
        api_payload.get("finance", {}).get("result", [{}])[0].get("documents", [])
    )
    if not documents:
        logger.info("No earnings rows returned by Yahoo.")
        return pd.DataFrame()

    doc = documents[0]
    rows = doc.get("rows", [])
    columns_meta = doc.get("columns", [])
    if not rows or not columns_meta:
        logger.info("Unexpected response structure – rows or columns missing.")
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

    logger.debug("Successfully fetched %d earnings rows.", len(df))
    return df


# ---------------------------------------------------------------------------
# Validation helpers (adapted from original class methods)
# ---------------------------------------------------------------------------


def _validate_ticker(ticker: str) -> str | None:
    if not ticker or len(ticker) > 10:
        return None
    cleaned = "".join(c for c in ticker if c.isalnum() or c in ".-")
    return cleaned.upper() if cleaned else None


def _validate_company_name(name: Any) -> str | None:
    if pd.isna(name) or not name:
        return None
    name_str = str(name).strip()
    return name_str if name_str and len(name_str) <= 200 else None


def _validate_datetime(dt: Any) -> str | None:
    if pd.isna(dt):
        return None
    try:
        if isinstance(dt, (pd.Timestamp, datetime)):
            return dt.isoformat()
        if isinstance(dt, str):
            parsed = pd.to_datetime(dt, utc=True, errors="coerce")
            if pd.notna(parsed):
                return parsed.isoformat()
    except Exception:  # pragma: no cover
        pass
    return None


def _validate_numeric(val: Any) -> float | None:
    if pd.isna(val):
        return None
    try:
        num = float(val)
        return None if abs(num) > 1e12 else num
    except (ValueError, TypeError):
        return None


def _validate_string(val: Any) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    return s if s and len(s) <= 500 else None
