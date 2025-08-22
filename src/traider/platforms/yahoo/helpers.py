from typing import Any, Optional, Tuple

from bs4 import BeautifulSoup
import pandas as pd 
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('earnings_collection.log')
    ]
)

def extract_profile_data_html(html: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse the Yahoo profile HTML and return (website_url, sector, industry).

    All values may be *None* if the corresponding element could not be
    located.
    """

    soup = BeautifulSoup(html, "html.parser")

    # --- Website URL --------------------------------------------------------
    website_url: Optional[str] = None
    website_tag = soup.select_one("a[data-ylk*='business-url']")
    if website_tag is not None:
        # The visible text already contains the fully-qualified URL
        website_url = website_tag.get_text(strip=True)
        # Occasionally the anchor text contains an ellipsis while the full URL
        # is stored inside the *href*.  Prefer *href* in that case.
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
    dt_industry = soup.find("dt", string=lambda s: isinstance(s, str) and "Industry" in s)
    if dt_industry is not None:
        industry_anchor = dt_industry.find_next("a")
        if industry_anchor is not None:
            industry = industry_anchor.get_text(strip=True)

    return website_url, sector, industry

def extract_profile_data_json(json: dict[str, Any]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Parse the Yahoo profile JSON and return (website_url, sector, industry).

    All values may be *None* if the corresponding element could not be
    located.
    """

    profile = (
                json.get("quoteSummary", {})
                .get("result", [{}])[0]  # type: ignore[index]
                .get("assetProfile", {})
            )
    website = profile.get("website")
    sector = profile.get("sector")
    industry = profile.get("industry")

    return website, sector, industry


def extract_earnings_data_json(api_payload: dict[str, Any]) -> pd.DataFrame:
    """Parse the Yahoo earnings JSON and return a DataFrame."""
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
            df["Earnings Call Time"] = pd.to_datetime(col, unit="ms", utc=True)
        else:
            # ISO‐8601 strings like 2025-08-14T04:00:00.000Z
            df["Earnings Call Time"] = pd.to_datetime(col, utc=True, errors="coerce")

    # Ensure numeric columns are typed correctly
    for col in ["EPS Estimate", "Reported EPS", "Surprise (%)", "Market Cap"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info(f"Successfully fetched {len(df)} earnings rows.")
    return df