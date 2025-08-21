from typing import Any, Optional, Tuple

from bs4 import BeautifulSoup

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
