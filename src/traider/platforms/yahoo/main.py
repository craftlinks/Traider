from datetime import date
import logging
from typing import Any, Final, Optional

import requests
from traider.platforms.yahoo.helpers import extract_profile_data_html, extract_profile_data_json

# ---------------------------------------------------------------------------
# Configuration & constants
# ---------------------------------------------------------------------------

# Set up module-level logger
logger = logging.getLogger(__name__)

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
# YahooFinance class
# ---------------------------------------------------------------------------

class YahooFinance:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": _USER_AGENT})
        self.cookie, self.crumb = self._fetch_cookie_and_crumb()
        if self.cookie and self.crumb:
            logger.info(f"Successfully obtained crumb: {self.crumb}")
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
        self.cookie, self.crumb = self._fetch_cookie_and_crumb()
        if self.cookie and self.crumb:
            logger.info(f"Successfully refreshed crumb: {self.crumb}")
        else:
            raise RuntimeError("Unable to obtain Yahoo crumb token via any method.")

    def get_profile(self, ticker: str, from_json: bool = True) -> dict[str, Any]:
        html_url = _YF_PROFILE_TEMPLATE.format(ticker=ticker)
        json_url = _YF_PROFILE_JSON_TEMPLATE.format(ticker=ticker)

        if from_json:
            return self._get_profile_with_retry(json_url, ticker, from_json=True)
        else:
            return self._get_profile_with_retry(html_url, ticker, from_json=False)

    def _get_profile_with_retry(self, url: str, ticker: str, from_json: bool) -> dict[str, Any]:
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

                return {
                    "website_url": website_url,
                    "sector": sector,
                    "industry": industry,
                }
            except requests.RequestException as exc:
                if attempt < max_attempts - 1:
                    self._refresh_cookie_and_crumb()
                    logger.info(f"Failed to get profile for {ticker} (attempt {attempt + 1}/{max_attempts}): {exc}. Retrying...")
                else:
                    logger.info(f"Failed to get profile for {ticker} after {max_attempts} attempts: {exc}")
                    return {}

        return {}

    def get_earnings(self, start_date: date, end_date: Optional[date] = None) -> dict[str, Any]:
        url = YF_CALENDAR_URL_TEMPLATE.format(date=start_date)
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()