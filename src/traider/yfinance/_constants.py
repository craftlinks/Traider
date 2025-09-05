# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
from typing import Final


_USER_AGENT: Final[str] = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/121.0.0.0 Safari/537.36"
)

_YF_PROFILE_TEMPLATE: Final[str] = (
    "https://finance.yahoo.com/quote/{ticker}/profile?p={ticker}"
)
_YF_PROFILE_JSON_TEMPLATE: Final[str] = (
    "https://query2.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=assetProfile&lang=en-US&region=US"
)
YF_CALENDAR_URL_TEMPLATE: Final[str] = (
    "https://finance.yahoo.com/calendar/earnings?day={date}"
)
YF_VISUALIZATION_API: Final[str] = (
    "https://query1.finance.yahoo.com/v1/finance/visualization?lang=en-US&region=US&crumb={crumb}"
)
_REQUEST_DELAY_S: Final[float] = 1.0  # politeness delay (seconds)
