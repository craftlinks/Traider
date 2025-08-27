"""PR Newswire HTML poller - refactored version."""
from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
import logging
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from .common.base_poller import BaseItem, PollerConfig  
from .common.poller_utils import strip_tags
from .common.specialized_pollers import HTMLPoller
logger = logging.getLogger(__name__)



def _parse_et_time_to_utc(time_str: str | None) -> datetime.datetime | None:
    """Parse a timestamp string like 'Aug 15, 2025, 17:04 ET' to a UTC datetime."""
    if not time_str:
        return None

    # Expects "MONTH DAY, YEAR, HH:MM ET"
    cleaned_str = time_str.replace(" ET", "").strip()

    try:
        # Parse the naive datetime
        naive_dt = datetime.datetime.strptime(cleaned_str, "%b %d, %Y, %H:%M")

        # Localize it to America/New_York, which handles EDT/EST automatically
        et_zone = ZoneInfo("America/New_York")
        aware_dt_et = naive_dt.replace(tzinfo=et_zone)

        # Convert to UTC
        return aware_dt_et.astimezone(datetime.timezone.utc)
    except ValueError:
        logger.warning("Could not parse timestamp from PR Newswire: '%s'", time_str)
        return None
    except Exception:
        logger.warning("Failed to parse time string: '%s'", time_str, exc_info=True)
        return None


# Configuration
LIST_URL: str = (
    "https://www.prnewswire.com/news-releases/financial-services-latest-news/earnings-list/?page=1&pagesize=10"
)
BASE_URL: str = "https://www.prnewswire.com"


class PRNewswirePoller(HTMLPoller):
    """PR Newswire earnings list HTML poller."""
    
    def __init__(self):
        config = PollerConfig.from_env(
            "PRN",
            default_interval=3,
            default_user_agent="TraderPRNWatcher/1.0 admin@example.com",
            default_min_interval=0.25
        )
        
        super().__init__(LIST_URL, config)

    def get_poller_name(self) -> str:
        return "PR Newswire"

    def parse_html_items(self, html: str) -> list[BaseItem]:
        """Parse PR Newswire earnings list HTML."""
        items = []

        # Find card anchors with H3 and P content
        card_pattern = re.compile(
            r"<a[^>]+class=\"[^\"]*newsreleaseconsolidatelink[^\"]*\"[^>]+href=\"([^\"]+)\"[^>]*>[\s\S]*?<h3[^>]*>([\s\S]*?)</h3>[\s\S]*?(?:<p[^>]*class=\"[^\"]*remove-outline[^\"]*\"[^>]*>([\s\S]*?)</p>)?",
            flags=re.IGNORECASE,
        )

        for match in card_pattern.finditer(html):
            href_rel = (match.group(1) or "").strip()
            h3_html = match.group(2) or ""
            p_html = match.group(3) or ""

            if not href_rel:
                continue
                
            url = urljoin(BASE_URL, href_rel)

            # Extract time from <small> tag within h3
            time_match = re.search(r"<small[^>]*>([\s\S]*?)</small>", h3_html, flags=re.IGNORECASE)
            time_et_str = None
            if time_match:
                time_et_str = strip_tags(time_match.group(1)) or None
                h3_html = h3_html.replace(time_match.group(0), " ")
                
            title = strip_tags(h3_html)
            summary = strip_tags(p_html) if p_html else None

            items.append(BaseItem(
                id=url,  # Use URL as stable ID
                title=title,
                url=url, 
                summary=summary,
                timestamp=_parse_et_time_to_utc(time_et_str)
            ))

        return items


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the PR Newswire poller."""
    poller = PRNewswirePoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()