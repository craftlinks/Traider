"""Business Wire HTML poller - refactored version."""
from __future__ import annotations

import re
from urllib.parse import urljoin

from .common.base_poller import BaseItem, PollerConfig
from .common.poller_utils import strip_tags
from .common.specialized_pollers import HTMLPoller


# Configuration
LIST_URL: str = "https://www.businesswire.com/newsroom"
BASE_URL: str = "https://www.businesswire.com"


class BusinessWirePoller(HTMLPoller):
    """Business Wire HTML scraping poller."""
    
    def __init__(self):
        config = PollerConfig.from_env(
            "BW",
            default_interval=3,
            default_user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            default_min_interval=0.25
        )
        
        # Extra headers to mimic browser behavior
        extra_headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate", 
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }
        super().__init__(LIST_URL, config, use_cloudscraper=True, extra_headers=extra_headers)

    def get_poller_name(self) -> str:
        return "Business Wire"

    def parse_html_items(self, html: str) -> list[BaseItem]:
        """Parse Business Wire newsroom HTML."""
        items = []

        # Find the news items list
        news_items_scope = re.search(r'<ul class="bw-news-items">([\s\S]*?)</ul>', html, re.IGNORECASE)
        if not news_items_scope:
            return items

        # Extract individual news items
        news_items = re.findall(
            r'<li class="bw-news-item">([\s\S]*?)</li>', 
            news_items_scope.group(1), 
            re.IGNORECASE
        )

        for item_html in news_items:
            href_match = re.search(r'href="([^"]+)"', item_html)
            title_match = re.search(r"<h2>([^<]+)</h2>", item_html)
            summary_match = re.search(r'<div class="bw-news-item-snippet">([\s\S]*?)</div>', item_html)
            ts_match = re.search(r'<time datetime="([^"]+)">', item_html)

            if not href_match or not title_match:
                continue

            url = urljoin(BASE_URL, href_match.group(1).strip())
            title = strip_tags(title_match.group(1))
            summary = strip_tags(summary_match.group(1)) if summary_match else None
            timestamp = ts_match.group(1).strip() if ts_match else None

            items.append(BaseItem(
                id=url,  # Use URL as ID since it's unique
                title=title,
                url=url,
                summary=summary,
                timestamp=timestamp
            ))

        return items


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the Business Wire poller."""
    import warnings
    warnings.warn("Business Wire poller is currently broken and will not run.", RuntimeWarning, stacklevel=2)
    return
    # poller = BusinessWirePoller()
    # poller.run(polling_interval_seconds, user_agent)

if __name__ == "__main__":
    run_poller()