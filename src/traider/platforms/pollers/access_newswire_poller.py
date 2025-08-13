"""Access Newswire API poller - refactored version."""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List
from urllib.parse import urljoin

from .common.base_poller import BaseItem, PollerConfig
from .common.poller_utils import strip_tags, extract_primary_text_from_html
from .common.specialized_pollers import APIPoller


# Configuration
API_URL: str = "https://www.accessnewswire.com/newsroom/api"
BASE_URL: str = "https://www.accessnewswire.com"
DEFAULT_PAGE_SIZE: int = 20


@dataclass(frozen=True)
class ANWItem(BaseItem):
    """Access Newswire specific item with additional fields."""
    company: str | None = None
    topics: List[str] | None = None


class AccessNewswirePoller(APIPoller):
    """Access Newswire API poller."""
    
    def __init__(self, page_size: int = DEFAULT_PAGE_SIZE):
        config = PollerConfig.from_env(
            "ANW", 
            default_interval=3,
            default_user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            default_min_interval=0.25
        )
        
        # Add page size parameter to API URL
        api_url_with_params = f"{API_URL}?pageindex=0&pageSize={page_size}"
        
        extra_headers = {
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9", 
            "Referer": urljoin(BASE_URL, "newsroom"),
            "X-Requested-With": "XMLHttpRequest",
            "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        }
        
        super().__init__(api_url_with_params, config, use_cloudscraper=True, extra_headers=extra_headers)
        self.container_patterns = [r'<div class="mw-100">([\s\S]*?)</div>']

    def get_poller_name(self) -> str:
        return "Access Newswire"

    def parse_api_items(self, json_data: Dict[str, Any]) -> List[BaseItem]:
        """Parse Access Newswire API response."""
        items = []
        
        articles = json_data.get("data", {}).get("articles", [])
        if not isinstance(articles, list):
            return items

        for article_data in articles:
            try:
                item = ANWItem(
                    id=str(article_data["id"]),
                    title=article_data["title"],
                    url=article_data["releaseurl"],
                    summary=strip_tags(article_data.get("body", "")),
                    timestamp=article_data.get("adate"),
                    company=article_data.get("company"),
                    topics=article_data.get("topics"),
                )
                items.append(item)
            except (KeyError, TypeError) as e:
                print(f"Skipping article due to parsing error: {e}")
                continue
                
        return items

    def display_item(self, item: BaseItem) -> None:
        """Display Access Newswire specific item information."""
        super().display_item(item)
        if isinstance(item, ANWItem):
            if item.company:
                print(f"     COMPANY: {item.company}")
            if item.topics:
                print(f"     TOPICS: {', '.join(item.topics)}")

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract article text using Access Newswire specific container."""
        headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
        response = self.session.get(item.url, headers=headers, timeout=self.config.article_timeout_seconds)
        response.raise_for_status()
        return extract_primary_text_from_html(response.text, self.container_patterns)


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the Access Newswire poller."""
    poller = AccessNewswirePoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()