"""Access Newswire API poller - refactored version."""
from __future__ import annotations

import re
from dataclasses import dataclass
import logging
from typing import Any, Dict, List
from urllib.parse import urljoin

from .common.base_poller import BaseItem, PollerConfig
from .common.poller_utils import strip_tags
from .common.specialized_pollers import APIPoller

logger = logging.getLogger(__name__)


# Configuration
API_URL: str = "https://www.accessnewswire.com/newsroom/api"
BASE_URL: str = "https://www.accessnewswire.com"
DEFAULT_PAGE_SIZE: int = 20


@dataclass
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
        # No container-specific extraction; rely on default_simple_text_extractor

    def get_poller_name(self) -> str:
        return "Access Newswire"

    def parse_api_items(self, data: Dict[str, Any]) -> List[BaseItem]:
        """Parse Access Newswire API response."""
        items = []
        
        articles = data.get("data", {}).get("articles", [])
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
                logger.warning("Skipping article due to parsing error: %s", e)
                continue
                
        return items

    # Use base APIPoller.extract_article_text with default_simple_text_extractor


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the Access Newswire poller."""
    import warnings
    warnings.warn("Access Newswire poller is currently broken and will not run.", RuntimeWarning, stacklevel=2)
    return
    # poller = AccessNewswirePoller()
    # poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()