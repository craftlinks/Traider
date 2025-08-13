"""PR Newswire HTML poller - refactored version."""
from __future__ import annotations

import re
from dataclasses import dataclass
import logging
from urllib.parse import urljoin

from .common.base_poller import BaseItem, PollerConfig  
from .common.poller_utils import strip_tags
from .common.specialized_pollers import HTMLPoller
logger = logging.getLogger(__name__)



# Configuration
LIST_URL: str = (
    "https://www.prnewswire.com/news-releases/financial-services-latest-news/earnings-list/?page=1&pagesize=10"
)
BASE_URL: str = "https://www.prnewswire.com"


@dataclass(frozen=True) 
class PRNItem(BaseItem):
    """PR Newswire specific item with time in ET."""
    time_et: str | None = None


class PRNewswirePoller(HTMLPoller):
    """PR Newswire earnings list HTML poller."""
    
    def __init__(self):
        config = PollerConfig.from_env(
            "PRN",
            default_interval=3,
            default_user_agent="TraderPRNWatcher/1.0 admin@example.com",
            default_min_interval=0.25
        )
        
        # PR Newswire specific container patterns for article extraction
        container_patterns = [
            r'<(div|section)[^>]+class="[^\"]*(release-body|articleBody|story-body|post-content)[^\"]*\"[^>]*>([\s\S]*?)</\1>',
        ]
        
        super().__init__(LIST_URL, container_patterns, config)

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
            time_et = None
            if time_match:
                time_et = strip_tags(time_match.group(1)) or None
                h3_html = h3_html.replace(time_match.group(0), " ")
                
            title = strip_tags(h3_html)
            summary = strip_tags(p_html) if p_html else None

            items.append(PRNItem(
                id=url,  # Use URL as stable ID
                title=title,
                url=url, 
                summary=summary,
                time_et=time_et
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