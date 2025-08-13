"""Newswire.com RSS feed poller - refactored version."""
from __future__ import annotations

from .common.base_poller import PollerConfig
from .common.specialized_pollers import RSSFeedPoller


# Configuration
RSS_FEED_URL: str = "https://www.newswire.com/newsroom/rss"


class NewsroomPoller(RSSFeedPoller):
    """Newswire.com RSS feed poller."""
    
    def __init__(self):
        config = PollerConfig.from_env(
            "NWR",
            default_interval=5,
            default_user_agent="TraderNWRWatcher/1.0 admin@example.com",
            default_min_interval=0.25
        )
        super().__init__(RSS_FEED_URL, config)

    def get_poller_name(self) -> str:
        return "Newswire.com"


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the Newswire.com RSS poller."""
    poller = NewsroomPoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()