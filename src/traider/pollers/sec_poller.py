"""SEC 8-K filings Atom feed poller - refactored version."""

from __future__ import annotations

from datetime import datetime
import logging
from dataclasses import dataclass
from .common.base_poller import BaseItem, PollerConfig
from .common.specialized_pollers import AtomFeedPoller

# SEC parser import - assuming it exists
try:
    from traider.platforms.parsers.sec.sec_8k_parser import (
        get_filing_text_url,
        fetch_submission_text,
        analyze_and_extract_8k,
    )

    HAS_SEC_PARSER = True
except ImportError:
    HAS_SEC_PARSER = False
    # Provide stub functions to avoid unbound variable errors
    get_filing_text_url = None  # type: ignore
    fetch_submission_text = None  # type: ignore
    analyze_and_extract_8k = None  # type: ignore


logger = logging.getLogger(__name__)


# Configuration
ATOM_FEED_URL: str = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&owner=exclude&count=100&output=atom"


@dataclass
class SECItem(BaseItem):
    """SEC 8-K item enriched with structured metadata for downstream workers."""

    items: list[str] | None = None
    items_tier_map: dict[str, int] | None = None
    highest_item_tier: int | None = None
    primary_exhibit_type: str | None = None
    exhibits_found: list[str] | None = None
    has_material_contract_exhibit: bool = False
    fallback_used: bool = False
    acceptance_datetime_utc: datetime | None = None


class SECPoller(AtomFeedPoller):
    """SEC 8-K filings Atom feed poller with specialized parsing."""

    def __init__(self):
        config = PollerConfig.from_env(
            "SEC",
            default_interval=3,
            default_user_agent="TraderSECWatcher/1.0 admin@example.com",
            default_min_interval=0.2,  # SEC fair access: 5 req/sec
        )
        super().__init__(ATOM_FEED_URL, config)

    def get_poller_name(self) -> str:
        return "SEC 8-K"

    def handle_new_items(self, new_items: list[BaseItem]) -> None:
        """Override to emit SECItem with rich metadata along with chosen text.

        Falls back to the base implementation if the SEC parser is unavailable
        or if extraction is disabled via configuration.
        """
        if not new_items:
            return

        if (
            (not HAS_SEC_PARSER)
            or (
                get_filing_text_url is None
                or fetch_submission_text is None
                or analyze_and_extract_8k is None
            )
            or self.config.skip_extraction
        ):
            # Use the default behavior
            super().handle_new_items(new_items)
            return

        for base_item in new_items:
            article_text: str | None = None
            sec_item: BaseItem = base_item

            try:
                txt_url = get_filing_text_url(base_item.url, session=self.session)
                if not txt_url:
                    logger.warning(
                        "[PARSER] No .txt submission link found on index page."
                    )
                else:
                    submission_text = fetch_submission_text(
                        txt_url, session=self.session
                    )
                    if not submission_text:
                        logger.warning("[PARSER] Failed to fetch submission text.")
                    else:
                        result = analyze_and_extract_8k(
                            txt_url,
                            session=self.session,
                            prefetched_text=submission_text,
                        )
                        if not result:
                            logger.warning("[PARSER] Analysis failed.")
                        else:
                            # Choose the body text we emit
                            if result.primary_text:
                                article_text = result.primary_text
                            elif result.fallback_used and result.fallback_text:
                                article_text = result.fallback_text

                            # Enrich the item for downstream processing
                            sec_item = SECItem(
                                id=base_item.id,
                                title=base_item.title,
                                url=base_item.url,
                                timestamp=base_item.timestamp,
                                summary=base_item.summary,
                                items=result.items,
                                items_tier_map=result.items_tier_map,
                                highest_item_tier=result.highest_item_tier,
                                primary_exhibit_type=result.primary_exhibit_type,
                                exhibits_found=result.exhibits_found,
                                has_material_contract_exhibit=result.has_material_contract_exhibit,
                                fallback_used=result.fallback_used,
                                article_text=article_text,
                                acceptance_datetime_utc=result.acceptance_datetime_utc,
                            )
            except Exception as parse_exc:
                logger.exception("[PARSER] Unexpected parsing error: %s", parse_exc)

            # Emit downstream via sink if configured
            if self._sink is not None:
                try:
                    self._sink(self.get_poller_name(), sec_item)
                except Exception as sink_exc:
                    logger.exception("[SINK] Error while emitting item: %s", sink_exc)

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract SEC filing text using specialized parser if available."""
        if (
            not HAS_SEC_PARSER
            or get_filing_text_url is None
            or fetch_submission_text is None
            or analyze_and_extract_8k is None
        ):
            return super().extract_article_text(item)

        try:
            # Try to get the .txt submission URL
            txt_url = get_filing_text_url(item.url, session=self.session)
            if not txt_url:
                logger.warning("[PARSER] No .txt submission link found on index page.")
                return None

            # Fetch and analyze the submission
            submission_text = fetch_submission_text(txt_url, session=self.session)
            if not submission_text:
                logger.warning("[PARSER] Failed to fetch submission text.")
                return None

            # Analyze 8-K and extract exhibits
            result = analyze_and_extract_8k(
                txt_url, session=self.session, prefetched_text=submission_text
            )
            if not result:
                logger.warning("[PARSER] Analysis failed.")
                return None

            # Return the extracted text
            if result.primary_text:
                return result.primary_text
            elif result.fallback_used and result.fallback_text:
                return result.fallback_text
            else:
                return None

        except Exception as parse_exc:
            logger.exception("[PARSER] Unexpected parsing error: %s", parse_exc)
            return None


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the SEC 8-K filings poller."""
    poller = SECPoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()
