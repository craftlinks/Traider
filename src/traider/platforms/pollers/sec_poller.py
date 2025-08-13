"""SEC 8-K filings Atom feed poller - refactored version."""
from __future__ import annotations

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


# Configuration
ATOM_FEED_URL: str = (
    "https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK=&type=8-K&owner=exclude&count=100&output=atom"
)


class SECPoller(AtomFeedPoller):
    """SEC 8-K filings Atom feed poller with specialized parsing."""
    
    def __init__(self):
        config = PollerConfig.from_env(
            "SEC",
            default_interval=3,
            default_user_agent="TraderSECWatcher/1.0 admin@example.com", 
            default_min_interval=0.2  # SEC fair access: 5 req/sec
        )
        super().__init__(ATOM_FEED_URL, config)

    def get_poller_name(self) -> str:
        return "SEC 8-K"

    def extract_article_text(self, item: BaseItem) -> str | None:
        """Extract SEC filing text using specialized parser if available."""
        if not HAS_SEC_PARSER:
            return super().extract_article_text(item)
        
        try:
            # Try to get the .txt submission URL
            txt_url = get_filing_text_url(item.url, session=self.session)
            if not txt_url:
                print("   [PARSER] No .txt submission link found on index page.")
                return None
                
            # Fetch and analyze the submission
            submission_text = fetch_submission_text(txt_url, session=self.session)
            if not submission_text:
                print("   [PARSER] Failed to fetch submission text.")
                return None

            # Analyze 8-K and extract exhibits
            result = analyze_and_extract_8k(txt_url, session=self.session, prefetched_text=submission_text)
            if not result:
                print("   [PARSER] Analysis failed.")
                return None

            self._display_sec_analysis(result)
            
            # Return the extracted text
            if result.primary_text:
                return result.primary_text
            elif result.fallback_used and result.fallback_text:
                return result.fallback_text
            else:
                return None
                
        except Exception as parse_exc:
            print(f"   [PARSER] Unexpected parsing error: {parse_exc}")
            return None

    def _display_sec_analysis(self, result) -> None:
        """Display SEC-specific analysis results."""
        if result.items:
            print(f"   [ITEMS] Detected 8-K Items: {result.items}")
        else:
            print("   [ITEMS] No explicit 8-K item matches found near the top.")

        if result.has_material_contract_exhibit:
            print("   [FLAG] Material contract-related exhibit present (EX-10.1/EX-2.1).")

        if result.highest_item_tier is not None:
            print(f"   [TIER] Highest detected item tier: {result.highest_item_tier}")

        if result.acceptance_datetime_utc:
            print(f"   [TIME] SEC acceptance (UTC): {result.acceptance_datetime_utc}")

        if result.primary_text:
            print(f"   [PARSER] {result.primary_exhibit_type or 'EX-99.x'} extracted.")
        elif result.fallback_used and result.fallback_text:
            print("   [FALLBACK] Used 8-K body text due to high-impact item.")
        else:
            print("   [PARSER] No narrative exhibit or fallback text extracted.")

    def display_article_text(self, item: BaseItem, article_text: str | None) -> None:
        """Override to not duplicate SEC parser output."""
        if article_text and HAS_SEC_PARSER:
            preview = article_text[:200].replace("\n", " ")
            print(f"     Preview: {preview}...")
        else:
            super().display_article_text(item, article_text)


def run_poller(
    polling_interval_seconds: int | None = None,
    user_agent: str | None = None,
) -> None:
    """Run the SEC 8-K filings poller."""
    poller = SECPoller()
    poller.run(polling_interval_seconds, user_agent)


if __name__ == "__main__":
    run_poller()