
import asyncio
from datetime import date, timedelta
from crawlee import Request
from modules.earnings.crawlee import crawler
from traider.platforms.yahoo.main import YahooFinance
from traider.db.database import get_db_connection, create_tables
from .helpers import fetch_urls_from_db, get_earnings_for_date_range, get_earnings_tickers_for_date_range, save_earnings_data, save_profile_to_db
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('earnings_collection.log')
    ]
)


async def earnings_main():

    logger.info("Initializing database…")
    create_tables()

    # Run crawler to get press releases urls, standby mode
    crawler_task = asyncio.create_task(crawler.run())
    
    #---- Step 1: Get earnings data and save to database ----

    today = date.today()
    yesterday = today - timedelta(days=1)
    tomorrow = today + timedelta(days=1)

    yf = YahooFinance()
    all_earnings_df = get_earnings_for_date_range(yesterday, tomorrow, yf)

    if all_earnings_df.empty:
        return

    try:
        with get_db_connection() as db_conn:
            save_earnings_data(all_earnings_df, db_conn)
    except Exception as e:
        logger.error(f"Failed to save earnings data to database: {e}")
        raise

    #---- Step 2: Retrieve Press Releases urls for companies with earnings data ----
    with get_db_connection() as db_conn:
        tickers = get_earnings_tickers_for_date_range(db_conn, today, tomorrow)
        logger.info(f"Found {len(tickers)} tickers with earnings data for today")
        logger.info(tickers)
    
    if len(tickers) == 0:
        logger.info("No tickers with earnings data for today")
        return
    
    # Fetch and store missing press releases urls and homepage urls
    # TODO Geert: cleanup this code
    with get_db_connection() as db_conn:
        for ticker in tickers:
            press_releases_urls = fetch_urls_from_db(db_conn, [ticker], "press_releases")

            if not press_releases_urls:
                logger.info(f"No press releases urls found in our database for {ticker}, fetching homepage url")
                homepage_urls = fetch_urls_from_db(db_conn, [ticker], "website")
                if homepage_urls:
                    logger.info(f"Homepage urls found in our databasefor {ticker}: {homepage_urls}")
                else:
                    logger.info(f"No homepage urls found in our database for {ticker}, fetching from Yahoo Finance")
                    profile = yf.get_profile(ticker)
                    if profile:
                        save_profile_to_db(ticker, profile, db_conn)
                        homepage_urls = profile.website_url
                        if not homepage_urls:
                            logger.error(f"No homepage url found for {ticker} in Yahoo Finance, skipping")
                            continue
                        else:
                            logger.info(f"Homepage url found for {ticker} in Yahoo Finance: {homepage_urls}")
                    else:
                        logger.error(f"No profile found for {ticker} in Yahoo Finance, skipping")
                        continue
                try:
                    request = Request.from_url(homepage_urls[0], label='press_releases', user_data={'ticker': ticker})
                    await crawler.add_requests([request])
                    logger.info(f"Added homepage url for {ticker} to crawler for searching press releases urls")
                except Exception as e:
                    logger.error(f"Crawler execution failed: {e}")
                    continue

    with get_db_connection() as db_conn:
        press_releases_urls = fetch_urls_from_db(db_conn, tickers, "press_releases")
        logger.info(f"Press releases urls to be polled for earnings: {press_releases_urls}")

    # TODO Geert: poll the press releases urls for earnings now


if __name__ == "__main__": 
    asyncio.run(earnings_main())