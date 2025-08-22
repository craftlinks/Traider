from datetime import date, timedelta
from traider.platforms.yahoo.main import YahooFinance
from traider.db.database import get_db_connection, create_tables
from .helpers import get_earnings_for_date_range, get_earnings_tickers_for_date_range, save_earnings_data
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


def earnings_main():

    logger.info("Initializing database…")
    create_tables()
    
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
    




if __name__ == "__main__": 
    earnings_main()