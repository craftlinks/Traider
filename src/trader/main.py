import logging
import time
from dotenv import load_dotenv
from trader.platforms import AlpacaMarketData
from trader.models import Trade, Quote

def main():
    load_dotenv()  # Load environment variables from .env file
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    from alpaca.data.enums import DataFeed

    # Use free IEX feed by default to avoid subscription errors
    alpaca_market_data = AlpacaMarketData(feed=DataFeed.IEX)

    # --- Latest Trade ---
    print("--- Synchronous Call: Latest Trade ---")
    trade_sync = alpaca_market_data.get_latest_trade("AAPL")
    if trade_sync:
        print(f"Latest trade for AAPL: {trade_sync}")

    print("\n--- Second Trade Snapshot ---")
    trade_second = alpaca_market_data.get_latest_trade("GOOG")
    if trade_second:
        print(f"Latest trade for GOOG: {trade_second}")

    # --- Latest Quote ---
    print("\n--- Synchronous Call: Latest Quote ---")
    quote_sync = alpaca_market_data.get_latest_quote("NVDA")
    if quote_sync:
        print(f"Latest quote for NVDA: {quote_sync}")
        print(f"  Spread: {round(quote_sync.ask_price - quote_sync.bid_price, 2)}")

    print("\n--- Second Quote Snapshot ---")
    quote_second = alpaca_market_data.get_latest_quote("MSFT")
    if quote_second:
        print(f"Latest quote for MSFT: {quote_second}")
        print(f"  Spread: {round(quote_second.ask_price - quote_second.bid_price, 2)}")

    # --- Streaming demo: subscribe, receive briefly, then unsubscribe ---
    print("\n--- Streaming: AAPL trades & quotes for 5s ---")

    def on_trade(trade: Trade) -> None:
        logging.info("TRADE %s: %s x %s", trade.timestamp, trade.price, trade.size)

    def on_quote(quote: Quote) -> None:
        logging.info(
            "QUOTE %s: bid %.2f x%d / ask %.2f x%d",
            quote.timestamp,
            quote.bid_price,
            quote.bid_size,
            quote.ask_price,
            quote.ask_size,
        )

    alpaca_market_data.subscribe_trades("AAPL", on_trade)
    alpaca_market_data.subscribe_quotes("AAPL", on_quote)
    time.sleep(1.0)
    alpaca_market_data.unsubscribe_trades("AAPL")
    alpaca_market_data.unsubscribe_quotes("AAPL")
    alpaca_market_data.close()
    logging.info("Done!")
    time.sleep(5)


if __name__ == "__main__":
    main()
