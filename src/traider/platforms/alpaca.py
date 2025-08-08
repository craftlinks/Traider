from typing import Optional, Callable, Set
import os
import threading
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockLatestTradeRequest, StockLatestQuoteRequest
from alpaca.data.live import StockDataStream
from alpaca.data.enums import DataFeed
from traider.interfaces.market_data import MarketDataInterface
from traider.models import Trade, Quote

class AlpacaMarketData(MarketDataInterface):
    """
    A wrapper for Alpaca's market data API.
    """

    def __init__(self, api_key: Optional[str] = None, secret_key: Optional[str] = None, feed: DataFeed = DataFeed.IEX):
        api_key_val = api_key or os.getenv("ALPACA_API_KEY")
        secret_key_val = secret_key or os.getenv("ALPACA_SECRET_KEY")
        if not api_key_val or not secret_key_val:
            raise ValueError("API key and secret key must be provided or set as environment variables.")

        # Save as non-Optional for type-checkers
        self.api_key: str = api_key_val
        self.secret_key: str = secret_key_val

        self.data_client = StockHistoricalDataClient(self.api_key, self.secret_key)
        self._feed: DataFeed = feed
        self._stream: StockDataStream | None = None
        self._stream_thread: threading.Thread | None = None
        self._subscribed_trades: Set[str] = set()
        self._subscribed_quotes: Set[str] = set()

    def get_latest_trade(self, symbol: str) -> Optional[Trade]:
        """
        Retrieves the latest trade for a given symbol.
        """
        try:
            request_params = StockLatestTradeRequest(symbol_or_symbols=symbol)
            latest_trade = self.data_client.get_stock_latest_trade(request_params)
            
            if latest_trade and symbol in latest_trade:
                trade_data = latest_trade[symbol]
                return Trade(
                    price=trade_data.price,
                    size=int(trade_data.size),
                    timestamp=trade_data.timestamp,
                    exchange=str(trade_data.exchange),
                )
            return None
        except Exception as e:
            print(f"Error fetching latest trade for {symbol}: {e}")
            return None

    def get_latest_quote(self, symbol: str) -> Optional[Quote]:
        """
        Retrieves the latest quote for a given symbol.
        """
        try:
            request_params = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            latest_quote = self.data_client.get_stock_latest_quote(request_params)

            if latest_quote and symbol in latest_quote:
                quote_data = latest_quote[symbol]
                return Quote(
                    bid_price=quote_data.bid_price,
                    bid_size=int(quote_data.bid_size),
                    ask_price=quote_data.ask_price,
                    ask_size=int(quote_data.ask_size),
                    timestamp=quote_data.timestamp,
                )
            return None
        except Exception as e:
            print(f"Error fetching latest quote for {symbol}: {e}")
            return None

    def _ensure_stream_running(self) -> None:
        if self._stream is None:
            self._stream = StockDataStream(self.api_key, self.secret_key, feed=self._feed)
        if self._stream_thread is None or not self._stream_thread.is_alive():
            self._stream_thread = threading.Thread(target=self._stream.run, daemon=True)
            self._stream_thread.start()

    def subscribe_trades(self, symbol: str, handler: Callable[[Trade], None]) -> None:
        self._ensure_stream_running()

        async def _on_trade(msg) -> None:
            trade = Trade(
                price=msg.price,
                size=int(msg.size),
                timestamp=msg.timestamp,
                exchange=str(getattr(msg, "exchange", "")),
            )
            try:
                handler(trade)
            except Exception as e:
                print(f"Trade handler error: {e}")

        # Register handler and track subscription
        assert self._stream is not None
        self._stream.subscribe_trades(_on_trade, symbol)
        self._subscribed_trades.add(symbol)

    def subscribe_quotes(self, symbol: str, handler: Callable[[Quote], None]) -> None:
        self._ensure_stream_running()

        async def _on_quote(msg) -> None:
            quote = Quote(
                bid_price=msg.bid_price,
                bid_size=int(msg.bid_size),
                ask_price=msg.ask_price,
                ask_size=int(msg.ask_size),
                timestamp=msg.timestamp,
            )
            try:
                handler(quote)
            except Exception as e:
                print(f"Quote handler error: {e}")

        assert self._stream is not None
        self._stream.subscribe_quotes(_on_quote, symbol)
        self._subscribed_quotes.add(symbol)

    def unsubscribe_trades(self, symbol: str) -> None:
        if self._stream is not None and symbol in self._subscribed_trades:
            self._stream.unsubscribe_trades(symbol)
            self._subscribed_trades.discard(symbol)
        self._maybe_stop_stream()

    def unsubscribe_quotes(self, symbol: str) -> None:
        if self._stream is not None and symbol in self._subscribed_quotes:
            self._stream.unsubscribe_quotes(symbol)
            self._subscribed_quotes.discard(symbol)
        self._maybe_stop_stream()

    def _maybe_stop_stream(self) -> None:
        if self._stream is None:
            return
        if not self._subscribed_trades and not self._subscribed_quotes:
            # Stop the stream and wait for the run future to complete
            self._stream.stop()
            if self._stream_thread is not None:
                self._stream_thread.join(timeout=5)
            self._stream = None
            self._stream_thread = None

    def close(self) -> None:
        # Forcefully stop regardless of subscriptions
        if self._stream is not None:
            self._stream.stop()
        if self._stream_thread is not None:
            self._stream_thread.join(timeout=5)
        self._stream = None
        self._stream_thread = None
