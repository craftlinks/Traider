from abc import ABC, abstractmethod
from typing import Optional, Callable
from traider.models import Trade, Quote

class MarketDataInterface(ABC):
    """
    An abstract interface for a market data provider.
    """

    @abstractmethod
    def get_latest_trade(self, symbol: str) -> Optional[Trade]:
        """
        Retrieves the latest trade for a given symbol.
        """
        pass

    @abstractmethod
    def get_latest_quote(self, symbol: str) -> Optional[Quote]:
        """
        Retrieves the latest quote for a given symbol.
        """
        pass

    # Streaming APIs
    @abstractmethod
    def subscribe_trades(self, symbol: str, handler: Callable[[Trade], None]) -> None:
        """
        Subscribe to real-time trades for a symbol. Handler is called in background thread.
        """
        pass

    @abstractmethod
    def subscribe_quotes(self, symbol: str, handler: Callable[[Quote], None]) -> None:
        """
        Subscribe to real-time quotes (bid/ask) for a symbol. Handler is called in background thread.
        """
        pass

    @abstractmethod
    def unsubscribe_trades(self, symbol: str) -> None:
        """
        Unsubscribe from real-time trades for a symbol.
        """
        pass

    @abstractmethod
    def unsubscribe_quotes(self, symbol: str) -> None:
        """
        Unsubscribe from real-time quotes for a symbol.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """
        Close any underlying streaming connections and release resources.
        """
        pass
