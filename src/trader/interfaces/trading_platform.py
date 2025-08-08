from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional

from trader.models import Contract, Order, MarketData


class TradingPlatform(ABC):
    """
    An abstract interface for a trading platform.
    """

    @abstractmethod
    def connect(self, host: str, port: int, client_id: int):
        """Connect to the trading platform."""
        pass

    @abstractmethod
    def disconnect(self):
        """Disconnect from the trading platform."""
        pass

    @abstractmethod
    def get_account_summary(self) -> Dict[str, Any]:
        """Retrieve account summary information."""
        pass

    @abstractmethod
    def buy(self, contract: Contract, order: Order) -> int:
        """Places a buy order and returns the assigned order ID."""
        pass

    @abstractmethod
    def sell(self, contract: Contract, order: Order) -> int:
        """Places a sell order and returns the assigned order ID."""
        pass

    @abstractmethod
    def get_open_orders(self, timeout_seconds: float = 5.0) -> List[Order]:
        """Retrieves all open orders (with a timeout to avoid hangs)."""
        pass

    @abstractmethod
    def cancel_order(self, order_id: int) -> None:
        """Cancels an existing order."""
        pass

    @abstractmethod
    def modify_order(self, order_id: int, order: Order) -> None:
        """Modifies an existing order."""
        pass

    @abstractmethod
    def get_market_data(self, contract: Contract) -> Optional[MarketData]:
        """
        Retrieves market data for a given contract.
        """
        pass

    @abstractmethod
    def cancel_market_data(self, ticker_id: int) -> None:
        """
        Cancels a market data subscription.
        """
        pass

    @abstractmethod
    def get_order_status(self, order_id: int) -> Optional[str]:
        """Returns the latest known status string for an order ID, if available."""
        pass

    @abstractmethod
    def wait_for_fill(self, order_id: int, timeout_seconds: float = 30.0) -> bool:
        """Blocks until the given order is Fully Filled or timeout; returns True if filled."""
        pass
