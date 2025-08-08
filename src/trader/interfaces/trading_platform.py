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
    def buy(self, contract: Contract, order: Order) -> None:
        """Places a buy order."""
        pass

    @abstractmethod
    def sell(self, contract: Contract, order: Order) -> None:
        """Places a sell order."""
        pass

    @abstractmethod
    def get_open_orders(self) -> List[Order]:
        """
        Retrieves all open orders.
        """
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
