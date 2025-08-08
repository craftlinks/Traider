from abc import ABC, abstractmethod
from typing import Dict, Any, List

from trader.models import Contract, Order


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
