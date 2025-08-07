from abc import ABC, abstractmethod
from typing import Dict, Any

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