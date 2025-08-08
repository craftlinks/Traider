from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional

class SecurityType(Enum):
    STOCK = "STK"
    OPTION = "OPT"
    FUTURE = "FUT"
    FOREX = "CASH"

@dataclass
class Contract:
    symbol: str
    sec_type: SecurityType = SecurityType.STOCK
    currency: str = "USD"
    exchange: str = "SMART"
    strike: Optional[float] = None
    right: Optional[str] = None
    last_trade_date_or_contract_month: Optional[str] = None

class OrderAction(Enum):
    BUY = "BUY"
    SELL = "SELL"

class OrderType(Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"

class TimeInForce(Enum):
    DAY = "DAY"
    GOOD_TIL_CANCELED = "GTC"
    IMMEDIATE_OR_CANCEL = "IOC"

@dataclass
class Order:
    contract: Contract
    action: OrderAction
    order_type: OrderType
    quantity: float
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Optional[float] = None
    order_id: Optional[int] = None
    status: Optional[str] = None
    outside_rth: bool = False

@dataclass
class MarketData:
    """Represents market data for a financial instrument."""
    ticker_id: int
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    last_price: Optional[float] = None
    volume: Optional[int] = None


@dataclass
class Bar:
    """Represents a single bar of historical data."""
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


@dataclass
class Trade:
    """Represents a single trade."""
    price: float
    size: int
    timestamp: datetime
    exchange: str


@dataclass
class Quote:
    """Represents a single bid/ask quote."""
    bid_price: float
    bid_size: int
    ask_price: float
    ask_size: int
    timestamp: datetime
