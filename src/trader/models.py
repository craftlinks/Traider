from dataclasses import dataclass
from enum import Enum
from decimal import Decimal
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
    action: OrderAction
    order_type: OrderType
    quantity: Decimal
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Optional[Decimal] = None

