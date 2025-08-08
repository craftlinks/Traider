import logging
import threading
from typing import Dict, Any, List, Optional
from decimal import Decimal

from ibapi.const import UNSET_DOUBLE
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract as IBContract
from ibapi.order_cancel import OrderCancel
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState
from ibapi.execution import Execution
from ibapi.ticktype import TickTypeEnum

from trader.interfaces.trading_platform import TradingPlatform
from trader.models import Contract, Order, OrderAction, OrderType, TimeInForce, SecurityType, MarketData

# Configure logging
logger = logging.getLogger(__name__)

def _ib_contract_to_contract(ib_contract: IBContract) -> Contract:
    """Converts an Interactive Brokers contract to our application's contract model."""
    return Contract(
        symbol=ib_contract.symbol,
        sec_type=SecurityType(ib_contract.secType),
        currency=ib_contract.currency,
        exchange=ib_contract.exchange,
        strike=ib_contract.strike,
        right=ib_contract.right,
        last_trade_date_or_contract_month=ib_contract.lastTradeDateOrContractMonth
    )

class IBApp(EWrapper, EClient):
    """
    The main application class for interacting with the TWS API.
    It handles sending requests and receiving data.
    """
    def __init__(self):
        EClient.__init__(self, self)
        self.account_summary: Dict[str, Any] = {}
        self.account_summary_event = threading.Event()
        self.next_valid_order_id: int | None = None
        self.order_id_received = threading.Event()
        self.open_orders: Dict[int, Order] = {}
        self.open_orders_event = threading.Event()
        self.market_data: Dict[int, MarketData] = {}
        self.market_data_events: Dict[int, threading.Event] = {}

    def error(self, reqId: int, errorTime: int, errorCode: int, errorString: str, advancedOrderRejectJson=""):
        # IB's error messages are a mixed bag, so we need to filter them
        if advancedOrderRejectJson:
            logger.error("Request %d: %d - %s, %s", reqId, errorCode, errorString, advancedOrderRejectJson)
        else:
            # Informational messages
            if errorCode in [2104, 2106, 2158]:
                logger.info("Request %d: %d - %s", reqId, errorCode, errorString)
            # Warnings
            elif errorCode in [399]:
                logger.warning("Request %d: %d - %s", reqId, errorCode, errorString)
            # Errors
            else:
                logger.error("Request %d: %d - %s", reqId, errorCode, errorString)
        
        # Unblock waiting events on error
        if reqId in self.market_data_events:
            self.market_data_events[reqId].set()
        elif reqId == 9001:
            self.account_summary_event.set()

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.next_valid_order_id = orderId
        self.order_id_received.set()

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        self.account_summary[tag] = {"value": value, "currency": currency, "account": account}

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        logger.info("AccountSummaryEnd. ReqId: %d", reqId)
        self.account_summary_event.set()

    def openOrder(self, orderId: int, contract: IBContract, order: IBOrder, orderState: OrderState):
        super().openOrder(orderId, contract, order, orderState)
        app_order = Order(
            contract=_ib_contract_to_contract(contract),
            action=OrderAction(order.action),
            order_type=OrderType(order.orderType),
            quantity=float(order.totalQuantity),
            time_in_force=TimeInForce(order.tif),
            limit_price=float(order.lmtPrice) if order.lmtPrice is not None and order.lmtPrice != UNSET_DOUBLE else None,
            order_id=orderId,
            status=orderState.status,
            outside_rth=order.outsideRth
        )
        self.open_orders[orderId] = app_order

    def openOrderEnd(self):
        super().openOrderEnd()
        logger.info("OpenOrderEnd")
        self.open_orders_event.set()

    def orderStatus(self, orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice):
        super().orderStatus(orderId, status, filled, remaining, avgFillPrice, permId, parentId, lastFillPrice, clientId, whyHeld, mktCapPrice)
        logger.info("OrderStatus. Id: %s, Status: %s, Filled: %s, Remaining: %s, AvgFillPrice: %s", 
                    orderId, status, filled, remaining, avgFillPrice)
        
    def execDetails(self, reqId: int, contract: IBContract, execution: Execution):
        super().execDetails(reqId, contract, execution)
        logger.info("ExecDetails. ReqId: %s, Contract: %s, Execution: %s", reqId, contract, execution)

    def tickPrice(self, reqId, tickType, price, attrib):
        super().tickPrice(reqId, tickType, price, attrib)
        if reqId in self.market_data:
            if tickType == TickTypeEnum.BID:  # type: ignore
                self.market_data[reqId].bid_price = price
            elif tickType == TickTypeEnum.ASK:  # type: ignore
                self.market_data[reqId].ask_price = price
            elif tickType == TickTypeEnum.LAST:  # type: ignore
                self.market_data[reqId].last_price = price
            
            if self.market_data[reqId].bid_price is not None and self.market_data[reqId].ask_price is not None:
                if reqId in self.market_data_events:
                    self.market_data_events[reqId].set()


    def tickSize(self, reqId, tickType, size):
        super().tickSize(reqId, tickType, size)
        if reqId in self.market_data:
            if tickType == TickTypeEnum.VOLUME:  # type: ignore
                self.market_data[reqId].volume = int(size)

    def tickSnapshotEnd(self, reqId: int):
        """Called when a snapshot request is completed."""
        super().tickSnapshotEnd(reqId)
        logger.info("Snapshot data reception completed for ticker %d", reqId)
        # Trigger the event to indicate snapshot is complete
        if reqId in self.market_data_events:
            self.market_data_events[reqId].set()

class InteractiveBrokersPlatform(TradingPlatform):
    """
    A wrapper for the Interactive Brokers TWS API that implements the TradingPlatform interface.
    """

    def __init__(self):
        self.app = IBApp()
        self.next_ticker_id = 0

    def connect(self, host: str, port: int, client_id: int):
        """Connect to the trading platform."""
        self.app.connect(host, port, client_id)
        
        api_thread = threading.Thread(target=self.app.run, daemon=True)
        api_thread.start()

        # Wait for the next valid order ID to be received
        self.app.order_id_received.wait()

    def disconnect(self):
        """Disconnect from the trading platform."""
        self.app.disconnect()

    def get_account_summary(self) -> Dict[str, Any]:
        """Retrieve account summary information."""
        # Use a unique request id
        reqId = 9001
        self.app.account_summary.clear()
        self.app.account_summary_event.clear()
        
        self.app.reqAccountSummary(reqId, "All", "$LEDGER")
        
        # Wait for the account summary data to be received
        self.app.account_summary_event.wait()
        return self.app.account_summary
    
    def buy(self, contract: Contract, order: Order) -> None:
        ib_order = self._create_ib_order(order)
        ib_order.action = "BUY"
        self._place_order(contract, ib_order)

    def sell(self, contract: Contract, order: Order) -> None:
        ib_order = self._create_ib_order(order)
        ib_order.action = "SELL"
        self._place_order(contract, ib_order)

    def modify_order(self, order_id: int, order: Order) -> None:
        """Modifies an existing order."""
        ib_order = self._create_ib_order(order)
        self._place_order(order.contract, ib_order, order_id)

    def get_open_orders(self) -> List[Order]:
        """Retrieves all open orders."""
        self.app.open_orders.clear()
        self.app.open_orders_event.clear()
        self.app.reqAllOpenOrders()
        self.app.open_orders_event.wait()
        return list(self.app.open_orders.values())

    def cancel_order(self, order_id: int) -> None:
        """Cancels an existing order."""
        self.app.cancelOrder(order_id, OrderCancel())

    def get_market_data(self, contract: Contract, snapshot: bool = False, regulatory_snapshot: bool = False) -> Optional[MarketData]:
        """
        Retrieves market data for a given contract.
        
        Args:
            contract: The contract to get market data for
            snapshot: If True, requests a free delayed snapshot (cancels automatically)
            regulatory_snapshot: If True, requests a real-time snapshot ($0.01 USD per US equity)
        
        Returns:
            MarketData object or None if the request fails or times out.
        """
        ticker_id = self.next_ticker_id
        self.next_ticker_id += 1
        
        ib_contract = self._create_ib_contract(contract)
        market_data_event = threading.Event()
        self.app.market_data_events[ticker_id] = market_data_event
        self.app.market_data[ticker_id] = MarketData(ticker_id=ticker_id)
        
        # For snapshots, don't use genericTickList (should be empty string)
        generic_tick_list = "" if (snapshot or regulatory_snapshot) else ""
        
        self.app.reqMktData(ticker_id, ib_contract, generic_tick_list, snapshot, regulatory_snapshot, [])
        
        # Wait for the data to be populated, with a timeout
        # Snapshots typically arrive faster, but we'll use the same timeout
        event_was_set = market_data_event.wait(timeout=10)
        
        # Clean up the event
        del self.app.market_data_events[ticker_id]

        if not event_was_set:
            logger.error("Market data request for %s timed out.", contract.symbol)
            # For non-snapshot requests, cancel the hanging request
            if not snapshot and not regulatory_snapshot:
                self.cancel_market_data(ticker_id)
            return None
        
        # Check if data is valid
        data = self.app.market_data.get(ticker_id)
        if data and data.bid_price is not None and data.ask_price is not None:
             return data
        else:
             logger.warning("Market data for %s received but incomplete.", contract.symbol)
             return None


    def cancel_market_data(self, ticker_id: int) -> None:
        """Cancels a market data subscription."""
        self.app.cancelMktData(ticker_id)
        if ticker_id in self.app.market_data:
            del self.app.market_data[ticker_id]
        if ticker_id in self.app.market_data_events:
            del self.app.market_data_events[ticker_id]

    def _get_next_order_id(self) -> int:
        """Gets the next valid order ID and increments it."""
        if self.app.next_valid_order_id is None:
            raise ConnectionError("Order ID not available.")
        
        order_id = self.app.next_valid_order_id
        self.app.next_valid_order_id += 1
        return order_id

    def _place_order(self, contract: Contract, order: IBOrder, order_id: int | None = None):
        """Creates the IB contract and places the order."""
        ib_contract = self._create_ib_contract(contract)
        order_id_to_use = order_id if order_id is not None else self._get_next_order_id()
        self.app.placeOrder(order_id_to_use, ib_contract, order)


    def _create_ib_contract(self, contract: Contract) -> IBContract:
        ib_contract = IBContract()
        ib_contract.symbol = contract.symbol
        ib_contract.secType = contract.sec_type.value
        ib_contract.currency = contract.currency
        ib_contract.exchange = contract.exchange
        if contract.strike:
            ib_contract.strike = contract.strike
        if contract.right:
            ib_contract.right = contract.right
        if contract.last_trade_date_or_contract_month:
            ib_contract.lastTradeDateOrContractMonth = contract.last_trade_date_or_contract_month
        return ib_contract

    def _create_ib_order(self, order: Order) -> IBOrder:
        ib_order = IBOrder()
        ib_order.action = order.action.value
        ib_order.orderType = order.order_type.value
        ib_order.totalQuantity = Decimal(str(order.quantity))
        ib_order.tif = order.time_in_force.value
        ib_order.outsideRth = order.outside_rth
        if order.limit_price:
            ib_order.lmtPrice = float(order.limit_price)
        return ib_order
