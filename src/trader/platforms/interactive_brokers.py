import logging
import threading
from typing import Dict, Any, List

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract as IBContract
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState

from trader.interfaces.trading_platform import TradingPlatform
from trader.models import Contract, Order, OrderAction, OrderType, TimeInForce

# Configure logging
logger = logging.getLogger(__name__)

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
        # Error handling for account summary request
        if reqId == 9001:
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
            action=OrderAction(order.action),
            order_type=OrderType(order.orderType),
            quantity=order.totalQuantity,
            time_in_force=TimeInForce(order.tif),
            limit_price=order.lmtPrice,
            order_id=orderId,
            status=orderState.status
        )
        self.open_orders[orderId] = app_order

    def openOrderEnd(self):
        super().openOrderEnd()
        logger.info("OpenOrderEnd")
        self.open_orders_event.set()



class InteractiveBrokersPlatform(TradingPlatform):
    """
    A wrapper for the Interactive Brokers TWS API that implements the TradingPlatform interface.
    """

    def __init__(self):
        self.app = IBApp()

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

    def get_open_orders(self) -> List[Order]:
        """Retrieves all open orders."""
        self.app.open_orders.clear()
        self.app.open_orders_event.clear()
        self.app.reqAllOpenOrders()
        self.app.open_orders_event.wait()
        return list(self.app.open_orders.values())

    def _get_next_order_id(self) -> int:
        """Gets the next valid order ID and increments it."""
        if self.app.next_valid_order_id is None:
            raise ConnectionError("Order ID not available.")
        
        order_id = self.app.next_valid_order_id
        self.app.next_valid_order_id += 1
        return order_id

    def _place_order(self, contract: Contract, order: IBOrder):
        """Creates the IB contract and places the order."""
        ib_contract = self._create_ib_contract(contract)
        self.app.placeOrder(self._get_next_order_id(), ib_contract, order)

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
        ib_order.totalQuantity = order.quantity
        ib_order.tif = order.time_in_force.value
        if order.limit_price:
            ib_order.lmtPrice = float(order.limit_price)
        return ib_order
