import logging
import threading
from typing import Dict, Any, List
from decimal import Decimal

from ibapi.const import UNSET_DOUBLE
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract as IBContract
from ibapi.order_cancel import OrderCancel
from ibapi.order import Order as IBOrder
from ibapi.order_state import OrderState
from ibapi.execution import Execution

from trader.interfaces.trading_platform import TradingPlatform
from trader.models import Contract, Order, OrderAction, OrderType, TimeInForce, SecurityType

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
