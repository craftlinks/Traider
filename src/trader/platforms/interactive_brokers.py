import threading
from typing import Dict, Any

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from trader.interfaces.trading_platform import TradingPlatform

class IBApp(EWrapper, EClient):
    """
    The main application class for interacting with the TWS API.
    It handles sending requests and receiving data.
    """
    def __init__(self):
        EClient.__init__(self, self)
        self.account_summary: Dict[str, Any] = {}
        self.account_summary_event = threading.Event()

    def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
        super().error(reqId, errorTime, errorCode, errorString, advancedOrderRejectJson)
        print(f"Error: {reqId}, {errorCode}, {errorString}")
        # Error handling for account summary request
        if reqId == 9001:
            self.account_summary_event.set()

    def accountSummary(self, reqId: int, account: str, tag: str, value: str, currency: str):
        super().accountSummary(reqId, account, tag, value, currency)
        self.account_summary[tag] = {"value": value, "currency": currency, "account": account}

    def accountSummaryEnd(self, reqId: int):
        super().accountSummaryEnd(reqId)
        print("AccountSummaryEnd. ReqId:", reqId)
        self.account_summary_event.set()


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

    def disconnect(self):
        """Disconnect from the trading platform."""
        self.app.disconnect()

    def get_account_summary(self) -> Dict[str, Any]:
        """Retrieve account summary information."""
        # Use a unique request id
        reqId = 9001
        self.app.account_summary.clear()
        self.app.account_summary_event.clear()
        
        self.app.reqAccountSummary(reqId, "All", "AccountType,NetLiquidation,TotalCashValue,SettledCash,AccruedCash,BuyingPower,EquityWithLoanValue,PreviousEquityWithLoanValue,GrossPositionValue,ReqTEquity,ReqTMargin,SMA,InitMarginReq,MaintMarginReq,AvailableFunds,ExcessLiquidity,Cushion,FullInitMarginReq,FullMaintMarginReq,FullAvailableFunds,FullExcessLiquidity,LookAheadNextChange,LookAheadInitMarginReq,LookAheadMaintMarginReq,LookAheadAvailableFunds,LookAheadExcessLiquidity,HighestSeverity,DayTradesRemaining,Leverage")
        
        # Wait for the account summary data to be received
        self.app.account_summary_event.wait()
        return self.app.account_summary