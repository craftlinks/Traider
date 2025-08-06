from ibapi.client import *
from ibapi.wrapper import *
import time
import threading

class TestApp(EClient, EWrapper):
  def __init__(self):
    EClient.__init__(self, self)
  
  def nextValidId(self, orderId):
    self.orderId = orderId
  
  def nextId(self):
    self.orderId += 1
    return self.orderId

  def error(self, reqId, errorTime, errorCode, errorString, advancedOrderRejectJson=""):
    print(f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderRejectJson}")

  def contractDetails(self, reqId, contractDetails: ContractDetails):
    contract_details = vars(contractDetails)
    print("\n".join(f"{name}: {value}" for name,value in contract_details.items()))
    # print(contractDetails.contract)

  def contractDetailsEnd(self, reqId):
    print("End of contract details")
    self.disconnect()

app = TestApp()
app.connect("127.0.0.1", 4002, 0)
threading.Thread(target=app.run).start()
time.sleep(1)

mycontract = Contract()
# Stock
# mycontract.symbol = "AAPL"
# mycontract.secType = "STK"
# mycontract.currency = "USD"
# mycontract.exchange = "SMART"
# mycontract.primaryExchange = "NASDAQ"

# Future
# mycontract.symbol = "ES"
# mycontract.secType = "FUT"
# mycontract.currency = "USD"
# mycontract.exchange = "CME"
# mycontract.lastTradeDateOrContractMonth = 202412

# Option
mycontract.symbol = "SPX"
mycontract.secType = "OPT"
mycontract.currency = "USD"
mycontract.exchange = "SMART"
mycontract.lastTradeDateOrContractMonth = 202412
mycontract.right = "P"
mycontract.tradingClass = "SPXW"
mycontract.strike = 5300

app.reqContractDetails(app.nextId(), mycontract)
  


# Platform

# Port

# TWS Live

# 7496

# TWS Paper

# 7497

# IBG Live

# 4001

# IBG Paper

# 4002