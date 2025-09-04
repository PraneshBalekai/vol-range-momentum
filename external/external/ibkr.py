# historical
import warnings

from ibapi.client import EClient, OrderId
from ibapi.wrapper import EWrapper

warnings.filterwarnings("ignore")


class IBBaseApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)

    def nextValidId(self, orderId: OrderId):
        self.orderId = orderId

    def nextId(self):
        self.orderId += 1
        return self.orderId

    def error(self, reqId, errorCode, errorString, advancedOrderReject=""):
        print(
            f"reqId: {reqId}, errorCode: {errorCode}, errorString: {errorString}, orderReject: {advancedOrderReject}"
        )

    def historicalData(self, reqId, bar):
        print(f"Received historical data for {reqId}. Bar: {bar}")

    def historicalDataEnd(self, reqId, start, end):
        print(f"Historical Data Ended for {reqId}. Started at {start}, ending at {end}")
        self.cancelHistoricalData(reqId)

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print(f"reqId: {reqId}, headTimestamp: {headTimestamp}")
        self.cancelHeadTimeStamp(reqId)
