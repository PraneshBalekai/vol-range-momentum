from __future__ import annotations

import threading
import time
from decimal import Decimal

import pandas as pd
import schedule
from ibapi.contract import Contract

import external.ibkr as ibkr

# from core.strategy import load_noise_area

# Pre run, load yesterday's data and keep ready

# TODO:
# Subscribe to 5s bars, consolidate it to 30 min bars

# Find a way to check entry / exit conditions at the completion of a 30 min bar
# Rem, position sizing, sending orders

# Strategy defaults
lookback_days = 20
volatility_multiplier = 1
vol_scaling_target = 0.02


class IntradayMomentum(ibkr.IBBaseApp):
    def __init__(self, config: dict):
        super().__init__()
        self.live_data = pd.DataFrame()
        self.config = config
        self.number_of_bars = 1

    # We might not use this until we have a live data subscription
    def is_complete_bar(self, data: pd.DataFrame):
        """Re-sample 5s live data dataframe and returns True, if it is a complete bar at target freq

        A bar is considered complete once we have the start of the new bar. For example, a 30 min bar is
        complete at 10:30 if we see the first occurance of data in 11:00 bar.
        """
        freq = self.config["freq"]
        assert pd.to_timedelta(freq) >= pd.to_timedelta(
            "5s"
        ), f"Min. freq to build bars is 5s. Input freq is {freq}"

        # concat the current data to live data
        self.live_data = pd.concat([self.live_data, data])

        # resample live data to target frequency
        self.live_data = self.live_data.resample(freq).agg(
            open=("open", "first"),
            close=("close", "last"),
            high=("high", "max"),
            low=("low", "min"),
        )

        # check if the number of rows is higher than it was
        if len(self.live_data) > self.number_of_bars:
            self.number_of_bars = len(self.live_data)
            return True
        else:
            return False

    def realtimeBar(
        self,
        reqId: int,
        time: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: Decimal,
        wap: Decimal,
        count: int,
    ):
        """Handler function to receive 5s bars. This function, will make 30min bars from it."""
        print(time)
        print(open_)
        bar = {}
        data = {
            "close": close,
            "open": open_,
            "low": low,
            "high": high,
        }
        bar[time] = data
        bar = pd.DataFrame().from_dict(bar, orient="index")

    def cancelRealTimeBars(self, reqId: int):
        return super().cancelRealTimeBars(reqId)

    def marketDataType(self, reqId: int, marketDataType: int):
        print("MarketDataType. ReqId:", reqId, "Type:", marketDataType)

    # TODO: Convert ticks into bars. For now, we get delayed data,
    # so the bars will NOT reflect real-time / historical values.
    # This is just to get into paper trading. Will need to pay for live subscriptions.
    def tickPrice(self, reqId, tickType, price, attrib):
        # append to a kv store with incoming time and ltp (tick ID, 68?)
        print(reqId, tickType, price, attrib)

    def run_strategy(self):
        pass


app = IntradayMomentum({})
app.connect("127.0.0.1", 4002, clientId=1)

threading.Thread(target=app.run).start()
time.sleep(1)

contract = Contract()
contract.symbol = "SPY"
contract.secType = "STK"
contract.currency = "USD"
contract.exchange = "SMART"

# Gotta start paper trading with delayed data
app.reqMarketDataType(3)
app.reqMktData(app.nextId(), contract, "", False, False, [])


# New thread - run this in a new thread
def custom_sched():
    schedule.every().hour.at(":30").do(app.run_strategy)
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)
