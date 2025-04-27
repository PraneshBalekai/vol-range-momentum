from __future__ import annotations

import argparse
import datetime
import json
import threading
import time
from decimal import Decimal

import pandas as pd
import schedule
from ibapi.contract import Contract

import external.ibkr as ibkr
from cio.data_loader import load_data
from core.strategy import load_noise_area

parser = argparse.ArgumentParser(description="Path of config file to pass to script")
parser.add_argument("-c", "--config-path", type=str, help="Path to config file")
parser.add_argument(
    "-d",
    "--docker-run",
    action=argparse.BooleanOptionalAction,
    help="Flag to set if this script is run in docker",
)


class IntradayMomentum(ibkr.IBBaseApp):
    def __init__(self, config: dict):
        super().__init__()
        self.current_limits = None
        self.current_open = None
        self.live_data = pd.DataFrame()  # used by 5 min bars
        self.mins = {}  # used for tick by tick data
        self.config = config
        self.number_of_bars = 1  # used by 5 min bars
        (
            self.historical_data,
            self.latest_avg,
            self.last_close,
        ) = self.init_historical_data_to_strategy()

    # We might not use this until we have a live data subscription
    # TODO: Assume incomplete until we have paid for subscription and tested
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

    def tickPrice(self, reqId, tickType, price, attrib):
        # TODO: Change this to real time last price once we switch to paid subscription.
        # type 68 is delayed last price
        if tickType == 68:
            dt = datetime.datetime.now(datetime.timezone.utc)
            dt = dt.replace(second=0, microsecond=0)

            if self.current_open is None:
                self.current_open = price
                self.current_limits = self.load_strategy_limits()
            # TODO: is there a defaultdict that does this?
            if dt in self.mins:
                self.mins[dt]["close"] = price
                if "open" not in self.mins[dt]:
                    self.mins[dt]["open"] = price
                if "low" not in self.mins[dt] or self.mins[dt]["low"] > price:
                    self.mins[dt]["low"] = price
                if "high" not in self.mins[dt] or self.mins[dt]["high"] < price:
                    self.mins[dt]["high"] = price
            elif dt not in self.mins:
                self.mins[dt] = {}
                self.mins[dt]["open"] = price
                self.mins[dt]["close"] = price
                self.mins[dt]["low"] = price
                self.mins[dt]["high"] = price

    def tickSize(self, reqId, tickType, size):
        # TODO: Change this to real time last price once we switch to paid subscription.
        # tick type 71, delayed last size
        if tickType == 71:
            dt = datetime.datetime.now(datetime.timezone.utc)
            dt = dt.replace(second=0, microsecond=0)

            if dt in self.mins:
                if "volume" in self.mins[dt]:
                    self.mins[dt]["volume"] += size
                else:
                    self.mins[dt]["volume"] = size
            elif dt not in self.mins:
                self.mins[dt] = {}
                self.mins[dt]["volume"] = size

    def load_strategy_limits(self):
        if self.current_limits is None:
            limits = self.latest_avg.copy()
            limits["upper_limit"] = max(self.last_close, self.current_open) * (
                1 + (self.config["strategy"]["volatility_multiplier"] * self.latest_avg)
            )
            limits["lower_limit"] = min(self.last_close, self.current_open) * (
                1 - (self.config["strategy"]["volatility_multiplier"] * self.latest_avg)
            )
            return limits

    def run_strategy(self):
        df = pd.DataFrame().from_dict(self.mins, orient="index")
        # TODO: see how to get TZ from config
        df.index = pd.Series(df.index).dt.tz_convert("US/Eastern")

        # At this point, index type and tz should be aligned
        df = df.resample("30min").agg(
            open=("open", "first"),
            close=("close", "last"),
            high=("high", "max"),
            low=("low", "min"),
            volume=("volume", "sum"),
        )

    def init_historical_data_to_strategy(self):
        """Loads historical data and manipulate as required for the strategy."""
        df = load_data(self.config["historical_data"])
        df, latest_avg = load_noise_area(
            df,
            lookback_days=self.config["strategy"]["lookback_days"],
            volatility_multiplier=self.config["strategy"]["volatility_multiplier"],
        )
        last_close = df.iloc[-1]["day_close"]
        return df, latest_avg.to_frame(), last_close


def custom_sched(app: IntradayMomentum):
    """Scheduler to start, run and end strategy."""
    schedule.every(1).minutes.do(app.run_strategy)
    while True:
        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()
        time.sleep(1)


def main(config_path: str, is_docker_run: bool):
    """
    Example Config:
    {
        "historical_data": {
            "loader_class": "ParquetLoader",
            "filename": "/Users/praneshbalekai/Desktop/IB_PRD/data/spy_mins.parquet"
        },
        "strategy": {
            "lookback_days": 20,
            "volatility_multiplier": 0.8
        },
        "ibkr_params": {
            "genericTickList": "",
            "snapshot": false,
            "regulatorySnapshot": false,
            "chartOptions": []
        },
        "contract": {
            "symbol": "SPY",
            "secType": "STK",
            "exchange": "SMART",
            "currency": "USD"
        }
    }
    """
    config = open(config_path)
    config = json.load(config)

    host = "host.docker.internal" if is_docker_run else "127.0.0.1"

    app = IntradayMomentum(config)
    app.connect(host, 4002, clientId=1)

    threading.Thread(target=app.run).start()
    time.sleep(1)
    threading.Thread(target=custom_sched, args=(app,)).start()
    time.sleep(1)

    ibkr_params = config["ibkr_params"]
    ibkr_params["reqId"] = app.nextId()
    ibkr_params["contract"] = Contract()
    for k, v in config["contract"].items():
        setattr(ibkr_params["contract"], k, v)

    # Gotta start paper trading with delayed data
    app.reqMarketDataType(3)
    app.reqMktData(**ibkr_params)


if __name__ == "__main__":
    args = parser.parse_args()
    print(f"Input args: {args.__dict__}")

    main(args.config_path, args.docker_run)
