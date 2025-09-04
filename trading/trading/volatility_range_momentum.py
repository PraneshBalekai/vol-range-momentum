from __future__ import annotations

import argparse
import datetime
import json
import threading
import time
from decimal import Decimal
from queue import Queue

import pandas as pd
from ibapi.contract import Contract
from ibapi.order import Order

import external.ibkr as ibkr
from cio.data_loader import load_data
from core.strategy import load_noise_area
from trading.consts import ENTER_LONG, ENTER_SHORT, EXIT_LONG, EXIT_SHORT

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
        self.resampled_current_limits = None
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

        # Order management-related
        self.capital = self.config["strategy"]["capital"]
        self.volatility_target = self.config["strategy"]["volatility_target"]
        self.max_leverage = self.config["strategy"]["max_leverage"]
        self.curr_position = 0

    # Market Data - related functions
    def marketDataType(self, reqId: int, marketDataType: int):
        print("MarketDataType. ReqId:", reqId, "Type:", marketDataType)

    def tickPrice(self, reqId, tickType, price, attrib):
        # TODO: Change this to real time last price once we switch to paid subscription.
        # type 68 is delayed last price
        print(price)
        if tickType == 68:
            dt = datetime.datetime.now(datetime.timezone.utc)
            dt = dt.replace(second=0, microsecond=0)

            if self.current_open is None:
                self.current_open = price
                (
                    self.current_limits,
                    self.resampled_current_limits,
                ) = self.load_strategy_limits()
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
        # https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#available-tick-types
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
            limits.index = limits.index.to_series().apply(
                lambda s: pd.Timestamp(
                    f"{pd.Timestamp.now(self.config['strategy']['iana_timezone']).date()} {s}"
                )
            )
            limits.columns = ["latest_avg"]

            limits["upper_limit"] = max(self.last_close, self.current_open) * (
                1
                + (
                    self.config["strategy"]["volatility_multiplier"]
                    * limits["latest_avg"]
                )
            )
            limits["lower_limit"] = min(self.last_close, self.current_open) * (
                1
                - (
                    self.config["strategy"]["volatility_multiplier"]
                    * limits["latest_avg"]
                )
            )
            resampled_limits = limits.copy()
            resampled_limits = resampled_limits.resample("30min").agg(
                upper_limit=("upper_limit", "last"), lower_limit=("lower_limit", "last")
            )

            return limits, resampled_limits

    def run_strategy(self, orders_queue: Queue):
        while True:
            if datetime.datetime.now().minute == 30:
                df = pd.DataFrame().from_dict(self.mins, orient="index")
                # TODO: see how to get TZ from config
                df.index = pd.Series(df.index).dt.tz_convert(
                    self.config["strategy"]["iana_timezone"]
                )

                # we use this to calculate vwap
                df["typical_px"] = (df["high"] + df["low"] + df["close"]) / 3
                df["typical_px"] = df["typical_px"] * df["volume"]
                df["vwap"] = (
                    df.groupby(df.index)["typical_px"].cumsum()
                    / df.groupby(df.index)["volume"].cumsum()
                )

                # At this point, index type and tz should be aligned
                df = df.resample("30min").agg(
                    open=("open", "first"),
                    close=("close", "last"),
                    high=("high", "max"),
                    low=("low", "min"),
                    volume=("volume", "sum"),
                    vwap=("vwap", "last"),
                )

                vwap = df.loc[df.index[-1], "vwap"]
                px = df.loc[df.index[-1], "close"]
                up_lim = self.resampled_current_limits.loc[
                    self.resampled_current_limits.index[-1], "upper_limit"
                ]
                low_lim = self.resampled_current_limits.loc[
                    self.resampled_current_limits.index[-1], "lower_limit"
                ]

                # Decide what position you want to take
                if px > up_lim:
                    orders_queue.put(ENTER_LONG)
                if px < low_lim:
                    orders_queue.put(ENTER_SHORT)
                if px < vwap or px < up_lim:
                    orders_queue.put(EXIT_LONG)
                if px > vwap or px > low_lim:
                    orders_queue.put(EXIT_SHORT)

                # This task happens at the 30th min, make sure you wait more than a minute to not execute again
                time.sleep(100)
            else:
                time.sleep(1)

    # Order management related functions
    def openOrder(self, orderId, contract: Contract, order: Order, orderState):
        print(orderId, contract, order, orderState)

    def openOrderEnd(self):
        print("OpenOrderEnd")

    def orderStatus(
        self,
        orderId,
        status: str,
        filled: Decimal,
        remaining: Decimal,
        avgFillPrice: float,
        permId: int,
        parentId: int,
        lastFillPrice: float,
        clientId: int,
        whyHeld: str,
        mktCapPrice: float,
    ):
        super().orderStatus(
            orderId,
            status,
            filled,
            remaining,
            avgFillPrice,
            permId,
            parentId,
            lastFillPrice,
            clientId,
            whyHeld,
            mktCapPrice,
        )
        self.curr_position += filled

    def get_order_aux_price(self):
        # TODO: get like last price from contract and do some +/- 1% stuff
        return 0

    def get_order_lmt_price(self):
        # TODO: get last price from contract and set as limit price
        return 0

    def manage_positions(self, orders_queue: Queue, contract: Contract):
        def calculate_position_size(strategy_capital, max_leverage, volatility_target):
            """
            Returns the abs. value of position size.
            """
            capital = strategy_capital * min(
                max_leverage, volatility_target / self.historical_data.iloc[-1, "sigma"]
            )
            return capital / self.current_open

        while True:
            instruction = orders_queue.get(block=True, timeout=None)
            if instruction == ENTER_LONG:
                if self.curr_position != 0:
                    print(
                        f"Incorrect instruction: {instruction} when curr_position is {self.curr_position}"
                    )
                else:
                    order_total_quantity = calculate_position_size(
                        self.capital, self.max_leverage, self.volatility_target
                    )
                    order = Order()
                    order.action = "BUY"
                    order.auxPrice = self.get_order_aux_price()
                    order.lmtPrice = self.get_order_lmt_price()
                    order.orderType = "LMT"
                    order.totalQuantity = order_total_quantity

                    self.placeOrder(self.nextId(), contract, order)
                    pass
            elif instruction == ENTER_SHORT:
                if self.curr_position != 0:
                    print(
                        f"Incorrect instruction: {instruction} when curr_position is {self.curr_position}"
                    )
                else:
                    order_total_quantity = calculate_position_size(
                        self.capital, self.max_leverage, self.volatility_target
                    )
                    order = Order()
                    order.action = "SELL"
                    order.auxPrice = self.get_order_aux_price()
                    order.lmtPrice = self.get_order_lmt_price()
                    order.orderType = "LMT"
                    order.totalQuantity = order_total_quantity

                    self.placeOrder(self.nextId(), contract, order)
                    pass
            elif instruction == EXIT_LONG:
                if self.curr_position > 1:
                    # TODO:
                    # get curr position
                    # place exit order
                    pass
                else:
                    print(
                        f"Incorrect instruction: {instruction} when curr_position is {self.curr_position}"
                    )
            elif instruction == EXIT_SHORT:
                if self.curr_position < 1:
                    # TODO:
                    # get curr position
                    # place exit order
                    pass
                else:
                    print(
                        f"Incorrect instruction: {instruction} when curr_position is {self.curr_position}"
                    )

        return

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

    orders_queue = Queue()

    threading.Thread(target=app.run).start()
    time.sleep(1)
    threading.Thread(
        target=app.run_strategy, kwargs=dict(orders_queue=orders_queue)
    ).start()
    time.sleep(1)

    ibkr_params = config["ibkr_params"]
    ibkr_params["reqId"] = app.nextId()
    ibkr_params["contract"] = Contract()
    for k, v in config["contract"].items():
        setattr(ibkr_params["contract"], k, v)

    threading.Thread(
        target=app.manage_positions,
        kwargs=dict(orders_queue=orders_queue, contract=ibkr_params["contract"]),
    ).start()
    time.sleep(1)

    # Gotta start paper trading soon
    app.reqMarketDataType(3)
    app.reqMktData(**ibkr_params)


if __name__ == "__main__":
    args = parser.parse_args()
    print(f"Input args: {args.__dict__}")

    main(args.config_path, args.docker_run)
