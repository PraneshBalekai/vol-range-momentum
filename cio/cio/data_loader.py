from __future__ import annotations

import threading
import time
from abc import ABC, abstractmethod

import pandas as pd

import cio.constants as c
import external.ibkr as ibkr


class BaseLoader(ABC):
    def __init__(self, config: dict):
        """Base Loader class to load data from different sources based on config.

        To begin with, we will stick to cross-sectional dataframes for data and all data classes
        will returns a None / pd.DataFrame

        Args:
            config (dict): Dictionary with values related to the source and other details
                of the data loader
        """
        self.config = config

    @abstractmethod
    def load_data(self):
        pass


class ParquetDataFrameLoader(BaseLoader):
    """Loads data from a parquet file as a dataframe.

    Example config:
    {
        "loader_class": "ParquetDataFrameLoader",
        "filename": "../data/instruments_token.parquet"
    }
    """

    def load_data(self):
        data = pd.read_parquet(self.config["filename"])
        return data


class IBKRHistoricalDataLoader(BaseLoader):
    """Loads historical data from IBKR based on config.

    This IBKR implementation connects to the app, loads and disconnects. This makes the app
    synchronous. What is an ideal solution that can use the same app but can pass custom hanndler functions
    for each function, for ex, load historical data, send order etc?

    Example config:
    {
        "loader_class": "IBKRHistoricalDataLoader",
        "contract": {
            "symbol": "SPY",
            "secType": "STK",
            "exchange": "SMART",
            "currency": "USD"
        },
        "ibkr_params": {
            "endDateTime": "20241211 09:30:00 US/Eastern",
            "durationStr": "1 D",
            "barSizeSetting": "1 min",
            "whatToShow": "TRADES",
            "useRTH": True,
            "formatDate": 2, #2 stands for epoch seconds - use `localize_index` under loader params
            "keepUpToDate": False,
            "chartOptions": []
        }
    }
    """

    def __init__(self, config: dict):
        self.config = config
        self.data = pd.DataFrame()
        self.historical_query_end = False

    class IBKRHistoricalDataApp(ibkr.IBBaseApp):
        def __init__(self, main):
            super().__init__()
            self.main = main

        # override function from base class
        def historicalData(self, reqId, bar):
            """Handler function that gets triggered when IBKR App recieves data for query

            Args:
                reqId (int): Unique ID passed to identify IBKR contract that data is for
                bar (IBKR bar): Has fields
                Date: 12345678, Open: 222.97, High: 222.97, Low: 222.96, Close: 222.97, Volume: 300, WAP: 222.965, BarCount: 2
            """
            df = {}
            bar_data = {
                "close": bar.close,
                "open": bar.open,
                "low": bar.low,
                "high": bar.high,
                "volume": bar.volume,
                "count": bar.barCount,
            }
            df[bar.date] = bar_data
            df = pd.DataFrame.from_dict(df, orient="index")
            self.main.data = pd.concat([self.main.data, df])

        def historicalDataEnd(self, reqId, start, end):
            print(
                f"Historical Data Ended for {reqId}. Started at {start}, ending at {end}"
            )
            self.cancelHistoricalData(reqId)
            self.main.historical_query_end = True

    def load_data(self):
        # Init App
        app = self.IBKRHistoricalDataApp(self)
        app.connect("127.0.0.1", 4002, 0)

        threading.Thread(target=app.run).start()
        time.sleep(1)

        # Send Query
        ibkr_params = self.config["ibkr_params"]
        ibkr_params["reqId"] = app.nextId()
        ibkr_params["contract"] = ibkr.Contract()
        for k, v in self.config["contract"].items():
            setattr(ibkr_params["contract"], k, v)
        app.reqHistoricalData(**ibkr_params)

        # Run while loop until self.historical_query_end is set to True
        while not self.historical_query_end:
            time.sleep(1)
        time.sleep(15)

        app.disconnect()
        return self.data


def load_data(config: dict):
    """Based on config, call relevant data loader function.

    Args:
        config (dict): Config Dict object
    """
    source = config[c.loader_class]
    if source == "ParquetDataFrameLoader":
        loader = ParquetDataFrameLoader(config)
    elif source == "IBKRHistoricalDataLoader":
        loader = IBKRHistoricalDataLoader(config)
    else:
        raise ValueError("Not a valid data source for data loader")

    data = loader.load_data()
    return data
