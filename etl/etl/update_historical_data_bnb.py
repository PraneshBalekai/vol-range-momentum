from __future__ import annotations

import logging
from typing import Dict

import pandas as pd
from dynaconf import Dynaconf

from cio.data_loader import load_data
from cio.data_writer import write_data

logging.getLogger("update_historical_data_bnb")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:S",
)


def main(config: Dict, **kwargs):
    """Loads config from config_path, gets historical data and writes to target in config.

    Example config: {
        "loader_config": {
            "loader_class": "BinanceHistoricalDataLoader",
            "endpoint_type": None,
            "params": {
                "symbol": "{this.symbol}",
                "interval": "1m",
                "startTime": "{this.startTime}",
                "endTime": "{this.endTime}",
                "limit":1000
            }
        },
        "writer_config": {
            "writer_class": "ParquetWriter",
            "filename": "/Users/praneshbalekai/Desktop/IB_PRD/data/btcusdt_mins_proc.parquet",
            "writer_params": {
                "append_if_exists": True,
                "sort_index": True,
                "deduplicate_index": True
            }
        }
    }
    """
    logging.info("Updating historical data for {}".format(kwargs["symbol"]))
    dates = pd.date_range(kwargs["start_date"], kwargs["end_date"], freq="12h")

    for si in range(len(dates) - 1):
        ei = si + 1
        st = int(dates[si].timestamp()) * 1000
        et = int(dates[ei].timestamp()) * 1000

        logging.info("Querying from {} to {}".format(dates[si], dates[ei]))

        dc = Dynaconf()
        dc["symbol"] = kwargs["symbol"]
        dc["startTime"] = str(st)
        dc["endTime"] = str(et)

        dc.update(config)

        data = load_data(dc["loader_config"])
        data.index = pd.to_datetime(data["kline_open_time"], unit="ms")
        data.index = pd.Series(data.index).dt.tz_localize("UTC")

        data = data.rename(
            columns={
                "open_price": "open",
                "high_price": "high",
                "low_price": "low",
                "close_price": "close",
                "volume": "volume",
                "number_of_trades": "count",
            }
        )
        data = data[["close", "open", "low", "high", "volume", "count"]]
        data.index.name = None
        write_data(data, dc["writer_config"])
