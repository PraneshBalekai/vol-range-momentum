from __future__ import annotations

import pandas as pd
from cio.data_loader import load_data
from cio.data_writer import write_data
from dynaconf import Dynaconf
import argparse
import json
import datetime

parser = argparse.ArgumentParser(description="Path of config file to pass to script")
parser.add_argument("--config_path", type=str, help="Path to config file")


def main(config_path: str):
    """Loads config from config_path, gets historical data and writes to target in config.

    Example config: {
        "loader_config": {
            {
                "loader_class": "IBKRHistoricalDataLoader",
                "contract": {
                    "symbol": "SPY",
                    "secType": "STK",
                    "exchange": "SMART",
                    "currency": "USD"
                },
                "ibkr_params": {
                    "endDateTime": "@format {this.endDateTime} 09:30:00 US/Eastern",
                    "durationStr": "5 D",
                    "barSizeSetting": "1 min",
                    "whatToShow": "TRADES",
                    "useRTH": True,
                    "formatDate": 2, #2 stands for epoch seconds - use `localize_index` under loader params
                    "keepUpToDate": False,
                    "chartOptions": []
                }
            }
        },
        "script_config": {
            "timezone": "US/Eastern"
        }
        "writer_config": {
            "writer_class": "ParquetWriter",
            "filename": "Users/praneshbalekai/Desktop/IB_PRD/data/SPY_mins.parquet",
            "writer_params": {
                "append_if_exists": True,
                "sort_index": True,
                "deduplicate_index": True
            }
        }
    }
    """
    # load all config params
    config = open(config_path)
    config = json.load(config)
    dc = Dynaconf()

    # Load data
    endDateTime = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    dc['endDateTime'] = endDateTime
    dc.update(config)

    data = load_data(dc['loader_config'])
    
    # Change to timezone aware timestamp
    data.index = pd.to_datetime(data.index, unit='s')
    data.index = pd.Series(data.index).dt.tz_localize('UTC')
    data.index = pd.Series(data.index).dt.tz_convert(dc['script_config']['timezone'])

    write_data(data, dc['writer_config'])

    return


if __name__ == "__main__":
    args = parser.parse_args()
    print(f"Input args: {args.__dict__}")

    main(args.config_path)
