from __future__ import annotations

import datetime as dt
import json

import click
from dateutil.parser import parse as parse_datetime

from etl.update_historical_data_bnb import main as bnb_update


@click.command
@click.option("--config-path", type=str, required=True)
@click.option("--symbol", type=str, required=True)
@click.option("--start-date", type=parse_datetime, required=True)
@click.option("--end-date", type=parse_datetime, required=True)
def update_historical_data_bnb(
    config_path: str, symbol: str, start_date: dt.datetime, end_date: dt.datetime
):
    """Queries data from Binance API as per config, writes to output config.

    Args:
        config_path (str): Path to JSON config
        symbol (str): Symbol to extract data for.
        start_date (str): Start date.
        end_date (str): End date.
    """
    config = open(config_path)
    config = json.load(config)

    kwargs = {"symbol": symbol, "start_date": start_date, "end_date": end_date}

    bnb_update(config, **kwargs)
