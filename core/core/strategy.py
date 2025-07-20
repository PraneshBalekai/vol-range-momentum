from __future__ import annotations

from typing import Tuple

import numpy as np
import pandas as pd


# Intraday momentum based on dynamic noise area
def load_noise_area(
    df: pd.DataFrame, lookback_days: int, volatility_multiplier: float
) -> Tuple[pd.DataFrame, pd.Series]:
    """Following the paper on intraday momentum from concretum research, this function returns noise area.

    For ex: at 09:43, we calculate the mean of the returns from 09:30 to 09:43 over the last n lookback_days.
    We then calculate the UPPER and LOWER bound of the noise area using
    upper_bound = max(prev_day_close, today_open) * (vol_multiplier + avg_move)
    lower_bound = min(prev_day_close, today_open) * (vol_multiplier - avg_move)

    Args:
        df (pd.DataFrame): A dataframe with intraday data with
            datetime index, and with at least the following fields: 'open', 'close'
        lookback_days (int): Number of days to calculate the avg_move over
        volatility_multiplier (float): Volatility multiplier to scale the noise area.

    Returns:
        pd.DataFrame: DataFrame with columns:
            [open, close, day_close,  day_open, prev_close, upper_bound, lower_bound]
        pd.Series: avg_move over the latest n lookback_days in the dataframe.
    """
    assert (
        len(df) >= lookback_days
    ), f"Not enough input data in the dataframe, lookback days: {lookback_days}, length of df: {len(df)}"
    df["date"] = df.index.date
    df["minute"] = df.index.time

    # calculate VWAP
    df["typical_px"] = (df["high"] + df["low"] + df["close"]) / 3
    df["typical_px"] = df["typical_px"] * df["volume"]
    df["vwap"] = (
        df.groupby(df.index.date)["typical_px"].cumsum()
        / df.groupby(df.index.date)["volume"].cumsum()
    )

    # store daily close and open values
    daily_data = df.groupby("date").agg(
        day_open=("open", "first"), day_close=("close", "last")
    )
    daily_data["prev_close"] = daily_data["day_close"].shift(1)

    # stats for vol scaling
    daily_data["mu"] = (
        daily_data["day_close"].pct_change().rolling(lookback_days).mean().shift(1)
    )  # see pg.14 on the paper
    daily_data["sigma"] = np.nan

    # last row is 't', the window's first row pct_change == NaN, so we add +2.
    for window in daily_data.rolling(lookback_days + 2):
        if len(window) < (lookback_days + 2):
            continue
        sigma = (
            window[1:lookback_days]["day_close"].pct_change()
            - window.loc[window.index[-1], "mu"]
        )
        sigma = sigma**2
        sigma = sigma.sum() / (lookback_days - 1)
        daily_data.loc[window.index[-1], "sigma"] = np.sqrt(sigma)

    df = df.merge(daily_data, left_on="date", right_index=True)

    # calculat avg move
    df["move"] = ((df["close"] / df["day_open"]) - 1).abs()

    # ffill to fill up data for those days where market closes at 13:00
    pivoted_table = pd.pivot_table(df, "move", index="date", columns="minute").ffill()
    avg_move = (
        pivoted_table.rolling(lookback_days, min_periods=lookback_days).mean().shift()
    )
    latest_avg = pivoted_table[-lookback_days:].mean()
    latest_avg.index = pivoted_table.columns

    avg_move = pd.melt(
        avg_move.reset_index(),
        id_vars="date",
        value_vars=list(avg_move.columns),
        var_name="minute",
        value_name="avg_move",
    )
    df = df.merge(avg_move, left_on=["date", "minute"], right_on=["date", "minute"])

    df["datetime"] = pd.to_datetime(
        df["date"].astype(str) + df["minute"].astype(str), format="%Y-%m-%d%H:%M:%S"
    )

    df.index = df["datetime"]
    df = df[
        [
            "date",
            "minute",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "vwap",
            "day_open",
            "day_close",
            "prev_close",
            "move",
            "avg_move",
            "mu",
            "sigma",
        ]
    ]

    df["upper_bound"] = df.apply(
        lambda x: max(x["prev_close"], x["day_open"])
        * (1 + (volatility_multiplier * x["avg_move"])),
        axis=1,
    )
    df["lower_bound"] = df.apply(
        lambda x: min(x["prev_close"], x["day_open"])
        * (1 - (volatility_multiplier * x["avg_move"])),
        axis=1,
    )

    return df, latest_avg
