from __future__ import annotations
from matplotlib import pyplot as plt
from typing import Tuple
import pandas as pd
import numpy as np
from core.analytics import calc_n_period_forward_returns
from core.linear_regression import ols_regression


def plot_ts(ts, ax=None, step=5, figsize=(13,7), title='', see_xaxis=False):
    """
    plot timeseries ignoring date gaps
    https://stackoverflow.com/questions/58476654/how-to-remove-or-hide-x-axis-labels-from-a-plot

    Params
    ------
    ts : pd.DataFrame or pd.Series
    step : int, display interval for ticks
    figsize : tuple, figure size
    title: str
    see_xaxis (bool): Set xaxis label visiblity
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    ax.plot(range(ts.dropna().shape[0]), ts.dropna())
    ax.set_title(title)
    ax.set_xticks(np.arange(len(ts.dropna())))
    ax.set_xticklabels(ts.dropna().index.tolist())
    ax.get_xaxis().set_visible(see_xaxis)

    return ax


def plot_ic_decay(signal: pd.DataFrame | pd.Series, 
                  prices: pd.DataFrame | pd.Series, 
                  n_periods: list = [1, 2, 3, 4, 5, 10, 15, 20],
                  figsize: Tuple = (13,7),
                  ax = None):
    """Given a signal and prices, calculate and plot IC decay over n_periods.

    Args:
        signal (pd.DataFrame | pd.Series): DateTime index signal values as columns.
        prices (pd.DataFrame | pd.Series): DateTime index and price of an asset whose returns we want to use.
        n_periods (list, optional): Defaults to [1, 2, 3, 4, 5, 10, 15, 20].
    """
    if ax is None:
        fig, ax = plt.subplots(figsize=figsize)

    ic_data = {}

    for period in n_periods:
        fwd_returns = calc_n_period_forward_returns(prices, period)
        ic_data[period] = signal.corr(fwd_returns, method="spearman")

    ic_data = pd.DataFrame().from_dict(ic_data, orient="index", columns=["IC"])
    ic_data.plot(ax=ax)
    plt.ylabel("Information Coefficient")
    plt.xlabel("N periods")
    plt.grid()


def plot_rolling_ic(signal: pd.Series, 
                    fwd_returns: pd.Series, 
                    n_periods: list = [12, 75], 
                    period_labels: list = ["Hourly", "Daily"], 
                    ):
    """Given Signal and Forward Returns, plot rolling IC stats for different periods. 

    Args:
        signal (pd.Series): DateTime Index and Signal values.
        fwd_returns (pd.Series): DateTime Index and Forward Return values. 
        n_periods (list, optional): Number of periods to do rolling IC stats on. Defaults to [12, 75].
        period_labels (list, optional): Labels for each period in n_periods. Defaults to ["Hourly", "Daily"].
    """
    def calc_rolling_ic(df, period):
        roll_ic = {}
        for window in df.rolling(period):
            if len(window) < period:
                continue
            roll_ic[window.index[-1]] = window["signal"].corr(window["fwd_returns"], method="spearman")
        return pd.DataFrame().from_dict(roll_ic, orient="index")

    sig_res_df = signal.to_frame()
    sig_res_df.columns = ["signal"]
    sig_res_df["fwd_returns"] = fwd_returns

    ic = signal.corr(fwd_returns, method="spearman")

    roll_ic_df = pd.DataFrame()
    for period in n_periods:
        tmp = calc_rolling_ic(sig_res_df, period)
        roll_ic_df = pd.concat([roll_ic_df, tmp], axis=1)
    
    roll_ic_df.columns = period_labels

    ax = plot_ts(roll_ic_df)
    ax.axhline(0, color="black")
    ax.axhline(ic, color="green", linestyle="-")
    ax.text(0, ic, f"{round(ic, 2)}", bbox=dict(boxstyle='square'))
    ax.grid()


def plot_signal_response(signal: pd.Series,
                         fwd_returns: pd.Series):
    """Given Signal and Forward Returns, visualize signal/Response.

    Args:
        signal (pd.Series): Series with DateTime Index and Signal as values.
        fwd_returns (pd.Series): Series with DateTime Index and Forward Returns as values.
    """
    # Prepare subplots
    _, axes = plt.subplots(1, 3, figsize=(20, 7))
     
    # Signal Distribution
    signal.hist(bins=50, ax=axes[0])
    axes[0].set_xlabel("Signal")
    axes[0].set_ylabel("Frequency")

    # Forward Returns Distribution
    fwd_returns.hist(bins=50, ax=axes[1])
    axes[1].set_xlabel("Forward Returns")
    axes[1].set_ylabel("Frequency")

    # Scatter Plot Signal / Response
    axes[2].scatter(signal, fwd_returns, alpha=0.3)
    axes[2].set_xlabel("Signal")
    axes[2].set_ylabel("Forward Returns")
    axes[2].grid()
    # Regression line
    c, beta = ols_regression(signal.dropna(), fwd_returns.dropna())
    xseq = np.linspace(signal.min(), signal.max(), num=100)
    yseq = c + (beta * xseq)
    axes[2].plot(xseq, yseq, color="k")
    axes[2].text(xseq[-1], yseq[-1], "{:.2f}".format(beta))


def plot_signal_bucket_characterstics(signal: pd.Series, fwd_returns: pd.Series):
    """Given Signal and Forward Returrns, visualize characterstics of signal strength by classifying them into deciles.

    Args:
        signal (pd.Series): Series with DateTime Index and Signal as values.
        fwd_returns (pd.Series): Series with DateTime Index and Forward Returns as values.
    """
    _, axes = plt.subplots(1, 3, figsize=(20, 7))

    # Make dataframe
    df = signal.to_frame()
    df.columns = ["signal"]
    df["fwd_returns"] = fwd_returns

    df["signal_returns"] = df["signal"] * df["fwd_returns"]

    df["signal_decile"] = pd.qcut(df["signal"], 10, labels=False)

    bucket_mean = df.groupby("signal_decile").mean()

    axes[0].bar(bucket_mean.index, bucket_mean["signal"])
    axes[0].set_xlabel("Signal")

    axes[1].bar(bucket_mean.index, bucket_mean["fwd_returns"])
    axes[1].set_xlabel("Forward Returns // Classifed by Signal Deciles")

    axes[2].bar(bucket_mean.index, bucket_mean["signal_returns"])
    axes[2].set_xlabel("Signal Weighted Forward Returns // Classified by Signal Deciles")
