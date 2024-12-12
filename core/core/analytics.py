from __future__ import annotations
import pandas as pd


def calc_n_period_forward_returns(prices: pd.DataFrame | pd.Series, n_periods: int = 1) -> pd.DataFrame | pd.Series:
    """Returns n period forward returns, shifted back by n periods.

    Fwd Return at index 1 = Fwd Returns for signal at index 1. We do not need to 
    shift the signals to match signal and returns in this case.

    Args:
        prices (pd.DataFrame | pd.Series): Datetime index and columns / values with prices. 
        n_periods (int): n period cumulative forward returns. 

    Returns:
        pd.DataFrame | pd.Series: Same format as input but with n period forward return values. 
    """
    fwd_returns = prices.pct_change(n_periods).shift(-n_periods)
    return fwd_returns
