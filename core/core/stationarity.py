from __future__ import annotations
from typing import Tuple
from statsmodels.tsa.stattools import adfuller
import pandas as pd


def adf_test(s: pd.Series) -> Tuple[float, float]:
    """Performs adf test using statsmodels and returns t_stat and p_value.

    https://www.statsmodels.org/dev/generated/statsmodels.tsa.stattools.adfuller.html

    Args:
        s (pd.Series): Pandas series usually with datetime index and values
            to test for stationarity

    Returns: 
        Tstat, Pvalue
    """
    results = adfuller(s)
    return results[0], results[1]
