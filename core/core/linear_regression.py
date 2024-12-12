from __future__ import annotations

from typing import Tuple

import pandas as pd
import statsmodels.api as sm


def ols_regression(
    x: pd.Series, y: pd.Series, with_const: bool = True
) -> Tuple[float, float]:
    """Perform OLS regression using stats models.

    Args:
        x (pd.Series): Independent Variable.
        y (pd.Series): Dependent Variable.
        with_const (bool, optional): Regress with intercept, if set to True. Defaults to True.

    Returns:
        Tuple[float, float]: Const (Intercept), Beta (Slope)
    """
    # align x and y
    x_idx = x.index
    y_idx = y.index
    idx = list(set(x_idx).intersection(y_idx))
    x = x[x.index.isin(idx)]
    y = y[y.index.isin(idx)]
    x = x.values
    y = y.values
    if with_const:
        x = sm.add_constant(x)
    model = sm.OLS(y, x)
    results = model.fit()
    return results.params[0], results.params[1]
