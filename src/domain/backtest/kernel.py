from __future__ import annotations

import numpy as np

try:
    import numba as nb
    NUMBA_AVAILABLE = True
except ImportError:  # pragma: no cover - local fallback for mismatched environments
    NUMBA_AVAILABLE = False

    class _NumbaFallback:
        @staticmethod
        def njit(*_args, **_kwargs):
            def decorator(func):
                return func

            return decorator

    nb = _NumbaFallback()


@nb.njit(cache=True)
def compute_spread(price_1: np.ndarray, price_2: np.ndarray, hedge_ratio: float) -> np.ndarray:
    return price_1 - hedge_ratio * price_2


@nb.njit(cache=True)
def compute_drawdown(equity: np.ndarray) -> np.ndarray:
    peak = equity[0]
    out = np.empty_like(equity)
    for idx in range(equity.shape[0]):
        if equity[idx] > peak:
            peak = equity[idx]
        out[idx] = equity[idx] - peak
    return out


@nb.njit(cache=True)
def rolling_mean_std(values: np.ndarray, lookback: int) -> tuple[np.ndarray, np.ndarray]:
    count = values.shape[0]
    mean = np.full(count, np.nan, dtype=np.float64)
    std = np.full(count, np.nan, dtype=np.float64)
    if lookback <= 1 or lookback > count:
        return mean, std

    window_sum = 0.0
    window_sum_sq = 0.0
    for idx in range(count):
        value = values[idx]
        window_sum += value
        window_sum_sq += value * value
        if idx >= lookback:
            old_value = values[idx - lookback]
            window_sum -= old_value
            window_sum_sq -= old_value * old_value
        if idx >= lookback - 1:
            current_mean = window_sum / lookback
            variance = (window_sum_sq / lookback) - (current_mean * current_mean)
            if variance < 0.0 and variance > -1e-12:
                variance = 0.0
            mean[idx] = current_mean
            std[idx] = np.sqrt(variance) if variance > 0.0 else 0.0
    return mean, std
