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