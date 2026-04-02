from __future__ import annotations

import math

from domain.backtest.metric_formulas import MAX_FINITE_METRIC_ABS, compute_cagr


def test_compute_cagr_caps_extreme_short_window_growth() -> None:
    cagr = compute_cagr(
        initial_capital=10_000.0,
        ending_equity=448_634.32,
        duration_years=0.00010457283707247774,
    )

    assert math.isfinite(cagr)
    assert cagr == MAX_FINITE_METRIC_ABS
