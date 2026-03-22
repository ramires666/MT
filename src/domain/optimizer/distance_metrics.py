from __future__ import annotations

from math import log, sqrt
from typing import Mapping

import numpy as np

from domain.backtest.distance import DistanceBacktestResult, DistanceParameters
from domain.optimizer.distance_models import (
    OBJECTIVE_METRICS,
    Candidate,
    DistanceGridSearchSpace,
    DistanceOptimizationRow,
)


def safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-12:
        return 0.0
    return numerator / denominator


def compute_k_ratio(equity: np.ndarray) -> float:
    if equity.size < 3:
        return 0.0
    clipped = np.maximum(equity, 1e-9)
    y = np.log(clipped)
    x = np.arange(y.size, dtype=np.float64)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    ss_x = float(np.square(x - x_mean).sum())
    if ss_x <= 1e-12:
        return 0.0
    slope = float(np.dot(x - x_mean, y - y_mean) / ss_x)
    intercept = y_mean - slope * x_mean
    residuals = y - (intercept + slope * x)
    dof = y.size - 2
    if dof <= 0:
        return 0.0
    sigma = float(np.sqrt(np.square(residuals).sum() / dof))
    if sigma <= 1e-12:
        return 0.0
    slope_stderr = sigma / sqrt(ss_x)
    if slope_stderr <= 1e-12:
        return 0.0
    return float(slope / slope_stderr)


def equity_metrics(result: DistanceBacktestResult) -> dict[str, float]:
    equity = (
        result.frame.get_column("equity_total").to_numpy()
        if not result.frame.is_empty()
        else np.asarray([], dtype=np.float64)
    )
    if equity.size == 0:
        return {
            "net_profit": 0.0,
            "ending_equity": 0.0,
            "max_drawdown": 0.0,
            "pnl_to_maxdd": 0.0,
            "omega_ratio": 0.0,
            "k_ratio": 0.0,
            "score_log_trades": 0.0,
            "ulcer_index": 0.0,
            "ulcer_performance": 0.0,
            "gross_profit": 0.0,
            "spread_cost": 0.0,
            "slippage_cost": 0.0,
            "commission_cost": 0.0,
            "total_cost": 0.0,
        }

    trades = int(result.summary.get("trades", 0) or 0)
    net_profit = float(result.summary.get("net_pnl", 0.0) or 0.0)
    ending_equity = float(result.summary.get("ending_equity", equity[-1]) or equity[-1])
    running_peak = np.maximum.accumulate(equity)
    drawdown_abs = running_peak - equity
    max_drawdown = float(drawdown_abs.max()) if drawdown_abs.size else 0.0
    pnl_to_maxdd = safe_ratio(net_profit, max_drawdown)

    pnl_steps = np.diff(equity, prepend=equity[:1])
    gains = float(np.clip(pnl_steps, 0.0, None).sum())
    losses = float(np.clip(-pnl_steps, 0.0, None).sum())
    omega_ratio = gains if losses <= 1e-12 else gains / losses
    score_log_trades = pnl_to_maxdd * log(1.0 + max(0, trades))

    dd_pct = np.divide(drawdown_abs, running_peak, out=np.zeros_like(drawdown_abs), where=running_peak > 1e-12)
    ulcer_index = float(np.sqrt(np.mean(dd_pct**2))) if dd_pct.size else 0.0
    ulcer_performance = safe_ratio(net_profit, ulcer_index)

    return {
        "net_profit": net_profit,
        "ending_equity": ending_equity,
        "max_drawdown": max_drawdown,
        "pnl_to_maxdd": pnl_to_maxdd,
        "omega_ratio": float(omega_ratio),
        "k_ratio": compute_k_ratio(equity),
        "score_log_trades": float(score_log_trades),
        "ulcer_index": ulcer_index,
        "ulcer_performance": ulcer_performance,
        "gross_profit": float(result.summary.get("gross_pnl", 0.0) or 0.0),
        "spread_cost": float(result.summary.get("total_spread_cost", 0.0) or 0.0),
        "slippage_cost": float(result.summary.get("total_slippage_cost", 0.0) or 0.0),
        "commission_cost": float(result.summary.get("total_commission", 0.0) or 0.0),
        "total_cost": float(result.summary.get("total_cost", 0.0) or 0.0),
    }


def objective_score(metric: str, metrics: Mapping[str, float]) -> float:
    if metric == "ulcer_index":
        return -float(metrics.get(metric, 0.0))
    return float(metrics.get(metric, 0.0))


def validate_objective_metric(objective_metric: str) -> None:
    if objective_metric not in OBJECTIVE_METRICS:
        raise ValueError(f"Unsupported objective metric: {objective_metric}")


def sort_rows(rows: list[DistanceOptimizationRow]) -> list[DistanceOptimizationRow]:
    return sorted(
        rows,
        key=lambda row: (
            row.objective_score,
            row.net_profit,
            -row.max_drawdown,
            row.trades,
        ),
        reverse=True,
    )


def params_from_candidate(search_space: DistanceGridSearchSpace, candidate: Candidate) -> DistanceParameters | None:
    raw_stop = search_space.stop_z[candidate[3]]
    params = DistanceParameters(
        lookback_bars=int(search_space.lookback_bars[candidate[0]]),
        entry_z=float(search_space.entry_z[candidate[1]]),
        exit_z=float(search_space.exit_z[candidate[2]]),
        stop_z=None if raw_stop is None else float(raw_stop),
        bollinger_k=float(search_space.bollinger_k[candidate[4]]),
    )
    if params.exit_z >= params.entry_z or (params.stop_z is not None and params.stop_z <= params.entry_z):
        return None
    return params
