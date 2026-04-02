from __future__ import annotations

from typing import Mapping

import numpy as np

from domain.backtest.distance import DistanceBacktestResult, DistanceParameters
from domain.backtest.metric_formulas import compute_equity_curve_metrics, duration_years_from_times
from domain.optimizer.distance_models import (
    OBJECTIVE_METRICS,
    Candidate,
    DistanceGridSearchSpace,
    DistanceOptimizationRow,
)


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
            "cagr": 0.0,
            "cagr_to_ulcer": 0.0,
            "r_squared": 0.0,
            "calmar": 0.0,
            "beauty_score": 0.0,
            "gross_profit": 0.0,
            "spread_cost": 0.0,
            "slippage_cost": 0.0,
            "commission_cost": 0.0,
            "total_cost": 0.0,
        }

    trades = int(result.summary.get("trades", 0) or 0)
    net_profit = float(result.summary.get("net_pnl", 0.0) or 0.0)
    wins = int(result.summary.get("wins", 0) or 0)
    return compute_equity_curve_metrics(
        equity_total=equity,
        initial_capital=float(result.summary.get("initial_capital", equity[0]) or equity[0]),
        trades_count=trades,
        wins=wins,
        gross_profit=float(result.summary.get("gross_pnl", 0.0) or 0.0),
        spread_cost=float(result.summary.get("total_spread_cost", 0.0) or 0.0),
        slippage_cost=float(result.summary.get("total_slippage_cost", 0.0) or 0.0),
        commission_cost=float(result.summary.get("total_commission", 0.0) or 0.0),
        net_profit=net_profit,
        duration_years=duration_years_from_times(result.frame.get_column("time").to_list()),
    )


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
        bollinger_k=float(search_space.bollinger_k),
    )
    if params.exit_z >= params.entry_z or (params.stop_z is not None and params.stop_z <= params.entry_z):
        return None
    return params
