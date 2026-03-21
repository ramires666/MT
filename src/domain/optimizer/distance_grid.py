from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from itertools import product
from math import sqrt
from typing import Any, Iterable, Sequence

import numpy as np
import polars as pl

from domain.backtest.distance import DistanceParameters, load_pair_frame, run_distance_backtest_frame
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec

DEFAULT_DISTANCE_GRID: dict[str, list[float | int]] = {
    "lookback_bars": [48, 96, 144],
    "entry_z": [1.5, 2.0, 2.5],
    "exit_z": [0.25, 0.5, 0.75],
    "stop_z": [3.0, 3.5, 4.0],
    "bollinger_k": [2.0],
}


@dataclass(slots=True)
class DistanceOptimizationTrial:
    trial_id: int
    objective_metric: str
    objective_value: float
    net_profit: float
    ending_equity: float
    max_drawdown: float
    pnl_to_drawdown: float
    win_rate: float
    trade_count: int
    omega_ratio: float
    ulcer_index: float
    ulcer_performance_index: float
    k_ratio: float
    lookback_bars: int
    entry_z: float
    exit_z: float
    stop_z: float
    bollinger_k: float


@dataclass(slots=True)
class DistanceOptimizationResult:
    pair: PairSelection
    timeframe: str
    started_at: datetime
    ended_at: datetime
    objective_metric: str
    trial_count: int
    trials: list[DistanceOptimizationTrial]

    @property
    def best_trial(self) -> DistanceOptimizationTrial | None:
        return self.trials[0] if self.trials else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "pair": {"symbol_1": self.pair.symbol_1, "symbol_2": self.pair.symbol_2},
            "timeframe": self.timeframe,
            "started_at": self.started_at.isoformat(),
            "ended_at": self.ended_at.isoformat(),
            "objective_metric": self.objective_metric,
            "trial_count": self.trial_count,
            "best_trial": asdict(self.best_trial) if self.best_trial else None,
            "trials": [asdict(trial) for trial in self.trials],
        }


def _coerce_numeric_list(values: Any, fallback: Sequence[float | int]) -> list[float | int]:
    if values is None:
        return list(fallback)
    if isinstance(values, str):
        parts = [part.strip() for part in values.split(",")]
        parsed: list[float | int] = []
        for part in parts:
            if not part:
                continue
            parsed.append(float(part) if "." in part else int(part))
        return parsed or list(fallback)
    if isinstance(values, Iterable):
        parsed = [value for value in values if value is not None]
        return parsed or list(fallback)
    raise TypeError(f"Unsupported search space values: {values!r}")


def normalize_distance_search_space(search_space: dict[str, Any] | None) -> dict[str, list[float | int]]:
    source = search_space or {}
    normalized: dict[str, list[float | int]] = {}
    for key, fallback in DEFAULT_DISTANCE_GRID.items():
        normalized[key] = _coerce_numeric_list(source.get(key), fallback)
    normalized["lookback_bars"] = [int(value) for value in normalized["lookback_bars"] if int(value) > 1]
    normalized["entry_z"] = [float(value) for value in normalized["entry_z"] if float(value) > 0]
    normalized["exit_z"] = [float(value) for value in normalized["exit_z"] if float(value) >= 0]
    normalized["stop_z"] = [float(value) for value in normalized["stop_z"] if float(value) > 0]
    normalized["bollinger_k"] = [float(value) for value in normalized["bollinger_k"] if float(value) > 0]
    return normalized


def _safe_returns(equity_total: np.ndarray) -> np.ndarray:
    if equity_total.size < 2:
        return np.zeros(0, dtype=np.float64)
    previous = equity_total[:-1]
    current = equity_total[1:]
    mask = previous != 0
    returns = np.zeros(current.shape[0], dtype=np.float64)
    returns[mask] = (current[mask] - previous[mask]) / previous[mask]
    return returns[np.isfinite(returns)]


def _omega_ratio(returns: np.ndarray, threshold: float = 0.0) -> float:
    if returns.size == 0:
        return 0.0
    gains = np.maximum(returns - threshold, 0.0).sum()
    losses = np.maximum(threshold - returns, 0.0).sum()
    if losses <= 0:
        return float("inf") if gains > 0 else 0.0
    return float(gains / losses)


def _ulcer_index(equity_total: np.ndarray) -> float:
    if equity_total.size == 0:
        return 0.0
    peaks = np.maximum.accumulate(equity_total)
    safe_peaks = np.where(peaks == 0.0, 1.0, peaks)
    drawdown_pct = 100.0 * (equity_total - peaks) / safe_peaks
    return float(sqrt(np.mean(np.square(drawdown_pct)))) if drawdown_pct.size else 0.0


def _ulcer_performance_index(net_profit: float, initial_capital: float, ulcer_index: float) -> float:
    if ulcer_index <= 0:
        return float("inf") if net_profit > 0 else 0.0
    total_return_pct = 100.0 * (net_profit / max(initial_capital, 1e-9))
    return float(total_return_pct / ulcer_index)


def _k_ratio(equity_total: np.ndarray) -> float:
    if equity_total.size < 3:
        return 0.0
    safe_equity = np.maximum(equity_total, 1e-9)
    y = np.log(safe_equity)
    x = np.arange(y.size, dtype=np.float64)
    x_mean = x.mean()
    y_mean = y.mean()
    denominator = np.sum(np.square(x - x_mean))
    if denominator <= 0:
        return 0.0
    slope = np.sum((x - x_mean) * (y - y_mean)) / denominator
    intercept = y_mean - slope * x_mean
    residuals = y - (intercept + slope * x)
    dof = y.size - 2
    if dof <= 0:
        return 0.0
    residual_variance = np.sum(np.square(residuals)) / dof
    standard_error = sqrt(residual_variance / denominator) if denominator > 0 else 0.0
    if standard_error <= 0:
        return 0.0
    return float(slope / standard_error)


def _objective_value(metric: str, metrics: dict[str, float | int]) -> float:
    value = float(metrics.get(metric, 0.0))
    if metric == "ulcer_index":
        return -value
    return value


def _sanitize_objective(value: float) -> float:
    if np.isnan(value):
        return float("-inf")
    if np.isposinf(value):
        return 1_000_000.0
    if np.isneginf(value):
        return -1_000_000.0
    return float(value)


def _finite_metric(value: float, *, positive_cap: float = 1_000_000.0) -> float:
    if np.isnan(value):
        return 0.0
    if np.isposinf(value):
        return positive_cap
    if np.isneginf(value):
        return -positive_cap
    return float(value)


def _trial_metrics(
    equity_total: np.ndarray,
    summary: dict[str, float | int | str],
    defaults: StrategyDefaults,
) -> dict[str, float | int]:
    net_profit = float(summary.get("net_pnl", 0.0))
    max_drawdown = float(summary.get("max_drawdown", 0.0))
    trade_count = int(summary.get("trades", 0))
    win_rate = float(summary.get("win_rate", 0.0))
    ending_equity = float(summary.get("ending_equity", defaults.initial_capital))
    returns = _safe_returns(equity_total)
    omega_ratio = _omega_ratio(returns)
    ulcer_index = _ulcer_index(equity_total)
    ulcer_performance_index = _ulcer_performance_index(net_profit, defaults.initial_capital, ulcer_index)
    k_ratio = _k_ratio(equity_total)
    pnl_to_drawdown = net_profit / abs(max_drawdown) if abs(max_drawdown) > 1e-9 else (float("inf") if net_profit > 0 else 0.0)
    return {
        "net_profit": _finite_metric(net_profit),
        "ending_equity": _finite_metric(ending_equity),
        "max_drawdown": _finite_metric(max_drawdown),
        "pnl_to_drawdown": _finite_metric(pnl_to_drawdown),
        "win_rate": _finite_metric(win_rate),
        "trade_count": trade_count,
        "omega_ratio": _finite_metric(omega_ratio),
        "ulcer_index": _finite_metric(ulcer_index),
        "ulcer_performance_index": _finite_metric(ulcer_performance_index),
        "k_ratio": _finite_metric(k_ratio),
    }


def run_distance_grid_search_frame(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    search_space: dict[str, Any] | None,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    timeframe: str = "M15",
    started_at: datetime | None = None,
    ended_at: datetime | None = None,
) -> DistanceOptimizationResult:
    normalized_space = normalize_distance_search_space(search_space)
    trials: list[DistanceOptimizationTrial] = []
    if frame.is_empty():
        return DistanceOptimizationResult(
            pair=pair,
            timeframe=timeframe,
            started_at=started_at or datetime.min,
            ended_at=ended_at or datetime.min,
            objective_metric=objective_metric,
            trial_count=0,
            trials=[],
        )

    combinations = product(
        normalized_space["lookback_bars"],
        normalized_space["entry_z"],
        normalized_space["exit_z"],
        normalized_space["stop_z"],
        normalized_space["bollinger_k"],
    )
    for trial_id, (lookback_bars, entry_z, exit_z, stop_z, bollinger_k) in enumerate(combinations, start=1):
        params = DistanceParameters(
            lookback_bars=int(lookback_bars),
            entry_z=float(entry_z),
            exit_z=float(exit_z),
            stop_z=float(stop_z),
            bollinger_k=float(bollinger_k),
        )
        backtest = run_distance_backtest_frame(
            frame=frame,
            pair=pair,
            defaults=defaults,
            params=params,
            point_1=point_1,
            point_2=point_2,
            contract_size_1=contract_size_1,
            contract_size_2=contract_size_2,
        )
        metrics = _trial_metrics(
            backtest.frame.get_column("equity_total").to_numpy() if not backtest.frame.is_empty() else np.zeros(0, dtype=np.float64),
            backtest.summary,
            defaults,
        )
        objective_value = _sanitize_objective(_objective_value(objective_metric, metrics))
        trials.append(
            DistanceOptimizationTrial(
                trial_id=trial_id,
                objective_metric=objective_metric,
                objective_value=objective_value,
                net_profit=float(metrics["net_profit"]),
                ending_equity=float(metrics["ending_equity"]),
                max_drawdown=float(metrics["max_drawdown"]),
                pnl_to_drawdown=float(metrics["pnl_to_drawdown"]),
                win_rate=float(metrics["win_rate"]),
                trade_count=int(metrics["trade_count"]),
                omega_ratio=float(metrics["omega_ratio"]),
                ulcer_index=float(metrics["ulcer_index"]),
                ulcer_performance_index=float(metrics["ulcer_performance_index"]),
                k_ratio=float(metrics["k_ratio"]),
                lookback_bars=int(lookback_bars),
                entry_z=float(entry_z),
                exit_z=float(exit_z),
                stop_z=float(stop_z),
                bollinger_k=float(bollinger_k),
            )
        )

    trials.sort(
        key=lambda trial: (
            trial.objective_value,
            trial.net_profit,
            -abs(trial.max_drawdown),
            trial.trade_count,
        ),
        reverse=True,
    )
    return DistanceOptimizationResult(
        pair=pair,
        timeframe=timeframe,
        started_at=started_at or frame.get_column("time")[0],
        ended_at=ended_at or frame.get_column("time")[-1],
        objective_metric=objective_metric,
        trial_count=len(trials),
        trials=trials,
    )


def run_distance_grid_search(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    defaults: StrategyDefaults,
    search_space: dict[str, Any] | None,
    objective_metric: str = "net_profit",
) -> DistanceOptimizationResult:
    frame = load_pair_frame(broker=broker, pair=pair, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return DistanceOptimizationResult(
            pair=pair,
            timeframe=timeframe.value,
            started_at=started_at,
            ended_at=ended_at,
            objective_metric=objective_metric,
            trial_count=0,
            trials=[],
        )

    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    return run_distance_grid_search_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        search_space=search_space,
        objective_metric=objective_metric,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        timeframe=timeframe.value,
        started_at=started_at,
        ended_at=ended_at,
    )
