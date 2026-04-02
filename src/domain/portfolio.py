from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math

import numpy as np
import polars as pl

from domain.backtest.metric_formulas import compute_equity_curve_metrics, duration_years_from_times
from domain.contracts import StrategyDefaults
from storage.portfolio_store import PortfolioItem


@dataclass(slots=True)
class PortfolioRunRow:
    item_id: str
    symbol_1: str
    symbol_2: str
    timeframe: str
    allocation_capital: float
    net_profit: float | None
    ending_equity: float | None
    max_drawdown: float | None
    trades: int | None
    cagr: float | None
    cagr_to_ulcer: float | None
    r_squared: float | None
    calmar: float | None
    beauty_score: float | None
    status: str


@dataclass(slots=True)
class PortfolioCurve:
    item_id: str
    symbol_1: str
    symbol_2: str
    timeframe: str
    initial_capital: float
    times: list[datetime]
    equities: list[float]
    unrealized_drawdowns: list[float] = field(default_factory=list)
    capital_loads: list[float] = field(default_factory=list)
    open_positions: list[int] = field(default_factory=list)


@dataclass(slots=True)
class PortfolioEquitySummary:
    initial_capital: float
    peak_equity: float
    net_profit: float
    ending_equity: float
    max_drawdown: float
    trades: int
    cagr: float
    cagr_to_ulcer: float
    r_squared: float
    calmar: float
    beauty_score: float
    max_unrealized_drawdown: float
    avg_unrealized_drawdown: float
    max_capital_load: float
    avg_capital_load: float
    max_capital_load_pct: float
    avg_capital_load_pct: float
    max_open_positions: int
    avg_open_positions: float


@dataclass(slots=True)
class PortfolioCorrelationRow:
    left_item_id: str
    left_label: str
    right_item_id: str
    right_label: str
    equity_corr: float
    return_corr: float


@dataclass(slots=True)
class PortfolioAllocationSuggestionRow:
    item_id: str
    label: str
    return_volatility: float
    mean_abs_return_corr: float
    diversification_score: float
    suggested_weight: float


_CONTEXTUAL_PORTFOLIO_SOURCE_KINDS = frozenset(
    {
        "optimization_row",
        "meta_selected_fold",
        "meta_robustness_row",
        "scan_row",
    }
)


def scale_defaults_for_portfolio_item(item: PortfolioItem, allocation_capital: float) -> StrategyDefaults:
    saved_capital = max(float(item.initial_capital), 1e-9)
    scale = float(allocation_capital) / saved_capital
    return StrategyDefaults(
        initial_capital=float(allocation_capital),
        leverage=float(item.leverage),
        margin_budget_per_leg=float(item.margin_budget_per_leg) * scale,
        slippage_points=float(item.slippage_points),
    )


def portfolio_strategy_started_at(item: PortfolioItem, *, started_at: datetime, ended_at: datetime) -> datetime:
    context_started_at = item.context_started_at
    if item.source_kind not in _CONTEXTUAL_PORTFOLIO_SOURCE_KINDS or context_started_at is None:
        return started_at
    if started_at < context_started_at < ended_at:
        return context_started_at
    return started_at


def portfolio_analysis_window(item: PortfolioItem, *, started_at: datetime, ended_at: datetime) -> tuple[datetime, datetime]:
    analysis_end = ended_at
    if item.oos_started_at is not None:
        if item.oos_started_at <= started_at:
            return started_at, started_at
        if item.oos_started_at < ended_at:
            analysis_end = item.oos_started_at

    analysis_start = started_at
    context_started_at = item.context_started_at
    if context_started_at is not None and started_at < context_started_at < analysis_end:
        analysis_start = context_started_at
    return analysis_start, analysis_end


def prepend_flat_equity_prefix(
    times: list[datetime],
    equities: list[float],
    *,
    period_started_at: datetime,
    strategy_started_at: datetime,
    initial_capital: float,
) -> tuple[list[datetime], list[float]]:
    if not times or strategy_started_at <= period_started_at:
        return times, equities

    prefixed_times = list(times)
    prefixed_equities = list(equities)
    if prefixed_times[0] > strategy_started_at:
        prefixed_times.insert(0, strategy_started_at)
        prefixed_equities.insert(0, float(initial_capital))
    prefixed_times.insert(0, period_started_at)
    prefixed_equities.insert(0, float(initial_capital))
    return prefixed_times, prefixed_equities


def prepend_constant_series_prefix(
    times: list[datetime],
    values: list[float] | list[int],
    *,
    period_started_at: datetime,
    strategy_started_at: datetime,
    fill_value: float | int,
) -> list[float] | list[int]:
    if not times or strategy_started_at <= period_started_at:
        return values

    prefixed = list(values)
    if times[0] > strategy_started_at:
        prefixed.insert(0, fill_value)
    prefixed.insert(0, fill_value)
    return prefixed


def latest_portfolio_oos_started_at(items: list[PortfolioItem]) -> datetime | None:
    candidates = [item.oos_started_at for item in items if item.oos_started_at is not None]
    if not candidates:
        return None
    return max(candidates)


def derive_portfolio_curve_risk_series(
    equities: list[float],
    positions: list[int],
    *,
    margin_budget_per_leg: float,
) -> tuple[list[float], list[float], list[int]]:
    if not equities or not positions:
        return [], [], []

    unrealized_drawdowns: list[float] = []
    capital_loads: list[float] = []
    open_positions: list[int] = []
    current_realized_equity = float(equities[0])
    load_per_pair = max(float(margin_budget_per_leg), 0.0) * 2.0

    for index, (equity_value, position_value) in enumerate(zip(equities, positions, strict=False)):
        equity = float(equity_value)
        is_open = int(position_value) != 0
        if is_open and (index == 0 or int(positions[index - 1]) == 0):
            current_realized_equity = equity
        if not is_open:
            current_realized_equity = equity
            unrealized_drawdowns.append(0.0)
            capital_loads.append(0.0)
            open_positions.append(0)
            continue
        unrealized_drawdowns.append(min(0.0, equity - current_realized_equity))
        capital_loads.append(load_per_pair)
        open_positions.append(1)
    return unrealized_drawdowns, capital_loads, open_positions


def materialize_portfolio_backtest_allocations(
    item_ids: list[str],
    *,
    allocation_capitals_by_id: dict[str, float],
    fallback_allocation_capital: float,
) -> dict[str, float]:
    fallback = max(float(fallback_allocation_capital or 0.0), 0.0)
    materialized: dict[str, float] = {}
    for item_id in item_ids:
        allocation = max(float(allocation_capitals_by_id.get(item_id, 0.0) or 0.0), 0.0)
        if allocation <= 0.0:
            allocation = fallback
        materialized[item_id] = allocation
    return materialized


def _curve_label(curve: PortfolioCurve) -> str:
    return f"{curve.symbol_1} / {curve.symbol_2} [{curve.timeframe}]"


def _aligned_normalized_equities(curves: list[PortfolioCurve]) -> tuple[list[datetime], np.ndarray]:
    valid_curves = [curve for curve in curves if curve.times and curve.equities]
    if not valid_curves:
        return [], np.zeros((0, 0), dtype=np.float64)

    all_times = sorted({moment for curve in valid_curves for moment in curve.times})
    if not all_times:
        return [], np.zeros((0, 0), dtype=np.float64)

    matrix = np.ones((len(valid_curves), len(all_times)), dtype=np.float64)
    for curve_index, curve in enumerate(valid_curves):
        capital = max(float(curve.initial_capital), 1e-9)
        pointer = 0
        current_equity = capital
        for time_index, moment in enumerate(all_times):
            while pointer < len(curve.times) and curve.times[pointer] <= moment:
                current_equity = float(curve.equities[pointer])
                pointer += 1
            matrix[curve_index, time_index] = current_equity / capital
    return all_times, matrix


def _safe_corr(left: np.ndarray, right: np.ndarray) -> float:
    if left.size == 0 or right.size == 0 or left.size != right.size:
        return 0.0
    left_std = float(np.std(left))
    right_std = float(np.std(right))
    if left_std <= 1e-12 or right_std <= 1e-12:
        return 0.0
    corr = float(np.corrcoef(left, right)[0, 1])
    if not math.isfinite(corr):
        return 0.0
    return max(-1.0, min(1.0, corr))


def analyze_portfolio_curves(
    curves: list[PortfolioCurve],
) -> tuple[list[PortfolioCorrelationRow], list[PortfolioAllocationSuggestionRow]]:
    valid_curves = [curve for curve in curves if curve.times and curve.equities]
    if not valid_curves:
        return [], []

    _all_times, normalized_equities = _aligned_normalized_equities(valid_curves)
    if normalized_equities.size == 0:
        return [], []

    if normalized_equities.shape[1] >= 2:
        returns_matrix = (normalized_equities[:, 1:] / normalized_equities[:, :-1]) - 1.0
    else:
        returns_matrix = np.zeros((len(valid_curves), 0), dtype=np.float64)

    pairwise_rows: list[PortfolioCorrelationRow] = []
    return_corr_matrix = np.zeros((len(valid_curves), len(valid_curves)), dtype=np.float64)
    for left_index, left_curve in enumerate(valid_curves):
        for right_index in range(left_index + 1, len(valid_curves)):
            right_curve = valid_curves[right_index]
            equity_corr = _safe_corr(normalized_equities[left_index], normalized_equities[right_index])
            return_corr = _safe_corr(returns_matrix[left_index], returns_matrix[right_index])
            return_corr_matrix[left_index, right_index] = return_corr
            return_corr_matrix[right_index, left_index] = return_corr
            pairwise_rows.append(
                PortfolioCorrelationRow(
                    left_item_id=left_curve.item_id,
                    left_label=_curve_label(left_curve),
                    right_item_id=right_curve.item_id,
                    right_label=_curve_label(right_curve),
                    equity_corr=equity_corr,
                    return_corr=return_corr,
                )
            )

    raw_scores: list[float] = []
    suggestion_rows: list[PortfolioAllocationSuggestionRow] = []
    for index, curve in enumerate(valid_curves):
        returns = returns_matrix[index]
        return_volatility = float(np.std(returns)) if returns.size else 0.0
        if len(valid_curves) == 1:
            mean_abs_return_corr = 0.0
        else:
            others = [abs(float(return_corr_matrix[index, other_index])) for other_index in range(len(valid_curves)) if other_index != index]
            mean_abs_return_corr = float(sum(others) / float(len(others))) if others else 0.0
        diversification_score = 1.0 / (max(return_volatility, 1e-9) * (1.0 + mean_abs_return_corr))
        raw_scores.append(diversification_score)
        suggestion_rows.append(
            PortfolioAllocationSuggestionRow(
                item_id=curve.item_id,
                label=_curve_label(curve),
                return_volatility=return_volatility,
                mean_abs_return_corr=mean_abs_return_corr,
                diversification_score=diversification_score,
                suggested_weight=0.0,
            )
        )

    total_score = float(sum(raw_scores))
    if total_score <= 1e-12:
        equal_weight = 1.0 / float(len(suggestion_rows))
        for row in suggestion_rows:
            row.suggested_weight = equal_weight
    else:
        for row, score in zip(suggestion_rows, raw_scores, strict=False):
            row.suggested_weight = float(score) / total_score
    return pairwise_rows, suggestion_rows


def combine_portfolio_equity_curves(
    curves: list[PortfolioCurve],
    *,
    included_item_ids: set[str] | None = None,
    allocation_capitals_by_id: dict[str, float] | None = None,
) -> pl.DataFrame:
    if included_item_ids is not None:
        curves = [curve for curve in curves if curve.item_id in included_item_ids]
    if not curves:
        return pl.DataFrame(
            {"time": [], "equity": [], "unrealized_drawdown": [], "capital_load": [], "open_positions": []},
            schema={
                "time": pl.Datetime(time_zone="UTC"),
                "equity": pl.Float64,
                "unrealized_drawdown": pl.Float64,
                "capital_load": pl.Float64,
                "open_positions": pl.Int64,
            },
        )

    all_times = sorted({moment for curve in curves for moment in curve.times})
    if not all_times:
        return pl.DataFrame(
            {"time": [], "equity": [], "unrealized_drawdown": [], "capital_load": [], "open_positions": []},
            schema={
                "time": pl.Datetime(time_zone="UTC"),
                "equity": pl.Float64,
                "unrealized_drawdown": pl.Float64,
                "capital_load": pl.Float64,
                "open_positions": pl.Int64,
            },
        )

    indices = [0 for _ in curves]
    effective_capitals = [
        max(
            float(allocation_capitals_by_id.get(curve.item_id, curve.initial_capital) or curve.initial_capital),
            0.0,
        )
        if allocation_capitals_by_id is not None
        else float(curve.initial_capital)
        for curve in curves
    ]
    current_equities = list(effective_capitals)
    current_unrealized_drawdowns = [0.0 for _ in curves]
    current_capital_loads = [0.0 for _ in curves]
    current_open_positions = [0 for _ in curves]
    totals: list[float] = []
    unrealized_totals: list[float] = []
    capital_load_totals: list[float] = []
    open_positions_totals: list[int] = []
    for moment in all_times:
        total = 0.0
        total_unrealized_drawdown = 0.0
        total_capital_load = 0.0
        total_open_positions = 0
        for curve_index, curve in enumerate(curves):
            while indices[curve_index] < len(curve.times) and curve.times[indices[curve_index]] <= moment:
                raw_initial_capital = max(float(curve.initial_capital), 1e-9)
                scale = effective_capitals[curve_index] / raw_initial_capital
                raw_equity = float(curve.equities[indices[curve_index]])
                current_equities[curve_index] = effective_capitals[curve_index] * (raw_equity / raw_initial_capital)
                if curve.unrealized_drawdowns:
                    current_unrealized_drawdowns[curve_index] = float(curve.unrealized_drawdowns[indices[curve_index]]) * scale
                else:
                    current_unrealized_drawdowns[curve_index] = 0.0
                if curve.capital_loads:
                    current_capital_loads[curve_index] = float(curve.capital_loads[indices[curve_index]]) * scale
                else:
                    current_capital_loads[curve_index] = 0.0
                if curve.open_positions:
                    current_open_positions[curve_index] = int(curve.open_positions[indices[curve_index]])
                else:
                    current_open_positions[curve_index] = 0
                indices[curve_index] += 1
            total += current_equities[curve_index]
            total_unrealized_drawdown += current_unrealized_drawdowns[curve_index]
            total_capital_load += current_capital_loads[curve_index]
            total_open_positions += current_open_positions[curve_index]
        totals.append(total)
        unrealized_totals.append(total_unrealized_drawdown)
        capital_load_totals.append(total_capital_load)
        open_positions_totals.append(total_open_positions)
    return pl.DataFrame(
        {
            "time": all_times,
            "equity": totals,
            "unrealized_drawdown": unrealized_totals,
            "capital_load": capital_load_totals,
            "open_positions": open_positions_totals,
        },
        schema={
            "time": pl.Datetime(time_zone="UTC"),
            "equity": pl.Float64,
            "unrealized_drawdown": pl.Float64,
            "capital_load": pl.Float64,
            "open_positions": pl.Int64,
        },
    )


def summarize_portfolio_equity_series(
    *,
    times: list[datetime],
    equities: list[float],
    unrealized_drawdowns: list[float],
    capital_loads: list[float],
    open_positions: list[int],
    trades_count: int,
) -> PortfolioEquitySummary:
    initial_capital = float(equities[0]) if equities else 0.0
    equity_array = np.asarray(equities, dtype=np.float64)
    ending_equity = float(equities[-1]) if equities else initial_capital
    net_profit = ending_equity - initial_capital
    metrics = compute_equity_curve_metrics(
        equity_total=equity_array,
        initial_capital=initial_capital,
        trades_count=int(trades_count),
        wins=0,
        gross_profit=max(net_profit, 0.0),
        spread_cost=0.0,
        slippage_cost=0.0,
        commission_cost=0.0,
        net_profit=net_profit,
        duration_years=duration_years_from_times(times),
    )
    unrealized_array = np.asarray(unrealized_drawdowns, dtype=np.float64) if unrealized_drawdowns else np.zeros(0, dtype=np.float64)
    load_array = np.asarray(capital_loads, dtype=np.float64) if capital_loads else np.zeros(0, dtype=np.float64)
    open_array = np.asarray(open_positions, dtype=np.int64) if open_positions else np.zeros(0, dtype=np.int64)
    negative_unrealized = -unrealized_array[unrealized_array < 0.0]
    active_loads = load_array[load_array > 0.0]
    active_opens = open_array[open_array > 0]
    peak_equity = float(np.max(equity_array)) if equity_array.size else initial_capital
    return PortfolioEquitySummary(
        initial_capital=initial_capital,
        peak_equity=peak_equity,
        net_profit=float(metrics.get("net_profit", net_profit) or net_profit),
        ending_equity=float(metrics.get("ending_equity", ending_equity) or ending_equity),
        max_drawdown=float(metrics.get("max_drawdown", 0.0) or 0.0),
        trades=int(metrics.get("trades", trades_count) or trades_count),
        cagr=float(metrics.get("cagr", 0.0) or 0.0),
        cagr_to_ulcer=float(metrics.get("cagr_to_ulcer", 0.0) or 0.0),
        r_squared=float(metrics.get("r_squared", 0.0) or 0.0),
        calmar=float(metrics.get("calmar", 0.0) or 0.0),
        beauty_score=float(metrics.get("beauty_score", 0.0) or 0.0),
        max_unrealized_drawdown=float(negative_unrealized.max()) if negative_unrealized.size else 0.0,
        avg_unrealized_drawdown=float(negative_unrealized.mean()) if negative_unrealized.size else 0.0,
        max_capital_load=float(active_loads.max()) if active_loads.size else 0.0,
        avg_capital_load=float(active_loads.mean()) if active_loads.size else 0.0,
        max_capital_load_pct=(float(active_loads.max()) / initial_capital) if active_loads.size and abs(initial_capital) > 1e-12 else 0.0,
        avg_capital_load_pct=(float(active_loads.mean()) / initial_capital) if active_loads.size and abs(initial_capital) > 1e-12 else 0.0,
        max_open_positions=int(active_opens.max()) if active_opens.size else 0,
        avg_open_positions=float(active_opens.mean()) if active_opens.size else 0.0,
    )
