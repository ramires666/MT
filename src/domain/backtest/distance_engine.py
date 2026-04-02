from __future__ import annotations

from dataclasses import dataclass, field
from math import isnan
from typing import Any, Mapping

import numpy as np
import polars as pl

from domain.backtest.metric_formulas import compute_equity_curve_metrics, duration_years_from_times
from domain.backtest.distance_models import (
    TRADE_SCHEMA,
    DistanceBacktestResult,
    DistanceParameters,
    _LegSpec,
    _Position,
)
from domain.backtest.kernel import rolling_mean_std
from domain.backtest.distance_pricing import (
    coerce_leg_spec,
    commission_for_fill,
    margin_basis_per_lot,
    normalize_volume,
    price_to_account_pnl,
    price_with_costs,
)
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec, load_quotes_range


def _suffix_quotes(frame: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return frame.rename({column: f"{column}{suffix}" for column in frame.columns if column != "time"})


def load_pair_frame(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at,
    ended_at,
) -> pl.DataFrame:
    frame_1 = _suffix_quotes(load_quotes_range(broker, pair.symbol_1, timeframe, started_at, ended_at), "_1")
    frame_2 = _suffix_quotes(load_quotes_range(broker, pair.symbol_2, timeframe, started_at, ended_at), "_2")
    if frame_1.is_empty() or frame_2.is_empty():
        return pl.DataFrame()
    return frame_1.join(frame_2, on="time", how="inner").sort("time")


@dataclass(slots=True)
class _DistanceSignalState:
    spread_mean: np.ndarray
    spread_std: np.ndarray
    zscore: np.ndarray
    z_mean: np.ndarray
    z_std: np.ndarray


@dataclass(slots=True)
class _DistanceBacktestContext:
    times: list[object]
    open_1: np.ndarray
    close_1: np.ndarray
    open_2: np.ndarray
    close_2: np.ndarray
    spread_points_1: np.ndarray
    spread_points_2: np.ndarray
    spread: np.ndarray
    defaults: StrategyDefaults
    leg_spec_1: _LegSpec
    leg_spec_2: _LegSpec
    exposure_per_leg: float
    signal_cache: dict[int, _DistanceSignalState] = field(default_factory=dict)


def _column_or_zeros(frame: pl.DataFrame, name: str) -> np.ndarray:
    if name not in frame.columns:
        return np.zeros(frame.height, dtype=np.float64)
    return frame.get_column(name).to_numpy().astype(np.float64)


def prepare_distance_backtest_context(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
) -> _DistanceBacktestContext:
    leg_spec_1 = coerce_leg_spec(pair.symbol_1, spec_1, point=point_1, contract_size=contract_size_1)
    leg_spec_2 = coerce_leg_spec(pair.symbol_2, spec_2, point=point_2, contract_size=contract_size_2)
    close_1 = frame.get_column("close_1").to_numpy().astype(np.float64)
    close_2 = frame.get_column("close_2").to_numpy().astype(np.float64)
    normalized_1 = close_1 / close_1[0]
    normalized_2 = close_2 / close_2[0]
    return _DistanceBacktestContext(
        times=frame.get_column("time").to_list(),
        open_1=frame.get_column("open_1").to_numpy().astype(np.float64),
        close_1=close_1,
        open_2=frame.get_column("open_2").to_numpy().astype(np.float64),
        close_2=close_2,
        spread_points_1=_column_or_zeros(frame, "spread_1"),
        spread_points_2=_column_or_zeros(frame, "spread_2"),
        spread=normalized_1 - normalized_2,
        defaults=defaults,
        leg_spec_1=leg_spec_1,
        leg_spec_2=leg_spec_2,
        exposure_per_leg=defaults.margin_budget_per_leg * defaults.leverage,
    )


def _signal_state(context: _DistanceBacktestContext, lookback: int) -> _DistanceSignalState:
    cached = context.signal_cache.get(int(lookback))
    if cached is not None:
        return cached
    spread_mean, spread_std = rolling_mean_std(context.spread, int(lookback))
    zscore = np.full(context.spread.shape[0], np.nan, dtype=np.float64)
    valid_spread_mask = spread_std > 0
    zscore[valid_spread_mask] = (
        context.spread[valid_spread_mask] - spread_mean[valid_spread_mask]
    ) / spread_std[valid_spread_mask]
    z_mean, z_std = rolling_mean_std(np.nan_to_num(zscore, nan=0.0), int(lookback))
    cached = _DistanceSignalState(
        spread_mean=spread_mean,
        spread_std=spread_std,
        zscore=zscore,
        z_mean=z_mean,
        z_std=z_std,
    )
    context.signal_cache[int(lookback)] = cached
    return cached


def _signal_exit_reason(spread_side: int, signal: float, exit_z: float) -> str | None:
    if spread_side == -1 and signal <= exit_z:
        return "opposite_signal" if exit_z < 0.0 else "mean_reversion"
    if spread_side == 1 and signal >= -exit_z:
        return "opposite_signal" if exit_z < 0.0 else "mean_reversion"
    return None


def _build_summary(
    pair: PairSelection,
    defaults: StrategyDefaults,
    net_pnl: float,
    trades: pl.DataFrame,
    equity_total: np.ndarray,
    times: list[object],
) -> dict[str, float | int | str]:
    trades_count = trades.height
    wins = trades.filter(pl.col("net_pnl") > 0).height if trades_count else 0
    peak = float(np.maximum.accumulate(equity_total).max()) if equity_total.size else defaults.initial_capital
    drawdowns = equity_total - np.maximum.accumulate(equity_total)
    max_drawdown = float(drawdowns.min()) if drawdowns.size else 0.0
    gross_pnl = float(trades.get_column("gross_pnl").sum()) if trades_count else 0.0
    spread_cost = float(trades.get_column("spread_cost_total").sum()) if trades_count else 0.0
    slippage_cost = float(trades.get_column("slippage_cost_total").sum()) if trades_count else 0.0
    commission_cost = float(trades.get_column("commission_total").sum()) if trades_count else 0.0
    total_cost = spread_cost + slippage_cost + commission_cost
    summary = {
        "symbol_1": pair.symbol_1,
        "symbol_2": pair.symbol_2,
        "trades": trades_count,
        "wins": wins,
        "losses": trades_count - wins,
        "win_rate": (wins / trades_count) if trades_count else 0.0,
        "gross_pnl": gross_pnl,
        "total_spread_cost": spread_cost,
        "total_slippage_cost": slippage_cost,
        "total_commission": commission_cost,
        "total_cost": total_cost,
        "net_pnl": net_pnl,
        "initial_capital": float(defaults.initial_capital),
        "ending_equity": float(equity_total[-1]) if equity_total.size else defaults.initial_capital,
        "max_drawdown": max_drawdown,
        "peak_equity": peak,
    }
    curve_metrics = compute_equity_curve_metrics(
        equity_total=equity_total,
        initial_capital=float(defaults.initial_capital),
        trades_count=trades_count,
        wins=wins,
        gross_profit=gross_pnl,
        spread_cost=spread_cost,
        slippage_cost=slippage_cost,
        commission_cost=commission_cost,
        net_profit=net_pnl,
        duration_years=duration_years_from_times(times),
    )
    curve_metrics.pop("max_drawdown", None)
    summary.update(curve_metrics)
    return summary


def _empty_result(pair: PairSelection) -> DistanceBacktestResult:
    return DistanceBacktestResult(
        pl.DataFrame(),
        pl.DataFrame(schema=TRADE_SCHEMA),
        {"symbol_1": pair.symbol_1, "symbol_2": pair.symbol_2, "trades": 0},
    )


def _empty_metrics(defaults: StrategyDefaults) -> dict[str, float | int]:
    return {
        "net_profit": 0.0,
        "ending_equity": float(defaults.initial_capital),
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
        "trades": 0,
        "win_rate": 0.0,
    }


def _finalize_metrics(
    *,
    defaults: StrategyDefaults,
    equity_total: np.ndarray,
    times: list[object],
    trades_count: int,
    wins: int,
    gross_profit: float,
    spread_cost: float,
    slippage_cost: float,
    commission_cost: float,
    net_profit: float,
) -> dict[str, float | int]:
    if equity_total.size == 0:
        return _empty_metrics(defaults)
    return compute_equity_curve_metrics(
        equity_total=equity_total,
        initial_capital=float(defaults.initial_capital),
        trades_count=trades_count,
        wins=wins,
        gross_profit=float(gross_profit),
        spread_cost=float(spread_cost),
        slippage_cost=float(slippage_cost),
        commission_cost=float(commission_cost),
        net_profit=float(net_profit),
        duration_years=duration_years_from_times(times),
    )


def run_distance_backtest_metrics_frame(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    params: DistanceParameters,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
    context: _DistanceBacktestContext | None = None,
) -> dict[str, float | int]:
    if frame.is_empty():
        return _empty_metrics(defaults)

    context = context or prepare_distance_backtest_context(
        frame=frame,
        pair=pair,
        defaults=defaults,
        point_1=point_1,
        point_2=point_2,
        contract_size_1=contract_size_1,
        contract_size_2=contract_size_2,
        spec_1=spec_1,
        spec_2=spec_2,
    )
    state = _signal_state(context, params.lookback_bars)
    defaults_local = context.defaults
    close_1 = context.close_1
    close_2 = context.close_2
    open_1 = context.open_1
    open_2 = context.open_2
    spread_points_1 = context.spread_points_1
    spread_points_2 = context.spread_points_2
    zscore = state.zscore
    leg_spec_1 = context.leg_spec_1
    leg_spec_2 = context.leg_spec_2

    equity_total = np.full(frame.height, defaults_local.initial_capital, dtype=np.float64)
    cumulative_leg_1 = 0.0
    cumulative_leg_2 = 0.0
    active = False
    active_spread_side = 0
    active_entry_price_1 = 0.0
    active_entry_price_2 = 0.0
    active_reference_entry_price_1 = 0.0
    active_reference_entry_price_2 = 0.0
    active_spread_entry_price_1 = 0.0
    active_spread_entry_price_2 = 0.0
    active_lots_1 = 0.0
    active_lots_2 = 0.0
    active_leg_1_side = 0
    active_leg_2_side = 0
    active_entry_commission_1 = 0.0
    active_entry_commission_2 = 0.0
    trades_count = 0
    wins = 0
    gross_profit = 0.0
    spread_cost = 0.0
    slippage_cost = 0.0
    commission_cost = 0.0
    net_profit = 0.0

    for idx in range(frame.height):
        if active:
            leg_1_pnl = price_to_account_pnl(close_1[idx] - active_entry_price_1, active_lots_1, active_leg_1_side, leg_spec_1)
            leg_2_pnl = price_to_account_pnl(close_2[idx] - active_entry_price_2, active_lots_2, active_leg_2_side, leg_spec_2)
            equity_total[idx] = defaults_local.initial_capital + cumulative_leg_1 + cumulative_leg_2 + leg_1_pnl + leg_2_pnl
        elif idx > 0:
            equity_total[idx] = equity_total[idx - 1]

        if idx == 0:
            continue

        signal = zscore[idx - 1]
        if isnan(signal):
            continue

        if not active:
            if signal >= params.entry_z:
                leg_1_side = -1
                leg_2_side = 1
                spread_side = -1
            elif signal <= -params.entry_z:
                leg_1_side = 1
                leg_2_side = -1
                spread_side = 1
            else:
                continue

            reference_entry_price_1 = float(open_1[idx])
            reference_entry_price_2 = float(open_2[idx])
            spread_entry_price_1, entry_price_1 = price_with_costs(reference_entry_price_1, leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults_local.slippage_points)
            spread_entry_price_2, entry_price_2 = price_with_costs(reference_entry_price_2, leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults_local.slippage_points)
            raw_lots_1 = context.exposure_per_leg / max(margin_basis_per_lot(reference_entry_price_1, leg_spec_1), 1e-9)
            raw_lots_2 = context.exposure_per_leg / max(margin_basis_per_lot(reference_entry_price_2, leg_spec_2), 1e-9)
            lots_1 = normalize_volume(raw_lots_1, leg_spec_1)
            lots_2 = normalize_volume(raw_lots_2, leg_spec_2)
            entry_commission_1 = commission_for_fill(entry_price_1, lots_1, leg_spec_1, is_entry=True)
            entry_commission_2 = commission_for_fill(entry_price_2, lots_2, leg_spec_2, is_entry=True)
            cumulative_leg_1 -= entry_commission_1
            cumulative_leg_2 -= entry_commission_2
            equity_total[idx] = defaults_local.initial_capital + cumulative_leg_1 + cumulative_leg_2
            active = True
            active_spread_side = spread_side
            active_reference_entry_price_1 = reference_entry_price_1
            active_reference_entry_price_2 = reference_entry_price_2
            active_spread_entry_price_1 = spread_entry_price_1
            active_spread_entry_price_2 = spread_entry_price_2
            active_entry_price_1 = entry_price_1
            active_entry_price_2 = entry_price_2
            active_lots_1 = lots_1
            active_lots_2 = lots_2
            active_leg_1_side = leg_1_side
            active_leg_2_side = leg_2_side
            active_entry_commission_1 = entry_commission_1
            active_entry_commission_2 = entry_commission_2
            continue

        should_exit = False
        exit_reason = _signal_exit_reason(active_spread_side, signal, params.exit_z)
        if exit_reason is not None:
            should_exit = True
        elif params.stop_z is not None and abs(signal) >= params.stop_z:
            should_exit = True
        elif idx == frame.height - 1:
            should_exit = True

        if not should_exit:
            continue

        reference_exit_price_1 = float(open_1[idx])
        reference_exit_price_2 = float(open_2[idx])
        spread_exit_price_1, exit_price_1 = price_with_costs(reference_exit_price_1, -active_leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults_local.slippage_points)
        spread_exit_price_2, exit_price_2 = price_with_costs(reference_exit_price_2, -active_leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults_local.slippage_points)
        exit_commission_1 = commission_for_fill(exit_price_1, active_lots_1, leg_spec_1, is_entry=False)
        exit_commission_2 = commission_for_fill(exit_price_2, active_lots_2, leg_spec_2, is_entry=False)
        gross_pnl_leg_1 = price_to_account_pnl(reference_exit_price_1 - active_reference_entry_price_1, active_lots_1, active_leg_1_side, leg_spec_1)
        gross_pnl_leg_2 = price_to_account_pnl(reference_exit_price_2 - active_reference_entry_price_2, active_lots_2, active_leg_2_side, leg_spec_2)
        spread_pnl_leg_1 = price_to_account_pnl(spread_exit_price_1 - active_spread_entry_price_1, active_lots_1, active_leg_1_side, leg_spec_1)
        spread_pnl_leg_2 = price_to_account_pnl(spread_exit_price_2 - active_spread_entry_price_2, active_lots_2, active_leg_2_side, leg_spec_2)
        slippage_pnl_leg_1 = price_to_account_pnl(exit_price_1 - active_entry_price_1, active_lots_1, active_leg_1_side, leg_spec_1)
        slippage_pnl_leg_2 = price_to_account_pnl(exit_price_2 - active_entry_price_2, active_lots_2, active_leg_2_side, leg_spec_2)
        spread_cost_leg_1 = gross_pnl_leg_1 - spread_pnl_leg_1
        spread_cost_leg_2 = gross_pnl_leg_2 - spread_pnl_leg_2
        slippage_cost_leg_1 = spread_pnl_leg_1 - slippage_pnl_leg_1
        slippage_cost_leg_2 = spread_pnl_leg_2 - slippage_pnl_leg_2
        commission_leg_1 = active_entry_commission_1 + exit_commission_1
        commission_leg_2 = active_entry_commission_2 + exit_commission_2
        realized_leg_1 = slippage_pnl_leg_1 - exit_commission_1
        realized_leg_2 = slippage_pnl_leg_2 - exit_commission_2
        trade_pnl_leg_1 = slippage_pnl_leg_1 - commission_leg_1
        trade_pnl_leg_2 = slippage_pnl_leg_2 - commission_leg_2
        trade_net = trade_pnl_leg_1 + trade_pnl_leg_2
        cumulative_leg_1 += realized_leg_1
        cumulative_leg_2 += realized_leg_2
        equity_total[idx] = defaults_local.initial_capital + cumulative_leg_1 + cumulative_leg_2
        trades_count += 1
        if trade_net > 0.0:
            wins += 1
        gross_profit += gross_pnl_leg_1 + gross_pnl_leg_2
        spread_cost += spread_cost_leg_1 + spread_cost_leg_2
        slippage_cost += slippage_cost_leg_1 + slippage_cost_leg_2
        commission_cost += commission_leg_1 + commission_leg_2
        net_profit += trade_net
        active = False

    return _finalize_metrics(
        defaults=defaults_local,
        equity_total=equity_total,
        times=context.times,
        trades_count=trades_count,
        wins=wins,
        gross_profit=gross_profit,
        spread_cost=spread_cost,
        slippage_cost=slippage_cost,
        commission_cost=commission_cost,
        net_profit=net_profit,
    )


def run_distance_backtest_frame(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    params: DistanceParameters,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
) -> DistanceBacktestResult:
    if frame.is_empty():
        return _empty_result(pair)

    context = prepare_distance_backtest_context(
        frame=frame,
        pair=pair,
        defaults=defaults,
        point_1=point_1,
        point_2=point_2,
        contract_size_1=contract_size_1,
        contract_size_2=contract_size_2,
        spec_1=spec_1,
        spec_2=spec_2,
    )
    state = _signal_state(context, params.lookback_bars)
    leg_spec_1 = context.leg_spec_1
    leg_spec_2 = context.leg_spec_2
    times = context.times
    open_1 = context.open_1
    close_1 = context.close_1
    open_2 = context.open_2
    close_2 = context.close_2
    spread_points_1 = context.spread_points_1
    spread_points_2 = context.spread_points_2
    spread = context.spread
    spread_mean = state.spread_mean
    zscore = state.zscore
    z_mean = state.z_mean
    z_std = state.z_std
    z_upper = z_mean + params.bollinger_k * z_std
    z_lower = z_mean - params.bollinger_k * z_std

    equity_total = np.full(frame.height, defaults.initial_capital, dtype=np.float64)
    equity_leg_1 = np.full(frame.height, defaults.initial_capital, dtype=np.float64)
    equity_leg_2 = np.full(frame.height, defaults.initial_capital, dtype=np.float64)
    position = np.zeros(frame.height, dtype=np.int8)

    trades: list[dict[str, object]] = []
    active: _Position | None = None
    cumulative_leg_1 = 0.0
    cumulative_leg_2 = 0.0
    exposure_per_leg = context.exposure_per_leg

    for idx in range(frame.height):
        if active is not None:
            leg_1_pnl = price_to_account_pnl(close_1[idx] - active.entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
            leg_2_pnl = price_to_account_pnl(close_2[idx] - active.entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
            equity_leg_1[idx] = defaults.initial_capital + cumulative_leg_1 + leg_1_pnl
            equity_leg_2[idx] = defaults.initial_capital + cumulative_leg_2 + leg_2_pnl
            equity_total[idx] = defaults.initial_capital + cumulative_leg_1 + cumulative_leg_2 + leg_1_pnl + leg_2_pnl
            position[idx] = active.spread_side
        elif idx > 0:
            equity_leg_1[idx] = equity_leg_1[idx - 1]
            equity_leg_2[idx] = equity_leg_2[idx - 1]
            equity_total[idx] = equity_total[idx - 1]

        if idx == 0:
            continue

        signal = zscore[idx - 1]
        if isnan(signal):
            continue

        if active is None:
            if signal >= params.entry_z:
                leg_1_side = -1
                leg_2_side = 1
                spread_side = -1
            elif signal <= -params.entry_z:
                leg_1_side = 1
                leg_2_side = -1
                spread_side = 1
            else:
                continue

            reference_entry_price_1 = float(open_1[idx])
            reference_entry_price_2 = float(open_2[idx])
            spread_entry_price_1, entry_price_1 = price_with_costs(reference_entry_price_1, leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults.slippage_points)
            spread_entry_price_2, entry_price_2 = price_with_costs(reference_entry_price_2, leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults.slippage_points)
            raw_lots_1 = exposure_per_leg / max(margin_basis_per_lot(reference_entry_price_1, leg_spec_1), 1e-9)
            raw_lots_2 = exposure_per_leg / max(margin_basis_per_lot(reference_entry_price_2, leg_spec_2), 1e-9)
            lots_1 = normalize_volume(raw_lots_1, leg_spec_1)
            lots_2 = normalize_volume(raw_lots_2, leg_spec_2)
            entry_commission_1 = commission_for_fill(entry_price_1, lots_1, leg_spec_1, is_entry=True)
            entry_commission_2 = commission_for_fill(entry_price_2, lots_2, leg_spec_2, is_entry=True)
            cumulative_leg_1 -= entry_commission_1
            cumulative_leg_2 -= entry_commission_2
            equity_leg_1[idx] = defaults.initial_capital + cumulative_leg_1
            equity_leg_2[idx] = defaults.initial_capital + cumulative_leg_2
            equity_total[idx] = defaults.initial_capital + cumulative_leg_1 + cumulative_leg_2
            active = _Position(
                spread_side=spread_side,
                entry_index=idx,
                entry_time=times[idx],
                reference_entry_price_1=reference_entry_price_1,
                reference_entry_price_2=reference_entry_price_2,
                spread_entry_price_1=spread_entry_price_1,
                spread_entry_price_2=spread_entry_price_2,
                entry_price_1=entry_price_1,
                entry_price_2=entry_price_2,
                lots_1=lots_1,
                lots_2=lots_2,
                leg_1_side=leg_1_side,
                leg_2_side=leg_2_side,
                entry_commission_1=entry_commission_1,
                entry_commission_2=entry_commission_2,
            )
            position[idx] = active.spread_side
            continue

        should_exit = False
        exit_reason = _signal_exit_reason(active.spread_side, signal, params.exit_z)
        if exit_reason is not None:
            should_exit = True
        elif params.stop_z is not None and abs(signal) >= params.stop_z:
            should_exit = True
            exit_reason = "stop_z"
        elif idx == frame.height - 1:
            should_exit = True
            exit_reason = "end_of_period"

        if not should_exit:
            continue

        reference_exit_price_1 = float(open_1[idx])
        reference_exit_price_2 = float(open_2[idx])
        spread_exit_price_1, exit_price_1 = price_with_costs(reference_exit_price_1, -active.leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults.slippage_points)
        spread_exit_price_2, exit_price_2 = price_with_costs(reference_exit_price_2, -active.leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults.slippage_points)
        exit_commission_1 = commission_for_fill(exit_price_1, active.lots_1, leg_spec_1, is_entry=False)
        exit_commission_2 = commission_for_fill(exit_price_2, active.lots_2, leg_spec_2, is_entry=False)

        gross_pnl_leg_1 = price_to_account_pnl(reference_exit_price_1 - active.reference_entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        gross_pnl_leg_2 = price_to_account_pnl(reference_exit_price_2 - active.reference_entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
        spread_pnl_leg_1 = price_to_account_pnl(spread_exit_price_1 - active.spread_entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        spread_pnl_leg_2 = price_to_account_pnl(spread_exit_price_2 - active.spread_entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
        slippage_pnl_leg_1 = price_to_account_pnl(exit_price_1 - active.entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        slippage_pnl_leg_2 = price_to_account_pnl(exit_price_2 - active.entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)

        spread_cost_leg_1 = gross_pnl_leg_1 - spread_pnl_leg_1
        spread_cost_leg_2 = gross_pnl_leg_2 - spread_pnl_leg_2
        slippage_cost_leg_1 = spread_pnl_leg_1 - slippage_pnl_leg_1
        slippage_cost_leg_2 = spread_pnl_leg_2 - slippage_pnl_leg_2
        commission_leg_1 = active.entry_commission_1 + exit_commission_1
        commission_leg_2 = active.entry_commission_2 + exit_commission_2
        realized_leg_1 = slippage_pnl_leg_1 - exit_commission_1
        realized_leg_2 = slippage_pnl_leg_2 - exit_commission_2
        trade_pnl_leg_1 = slippage_pnl_leg_1 - commission_leg_1
        trade_pnl_leg_2 = slippage_pnl_leg_2 - commission_leg_2
        cumulative_leg_1 += realized_leg_1
        cumulative_leg_2 += realized_leg_2
        equity_leg_1[idx] = defaults.initial_capital + cumulative_leg_1
        equity_leg_2[idx] = defaults.initial_capital + cumulative_leg_2
        equity_total[idx] = defaults.initial_capital + cumulative_leg_1 + cumulative_leg_2

        trades.append(
            {
                "entry_time": active.entry_time,
                "exit_time": times[idx],
                "spread_side": "long_spread" if active.spread_side > 0 else "short_spread",
                "leg_1_side": "long" if active.leg_1_side > 0 else "short",
                "leg_2_side": "long" if active.leg_2_side > 0 else "short",
                "entry_price_1": active.entry_price_1,
                "entry_price_2": active.entry_price_2,
                "exit_price_1": exit_price_1,
                "exit_price_2": exit_price_2,
                "lots_1": active.lots_1,
                "lots_2": active.lots_2,
                "gross_pnl_leg_1": gross_pnl_leg_1,
                "gross_pnl_leg_2": gross_pnl_leg_2,
                "gross_pnl": gross_pnl_leg_1 + gross_pnl_leg_2,
                "spread_cost_leg_1": spread_cost_leg_1,
                "spread_cost_leg_2": spread_cost_leg_2,
                "spread_cost_total": spread_cost_leg_1 + spread_cost_leg_2,
                "slippage_cost_leg_1": slippage_cost_leg_1,
                "slippage_cost_leg_2": slippage_cost_leg_2,
                "slippage_cost_total": slippage_cost_leg_1 + slippage_cost_leg_2,
                "commission_leg_1": commission_leg_1,
                "commission_leg_2": commission_leg_2,
                "commission_total": commission_leg_1 + commission_leg_2,
                "pnl_leg_1": trade_pnl_leg_1,
                "pnl_leg_2": trade_pnl_leg_2,
                "net_pnl": trade_pnl_leg_1 + trade_pnl_leg_2,
                "exit_reason": exit_reason,
            }
        )
        active = None

    result_frame = frame.with_columns(
        pl.Series("spread", spread),
        pl.Series("spread_mean", spread_mean),
        pl.Series("zscore", zscore),
        pl.Series("zscore_mean", z_mean),
        pl.Series("zscore_upper", z_upper),
        pl.Series("zscore_lower", z_lower),
        pl.Series("equity_total", equity_total),
        pl.Series("equity_leg_1", equity_leg_1),
        pl.Series("equity_leg_2", equity_leg_2),
        pl.Series("position", position),
    )
    trades_frame = pl.DataFrame(trades) if trades else pl.DataFrame(schema=TRADE_SCHEMA)
    net_pnl = float(trades_frame.get_column("net_pnl").sum()) if not trades_frame.is_empty() else 0.0
    summary = _build_summary(
        pair=pair,
        defaults=defaults,
        net_pnl=net_pnl,
        trades=trades_frame,
        equity_total=equity_total,
        times=result_frame.get_column("time").to_list(),
    )
    return DistanceBacktestResult(frame=result_frame, trades=trades_frame, summary=summary)


def run_distance_backtest(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at,
    ended_at,
    defaults: StrategyDefaults,
    params: DistanceParameters,
) -> DistanceBacktestResult:
    frame = load_pair_frame(broker=broker, pair=pair, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return _empty_result(pair)

    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    return run_distance_backtest_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
    )
