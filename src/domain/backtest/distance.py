from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import floor, isnan
from typing import Any, Mapping

import numpy as np
import polars as pl

from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec, load_quotes_range


@dataclass(slots=True)
class DistanceParameters:
    lookback_bars: int = 96
    entry_z: float = 2.0
    exit_z: float = 0.5
    stop_z: float | None = 3.5
    bollinger_k: float = 2.0


@dataclass(slots=True)
class DistanceBacktestResult:
    frame: pl.DataFrame
    trades: pl.DataFrame
    summary: dict[str, float | int | str]


@dataclass(slots=True)
class _LegSpec:
    symbol: str
    point: float
    contract_size: float
    volume_min: float
    volume_max: float
    volume_step: float
    trade_tick_size: float
    trade_tick_value: float
    trade_tick_value_profit: float
    trade_tick_value_loss: float
    margin_initial: float
    trade_calc_mode: int
    commission_mode: str
    commission_value: float
    commission_currency: str
    commission_minimum: float
    commission_entry_only: bool


@dataclass(slots=True)
class _Position:
    spread_side: int
    entry_index: int
    entry_time: datetime
    reference_entry_price_1: float
    reference_entry_price_2: float
    spread_entry_price_1: float
    spread_entry_price_2: float
    entry_price_1: float
    entry_price_2: float
    lots_1: float
    lots_2: float
    leg_1_side: int
    leg_2_side: int
    entry_commission_1: float
    entry_commission_2: float


TRADE_SCHEMA = {
    "entry_time": pl.Datetime(time_zone="UTC"),
    "exit_time": pl.Datetime(time_zone="UTC"),
    "spread_side": pl.String,
    "leg_1_side": pl.String,
    "leg_2_side": pl.String,
    "entry_price_1": pl.Float64,
    "entry_price_2": pl.Float64,
    "exit_price_1": pl.Float64,
    "exit_price_2": pl.Float64,
    "lots_1": pl.Float64,
    "lots_2": pl.Float64,
    "gross_pnl_leg_1": pl.Float64,
    "gross_pnl_leg_2": pl.Float64,
    "gross_pnl": pl.Float64,
    "spread_cost_leg_1": pl.Float64,
    "spread_cost_leg_2": pl.Float64,
    "spread_cost_total": pl.Float64,
    "slippage_cost_leg_1": pl.Float64,
    "slippage_cost_leg_2": pl.Float64,
    "slippage_cost_total": pl.Float64,
    "commission_leg_1": pl.Float64,
    "commission_leg_2": pl.Float64,
    "commission_total": pl.Float64,
    "pnl_leg_1": pl.Float64,
    "pnl_leg_2": pl.Float64,
    "net_pnl": pl.Float64,
    "exit_reason": pl.String,
}


def _suffix_quotes(frame: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return frame.rename({column: f"{column}{suffix}" for column in frame.columns if column != "time"})


def load_pair_frame(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
) -> pl.DataFrame:
    frame_1 = _suffix_quotes(load_quotes_range(broker, pair.symbol_1, timeframe, started_at, ended_at), "_1")
    frame_2 = _suffix_quotes(load_quotes_range(broker, pair.symbol_2, timeframe, started_at, ended_at), "_2")
    if frame_1.is_empty() or frame_2.is_empty():
        return pl.DataFrame()
    return frame_1.join(frame_2, on="time", how="inner").sort("time")


def _rolling_mean_std(values: np.ndarray, lookback: int) -> tuple[np.ndarray, np.ndarray]:
    mean = np.full(values.shape[0], np.nan, dtype=np.float64)
    std = np.full(values.shape[0], np.nan, dtype=np.float64)
    if lookback <= 1:
        return mean, std

    for idx in range(lookback - 1, values.shape[0]):
        window = values[idx - lookback + 1 : idx + 1]
        mean[idx] = float(window.mean())
        std[idx] = float(window.std())
    return mean, std


def _coerce_leg_spec(symbol: str, spec: Mapping[str, Any] | None, *, point: float, contract_size: float) -> _LegSpec:
    payload = dict(spec or {})
    return _LegSpec(
        symbol=str(payload.get("symbol", symbol) or symbol),
        point=float(payload.get("point", point) or point),
        contract_size=float(payload.get("contract_size", contract_size) or contract_size or 1.0),
        volume_min=float(payload.get("volume_min", 0.0) or 0.0),
        volume_max=float(payload.get("volume_max", 0.0) or 0.0),
        volume_step=float(payload.get("volume_step", 0.0) or 0.0),
        trade_tick_size=float(payload.get("trade_tick_size", 0.0) or 0.0),
        trade_tick_value=float(payload.get("trade_tick_value", 0.0) or 0.0),
        trade_tick_value_profit=float(payload.get("trade_tick_value_profit", 0.0) or 0.0),
        trade_tick_value_loss=float(payload.get("trade_tick_value_loss", 0.0) or 0.0),
        margin_initial=float(payload.get("margin_initial", 0.0) or 0.0),
        trade_calc_mode=int(payload.get("trade_calc_mode", 0) or 0),
        commission_mode=str(payload.get("commission_mode", "none") or "none"),
        commission_value=float(payload.get("commission_value", 0.0) or 0.0),
        commission_currency=str(payload.get("commission_currency", "") or ""),
        commission_minimum=float(payload.get("commission_minimum", 0.0) or 0.0),
        commission_entry_only=bool(payload.get("commission_entry_only", False)),
    )


def _normalize_volume(raw_lots: float, spec: _LegSpec) -> float:
    lots = max(float(raw_lots), 0.0)
    if lots <= 0.0:
        return 0.0
    step = spec.volume_step if spec.volume_step > 0 else 0.0
    if step > 0.0:
        lots = floor((lots + 1e-12) / step) * step
    if spec.volume_min > 0.0:
        lots = max(lots, spec.volume_min)
    if spec.volume_max > 0.0:
        lots = min(lots, spec.volume_max)
    if step > 0.0:
        digits = min(8, max(0, len(f"{step:.8f}".rstrip("0").split(".")[-1]) if "." in f"{step:.8f}" else 0))
        lots = round(lots, digits)
    return max(lots, 0.0)


def _adverse_slippage_offset(side: int, slippage_points: float, point: float) -> float:
    if point <= 0.0 or slippage_points <= 0.0:
        return 0.0
    offset = slippage_points * point
    return offset if side > 0 else -offset


def _buy_spread_offset(side: int, spread_points: float, point: float) -> float:
    if side <= 0 or point <= 0.0 or spread_points <= 0.0:
        return 0.0
    return spread_points * point


def _price_to_account_pnl(price_delta: float, lots: float, side: int, spec: _LegSpec) -> float:
    if abs(price_delta) <= 1e-12 or abs(lots) <= 1e-12:
        return 0.0
    signed_delta = price_delta * side
    profit_tick_value = spec.trade_tick_value_profit or spec.trade_tick_value
    loss_tick_value = spec.trade_tick_value_loss or spec.trade_tick_value
    if spec.trade_tick_size > 0.0 and (profit_tick_value > 0.0 or loss_tick_value > 0.0):
        tick_value = profit_tick_value if signed_delta >= 0.0 else loss_tick_value
        if tick_value > 0.0:
            ticks = abs(price_delta) / spec.trade_tick_size
            return ticks * tick_value * abs(lots) * (1.0 if signed_delta >= 0.0 else -1.0)
    return price_delta * spec.contract_size * abs(lots) * side


def _notional_value(price: float, lots: float, spec: _LegSpec) -> float:
    return abs(price) * spec.contract_size * abs(lots)


def _margin_basis_per_lot(price: float, spec: _LegSpec) -> float:
    margin_initial = float(spec.margin_initial)
    if margin_initial > 0.0:
        return margin_initial
    if int(spec.trade_calc_mode) == 5 and spec.contract_size > 0.0:
        return spec.contract_size
    return abs(price) * max(spec.contract_size, 1e-9)


def _commission_for_fill(price: float, lots: float, spec: _LegSpec, *, is_entry: bool) -> float:
    mode = spec.commission_mode.lower().strip()
    value = float(spec.commission_value)
    if abs(lots) <= 1e-12 or value <= 0.0 or mode in {"", "none"}:
        return 0.0
    if spec.commission_entry_only and not is_entry:
        return 0.0
    if mode == "per_lot_round_turn":
        fee = abs(lots) * value
        if not spec.commission_entry_only:
            fee /= 2.0
    elif mode in {"per_lot_per_side", "mt5_per_lot_per_side"}:
        fee = abs(lots) * value
    elif mode == "percent_notional":
        fee = _notional_value(price, lots, spec) * (value / 100.0)
    elif mode == "flat_per_side":
        fee = value
    elif mode == "points":
        fee = abs(_price_to_account_pnl(value * spec.point, lots, 1, spec))
    else:
        return 0.0
    if fee > 0.0 and spec.commission_minimum > 0.0:
        fee = max(fee, spec.commission_minimum)
    return fee


def _price_with_costs(reference_price: float, side: int, spread_points: float, point: float, slippage_points: float) -> tuple[float, float]:
    spread_offset = _buy_spread_offset(side, spread_points, point)
    spread_price = reference_price + spread_offset
    actual_price = spread_price + _adverse_slippage_offset(side, slippage_points, point)
    return spread_price, actual_price


def _column_or_zeros(frame: pl.DataFrame, name: str) -> np.ndarray:
    if name not in frame.columns:
        return np.zeros(frame.height, dtype=np.float64)
    return frame.get_column(name).to_numpy().astype(np.float64)


def _build_summary(
    pair: PairSelection,
    defaults: StrategyDefaults,
    net_pnl: float,
    trades: pl.DataFrame,
    equity_total: np.ndarray,
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
    return {
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
        return DistanceBacktestResult(pl.DataFrame(), pl.DataFrame(schema=TRADE_SCHEMA), {"symbol_1": pair.symbol_1, "symbol_2": pair.symbol_2, "trades": 0})

    leg_spec_1 = _coerce_leg_spec(pair.symbol_1, spec_1, point=point_1, contract_size=contract_size_1)
    leg_spec_2 = _coerce_leg_spec(pair.symbol_2, spec_2, point=point_2, contract_size=contract_size_2)

    times = frame.get_column("time").to_list()
    open_1 = frame.get_column("open_1").to_numpy()
    close_1 = frame.get_column("close_1").to_numpy()
    open_2 = frame.get_column("open_2").to_numpy()
    close_2 = frame.get_column("close_2").to_numpy()
    spread_points_1 = _column_or_zeros(frame, "spread_1")
    spread_points_2 = _column_or_zeros(frame, "spread_2")

    normalized_1 = close_1 / close_1[0]
    normalized_2 = close_2 / close_2[0]
    spread = normalized_1 - normalized_2
    spread_mean, spread_std = _rolling_mean_std(spread, params.lookback_bars)
    zscore = np.full(frame.height, np.nan, dtype=np.float64)
    valid_spread_mask = spread_std > 0
    zscore[valid_spread_mask] = (
        spread[valid_spread_mask] - spread_mean[valid_spread_mask]
    ) / spread_std[valid_spread_mask]
    z_mean, z_std = _rolling_mean_std(np.nan_to_num(zscore, nan=0.0), params.lookback_bars)
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
    exposure_per_leg = defaults.margin_budget_per_leg * defaults.leverage

    for idx in range(frame.height):
        if active is not None:
            leg_1_pnl = _price_to_account_pnl(close_1[idx] - active.entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
            leg_2_pnl = _price_to_account_pnl(close_2[idx] - active.entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
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
            spread_entry_price_1, entry_price_1 = _price_with_costs(reference_entry_price_1, leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults.slippage_points)
            spread_entry_price_2, entry_price_2 = _price_with_costs(reference_entry_price_2, leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults.slippage_points)
            raw_lots_1 = exposure_per_leg / max(_margin_basis_per_lot(reference_entry_price_1, leg_spec_1), 1e-9)
            raw_lots_2 = exposure_per_leg / max(_margin_basis_per_lot(reference_entry_price_2, leg_spec_2), 1e-9)
            lots_1 = _normalize_volume(raw_lots_1, leg_spec_1)
            lots_2 = _normalize_volume(raw_lots_2, leg_spec_2)
            entry_commission_1 = _commission_for_fill(entry_price_1, lots_1, leg_spec_1, is_entry=True)
            entry_commission_2 = _commission_for_fill(entry_price_2, lots_2, leg_spec_2, is_entry=True)
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
        exit_reason = ""
        if active.spread_side == -1 and signal <= params.exit_z:
            should_exit = True
            exit_reason = "mean_reversion"
        elif active.spread_side == 1 and signal >= -params.exit_z:
            should_exit = True
            exit_reason = "mean_reversion"
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
        spread_exit_price_1, exit_price_1 = _price_with_costs(reference_exit_price_1, -active.leg_1_side, float(spread_points_1[idx]), leg_spec_1.point, defaults.slippage_points)
        spread_exit_price_2, exit_price_2 = _price_with_costs(reference_exit_price_2, -active.leg_2_side, float(spread_points_2[idx]), leg_spec_2.point, defaults.slippage_points)
        exit_commission_1 = _commission_for_fill(exit_price_1, active.lots_1, leg_spec_1, is_entry=False)
        exit_commission_2 = _commission_for_fill(exit_price_2, active.lots_2, leg_spec_2, is_entry=False)

        gross_pnl_leg_1 = _price_to_account_pnl(reference_exit_price_1 - active.reference_entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        gross_pnl_leg_2 = _price_to_account_pnl(reference_exit_price_2 - active.reference_entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
        spread_pnl_leg_1 = _price_to_account_pnl(spread_exit_price_1 - active.spread_entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        spread_pnl_leg_2 = _price_to_account_pnl(spread_exit_price_2 - active.spread_entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)
        slippage_pnl_leg_1 = _price_to_account_pnl(exit_price_1 - active.entry_price_1, active.lots_1, active.leg_1_side, leg_spec_1)
        slippage_pnl_leg_2 = _price_to_account_pnl(exit_price_2 - active.entry_price_2, active.lots_2, active.leg_2_side, leg_spec_2)

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
    )
    return DistanceBacktestResult(frame=result_frame, trades=trades_frame, summary=summary)


def run_distance_backtest(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    defaults: StrategyDefaults,
    params: DistanceParameters,
) -> DistanceBacktestResult:
    frame = load_pair_frame(broker=broker, pair=pair, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return DistanceBacktestResult(pl.DataFrame(), pl.DataFrame(schema=TRADE_SCHEMA), {"symbol_1": pair.symbol_1, "symbol_2": pair.symbol_2, "trades": 0})

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
