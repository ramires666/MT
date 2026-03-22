from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import polars as pl


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
