from __future__ import annotations

from math import floor
from typing import Any, Mapping

from domain.backtest.distance_models import _LegSpec


def coerce_leg_spec(symbol: str, spec: Mapping[str, Any] | None, *, point: float, contract_size: float) -> _LegSpec:
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


def normalize_volume(raw_lots: float, spec: _LegSpec) -> float:
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


def adverse_slippage_offset(side: int, slippage_points: float, point: float) -> float:
    if point <= 0.0 or slippage_points <= 0.0:
        return 0.0
    offset = slippage_points * point
    return offset if side > 0 else -offset


def buy_spread_offset(side: int, spread_points: float, point: float) -> float:
    if side <= 0 or point <= 0.0 or spread_points <= 0.0:
        return 0.0
    return spread_points * point


def price_to_account_pnl(price_delta: float, lots: float, side: int, spec: _LegSpec) -> float:
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


def notional_value(price: float, lots: float, spec: _LegSpec) -> float:
    return abs(price) * spec.contract_size * abs(lots)


def margin_basis_per_lot(price: float, spec: _LegSpec) -> float:
    margin_initial = float(spec.margin_initial)
    if margin_initial > 0.0:
        return margin_initial
    if int(spec.trade_calc_mode) == 5 and spec.contract_size > 0.0:
        return spec.contract_size
    return abs(price) * max(spec.contract_size, 1e-9)


def commission_for_fill(price: float, lots: float, spec: _LegSpec, *, is_entry: bool) -> float:
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
        fee = notional_value(price, lots, spec) * (value / 100.0)
    elif mode == "flat_per_side":
        fee = value
    elif mode == "points":
        fee = abs(price_to_account_pnl(value * spec.point, lots, 1, spec))
    else:
        return 0.0
    if fee > 0.0 and spec.commission_minimum > 0.0:
        fee = max(fee, spec.commission_minimum)
    return fee


def price_with_costs(reference_price: float, side: int, spread_points: float, point: float, slippage_points: float) -> tuple[float, float]:
    spread_offset = buy_spread_offset(side, spread_points, point)
    spread_price = reference_price + spread_offset
    actual_price = spread_price + adverse_slippage_offset(side, slippage_points, point)
    return spread_price, actual_price
