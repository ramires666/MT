from __future__ import annotations

from datetime import datetime
from math import expm1, log, log1p, sqrt
from typing import Sequence

import numpy as np


SECONDS_PER_YEAR = 365.2425 * 24.0 * 60.0 * 60.0
MAX_FINITE_METRIC_ABS = 1_000_000_000_000.0
LOG_MAX_FINITE_METRIC = log1p(MAX_FINITE_METRIC_ABS)


def clamp_metric_value(value: float) -> float:
    if np.isnan(value):
        return 0.0
    if np.isposinf(value):
        return MAX_FINITE_METRIC_ABS
    if np.isneginf(value):
        return -MAX_FINITE_METRIC_ABS
    return max(-MAX_FINITE_METRIC_ABS, min(MAX_FINITE_METRIC_ABS, float(value)))


def safe_ratio(numerator: float, denominator: float) -> float:
    if not np.isfinite(numerator) or not np.isfinite(denominator):
        return 0.0
    if abs(denominator) <= 1e-12:
        return 0.0
    return clamp_metric_value(numerator / denominator)


def compute_k_ratio(equity: np.ndarray) -> float:
    if equity.size < 3:
        return 0.0
    clipped = np.maximum(equity.astype(np.float64, copy=False), 1e-9)
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


def compute_r_squared(equity: np.ndarray) -> float:
    if equity.size < 2:
        return 0.0
    clipped = np.maximum(equity.astype(np.float64, copy=False), 1e-9)
    y = np.log(clipped)
    x = np.arange(y.size, dtype=np.float64)
    x_mean = float(x.mean())
    y_mean = float(y.mean())
    ss_x = float(np.square(x - x_mean).sum())
    if ss_x <= 1e-12:
        return 0.0
    slope = float(np.dot(x - x_mean, y - y_mean) / ss_x)
    intercept = y_mean - slope * x_mean
    fitted = intercept + (slope * x)
    residuals = y - fitted
    ss_tot = float(np.square(y - y_mean).sum())
    if ss_tot <= 1e-12:
        return 1.0
    r_squared = 1.0 - (float(np.square(residuals).sum()) / ss_tot)
    if not np.isfinite(r_squared):
        return 0.0
    return max(0.0, min(1.0, float(r_squared)))


def compute_hurst_exponent(values: np.ndarray) -> float:
    series = np.asarray(values, dtype=np.float64)
    if series.size < 32:
        return 0.0
    finite = series[np.isfinite(series)]
    if finite.size < 32:
        return 0.0
    clipped = np.maximum(finite, 1e-9)
    transformed = np.log(clipped)
    max_lag = min(64, max(8, transformed.size // 2))
    lag_values: list[float] = []
    tau_values: list[float] = []
    for lag in range(2, max_lag + 1):
        diff = transformed[lag:] - transformed[:-lag]
        if diff.size < 2:
            continue
        tau = float(np.std(diff, ddof=0))
        if not np.isfinite(tau) or tau <= 1e-12:
            continue
        lag_values.append(float(lag))
        tau_values.append(tau)
    if len(lag_values) < 2:
        return 0.0
    slope, _intercept = np.polyfit(np.log(lag_values), np.log(tau_values), 1)
    if not np.isfinite(slope):
        return 0.0
    return max(0.0, min(1.0, float(slope)))


def duration_years_from_times(times: Sequence[object]) -> float:
    if len(times) < 2:
        return 0.0
    started_at = times[0]
    ended_at = times[-1]
    if not isinstance(started_at, datetime) or not isinstance(ended_at, datetime):
        return 0.0
    duration_seconds = float((ended_at - started_at).total_seconds())
    if duration_seconds <= 0.0:
        return 0.0
    return duration_seconds / SECONDS_PER_YEAR


def compute_cagr(initial_capital: float, ending_equity: float, duration_years: float) -> float:
    initial = max(float(initial_capital), 0.0)
    ending = float(ending_equity)
    if duration_years <= 1e-12 or initial <= 1e-12 or ending <= 0.0:
        return 0.0
    growth_ratio = ending / initial
    if growth_ratio <= 0.0:
        return 0.0
    annualized_log_growth = log(growth_ratio) / duration_years
    if annualized_log_growth >= LOG_MAX_FINITE_METRIC:
        return MAX_FINITE_METRIC_ABS
    return clamp_metric_value(expm1(annualized_log_growth))


def compute_equity_curve_metrics(
    *,
    equity_total: np.ndarray,
    initial_capital: float,
    trades_count: int,
    wins: int,
    gross_profit: float,
    spread_cost: float,
    slippage_cost: float,
    commission_cost: float,
    net_profit: float,
    duration_years: float,
) -> dict[str, float | int]:
    if equity_total.size == 0:
        return {
            "net_profit": 0.0,
            "ending_equity": float(initial_capital),
            "max_drawdown": 0.0,
            "pnl_to_maxdd": 0.0,
            "omega_ratio": 0.0,
            "k_ratio": 0.0,
            "ulcer_index": 0.0,
            "ulcer_performance": 0.0,
            "cagr": 0.0,
            "cagr_to_ulcer": 0.0,
            "r_squared": 0.0,
            "hurst_exponent": 0.0,
            "calmar": 0.0,
            "gross_profit": 0.0,
            "spread_cost": 0.0,
            "slippage_cost": 0.0,
            "commission_cost": 0.0,
            "total_cost": 0.0,
            "trades": int(trades_count),
            "win_rate": (wins / trades_count) if trades_count else 0.0,
        }

    running_peak = np.maximum.accumulate(equity_total)
    drawdown_abs = running_peak - equity_total
    max_drawdown = float(drawdown_abs.max()) if drawdown_abs.size else 0.0
    pnl_to_maxdd = safe_ratio(net_profit, max_drawdown)

    pnl_steps = np.diff(equity_total, prepend=equity_total[:1])
    gains = float(np.clip(pnl_steps, 0.0, None).sum())
    losses = float(np.clip(-pnl_steps, 0.0, None).sum())
    omega_ratio = gains if losses <= 1e-12 else gains / losses

    dd_pct = np.divide(drawdown_abs, running_peak, out=np.zeros_like(drawdown_abs), where=running_peak > 1e-12)
    max_drawdown_pct = float(dd_pct.max()) if dd_pct.size else 0.0
    ulcer_index = float(np.sqrt(np.mean(dd_pct**2))) if dd_pct.size else 0.0
    ending_equity = float(equity_total[-1])
    cagr = compute_cagr(float(initial_capital), ending_equity, duration_years)
    cagr_to_ulcer = safe_ratio(cagr, ulcer_index)
    r_squared = compute_r_squared(equity_total)
    hurst_exponent = compute_hurst_exponent(equity_total)
    calmar = safe_ratio(cagr, max_drawdown_pct)
    total_cost = spread_cost + slippage_cost + commission_cost

    return {
        "net_profit": float(net_profit),
        "ending_equity": ending_equity,
        "max_drawdown": max_drawdown,
        "pnl_to_maxdd": pnl_to_maxdd,
        "omega_ratio": float(omega_ratio),
        "k_ratio": compute_k_ratio(equity_total),
        "ulcer_index": ulcer_index,
        "ulcer_performance": safe_ratio(net_profit, ulcer_index),
        "cagr": cagr,
        "cagr_to_ulcer": cagr_to_ulcer,
        "r_squared": r_squared,
        "hurst_exponent": hurst_exponent,
        "calmar": calmar,
        "gross_profit": float(gross_profit),
        "spread_cost": float(spread_cost),
        "slippage_cost": float(slippage_cost),
        "commission_cost": float(commission_cost),
        "total_cost": float(total_cost),
        "trades": int(trades_count),
        "win_rate": (wins / trades_count) if trades_count else 0.0,
    }
