from __future__ import annotations

from datetime import datetime
from math import log

import numpy as np
import polars as pl

from domain.backtest.distance import load_pair_frame
from domain.contracts import PairSelection, Timeframe, UnitRootGate
from domain.scan.johansen_models import (
    JohansenPairScanResult,
    JohansenScanParameters,
    JohansenUniverseScanRow,
)
from domain.scan.unit_root import PairUnitRootScreenResult, UnitRootScreenResult, screen_pair_for_cointegration


def validate_det_order(det_order: int) -> None:
    if int(det_order) not in (-1, 0, 1):
        raise ValueError(
            f"Unsupported Johansen det_order={det_order}. statsmodels coint_johansen supports only -1, 0, 1."
        )


def critical_column(significance_level: float) -> tuple[int, str]:
    options = {0.10: (0, "90%"), 0.05: (1, "95%"), 0.01: (2, "99%")}
    level = min(options, key=lambda current: abs(current - significance_level))
    return options[level]


def prepare_series(
    leg_1_values: np.ndarray | list[float],
    leg_2_values: np.ndarray | list[float],
    *,
    use_log_prices: bool,
) -> tuple[np.ndarray, np.ndarray, str]:
    series_1 = np.asarray(leg_1_values, dtype=np.float64)
    series_2 = np.asarray(leg_2_values, dtype=np.float64)
    mask = np.isfinite(series_1) & np.isfinite(series_2)
    series_1 = series_1[mask]
    series_2 = series_2[mask]
    transformed_mode = "levels"
    if use_log_prices and series_1.size and series_2.size and np.all(series_1 > 0.0) and np.all(series_2 > 0.0):
        series_1 = np.log(series_1)
        series_2 = np.log(series_2)
        transformed_mode = "log_prices"
    return series_1, series_2, transformed_mode


def compute_rank(trace_statistics: np.ndarray, critical_values: np.ndarray, column_index: int) -> int:
    rank = 0
    for statistic, thresholds in zip(trace_statistics, critical_values, strict=False):
        if float(statistic) > float(thresholds[column_index]):
            rank += 1
        else:
            break
    return rank


def compute_half_life(spread: np.ndarray) -> float | None:
    if spread.size < 3:
        return None
    lagged = spread[:-1]
    delta = np.diff(spread)
    design = np.column_stack([np.ones(lagged.size), lagged])
    try:
        coefficients, *_ = np.linalg.lstsq(design, delta, rcond=None)
    except np.linalg.LinAlgError:
        return None
    beta = float(coefficients[1])
    if beta >= 0.0:
        return None
    return log(2.0) / -beta


def compute_last_zscore(spread: np.ndarray, lookback: int) -> float | None:
    if spread.size == 0:
        return None
    window = spread[-min(max(lookback, 2), spread.size):]
    std = float(window.std())
    if std <= 0.0:
        return None
    return float((window[-1] - window.mean()) / std)


def build_scan_row(result: JohansenPairScanResult) -> JohansenUniverseScanRow:
    trace_stat_0 = result.trace_statistics[0] if result.trace_statistics else None
    max_eigen_stat_0 = result.max_eigen_statistics[0] if result.max_eigen_statistics else None
    return JohansenUniverseScanRow(
        symbol_1=result.pair.symbol_1,
        symbol_2=result.pair.symbol_2,
        sample_size=result.sample_size,
        eligible_for_cointegration=result.eligible_for_cointegration,
        unit_root_leg_1=result.unit_root.leg_1.inferred_order,
        unit_root_leg_2=result.unit_root.leg_2.inferred_order,
        rank=result.rank,
        threshold_passed=result.threshold_passed,
        trace_stat_0=float(trace_stat_0) if trace_stat_0 is not None else None,
        max_eigen_stat_0=float(max_eigen_stat_0) if max_eigen_stat_0 is not None else None,
        hedge_ratio=result.hedge_ratio,
        half_life_bars=result.half_life_bars,
        last_zscore=result.last_zscore,
        failure_reason=result.failure_reason,
    )


def sort_rows(rows: list[JohansenUniverseScanRow]) -> list[JohansenUniverseScanRow]:
    return sorted(
        rows,
        key=lambda row: (
            row.threshold_passed,
            row.rank,
            row.sample_size,
            abs(row.last_zscore or 0.0),
        ),
        reverse=True,
    )


def scan_pair_payload(
    payload: tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters]
    | tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters, UnitRootScreenResult, UnitRootScreenResult],
) -> JohansenUniverseScanRow:
    if len(payload) == 8:
        symbol_1, symbol_2, leg_1_values, leg_2_values, unit_root_gate, params, precomputed_leg_1, precomputed_leg_2 = payload
    else:
        symbol_1, symbol_2, leg_1_values, leg_2_values, unit_root_gate, params = payload
        precomputed_leg_1 = None
        precomputed_leg_2 = None
    result = scan_pair_johansen_arrays(
        pair=PairSelection(symbol_1=symbol_1, symbol_2=symbol_2),
        leg_1_values=leg_1_values,
        leg_2_values=leg_2_values,
        unit_root_gate=unit_root_gate,
        params=params,
        precomputed_leg_1=precomputed_leg_1,
        precomputed_leg_2=precomputed_leg_2,
    )
    return build_scan_row(result)


def scan_pair_johansen_arrays(
    pair: PairSelection,
    leg_1_values: np.ndarray | list[float],
    leg_2_values: np.ndarray | list[float],
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
    precomputed_leg_1: UnitRootScreenResult | None = None,
    precomputed_leg_2: UnitRootScreenResult | None = None,
) -> JohansenPairScanResult:
    validate_det_order(params.det_order)
    series_1, series_2, transformed_mode = prepare_series(
        leg_1_values,
        leg_2_values,
        use_log_prices=params.use_log_prices,
    )
    sample_size = int(min(series_1.size, series_2.size))
    if precomputed_leg_1 is not None and precomputed_leg_2 is not None:
        unit_root = PairUnitRootScreenResult(
            leg_1=precomputed_leg_1,
            leg_2=precomputed_leg_2,
            eligible_for_cointegration=bool(precomputed_leg_1.passes_gate and precomputed_leg_2.passes_gate),
        )
    else:
        unit_root = screen_pair_for_cointegration(series_1, series_2, unit_root_gate)

    if sample_size < 32:
        return JohansenPairScanResult(
            pair=pair,
            sample_size=sample_size,
            transformed_mode=transformed_mode,
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason="insufficient_observations",
        )

    if not unit_root.eligible_for_cointegration:
        return JohansenPairScanResult(
            pair=pair,
            sample_size=sample_size,
            transformed_mode=transformed_mode,
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason="unit_root_gate_failed",
        )

    from statsmodels.tsa.vector_ar.vecm import coint_johansen

    try:
        johansen_result = coint_johansen(
            np.column_stack([series_1, series_2]),
            params.det_order,
            params.k_ar_diff,
        )
    except (ValueError, ZeroDivisionError, np.linalg.LinAlgError):
        return JohansenPairScanResult(
            pair=pair,
            sample_size=sample_size,
            transformed_mode=transformed_mode,
            unit_root=unit_root,
            eligible_for_cointegration=True,
            failure_reason="johansen_failed",
        )

    column_index, significance_column = critical_column(params.significance_level)
    trace_statistics = np.asarray(johansen_result.lr1, dtype=np.float64)
    max_eigen_statistics = np.asarray(johansen_result.lr2, dtype=np.float64)
    trace_critical_values = np.asarray(johansen_result.cvt, dtype=np.float64)
    max_eigen_critical_values = np.asarray(johansen_result.cvm, dtype=np.float64)
    rank = compute_rank(trace_statistics, trace_critical_values, column_index)

    eigenvector = np.asarray(johansen_result.evec[:, 0], dtype=np.float64)
    spread = np.asarray([], dtype=np.float64)
    hedge_ratio = None
    eigen_tuple = None
    if np.all(np.isfinite(eigenvector)):
        scale = eigenvector[0] if abs(eigenvector[0]) > 1e-12 else 1.0
        normalized = eigenvector / scale
        hedge_ratio = float(-normalized[1]) if normalized.size > 1 else None
        if hedge_ratio is not None:
            spread = series_1 - hedge_ratio * series_2
        eigen_tuple = tuple(float(value) for value in normalized.tolist())

    return JohansenPairScanResult(
        pair=pair,
        sample_size=sample_size,
        transformed_mode=transformed_mode,
        unit_root=unit_root,
        eligible_for_cointegration=True,
        rank=rank,
        trace_statistics=tuple(float(value) for value in trace_statistics.tolist()),
        max_eigen_statistics=tuple(float(value) for value in max_eigen_statistics.tolist()),
        trace_critical_values=tuple(tuple(float(item) for item in row.tolist()) for row in trace_critical_values),
        max_eigen_critical_values=tuple(tuple(float(item) for item in row.tolist()) for row in max_eigen_critical_values),
        significance_column=significance_column,
        threshold_passed=rank >= 1,
        hedge_ratio=hedge_ratio,
        eigenvector=eigen_tuple,
        half_life_bars=compute_half_life(spread) if spread.size else None,
        last_zscore=compute_last_zscore(spread, params.zscore_lookback_bars) if spread.size else None,
    )


def scan_pair_frame_johansen(
    frame: pl.DataFrame,
    pair: PairSelection,
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
) -> JohansenPairScanResult:
    if frame.is_empty():
        unit_root = screen_pair_for_cointegration([], [], unit_root_gate)
        return JohansenPairScanResult(
            pair=pair,
            sample_size=0,
            transformed_mode="levels",
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason="empty_frame",
        )
    return scan_pair_johansen_arrays(
        pair=pair,
        leg_1_values=frame.get_column("close_1").to_numpy(),
        leg_2_values=frame.get_column("close_2").to_numpy(),
        unit_root_gate=unit_root_gate,
        params=params,
    )


def scan_pair_johansen(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
) -> JohansenPairScanResult:
    frame = load_pair_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
    )
    return scan_pair_frame_johansen(
        frame=frame,
        pair=pair,
        unit_root_gate=unit_root_gate,
        params=params,
    )
