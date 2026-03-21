from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from math import log
from typing import Callable, Mapping, Sequence

import numpy as np
import polars as pl

from domain.backtest.distance import load_pair_frame
from domain.contracts import NormalizedGroup, PairSelection, ScanUniverseMode, Timeframe, UnitRootGate
from domain.data.io import load_instrument_catalog_frame, load_quotes_range
from domain.scan.unit_root import PairUnitRootScreenResult, UnitRootScreenResult, screen_pair_for_cointegration, screen_series_for_cointegration
from workers.executor import build_process_pool


@dataclass(slots=True)
class JohansenScanParameters:
    det_order: int = 0
    k_ar_diff: int = 1
    significance_level: float = 0.05
    use_log_prices: bool = True
    zscore_lookback_bars: int = 96


@dataclass(slots=True)
class JohansenPairScanResult:
    pair: PairSelection
    sample_size: int
    transformed_mode: str
    unit_root: PairUnitRootScreenResult
    eligible_for_cointegration: bool
    rank: int = 0
    trace_statistics: tuple[float, ...] = ()
    max_eigen_statistics: tuple[float, ...] = ()
    trace_critical_values: tuple[tuple[float, ...], ...] = ()
    max_eigen_critical_values: tuple[tuple[float, ...], ...] = ()
    significance_column: str = '95%'
    threshold_passed: bool = False
    hedge_ratio: float | None = None
    eigenvector: tuple[float, ...] | None = None
    half_life_bars: float | None = None
    last_zscore: float | None = None
    failure_reason: str | None = None


@dataclass(slots=True)
class JohansenUniverseScanRow:
    symbol_1: str
    symbol_2: str
    sample_size: int
    eligible_for_cointegration: bool
    unit_root_leg_1: str
    unit_root_leg_2: str
    rank: int
    threshold_passed: bool
    trace_stat_0: float | None
    max_eigen_stat_0: float | None
    hedge_ratio: float | None
    half_life_bars: float | None
    last_zscore: float | None
    failure_reason: str | None


@dataclass(slots=True)
class JohansenUniverseScanSummary:
    total_symbols_requested: int
    loaded_symbols: int
    prefiltered_i1_symbols: int
    total_pairs_evaluated: int
    threshold_passed_pairs: int
    screened_out_pairs: int


@dataclass(slots=True)
class JohansenUniverseScanResult:
    summary: JohansenUniverseScanSummary
    rows: list[JohansenUniverseScanRow]
    universe_symbols: list[str]


ProgressCallback = Callable[[int, int, str], None]


def _critical_column(significance_level: float) -> tuple[int, str]:
    options = {0.10: (0, '90%'), 0.05: (1, '95%'), 0.01: (2, '99%')}
    level = min(options, key=lambda current: abs(current - significance_level))
    return options[level]


def _emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


def _resolve_worker_count(parallel_workers: int | None, task_count: int) -> int:
    if task_count <= 1:
        return 1
    if parallel_workers is None:
        return 1
    return max(1, min(int(parallel_workers), int(task_count)))


def _prepare_series(
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
    transformed_mode = 'levels'
    if use_log_prices and series_1.size and series_2.size and np.all(series_1 > 0.0) and np.all(series_2 > 0.0):
        series_1 = np.log(series_1)
        series_2 = np.log(series_2)
        transformed_mode = 'log_prices'
    return series_1, series_2, transformed_mode


def _compute_rank(trace_statistics: np.ndarray, critical_values: np.ndarray, column_index: int) -> int:
    rank = 0
    for statistic, thresholds in zip(trace_statistics, critical_values, strict=False):
        if float(statistic) > float(thresholds[column_index]):
            rank += 1
        else:
            break
    return rank


def _compute_half_life(spread: np.ndarray) -> float | None:
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


def _compute_last_zscore(spread: np.ndarray, lookback: int) -> float | None:
    if spread.size == 0:
        return None
    window = spread[-min(max(lookback, 2), spread.size):]
    std = float(window.std())
    if std <= 0.0:
        return None
    return float((window[-1] - window.mean()) / std)


def _normalize_group_value(group: NormalizedGroup | str | None) -> str | None:
    if group is None:
        return None
    if isinstance(group, NormalizedGroup):
        return group.value
    return group


def _load_symbol_close_frame(
    broker: str,
    symbol: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
) -> pl.DataFrame:
    frame = load_quotes_range(broker=broker, symbol=symbol, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return pl.DataFrame(schema={'time': pl.Datetime(time_zone='UTC'), 'close': pl.Float64})
    return frame.select('time', 'close').sort('time')


def resolve_scan_symbols(
    broker: str,
    universe_mode: ScanUniverseMode,
    normalized_group: NormalizedGroup | str | None = None,
    symbols: Sequence[str] | None = None,
) -> list[str]:
    explicit_symbols = list(dict.fromkeys(symbols or []))
    if universe_mode is ScanUniverseMode.MANUAL:
        return explicit_symbols

    catalog = load_instrument_catalog_frame(broker)
    if universe_mode is ScanUniverseMode.GROUP:
        group_value = _normalize_group_value(normalized_group)
        if group_value:
            catalog = catalog.filter(pl.col('normalized_group') == group_value)

    if catalog.is_empty():
        return []
    return catalog.get_column('symbol').sort().to_list()


def _build_scan_row(result: JohansenPairScanResult) -> JohansenUniverseScanRow:
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


def _sort_rows(rows: list[JohansenUniverseScanRow]) -> list[JohansenUniverseScanRow]:
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


def _screen_symbol_payload(payload: tuple[str, np.ndarray, UnitRootGate]) -> tuple[str, UnitRootScreenResult]:
    symbol, values, unit_root_gate = payload
    result = screen_series_for_cointegration(np.asarray(values, dtype=np.float64), unit_root_gate)
    return symbol, result


def _scan_pair_payload(
    payload: tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters],
) -> JohansenUniverseScanRow:
    symbol_1, symbol_2, leg_1_values, leg_2_values, unit_root_gate, params = payload
    result = scan_pair_johansen_arrays(
        pair=PairSelection(symbol_1=symbol_1, symbol_2=symbol_2),
        leg_1_values=leg_1_values,
        leg_2_values=leg_2_values,
        unit_root_gate=unit_root_gate,
        params=params,
    )
    return _build_scan_row(result)


def scan_pair_johansen_arrays(
    pair: PairSelection,
    leg_1_values: np.ndarray | list[float],
    leg_2_values: np.ndarray | list[float],
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
) -> JohansenPairScanResult:
    series_1, series_2, transformed_mode = _prepare_series(
        leg_1_values,
        leg_2_values,
        use_log_prices=params.use_log_prices,
    )
    sample_size = int(min(series_1.size, series_2.size))
    unit_root = screen_pair_for_cointegration(series_1, series_2, unit_root_gate)

    if sample_size < 32:
        return JohansenPairScanResult(
            pair=pair,
            sample_size=sample_size,
            transformed_mode=transformed_mode,
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason='insufficient_observations',
        )

    if not unit_root.eligible_for_cointegration:
        return JohansenPairScanResult(
            pair=pair,
            sample_size=sample_size,
            transformed_mode=transformed_mode,
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason='unit_root_gate_failed',
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
            failure_reason='johansen_failed',
        )

    critical_column, significance_column = _critical_column(params.significance_level)
    trace_statistics = np.asarray(johansen_result.lr1, dtype=np.float64)
    max_eigen_statistics = np.asarray(johansen_result.lr2, dtype=np.float64)
    trace_critical_values = np.asarray(johansen_result.cvt, dtype=np.float64)
    max_eigen_critical_values = np.asarray(johansen_result.cvm, dtype=np.float64)
    rank = _compute_rank(trace_statistics, trace_critical_values, critical_column)

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
        half_life_bars=_compute_half_life(spread) if spread.size else None,
        last_zscore=_compute_last_zscore(spread, params.zscore_lookback_bars) if spread.size else None,
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
            transformed_mode='levels',
            unit_root=unit_root,
            eligible_for_cointegration=False,
            failure_reason='empty_frame',
        )
    return scan_pair_johansen_arrays(
        pair=pair,
        leg_1_values=frame.get_column('close_1').to_numpy(),
        leg_2_values=frame.get_column('close_2').to_numpy(),
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


def scan_symbol_frames_johansen(
    symbol_frames: Mapping[str, pl.DataFrame],
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
    progress_callback: ProgressCallback | None = None,
    parallel_workers: int | None = None,
) -> JohansenUniverseScanResult:
    loaded_frames: dict[str, pl.DataFrame] = {
        symbol: frame.select('time', 'close').sort('time')
        for symbol, frame in symbol_frames.items()
        if not frame.is_empty()
    }
    symbol_results: dict[str, UnitRootScreenResult] = {}
    total_loaded = len(loaded_frames)
    _emit_progress(progress_callback, 0, total_loaded, 'Unit root screening')
    symbol_worker_count = _resolve_worker_count(parallel_workers, total_loaded)
    symbol_payloads = [
        (symbol, frame.get_column('close').to_numpy(), unit_root_gate)
        for symbol, frame in loaded_frames.items()
    ]
    if symbol_worker_count <= 1:
        for index, payload in enumerate(symbol_payloads, start=1):
            symbol, result = _screen_symbol_payload(payload)
            symbol_results[symbol] = result
            _emit_progress(progress_callback, index, total_loaded, 'Unit root screening')
    else:
        with build_process_pool(max_workers=symbol_worker_count) as executor:
            for index, (symbol, result) in enumerate(executor.map(_screen_symbol_payload, symbol_payloads), start=1):
                symbol_results[symbol] = result
                _emit_progress(progress_callback, index, total_loaded, f'Unit root screening ({symbol_worker_count} workers)')

    i1_symbols = sorted(symbol for symbol, result in symbol_results.items() if result.passes_gate)
    rows: list[JohansenUniverseScanRow] = []
    pair_candidates = list(combinations(i1_symbols, 2))
    total_pairs = len(pair_candidates)
    _emit_progress(progress_callback, 0, total_pairs, 'Johansen pairs')

    def iter_pair_payloads() -> Sequence[tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters]]:
        for symbol_1, symbol_2 in pair_candidates:
            left = loaded_frames[symbol_1].rename({'close': 'close_1'})
            right = loaded_frames[symbol_2].rename({'close': 'close_2'})
            aligned = left.join(right, on='time', how='inner').sort('time')
            if aligned.is_empty():
                yield (symbol_1, symbol_2, np.asarray([], dtype=np.float64), np.asarray([], dtype=np.float64), unit_root_gate, params)
                continue
            yield (
                symbol_1,
                symbol_2,
                aligned.get_column('close_1').to_numpy(),
                aligned.get_column('close_2').to_numpy(),
                unit_root_gate,
                params,
            )

    pair_worker_count = _resolve_worker_count(parallel_workers, total_pairs)
    if pair_worker_count <= 1:
        for pair_index, payload in enumerate(iter_pair_payloads(), start=1):
            rows.append(_scan_pair_payload(payload))
            _emit_progress(progress_callback, pair_index, total_pairs, 'Johansen pairs')
    else:
        with build_process_pool(max_workers=pair_worker_count) as executor:
            for pair_index, row in enumerate(executor.map(_scan_pair_payload, iter_pair_payloads()), start=1):
                rows.append(row)
                _emit_progress(progress_callback, pair_index, total_pairs, f'Johansen pairs ({pair_worker_count} workers)')

    rows = _sort_rows(rows)
    summary = JohansenUniverseScanSummary(
        total_symbols_requested=len(symbol_frames),
        loaded_symbols=len(loaded_frames),
        prefiltered_i1_symbols=len(i1_symbols),
        total_pairs_evaluated=len(rows),
        threshold_passed_pairs=sum(1 for row in rows if row.threshold_passed),
        screened_out_pairs=sum(1 for row in rows if not row.eligible_for_cointegration or row.failure_reason is not None),
    )
    return JohansenUniverseScanResult(summary=summary, rows=rows, universe_symbols=sorted(symbol_frames))


def scan_universe_johansen(
    broker: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    universe_mode: ScanUniverseMode,
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
    normalized_group: NormalizedGroup | str | None = None,
    symbols: Sequence[str] | None = None,
    progress_callback: ProgressCallback | None = None,
    parallel_workers: int | None = None,
) -> JohansenUniverseScanResult:
    universe_symbols = resolve_scan_symbols(
        broker=broker,
        universe_mode=universe_mode,
        normalized_group=normalized_group,
        symbols=symbols,
    )
    total_symbols = len(universe_symbols)
    _emit_progress(progress_callback, 0, total_symbols, 'Loading quotes')
    symbol_frames: dict[str, pl.DataFrame] = {}
    for index, symbol in enumerate(universe_symbols, start=1):
        symbol_frames[symbol] = _load_symbol_close_frame(
            broker=broker,
            symbol=symbol,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
        )
        _emit_progress(progress_callback, index, total_symbols, 'Loading quotes')
    return scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=unit_root_gate,
        params=params,
        progress_callback=progress_callback,
        parallel_workers=parallel_workers,
    )
