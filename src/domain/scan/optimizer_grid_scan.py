from __future__ import annotations

from concurrent.futures import Executor
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime
from itertools import combinations
from typing import Any, Callable, Mapping, Sequence

import polars as pl

from domain.contracts import NormalizedGroup, PairSelection, ScanUniverseMode, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec, load_quotes_range
from domain.optimizer import DistanceOptimizationResult, DistanceOptimizationRow, optimize_distance_grid_frame
from domain.optimizer.distance_models import CancellationCheck
from domain.scan.johansen_universe import normalize_group_value, resolve_scan_symbols
from workers.executor import shared_process_pool


ProgressCallback = Callable[[int, int, str], None]
PartialResultCallback = Callable[["OptimizerGridScanResult"], None]


@dataclass(slots=True)
class OptimizerGridScanRow:
    symbol_1: str
    symbol_2: str
    pair_rank: int
    optimization_row: DistanceOptimizationRow
    universe_scope: str = ScanUniverseMode.ALL.value
    timeframe: str = Timeframe.M15.value
    initial_capital: float = 10_000.0
    leverage: float = 100.0
    margin_budget_per_leg: float = 500.0
    slippage_points: float = 1.0
    fee_mode: str = ""


@dataclass(slots=True)
class OptimizerGridScanSummary:
    total_symbols_requested: int
    loaded_symbols: int
    total_pair_candidates: int
    total_pairs_evaluated: int
    pairs_with_data: int
    pairs_with_hits: int
    total_rows: int
    total_trials_evaluated: int
    min_r_squared: float
    top_n_per_pair: int


@dataclass(slots=True)
class OptimizerGridScanResult:
    summary: OptimizerGridScanSummary
    rows: list[OptimizerGridScanRow]
    universe_symbols: list[str]
    universe_scope: str = ScanUniverseMode.ALL.value
    processed_pair_keys: list[str] | None = None
    cancelled: bool = False
    failure_reason: str | None = None


def emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


def is_cancelled(cancel_check: CancellationCheck | None) -> bool:
    return bool(cancel_check and cancel_check())


def pair_key(symbol_1: str, symbol_2: str) -> str:
    return f"{symbol_1}::{symbol_2}"


def load_symbol_quote_frame(
    broker: str,
    symbol: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
) -> pl.DataFrame:
    frame = load_quotes_range(
        broker=broker,
        symbol=symbol,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
    )
    if frame.is_empty():
        return pl.DataFrame()
    available_columns = [
        column
        for column in ("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")
        if column in frame.columns
    ]
    return frame.select(available_columns).sort("time")


def _suffix_quotes(frame: pl.DataFrame, suffix: str) -> pl.DataFrame:
    return frame.rename({column: f"{column}{suffix}" for column in frame.columns if column != "time"})


def _align_pair_frame(frame_1: pl.DataFrame, frame_2: pl.DataFrame) -> pl.DataFrame:
    if frame_1.is_empty() or frame_2.is_empty():
        return pl.DataFrame()
    return _suffix_quotes(frame_1, "_1").join(_suffix_quotes(frame_2, "_2"), on="time", how="inner").sort("time")


def _top_rows_for_pair(
    result: DistanceOptimizationResult,
    *,
    min_r_squared: float,
    top_n_per_pair: int,
) -> list[DistanceOptimizationRow]:
    eligible = [
        row
        for row in result.rows
        if float(row.net_profit) > 0.0 and int(row.trades) > 0
    ]
    eligible.sort(
        key=lambda row: (
            float(row.net_profit),
            float(row.r_squared) >= float(min_r_squared),
            float(row.r_squared),
            float(row.beauty_score),
            -int(row.trial_id),
        ),
        reverse=True,
    )
    return eligible[:top_n_per_pair]


def _sort_scan_rows(rows: list[OptimizerGridScanRow]) -> None:
    rows.sort(
        key=lambda item: (
            float(item.optimization_row.net_profit),
            float(item.optimization_row.r_squared),
            float(item.optimization_row.beauty_score),
            str(item.universe_scope or ScanUniverseMode.ALL.value),
            item.symbol_1,
            item.symbol_2,
            -int(item.pair_rank),
        ),
        reverse=True,
    )


def _scan_result(
    *,
    total_symbols_requested: int,
    loaded_symbols: int,
    total_pair_candidates: int,
    total_pairs_evaluated: int,
    pairs_with_data: int,
    pairs_with_hits: int,
    rows: list[OptimizerGridScanRow],
    total_trials_evaluated: int,
    min_r_squared: float,
    top_n_per_pair: int,
    universe_symbols: Sequence[str],
    universe_scope: str,
    processed_pair_keys: Sequence[str],
    cancelled: bool = False,
    failure_reason: str | None = None,
) -> OptimizerGridScanResult:
    return OptimizerGridScanResult(
        summary=OptimizerGridScanSummary(
            total_symbols_requested=int(total_symbols_requested),
            loaded_symbols=int(loaded_symbols),
            total_pair_candidates=int(total_pair_candidates),
            total_pairs_evaluated=int(total_pairs_evaluated),
            pairs_with_data=int(pairs_with_data),
            pairs_with_hits=int(pairs_with_hits),
            total_rows=len(rows),
            total_trials_evaluated=int(total_trials_evaluated),
            min_r_squared=float(min_r_squared),
            top_n_per_pair=int(top_n_per_pair),
        ),
        rows=rows,
        universe_symbols=sorted(str(symbol) for symbol in universe_symbols),
        universe_scope=str(universe_scope or ScanUniverseMode.ALL.value),
        processed_pair_keys=sorted(str(item) for item in processed_pair_keys if str(item)),
        cancelled=bool(cancelled),
        failure_reason=failure_reason,
    )


def combine_optimizer_grid_scan_results(results: Sequence[OptimizerGridScanResult]) -> OptimizerGridScanResult:
    if not results:
        return _scan_result(
            total_symbols_requested=0,
            loaded_symbols=0,
            total_pair_candidates=0,
            total_pairs_evaluated=0,
            pairs_with_data=0,
            pairs_with_hits=0,
            rows=[],
            total_trials_evaluated=0,
            min_r_squared=0.9,
            top_n_per_pair=10,
            universe_symbols=[],
            universe_scope=ScanUniverseMode.ALL.value,
            processed_pair_keys=[],
            cancelled=False,
            failure_reason="no_rows_passed_filters",
        )

    rows: list[OptimizerGridScanRow] = []
    universe_symbols: set[str] = set()
    processed_pair_keys: list[str] = []
    total_symbols_requested = 0
    loaded_symbols = 0
    total_pair_candidates = 0
    total_pairs_evaluated = 0
    pairs_with_data = 0
    pairs_with_hits = 0
    total_trials_evaluated = 0
    cancelled = False
    failure_reasons: list[str] = []
    min_r_squared = float(results[0].summary.min_r_squared)
    top_n_per_pair = int(results[0].summary.top_n_per_pair)

    for result in results:
        rows.extend(result.rows)
        universe_symbols.update(str(symbol) for symbol in result.universe_symbols)
        processed_pair_keys.extend(
            f"{result.universe_scope}:{item}"
            for item in (result.processed_pair_keys or [])
            if str(item)
        )
        total_symbols_requested += int(result.summary.total_symbols_requested)
        loaded_symbols += int(result.summary.loaded_symbols)
        total_pair_candidates += int(result.summary.total_pair_candidates)
        total_pairs_evaluated += int(result.summary.total_pairs_evaluated)
        pairs_with_data += int(result.summary.pairs_with_data)
        pairs_with_hits += int(result.summary.pairs_with_hits)
        total_trials_evaluated += int(result.summary.total_trials_evaluated)
        cancelled = cancelled or bool(result.cancelled)
        if result.failure_reason:
            failure_reasons.append(str(result.failure_reason))

    _sort_scan_rows(rows)
    failure_reason = None if rows else next((reason for reason in failure_reasons if reason), None)
    if cancelled and failure_reason is None:
        failure_reason = "cancelled"
    return _scan_result(
        total_symbols_requested=total_symbols_requested,
        loaded_symbols=loaded_symbols,
        total_pair_candidates=total_pair_candidates,
        total_pairs_evaluated=total_pairs_evaluated,
        pairs_with_data=pairs_with_data,
        pairs_with_hits=pairs_with_hits,
        rows=rows,
        total_trials_evaluated=total_trials_evaluated,
        min_r_squared=min_r_squared,
        top_n_per_pair=top_n_per_pair,
        universe_symbols=sorted(universe_symbols),
        universe_scope=ScanUniverseMode.ALL.value,
        processed_pair_keys=processed_pair_keys,
        cancelled=cancelled,
        failure_reason=failure_reason,
    )


def filter_optimizer_grid_scan_result(
    result: OptimizerGridScanResult,
    *,
    universe_scope: str | None,
) -> OptimizerGridScanResult:
    normalized_scope = str(universe_scope or "").strip()
    if not normalized_scope or normalized_scope == ScanUniverseMode.ALL.value:
        rows = list(result.rows)
        _sort_scan_rows(rows)
        return _scan_result(
            total_symbols_requested=result.summary.total_symbols_requested,
            loaded_symbols=result.summary.loaded_symbols,
            total_pair_candidates=result.summary.total_pair_candidates,
            total_pairs_evaluated=result.summary.total_pairs_evaluated,
            pairs_with_data=result.summary.pairs_with_data,
            pairs_with_hits=result.summary.pairs_with_hits,
            rows=rows,
            total_trials_evaluated=result.summary.total_trials_evaluated,
            min_r_squared=result.summary.min_r_squared,
            top_n_per_pair=result.summary.top_n_per_pair,
            universe_symbols=result.universe_symbols,
            universe_scope=ScanUniverseMode.ALL.value,
            processed_pair_keys=result.processed_pair_keys or [],
            cancelled=result.cancelled,
            failure_reason=result.failure_reason,
        )

    rows = [row for row in result.rows if str(row.universe_scope) == normalized_scope]
    _sort_scan_rows(rows)
    return _scan_result(
        total_symbols_requested=result.summary.total_symbols_requested,
        loaded_symbols=result.summary.loaded_symbols,
        total_pair_candidates=result.summary.total_pair_candidates,
        total_pairs_evaluated=result.summary.total_pairs_evaluated,
        pairs_with_data=result.summary.pairs_with_data,
        pairs_with_hits=len({pair_key(row.symbol_1, row.symbol_2) for row in rows}),
        rows=rows,
        total_trials_evaluated=result.summary.total_trials_evaluated,
        min_r_squared=result.summary.min_r_squared,
        top_n_per_pair=result.summary.top_n_per_pair,
        universe_symbols=result.universe_symbols,
        universe_scope=normalized_scope,
        processed_pair_keys=result.processed_pair_keys or [],
        cancelled=result.cancelled,
        failure_reason=result.failure_reason if rows else "no_rows_passed_filters",
    )


def scan_symbol_frames_optimizer_grid(
    *,
    symbol_frames: Mapping[str, pl.DataFrame],
    specs_by_symbol: Mapping[str, Mapping[str, Any]] | None,
    defaults: StrategyDefaults,
    search_space: Mapping[str, Any],
    timeframe: Timeframe | None = None,
    fee_mode: str | None = None,
    min_r_squared: float = 0.9,
    top_n_per_pair: int = 10,
    progress_callback: ProgressCallback | None = None,
    partial_result_callback: PartialResultCallback | None = None,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    parallel_executor: Executor | None = None,
    universe_scope: str = ScanUniverseMode.ALL.value,
    resume_result: OptimizerGridScanResult | None = None,
    allowed_pair_keys: Sequence[str] | None = None,
) -> OptimizerGridScanResult:
    normalized_top_n = max(1, int(top_n_per_pair))
    loaded_frames = {
        str(symbol): frame.sort("time")
        for symbol, frame in symbol_frames.items()
        if not frame.is_empty()
    }
    loaded_symbols = sorted(loaded_frames)
    allowed_pair_key_set = {str(item) for item in (allowed_pair_keys or []) if str(item)}
    pair_candidates = [
        (symbol_1, symbol_2)
        for symbol_1, symbol_2 in combinations(loaded_symbols, 2)
        if not allowed_pair_key_set or pair_key(symbol_1, symbol_2) in allowed_pair_key_set
    ]
    total_pairs = len(pair_candidates)
    resumed_rows = [
        row
        for row in list((resume_result.rows if resume_result is not None else []) or [])
        if not allowed_pair_key_set or pair_key(str(row.symbol_1), str(row.symbol_2)) in allowed_pair_key_set
    ]
    aggregated_rows: list[OptimizerGridScanRow] = [
        OptimizerGridScanRow(
            symbol_1=str(row.symbol_1),
            symbol_2=str(row.symbol_2),
            pair_rank=int(row.pair_rank),
            optimization_row=row.optimization_row,
            universe_scope=str(row.universe_scope or universe_scope or ScanUniverseMode.ALL.value),
            timeframe=str(row.timeframe),
            initial_capital=float(row.initial_capital),
            leverage=float(row.leverage),
            margin_budget_per_leg=float(row.margin_budget_per_leg),
            slippage_points=float(row.slippage_points),
            fee_mode=str(row.fee_mode or ""),
        )
        for row in resumed_rows
    ]
    total_trials_evaluated = int((resume_result.summary.total_trials_evaluated if resume_result is not None else 0) or 0)
    pairs_with_data = int((resume_result.summary.pairs_with_data if resume_result is not None else 0) or 0)
    pairs_with_hits = int((resume_result.summary.pairs_with_hits if resume_result is not None else 0) or 0)
    processed_pair_keys = {
        str(item)
        for item in ((resume_result.processed_pair_keys if resume_result is not None else []) or [])
        if str(item)
    }
    processed_pairs = min(len(processed_pair_keys), total_pairs)
    cancelled = False
    emit_progress(progress_callback, processed_pairs, total_pairs, "Scanner pairs")

    for symbol_1, symbol_2 in pair_candidates:
        current_pair_key = pair_key(symbol_1, symbol_2)
        if current_pair_key in processed_pair_keys:
            continue
        if is_cancelled(cancel_check):
            cancelled = True
            break

        frame = _align_pair_frame(loaded_frames[symbol_1], loaded_frames[symbol_2])
        if frame.is_empty():
            processed_pair_keys.add(current_pair_key)
            processed_pairs = len(processed_pair_keys)
            emit_progress(progress_callback, processed_pairs, total_pairs, f"Scanner pairs ({symbol_1}/{symbol_2}: no aligned data)")
            if partial_result_callback is not None:
                partial_result_callback(
                    _scan_result(
                        total_symbols_requested=len(symbol_frames),
                        loaded_symbols=len(loaded_symbols),
                        total_pair_candidates=total_pairs,
                        total_pairs_evaluated=processed_pairs,
                        pairs_with_data=pairs_with_data,
                        pairs_with_hits=pairs_with_hits,
                        rows=list(aggregated_rows),
                        total_trials_evaluated=total_trials_evaluated,
                        min_r_squared=min_r_squared,
                        top_n_per_pair=normalized_top_n,
                        universe_symbols=list(symbol_frames),
                        universe_scope=universe_scope,
                        processed_pair_keys=processed_pair_keys,
                        cancelled=False,
                        failure_reason=None if aggregated_rows else "no_rows_passed_filters",
                    )
                )
            continue

        pairs_with_data += 1
        pair = PairSelection(symbol_1=symbol_1, symbol_2=symbol_2)
        spec_1 = dict((specs_by_symbol or {}).get(symbol_1) or {})
        spec_2 = dict((specs_by_symbol or {}).get(symbol_2) or {})

        def pair_progress(completed: int, total: int, stage: str) -> None:
            emit_progress(
                progress_callback,
                max(0, len(processed_pair_keys)),
                total_pairs,
                f"Scanner {len(processed_pair_keys) + 1}/{total_pairs}: {symbol_1}/{symbol_2} · {stage} {completed}/{total}",
            )

        result = optimize_distance_grid_frame(
            frame=frame,
            pair=pair,
            defaults=defaults,
            search_space=search_space,
            objective_metric="net_profit",
            point_1=float(spec_1.get("point", 0.0) or 0.0),
            point_2=float(spec_2.get("point", 0.0) or 0.0),
            contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
            contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
            spec_1=spec_1,
            spec_2=spec_2,
            cancel_check=cancel_check,
            parallel_workers=parallel_workers,
            progress_callback=pair_progress,
            parallel_executor=parallel_executor,
        )
        total_trials_evaluated += int(result.evaluated_trials)
        top_rows = _top_rows_for_pair(
            result,
            min_r_squared=min_r_squared,
            top_n_per_pair=normalized_top_n,
        )
        if top_rows:
            pairs_with_hits += 1
            for pair_rank, optimization_row in enumerate(top_rows, start=1):
                aggregated_rows.append(
                    OptimizerGridScanRow(
                        symbol_1=symbol_1,
                        symbol_2=symbol_2,
                        pair_rank=pair_rank,
                        optimization_row=optimization_row,
                        universe_scope=str(universe_scope or ScanUniverseMode.ALL.value),
                        timeframe=timeframe.value if isinstance(timeframe, Timeframe) else Timeframe.M15.value,
                        initial_capital=float(defaults.initial_capital),
                        leverage=float(defaults.leverage),
                        margin_budget_per_leg=float(defaults.margin_budget_per_leg),
                        slippage_points=float(defaults.slippage_points),
                        fee_mode=str(fee_mode or ""),
                    )
                )
            _sort_scan_rows(aggregated_rows)

        processed_pair_keys.add(current_pair_key)
        processed_pairs = len(processed_pair_keys)
        emit_progress(progress_callback, processed_pairs, total_pairs, f"Scanner pairs ({symbol_1}/{symbol_2})")
        if partial_result_callback is not None:
            partial_result_callback(
                _scan_result(
                    total_symbols_requested=len(symbol_frames),
                    loaded_symbols=len(loaded_symbols),
                    total_pair_candidates=total_pairs,
                    total_pairs_evaluated=processed_pairs,
                    pairs_with_data=pairs_with_data,
                    pairs_with_hits=pairs_with_hits,
                    rows=list(aggregated_rows),
                    total_trials_evaluated=total_trials_evaluated,
                    min_r_squared=min_r_squared,
                    top_n_per_pair=normalized_top_n,
                    universe_symbols=list(symbol_frames),
                    universe_scope=universe_scope,
                    processed_pair_keys=processed_pair_keys,
                    cancelled=False,
                    failure_reason=None if aggregated_rows else "no_rows_passed_filters",
                )
            )
        if result.cancelled:
            cancelled = True
            break

    _sort_scan_rows(aggregated_rows)
    failure_reason = None
    if cancelled:
        failure_reason = "cancelled"
    elif not loaded_symbols:
        failure_reason = "no_loaded_quotes"
    elif not pair_candidates:
        failure_reason = "no_pair_candidates"
    elif not aggregated_rows:
        failure_reason = "no_rows_passed_filters"
    return _scan_result(
        total_symbols_requested=len(symbol_frames),
        loaded_symbols=len(loaded_symbols),
        total_pair_candidates=total_pairs,
        total_pairs_evaluated=processed_pairs,
        pairs_with_data=pairs_with_data,
        pairs_with_hits=pairs_with_hits,
        rows=aggregated_rows,
        total_trials_evaluated=total_trials_evaluated,
        min_r_squared=min_r_squared,
        top_n_per_pair=normalized_top_n,
        universe_symbols=list(symbol_frames),
        universe_scope=universe_scope,
        processed_pair_keys=processed_pair_keys,
        cancelled=cancelled,
        failure_reason=failure_reason,
    )


def scan_universe_optimizer_grid(
    *,
    broker: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    universe_mode: ScanUniverseMode,
    defaults: StrategyDefaults,
    search_space: Mapping[str, Any],
    fee_mode: str | None = None,
    normalized_group: NormalizedGroup | str | None = None,
    symbols: Sequence[str] | None = None,
    min_r_squared: float = 0.9,
    top_n_per_pair: int = 10,
    progress_callback: ProgressCallback | None = None,
    partial_result_callback: PartialResultCallback | None = None,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    universe_scope: str = ScanUniverseMode.ALL.value,
    resume_result: OptimizerGridScanResult | None = None,
    allowed_pair_keys: Sequence[str] | None = None,
) -> OptimizerGridScanResult:
    universe_symbols = resolve_scan_symbols(
        broker=broker,
        universe_mode=universe_mode,
        normalized_group=normalize_group_value(normalized_group),
        symbols=symbols,
    )
    total_symbols = len(universe_symbols)
    symbol_frames: dict[str, pl.DataFrame] = {}
    specs_by_symbol: dict[str, Mapping[str, Any]] = {}
    emit_progress(progress_callback, 0, total_symbols, "Loading scanner quotes")
    for index, symbol in enumerate(universe_symbols, start=1):
        if is_cancelled(cancel_check):
            resumed_rows = list((resume_result.rows if resume_result is not None else []) or [])
            resumed_processed_pair_keys = list((resume_result.processed_pair_keys if resume_result is not None else []) or [])
            resumed_summary = resume_result.summary if resume_result is not None else None
            return _scan_result(
                total_symbols_requested=total_symbols,
                loaded_symbols=len(symbol_frames) if resume_result is None else int(resumed_summary.loaded_symbols),
                total_pair_candidates=0 if resume_result is None else int(resumed_summary.total_pair_candidates),
                total_pairs_evaluated=0 if resume_result is None else int(resumed_summary.total_pairs_evaluated),
                pairs_with_data=0 if resume_result is None else int(resumed_summary.pairs_with_data),
                pairs_with_hits=0 if resume_result is None else int(resumed_summary.pairs_with_hits),
                rows=resumed_rows,
                total_trials_evaluated=0 if resume_result is None else int(resumed_summary.total_trials_evaluated),
                min_r_squared=min_r_squared,
                top_n_per_pair=top_n_per_pair,
                universe_symbols=universe_symbols,
                universe_scope=universe_scope,
                processed_pair_keys=resumed_processed_pair_keys,
                cancelled=True,
                failure_reason="cancelled",
            )
        frame = load_symbol_quote_frame(
            broker=broker,
            symbol=symbol,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
        )
        if not frame.is_empty():
            symbol_frames[symbol] = frame
            specs_by_symbol[symbol] = load_instrument_spec(broker, symbol)
        emit_progress(progress_callback, index, total_symbols, "Loading scanner quotes")

    managed_executor_context = (
        shared_process_pool(parallel_workers)
        if parallel_workers is not None and int(parallel_workers) > 1
        else nullcontext(None)
    )
    with managed_executor_context as parallel_executor:
        return scan_symbol_frames_optimizer_grid(
            symbol_frames=symbol_frames,
            specs_by_symbol=specs_by_symbol,
            defaults=defaults,
            search_space=search_space,
            timeframe=timeframe,
            fee_mode=fee_mode,
            min_r_squared=min_r_squared,
            top_n_per_pair=top_n_per_pair,
            progress_callback=progress_callback,
            partial_result_callback=partial_result_callback,
            cancel_check=cancel_check,
            parallel_workers=parallel_workers,
            parallel_executor=parallel_executor,
            universe_scope=universe_scope,
            resume_result=resume_result,
            allowed_pair_keys=allowed_pair_keys,
        )
