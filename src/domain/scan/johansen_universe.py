from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, wait
from datetime import datetime
from itertools import combinations, islice
from typing import Mapping, Sequence

import numpy as np
import polars as pl

from domain.contracts import NormalizedGroup, ScanUniverseMode, Timeframe, UnitRootGate
from domain.data.catalog_groups import filter_catalog_by_group
from domain.data.io import load_instrument_catalog_frame, load_quotes_range
from domain.scan.johansen_core import scan_pair_payload, sort_rows
from domain.scan.johansen_models import (
    CancellationCheck,
    JohansenScanParameters,
    JohansenUniverseScanResult,
    JohansenUniverseScanRow,
    JohansenUniverseScanSummary,
    PartialResultCallback,
    ProgressCallback,
)
from domain.scan.unit_root import UnitRootScreenResult, screen_series_for_cointegration
from workers.executor import build_process_pool, shutdown_executor


def emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


def is_cancelled(cancel_check: CancellationCheck | None) -> bool:
    return bool(cancel_check and cancel_check())


def resolve_worker_count(parallel_workers: int | None, task_count: int) -> int:
    if task_count <= 1:
        return 1
    if parallel_workers is None:
        return 1
    return max(1, min(int(parallel_workers), int(task_count)))


def normalize_group_value(group: NormalizedGroup | str | None) -> str | None:
    if group is None:
        return None
    if isinstance(group, NormalizedGroup):
        return group.value
    return group


def load_symbol_close_frame(
    broker: str,
    symbol: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
) -> pl.DataFrame:
    frame = load_quotes_range(broker=broker, symbol=symbol, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return pl.DataFrame(schema={"time": pl.Datetime(time_zone="UTC"), "close": pl.Float64})
    return frame.select("time", "close").sort("time")


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
        group_value = normalize_group_value(normalized_group)
        if group_value:
            catalog = filter_catalog_by_group(catalog, group_value, broker=broker)

    if catalog.is_empty():
        return []
    return catalog.get_column("symbol").sort().to_list()


def screen_symbol_payload(payload: tuple[str, np.ndarray, UnitRootGate]) -> tuple[str, UnitRootScreenResult]:
    symbol, values, unit_root_gate = payload
    result = screen_series_for_cointegration(np.asarray(values, dtype=np.float64), unit_root_gate)
    return symbol, result


def screen_symbol_payloads_chunk(
    payloads: Sequence[tuple[str, np.ndarray, UnitRootGate]],
) -> list[tuple[str, UnitRootScreenResult]]:
    return [screen_symbol_payload(payload) for payload in payloads]


def scan_pair_payloads_chunk(
    payloads: Sequence[
        tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters, UnitRootScreenResult, UnitRootScreenResult]
    ],
) -> list[JohansenUniverseScanRow]:
    return [scan_pair_payload(payload) for payload in payloads]


def _chunk_size(total_items: int, worker_count: int) -> int:
    return max(1, min(32, (total_items + (worker_count * 8) - 1) // max(1, worker_count * 8)))


def _chunk_payloads[T](payloads: Sequence[T], worker_count: int) -> list[list[T]]:
    size = _chunk_size(len(payloads), worker_count)
    return [list(payloads[index : index + size]) for index in range(0, len(payloads), size)]


def _take_next_payload_chunk(payload_iter, chunk_size: int):
    return list(islice(payload_iter, max(1, int(chunk_size))))


def _build_result(
    *,
    symbol_frames: Mapping[str, pl.DataFrame],
    loaded_frames: Mapping[str, pl.DataFrame],
    i1_symbols: Sequence[str],
    rows: Sequence[JohansenUniverseScanRow],
    cancelled: bool,
) -> JohansenUniverseScanResult:
    sorted_rows = sort_rows(list(rows))
    summary = JohansenUniverseScanSummary(
        total_symbols_requested=len(symbol_frames),
        loaded_symbols=len(loaded_frames),
        prefiltered_i1_symbols=len(i1_symbols),
        total_pairs_evaluated=len(sorted_rows),
        threshold_passed_pairs=sum(1 for row in sorted_rows if row.threshold_passed),
        screened_out_pairs=sum(1 for row in sorted_rows if not row.eligible_for_cointegration or row.failure_reason is not None),
    )
    return JohansenUniverseScanResult(
        summary=summary,
        rows=sorted_rows,
        universe_symbols=sorted(symbol_frames),
        cancelled=bool(cancelled),
    )


def _emit_partial_result(
    partial_result_callback: PartialResultCallback | None,
    *,
    symbol_frames: Mapping[str, pl.DataFrame],
    loaded_frames: Mapping[str, pl.DataFrame],
    i1_symbols: Sequence[str],
    rows: Sequence[JohansenUniverseScanRow],
    cancelled: bool,
) -> JohansenUniverseScanResult:
    result = _build_result(
        symbol_frames=symbol_frames,
        loaded_frames=loaded_frames,
        i1_symbols=i1_symbols,
        rows=rows,
        cancelled=cancelled,
    )
    if partial_result_callback is not None:
        partial_result_callback(result)
    return result


def scan_symbol_frames_johansen(
    symbol_frames: Mapping[str, pl.DataFrame],
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
    progress_callback: ProgressCallback | None = None,
    parallel_workers: int | None = None,
    partial_result_callback: PartialResultCallback | None = None,
    cancel_check: CancellationCheck | None = None,
    resume_result: JohansenUniverseScanResult | None = None,
    allowed_pair_keys: Sequence[str] | None = None,
) -> JohansenUniverseScanResult:
    loaded_frames: dict[str, pl.DataFrame] = {
        symbol: frame.select("time", "close").sort("time")
        for symbol, frame in symbol_frames.items()
        if not frame.is_empty()
    }
    symbol_results: dict[str, UnitRootScreenResult] = {}
    total_loaded = len(loaded_frames)
    emit_progress(progress_callback, 0, total_loaded, "Unit root screening")
    symbol_worker_count = resolve_worker_count(parallel_workers, total_loaded)
    symbol_payloads = [
        (symbol, frame.get_column("close").to_numpy(), unit_root_gate)
        for symbol, frame in loaded_frames.items()
    ]
    if symbol_worker_count <= 1:
        for index, payload in enumerate(symbol_payloads, start=1):
            if is_cancelled(cancel_check):
                return _emit_partial_result(
                    partial_result_callback,
                    symbol_frames=symbol_frames,
                    loaded_frames=loaded_frames,
                    i1_symbols=[],
                    rows=list((resume_result.rows if resume_result is not None else []) or []),
                    cancelled=True,
                )
            symbol, result = screen_symbol_payload(payload)
            symbol_results[symbol] = result
            emit_progress(progress_callback, index, total_loaded, "Unit root screening")
    else:
        symbol_chunks = _chunk_payloads(symbol_payloads, symbol_worker_count)
        executor = build_process_pool(max_workers=symbol_worker_count)
        pending_chunks = deque(symbol_chunks)
        in_flight: dict[Future, Sequence[tuple[str, np.ndarray, UnitRootGate]]] = {}
        completed_symbols = 0
        cancelled = False

        def submit_symbol_chunks() -> None:
            while pending_chunks and len(in_flight) < symbol_worker_count * 2:
                chunk = pending_chunks.popleft()
                in_flight[executor.submit(screen_symbol_payloads_chunk, chunk)] = chunk

        try:
            submit_symbol_chunks()
            while in_flight:
                if is_cancelled(cancel_check):
                    cancelled = True
                    for future in in_flight:
                        future.cancel()
                    break
                done, _ = wait(tuple(in_flight), timeout=0.1, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    in_flight.pop(future, None)
                    for symbol, result in future.result():
                        symbol_results[symbol] = result
                        completed_symbols += 1
                        emit_progress(progress_callback, completed_symbols, total_loaded, f"Unit root screening ({symbol_worker_count} workers)")
                submit_symbol_chunks()
        finally:
            if cancelled:
                shutdown_executor(executor, wait=False, cancel_futures=True, force_kill=True)
            else:
                shutdown_executor(executor)
        if cancelled:
            return _emit_partial_result(
                partial_result_callback,
                symbol_frames=symbol_frames,
                loaded_frames=loaded_frames,
                i1_symbols=[],
                rows=list((resume_result.rows if resume_result is not None else []) or []),
                cancelled=True,
            )

    i1_symbols = sorted(symbol for symbol, result in symbol_results.items() if result.passes_gate)
    allowed_pair_key_set = {str(item) for item in (allowed_pair_keys or []) if str(item)}
    pair_candidates = [
        (symbol_1, symbol_2)
        for symbol_1, symbol_2 in combinations(i1_symbols, 2)
        if not allowed_pair_key_set or f"{symbol_1}::{symbol_2}" in allowed_pair_key_set
    ]
    total_pairs = len(pair_candidates)
    resumed_rows = [
        row
        for row in list((resume_result.rows if resume_result is not None else []) or [])
        if not allowed_pair_key_set or f"{row.symbol_1}::{row.symbol_2}" in allowed_pair_key_set
    ]
    processed_pair_keys = {
        (str(row.symbol_1), str(row.symbol_2))
        for row in resumed_rows
    }
    processed_pairs = len(processed_pair_keys)
    emit_progress(progress_callback, processed_pairs, total_pairs, "Johansen pairs")

    def iter_pair_payloads() -> Sequence[tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters, UnitRootScreenResult, UnitRootScreenResult]]:
        for symbol_1, symbol_2 in pair_candidates:
            if (symbol_1, symbol_2) in processed_pair_keys:
                continue
            left = loaded_frames[symbol_1].rename({"close": "close_1"})
            right = loaded_frames[symbol_2].rename({"close": "close_2"})
            aligned = left.join(right, on="time", how="inner").sort("time")
            if aligned.is_empty():
                yield (
                    symbol_1,
                    symbol_2,
                    np.asarray([], dtype=np.float64),
                    np.asarray([], dtype=np.float64),
                    unit_root_gate,
                    params,
                    symbol_results[symbol_1],
                    symbol_results[symbol_2],
                )
                continue
            yield (
                symbol_1,
                symbol_2,
                aligned.get_column("close_1").to_numpy(),
                aligned.get_column("close_2").to_numpy(),
                unit_root_gate,
                params,
                symbol_results[symbol_1],
                symbol_results[symbol_2],
            )

    rows = list(resumed_rows)
    remaining_pairs = max(0, total_pairs - processed_pairs)
    pair_worker_count = resolve_worker_count(parallel_workers, remaining_pairs or total_pairs)
    if pair_worker_count <= 1:
        for payload in iter_pair_payloads():
            if is_cancelled(cancel_check):
                return _emit_partial_result(
                    partial_result_callback,
                    symbol_frames=symbol_frames,
                    loaded_frames=loaded_frames,
                    i1_symbols=i1_symbols,
                    rows=rows,
                    cancelled=True,
                )
            rows.append(scan_pair_payload(payload))
            processed_pairs += 1
            emit_progress(progress_callback, processed_pairs, total_pairs, "Johansen pairs")
            _emit_partial_result(
                partial_result_callback,
                symbol_frames=symbol_frames,
                loaded_frames=loaded_frames,
                i1_symbols=i1_symbols,
                rows=rows,
                cancelled=False,
            )
    else:
        pair_chunk_size = max(1, min(8, _chunk_size(remaining_pairs, pair_worker_count)))
        pair_payload_iter = iter(iter_pair_payloads())
        executor = build_process_pool(max_workers=pair_worker_count)
        in_flight: dict[Future, int] = {}
        cancelled = False
        prepared_pairs = processed_pairs

        def submit_pair_chunks() -> None:
            nonlocal prepared_pairs
            while len(in_flight) < pair_worker_count:
                chunk = _take_next_payload_chunk(pair_payload_iter, pair_chunk_size)
                if not chunk:
                    break
                prepared_pairs += len(chunk)
                emit_progress(
                    progress_callback,
                    processed_pairs,
                    total_pairs,
                    f"Preparing Johansen pair payloads {prepared_pairs}/{total_pairs}",
                )
                in_flight[executor.submit(scan_pair_payloads_chunk, chunk)] = len(chunk)

        try:
            submit_pair_chunks()
            while in_flight:
                if is_cancelled(cancel_check):
                    cancelled = True
                    for future in in_flight:
                        future.cancel()
                    break
                done, _ = wait(tuple(in_flight), timeout=0.1, return_when=FIRST_COMPLETED)
                if not done:
                    continue
                for future in done:
                    in_flight.pop(future, None)
                    chunk_rows = future.result()
                    if chunk_rows:
                        rows.extend(chunk_rows)
                        processed_pairs += len(chunk_rows)
                        emit_progress(progress_callback, processed_pairs, total_pairs, f"Johansen pairs ({pair_worker_count} workers)")
                        _emit_partial_result(
                            partial_result_callback,
                            symbol_frames=symbol_frames,
                            loaded_frames=loaded_frames,
                            i1_symbols=i1_symbols,
                            rows=rows,
                            cancelled=False,
                        )
                submit_pair_chunks()
        finally:
            if cancelled:
                shutdown_executor(executor, wait=False, cancel_futures=True, force_kill=True)
            else:
                shutdown_executor(executor)
        if cancelled:
            return _emit_partial_result(
                partial_result_callback,
                symbol_frames=symbol_frames,
                loaded_frames=loaded_frames,
                i1_symbols=i1_symbols,
                rows=rows,
                cancelled=True,
            )

    return _build_result(
        symbol_frames=symbol_frames,
        loaded_frames=loaded_frames,
        i1_symbols=i1_symbols,
        rows=rows,
        cancelled=False,
    )


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
    partial_result_callback: PartialResultCallback | None = None,
    cancel_check: CancellationCheck | None = None,
    resume_result: JohansenUniverseScanResult | None = None,
    allowed_pair_keys: Sequence[str] | None = None,
) -> JohansenUniverseScanResult:
    universe_symbols = resolve_scan_symbols(
        broker=broker,
        universe_mode=universe_mode,
        normalized_group=normalized_group,
        symbols=symbols,
    )
    total_symbols = len(universe_symbols)
    emit_progress(progress_callback, 0, total_symbols, "Loading quotes")
    symbol_frames: dict[str, pl.DataFrame] = {}
    for index, symbol in enumerate(universe_symbols, start=1):
        if is_cancelled(cancel_check):
            return _emit_partial_result(
                partial_result_callback,
                symbol_frames=symbol_frames,
                loaded_frames=symbol_frames,
                i1_symbols=[],
                rows=list((resume_result.rows if resume_result is not None else []) or []),
                cancelled=True,
            )
        symbol_frames[symbol] = load_symbol_close_frame(
            broker=broker,
            symbol=symbol,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
        )
        emit_progress(progress_callback, index, total_symbols, "Loading quotes")
    return scan_symbol_frames_johansen(
        symbol_frames=symbol_frames,
        unit_root_gate=unit_root_gate,
        params=params,
        progress_callback=progress_callback,
        parallel_workers=parallel_workers,
        partial_result_callback=partial_result_callback,
        cancel_check=cancel_check,
        resume_result=resume_result,
        allowed_pair_keys=allowed_pair_keys,
    )
