from __future__ import annotations

from datetime import datetime
from itertools import combinations
from typing import Mapping, Sequence

import numpy as np
import polars as pl

from domain.contracts import NormalizedGroup, ScanUniverseMode, Timeframe, UnitRootGate
from domain.data.catalog_groups import filter_catalog_by_group
from domain.data.io import load_instrument_catalog_frame, load_quotes_range
from domain.scan.johansen_core import scan_pair_payload, sort_rows
from domain.scan.johansen_models import (
    JohansenScanParameters,
    JohansenUniverseScanResult,
    JohansenUniverseScanSummary,
    ProgressCallback,
)
from domain.scan.unit_root import UnitRootScreenResult, screen_series_for_cointegration
from workers.executor import build_process_pool


def emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


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
            catalog = filter_catalog_by_group(catalog, group_value)

    if catalog.is_empty():
        return []
    return catalog.get_column("symbol").sort().to_list()


def screen_symbol_payload(payload: tuple[str, np.ndarray, UnitRootGate]) -> tuple[str, UnitRootScreenResult]:
    symbol, values, unit_root_gate = payload
    result = screen_series_for_cointegration(np.asarray(values, dtype=np.float64), unit_root_gate)
    return symbol, result


def scan_symbol_frames_johansen(
    symbol_frames: Mapping[str, pl.DataFrame],
    unit_root_gate: UnitRootGate,
    params: JohansenScanParameters,
    progress_callback: ProgressCallback | None = None,
    parallel_workers: int | None = None,
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
            symbol, result = screen_symbol_payload(payload)
            symbol_results[symbol] = result
            emit_progress(progress_callback, index, total_loaded, "Unit root screening")
    else:
        with build_process_pool(max_workers=symbol_worker_count) as executor:
            for index, (symbol, result) in enumerate(executor.map(screen_symbol_payload, symbol_payloads), start=1):
                symbol_results[symbol] = result
                emit_progress(progress_callback, index, total_loaded, f"Unit root screening ({symbol_worker_count} workers)")

    i1_symbols = sorted(symbol for symbol, result in symbol_results.items() if result.passes_gate)
    pair_candidates = list(combinations(i1_symbols, 2))
    total_pairs = len(pair_candidates)
    emit_progress(progress_callback, 0, total_pairs, "Johansen pairs")

    def iter_pair_payloads() -> Sequence[tuple[str, str, np.ndarray, np.ndarray, UnitRootGate, JohansenScanParameters]]:
        for symbol_1, symbol_2 in pair_candidates:
            left = loaded_frames[symbol_1].rename({"close": "close_1"})
            right = loaded_frames[symbol_2].rename({"close": "close_2"})
            aligned = left.join(right, on="time", how="inner").sort("time")
            if aligned.is_empty():
                yield (symbol_1, symbol_2, np.asarray([], dtype=np.float64), np.asarray([], dtype=np.float64), unit_root_gate, params)
                continue
            yield (
                symbol_1,
                symbol_2,
                aligned.get_column("close_1").to_numpy(),
                aligned.get_column("close_2").to_numpy(),
                unit_root_gate,
                params,
            )

    rows = []
    pair_worker_count = resolve_worker_count(parallel_workers, total_pairs)
    if pair_worker_count <= 1:
        for pair_index, payload in enumerate(iter_pair_payloads(), start=1):
            rows.append(scan_pair_payload(payload))
            emit_progress(progress_callback, pair_index, total_pairs, "Johansen pairs")
    else:
        with build_process_pool(max_workers=pair_worker_count) as executor:
            for pair_index, row in enumerate(executor.map(scan_pair_payload, iter_pair_payloads()), start=1):
                rows.append(row)
                emit_progress(progress_callback, pair_index, total_pairs, f"Johansen pairs ({pair_worker_count} workers)")

    rows = sort_rows(rows)
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
    emit_progress(progress_callback, 0, total_symbols, "Loading quotes")
    symbol_frames: dict[str, pl.DataFrame] = {}
    for index, symbol in enumerate(universe_symbols, start=1):
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
    )
