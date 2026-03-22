from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, wait
from typing import Any, Callable, Mapping, Sequence

import polars as pl

from domain.contracts import PairSelection, StrategyDefaults
from domain.optimizer.distance_models import CandidateTask, CancellationCheck, DistanceOptimizationRow, DistanceTask, ProgressCallback
from workers.executor import build_process_pool


EvaluateParamsFn = Callable[..., DistanceOptimizationRow]


def is_cancelled(cancel_check: CancellationCheck | None) -> bool:
    return bool(cancel_check and cancel_check())


def emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


def _chunk_size(total_items: int, worker_count: int) -> int:
    return max(1, min(32, (total_items + (worker_count * 8) - 1) // (worker_count * 8)))


def _chunk_tasks(tasks: Sequence[Any], worker_count: int) -> list[list[Any]]:
    size = _chunk_size(len(tasks), worker_count)
    return [list(tasks[index : index + size]) for index in range(0, len(tasks), size)]


def resolve_worker_count(parallel_workers: int | None, task_count: int) -> int:
    if task_count <= 0:
        return 1
    if parallel_workers is None:
        return 1
    return max(1, min(int(parallel_workers), task_count))


def _collect_parallel_results(
    submit_chunk,
    chunks: Sequence[Sequence[Any]],
    worker_count: int,
    cancel_check: CancellationCheck | None,
    progress_callback: ProgressCallback | None = None,
    progress_total: int = 0,
    progress_stage: str = "",
    completed_offset: int = 0,
) -> tuple[list[Any], bool]:
    if not chunks:
        emit_progress(progress_callback, completed_offset, progress_total, progress_stage)
        return [], False

    executor = build_process_pool(max_workers=worker_count)
    cancelled = False
    results: list[Any] = []
    completed = completed_offset
    pending_chunks = deque(chunks)
    in_flight: dict[Future, Sequence[Any]] = {}

    def submit_available() -> None:
        while pending_chunks and len(in_flight) < worker_count * 2:
            chunk = pending_chunks.popleft()
            in_flight[submit_chunk(executor, chunk)] = chunk

    emit_progress(progress_callback, completed, progress_total, progress_stage)

    try:
        submit_available()
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
                chunk_results = future.result()
                results.extend(chunk_results)
                completed += len(chunk_results)
                emit_progress(progress_callback, completed, progress_total, progress_stage)
            submit_available()
    finally:
        executor.shutdown(wait=not cancelled, cancel_futures=cancelled)

    return results, cancelled


def _evaluate_params_chunk(
    tasks: Sequence[DistanceTask],
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
    evaluate_params_fn: EvaluateParamsFn,
) -> list[DistanceOptimizationRow]:
    return [
        evaluate_params_fn(
            trial_id=trial_id,
            frame=frame,
            pair=pair,
            defaults=defaults,
            params=params,
            objective_metric=objective_metric,
            point_1=point_1,
            point_2=point_2,
            contract_size_1=contract_size_1,
            contract_size_2=contract_size_2,
            spec_1=spec_1,
            spec_2=spec_2,
        )
        for trial_id, params in tasks
    ]


def _evaluate_candidate_chunk(
    tasks: Sequence[CandidateTask],
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
    evaluate_params_fn: EvaluateParamsFn,
) -> list[tuple[Any, DistanceOptimizationRow]]:
    return [
        (
            candidate,
            evaluate_params_fn(
                trial_id=trial_id,
                frame=frame,
                pair=pair,
                defaults=defaults,
                params=params,
                objective_metric=objective_metric,
                point_1=point_1,
                point_2=point_2,
                contract_size_1=contract_size_1,
                contract_size_2=contract_size_2,
                spec_1=spec_1,
                spec_2=spec_2,
            ),
        )
        for candidate, trial_id, params in tasks
    ]


def evaluate_distance_tasks(
    *,
    tasks: Sequence[DistanceTask],
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
    parallel_workers: int | None,
    cancel_check: CancellationCheck | None,
    evaluate_params_fn: EvaluateParamsFn,
    progress_callback: ProgressCallback | None = None,
    progress_stage: str = "Grid search",
) -> tuple[list[DistanceOptimizationRow], bool]:
    worker_count = resolve_worker_count(parallel_workers, len(tasks))
    total_tasks = len(tasks)
    if worker_count <= 1:
        rows: list[DistanceOptimizationRow] = []
        cancelled = False
        emit_progress(progress_callback, 0, total_tasks, progress_stage)
        for trial_id, params in tasks:
            if is_cancelled(cancel_check):
                cancelled = True
                break
            rows.append(
                evaluate_params_fn(
                    trial_id=trial_id,
                    frame=frame,
                    pair=pair,
                    defaults=defaults,
                    params=params,
                    objective_metric=objective_metric,
                    point_1=point_1,
                    point_2=point_2,
                    contract_size_1=contract_size_1,
                    contract_size_2=contract_size_2,
                    spec_1=spec_1,
                    spec_2=spec_2,
                )
            )
            emit_progress(progress_callback, len(rows), total_tasks, progress_stage)
        return rows, cancelled

    chunks = _chunk_tasks(tasks, worker_count)

    def submit_chunk(executor, chunk):
        return executor.submit(
            _evaluate_params_chunk,
            chunk,
            frame,
            pair,
            defaults,
            objective_metric,
            point_1,
            point_2,
            contract_size_1,
            contract_size_2,
            spec_1,
            spec_2,
            evaluate_params_fn,
        )

    return _collect_parallel_results(
        submit_chunk,
        chunks,
        worker_count,
        cancel_check,
        progress_callback=progress_callback,
        progress_total=total_tasks,
        progress_stage=progress_stage,
    )


def evaluate_candidate_distance_tasks(
    *,
    tasks: Sequence[CandidateTask],
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
    parallel_workers: int | None,
    cancel_check: CancellationCheck | None,
    evaluate_params_fn: EvaluateParamsFn,
    progress_callback: ProgressCallback | None = None,
    progress_total: int = 0,
    progress_stage: str = "Genetic search",
    completed_offset: int = 0,
) -> tuple[list[tuple[Any, DistanceOptimizationRow]], bool]:
    worker_count = resolve_worker_count(parallel_workers, len(tasks))
    if worker_count <= 1:
        rows: list[tuple[Any, DistanceOptimizationRow]] = []
        cancelled = False
        emit_progress(progress_callback, completed_offset, progress_total, progress_stage)
        for candidate, trial_id, params in tasks:
            if is_cancelled(cancel_check):
                cancelled = True
                break
            rows.append(
                (
                    candidate,
                    evaluate_params_fn(
                        trial_id=trial_id,
                        frame=frame,
                        pair=pair,
                        defaults=defaults,
                        params=params,
                        objective_metric=objective_metric,
                        point_1=point_1,
                        point_2=point_2,
                        contract_size_1=contract_size_1,
                        contract_size_2=contract_size_2,
                        spec_1=spec_1,
                        spec_2=spec_2,
                    ),
                )
            )
            emit_progress(progress_callback, completed_offset + len(rows), progress_total, progress_stage)
        return rows, cancelled

    chunks = _chunk_tasks(tasks, worker_count)

    def submit_chunk(executor, chunk):
        return executor.submit(
            _evaluate_candidate_chunk,
            chunk,
            frame,
            pair,
            defaults,
            objective_metric,
            point_1,
            point_2,
            contract_size_1,
            contract_size_2,
            spec_1,
            spec_2,
            evaluate_params_fn,
        )

    return _collect_parallel_results(
        submit_chunk,
        chunks,
        worker_count,
        cancel_check,
        progress_callback=progress_callback,
        progress_total=progress_total,
        progress_stage=progress_stage,
        completed_offset=completed_offset,
    )
