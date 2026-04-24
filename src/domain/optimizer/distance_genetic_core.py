from __future__ import annotations

from concurrent.futures import Executor
from collections.abc import Callable, Sequence
from dataclasses import replace

import numpy as np
import polars as pl

from domain.contracts import PairSelection, StrategyDefaults
from domain.optimizer.distance_metrics import params_from_candidate
from domain.optimizer.distance_models import (
    Candidate,
    CancellationCheck,
    DistanceGeneticConfig,
    DistanceGridSearchSpace,
    DistanceOptimizationRow,
    ProgressCallback,
)


EvaluateCandidateTasksFn = Callable[..., tuple[list[tuple[Candidate, DistanceOptimizationRow]], bool]]


def _execution_signature(params) -> tuple[int, float, float, float | None]:
    return (
        int(params.lookback_bars),
        float(params.entry_z),
        float(params.exit_z),
        None if params.stop_z is None else float(params.stop_z),
    )


def random_candidate(search_space: DistanceGridSearchSpace, rng: np.random.Generator) -> Candidate:
    max_attempts = 256
    for _ in range(max_attempts):
        candidate: Candidate = (
            int(rng.integers(0, len(search_space.lookback_bars))),
            int(rng.integers(0, len(search_space.entry_z))),
            int(rng.integers(0, len(search_space.exit_z))),
            int(rng.integers(0, len(search_space.stop_z))),
        )
        if params_from_candidate(search_space, candidate) is not None:
            return candidate
    raise ValueError("No valid parameter combinations available for genetic optimization.")


def mutate_candidate(
    candidate: Candidate,
    search_space: DistanceGridSearchSpace,
    config: DistanceGeneticConfig,
    rng: np.random.Generator,
) -> Candidate:
    values = list(candidate)
    lengths = (
        len(search_space.lookback_bars),
        len(search_space.entry_z),
        len(search_space.exit_z),
        len(search_space.stop_z),
    )
    for index, length in enumerate(lengths):
        if length > 1 and float(rng.random()) < config.mutation_rate:
            values[index] = int(rng.integers(0, length))
    mutated = tuple(values)
    if params_from_candidate(search_space, mutated) is not None:
        return mutated
    return random_candidate(search_space, rng)


def crossover_candidates(
    left: Candidate,
    right: Candidate,
    config: DistanceGeneticConfig,
    rng: np.random.Generator,
) -> tuple[Candidate, Candidate]:
    if float(rng.random()) >= config.crossover_rate:
        return left, right
    child_1 = []
    child_2 = []
    for left_gene, right_gene in zip(left, right, strict=True):
        if float(rng.random()) < 0.5:
            child_1.append(left_gene)
            child_2.append(right_gene)
        else:
            child_1.append(right_gene)
            child_2.append(left_gene)
    return tuple(child_1), tuple(child_2)


def tournament_select(
    population: list[Candidate],
    cache: dict[Candidate, DistanceOptimizationRow],
    config: DistanceGeneticConfig,
    rng: np.random.Generator,
) -> Candidate:
    indices = [int(rng.integers(0, len(population))) for _ in range(min(config.tournament_size, len(population)))]
    candidates = [population[index] for index in indices]
    return max(candidates, key=lambda candidate: cache[candidate].objective_score)


def evaluate_candidates_into_cache(
    *,
    population: Sequence[Candidate],
    cache: dict[Candidate, DistanceOptimizationRow],
    next_trial_id: int,
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    search_space: DistanceGridSearchSpace,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1,
    spec_2,
    parallel_workers: int | None,
    cancel_check: CancellationCheck | None,
    evaluate_candidate_tasks_fn: EvaluateCandidateTasksFn,
    execution_cache: dict[tuple[int, float, float, float | None], DistanceOptimizationRow] | None = None,
    progress_callback: ProgressCallback | None = None,
    progress_stage: str = "Genetic search",
    progress_total: int = 0,
    executor: Executor | None = None,
    evaluate_params_fn=None,
) -> tuple[int, bool]:
    task_items = []
    task_signatures: dict[Candidate, tuple[int, float, float, float | None]] = {}
    pending_by_signature: dict[tuple[int, float, float, float | None], list[tuple[Candidate, int, object]]] = {}
    seen: set[Candidate] = set()
    trial_id = next_trial_id
    for candidate in population:
        if candidate in seen or candidate in cache:
            continue
        seen.add(candidate)
        params = params_from_candidate(search_space, candidate)
        if params is None:
            continue
        signature = _execution_signature(params)
        if execution_cache is not None and signature in execution_cache:
            cache[candidate] = replace(
                execution_cache[signature],
                trial_id=int(trial_id),
                lookback_bars=int(params.lookback_bars),
                entry_z=float(params.entry_z),
                exit_z=float(params.exit_z),
                stop_z=params.stop_z,
                bollinger_k=float(params.bollinger_k),
            )
            trial_id += 1
            continue
        pending = pending_by_signature.setdefault(signature, [])
        pending.append((candidate, trial_id, params))
        if len(pending) == 1:
            task_items.append((candidate, trial_id, params))
            task_signatures[candidate] = signature
        trial_id += 1

    results, cancelled = evaluate_candidate_tasks_fn(
        tasks=task_items,
        frame=frame,
        pair=pair,
        defaults=defaults,
        objective_metric=objective_metric,
        point_1=point_1,
        point_2=point_2,
        contract_size_1=contract_size_1,
        contract_size_2=contract_size_2,
        spec_1=spec_1,
        spec_2=spec_2,
        parallel_workers=parallel_workers,
        cancel_check=cancel_check,
        progress_callback=progress_callback,
        progress_total=progress_total,
        progress_stage=progress_stage,
        completed_offset=len(cache),
        executor=executor,
        evaluate_params_fn=evaluate_params_fn,
    )
    rows_by_signature: dict[tuple[int, float, float, float | None], DistanceOptimizationRow] = {}
    for candidate, row in results:
        signature = task_signatures.get(candidate)
        if signature is None:
            continue
        rows_by_signature[signature] = row
        if execution_cache is not None:
            execution_cache[signature] = row
    for signature, pending_items in pending_by_signature.items():
        row = rows_by_signature.get(signature)
        if row is None:
            continue
        for pending_candidate, pending_trial_id, params in pending_items:
            cache[pending_candidate] = replace(
                row,
                trial_id=int(pending_trial_id),
                lookback_bars=int(params.lookback_bars),
                entry_z=float(params.entry_z),
                exit_z=float(params.exit_z),
                stop_z=params.stop_z,
                bollinger_k=float(params.bollinger_k),
            )
    return trial_id, cancelled
