from __future__ import annotations

from collections.abc import Callable, Sequence

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


def random_candidate(search_space: DistanceGridSearchSpace, rng: np.random.Generator) -> Candidate:
    max_attempts = 256
    for _ in range(max_attempts):
        candidate: Candidate = (
            int(rng.integers(0, len(search_space.lookback_bars))),
            int(rng.integers(0, len(search_space.entry_z))),
            int(rng.integers(0, len(search_space.exit_z))),
            int(rng.integers(0, len(search_space.stop_z))),
            int(rng.integers(0, len(search_space.bollinger_k))),
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
        len(search_space.bollinger_k),
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
    progress_callback: ProgressCallback | None = None,
    progress_stage: str = "Genetic search",
    progress_total: int = 0,
) -> tuple[int, bool]:
    task_items = []
    seen: set[Candidate] = set()
    trial_id = next_trial_id
    for candidate in population:
        if candidate in seen or candidate in cache:
            continue
        seen.add(candidate)
        params = params_from_candidate(search_space, candidate)
        if params is None:
            continue
        task_items.append((candidate, trial_id, params))
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
    )
    for candidate, row in results:
        cache[candidate] = row
    return trial_id, cancelled
