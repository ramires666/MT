from __future__ import annotations

from concurrent.futures import Executor
from contextlib import nullcontext
from dataclasses import replace
from itertools import product
from typing import Any, Iterable, Mapping

import numpy as np
import polars as pl

from domain.backtest.distance import (
    DistanceParameters,
    load_pair_frame,
    prepare_distance_backtest_context,
    run_distance_backtest_metrics_frame,
)
from domain.contracts import Algorithm, PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec
from domain.optimizer.distance_genetic_core import (
    crossover_candidates as _crossover_candidates,
    evaluate_candidates_into_cache,
    mutate_candidate as _mutate_candidate,
    random_candidate as _random_candidate,
    tournament_select as _tournament_select,
)
from domain.optimizer.distance_metrics import (
    equity_metrics as _equity_metrics,
    objective_score as _objective_score,
    sort_rows as _sort_rows,
    validate_objective_metric as _validate_objective_metric,
)
from domain.optimizer.distance_models import (
    OBJECTIVE_METRICS,
    Candidate,
    CancellationCheck,
    DistanceGeneticConfig,
    DistanceGridSearchSpace,
    DistanceOptimizationResult,
    DistanceOptimizationRow,
    DistanceTask,
    parse_distance_genetic_config,
    parse_distance_search_space,
)
from domain.optimizer.distance_parallel import (
    emit_progress as _emit_progress,
    evaluate_candidate_distance_tasks,
    evaluate_distance_tasks,
    is_cancelled as _is_cancelled,
)
from workers.executor import shared_process_pool


def _normalize_algorithm_name(algorithm: str | Algorithm | None) -> str:
    raw_value = getattr(algorithm, "value", algorithm)
    normalized = str(raw_value or Algorithm.DISTANCE.value).strip().lower()
    if normalized not in {Algorithm.DISTANCE.value, Algorithm.OLS.value}:
        raise ValueError(f"Unsupported optimization algorithm: {normalized}")
    return normalized


def _evaluate_params(
    trial_id: int,
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    params: DistanceParameters,
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
    context=None,
    algorithm: str | Algorithm = Algorithm.DISTANCE,
) -> DistanceOptimizationRow:
    metrics = run_distance_backtest_metrics_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=point_1,
        point_2=point_2,
        contract_size_1=contract_size_1,
        contract_size_2=contract_size_2,
        spec_1=spec_1,
        spec_2=spec_2,
        context=context,
        algorithm=algorithm,
    )
    return DistanceOptimizationRow(
        trial_id=trial_id,
        objective_metric=objective_metric,
        objective_score=_objective_score(objective_metric, metrics),
        net_profit=float(metrics["net_profit"]),
        ending_equity=float(metrics["ending_equity"]),
        max_drawdown=float(metrics["max_drawdown"]),
        pnl_to_maxdd=float(metrics["pnl_to_maxdd"]),
        omega_ratio=float(metrics["omega_ratio"]),
        k_ratio=float(metrics["k_ratio"]),
        ulcer_index=float(metrics["ulcer_index"]),
        ulcer_performance=float(metrics["ulcer_performance"]),
        cagr=float(metrics["cagr"]),
        cagr_to_ulcer=float(metrics["cagr_to_ulcer"]),
        r_squared=float(metrics["r_squared"]),
        hurst_exponent=float(metrics["hurst_exponent"]),
        calmar=float(metrics["calmar"]),
        trades=int(metrics.get("trades", 0) or 0),
        win_rate=float(metrics.get("win_rate", 0.0) or 0.0),
        lookback_bars=params.lookback_bars,
        entry_z=params.entry_z,
        exit_z=params.exit_z,
        stop_z=params.stop_z,
        bollinger_k=params.bollinger_k,
        gross_profit=float(metrics["gross_profit"]),
        spread_cost=float(metrics["spread_cost"]),
        slippage_cost=float(metrics["slippage_cost"]),
        commission_cost=float(metrics["commission_cost"]),
        total_cost=float(metrics["total_cost"]),
    )


def _evaluate_distance_params(**kwargs) -> DistanceOptimizationRow:
    kwargs["algorithm"] = Algorithm.DISTANCE
    return _evaluate_params(**kwargs)


def _evaluate_ols_params(**kwargs) -> DistanceOptimizationRow:
    kwargs["algorithm"] = Algorithm.OLS
    return _evaluate_params(**kwargs)


def _prepare_optimizer_context(
    *,
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
):
    if frame.is_empty():
        return None
    return prepare_distance_backtest_context(
        frame=frame,
        pair=pair,
        defaults=defaults,
        point_1=point_1,
        point_2=point_2,
        contract_size_1=contract_size_1,
        contract_size_2=contract_size_2,
        spec_1=spec_1,
        spec_2=spec_2,
    )


def _execution_signature(params: DistanceParameters) -> tuple[int, float, float, float | None]:
    return (
        int(params.lookback_bars),
        float(params.entry_z),
        float(params.exit_z),
        None if params.stop_z is None else float(params.stop_z),
    )


def _clone_row_for_params(
    row: DistanceOptimizationRow,
    *,
    trial_id: int,
    params: DistanceParameters,
) -> DistanceOptimizationRow:
    return replace(
        row,
        trial_id=int(trial_id),
        lookback_bars=int(params.lookback_bars),
        entry_z=float(params.entry_z),
        exit_z=float(params.exit_z),
        stop_z=params.stop_z,
        bollinger_k=float(params.bollinger_k),
    )


def _evaluate_params_parallel(
    tasks,
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
    progress_callback=None,
    progress_stage: str = "Grid search",
    executor: Executor | None = None,
    evaluate_params_fn=_evaluate_distance_params,
):
    return evaluate_distance_tasks(
        tasks=tasks,
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
        evaluate_params_fn=evaluate_params_fn,
        prepare_context_fn=_prepare_optimizer_context,
        progress_callback=progress_callback,
        progress_stage=progress_stage,
        executor=executor,
    )


def _evaluate_candidate_tasks_parallel(
    tasks,
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
    progress_callback=None,
    progress_total: int = 0,
    progress_stage: str = "Genetic search",
    completed_offset: int = 0,
    executor: Executor | None = None,
    evaluate_params_fn=_evaluate_distance_params,
):
    return evaluate_candidate_distance_tasks(
        tasks=tasks,
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
        evaluate_params_fn=evaluate_params_fn,
        prepare_context_fn=_prepare_optimizer_context,
        progress_callback=progress_callback,
        progress_total=progress_total,
        progress_stage=progress_stage,
        completed_offset=completed_offset,
        executor=executor,
    )


def iter_distance_parameter_grid(search_space: DistanceGridSearchSpace) -> Iterable[DistanceParameters]:
    fixed_bollinger_k = float(search_space.bollinger_k)
    for lookback_bars, entry_z, exit_z, stop_z in product(
        search_space.lookback_bars,
        search_space.entry_z,
        search_space.exit_z,
        search_space.stop_z,
    ):
        if exit_z >= entry_z or (stop_z is not None and stop_z <= entry_z):
            continue
        yield DistanceParameters(
            lookback_bars=int(lookback_bars),
            entry_z=float(entry_z),
            exit_z=float(exit_z),
            stop_z=None if stop_z is None else float(stop_z),
            bollinger_k=fixed_bollinger_k,
        )


def count_distance_parameter_grid(search_space: DistanceGridSearchSpace | Mapping[str, Any]) -> int:
    if isinstance(search_space, Mapping):
        search_space = parse_distance_search_space(search_space)
    return sum(1 for _ in iter_distance_parameter_grid(search_space))


def optimize_distance_grid_frame(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    search_space: DistanceGridSearchSpace | Mapping[str, Any],
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    progress_callback=None,
    parallel_executor: Executor | None = None,
    algorithm: str | Algorithm = Algorithm.DISTANCE,
) -> DistanceOptimizationResult:
    managed_executor_context = (
        shared_process_pool(parallel_workers)
        if parallel_executor is None
        else nullcontext(parallel_executor)
    )
    with managed_executor_context as managed_executor:
        executor = parallel_executor or managed_executor
        algorithm_name = _normalize_algorithm_name(algorithm)
        _validate_objective_metric(objective_metric)
        if isinstance(search_space, Mapping):
            search_space = parse_distance_search_space(search_space)
        task_items = list(enumerate(iter_distance_parameter_grid(search_space), start=1))
        unique_tasks: list[tuple[int, DistanceParameters]] = []
        signature_by_trial: dict[int, tuple[int, float, float, float | None]] = {}
        first_seen: set[tuple[int, float, float, float | None]] = set()
        for trial_id, params in task_items:
            signature = _execution_signature(params)
            signature_by_trial[int(trial_id)] = signature
            if signature in first_seen:
                continue
            first_seen.add(signature)
            unique_tasks.append((trial_id, params))
        unique_rows, cancelled = _evaluate_params_parallel(
            tasks=unique_tasks,
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
            executor=executor,
            evaluate_params_fn=_evaluate_distance_params if algorithm_name == Algorithm.DISTANCE.value else _evaluate_ols_params,
        )
        unique_rows_by_signature = {
            signature_by_trial[int(row.trial_id)]: row
            for row in unique_rows
        }
        rows = [
            _clone_row_for_params(row, trial_id=trial_id, params=params)
            for trial_id, params in task_items
            if (row := unique_rows_by_signature.get(signature_by_trial[int(trial_id)])) is not None
        ]
        rows = _sort_rows(rows)
        return DistanceOptimizationResult(
            objective_metric=objective_metric,
            evaluated_trials=len(rows),
            rows=rows,
            best_trial_id=rows[0].trial_id if rows else None,
            cancelled=cancelled,
            failure_reason=None if rows or cancelled else "no_valid_parameter_combinations",
        )


def optimize_distance_genetic_frame(
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    search_space: DistanceGridSearchSpace | Mapping[str, Any],
    objective_metric: str,
    point_1: float,
    point_2: float,
    contract_size_1: float,
    contract_size_2: float,
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
    config: DistanceGeneticConfig | Mapping[str, Any] | None = None,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    progress_callback=None,
    parallel_executor: Executor | None = None,
    algorithm: str | Algorithm = Algorithm.DISTANCE,
) -> DistanceOptimizationResult:
    managed_executor_context = (
        shared_process_pool(parallel_workers)
        if parallel_executor is None
        else nullcontext(parallel_executor)
    )
    with managed_executor_context as managed_executor:
        executor = parallel_executor or managed_executor
        algorithm_name = _normalize_algorithm_name(algorithm)
        _validate_objective_metric(objective_metric)
        if isinstance(search_space, Mapping):
            search_space = parse_distance_search_space(search_space)
        if not isinstance(config, DistanceGeneticConfig):
            config = parse_distance_genetic_config(config)

        rng = np.random.default_rng(config.random_seed)
        cache: dict[Candidate, DistanceOptimizationRow] = {}
        execution_cache: dict[tuple[int, float, float, float | None], DistanceOptimizationRow] = {}
        next_trial_id = 1
        cancelled = False
        estimated_total = config.population_size * (config.generations + 1)
        population = [_random_candidate(search_space, rng) for _ in range(config.population_size)]
        _emit_progress(progress_callback, 0, estimated_total, "Genetic search")
        for generation in range(config.generations):
            if _is_cancelled(cancel_check):
                cancelled = True
                break
            next_trial_id, cancelled = evaluate_candidates_into_cache(
                population=population,
                cache=cache,
                next_trial_id=next_trial_id,
                frame=frame,
                pair=pair,
                defaults=defaults,
                search_space=search_space,
                objective_metric=objective_metric,
                point_1=point_1,
                point_2=point_2,
                contract_size_1=contract_size_1,
                contract_size_2=contract_size_2,
                spec_1=spec_1,
                spec_2=spec_2,
                parallel_workers=parallel_workers,
                cancel_check=cancel_check,
                evaluate_candidate_tasks_fn=_evaluate_candidate_tasks_parallel,
                execution_cache=execution_cache,
                progress_callback=progress_callback,
                progress_stage=f"Generation {generation + 1}/{config.generations}",
                progress_total=estimated_total,
                executor=executor,
                evaluate_params_fn=_evaluate_distance_params if algorithm_name == Algorithm.DISTANCE.value else _evaluate_ols_params,
            )
            if cancelled:
                break

            ranked_population = sorted(population, key=lambda candidate: cache[candidate].objective_score, reverse=True)
            elites = ranked_population[: config.elite_count]
            next_population = list(elites)
            while len(next_population) < config.population_size:
                if _is_cancelled(cancel_check):
                    cancelled = True
                    break
                parent_left = _tournament_select(ranked_population, cache, config, rng)
                parent_right = _tournament_select(ranked_population, cache, config, rng)
                child_left, child_right = _crossover_candidates(parent_left, parent_right, config, rng)
                child_left = _mutate_candidate(child_left, search_space, config, rng)
                next_population.append(child_left)
                if len(next_population) < config.population_size:
                    next_population.append(_mutate_candidate(child_right, search_space, config, rng))
            if cancelled:
                break
            population = next_population

        if not cancelled:
            next_trial_id, cancelled = evaluate_candidates_into_cache(
                population=population,
                cache=cache,
                next_trial_id=next_trial_id,
                frame=frame,
                pair=pair,
                defaults=defaults,
                search_space=search_space,
                objective_metric=objective_metric,
                point_1=point_1,
                point_2=point_2,
                contract_size_1=contract_size_1,
                contract_size_2=contract_size_2,
                spec_1=spec_1,
                spec_2=spec_2,
                parallel_workers=parallel_workers,
                cancel_check=cancel_check,
                evaluate_candidate_tasks_fn=_evaluate_candidate_tasks_parallel,
                execution_cache=execution_cache,
                progress_callback=progress_callback,
                progress_stage="Final population",
                progress_total=estimated_total,
                executor=executor,
                evaluate_params_fn=_evaluate_distance_params if algorithm_name == Algorithm.DISTANCE.value else _evaluate_ols_params,
            )

        rows = _sort_rows(list(cache.values()))
        return DistanceOptimizationResult(
            objective_metric=objective_metric,
            evaluated_trials=len(rows),
            rows=rows,
            best_trial_id=rows[0].trial_id if rows else None,
            cancelled=cancelled,
            failure_reason=None if rows or cancelled else "no_valid_parameter_combinations",
        )


def _optimize_with_loaded_frame(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at,
    ended_at,
    objective_metric: str,
):
    frame = load_pair_frame(broker=broker, pair=pair, timeframe=timeframe, started_at=started_at, ended_at=ended_at)
    if frame.is_empty():
        return frame, None, None, DistanceOptimizationResult(objective_metric=objective_metric, evaluated_trials=0, rows=[], best_trial_id=None, failure_reason="no_aligned_quotes")
    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    return frame, spec_1, spec_2, None


def optimize_distance_grid(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at,
    ended_at,
    defaults: StrategyDefaults,
    search_space: DistanceGridSearchSpace | Mapping[str, Any],
    objective_metric: str,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    progress_callback=None,
    algorithm: str | Algorithm = Algorithm.DISTANCE,
) -> DistanceOptimizationResult:
    frame, spec_1, spec_2, empty_result = _optimize_with_loaded_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
        objective_metric=objective_metric,
    )
    if empty_result is not None:
        return empty_result
    return optimize_distance_grid_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        search_space=search_space,
        objective_metric=objective_metric,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
        cancel_check=cancel_check,
        parallel_workers=parallel_workers,
        progress_callback=progress_callback,
        algorithm=algorithm,
    )


def optimize_distance_genetic(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at,
    ended_at,
    defaults: StrategyDefaults,
    search_space: DistanceGridSearchSpace | Mapping[str, Any],
    objective_metric: str,
    config: DistanceGeneticConfig | Mapping[str, Any] | None = None,
    cancel_check: CancellationCheck | None = None,
    parallel_workers: int | None = None,
    progress_callback=None,
    algorithm: str | Algorithm = Algorithm.DISTANCE,
) -> DistanceOptimizationResult:
    frame, spec_1, spec_2, empty_result = _optimize_with_loaded_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
        objective_metric=objective_metric,
    )
    if empty_result is not None:
        return empty_result
    return optimize_distance_genetic_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        search_space=search_space,
        objective_metric=objective_metric,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
        config=config,
        cancel_check=cancel_check,
        parallel_workers=parallel_workers,
        progress_callback=progress_callback,
        algorithm=algorithm,
    )
