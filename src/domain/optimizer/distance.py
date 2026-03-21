from __future__ import annotations

from collections import deque
from concurrent.futures import FIRST_COMPLETED, Future, wait
from dataclasses import dataclass
from itertools import product
from math import log, sqrt
from typing import Any, Callable, Iterable, Mapping, Sequence

import numpy as np
import polars as pl

from domain.backtest.distance import (
    DistanceBacktestResult,
    DistanceParameters,
    load_pair_frame,
    run_distance_backtest_frame,
)
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec
from workers.executor import build_process_pool


OBJECTIVE_METRICS = (
    "net_profit",
    "ending_equity",
    "pnl_to_maxdd",
    "omega_ratio",
    "k_ratio",
    "score_log_trades",
    "ulcer_index",
    "ulcer_performance",
)


@dataclass(slots=True)
class DistanceGridSearchSpace:
    lookback_bars: tuple[int, ...]
    entry_z: tuple[float, ...]
    exit_z: tuple[float, ...]
    stop_z: tuple[float | None, ...]
    bollinger_k: tuple[float, ...]


@dataclass(slots=True)
class DistanceGeneticConfig:
    population_size: int = 24
    generations: int = 12
    elite_count: int = 4
    mutation_rate: float = 0.25
    crossover_rate: float = 0.70
    tournament_size: int = 3
    random_seed: int | None = None


@dataclass(slots=True)
class DistanceOptimizationRow:
    trial_id: int
    objective_metric: str
    objective_score: float
    net_profit: float
    ending_equity: float
    max_drawdown: float
    pnl_to_maxdd: float
    omega_ratio: float
    k_ratio: float
    score_log_trades: float
    ulcer_index: float
    ulcer_performance: float
    trades: int
    win_rate: float
    lookback_bars: int
    entry_z: float
    exit_z: float
    stop_z: float | None
    bollinger_k: float
    gross_profit: float = 0.0
    spread_cost: float = 0.0
    slippage_cost: float = 0.0
    commission_cost: float = 0.0
    total_cost: float = 0.0


@dataclass(slots=True)
class DistanceOptimizationResult:
    objective_metric: str
    evaluated_trials: int
    rows: list[DistanceOptimizationRow]
    best_trial_id: int | None
    cancelled: bool = False
    failure_reason: str | None = None


CancellationCheck = Callable[[], bool]
ProgressCallback = Callable[[int, int, str], None]
Candidate = tuple[int, int, int, int, int]
DistanceTask = tuple[int, DistanceParameters]
CandidateTask = tuple[Candidate, int, DistanceParameters]


def _grid_values(raw: Any, *, cast: type[int] | type[float]) -> tuple[int, ...] | tuple[float, ...]:
    if isinstance(raw, (list, tuple)):
        return tuple(cast(item) for item in raw)
    if isinstance(raw, Mapping):
        start = cast(raw["start"])
        stop = cast(raw["stop"])
        step = cast(raw.get("step", 1 if cast is int else 0.1))
        if step == 0:
            raise ValueError("Grid step cannot be zero.")

        values: list[int] | list[float] = []
        current = start
        if cast is int:
            while current <= stop:
                values.append(int(current))
                current += step
        else:
            epsilon = abs(float(step)) / 1_000_000.0
            while float(current) <= float(stop) + epsilon:
                values.append(round(float(current), 10))
                current = cast(float(current) + float(step))
        return tuple(values)
    raise TypeError(f"Unsupported grid values: {raw!r}")


def _optional_stop_values(raw: Any) -> tuple[float | None, ...]:
    if raw is None:
        return (None,)
    if isinstance(raw, (list, tuple)):
        values: list[float | None] = []
        for item in raw:
            if item is None or item == "":
                values.append(None)
            else:
                values.append(float(item))
        return tuple(values or [None])
    if isinstance(raw, Mapping):
        return tuple(float(item) for item in _grid_values(raw, cast=float))
    return (float(raw),)


def parse_distance_search_space(search_space: Mapping[str, Any]) -> DistanceGridSearchSpace:
    return DistanceGridSearchSpace(
        lookback_bars=tuple(int(item) for item in _grid_values(search_space["lookback_bars"], cast=int)),
        entry_z=tuple(float(item) for item in _grid_values(search_space["entry_z"], cast=float)),
        exit_z=tuple(float(item) for item in _grid_values(search_space["exit_z"], cast=float)),
        stop_z=_optional_stop_values(search_space.get("stop_z")),
        bollinger_k=tuple(float(item) for item in _grid_values(search_space["bollinger_k"], cast=float)),
    )


def parse_distance_genetic_config(config: Mapping[str, Any] | None = None) -> DistanceGeneticConfig:
    source = config or {}
    result = DistanceGeneticConfig(
        population_size=int(source.get("population_size", 24)),
        generations=int(source.get("generations", 12)),
        elite_count=int(source.get("elite_count", 4)),
        mutation_rate=float(source.get("mutation_rate", 0.25)),
        crossover_rate=float(source.get("crossover_rate", 0.70)),
        tournament_size=int(source.get("tournament_size", 3)),
        random_seed=int(source["random_seed"]) if source.get("random_seed") is not None else None,
    )
    if result.population_size < 2:
        raise ValueError("population_size must be >= 2")
    if result.generations < 1:
        raise ValueError("generations must be >= 1")
    if result.elite_count < 1:
        raise ValueError("elite_count must be >= 1")
    if result.elite_count >= result.population_size:
        raise ValueError("elite_count must be smaller than population_size")
    if not 0.0 <= result.mutation_rate <= 1.0:
        raise ValueError("mutation_rate must be between 0 and 1")
    if not 0.0 <= result.crossover_rate <= 1.0:
        raise ValueError("crossover_rate must be between 0 and 1")
    if result.tournament_size < 2:
        raise ValueError("tournament_size must be >= 2")
    return result


def _safe_ratio(numerator: float, denominator: float) -> float:
    if abs(denominator) <= 1e-12:
        return 0.0
    return numerator / denominator


def _compute_k_ratio(equity: np.ndarray) -> float:
    if equity.size < 3:
        return 0.0
    clipped = np.maximum(equity, 1e-9)
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


def _equity_metrics(result: DistanceBacktestResult) -> dict[str, float]:
    equity = (
        result.frame.get_column("equity_total").to_numpy()
        if not result.frame.is_empty()
        else np.asarray([], dtype=np.float64)
    )
    if equity.size == 0:
        return {
            "net_profit": 0.0,
            "ending_equity": 0.0,
            "max_drawdown": 0.0,
            "pnl_to_maxdd": 0.0,
            "omega_ratio": 0.0,
            "k_ratio": 0.0,
            "score_log_trades": 0.0,
            "ulcer_index": 0.0,
            "ulcer_performance": 0.0,
            "gross_profit": 0.0,
            "spread_cost": 0.0,
            "slippage_cost": 0.0,
            "commission_cost": 0.0,
            "total_cost": 0.0,
        }

    trades = int(result.summary.get("trades", 0) or 0)
    net_profit = float(result.summary.get("net_pnl", 0.0) or 0.0)
    ending_equity = float(result.summary.get("ending_equity", equity[-1]) or equity[-1])
    running_peak = np.maximum.accumulate(equity)
    drawdown_abs = running_peak - equity
    max_drawdown = float(drawdown_abs.max()) if drawdown_abs.size else 0.0
    pnl_to_maxdd = _safe_ratio(net_profit, max_drawdown)

    pnl_steps = np.diff(equity, prepend=equity[:1])
    gains = float(np.clip(pnl_steps, 0.0, None).sum())
    losses = float(np.clip(-pnl_steps, 0.0, None).sum())
    omega_ratio = gains if losses <= 1e-12 else gains / losses
    score_log_trades = pnl_to_maxdd * log(1.0 + max(0, trades))

    dd_pct = np.divide(drawdown_abs, running_peak, out=np.zeros_like(drawdown_abs), where=running_peak > 1e-12)
    ulcer_index = float(np.sqrt(np.mean(dd_pct**2))) if dd_pct.size else 0.0
    ulcer_performance = _safe_ratio(net_profit, ulcer_index)

    return {
        "net_profit": net_profit,
        "ending_equity": ending_equity,
        "max_drawdown": max_drawdown,
        "pnl_to_maxdd": pnl_to_maxdd,
        "omega_ratio": float(omega_ratio),
        "k_ratio": _compute_k_ratio(equity),
        "score_log_trades": float(score_log_trades),
        "ulcer_index": ulcer_index,
        "ulcer_performance": ulcer_performance,
        "gross_profit": float(result.summary.get("gross_pnl", 0.0) or 0.0),
        "spread_cost": float(result.summary.get("total_spread_cost", 0.0) or 0.0),
        "slippage_cost": float(result.summary.get("total_slippage_cost", 0.0) or 0.0),
        "commission_cost": float(result.summary.get("total_commission", 0.0) or 0.0),
        "total_cost": float(result.summary.get("total_cost", 0.0) or 0.0),
    }


def _objective_score(metric: str, metrics: Mapping[str, float]) -> float:
    if metric == "ulcer_index":
        return -float(metrics.get(metric, 0.0))
    return float(metrics.get(metric, 0.0))


def _validate_objective_metric(objective_metric: str) -> None:
    if objective_metric not in OBJECTIVE_METRICS:
        raise ValueError(f"Unsupported objective metric: {objective_metric}")


def _is_cancelled(cancel_check: CancellationCheck | None) -> bool:
    return bool(cancel_check and cancel_check())


def _emit_progress(progress_callback: ProgressCallback | None, completed: int, total: int, stage: str) -> None:
    if progress_callback is None:
        return
    progress_callback(int(completed), int(total), stage)


def _sort_rows(rows: list[DistanceOptimizationRow]) -> list[DistanceOptimizationRow]:
    return sorted(
        rows,
        key=lambda row: (
            row.objective_score,
            row.net_profit,
            -row.max_drawdown,
            row.trades,
        ),
        reverse=True,
    )


def _params_from_candidate(search_space: DistanceGridSearchSpace, candidate: Candidate) -> DistanceParameters | None:
    raw_stop = search_space.stop_z[candidate[3]]
    params = DistanceParameters(
        lookback_bars=int(search_space.lookback_bars[candidate[0]]),
        entry_z=float(search_space.entry_z[candidate[1]]),
        exit_z=float(search_space.exit_z[candidate[2]]),
        stop_z=None if raw_stop is None else float(raw_stop),
        bollinger_k=float(search_space.bollinger_k[candidate[4]]),
    )
    if params.exit_z >= params.entry_z or (params.stop_z is not None and params.stop_z <= params.entry_z):
        return None
    return params


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
) -> DistanceOptimizationRow:
    result = run_distance_backtest_frame(
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
    )
    metrics = _equity_metrics(result)
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
        score_log_trades=float(metrics["score_log_trades"]),
        ulcer_index=float(metrics["ulcer_index"]),
        ulcer_performance=float(metrics["ulcer_performance"]),
        trades=int(result.summary.get("trades", 0) or 0),
        win_rate=float(result.summary.get("win_rate", 0.0) or 0.0),
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


def _chunk_size(total_items: int, worker_count: int) -> int:
    return max(1, min(32, (total_items + (worker_count * 8) - 1) // (worker_count * 8)))


def _chunk_tasks(tasks: Sequence[Any], worker_count: int) -> list[list[Any]]:
    size = _chunk_size(len(tasks), worker_count)
    return [list(tasks[index : index + size]) for index in range(0, len(tasks), size)]


def _resolve_worker_count(parallel_workers: int | None, task_count: int) -> int:
    if task_count <= 0:
        return 1
    if parallel_workers is None:
        return 1
    return max(1, min(int(parallel_workers), task_count))


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
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
) -> list[DistanceOptimizationRow]:
    return [
        _evaluate_params(
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
    spec_1: Mapping[str, Any] | None = None,
    spec_2: Mapping[str, Any] | None = None,
) -> list[tuple[Candidate, DistanceOptimizationRow]]:
    rows: list[tuple[Candidate, DistanceOptimizationRow]] = []
    for candidate, trial_id, params in tasks:
        rows.append(
            (
                candidate,
                _evaluate_params(
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
    return rows


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
        _emit_progress(progress_callback, completed_offset, progress_total, progress_stage)
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

    _emit_progress(progress_callback, completed, progress_total, progress_stage)

    try:
        submit_available()
        while in_flight:
            if _is_cancelled(cancel_check):
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
                _emit_progress(progress_callback, completed, progress_total, progress_stage)
            submit_available()
    finally:
        executor.shutdown(wait=not cancelled, cancel_futures=cancelled)

    return results, cancelled


def _evaluate_params_parallel(
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
    progress_callback: ProgressCallback | None = None,
    progress_stage: str = "Grid search",
) -> tuple[list[DistanceOptimizationRow], bool]:
    worker_count = _resolve_worker_count(parallel_workers, len(tasks))
    total_tasks = len(tasks)
    if worker_count <= 1:
        rows: list[DistanceOptimizationRow] = []
        cancelled = False
        _emit_progress(progress_callback, 0, total_tasks, progress_stage)
        for trial_id, params in tasks:
            if _is_cancelled(cancel_check):
                cancelled = True
                break
            rows.append(
                _evaluate_params(
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
            _emit_progress(progress_callback, len(rows), total_tasks, progress_stage)
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


def _evaluate_candidate_tasks_parallel(
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
    progress_callback: ProgressCallback | None = None,
    progress_total: int = 0,
    progress_stage: str = "Genetic search",
    completed_offset: int = 0,
) -> tuple[list[tuple[Candidate, DistanceOptimizationRow]], bool]:
    worker_count = _resolve_worker_count(parallel_workers, len(tasks))
    if worker_count <= 1:
        rows: list[tuple[Candidate, DistanceOptimizationRow]] = []
        cancelled = False
        _emit_progress(progress_callback, completed_offset, progress_total, progress_stage)
        for candidate, trial_id, params in tasks:
            if _is_cancelled(cancel_check):
                cancelled = True
                break
            rows.append(
                (
                    candidate,
                    _evaluate_params(
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
            _emit_progress(progress_callback, completed_offset + len(rows), progress_total, progress_stage)
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


def iter_distance_parameter_grid(search_space: DistanceGridSearchSpace) -> Iterable[DistanceParameters]:
    for lookback_bars, entry_z, exit_z, stop_z, bollinger_k in product(
        search_space.lookback_bars,
        search_space.entry_z,
        search_space.exit_z,
        search_space.stop_z,
        search_space.bollinger_k,
    ):
        if exit_z >= entry_z or (stop_z is not None and stop_z <= entry_z):
            continue
        yield DistanceParameters(
            lookback_bars=int(lookback_bars),
            entry_z=float(entry_z),
            exit_z=float(exit_z),
            stop_z=None if stop_z is None else float(stop_z),
            bollinger_k=float(bollinger_k),
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
    progress_callback: ProgressCallback | None = None,
) -> DistanceOptimizationResult:
    _validate_objective_metric(objective_metric)

    if isinstance(search_space, Mapping):
        search_space = parse_distance_search_space(search_space)

    task_items = list(enumerate(iter_distance_parameter_grid(search_space), start=1))
    rows, cancelled = _evaluate_params_parallel(
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
    )

    rows = _sort_rows(rows)
    best_trial_id = rows[0].trial_id if rows else None
    return DistanceOptimizationResult(
        objective_metric=objective_metric,
        evaluated_trials=len(rows),
        rows=rows,
        best_trial_id=best_trial_id,
        cancelled=cancelled,
        failure_reason=None if rows or cancelled else "no_valid_parameter_combinations",
    )


def _random_candidate(search_space: DistanceGridSearchSpace, rng: np.random.Generator) -> Candidate:
    max_attempts = 256
    for _ in range(max_attempts):
        candidate: Candidate = (
            int(rng.integers(0, len(search_space.lookback_bars))),
            int(rng.integers(0, len(search_space.entry_z))),
            int(rng.integers(0, len(search_space.exit_z))),
            int(rng.integers(0, len(search_space.stop_z))),
            int(rng.integers(0, len(search_space.bollinger_k))),
        )
        if _params_from_candidate(search_space, candidate) is not None:
            return candidate
    raise ValueError("No valid parameter combinations available for genetic optimization.")


def _mutate_candidate(
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
    if _params_from_candidate(search_space, mutated) is not None:
        return mutated
    return _random_candidate(search_space, rng)


def _crossover_candidates(
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


def _tournament_select(
    population: list[Candidate],
    cache: Mapping[Candidate, DistanceOptimizationRow],
    config: DistanceGeneticConfig,
    rng: np.random.Generator,
) -> Candidate:
    indices = [int(rng.integers(0, len(population))) for _ in range(min(config.tournament_size, len(population)))]
    candidates = [population[index] for index in indices]
    return max(candidates, key=lambda candidate: cache[candidate].objective_score)


def _evaluate_candidates_into_cache(
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
    spec_1: Mapping[str, Any] | None,
    spec_2: Mapping[str, Any] | None,
    parallel_workers: int | None,
    cancel_check: CancellationCheck | None,
    progress_callback: ProgressCallback | None = None,
    progress_stage: str = "Genetic search",
    progress_total: int = 0,
) -> tuple[int, bool]:
    task_items: list[CandidateTask] = []
    seen: set[Candidate] = set()
    trial_id = next_trial_id
    for candidate in population:
        if candidate in seen or candidate in cache:
            continue
        seen.add(candidate)
        params = _params_from_candidate(search_space, candidate)
        if params is None:
            continue
        task_items.append((candidate, trial_id, params))
        trial_id += 1

    results, cancelled = _evaluate_candidate_tasks_parallel(
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
    progress_callback: ProgressCallback | None = None,
) -> DistanceOptimizationResult:
    _validate_objective_metric(objective_metric)

    if isinstance(search_space, Mapping):
        search_space = parse_distance_search_space(search_space)
    if not isinstance(config, DistanceGeneticConfig):
        config = parse_distance_genetic_config(config)

    rng = np.random.default_rng(config.random_seed)
    cache: dict[Candidate, DistanceOptimizationRow] = {}
    next_trial_id = 1
    cancelled = False
    estimated_total = config.population_size * (config.generations + 1)

    population = [_random_candidate(search_space, rng) for _ in range(config.population_size)]
    _emit_progress(progress_callback, 0, estimated_total, "Genetic search")
    for _generation in range(config.generations):
        if _is_cancelled(cancel_check):
            cancelled = True
            break

        next_trial_id, cancelled = _evaluate_candidates_into_cache(
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
            progress_callback=progress_callback,
            progress_stage=f"Generation {_generation + 1}/{config.generations}",
            progress_total=estimated_total,
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
                child_right = _mutate_candidate(child_right, search_space, config, rng)
                next_population.append(child_right)
        if cancelled:
            break
        population = next_population

    if not cancelled:
        next_trial_id, cancelled = _evaluate_candidates_into_cache(
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
            progress_callback=progress_callback,
            progress_stage="Final population",
            progress_total=estimated_total,
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
    progress_callback: ProgressCallback | None = None,
) -> DistanceOptimizationResult:
    frame = load_pair_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
    )
    if frame.is_empty():
        return DistanceOptimizationResult(
            objective_metric=objective_metric,
            evaluated_trials=0,
            rows=[],
            best_trial_id=None,
            failure_reason="no_aligned_quotes",
        )

    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
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
    progress_callback: ProgressCallback | None = None,
) -> DistanceOptimizationResult:
    frame = load_pair_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
    )
    if frame.is_empty():
        return DistanceOptimizationResult(
            objective_metric=objective_metric,
            evaluated_trials=0,
            rows=[],
            best_trial_id=None,
            failure_reason="no_aligned_quotes",
        )

    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
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
    )
