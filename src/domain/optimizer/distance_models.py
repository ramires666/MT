from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from domain.backtest.distance import DistanceParameters


OBJECTIVE_METRICS = (
    "net_profit",
    "ending_equity",
    "pnl_to_maxdd",
    "omega_ratio",
    "k_ratio",
    "score_log_trades",
    "ulcer_index",
    "ulcer_performance",
    "cagr",
    "cagr_to_ulcer",
    "r_squared",
    "calmar",
    "beauty_score",
)


@dataclass(slots=True)
class DistanceGridSearchSpace:
    lookback_bars: tuple[int, ...]
    entry_z: tuple[float, ...]
    exit_z: tuple[float, ...]
    stop_z: tuple[float | None, ...]
    bollinger_k: float


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
    cagr: float
    cagr_to_ulcer: float
    r_squared: float
    calmar: float
    beauty_score: float
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
Candidate = tuple[int, int, int, int]
DistanceTask = tuple[int, DistanceParameters]
CandidateTask = tuple[Candidate, int, DistanceParameters]


def _grid_values(raw: Any, *, cast: type[int] | type[float]) -> tuple[int, ...] | tuple[float, ...]:
    if isinstance(raw, (list, tuple)):
        return tuple(cast(item) for item in raw)
    if isinstance(raw, Mapping):
        start = cast(raw["start"])
        stop = cast(raw["stop"])
        step = cast(raw.get("step", 1 if cast is int else 0.1))
        step_value = abs(float(step))
        if step_value == 0.0:
            raise ValueError("Grid step cannot be zero.")

        values: list[int] | list[float] = []
        current = start
        ascending = float(stop) >= float(start)
        if cast is int:
            step_int = int(step_value)
            if step_int == 0:
                raise ValueError("Integer grid step cannot round down to zero.")
            if ascending:
                while current <= stop:
                    values.append(int(current))
                    current += step_int
            else:
                while current >= stop:
                    values.append(int(current))
                    current -= step_int
        else:
            epsilon = step_value / 1_000_000.0
            signed_step = step_value if ascending else -step_value
            if ascending:
                while float(current) <= float(stop) + epsilon:
                    values.append(round(float(current), 10))
                    current = cast(float(current) + signed_step)
            else:
                while float(current) >= float(stop) - epsilon:
                    values.append(round(float(current), 10))
                    current = cast(float(current) + signed_step)
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


def _fixed_float_value(raw: Any, *, fallback: float) -> float:
    if raw is None:
        return float(fallback)
    if isinstance(raw, Mapping):
        if "value" in raw:
            return float(raw["value"])
        if "start" in raw:
            return float(raw["start"])
        if "stop" in raw:
            return float(raw["stop"])
        raise TypeError(f"Unsupported fixed float mapping: {raw!r}")
    if isinstance(raw, (list, tuple)):
        for item in raw:
            if item is None or item == "":
                continue
            return float(item)
        return float(fallback)
    return float(raw)


def parse_distance_search_space(search_space: Mapping[str, Any]) -> DistanceGridSearchSpace:
    return DistanceGridSearchSpace(
        lookback_bars=tuple(int(item) for item in _grid_values(search_space["lookback_bars"], cast=int)),
        entry_z=tuple(float(item) for item in _grid_values(search_space["entry_z"], cast=float)),
        exit_z=tuple(float(item) for item in _grid_values(search_space["exit_z"], cast=float)),
        stop_z=_optional_stop_values(search_space.get("stop_z")),
        bollinger_k=_fixed_float_value(search_space.get("bollinger_k"), fallback=2.0),
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
