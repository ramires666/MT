"""Grid and genetic optimizer orchestration."""

from domain.optimizer.distance import (
    OBJECTIVE_METRICS,
    DistanceGeneticConfig,
    DistanceGridSearchSpace,
    DistanceOptimizationResult,
    DistanceOptimizationRow,
    count_distance_parameter_grid,
    iter_distance_parameter_grid,
    optimize_distance_genetic,
    optimize_distance_genetic_frame,
    optimize_distance_grid,
    optimize_distance_grid_frame,
    parse_distance_genetic_config,
    parse_distance_search_space,
)
from domain.optimizer.distance_grid import (
    DEFAULT_DISTANCE_GRID,
    DistanceOptimizationResult as LegacyDistanceOptimizationResult,
    DistanceOptimizationTrial,
    normalize_distance_search_space,
    run_distance_grid_search,
    run_distance_grid_search_frame,
)

__all__ = [
    "DEFAULT_DISTANCE_GRID",
    "LegacyDistanceOptimizationResult",
    "OBJECTIVE_METRICS",
    "DistanceGeneticConfig",
    "DistanceGridSearchSpace",
    "DistanceOptimizationResult",
    "DistanceOptimizationRow",
    "DistanceOptimizationTrial",
    "count_distance_parameter_grid",
    "iter_distance_parameter_grid",
    "normalize_distance_search_space",
    "optimize_distance_genetic",
    "optimize_distance_genetic_frame",
    "optimize_distance_grid",
    "optimize_distance_grid_frame",
    "parse_distance_genetic_config",
    "parse_distance_search_space",
    "run_distance_grid_search",
    "run_distance_grid_search_frame",
]
