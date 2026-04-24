from domain.contracts import Algorithm
from domain.optimizer.distance import (
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


def optimize_ols_grid(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return optimize_distance_grid(*args, **kwargs)


def optimize_ols_grid_frame(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return optimize_distance_grid_frame(*args, **kwargs)


def optimize_ols_genetic(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return optimize_distance_genetic(*args, **kwargs)


def optimize_ols_genetic_frame(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return optimize_distance_genetic_frame(*args, **kwargs)


__all__ = [
    "DistanceGeneticConfig",
    "DistanceGridSearchSpace",
    "DistanceOptimizationResult",
    "DistanceOptimizationRow",
    "count_distance_parameter_grid",
    "iter_distance_parameter_grid",
    "optimize_ols_genetic",
    "optimize_ols_genetic_frame",
    "optimize_ols_grid",
    "optimize_ols_grid_frame",
    "parse_distance_genetic_config",
    "parse_distance_search_space",
]
