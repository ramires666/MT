from domain.backtest.distance import (
    TRADE_SCHEMA,
    DistanceBacktestResult,
    DistanceParameters,
    load_pair_frame,
    prepare_distance_backtest_context,
)
from domain.backtest.distance_engine import run_distance_backtest, run_distance_backtest_frame, run_distance_backtest_metrics_frame
from domain.contracts import Algorithm


def run_ols_backtest(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return run_distance_backtest(*args, **kwargs)


def run_ols_backtest_frame(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return run_distance_backtest_frame(*args, **kwargs)


def run_ols_backtest_metrics_frame(*args, **kwargs):
    kwargs["algorithm"] = Algorithm.OLS
    return run_distance_backtest_metrics_frame(*args, **kwargs)


__all__ = [
    "TRADE_SCHEMA",
    "DistanceBacktestResult",
    "DistanceParameters",
    "load_pair_frame",
    "prepare_distance_backtest_context",
    "run_ols_backtest",
    "run_ols_backtest_frame",
    "run_ols_backtest_metrics_frame",
]
