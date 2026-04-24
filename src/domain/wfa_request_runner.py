from __future__ import annotations

from typing import Any

from domain.backtest.distance import load_pair_frame
from domain.contracts import Algorithm, WfaRequest
from domain.optimizer.distance import _validate_objective_metric
from domain.wfa_evaluation import run_pair_window_trial
from domain.wfa_serialization import combine_pair_equity_series
from domain.wfa_windowing import build_walk_windows


def run_wfa_request(broker: str, request: WfaRequest) -> dict[str, Any]:
    if request.algorithm not in {Algorithm.DISTANCE, Algorithm.OLS}:
        raise ValueError("Only distance and OLS WFA are implemented right now.")
    _validate_objective_metric(request.objective_metric)

    window_search = request.window_search
    train_values = range(window_search.train.start, window_search.train.stop + 1, window_search.train.step)
    validation_values = range(window_search.validation.start, window_search.validation.stop + 1, window_search.validation.step)
    test_values = range(window_search.test.start, window_search.test.stop + 1, window_search.test.step)
    walk_step_values = range(window_search.walk_step.start, window_search.walk_step.stop + 1, window_search.walk_step.step)

    base_frames = {
        f"{pair.symbol_1}::{pair.symbol_2}": load_pair_frame(
            broker=broker,
            pair=pair,
            timeframe=request.timeframe,
            started_at=request.started_at,
            ended_at=request.ended_at,
        )
        for pair in request.pairs
    }

    trial_summaries: list[dict[str, Any]] = []
    best_trial_detail: dict[str, Any] | None = None
    best_score: float | None = None
    trial_id = 1

    for train_units in train_values:
        for validation_units in validation_values:
            for test_units in test_values:
                for walk_step_units in walk_step_values:
                    windows = build_walk_windows(
                        started_at=request.started_at,
                        ended_at=request.ended_at,
                        mode=request.wfa_mode,
                        timeframe=request.timeframe,
                        train_units=train_units,
                        validation_units=validation_units,
                        test_units=test_units,
                        walk_step_units=walk_step_units,
                        train_unit=window_search.resolved_train_unit(),
                        validation_unit=window_search.resolved_validation_unit(),
                        test_unit=window_search.resolved_test_unit(),
                        walk_step_unit=window_search.resolved_walk_step_unit(),
                    )
                    if not windows:
                        trial_id += 1
                        continue

                    pair_results: list[dict[str, Any]] = []
                    pair_equity_series: list[list[dict[str, Any]]] = []
                    total_chunks = 0
                    total_net_profit = 0.0
                    total_commission = 0.0
                    total_cost = 0.0
                    total_trades = 0
                    validation_scores: list[float] = []

                    for pair in request.pairs:
                        pair_key = f"{pair.symbol_1}::{pair.symbol_2}"
                        pair_result = run_pair_window_trial(
                            broker=broker,
                            pair=pair,
                            timeframe=request.timeframe,
                            defaults=request.defaults,
                            objective_metric=request.objective_metric,
                            algorithm_params=request.algorithm_params,
                            parameter_search_space=request.parameter_search_space,
                            base_frame=base_frames[pair_key],
                            windows=windows,
                            algorithm=request.algorithm,
                        )
                        if pair_result is None:
                            continue
                        pair_results.append(pair_result)
                        pair_equity_series.append(pair_result["oos_equity"])
                        total_chunks += int(pair_result["chunk_count"])
                        total_net_profit += float(pair_result["oos_net_profit"])
                        total_commission += float(pair_result["total_commission"])
                        total_cost += float(pair_result["total_cost"])
                        total_trades += int(pair_result["total_trades"])
                        validation_scores.append(float(pair_result["validation_objective_score"]))

                    if not pair_results:
                        trial_id += 1
                        continue

                    objective_score = sum(validation_scores) / len(validation_scores) if validation_scores else 0.0
                    aggregate_equity = combine_pair_equity_series(pair_equity_series, request.defaults.initial_capital)
                    summary = {
                        "trial_id": trial_id,
                        "mode": request.wfa_mode.value,
                        "train_units": train_units,
                        "validation_units": validation_units,
                        "test_units": test_units,
                        "walk_step_units": walk_step_units,
                        "window_count": len(windows),
                        "evaluated_chunk_count": total_chunks,
                        "pair_count": len(pair_results),
                        "objective_score": round(objective_score, 6),
                        "aggregate_oos_net_profit": round(total_net_profit, 6),
                        "aggregate_total_commission": round(total_commission, 6),
                        "aggregate_total_cost": round(total_cost, 6),
                        "aggregate_total_trades": int(total_trades),
                    }
                    trial_summaries.append(summary)
                    if best_score is None or objective_score > best_score:
                        best_score = objective_score
                        best_trial_detail = {
                            **summary,
                            "aggregate_equity": aggregate_equity,
                            "pair_results": pair_results,
                        }
                    trial_id += 1

    trial_summaries.sort(
        key=lambda row: (
            float(row["objective_score"]),
            float(row["aggregate_oos_net_profit"]),
            -float(row["aggregate_total_cost"]),
        ),
        reverse=True,
    )

    return {
        "status": "completed",
        "algorithm": request.algorithm.value,
        "mode": request.wfa_mode.value,
        "pair_mode": request.pair_mode.value,
        "pair_count": len(request.pairs),
        "selection_source": request.selection_source.value,
        "objective_metric": request.objective_metric,
        "trial_count": len(trial_summaries),
        "window_trials": trial_summaries,
        "best_trial": best_trial_detail,
        "failure_reason": None if trial_summaries else "no_wfa_trials",
    }
