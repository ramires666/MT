from __future__ import annotations

from typing import Any, Mapping, Sequence

import polars as pl

from domain.backtest.distance import DistanceParameters, run_distance_backtest_frame
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec
from domain.optimizer.distance import (
    _equity_metrics,
    _objective_score,
    optimize_distance_grid_frame,
)
from domain.wfa_serialization import serialize_pair, serialize_time, slice_frame, stitch_pair_oos_equity
from domain.wfa_windowing import WalkWindow


def distance_params_from_payload(payload: Mapping[str, Any]) -> DistanceParameters:
    raw_stop = payload.get("stop_z", 3.5)
    if payload.get("stop_enabled") is False:
        raw_stop = None
    return DistanceParameters(
        lookback_bars=int(payload.get("lookback_bars", 96) or 96),
        entry_z=float(payload.get("entry_z", 2.0) or 2.0),
        exit_z=float(payload.get("exit_z", 0.5) or 0.5),
        stop_z=None if raw_stop in {None, ""} else float(raw_stop),
        bollinger_k=float(payload.get("bollinger_k", 2.0) or 2.0),
    )


def candidate_params(
    *,
    train_frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    objective_metric: str,
    parameter_search_space: Mapping[str, Any],
    algorithm_params: Mapping[str, Any],
    spec_1: Mapping[str, Any],
    spec_2: Mapping[str, Any],
) -> list[tuple[DistanceParameters, float]]:
    if parameter_search_space:
        optimization = optimize_distance_grid_frame(
            frame=train_frame,
            pair=pair,
            defaults=defaults,
            search_space=parameter_search_space,
            objective_metric=objective_metric,
            point_1=float(spec_1.get("point", 0.0) or 0.0),
            point_2=float(spec_2.get("point", 0.0) or 0.0),
            contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
            contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
            spec_1=spec_1,
            spec_2=spec_2,
        )
        return [
            (
                DistanceParameters(
                    lookback_bars=row.lookback_bars,
                    entry_z=row.entry_z,
                    exit_z=row.exit_z,
                    stop_z=row.stop_z,
                    bollinger_k=row.bollinger_k,
                ),
                float(row.objective_score),
            )
            for row in optimization.rows
        ]

    params = distance_params_from_payload(algorithm_params)
    train_result = run_distance_backtest_frame(
        frame=train_frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
    )
    train_score = _objective_score(objective_metric, _equity_metrics(train_result))
    return [(params, train_score)]


def evaluate_distance_params(
    *,
    frame: pl.DataFrame,
    pair: PairSelection,
    defaults: StrategyDefaults,
    params: DistanceParameters,
    spec_1: Mapping[str, Any],
    spec_2: Mapping[str, Any],
) -> dict[str, Any]:
    result = run_distance_backtest_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
    )
    return {"result": result, "metrics": _equity_metrics(result)}


def run_pair_window_trial(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    defaults: StrategyDefaults,
    objective_metric: str,
    algorithm_params: Mapping[str, Any],
    parameter_search_space: Mapping[str, Any],
    base_frame: pl.DataFrame,
    windows: Sequence[WalkWindow],
) -> dict[str, Any] | None:
    if base_frame.is_empty():
        return None
    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    chunk_results: list[dict[str, Any]] = []
    validation_score_total = 0.0
    total_trades = 0
    total_commission = 0.0
    total_cost = 0.0
    total_net_profit = 0.0

    for window in windows:
        train_frame = slice_frame(base_frame, window.train_started_at, window.train_ended_at)
        validation_frame = slice_frame(base_frame, window.validation_started_at, window.validation_ended_at)
        test_frame = slice_frame(base_frame, window.test_started_at, window.test_ended_at)
        if train_frame.is_empty() or validation_frame.is_empty() or test_frame.is_empty():
            continue

        candidates = candidate_params(
            train_frame=train_frame,
            pair=pair,
            defaults=defaults,
            objective_metric=objective_metric,
            parameter_search_space=parameter_search_space,
            algorithm_params=algorithm_params,
            spec_1=spec_1,
            spec_2=spec_2,
        )
        if not candidates:
            continue

        best_choice: dict[str, Any] | None = None
        for params, train_score in candidates:
            validation_eval = evaluate_distance_params(
                frame=validation_frame,
                pair=pair,
                defaults=defaults,
                params=params,
                spec_1=spec_1,
                spec_2=spec_2,
            )
            validation_score = _objective_score(objective_metric, validation_eval["metrics"])
            candidate = {
                "params": params,
                "train_score": float(train_score),
                "validation_score": float(validation_score),
                "validation_metrics": validation_eval["metrics"],
            }
            if best_choice is None:
                best_choice = candidate
                continue
            if candidate["validation_score"] > best_choice["validation_score"]:
                best_choice = candidate
            elif candidate["validation_score"] == best_choice["validation_score"] and float(candidate["validation_metrics"].get("net_profit", 0.0) or 0.0) > float(best_choice["validation_metrics"].get("net_profit", 0.0) or 0.0):
                best_choice = candidate

        if best_choice is None:
            continue

        test_eval = evaluate_distance_params(
            frame=test_frame,
            pair=pair,
            defaults=defaults,
            params=best_choice["params"],
            spec_1=spec_1,
            spec_2=spec_2,
        )
        test_summary = test_eval["result"].summary
        validation_score_total += float(best_choice["validation_score"])
        total_trades += int(test_summary.get("trades", 0) or 0)
        total_commission += float(test_summary.get("total_commission", 0.0) or 0.0)
        total_cost += float(test_summary.get("total_cost", 0.0) or 0.0)
        total_net_profit += float(test_summary.get("net_pnl", 0.0) or 0.0)
        chunk_results.append(
            {
                "window_index": window.index,
                "train_started_at": serialize_time(window.train_started_at),
                "train_ended_at": serialize_time(window.train_ended_at),
                "validation_started_at": serialize_time(window.validation_started_at),
                "validation_ended_at": serialize_time(window.validation_ended_at),
                "test_started_at": serialize_time(window.test_started_at),
                "test_ended_at": serialize_time(window.test_ended_at),
                "selected_params": {
                    "lookback_bars": best_choice["params"].lookback_bars,
                    "entry_z": best_choice["params"].entry_z,
                    "exit_z": best_choice["params"].exit_z,
                    "stop_z": best_choice["params"].stop_z,
                    "bollinger_k": best_choice["params"].bollinger_k,
                },
                "train_objective_score": round(float(best_choice["train_score"]), 6),
                "validation_objective_score": round(float(best_choice["validation_score"]), 6),
                "test_objective_score": round(float(_objective_score(objective_metric, test_eval["metrics"])), 6),
                "test_net_profit": round(float(test_summary.get("net_pnl", 0.0) or 0.0), 6),
                "test_gross_profit": round(float(test_summary.get("gross_pnl", 0.0) or 0.0), 6),
                "test_spread_cost": round(float(test_summary.get("total_spread_cost", 0.0) or 0.0), 6),
                "test_slippage_cost": round(float(test_summary.get("total_slippage_cost", 0.0) or 0.0), 6),
                "test_commission_cost": round(float(test_summary.get("total_commission", 0.0) or 0.0), 6),
                "test_total_cost": round(float(test_summary.get("total_cost", 0.0) or 0.0), 6),
                "test_trades": int(test_summary.get("trades", 0) or 0),
                "test_ending_equity": round(float(test_summary.get("ending_equity", defaults.initial_capital) or defaults.initial_capital), 6),
                "test_max_drawdown": round(float(test_summary.get("max_drawdown", 0.0) or 0.0), 6),
                "test_result": test_eval["result"],
            }
        )

    if not chunk_results:
        return None

    stitched_equity = stitch_pair_oos_equity(chunk_results, defaults.initial_capital)
    sanitized_chunks = []
    for chunk in chunk_results:
        sanitized = dict(chunk)
        sanitized.pop("test_result", None)
        sanitized_chunks.append(sanitized)

    chunk_count = len(chunk_results)
    return {
        "pair": serialize_pair(pair),
        "chunk_count": chunk_count,
        "validation_objective_score": round(validation_score_total / chunk_count, 6),
        "oos_net_profit": round(total_net_profit, 6),
        "total_trades": int(total_trades),
        "total_commission": round(total_commission, 6),
        "total_cost": round(total_cost, 6),
        "oos_equity": stitched_equity,
        "chunks": sanitized_chunks,
    }
