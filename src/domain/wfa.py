from __future__ import annotations

from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Mapping, Sequence

import polars as pl

from domain.backtest.distance import DistanceParameters, run_distance_backtest_frame
from domain.contracts import (
    Algorithm,
    PairSelection,
    StrategyDefaults,
    Timeframe,
    WfaMode,
    WfaRequest,
    WfaWindowUnit,
)
from domain.data.io import load_instrument_spec
from domain.optimizer.distance import (
    _equity_metrics,
    _objective_score,
    _validate_objective_metric,
    iter_distance_parameter_grid,
    load_pair_frame,
    optimize_distance_grid_frame,
    parse_distance_search_space,
)


@dataclass(slots=True)
class WalkWindow:
    index: int
    train_started_at: datetime
    train_ended_at: datetime
    validation_started_at: datetime
    validation_ended_at: datetime
    test_started_at: datetime
    test_ended_at: datetime


BAR_MINUTES = {
    Timeframe.M5: 5,
    Timeframe.M15: 15,
    Timeframe.M30: 30,
    Timeframe.H1: 60,
    Timeframe.H4: 240,
    Timeframe.D1: 1440,
}


def _add_months(moment: datetime, months: int) -> datetime:
    month_index = (moment.month - 1) + int(months)
    year = moment.year + month_index // 12
    month = (month_index % 12) + 1
    day = min(moment.day, monthrange(year, month)[1])
    return moment.replace(year=year, month=month, day=day)


def _advance(moment: datetime, units: int, unit: WfaWindowUnit, timeframe: Timeframe) -> datetime:
    units = int(units)
    if unit == WfaWindowUnit.WEEKS:
        return moment + timedelta(weeks=units)
    if unit == WfaWindowUnit.MONTHS:
        return _add_months(moment, units)
    if unit == WfaWindowUnit.BARS:
        return moment + timedelta(minutes=BAR_MINUTES[timeframe] * units)
    raise ValueError(f"Unsupported WFA window unit: {unit}")


def build_walk_windows(
    *,
    started_at: datetime,
    ended_at: datetime,
    mode: WfaMode,
    timeframe: Timeframe,
    train_units: int,
    validation_units: int,
    test_units: int,
    walk_step_units: int,
    train_unit: WfaWindowUnit,
    validation_unit: WfaWindowUnit,
    test_unit: WfaWindowUnit,
    walk_step_unit: WfaWindowUnit,
) -> list[WalkWindow]:
    if min(train_units, validation_units, test_units, walk_step_units) <= 0:
        return []

    windows: list[WalkWindow] = []
    if mode == WfaMode.ANCHORED:
        train_start = started_at
        train_end = _advance(train_start, train_units, train_unit, timeframe)
        index = 0
        while True:
            validation_start = train_end
            validation_end = _advance(validation_start, validation_units, validation_unit, timeframe)
            test_start = validation_end
            test_end = _advance(test_start, test_units, test_unit, timeframe)
            if test_end > ended_at:
                break
            windows.append(
                WalkWindow(
                    index=index,
                    train_started_at=train_start,
                    train_ended_at=train_end,
                    validation_started_at=validation_start,
                    validation_ended_at=validation_end,
                    test_started_at=test_start,
                    test_ended_at=test_end,
                )
            )
            train_end = _advance(train_end, walk_step_units, walk_step_unit, timeframe)
            index += 1
        return windows

    train_start = started_at
    index = 0
    while True:
        train_end = _advance(train_start, train_units, train_unit, timeframe)
        validation_start = train_end
        validation_end = _advance(validation_start, validation_units, validation_unit, timeframe)
        test_start = validation_end
        test_end = _advance(test_start, test_units, test_unit, timeframe)
        if test_end > ended_at:
            break
        windows.append(
            WalkWindow(
                index=index,
                train_started_at=train_start,
                train_ended_at=train_end,
                validation_started_at=validation_start,
                validation_ended_at=validation_end,
                test_started_at=test_start,
                test_ended_at=test_end,
            )
        )
        train_start = _advance(train_start, walk_step_units, walk_step_unit, timeframe)
        index += 1
    return windows


def _slice_frame(frame: pl.DataFrame, started_at: datetime, ended_at: datetime) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.filter((pl.col("time") >= started_at) & (pl.col("time") <= ended_at)).sort("time")


def _distance_params_from_payload(payload: Mapping[str, Any]) -> DistanceParameters:
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


def _serialize_time(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def _serialize_pair(pair: PairSelection) -> dict[str, Any]:
    return pair.model_dump(mode="json")


def _candidate_params(
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

    params = _distance_params_from_payload(algorithm_params)
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


def _evaluate_distance_params(
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


def _stitch_pair_oos_equity(chunks: Sequence[dict[str, Any]], initial_capital: float) -> list[dict[str, Any]]:
    stitched: list[dict[str, Any]] = []
    current_equity = float(initial_capital)
    for chunk in chunks:
        frame = chunk["test_result"].frame
        if frame.is_empty():
            continue
        times = frame.get_column("time").to_list()
        equities = frame.get_column("equity_total").to_list()
        if not times:
            continue
        for time_value, equity_value in zip(times, equities):
            adjusted = current_equity + (float(equity_value) - float(initial_capital))
            stitched.append({"time": _serialize_time(time_value), "equity": round(adjusted, 6)})
        current_equity = stitched[-1]["equity"]
    return stitched


def _combine_pair_equity_series(pair_series: Sequence[list[dict[str, Any]]], initial_capital: float) -> list[dict[str, Any]]:
    if not pair_series:
        return []
    event_map: dict[str, list[tuple[int, float]]] = {}
    for pair_index, series in enumerate(pair_series):
        for point in series:
            event_map.setdefault(str(point["time"]), []).append((pair_index, float(point["equity"])))
    current = [float(initial_capital)] * len(pair_series)
    aggregate: list[dict[str, Any]] = []
    for time_key in sorted(event_map):
        for pair_index, equity in event_map[time_key]:
            current[pair_index] = equity
        aggregate.append({"time": time_key, "equity": round(sum(current), 6)})
    return aggregate


def _run_pair_window_trial(
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
        train_frame = _slice_frame(base_frame, window.train_started_at, window.train_ended_at)
        validation_frame = _slice_frame(base_frame, window.validation_started_at, window.validation_ended_at)
        test_frame = _slice_frame(base_frame, window.test_started_at, window.test_ended_at)
        if train_frame.is_empty() or validation_frame.is_empty() or test_frame.is_empty():
            continue

        candidates = _candidate_params(
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
            validation_eval = _evaluate_distance_params(
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

        test_eval = _evaluate_distance_params(
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
                "train_started_at": _serialize_time(window.train_started_at),
                "train_ended_at": _serialize_time(window.train_ended_at),
                "validation_started_at": _serialize_time(window.validation_started_at),
                "validation_ended_at": _serialize_time(window.validation_ended_at),
                "test_started_at": _serialize_time(window.test_started_at),
                "test_ended_at": _serialize_time(window.test_ended_at),
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

    stitched_equity = _stitch_pair_oos_equity(chunk_results, defaults.initial_capital)
    sanitized_chunks = []
    for chunk in chunk_results:
        sanitized = dict(chunk)
        sanitized.pop("test_result", None)
        sanitized_chunks.append(sanitized)

    chunk_count = len(chunk_results)
    return {
        "pair": _serialize_pair(pair),
        "chunk_count": chunk_count,
        "validation_objective_score": round(validation_score_total / chunk_count, 6),
        "oos_net_profit": round(total_net_profit, 6),
        "total_trades": int(total_trades),
        "total_commission": round(total_commission, 6),
        "total_cost": round(total_cost, 6),
        "oos_equity": stitched_equity,
        "chunks": sanitized_chunks,
    }


def run_wfa_request(broker: str, request: WfaRequest) -> dict[str, Any]:
    if request.algorithm != Algorithm.DISTANCE:
        raise ValueError("Only distance WFA is implemented right now.")
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
                        pair_result = _run_pair_window_trial(
                            broker=broker,
                            pair=pair,
                            timeframe=request.timeframe,
                            defaults=request.defaults,
                            objective_metric=request.objective_metric,
                            algorithm_params=request.algorithm_params,
                            parameter_search_space=request.parameter_search_space,
                            base_frame=base_frames[pair_key],
                            windows=windows,
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
                    aggregate_equity = _combine_pair_equity_series(pair_equity_series, request.defaults.initial_capital)
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
