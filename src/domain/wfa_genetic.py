from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Callable, Mapping
from uuid import uuid4

from domain.backtest.distance import DistanceParameters, load_pair_frame
from domain.contracts import PairSelection, StrategyDefaults, Timeframe, WfaWindowUnit
from domain.data.io import load_instrument_spec
from domain.optimizer.distance import _objective_score, _validate_objective_metric, optimize_distance_genetic_frame
from domain.wfa_evaluation import evaluate_distance_params
from domain.wfa_serialization import build_fold_history_rows, serialize_pair, serialize_time, slice_frame, stitch_pair_oos_equity
from domain.wfa_windowing import build_train_test_windows
from storage.wfa_results import persist_wfa_optimization_history, persist_wfa_run_snapshot


WfaProgressCallback = Callable[[int, int, str], None]
WfaPartialResultCallback = Callable[[dict[str, Any]], None]


def _empty_wfa_result(
    *,
    pair: PairSelection,
    objective_metric: str,
    started_at: datetime,
    ended_at: datetime,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
    failure_reason: str,
) -> dict[str, Any]:
    return {
        "status": "completed",
        "pair": serialize_pair(pair),
        "objective_metric": objective_metric,
        "started_at": serialize_time(started_at),
        "ended_at": serialize_time(ended_at),
        "lookback_units": int(lookback_units),
        "test_units": int(test_units),
        "step_units": int(step_units),
        "unit": unit.value,
        "fold_count": 0,
        "folds": [],
        "stitched_equity": [],
        "failure_reason": failure_reason,
    }


def run_distance_genetic_wfa(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    defaults: StrategyDefaults,
    objective_metric: str,
    parameter_search_space: Mapping[str, Any],
    genetic_config: Mapping[str, Any] | None,
    lookback_units: int,
    test_units: int,
    step_units: int | None = None,
    unit: WfaWindowUnit = WfaWindowUnit.WEEKS,
    parallel_workers: int | None = None,
    progress_callback: WfaProgressCallback | None = None,
    partial_result_callback: WfaPartialResultCallback | None = None,
) -> dict[str, Any]:
    _validate_objective_metric(objective_metric)
    effective_step_units = int(step_units or test_units)
    windows = build_train_test_windows(
        started_at=started_at,
        ended_at=ended_at,
        timeframe=timeframe,
        lookback_units=int(lookback_units),
        test_units=int(test_units),
        step_units=effective_step_units,
        unit=unit,
    )
    if not windows:
        return _empty_wfa_result(
            pair=pair,
            objective_metric=objective_metric,
            started_at=started_at,
            ended_at=ended_at,
            lookback_units=lookback_units,
            test_units=test_units,
            step_units=effective_step_units,
            unit=unit,
            failure_reason="no_wfa_windows",
        )

    base_frame = load_pair_frame(
        broker=broker,
        pair=pair,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
    )
    if base_frame.is_empty():
        return _empty_wfa_result(
            pair=pair,
            objective_metric=objective_metric,
            started_at=started_at,
            ended_at=ended_at,
            lookback_units=lookback_units,
            test_units=test_units,
            step_units=effective_step_units,
            unit=unit,
            failure_reason="no_aligned_quotes",
        )

    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    fold_rows: list[dict[str, Any]] = []
    stitched_chunks: list[dict[str, Any]] = []
    total_windows = len(windows)
    wfa_run_id = f"{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}_{uuid4().hex[:8]}"
    history_rows_written = 0
    history_path = None

    for position, window in enumerate(windows, start=1):
        if progress_callback is not None:
            progress_callback(position - 1, total_windows, f"WFA fold {position}/{total_windows}")
        train_frame = slice_frame(base_frame, window.train_started_at, window.train_ended_at)
        test_frame = slice_frame(base_frame, window.test_started_at, window.test_ended_at)
        if train_frame.is_empty() or test_frame.is_empty():
            continue

        optimization = optimize_distance_genetic_frame(
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
            config=genetic_config,
            parallel_workers=parallel_workers,
        )
        if not optimization.rows:
            continue

        fold_history_rows = build_fold_history_rows(
            optimization_rows=optimization.rows,
            test_frame=test_frame,
            pair=pair,
            broker=broker,
            timeframe=timeframe,
            defaults=defaults,
            objective_metric=objective_metric,
            spec_1=spec_1,
            spec_2=spec_2,
            window=window,
            wfa_run_id=wfa_run_id,
            lookback_units=int(lookback_units),
            test_units=int(test_units),
            step_units=int(effective_step_units),
            unit=unit,
            parallel_workers=parallel_workers,
        )
        if fold_history_rows:
            persisted = persist_wfa_optimization_history(
                broker=broker,
                pair=pair,
                timeframe=timeframe,
                rows=fold_history_rows,
            )
            history_path = str(persisted) if persisted is not None else history_path
            history_rows_written += len(fold_history_rows)

        best = optimization.rows[0]
        selected_params = DistanceParameters(
            lookback_bars=best.lookback_bars,
            entry_z=best.entry_z,
            exit_z=best.exit_z,
            stop_z=best.stop_z,
            bollinger_k=best.bollinger_k,
        )
        test_eval = evaluate_distance_params(
            frame=test_frame,
            pair=pair,
            defaults=defaults,
            params=selected_params,
            spec_1=spec_1,
            spec_2=spec_2,
        )
        test_metrics = test_eval["metrics"]
        test_summary = test_eval["result"].summary
        fold_rows.append(
            {
                "fold": int(window.index + 1),
                "train_started_at": serialize_time(window.train_started_at),
                "train_ended_at": serialize_time(window.train_ended_at),
                "test_started_at": serialize_time(window.test_started_at),
                "test_ended_at": serialize_time(window.test_ended_at),
                "lookback_bars": int(best.lookback_bars),
                "entry_z": float(best.entry_z),
                "exit_z": float(best.exit_z),
                "stop_z": best.stop_z,
                "bollinger_k": float(best.bollinger_k),
                "train_score": round(float(best.score_log_trades), 6),
                "train_net_profit": round(float(best.net_profit), 6),
                "train_max_drawdown": round(float(best.max_drawdown), 6),
                "train_trades": int(best.trades),
                "test_score": round(float(_objective_score(objective_metric, test_metrics)), 6),
                "test_net_profit": round(float(test_summary.get("net_pnl", 0.0) or 0.0), 6),
                "test_ending_equity": round(float(test_summary.get("ending_equity", defaults.initial_capital) or defaults.initial_capital), 6),
                "test_max_drawdown": round(float(test_summary.get("max_drawdown", 0.0) or 0.0), 6),
                "test_trades": int(test_summary.get("trades", 0) or 0),
                "test_commission": round(float(test_summary.get("total_commission", 0.0) or 0.0), 6),
                "test_total_cost": round(float(test_summary.get("total_cost", 0.0) or 0.0), 6),
                "test_gross_profit": round(float(test_summary.get("gross_pnl", 0.0) or 0.0), 6),
                "test_spread_cost": round(float(test_summary.get("total_spread_cost", 0.0) or 0.0), 6),
                "test_slippage_cost": round(float(test_summary.get("total_slippage_cost", 0.0) or 0.0), 6),
                "test_result": test_eval["result"],
            }
        )
        stitched_chunks.append({"test_result": test_eval["result"]})
        partial_stitched_equity = stitch_pair_oos_equity(stitched_chunks, defaults.initial_capital)
        partial_rows: list[dict[str, Any]] = []
        for row in fold_rows:
            cleaned = dict(row)
            cleaned.pop("test_result", None)
            partial_rows.append(cleaned)
        if partial_result_callback is not None:
            partial_result_callback(
                {
                    "status": "running",
                    "pair": serialize_pair(pair),
                    "objective_metric": objective_metric,
                    "wfa_run_id": wfa_run_id,
                    "optimization_history_path": history_path,
                    "optimization_history_rows": int(history_rows_written),
                    "lookback_units": int(lookback_units),
                    "test_units": int(test_units),
                    "step_units": int(effective_step_units),
                    "unit": unit.value,
                    "fold_count": len(partial_rows),
                    "total_net_profit": round(sum(float(item["test_net_profit"]) for item in partial_rows), 6),
                    "total_trades": int(sum(int(item["test_trades"]) for item in partial_rows)),
                    "total_commission": round(sum(float(item["test_commission"]) for item in partial_rows), 6),
                    "total_cost": round(sum(float(item["test_total_cost"]) for item in partial_rows), 6),
                    "stitched_equity": partial_stitched_equity,
                    "folds": partial_rows,
                    "failure_reason": None,
                }
            )
        if progress_callback is not None:
            progress_callback(position, total_windows, f"WFA fold {position}/{total_windows}")

    stitched_equity = stitch_pair_oos_equity(stitched_chunks, defaults.initial_capital)
    total_net_profit = sum(float(row["test_net_profit"]) for row in fold_rows)
    total_trades = sum(int(row["test_trades"]) for row in fold_rows)
    total_commission = sum(float(row["test_commission"]) for row in fold_rows)
    total_cost = sum(float(row["test_total_cost"]) for row in fold_rows)
    sanitized_rows = []
    for row in fold_rows:
        cleaned = dict(row)
        cleaned.pop("test_result", None)
        sanitized_rows.append(cleaned)
    result_payload = {
        "status": "completed",
        "pair": serialize_pair(pair),
        "objective_metric": objective_metric,
        "wfa_run_id": wfa_run_id,
        "optimization_history_path": history_path,
        "optimization_history_rows": int(history_rows_written),
        "started_at": serialize_time(started_at),
        "ended_at": serialize_time(ended_at),
        "lookback_units": int(lookback_units),
        "test_units": int(test_units),
        "step_units": int(effective_step_units),
        "unit": unit.value,
        "fold_count": len(sanitized_rows),
        "total_net_profit": round(total_net_profit, 6),
        "total_trades": int(total_trades),
        "total_commission": round(total_commission, 6),
        "total_cost": round(total_cost, 6),
        "stitched_equity": stitched_equity,
        "folds": sanitized_rows,
        "failure_reason": None if sanitized_rows else "no_wfa_folds",
    }
    if sanitized_rows:
        snapshot_path = persist_wfa_run_snapshot(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
            lookback_units=int(lookback_units),
            test_units=int(test_units),
            step_units=int(effective_step_units),
            unit=unit,
            result=result_payload,
        )
        result_payload["snapshot_path"] = str(snapshot_path)
    return result_payload
