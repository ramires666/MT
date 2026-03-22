from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping, Sequence

import polars as pl

from domain.backtest.distance import DistanceParameters
from domain.contracts import PairSelection, StrategyDefaults, Timeframe, WfaWindowUnit
from domain.optimizer.distance import DistanceOptimizationRow, _evaluate_params_parallel
from domain.wfa_windowing import WalkWindow


def slice_frame(frame: pl.DataFrame, started_at: datetime, ended_at: datetime) -> pl.DataFrame:
    if frame.is_empty():
        return frame
    return frame.filter((pl.col("time") >= started_at) & (pl.col("time") <= ended_at)).sort("time")


def serialize_time(moment: datetime) -> str:
    return moment.isoformat().replace("+00:00", "Z")


def serialize_pair(pair: PairSelection) -> dict[str, Any]:
    return pair.model_dump(mode="json")


def serialize_optimization_row(prefix: str, row: DistanceOptimizationRow) -> dict[str, Any]:
    return {
        f"{prefix}_objective_metric": row.objective_metric,
        f"{prefix}_objective_score": round(float(row.objective_score), 6),
        f"{prefix}_net_profit": round(float(row.net_profit), 6),
        f"{prefix}_ending_equity": round(float(row.ending_equity), 6),
        f"{prefix}_max_drawdown": round(float(row.max_drawdown), 6),
        f"{prefix}_pnl_to_maxdd": round(float(row.pnl_to_maxdd), 6),
        f"{prefix}_omega_ratio": round(float(row.omega_ratio), 6),
        f"{prefix}_k_ratio": round(float(row.k_ratio), 6),
        f"{prefix}_score_log_trades": round(float(row.score_log_trades), 6),
        f"{prefix}_ulcer_index": round(float(row.ulcer_index), 6),
        f"{prefix}_ulcer_performance": round(float(row.ulcer_performance), 6),
        f"{prefix}_trades": int(row.trades),
        f"{prefix}_win_rate": round(float(row.win_rate), 6),
        f"{prefix}_gross_profit": round(float(row.gross_profit), 6),
        f"{prefix}_spread_cost": round(float(row.spread_cost), 6),
        f"{prefix}_slippage_cost": round(float(row.slippage_cost), 6),
        f"{prefix}_commission_cost": round(float(row.commission_cost), 6),
        f"{prefix}_total_cost": round(float(row.total_cost), 6),
    }


def build_fold_history_rows(
    *,
    optimization_rows: Sequence[DistanceOptimizationRow],
    test_frame: pl.DataFrame,
    pair: PairSelection,
    broker: str,
    timeframe: Timeframe,
    defaults: StrategyDefaults,
    objective_metric: str,
    spec_1: Mapping[str, Any],
    spec_2: Mapping[str, Any],
    window: WalkWindow,
    wfa_run_id: str,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
    parallel_workers: int | None,
) -> list[dict[str, Any]]:
    if not optimization_rows or test_frame.is_empty():
        return []

    tasks = [
        (
            int(row.trial_id),
            DistanceParameters(
                lookback_bars=int(row.lookback_bars),
                entry_z=float(row.entry_z),
                exit_z=float(row.exit_z),
                stop_z=row.stop_z,
                bollinger_k=float(row.bollinger_k),
            ),
        )
        for row in optimization_rows
    ]
    test_rows, _cancelled = _evaluate_params_parallel(
        tasks=tasks,
        frame=test_frame,
        pair=pair,
        defaults=defaults,
        objective_metric=objective_metric,
        point_1=float(spec_1.get("point", 0.0) or 0.0),
        point_2=float(spec_2.get("point", 0.0) or 0.0),
        contract_size_1=float(spec_1.get("contract_size", 1.0) or 1.0),
        contract_size_2=float(spec_2.get("contract_size", 1.0) or 1.0),
        spec_1=spec_1,
        spec_2=spec_2,
        parallel_workers=parallel_workers,
        cancel_check=None,
        progress_callback=None,
        progress_stage="WFA test evaluation",
    )
    test_map = {int(row.trial_id): row for row in test_rows}
    history_rows: list[dict[str, Any]] = []
    for train_rank, train_row in enumerate(optimization_rows, start=1):
        test_row = test_map.get(int(train_row.trial_id))
        if test_row is None:
            continue
        history_rows.append(
            {
                "wfa_run_id": wfa_run_id,
                "created_at": datetime.now(UTC).isoformat(),
                "broker": broker,
                "timeframe": timeframe.value,
                "symbol_1": pair.symbol_1,
                "symbol_2": pair.symbol_2,
                "fold": int(window.index + 1),
                "train_started_at": serialize_time(window.train_started_at),
                "train_ended_at": serialize_time(window.train_ended_at),
                "test_started_at": serialize_time(window.test_started_at),
                "test_ended_at": serialize_time(window.test_ended_at),
                "wfa_unit": unit.value if isinstance(unit, WfaWindowUnit) else str(unit),
                "wfa_lookback_units": int(lookback_units),
                "wfa_test_units": int(test_units),
                "wfa_step_units": int(step_units),
                "trial_id": int(train_row.trial_id),
                "train_rank": int(train_rank),
                "selected_for_fold": bool(train_rank == 1),
                "lookback_bars": int(train_row.lookback_bars),
                "entry_z": float(train_row.entry_z),
                "exit_z": float(train_row.exit_z),
                "stop_enabled": train_row.stop_z is not None,
                "stop_z": train_row.stop_z,
                "stop_z_value": None if train_row.stop_z is None else float(train_row.stop_z),
                "bollinger_k": float(train_row.bollinger_k),
                **serialize_optimization_row("train", train_row),
                **serialize_optimization_row("test", test_row),
            }
        )
    return history_rows


def stitch_pair_oos_equity(chunks: Sequence[dict[str, Any]], initial_capital: float) -> list[dict[str, Any]]:
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
            stitched.append({"time": serialize_time(time_value), "equity": round(adjusted, 6)})
        current_equity = stitched[-1]["equity"]
    return stitched


def combine_pair_equity_series(pair_series: Sequence[list[dict[str, Any]]], initial_capital: float) -> list[dict[str, Any]]:
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
