from __future__ import annotations

from datetime import datetime
from typing import Any

from domain.backtest.distance import DistanceParameters, run_distance_backtest_frame
from domain.contracts import PairSelection, StrategyDefaults, Timeframe
from domain.data.io import load_instrument_spec
from domain.optimizer.distance import load_pair_frame
from domain.meta_selector_ml import objective_score_columns


def stitch_equity_chunks(chunks: list[dict[str, Any]], initial_capital: float) -> list[dict[str, Any]]:
    stitched: list[dict[str, Any]] = []
    current_equity = float(initial_capital)
    for chunk in chunks:
        times = list(chunk.get("times", []))
        equities = list(chunk.get("equities", []))
        if not times or not equities:
            continue
        for time_value, equity_value in zip(times, equities):
            adjusted = current_equity + (float(equity_value) - float(initial_capital))
            stitched.append({"time": time_value, "equity": round(adjusted, 6)})
        current_equity = stitched[-1]["equity"]
    return stitched


def max_drawdown(points: list[dict[str, Any]]) -> float:
    if not points:
        return 0.0
    peak = float(points[0]["equity"])
    max_dd = 0.0
    for point in points:
        equity = float(point["equity"])
        if equity > peak:
            peak = equity
        drawdown = equity - peak
        if drawdown < max_dd:
            max_dd = drawdown
    return float(max_dd)


def build_selected_fold_outputs(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    defaults: StrategyDefaults,
    selected,
    serialize_time,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], float, float, int, float, float]:
    if selected.is_empty():
        return [], [], 0.0, 0.0, 0, 0.0, 0.0
    _train_score_column, test_score_column = objective_score_columns(selected)
    spec_1 = load_instrument_spec(broker, pair.symbol_1)
    spec_2 = load_instrument_spec(broker, pair.symbol_2)
    selected_rows: list[dict[str, Any]] = []
    stitched_chunks: list[dict[str, Any]] = []
    total_net = 0.0
    total_trades = 0
    total_commission = 0.0
    total_cost = 0.0

    for row in selected.iter_rows(named=True):
        test_started_at = row.get("test_started_at")
        test_ended_at = row.get("test_ended_at")
        if not isinstance(test_started_at, datetime) or not isinstance(test_ended_at, datetime):
            continue
        params = DistanceParameters(
            lookback_bars=int(round(float(row.get("lookback_bars", 0.0) or 0.0))),
            entry_z=float(row.get("entry_z", 0.0) or 0.0),
            exit_z=float(row.get("exit_z", 0.0) or 0.0),
            stop_z=None if not bool(int(row.get("stop_enabled", 0) or 0)) else float(row.get("stop_z_value", 0.0) or 0.0),
            bollinger_k=float(row.get("bollinger_k", 0.0) or 0.0),
        )
        frame = load_pair_frame(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=test_started_at,
            ended_at=test_ended_at,
        )
        if frame.is_empty():
            continue
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
        summary = result.summary
        stitched_chunks.append(
            {
                "times": [time_value.isoformat().replace("+00:00", "Z") for time_value in result.frame.get_column("time").to_list()],
                "equities": [float(value) for value in result.frame.get_column("equity_total").to_list()],
            }
        )
        total_net += float(summary.get("net_pnl", 0.0) or 0.0)
        total_trades += int(summary.get("trades", 0) or 0)
        total_commission += float(summary.get("total_commission", 0.0) or 0.0)
        total_cost += float(summary.get("total_cost", 0.0) or 0.0)
        stop_enabled = bool(int(row.get("stop_enabled", 0) or 0))
        stop_value = row.get("stop_z_value")
        selected_rows.append(
            {
                "fold": int(row.get("fold", 0) or 0),
                "test_started_at": serialize_time(test_started_at),
                "test_ended_at": serialize_time(test_ended_at),
                "lookback_bars": int(params.lookback_bars),
                "entry_z": float(params.entry_z),
                "exit_z": float(params.exit_z),
                "stop_z": params.stop_z,
                "stop_z_label": "disabled" if not stop_enabled else f"{float(stop_value or 0.0):.2f}",
                "bollinger_k": float(params.bollinger_k),
                "predicted_target": float(row.get("predicted_target", 0.0) or 0.0),
                "test_score": float(row.get(test_score_column, 0.0) or 0.0),
                "test_net_profit": float(summary.get("net_pnl", 0.0) or 0.0),
                "test_max_drawdown": float(summary.get("max_drawdown", 0.0) or 0.0),
                "test_trades": int(summary.get("trades", 0) or 0),
                "test_commission": float(summary.get("total_commission", 0.0) or 0.0),
                "test_total_cost": float(summary.get("total_cost", 0.0) or 0.0),
            }
        )
    stitched_equity = stitch_equity_chunks(stitched_chunks, defaults.initial_capital)
    return (
        selected_rows,
        stitched_equity,
        round(total_net, 6),
        round(max_drawdown(stitched_equity), 6),
        int(total_trades),
        round(total_commission, 6),
        round(total_cost, 6),
    )
