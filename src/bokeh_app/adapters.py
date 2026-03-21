from __future__ import annotations

from bisect import bisect_left
from datetime import datetime
from typing import Any

from domain.backtest.distance import DistanceBacktestResult
from domain.optimizer.distance import DistanceOptimizationResult
from domain.scan.johansen import JohansenUniverseScanResult


def empty_backtest_sources() -> dict[str, dict[str, list[Any]]]:
    return {
        "price_1": {"x": [], "time": [], "price": []},
        "price_2": {"x": [], "time": [], "price": []},
        "spread": {"x": [], "time": [], "spread": []},
        "zscore": {"x": [], "time": [], "zscore": [], "upper": [], "lower": []},
        "equity": {"x": [], "time": [], "total": [], "leg1": [], "leg2": [], "drawdown": [], "drawdown_top": [], "drawdown_width": []},
        "trades": {
            "trade_id": [],
            "entry_time": [],
            "exit_time": [],
            "entry_x": [],
            "exit_x": [],
            "spread_side": [],
            "lots_1": [],
            "lots_2": [],
            "gross_pnl": [],
            "spread_cost_total": [],
            "slippage_cost_total": [],
            "commission_total": [],
            "net_pnl": [],
            "entry_price_1": [],
            "exit_price_1": [],
            "entry_price_2": [],
            "exit_price_2": [],
            "exit_reason": [],
        },
        "markers_1": {"x": [], "time": [], "price": [], "marker": [], "event": []},
        "markers_2": {"x": [], "time": [], "price": [], "marker": [], "event": []},
        "segments_1": {"x0": [], "y0": [], "x1": [], "y1": []},
        "segments_2": {"x0": [], "y0": [], "x1": [], "y1": []},
    }


def _trade_marker_shape(leg_side: str, event: str) -> str:
    is_long = str(leg_side).lower() == "long"
    if event == "entry":
        return "triangle" if is_long else "inverted_triangle"
    return "inverted_triangle" if is_long else "triangle"


def _lookup_bar_x(times: list[datetime], target: datetime) -> float:
    if not times:
        return 0.0
    position = bisect_left(times, target)
    if position <= 0:
        return 0.0
    if position >= len(times):
        return float(len(times) - 1)
    previous = times[position - 1]
    current = times[position]
    if abs((target - previous).total_seconds()) <= abs((current - target).total_seconds()):
        return float(position - 1)
    return float(position)


def result_to_sources(result: DistanceBacktestResult) -> dict[str, dict[str, list[Any]]]:
    frame = result.frame
    trades = result.trades

    times = frame.get_column("time").to_list()
    x_values = [float(index) for index in range(len(times))]
    time_to_x = {moment: x_values[index] for index, moment in enumerate(times)}

    price_1 = {
        "x": x_values,
        "time": times,
        "price": frame.get_column("close_1").to_list(),
    }
    price_2 = {
        "x": x_values,
        "time": times,
        "price": frame.get_column("close_2").to_list(),
    }
    spread = {
        "x": x_values,
        "time": times,
        "spread": frame.get_column("spread").to_list(),
    }
    zscore = {
        "x": x_values,
        "time": times,
        "zscore": frame.get_column("zscore").to_list(),
        "upper": frame.get_column("zscore_upper").to_list(),
        "lower": frame.get_column("zscore_lower").to_list(),
    }
    equity_total = [float(value) for value in frame.get_column("equity_total").to_list()]
    equity_leg_1 = [float(value) for value in frame.get_column("equity_leg_1").to_list()]
    equity_leg_2 = [float(value) for value in frame.get_column("equity_leg_2").to_list()]
    running_peak = float("-inf")
    drawdown: list[float] = []
    for value in equity_total:
        running_peak = max(running_peak, value)
        drawdown.append(value - running_peak)

    equity = {
        "x": x_values,
        "time": times,
        "total": equity_total,
        "leg1": equity_leg_1,
        "leg2": equity_leg_2,
        "drawdown": drawdown,
        "drawdown_top": [0.0] * len(times),
        "drawdown_width": [0.82] * len(times),
    }

    trades_rows = {
        "trade_id": [],
        "entry_time": [],
        "exit_time": [],
        "entry_x": [],
        "exit_x": [],
        "spread_side": [],
        "lots_1": [],
        "lots_2": [],
        "gross_pnl": [],
        "spread_cost_total": [],
        "slippage_cost_total": [],
        "commission_total": [],
        "net_pnl": [],
        "entry_price_1": [],
        "exit_price_1": [],
        "entry_price_2": [],
        "exit_price_2": [],
        "exit_reason": [],
    }
    markers_1 = {"x": [], "time": [], "price": [], "marker": [], "event": []}
    markers_2 = {"x": [], "time": [], "price": [], "marker": [], "event": []}
    segments_1 = {"x0": [], "y0": [], "x1": [], "y1": []}
    segments_2 = {"x0": [], "y0": [], "x1": [], "y1": []}

    for trade_id, trade in enumerate(trades.to_dicts(), start=1):
        entry_time = trade["entry_time"]
        exit_time = trade["exit_time"]
        entry_x = float(time_to_x.get(entry_time, _lookup_bar_x(times, entry_time)))
        exit_x = float(time_to_x.get(exit_time, _lookup_bar_x(times, exit_time)))
        entry_price_1 = float(trade["entry_price_1"])
        exit_price_1 = float(trade["exit_price_1"])
        entry_price_2 = float(trade["entry_price_2"])
        exit_price_2 = float(trade["exit_price_2"])

        trades_rows["trade_id"].append(trade_id)
        trades_rows["entry_time"].append(entry_time)
        trades_rows["exit_time"].append(exit_time)
        trades_rows["entry_x"].append(entry_x)
        trades_rows["exit_x"].append(exit_x)
        trades_rows["spread_side"].append(trade["spread_side"])
        trades_rows["lots_1"].append(round(float(trade["lots_1"]), 4))
        trades_rows["lots_2"].append(round(float(trade["lots_2"]), 4))
        trades_rows["gross_pnl"].append(round(float(trade.get("gross_pnl", 0.0)), 4))
        trades_rows["spread_cost_total"].append(round(float(trade.get("spread_cost_total", 0.0)), 4))
        trades_rows["slippage_cost_total"].append(round(float(trade.get("slippage_cost_total", 0.0)), 4))
        trades_rows["commission_total"].append(round(float(trade.get("commission_total", 0.0)), 4))
        trades_rows["net_pnl"].append(round(float(trade["net_pnl"]), 4))
        trades_rows["entry_price_1"].append(entry_price_1)
        trades_rows["exit_price_1"].append(exit_price_1)
        trades_rows["entry_price_2"].append(entry_price_2)
        trades_rows["exit_price_2"].append(exit_price_2)
        trades_rows["exit_reason"].append(trade["exit_reason"])

        markers_1["x"].extend([entry_x, exit_x])
        markers_1["time"].extend([entry_time, exit_time])
        markers_1["price"].extend([entry_price_1, exit_price_1])
        markers_1["marker"].extend([
            _trade_marker_shape(str(trade.get("leg_1_side", "long")), "entry"),
            _trade_marker_shape(str(trade.get("leg_1_side", "long")), "exit"),
        ])
        markers_1["event"].extend(["entry", "exit"])
        segments_1["x0"].append(entry_x)
        segments_1["y0"].append(entry_price_1)
        segments_1["x1"].append(exit_x)
        segments_1["y1"].append(exit_price_1)

        markers_2["x"].extend([entry_x, exit_x])
        markers_2["time"].extend([entry_time, exit_time])
        markers_2["price"].extend([entry_price_2, exit_price_2])
        markers_2["marker"].extend([
            _trade_marker_shape(str(trade.get("leg_2_side", "long")), "entry"),
            _trade_marker_shape(str(trade.get("leg_2_side", "long")), "exit"),
        ])
        markers_2["event"].extend(["entry", "exit"])
        segments_2["x0"].append(entry_x)
        segments_2["y0"].append(entry_price_2)
        segments_2["x1"].append(exit_x)
        segments_2["y1"].append(exit_price_2)

    return {
        "price_1": price_1,
        "price_2": price_2,
        "spread": spread,
        "zscore": zscore,
        "equity": equity,
        "trades": trades_rows,
        "markers_1": markers_1,
        "markers_2": markers_2,
        "segments_1": segments_1,
        "segments_2": segments_2,
    }

def _blend_channel(start: int, end: int, ratio: float) -> int:
    return int(round(start + (end - start) * ratio))


def _soft_metric_backgrounds(values: list[float | int | None], *, higher_is_better: bool = True) -> list[str]:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return ["transparent"] * len(values)
    low = min(clean)
    high = max(clean)
    if abs(high - low) <= 1e-12:
        return ["rgba(0,0,0,0)"] * len(values)
    bad = (254, 242, 242)
    good = (236, 253, 245)
    colors: list[str] = []
    for value in values:
        if value is None:
            colors.append("transparent")
            continue
        ratio = (float(value) - low) / (high - low)
        if not higher_is_better:
            ratio = 1.0 - ratio
        red = _blend_channel(bad[0], good[0], ratio)
        green = _blend_channel(bad[1], good[1], ratio)
        blue = _blend_channel(bad[2], good[2], ratio)
        colors.append(f"rgb({red}, {green}, {blue})")
    return colors


def optimization_results_to_source(result: DistanceOptimizationResult) -> dict[str, list[Any]]:
    rows = {
        "trial_id": [],
        "objective_metric": [],
        "objective_score": [],
        "net_profit": [],
        "net_profit_display": [],
        "net_profit_bg": [],
        "ending_equity": [],
        "ending_equity_display": [],
        "ending_equity_bg": [],
        "max_drawdown": [],
        "max_drawdown_display": [],
        "max_drawdown_bg": [],
        "pnl_to_maxdd": [],
        "pnl_to_maxdd_display": [],
        "pnl_to_maxdd_bg": [],
        "omega_ratio": [],
        "omega_ratio_display": [],
        "omega_ratio_bg": [],
        "k_ratio": [],
        "k_ratio_display": [],
        "k_ratio_bg": [],
        "score_log_trades": [],
        "ulcer_index": [],
        "ulcer_index_display": [],
        "ulcer_index_bg": [],
        "ulcer_performance": [],
        "ulcer_performance_display": [],
        "ulcer_performance_bg": [],
        "gross_profit": [],
        "spread_cost": [],
        "slippage_cost": [],
        "commission_cost": [],
        "total_cost": [],
        "trades": [],
        "win_rate": [],
        "lookback_bars": [],
        "entry_z": [],
        "exit_z": [],
        "stop_z": [],
        "stop_z_label": [],
        "bollinger_k": [],
    }
    metric_values = {
        "net_profit": [],
        "ending_equity": [],
        "max_drawdown": [],
        "pnl_to_maxdd": [],
        "omega_ratio": [],
        "k_ratio": [],
        "score_log_trades": [],
        "ulcer_index": [],
        "ulcer_performance": [],
    }
    for row in result.rows:
        rows["trial_id"].append(row.trial_id)
        rows["objective_metric"].append(row.objective_metric)
        rows["objective_score"].append(row.objective_score)
        rows["net_profit"].append(row.net_profit)
        rows["net_profit_display"].append(f"{float(row.net_profit):.2f}")
        rows["ending_equity"].append(row.ending_equity)
        rows["ending_equity_display"].append(f"{float(row.ending_equity):.2f}")
        rows["max_drawdown"].append(row.max_drawdown)
        rows["max_drawdown_display"].append(f"{float(row.max_drawdown):.2f}")
        rows["pnl_to_maxdd"].append(row.pnl_to_maxdd)
        rows["pnl_to_maxdd_display"].append(f"{float(row.pnl_to_maxdd):.3f}")
        rows["omega_ratio"].append(row.omega_ratio)
        rows["omega_ratio_display"].append(f"{float(row.omega_ratio):.3f}")
        rows["k_ratio"].append(row.k_ratio)
        rows["k_ratio_display"].append(f"{float(row.k_ratio):.3f}")
        rows["score_log_trades"].append(row.score_log_trades)
        rows["ulcer_index"].append(row.ulcer_index)
        rows["ulcer_index_display"].append(f"{float(row.ulcer_index):.4f}")
        rows["ulcer_performance"].append(row.ulcer_performance)
        rows["ulcer_performance_display"].append(f"{float(row.ulcer_performance):.3f}")
        rows["gross_profit"].append(row.gross_profit)
        rows["spread_cost"].append(row.spread_cost)
        rows["slippage_cost"].append(row.slippage_cost)
        rows["commission_cost"].append(row.commission_cost)
        rows["total_cost"].append(row.total_cost)
        rows["trades"].append(row.trades)
        rows["win_rate"].append(row.win_rate)
        rows["lookback_bars"].append(row.lookback_bars)
        rows["entry_z"].append(row.entry_z)
        rows["exit_z"].append(row.exit_z)
        rows["stop_z"].append(row.stop_z)
        rows["stop_z_label"].append("disabled" if row.stop_z is None else f"{float(row.stop_z):.1f}")
        rows["bollinger_k"].append(row.bollinger_k)
        metric_values["net_profit"].append(row.net_profit)
        metric_values["ending_equity"].append(row.ending_equity)
        metric_values["max_drawdown"].append(row.max_drawdown)
        metric_values["pnl_to_maxdd"].append(row.pnl_to_maxdd)
        metric_values["omega_ratio"].append(row.omega_ratio)
        metric_values["k_ratio"].append(row.k_ratio)
        metric_values["score_log_trades"].append(row.score_log_trades)
        metric_values["ulcer_index"].append(row.ulcer_index)
        metric_values["ulcer_performance"].append(row.ulcer_performance)

    rows["net_profit_bg"] = _soft_metric_backgrounds(metric_values["net_profit"], higher_is_better=True)
    rows["ending_equity_bg"] = _soft_metric_backgrounds(metric_values["ending_equity"], higher_is_better=True)
    rows["max_drawdown_bg"] = _soft_metric_backgrounds(metric_values["max_drawdown"], higher_is_better=True)
    rows["pnl_to_maxdd_bg"] = _soft_metric_backgrounds(metric_values["pnl_to_maxdd"], higher_is_better=True)
    rows["omega_ratio_bg"] = _soft_metric_backgrounds(metric_values["omega_ratio"], higher_is_better=True)
    rows["k_ratio_bg"] = _soft_metric_backgrounds(metric_values["k_ratio"], higher_is_better=True)
    rows["ulcer_index_bg"] = _soft_metric_backgrounds(metric_values["ulcer_index"], higher_is_better=False)
    rows["ulcer_performance_bg"] = _soft_metric_backgrounds(metric_values["ulcer_performance"], higher_is_better=True)
    return rows


def scan_results_to_source(result: JohansenUniverseScanResult, *, passed_only: bool = False) -> dict[str, list[Any]]:
    rows = {
        "symbol_1": [],
        "symbol_2": [],
        "sample_size": [],
        "eligible_for_cointegration": [],
        "unit_root_leg_1": [],
        "unit_root_leg_2": [],
        "rank": [],
        "threshold_passed": [],
        "trace_stat_0": [],
        "max_eigen_stat_0": [],
        "hedge_ratio": [],
        "half_life_bars": [],
        "last_zscore": [],
        "failure_reason": [],
    }
    iterable_rows = [row for row in result.rows if row.threshold_passed] if passed_only else result.rows
    for row in iterable_rows:
        rows["symbol_1"].append(row.symbol_1)
        rows["symbol_2"].append(row.symbol_2)
        rows["sample_size"].append(row.sample_size)
        rows["eligible_for_cointegration"].append(row.eligible_for_cointegration)
        rows["unit_root_leg_1"].append(row.unit_root_leg_1)
        rows["unit_root_leg_2"].append(row.unit_root_leg_2)
        rows["rank"].append(row.rank)
        rows["threshold_passed"].append(row.threshold_passed)
        rows["trace_stat_0"].append(row.trace_stat_0)
        rows["max_eigen_stat_0"].append(row.max_eigen_stat_0)
        rows["hedge_ratio"].append(row.hedge_ratio)
        rows["half_life_bars"].append(row.half_life_bars)
        rows["last_zscore"].append(row.last_zscore)
        rows["failure_reason"].append(row.failure_reason or "")
    return rows
