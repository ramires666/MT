from __future__ import annotations

from dataclasses import dataclass, field

from bokeh.models import ColumnDataSource, Range1d


def _empty_price_source() -> ColumnDataSource:
    return ColumnDataSource({"x": [], "time": [], "price": []})


def _empty_spread_source() -> ColumnDataSource:
    return ColumnDataSource({"x": [], "time": [], "spread": []})


def _empty_zscore_source() -> ColumnDataSource:
    return ColumnDataSource({"x": [], "time": [], "zscore": [], "upper": [], "lower": []})


def _empty_equity_source() -> ColumnDataSource:
    return ColumnDataSource({
        "x": [],
        "time": [],
        "total": [],
        "leg1": [],
        "leg2": [],
        "drawdown": [],
        "drawdown_top": [],
        "drawdown_width": [],
    })


def _empty_trades_source() -> ColumnDataSource:
    return ColumnDataSource(
        {
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
    )


def _empty_trade_markers_source() -> ColumnDataSource:
    return ColumnDataSource({"x": [], "time": [], "price": [], "marker": [], "event": []})


def _empty_selected_trade_markers_source() -> ColumnDataSource:
    return ColumnDataSource({"x": [], "time": [], "price": []})


def _empty_trade_segments_source() -> ColumnDataSource:
    return ColumnDataSource({"x0": [], "y0": [], "x1": [], "y1": []})


def _empty_optimization_source() -> ColumnDataSource:
    return ColumnDataSource(
        {
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
            "cagr": [],
            "cagr_display": [],
            "cagr_bg": [],
            "cagr_to_ulcer": [],
            "cagr_to_ulcer_display": [],
            "cagr_to_ulcer_bg": [],
            "r_squared": [],
            "r_squared_display": [],
            "r_squared_bg": [],
            "hurst_exponent": [],
            "hurst_exponent_display": [],
            "hurst_exponent_bg": [],
            "calmar": [],
            "calmar_display": [],
            "calmar_bg": [],
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
    )


def _empty_scan_source() -> ColumnDataSource:
    return ColumnDataSource(
        {
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
    )


@dataclass(slots=True)
class AppState:
    shared_x_range: Range1d
    price_1_source: ColumnDataSource = field(default_factory=_empty_price_source)
    price_2_source: ColumnDataSource = field(default_factory=_empty_price_source)
    spread_source: ColumnDataSource = field(default_factory=_empty_spread_source)
    zscore_source: ColumnDataSource = field(default_factory=_empty_zscore_source)
    equity_source: ColumnDataSource = field(default_factory=_empty_equity_source)
    trades_source: ColumnDataSource = field(default_factory=_empty_trades_source)
    trade_markers_1: ColumnDataSource = field(default_factory=_empty_trade_markers_source)
    trade_markers_2: ColumnDataSource = field(default_factory=_empty_trade_markers_source)
    trade_segments_1: ColumnDataSource = field(default_factory=_empty_trade_segments_source)
    trade_segments_2: ColumnDataSource = field(default_factory=_empty_trade_segments_source)
    selected_trade_markers_1: ColumnDataSource = field(default_factory=_empty_selected_trade_markers_source)
    selected_trade_markers_2: ColumnDataSource = field(default_factory=_empty_selected_trade_markers_source)
    selected_trade_segments_1: ColumnDataSource = field(default_factory=_empty_trade_segments_source)
    selected_trade_segments_2: ColumnDataSource = field(default_factory=_empty_trade_segments_source)
    optimization_source: ColumnDataSource = field(default_factory=_empty_optimization_source)
    scan_source: ColumnDataSource = field(default_factory=_empty_scan_source)
