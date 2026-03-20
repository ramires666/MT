from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from html import unescape
import os
import re
from threading import Event

import polars as pl
from bokeh.layouts import column, row
from bokeh.models import (
    Button,
    DataTable,
    DateFormatter,
    DateRangeSlider,
    Div,
    Label,
    LinearAxis,
    NumberFormatter,
    PreText,
    Range1d,
    Select,
    Span,
    Spinner,
    TableColumn,
    WheelZoomTool,
)
from bokeh.plotting import curdoc, figure
from bokeh.transform import factor_cmap

from app_config import get_settings
from bokeh_app.adapters import empty_backtest_sources, optimization_results_to_source, result_to_sources, scan_results_to_source
from bokeh_app.browser_state import BrowserStateBinding
from bokeh_app.file_state import FileStateController
from bokeh_app.state import AppState
from bokeh_app.view_utils import compute_plot_height, compute_series_bounds
from domain.backtest.distance import DistanceParameters, run_distance_backtest
from domain.contracts import (
    OptimizationMode,
    PairSelection,
    ScanUniverseMode,
    StrategyDefaults,
    Timeframe,
    UnitRootGate,
    UnitRootTest,
)
from domain.data.io import load_instrument_catalog_frame
from domain.optimizer import DistanceOptimizationResult, OBJECTIVE_METRICS, count_distance_parameter_grid, optimize_distance_genetic, optimize_distance_grid
from domain.scan.johansen import JohansenScanParameters, JohansenUniverseScanResult, scan_universe_johansen
from storage.paths import ui_state_path
from storage.scan_results import (
    COINTEGRATION_KIND_OPTIONS,
    load_latest_saved_scan_result,
    partner_symbols_from_snapshot,
    persist_johansen_scan_result,
)

INSTRUMENT_PLACEHOLDER = "-- refresh from MT5 --"
GROUP_OPTIONS = ["all", "forex", "indices", "stocks", "commodities", "crypto", "custom"]
TIMEFRAME_OPTIONS = [item.value for item in Timeframe]
UNIT_ROOT_TEST_OPTIONS = [item.value for item in UnitRootTest]
SCAN_UNIVERSE_OPTIONS = list(GROUP_OPTIONS)
SIGNIFICANCE_OPTIONS = ["0.10", "0.05", "0.01"]
OPTIMIZATION_MODE_OPTIONS = [item.value for item in OptimizationMode]
OBJECTIVE_OPTIONS = list(OBJECTIVE_METRICS)
BYBIT_FEE_MODE_OPTIONS = ["tight_spread", "zero_fee"]
STOP_MODE_OPTIONS = ["enabled", "disabled"]
OPTIMIZATION_RANGE_MODE_OPTIONS = ["manual", "auto"]
LEG_2_FILTER_OPTIONS = ["all_symbols", "cointegrated_only"]
def _build_figure(title: str, shared_x_range: Range1d, ylabel: str, height: int) -> figure:
    plot = figure(
        title=title,
        x_range=shared_x_range,
        y_range=Range1d(start=0.0, end=1.0),
        height=height,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
        x_axis_type="datetime",
    )
    wheel_zoom = plot.select_one(WheelZoomTool)
    if wheel_zoom is not None:
        wheel_zoom.modifiers = {"ctrl": True}
    plot.toolbar.autohide = True
    plot.yaxis.axis_label = ylabel
    plot.xaxis.visible = False
    return plot


def _build_equity_summary_text(summary: dict[str, float | int | str]) -> str:
    initial_capital = float(summary.get('initial_capital', 0.0) or 0.0)
    ending_equity = float(summary.get('ending_equity', 0.0) or 0.0)
    net_pnl = float(summary.get('net_pnl', 0.0) or 0.0)
    peak_equity = float(summary.get('peak_equity', 0.0) or 0.0)
    max_drawdown = float(summary.get('max_drawdown', 0.0) or 0.0)
    if abs(initial_capital) <= 1e-12:
        initial_capital = ending_equity - net_pnl
    net_pct = (net_pnl / initial_capital * 100.0) if abs(initial_capital) > 1e-12 else 0.0
    max_dd_pct = (max_drawdown / peak_equity * 100.0) if abs(peak_equity) > 1e-12 else 0.0
    rows = [
        (f"Pair: {summary['symbol_1']} / {summary['symbol_2']}", f"Trades: {int(summary.get('trades', 0) or 0)}"),
        (f"Gross: {float(summary.get('gross_pnl', 0.0) or 0.0):.2f}", f"Spread: {float(summary.get('total_spread_cost', 0.0) or 0.0):.2f}"),
        (f"Slip: {float(summary.get('total_slippage_cost', 0.0) or 0.0):.2f}", f"Commission: {float(summary.get('total_commission', 0.0) or 0.0):.2f}"),
        (f"Net PnL: {net_pnl:.2f} ({net_pct:.1f}%)", f"Ending: {ending_equity:.2f}"),
        (f"Max DD: {max_drawdown:.2f} ({max_dd_pct:.1f}%)", f"Win rate: {float(summary.get('win_rate', 0.0) or 0.0) * 100.0:.1f}%"),
    ]
    left_width = max(len(left) for left, _right in rows) + 10
    return "\n".join(f"{left:<{left_width}}{right}" for left, right in rows)


def _coerce_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=UTC)
    raise TypeError(f"Unsupported date slider value: {value!r}")


def _ui_datetime(value: datetime) -> datetime:
    current = value.astimezone(UTC)
    return current.replace(tzinfo=None)


def _datetime_to_bokeh_millis(value: datetime) -> float:
    current = value if value.tzinfo else value.replace(tzinfo=UTC)
    return current.astimezone(UTC).timestamp() * 1000.0


def _read_spinner_value(widget: Spinner, fallback: float, *, cast=float) -> float | int:
    if widget.value is None:
        return cast(fallback)
    return cast(widget.value)


def _merge_symbol_options(options: list[str], *symbols: str) -> list[str]:
    merged = {option for option in options if option != INSTRUMENT_PLACEHOLDER}
    merged.update(symbol for symbol in symbols if symbol)
    if not merged:
        return [INSTRUMENT_PLACEHOLDER]
    return sorted(merged)


def _build_section(title: str, content, *, button_width: int = 128):
    toggle_button = Button(label=title, button_type="primary", width=button_width)
    return content, content, toggle_button


def build_document() -> None:
    settings = get_settings()
    broker = settings.default_broker_id
    now_utc = datetime.now(UTC).replace(microsecond=0)
    year_start = datetime(now_utc.year, 1, 1, tzinfo=UTC)
    shared_range = Range1d(start=0, end=1)
    state = AppState(shared_x_range=shared_range)
    doc = curdoc()
    optimization_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="distance-optimizer")
    optimization_future: Future[tuple[DistanceOptimizationResult, str, PairSelection]] | None = None
    optimization_poll_callback: object | None = None
    optimization_cancel_event = Event()
    optimizer_parallel_workers = max(1, int(settings.optimizer_parallel_workers))
    optimization_progress = {"completed": 0, "total": 0, "stage": "Idle"}
    scan_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="johansen-scan")
    scan_future: Future[JohansenUniverseScanResult] | None = None
    scan_poll_callback: object | None = None
    scan_parallel_workers = max(1, int(settings.scan_parallel_workers))
    scan_progress = {"completed": 0, "total": 0, "stage": "Idle"}
    scan_request_context: dict[str, object] | None = None
    service_log_lines: list[str] = []
    service_log_last_message: dict[str, str] = {}

    summary_div = Div(text="<p>Ready. Refresh instruments, choose a pair, and run the first Distance test.</p>")
    refresh_button = Button(label="Refresh Instruments", button_type="primary")
    reset_defaults_button = Button(label="Reset Defaults", button_type="default")
    group_select = Select(title="Group", value=GROUP_OPTIONS[0], options=GROUP_OPTIONS)
    symbol_1_select = Select(title="Symbol 1", value=INSTRUMENT_PLACEHOLDER, options=[INSTRUMENT_PLACEHOLDER])
    symbol_2_select = Select(title="Symbol 2", value=INSTRUMENT_PLACEHOLDER, options=[INSTRUMENT_PLACEHOLDER])
    leg_2_filter_select = Select(
        title="Leg 2 Filter",
        value="all_symbols",
        options=LEG_2_FILTER_OPTIONS,
        description="all_symbols keeps the full group list. cointegrated_only limits leg 2 to partners from the latest saved scan for the selected type and timeframe.",
    )
    leg_2_cointegration_kind_select = Select(
        title="Cointegration Type",
        value=COINTEGRATION_KIND_OPTIONS[0],
        options=list(COINTEGRATION_KIND_OPTIONS),
        description="Select which saved cointegration list is used for leg 2 filtering. New scans are currently implemented for johansen; other types can still be browsed when saved.",
    )
    timeframe_select = Select(title="Timeframe", value=Timeframe.M15.value, options=TIMEFRAME_OPTIONS)
    period_slider = DateRangeSlider(
        title="Test Period",
        start=_ui_datetime(year_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(year_start), _ui_datetime(now_utc)),
    )
    algorithm_select = Select(title="Algorithm", value="distance", options=["distance", "johansen", "copula"])
    capital_input = Spinner(title="Initial Capital", low=100.0, step=100.0, value=10_000.0)
    leverage_input = Spinner(title="Leverage", low=1.0, step=1.0, value=100.0)
    margin_budget_input = Spinner(title="Margin Budget / Leg", low=10.0, step=10.0, value=500.0)
    slippage_input = Spinner(title="Slippage (points)", low=0.0, step=0.1, value=1.0)
    bybit_fee_mode_select = Select(title="Bybit Fee Mode", value=settings.bybit_tradfi_fee_mode, options=BYBIT_FEE_MODE_OPTIONS)
    stop_mode_select = Select(title="Stop Z Mode", value="enabled", options=STOP_MODE_OPTIONS)
    lookback_input = Spinner(title="Lookback Bars", low=5, step=1, value=96)
    entry_input = Spinner(title="Entry Z", low=0.1, step=0.1, value=2.0)
    exit_input = Spinner(title="Exit Z", low=0.0, step=0.1, value=0.5)
    stop_input = Spinner(title="Stop Z", low=0.1, step=0.1, value=3.5)
    bollinger_input = Spinner(title="Bollinger K", low=0.1, step=0.1, value=2.0)
    run_button = Button(label="Run Test", button_type="success")

    sidebar = column(
        Div(text="<div class='sidebar-heading'><h2>MT Pair Tester</h2><p>Distance tester slice with responsive charts.</p></div>"),
        Div(text="<h3>Universe</h3>"),
        row(refresh_button, reset_defaults_button, sizing_mode="stretch_width"),
        group_select,
        symbol_1_select,
        symbol_2_select,
        leg_2_filter_select,
        leg_2_cointegration_kind_select,
        timeframe_select,
        period_slider,
        Div(text="<h3>Algorithm</h3>"),
        algorithm_select,
        lookback_input,
        entry_input,
        exit_input,
        stop_mode_select,
        stop_input,
        bollinger_input,
        Div(text="<h3>Capital & Costs</h3>"),
        capital_input,
        leverage_input,
        margin_budget_input,
        slippage_input,
        bybit_fee_mode_select,
        run_button,
        summary_div,
        sizing_mode="stretch_height",
        width=340,
    )

    optimization_mode_select = Select(title="Mode", value=OptimizationMode.GRID.value, options=OPTIMIZATION_MODE_OPTIONS)
    optimization_objective_select = Select(title="Objective", value="net_profit", options=OBJECTIVE_OPTIONS)
    optimization_range_mode_select = Select(title="Grid Steps", value="manual", options=OPTIMIZATION_RANGE_MODE_OPTIONS)
    auto_grid_trials_input = Spinner(title="Auto Grid Trials", low=10, step=10, value=500)
    optimization_period_slider = DateRangeSlider(
        title="Optimization Period",
        start=_ui_datetime(year_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(year_start), _ui_datetime(now_utc)),
    )
    opt_lookback_start = Spinner(title="Lookback Start", low=5, step=1, value=48)
    opt_lookback_stop = Spinner(title="Lookback Stop", low=5, step=1, value=144)
    opt_lookback_step = Spinner(title="Lookback Step", low=1, step=1, value=24)
    opt_entry_start = Spinner(title="Entry Z Start", low=0.1, step=0.1, value=1.5)
    opt_entry_stop = Spinner(title="Entry Z Stop", low=0.1, step=0.1, value=2.5)
    opt_entry_step = Spinner(title="Entry Z Step", low=0.1, step=0.1, value=0.5)
    opt_exit_start = Spinner(title="Exit Z Start", low=0.0, step=0.1, value=0.3)
    opt_exit_stop = Spinner(title="Exit Z Stop", low=0.0, step=0.1, value=0.7)
    opt_exit_step = Spinner(title="Exit Z Step", low=0.1, step=0.1, value=0.2)
    opt_stop_mode_select = Select(title="Opt Stop Z", value="enabled", options=STOP_MODE_OPTIONS)
    opt_stop_start = Spinner(title="Stop Z Start", low=0.1, step=0.1, value=3.0)
    opt_stop_stop = Spinner(title="Stop Z Stop", low=0.1, step=0.1, value=4.0)
    opt_stop_step = Spinner(title="Stop Z Step", low=0.1, step=0.1, value=0.5)
    opt_bollinger_start = Spinner(title="Bollinger Start", low=0.1, step=0.1, value=1.5)
    opt_bollinger_stop = Spinner(title="Bollinger Stop", low=0.1, step=0.1, value=2.5)
    opt_bollinger_step = Spinner(title="Bollinger Step", low=0.1, step=0.1, value=0.5)
    genetic_population_input = Spinner(title="Population Size", low=4, step=1, value=24)
    genetic_generations_input = Spinner(title="Generations", low=1, step=1, value=12)
    genetic_elite_input = Spinner(title="Elite Count", low=1, step=1, value=4)
    genetic_mutation_input = Spinner(title="Mutation Rate", low=0.0, high=1.0, step=0.05, value=0.25)
    genetic_seed_input = Spinner(title="Random Seed", low=0, step=1, value=17)
    optimization_run_button = Button(label="Run Optimization", button_type="primary")
    optimization_status_div = Div(text="<p>Ready to optimize the selected pair on a separate period.</p>")

    price_1 = _build_figure("Price 1", state.shared_x_range, "Price", 440)
    price_1.line("time", "price", source=state.price_1_source, line_width=2, color="#10b981")
    price_1.scatter(
        "time",
        "price",
        source=state.trade_markers_1,
        size=9,
        marker="marker",
        color=factor_cmap("event", palette=["#10b981", "#ef4444"], factors=["entry", "exit"]),
    )
    price_1.segment("x0", "y0", "x1", "y1", source=state.trade_segments_1, line_color="#f59e0b", line_width=2)
    price_1.segment("x0", "y0", "x1", "y1", source=state.selected_trade_segments_1, line_color="#111827", line_width=4, line_alpha=0.9)
    price_1.scatter("time", "price", source=state.selected_trade_markers_1, size=12, marker="diamond", color="#111827", line_color="#f8fafc", line_width=1.2)

    price_2 = _build_figure("Price 2", state.shared_x_range, "Price", 440)
    price_2.line("time", "price", source=state.price_2_source, line_width=2, color="#2563eb")
    price_2.scatter(
        "time",
        "price",
        source=state.trade_markers_2,
        size=9,
        marker="marker",
        color=factor_cmap("event", palette=["#10b981", "#ef4444"], factors=["entry", "exit"]),
    )
    price_2.segment("x0", "y0", "x1", "y1", source=state.trade_segments_2, line_color="#f59e0b", line_width=2)
    price_2.segment("x0", "y0", "x1", "y1", source=state.selected_trade_segments_2, line_color="#111827", line_width=4, line_alpha=0.9)
    price_2.scatter("time", "price", source=state.selected_trade_markers_2, size=12, marker="diamond", color="#111827", line_color="#f8fafc", line_width=1.2)

    spread_plot = _build_figure("Spread / Residual", state.shared_x_range, "Spread", 360)
    spread_plot.line("time", "spread", source=state.spread_source, line_width=2, color="#6366f1")

    zscore_plot = _build_figure("Z-score + Bollinger", state.shared_x_range, "Z-score", 360)
    zscore_plot.line("time", "zscore", source=state.zscore_source, line_width=2, color="#f97316")
    zscore_plot.line("time", "upper", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")
    zscore_plot.line("time", "lower", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")

    equity_plot = _build_figure("Equity", state.shared_x_range, "Unrealized DD", 440)
    equity_plot.extra_y_ranges = {"equity": Range1d(start=0.0, end=1.0)}
    equity_plot.add_layout(LinearAxis(y_range_name="equity", axis_label="Equity"), "right")
    equity_plot.vbar(
        x="time",
        width="drawdown_width",
        top="drawdown_top",
        bottom="drawdown",
        source=state.equity_source,
        fill_color="#f97316",
        fill_alpha=0.24,
        line_alpha=0.0,
    )
    equity_plot.line("time", "total", source=state.equity_source, line_width=3, color="#ec4899", legend_label="Total Equity", y_range_name="equity")
    equity_plot.line("time", "leg1", source=state.equity_source, line_width=1.5, color="#22d3ee", legend_label="Leg 1 Equity", y_range_name="equity")
    equity_plot.line("time", "leg2", source=state.equity_source, line_width=1.5, color="#16a34a", legend_label="Leg 2 Equity", y_range_name="equity")
    equity_plot.add_layout(Span(location=0.0, dimension="width", line_color="#94a3b8", line_alpha=0.35, line_width=1.0))
    equity_plot.legend.location = "top_right"
    equity_plot.legend.background_fill_alpha = 0.72
    equity_plot.legend.border_line_alpha = 0.45
    equity_plot.legend.label_text_font_size = "11px"
    equity_plot.legend.click_policy = "hide"
    equity_summary_label = Label(
        x=18,
        y=max(72, equity_plot.height - 64),
        x_units="screen",
        y_units="screen",
        text="",
        text_font="Consolas",
        text_font_size="15px",
        text_font_style="bold",
        text_color="#111827",
        text_align="left",
        text_baseline="top",
        background_fill_color="#ffffff",
        text_line_height=1.45,
        padding=18,
        background_fill_alpha=0.0,
        border_line_color="#94a3b8",
        border_line_alpha=0.0,
        border_line_width=1.5,
        border_radius=8,
        visible=False,
    )
    equity_plot.add_layout(equity_summary_label)

    trades_table = DataTable(
        source=state.trades_source,
        columns=[
            TableColumn(field="trade_id", title="#"),
            TableColumn(field="entry_time", title="Entry", formatter=DateFormatter(format="%Y-%m-%d %H:%M")),
            TableColumn(field="exit_time", title="Exit", formatter=DateFormatter(format="%Y-%m-%d %H:%M")),
            TableColumn(field="spread_side", title="Side"),
            TableColumn(field="lots_1", title="Lot 1", formatter=NumberFormatter(format="0.0000")),
            TableColumn(field="lots_2", title="Lot 2", formatter=NumberFormatter(format="0.0000")),
            TableColumn(field="gross_pnl", title="Gross", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="spread_cost_total", title="Spread", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="slippage_cost_total", title="Slip", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="commission_total", title="Commission", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="net_pnl", title="Net PnL", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="exit_reason", title="Exit"),
        ],
        sizing_mode="stretch_width",
        height=220,
        sortable=True,
        index_position=None,
    )

    optimization_table = DataTable(
        source=state.optimization_source,
        columns=[
            TableColumn(field="trial_id", title="Trial"),
            TableColumn(field="objective_score", title="Objective", formatter=NumberFormatter(format="0.000")),
            TableColumn(field="net_profit", title="Net", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="gross_profit", title="Gross", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="spread_cost", title="Spread", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="slippage_cost", title="Slip", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="commission_cost", title="Comm", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="total_cost", title="Costs", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="max_drawdown", title="Max DD", formatter=NumberFormatter(format="0.00")),
            TableColumn(field="omega_ratio", title="Omega", formatter=NumberFormatter(format="0.000")),
            TableColumn(field="k_ratio", title="K", formatter=NumberFormatter(format="0.000")),
            TableColumn(field="ulcer_index", title="Ulcer", formatter=NumberFormatter(format="0.0000")),
            TableColumn(field="trades", title="Trades"),
            TableColumn(field="lookback_bars", title="Lookback"),
            TableColumn(field="entry_z", title="Entry Z", formatter=NumberFormatter(format="0.0")),
            TableColumn(field="exit_z", title="Exit Z", formatter=NumberFormatter(format="0.0")),
            TableColumn(field="stop_z_label", title="Stop Z"),
            TableColumn(field="bollinger_k", title="Boll K", formatter=NumberFormatter(format="0.0")),
        ],
        sizing_mode="stretch_width",
        height=240,
        sortable=True,
        index_position=None,
    )

    scan_universe_select = Select(
        title="Universe",
        value=GROUP_OPTIONS[0],
        options=SCAN_UNIVERSE_OPTIONS,
        width=150,
        description="Choose which universe to scan or browse: all symbols or one normalized tester group.",
    )
    scan_kind_select = Select(
        title="Type",
        value=COINTEGRATION_KIND_OPTIONS[0],
        options=list(COINTEGRATION_KIND_OPTIONS),
        width=115,
        description="Select which cointegration type to display and use. New scans are currently implemented only for johansen.",
    )
    scan_unit_root_select = Select(
        title="Unit Root",
        value=UnitRootTest.ADF.value,
        options=UNIT_ROOT_TEST_OPTIONS,
        width=150,
        description="Unit-root gate before Johansen. Only pairs where both legs pass the I(1) screen are evaluated.",
    )
    scan_significance_select = Select(
        title="Significance",
        value="0.05",
        options=SIGNIFICANCE_OPTIONS,
        width=130,
        description="Johansen significance threshold. Lower values mean stricter pair selection.",
    )
    scan_det_order_input = Spinner(
        title="det_order",
        low=-1,
        high=2,
        step=1,
        value=0,
        width=105,
        description="Johansen deterministic term setting. Value 0 is the usual constant-without-trend baseline.",
    )
    scan_k_ar_diff_input = Spinner(
        title="k_ar_diff",
        low=1,
        step=1,
        value=1,
        width=105,
        description="Number of lagged differences in the VECM. More lags can fit more structure but also add noise and runtime.",
    )
    scan_run_button = Button(label="Run Johansen Scan", button_type="primary", width=150)
    scan_status_div = Div(text="<p>Ready to scan the selected universe.</p>")

    scan_table = DataTable(
        source=state.scan_source,
        columns=[
            TableColumn(field="symbol_1", title="Symbol 1"),
            TableColumn(field="symbol_2", title="Symbol 2"),
            TableColumn(field="rank", title="Rank"),
            TableColumn(field="threshold_passed", title="Passed"),
            TableColumn(field="last_zscore", title="Last Z", formatter=NumberFormatter(format="0.000")),
            TableColumn(field="half_life_bars", title="Half-life", formatter=NumberFormatter(format="0.0")),
            TableColumn(field="failure_reason", title="Status"),
        ],
        sizing_mode="stretch_width",
        height=260,
        sortable=True,
        index_position=None,
    )

    trades_content = column(trades_table, sizing_mode="stretch_width")
    optimization_shared_controls = row(
        optimization_mode_select,
        optimization_objective_select,
        optimization_range_mode_select,
        auto_grid_trials_input,
        optimization_period_slider,
        optimization_run_button,
        sizing_mode="stretch_width",
    )
    optimization_lookback_controls = column(
        opt_lookback_start,
        opt_lookback_stop,
        opt_lookback_step,
        sizing_mode="stretch_width",
        width=150,
    )
    optimization_entry_controls = column(
        opt_entry_start,
        opt_entry_stop,
        opt_entry_step,
        sizing_mode="stretch_width",
        width=170,
    )
    optimization_exit_controls = column(
        opt_exit_start,
        opt_exit_stop,
        opt_exit_step,
        sizing_mode="stretch_width",
        width=170,
    )
    optimization_stop_controls = column(
        opt_stop_mode_select,
        opt_stop_start,
        opt_stop_stop,
        opt_stop_step,
        sizing_mode="stretch_width",
        width=170,
    )
    optimization_bollinger_genetic_controls = column(
        opt_bollinger_start,
        opt_bollinger_stop,
        opt_bollinger_step,
        genetic_population_input,
        genetic_generations_input,
        genetic_elite_input,
        genetic_mutation_input,
        genetic_seed_input,
        sizing_mode="stretch_width",
        width=210,
    )
    optimization_controls = column(
        optimization_shared_controls,
        row(
            optimization_lookback_controls,
            optimization_entry_controls,
            optimization_exit_controls,
            optimization_stop_controls,
            optimization_bollinger_genetic_controls,
            sizing_mode="stretch_width",
        ),
        optimization_status_div,
        sizing_mode="stretch_width",
    )
    optimization_content = column(
        optimization_controls,
        optimization_table,
        sizing_mode="stretch_width",
    )
    scan_controls = column(
        row(
            scan_universe_select,
            scan_kind_select,
            scan_unit_root_select,
            scan_significance_select,
            scan_det_order_input,
            scan_k_ar_diff_input,
            scan_run_button,
            sizing_mode="stretch_width",
        ),
        scan_status_div,
        sizing_mode="stretch_width",
    )
    scan_content = column(scan_controls, scan_table, sizing_mode="stretch_width")

    service_log_pretext = PreText(
        text="",
        height=118,
        sizing_mode="stretch_width",
        styles={
            "background": "#0f172a",
            "color": "#cbd5e1",
            "padding": "8px 12px",
            "overflow-y": "auto",
            "border": "0",
            "margin": "0",
            "white-space": "pre-wrap",
            "font-size": "11px",
            "line-height": "1.35",
        },
    )
    service_log_body = column(service_log_pretext, sizing_mode="stretch_width", visible=False)
    service_log_toggle = Button(label="Service Logs (0)", button_type="default", sizing_mode="stretch_width")

    price_1_section, price_1_body, price_1_toggle = _build_section("Price 1", price_1)
    price_2_section, price_2_body, price_2_toggle = _build_section("Price 2", price_2)
    spread_section, spread_body, spread_toggle = _build_section("Spread / Residual", spread_plot)
    zscore_section, zscore_body, zscore_toggle = _build_section("Z-score + Bollinger", zscore_plot)
    equity_section, equity_body, equity_toggle = _build_section("Equity", equity_plot)
    trades_block, trades_body, trades_toggle = _build_section("Trades", trades_content)
    optimization_block, optimization_body, optimization_toggle = _build_section("Optimization Results", optimization_content)
    scan_block, scan_body, scan_toggle = _build_section("Cointegration Results", scan_content)

    plots = [price_1, price_2, spread_plot, zscore_plot, equity_plot]
    optimization_cutoff_spans = []
    for plot in plots:
        cutoff_span = Span(
            location=0.0,
            dimension="height",
            line_color="#0f172a",
            line_alpha=0.22,
            line_width=3,
            line_dash="dashed",
            visible=False,
        )
        plot.add_layout(cutoff_span)
        optimization_cutoff_spans.append(cutoff_span)
    plot_bindings = [
        ("price_1", price_1, state.price_1_source, ["price"], price_1_body, price_1_toggle),
        ("price_2", price_2, state.price_2_source, ["price"], price_2_body, price_2_toggle),
        ("spread", spread_plot, state.spread_source, ["spread"], spread_body, spread_toggle),
        ("zscore", zscore_plot, state.zscore_source, ["zscore", "upper", "lower"], zscore_body, zscore_toggle),
        ("equity", equity_plot, state.equity_source, ["total", "leg1", "leg2"], equity_body, equity_toggle),
    ]
    block_bindings = [
        ("trades", trades_body, trades_toggle),
        ("optimization", optimization_body, optimization_toggle),
        ("cointegration", scan_body, scan_toggle),
    ]

    browser_state_bindings = [
        BrowserStateBinding("group", group_select, kind="select"),
        BrowserStateBinding("symbol_1", symbol_1_select, kind="select", restore_on_options_change=True),
        BrowserStateBinding("symbol_2", symbol_2_select, kind="select", restore_on_options_change=True),
        BrowserStateBinding("leg2_filter_mode", leg_2_filter_select, kind="select"),
        BrowserStateBinding("leg2_cointegration_kind", leg_2_cointegration_kind_select, kind="select"),
        BrowserStateBinding("timeframe", timeframe_select, kind="select"),
        BrowserStateBinding("test_period", period_slider, kind="range"),
        BrowserStateBinding("algorithm", algorithm_select, kind="select"),
        BrowserStateBinding("initial_capital", capital_input),
        BrowserStateBinding("leverage", leverage_input),
        BrowserStateBinding("margin_budget_per_leg", margin_budget_input),
        BrowserStateBinding("slippage_points", slippage_input),
        BrowserStateBinding("bybit_fee_mode", bybit_fee_mode_select, kind="select"),
        BrowserStateBinding("stop_mode", stop_mode_select, kind="select"),
        BrowserStateBinding("lookback_bars", lookback_input),
        BrowserStateBinding("entry_z", entry_input),
        BrowserStateBinding("exit_z", exit_input),
        BrowserStateBinding("stop_z", stop_input),
        BrowserStateBinding("bollinger_k", bollinger_input),
        BrowserStateBinding("optimization_mode", optimization_mode_select, kind="select"),
        BrowserStateBinding("optimization_objective", optimization_objective_select, kind="select"),
        BrowserStateBinding("optimization_range_mode", optimization_range_mode_select, kind="select"),
        BrowserStateBinding("optimization_auto_trials", auto_grid_trials_input),
        BrowserStateBinding("opt_stop_mode", opt_stop_mode_select, kind="select"),
        BrowserStateBinding("optimization_period", optimization_period_slider, kind="range"),
        BrowserStateBinding("opt_lookback_start", opt_lookback_start),
        BrowserStateBinding("opt_lookback_stop", opt_lookback_stop),
        BrowserStateBinding("opt_lookback_step", opt_lookback_step),
        BrowserStateBinding("opt_entry_start", opt_entry_start),
        BrowserStateBinding("opt_entry_stop", opt_entry_stop),
        BrowserStateBinding("opt_entry_step", opt_entry_step),
        BrowserStateBinding("opt_exit_start", opt_exit_start),
        BrowserStateBinding("opt_exit_stop", opt_exit_stop),
        BrowserStateBinding("opt_exit_step", opt_exit_step),
        BrowserStateBinding("opt_stop_start", opt_stop_start),
        BrowserStateBinding("opt_stop_stop", opt_stop_stop),
        BrowserStateBinding("opt_stop_step", opt_stop_step),
        BrowserStateBinding("opt_bollinger_start", opt_bollinger_start),
        BrowserStateBinding("opt_bollinger_stop", opt_bollinger_stop),
        BrowserStateBinding("opt_bollinger_step", opt_bollinger_step),
        BrowserStateBinding("genetic_population", genetic_population_input),
        BrowserStateBinding("genetic_generations", genetic_generations_input),
        BrowserStateBinding("genetic_elite", genetic_elite_input),
        BrowserStateBinding("genetic_mutation", genetic_mutation_input),
        BrowserStateBinding("genetic_seed", genetic_seed_input),
        BrowserStateBinding("scan_universe", scan_universe_select, kind="select"),
        BrowserStateBinding("scan_kind", scan_kind_select, kind="select"),
        BrowserStateBinding("scan_unit_root", scan_unit_root_select, kind="select"),
        BrowserStateBinding("scan_significance", scan_significance_select, kind="select"),
        BrowserStateBinding("scan_det_order", scan_det_order_input),
        BrowserStateBinding("scan_k_ar_diff", scan_k_ar_diff_input),
        BrowserStateBinding("show_price_1", price_1_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_price_1_button", price_1_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_price_2", price_2_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_price_2_button", price_2_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_spread", spread_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_spread_button", spread_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_zscore", zscore_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_zscore_button", zscore_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_equity", equity_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_equity_button", equity_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_trades", trades_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_trades_button", trades_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_optimization", optimization_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_optimization_button", optimization_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_cointegration", scan_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_cointegration_button", scan_toggle, property_name="button_type", default="primary"),
        BrowserStateBinding("show_service_logs", service_log_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_service_logs_button", service_log_toggle, property_name="button_type", default="default"),
    ]
    file_state_controller = FileStateController(ui_state_path(), browser_state_bindings)

    def _normalize_log_message(raw_text: str) -> str:
        cleaned = unescape(re.sub(r"<[^>]+>", " ", raw_text or ""))
        cleaned = re.sub(r"\s+", " ", cleaned).strip()
        return cleaned

    def append_service_log(source: str, raw_text: str) -> None:
        message = _normalize_log_message(raw_text)
        if not message:
            return
        if service_log_last_message.get(source) == message:
            return
        service_log_last_message[source] = message
        timestamp = datetime.now().strftime("%H:%M:%S")
        service_log_lines.append(f"[{timestamp}] {source}: {message}")
        if len(service_log_lines) > 1000:
            del service_log_lines[:-1000]
        service_log_pretext.text = "\n".join(service_log_lines[-500:])
        service_log_toggle.label = f"Service Logs ({len(service_log_lines)})"

    def build_service_log_handler(source: str):
        def _handler(_attr: str, _old: object, new: object) -> None:
            append_service_log(source, str(new))
        return _handler

    def sync_service_log_toggle() -> None:
        service_log_toggle.button_type = "primary" if service_log_body.visible else "default"

    def set_section_visibility(body, toggle: Button, visible: bool) -> None:
        body.visible = visible
        toggle.button_type = "primary" if visible else "default"

    def build_section_toggle_handler(body, toggle: Button):
        def _handler() -> None:
            set_section_visibility(body, toggle, not body.visible)
        return _handler

    def toggle_service_log() -> None:
        set_section_visibility(service_log_body, service_log_toggle, not service_log_body.visible)

    def clear_trade_highlights() -> None:
        state.selected_trade_markers_1.data = {"time": [], "price": []}
        state.selected_trade_markers_2.data = {"time": [], "price": []}
        state.selected_trade_segments_1.data = {"x0": [], "y0": [], "x1": [], "y1": []}
        state.selected_trade_segments_2.data = {"x0": [], "y0": [], "x1": [], "y1": []}

    def set_equity_summary_overlay(summary: dict[str, float | int | str] | None) -> None:
        if not summary:
            equity_summary_label.text = ""
            equity_summary_label.visible = False
            return
        equity_summary_label.text = _build_equity_summary_text(summary)
        equity_summary_label.visible = True

    def sync_equity_summary_overlay_position() -> None:
        equity_summary_label.x = 18
        equity_summary_label.y = max(72, int(equity_plot.height) - 64)

    def sync_optimization_cutoff_marker(*, test_started_at: datetime | None, test_ended_at: datetime | None) -> None:
        if test_started_at is None or test_ended_at is None:
            for span in optimization_cutoff_spans:
                span.visible = False
            return
        optimization_ended_at = _coerce_datetime(optimization_period_slider.value[1])
        should_show = test_ended_at > optimization_ended_at and test_started_at <= optimization_ended_at
        location = _datetime_to_bokeh_millis(optimization_ended_at)
        for span in optimization_cutoff_spans:
            span.location = location
            span.visible = should_show

    def clear_backtest_outputs(message: str) -> bool:
        sources = empty_backtest_sources()
        state.price_1_source.data = sources["price_1"]
        state.price_2_source.data = sources["price_2"]
        state.spread_source.data = sources["spread"]
        state.zscore_source.data = sources["zscore"]
        state.equity_source.data = sources["equity"]
        state.trades_source.data = sources["trades"]
        state.trade_markers_1.data = sources["markers_1"]
        state.trade_markers_2.data = sources["markers_2"]
        state.trade_segments_1.data = sources["segments_1"]
        state.trade_segments_2.data = sources["segments_2"]
        state.trades_source.selected.indices = []
        clear_trade_highlights()
        state.shared_x_range.start = 0
        state.shared_x_range.end = 1
        rebalance_layout()
        refresh_plot_ranges()
        sync_optimization_cutoff_marker(test_started_at=None, test_ended_at=None)
        set_equity_summary_overlay(None)
        summary_div.text = message
        return False

    def refresh_plot_ranges() -> None:
        for key, plot, source, value_columns, body, _toggle in plot_bindings:
            if not body.visible:
                continue
            data = source.data
            if key == "equity":
                times = data.get("time", [])
                equity_low, equity_high = compute_series_bounds(
                    times,
                    [data.get("total", []), data.get("leg1", []), data.get("leg2", [])],
                    state.shared_x_range.start,
                    state.shared_x_range.end,
                )
                if abs(float(equity_high) - float(equity_low)) <= 1e-9:
                    equity_high = float(equity_low) + 1.0
                equity_plot.extra_y_ranges["equity"].start = float(equity_low)
                equity_plot.extra_y_ranges["equity"].end = float(equity_high)

                drawdown_low, _drawdown_high = compute_series_bounds(
                    times,
                    [data.get("drawdown", [])],
                    state.shared_x_range.start,
                    state.shared_x_range.end,
                    pad_ratio=0.08,
                    fallback=(-1.0, 0.0),
                )
                drawdown_low = min(float(drawdown_low), -1e-6)
                plot.y_range.start = drawdown_low
                plot.y_range.end = 0.0
                continue
            lower, upper = compute_series_bounds(
                data.get("time", []),
                [data.get(column, []) for column in value_columns],
                state.shared_x_range.start,
                state.shared_x_range.end,
            )
            plot.y_range.start = lower
            plot.y_range.end = upper

    def rebalance_layout() -> None:
        visible_plots = [plot for _key, plot, _source, _columns, body, _toggle in plot_bindings if body.visible]
        visible_blocks = [body for _key, body, _toggle in block_bindings if body.visible]
        plot_height = compute_plot_height(len(visible_plots), len(visible_blocks))
        for _key, plot, _source, _columns, body, toggle in plot_bindings:
            plot.visible = body.visible
            toggle.button_type = "primary" if body.visible else "default"
            if body.visible:
                plot.height = plot_height
        sync_equity_summary_overlay_position()
        for _key, body, toggle in block_bindings:
            toggle.button_type = "primary" if body.visible else "default"

        last_visible_plot = visible_plots[-1] if visible_plots else None
        for plot in plots:
            plot.xaxis.visible = plot is last_visible_plot

    def build_defaults() -> StrategyDefaults:
        return StrategyDefaults(
            initial_capital=float(_read_spinner_value(capital_input, 10_000.0)),
            leverage=float(_read_spinner_value(leverage_input, 100.0)),
            margin_budget_per_leg=float(_read_spinner_value(margin_budget_input, 500.0)),
            slippage_points=float(_read_spinner_value(slippage_input, 1.0)),
        )

    def sync_bybit_fee_mode() -> None:
        os.environ["MT_SERVICE_BYBIT_TRADFI_FEE_MODE"] = bybit_fee_mode_select.value
        get_settings.cache_clear()

    def build_distance_params() -> DistanceParameters:
        return DistanceParameters(
            lookback_bars=int(_read_spinner_value(lookback_input, 96, cast=int)),
            entry_z=float(_read_spinner_value(entry_input, 2.0)),
            exit_z=float(_read_spinner_value(exit_input, 0.5)),
            stop_z=None if stop_mode_select.value == "disabled" else float(_read_spinner_value(stop_input, 3.5)),
            bollinger_k=float(_read_spinner_value(bollinger_input, 2.0)),
        )

    def current_pair() -> PairSelection | None:
        if symbol_1_select.value == INSTRUMENT_PLACEHOLDER or symbol_2_select.value == INSTRUMENT_PLACEHOLDER:
            return None
        if symbol_1_select.value == symbol_2_select.value:
            return None
        return PairSelection(symbol_1=symbol_1_select.value, symbol_2=symbol_2_select.value)

    def _empty_scan_source_data() -> dict[str, list[object]]:
        return {key: [] for key in state.scan_source.data.keys()}

    def _scan_frame_to_source(frame: pl.DataFrame) -> dict[str, list[object]]:
        data = _empty_scan_source_data()
        if frame.is_empty():
            return data
        for column in data:
            if column in frame.columns:
                data[column] = frame.get_column(column).to_list()
        return data

    def _preferred_symbol_1(options: list[str], current: str | None) -> str:
        candidates = [option for option in options if option != INSTRUMENT_PLACEHOLDER]
        if not candidates:
            return INSTRUMENT_PLACEHOLDER
        if current in candidates:
            return str(current)
        if 'US2000' in candidates:
            return 'US2000'
        return candidates[0]

    def _preferred_symbol_2(options: list[str], current: str | None, symbol_1: str | None) -> str:
        candidates = [option for option in options if option != INSTRUMENT_PLACEHOLDER and option != symbol_1]
        if not candidates:
            return INSTRUMENT_PLACEHOLDER if INSTRUMENT_PLACEHOLDER in options else (options[0] if options else INSTRUMENT_PLACEHOLDER)
        if current in candidates:
            return str(current)
        if 'NAS100' in candidates:
            return 'NAS100'
        return candidates[0]

    def instrument_options_for_group(selected_group: str) -> list[str]:
        try:
            catalog = load_instrument_catalog_frame(broker)
        except Exception as exc:  # pragma: no cover - runtime UI path
            summary_div.text = f"<p>Instrument catalog unavailable: {exc}</p>"
            return [INSTRUMENT_PLACEHOLDER]
        if selected_group != 'all':
            catalog = catalog.filter(pl.col('normalized_group') == selected_group)
        options = catalog.get_column('symbol').sort().to_list() if not catalog.is_empty() else []
        return options or [INSTRUMENT_PLACEHOLDER]

    def current_tester_cointegration_scope() -> tuple[ScanUniverseMode, str | None]:
        if group_select.value != 'all':
            return ScanUniverseMode.GROUP, group_select.value
        return ScanUniverseMode.ALL, None

    def current_scan_selection_scope() -> tuple[ScanUniverseMode, str | None]:
        if scan_universe_select.value != 'all':
            return ScanUniverseMode.GROUP, scan_universe_select.value
        return ScanUniverseMode.ALL, None

    def load_tester_cointegration_snapshot() -> object | None:
        universe_mode, normalized_group = current_tester_cointegration_scope()
        return load_latest_saved_scan_result(
            broker=broker,
            scan_kind=leg_2_cointegration_kind_select.value,
            timeframe=Timeframe(timeframe_select.value),
            universe_mode=universe_mode,
            normalized_group=normalized_group,
            allow_all_fallback=True,
        )

    def restore_saved_scan_table(*, show_message: bool) -> object | None:
        universe_mode, normalized_group = current_scan_selection_scope()
        snapshot = load_latest_saved_scan_result(
            broker=broker,
            scan_kind=scan_kind_select.value,
            timeframe=Timeframe(timeframe_select.value),
            universe_mode=universe_mode,
            normalized_group=normalized_group,
            allow_all_fallback=False,
        )
        if snapshot is None:
            state.scan_source.data = _empty_scan_source_data()
            state.scan_source.selected.indices = []
            if show_message:
                scope_label = normalized_group or 'all'
                scan_status_div.text = (
                    f"<p>No saved <b>{scan_kind_select.value}</b> cointegration table found for "
                    f"<b>{timeframe_select.value}</b> / <b>{scope_label}</b>.</p>"
                )
            return None

        state.scan_source.data = _scan_frame_to_source(snapshot.passed_pairs)
        state.scan_source.selected.indices = []
        if show_message:
            scan_summary = snapshot.summary.get('summary', {})
            created_label = snapshot.created_at.astimezone(UTC).strftime('%Y-%m-%d %H:%M UTC') if snapshot.created_at else 'unknown time'
            started_label = str(snapshot.summary.get('started_at', ''))
            ended_label = str(snapshot.summary.get('ended_at', ''))
            scan_status_div.text = (
                f"<p>Loaded saved <b>{snapshot.scan_kind}</b> table for <b>{snapshot.timeframe.value}</b> / <b>{snapshot.scope}</b>. "
                f"Passed: {int(scan_summary.get('threshold_passed_pairs', snapshot.passed_pairs.height) or 0)}, "
                f"Pairs: {int(scan_summary.get('total_pairs_evaluated', snapshot.all_pairs.height) or 0)}, "
                f"saved at <b>{created_label}</b>. Period: <b>{started_label}</b> .. <b>{ended_label}</b>.</p>"
            )
        return snapshot

    def sync_symbol_2_filter(*, base_options: list[str] | None = None, show_message: bool) -> None:
        options = list(base_options or instrument_options_for_group(group_select.value))
        if not options:
            options = [INSTRUMENT_PLACEHOLDER]

        current_symbol_1 = symbol_1_select.value if symbol_1_select.value in options else _preferred_symbol_1(options, symbol_1_select.value)
        symbol_1_select.options = options
        if symbol_1_select.value != current_symbol_1:
            symbol_1_select.value = current_symbol_1

        if leg_2_filter_select.value != 'cointegrated_only':
            symbol_2_select.options = options
            symbol_2_select.value = _preferred_symbol_2(options, symbol_2_select.value, current_symbol_1)
            return

        if current_symbol_1 == INSTRUMENT_PLACEHOLDER:
            symbol_2_select.options = [INSTRUMENT_PLACEHOLDER]
            symbol_2_select.value = INSTRUMENT_PLACEHOLDER
            return

        snapshot = load_tester_cointegration_snapshot()
        if snapshot is None:
            symbol_2_select.options = options
            symbol_2_select.value = _preferred_symbol_2(options, symbol_2_select.value, current_symbol_1)
            if show_message:
                scope_label = group_select.value if group_select.value != 'all' else 'all'
                summary_div.text = (
                    f"<p>Leg 2 cointegration filter needs a saved <b>{leg_2_cointegration_kind_select.value}</b> table for "
                    f"<b>{timeframe_select.value}</b> / <b>{scope_label}</b>. No matching saved scan was found.</p>"
                )
            return

        partner_options = partner_symbols_from_snapshot(snapshot, symbol_1=current_symbol_1, allowed_symbols=options)
        partner_options = [option for option in partner_options if option != current_symbol_1]
        if not partner_options:
            symbol_2_select.options = [INSTRUMENT_PLACEHOLDER]
            symbol_2_select.value = INSTRUMENT_PLACEHOLDER
            if show_message:
                summary_div.text = (
                    f"<p>No saved <b>{leg_2_cointegration_kind_select.value}</b> partners found for <b>{current_symbol_1}</b> "
                    f"on <b>{timeframe_select.value}</b> inside the saved <b>{snapshot.scope}</b> universe.</p>"
                )
            return

        symbol_2_select.options = partner_options
        symbol_2_select.value = _preferred_symbol_2(partner_options, symbol_2_select.value, current_symbol_1)
        if show_message:
            summary_div.text = (
                f"<p>Leg 2 filter loaded <b>{len(partner_options)}</b> saved <b>{leg_2_cointegration_kind_select.value}</b> partners "
                f"for <b>{current_symbol_1}</b> on <b>{timeframe_select.value}</b> from the <b>{snapshot.scope}</b> table.</p>"
            )

    def _auto_axis_values(start: float, stop: float, count: int, *, integer: bool) -> list[int | float]:
        if count <= 1 or abs(stop - start) <= (0 if integer else 1e-12):
            return [int(round(start)) if integer else round(float(start), 10)]
        if integer:
            start_i = int(round(start))
            stop_i = int(round(stop))
            if stop_i < start_i:
                start_i, stop_i = stop_i, start_i
            if count >= (stop_i - start_i + 1):
                return list(range(start_i, stop_i + 1))
            values = [int(round(start_i + (stop_i - start_i) * idx / (count - 1))) for idx in range(count)]
            deduped: list[int] = []
            for value in values:
                if value not in deduped:
                    deduped.append(value)
            if deduped[0] != start_i:
                deduped.insert(0, start_i)
            if deduped[-1] != stop_i:
                deduped.append(stop_i)
            return deduped
        values_f = [round(float(start + (stop - start) * idx / (count - 1)), 10) for idx in range(count)]
        deduped_f: list[float] = []
        for value in values_f:
            if not deduped_f or abs(deduped_f[-1] - value) > 1e-9:
                deduped_f.append(value)
        if abs(deduped_f[0] - float(start)) > 1e-9:
            deduped_f.insert(0, round(float(start), 10))
        if abs(deduped_f[-1] - float(stop)) > 1e-9:
            deduped_f.append(round(float(stop), 10))
        return deduped_f

    def _product_value(values: list[int]) -> int:
        result = 1
        for value in values:
            result *= int(value)
        return result

    def _build_auto_search_space(
        bounds: dict[str, dict[str, float | int | bool | None]],
        counts: dict[str, int],
    ) -> dict[str, list[int | float | None]]:
        result: dict[str, list[int | float | None]] = {}
        for name, bound in bounds.items():
            if bound.get("disabled"):
                result[name] = [None]
                continue
            result[name] = _auto_axis_values(
                float(bound["start"]),
                float(bound["stop"]),
                counts[name],
                integer=bool(bound["integer"]),
            )
        return result

    def _auto_grid_point_counts(bounds: dict[str, dict[str, float | int | bool | None]], target_trials: int) -> dict[str, int]:
        variable_keys = [name for name, bound in bounds.items() if not bound.get("disabled") and float(bound["stop"]) > float(bound["start"])]
        counts = {name: 1 for name in bounds}
        for name in variable_keys:
            counts[name] = 2
        if not variable_keys:
            return counts
        spans = {name: max(float(bounds[name]["stop"]) - float(bounds[name]["start"]), 1e-9) for name in variable_keys}
        previous = dict(counts)
        previous_product = _product_value(list(counts.values()))
        while True:
            current_product = _product_value(list(counts.values()))
            if current_product >= target_trials:
                if abs(previous_product - target_trials) < abs(current_product - target_trials):
                    return previous
                return counts
            previous = dict(counts)
            previous_product = current_product
            next_key = max(variable_keys, key=lambda name: spans[name] / max(counts[name] - 1, 1))
            counts[next_key] += 1
            if counts[next_key] > 32:
                return counts

    def _auto_optimization_search_space(target_trials: int = 500) -> dict[str, list[int | float | None]]:
        bounds: dict[str, dict[str, float | int | bool | None]] = {
            "lookback_bars": {
                "start": int(_read_spinner_value(opt_lookback_start, 48, cast=int)),
                "stop": int(_read_spinner_value(opt_lookback_stop, 144, cast=int)),
                "integer": True,
                "disabled": False,
            },
            "entry_z": {
                "start": float(_read_spinner_value(opt_entry_start, 1.5)),
                "stop": float(_read_spinner_value(opt_entry_stop, 2.5)),
                "integer": False,
                "disabled": False,
            },
            "exit_z": {
                "start": float(_read_spinner_value(opt_exit_start, 0.3)),
                "stop": float(_read_spinner_value(opt_exit_stop, 0.7)),
                "integer": False,
                "disabled": False,
            },
            "stop_z": {
                "start": float(_read_spinner_value(opt_stop_start, 3.0)),
                "stop": float(_read_spinner_value(opt_stop_stop, 4.0)),
                "integer": False,
                "disabled": opt_stop_mode_select.value == "disabled",
            },
            "bollinger_k": {
                "start": float(_read_spinner_value(opt_bollinger_start, 1.5)),
                "stop": float(_read_spinner_value(opt_bollinger_stop, 2.5)),
                "integer": False,
                "disabled": False,
            },
        }
        counts = _auto_grid_point_counts(bounds, target_trials)
        variable_keys = [name for name, bound in bounds.items() if not bound.get("disabled") and float(bound["stop"]) > float(bound["start"])]
        best_counts = dict(counts)
        best_space = _build_auto_search_space(bounds, best_counts)
        best_valid = count_distance_parameter_grid(best_space)
        best_delta = abs(best_valid - target_trials)
        if not variable_keys:
            return best_space

        for _ in range(64):
            improved = False
            candidate_counts = best_counts
            candidate_space = best_space
            candidate_valid = best_valid
            candidate_delta = best_delta
            for name in variable_keys:
                for delta in (-1, 1):
                    next_count = int(best_counts[name]) + delta
                    if next_count < 1 or next_count > 32:
                        continue
                    proposal = dict(best_counts)
                    proposal[name] = next_count
                    proposal_space = _build_auto_search_space(bounds, proposal)
                    proposal_valid = count_distance_parameter_grid(proposal_space)
                    proposal_delta = abs(proposal_valid - target_trials)
                    if proposal_delta < candidate_delta or (
                        proposal_delta == candidate_delta and abs(proposal_valid - target_trials) == abs(candidate_valid - target_trials) and proposal_valid > candidate_valid
                    ):
                        candidate_counts = proposal
                        candidate_space = proposal_space
                        candidate_valid = proposal_valid
                        candidate_delta = proposal_delta
                        improved = True
            if not improved:
                break
            best_counts = candidate_counts
            best_space = candidate_space
            best_valid = candidate_valid
            best_delta = candidate_delta
        return best_space

    def optimization_search_space() -> dict[str, Any]:
        if optimization_range_mode_select.value == "auto":
            target_trials = max(1, int(_read_spinner_value(auto_grid_trials_input, 500, cast=int)))
            return _auto_optimization_search_space(target_trials=target_trials)
        return {
            "lookback_bars": {
                "start": int(_read_spinner_value(opt_lookback_start, 48, cast=int)),
                "stop": int(_read_spinner_value(opt_lookback_stop, 144, cast=int)),
                "step": int(_read_spinner_value(opt_lookback_step, 24, cast=int)),
            },
            "entry_z": {
                "start": float(_read_spinner_value(opt_entry_start, 1.5)),
                "stop": float(_read_spinner_value(opt_entry_stop, 2.5)),
                "step": float(_read_spinner_value(opt_entry_step, 0.5)),
            },
            "exit_z": {
                "start": float(_read_spinner_value(opt_exit_start, 0.3)),
                "stop": float(_read_spinner_value(opt_exit_stop, 0.7)),
                "step": float(_read_spinner_value(opt_exit_step, 0.2)),
            },
            "stop_z": [None] if opt_stop_mode_select.value == "disabled" else {
                "start": float(_read_spinner_value(opt_stop_start, 3.0)),
                "stop": float(_read_spinner_value(opt_stop_stop, 4.0)),
                "step": float(_read_spinner_value(opt_stop_step, 0.5)),
            },
            "bollinger_k": {
                "start": float(_read_spinner_value(opt_bollinger_start, 1.5)),
                "stop": float(_read_spinner_value(opt_bollinger_stop, 2.5)),
                "step": float(_read_spinner_value(opt_bollinger_step, 0.5)),
            },
        }

    def genetic_optimizer_config() -> dict[str, float | int]:
        seed_value = genetic_seed_input.value
        return {
            "population_size": int(_read_spinner_value(genetic_population_input, 24, cast=int)),
            "generations": int(_read_spinner_value(genetic_generations_input, 12, cast=int)),
            "elite_count": int(_read_spinner_value(genetic_elite_input, 4, cast=int)),
            "mutation_rate": float(_read_spinner_value(genetic_mutation_input, 0.25)),
            "random_seed": int(seed_value) if seed_value is not None else None,
        }

    def sync_stop_mode_ui() -> None:
        stop_input.visible = stop_mode_select.value == "enabled"
        stop_enabled = opt_stop_mode_select.value == "enabled"
        opt_stop_start.visible = stop_enabled
        opt_stop_stop.visible = stop_enabled

    def sync_optimization_mode_ui() -> None:
        is_genetic = optimization_mode_select.value == OptimizationMode.GENETIC.value
        is_auto = optimization_range_mode_select.value == "auto"
        optimization_range_mode_select.visible = not is_genetic
        auto_grid_trials_input.visible = (not is_genetic) and is_auto
        for widget in [opt_lookback_step, opt_entry_step, opt_exit_step, opt_bollinger_step]:
            widget.visible = (not is_genetic) and (not is_auto)
        opt_stop_step.visible = (not is_genetic) and (not is_auto) and opt_stop_mode_select.value == "enabled"
        for widget in [genetic_population_input, genetic_generations_input, genetic_elite_input, genetic_mutation_input, genetic_seed_input]:
            widget.visible = is_genetic
        sync_stop_mode_ui()

    def reset_optimization_button() -> None:
        optimization_run_button.label = "Run Optimization"
        optimization_run_button.button_type = "primary"

    def mark_optimization_running(*, stopping: bool = False) -> None:
        optimization_run_button.label = "Stopping..." if stopping else "Stop Optimization"
        optimization_run_button.button_type = "warning"

    def update_optimization_progress(completed: int, total: int, stage: str) -> None:
        optimization_progress["completed"] = completed
        optimization_progress["total"] = total
        optimization_progress["stage"] = stage

    def render_optimization_progress() -> None:
        total = int(optimization_progress.get("total", 0) or 0)
        completed = int(optimization_progress.get("completed", 0) or 0)
        stage = str(optimization_progress.get("stage", "Optimization"))
        if total > 0:
            optimization_status_div.text = f"<p>{stage}: <b>{completed}</b> / <b>{total}</b> evaluated using <b>{optimizer_parallel_workers}</b> workers.</p>"

    def reset_scan_button() -> None:
        scan_run_button.label = "Run Johansen Scan"
        scan_run_button.button_type = "primary"

    def mark_scan_running() -> None:
        scan_run_button.label = "Scanning..."
        scan_run_button.button_type = "warning"

    def update_scan_progress(completed: int, total: int, stage: str) -> None:
        scan_progress["completed"] = completed
        scan_progress["total"] = total
        scan_progress["stage"] = stage

    def render_scan_progress() -> None:
        total = int(scan_progress.get("total", 0) or 0)
        completed = int(scan_progress.get("completed", 0) or 0)
        stage = str(scan_progress.get("stage", "Johansen scan"))
        if total > 0:
            scan_status_div.text = f"<p>{stage}: <b>{completed}</b> / <b>{total}</b> using <b>{scan_parallel_workers}</b> workers.</p>"

    def complete_optimization(result: DistanceOptimizationResult, mode_label: str, pair: PairSelection) -> None:
        state.optimization_source.data = optimization_results_to_source(result)
        state.optimization_source.selected.indices = []
        if not result.rows:
            if result.cancelled:
                optimization_status_div.text = "<p>Optimization stopped before evaluating any trials.</p>"
            elif result.failure_reason == "no_aligned_quotes":
                optimization_status_div.text = (
                    f"<p>No aligned parquet data for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on the selected optimization period and timeframe.</p>"
                )
            elif result.failure_reason == "no_valid_parameter_combinations":
                stop_rule = " and <b>entry_z &lt; stop_z</b>" if opt_stop_mode_select.value == "enabled" else ""
                optimization_status_div.text = f"<p>Optimizer ranges produced zero valid combinations. Use values that satisfy <b>exit_z &lt; entry_z</b>{stop_rule}.</p>"
            else:
                optimization_status_div.text = "<p>No trials evaluated. Check optimizer ranges and parquet coverage.</p>"
            return

        best = result.rows[0]
        if result.cancelled:
            optimization_status_div.text = (
                f"<p>Stopped {mode_label} optimization for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> "
                f"after {result.evaluated_trials} trials. Best partial objective: {best.objective_score:.3f}, "
                f"Net: {best.net_profit:.2f}, Max DD: {best.max_drawdown:.2f}.</p>"
            )
            return

        optimization_status_div.text = (
            f"<p>Evaluated {result.evaluated_trials} {mode_label} trials for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
            f"Best objective: {best.objective_score:.3f}, Net: {best.net_profit:.2f}, Max DD: {best.max_drawdown:.2f}.</p>"
        )


    def apply_backtest_result(*, period_override: tuple[datetime, datetime] | None = None) -> bool:
        if algorithm_select.value != "distance":
            return clear_backtest_outputs("<p>Only the first Distance tester slice is wired right now.</p>")

        pair = current_pair()
        if pair is None:
            return clear_backtest_outputs("<p>Refresh instruments and choose two different valid instruments first.</p>")

        if period_override is None:
            started_at = _coerce_datetime(period_slider.value[0])
            ended_at = _coerce_datetime(period_slider.value[1])
        else:
            started_at, ended_at = period_override
        result = run_distance_backtest(
            broker=broker,
            pair=pair,
            timeframe=Timeframe(timeframe_select.value),
            started_at=started_at,
            ended_at=ended_at,
            defaults=build_defaults(),
            params=build_distance_params(),
        )
        if result.frame.is_empty():
            return clear_backtest_outputs(
                f"<p>No aligned parquet data found for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> "
                f"on <b>{timeframe_select.value}</b> between "
                f"<b>{started_at:%Y-%m-%d %H:%M}</b> and <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

        sources = result_to_sources(result)
        state.price_1_source.data = sources["price_1"]
        state.price_2_source.data = sources["price_2"]
        state.spread_source.data = sources["spread"]
        state.zscore_source.data = sources["zscore"]
        state.equity_source.data = sources["equity"]
        state.trades_source.data = sources["trades"]
        state.trade_markers_1.data = sources["markers_1"]
        state.trade_markers_2.data = sources["markers_2"]
        state.trade_segments_1.data = sources["segments_1"]
        state.trade_segments_2.data = sources["segments_2"]
        state.trades_source.selected.indices = []
        clear_trade_highlights()

        requested_start = _datetime_to_bokeh_millis(started_at)
        requested_end = _datetime_to_bokeh_millis(ended_at)
        if requested_end <= requested_start:
            requested_end = requested_start + 1.0
        state.shared_x_range.start = requested_start
        state.shared_x_range.end = requested_end

        rebalance_layout()
        refresh_plot_ranges()
        sync_optimization_cutoff_marker(test_started_at=started_at, test_ended_at=ended_at)

        summary = result.summary
        set_equity_summary_overlay(summary)
        completed_at = datetime.now(UTC)
        summary_div.text = (
            f"<p>Distance test completed at <b>{completed_at:%Y-%m-%d %H:%M:%S} UTC</b> for "
            f"<b>{summary['symbol_1']}</b> / <b>{summary['symbol_2']}</b> on <b>{timeframe_select.value}</b>. "
            f"Test period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>. "
            f"Trades: <b>{int(summary.get('trades', 0) or 0)}</b>, Net: <b>{float(summary.get('net_pnl', 0.0) or 0.0):.2f}</b>.</p>"
        )
        return True

    def refresh_instruments() -> None:
        selected_group = group_select.value
        options = instrument_options_for_group(selected_group)
        symbol_1_select.options = options
        symbol_1_select.value = _preferred_symbol_1(options, symbol_1_select.value)
        sync_symbol_2_filter(base_options=options, show_message=leg_2_filter_select.value == 'cointegrated_only')
        if leg_2_filter_select.value != 'cointegrated_only':
            visible_count = len([option for option in options if option != INSTRUMENT_PLACEHOLDER])
            summary_div.text = f"<p>Loaded {visible_count} instruments for group '{selected_group}'.</p>"

    def on_group_change(_attr: str, _old: object, _new: object) -> None:
        refresh_instruments()

    def on_symbol_1_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')

    def on_leg2_filter_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')

    def on_leg2_cointegration_kind_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')

    def on_timeframe_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')
        restore_saved_scan_table(show_message=True)

    def on_scan_universe_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_scan_table(show_message=True)

    def on_scan_kind_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_scan_table(show_message=True)

    def on_bybit_fee_mode_change(_attr: str, _old: object, new: object) -> None:
        sync_bybit_fee_mode()
        summary_div.text = f"<p>Bybit fee mode set to <b>{new}</b>. Next test and optimization runs will use this commission model.</p>"

    def on_optimization_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()

    def on_stop_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_stop_mode_ui()

    def on_opt_stop_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()

    def on_optimization_range_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()

    def on_section_visibility_change(_attr: str, _old: object, _new: object) -> None:
        rebalance_layout()
        refresh_plot_ranges()

    def on_range_change(_attr: str, _old: object, _new: object) -> None:
        refresh_plot_ranges()

    def on_trade_selection(_attr: str, _old: object, _new: object) -> None:
        indices = state.trades_source.selected.indices
        if not indices:
            clear_trade_highlights()
            return

        index = indices[0]
        data = state.trades_source.data
        entry_time = data["entry_time"][index]
        exit_time = data["exit_time"][index]
        entry_price_1 = data["entry_price_1"][index]
        exit_price_1 = data["exit_price_1"][index]
        entry_price_2 = data["entry_price_2"][index]
        exit_price_2 = data["exit_price_2"][index]

        state.selected_trade_markers_1.data = {"time": [entry_time, exit_time], "price": [entry_price_1, exit_price_1]}
        state.selected_trade_markers_2.data = {"time": [entry_time, exit_time], "price": [entry_price_2, exit_price_2]}
        state.selected_trade_segments_1.data = {"x0": [entry_time], "y0": [entry_price_1], "x1": [exit_time], "y1": [exit_price_1]}
        state.selected_trade_segments_2.data = {"x0": [entry_time], "y0": [entry_price_2], "x1": [exit_time], "y1": [exit_price_2]}

    def on_optimization_selection(_attr: str, _old: object, _new: object) -> None:
        indices = state.optimization_source.selected.indices
        if not indices:
            return

        tester_period = (
            _coerce_datetime(period_slider.value[0]),
            _coerce_datetime(period_slider.value[1]),
        )
        index = indices[0]
        data = state.optimization_source.data
        lookback_input.value = int(data["lookback_bars"][index])
        entry_input.value = float(data["entry_z"][index])
        exit_input.value = float(data["exit_z"][index])
        raw_stop = data["stop_z"][index]
        if raw_stop in (None, ""):
            stop_mode_select.value = "disabled"
        else:
            stop_mode_select.value = "enabled"
            stop_input.value = float(raw_stop)
        bollinger_input.value = float(data["bollinger_k"][index])
        ran = apply_backtest_result(period_override=tester_period)
        if ran:
            optimization_status_div.text = (
                f"<p>Trial <b>{data['trial_id'][index]}</b> copied into tester inputs and executed on tester period "
                f"<b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. <b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

    def on_scan_selection(_attr: str, _old: object, _new: object) -> None:
        indices = state.scan_source.selected.indices
        if not indices:
            return
        tester_period = (
            _coerce_datetime(period_slider.value[0]),
            _coerce_datetime(period_slider.value[1]),
        )
        index = indices[0]
        data = state.scan_source.data
        symbol_1 = data["symbol_1"][index]
        symbol_2 = data["symbol_2"][index]
        options = _merge_symbol_options(list(symbol_1_select.options), symbol_1, symbol_2)
        symbol_1_select.options = options
        symbol_2_select.options = options
        symbol_1_select.value = symbol_1
        symbol_2_select.value = symbol_2
        ran = apply_backtest_result(period_override=tester_period)
        if ran:
            scan_status_div.text = (
                f"<p>Pair copied from cointegration scan and tested immediately on tester period "
                f"<b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. <b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>: "
                f"<b>{symbol_1}</b> / <b>{symbol_2}</b>.</p>"
            )
        else:
            scan_status_div.text = f"<p>Pair copied from cointegration scan: <b>{symbol_1}</b> / <b>{symbol_2}</b>. Test did not run because aligned data is missing.</p>"

    def on_run_test() -> None:
        try:
            apply_backtest_result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            clear_backtest_outputs(f"<p>Test run failed: {exc}</p>")

    def run_optimization_job(
        mode_value: str,
        pair: PairSelection,
        timeframe: Timeframe,
        started_at: datetime,
        ended_at: datetime,
        defaults: StrategyDefaults,
        search_space: dict[str, dict[str, float | int]],
        objective_metric: str,
        config: dict[str, float | int] | None,
    ) -> tuple[DistanceOptimizationResult, str, PairSelection]:
        optimization_progress["completed"] = 0
        optimization_progress["total"] = 0
        optimization_progress["stage"] = "Preparing optimization"
        if mode_value == OptimizationMode.GENETIC.value:
            result = optimize_distance_genetic(
                broker=broker,
                pair=pair,
                timeframe=timeframe,
                started_at=started_at,
                ended_at=ended_at,
                defaults=defaults,
                search_space=search_space,
                objective_metric=objective_metric,
                config=config,
                cancel_check=optimization_cancel_event.is_set,
                parallel_workers=optimizer_parallel_workers,
                progress_callback=update_optimization_progress,
            )
            return result, "genetic", pair

        result = optimize_distance_grid(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
            defaults=defaults,
            search_space=search_space,
            objective_metric=objective_metric,
            cancel_check=optimization_cancel_event.is_set,
            parallel_workers=optimizer_parallel_workers,
            progress_callback=update_optimization_progress,
        )
        return result, "grid", pair

    def clear_optimization_poll_callback() -> None:
        nonlocal optimization_poll_callback
        if optimization_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(optimization_poll_callback)
        except ValueError:
            pass
        optimization_poll_callback = None

    def poll_optimization_future() -> None:
        nonlocal optimization_future
        if optimization_future is None:
            return
        if not optimization_future.done():
            render_optimization_progress()
            return

        future = optimization_future
        optimization_future = None
        clear_optimization_poll_callback()
        reset_optimization_button()

        try:
            result, mode_label, pair = future.result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            optimization_status_div.text = f"<p>Optimization failed: {exc}</p>"
            return

        complete_optimization(result, mode_label, pair)

    def on_run_optimization() -> None:
        nonlocal optimization_future, optimization_poll_callback

        if optimization_future is not None and not optimization_future.done():
            if optimization_cancel_event.is_set():
                optimization_status_div.text = "<p>Stop already requested. Waiting for the current trial to finish.</p>"
                return
            optimization_cancel_event.set()
            mark_optimization_running(stopping=True)
            optimization_status_div.text = "<p>Stop requested. Waiting for the current trial to finish.</p>"
            return

        if optimization_future is not None and optimization_future.done():
            poll_optimization_future()

        if algorithm_select.value != "distance":
            optimization_status_div.text = "<p>Only Distance optimization is wired right now.</p>"
            return

        pair = current_pair()
        if pair is None:
            optimization_status_div.text = "<p>Choose two different valid instruments before optimization.</p>"
            return

        started_at = _coerce_datetime(optimization_period_slider.value[0])
        ended_at = _coerce_datetime(optimization_period_slider.value[1])
        search_space = optimization_search_space()
        defaults = build_defaults()
        timeframe = Timeframe(timeframe_select.value)
        objective_metric = optimization_objective_select.value
        config = genetic_optimizer_config() if optimization_mode_select.value == OptimizationMode.GENETIC.value else None

        valid_trial_count = count_distance_parameter_grid(search_space)
        optimization_cancel_event.clear()
        optimization_progress["completed"] = 0
        optimization_progress["total"] = valid_trial_count
        optimization_progress["stage"] = "Preparing optimization"
        mark_optimization_running()
        mode_label = "genetic" if optimization_mode_select.value == OptimizationMode.GENETIC.value else "grid"
        auto_suffix = ""
        if optimization_mode_select.value != OptimizationMode.GENETIC.value and optimization_range_mode_select.value == "auto":
            auto_target = max(1, int(_read_spinner_value(auto_grid_trials_input, 500, cast=int)))
            auto_suffix = f" Auto target: <b>{auto_target}</b>."
        optimization_status_div.text = (
            f"<p>Running {mode_label} optimization for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
            f"Valid combinations: {valid_trial_count}.{auto_suffix} Workers: <b>{optimizer_parallel_workers}</b>. Click the button again to stop.</p>"
        )
        optimization_future = optimization_executor.submit(
            run_optimization_job,
            optimization_mode_select.value,
            pair,
            timeframe,
            started_at,
            ended_at,
            defaults,
            search_space,
            objective_metric,
            config,
        )
        if optimization_poll_callback is None:
            optimization_poll_callback = doc.add_periodic_callback(poll_optimization_future, 250)

    def run_scan_job(
        timeframe: Timeframe,
        started_at: datetime,
        ended_at: datetime,
        universe_mode: ScanUniverseMode,
        normalized_group: str | None,
        unit_root_test_value: str,
        det_order: int,
        k_ar_diff: int,
        significance_level: float,
    ) -> JohansenUniverseScanResult:
        scan_progress["completed"] = 0
        scan_progress["total"] = 0
        scan_progress["stage"] = "Preparing Johansen scan"
        return scan_universe_johansen(
            broker=broker,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
            universe_mode=universe_mode,
            normalized_group=normalized_group,
            unit_root_gate=UnitRootGate(test=UnitRootTest(unit_root_test_value)),
            params=JohansenScanParameters(
                det_order=det_order,
                k_ar_diff=k_ar_diff,
                significance_level=significance_level,
            ),
            progress_callback=update_scan_progress,
            parallel_workers=scan_parallel_workers,
        )

    def clear_scan_poll_callback() -> None:
        nonlocal scan_poll_callback
        if scan_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(scan_poll_callback)
        except ValueError:
            pass
        scan_poll_callback = None

    def poll_scan_future() -> None:
        nonlocal scan_future, scan_request_context
        if scan_future is None:
            return
        if not scan_future.done():
            render_scan_progress()
            return

        future = scan_future
        scan_future = None
        clear_scan_poll_callback()
        reset_scan_button()

        try:
            scan_result = future.result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            scan_status_div.text = f"<p>Johansen scan failed: {exc}</p>"
            return

        storage_summary = ''
        if scan_request_context is not None:
            saved_paths = persist_johansen_scan_result(
                broker=broker,
                timeframe=scan_request_context['timeframe'],
                started_at=scan_request_context['started_at'],
                ended_at=scan_request_context['ended_at'],
                universe_mode=scan_request_context['universe_mode'],
                normalized_group=scan_request_context['normalized_group'],
                symbols=None,
                result=scan_result,
            )
            storage_summary = f" Saved passed pairs to <b>{saved_paths['latest_passed_pairs']}</b>."
        state.scan_source.data = scan_results_to_source(scan_result, passed_only=True)
        state.scan_source.selected.indices = []
        summary = scan_result.summary
        scan_status_div.text = (
            f"<p>Scanned {summary.total_symbols_requested} symbols. Loaded: {summary.loaded_symbols}, "
            f"I(1): {summary.prefiltered_i1_symbols}, Pairs: {summary.total_pairs_evaluated}, "
            f"Passed: {summary.threshold_passed_pairs}.{storage_summary}</p>"
        )
        if leg_2_filter_select.value == 'cointegrated_only' and leg_2_cointegration_kind_select.value == scan_kind_select.value:
            sync_symbol_2_filter(show_message=False)
        scan_request_context = None

    def on_run_scan() -> None:
        nonlocal scan_future, scan_poll_callback, scan_request_context

        if scan_future is not None and not scan_future.done():
            render_scan_progress()
            return

        if scan_future is not None and scan_future.done():
            poll_scan_future()

        if scan_kind_select.value != 'johansen':
            snapshot = restore_saved_scan_table(show_message=True)
            if snapshot is None:
                scan_status_div.text = (
                    f"<p>No saved <b>{scan_kind_select.value}</b> table is available for the selected universe and timeframe, "
                    f"and running a new scan is currently wired only for <b>johansen</b>.</p>"
                )
            else:
                scan_status_div.text += ' <p>Running a new scan is currently wired only for <b>johansen</b>.</p>'
            return

        started_at = _coerce_datetime(period_slider.value[0])
        ended_at = _coerce_datetime(period_slider.value[1])
        universe_mode, normalized_group = current_scan_selection_scope()

        scan_progress["completed"] = 0
        scan_progress["total"] = 0
        scan_progress["stage"] = "Preparing Johansen scan"
        scan_request_context = {
            'timeframe': Timeframe(timeframe_select.value),
            'started_at': started_at,
            'ended_at': ended_at,
            'universe_mode': universe_mode,
            'normalized_group': normalized_group,
        }
        mark_scan_running()
        scan_status_div.text = f"<p>Starting Johansen scan on <b>{scan_parallel_workers}</b> workers...</p>"
        scan_future = scan_executor.submit(
            run_scan_job,
            Timeframe(timeframe_select.value),
            started_at,
            ended_at,
            universe_mode,
            normalized_group,
            scan_unit_root_select.value,
            int(_read_spinner_value(scan_det_order_input, 0, cast=int)),
            int(_read_spinner_value(scan_k_ar_diff_input, 1, cast=int)),
            float(scan_significance_select.value),
        )
        if scan_poll_callback is None:
            scan_poll_callback = doc.add_periodic_callback(poll_scan_future, 250)

    def on_reset_defaults() -> None:
        if optimization_future is not None and not optimization_future.done():
            optimization_cancel_event.set()
            mark_optimization_running(stopping=True)
            optimization_status_div.text = "<p>Stop requested before reset. Waiting for the current trial to finish.</p>"
            return
        if scan_future is not None and not scan_future.done():
            scan_status_div.text = "<p>Johansen scan is still running. Wait for it to finish before reset.</p>"
            return

        file_state_controller.clear()
        group_select.value = GROUP_OPTIONS[0]
        timeframe_select.value = Timeframe.M15.value
        period_slider.value = (_ui_datetime(year_start), _ui_datetime(now_utc))
        leg_2_filter_select.value = 'all_symbols'
        leg_2_cointegration_kind_select.value = COINTEGRATION_KIND_OPTIONS[0]
        algorithm_select.value = "distance"
        capital_input.value = 10_000.0
        leverage_input.value = 100.0
        margin_budget_input.value = 500.0
        slippage_input.value = 1.0
        bybit_fee_mode_select.value = settings.bybit_tradfi_fee_mode
        stop_mode_select.value = "enabled"
        lookback_input.value = 96
        entry_input.value = 2.0
        exit_input.value = 0.5
        stop_input.value = 3.5
        bollinger_input.value = 2.0
        optimization_mode_select.value = OptimizationMode.GRID.value
        optimization_objective_select.value = "net_profit"
        optimization_range_mode_select.value = "manual"
        auto_grid_trials_input.value = 500
        opt_stop_mode_select.value = "enabled"
        optimization_period_slider.value = (_ui_datetime(year_start), _ui_datetime(now_utc))
        opt_lookback_start.value = 48
        opt_lookback_stop.value = 144
        opt_lookback_step.value = 24
        opt_entry_start.value = 1.5
        opt_entry_stop.value = 2.5
        opt_entry_step.value = 0.5
        opt_exit_start.value = 0.3
        opt_exit_stop.value = 0.7
        opt_exit_step.value = 0.2
        opt_stop_start.value = 3.0
        opt_stop_stop.value = 4.0
        opt_stop_step.value = 0.5
        opt_bollinger_start.value = 1.5
        opt_bollinger_stop.value = 2.5
        opt_bollinger_step.value = 0.5
        genetic_population_input.value = 24
        genetic_generations_input.value = 12
        genetic_elite_input.value = 4
        genetic_mutation_input.value = 0.25
        genetic_seed_input.value = 17
        scan_universe_select.value = GROUP_OPTIONS[0]
        scan_kind_select.value = COINTEGRATION_KIND_OPTIONS[0]
        scan_unit_root_select.value = UnitRootTest.ADF.value
        scan_significance_select.value = "0.05"
        scan_det_order_input.value = 0
        scan_k_ar_diff_input.value = 1

        for _key, _plot, _source, _columns, body, toggle in plot_bindings:
            set_section_visibility(body, toggle, True)
        for _key, body, toggle in block_bindings:
            set_section_visibility(body, toggle, True)
        set_section_visibility(service_log_body, service_log_toggle, False)

        refresh_instruments()
        restore_saved_scan_table(show_message=False)
        sync_optimization_mode_ui()
        sync_stop_mode_ui()
        reset_optimization_button()
        reset_scan_button()
        rebalance_layout()
        refresh_plot_ranges()
        file_state_controller.persist()
        summary_div.text = f"<p>Settings reset to defaults and saved to <b>{file_state_controller.state_path}</b>.</p>"

    def on_session_destroyed(_session_context: object) -> None:
        optimization_cancel_event.set()
        clear_optimization_poll_callback()
        clear_scan_poll_callback()
        optimization_executor.shutdown(wait=False, cancel_futures=True)
        scan_executor.shutdown(wait=False, cancel_futures=True)

    refresh_button.on_click(refresh_instruments)
    reset_defaults_button.on_click(on_reset_defaults)
    group_select.on_change("value", on_group_change)
    symbol_1_select.on_change("value", on_symbol_1_change)
    leg_2_filter_select.on_change("value", on_leg2_filter_change)
    leg_2_cointegration_kind_select.on_change("value", on_leg2_cointegration_kind_change)
    timeframe_select.on_change("value", on_timeframe_change)
    scan_universe_select.on_change("value", on_scan_universe_change)
    scan_kind_select.on_change("value", on_scan_kind_change)
    bybit_fee_mode_select.on_change("value", on_bybit_fee_mode_change)
    stop_mode_select.on_change("value", on_stop_mode_change)
    optimization_mode_select.on_change("value", on_optimization_mode_change)
    optimization_range_mode_select.on_change("value", on_optimization_range_mode_change)
    opt_stop_mode_select.on_change("value", on_opt_stop_mode_change)
    for _key, _plot, _source, _columns, body, toggle in plot_bindings:
        toggle.on_click(build_section_toggle_handler(body, toggle))
        body.on_change("visible", on_section_visibility_change)
    for _key, body, toggle in block_bindings:
        toggle.on_click(build_section_toggle_handler(body, toggle))
        body.on_change("visible", on_section_visibility_change)
    service_log_toggle.on_click(toggle_service_log)
    service_log_body.on_change("visible", lambda _attr, _old, _new: sync_service_log_toggle())
    summary_div.on_change("text", build_service_log_handler("tester"))
    optimization_status_div.on_change("text", build_service_log_handler("optimizer"))
    scan_status_div.on_change("text", build_service_log_handler("cointegration"))
    state.shared_x_range.on_change("start", on_range_change)
    state.shared_x_range.on_change("end", on_range_change)
    state.trades_source.selected.on_change("indices", on_trade_selection)
    state.optimization_source.selected.on_change("indices", on_optimization_selection)
    state.scan_source.selected.on_change("indices", on_scan_selection)
    run_button.on_click(on_run_test)
    optimization_run_button.on_click(on_run_optimization)
    scan_run_button.on_click(on_run_scan)

    top_toggle_bar = row(
        price_1_toggle,
        price_2_toggle,
        spread_toggle,
        zscore_toggle,
        equity_toggle,
        trades_toggle,
        optimization_toggle,
        scan_toggle,
        sizing_mode="stretch_width",
    )

    right_panel = column(
        top_toggle_bar,
        price_1_section,
        price_2_section,
        spread_section,
        zscore_section,
        equity_section,
        trades_block,
        optimization_block,
        scan_block,
        sizing_mode="stretch_both",
    )

    main_row = row(sidebar, right_panel, sizing_mode="stretch_both")
    root = column(main_row, service_log_body, service_log_toggle, sizing_mode="stretch_both")
    doc.title = "MT Pair Tester"
    doc.on_session_destroyed(on_session_destroyed)
    doc.add_root(root)
    file_state_controller.restore()
    refresh_instruments()
    file_state_controller.restore()
    sync_symbol_2_filter(show_message=False)
    file_state_controller.restore()
    restore_saved_scan_table(show_message=False)
    sync_bybit_fee_mode()
    sync_optimization_mode_ui()
    sync_stop_mode_ui()
    sync_service_log_toggle()
    append_service_log("tester", summary_div.text)
    append_service_log("optimizer", optimization_status_div.text)
    append_service_log("cointegration", scan_status_div.text)
    rebalance_layout()
    refresh_plot_ranges()
    file_state_controller.install_model_watchers()
    file_state_controller.persist()


build_document()

