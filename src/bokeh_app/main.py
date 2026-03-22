from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from html import unescape
import os
import re
from threading import Event

import polars as pl
from bokeh.events import DocumentReady, MouseLeave, MouseMove
from bokeh.layouts import column, row
from bokeh.models import (
    BoxAnnotation,
    Button,
    ColumnDataSource,
    CustomJSTickFormatter,
    DataTable,
    DateFormatter,
    DatetimeTickFormatter,
    DatePicker,
    DateRangeSlider,
    Div,
    CustomJS,
    HTMLTemplateFormatter,
    Label,
    LinearAxis,
    NumberFormatter,
    Spacer,
    PreText,
    FixedTicker,
    MonthsTicker,
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
    WfaWindowUnit,
)
from domain.data.io import load_instrument_catalog_frame
from domain.optimizer import DistanceOptimizationResult, OBJECTIVE_METRICS, count_distance_parameter_grid, optimize_distance_genetic, optimize_distance_grid
from domain.scan.johansen import JohansenScanParameters, JohansenUniverseScanResult, scan_universe_johansen
from domain.wfa import run_distance_genetic_wfa
from domain.meta_selector import DEFAULT_META_TARGET, SUPPORTED_META_MODELS, run_meta_selector
from storage.paths import ui_state_path
from tools.mt5_export_catalog_sync import build_jobs, chunked, resolve_symbols, symbol_partitions_exist
from tools.mt5_terminal_export_sync import decode_exports, default_common_root, read_export_statuses, run_terminal_export, write_job_manifest, write_startup_config
from storage.scan_results import (
    COINTEGRATION_KIND_OPTIONS,
    load_latest_saved_scan_result,
    partner_symbols_from_snapshot,
    persist_johansen_scan_result,
)
from storage.wfa_results import load_wfa_run_snapshot

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
DOWNLOAD_SCOPE_OPTIONS = ["symbol", "group"]
DOWNLOAD_POLICY_OPTIONS = ["missing_only", "force"]
WFA_UNIT_OPTIONS = [item.value for item in WfaWindowUnit]
DEFAULT_MT5_TERMINAL_PATH = r"C:\Program Files\Bybit MT5 Terminal\terminal64.exe"
def _build_figure(title: str, shared_x_range: Range1d, ylabel: str, height: int) -> figure:
    plot = figure(
        title=title,
        x_range=shared_x_range,
        y_range=Range1d(start=0.0, end=1.0),
        height=height,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    wheel_zoom = plot.select_one(WheelZoomTool)
    if wheel_zoom is not None:
        wheel_zoom.modifiers = {"ctrl": True}
    plot.toolbar.autohide = True
    plot.yaxis.axis_label = ylabel
    plot.xaxis.visible = False
    return plot


def _widget_with_help(widget, message: str, *, width: int | None = None):
    if width is not None:
        widget.width = width
    widget.tags = [*getattr(widget, "tags", []), f"tooltip::{message}"]
    return widget


def _build_equity_summary_columns(summary: dict[str, float | int | str]) -> list[str]:
    initial_capital = float(summary.get('initial_capital', 0.0) or 0.0)
    ending_equity = float(summary.get('ending_equity', 0.0) or 0.0)
    net_pnl = float(summary.get('net_pnl', 0.0) or 0.0)
    peak_equity = float(summary.get('peak_equity', 0.0) or 0.0)
    max_drawdown = float(summary.get('max_drawdown', 0.0) or 0.0)
    if abs(initial_capital) <= 1e-12:
        initial_capital = ending_equity - net_pnl
    net_pct = (net_pnl / initial_capital * 100.0) if abs(initial_capital) > 1e-12 else 0.0
    max_dd_pct = (max_drawdown / peak_equity * 100.0) if abs(peak_equity) > 1e-12 else 0.0
    win_rate = float(summary.get('win_rate', 0.0) or 0.0) * 100.0
    columns = [
        (
            f"{summary['symbol_1']} / {summary['symbol_2']}",
            f"Trades: {int(summary.get('trades', 0) or 0)}",
        ),
        (
            f"Gross: {float(summary.get('gross_pnl', 0.0) or 0.0):.2f}",
            f"Spread: {float(summary.get('total_spread_cost', 0.0) or 0.0):.2f}",
        ),
        (
            f"Slip: {float(summary.get('total_slippage_cost', 0.0) or 0.0):.2f}",
            f"Comm: {float(summary.get('total_commission', 0.0) or 0.0):.2f}",
        ),
        (
            f"Net: {net_pnl:.2f} ({net_pct:.1f}%)",
            f"Ending: {ending_equity:.2f}",
        ),
        (
            f"Max DD: {max_drawdown:.2f} ({max_dd_pct:.1f}%)",
            f"Win: {win_rate:.1f}%",
        ),
        (
            f"Cap: {initial_capital:.2f}",
            f"Peak: {peak_equity:.2f}",
        ),
    ]
    return ["\n".join(column) for column in columns]


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
    history_start = datetime(2024, 1, 1, tzinfo=UTC)
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
    download_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mt5-download")
    download_future: Future[dict[str, object]] | None = None
    download_poll_callback: object | None = None
    download_progress = {"completed": 0, "total": 0, "stage": "Idle"}
    wfa_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="distance-wfa")
    wfa_future: Future[dict[str, object]] | None = None
    wfa_poll_callback: object | None = None
    wfa_progress = {"completed": 0, "total": 0, "stage": "Idle"}
    wfa_live_state: dict[str, object] = {"version": 0, "applied_version": 0, "result": None}
    meta_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="meta-selector")
    meta_future: Future[tuple[dict[str, object], PairSelection]] | None = None
    meta_poll_callback: object | None = None
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
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
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
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
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

    optimization_control_wrappers = {}
    optimization_tooltips: list[tuple[object, str]] = []

    def optimizer_help(widget, message: str, *, width: int | None = None):
        wrapper = _widget_with_help(widget, message, width=width)
        optimization_control_wrappers[widget] = wrapper
        optimization_tooltips.append((widget, message))
        return wrapper

    optimization_mode_control = optimizer_help(optimization_mode_select, "Optimization engine. grid enumerates the parameter grid. genetic uses evolutionary search and is usually faster on large search spaces.", width=130)
    optimization_objective_control = optimizer_help(optimization_objective_select, "Metric to maximize. ending_equity = final account equity after spread, slippage and commission. net_profit = ending_equity minus initial capital. All optimizer metrics are post-cost.", width=190)
    optimization_range_mode_control = optimizer_help(optimization_range_mode_select, "manual uses your explicit step values. auto derives steps between start and stop to reach roughly the target number of grid trials.", width=132)
    auto_grid_trials_control = optimizer_help(auto_grid_trials_input, "Target number of runs for auto-grid. Used only when Grid Steps = auto.", width=170)
    optimization_period_control = optimizer_help(optimization_period_slider, "Period used by the optimizer. It is separate from the normal tester Test Period.", width=420)
    opt_lookback_start_control = optimizer_help(opt_lookback_start, "Minimum lookback window in bars for spread and z-score calculations.", width=160)
    opt_lookback_stop_control = optimizer_help(opt_lookback_stop, "Maximum lookback window in bars.", width=160)
    opt_lookback_step_control = optimizer_help(opt_lookback_step, "Lookback increment in manual-grid mode.", width=160)
    opt_entry_start_control = optimizer_help(opt_entry_start, "Minimum Entry Z threshold. A trade opens when absolute z-score reaches this level.", width=160)
    opt_entry_stop_control = optimizer_help(opt_entry_stop, "Maximum Entry Z threshold.", width=160)
    opt_entry_step_control = optimizer_help(opt_entry_step, "Entry Z increment in manual-grid mode.", width=160)
    opt_exit_start_control = optimizer_help(opt_exit_start, "Minimum Exit Z threshold. A trade closes when z-score reverts toward this level.", width=160)
    opt_exit_stop_control = optimizer_help(opt_exit_stop, "Maximum Exit Z threshold.", width=160)
    opt_exit_step_control = optimizer_help(opt_exit_step, "Exit Z increment in manual-grid mode.", width=160)
    opt_stop_mode_control = optimizer_help(opt_stop_mode_select, "Enable or disable the statistical stop. disabled means no emergency exit by Stop Z.", width=160)
    opt_stop_start_control = optimizer_help(opt_stop_start, "Minimum Stop Z. This is a statistical stop on spread divergence, not a plain price stop-loss.", width=160)
    opt_stop_stop_control = optimizer_help(opt_stop_stop, "Maximum Stop Z.", width=160)
    opt_stop_step_control = optimizer_help(opt_stop_step, "Stop Z increment in manual-grid mode.", width=160)
    opt_bollinger_start_control = optimizer_help(opt_bollinger_start, "Minimum Bollinger K multiplier for z-score bands.", width=158)
    opt_bollinger_stop_control = optimizer_help(opt_bollinger_stop, "Maximum Bollinger K multiplier.", width=158)
    opt_bollinger_step_control = optimizer_help(opt_bollinger_step, "Bollinger K increment in manual-grid mode.", width=158)
    genetic_population_control = optimizer_help(genetic_population_input, "Population size for genetic search. Larger populations usually improve search quality but increase runtime.", width=158)
    genetic_generations_control = optimizer_help(genetic_generations_input, "Number of generations in genetic search.", width=158)
    genetic_elite_control = optimizer_help(genetic_elite_input, "How many top candidates are copied into the next generation unchanged.", width=158)
    genetic_mutation_control = optimizer_help(genetic_mutation_input, "Mutation rate in genetic search. Too low can stagnate, too high can become noisy.", width=158)
    genetic_seed_control = optimizer_help(genetic_seed_input, "Random seed for reproducible genetic runs.", width=158)

    price_1 = _build_figure("Price 1", state.shared_x_range, "Price", 440)
    price_1.line("x", "price", source=state.price_1_source, line_width=2, color="#10b981")
    price_1.scatter(
        "x",
        "price",
        source=state.trade_markers_1,
        size=9,
        marker="marker",
        color=factor_cmap("event", palette=["#10b981", "#ef4444"], factors=["entry", "exit"]),
    )
    price_1.segment("x0", "y0", "x1", "y1", source=state.trade_segments_1, line_color="#f59e0b", line_width=2)
    price_1.segment("x0", "y0", "x1", "y1", source=state.selected_trade_segments_1, line_color="#111827", line_width=4, line_alpha=0.9)
    price_1.scatter("x", "price", source=state.selected_trade_markers_1, size=12, marker="diamond", color="#111827", line_color="#f8fafc", line_width=1.2)

    price_2 = _build_figure("Price 2", state.shared_x_range, "Price", 440)
    price_2.line("x", "price", source=state.price_2_source, line_width=2, color="#2563eb")
    price_2.scatter(
        "x",
        "price",
        source=state.trade_markers_2,
        size=9,
        marker="marker",
        color=factor_cmap("event", palette=["#10b981", "#ef4444"], factors=["entry", "exit"]),
    )
    price_2.segment("x0", "y0", "x1", "y1", source=state.trade_segments_2, line_color="#f59e0b", line_width=2)
    price_2.segment("x0", "y0", "x1", "y1", source=state.selected_trade_segments_2, line_color="#111827", line_width=4, line_alpha=0.9)
    price_2.scatter("x", "price", source=state.selected_trade_markers_2, size=12, marker="diamond", color="#111827", line_color="#f8fafc", line_width=1.2)

    spread_plot = _build_figure("Spread / Residual", state.shared_x_range, "Spread", 360)
    spread_plot.line("x", "spread", source=state.spread_source, line_width=2, color="#6366f1")

    zscore_plot = _build_figure("Z-score + Bollinger", state.shared_x_range, "Z-score", 360)
    zscore_plot.line("x", "zscore", source=state.zscore_source, line_width=2, color="#f97316")
    zscore_plot.line("x", "upper", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")
    zscore_plot.line("x", "lower", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")

    equity_plot = _build_figure("Equity", state.shared_x_range, "Unrealized DD", 440)
    equity_plot.extra_y_ranges = {"equity": Range1d(start=0.0, end=1.0)}
    equity_plot.add_layout(LinearAxis(y_range_name="equity", axis_label="Equity"), "right")
    equity_plot.vbar(
        x="x",
        width="drawdown_width",
        top="drawdown_top",
        bottom="drawdown",
        source=state.equity_source,
        fill_color="#f97316",
        fill_alpha=0.24,
        line_alpha=0.0,
    )
    equity_plot.line("x", "total", source=state.equity_source, line_width=3, color="#ec4899", legend_label="Total Equity", y_range_name="equity")
    equity_plot.line("x", "leg1", source=state.equity_source, line_width=1.5, color="#22d3ee", legend_label="Leg 1 Equity", y_range_name="equity")
    equity_plot.line("x", "leg2", source=state.equity_source, line_width=1.5, color="#16a34a", legend_label="Leg 2 Equity", y_range_name="equity")
    equity_plot.add_layout(Span(location=0.0, dimension="width", line_color="#94a3b8", line_alpha=0.35, line_width=1.0))
    equity_plot.legend.location = "top_center"
    equity_plot.legend.background_fill_alpha = 0.0
    equity_plot.legend.border_line_alpha = 0.0
    equity_plot.legend.label_text_font_size = "11px"
    equity_plot.legend.orientation = "horizontal"
    equity_plot.legend.padding = 10
    equity_plot.legend.spacing = 16
    equity_plot.legend.margin = 8
    equity_plot.legend.click_policy = "hide"
    equity_summary_labels: list[Label] = []
    for _index in range(6):
        label = Label(
            x=18,
            y=44,
            x_units="screen",
            y_units="screen",
            text="",
            text_font="Consolas",
            text_font_size="11px",
            text_font_style="normal",
            text_color="#111827",
            text_align="left",
            text_baseline="bottom",
            background_fill_color="#ffffff",
            text_line_height=1.45,
            padding=4,
            background_fill_alpha=0.0,
            border_line_color="#94a3b8",
            border_line_alpha=0.0,
            visible=False,
        )
        equity_summary_labels.append(label)
        equity_plot.add_layout(label)
    equity_hover_label = Label(
        x=0,
        y=0,
        x_units="screen",
        y_units="screen",
        text="",
        text_font="Consolas",
        text_font_size="10px",
        text_font_style="normal",
        text_color="#111827",
        text_align="left",
        text_baseline="top",
        background_fill_color="#ffffff",
        background_fill_alpha=0.08,
        border_line_color="#94a3b8",
        border_line_alpha=0.0,
        text_line_height=1.35,
        padding=6,
        visible=False,
    )
    equity_plot.add_layout(equity_hover_label)
    equity_hover_callback = CustomJS(args=dict(source=state.equity_source, label=equity_hover_label), code="""
        const xs = source.data.x || [];
        if (!xs.length || cb_obj.x == null || Number.isNaN(cb_obj.x)) {
            label.visible = false;
            return;
        }
        const index = Math.max(0, Math.min(xs.length - 1, Math.round(cb_obj.x)));
        const times = source.data.time || [];
        const total = source.data.total || [];
        const leg1 = source.data.leg1 || [];
        const leg2 = source.data.leg2 || [];
        const pad = (value) => String(value).padStart(2, '0');
        const moment = new Date(times[index]);
        const stamp = `${moment.getFullYear()}-${pad(moment.getMonth() + 1)}-${pad(moment.getDate())} ${pad(moment.getHours())}:${pad(moment.getMinutes())}`;
        const fmt = (value) => Number.isFinite(value) ? value.toFixed(2) : 'na';
        label.text = `${stamp}
T ${fmt(total[index])}  L1 ${fmt(leg1[index])}  L2 ${fmt(leg2[index])}`;
        label.visible = true;
    """)
    equity_plot.js_on_event(MouseMove, equity_hover_callback)
    equity_plot.js_on_event(MouseLeave, CustomJS(args=dict(label=equity_hover_label), code="label.visible = false;"))

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
        reorderable=False,
        index_position=None,
    )

    def build_optimization_table_columns(show_stop_z: bool) -> list[TableColumn]:
        columns = [
            TableColumn(field="net_profit", title="Net", formatter=NumberFormatter(format="0.00"), width=92),
            TableColumn(field="max_drawdown", title="Max DD", formatter=NumberFormatter(format="0.00"), width=92),
            TableColumn(field="trades", title="Trades", formatter=NumberFormatter(format="0"), width=68),
            TableColumn(field="lookback_bars", title="Lookback", formatter=NumberFormatter(format="0"), width=82),
            TableColumn(field="omega_ratio", title="Omega", formatter=NumberFormatter(format="0.000"), width=78),
            TableColumn(field="k_ratio", title="K", formatter=NumberFormatter(format="0.000"), width=70),
            TableColumn(field="score_log_trades", title="Score", formatter=NumberFormatter(format="0.000"), width=84),
            TableColumn(field="ending_equity", title="Ending", formatter=NumberFormatter(format="0.00"), width=90),
            TableColumn(field="pnl_to_maxdd", title="PnL/DD", formatter=NumberFormatter(format="0.000"), width=82),
            TableColumn(field="ulcer_index", title="Ulcer", formatter=NumberFormatter(format="0.0000"), width=82),
            TableColumn(field="ulcer_performance", title="UPI", formatter=NumberFormatter(format="0.000"), width=76),
            TableColumn(field="entry_z", title="Entry Z", formatter=NumberFormatter(format="0.0"), width=70),
            TableColumn(field="exit_z", title="Exit Z", formatter=NumberFormatter(format="0.0"), width=66),
        ]
        if show_stop_z:
            columns.append(TableColumn(field="stop_z_label", title="Stop Z", width=72))
        columns.extend([
            TableColumn(field="bollinger_k", title="Boll K", formatter=NumberFormatter(format="0.0"), width=72),
            TableColumn(field="win_rate", title="Win", formatter=NumberFormatter(format="0.000"), width=66),
            TableColumn(field="gross_profit", title="Gross", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="spread_cost", title="Spread", formatter=NumberFormatter(format="0.00"), width=86),
            TableColumn(field="slippage_cost", title="Slip", formatter=NumberFormatter(format="0.00"), width=76),
            TableColumn(field="commission_cost", title="Comm", formatter=NumberFormatter(format="0.00"), width=82),
            TableColumn(field="total_cost", title="Costs", formatter=NumberFormatter(format="0.00"), width=82),
        ])
        return columns

    optimization_table = DataTable(
        source=state.optimization_source,
        columns=build_optimization_table_columns(show_stop_z=opt_stop_mode_select.value == "enabled"),
        sizing_mode="stretch_width",
        height=320,
        sortable=True,
        reorderable=False,
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
    scan_period_slider = DateRangeSlider(
        title="Scan Period",
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
        width=420,
    )
    scan_run_button = Button(label="Run Johansen Scan", button_type="primary", width=150)
    scan_status_div = Div(text="<p>Ready to scan the selected universe on its own period.</p>")

    download_scope_select = Select(title="Mode", value=DOWNLOAD_SCOPE_OPTIONS[0], options=DOWNLOAD_SCOPE_OPTIONS, width=110)
    download_group_select = Select(title="Group", value=GROUP_OPTIONS[0], options=GROUP_OPTIONS, width=150)
    download_symbol_select = Select(
        title="Symbol",
        value=INSTRUMENT_PLACEHOLDER,
        options=[INSTRUMENT_PLACEHOLDER],
        width=190,
    )
    download_policy_select = Select(title="Policy", value=DOWNLOAD_POLICY_OPTIONS[0], options=DOWNLOAD_POLICY_OPTIONS, width=135)
    download_period_slider = DateRangeSlider(
        title="Download Period",
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
        width=420,
    )
    download_run_button = Button(label="Run Download", button_type="primary", width=155)
    download_status_div = Div(text="<p>Ready to download canonical <b>M5</b> history from MT5 into parquet.</p>")

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
        reorderable=False,
        index_position=None,
    )

    trades_content = column(trades_table, sizing_mode="stretch_width")
    optimization_run_button.width = 172
    optimization_shared_controls = row(
        optimization_mode_control,
        optimization_objective_control,
        optimization_range_mode_control,
        auto_grid_trials_control,
        optimization_period_control,
        optimization_run_button,
        sizing_mode="stretch_width",
        styles={"gap": "18px", "align-items": "flex-end"},
    )
    optimization_lookback_controls = column(
        opt_lookback_start_control,
        opt_lookback_stop_control,
        opt_lookback_step_control,
        width=160,
    )
    optimization_entry_controls = column(
        opt_entry_start_control,
        opt_entry_stop_control,
        opt_entry_step_control,
        width=160,
    )
    optimization_exit_controls = column(
        opt_exit_start_control,
        opt_exit_stop_control,
        opt_exit_step_control,
        width=160,
    )
    optimization_stop_controls = column(
        opt_stop_mode_control,
        opt_stop_start_control,
        opt_stop_stop_control,
        opt_stop_step_control,
        width=160,
    )
    optimization_bollinger_controls = row(
        opt_bollinger_start_control,
        opt_bollinger_stop_control,
        opt_bollinger_step_control,
        sizing_mode="stretch_width",
        styles={"gap": "14px", "align-items": "flex-end"},
    )
    optimization_genetic_controls = row(
        genetic_population_control,
        genetic_generations_control,
        genetic_elite_control,
        genetic_mutation_control,
        genetic_seed_control,
        sizing_mode="stretch_width",
        styles={"gap": "14px", "align-items": "flex-end"},
    )
    optimization_tail_controls = column(
        optimization_bollinger_controls,
        optimization_genetic_controls,
        sizing_mode="stretch_width",
        styles={"gap": "12px"},
    )
    optimization_parameter_columns = row(
        optimization_lookback_controls,
        optimization_entry_controls,
        optimization_exit_controls,
        optimization_stop_controls,
        optimization_tail_controls,
        sizing_mode="stretch_width",
        styles={"gap": "18px", "align-items": "flex-start"},
    )
    optimization_controls = column(
        optimization_shared_controls,
        optimization_parameter_columns,
        optimization_status_div,
        sizing_mode="stretch_width",
        styles={"gap": "12px"},
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
            scan_period_slider,
            scan_run_button,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        scan_status_div,
        sizing_mode="stretch_width",
    )
    scan_content = column(scan_controls, scan_table, sizing_mode="stretch_width")
    download_controls = column(
        row(
            download_scope_select,
            download_group_select,
            download_symbol_select,
            download_policy_select,
            download_period_slider,
            download_run_button,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        download_status_div,
        sizing_mode="stretch_width",
    )
    download_content = column(download_controls, sizing_mode="stretch_width")

    def empty_wfa_table_data() -> dict[str, list[object]]:
        return {
            "fold": [],
            "train_started_at": [],
            "train_ended_at": [],
            "test_started_at": [],
            "test_ended_at": [],
            "lookback_bars": [],
            "entry_z": [],
            "exit_z": [],
            "stop_z_label": [],
            "bollinger_k": [],
            "train_score": [],
            "train_net_profit": [],
            "train_max_drawdown": [],
            "train_trades": [],
            "test_score": [],
            "test_net_profit": [],
            "test_ending_equity": [],
            "test_max_drawdown": [],
            "test_trades": [],
            "test_commission": [],
            "test_total_cost": [],
        }

    def wfa_result_to_sources(result: dict[str, object]) -> tuple[dict[str, list[object]], dict[str, list[object]]]:
        table = empty_wfa_table_data()
        folds = list(result.get("folds", []) or [])
        for row in folds:
            table["fold"].append(int(row.get("fold", 0) or 0))
            table["train_started_at"].append(datetime.fromisoformat(str(row.get("train_started_at", "")).replace("Z", "+00:00")))
            table["train_ended_at"].append(datetime.fromisoformat(str(row.get("train_ended_at", "")).replace("Z", "+00:00")))
            table["test_started_at"].append(datetime.fromisoformat(str(row.get("test_started_at", "")).replace("Z", "+00:00")))
            table["test_ended_at"].append(datetime.fromisoformat(str(row.get("test_ended_at", "")).replace("Z", "+00:00")))
            table["lookback_bars"].append(int(row.get("lookback_bars", 0) or 0))
            table["entry_z"].append(float(row.get("entry_z", 0.0) or 0.0))
            table["exit_z"].append(float(row.get("exit_z", 0.0) or 0.0))
            raw_stop = row.get("stop_z")
            table["stop_z_label"].append("disabled" if raw_stop in (None, "") else f"{float(raw_stop):.2f}")
            table["bollinger_k"].append(float(row.get("bollinger_k", 0.0) or 0.0))
            table["train_score"].append(float(row.get("train_score", 0.0) or 0.0))
            table["train_net_profit"].append(float(row.get("train_net_profit", 0.0) or 0.0))
            table["train_max_drawdown"].append(float(row.get("train_max_drawdown", 0.0) or 0.0))
            table["train_trades"].append(int(row.get("train_trades", 0) or 0))
            table["test_score"].append(float(row.get("test_score", 0.0) or 0.0))
            table["test_net_profit"].append(float(row.get("test_net_profit", 0.0) or 0.0))
            table["test_ending_equity"].append(float(row.get("test_ending_equity", 0.0) or 0.0))
            table["test_max_drawdown"].append(float(row.get("test_max_drawdown", 0.0) or 0.0))
            table["test_trades"].append(int(row.get("test_trades", 0) or 0))
            table["test_commission"].append(float(row.get("test_commission", 0.0) or 0.0))
            table["test_total_cost"].append(float(row.get("test_total_cost", 0.0) or 0.0))
        stitched = {"time": [], "equity": []}
        for point in list(result.get("stitched_equity", []) or []):
            stitched["time"].append(datetime.fromisoformat(str(point.get("time", "")).replace("Z", "+00:00")))
            stitched["equity"].append(float(point.get("equity", 0.0) or 0.0))
        return table, stitched

    wfa_period_slider = DateRangeSlider(
        title="WFA Period",
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
        sizing_mode="stretch_width",
    )
    wfa_unit_select = Select(title="Unit", value=WfaWindowUnit.WEEKS.value, options=WFA_UNIT_OPTIONS, width=96)
    wfa_lookback_input = Spinner(title="Lookback", low=1, step=1, value=8, width=92)
    wfa_test_input = Spinner(title="Test", low=1, step=1, value=2, width=92)
    wfa_run_button = Button(label="Run WFA", button_type="primary", width=120)
    wfa_status_div = Div(text="<p>Ready to run rolling WFA on the selected tester pair using genetic optimization and Score.</p>")
    wfa_table_source = ColumnDataSource(empty_wfa_table_data())
    wfa_equity_source = ColumnDataSource({"time": [], "equity": []})
    wfa_equity_plot = figure(
        title="WFA Stitched Equity",
        x_axis_type="datetime",
        x_range=Range1d(start=_datetime_to_bokeh_millis(history_start), end=_datetime_to_bokeh_millis(now_utc)),
        y_range=Range1d(start=0.0, end=1.0),
        height=260,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    wfa_wheel_zoom = wfa_equity_plot.select_one(WheelZoomTool)
    if wfa_wheel_zoom is not None:
        wfa_wheel_zoom.modifiers = {"ctrl": True}
    wfa_equity_plot.toolbar.autohide = True
    wfa_equity_plot.yaxis.axis_label = "Equity"
    wfa_month_ticker = MonthsTicker()
    wfa_month_ticker.num_minor_ticks = 4
    wfa_equity_plot.xaxis.ticker = wfa_month_ticker
    wfa_equity_plot.xaxis.formatter = DatetimeTickFormatter(months="%Y-%m")
    wfa_equity_plot.xaxis.minor_tick_in = 0
    wfa_equity_plot.xaxis.minor_tick_out = 5
    wfa_equity_plot.xaxis.minor_tick_line_alpha = 0.55
    wfa_equity_plot.xaxis.minor_tick_line_width = 1
    wfa_equity_plot.xgrid.grid_line_alpha = 0.16
    wfa_equity_plot.xgrid.grid_line_color = "#94a3b8"
    wfa_test_window_box = BoxAnnotation(fill_color="#0ea5e9", fill_alpha=0.10, line_color="#0284c7", line_alpha=0.45, line_width=2, visible=False)
    wfa_equity_plot.add_layout(wfa_test_window_box)
    wfa_equity_plot.line("time", "equity", source=wfa_equity_source, line_width=2.5, color="#0f766e")
    wfa_table = DataTable(
        source=wfa_table_source,
        columns=[
            TableColumn(field="fold", title="Fold", formatter=NumberFormatter(format="0"), width=54),
            TableColumn(field="train_started_at", title="Train From", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="train_ended_at", title="Train To", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="test_started_at", title="Test From", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="test_ended_at", title="Test To", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="lookback_bars", title="Lookback", formatter=NumberFormatter(format="0"), width=74),
            TableColumn(field="entry_z", title="Entry", formatter=NumberFormatter(format="0.00"), width=64),
            TableColumn(field="exit_z", title="Exit", formatter=NumberFormatter(format="0.00"), width=64),
            TableColumn(field="stop_z_label", title="Stop", width=70),
            TableColumn(field="bollinger_k", title="Boll", formatter=NumberFormatter(format="0.00"), width=64),
            TableColumn(field="train_score", title="Train Score", formatter=NumberFormatter(format="0.000"), width=90),
            TableColumn(field="test_score", title="Test Score", formatter=NumberFormatter(format="0.000"), width=86),
            TableColumn(field="test_net_profit", title="Test Net", formatter=NumberFormatter(format="0.00"), width=84),
            TableColumn(field="test_max_drawdown", title="Test DD", formatter=NumberFormatter(format="0.00"), width=82),
            TableColumn(field="test_trades", title="Trades", formatter=NumberFormatter(format="0"), width=68),
            TableColumn(field="test_commission", title="Comm", formatter=NumberFormatter(format="0.00"), width=78),
            TableColumn(field="test_total_cost", title="Costs", formatter=NumberFormatter(format="0.00"), width=82),
        ],
        sizing_mode="stretch_width",
        height=260,
        sortable=True,
        reorderable=False,
        index_position=None,
    )
    wfa_controls = column(
        row(
            wfa_unit_select,
            wfa_lookback_input,
            wfa_test_input,
            wfa_period_slider,
            wfa_run_button,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        wfa_status_div,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )
    wfa_outputs = column(
        wfa_equity_plot,
        wfa_table,
        sizing_mode="stretch_width",
        visible=False,
        styles={"gap": "10px"},
    )
    wfa_content = column(wfa_controls, wfa_outputs, sizing_mode="stretch_width", styles={"gap": "10px"})

    def empty_meta_selector_table_data() -> dict[str, list[object]]:
        return {
            "fold": [],
            "test_started_at": [],
            "test_ended_at": [],
            "lookback_bars": [],
            "entry_z": [],
            "exit_z": [],
            "stop_z": [],
            "stop_z_label": [],
            "bollinger_k": [],
            "predicted_target": [],
            "test_score": [],
            "test_net_profit": [],
            "test_max_drawdown": [],
            "test_trades": [],
            "test_commission": [],
            "test_total_cost": [],
        }

    def empty_meta_ranking_table_data() -> dict[str, list[object]]:
        return {
            "rank": [],
            "lookback_bars": [],
            "entry_z": [],
            "exit_z": [],
            "stop_z": [],
            "stop_z_label": [],
            "bollinger_k": [],
            "rows": [],
            "folds": [],
            "predicted_mean": [],
            "predicted_std": [],
            "stability_score": [],
            "actual_test_score_mean": [],
            "actual_test_net_mean": [],
            "actual_test_maxdd_mean": [],
            "actual_test_trades_mean": [],
            "train_score_mean": [],
            "train_net_mean": [],
        }

    def meta_selector_result_to_source(result: dict[str, object]) -> dict[str, list[object]]:
        data = empty_meta_selector_table_data()
        for row in list(result.get("selected_folds", []) or []):
            for key in data:
                value = row.get(key)
                if key in {"test_started_at", "test_ended_at"} and value not in (None, ""):
                    value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
                data[key].append(value)
        return data

    def meta_ranking_result_to_source(result: dict[str, object]) -> dict[str, list[object]]:
        data = empty_meta_ranking_table_data()
        for row in list(result.get("ranking_rows", []) or []):
            for key in data:
                data[key].append(row.get(key))
        return data

    def meta_result_to_equity_source(result: dict[str, object]) -> dict[str, list[object]]:
        data = {"time": [], "equity": []}
        for point in list(result.get("stitched_equity", []) or []):
            raw_time = point.get("time")
            if raw_time in (None, ""):
                continue
            data["time"].append(datetime.fromisoformat(str(raw_time).replace("Z", "+00:00")))
            data["equity"].append(float(point.get("equity", 0.0) or 0.0))
        return data

    def default_meta_model() -> str:
        try:
            __import__("xgboost")
        except ModuleNotFoundError:
            return "decision_tree"
        return "xgboost"

    default_meta_oos_date = datetime(2026, 1, 1, tzinfo=UTC)
    if default_meta_oos_date < history_start:
        default_meta_oos_date = history_start
    if default_meta_oos_date > now_utc:
        default_meta_oos_date = now_utc

    meta_model_select = Select(title="Model", value=default_meta_model(), options=list(SUPPORTED_META_MODELS), width=140)
    meta_oos_start_picker = DatePicker(
        title="OOS Start",
        value=default_meta_oos_date.date().isoformat(),
        min_date=history_start.date().isoformat(),
        max_date=now_utc.date().isoformat(),
        width=132,
    )
    meta_tree_max_depth_input = Spinner(title="Tree Depth", low=1, high=64, step=1, value=5, width=96)
    meta_tree_min_samples_leaf_input = Spinner(title="Min Leaf", low=1, high=256, step=1, value=6, width=96)
    meta_rf_estimators_input = Spinner(title="RF Trees", low=20, high=2000, step=10, value=300, width=96)
    meta_rf_max_depth_input = Spinner(title="RF Depth", low=1, high=64, step=1, value=6, width=96)
    meta_rf_min_samples_leaf_input = Spinner(title="RF Min Leaf", low=1, high=256, step=1, value=4, width=104)
    meta_rf_max_features_select = Select(title="RF Features", value="sqrt", options=["sqrt", "log2", "0.5", "1.0"], width=92)
    meta_xgb_estimators_input = Spinner(title="Estimators", low=20, high=2000, step=10, value=240, width=96)
    meta_xgb_max_depth_input = Spinner(title="XGB Depth", low=1, high=16, step=1, value=4, width=88)
    meta_xgb_learning_rate_input = Spinner(title="LR", low=0.01, high=1.0, step=0.01, value=0.05, width=80)
    meta_xgb_subsample_input = Spinner(title="Subsample", low=0.1, high=1.0, step=0.05, value=0.9, width=96)
    meta_xgb_colsample_input = Spinner(title="Colsample", low=0.1, high=1.0, step=0.05, value=0.9, width=96)
    meta_run_button = Button(label="Run Meta Selector", button_type="primary", width=170)
    meta_status_div = Div(text="<p>Ready to learn from saved WFA optimization history before the chosen OOS date for the selected tester pair and timeframe.</p>")
    meta_table_source = ColumnDataSource(empty_meta_selector_table_data())
    meta_ranking_source = ColumnDataSource(empty_meta_ranking_table_data())
    meta_equity_source = ColumnDataSource({"time": [], "equity": []})
    meta_equity_plot = figure(
        title="Meta Stitched Equity",
        x_axis_type="datetime",
        y_range=Range1d(start=0.0, end=1.0),
        height=240,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    meta_wheel_zoom = meta_equity_plot.select_one(WheelZoomTool)
    if meta_wheel_zoom is not None:
        meta_wheel_zoom.modifiers = {"ctrl": True}
    meta_equity_plot.toolbar.autohide = True
    meta_equity_plot.yaxis.axis_label = "Equity"
    meta_equity_plot.line("time", "equity", source=meta_equity_source, line_width=2.5, color="#7c3aed")
    meta_ranking_title = Div(text="<p><b>Meta Robustness Grid</b></p>")
    meta_ranking_table = DataTable(
        source=meta_ranking_source,
        columns=[
            TableColumn(field="rank", title="Rank", formatter=NumberFormatter(format="0"), width=56),
            TableColumn(field="stability_score", title="Stability", formatter=NumberFormatter(format="0.000"), width=92),
            TableColumn(field="predicted_mean", title="Pred Mean", formatter=NumberFormatter(format="0.000"), width=92),
            TableColumn(field="predicted_std", title="Pred Std", formatter=NumberFormatter(format="0.000"), width=86),
            TableColumn(field="actual_test_score_mean", title="Test Score", formatter=NumberFormatter(format="0.000"), width=90),
            TableColumn(field="actual_test_net_mean", title="Test Net", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="actual_test_maxdd_mean", title="Test DD", formatter=NumberFormatter(format="0.00"), width=84),
            TableColumn(field="actual_test_trades_mean", title="Trades", formatter=NumberFormatter(format="0.0"), width=74),
            TableColumn(field="train_score_mean", title="Train Score", formatter=NumberFormatter(format="0.000"), width=92),
            TableColumn(field="train_net_mean", title="Train Net", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="rows", title="Rows", formatter=NumberFormatter(format="0"), width=62),
            TableColumn(field="folds", title="Folds", formatter=NumberFormatter(format="0"), width=62),
            TableColumn(field="lookback_bars", title="Lookback", formatter=NumberFormatter(format="0"), width=82),
            TableColumn(field="entry_z", title="Entry", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="exit_z", title="Exit", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="stop_z_label", title="Stop", width=70),
            TableColumn(field="bollinger_k", title="Boll", formatter=NumberFormatter(format="0.00"), width=68),
        ],
        sizing_mode="stretch_width",
        height=240,
        sortable=True,
        reorderable=False,
        index_position=None,
    )
    meta_selected_title = Div(text="<p><b>Selected OOS Folds</b></p>")
    meta_table = DataTable(
        source=meta_table_source,
        columns=[
            TableColumn(field="fold", title="Fold", formatter=NumberFormatter(format="0"), width=54),
            TableColumn(field="test_started_at", title="Test From", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="test_ended_at", title="Test To", formatter=DateFormatter(format="%Y-%m-%d"), width=96),
            TableColumn(field="predicted_target", title="Pred", formatter=NumberFormatter(format="0.000"), width=84),
            TableColumn(field="test_score", title="Score", formatter=NumberFormatter(format="0.000"), width=84),
            TableColumn(field="test_net_profit", title="Net", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="test_max_drawdown", title="Max DD", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="test_trades", title="Trades", formatter=NumberFormatter(format="0"), width=68),
            TableColumn(field="lookback_bars", title="Lookback", formatter=NumberFormatter(format="0"), width=82),
            TableColumn(field="entry_z", title="Entry", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="exit_z", title="Exit", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="stop_z_label", title="Stop", width=70),
            TableColumn(field="bollinger_k", title="Boll", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="test_commission", title="Comm", formatter=NumberFormatter(format="0.00"), width=78),
            TableColumn(field="test_total_cost", title="Costs", formatter=NumberFormatter(format="0.00"), width=82),
        ],
        sizing_mode="stretch_width",
        height=260,
        sortable=True,
        reorderable=False,
        index_position=None,
    )

    meta_tree_controls = row(
        meta_tree_max_depth_input,
        meta_tree_min_samples_leaf_input,
        sizing_mode="stretch_width",
        styles={"gap": "12px", "align-items": "flex-end"},
    )
    meta_rf_controls = row(
        meta_rf_estimators_input,
        meta_rf_max_depth_input,
        meta_rf_min_samples_leaf_input,
        meta_rf_max_features_select,
        sizing_mode="stretch_width",
        styles={"gap": "12px", "align-items": "flex-end"},
    )
    meta_xgb_controls = row(
        meta_xgb_estimators_input,
        meta_xgb_max_depth_input,
        meta_xgb_learning_rate_input,
        meta_xgb_subsample_input,
        meta_xgb_colsample_input,
        sizing_mode="stretch_width",
        styles={"gap": "12px", "align-items": "flex-end"},
    )

    def sync_meta_model_ui() -> None:
        model = meta_model_select.value
        meta_tree_controls.visible = model == "decision_tree"
        meta_rf_controls.visible = model == "random_forest"
        meta_xgb_controls.visible = model == "xgboost"

    meta_controls = column(
        row(
            meta_model_select,
            meta_oos_start_picker,
            meta_run_button,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        meta_tree_controls,
        meta_rf_controls,
        meta_xgb_controls,
        meta_status_div,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )
    meta_content = column(
        meta_controls,
        meta_equity_plot,
        meta_ranking_title,
        meta_ranking_table,
        meta_selected_title,
        meta_table,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )

    plot_font_size_input = Spinner(title="Plot Font", low=8, high=24, step=1, value=11, width=92)
    plot_height_single_input = Spinner(title="1 Plot", low=160, step=20, value=560, width=92)
    plot_height_two_input = Spinner(title="2 Plots", low=160, step=20, value=420, width=92)
    plot_height_three_input = Spinner(title="3 Plots", low=160, step=20, value=320, width=92)
    plot_height_four_input = Spinner(title="4+ Plots", low=160, step=20, value=260, width=92)
    display_controls = column(
        row(
            plot_font_size_input,
            plot_height_single_input,
            plot_height_two_input,
            plot_height_three_input,
            plot_height_four_input,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        sizing_mode="stretch_width",
    )
    display_content = column(display_controls, sizing_mode="stretch_width")

    service_log_pretext = PreText(
        text="",
        height=220,
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
    display_block, display_body, display_toggle = _build_section("Display", display_content)
    wfa_block, wfa_body, wfa_toggle = _build_section("WFA", wfa_content)
    meta_block, meta_body, meta_toggle = _build_section("Meta Selector", meta_content)
    scan_block, scan_body, scan_toggle = _build_section("Cointegration Results", scan_content)
    download_block, download_body, download_toggle = _build_section("Downloader", download_content)

    plots = [price_1, price_2, spread_plot, zscore_plot, equity_plot]
    gapless_x_ticker = FixedTicker(ticks=[])
    for plot in plots:
        plot.xaxis.ticker = gapless_x_ticker
        plot.xaxis.major_label_orientation = 1.15
        plot.xaxis.major_label_text_font_size = "8px"
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
        ("display", display_body, display_toggle),
        ("wfa", wfa_body, wfa_toggle),
        ("meta", meta_body, meta_toggle),
        ("cointegration", scan_body, scan_toggle),
        ("downloader", download_body, download_toggle),
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
        BrowserStateBinding("plot_font_size", plot_font_size_input),
        BrowserStateBinding("plot_height_single", plot_height_single_input),
        BrowserStateBinding("plot_height_two", plot_height_two_input),
        BrowserStateBinding("plot_height_three", plot_height_three_input),
        BrowserStateBinding("plot_height_four", plot_height_four_input),
        BrowserStateBinding("wfa_period", wfa_period_slider, kind="range"),
        BrowserStateBinding("wfa_unit", wfa_unit_select, kind="select"),
        BrowserStateBinding("wfa_lookback", wfa_lookback_input),
        BrowserStateBinding("wfa_test", wfa_test_input),
        BrowserStateBinding("meta_model", meta_model_select, kind="select"),
        BrowserStateBinding("meta_oos_start", meta_oos_start_picker),
        BrowserStateBinding("meta_tree_max_depth", meta_tree_max_depth_input),
        BrowserStateBinding("meta_tree_min_samples_leaf", meta_tree_min_samples_leaf_input),
        BrowserStateBinding("meta_rf_estimators", meta_rf_estimators_input),
        BrowserStateBinding("meta_rf_max_depth", meta_rf_max_depth_input),
        BrowserStateBinding("meta_rf_min_samples_leaf", meta_rf_min_samples_leaf_input),
        BrowserStateBinding("meta_rf_max_features", meta_rf_max_features_select, kind="select"),
        BrowserStateBinding("meta_xgb_estimators", meta_xgb_estimators_input),
        BrowserStateBinding("meta_xgb_max_depth", meta_xgb_max_depth_input),
        BrowserStateBinding("meta_xgb_learning_rate", meta_xgb_learning_rate_input),
        BrowserStateBinding("meta_xgb_subsample", meta_xgb_subsample_input),
        BrowserStateBinding("meta_xgb_colsample_bytree", meta_xgb_colsample_input),
        BrowserStateBinding("scan_universe", scan_universe_select, kind="select"),
        BrowserStateBinding("scan_kind", scan_kind_select, kind="select"),
        BrowserStateBinding("scan_unit_root", scan_unit_root_select, kind="select"),
        BrowserStateBinding("scan_significance", scan_significance_select, kind="select"),
        BrowserStateBinding("scan_det_order", scan_det_order_input),
        BrowserStateBinding("scan_k_ar_diff", scan_k_ar_diff_input),
        BrowserStateBinding("scan_period", scan_period_slider, kind="range"),
        BrowserStateBinding("download_scope", download_scope_select, kind="select"),
        BrowserStateBinding("download_group", download_group_select, kind="select"),
        BrowserStateBinding("download_symbol", download_symbol_select, kind="select", restore_on_options_change=True),
        BrowserStateBinding("download_policy", download_policy_select, kind="select"),
        BrowserStateBinding("download_period", download_period_slider, kind="range"),
        BrowserStateBinding("show_price_1", price_1_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_price_2", price_2_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_spread", spread_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_zscore", zscore_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_equity", equity_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_trades", trades_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_optimization", optimization_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_display", display_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_wfa", wfa_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_meta", meta_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_cointegration", scan_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_downloader", download_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_service_logs", service_log_body, property_name="visible", kind="visible"),
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

    def set_section_visibility(key: str, body, toggle: Button, visible: bool) -> None:
        body.visible = visible
        toggle.button_type = "primary" if visible else "default"
        rebalance_layout()
        refresh_plot_ranges()
        try:
            file_state_controller.persist()
        except NameError:
            pass

    def build_section_toggle_handler(key: str, body, toggle: Button):
        def _handler() -> None:
            set_section_visibility(key, body, toggle, not body.visible)
        return _handler

    def toggle_service_log() -> None:
        set_section_visibility("service_logs", service_log_body, service_log_toggle, not service_log_body.visible)

    def clear_trade_highlights() -> None:
        state.selected_trade_markers_1.data = {"x": [], "time": [], "price": []}
        state.selected_trade_markers_2.data = {"x": [], "time": [], "price": []}
        state.selected_trade_segments_1.data = {"x0": [], "y0": [], "x1": [], "y1": []}
        state.selected_trade_segments_2.data = {"x0": [], "y0": [], "x1": [], "y1": []}

    def set_equity_summary_overlay(summary: dict[str, float | int | str] | None) -> None:
        if not summary:
            for label in equity_summary_labels:
                label.text = ""
                label.visible = False
            return
        columns = _build_equity_summary_columns(summary)
        for label, column_text in zip(equity_summary_labels, columns):
            label.text = column_text
            label.visible = True

    def sync_equity_summary_overlay_position() -> None:
        plot_width = int(equity_plot.width or 1100)
        plot_height = int(equity_plot.height or 440)
        equity_hover_label.x = max(16, plot_width - 270)
        equity_hover_label.y = max(72, plot_height - 56)

        visible_labels = [label for label in equity_summary_labels if label.visible and label.text]
        if not visible_labels:
            return

        left_margin = 16
        right_margin = 16
        available_width = max(320, plot_width - left_margin - right_margin)
        baseline_y = 20
        gap = 18
        padding_px = 18

        base_font_px = max(8, int(_read_spinner_value(plot_font_size_input, 11, cast=int)))
        font_candidates = []
        seen_font_sizes: set[int] = set()
        for candidate in (base_font_px, base_font_px - 1, base_font_px - 2, base_font_px - 3, 8):
            candidate_int = max(8, int(candidate))
            if candidate_int not in seen_font_sizes:
                seen_font_sizes.add(candidate_int)
                font_candidates.append(candidate_int)
        chosen_font_px = font_candidates[-1] if font_candidates else 8
        chosen_widths: list[float] = []
        for font_px in font_candidates:
            char_px = font_px * 0.62
            widths = []
            for label in visible_labels:
                lines = label.text.splitlines() or [label.text]
                max_len = max(len(line) for line in lines)
                widths.append(max_len * char_px + padding_px)
            total_width = sum(widths) + gap * max(0, len(widths) - 1)
            chosen_font_px = font_px
            chosen_widths = widths
            if total_width <= available_width:
                break

        total_width = sum(chosen_widths) + gap * max(0, len(chosen_widths) - 1)
        if total_width > available_width and chosen_widths:
            squeeze_ratio = max(0.82, (available_width - gap * max(0, len(chosen_widths) - 1)) / max(1.0, sum(chosen_widths)))
            chosen_widths = [width * squeeze_ratio for width in chosen_widths]
            total_width = sum(chosen_widths) + gap * max(0, len(chosen_widths) - 1)

        cursor_x = left_margin + max(0.0, (available_width - total_width) / 2.0)
        for label, width in zip(visible_labels, chosen_widths):
            label.text_font_size = f"{chosen_font_px}px"
            label.x = int(cursor_x)
            label.y = baseline_y
            cursor_x += width + gap

    def sync_equity_legend(symbol_1: str | None, symbol_2: str | None) -> None:
        if not equity_plot.legend:
            return
        labels = [
            "Total Equity",
            f"{symbol_1} Equity" if symbol_1 else "Leg 1 Equity",
            f"{symbol_2} Equity" if symbol_2 else "Leg 2 Equity",
        ]
        for item, label in zip(equity_plot.legend[0].items, labels):
            item.label = {"value": label}

    def configured_plot_height(visible_plot_count: int, visible_block_count: int) -> int:
        fallback = compute_plot_height(visible_plot_count, visible_block_count)
        if visible_plot_count <= 0:
            return 0
        try:
            if visible_plot_count == 1:
                return max(160, int(_read_spinner_value(plot_height_single_input, fallback, cast=int)))
            if visible_plot_count == 2:
                return max(160, int(_read_spinner_value(plot_height_two_input, fallback, cast=int)))
            if visible_plot_count == 3:
                return max(160, int(_read_spinner_value(plot_height_three_input, fallback, cast=int)))
            return max(160, int(_read_spinner_value(plot_height_four_input, fallback, cast=int)))
        except Exception:
            return fallback

    def apply_plot_display_settings() -> None:
        base_font_px = max(8, int(_read_spinner_value(plot_font_size_input, 11, cast=int)))
        major_font_px = max(8, base_font_px)
        title_font_px = base_font_px + 1
        hover_font_px = max(9, base_font_px)
        for plot in [*plots, wfa_equity_plot]:
            plot.title.text_font_size = f"{title_font_px}px"
            for axis in [*plot.xaxis, *plot.yaxis]:
                axis.major_label_text_font_size = f"{major_font_px}px"
                axis.axis_label_text_font_size = f"{base_font_px}px"
            plot.xaxis.major_label_text_font_size = f"{major_font_px}px"
        if equity_plot.legend:
            equity_plot.legend.label_text_font_size = f"{base_font_px}px"
        for label in equity_summary_labels:
            label.text_font_size = f"{base_font_px}px"
        equity_hover_label.text_font_size = f"{hover_font_px}px"
        sync_equity_summary_overlay_position()

    def _axis_reference_series() -> tuple[list[float], list[datetime]]:
        for source in (state.price_1_source, state.price_2_source, state.spread_source, state.zscore_source, state.equity_source):
            x_values = [float(value) for value in source.data.get("x", [])]
            times = list(source.data.get("time", []))
            if x_values and times and len(x_values) == len(times):
                return x_values, times
        return [], []

    def _nearest_bar_x(target: datetime) -> float | None:
        x_values, times = _axis_reference_series()
        if not x_values or not times:
            return None
        import bisect
        position = bisect.bisect_left(times, target)
        if position <= 0:
            return x_values[0]
        if position >= len(times):
            return x_values[-1]
        previous = times[position - 1]
        current = times[position]
        if abs((target - previous).total_seconds()) <= abs((current - target).total_seconds()):
            return x_values[position - 1]
        return x_values[position]

    def update_gapless_x_axis() -> None:
        x_values, times = _axis_reference_series()
        if not x_values or not times:
            gapless_x_ticker.ticks = []
            for plot in plots:
                plot.xaxis.major_label_overrides = {}
            return

        start = state.shared_x_range.start if state.shared_x_range.start is not None else x_values[0]
        end = state.shared_x_range.end if state.shared_x_range.end is not None else x_values[-1]
        left_index = max(0, min(len(x_values) - 1, int(start)))
        right_index = max(0, min(len(x_values) - 1, int(end)))
        if right_index < left_index:
            left_index, right_index = right_index, left_index

        visible_seconds = max(0.0, (times[right_index] - times[left_index]).total_seconds()) if right_index > left_index else 0.0
        include_hourly = visible_seconds <= 7 * 86400
        max_labels = 20

        def reduce_positions(positions: list[int], limit: int) -> list[int]:
            unique_positions = sorted(dict.fromkeys(positions))
            if limit <= 0:
                return []
            if len(unique_positions) <= limit:
                return unique_positions
            if limit == 1:
                return [unique_positions[0]]
            if limit == 2:
                return [unique_positions[0], unique_positions[-1]]
            reduced: list[int] = []
            last_source_index = len(unique_positions) - 1
            for slot in range(limit):
                relative = slot / (limit - 1)
                source_index = int(round(relative * last_source_index))
                chosen = unique_positions[source_index]
                if not reduced or reduced[-1] != chosen:
                    reduced.append(chosen)
            if reduced[0] != unique_positions[0]:
                reduced[0] = unique_positions[0]
            if reduced[-1] != unique_positions[-1]:
                reduced[-1] = unique_positions[-1]
            if len(reduced) > limit:
                reduced = reduce_positions(reduced, limit)
            return reduced

        day_positions: list[int] = [left_index]
        previous_day = times[left_index].date()
        for index in range(left_index + 1, right_index + 1):
            current_day = times[index].date()
            if current_day != previous_day:
                day_positions.append(index)
                previous_day = current_day
        if day_positions[-1] != right_index:
            day_positions.append(right_index)

        required_positions = reduce_positions(day_positions, max_labels)
        required_position_set = set(required_positions)
        tick_positions = list(required_positions)

        if include_hourly and len(tick_positions) < max_labels:
            hourly_positions: list[int] = []
            previous_hour = (times[left_index].date(), times[left_index].hour)
            for index in range(left_index + 1, right_index + 1):
                current_time = times[index]
                current_hour = (current_time.date(), current_time.hour)
                if current_hour != previous_hour:
                    hourly_positions.append(index)
                    previous_hour = current_hour

            remaining_slots = max_labels - len(tick_positions)
            filtered_hourly = [
                index
                for index in hourly_positions
                if index not in required_position_set and index not in {left_index, right_index}
            ]
            if remaining_slots > 0 and filtered_hourly:
                tick_positions.extend(reduce_positions(filtered_hourly, remaining_slots))

        tick_positions = reduce_positions(tick_positions, max_labels)
        tick_values = [x_values[index] for index in tick_positions]
        label_overrides: dict[float, str] = {}
        required_position_set = set(required_positions)
        for index in tick_positions:
            current_time = times[index]
            if include_hourly:
                if index in required_position_set:
                    label = current_time.strftime("%m-%d\n%H:%M")
                else:
                    label = current_time.strftime("%H:%M")
            else:
                label = current_time.strftime("%m-%d")
            label_overrides[x_values[index]] = label

        gapless_x_ticker.ticks = tick_values
        for plot in plots:
            plot.xaxis.major_label_overrides = dict(label_overrides)

    def sync_optimization_cutoff_marker(*, test_started_at: datetime | None, test_ended_at: datetime | None) -> None:
        if test_started_at is None or test_ended_at is None:
            for span in optimization_cutoff_spans:
                span.visible = False
            return
        optimization_ended_at = _coerce_datetime(optimization_period_slider.value[1])
        should_show = test_ended_at > optimization_ended_at and test_started_at <= optimization_ended_at
        location = _nearest_bar_x(optimization_ended_at)
        if location is None:
            for span in optimization_cutoff_spans:
                span.visible = False
            return
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
        sync_equity_legend(None, None)
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
                    data.get("x", []),
                    [data.get("total", []), data.get("leg1", []), data.get("leg2", [])],
                    state.shared_x_range.start,
                    state.shared_x_range.end,
                )
                if abs(float(equity_high) - float(equity_low)) <= 1e-9:
                    equity_high = float(equity_low) + 1.0
                equity_plot.extra_y_ranges["equity"].start = float(equity_low)
                equity_plot.extra_y_ranges["equity"].end = float(equity_high)

                drawdown_low, _drawdown_high = compute_series_bounds(
                    data.get("x", []),
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
                data.get("x", []),
                [data.get(column, []) for column in value_columns],
                state.shared_x_range.start,
                state.shared_x_range.end,
            )
            plot.y_range.start = lower
            plot.y_range.end = upper

    def rebalance_layout() -> None:
        visible_plots = [plot for _key, plot, _source, _columns, body, _toggle in plot_bindings if body.visible]
        visible_blocks = [body for _key, body, _toggle in block_bindings if body.visible]
        plot_height = configured_plot_height(len(visible_plots), len(visible_blocks))
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
        update_gapless_x_axis()

    def ensure_nonempty_layout() -> None:
        any_plot_visible = any(body.visible for _key, _plot, _source, _columns, body, _toggle in plot_bindings)
        visible_non_display_blocks = {
            key for key, body, _toggle in block_bindings
            if key != "display" and body.visible
        }
        changed = False
        if not any_plot_visible:
            equity_body.visible = True
            equity_toggle.button_type = "primary"
            changed = True
        if changed:
            summary_div.text = "<p>Recovered a hidden layout from saved state and restored a safe default view.</p>"

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

    def current_download_terminal_path() -> Path:
        configured = settings.mt5_terminal_path or DEFAULT_MT5_TERMINAL_PATH
        return Path(configured)

    def sync_downloader_symbol_options(*, show_message: bool = False) -> None:
        options = instrument_options_for_group(download_group_select.value)
        if not options:
            options = [INSTRUMENT_PLACEHOLDER]
        download_symbol_select.options = options
        download_symbol_select.value = _preferred_symbol_1(options, download_symbol_select.value)
        if show_message and download_scope_select.value == "symbol":
            visible_count = len([option for option in options if option != INSTRUMENT_PLACEHOLDER])
            download_status_div.text = f"<p>Downloader loaded <b>{visible_count}</b> symbols for group <b>{download_group_select.value}</b>.</p>"

    def sync_downloader_mode_ui() -> None:
        download_symbol_select.visible = download_scope_select.value == "symbol"


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

    def set_optimizer_control_visible(widget, visible: bool) -> None:
        widget.visible = visible
        wrapper = optimization_control_wrappers.get(widget)
        if wrapper is not None:
            wrapper.visible = visible

    def install_optimizer_tooltips() -> None:
        tooltip_code = """
const selectors = [
  `[data-root-id="${widget.id}"]`,
  `[data-model-id="${widget.id}"]`,
  `[data-id="${widget.id}"]`
];
let root = null;
for (const selector of selectors) {
  root = document.querySelector(selector);
  if (root) break;
}
if (!root) return;
const targets = root.querySelectorAll("input, select, textarea, button, .bk-input, .bk-slider-title, .bk-slider-value, .bk-input-group, [role='slider']");
if (!targets.length) {
  root.title = message;
} else {
  for (const target of targets) target.title = message;
  root.title = message;
}
"""
        for widget, message in optimization_tooltips:
            callback = CustomJS(args=dict(widget=widget, message=message), code=tooltip_code)
            doc.js_on_event(DocumentReady, callback)
            widget.js_on_change("visible", callback)
            if "value" in widget.properties():
                widget.js_on_change("value", callback)

    def sync_stop_mode_ui() -> None:
        stop_input.visible = stop_mode_select.value == "enabled"
        stop_enabled = opt_stop_mode_select.value == "enabled"
        set_optimizer_control_visible(opt_stop_start, stop_enabled)
        set_optimizer_control_visible(opt_stop_stop, stop_enabled)

    def sync_optimization_mode_ui() -> None:
        is_genetic = optimization_mode_select.value == OptimizationMode.GENETIC.value
        is_auto = optimization_range_mode_select.value == "auto"
        set_optimizer_control_visible(optimization_range_mode_select, not is_genetic)
        set_optimizer_control_visible(auto_grid_trials_input, (not is_genetic) and is_auto)
        for widget in [opt_lookback_step, opt_entry_step, opt_exit_step, opt_bollinger_step]:
            set_optimizer_control_visible(widget, (not is_genetic) and (not is_auto))
        set_optimizer_control_visible(opt_stop_step, (not is_genetic) and (not is_auto) and opt_stop_mode_select.value == "enabled")
        for widget in [genetic_population_input, genetic_generations_input, genetic_elite_input, genetic_mutation_input, genetic_seed_input]:
            set_optimizer_control_visible(widget, is_genetic)
        sync_stop_mode_ui()
        optimization_table.columns = build_optimization_table_columns(show_stop_z=opt_stop_mode_select.value == "enabled")

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

    def reset_download_button() -> None:
        download_run_button.label = "Run Download"
        download_run_button.button_type = "primary"

    def mark_download_running() -> None:
        download_run_button.label = "Downloading..."
        download_run_button.button_type = "warning"

    def update_download_progress(completed: int, total: int, stage: str) -> None:
        download_progress["completed"] = completed
        download_progress["total"] = total
        download_progress["stage"] = stage

    def render_download_progress() -> None:
        total = int(download_progress.get("total", 0) or 0)
        completed = int(download_progress.get("completed", 0) or 0)
        stage = str(download_progress.get("stage", "MT5 download"))
        if total > 0:
            download_status_div.text = f"<p>{stage}: <b>{completed}</b> / <b>{total}</b> symbols.</p>"

    def reset_wfa_button() -> None:
        wfa_run_button.label = "Run WFA"
        wfa_run_button.button_type = "primary"

    def mark_wfa_running() -> None:
        wfa_run_button.label = "Running WFA..."
        wfa_run_button.button_type = "warning"

    def update_wfa_progress(completed: int, total: int, stage: str) -> None:
        wfa_progress["completed"] = completed
        wfa_progress["total"] = total
        wfa_progress["stage"] = stage

    def update_wfa_partial_result(result: dict[str, object]) -> None:
        wfa_live_state["result"] = result
        wfa_live_state["version"] = int(wfa_live_state.get("version", 0) or 0) + 1

    def sync_wfa_selection_highlight() -> None:
        indices = list(wfa_table_source.selected.indices)
        if not indices:
            wfa_test_window_box.visible = False
            return
        index = indices[0]
        data = wfa_table_source.data
        starts = data.get("test_started_at", [])
        ends = data.get("test_ended_at", [])
        if index >= len(starts) or index >= len(ends):
            wfa_test_window_box.visible = False
            return
        started_at = starts[index]
        ended_at = ends[index]
        if started_at is None or ended_at is None:
            wfa_test_window_box.visible = False
            return
        wfa_test_window_box.left = started_at
        wfa_test_window_box.right = ended_at
        wfa_test_window_box.visible = True

    def sync_wfa_outputs_visibility() -> None:
        has_equity = bool(wfa_equity_source.data.get("time", []))
        has_rows = bool(wfa_table_source.data.get("fold", []))
        wfa_outputs.visible = has_equity or has_rows

    def refresh_wfa_equity_ranges() -> None:
        times = list(wfa_equity_source.data.get("time", []))
        equities = [float(value) for value in wfa_equity_source.data.get("equity", [])]
        if not times or not equities:
            wfa_equity_plot.x_range.start = _datetime_to_bokeh_millis(history_start)
            wfa_equity_plot.x_range.end = _datetime_to_bokeh_millis(now_utc)
            wfa_equity_plot.y_range.start = 0.0
            wfa_equity_plot.y_range.end = 1.0
            return
        x_start = min(times)
        x_end = max(times)
        if x_start == x_end:
            x_end = x_end.replace(minute=x_end.minute)
            x_end = x_end + (datetime.resolution * 1000)
        y_low = min(equities)
        y_high = max(equities)
        if abs(y_high - y_low) <= 1e-9:
            y_high = y_low + 1.0
        padding = max(1.0, (y_high - y_low) * 0.08)
        wfa_equity_plot.x_range.start = x_start
        wfa_equity_plot.x_range.end = x_end
        wfa_equity_plot.y_range.start = y_low - padding
        wfa_equity_plot.y_range.end = y_high + padding

    def refresh_meta_equity_ranges() -> None:
        times = list(meta_equity_source.data.get("time", []))
        equities = [float(value) for value in meta_equity_source.data.get("equity", [])]
        if not times or not equities:
            meta_equity_plot.y_range.start = 0.0
            meta_equity_plot.y_range.end = 1.0
            return
        y_low = min(equities)
        y_high = max(equities)
        if abs(y_high - y_low) <= 1e-9:
            y_high = y_low + 1.0
        padding = max(1.0, (y_high - y_low) * 0.08)
        meta_equity_plot.y_range.start = y_low - padding
        meta_equity_plot.y_range.end = y_high + padding

    def read_meta_oos_started_at() -> datetime | None:
        raw_value = meta_oos_start_picker.value
        if raw_value in (None, ""):
            return None
        return datetime.fromisoformat(str(raw_value)).replace(tzinfo=UTC)

    def build_meta_model_config() -> dict[str, float | int | str]:
        if meta_model_select.value == "decision_tree":
            return {
                "max_depth": int(_read_spinner_value(meta_tree_max_depth_input, 5, cast=int)),
                "min_samples_leaf": int(_read_spinner_value(meta_tree_min_samples_leaf_input, 6, cast=int)),
            }
        if meta_model_select.value == "random_forest":
            return {
                "n_estimators": int(_read_spinner_value(meta_rf_estimators_input, 300, cast=int)),
                "max_depth": int(_read_spinner_value(meta_rf_max_depth_input, 6, cast=int)),
                "min_samples_leaf": int(_read_spinner_value(meta_rf_min_samples_leaf_input, 4, cast=int)),
                "max_features": str(meta_rf_max_features_select.value),
            }
        return {
            "n_estimators": int(_read_spinner_value(meta_xgb_estimators_input, 240, cast=int)),
            "max_depth": int(_read_spinner_value(meta_xgb_max_depth_input, 4, cast=int)),
            "learning_rate": float(_read_spinner_value(meta_xgb_learning_rate_input, 0.05)),
            "subsample": float(_read_spinner_value(meta_xgb_subsample_input, 0.9)),
            "colsample_bytree": float(_read_spinner_value(meta_xgb_colsample_input, 0.9)),
        }

    def restore_saved_wfa_result(*, show_message: bool = False) -> None:
        if wfa_future is not None and not wfa_future.done():
            return
        pair = current_pair()
        if pair is None:
            return
        started_at = _coerce_datetime(wfa_period_slider.value[0])
        ended_at = _coerce_datetime(wfa_period_slider.value[1])
        if ended_at <= started_at:
            return
        lookback_units = int(_read_spinner_value(wfa_lookback_input, 8, cast=int))
        test_units = int(_read_spinner_value(wfa_test_input, 2, cast=int))
        snapshot = load_wfa_run_snapshot(
            broker=broker,
            pair=pair,
            timeframe=Timeframe(timeframe_select.value),
            started_at=started_at,
            ended_at=ended_at,
            lookback_units=lookback_units,
            test_units=test_units,
            step_units=test_units,
            unit=WfaWindowUnit(wfa_unit_select.value),
        )
        if snapshot is None:
            wfa_table_source.data = empty_wfa_table_data()
            wfa_table_source.selected.indices = []
            wfa_equity_source.data = {"time": [], "equity": []}
            wfa_outputs.visible = False
            wfa_test_window_box.visible = False
            refresh_wfa_equity_ranges()
            if show_message and wfa_body.visible:
                wfa_status_div.text = (
                    f"<p>No saved WFA result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on "
                    f"<b>{timeframe_select.value}</b> and the selected WFA settings.</p>"
                )
            return
        apply_wfa_snapshot(snapshot, final=True)
        refresh_wfa_equity_ranges()
        reset_wfa_button()
        if show_message:
            wfa_status_div.text = (
                f"<p>Loaded saved WFA result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
                f"Folds: <b>{int(snapshot.get('fold_count', 0) or 0)}</b>, stitched net: "
                f"<b>{float(snapshot.get('total_net_profit', 0.0) or 0.0):.2f}</b>.</p>"
            )

    def apply_wfa_snapshot(result: dict[str, object], *, final: bool) -> None:
        previous_indices = list(wfa_table_source.selected.indices)
        table_data, equity_data = wfa_result_to_sources(result)
        wfa_table_source.data = table_data
        wfa_equity_source.data = equity_data
        sync_wfa_outputs_visibility()
        refresh_wfa_equity_ranges()
        if previous_indices and previous_indices[0] < len(table_data.get("fold", [])):
            wfa_table_source.selected.indices = previous_indices
        else:
            wfa_table_source.selected.indices = []
        sync_wfa_selection_highlight()
        if final:
            return
        completed_folds = int(result.get("fold_count", 0) or 0)
        total = int(wfa_progress.get("total", 0) or 0)
        stitched_net = float(result.get("total_net_profit", 0.0) or 0.0)
        stitched_trades = int(result.get("total_trades", 0) or 0)
        history_rows = int(result.get("optimization_history_rows", 0) or 0)
        wfa_status_div.text = (
            f"<p>WFA live update: <b>{completed_folds}</b> / <b>{total}</b> folds ready. "
            f"Current stitched net: <b>{stitched_net:.2f}</b>, trades: <b>{stitched_trades}</b>, saved optimization rows: <b>{history_rows}</b>.</p>"
        )

    def render_wfa_progress() -> None:
        total = int(wfa_progress.get("total", 0) or 0)
        completed = int(wfa_progress.get("completed", 0) or 0)
        stage = str(wfa_progress.get("stage", "WFA"))
        if total > 0:
            wfa_status_div.text = f"<p>{stage}: <b>{completed}</b> / <b>{total}</b> folds completed using genetic optimization.</p>"

    def complete_wfa(result: dict[str, object], pair: PairSelection) -> None:
        table_data, equity_data = wfa_result_to_sources(result)
        wfa_table_source.data = table_data
        wfa_equity_source.data = equity_data
        sync_wfa_outputs_visibility()
        refresh_wfa_equity_ranges()
        failure_reason = str(result.get("failure_reason", "") or "")
        if failure_reason:
            meta_equity_source.data = {"time": [], "equity": []}
            refresh_meta_equity_ranges()
            if failure_reason == "no_aligned_quotes":
                wfa_status_div.text = f"<p>No aligned parquet data for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on the selected WFA period.</p>"
            elif failure_reason == "no_wfa_windows":
                wfa_status_div.text = "<p>No WFA windows could be built from the selected period, unit, lookback and test settings.</p>"
            else:
                wfa_status_div.text = "<p>WFA produced no completed folds.</p>"
            return
        wfa_status_div.text = (
            f"<p>WFA completed for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
            f"Folds: <b>{int(result.get('fold_count', 0) or 0)}</b>, "
            f"stitched net: <b>{float(result.get('total_net_profit', 0.0) or 0.0):.2f}</b>, "
            f"trades: <b>{int(result.get('total_trades', 0) or 0)}</b>, "
            f"commission: <b>{float(result.get('total_commission', 0.0) or 0.0):.2f}</b>, "
            f"saved optimization rows: <b>{int(result.get('optimization_history_rows', 0) or 0)}</b>. "
            f"Parquet: <b>{result.get('optimization_history_path') or 'n/a'}</b>.</p>"
        )

    def run_wfa_job(
        pair: PairSelection,
        timeframe: Timeframe,
        started_at: datetime,
        ended_at: datetime,
        defaults: StrategyDefaults,
        lookback_units: int,
        test_units: int,
        unit_value: str,
    ) -> dict[str, object]:
        wfa_progress["completed"] = 0
        wfa_progress["total"] = 0
        wfa_progress["stage"] = "Preparing WFA"
        return run_distance_genetic_wfa(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
            defaults=defaults,
            objective_metric="score_log_trades",
            parameter_search_space=optimization_search_space(),
            genetic_config=genetic_optimizer_config(),
            lookback_units=lookback_units,
            test_units=test_units,
            step_units=test_units,
            unit=WfaWindowUnit(unit_value),
            parallel_workers=optimizer_parallel_workers,
            progress_callback=update_wfa_progress,
            partial_result_callback=update_wfa_partial_result,
        )

    def clear_wfa_poll_callback() -> None:
        nonlocal wfa_poll_callback
        if wfa_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(wfa_poll_callback)
        except ValueError:
            pass
        wfa_poll_callback = None

    def poll_wfa_future() -> None:
        nonlocal wfa_future
        if wfa_future is None:
            return
        latest_version = int(wfa_live_state.get("version", 0) or 0)
        applied_version = int(wfa_live_state.get("applied_version", 0) or 0)
        if latest_version > applied_version:
            partial_result = wfa_live_state.get("result")
            if isinstance(partial_result, dict):
                apply_wfa_snapshot(partial_result, final=False)
            wfa_live_state["applied_version"] = latest_version
        if not wfa_future.done():
            render_wfa_progress()
            return
        future = wfa_future
        wfa_future = None
        clear_wfa_poll_callback()
        reset_wfa_button()
        try:
            result, pair = future.result()
        except Exception as exc:
            wfa_status_div.text = f"<p>WFA failed: {exc}</p>"
            return
        complete_wfa(result, pair)

    def on_run_wfa() -> None:
        nonlocal wfa_future, wfa_poll_callback
        if wfa_future is not None and not wfa_future.done():
            render_wfa_progress()
            return
        if wfa_future is not None and wfa_future.done():
            poll_wfa_future()
        pair = current_pair()
        if pair is None:
            wfa_status_div.text = "<p>Choose two different valid instruments before running WFA.</p>"
            return
        started_at = _coerce_datetime(wfa_period_slider.value[0])
        ended_at = _coerce_datetime(wfa_period_slider.value[1])
        if ended_at <= started_at:
            wfa_status_div.text = "<p>WFA Period is invalid. End must be after start.</p>"
            return
        lookback_units = int(_read_spinner_value(wfa_lookback_input, 8, cast=int))
        test_units = int(_read_spinner_value(wfa_test_input, 2, cast=int))
        if lookback_units <= 0 or test_units <= 0:
            wfa_status_div.text = "<p>Lookback and Test must be positive integers.</p>"
            return
        wfa_table_source.data = empty_wfa_table_data()
        wfa_table_source.selected.indices = []
        wfa_equity_source.data = {"time": [], "equity": []}
        wfa_outputs.visible = False
        wfa_test_window_box.visible = False
        wfa_progress["completed"] = 0
        wfa_progress["total"] = 0
        wfa_progress["stage"] = "Starting WFA"
        wfa_live_state["version"] = 0
        wfa_live_state["applied_version"] = 0
        wfa_live_state["result"] = None
        mark_wfa_running()
        wfa_status_div.text = (
            f"<p>Running WFA for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{started_at:%Y-%m-%d %H:%M}</b> .. "
            f"<b>{ended_at:%Y-%m-%d %H:%M} UTC</b> with lookback <b>{lookback_units}</b> {wfa_unit_select.value} and test <b>{test_units}</b> {wfa_unit_select.value}."
            f" Objective is fixed to <b>score_log_trades</b>.</p>"
        )
        wfa_future = wfa_executor.submit(
            lambda: (
                run_wfa_job(
                    pair,
                    Timeframe(timeframe_select.value),
                    started_at,
                    ended_at,
                    build_defaults(),
                    lookback_units,
                    test_units,
                    wfa_unit_select.value,
                ),
                pair,
            )
        )
        if wfa_poll_callback is None:
            wfa_poll_callback = doc.add_periodic_callback(poll_wfa_future, 250)

    def reset_meta_button() -> None:
        meta_run_button.label = "Run Meta Selector"
        meta_run_button.button_type = "primary"

    def mark_meta_running() -> None:
        meta_run_button.label = "Running Meta..."
        meta_run_button.button_type = "warning"

    def complete_meta(result: dict[str, object], pair: PairSelection) -> None:
        meta_table_source.data = meta_selector_result_to_source(result)
        meta_table_source.selected.indices = []
        meta_ranking_source.data = meta_ranking_result_to_source(result)
        meta_ranking_source.selected.indices = []
        meta_equity_source.data = meta_result_to_equity_source(result)
        refresh_meta_equity_ranges()
        failure_reason = str(result.get("failure_reason", "") or "")
        oos_started_at_raw = result.get("oos_started_at")
        oos_label = "full history" if oos_started_at_raw in (None, "") else str(oos_started_at_raw).replace("T00:00:00Z", "").replace("Z", " UTC")
        if failure_reason:
            if failure_reason == "no_wfa_history":
                meta_status_div.text = (
                    f"<p>No saved WFA optimization history found for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on "
                    f"<b>{timeframe_select.value}</b>. Run WFA first.</p>"
                )
            elif failure_reason == "no_pre_oos_history":
                meta_status_div.text = (
                    f"<p>No WFA history rows exist before OOS start <b>{oos_label}</b>. Move the cutoff later.</p>"
                )
            elif failure_reason == "no_oos_folds_after_cutoff":
                meta_status_div.text = (
                    f"<p>No WFA fold rows exist on or after OOS start <b>{oos_label}</b>. Move the cutoff earlier.</p>"
                )
            else:
                meta_status_div.text = f"<p>Meta Selector finished with no ranking output: <b>{failure_reason}</b>.</p>"
            return
        validation_mae = result.get("validation_mae")
        validation_r2 = result.get("validation_r2")
        mae_label = "n/a" if validation_mae is None else f"{float(validation_mae or 0.0):.4f}"
        r2_label = "n/a" if validation_r2 is None else f"{float(validation_r2 or 0.0):.4f}"
        meta_status_div.text = (
            f"<p>Meta Selector completed for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> using <b>{result.get('model_type')}</b>. "
            f"OOS start: <b>{oos_label}</b>, pre-OOS rows: <b>{int(result.get('train_rows', 0) or 0)}</b>, "
            f"validation rows: <b>{int(result.get('validation_rows', 0) or 0)}</b>, OOS rows: <b>{int(result.get('oos_rows', 0) or 0)}</b>, "
            f"ranking rows: <b>{len(result.get('ranking_rows', []) or [])}</b>, "
            f"selected folds: <b>{len(result.get('selected_folds', []) or [])}</b>, stitched net: "
            f"<b>{float(result.get('stitched_net_profit', 0.0) or 0.0):.2f}</b>, stitched max DD: "
            f"<b>{float(result.get('stitched_max_drawdown', 0.0) or 0.0):.2f}</b>, trades: "
            f"<b>{int(result.get('stitched_total_trades', 0) or 0)}</b>. "
            f"MAE: <b>{mae_label}</b>, R2: <b>{r2_label}</b>. "
            f"History: <b>{result.get('history_path') or 'n/a'}</b>.</p>"
        )

    def run_meta_job(
        pair: PairSelection,
        timeframe: Timeframe,
        model_type: str,
        oos_started_at: datetime | None,
        model_config: dict[str, float | int],
    ) -> dict[str, object]:
        return run_meta_selector(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            model_type=model_type,
            target_metric=DEFAULT_META_TARGET,
            defaults=build_defaults(),
            oos_started_at=oos_started_at,
            model_config=model_config,
        )

    def clear_meta_poll_callback() -> None:
        nonlocal meta_poll_callback
        if meta_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(meta_poll_callback)
        except ValueError:
            pass
        meta_poll_callback = None

    def poll_meta_future() -> None:
        nonlocal meta_future
        if meta_future is None:
            return
        if not meta_future.done():
            return
        future = meta_future
        meta_future = None
        clear_meta_poll_callback()
        reset_meta_button()
        try:
            result, pair = future.result()
        except Exception as exc:
            meta_status_div.text = f"<p>Meta Selector failed: {exc}</p>"
            return
        complete_meta(result, pair)

    def on_run_meta() -> None:
        nonlocal meta_future, meta_poll_callback
        if meta_future is not None and not meta_future.done():
            return
        if meta_future is not None and meta_future.done():
            poll_meta_future()
        pair = current_pair()
        if pair is None:
            meta_status_div.text = "<p>Choose two different valid instruments before running Meta Selector.</p>"
            return
        model_config = build_meta_model_config()
        oos_started_at = read_meta_oos_started_at()
        oos_label = "full history" if oos_started_at is None else f"{oos_started_at:%Y-%m-%d}"
        mark_meta_running()
        meta_status_div.text = (
            f"<p>Running Meta Selector for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{timeframe_select.value}</b> "
            f"using <b>{meta_model_select.value}</b>, OOS start <b>{oos_label}</b> and target <b>{DEFAULT_META_TARGET}</b>.</p>"
        )
        meta_future = meta_executor.submit(
            lambda: (
                run_meta_job(pair, Timeframe(timeframe_select.value), meta_model_select.value, oos_started_at, model_config),
                pair,
            )
        )
        if meta_poll_callback is None:
            meta_poll_callback = doc.add_periodic_callback(poll_meta_future, 250)

    def on_meta_selection(_attr: str, _old: object, _new: object) -> None:
        indices = meta_table_source.selected.indices
        if not indices:
            return
        index = indices[0]
        data = meta_table_source.data
        tester_period = (
            _coerce_datetime(period_slider.value[0]),
            _coerce_datetime(period_slider.value[1]),
        )
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
            meta_status_div.text = (
                f"<p>Meta-selected parameters copied into tester and executed on tester period <b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. "
                f"<b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

    def on_meta_ranking_selection(_attr: str, _old: object, _new: object) -> None:
        indices = meta_ranking_source.selected.indices
        if not indices:
            return
        index = indices[0]
        data = meta_ranking_source.data
        tester_period = (
            _coerce_datetime(period_slider.value[0]),
            _coerce_datetime(period_slider.value[1]),
        )
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
            meta_status_div.text = (
                f"<p>Robustness rank <b>{int(data['rank'][index])}</b> copied into tester and executed on tester period "
                f"<b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. <b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

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

        x_values = [float(value) for value in sources["price_1"].get("x", [])]
        state.shared_x_range.start = 0.0
        state.shared_x_range.end = float(max(1, len(x_values) - 1))

        rebalance_layout()
        refresh_plot_ranges()
        sync_optimization_cutoff_marker(test_started_at=started_at, test_ended_at=ended_at)

        summary = result.summary
        sync_equity_legend(str(summary['symbol_1']), str(summary['symbol_2']))
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
        sync_downloader_symbol_options(show_message=False)
        if leg_2_filter_select.value != 'cointegrated_only':
            visible_count = len([option for option in options if option != INSTRUMENT_PLACEHOLDER])
            summary_div.text = f"<p>Loaded {visible_count} instruments for group '{selected_group}'.</p>"

    def on_group_change(_attr: str, _old: object, _new: object) -> None:
        refresh_instruments()

    def on_symbol_1_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')
        restore_saved_wfa_result(show_message=False)

    def on_symbol_2_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_wfa_result(show_message=False)

    def on_leg2_filter_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')

    def on_leg2_cointegration_kind_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')

    def on_timeframe_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value == 'cointegrated_only')
        restore_saved_scan_table(show_message=True)
        restore_saved_wfa_result(show_message=False)

    def on_scan_universe_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_scan_table(show_message=True)

    def on_scan_kind_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_scan_table(show_message=True)

    def on_download_group_change(_attr: str, _old: object, _new: object) -> None:
        sync_downloader_symbol_options(show_message=download_scope_select.value == "symbol")

    def on_download_scope_change(_attr: str, _old: object, _new: object) -> None:
        sync_downloader_mode_ui()
        if download_scope_select.value == "symbol":
            sync_downloader_symbol_options(show_message=False)
        else:
            download_status_div.text = f"<p>Downloader ready to export the <b>{download_group_select.value}</b> group on the selected period.</p>"

    def on_wfa_config_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_wfa_result(show_message=False)

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

    def on_display_settings_change(_attr: str, _old: object, _new: object) -> None:
        apply_plot_display_settings()
        rebalance_layout()
        refresh_plot_ranges()

    def on_section_visibility_change(_attr: str, _old: object, _new: object) -> None:
        rebalance_layout()
        refresh_plot_ranges()

    def on_range_change(_attr: str, _old: object, _new: object) -> None:
        refresh_plot_ranges()
        update_gapless_x_axis()

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

        entry_x = data["entry_x"][index]
        exit_x = data["exit_x"][index]

        state.selected_trade_markers_1.data = {"x": [entry_x, exit_x], "time": [entry_time, exit_time], "price": [entry_price_1, exit_price_1]}
        state.selected_trade_markers_2.data = {"x": [entry_x, exit_x], "time": [entry_time, exit_time], "price": [entry_price_2, exit_price_2]}
        state.selected_trade_segments_1.data = {"x0": [entry_x], "y0": [entry_price_1], "x1": [exit_x], "y1": [exit_price_1]}
        state.selected_trade_segments_2.data = {"x0": [entry_x], "y0": [entry_price_2], "x1": [exit_x], "y1": [exit_price_2]}

    def on_wfa_selection(_attr: str, _old: object, _new: object) -> None:
        sync_wfa_selection_highlight()

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

        started_at = _coerce_datetime(scan_period_slider.value[0])
        ended_at = _coerce_datetime(scan_period_slider.value[1])
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

    def run_download_job(
        scope_value: str,
        normalized_group: str,
        symbol: str,
        started_at: datetime,
        ended_at: datetime,
        policy_value: str,
    ) -> dict[str, object]:
        download_progress["completed"] = 0
        download_progress["total"] = 0
        download_progress["stage"] = "Preparing MT5 download"
        terminal_path = current_download_terminal_path()
        if not terminal_path.exists():
            raise FileNotFoundError(f"MT5 terminal not found: {terminal_path}")

        if scope_value == "symbol":
            if symbol == INSTRUMENT_PLACEHOLDER:
                raise ValueError("Choose a concrete symbol for Downloader mode=symbol.")
            requested_symbols = [symbol]
        else:
            requested_symbols = resolve_symbols(
                broker,
                [],
                all_symbols=True,
                groups=[] if normalized_group == "all" else [normalized_group],
                limit=None,
            )

        if not requested_symbols:
            return {
                "status": "noop",
                "scope": scope_value,
                "group": normalized_group,
                "symbol": symbol,
                "requested_symbols_total": 0,
                "exported_symbols_total": 0,
                "written_partitions_total": 0,
                "skipped_symbols_total": 0,
                "terminal_path": str(terminal_path),
                "message": "No symbols matched the selected Downloader scope.",
            }

        skipped_symbols: list[str] = []
        export_symbols = list(requested_symbols)
        if policy_value == "missing_only":
            export_symbols = []
            for candidate in requested_symbols:
                if symbol_partitions_exist(broker, candidate, started_at, ended_at):
                    skipped_symbols.append(candidate)
                else:
                    export_symbols.append(candidate)

        if not export_symbols:
            return {
                "status": "noop",
                "scope": scope_value,
                "group": normalized_group,
                "symbol": symbol,
                "requested_symbols_total": len(requested_symbols),
                "exported_symbols_total": 0,
                "written_partitions_total": 0,
                "skipped_symbols_total": len(skipped_symbols),
                "terminal_path": str(terminal_path),
                "message": "All requested parquet partitions already exist for the selected period.",
            }

        common_root = default_common_root()
        config_path = Path.cwd() / "codex_export_run.ini"
        written_partitions: list[str] = []
        written_symbols: set[str] = set()
        zero_partition_symbols: list[str] = []
        failed_symbols: list[str] = []
        completed_symbols = 0
        terminal_exit = 0
        update_download_progress(0, len(export_symbols), "Starting MT5 export")
        for batch_index, batch in enumerate(chunked(export_symbols, 40), start=1):
            jobs = build_jobs(batch, started_at, ended_at)
            write_job_manifest(common_root=common_root, jobs=jobs)
            write_startup_config(config_path=config_path, chart_symbol=batch[0])
            terminal_exit = run_terminal_export(terminal_path=terminal_path, config_path=config_path)
            statuses = read_export_statuses(common_root)
            for job in jobs:
                status = statuses.get(job.symbol)
                if status is not None and not status.ok:
                    failed_symbols.append(job.symbol)
            written = decode_exports(common_root=common_root, broker=broker, jobs=jobs)
            written_partitions.extend(str(path) for path in written)
            batch_written_symbols = {path.parts[-5] for path in written if len(path.parts) >= 5}
            written_symbols.update(batch_written_symbols)
            for job in jobs:
                if job.symbol not in batch_written_symbols and job.symbol not in failed_symbols:
                    zero_partition_symbols.append(job.symbol)
            completed_symbols += len(batch)
            update_download_progress(
                completed_symbols,
                len(export_symbols),
                f"Downloaded chunk {batch_index}: <b>{completed_symbols}</b> / <b>{len(export_symbols)}</b> symbols",
            )

        return {
            "status": "completed",
            "scope": scope_value,
            "group": normalized_group,
            "symbol": symbol,
            "requested_symbols_total": len(requested_symbols),
            "exported_symbols_total": len(export_symbols),
            "written_symbols_total": len(written_symbols),
            "written_partitions_total": len(written_partitions),
            "skipped_symbols_total": len(skipped_symbols),
            "failed_symbols_total": len(failed_symbols),
            "failed_symbols_preview": ", ".join(failed_symbols[:8]),
            "zero_partition_symbols_total": len(zero_partition_symbols),
            "zero_partition_symbols_preview": ", ".join(zero_partition_symbols[:8]),
            "terminal_path": str(terminal_path),
            "terminal_exit": terminal_exit,
            "message": "Download completed.",
        }

    def clear_download_poll_callback() -> None:
        nonlocal download_poll_callback
        if download_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(download_poll_callback)
        except ValueError:
            pass
        download_poll_callback = None

    def poll_download_future() -> None:
        nonlocal download_future
        if download_future is None:
            return
        if not download_future.done():
            render_download_progress()
            return

        future = download_future
        download_future = None
        clear_download_poll_callback()
        reset_download_button()

        try:
            result = future.result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            download_status_div.text = f"<p>Downloader failed: {exc}</p>"
            return

        status = str(result.get("status", "completed"))
        if status == "noop":
            download_status_div.text = f"<p>{result.get('message', 'No download work was required.')}</p>"
            return

        scope_label = str(result.get("symbol")) if result.get("scope") == "symbol" else f"group {result.get('group')}"
        failure_suffix = ""
        failed_total = int(result.get("failed_symbols_total", 0) or 0)
        failed_preview = str(result.get("failed_symbols_preview", "") or "")
        if failed_total > 0:
            failure_suffix = f" Failed: <b>{failed_total}</b>"
            if failed_preview:
                failure_suffix += f" (<b>{failed_preview}</b>)"
            failure_suffix += "."
        zero_partition_suffix = ""
        zero_partition_total = int(result.get("zero_partition_symbols_total", 0) or 0)
        zero_partition_preview = str(result.get("zero_partition_symbols_preview", "") or "")
        if zero_partition_total > 0:
            zero_partition_suffix = f" No new parquet data: <b>{zero_partition_total}</b>"
            if zero_partition_preview:
                zero_partition_suffix += f" (<b>{zero_partition_preview}</b>)"
            zero_partition_suffix += "."
        download_status_div.text = (
            f"<p>Downloader finished for <b>{scope_label}</b>. Exported <b>{int(result.get('exported_symbols_total', 0) or 0)}</b> symbols, "
            f"wrote parquet for <b>{int(result.get('written_symbols_total', 0) or 0)}</b> symbols "
            f"and <b>{int(result.get('written_partitions_total', 0) or 0)}</b> partitions, skipped <b>{int(result.get('skipped_symbols_total', 0) or 0)}</b>."
            f"{failure_suffix}{zero_partition_suffix} Terminal exit: <b>{int(result.get('terminal_exit', 0) or 0)}</b>.</p>"
        )

    def on_run_download() -> None:
        nonlocal download_future, download_poll_callback

        if download_future is not None and not download_future.done():
            render_download_progress()
            return

        if download_future is not None and download_future.done():
            poll_download_future()

        started_at = _coerce_datetime(download_period_slider.value[0])
        ended_at = _coerce_datetime(download_period_slider.value[1])
        if ended_at < started_at:
            download_status_div.text = "<p>Download Period is invalid. End must be after start.</p>"
            return

        if download_scope_select.value == "symbol" and download_symbol_select.value == INSTRUMENT_PLACEHOLDER:
            download_status_div.text = "<p>Choose a concrete symbol before running Downloader in symbol mode.</p>"
            return

        requested_label = download_symbol_select.value if download_scope_select.value == "symbol" else f"group {download_group_select.value}"
        download_progress["completed"] = 0
        download_progress["total"] = 0
        download_progress["stage"] = "Preparing MT5 download"
        mark_download_running()
        download_status_div.text = (
            f"<p>Starting Downloader for <b>{requested_label}</b> on <b>{started_at:%Y-%m-%d %H:%M}</b> .. "
            f"<b>{ended_at:%Y-%m-%d %H:%M} UTC</b> with policy <b>{download_policy_select.value}</b>.</p>"
        )
        download_future = download_executor.submit(
            run_download_job,
            download_scope_select.value,
            download_group_select.value,
            download_symbol_select.value,
            started_at,
            ended_at,
            download_policy_select.value,
        )
        if download_poll_callback is None:
            download_poll_callback = doc.add_periodic_callback(poll_download_future, 250)

    def on_reset_defaults() -> None:
        if optimization_future is not None and not optimization_future.done():
            optimization_cancel_event.set()
            mark_optimization_running(stopping=True)
            optimization_status_div.text = "<p>Stop requested before reset. Waiting for the current trial to finish.</p>"
            return
        if scan_future is not None and not scan_future.done():
            scan_status_div.text = "<p>Johansen scan is still running. Wait for it to finish before reset.</p>"
            return
        if download_future is not None and not download_future.done():
            download_status_div.text = "<p>Downloader is still running. Wait for it to finish before reset.</p>"
            return
        if wfa_future is not None and not wfa_future.done():
            wfa_status_div.text = "<p>WFA is still running. Wait for it to finish before reset.</p>"
            return
        if meta_future is not None and not meta_future.done():
            meta_status_div.text = "<p>Meta Selector is still running. Wait for it to finish before reset.</p>"
            return

        file_state_controller.clear()
        group_select.value = GROUP_OPTIONS[0]
        timeframe_select.value = Timeframe.M15.value
        period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))
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
        optimization_period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))
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
        plot_font_size_input.value = 11
        plot_height_single_input.value = 560
        plot_height_two_input.value = 420
        plot_height_three_input.value = 320
        plot_height_four_input.value = 260
        wfa_period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))
        wfa_unit_select.value = WfaWindowUnit.WEEKS.value
        wfa_lookback_input.value = 8
        wfa_test_input.value = 2
        meta_model_select.value = default_meta_model()
        meta_oos_start_picker.value = default_meta_oos_date.date().isoformat()
        meta_tree_max_depth_input.value = 5
        meta_tree_min_samples_leaf_input.value = 6
        meta_rf_estimators_input.value = 300
        meta_rf_max_depth_input.value = 6
        meta_rf_min_samples_leaf_input.value = 4
        meta_rf_max_features_select.value = "sqrt"
        meta_xgb_estimators_input.value = 240
        meta_xgb_max_depth_input.value = 4
        meta_xgb_learning_rate_input.value = 0.05
        meta_xgb_subsample_input.value = 0.9
        meta_xgb_colsample_input.value = 0.9
        scan_universe_select.value = GROUP_OPTIONS[0]
        scan_kind_select.value = COINTEGRATION_KIND_OPTIONS[0]
        scan_unit_root_select.value = UnitRootTest.ADF.value
        scan_significance_select.value = "0.05"
        scan_det_order_input.value = 0
        scan_k_ar_diff_input.value = 1
        scan_period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))
        download_scope_select.value = DOWNLOAD_SCOPE_OPTIONS[0]
        download_group_select.value = GROUP_OPTIONS[0]
        download_policy_select.value = DOWNLOAD_POLICY_OPTIONS[0]
        download_period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))

        for _key, _plot, _source, _columns, body, toggle in plot_bindings:
            set_section_visibility(_key, body, toggle, True)
        for _key, body, toggle in block_bindings:
            set_section_visibility(_key, body, toggle, True)
        set_section_visibility("service_logs", service_log_body, service_log_toggle, False)

        refresh_instruments()
        restore_saved_scan_table(show_message=False)
        sync_downloader_mode_ui()
        sync_downloader_symbol_options(show_message=False)
        sync_optimization_mode_ui()
        sync_stop_mode_ui()
        sync_meta_model_ui()
        reset_optimization_button()
        reset_wfa_button()
        reset_meta_button()
        reset_scan_button()
        reset_download_button()
        apply_plot_display_settings()
        rebalance_layout()
        refresh_plot_ranges()
        file_state_controller.persist()
        summary_div.text = f"<p>Settings reset to defaults and saved to <b>{file_state_controller.state_path}</b>.</p>"

    def on_session_destroyed(_session_context: object) -> None:
        optimization_cancel_event.set()
        clear_optimization_poll_callback()
        clear_scan_poll_callback()
        clear_download_poll_callback()
        clear_wfa_poll_callback()
        clear_meta_poll_callback()
        optimization_executor.shutdown(wait=False, cancel_futures=True)
        scan_executor.shutdown(wait=False, cancel_futures=True)
        download_executor.shutdown(wait=False, cancel_futures=True)
        wfa_executor.shutdown(wait=False, cancel_futures=True)
        meta_executor.shutdown(wait=False, cancel_futures=True)

    refresh_button.on_click(refresh_instruments)
    reset_defaults_button.on_click(on_reset_defaults)
    group_select.on_change("value", on_group_change)
    symbol_1_select.on_change("value", on_symbol_1_change)
    symbol_2_select.on_change("value", on_symbol_2_change)
    leg_2_filter_select.on_change("value", on_leg2_filter_change)
    leg_2_cointegration_kind_select.on_change("value", on_leg2_cointegration_kind_change)
    timeframe_select.on_change("value", on_timeframe_change)
    scan_universe_select.on_change("value", on_scan_universe_change)
    scan_kind_select.on_change("value", on_scan_kind_change)
    download_group_select.on_change("value", on_download_group_change)
    download_scope_select.on_change("value", on_download_scope_change)
    bybit_fee_mode_select.on_change("value", on_bybit_fee_mode_change)
    stop_mode_select.on_change("value", on_stop_mode_change)
    optimization_mode_select.on_change("value", on_optimization_mode_change)
    optimization_range_mode_select.on_change("value", on_optimization_range_mode_change)
    opt_stop_mode_select.on_change("value", on_opt_stop_mode_change)
    meta_model_select.on_change("value", lambda _attr, _old, _new: sync_meta_model_ui())
    wfa_period_slider.on_change("value", on_wfa_config_change)
    wfa_unit_select.on_change("value", on_wfa_config_change)
    wfa_lookback_input.on_change("value", on_wfa_config_change)
    wfa_test_input.on_change("value", on_wfa_config_change)
    for display_widget in [plot_font_size_input, plot_height_single_input, plot_height_two_input, plot_height_three_input, plot_height_four_input]:
        display_widget.on_change("value", on_display_settings_change)
        if "value_throttled" in display_widget.properties():
            display_widget.on_change("value_throttled", on_display_settings_change)
    for key, _plot, _source, _columns, body, toggle in plot_bindings:
        toggle.on_click(build_section_toggle_handler(key, body, toggle))
        body.on_change("visible", on_section_visibility_change)
    for key, body, toggle in block_bindings:
        toggle.on_click(build_section_toggle_handler(key, body, toggle))
        body.on_change("visible", on_section_visibility_change)
    service_log_toggle.on_click(toggle_service_log)
    service_log_body.on_change("visible", lambda _attr, _old, _new: sync_service_log_toggle())
    summary_div.on_change("text", build_service_log_handler("tester"))
    optimization_status_div.on_change("text", build_service_log_handler("optimizer"))
    scan_status_div.on_change("text", build_service_log_handler("cointegration"))
    download_status_div.on_change("text", build_service_log_handler("downloader"))
    wfa_status_div.on_change("text", build_service_log_handler("wfa"))
    meta_status_div.on_change("text", build_service_log_handler("meta_selector"))
    state.shared_x_range.on_change("start", on_range_change)
    state.shared_x_range.on_change("end", on_range_change)
    state.trades_source.selected.on_change("indices", on_trade_selection)
    wfa_table_source.selected.on_change("indices", on_wfa_selection)
    meta_table_source.selected.on_change("indices", on_meta_selection)
    meta_ranking_source.selected.on_change("indices", on_meta_ranking_selection)
    state.optimization_source.selected.on_change("indices", on_optimization_selection)
    state.scan_source.selected.on_change("indices", on_scan_selection)
    run_button.on_click(on_run_test)
    optimization_run_button.on_click(on_run_optimization)
    scan_run_button.on_click(on_run_scan)
    download_run_button.on_click(on_run_download)
    wfa_run_button.on_click(on_run_wfa)
    meta_run_button.on_click(on_run_meta)

    top_toggle_bar = row(
        price_1_toggle,
        price_2_toggle,
        spread_toggle,
        zscore_toggle,
        equity_toggle,
        trades_toggle,
        optimization_toggle,
        display_toggle,
        wfa_toggle,
        meta_toggle,
        scan_toggle,
        download_toggle,
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
        display_block,
        wfa_block,
        meta_block,
        scan_block,
        download_block,
        Spacer(sizing_mode="stretch_both"),
        service_log_body,
        service_log_toggle,
        sizing_mode="stretch_both",
    )

    root = row(sidebar, right_panel, sizing_mode="stretch_both")
    doc.title = "MT Pair Tester"
    doc.on_session_destroyed(on_session_destroyed)
    doc.add_root(root)
    file_state_controller.restore()
    refresh_instruments()
    file_state_controller.restore()
    sync_symbol_2_filter(show_message=False)
    file_state_controller.restore()
    restore_saved_scan_table(show_message=False)
    sync_downloader_mode_ui()
    sync_downloader_symbol_options(show_message=False)
    sync_bybit_fee_mode()
    sync_optimization_mode_ui()
    sync_stop_mode_ui()
    sync_meta_model_ui()
    install_optimizer_tooltips()
    sync_service_log_toggle()
    append_service_log("tester", summary_div.text)
    append_service_log("optimizer", optimization_status_div.text)
    append_service_log("cointegration", scan_status_div.text)
    append_service_log("downloader", download_status_div.text)
    append_service_log("wfa", wfa_status_div.text)
    append_service_log("meta_selector", meta_status_div.text)
    restore_saved_wfa_result(show_message=False)
    ensure_nonempty_layout()
    apply_plot_display_settings()
    rebalance_layout()
    refresh_plot_ranges()
    file_state_controller.install_model_watchers()
    file_state_controller.persist()


build_document()

