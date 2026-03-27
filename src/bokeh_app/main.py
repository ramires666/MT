from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
import json
from html import unescape
from math import ceil, floor
import os
import re
from pathlib import Path
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
from bokeh_app.adapters import (
    empty_backtest_sources,
    optimization_results_to_source,
    result_to_padded_sources,
    result_to_sources,
    scan_results_to_source,
)
from bokeh_app.browser_state import BrowserStateBinding
from bokeh_app.file_state import FileStateController
from bokeh_app.numeric_inputs import normalize_fractional_value
from bokeh_app.state import AppState
from bokeh_app.table_export import export_table_to_xlsx, metadata_rows_from_mapping
from bokeh_app.view_utils import (
    compute_overlay_label_layout,
    compute_plot_height,
    compute_relative_plot_height,
    compute_series_bounds,
    display_symbol_label,
)
from bokeh_app.zscore_diagnostics import (
    build_zscore_diagnostics,
    empty_zscore_hist_source,
    empty_zscore_metric_source,
)
from domain.backtest.distance import DistanceParameters, load_pair_frame, run_distance_backtest
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
from domain.data.co_movers import (
    ALL_CO_MOVERS_LABEL,
    co_mover_group_labels_for_symbol,
    co_mover_symbols_for_symbol,
)
from domain.data.catalog_groups import ALL_GROUP_OPTION, filter_catalog_by_group, list_mt5_group_options
from domain.data.io import load_instrument_catalog_frame
from domain.optimizer import DistanceOptimizationResult, OBJECTIVE_METRICS, count_distance_parameter_grid, optimize_distance_genetic, optimize_distance_grid
from domain.scan.johansen import JohansenScanParameters, JohansenUniverseScanResult, scan_universe_johansen
from domain.wfa import run_distance_genetic_wfa
from domain.wfa_serialization import serialize_time
from domain.meta_selector import DEFAULT_META_TARGET, SUPPORTED_META_MODELS, load_saved_meta_selector_result, run_meta_selector
from domain.meta_selector_ml import normalized_model_config
from domain.portfolio import (
    PortfolioAllocationSuggestionRow,
    PortfolioCorrelationRow,
    PortfolioCurve,
    PortfolioRunRow,
    analyze_portfolio_curves,
    combine_portfolio_equity_curves,
    latest_portfolio_oos_started_at,
    materialize_portfolio_backtest_allocations,
    portfolio_strategy_started_at,
    prepend_flat_equity_prefix,
    scale_defaults_for_portfolio_item,
)
from storage.paths import ui_state_path
from storage.portfolio_store import build_portfolio_item, load_portfolio_items, remove_portfolio_items, upsert_portfolio_item
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
GROUP_OPTIONS = [ALL_GROUP_OPTION]
TIMEFRAME_OPTIONS = [item.value for item in Timeframe]
UNIT_ROOT_TEST_OPTIONS = [item.value for item in UnitRootTest]
SCAN_UNIVERSE_OPTIONS = list(GROUP_OPTIONS)
SIGNIFICANCE_OPTIONS = ["0.10", "0.05", "0.01"]
OPTIMIZATION_MODE_OPTIONS = [item.value for item in OptimizationMode]
OBJECTIVE_OPTIONS = list(OBJECTIVE_METRICS)
BYBIT_FEE_MODE_OPTIONS = ["tight_spread", "zero_fee"]
STOP_MODE_OPTIONS = ["enabled", "disabled"]
OPTIMIZATION_RANGE_MODE_OPTIONS = ["manual", "auto"]
LEG_2_FILTER_OPTIONS = ["all_symbols", "co_movers", "cointegrated_only"]
NO_CO_MOVER_GROUP_LABEL = "-- no co-mover groups --"
DOWNLOAD_SCOPE_OPTIONS = ["symbol", "group"]
DOWNLOAD_POLICY_OPTIONS = ["missing_only", "force"]
WFA_UNIT_OPTIONS = [item.value for item in WfaWindowUnit]
PORTFOLIO_ALLOCATION_OPTIONS = ["equal_weight", "diversified_risk"]
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
    if cast is not int:
        normalized = normalize_fractional_value(widget.value, step=widget.step, low=widget.low, high=widget.high)
        if normalized is not None:
            return float(normalized)
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
    tester_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="distance-tester")
    tester_future: Future[dict[str, object]] | None = None
    tester_poll_callback: object | None = None
    optimization_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="distance-optimizer")
    optimization_future: Future[
        tuple[DistanceOptimizationResult, dict[str, list[object]], str, PairSelection, dict[str, object]]
    ] | None = None
    optimization_poll_callback: object | None = None
    optimization_cancel_event = Event()
    optimizer_parallel_workers = max(1, int(settings.optimizer_parallel_workers))
    optimization_progress = {"completed": 0, "total": 0, "stage": "Idle"}
    optimization_request_context: dict[str, object] | None = None
    displayed_optimization_signature: dict[str, object] | None = None
    optimization_summary_html = ""
    suppress_optimization_selection = False
    suppress_optimization_config_change = False
    suppress_shared_range_change = False
    suppress_portfolio_range_change = False
    suppress_symbol_change_refresh = 0
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
    displayed_meta_signature: dict[str, object] | None = None
    portfolio_run_rows_by_id: dict[str, PortfolioRunRow] = {}
    portfolio_curves_by_id: dict[str, PortfolioCurve] = {}
    portfolio_curve_sources_by_id: dict[str, dict[str, list[object]]] = {}
    portfolio_items_by_id: dict[str, object] = {}
    portfolio_excluded_item_ids: set[str] = set()
    portfolio_preview_item_id: str | None = None
    current_tester_context: dict[str, object] = {
        "source_kind": "tester_manual",
        "oos_started_at": None,
        "context_started_at": None,
        "context_ended_at": None,
    }
    service_log_lines: list[str] = []
    service_log_last_message: dict[str, str] = {}

    summary_div = Div(text="<p>Ready. Refresh instruments, choose a pair, and run the first Distance test.</p>")
    refresh_button = Button(label="Refresh Instruments", button_type="primary")
    reset_defaults_button = Button(label="Reset Defaults", button_type="default")
    group_select = Select(title="Group", value=GROUP_OPTIONS[0], options=GROUP_OPTIONS, width=170)
    symbol_1_select = Select(title="Symbol 1", value=INSTRUMENT_PLACEHOLDER, options=[INSTRUMENT_PLACEHOLDER], width=150)
    symbol_2_select = Select(title="Symbol 2", value=INSTRUMENT_PLACEHOLDER, options=[INSTRUMENT_PLACEHOLDER], width=150)
    leg_2_filter_select = Select(
        title="Leg 2 Filter",
        value="all_symbols",
        options=LEG_2_FILTER_OPTIONS,
        description="all_symbols keeps the full group list. co_movers limits leg 2 to local predefined co-move groups for Symbol 1. cointegrated_only limits leg 2 to partners from the latest saved scan for the selected type and timeframe.",
    )
    co_mover_group_select = Select(
        title="Co-Mover Group",
        value=ALL_CO_MOVERS_LABEL,
        options=[ALL_CO_MOVERS_LABEL],
        visible=False,
        description="Choose which local co-mover cluster is used for Symbol 2 filtering. The default unions all matching co-mover groups for Symbol 1.",
    )
    leg_2_cointegration_kind_select = Select(
        title="Cointegration Type",
        value=COINTEGRATION_KIND_OPTIONS[0],
        options=list(COINTEGRATION_KIND_OPTIONS),
        visible=False,
        description="Select which saved cointegration list is used for leg 2 filtering. New scans are currently implemented for johansen; other types can still be browsed when saved.",
    )
    timeframe_select = Select(title="Timeframe", value=Timeframe.M15.value, options=TIMEFRAME_OPTIONS, width=120)
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
    exit_input = Spinner(title="Exit Z", low=-5.0, step=0.1, value=0.5)
    stop_input = Spinner(title="Stop Z", low=0.1, step=0.1, value=3.5)
    bollinger_input = Spinner(title="Bollinger K", low=0.1, step=0.1, value=2.0)
    run_button = Button(label="Run Test", button_type="success")
    add_to_portfolio_button = Button(label="В портфель", button_type="default")

    universe_group_row = row(
        group_select,
        timeframe_select,
        sizing_mode="stretch_width",
        styles={"gap": "12px", "align-items": "flex-end"},
    )
    universe_symbols_row = row(
        symbol_1_select,
        symbol_2_select,
        sizing_mode="stretch_width",
        styles={"gap": "12px", "align-items": "flex-end"},
    )

    run_controls_row = row(
        run_button,
        add_to_portfolio_button,
        sizing_mode="stretch_width",
        styles={"gap": "12px"},
    )

    sidebar = column(
        Div(text="<div class='sidebar-heading'><h2>MT Pair Tester</h2><p>Distance tester slice with responsive charts.</p></div>"),
        Div(text="<h3>Universe</h3>"),
        row(refresh_button, reset_defaults_button, sizing_mode="stretch_width"),
        universe_group_row,
        universe_symbols_row,
        leg_2_filter_select,
        co_mover_group_select,
        leg_2_cointegration_kind_select,
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
        run_controls_row,
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
    opt_exit_start = Spinner(title="Exit Z Start", low=-5.0, step=0.1, value=0.3)
    opt_exit_stop = Spinner(title="Exit Z Stop", low=-5.0, step=0.1, value=0.7)
    opt_exit_step = Spinner(title="Exit Z Step", low=0.1, step=0.1, value=0.2)
    opt_stop_mode_select = Select(title="Opt Stop Z", value="enabled", options=STOP_MODE_OPTIONS)
    opt_stop_start = Spinner(title="Stop Z Start", low=0.1, step=0.1, value=3.0)
    opt_stop_stop = Spinner(title="Stop Z Stop", low=0.1, step=0.1, value=4.0)
    opt_stop_step = Spinner(title="Stop Z Step", low=0.1, step=0.1, value=0.5)
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
    opt_exit_start_control = optimizer_help(opt_exit_start, "Minimum Exit Z threshold. Negative values mean hold the trade until z-score crosses to the opposite side. Example: exit_z = -1.0 exits only after the opposite signal reaches 1.0.", width=160)
    opt_exit_stop_control = optimizer_help(opt_exit_stop, "Maximum Exit Z threshold. Keep exit_z lower than entry_z; negative values are allowed for opposite-signal exits.", width=160)
    opt_exit_step_control = optimizer_help(opt_exit_step, "Exit Z increment in manual-grid mode. Step stays positive even when the range includes negative Exit Z values.", width=160)
    opt_stop_mode_control = optimizer_help(opt_stop_mode_select, "Enable or disable the statistical stop. disabled means no emergency exit by Stop Z.", width=160)
    opt_stop_start_control = optimizer_help(opt_stop_start, "Minimum Stop Z. This is a statistical stop on spread divergence, not a plain price stop-loss.", width=160)
    opt_stop_stop_control = optimizer_help(opt_stop_stop, "Maximum Stop Z.", width=160)
    opt_stop_step_control = optimizer_help(opt_stop_step, "Stop Z increment in manual-grid mode.", width=160)
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

    def sync_price_plot_labels() -> None:
        label_1 = display_symbol_label(symbol_1_select.value, fallback="Price 1", placeholder=INSTRUMENT_PLACEHOLDER)
        label_2 = display_symbol_label(symbol_2_select.value, fallback="Price 2", placeholder=INSTRUMENT_PLACEHOLDER)
        price_1.title.text = label_1
        price_2.title.text = label_2
        price_1.yaxis.axis_label = label_1
        price_2.yaxis.axis_label = label_2

    sync_price_plot_labels()

    spread_plot = _build_figure("Spread / Residual", state.shared_x_range, "Spread", 360)
    spread_plot.line("x", "spread", source=state.spread_source, line_width=2, color="#6366f1")

    zscore_plot = _build_figure("Z-score + Bollinger", state.shared_x_range, "Z-score", 360)
    zscore_plot.line("x", "zscore", source=state.zscore_source, line_width=2, color="#f97316")
    zscore_plot.line("x", "upper", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")
    zscore_plot.line("x", "lower", source=state.zscore_source, line_dash="dashed", line_color="#9ca3af")

    zscore_metrics_source = ColumnDataSource(empty_zscore_metric_source())
    zscore_hist_source = ColumnDataSource(empty_zscore_hist_source())
    zscore_hist_plot = figure(
        title="Z-score Distribution",
        x_range=Range1d(start=-1.0, end=1.0),
        y_range=Range1d(start=0.0, end=1.0),
        height=300,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    zscore_hist_plot.toolbar.autohide = True
    zscore_hist_plot.xaxis.axis_label = "Z-score"
    zscore_hist_plot.yaxis.axis_label = "Share of Valid Bars"
    zscore_hist_plot.xaxis.ticker = FixedTicker(ticks=[-1.0, 0.0, 1.0])
    zscore_hist_plot.quad(
        top="share",
        bottom=0.0,
        left="left",
        right="right",
        source=zscore_hist_source,
        fill_color="#f97316",
        fill_alpha=0.36,
        line_color="#c2410c",
        line_alpha=0.55,
    )
    zscore_hist_zero_span = Span(location=0.0, dimension="height", line_color="#0f172a", line_alpha=0.45, line_width=2)
    zscore_hist_entry_pos_span = Span(location=0.0, dimension="height", line_color="#2563eb", line_alpha=0.65, line_width=2, line_dash="dashed", visible=False)
    zscore_hist_entry_neg_span = Span(location=0.0, dimension="height", line_color="#2563eb", line_alpha=0.65, line_width=2, line_dash="dashed", visible=False)
    zscore_hist_exit_pos_span = Span(location=0.0, dimension="height", line_color="#16a34a", line_alpha=0.70, line_width=2, line_dash="dotdash", visible=False)
    zscore_hist_exit_neg_span = Span(location=0.0, dimension="height", line_color="#16a34a", line_alpha=0.70, line_width=2, line_dash="dotdash", visible=False)
    zscore_hist_stop_pos_span = Span(location=0.0, dimension="height", line_color="#dc2626", line_alpha=0.70, line_width=2, line_dash="dotted", visible=False)
    zscore_hist_stop_neg_span = Span(location=0.0, dimension="height", line_color="#dc2626", line_alpha=0.70, line_width=2, line_dash="dotted", visible=False)
    for span in [
        zscore_hist_zero_span,
        zscore_hist_entry_pos_span,
        zscore_hist_entry_neg_span,
        zscore_hist_exit_pos_span,
        zscore_hist_exit_neg_span,
        zscore_hist_stop_pos_span,
        zscore_hist_stop_neg_span,
    ]:
        zscore_hist_plot.add_layout(span)
    zscore_metrics_table = DataTable(
        source=zscore_metrics_source,
        columns=[
            TableColumn(field="metric", title="Metric", width=180),
            TableColumn(field="value", title="Value", width=92),
            TableColumn(field="note", title="Meaning", width=220),
        ],
        sizing_mode="stretch_width",
        height=300,
        sortable=False,
        reorderable=False,
        index_position=None,
    )
    zscore_metrics_div = Div(text="<p>Run a test to inspect z-score distribution, percentiles and threshold hit-rates.</p>")

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
    optimization_train_box = BoxAnnotation(fill_color="#9ca3af", fill_alpha=0.10, line_alpha=0.0, visible=False)
    optimization_train_start_span = Span(location=0.0, dimension="height", line_color="#6b7280", line_alpha=0.75, line_width=2, line_dash="dashed", visible=False)
    equity_plot.add_layout(optimization_train_box)
    equity_plot.add_layout(optimization_train_start_span)
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
        description="Choose which universe to scan or browse: all symbols or one MT5 catalog group.",
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
        high=1,
        step=1,
        value=0,
        width=105,
        description="Johansen deterministic term setting. statsmodels coint_johansen supports only -1 (none), 0 (constant), 1 (linear trend).",
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

    def _selected_symbols_for_export() -> tuple[str, str]:
        symbol_1 = str(symbol_1_select.value or "na")
        symbol_2 = str(symbol_2_select.value or "na")
        if symbol_1 == INSTRUMENT_PLACEHOLDER:
            symbol_1 = "na"
        if symbol_2 == INSTRUMENT_PLACEHOLDER:
            symbol_2 = "na"
        return symbol_1, symbol_2

    def _format_export_period(raw_start: object, raw_end: object) -> str:
        started_at = _coerce_datetime(raw_start)
        ended_at = _coerce_datetime(raw_end)
        return f"{started_at:%Y-%m-%d %H:%M} .. {ended_at:%Y-%m-%d %H:%M} UTC"

    def _format_manual_range(start: object, stop: object, step: object) -> str:
        return f"{start} .. {stop} step {step}"

    def _tester_export_metadata() -> list[tuple[str, object]]:
        defaults = build_defaults()
        params = build_distance_params()
        return [
            ("Algorithm", algorithm_select.value),
            ("Test Period", _format_export_period(period_slider.value[0], period_slider.value[1])),
            ("Fee Mode", bybit_fee_mode_select.value),
            ("Initial Capital", defaults.initial_capital),
            ("Leverage", defaults.leverage),
            ("Margin Budget Per Leg", defaults.margin_budget_per_leg),
            ("Slippage Points", defaults.slippage_points),
            ("Lookback Bars", params.lookback_bars),
            ("Entry Z", params.entry_z),
            ("Exit Z", params.exit_z),
            ("Stop Mode", stop_mode_select.value),
            ("Stop Z", "disabled" if params.stop_z is None else params.stop_z),
            ("Bollinger K", params.bollinger_k),
        ]

    def _optimization_export_metadata() -> list[tuple[str, object]]:
        defaults = build_defaults()
        metadata: list[tuple[str, object]] = [
            ("Mode", optimization_mode_select.value),
            ("Objective", optimization_objective_select.value),
            ("Range Mode", optimization_range_mode_select.value),
            ("Optimization Period", _format_export_period(optimization_period_slider.value[0], optimization_period_slider.value[1])),
            ("Fee Mode", bybit_fee_mode_select.value),
            ("Initial Capital", defaults.initial_capital),
            ("Leverage", defaults.leverage),
            ("Margin Budget Per Leg", defaults.margin_budget_per_leg),
            ("Slippage Points", defaults.slippage_points),
            ("Lookback Range", _format_manual_range(opt_lookback_start.value, opt_lookback_stop.value, opt_lookback_step.value)),
            ("Entry Z Range", _format_manual_range(opt_entry_start.value, opt_entry_stop.value, opt_entry_step.value)),
            ("Exit Z Range", _format_manual_range(opt_exit_start.value, opt_exit_stop.value, opt_exit_step.value)),
            ("Stop Mode", opt_stop_mode_select.value),
            ("Stop Z Range", "disabled" if opt_stop_mode_select.value == "disabled" else _format_manual_range(opt_stop_start.value, opt_stop_stop.value, opt_stop_step.value)),
            ("Bollinger K", bollinger_input.value),
        ]
        if optimization_mode_select.value == OptimizationMode.GENETIC.value:
            metadata.extend(
                [
                    ("Population Size", genetic_population_input.value),
                    ("Generations", genetic_generations_input.value),
                    ("Elite Count", genetic_elite_input.value),
                    ("Mutation Rate", genetic_mutation_input.value),
                    ("Random Seed", genetic_seed_input.value if genetic_seed_input.value is not None else "none"),
                ]
            )
        else:
            metadata.append(("Auto Grid Target Trials", auto_grid_trials_input.value))
        return metadata

    def _scan_export_metadata() -> list[tuple[str, object]]:
        return [
            ("Universe", scan_universe_select.value),
            ("Type", scan_kind_select.value),
            ("Unit Root Gate", scan_unit_root_select.value),
            ("Significance", scan_significance_select.value),
            ("det_order", scan_det_order_input.value),
            ("k_ar_diff", scan_k_ar_diff_input.value),
            ("Scan Period", _format_export_period(scan_period_slider.value[0], scan_period_slider.value[1])),
        ]

    def _build_table_export_controls(
        *,
        block_name: str,
        table: DataTable,
        extra_metadata_builder,
    ):
        button = Button(label="Save XLSX", width=104)
        status = Div(text="", sizing_mode="stretch_width")

        def on_export_click() -> None:
            symbol_1, symbol_2 = _selected_symbols_for_export()
            try:
                metadata = metadata_rows_from_mapping(
                    [
                        ("Export Block", block_name),
                        ("Exported At UTC", datetime.now(UTC)),
                        ("Symbol 1", symbol_1),
                        ("Symbol 2", symbol_2),
                        ("Timeframe", timeframe_select.value),
                        *list(extra_metadata_builder()),
                    ]
                )
                path = export_table_to_xlsx(
                    table=table,
                    block_name=block_name,
                    symbol_1=symbol_1,
                    symbol_2=symbol_2,
                    timeframe=timeframe_select.value,
                    metadata_rows=metadata,
                )
                try:
                    relative_path = path.relative_to(Path(__file__).resolve().parents[2])
                except ValueError:
                    relative_path = path
                status.text = f"<p>Saved <b>{block_name}</b> to <b>{relative_path}</b>.</p>"
            except Exception as exc:
                status.text = f"<p>Failed to save <b>{block_name}</b>: {exc}</p>"

        button.on_click(on_export_click)
        return row(button, status, sizing_mode="stretch_width", styles={"gap": "12px", "align-items": "center"})

    trades_export_controls = _build_table_export_controls(
        block_name="trades",
        table=trades_table,
        extra_metadata_builder=_tester_export_metadata,
    )
    zscore_metrics_export_controls = _build_table_export_controls(
        block_name="zscore_metrics",
        table=zscore_metrics_table,
        extra_metadata_builder=_tester_export_metadata,
    )
    optimization_export_controls = _build_table_export_controls(
        block_name="optimization_results",
        table=optimization_table,
        extra_metadata_builder=_optimization_export_metadata,
    )
    scan_export_controls = _build_table_export_controls(
        block_name="johansen_scan_results",
        table=scan_table,
        extra_metadata_builder=_scan_export_metadata,
    )

    trades_content = column(trades_export_controls, trades_table, sizing_mode="stretch_width", styles={"gap": "10px"})
    zscore_metrics_content = column(
        zscore_metrics_div,
        zscore_metrics_export_controls,
        row(
            zscore_metrics_table,
            zscore_hist_plot,
            sizing_mode="stretch_width",
            styles={"gap": "18px", "align-items": "stretch"},
        ),
        sizing_mode="stretch_width",
        styles={"gap": "12px"},
    )
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
        optimization_export_controls,
        optimization_table,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
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
    scan_content = column(scan_controls, scan_export_controls, scan_table, sizing_mode="stretch_width", styles={"gap": "10px"})
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
    wfa_objective_select = Select(title="Objective", value="score_log_trades", options=OBJECTIVE_OPTIONS, width=150)
    wfa_unit_select = Select(title="Unit", value=WfaWindowUnit.WEEKS.value, options=WFA_UNIT_OPTIONS, width=96)
    wfa_lookback_input = Spinner(title="Lookback", low=1, step=1, value=8, width=92)
    wfa_test_input = Spinner(title="Test", low=1, step=1, value=2, width=92)
    wfa_run_button = Button(label="Run WFA", button_type="primary", width=120)
    wfa_status_div = Div(text="<p>Ready to run rolling WFA on the selected tester pair using genetic optimization and the selected objective.</p>")
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
    wfa_train_window_box = BoxAnnotation(fill_color="#9ca3af", fill_alpha=0.12, line_alpha=0.0, visible=False)
    wfa_train_start_span = Span(location=0.0, dimension="height", line_color="#6b7280", line_alpha=0.75, line_width=2, line_dash="dashed", visible=False)
    wfa_test_window_box = BoxAnnotation(fill_color="#0ea5e9", fill_alpha=0.10, line_color="#0284c7", line_alpha=0.45, line_width=2, visible=False)
    wfa_equity_plot.add_layout(wfa_train_window_box)
    wfa_equity_plot.add_layout(wfa_train_start_span)
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
            wfa_objective_select,
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
    def _wfa_export_metadata() -> list[tuple[str, object]]:
        defaults = build_defaults()
        return [
            ("WFA Period", _format_export_period(wfa_period_slider.value[0], wfa_period_slider.value[1])),
            ("Objective", wfa_objective_select.value),
            ("Unit", wfa_unit_select.value),
            ("Lookback Windows", wfa_lookback_input.value),
            ("Test Windows", wfa_test_input.value),
            ("Fee Mode", bybit_fee_mode_select.value),
            ("Initial Capital", defaults.initial_capital),
            ("Leverage", defaults.leverage),
            ("Margin Budget Per Leg", defaults.margin_budget_per_leg),
            ("Slippage Points", defaults.slippage_points),
        ]

    wfa_export_controls = _build_table_export_controls(
        block_name="wfa_folds",
        table=wfa_table,
        extra_metadata_builder=_wfa_export_metadata,
    )
    wfa_outputs = column(
        wfa_equity_plot,
        wfa_export_controls,
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
    meta_objective_select = Select(
        title="Objective",
        value=str(wfa_objective_select.value or "score_log_trades"),
        options=OBJECTIVE_OPTIONS,
        width=150,
    )
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
    meta_xgb_early_stopping_rounds_input = Spinner(title="Early Stop", low=1, high=1000, step=1, value=30, width=96)
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
    meta_oos_cutoff_span = Span(location=0.0, dimension="height", line_color="#dc2626", line_alpha=0.55, line_width=2, visible=False)
    meta_selected_fold_box = BoxAnnotation(fill_color="#7c3aed", fill_alpha=0.08, line_color="#7c3aed", line_alpha=0.30, line_width=2, visible=False)
    meta_selected_fold_span = Span(location=0.0, dimension="height", line_color="#7c3aed", line_alpha=0.85, line_width=2, visible=False)
    meta_equity_plot.add_layout(meta_selected_fold_box)
    meta_equity_plot.add_layout(meta_oos_cutoff_span)
    meta_equity_plot.add_layout(meta_selected_fold_span)
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
        meta_xgb_early_stopping_rounds_input,
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
            meta_objective_select,
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
    def _meta_export_metadata() -> list[tuple[str, object]]:
        model_config = normalized_model_config(str(meta_model_select.value or ""), build_meta_model_config())
        return [
            ("Model", meta_model_select.value),
            ("Meta Objective", meta_objective_select.value),
            ("WFA Objective", wfa_objective_select.value),
            ("OOS Start", meta_oos_start_picker.value or "unset"),
            ("Model Config", model_config),
        ]

    meta_ranking_export_controls = _build_table_export_controls(
        block_name="meta_robustness_grid",
        table=meta_ranking_table,
        extra_metadata_builder=_meta_export_metadata,
    )
    meta_selected_export_controls = _build_table_export_controls(
        block_name="selected_oos_folds",
        table=meta_table,
        extra_metadata_builder=_meta_export_metadata,
    )
    meta_content = column(
        meta_controls,
        meta_equity_plot,
        meta_ranking_title,
        meta_ranking_export_controls,
        meta_ranking_table,
        meta_selected_title,
        meta_selected_export_controls,
        meta_table,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )

    def empty_portfolio_table_data() -> dict[str, list[object]]:
        return {
            "item_id": [],
            "portfolio_use": [],
            "saved_at": [],
            "source_kind": [],
            "symbol_1": [],
            "symbol_2": [],
            "timeframe": [],
            "oos_started_at_label": [],
            "lookback_bars": [],
            "entry_z": [],
            "exit_z": [],
            "stop_z_label": [],
            "bollinger_k": [],
            "allocation_capital": [],
            "net_profit": [],
            "ending_equity": [],
            "max_drawdown": [],
            "trades": [],
            "run_status": [],
        }

    def portfolio_items_to_source(
        items: list,
        run_rows_by_id: dict[str, PortfolioRunRow] | None = None,
        *,
        active_item_ids: set[str] | None = None,
    ) -> dict[str, list[object]]:
        rows = empty_portfolio_table_data()
        lookup = run_rows_by_id or {}
        active_lookup = active_item_ids if active_item_ids is not None else {item.item_id for item in items}
        for item in items:
            run_row = lookup.get(item.item_id)
            rows["item_id"].append(item.item_id)
            rows["portfolio_use"].append("on" if item.item_id in active_lookup else "off")
            rows["saved_at"].append(item.saved_at)
            rows["source_kind"].append(item.source_kind)
            rows["symbol_1"].append(item.symbol_1)
            rows["symbol_2"].append(item.symbol_2)
            rows["timeframe"].append(item.timeframe.value)
            rows["oos_started_at_label"].append("" if item.oos_started_at is None else f"{item.oos_started_at:%Y-%m-%d}")
            rows["lookback_bars"].append(int(item.lookback_bars))
            rows["entry_z"].append(float(item.entry_z))
            rows["exit_z"].append(float(item.exit_z))
            rows["stop_z_label"].append("disabled" if item.stop_z is None else f"{float(item.stop_z):.2f}")
            rows["bollinger_k"].append(float(item.bollinger_k))
            rows["allocation_capital"].append(None if run_row is None else float(run_row.allocation_capital))
            rows["net_profit"].append(None if run_row is None or run_row.net_profit is None else float(run_row.net_profit))
            rows["ending_equity"].append(None if run_row is None or run_row.ending_equity is None else float(run_row.ending_equity))
            rows["max_drawdown"].append(None if run_row is None or run_row.max_drawdown is None else float(run_row.max_drawdown))
            rows["trades"].append(None if run_row is None or run_row.trades is None else int(run_row.trades))
            rows["run_status"].append("" if run_row is None else run_row.status)
        return rows

    def empty_portfolio_weight_data() -> dict[str, list[object]]:
        return {
            "item_id": [],
            "label": [],
            "return_volatility": [],
            "mean_abs_return_corr": [],
            "diversification_score": [],
            "suggested_weight_pct": [],
        }

    def empty_portfolio_correlation_data() -> dict[str, list[object]]:
        return {
            "left_label": [],
            "right_label": [],
            "equity_corr": [],
            "return_corr": [],
        }

    def portfolio_weight_rows_to_source(rows: list[PortfolioAllocationSuggestionRow]) -> dict[str, list[object]]:
        data = empty_portfolio_weight_data()
        for row in rows:
            data["item_id"].append(row.item_id)
            data["label"].append(row.label)
            data["return_volatility"].append(float(row.return_volatility))
            data["mean_abs_return_corr"].append(float(row.mean_abs_return_corr))
            data["diversification_score"].append(float(row.diversification_score))
            data["suggested_weight_pct"].append(float(row.suggested_weight) * 100.0)
        return data

    def portfolio_correlation_rows_to_source(rows: list[PortfolioCorrelationRow]) -> dict[str, list[object]]:
        data = empty_portfolio_correlation_data()
        for row in rows:
            data["left_label"].append(row.left_label)
            data["right_label"].append(row.right_label)
            data["equity_corr"].append(float(row.equity_corr))
            data["return_corr"].append(float(row.return_corr))
        return data

    portfolio_table_source = ColumnDataSource(empty_portfolio_table_data())
    portfolio_table_action_source = ColumnDataSource({"item_id": [], "nonce": []})
    portfolio_equity_source = ColumnDataSource({"time": [], "equity": []})
    portfolio_weight_source = ColumnDataSource(empty_portfolio_weight_data())
    portfolio_correlation_source = ColumnDataSource(empty_portfolio_correlation_data())
    portfolio_period_slider = DateRangeSlider(
        title="Portfolio Period",
        start=_ui_datetime(history_start),
        end=_ui_datetime(now_utc),
        value=(_ui_datetime(history_start), _ui_datetime(now_utc)),
    )
    portfolio_allocation_select = Select(title="Allocation", value=PORTFOLIO_ALLOCATION_OPTIONS[0], options=PORTFOLIO_ALLOCATION_OPTIONS, width=170)
    portfolio_run_button = Button(label="Run Portfolio", button_type="primary", width=130)
    portfolio_analyze_button = Button(label="Analyze Portfolio", button_type="default", width=150)
    portfolio_reload_button = Button(label="Reload", button_type="default", width=90)
    portfolio_remove_button = Button(label="Remove Selected", button_type="default", width=140)
    portfolio_status_div = Div(text="<p>Portfolio is empty. Use <b>В портфель</b> in the tester to save pairs and strategy parameters.</p>")
    portfolio_analysis_div = Div(
        text=(
            "<p>Use <b>Analyze Portfolio</b> to calculate pairwise equity/return correlations and get "
            "a diversification-weight suggestion. The second allocation mode, <b>diversified_risk</b>, "
            "uses inverse return volatility penalized by mean absolute return correlation.</p>"
        )
    )
    portfolio_equity_base_title = "Portfolio Equity"
    portfolio_equity_plot = figure(
        title=portfolio_equity_base_title,
        x_axis_type="datetime",
        x_range=Range1d(start=_datetime_to_bokeh_millis(history_start), end=_datetime_to_bokeh_millis(now_utc)),
        y_range=Range1d(start=0.0, end=1.0),
        height=390,
        sizing_mode="stretch_width",
        tools="pan,wheel_zoom,box_zoom,reset,save",
        active_scroll="wheel_zoom",
    )
    portfolio_wheel_zoom = portfolio_equity_plot.select_one(WheelZoomTool)
    if portfolio_wheel_zoom is not None:
        portfolio_wheel_zoom.modifiers = {"ctrl": True}
    portfolio_equity_plot.toolbar.autohide = True
    portfolio_equity_plot.yaxis.axis_label = "Equity"
    portfolio_oos_cutoff_span = Span(location=0.0, dimension="height", line_color="#dc2626", line_alpha=0.60, line_width=2, line_dash="dashed", visible=False)
    portfolio_equity_plot.add_layout(portfolio_oos_cutoff_span)
    portfolio_equity_plot.line("time", "equity", source=portfolio_equity_source, line_width=2.5, color="#0f172a")
    portfolio_table = DataTable(
        source=portfolio_table_source,
        columns=[
            TableColumn(field="portfolio_use", title="Use", width=52),
            TableColumn(field="saved_at", title="Saved", formatter=DateFormatter(format="%Y-%m-%d %H:%M"), width=122),
            TableColumn(field="source_kind", title="Source", width=112),
            TableColumn(field="symbol_1", title="Symbol 1", width=96),
            TableColumn(field="symbol_2", title="Symbol 2", width=96),
            TableColumn(field="timeframe", title="TF", width=54),
            TableColumn(field="oos_started_at_label", title="OOS", width=86),
            TableColumn(field="lookback_bars", title="Lookback", formatter=NumberFormatter(format="0"), width=82),
            TableColumn(field="entry_z", title="Entry", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="exit_z", title="Exit", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="stop_z_label", title="Stop", width=72),
            TableColumn(field="bollinger_k", title="Boll", formatter=NumberFormatter(format="0.00"), width=68),
            TableColumn(field="allocation_capital", title="Alloc", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="net_profit", title="Net", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="ending_equity", title="Ending", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="max_drawdown", title="Max DD", formatter=NumberFormatter(format="0.00"), width=88),
            TableColumn(field="trades", title="Trades", formatter=NumberFormatter(format="0"), width=68),
            TableColumn(field="run_status", title="Run", width=92),
        ],
        sizing_mode="stretch_width",
        height=260,
        sortable=True,
        reorderable=False,
        index_position=None,
    )
    portfolio_weight_table = DataTable(
        source=portfolio_weight_source,
        columns=[
            TableColumn(field="label", title="Pair", width=220),
            TableColumn(field="return_volatility", title="Ret Vol", formatter=NumberFormatter(format="0.000000"), width=98),
            TableColumn(field="mean_abs_return_corr", title="Mean |Ret Corr|", formatter=NumberFormatter(format="0.000"), width=110),
            TableColumn(field="diversification_score", title="Score", formatter=NumberFormatter(format="0.000"), width=86),
            TableColumn(field="suggested_weight_pct", title="Suggested %", formatter=NumberFormatter(format="0.00"), width=92),
        ],
        sizing_mode="stretch_width",
        height=200,
        sortable=True,
        reorderable=False,
        index_position=None,
    )
    portfolio_correlation_table = DataTable(
        source=portfolio_correlation_source,
        columns=[
            TableColumn(field="left_label", title="Pair A", width=220),
            TableColumn(field="right_label", title="Pair B", width=220),
            TableColumn(field="equity_corr", title="Equity Corr", formatter=NumberFormatter(format="0.000"), width=96),
            TableColumn(field="return_corr", title="Return Corr", formatter=NumberFormatter(format="0.000"), width=96),
        ],
        sizing_mode="stretch_width",
        height=220,
        sortable=True,
        reorderable=False,
        index_position=None,
    )
    portfolio_controls = column(
        row(
            portfolio_period_slider,
            portfolio_allocation_select,
            portfolio_run_button,
            portfolio_analyze_button,
            portfolio_reload_button,
            portfolio_remove_button,
            sizing_mode="stretch_width",
            styles={"gap": "12px", "align-items": "flex-end"},
        ),
        portfolio_status_div,
        portfolio_analysis_div,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )
    portfolio_content = column(
        portfolio_controls,
        portfolio_equity_plot,
        portfolio_table,
        Div(text="<b>Portfolio Allocation Analysis</b>"),
        portfolio_weight_table,
        portfolio_correlation_table,
        sizing_mode="stretch_width",
        styles={"gap": "10px"},
    )

    portfolio_table_double_click_callback = CustomJS(
        args=dict(table=portfolio_table, source=portfolio_table_source, action_source=portfolio_table_action_source),
        code="""
function pushChildViews(stack, view) {
  if (view == null) return;
  const childViews = view.child_views != null ? view.child_views : view._child_views;
  if (childViews == null) return;
  if (Array.isArray(childViews)) {
    for (const child of childViews) stack.push(child);
    return;
  }
  if (typeof childViews.values === "function") {
    for (const child of childViews.values()) stack.push(child);
    return;
  }
  if (typeof childViews[Symbol.iterator] === "function") {
    for (const child of childViews) stack.push(child);
  }
}
function findView(modelId) {
  const rootViews = Object.values(Bokeh.index || {});
  const stack = rootViews.slice();
  while (stack.length > 0) {
    const view = stack.pop();
    if (view == null) continue;
    if (view.model != null && view.model.id === modelId) return view;
    pushChildViews(stack, view);
  }
  return null;
}
function attach() {
  const view = findView(table.id);
  if (view == null) return false;
  const grid = view.grid;
  if (grid == null || grid.onDblClick == null || typeof grid.onDblClick.subscribe !== "function") return false;
  window.__mtTableDblClickBindings = window.__mtTableDblClickBindings || {};
  if (window.__mtTableDblClickBindings[table.id] === grid) return true;
  grid.onDblClick.subscribe((_event, args) => {
    const row = args != null ? args.row : null;
    if (!Number.isInteger(row) || row < 0) return;
    const sourceIndex = view.data != null && Array.isArray(view.data.index) && row < view.data.index.length
      ? view.data.index[row]
      : row;
    if (!Number.isInteger(sourceIndex) || sourceIndex < 0) return;
    const itemIds = source.data.item_id || [];
    if (sourceIndex >= itemIds.length) return;
    action_source.data = {
      item_id: [String(itemIds[sourceIndex])],
      nonce: [Date.now()],
    };
    action_source.change.emit();
  });
  window.__mtTableDblClickBindings[table.id] = grid;
  return true;
}
if (!attach()) {
  window.setTimeout(attach, 250);
}
""",
    )
    doc.js_on_event(DocumentReady, portfolio_table_double_click_callback)

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
    zscore_metrics_block, zscore_metrics_body, zscore_metrics_toggle = _build_section("Z-score Metrics", zscore_metrics_content)
    portfolio_block, portfolio_body, portfolio_toggle = _build_section("Portfolio", portfolio_content)
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
        ("zscore_metrics", zscore_metrics_body, zscore_metrics_toggle),
        ("portfolio", portfolio_body, portfolio_toggle),
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
        BrowserStateBinding("co_mover_group", co_mover_group_select, kind="select", restore_on_options_change=True),
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
        BrowserStateBinding("wfa_objective", wfa_objective_select, kind="select"),
        BrowserStateBinding("wfa_unit", wfa_unit_select, kind="select"),
        BrowserStateBinding("wfa_lookback", wfa_lookback_input),
        BrowserStateBinding("wfa_test", wfa_test_input),
        BrowserStateBinding("meta_model", meta_model_select, kind="select"),
        BrowserStateBinding("meta_objective", meta_objective_select, kind="select"),
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
        BrowserStateBinding("meta_xgb_early_stopping_rounds", meta_xgb_early_stopping_rounds_input),
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
        BrowserStateBinding("portfolio_period", portfolio_period_slider, kind="range"),
        BrowserStateBinding("portfolio_allocation", portfolio_allocation_select, kind="select"),
        BrowserStateBinding("show_price_1", price_1_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_price_2", price_2_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_spread", spread_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_zscore", zscore_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_equity", equity_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_trades", trades_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_zscore_metrics", zscore_metrics_body, property_name="visible", kind="visible"),
        BrowserStateBinding("show_portfolio", portfolio_body, property_name="visible", kind="visible"),
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
        baseline_y = 20
        gap = 18
        padding_px = 18
        overlay_anchor = "top" if plot_height <= 340 else "bottom"

        base_font_px = max(8, int(_read_spinner_value(plot_font_size_input, 11, cast=int)))
        chosen_font_px, positions = compute_overlay_label_layout(
            [label.text for label in visible_labels],
            plot_width,
            base_font_px=base_font_px,
            plot_height=plot_height,
            left_margin=left_margin,
            right_margin=16,
            gap=gap,
            padding_px=padding_px,
            baseline_y=baseline_y,
            top_margin=72,
            vertical_anchor=overlay_anchor,
        )
        for label, (x_pos, y_pos) in zip(visible_labels, positions):
            label.text_font_size = f"{chosen_font_px}px"
            label.x = x_pos
            label.y = y_pos

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
        for plot in [*plots, wfa_equity_plot, zscore_hist_plot]:
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

    def sync_optimization_train_overlay(*, test_started_at: datetime | None, test_ended_at: datetime | None) -> None:
        if test_started_at is None or test_ended_at is None:
            optimization_train_box.visible = False
            optimization_train_start_span.visible = False
            return
        train_started_at = _coerce_datetime(optimization_period_slider.value[0])
        train_ended_at = _coerce_datetime(optimization_period_slider.value[1])
        overlap_started_at = max(test_started_at, train_started_at)
        overlap_ended_at = min(test_ended_at, train_ended_at)
        if overlap_started_at >= overlap_ended_at:
            optimization_train_box.visible = False
            optimization_train_start_span.visible = False
            return
        left = _nearest_bar_x(overlap_started_at)
        right = _nearest_bar_x(overlap_ended_at)
        if left is None or right is None or right <= left:
            optimization_train_box.visible = False
            optimization_train_start_span.visible = False
            return
        optimization_train_box.left = left
        optimization_train_box.right = right
        optimization_train_box.visible = True
        if test_started_at <= train_started_at <= test_ended_at:
            start_location = _nearest_bar_x(train_started_at)
            if start_location is None:
                optimization_train_start_span.visible = False
            else:
                optimization_train_start_span.location = start_location
                optimization_train_start_span.visible = True
        else:
            optimization_train_start_span.visible = False

    def clear_zscore_diagnostics_outputs() -> None:
        zscore_metrics_source.data = empty_zscore_metric_source()
        zscore_hist_source.data = empty_zscore_hist_source()
        zscore_metrics_div.text = "<p>Run a test to inspect z-score distribution, percentiles and threshold hit-rates.</p>"
        zscore_hist_plot.x_range.start = -1.0
        zscore_hist_plot.x_range.end = 1.0
        zscore_hist_plot.y_range.start = 0.0
        zscore_hist_plot.y_range.end = 1.0
        zscore_hist_plot.xaxis.ticker = FixedTicker(ticks=[-1.0, 0.0, 1.0])
        for span in [
            zscore_hist_entry_pos_span,
            zscore_hist_entry_neg_span,
            zscore_hist_exit_pos_span,
            zscore_hist_exit_neg_span,
            zscore_hist_stop_pos_span,
            zscore_hist_stop_neg_span,
        ]:
            span.visible = False

    def apply_zscore_diagnostics(frame: pl.DataFrame, params: DistanceParameters) -> None:
        payload = build_zscore_diagnostics(
            frame,
            entry_z=float(params.entry_z),
            exit_z=float(params.exit_z),
            stop_z=None if params.stop_z is None else float(params.stop_z),
        )
        apply_zscore_diagnostics_payload(payload)

    def apply_zscore_diagnostics_payload(payload) -> None:
        zscore_metrics_source.data = payload.metrics_source
        zscore_hist_source.data = payload.histogram_source
        zscore_metrics_div.text = payload.summary_html
        zscore_hist_plot.x_range.start = float(payload.histogram_x_start)
        zscore_hist_plot.x_range.end = float(payload.histogram_x_end)
        zscore_hist_plot.y_range.start = 0.0
        zscore_hist_plot.y_range.end = float(payload.histogram_y_end)
        tick_start = int(ceil(float(payload.histogram_x_start)))
        tick_end = int(floor(float(payload.histogram_x_end)))
        if tick_end < tick_start:
            tick_values = [0.0]
        else:
            tick_values = [float(value) for value in range(tick_start, tick_end + 1)]
        zscore_hist_plot.xaxis.ticker = FixedTicker(ticks=tick_values)

        zscore_hist_entry_pos_span.location = float(payload.entry_threshold)
        zscore_hist_entry_neg_span.location = -float(payload.entry_threshold)
        zscore_hist_entry_pos_span.visible = True
        zscore_hist_entry_neg_span.visible = True

        if payload.exit_threshold is None:
            zscore_hist_exit_pos_span.visible = False
            zscore_hist_exit_neg_span.visible = False
        else:
            zscore_hist_exit_pos_span.location = float(payload.exit_threshold)
            zscore_hist_exit_neg_span.location = -float(payload.exit_threshold)
            zscore_hist_exit_pos_span.visible = True
            zscore_hist_exit_neg_span.visible = True

        if payload.stop_threshold is None:
            zscore_hist_stop_pos_span.visible = False
            zscore_hist_stop_neg_span.visible = False
        else:
            zscore_hist_stop_pos_span.location = float(payload.stop_threshold)
            zscore_hist_stop_neg_span.location = -float(payload.stop_threshold)
            zscore_hist_stop_pos_span.visible = True
            zscore_hist_stop_neg_span.visible = True

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
        clear_zscore_diagnostics_outputs()
        set_shared_x_range(0, 1)
        refresh_plot_ranges()
        update_gapless_x_axis()
        sync_optimization_cutoff_marker(test_started_at=None, test_ended_at=None)
        sync_optimization_train_overlay(test_started_at=None, test_ended_at=None)
        set_equity_summary_overlay(None)
        sync_equity_legend(None, None)
        summary_div.text = message
        return False

    def current_backtest_request(
        *,
        period_override: tuple[datetime, datetime] | None = None,
    ) -> tuple[dict[str, object] | None, str | None]:
        if algorithm_select.value != "distance":
            return None, "<p>Only the first Distance tester slice is wired right now.</p>"

        pair = current_pair()
        if pair is None:
            return None, "<p>Refresh instruments and choose two different valid instruments first.</p>"

        if period_override is None:
            started_at = _coerce_datetime(period_slider.value[0])
            ended_at = _coerce_datetime(period_slider.value[1])
        else:
            started_at, ended_at = period_override

        return (
            {
                "pair": pair,
                "timeframe": Timeframe(timeframe_select.value),
                "started_at": started_at,
                "ended_at": ended_at,
                "defaults": build_defaults(),
                "params": build_distance_params(),
            },
            None,
        )

    def run_tester_job(request: dict[str, object]) -> dict[str, object]:
        pair = request["pair"]
        timeframe = request["timeframe"]
        started_at = request["started_at"]
        ended_at = request["ended_at"]
        activation_started_at = request.get("activation_started_at")
        defaults = request["defaults"]
        params = request["params"]
        assert isinstance(pair, PairSelection)
        assert isinstance(timeframe, Timeframe)
        assert isinstance(started_at, datetime)
        assert isinstance(ended_at, datetime)
        assert isinstance(defaults, StrategyDefaults)
        assert isinstance(params, DistanceParameters)
        if activation_started_at is not None:
            assert isinstance(activation_started_at, datetime)

        strategy_started_at = started_at
        padded_display = False
        if (
            isinstance(activation_started_at, datetime)
            and started_at < activation_started_at < ended_at
        ):
            strategy_started_at = activation_started_at
            padded_display = True

        result = run_distance_backtest(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=strategy_started_at,
            ended_at=ended_at,
            defaults=defaults,
            params=params,
        )
        if result.frame.is_empty():
            return {
                "status": "empty",
                "symbol_1": pair.symbol_1,
                "symbol_2": pair.symbol_2,
                "timeframe": timeframe.value,
                "started_at": started_at,
                "ended_at": ended_at,
            }

        if padded_display:
            display_frame = load_pair_frame(
                broker=broker,
                pair=pair,
                timeframe=timeframe,
                started_at=started_at,
                ended_at=ended_at,
            )
            sources = result_to_padded_sources(
                result,
                display_frame,
                initial_capital=float(defaults.initial_capital),
            )
        else:
            sources = result_to_sources(result)

        return {
            "status": "ok",
            "sources": sources,
            "summary": result.summary,
            "zscore_payload": build_zscore_diagnostics(
                result.frame,
                entry_z=float(params.entry_z),
                exit_z=float(params.exit_z),
                stop_z=None if params.stop_z is None else float(params.stop_z),
            ),
            "timeframe": timeframe.value,
            "started_at": started_at,
            "ended_at": ended_at,
            "strategy_started_at": strategy_started_at if padded_display else None,
        }

    def apply_backtest_payload(payload: dict[str, object]) -> bool:
        status = str(payload.get("status", "error"))
        if status != "ok":
            symbol_1 = str(payload.get("symbol_1", ""))
            symbol_2 = str(payload.get("symbol_2", ""))
            timeframe_value = str(payload.get("timeframe", timeframe_select.value))
            started_at = payload.get("started_at")
            ended_at = payload.get("ended_at")
            if isinstance(started_at, datetime) and isinstance(ended_at, datetime):
                return clear_backtest_outputs(
                    f"<p>No aligned parquet data found for <b>{symbol_1}</b> / <b>{symbol_2}</b> "
                    f"on <b>{timeframe_value}</b> between "
                    f"<b>{started_at:%Y-%m-%d %H:%M}</b> and <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
                )
            return clear_backtest_outputs("<p>Test run produced no aligned data.</p>")

        sources = payload.get("sources")
        summary = payload.get("summary")
        zscore_payload = payload.get("zscore_payload")
        started_at = payload.get("started_at")
        ended_at = payload.get("ended_at")
        strategy_started_at = payload.get("strategy_started_at")
        timeframe_value = str(payload.get("timeframe", timeframe_select.value))
        if not isinstance(sources, dict) or not isinstance(summary, dict):
            return clear_backtest_outputs("<p>Test run returned an invalid payload.</p>")
        if not isinstance(started_at, datetime) or not isinstance(ended_at, datetime):
            return clear_backtest_outputs("<p>Test run returned an invalid tester period.</p>")

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
        if zscore_payload is None:
            clear_zscore_diagnostics_outputs()
        else:
            apply_zscore_diagnostics_payload(zscore_payload)

        x_values = sources["price_1"].get("x", [])
        set_shared_x_range(0.0, float(max(1, len(x_values) - 1)))
        refresh_plot_ranges()
        update_gapless_x_axis()
        sync_optimization_cutoff_marker(test_started_at=started_at, test_ended_at=ended_at)
        sync_optimization_train_overlay(test_started_at=started_at, test_ended_at=ended_at)

        sync_equity_legend(str(summary["symbol_1"]), str(summary["symbol_2"]))
        set_equity_summary_overlay(summary)
        completed_at = datetime.now(UTC)
        strategy_window_note = ""
        if isinstance(strategy_started_at, datetime) and strategy_started_at > started_at:
            strategy_window_note = (
                f" Strategy logic starts at <b>{strategy_started_at:%Y-%m-%d %H:%M} UTC</b> "
                f"to match the optimizer window while keeping the full tester axis visible."
            )
        summary_div.text = (
            f"<p>Distance test completed at <b>{completed_at:%Y-%m-%d %H:%M:%S} UTC</b> for "
            f"<b>{summary['symbol_1']}</b> / <b>{summary['symbol_2']}</b> on <b>{timeframe_value}</b>. "
            f"Test period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>. "
            f"Trades: <b>{int(summary.get('trades', 0) or 0)}</b>, Net: <b>{float(summary.get('net_pnl', 0.0) or 0.0):.2f}</b>."
            f"{strategy_window_note}</p>"
        )
        return True

    def submit_tester_request(
        request: dict[str, object],
        *,
        pending_message: str,
    ) -> tuple[bool, str | None]:
        nonlocal tester_future, tester_poll_callback
        if tester_future is not None and not tester_future.done():
            return False, "<p>Distance test is already running. Wait for the current run to finish.</p>"
        if tester_future is not None and tester_future.done():
            poll_tester_future()
        mark_test_running()
        summary_div.text = pending_message
        tester_future = tester_executor.submit(run_tester_job, request)
        if tester_poll_callback is None:
            tester_poll_callback = doc.add_periodic_callback(poll_tester_future, 100)
        return True, None

    def poll_tester_future() -> None:
        nonlocal tester_future
        if tester_future is None:
            return
        if not tester_future.done():
            return

        future = tester_future
        tester_future = None
        clear_tester_poll_callback()
        reset_run_button()

        try:
            payload = future.result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            clear_backtest_outputs(f"<p>Test run failed: {exc}</p>")
            return
        apply_backtest_payload(payload)

    def set_shared_x_range(start: float, end: float) -> None:
        nonlocal suppress_shared_range_change
        suppress_shared_range_change = True
        try:
            state.shared_x_range.start = start
            state.shared_x_range.end = end
        finally:
            suppress_shared_range_change = False

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
        equity_visible = equity_body.visible
        for _key, plot, _source, _columns, body, toggle in plot_bindings:
            plot.visible = body.visible
            toggle.button_type = "primary" if body.visible else "default"
            if body.visible:
                plot.height = compute_relative_plot_height(_key, plot_height, equity_visible=equity_visible)
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

    def set_tester_context(
        source_kind: str,
        *,
        oos_started_at: datetime | None = None,
        context_started_at: datetime | None = None,
        context_ended_at: datetime | None = None,
    ) -> None:
        nonlocal current_tester_context
        current_tester_context = {
            "source_kind": str(source_kind or "tester_manual"),
            "oos_started_at": oos_started_at,
            "context_started_at": context_started_at,
            "context_ended_at": context_ended_at,
        }

    def current_pair() -> PairSelection | None:
        if symbol_1_select.value == INSTRUMENT_PLACEHOLDER or symbol_2_select.value == INSTRUMENT_PLACEHOLDER:
            return None
        if symbol_1_select.value == symbol_2_select.value:
            return None
        return PairSelection(symbol_1=symbol_1_select.value, symbol_2=symbol_2_select.value)

    def _context_datetime(value: object) -> datetime | None:
        return value if isinstance(value, datetime) else None

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
        catalog = filter_catalog_by_group(catalog, selected_group)
        options = catalog.get_column('symbol').sort().to_list() if not catalog.is_empty() else []
        return options or [INSTRUMENT_PLACEHOLDER]

    def available_catalog_group_options() -> list[str]:
        try:
            catalog = load_instrument_catalog_frame(broker)
        except Exception:
            return list(GROUP_OPTIONS)
        return list_mt5_group_options(catalog)

    def sync_catalog_group_select_options() -> None:
        options = available_catalog_group_options()
        for selector in (group_select, scan_universe_select, download_group_select):
            current_value = selector.value if selector.value in options else options[0]
            selector.options = options
            if selector.value != current_value:
                selector.value = current_value

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

    def sync_leg_2_filter_ui() -> None:
        is_co_movers = leg_2_filter_select.value == "co_movers"
        is_cointegrated = leg_2_filter_select.value == "cointegrated_only"
        co_mover_group_select.visible = is_co_movers
        leg_2_cointegration_kind_select.visible = is_cointegrated

    def _sync_co_mover_group_options(current_symbol_1: str, allowed_options: list[str]) -> list[str]:
        if current_symbol_1 == INSTRUMENT_PLACEHOLDER:
            co_mover_group_select.options = [NO_CO_MOVER_GROUP_LABEL]
            co_mover_group_select.value = NO_CO_MOVER_GROUP_LABEL
            return []
        group_labels = co_mover_group_labels_for_symbol(current_symbol_1, available_symbols=allowed_options)
        if not group_labels:
            co_mover_group_select.options = [NO_CO_MOVER_GROUP_LABEL]
            co_mover_group_select.value = NO_CO_MOVER_GROUP_LABEL
            return []
        co_mover_group_select.options = group_labels
        if co_mover_group_select.value not in group_labels:
            co_mover_group_select.value = group_labels[0]
        return group_labels

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
        sync_leg_2_filter_ui()
        options = list(base_options or instrument_options_for_group(group_select.value))
        if not options:
            options = [INSTRUMENT_PLACEHOLDER]

        current_symbol_1 = symbol_1_select.value if symbol_1_select.value in options else _preferred_symbol_1(options, symbol_1_select.value)
        symbol_1_select.options = options
        if symbol_1_select.value != current_symbol_1:
            symbol_1_select.value = current_symbol_1

        if leg_2_filter_select.value == 'all_symbols':
            symbol_2_select.options = options
            symbol_2_select.value = _preferred_symbol_2(options, symbol_2_select.value, current_symbol_1)
            return

        if leg_2_filter_select.value == 'co_movers':
            group_labels = _sync_co_mover_group_options(current_symbol_1, options)
            if current_symbol_1 == INSTRUMENT_PLACEHOLDER:
                symbol_2_select.options = [INSTRUMENT_PLACEHOLDER]
                symbol_2_select.value = INSTRUMENT_PLACEHOLDER
                return
            if not group_labels:
                symbol_2_select.options = [INSTRUMENT_PLACEHOLDER]
                symbol_2_select.value = INSTRUMENT_PLACEHOLDER
                if show_message:
                    summary_div.text = (
                        f"<p>No local co-mover groups are defined for <b>{current_symbol_1}</b> "
                        f"inside the current tester universe <b>{group_select.value}</b>.</p>"
                    )
                return
            partner_options = co_mover_symbols_for_symbol(
                current_symbol_1,
                available_symbols=options,
                group_label=co_mover_group_select.value,
            )
            if not partner_options:
                symbol_2_select.options = [INSTRUMENT_PLACEHOLDER]
                symbol_2_select.value = INSTRUMENT_PLACEHOLDER
                if show_message:
                    summary_div.text = (
                        f"<p>Co-mover group <b>{co_mover_group_select.value}</b> has no eligible Symbol 2 candidates "
                        f"for <b>{current_symbol_1}</b> inside the current tester universe <b>{group_select.value}</b>.</p>"
                    )
                return
            symbol_2_select.options = partner_options
            symbol_2_select.value = _preferred_symbol_2(partner_options, symbol_2_select.value, current_symbol_1)
            if show_message:
                summary_div.text = (
                    f"<p>Leg 2 filter loaded <b>{len(partner_options)}</b> local co-movers for <b>{current_symbol_1}</b> "
                    f"from <b>{co_mover_group_select.value}</b>.</p>"
                )
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
            "bollinger_k": float(_read_spinner_value(bollinger_input, 2.0)),
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
        for widget in [opt_lookback_step, opt_entry_step, opt_exit_step]:
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

    def reset_run_button() -> None:
        run_button.label = "Run Test"
        run_button.button_type = "success"

    def mark_test_running() -> None:
        run_button.label = "Running Test..."
        run_button.button_type = "warning"

    def clear_tester_poll_callback() -> None:
        nonlocal tester_poll_callback
        if tester_poll_callback is None:
            return
        try:
            doc.remove_periodic_callback(tester_poll_callback)
        except ValueError:
            pass
        tester_poll_callback = None

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

    def _serialize_optimization_signature_value(value: object) -> object:
        if isinstance(value, datetime):
            return serialize_time(value if value.tzinfo else value.replace(tzinfo=UTC))
        if isinstance(value, dict):
            return {str(key): _serialize_optimization_signature_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [_serialize_optimization_signature_value(item) for item in value]
        return value

    def optimization_signature(
        *,
        pair: PairSelection,
        timeframe: Timeframe,
        started_at: datetime,
        ended_at: datetime,
        mode_value: str,
        objective_metric: str,
        search_space: dict[str, object],
        defaults: StrategyDefaults,
        fee_mode: str,
        config: dict[str, object] | None,
    ) -> dict[str, object]:
        return {
            "symbol_1": pair.symbol_1,
            "symbol_2": pair.symbol_2,
            "timeframe": timeframe.value,
            "started_at": serialize_time(started_at),
            "ended_at": serialize_time(ended_at),
            "mode": str(mode_value or ""),
            "objective_metric": str(objective_metric or ""),
            "search_space": _serialize_optimization_signature_value(search_space),
            "defaults": _serialize_optimization_signature_value(
                {
                    "initial_capital": float(defaults.initial_capital),
                    "leverage": float(defaults.leverage),
                    "margin_budget_per_leg": float(defaults.margin_budget_per_leg),
                    "slippage_points": float(defaults.slippage_points),
                }
            ),
            "fee_mode": str(fee_mode or ""),
            "config": _serialize_optimization_signature_value(config or {}),
        }

    def optimization_signature_matches(left: dict[str, object] | None, right: dict[str, object] | None) -> bool:
        if left is None or right is None:
            return False
        return json.dumps(left, sort_keys=True, default=str) == json.dumps(right, sort_keys=True, default=str)

    def optimization_outputs_present() -> bool:
        return bool(state.optimization_source.data.get("trial_id", []))

    def copy_optimization_trial_to_tester(
        index: int,
        *,
        period_override: tuple[datetime, datetime] | None = None,
    ) -> tuple[bool, object | None, tuple[datetime, datetime]]:
        nonlocal suppress_optimization_config_change
        data = state.optimization_source.data
        trial_ids = list(data.get("trial_id", []))
        optimization_started_at = _coerce_datetime(optimization_period_slider.value[0])
        optimization_ended_at = _coerce_datetime(optimization_period_slider.value[1])
        period = period_override or (
            _coerce_datetime(period_slider.value[0]),
            _coerce_datetime(period_slider.value[1]),
        )
        if index < 0 or index >= len(trial_ids):
            return False, None, period

        doc.hold("combine")
        suppress_optimization_config_change = True
        try:
            with file_state_controller.suspend():
                set_tester_context(
                    "optimization_row",
                    oos_started_at=optimization_ended_at,
                    context_started_at=optimization_started_at,
                    context_ended_at=optimization_ended_at,
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
        finally:
            suppress_optimization_config_change = False
            doc.unhold()
        file_state_controller.persist()
        return True, trial_ids[index], period

    def apply_optimization_trial(
        index: int,
        *,
        period_override: tuple[datetime, datetime] | None = None,
    ) -> tuple[bool, object | None, tuple[datetime, datetime]]:
        copied, trial_id, period = copy_optimization_trial_to_tester(index, period_override=period_override)
        if not copied:
            return False, trial_id, period
        ran = apply_backtest_result(period_override=period)
        return ran, trial_id, period

    def render_optimization_trial_in_tester(
        index: int,
        *,
        selection_label: str,
    ) -> bool:
        copied, trial_id, tester_period = copy_optimization_trial_to_tester(index)
        if not copied:
            return False
        request, error_message = current_backtest_request(period_override=tester_period)
        if request is None:
            clear_backtest_outputs(error_message or "<p>Test request is invalid.</p>")
            return False
        pair = request["pair"]
        timeframe = request["timeframe"]
        started_at = request["started_at"]
        ended_at = request["ended_at"]
        assert isinstance(pair, PairSelection)
        assert isinstance(timeframe, Timeframe)
        assert isinstance(started_at, datetime)
        assert isinstance(ended_at, datetime)
        # UX contract:
        # clicking an optimizer row copies only strategy parameters into the
        # tester; the replay itself must run on the full current tester period.
        submitted, busy_message = submit_tester_request(
            request,
            pending_message=(
                f"<p>Running Distance test for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{timeframe.value}</b>. "
                f"Test period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
            ),
        )
        if not submitted:
            optimization_status_div.text = f"{optimization_summary_html}{busy_message or ''}"
            return False
        optimization_status_div.text = (
            f"{optimization_summary_html}"
            f"<p>{selection_label} <b>{trial_id}</b> copied into tester inputs and is rendering on tester period "
            f"<b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. <b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
        )
        return True

    def current_optimization_signature() -> dict[str, object] | None:
        pair = current_pair()
        if pair is None:
            return None
        started_at = _coerce_datetime(optimization_period_slider.value[0])
        ended_at = _coerce_datetime(optimization_period_slider.value[1])
        if ended_at <= started_at:
            return None
        return optimization_signature(
            pair=pair,
            timeframe=Timeframe(timeframe_select.value),
            started_at=started_at,
            ended_at=ended_at,
            mode_value=str(optimization_mode_select.value or ""),
            objective_metric=str(optimization_objective_select.value or "net_profit"),
            search_space=optimization_search_space(),
            defaults=build_defaults(),
            fee_mode=str(bybit_fee_mode_select.value or ""),
            config=genetic_optimizer_config() if optimization_mode_select.value == OptimizationMode.GENETIC.value else None,
        )

    def optimization_signature_changes(
        previous: dict[str, object] | None,
        current: dict[str, object] | None,
    ) -> list[str]:
        if previous is None or current is None:
            return []
        labels = [
            ("symbol_1", "symbol 1"),
            ("symbol_2", "symbol 2"),
            ("timeframe", "timeframe"),
            ("started_at", "period start"),
            ("ended_at", "period end"),
            ("mode", "mode"),
            ("objective_metric", "objective"),
            ("search_space", "optimizer ranges"),
            ("defaults", "capital / leverage / budget / slippage"),
            ("fee_mode", "fee mode"),
            ("config", "genetic settings"),
        ]
        changed: list[str] = []
        for key, label in labels:
            if not optimization_signature_matches(
                {"value": previous.get(key)},
                {"value": current.get(key)},
            ):
                changed.append(label)
        return changed

    def on_optimization_config_change(_attr: str, _old: object, _new: object) -> None:
        if suppress_optimization_config_change:
            return
        tester_started_at = _coerce_datetime(period_slider.value[0])
        tester_ended_at = _coerce_datetime(period_slider.value[1])
        sync_optimization_cutoff_marker(test_started_at=tester_started_at, test_ended_at=tester_ended_at)
        sync_optimization_train_overlay(test_started_at=tester_started_at, test_ended_at=tester_ended_at)
        current_signature = current_optimization_signature()
        if not optimization_outputs_present() or displayed_optimization_signature is None or current_signature is None:
            return
        if optimization_signature_matches(displayed_optimization_signature, current_signature):
            return
        shown_started = str(displayed_optimization_signature.get("started_at") or "").replace("T", " ").replace("Z", " UTC")
        shown_ended = str(displayed_optimization_signature.get("ended_at") or "").replace("T", " ").replace("Z", " UTC")
        current_started = str(current_signature.get("started_at") or "").replace("T", " ").replace("Z", " UTC")
        current_ended = str(current_signature.get("ended_at") or "").replace("T", " ").replace("Z", " UTC")
        changed_parts = optimization_signature_changes(displayed_optimization_signature, current_signature)
        changed_label = ", ".join(changed_parts) if changed_parts else "optimizer settings"
        optimization_status_div.text = (
            f"<p>Showing the last optimization table for <b>{shown_started}</b> .. <b>{shown_ended}</b>, "
            f"but the current optimizer configuration changed: <b>{changed_label}</b>. "
            f"Current target period: <b>{current_started}</b> .. <b>{current_ended}</b>. "
            f"Run Optimization again to refresh the table for the current settings.</p>"
        )

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
            wfa_train_window_box.visible = False
            wfa_train_start_span.visible = False
            wfa_test_window_box.visible = False
            return
        index = indices[0]
        data = wfa_table_source.data
        train_starts = data.get("train_started_at", [])
        train_ends = data.get("train_ended_at", [])
        starts = data.get("test_started_at", [])
        ends = data.get("test_ended_at", [])
        if index >= len(train_starts) or index >= len(train_ends) or index >= len(starts) or index >= len(ends):
            wfa_train_window_box.visible = False
            wfa_train_start_span.visible = False
            wfa_test_window_box.visible = False
            return
        train_started_at = train_starts[index]
        train_ended_at = train_ends[index]
        started_at = starts[index]
        ended_at = ends[index]
        if train_started_at is None or train_ended_at is None or started_at is None or ended_at is None:
            wfa_train_window_box.visible = False
            wfa_train_start_span.visible = False
            wfa_test_window_box.visible = False
            return
        wfa_train_window_box.left = train_started_at
        wfa_train_window_box.right = train_ended_at
        wfa_train_window_box.visible = True
        wfa_train_start_span.location = train_started_at
        wfa_train_start_span.visible = True
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

    def active_portfolio_item_ids(items: list | None = None) -> set[str]:
        current_items = items if items is not None else list(portfolio_items_by_id.values())
        valid_ids = {item.item_id for item in current_items}
        return {item_id for item_id in valid_ids if item_id not in portfolio_excluded_item_ids}

    def portfolio_allocation_capitals_by_id() -> dict[str, float]:
        return {
            item_id: max(float(row.allocation_capital or 0.0), 0.0)
            for item_id, row in portfolio_run_rows_by_id.items()
        }

    def portfolio_display_allocation_capitals(
        items: list,
        *,
        active_item_ids: set[str],
        active_allocation_capitals_by_id: dict[str, float],
        fallback_allocation_capital: float,
    ) -> dict[str, float]:
        return materialize_portfolio_backtest_allocations(
            [item.item_id for item in items],
            allocation_capitals_by_id={
                item_id: active_allocation_capitals_by_id[item_id]
                for item_id in active_item_ids
                if item_id in active_allocation_capitals_by_id
            },
            fallback_allocation_capital=max(float(fallback_allocation_capital), 1e-9),
        )

    def selected_portfolio_item_id() -> str | None:
        indices = list(portfolio_table_source.selected.indices)
        if not indices:
            return None
        item_ids = portfolio_table_source.data.get("item_id", [])
        index = indices[0]
        if index < 0 or index >= len(item_ids):
            return None
        return str(item_ids[index])

    def build_portfolio_curve_source_data(curve: PortfolioCurve) -> dict[str, list[object]]:
        return {
            "time": [_datetime_to_bokeh_millis(moment) for moment in curve.times],
            "equity": curve.equities,
        }

    def portfolio_curve_source_data(curve: PortfolioCurve) -> dict[str, list[object]]:
        cached = portfolio_curve_sources_by_id.get(curve.item_id)
        if cached is not None:
            return cached
        source_data = build_portfolio_curve_source_data(curve)
        portfolio_curve_sources_by_id[curve.item_id] = source_data
        return source_data

    def set_portfolio_x_range(start: object, end: object) -> None:
        nonlocal suppress_portfolio_range_change
        suppress_portfolio_range_change = True
        try:
            portfolio_equity_plot.x_range.start = start
            portfolio_equity_plot.x_range.end = end
        finally:
            suppress_portfolio_range_change = False

    def render_portfolio_equity_curve(
        curve: PortfolioCurve,
        *,
        title: str,
        oos_started_at: datetime | None,
    ) -> None:
        source_data = portfolio_curve_source_data(curve)
        doc.hold("combine")
        try:
            portfolio_equity_plot.title.text = title
            portfolio_equity_source.data = source_data
            refresh_portfolio_equity_ranges(source_data=source_data)
            sync_portfolio_oos_cutoff_marker(oos_started_at)
        finally:
            doc.unhold()

    def render_portfolio_combined_equity() -> None:
        active_ids = active_portfolio_item_ids()
        combined = combine_portfolio_equity_curves(
            list(portfolio_curves_by_id.values()),
            included_item_ids=active_ids,
            allocation_capitals_by_id=portfolio_allocation_capitals_by_id(),
        )
        if combined.is_empty():
            portfolio_equity_plot.title.text = portfolio_equity_base_title
            clear_portfolio_equity_outputs()
            return
        active_items = [item for item_id, item in portfolio_items_by_id.items() if item_id in active_ids]
        source_data = {
            "time": [_datetime_to_bokeh_millis(moment) for moment in combined.get_column("time").to_list()],
            "equity": [float(value) for value in combined.get_column("equity").to_list()],
        }
        latest_oos_started_at = latest_portfolio_oos_started_at(active_items)
        doc.hold("combine")
        try:
            portfolio_equity_plot.title.text = portfolio_equity_base_title
            portfolio_equity_source.data = source_data
            refresh_portfolio_equity_ranges(source_data=source_data)
            sync_portfolio_oos_cutoff_marker(latest_oos_started_at)
        finally:
            doc.unhold()

    def sync_portfolio_equity_view() -> None:
        selected_item_id = portfolio_preview_item_id or selected_portfolio_item_id()
        if selected_item_id:
            curve = portfolio_curves_by_id.get(selected_item_id)
            item = portfolio_items_by_id.get(selected_item_id)
            if curve is not None and curve.times and curve.equities:
                title = f"{curve.symbol_1} / {curve.symbol_2} [{curve.timeframe}]"
                render_portfolio_equity_curve(
                    curve,
                    title=title,
                    oos_started_at=None if item is None else item.oos_started_at,
                )
                return
        render_portfolio_combined_equity()

    def clear_portfolio_equity_outputs() -> None:
        portfolio_equity_plot.title.text = portfolio_equity_base_title
        portfolio_equity_source.data = {"time": [], "equity": []}
        portfolio_oos_cutoff_span.visible = False
        set_portfolio_x_range(_datetime_to_bokeh_millis(history_start), _datetime_to_bokeh_millis(now_utc))
        portfolio_equity_plot.y_range.start = 0.0
        portfolio_equity_plot.y_range.end = 1.0

    def clear_portfolio_analysis_outputs() -> None:
        portfolio_weight_source.data = empty_portfolio_weight_data()
        portfolio_correlation_source.data = empty_portfolio_correlation_data()
        portfolio_analysis_div.text = (
            "<p>Use <b>Analyze Portfolio</b> to calculate pairwise equity/return correlations and get "
            "a diversification-weight suggestion. The second allocation mode, <b>diversified_risk</b>, "
            "uses inverse return volatility penalized by mean absolute return correlation.</p>"
        )

    def refresh_portfolio_equity_ranges(
        *,
        reset_x_range: bool = True,
        source_data: dict[str, list[object]] | None = None,
    ) -> None:
        data = source_data if source_data is not None else portfolio_equity_source.data
        times = list(data.get("time", []))
        equities = [float(value) for value in data.get("equity", [])]
        if not times or not equities:
            clear_portfolio_equity_outputs()
            return
        full_x_start = float(times[0]) if times else _datetime_to_bokeh_millis(history_start)
        full_x_end = float(times[-1]) if times else _datetime_to_bokeh_millis(now_utc)
        if full_x_end < full_x_start:
            full_x_start = min(float(value) for value in times)
            full_x_end = max(float(value) for value in times)
        if full_x_start == full_x_end:
            full_x_end = full_x_start + 1.0
        if reset_x_range:
            set_portfolio_x_range(full_x_start, full_x_end)
            visible_x_start = full_x_start
            visible_x_end = full_x_end
            y_low = min(equities)
            y_high = max(equities)
        else:
            visible_x_start = portfolio_equity_plot.x_range.start
            visible_x_end = portfolio_equity_plot.x_range.end
            if visible_x_start is None or visible_x_end is None:
                visible_x_start = full_x_start
                visible_x_end = full_x_end
            y_low, y_high = compute_series_bounds(
                times,
                [equities],
                visible_x_start,
                visible_x_end,
                pad_ratio=0.08,
            )
        if reset_x_range:
            if abs(y_high - y_low) <= 1e-9:
                y_high = y_low + 1.0
            padding = max(1.0, (y_high - y_low) * 0.08)
            y_low -= padding
            y_high += padding
        if abs(y_high - y_low) <= 1e-9:
            y_high = y_low + 1.0
        portfolio_equity_plot.y_range.start = y_low
        portfolio_equity_plot.y_range.end = y_high

    def sync_portfolio_oos_cutoff_marker(oos_started_at: datetime | None) -> None:
        if oos_started_at is None:
            portfolio_oos_cutoff_span.visible = False
            return
        portfolio_oos_cutoff_span.location = oos_started_at
        portfolio_oos_cutoff_span.visible = True

    def run_portfolio_backtests(
        items: list,
        *,
        started_at: datetime,
        ended_at: datetime,
        allocation_capitals_by_id: dict[str, float],
        included_item_ids: set[str] | None = None,
    ) -> tuple[list[PortfolioCurve], dict[str, PortfolioRunRow], int, int]:
        current_fee_mode = str(bybit_fee_mode_select.value or settings.bybit_tradfi_fee_mode)
        curves: list[PortfolioCurve] = []
        next_rows: dict[str, PortfolioRunRow] = {}
        no_data_count = 0
        included_lookup = included_item_ids if included_item_ids is not None else {item.item_id for item in items}
        excluded_count = sum(1 for item in items if item.item_id not in included_lookup)
        try:
            for item in items:
                allocation_capital = max(float(allocation_capitals_by_id.get(item.item_id, 0.0) or 0.0), 0.0)
                if allocation_capital <= 0.0:
                    allocation_capital = 0.0
                os.environ["MT_SERVICE_BYBIT_TRADFI_FEE_MODE"] = str(item.fee_mode or current_fee_mode)
                get_settings.cache_clear()
                defaults = scale_defaults_for_portfolio_item(item, float(item.initial_capital))
                strategy_started_at = portfolio_strategy_started_at(
                    item,
                    started_at=started_at,
                    ended_at=ended_at,
                )
                result = run_distance_backtest(
                    broker=broker,
                    pair=PairSelection(symbol_1=item.symbol_1, symbol_2=item.symbol_2),
                    timeframe=item.timeframe,
                    started_at=strategy_started_at,
                    ended_at=ended_at,
                    defaults=defaults,
                    params=item.params(),
                )
                if result.frame.is_empty():
                    no_data_count += 1
                    curves.append(
                        PortfolioCurve(
                            item_id=item.item_id,
                            symbol_1=item.symbol_1,
                            symbol_2=item.symbol_2,
                            timeframe=item.timeframe.value,
                            initial_capital=defaults.initial_capital,
                            times=[],
                            equities=[],
                        )
                    )
                    next_rows[item.item_id] = PortfolioRunRow(
                        item_id=item.item_id,
                        symbol_1=item.symbol_1,
                        symbol_2=item.symbol_2,
                        timeframe=item.timeframe.value,
                        allocation_capital=allocation_capital,
                        net_profit=None,
                        ending_equity=None,
                        max_drawdown=None,
                        trades=None,
                        status="no_data",
                    )
                    continue
                frame = result.frame
                curve_times = list(frame.get_column("time").to_list())
                curve_equities = [float(value) for value in frame.get_column("equity_total").to_list()]
                curve_times, curve_equities = prepend_flat_equity_prefix(
                    curve_times,
                    curve_equities,
                    period_started_at=started_at,
                    strategy_started_at=strategy_started_at,
                    initial_capital=defaults.initial_capital,
                )
                curves.append(
                    PortfolioCurve(
                        item_id=item.item_id,
                        symbol_1=item.symbol_1,
                        symbol_2=item.symbol_2,
                        timeframe=item.timeframe.value,
                        initial_capital=defaults.initial_capital,
                        times=curve_times,
                        equities=curve_equities,
                    )
                )
                summary = result.summary
                next_rows[item.item_id] = PortfolioRunRow(
                    item_id=item.item_id,
                    symbol_1=item.symbol_1,
                    symbol_2=item.symbol_2,
                    timeframe=item.timeframe.value,
                    allocation_capital=allocation_capital,
                    net_profit=float(summary.get("net_pnl", 0.0) or 0.0),
                    ending_equity=float(summary.get("ending_equity", defaults.initial_capital) or defaults.initial_capital),
                    max_drawdown=float(summary.get("max_drawdown", 0.0) or 0.0),
                    trades=int(summary.get("trades", 0) or 0),
                    status="ok",
                )
        finally:
            os.environ["MT_SERVICE_BYBIT_TRADFI_FEE_MODE"] = current_fee_mode
            get_settings.cache_clear()
        return curves, next_rows, no_data_count, excluded_count

    def refresh_portfolio_analysis(
        curves: list[PortfolioCurve],
        *,
        started_at: datetime,
        ended_at: datetime,
    ) -> list[PortfolioAllocationSuggestionRow]:
        valid_curves = [curve for curve in curves if curve.times and curve.equities]
        pairwise_rows, suggestion_rows = analyze_portfolio_curves(valid_curves)
        portfolio_weight_source.data = portfolio_weight_rows_to_source(suggestion_rows)
        portfolio_correlation_source.data = portfolio_correlation_rows_to_source(pairwise_rows)
        if not suggestion_rows:
            portfolio_analysis_div.text = (
                f"<p>Portfolio analysis found no valid equity curves on <b>{started_at:%Y-%m-%d %H:%M}</b> .. "
                f"<b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )
            return []
        portfolio_analysis_div.text = (
            f"<p>Portfolio analysis finished on <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>. "
            f"`equity_corr` is Pearson correlation of normalized equity curves. `return_corr` is Pearson correlation of aligned equity returns. "
            f"The recommended second allocation mode is <b>diversified_risk</b>: weight = "
            f"<b>1 / (return volatility * (1 + mean |return correlation|))</b>, normalized to 100%.</p>"
        )
        return suggestion_rows

    def refresh_portfolio_table(
        *,
        status_text: str | None = None,
        preserve_selected_item_id: str | None = None,
    ) -> None:
        nonlocal portfolio_run_rows_by_id, portfolio_preview_item_id
        items = load_portfolio_items()
        valid_ids = {item.item_id for item in items}
        portfolio_run_rows_by_id = {item_id: row for item_id, row in portfolio_run_rows_by_id.items() if item_id in valid_ids}
        portfolio_curves_by_id_keys = list(portfolio_curves_by_id.keys())
        for item_id in portfolio_curves_by_id_keys:
            if item_id not in valid_ids:
                portfolio_curves_by_id.pop(item_id, None)
                portfolio_curve_sources_by_id.pop(item_id, None)
        portfolio_items_by_id.clear()
        portfolio_items_by_id.update({item.item_id: item for item in items})
        portfolio_excluded_item_ids.intersection_update(valid_ids)
        if portfolio_preview_item_id not in valid_ids:
            portfolio_preview_item_id = None
        selected_item_id = preserve_selected_item_id if preserve_selected_item_id in valid_ids else None
        portfolio_table_source.data = portfolio_items_to_source(
            items,
            portfolio_run_rows_by_id,
            active_item_ids=active_portfolio_item_ids(items),
        )
        if selected_item_id is None:
            portfolio_table_source.selected.indices = []
        else:
            item_ids = list(portfolio_table_source.data.get("item_id", []))
            try:
                portfolio_table_source.selected.indices = [item_ids.index(selected_item_id)]
            except ValueError:
                portfolio_table_source.selected.indices = []
        if not items:
            portfolio_run_rows_by_id = {}
            portfolio_curves_by_id.clear()
            portfolio_curve_sources_by_id.clear()
            portfolio_preview_item_id = None
            clear_portfolio_equity_outputs()
            clear_portfolio_analysis_outputs()
            portfolio_status_div.text = status_text or "<p>Portfolio is empty. Use <b>В портфель</b> in the tester to save pairs and strategy parameters.</p>"
            return
        if status_text is not None:
            portfolio_status_div.text = status_text

    def on_add_to_portfolio() -> None:
        pair = current_pair()
        if pair is None:
            portfolio_status_div.text = "<p>Choose two different valid instruments before saving to portfolio.</p>"
            return
        # Portfolio rows are always saved from the current tester state:
        # current pair, timeframe, strategy params, defaults, and the latest
        # tester context metadata (for example optimizer/meta-derived OOS info).
        item = build_portfolio_item(
            symbol_1=pair.symbol_1,
            symbol_2=pair.symbol_2,
            timeframe=Timeframe(timeframe_select.value),
            params=build_distance_params(),
            defaults=build_defaults(),
            fee_mode=str(bybit_fee_mode_select.value or settings.bybit_tradfi_fee_mode),
            source_kind=str(current_tester_context.get("source_kind") or "tester_manual"),
            oos_started_at=_context_datetime(current_tester_context.get("oos_started_at")),
            context_started_at=_context_datetime(current_tester_context.get("context_started_at")),
            context_ended_at=_context_datetime(current_tester_context.get("context_ended_at")),
        )
        stored_item, created = upsert_portfolio_item(item)
        clear_portfolio_analysis_outputs()
        refresh_portfolio_table(
            status_text=(
                f"<p>{'Saved' if created else 'Updated'} portfolio item for <b>{stored_item.symbol_1}</b> / <b>{stored_item.symbol_2}</b> "
                f"on <b>{stored_item.timeframe.value}</b>. Source: <b>{stored_item.source_kind}</b>.</p>"
            )
        )

    def on_reload_portfolio() -> None:
        clear_portfolio_analysis_outputs()
        refresh_portfolio_table(status_text="<p>Reloaded portfolio CSV from disk.</p>")

    def on_remove_selected_portfolio_items() -> None:
        indices = list(portfolio_table_source.selected.indices)
        if not indices:
            portfolio_status_div.text = "<p>Select one or more portfolio rows to remove them.</p>"
            return
        item_ids = [str(portfolio_table_source.data["item_id"][index]) for index in indices]
        removed = remove_portfolio_items(item_ids)
        if removed <= 0:
            portfolio_status_div.text = "<p>No portfolio rows were removed.</p>"
            return
        clear_portfolio_analysis_outputs()
        refresh_portfolio_table(status_text=f"<p>Removed <b>{removed}</b> selected portfolio row(s).</p>")

    def on_analyze_portfolio() -> None:
        items = load_portfolio_items()
        if not items:
            clear_portfolio_analysis_outputs()
            portfolio_analysis_div.text = "<p>Portfolio is empty. Save at least one tester pair first.</p>"
            return
        started_at = _coerce_datetime(portfolio_period_slider.value[0])
        ended_at = _coerce_datetime(portfolio_period_slider.value[1])
        if ended_at <= started_at:
            portfolio_analysis_div.text = "<p>Portfolio Period is invalid. End must be after start.</p>"
            return
        total_capital = float(_read_spinner_value(capital_input, 10_000.0))
        active_ids = active_portfolio_item_ids(items)
        if not active_ids:
            clear_portfolio_analysis_outputs()
            portfolio_analysis_div.text = "<p>No active portfolio rows. Double-click a row again to re-enable it.</p>"
            return
        excluded_count = max(0, len(items) - len(active_ids))
        equal_allocation = total_capital / float(len(active_ids))
        active_allocation = {
            item.item_id: (equal_allocation if item.item_id in active_ids else 0.0)
            for item in items
        }
        curves, _rows, no_data_count, _ignored_excluded_count = run_portfolio_backtests(
            items,
            started_at=started_at,
            ended_at=ended_at,
            allocation_capitals_by_id=active_allocation,
            included_item_ids=active_ids,
        )
        active_curves = [curve for curve in curves if curve.item_id in active_ids]
        suggestions = refresh_portfolio_analysis(active_curves, started_at=started_at, ended_at=ended_at)
        portfolio_status_div.text = (
            f"<p>Portfolio analysis finished for <b>{len(items)}</b> saved pair(s). "
            f"Provisional equal allocation per pair: <b>{equal_allocation:.2f}</b>. "
            f"Valid suggestion rows: <b>{len(suggestions)}</b>. Missing data rows: <b>{no_data_count}</b>. "
            f"Rows excluded from combined equity: <b>{excluded_count}</b>.</p>"
        )

    def on_run_portfolio() -> None:
        nonlocal portfolio_run_rows_by_id, portfolio_preview_item_id
        items = load_portfolio_items()
        if not items:
            portfolio_status_div.text = "<p>Portfolio is empty. Save at least one tester pair first.</p>"
            clear_portfolio_equity_outputs()
            clear_portfolio_analysis_outputs()
            return
        started_at = _coerce_datetime(portfolio_period_slider.value[0])
        ended_at = _coerce_datetime(portfolio_period_slider.value[1])
        if ended_at <= started_at:
            portfolio_status_div.text = "<p>Portfolio Period is invalid. End must be after start.</p>"
            return

        total_capital = float(_read_spinner_value(capital_input, 10_000.0))
        allocation_mode = str(portfolio_allocation_select.value or PORTFOLIO_ALLOCATION_OPTIONS[0])
        active_ids = active_portfolio_item_ids(items)
        if not active_ids:
            portfolio_run_rows_by_id = {}
            portfolio_curves_by_id.clear()
            portfolio_curve_sources_by_id.clear()
            portfolio_preview_item_id = None
            clear_portfolio_equity_outputs()
            refresh_portfolio_table(status_text="<p>No active portfolio rows. Double-click a row again to re-enable it.</p>")
            return
        equal_allocation = total_capital / float(len(active_ids))
        final_allocation_by_id = {
            item.item_id: (equal_allocation if item.item_id in active_ids else 0.0)
            for item in items
        }
        final_mode_label = "equal_weight"
        no_data_count = 0
        excluded_count = max(0, len(items) - len(active_ids))
        display_allocation_by_id = portfolio_display_allocation_capitals(
            items,
            active_item_ids=active_ids,
            active_allocation_capitals_by_id=final_allocation_by_id,
            fallback_allocation_capital=equal_allocation,
        )

        if allocation_mode == "diversified_risk":
            provisional_curves, _rows, provisional_no_data_count, _ignored_excluded_count = run_portfolio_backtests(
                items,
                started_at=started_at,
                ended_at=ended_at,
                allocation_capitals_by_id=final_allocation_by_id,
                included_item_ids=active_ids,
            )
            active_provisional_curves = [curve for curve in provisional_curves if curve.item_id in active_ids]
            suggestion_rows = refresh_portfolio_analysis(active_provisional_curves, started_at=started_at, ended_at=ended_at)
            if suggestion_rows:
                final_allocation_by_id = {
                    item.item_id: 0.0
                    for item in items
                }
                for row in suggestion_rows:
                    final_allocation_by_id[row.item_id] = total_capital * float(row.suggested_weight)
                display_allocation_by_id = portfolio_display_allocation_capitals(
                    items,
                    active_item_ids=active_ids,
                    active_allocation_capitals_by_id=final_allocation_by_id,
                    fallback_allocation_capital=equal_allocation,
                )
                final_mode_label = "diversified_risk"
            else:
                final_mode_label = "equal_weight (fallback)"
            no_data_count = provisional_no_data_count

        curves, next_rows, run_no_data_count, _ignored_excluded_count = run_portfolio_backtests(
            items,
            started_at=started_at,
            ended_at=ended_at,
            allocation_capitals_by_id=display_allocation_by_id,
            included_item_ids=active_ids,
        )
        no_data_count = max(no_data_count, run_no_data_count)
        portfolio_run_rows_by_id = next_rows
        portfolio_curves_by_id.clear()
        portfolio_curve_sources_by_id.clear()
        portfolio_curves_by_id.update({curve.item_id: curve for curve in curves})
        if not any(curve.times and curve.equities for curve in curves if curve.item_id in active_ids):
            portfolio_preview_item_id = None
            clear_portfolio_equity_outputs()
            refresh_portfolio_table(
                status_text=(
                    f"<p>Portfolio run found no aligned data for active rows on <b>{started_at:%Y-%m-%d %H:%M}</b> .. "
                    f"<b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
                ),
            )
            return
        if allocation_mode != "diversified_risk":
            active_curves = [curve for curve in curves if curve.item_id in active_ids]
            refresh_portfolio_analysis(active_curves, started_at=started_at, ended_at=ended_at)
        refresh_portfolio_table(
            preserve_selected_item_id=portfolio_preview_item_id,
            status_text=(
                f"<p>Portfolio run finished for <b>{len(items)}</b> saved pair(s) on <b>{started_at:%Y-%m-%d %H:%M}</b> .. "
                f"<b>{ended_at:%Y-%m-%d %H:%M} UTC</b>. Total capital: <b>{total_capital:.2f}</b>, "
                f"allocation mode: <b>{final_mode_label}</b>. Active rows: <b>{len(active_ids)}</b>. "
                f"Baseline equal slice: <b>{equal_allocation:.2f}</b>. Missing data rows: <b>{no_data_count}</b>. "
                f"Rows excluded from combined equity: <b>{excluded_count}</b>.</p>"
            ),
        )
        sync_portfolio_equity_view()
        return
    def on_portfolio_period_change(_attr: str, _old: object, _new: object) -> None:
        clear_portfolio_analysis_outputs()
        if portfolio_run_rows_by_id:
            portfolio_status_div.text = "<p>Portfolio period changed. Run Portfolio again to refresh the equity curve.</p>"

    def on_portfolio_allocation_change(_attr: str, _old: object, _new: object) -> None:
        clear_portfolio_analysis_outputs()
        if portfolio_run_rows_by_id or portfolio_curves_by_id:
            on_run_portfolio()
            return
        portfolio_status_div.text = (
            f"<p>Allocation mode set to <b>{portfolio_allocation_select.value}</b>. "
            f"Run Portfolio to build the updated equity curve.</p>"
        )

    def on_portfolio_selection(_attr: str, _old: object, _new: object) -> None:
        nonlocal portfolio_preview_item_id
        portfolio_preview_item_id = selected_portfolio_item_id()
        sync_portfolio_equity_view()

    def on_portfolio_table_action(_attr: str, _old: object, _new: object) -> None:
        nonlocal portfolio_preview_item_id
        item_ids = list(portfolio_table_action_source.data.get("item_id", []))
        if not item_ids:
            return
        item_id = str(item_ids[0])
        item = portfolio_items_by_id.get(item_id)
        if item is None:
            return
        if item_id in portfolio_excluded_item_ids:
            portfolio_excluded_item_ids.remove(item_id)
            state_label = "included"
        else:
            portfolio_excluded_item_ids.add(item_id)
            state_label = "excluded"
        portfolio_preview_item_id = None
        refresh_portfolio_table(
            status_text=(
                f"<p><b>{item.symbol_1}</b> / <b>{item.symbol_2}</b> is now <b>{state_label}</b> "
                f"in current portfolio accounting. Single click previews only this row.</p>"
            ),
        )
        sync_portfolio_equity_view()

    def _coerce_meta_datetime(value: object) -> datetime | None:
        if value in (None, ""):
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None

    def sync_meta_oos_cutoff_marker(oos_started_at: object) -> None:
        cutoff_at = _coerce_meta_datetime(oos_started_at)
        if cutoff_at is None:
            meta_oos_cutoff_span.visible = False
            return
        meta_oos_cutoff_span.location = cutoff_at
        meta_oos_cutoff_span.visible = True

    def clear_meta_selection_highlight() -> None:
        meta_selected_fold_box.visible = False
        meta_selected_fold_span.visible = False

    def sync_meta_selection_highlight() -> None:
        indices = list(meta_table_source.selected.indices)
        if not indices:
            clear_meta_selection_highlight()
            return
        index = indices[0]
        data = meta_table_source.data
        starts = data.get("test_started_at", [])
        ends = data.get("test_ended_at", [])
        if index >= len(starts) or index >= len(ends):
            clear_meta_selection_highlight()
            return
        started_at = _coerce_meta_datetime(starts[index])
        ended_at = _coerce_meta_datetime(ends[index])
        if started_at is None or ended_at is None:
            clear_meta_selection_highlight()
            return
        meta_selected_fold_box.left = started_at
        meta_selected_fold_box.right = ended_at
        meta_selected_fold_box.visible = True
        meta_selected_fold_span.location = started_at
        meta_selected_fold_span.visible = True

    def read_meta_oos_started_at() -> datetime | None:
        raw_value = meta_oos_start_picker.value
        if raw_value in (None, ""):
            return None
        return datetime.fromisoformat(str(raw_value)).replace(tzinfo=UTC)

    def same_meta_oos_started_at(saved_value: object, current_value: datetime | None) -> bool:
        if saved_value in (None, "") and current_value is None:
            return True
        if saved_value in (None, "") or current_value is None:
            return False
        try:
            saved_dt = datetime.fromisoformat(str(saved_value).replace("Z", "+00:00"))
        except ValueError:
            return False
        return saved_dt == current_value

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
            "early_stopping_rounds": int(_read_spinner_value(meta_xgb_early_stopping_rounds_input, 30, cast=int)),
        }

    def clear_meta_outputs() -> None:
        nonlocal displayed_meta_signature
        meta_table_source.data = empty_meta_selector_table_data()
        meta_table_source.selected.indices = []
        meta_ranking_source.data = empty_meta_ranking_table_data()
        meta_ranking_source.selected.indices = []
        meta_equity_source.data = {"time": [], "equity": []}
        clear_meta_selection_highlight()
        sync_meta_oos_cutoff_marker(None)
        refresh_meta_equity_ranges()
        displayed_meta_signature = None

    def meta_outputs_present() -> bool:
        return bool(
            meta_table_source.data.get("fold")
            or meta_ranking_source.data.get("rank")
            or meta_equity_source.data.get("time")
        )

    def meta_result_signature(result: dict[str, object], *, fallback_pair: PairSelection | None = None) -> dict[str, object]:
        result_pair = result.get("pair")
        pair_payload = result_pair if isinstance(result_pair, dict) else {}
        model_type = str(result.get("model_type") or meta_model_select.value or "")
        model_config = normalized_model_config(model_type, dict(result.get("model_config") or {}))
        return {
            "symbol_1": str(pair_payload.get("symbol_1") or (fallback_pair.symbol_1 if fallback_pair is not None else "")),
            "symbol_2": str(pair_payload.get("symbol_2") or (fallback_pair.symbol_2 if fallback_pair is not None else "")),
            "timeframe": str(result.get("timeframe") or timeframe_select.value or ""),
            "model_type": model_type,
            "objective_metric": str(result.get("selected_objective_metric") or result.get("source_objective_metric") or ""),
            "oos_started_at": result.get("oos_started_at"),
            "model_config": model_config,
        }

    def can_keep_displayed_meta_result(pair: PairSelection, model_type: str) -> bool:
        if not meta_outputs_present() or displayed_meta_signature is None:
            return False
        return (
            str(displayed_meta_signature.get("symbol_1") or "") == pair.symbol_1
            and str(displayed_meta_signature.get("symbol_2") or "") == pair.symbol_2
            and str(displayed_meta_signature.get("timeframe") or "") == timeframe_select.value
            and str(displayed_meta_signature.get("model_type") or "") == model_type
        )

    def restore_saved_meta_result(*, show_message: bool = False) -> None:
        if meta_future is not None and not meta_future.done():
            return
        pair = current_pair()
        if pair is None:
            clear_meta_outputs()
            return
        model_type = str(meta_model_select.value or "")
        current_objective_metric = str(meta_objective_select.value or wfa_objective_select.value or "score_log_trades")
        current_oos_started_at = read_meta_oos_started_at()
        current_model_config = normalized_model_config(model_type, build_meta_model_config())
        keep_displayed = can_keep_displayed_meta_result(pair, model_type)
        saved = load_saved_meta_selector_result(
            broker=broker,
            pair=pair,
            timeframe=Timeframe(timeframe_select.value),
            model_type=model_type,
        )
        if saved is None:
            if not keep_displayed:
                clear_meta_outputs()
            if show_message and meta_body.visible:
                meta_status_div.text = (
                    f"<p>No saved Meta Selector result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on "
                    f"<b>{timeframe_select.value}</b> with model <b>{model_type}</b>.</p>"
                )
            elif keep_displayed:
                meta_status_div.text = (
                    f"<p>Showing the last loaded Meta Selector result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>, "
                    f"but no saved snapshot exists for the current selector state. Run Meta Selector again to refresh the grid.</p>"
                )
            return
        saved_objective_metric = str(saved.get("selected_objective_metric", "") or saved.get("source_objective_metric", "") or "")
        if saved_objective_metric and saved_objective_metric != current_objective_metric:
            if not keep_displayed:
                clear_meta_outputs()
            if show_message and meta_body.visible:
                meta_status_div.text = (
                    f"<p>Saved Meta Selector result exists, but it was built with objective <b>{saved_objective_metric}</b> "
                    f"while the current selector is <b>{current_objective_metric}</b>. Run Meta Selector again for the current objective.</p>"
                )
            elif keep_displayed:
                meta_status_div.text = (
                    f"<p>Showing the last loaded Meta Selector result, but the current meta objective is <b>{current_objective_metric}</b> "
                    f"while the shown grid was built with <b>{saved_objective_metric}</b>. Run Meta Selector again to refresh it.</p>"
                )
            return
        if not same_meta_oos_started_at(saved.get("oos_started_at"), current_oos_started_at):
            if not keep_displayed:
                clear_meta_outputs()
            if show_message and meta_body.visible:
                meta_status_div.text = (
                    f"<p>Saved Meta Selector result exists, but its OOS start does not match the current selector. "
                    f"Run Meta Selector again for the current OOS cutoff.</p>"
                )
            elif keep_displayed:
                saved_oos_label = "full history" if saved.get("oos_started_at") in (None, "") else str(saved.get("oos_started_at")).replace("T00:00:00Z", "").replace("Z", " UTC")
                current_oos_label = "full history" if current_oos_started_at is None else f"{current_oos_started_at:%Y-%m-%d}"
                meta_status_div.text = (
                    f"<p>Showing the last loaded Meta Selector result, but the current OOS start is <b>{current_oos_label}</b> "
                    f"while the shown grid was built with <b>{saved_oos_label}</b>. Run Meta Selector again to refresh it.</p>"
                )
            return
        saved_model_config = normalized_model_config(model_type, dict(saved.get("model_config") or {}))
        if saved_model_config != current_model_config:
            if not keep_displayed:
                clear_meta_outputs()
            if show_message and meta_body.visible:
                meta_status_div.text = (
                    f"<p>Saved Meta Selector result exists, but its model parameters do not match the current selector. "
                    f"Run Meta Selector again for the current model config.</p>"
                )
            elif keep_displayed:
                meta_status_div.text = (
                    f"<p>Showing the last loaded Meta Selector result, but the current model parameters differ from the shown grid. "
                    f"Run Meta Selector again to refresh it.</p>"
                )
            return
        complete_meta(saved, pair)
        if show_message:
            meta_status_div.text = (
                f"<p>Loaded saved Meta Selector result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> using "
                f"<b>{saved.get('model_type') or model_type}</b>. Selected folds: "
                f"<b>{len(saved.get('selected_folds', []) or [])}</b>, ranking rows: <b>{len(saved.get('ranking_rows', []) or [])}</b>.</p>"
            )

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
        objective_metric = str(wfa_objective_select.value or "score_log_trades")
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
            objective_metric=objective_metric,
        )
        if snapshot is None:
            wfa_table_source.data = empty_wfa_table_data()
            wfa_table_source.selected.indices = []
            wfa_equity_source.data = {"time": [], "equity": []}
            wfa_outputs.visible = False
            wfa_train_window_box.visible = False
            wfa_train_start_span.visible = False
            wfa_test_window_box.visible = False
            refresh_wfa_equity_ranges()
            if show_message and wfa_body.visible:
                wfa_status_div.text = (
                    f"<p>No saved WFA result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on "
                    f"<b>{timeframe_select.value}</b> with objective <b>{objective_metric}</b> and the selected WFA settings.</p>"
                )
            return
        apply_wfa_snapshot(snapshot, final=True)
        refresh_wfa_equity_ranges()
        reset_wfa_button()
        if show_message:
            wfa_status_div.text = (
                f"<p>Loaded saved WFA result for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
                f"Objective: <b>{snapshot.get('objective_metric') or objective_metric}</b>, "
                f"folds: <b>{int(snapshot.get('fold_count', 0) or 0)}</b>, stitched net: "
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
            f"Objective: <b>{result.get('objective_metric') or 'score_log_trades'}</b>, "
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
        objective_metric: str,
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
            objective_metric=objective_metric,
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
        objective_metric = str(wfa_objective_select.value or "score_log_trades")
        if lookback_units <= 0 or test_units <= 0:
            wfa_status_div.text = "<p>Lookback and Test must be positive integers.</p>"
            return
        wfa_table_source.data = empty_wfa_table_data()
        wfa_table_source.selected.indices = []
        wfa_equity_source.data = {"time": [], "equity": []}
        wfa_outputs.visible = False
        wfa_train_window_box.visible = False
        wfa_train_start_span.visible = False
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
            f" Objective: <b>{objective_metric}</b>.</p>"
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
                    objective_metric,
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
        nonlocal displayed_meta_signature
        meta_table_source.data = meta_selector_result_to_source(result)
        meta_table_source.selected.indices = []
        meta_ranking_source.data = meta_ranking_result_to_source(result)
        meta_ranking_source.selected.indices = []
        meta_equity_source.data = meta_result_to_equity_source(result)
        clear_meta_selection_highlight()
        sync_meta_oos_cutoff_marker(result.get("oos_started_at"))
        refresh_meta_equity_ranges()
        displayed_meta_signature = meta_result_signature(result, fallback_pair=pair)
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
        quality_metrics = dict(result.get("quality_metrics") or {})
        train_r2_metric = quality_metrics.get("train_r2")
        train_r2_label = "n/a" if train_r2_metric is None else f"{float(train_r2_metric or 0.0):.4f}"
        validation_r2_metric = quality_metrics.get("validation_r2")
        validation_r2_detail_label = "n/a" if validation_r2_metric is None else f"{float(validation_r2_metric or 0.0):.4f}"
        train_quality_label = (
            "n/a"
            if not quality_metrics
            else (
                f"MAE {float(quality_metrics.get('train_mae', 0.0) or 0.0):.4f}, "
                f"MSE {float(quality_metrics.get('train_mse', 0.0) or 0.0):.4f}, "
                f"RMSE {float(quality_metrics.get('train_rmse', 0.0) or 0.0):.4f}, "
                f"R2 {train_r2_label}"
            )
        )
        validation_quality_label = (
            "n/a"
            if quality_metrics.get("validation_mae") is None
            else (
                f"MAE {float(quality_metrics.get('validation_mae', 0.0) or 0.0):.4f}, "
                f"MSE {float(quality_metrics.get('validation_mse', 0.0) or 0.0):.4f}, "
                f"RMSE {float(quality_metrics.get('validation_rmse', 0.0) or 0.0):.4f}, "
                f"R2 {validation_r2_detail_label}"
            )
        )
        xgb_quality_sentence = ""
        if result.get("model_type") == "xgboost":
            xgb_quality_sentence = (
                f" XGB early stop: <b>{int(quality_metrics.get('xgboost_early_stopping_rounds', 0) or 0)}</b>, "
                f"best iter: <b>{quality_metrics.get('xgboost_best_iteration', 'n/a')}</b>, "
                f"best trees: <b>{quality_metrics.get('xgboost_best_n_estimators', 'n/a')}</b>, "
                f"final trees: <b>{quality_metrics.get('xgboost_final_n_estimators', 'n/a')}</b>."
            )
        source_wfa_run_id = str(result.get("source_wfa_run_id", "") or "")
        source_objective_metric = str(result.get("source_objective_metric", "") or "")
        selected_objective_metric = str(result.get("selected_objective_metric", "") or source_objective_metric or "")
        source_wfa_run_sentence = "" if not source_wfa_run_id else f" WFA run: <b>{source_wfa_run_id}</b>."
        objective_sentence = "" if not selected_objective_metric else f" Meta objective: <b>{selected_objective_metric}</b>."
        source_objective_sentence = "" if not source_objective_metric or source_objective_metric == selected_objective_metric else f" WFA objective: <b>{source_objective_metric}</b>."
        meta_status_div.text = (
            f"<p>Meta Selector completed for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> using <b>{result.get('model_type')}</b>. "
            f"OOS start: <b>{oos_label}</b>, pre-OOS rows: <b>{int(result.get('train_rows', 0) or 0)}</b>, "
            f"validation rows: <b>{int(result.get('validation_rows', 0) or 0)}</b>, OOS rows: <b>{int(result.get('oos_rows', 0) or 0)}</b>, "
            f"ranking rows: <b>{len(result.get('ranking_rows', []) or [])}</b>, "
            f"selected folds: <b>{len(result.get('selected_folds', []) or [])}</b>, stitched net: "
            f"<b>{float(result.get('stitched_net_profit', 0.0) or 0.0):.2f}</b>, stitched max DD: "
            f"<b>{float(result.get('stitched_max_drawdown', 0.0) or 0.0):.2f}</b>, trades: "
            f"<b>{int(result.get('stitched_total_trades', 0) or 0)}</b>. "
            f"Validation MAE: <b>{mae_label}</b>, Validation R2: <b>{r2_label}</b>.<br>"
            f"Train quality: <b>{train_quality_label}</b>.<br>"
            f"Validation quality: <b>{validation_quality_label}</b>.{xgb_quality_sentence}<br>"
            f"History: <b>{result.get('history_path') or 'n/a'}</b>.{source_wfa_run_sentence}{objective_sentence}{source_objective_sentence}</p>"
        )

    def run_meta_job(
        pair: PairSelection,
        timeframe: Timeframe,
        model_type: str,
        objective_metric: str,
        oos_started_at: datetime | None,
        model_config: dict[str, float | int],
    ) -> dict[str, object]:
        return run_meta_selector(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            model_type=model_type,
            target_metric=DEFAULT_META_TARGET,
            objective_metric=objective_metric,
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
        objective_metric = str(meta_objective_select.value or wfa_objective_select.value or "score_log_trades")
        oos_label = "full history" if oos_started_at is None else f"{oos_started_at:%Y-%m-%d}"
        mark_meta_running()
        meta_status_div.text = (
            f"<p>Running Meta Selector for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{timeframe_select.value}</b> "
            f"using <b>{meta_model_select.value}</b>, objective <b>{objective_metric}</b> and OOS start <b>{oos_label}</b>.</p>"
        )
        meta_future = meta_executor.submit(
            lambda: (
                run_meta_job(pair, Timeframe(timeframe_select.value), meta_model_select.value, objective_metric, oos_started_at, model_config),
                pair,
            )
        )
        if meta_poll_callback is None:
            meta_poll_callback = doc.add_periodic_callback(poll_meta_future, 250)

    def on_meta_selection(_attr: str, _old: object, _new: object) -> None:
        # UX contract:
        # Clicking a Selected OOS Folds row copies only that fold's strategy
        # parameters into the tester and replays charts on the current tester
        # period. It must not mutate tester dates or rebuild the fold table.
        sync_meta_selection_highlight()
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
        selected_test_started_at = _coerce_meta_datetime(data["test_started_at"][index])
        selected_test_ended_at = _coerce_meta_datetime(data["test_ended_at"][index])
        set_tester_context(
            "meta_selected_fold",
            oos_started_at=selected_test_started_at,
            context_started_at=selected_test_started_at,
            context_ended_at=selected_test_ended_at,
        )
        ran = apply_backtest_result(period_override=tester_period)
        if ran:
            meta_status_div.text = (
                f"<p>Meta-selected parameters copied into tester and executed on tester period <b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. "
                f"<b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

    def on_meta_ranking_selection(_attr: str, _old: object, _new: object) -> None:
        # UX contract:
        # Clicking a Meta Robustness Grid row replays that ranked parameter set
        # on the current tester period only. It must not overwrite tester dates
        # and must not be interpreted as "rebuild Selected OOS Folds by this row".
        clear_meta_selection_highlight()
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
        current_meta_oos_started_at = read_meta_oos_started_at()
        set_tester_context(
            "meta_robustness_row",
            oos_started_at=current_meta_oos_started_at,
            context_started_at=current_meta_oos_started_at,
            context_ended_at=None,
        )
        ran = apply_backtest_result(period_override=tester_period)
        if ran:
            meta_status_div.text = (
                f"<p>Robustness rank <b>{int(data['rank'][index])}</b> copied into tester and executed on tester period "
                f"<b>{tester_period[0]:%Y-%m-%d %H:%M}</b> .. <b>{tester_period[1]:%Y-%m-%d %H:%M} UTC</b>.</p>"
            )

    def complete_optimization(
        result: DistanceOptimizationResult,
        source_data: dict[str, list[object]],
        mode_label: str,
        pair: PairSelection,
    ) -> None:
        nonlocal displayed_optimization_signature, optimization_summary_html, suppress_optimization_selection
        state.optimization_source.data = source_data
        state.optimization_source.selected.indices = []
        request_context = optimization_request_context or {}
        requested_started_at = request_context.get("started_at")
        requested_ended_at = request_context.get("ended_at")
        range_label = ""
        if isinstance(requested_started_at, datetime) and isinstance(requested_ended_at, datetime):
            range_label = (
                f" Period: <b>{requested_started_at:%Y-%m-%d %H:%M}</b> .. "
                f"<b>{requested_ended_at:%Y-%m-%d %H:%M} UTC</b>."
            )
        signature_payload = request_context.get("signature")
        displayed_optimization_signature = dict(signature_payload) if isinstance(signature_payload, dict) else None
        if not result.rows:
            if result.cancelled:
                optimization_summary_html = "<p>Optimization stopped before evaluating any trials.</p>"
            elif result.failure_reason == "no_aligned_quotes":
                optimization_summary_html = (
                    f"<p>No aligned parquet data for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on the selected optimization period and timeframe.{range_label}</p>"
                )
            elif result.failure_reason == "no_valid_parameter_combinations":
                stop_rule = " and <b>entry_z &lt; stop_z</b>" if opt_stop_mode_select.value == "enabled" else ""
                optimization_summary_html = f"<p>Optimizer ranges produced zero valid combinations.{range_label} Use values that satisfy <b>exit_z &lt; entry_z</b>{stop_rule}. Negative <b>exit_z</b> means exit on the opposite z-score signal.</p>"
            else:
                optimization_summary_html = f"<p>No trials evaluated.{range_label} Check optimizer ranges and parquet coverage.</p>"
            optimization_status_div.text = optimization_summary_html
            return

        best = result.rows[0]
        if result.cancelled:
            optimization_summary_html = (
                f"<p>Stopped {mode_label} optimization for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> "
                f"after {result.evaluated_trials} trials. Best partial objective: {best.objective_score:.3f}, "
                f"Net: {best.net_profit:.2f}, Max DD: {best.max_drawdown:.2f}.{range_label}</p>"
            )
            optimization_status_div.text = optimization_summary_html
            return

        optimization_summary_html = (
            f"<p>Evaluated {result.evaluated_trials} {mode_label} trials for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
            f"Best objective: {best.objective_score:.3f}, Net: {best.net_profit:.2f}, Max DD: {best.max_drawdown:.2f}.{range_label}</p>"
        )
        suppress_optimization_selection = True
        try:
            state.optimization_source.selected.indices = [0]
        finally:
            suppress_optimization_selection = False
        if not render_optimization_trial_in_tester(0, selection_label="Best row"):
            optimization_status_div.text = (
                f"{optimization_summary_html}"
                f"<p>The best row is selected. Single-click copies parameters into tester and renders it on the current tester period.</p>"
            )

    def apply_backtest_result(*, period_override: tuple[datetime, datetime] | None = None) -> bool:
        request, error_message = current_backtest_request(period_override=period_override)
        if request is None:
            return clear_backtest_outputs(error_message or "<p>Test request is invalid.</p>")
        return apply_backtest_payload(run_tester_job(request))

    def begin_symbol_change_refresh_suppression() -> None:
        nonlocal suppress_symbol_change_refresh
        suppress_symbol_change_refresh += 1

    def end_symbol_change_refresh_suppression() -> None:
        nonlocal suppress_symbol_change_refresh
        suppress_symbol_change_refresh = max(0, suppress_symbol_change_refresh - 1)

    def maybe_refresh_price_plots_for_symbol_change() -> None:
        if suppress_symbol_change_refresh > 0:
            return
        if not (price_1_body.visible or price_2_body.visible):
            return

        request, _error_message = current_backtest_request()
        if request is None:
            return

        pair = request["pair"]
        timeframe = request["timeframe"]
        started_at = request["started_at"]
        ended_at = request["ended_at"]
        assert isinstance(pair, PairSelection)
        assert isinstance(timeframe, Timeframe)
        assert isinstance(started_at, datetime)
        assert isinstance(ended_at, datetime)

        set_tester_context("tester_manual", context_started_at=started_at, context_ended_at=ended_at)
        submit_tester_request(
            request,
            pending_message=(
                f"<p>Refreshing tester charts for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{timeframe.value}</b>. "
                f"Test period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
            ),
        )

    def refresh_instruments() -> None:
        sync_catalog_group_select_options()
        selected_group = group_select.value
        options = instrument_options_for_group(selected_group)
        begin_symbol_change_refresh_suppression()
        try:
            symbol_1_select.options = options
            symbol_1_select.value = _preferred_symbol_1(options, symbol_1_select.value)
            sync_symbol_2_filter(base_options=options, show_message=leg_2_filter_select.value != 'all_symbols')
        finally:
            end_symbol_change_refresh_suppression()
        sync_downloader_symbol_options(show_message=False)
        sync_price_plot_labels()
        if leg_2_filter_select.value == 'all_symbols':
            visible_count = len([option for option in options if option != INSTRUMENT_PLACEHOLDER])
            summary_div.text = f"<p>Loaded {visible_count} instruments for group '{selected_group}'.</p>"

    def on_group_change(_attr: str, _old: object, _new: object) -> None:
        refresh_instruments()
        on_optimization_config_change(_attr, _old, _new)

    def on_symbol_1_change(_attr: str, _old: object, _new: object) -> None:
        sync_price_plot_labels()
        begin_symbol_change_refresh_suppression()
        try:
            sync_symbol_2_filter(show_message=leg_2_filter_select.value != 'all_symbols')
        finally:
            end_symbol_change_refresh_suppression()
        restore_saved_wfa_result(show_message=False)
        restore_saved_meta_result(show_message=False)
        on_optimization_config_change(_attr, _old, _new)
        maybe_refresh_price_plots_for_symbol_change()

    def on_symbol_2_change(_attr: str, _old: object, _new: object) -> None:
        sync_price_plot_labels()
        restore_saved_wfa_result(show_message=False)
        restore_saved_meta_result(show_message=False)
        on_optimization_config_change(_attr, _old, _new)
        maybe_refresh_price_plots_for_symbol_change()

    def on_leg2_filter_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value != 'all_symbols')

    def on_leg2_cointegration_kind_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value != 'all_symbols')

    def on_co_mover_group_change(_attr: str, _old: object, _new: object) -> None:
        if leg_2_filter_select.value == "co_movers":
            sync_symbol_2_filter(show_message=True)

    def on_timeframe_change(_attr: str, _old: object, _new: object) -> None:
        sync_symbol_2_filter(show_message=leg_2_filter_select.value != 'all_symbols')
        restore_saved_scan_table(show_message=True)
        restore_saved_wfa_result(show_message=False)
        restore_saved_meta_result(show_message=False)
        on_optimization_config_change(_attr, _old, _new)

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
        on_optimization_config_change(_attr, _old, new)
        summary_div.text = f"<p>Bybit fee mode set to <b>{new}</b>. Next test and optimization runs will use this commission model.</p>"

    def on_optimization_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()
        on_optimization_config_change(_attr, _old, _new)

    def on_stop_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_stop_mode_ui()

    def on_opt_stop_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()
        on_optimization_config_change(_attr, _old, _new)

    def on_optimization_range_mode_change(_attr: str, _old: object, _new: object) -> None:
        sync_optimization_mode_ui()
        on_optimization_config_change(_attr, _old, _new)

    def on_display_settings_change(_attr: str, _old: object, _new: object) -> None:
        apply_plot_display_settings()
        rebalance_layout()
        refresh_plot_ranges()

    def on_meta_config_change(_attr: str, _old: object, _new: object) -> None:
        restore_saved_meta_result(show_message=False)

    def on_section_visibility_change(_attr: str, _old: object, _new: object) -> None:
        rebalance_layout()
        refresh_plot_ranges()

    def on_range_change(_attr: str, _old: object, _new: object) -> None:
        if suppress_shared_range_change:
            return
        refresh_plot_ranges()
        update_gapless_x_axis()

    def on_portfolio_range_change(_attr: str, _old: object, _new: object) -> None:
        if suppress_portfolio_range_change:
            return
        refresh_portfolio_equity_ranges(reset_x_range=False)

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
        if suppress_optimization_selection:
            return
        indices = state.optimization_source.selected.indices
        if not indices:
            return

        index = indices[0]
        render_optimization_trial_in_tester(index, selection_label="Trial")

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
        begin_symbol_change_refresh_suppression()
        try:
            symbol_1_select.options = options
            symbol_2_select.options = options
            symbol_1_select.value = symbol_1
            symbol_2_select.value = symbol_2
        finally:
            end_symbol_change_refresh_suppression()
        set_tester_context("scan_row", context_started_at=tester_period[0], context_ended_at=tester_period[1])
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
        tester_started_at = _coerce_datetime(period_slider.value[0])
        tester_ended_at = _coerce_datetime(period_slider.value[1])
        set_tester_context("tester_manual", context_started_at=tester_started_at, context_ended_at=tester_ended_at)
        request, error_message = current_backtest_request()
        if request is None:
            clear_backtest_outputs(error_message or "<p>Test request is invalid.</p>")
            return

        pair = request["pair"]
        timeframe = request["timeframe"]
        started_at = request["started_at"]
        ended_at = request["ended_at"]
        assert isinstance(pair, PairSelection)
        assert isinstance(timeframe, Timeframe)
        assert isinstance(started_at, datetime)
        assert isinstance(ended_at, datetime)

        submitted, busy_message = submit_tester_request(
            request,
            pending_message=(
                f"<p>Running Distance test for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b> on <b>{timeframe.value}</b>. "
                f"Test period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>.</p>"
            ),
        )
        if not submitted:
            summary_div.text = busy_message or "<p>Distance test is already running.</p>"

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
        request_context: dict[str, object],
        cancel_check,
    ) -> tuple[DistanceOptimizationResult, dict[str, list[object]], str, PairSelection, dict[str, object]]:
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
                cancel_check=cancel_check,
                parallel_workers=optimizer_parallel_workers,
                progress_callback=update_optimization_progress,
            )
            return result, optimization_results_to_source(result), "genetic", pair, request_context
        if mode_value != OptimizationMode.GRID.value:
            raise ValueError(f"Unsupported optimization mode: {mode_value}")

        result = optimize_distance_grid(
            broker=broker,
            pair=pair,
            timeframe=timeframe,
            started_at=started_at,
            ended_at=ended_at,
            defaults=defaults,
            search_space=search_space,
            objective_metric=objective_metric,
            cancel_check=cancel_check,
            parallel_workers=optimizer_parallel_workers,
            progress_callback=update_optimization_progress,
        )
        return result, optimization_results_to_source(result), "grid", pair, request_context

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
        nonlocal optimization_future, optimization_request_context
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
            result, source_data, mode_label, pair, request_context = future.result()
        except Exception as exc:  # pragma: no cover - runtime UI path
            optimization_status_div.text = f"<p>Optimization failed: {exc}</p>"
            return
        optimization_request_context = request_context
        complete_optimization(result, source_data, mode_label, pair)

    def on_run_optimization() -> None:
        nonlocal optimization_future, optimization_poll_callback, optimization_request_context

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
        if ended_at <= started_at:
            optimization_status_div.text = "<p>Optimization Period is invalid. End must be after start.</p>"
            return
        search_space = optimization_search_space()
        defaults = build_defaults()
        timeframe = Timeframe(timeframe_select.value)
        mode_value = str(optimization_mode_select.value or "")
        if mode_value not in OPTIMIZATION_MODE_OPTIONS:
            optimization_status_div.text = f"<p>Unsupported optimization mode selected: <b>{mode_value or 'empty'}</b>.</p>"
            return
        objective_metric = str(optimization_objective_select.value or "net_profit")
        config = genetic_optimizer_config() if mode_value == OptimizationMode.GENETIC.value else None
        optimization_request_context = {
            "started_at": started_at,
            "ended_at": ended_at,
            "signature": optimization_signature(
                pair=pair,
                timeframe=timeframe,
                started_at=started_at,
                ended_at=ended_at,
                mode_value=mode_value,
                objective_metric=objective_metric,
                search_space=search_space,
                defaults=defaults,
                fee_mode=str(bybit_fee_mode_select.value or ""),
                config=config,
            ),
        }

        valid_trial_count = count_distance_parameter_grid(search_space)
        if valid_trial_count <= 0:
            stop_rule = " and <b>entry_z &lt; stop_z</b>" if opt_stop_mode_select.value == "enabled" else ""
            optimization_status_div.text = (
                f"<p>Optimizer ranges produced zero valid combinations. "
                f"Use values that satisfy <b>exit_z &lt; entry_z</b>{stop_rule}. "
                f"Descending ranges are allowed, so <b>Start</b> may be greater than <b>Stop</b>. "
                f"Negative <b>exit_z</b> means exit on the opposite z-score signal.</p>"
            )
            return
        optimization_cancel_event = Event()
        optimization_progress["completed"] = 0
        optimization_progress["total"] = valid_trial_count
        optimization_progress["stage"] = "Preparing optimization"
        mark_optimization_running()
        mode_label = mode_value
        auto_suffix = ""
        if mode_value != OptimizationMode.GENETIC.value and optimization_range_mode_select.value == "auto":
            auto_target = max(1, int(_read_spinner_value(auto_grid_trials_input, 500, cast=int)))
            auto_suffix = f" Auto target: <b>{auto_target}</b>."
        optimization_status_div.text = (
            f"<p>Running {mode_label} optimization for <b>{pair.symbol_1}</b> / <b>{pair.symbol_2}</b>. "
            f"Optimization period: <b>{started_at:%Y-%m-%d %H:%M}</b> .. <b>{ended_at:%Y-%m-%d %H:%M} UTC</b>. "
            f"Valid combinations: {valid_trial_count}.{auto_suffix} Workers: <b>{optimizer_parallel_workers}</b>. Click the button again to stop.</p>"
        )
        optimization_future = optimization_executor.submit(
            run_optimization_job,
            mode_value,
            pair,
            timeframe,
            started_at,
            ended_at,
            defaults,
            search_space,
            objective_metric,
            config,
            optimization_request_context,
            optimization_cancel_event.is_set,
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
        co_mover_group_select.value = ALL_CO_MOVERS_LABEL
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
        wfa_objective_select.value = "score_log_trades"
        wfa_unit_select.value = WfaWindowUnit.WEEKS.value
        wfa_lookback_input.value = 8
        wfa_test_input.value = 2
        meta_model_select.value = default_meta_model()
        meta_objective_select.value = str(wfa_objective_select.value or "score_log_trades")
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
        meta_xgb_early_stopping_rounds_input.value = 30
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
        portfolio_period_slider.value = (_ui_datetime(history_start), _ui_datetime(now_utc))
        portfolio_allocation_select.value = PORTFOLIO_ALLOCATION_OPTIONS[0]

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
        refresh_portfolio_table()
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
    co_mover_group_select.on_change("value", on_co_mover_group_change)
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
    optimization_period_slider.on_change("value", on_optimization_config_change)
    optimization_objective_select.on_change("value", on_optimization_config_change)
    auto_grid_trials_input.on_change("value", on_optimization_config_change)
    opt_lookback_start.on_change("value", on_optimization_config_change)
    opt_lookback_stop.on_change("value", on_optimization_config_change)
    opt_lookback_step.on_change("value", on_optimization_config_change)
    opt_entry_start.on_change("value", on_optimization_config_change)
    opt_entry_stop.on_change("value", on_optimization_config_change)
    opt_entry_step.on_change("value", on_optimization_config_change)
    opt_exit_start.on_change("value", on_optimization_config_change)
    opt_exit_stop.on_change("value", on_optimization_config_change)
    opt_exit_step.on_change("value", on_optimization_config_change)
    opt_stop_start.on_change("value", on_optimization_config_change)
    opt_stop_stop.on_change("value", on_optimization_config_change)
    opt_stop_step.on_change("value", on_optimization_config_change)
    genetic_population_input.on_change("value", on_optimization_config_change)
    genetic_generations_input.on_change("value", on_optimization_config_change)
    genetic_elite_input.on_change("value", on_optimization_config_change)
    genetic_mutation_input.on_change("value", on_optimization_config_change)
    genetic_seed_input.on_change("value", on_optimization_config_change)
    meta_model_select.on_change("value", lambda _attr, _old, _new: sync_meta_model_ui())
    meta_model_select.on_change("value", on_meta_config_change)
    meta_objective_select.on_change("value", on_meta_config_change)
    meta_oos_start_picker.on_change("value", on_meta_config_change)
    meta_tree_max_depth_input.on_change("value", on_meta_config_change)
    meta_tree_min_samples_leaf_input.on_change("value", on_meta_config_change)
    meta_rf_estimators_input.on_change("value", on_meta_config_change)
    meta_rf_max_depth_input.on_change("value", on_meta_config_change)
    meta_rf_min_samples_leaf_input.on_change("value", on_meta_config_change)
    meta_rf_max_features_select.on_change("value", on_meta_config_change)
    meta_xgb_estimators_input.on_change("value", on_meta_config_change)
    meta_xgb_max_depth_input.on_change("value", on_meta_config_change)
    meta_xgb_learning_rate_input.on_change("value", on_meta_config_change)
    meta_xgb_subsample_input.on_change("value", on_meta_config_change)
    meta_xgb_colsample_input.on_change("value", on_meta_config_change)
    meta_xgb_early_stopping_rounds_input.on_change("value", on_meta_config_change)
    def sync_meta_objective_with_wfa(_attr: str, old: object, new: object) -> None:
        current_meta_objective = str(meta_objective_select.value or "")
        previous_wfa_objective = str(old or "")
        next_wfa_objective = str(new or "score_log_trades")
        if current_meta_objective in ("", previous_wfa_objective):
            meta_objective_select.value = next_wfa_objective
    wfa_period_slider.on_change("value", on_wfa_config_change)
    wfa_objective_select.on_change("value", sync_meta_objective_with_wfa)
    wfa_objective_select.on_change("value", on_wfa_config_change)
    wfa_unit_select.on_change("value", on_wfa_config_change)
    wfa_lookback_input.on_change("value", on_wfa_config_change)
    wfa_test_input.on_change("value", on_wfa_config_change)
    portfolio_period_slider.on_change("value", on_portfolio_period_change)
    portfolio_allocation_select.on_change("value", on_portfolio_allocation_change)
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
    portfolio_status_div.on_change("text", build_service_log_handler("portfolio"))
    state.shared_x_range.on_change("start", on_range_change)
    state.shared_x_range.on_change("end", on_range_change)
    portfolio_equity_plot.x_range.on_change("start", on_portfolio_range_change)
    portfolio_equity_plot.x_range.on_change("end", on_portfolio_range_change)
    state.trades_source.selected.on_change("indices", on_trade_selection)
    wfa_table_source.selected.on_change("indices", on_wfa_selection)
    meta_table_source.selected.on_change("indices", on_meta_selection)
    meta_ranking_source.selected.on_change("indices", on_meta_ranking_selection)
    state.optimization_source.selected.on_change("indices", on_optimization_selection)
    state.scan_source.selected.on_change("indices", on_scan_selection)
    portfolio_table_source.selected.on_change("indices", on_portfolio_selection)
    portfolio_table_action_source.on_change("data", on_portfolio_table_action)
    run_button.on_click(on_run_test)
    add_to_portfolio_button.on_click(on_add_to_portfolio)
    optimization_run_button.on_click(on_run_optimization)
    scan_run_button.on_click(on_run_scan)
    download_run_button.on_click(on_run_download)
    wfa_run_button.on_click(on_run_wfa)
    meta_run_button.on_click(on_run_meta)
    portfolio_run_button.on_click(on_run_portfolio)
    portfolio_analyze_button.on_click(on_analyze_portfolio)
    portfolio_reload_button.on_click(on_reload_portfolio)
    portfolio_remove_button.on_click(on_remove_selected_portfolio_items)

    top_toggle_bar = row(
        price_1_toggle,
        price_2_toggle,
        spread_toggle,
        zscore_toggle,
        equity_toggle,
        trades_toggle,
        zscore_metrics_toggle,
        portfolio_toggle,
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
        portfolio_block,
        trades_block,
        zscore_metrics_block,
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
    append_service_log("portfolio", portfolio_status_div.text)
    refresh_portfolio_table()
    restore_saved_wfa_result(show_message=False)
    restore_saved_meta_result(show_message=False)
    ensure_nonempty_layout()
    sync_price_plot_labels()
    apply_plot_display_settings()
    rebalance_layout()
    refresh_plot_ranges()
    file_state_controller.install_model_watchers()
    file_state_controller.persist()


build_document()

