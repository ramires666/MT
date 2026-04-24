# MT Pair Trading Service Functional Reference

Updated: 2026-04-02

## Scope

This document is a detailed functional map of the current codebase under `src/`.

What it covers:
- application purpose and current architecture
- Bokeh UI blocks and interaction rules
- domain flows: backtest, optimizer, Johansen scan, optimizer scanner, WFA, Meta Selector, portfolio
- storage layout and MT5 toolchain
- module-by-module function map
- current quirks, behavioral contracts, and coupling points

Inventory basis:
- AST scan across `src/` currently indexes `927` Python definitions across `70` files
- the single largest concentration is `src/bokeh_app/main.py` with `320` module-level, nested, and callback definitions
- the full generated repository-wide definition inventory lives in:
  - `docs/reference/src_callable_index.md`
- the full callback map for `main.py` is split into the companion appendix:
  - `docs/reference/bokeh_main_callback_index.md`

This document focuses on:
- complete block-level behavior
- complete module coverage
- explicit mention of important functions, helpers, and methods
- known hacks, edge contracts, and cross-module dependencies

Practical reading contract:
- this file is the narrative reference
- `docs/reference/src_callable_index.md` is the exhaustive AST appendix for literally every indexed function, nested callback, class, and method in `src/`
- `docs/reference/bokeh_main_callback_index.md` is the readable grouped appendix for the `main.py` callback monolith

## 1. System Overview

The project is a research and execution-prep environment for pair trading built around:
- MetaTrader 5 catalog and historical data
- Parquet-based storage
- Bokeh UI for interactive analysis
- FastAPI routes for service orchestration
- CPU-first domain stack using Polars, NumPy, Numba, and multiprocessing

Main top-level subsystems:
- `src/bokeh_app`: interactive research UI and runtime orchestration
- `src/domain`: research logic, backtests, optimizers, scans, WFA, meta-selection, portfolio analytics
- `src/storage`: persistence and snapshot loaders
- `src/tools`: MT5 export/sync and support scripts
- `src/mt5_gateway`: MT5 bridge-side download/catalog logic
- `src/core_api`: HTTP routes over selected flows

Main architectural reality today:
- `src/bokeh_app/main.py` is the dominant integration hotspot
- UI, orchestration, persistence restore/save, and job control are heavily coupled there
- domain modules are reasonably separated by problem area, but several boundaries are still blurred

## 2. UI Functional Description

## 2.1 Application Shell

The Bokeh application is built in `src/bokeh_app/main.py` almost entirely inside `build_document()`.

This function currently owns:
- widget construction
- plot construction
- results tables
- futures and executors for long-running jobs
- UI state restore/save
- all major selection callbacks
- job start/poll/cancel flows
- replay logic from tables back into the tester

Supporting UI modules:
- `src/bokeh_app/state.py`
  - central `AppState` with the shared `ColumnDataSource` objects for tester plots and tables
- `src/bokeh_app/adapters.py`
  - converts domain results into Bokeh source dictionaries
- `src/bokeh_app/view_utils.py`
  - overlay layout, bounds calculation, plot height helpers, toggle sync
- `src/bokeh_app/browser_state.py`
  - browser-side persistence helpers and bindings
- `src/bokeh_app/file_state.py`
  - JSON file persistence for state restore across restarts
- `src/bokeh_app/table_export.py`
  - XLSX export for visible tables with metadata rows
- `src/bokeh_app/zscore_diagnostics.py`
  - histogram and metrics payload for z-score diagnostics
- `src/bokeh_app/scanner_estimate.py`
  - rough ETA helpers for optimizer scanner

## 2.2 Sidebar Blocks

### Data / Universe / Pair Selection

Main controls:
- group selector
- `Symbol 1`
- `Symbol 2`
- `Leg 2 Filter`
- optional co-mover group
- cointegration-kind selector for leg-2 cointegrated partner filtering
- timeframe

Behavior:
- symbol dropdowns display `SYMBOL, Description` while storing raw symbol values
- group filtering uses MT5 catalog groups plus the synthetic `cointegration_pairs_candidates` group
- `Leg 2 Filter` modes:
  - `all_symbols`
  - `co_movers`
  - `cointegrated_only`
- the synthetic `cointegration_pairs_candidates` group is special:
  - it must behave as an exact pair list, not a symbol universe expansion
  - `Symbol 2` is restricted to exact CSV partners for the chosen `Symbol 1`

Key implementation helpers:
- `instrument_options_for_group()`
- `available_catalog_group_options()`
- `sync_catalog_group_select_options()`
- `sync_symbol_2_filter()`
- `copy_pair_to_selectors()`

### Tester Controls

Tester controls define:
- pair
- timeframe
- test period
- strategy parameters
- capital
- leverage
- margin budget per leg
- slippage
- fee mode

Tester behavioral contract:
- tester period is independent from optimizer period
- clicking optimizer/meta/WFA/scanner results copies parameters into tester
- replay runs on the current tester period unless the flow explicitly says otherwise

### Downloader Controls

Downloader controls allow:
- export by single symbol
- export by group
- period selection
- `missing_only` vs `force`

Important behavior:
- downloader is canonical `M5` import
- it uses MT5 terminal-side export, not the MetaTrader5 Python package as the primary historical path
- WSL and Windows path handling are supported through path coercion and `MT_SERVICE_MT5_COMMON_ROOT`

## 2.3 Main Workspace Blocks

### Price 1 / Price 2

Purpose:
- render leg prices
- show trade entry/exit markers
- support synchronized zoom/pan with the rest of the tester charts

### Spread / Residual

Purpose:
- plot spread/residual series
- acts as the central diagnostic panel between raw prices and z-score/equity

### Z-score + Bollinger

Purpose:
- visualize standardized spread
- show threshold bands and signal context

Additional z-score diagnostics:
- metrics table
- histogram-like distribution support

### Equity

Purpose:
- show total equity and leg-level equity
- overlay unrealized drawdown area
- show custom hover text with date and equity values
- show floating summary metrics overlay

Important quirks:
- total equity is thick black
- leg equity lines are thinner
- hover uses both Bokeh hover behavior and a custom moving `Label`
- summary overlay is manually positioned on plot geometry changes and on `DocumentReady`
- this overlay logic is one of the more fragile UI areas

### Trades Table

Purpose:
- inspect executed trades
- selecting a trade highlights markers on both legs

### Optimization Results

Purpose:
- show optimizer rows
- allow sort/export
- clicking a row copies parameters to tester and replays on tester period

### Cointegration Results

Purpose:
- show saved or running Johansen scan rows
- clicking a row copies pair into tester and optimizer selectors

### Scanner

Purpose:
- run grid optimizer across many pairs
- display top rows per pair
- support live partial updates, ETA, stop/resume, and table-click replay

Important behavior:
- can use all symbol combinations or restricted pair source
- cache is signature-based and includes timeframe, train/OOS window, defaults, search space, fee mode, and pair filter signature
- when using `cointegration_pairs_candidates`, pair enumeration should remain exact to CSV-defined pairs

### WFA

Purpose:
- run walk-forward optimization
- display fold table and stitched out-of-sample equity
- support replay from WFA folds back into tester

Behavioral contract:
- WFA replay is tester replay, not date mutation of the base tester period

### Meta Selector

Purpose:
- learn from saved WFA history
- rank candidate parameter sets
- select one row per fold
- build stitched equity and ranking tables

Important behavior:
- uses training-side engineered features, not direct OOS leakage features
- several safeguards sanitize NaN/inf-heavy metrics before model fitting
- UI can restore saved meta results for current pair/timeframe/model

### Portfolio

Purpose:
- store selected strategy rows
- analyze row-level metrics and cross-pair correlations
- run combined portfolio equity
- show capital load, unrealized drawdown, and open-position counts

Important split:
- `Analyze Portfolio` computes row metrics, allocations, and correlation diagnostics
- `Run Portfolio` builds the combined portfolio replay and combined equity/risk outputs

Important quirk:
- saved CSV stores strategy rows only
- runtime portfolio metrics are rebuilt in memory per session after analysis/run

## 2.4 Shared UI Contracts And Quirks

Important cross-block contracts:
- all main tester plots share x-range
- visible y-ranges are recalculated from the visible span
- table sorting is expected to work everywhere
- every major visible analysis table exposes XLSX export
- row selection is part of the UX contract for optimizer, scans, scanner, WFA, meta, and portfolio

Current UI quirks/hacks:
- `main.py` is very callback-heavy and stateful
- a lot of logic is nested inside `build_document()`, so testability is weaker than it should be
- some configuration knowledge exists both in UI and domain layers, especially around Meta Selector models
- scanner and scan restore logic rely on matching signatures; tiny config changes intentionally create distinct caches

## 3. Domain Functional Description

## 3.1 Backtest Stack

Main files:
- `src/domain/backtest/distance_engine.py`
- `src/domain/backtest/distance_pricing.py`
- `src/domain/backtest/distance_models.py`
- `src/domain/backtest/kernel.py`
- `src/domain/backtest/metric_formulas.py`

Main responsibilities:
- align pair frames
- prepare reusable backtest context
- simulate signal/position lifecycle
- compute costs and trade summaries
- compute curve metrics

Important behavior:
- optimizer/WFA use a fast metrics-only path when possible
- full chart/trade reconstruction is not always needed
- rolling mean/std is pushed into Numba for speed

## 3.2 Optimizer

Main files:
- `src/domain/optimizer/distance.py`
- `src/domain/optimizer/distance_models.py`
- `src/domain/optimizer/distance_metrics.py`
- `src/domain/optimizer/distance_parallel.py`
- `src/domain/optimizer/distance_genetic_core.py`
- `src/domain/optimizer/distance_grid.py`

Main public flows:
- `optimize_distance_grid()`
- `optimize_distance_grid_frame()`
- `optimize_distance_genetic()`
- `optimize_distance_genetic_frame()`

Execution pattern:
- load/aligned frame
- build reusable context
- expand search space or generate candidates
- evaluate candidates
- materialize `DistanceOptimizationRow`
- sort by selected objective

Notable quirks:
- execution-signature deduplication avoids recomputing equivalent trials
- more workers are not always faster after the metrics-only fast path
- objective changes can often reuse the same computed row metrics

## 3.3 Johansen / Cointegration Scan

Main files:
- `src/domain/scan/johansen_core.py`
- `src/domain/scan/johansen_universe.py`
- `src/domain/scan/unit_root.py`
- `src/storage/scan_results.py`

Flow:
- resolve symbols
- load close frames
- perform symbol-level unit-root screening
- construct allowed pair list
- run Johansen per pair
- build/save snapshots

Notable quirks:
- the symbol-level prefilter is critical for performance
- persistence matching is intentionally strict on timeframe, period, scope, and config
- the special pair-candidate CSV should now constrain allowed pairs exactly, not as a symbol combination explosion

## 3.4 Optimizer Scanner

Main files:
- `src/domain/scan/optimizer_grid_scan.py`
- `src/storage/scanner_results.py`

Flow:
- resolve symbols
- load quote frames/specs
- align each pair
- run grid optimizer per pair
- keep top rows per pair
- aggregate results
- persist progress/result snapshot

Notable quirks:
- supports `allowed_pair_keys`
- supports partial snapshots and resume
- request identity includes pair filter signature
- recent work added exact-pair handling for `cointegration_pairs_candidates`

## 3.5 WFA

Main files:
- `src/domain/wfa_genetic.py`
- `src/domain/wfa_windowing.py`
- `src/domain/wfa_evaluation.py`
- `src/domain/wfa_serialization.py`
- `src/storage/wfa_results.py`

Flow:
- build windows
- optimize per fold
- evaluate selected rows on OOS
- serialize fold history
- stitch OOS equity
- persist history/snapshot

Notable quirks:
- cancellation is cooperative
- history can be trimmed by `top-K`
- UI replay of WFA rows should not rewrite tester date anchors

## 3.6 Meta Selector

Main files:
- `src/domain/meta_selector.py`
- `src/domain/meta_selector_ml.py`
- `src/domain/meta_selector_outputs.py`
- `src/domain/meta_selector_types.py`

Flow:
- load latest WFA history
- resolve effective objective metric and target
- engineer `train_*` features
- split pre-OOS vs OOS folds
- fit configured model
- rank parameter sets
- build selected fold outputs and stitched equity
- persist meta outputs

Notable quirks:
- config normalization is split between UI and domain
- matrix sanitation is necessary because aggressive metrics can create huge/invalid values
- feature engineering and pipeline orchestration are currently spread across multiple files

## 3.7 Portfolio

Main files:
- `src/domain/portfolio.py`
- `src/storage/portfolio_store.py`

Flow:
- store portfolio item rows
- materialize scaled defaults per allocation
- derive valid analysis window per contextual row
- run per-item backtests
- compute combined equity/risk series
- compute correlations and allocation suggestions
- compute combined summary metrics

Notable quirks:
- portfolio rows preserve contextual metadata such as `oos_started_at` and contextual replay starts
- analysis window is intentionally clipped to in-sample when applicable
- combined portfolio risk overlays include unrealized drawdown, capital load, and open positions

## 4. Storage And MT5 Toolchain

## 4.1 Storage Roots

Defined in `src/storage/paths.py`:
- raw quotes root
- derived quotes root
- catalog root
- scans root
- scanner root
- UI state path
- WFA root
- meta selector root
- portfolio root

Notable quirk:
- `scanner_root()` exists, but optimizer scanner persistence is currently implemented under `scans_root()` via `src/storage/scanner_results.py`

## 4.2 Catalog And Quotes

Catalog:
- `src/storage/catalog.py`
- writes normalized catalog parquet
- enriches MT5 group path and instrument specs

Quotes:
- `src/storage/quotes.py`
- canonical raw write path is monthly `M5` parquet partitions

## 4.3 Scan Snapshots

Johansen scan snapshots:
- stored as `all_pairs.parquet`, `passed_pairs.parquet`, `summary.json`
- latest lookup and run listing live in `src/storage/scan_results.py`

Optimizer scanner snapshots:
- single parquet store under `data/scans/<broker>/optimizer_scanner_results.parquet`
- stores both result rows and per-pair progress rows
- signature-based identity is central

WFA:
- pair history parquet plus run snapshots

Portfolio:
- CSV-first storage for saved strategy rows

## 4.4 MT5 Export Toolchain

Main files:
- `src/tools/mt5_terminal_export_sync.py`
- `src/tools/mt5_export_catalog_sync.py`
- `src/tools/mt5_binary_export.py`
- `src/tools/mt5_sync.py`

Current historical data strategy:
- terminal-side scripted export is treated as the reliable path
- binary exports are decoded into Polars frames
- frames are written into canonical parquet partitions

WSL/Windows support:
- `coerce_platform_path()`
- `default_common_root()`
- optional `MT_SERVICE_MT5_COMMON_ROOT`

## 5. Module And Function Map

The list below covers the main functional modules in `src/`.

Important scope note:
- this section is intentionally descriptive and grouped by responsibility
- the exact generated dotted-name inventory for every indexed definition is in `docs/reference/src_callable_index.md`

### 5.1 Configuration And UI Support

#### `src/app_config.py`
- Purpose: environment-backed runtime settings.
- Function:
  - `get_settings`: returns cached `Settings`.
- Class:
  - `Settings`: pydantic settings model with ports, workers, broker, MT5 paths, fee mode, data roots.
- Quirk:
  - settings are cached by `lru_cache`, so tests and runtime overrides need `get_settings.cache_clear()`.

#### `src/bokeh_app/adapters.py`
- Purpose: convert domain results to Bokeh source payloads.
- Functions:
  - `empty_backtest_sources`
  - `_trade_marker_shape`
  - `_lookup_bar_x`
  - `result_to_sources`
  - `result_to_padded_sources`
  - `_blend_channel`
  - `_soft_metric_backgrounds`
  - `optimization_results_to_source`
  - `optimizer_scan_results_to_source`
  - `scan_results_to_source`
- Quirk:
  - any schema mismatch between domain result objects and UI tables breaks here first.

#### `src/bokeh_app/browser_state.py`
- Purpose: browser-local widget persistence glue.
- Functions:
  - `_serialize_default`
  - `_storage_helpers`
  - `_save_callback_code`
  - `_options_restore_code`
  - `_restore_assignment`
  - `_save_assignment`
  - `_needs_numeric_dom_persistence`
  - `attach_browser_state`
- Class:
  - `BrowserStateBinding`

#### `src/bokeh_app/file_state.py`
- Purpose: server-side JSON persistence of UI state.
- Functions:
  - `_serialize_value`
  - `_deserialize_range_value`
  - `_select_options`
  - `_sanitize_spinner_value`
  - `_binding_value`
  - `_read_json`
  - `_cleanup_temp_files`
  - `_atomic_write_json`
- Class:
  - `FileStateController` with methods `__post_init__`, `read_state`, `snapshot`, `persist`, `restore`, `clear`, `suspend`, `_restore_binding`, `install_model_watchers`

#### `src/bokeh_app/numeric_inputs.py`
- Purpose: numeric input normalization for fractional steps.
- Functions:
  - `fractional_step_decimals`
  - `has_fractional_step`
  - `normalize_fractional_value`

#### `src/bokeh_app/scanner_estimate.py`
- Purpose: rough optimizer-scanner ETA math.
- Functions:
  - `scanner_pair_count`
  - `estimate_scanner_runtime_seconds`

#### `src/bokeh_app/state.py`
- Purpose: central Bokeh source container for tester UI.
- Functions:
  - empty source factories for price, spread, z-score, equity, trades, markers, segments, optimization, scan
- Class:
  - `AppState`

#### `src/bokeh_app/table_export.py`
- Purpose: build XLSX export files with metadata rows.
- Functions:
  - filename/path helpers
  - XLSX XML builders
  - workbook assembly
  - metadata row helpers
  - `export_table_to_xlsx`

#### `src/bokeh_app/view_utils.py`
- Purpose: display labels, overlay layout, bounds, plot size, toggle sync.
- Functions:
  - `display_symbol_label`
  - `_measure_overlay_label_widths`
  - `compute_overlay_label_layout`
  - `coerce_datetime_ms`
  - `_collect_window_values`
  - `compute_series_bounds`
  - `compute_plot_height`
  - `compute_relative_plot_height`
  - `sync_toggle_button_types`

#### `src/bokeh_app/zscore_diagnostics.py`
- Purpose: z-score metrics and histogram payloads.
- Functions:
  - empty source builders
  - safe statistic helpers
  - formatters
  - `build_zscore_diagnostics`
- Class:
  - `ZScoreDiagnosticsPayload`

#### `src/bokeh_app/main.py`
- Purpose: monolithic Bokeh app builder and runtime controller.
- Module-level helpers:
  - `_option_value`, `_clone_select_options`, `_build_figure`, `_widget_with_help`, `_build_equity_summary_columns`, `_format_portfolio_metrics_line`, `_coerce_datetime`, `_ui_datetime`, `_datetime_to_bokeh_millis`, `_read_spinner_value`, `_format_compact_duration`, `normalized_model_config`, `_meta_selector_runtime`, `_has_xgboost_installed`, `_merge_symbol_options`, `_build_section`, `build_document`
- Full nested callback inventory:
  - see `docs/reference/bokeh_main_callback_index.md`
- Main quirks:
  - too much orchestration in one file
  - nested callback testability issues
  - direct imports from domain, storage, and tools
  - manual overlay placement and multi-job polling logic

### 5.2 Core API

#### `src/core_api/main.py`
- Functions:
  - `healthcheck`
  - `service_meta`

#### `src/core_api/routes_quotes.py`
- Functions:
  - `health_check`
  - `trigger_symbol_sync`
  - `trigger_quote_sync`

#### `src/core_api/routes_optimizer.py`
- Functions:
  - `optimization_health`
  - `_serialize_result`
  - `optimize_distance_grid_route`
  - `optimize_distance_genetic_route`

#### `src/core_api/routes_scan.py`
- Functions:
  - `health_check`
  - `run_johansen_pair_scan`
  - `run_johansen_batch_scan`

#### `src/core_api/routes_wfa.py`
- Functions:
  - `health_check`
  - `trigger_wfa`

### 5.3 Domain: Contracts, Data, Costs

#### `src/domain/contracts.py`
- Purpose: enums and shared dataclasses such as algorithm/timeframe/search-space contracts.
- Methods of note:
  - `WfaWindowSearchSpace.resolved_train_unit`
  - `WfaWindowSearchSpace.resolved_validation_unit`
  - `WfaWindowSearchSpace.resolved_test_unit`
  - `WfaWindowSearchSpace.resolved_walk_step_unit`

#### `src/domain/costs/profiles.py`
- Purpose: broker commission and cost-profile fallback logic.
- Functions:
  - `commission_overrides_path`
  - `read_commission_overrides`
  - `merge_commission_override`
  - normalization helpers
  - broker-specific fee profile builders
  - `apply_broker_commission_fallback`
- Class:
  - `CommissionProfile`

#### `src/domain/data/catalog_groups.py`
- Purpose: exact MT5 group filtering plus synthetic cointegration candidate group.
- Functions:
  - `cointegration_candidates_path`
  - `_read_cointegration_candidate_pairs_cached`
  - `is_cointegration_candidates_group`
  - `cointegration_candidate_pairs`
  - `cointegration_candidate_symbols`
  - `cointegration_candidate_pair_keys`
  - `cointegration_candidate_partner_symbols`
  - `cointegration_candidate_signature`
  - `mt5_group_path`
  - `with_mt5_group_column`
  - `list_mt5_group_options`
  - `filter_catalog_by_group`
- Quirk:
  - this module now knows about CSV-driven synthetic pair groups and therefore reaches into scan-storage territory.

#### `src/domain/data/co_movers.py`
- Purpose: manual co-mover partner groups.
- Functions:
  - `_available_symbol_set`
  - `co_mover_groups_for_symbol`
  - `co_mover_group_labels_for_symbol`
  - `co_mover_symbols_for_symbol`
- Class:
  - `CoMoverGroup` with `label`

#### `src/domain/data/instrument_groups.py`
- Function:
  - `normalize_group`

#### `src/domain/data/io.py`
- Purpose: cached quote/catalog/spec loading.
- Functions:
  - `raw_symbol_root`
  - `derived_symbol_root`
  - `scan_raw_quotes`
  - `_empty_quote_frame`
  - `_cached_raw_quotes_range`
  - `load_raw_quotes_range`
  - `_cached_quotes_range`
  - `load_quotes_range`
  - `_cached_instrument_catalog_frame`
  - `load_instrument_catalog_frame`
  - `_cached_instrument_spec_items`
  - `load_instrument_spec`
  - `list_symbols`

#### `src/domain/data/resample.py`
- Function:
  - `resample_m5_quotes`

#### `src/domain/data/timeframes.py`
- Function:
  - `to_polars_every`

### 5.4 Domain: Backtest

#### `src/domain/backtest/distance_engine.py`
- Purpose: distance backtest runtime core.
- Functions:
  - `_suffix_quotes`
  - `load_pair_frame`
  - `_column_or_zeros`
  - `prepare_distance_backtest_context`
  - `_signal_state`
  - `_signal_exit_reason`
  - `_build_summary`
  - `_empty_result`
  - `prepare_distance_backtest_metrics_frame`
  - `run_distance_backtest_frame`
  - `run_distance_backtest`
- Internal dataclasses:
  - `_DistanceSignalState`
  - `_DistanceBacktestContext`

#### `src/domain/backtest/distance_models.py`
- Purpose: parameter/result/position data models for the backtest engine.

#### `src/domain/backtest/distance_pricing.py`
- Functions:
  - `coerce_leg_spec`
  - `normalize_volume`
  - `adverse_slippage_offset`
  - `buy_spread_offset`
  - `price_to_account_pnl`
  - `notional_value`
  - `margin_basis_per_lot`
  - `commission_for_fill`
  - `price_with_costs`

#### `src/domain/backtest/kernel.py`
- Functions:
  - `compute_spread`
  - `compute_drawdown`
  - `rolling_mean_std`

#### `src/domain/backtest/metric_formulas.py`
- Functions:
  - `clamp_metric_value`
  - `safe_ratio`
  - `compute_k_ratio`
  - `compute_r_squared`
  - `duration_years_from_times`
  - `compute_cagr`
  - `compute_equity_curve_metrics`

### 5.5 Domain: Optimizer

#### `src/domain/optimizer/distance.py`
- Purpose: main optimization entrypoints.
- Functions:
  - `_evaluate_params`
  - `_prepare_optimizer_context`
  - `_execution_signature`
  - `_clone_row_for_params`
  - `_evaluate_params_parallel`
  - `_evaluate_candidate_tasks_parallel`
  - `iter_distance_parameter_grid`
  - `count_distance_parameter_grid`
  - `optimize_distance_grid_frame`
  - `optimize_distance_genetic_frame`
  - `_optimize_with_loaded_frame`
  - `optimize_distance_grid`
  - `optimize_distance_genetic`

#### `src/domain/optimizer/distance_genetic_core.py`
- Functions:
  - `_execution_signature`
  - `random_candidate`
  - `mutate_candidate`
  - `crossover_candidates`
  - `tournament_select`
  - `evaluate_candidates_into_cache`

#### `src/domain/optimizer/distance_grid.py`
- Functions:
  - search-space normalization and metric helpers
  - `run_distance_grid_search_frame`
  - `run_distance_grid_search`
- Classes:
  - `DistanceOptimizationTrial`
  - `DistanceOptimizationResult` with `best_trial`, `to_dict`

#### `src/domain/optimizer/distance_metrics.py`
- Functions:
  - `equity_metrics`
  - `objective_score`
  - `validate_objective_metric`
  - `sort_rows`
  - `params_from_candidate`

#### `src/domain/optimizer/distance_models.py`
- Purpose: search-space, config, row/result models and parsers.
- Functions:
  - `_grid_values`
  - `_optional_stop_values`
  - `_fixed_float_value`
  - `parse_distance_search_space`
  - `parse_distance_genetic_config`

#### `src/domain/optimizer/distance_parallel.py`
- Purpose: multiprocessing helpers for optimizer candidate evaluation.
- Functions:
  - `is_cancelled`
  - `emit_progress`
  - `_chunk_size`
  - `_chunk_tasks`
  - `resolve_worker_count`
  - `_collect_parallel_results`
  - `_evaluate_params_chunk`
  - `_evaluate_candidate_chunk`
  - `evaluate_distance_tasks`
  - `evaluate_candidate_distance_tasks`
- Nested closures:
  - `_collect_parallel_results.submit_available`
  - `evaluate_distance_tasks.submit_chunk`
  - `evaluate_candidate_distance_tasks.submit_chunk`

### 5.6 Domain: Scan

#### `src/domain/scan/johansen_core.py`
- Functions:
  - `validate_det_order`
  - `critical_column`
  - `prepare_series`
  - `compute_rank`
  - `compute_half_life`
  - `compute_last_zscore`
  - `build_scan_row`
  - `sort_rows`
  - `scan_pair_payload`
  - `scan_pair_johansen_arrays`
  - `scan_pair_frame_johansen`
  - `scan_pair_johansen`

#### `src/domain/scan/johansen_universe.py`
- Functions:
  - `emit_progress`
  - `is_cancelled`
  - `resolve_worker_count`
  - `normalize_group_value`
  - `load_symbol_close_frame`
  - `resolve_scan_symbols`
  - `screen_symbol_payload`
  - `screen_symbol_payloads_chunk`
  - `scan_pair_payloads_chunk`
  - `_chunk_size`
  - `_chunk_payloads`
  - `_take_next_payload_chunk`
  - `_build_result`
  - `_emit_partial_result`
  - `scan_symbol_frames_johansen`
  - `scan_universe_johansen`
- Nested helper:
  - `scan_symbol_frames_johansen.iter_pair_payloads`

#### `src/domain/scan/optimizer_grid_scan.py`
- Functions:
  - `emit_progress`
  - `is_cancelled`
  - `pair_key`
  - `load_symbol_quote_frame`
  - `_suffix_quotes`
  - `_align_pair_frame`
  - `_top_rows_for_pair`
  - `_sort_scan_rows`
  - `_scan_result`
  - `combine_optimizer_grid_scan_results`
  - `filter_optimizer_grid_scan_result`
  - `scan_symbol_frames_optimizer_grid`
  - `scan_universe_optimizer_grid`

#### `src/domain/scan/unit_root.py`
- Functions:
  - `_clean_series`
  - `_is_constant`
  - `_difference`
  - `_run_adf`
  - `_run_kpss`
  - `_evaluate_gate`
  - `screen_series_for_cointegration`
  - `screen_pair_for_cointegration`

### 5.7 Domain: WFA / Meta / Portfolio

#### `src/domain/wfa_evaluation.py`
- Functions:
  - `distance_params_from_payload`
  - `candidate_params`
  - `evaluate_distance_params`
  - `run_pair_window_trial`

#### `src/domain/wfa_genetic.py`
- Functions:
  - `_empty_wfa_result`
  - `_is_cancelled`
  - `run_distance_genetic_wfa`

#### `src/domain/wfa_request_runner.py`
- Function:
  - `run_wfa_request`

#### `src/domain/wfa_serialization.py`
- Functions:
  - `slice_frame`
  - `serialize_time`
  - `serialize_pair`
  - `serialize_optimization_row`
  - `build_fold_history_rows`
  - `stitch_pair_oos_equity`
  - `combine_pair_equity_series`

#### `src/domain/wfa_windowing.py`
- Functions:
  - `add_months`
  - `advance`
  - `build_walk_windows`
  - `build_train_test_windows`
- Class:
  - `WalkWindow`

#### `src/domain/meta_selector.py`
- Functions:
  - `_sanitize_symbol`
  - `_pair_key`
  - `_meta_output_dir`
  - `_output_paths`
  - `_serialize_time`
  - `_validation_split`
  - `_select_rows_per_fold`
  - `_latest_wfa_run_id`
  - `_latest_wfa_run_history`
  - `_latest_wfa_objective_metric`
  - `_resolved_meta_target`
  - `_selected_objective_metric`
  - `_objective_score_expr`
  - `_with_selected_objective_scores`
  - `_persist_meta_selector_outputs`
  - `load_saved_meta_selector_result`
  - `_empty_result`
  - `_history_window_count`
  - `run_meta_selector`

#### `src/domain/meta_selector_ml.py`
- Functions:
  - `with_time_columns`
  - `with_engineered_columns`
  - `_sanitize_numeric_matrix`
  - `_sanitize_numeric_vector`
  - `normalized_model_config`
  - `build_model`
  - `_fit_with_optional_early_stopping`
  - `_xgboost_best_n_estimators`
  - `_regression_quality`
  - `window_key_expr`
  - `with_window_key`
  - `objective_score_columns`
  - `validation_split`
  - `fit_predict`
  - `rank_parameter_sets`
  - `select_rows_per_fold`

#### `src/domain/meta_selector_outputs.py`
- Functions:
  - `stitch_equity_chunks`
  - `max_drawdown`
  - `build_selected_fold_outputs`

#### `src/domain/meta_selector_types.py`
- Class:
  - `MetaSelectorResult` with `to_dict`

#### `src/domain/portfolio.py`
- Functions:
  - `scale_defaults_for_portfolio_item`
  - `portfolio_strategy_started_at`
  - `portfolio_analysis_window`
  - `prepend_flat_equity_prefix`
  - `prepend_constant_series_prefix`
  - `latest_portfolio_oos_started_at`
  - `derive_portfolio_curve_risk_series`
  - `materialize_portfolio_backtest_allocations`
  - `_curve_label`
  - `_aligned_normalized_equities`
  - `_safe_corr`
  - `analyze_portfolio_curves`
  - `combine_portfolio_equity_curves`
  - `summarize_portfolio_equity_series`
- Dataclasses:
  - `PortfolioRunRow`
  - `PortfolioCurve`
  - `PortfolioEquitySummary`
  - `PortfolioCorrelationRow`
  - `PortfolioAllocationSuggestionRow`

### 5.8 Storage

#### `src/storage/catalog.py`
- Functions:
  - `catalog_path`
  - `instrument_catalog_path`
  - `_empty_catalog_frame`
  - `_normalize_catalog`
  - `write_instrument_catalog`
  - `read_instrument_catalog`

#### `src/storage/paths.py`
- Functions:
  - `raw_quotes_root`
  - `derived_quotes_root`
  - `catalog_root`
  - `scans_root`
  - `scanner_root`
  - `ui_state_path`
  - `wfa_root`
  - `meta_selector_root`
  - `portfolio_root`

#### `src/storage/portfolio_store.py`
- Functions:
  - `portfolio_items_path`
  - `_safe_float`
  - `_safe_int`
  - `_parse_datetime`
  - `_format_float`
  - `portfolio_item_signature`
  - `build_portfolio_item`
  - `_row_to_item`
  - `_item_to_row`
  - `load_portfolio_items`
  - `_write_portfolio_items`
  - `upsert_portfolio_item`
  - `remove_portfolio_items`
- Class:
  - `PortfolioItem` with `params` and `defaults`

#### `src/storage/quotes.py`
- Functions:
  - `raw_partition_path`
  - `write_m5_quotes`

#### `src/storage/scan_results.py`
- Functions:
  - path helpers
  - summary/snapshot readers
  - `load_latest_saved_scan_result`
  - `list_saved_scan_runs`
  - `load_saved_scan_result_by_summary_path`
  - `partner_symbols_from_snapshot`
  - `persist_johansen_scan_result`
  - `snapshot_to_johansen_result`
- Classes:
  - `SavedScanSnapshot`
  - `SavedScanRunOption`

#### `src/storage/scanner_results.py`
- Functions:
  - path/signature normalization
  - frame schema loading/writing
  - optimization-row serialization
  - progress/result record serialization
  - sort/summary reconstruction
  - `persist_optimizer_scanner_scope_snapshot`
  - `load_optimizer_scanner_scope_snapshot`
  - `load_optimizer_scanner_signature_snapshot`
  - `build_optimizer_scanner_request_signature`
  - `scanner_scope_label`
  - `clear_optimizer_scanner_scope`
  - `_pair_progress_rows_from_result`
  - `persist_optimizer_scanner_snapshot`
  - `_saved_at_from_frame`
  - `_universe_symbols_from_snapshot`
  - `_processed_pair_keys_from_snapshot`
  - `_result_from_snapshot`
  - `load_optimizer_scanner_snapshot`
- Classes:
  - `ScannerPairProgressRow`
  - `OptimizerScannerSnapshot`
  - `LoadedOptimizerScannerSnapshot`
- Nested helper:
  - `_summary_from_frame._sum_latest`

#### `src/storage/wfa_results.py`
- Functions:
  - key/suffix/path builders
  - `load_wfa_optimization_history`
  - `persist_wfa_optimization_history`
  - `persist_wfa_run_snapshot`
  - `load_wfa_run_snapshot`

### 5.9 Tools And MT5 Gateway

#### `src/tools/export_instrument_reference.py`
- Functions:
  - text cleanup and classification helpers
  - inventory and anomaly builders
  - markdown rendering
  - output writing
  - `main`

#### `src/tools/mt5_binary_export.py`
- Function:
  - `read_codex_binary`

#### `src/tools/mt5_export_catalog_sync.py`
- Functions:
  - `_parse_dt`
  - `month_partitions_between`
  - `chunked`
  - `symbol_partitions_exist`
  - `resolve_symbols`
  - `build_jobs`
  - `build_parser`
  - `main`

#### `src/tools/mt5_sync.py`
- Functions:
  - `_parse_dt`
  - `build_parser`
  - `main`

#### `src/tools/mt5_terminal_export_sync.py`
- Functions:
  - `coerce_platform_path`
  - `_windows_common_root_from_appdata`
  - `_iter_wsl_windows_common_root_candidates`
  - `default_common_root`
  - `codex_root`
  - `export_root`
  - `status_path`
  - `write_job_manifest`
  - `write_startup_config`
  - `run_terminal_export`
  - `read_export_statuses`
  - `_export_file_name`
  - `decode_exports`
  - `build_parser`
  - `main`
- Dataclasses:
  - `ExportJob`
  - `ExportStatus` with property `ok`

#### `src/tools/optimizer_clipboard_to_markdown.py`
- Functions:
  - header normalization and parsing helpers
  - `parse_optimizer_clipboard`
  - `render_markdown_table`
  - `_read_text`
  - `main`

#### `src/mt5_gateway/catalog.py`
- Class:
  - `CatalogSyncService` with `__init__`, `sync`, `refresh_universe`

#### `src/mt5_gateway/client.py`
- Class:
  - `MT5Client` with `__init__`, `_module`, `initialize`, `shutdown`, `_extract_commission`, `fetch_instruments`, `fetch_m5_quotes`, `fetch_latest_tick`

#### `src/mt5_gateway/ingestion.py`
- Class:
  - `QuoteIngestionService` with `__init__`, `download_m5`, `download_bulk`, `ensure_tf`

#### `src/mt5_gateway/service.py`
- Class:
  - `MT5GatewayService` with `__init__`, `refresh_instruments`, `download_quotes`, `stream_latest_ticks`

#### `src/workers/executor.py`
- Functions:
  - `_apply_cpu_worker_env_caps`
  - `build_process_pool`
  - `shared_process_pool`
  - `shutdown_executor`

## 6. Current Hacks, Fragile Areas, And Behavioral Edges

- `src/bokeh_app/main.py` is the main concentration of coupling and regression risk.
- The equity summary overlay is custom and more fragile than standard plot legends.
- Several restore/resume paths depend on exact signature equality; this is intentional but easy to break with ad hoc changes.
- Portfolio metrics are runtime-only and not part of the saved CSV.
- Optimizer scanner uses `scans_root()` despite `scanner_root()` existing.
- Synthetic exact-pair group logic now sits inside `catalog_groups.py`, which is functionally convenient but architecturally impure.
- Meta Selector config knowledge is duplicated between UI and domain.
- Scanner/Johansen/WFA all implement flavors of progress/cancel/partial persistence that are similar but not fully unified.

## 7. Companion Documents

Detailed callback appendix for the Bokeh monolith:
- `docs/reference/bokeh_main_callback_index.md`

Full generated AST inventory for all indexed definitions under `src/`:
- `docs/reference/src_callable_index.md`

Refactor plan collected during this documentation pass:
- `docs/notes/refactor_structure_plan_2026-04-02.md`
