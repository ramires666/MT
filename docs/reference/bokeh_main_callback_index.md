# Bokeh Main Callback Index

Updated: 2026-04-02

This appendix expands the callable map of `src/bokeh_app/main.py`.

Why it exists:
- `main.py` is the single largest logic hotspot in the repo
- most UI behavior is not encoded as top-level modules, but as nested helpers and callbacks inside `build_document()`
- the AST scan of `main.py` currently sees `320` indexed definitions
- the full repository-wide inventory is in `docs/reference/src_callable_index.md`

This file groups them by responsibility so the callback map is still readable.

## 1. Module-Level Helpers

- `_option_value`
- `_clone_select_options`
- `_build_figure`
- `_widget_with_help`
- `_build_equity_summary_columns`
- `_format_portfolio_metrics_line`
- `_coerce_datetime`
- `_ui_datetime`
- `_datetime_to_bokeh_millis`
- `_read_spinner_value`
- `_format_compact_duration`
- `normalized_model_config`
- `_meta_selector_runtime`
- `_has_xgboost_installed`
- `_merge_symbol_options`
- `_build_section`
- `build_document`

Role:
- generic formatting, widget wiring, time conversion, plot construction, and top-level document bootstrap

## 2. Static Source Builders And Export Helpers

- `optimizer_help`
- `sync_price_plot_labels`
- `build_optimization_table_columns`
- `empty_scanner_table_data`
- `_selected_symbols_for_export`
- `_format_export_period`
- `_format_manual_range`
- `_tester_export_metadata`
- `_optimization_export_metadata`
- `_scan_export_metadata`
- `_build_table_export_controls`
- `_build_table_export_controls.on_export_click`
- `empty_wfa_table_data`
- `wfa_result_to_sources`
- `_wfa_export_metadata`
- `empty_meta_selector_table_data`
- `empty_meta_ranking_table_data`
- `meta_selector_result_to_source`
- `meta_ranking_result_to_source`
- `meta_result_to_equity_source`
- `default_meta_model`
- `sync_meta_model_ui`
- `_meta_export_metadata`
- `empty_portfolio_table_data`
- `empty_portfolio_equity_source_data`
- `portfolio_items_to_source`
- `empty_portfolio_weight_data`
- `empty_portfolio_correlation_data`
- `portfolio_weight_rows_to_source`
- `portfolio_correlation_rows_to_source`

Role:
- define table columns, transform result payloads into Bokeh sources, and build export metadata for XLSX dumps

## 3. Service Log, Layout, Section Visibility

- `_normalize_log_message`
- `append_service_log`
- `build_service_log_handler`
- `build_service_log_handler._handler`
- `sync_service_log_toggle`
- `sync_section_toggle_states`
- `set_section_visibility`
- `build_section_toggle_handler`
- `build_section_toggle_handler._handler`
- `toggle_service_log`

Role:
- service log feed, block visibility, top-toggle state synchronization, and collapsible section behavior

## 4. Plot Geometry, Overlay, Axes, Diagnostics

- `clear_trade_highlights`
- `set_equity_summary_overlay`
- `queue_sync_equity_summary_overlay_position`
- `_safe_plot_dimension`
- `sync_equity_summary_overlay_position`
- `on_equity_plot_geometry_change`
- `on_document_ready`
- `sync_equity_legend`
- `configured_plot_height`
- `apply_plot_display_settings`
- `_axis_reference_series`
- `_nearest_bar_x`
- `update_gapless_x_axis`
- `update_gapless_x_axis.reduce_positions`
- `sync_optimization_cutoff_marker`
- `sync_optimization_train_overlay`
- `clear_zscore_diagnostics_outputs`
- `apply_zscore_diagnostics`
- `apply_zscore_diagnostics_payload`

Role:
- manual equity overlay placement, plot sizing, gapless x-axis mapping, range overlays, and z-score diagnostic rendering

Quirk:
- this group contains several of the most fragile UI behaviors because it depends on actual Bokeh layout geometry and x-range synchronization

## 5. Tester Core And Replay Pipeline

- `clear_backtest_outputs`
- `current_backtest_request`
- `run_tester_job`
- `apply_backtest_payload`
- `submit_tester_request`
- `poll_tester_future`
- `set_shared_x_range`
- `refresh_plot_ranges`
- `rebalance_layout`
- `ensure_nonempty_layout`
- `build_defaults`
- `sync_bybit_fee_mode`
- `build_distance_params`
- `set_tester_context`
- `current_pair`
- `_context_datetime`
- `_empty_scan_source_data`
- `_scan_frame_to_source`

Role:
- create normalized tester request payloads, submit background backtests, update plots/tables, and maintain synchronized ranges

## 6. Symbol Options, Group Resolution, Leg-2 Filtering

- `_instrument_option_label`
- `_symbol_label_map`
- `_symbol_select_options`
- `_preferred_symbol_1`
- `_preferred_symbol_2`
- `instrument_options_for_group`
- `available_catalog_group_options`
- `sync_catalog_group_select_options`
- `current_download_terminal_path`
- `current_download_common_root`
- `sync_downloader_symbol_options`
- `sync_downloader_mode_ui`
- `sync_leg_2_filter_ui`
- `_sync_co_mover_group_options`
- `current_tester_cointegration_scope`
- `current_scan_selection_scope`
- `current_scan_period`
- `current_johansen_scan_config`
- `scanner_cointegration_run_options`
- `sync_scanner_cointegration_run_options`
- `current_scanner_cointegration_snapshot`
- `current_scanner_pair_source_label`
- `scanner_allowed_pair_keys_for_scope`
- `exact_group_allowed_pair_keys`
- `merge_allowed_pair_key_filters`
- `scan_snapshot_pair_keys`
- `scan_snapshot_matches_exact_group_pairs`
- `current_scanner_selection_scope`
- `current_scanner_view_scope_label`
- `available_scanner_scope_labels`
- `selected_scanner_scope_labels`
- `scanner_target_grid_trials`
- `current_optimizer_oos_started_at`
- `current_scanner_train_period`
- `active_scanner_train_period`
- `active_scanner_oos_started_at`
- `scanner_search_space`
- `build_selected_scanner_scope_jobs`
- `current_scanner_request_signature`
- `copy_pair_to_selectors`
- `scanner_is_running`
- `scanner_busy_message`
- `running_heavy_job_label`
- `load_tester_cointegration_snapshot`
- `restore_saved_scan_table`
- `restore_saved_scanner_table`
- `sync_symbol_2_filter`

Role:
- maintain all group/symbol dropdowns, special synthetic groups, saved-scan partner filters, exact pair-list restrictions, and scanner request identity

Quirk:
- this is where exact-pair CSV logic, saved-scan partner logic, and ordinary group filtering all collide

## 7. Auto Search-Space And Optimizer Configuration

- `_auto_axis_values`
- `_product_value`
- `_build_auto_search_space`
- `_auto_grid_point_counts`
- `_auto_optimization_search_space`
- `optimization_search_space`
- `genetic_optimizer_config`
- `set_optimizer_control_visible`
- `install_optimizer_tooltips`
- `sync_stop_mode_ui`
- `sync_optimization_mode_ui`
- `reset_optimization_button`
- `mark_optimization_running`
- `reset_run_button`
- `mark_test_running`
- `clear_tester_poll_callback`
- `update_optimization_progress`
- `render_optimization_progress`
- `_serialize_optimization_signature_value`
- `optimization_signature`
- `optimization_signature_matches`
- `optimization_outputs_present`
- `copy_optimization_trial_to_tester`
- `apply_optimization_trial`
- `render_optimization_trial_in_tester`
- `current_optimization_signature`
- `optimization_signature_changes`
- `on_optimization_config_change`

Role:
- maintain optimizer search-space controls, genetic/grid config, signature tracking, progress rendering, and replay from optimizer rows back into tester

## 8. Johansen Scan UI And Runtime

- `reset_scan_button`
- `mark_scan_running`
- `update_scan_progress`
- `update_scan_partial_result`
- `selected_scan_row_key`
- `scan_row_index_by_key`
- `apply_scan_snapshot`
- `render_scan_progress`
- `run_scan_job`
- `run_scan_job.persist_partial_scan_snapshot`
- `run_scan_job.handle_partial_scan_result`
- `clear_scan_poll_callback`
- `poll_scan_future`
- `on_run_scan`

Role:
- drive Johansen run/start/stop/poll/restore, apply partial results into the scan table, and persist partial snapshots while the job is running

## 9. Optimizer Scanner UI And Runtime

- `reset_scanner_button`
- `mark_scanner_running`
- `update_scanner_progress`
- `update_scanner_partial_result`
- `selected_scanner_row_key`
- `scanner_row_index_by_key`
- `apply_scanner_snapshot`
- `render_scanner_progress`
- `persist_scanner_result_snapshot`
- `estimate_scanner_eta`
- `run_scanner_job`
- `run_scanner_job.combined_result`
- `clear_scanner_poll_callback`
- `poll_scanner_future`
- `on_run_scanner`

Role:
- manage optimizer-scanner live table updates, ETA, per-scope aggregation, partial persistence, cancellation, and final restore behavior

Quirk:
- this block has complex signature/resume logic because scope, pair source, timeframe, train window, defaults, and optimizer grid all participate in cache identity

## 10. Downloader Runtime

- `reset_download_button`
- `mark_download_running`
- `update_download_progress`
- `render_download_progress`
- `run_download_job`
- `clear_download_poll_callback`
- `poll_download_future`
- `on_run_download`

Role:
- submit MT5 download/export tasks, render progress, and reconcile symbol-scope behavior from the sidebar

## 11. WFA Runtime And UI

- `reset_wfa_button`
- `mark_wfa_running`
- `update_wfa_progress`
- `update_wfa_partial_result`
- `sync_wfa_selection_highlight`
- `sync_wfa_outputs_visibility`
- `refresh_wfa_equity_ranges`
- `restore_saved_wfa_result`
- `apply_wfa_snapshot`
- `render_wfa_progress`
- `complete_wfa`
- `run_wfa_job`
- `clear_wfa_poll_callback`
- `poll_wfa_future`
- `on_run_wfa`

Role:
- WFA run lifecycle, live partial updates, saved-result restore, stitched equity display, row selection highlight, and replay plumbing

## 12. Portfolio Runtime And UI

- `refresh_meta_equity_ranges`
- `active_portfolio_item_ids`
- `portfolio_allocation_capitals_by_id`
- `portfolio_display_allocation_capitals`
- `selected_portfolio_item_id`
- `build_portfolio_source_data`
- `update_portfolio_metrics_summary`
- `build_portfolio_curve_source_data`
- `portfolio_curve_source_data`
- `set_portfolio_x_range`
- `render_portfolio_equity_curve`
- `render_portfolio_combined_equity`
- `sync_portfolio_equity_view`
- `clear_portfolio_equity_outputs`
- `reset_portfolio_button`
- `mark_portfolio_running`
- `clear_portfolio_analysis_outputs`
- `refresh_portfolio_equity_ranges`
- `sync_portfolio_oos_cutoff_marker`
- `clear_portfolio_poll_callback`
- `run_portfolio_backtests`
- `run_portfolio_job`
- `refresh_portfolio_analysis`
- `refresh_portfolio_table`
- `on_add_to_portfolio`
- `on_reload_portfolio`
- `on_remove_selected_portfolio_items`
- `on_analyze_portfolio`
- `poll_portfolio_future`
- `on_run_portfolio`
- `on_portfolio_period_change`
- `on_portfolio_allocation_change`
- `on_portfolio_selection`
- `on_portfolio_table_action`

Role:
- manage saved portfolio rows, per-row analysis metrics, combined portfolio replay, risk overlays, allocation tables, and portfolio-specific async execution

## 13. Meta Selector Runtime And UI

- `_coerce_meta_datetime`
- `sync_meta_oos_cutoff_marker`
- `clear_meta_selection_highlight`
- `sync_meta_selection_highlight`
- `read_meta_oos_started_at`
- `same_meta_oos_started_at`
- `build_meta_model_config`
- `clear_meta_outputs`
- `meta_outputs_present`
- `meta_result_signature`
- `can_keep_displayed_meta_result`
- `restore_saved_meta_result`
- `reset_meta_button`
- `mark_meta_running`
- `complete_meta`
- `run_meta_job`
- `clear_meta_poll_callback`
- `poll_meta_future`
- `on_run_meta`
- `on_meta_selection`
- `on_meta_ranking_selection`

Role:
- construct meta model config, restore saved meta outputs, run/poll meta training jobs, and replay selected rows/folds back into tester

## 14. Shared Selection, Event, And Job Handlers

- `complete_optimization`
- `apply_backtest_result`
- `begin_symbol_change_refresh_suppression`
- `end_symbol_change_refresh_suppression`
- `maybe_refresh_price_plots_for_symbol_change`
- `refresh_instruments`
- `on_group_change`
- `on_symbol_1_change`
- `on_symbol_2_change`
- `on_leg2_filter_change`
- `on_leg2_cointegration_kind_change`
- `on_co_mover_group_change`
- `on_timeframe_change`
- `on_scan_universe_change`
- `on_scanner_universe_change`
- `on_scan_kind_change`
- `on_download_group_change`
- `on_download_scope_change`
- `on_wfa_config_change`
- `on_bybit_fee_mode_change`
- `on_optimization_mode_change`
- `on_stop_mode_change`
- `on_opt_stop_mode_change`
- `on_optimization_range_mode_change`
- `on_display_settings_change`
- `on_meta_config_change`
- `on_section_visibility_change`
- `on_range_change`
- `on_portfolio_range_change`
- `on_trade_selection`
- `on_wfa_selection`
- `on_optimization_selection`
- `on_scan_selection`
- `on_scanner_selection`
- `on_run_test`
- `run_optimization_job`
- `clear_optimization_poll_callback`
- `poll_optimization_future`
- `on_run_optimization`
- `on_reset_defaults`
- `on_session_destroyed`
- `sync_meta_objective_with_wfa`

Role:
- glue together all event sources, row selections, async lifecycles, refresh suppression, reset, and session teardown

## 15. Practical Reading Order

If the goal is to understand runtime behavior quickly, read in this order:
1. module-level imports and constants in `main.py`
2. source/table/plot builders
3. tester pipeline (`current_backtest_request` -> `submit_tester_request` -> `poll_tester_future`)
4. symbol/group filtering helpers
5. optimizer run flow
6. Johansen scan flow
7. optimizer scanner flow
8. portfolio flow
9. WFA flow
10. meta flow
11. reset/session-destroy logic

That order mirrors how most regressions propagate in practice.
