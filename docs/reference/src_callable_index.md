# Source Callable Index

Updated: 2026-04-02

This appendix is a generated inventory of Python definitions under `src/`.

Coverage rules:
- includes module-level functions
- includes nested functions and callbacks
- includes classes and class methods
- names are shown in dotted form within each module

Total Python files with definitions: `70`
Total indexed definitions: `927`
Definitions inside `src/bokeh_app/main.py`: `320`

## Count By Module

- `src/app_config.py`: `2`
- `src/bokeh_app/adapters.py`: `10`
- `src/bokeh_app/browser_state.py`: `9`
- `src/bokeh_app/file_state.py`: `20`
- `src/bokeh_app/main.py`: `320`
- `src/bokeh_app/numeric_inputs.py`: `3`
- `src/bokeh_app/scanner_estimate.py`: `2`
- `src/bokeh_app/state.py`: `11`
- `src/bokeh_app/table_export.py`: `16`
- `src/bokeh_app/view_utils.py`: `11`
- `src/bokeh_app/zscore_diagnostics.py`: `9`
- `src/core_api/main.py`: `2`
- `src/core_api/routes_optimizer.py`: `4`
- `src/core_api/routes_quotes.py`: `3`
- `src/core_api/routes_scan.py`: `3`
- `src/core_api/routes_wfa.py`: `2`
- `src/domain/backtest/distance_engine.py`: `15`
- `src/domain/backtest/distance_models.py`: `4`
- `src/domain/backtest/distance_pricing.py`: `9`
- `src/domain/backtest/kernel.py`: `6`
- `src/domain/backtest/metric_formulas.py`: `7`
- `src/domain/contracts.py`: `24`
- `src/domain/costs/profiles.py`: `10`
- `src/domain/data/catalog_groups.py`: `12`
- `src/domain/data/co_movers.py`: `6`
- `src/domain/data/instrument_groups.py`: `1`
- `src/domain/data/io.py`: `13`
- `src/domain/data/resample.py`: `1`
- `src/domain/data/timeframes.py`: `1`
- `src/domain/meta_selector.py`: `19`
- `src/domain/meta_selector_ml.py`: `16`
- `src/domain/meta_selector_outputs.py`: `3`
- `src/domain/meta_selector_types.py`: `2`
- `src/domain/optimizer/distance.py`: `13`
- `src/domain/optimizer/distance_genetic_core.py`: `6`
- `src/domain/optimizer/distance_grid.py`: `17`
- `src/domain/optimizer/distance_metrics.py`: `5`
- `src/domain/optimizer/distance_models.py`: `9`
- `src/domain/optimizer/distance_parallel.py`: `13`
- `src/domain/portfolio.py`: `19`
- `src/domain/scan/johansen_core.py`: `12`
- `src/domain/scan/johansen_models.py`: `5`
- `src/domain/scan/johansen_universe.py`: `19`
- `src/domain/scan/optimizer_grid_scan.py`: `17`
- `src/domain/scan/unit_root.py`: `10`
- `src/domain/wfa_evaluation.py`: `4`
- `src/domain/wfa_genetic.py`: `3`
- `src/domain/wfa_request_runner.py`: `1`
- `src/domain/wfa_serialization.py`: `7`
- `src/domain/wfa_windowing.py`: `5`
- `src/mt5_gateway/catalog.py`: `4`
- `src/mt5_gateway/client.py`: `9`
- `src/mt5_gateway/ingestion.py`: `5`
- `src/mt5_gateway/models.py`: `2`
- `src/mt5_gateway/service.py`: `6`
- `src/storage/catalog.py`: `6`
- `src/storage/paths.py`: `9`
- `src/storage/portfolio_store.py`: `16`
- `src/storage/quotes.py`: `2`
- `src/storage/scan_results.py`: `22`
- `src/storage/scanner_results.py`: `34`
- `src/storage/wfa_results.py`: `13`
- `src/tools/export_instrument_reference.py`: `12`
- `src/tools/mt5_binary_export.py`: `1`
- `src/tools/mt5_export_catalog_sync.py`: `8`
- `src/tools/mt5_sync.py`: `3`
- `src/tools/mt5_terminal_export_sync.py`: `18`
- `src/tools/optimizer_clipboard_to_markdown.py`: `10`
- `src/workers/executor.py`: `4`
- `src/workers/job_models.py`: `2`

## Definitions By Module

### `src/app_config.py`

Count: `2`

- `Settings`
- `get_settings`

### `src/bokeh_app/adapters.py`

Count: `10`

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

### `src/bokeh_app/browser_state.py`

Count: `9`

- `BrowserStateBinding`
- `_serialize_default`
- `_storage_helpers`
- `_save_callback_code`
- `_options_restore_code`
- `_restore_assignment`
- `_save_assignment`
- `_needs_numeric_dom_persistence`
- `attach_browser_state`

### `src/bokeh_app/file_state.py`

Count: `20`

- `_serialize_value`
- `_deserialize_range_value`
- `_select_options`
- `_sanitize_spinner_value`
- `_binding_value`
- `_read_json`
- `_cleanup_temp_files`
- `_atomic_write_json`
- `FileStateController`
- `FileStateController.__post_init__`
- `FileStateController.read_state`
- `FileStateController.snapshot`
- `FileStateController.persist`
- `FileStateController.restore`
- `FileStateController.clear`
- `FileStateController.suspend`
- `FileStateController._restore_binding`
- `FileStateController.install_model_watchers`
- `FileStateController.install_model_watchers._handler`
- `FileStateController.install_model_watchers._options_handler`

### `src/bokeh_app/main.py`

Count: `320`

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
- `build_document.optimizer_help`
- `build_document.sync_price_plot_labels`
- `build_document.build_optimization_table_columns`
- `build_document.empty_scanner_table_data`
- `build_document._selected_symbols_for_export`
- `build_document._format_export_period`
- `build_document._format_manual_range`
- `build_document._tester_export_metadata`
- `build_document._optimization_export_metadata`
- `build_document._scan_export_metadata`
- `build_document._build_table_export_controls`
- `build_document._build_table_export_controls.on_export_click`
- `build_document.empty_wfa_table_data`
- `build_document.wfa_result_to_sources`
- `build_document._wfa_export_metadata`
- `build_document.empty_meta_selector_table_data`
- `build_document.empty_meta_ranking_table_data`
- `build_document.meta_selector_result_to_source`
- `build_document.meta_ranking_result_to_source`
- `build_document.meta_result_to_equity_source`
- `build_document.default_meta_model`
- `build_document.sync_meta_model_ui`
- `build_document._meta_export_metadata`
- `build_document.empty_portfolio_table_data`
- `build_document.empty_portfolio_equity_source_data`
- `build_document.portfolio_items_to_source`
- `build_document.empty_portfolio_weight_data`
- `build_document.empty_portfolio_correlation_data`
- `build_document.portfolio_weight_rows_to_source`
- `build_document.portfolio_correlation_rows_to_source`
- `build_document._normalize_log_message`
- `build_document.append_service_log`
- `build_document.build_service_log_handler`
- `build_document.build_service_log_handler._handler`
- `build_document.sync_service_log_toggle`
- `build_document.sync_section_toggle_states`
- `build_document.set_section_visibility`
- `build_document.build_section_toggle_handler`
- `build_document.build_section_toggle_handler._handler`
- `build_document.toggle_service_log`
- `build_document.clear_trade_highlights`
- `build_document.set_equity_summary_overlay`
- `build_document.queue_sync_equity_summary_overlay_position`
- `build_document._safe_plot_dimension`
- `build_document.sync_equity_summary_overlay_position`
- `build_document.on_equity_plot_geometry_change`
- `build_document.on_document_ready`
- `build_document.sync_equity_legend`
- `build_document.configured_plot_height`
- `build_document.apply_plot_display_settings`
- `build_document._axis_reference_series`
- `build_document._nearest_bar_x`
- `build_document.update_gapless_x_axis`
- `build_document.update_gapless_x_axis.reduce_positions`
- `build_document.sync_optimization_cutoff_marker`
- `build_document.sync_optimization_train_overlay`
- `build_document.clear_zscore_diagnostics_outputs`
- `build_document.apply_zscore_diagnostics`
- `build_document.apply_zscore_diagnostics_payload`
- `build_document.clear_backtest_outputs`
- `build_document.current_backtest_request`
- `build_document.run_tester_job`
- `build_document.apply_backtest_payload`
- `build_document.submit_tester_request`
- `build_document.poll_tester_future`
- `build_document.set_shared_x_range`
- `build_document.refresh_plot_ranges`
- `build_document.rebalance_layout`
- `build_document.ensure_nonempty_layout`
- `build_document.build_defaults`
- `build_document.sync_bybit_fee_mode`
- `build_document.build_distance_params`
- `build_document.set_tester_context`
- `build_document.current_pair`
- `build_document._context_datetime`
- `build_document._empty_scan_source_data`
- `build_document._scan_frame_to_source`
- `build_document._instrument_option_label`
- `build_document._symbol_label_map`
- `build_document._symbol_select_options`
- `build_document._preferred_symbol_1`
- `build_document._preferred_symbol_2`
- `build_document.instrument_options_for_group`
- `build_document.available_catalog_group_options`
- `build_document.sync_catalog_group_select_options`
- `build_document.current_download_terminal_path`
- `build_document.current_download_common_root`
- `build_document.sync_downloader_symbol_options`
- `build_document.sync_downloader_mode_ui`
- `build_document.sync_leg_2_filter_ui`
- `build_document._sync_co_mover_group_options`
- `build_document.current_tester_cointegration_scope`
- `build_document.current_scan_selection_scope`
- `build_document.current_scan_period`
- `build_document.current_johansen_scan_config`
- `build_document.scanner_cointegration_run_options`
- `build_document.sync_scanner_cointegration_run_options`
- `build_document.current_scanner_cointegration_snapshot`
- `build_document.current_scanner_pair_source_label`
- `build_document.scanner_allowed_pair_keys_for_scope`
- `build_document.exact_group_allowed_pair_keys`
- `build_document.merge_allowed_pair_key_filters`
- `build_document.scan_snapshot_pair_keys`
- `build_document.scan_snapshot_matches_exact_group_pairs`
- `build_document.current_scanner_selection_scope`
- `build_document.current_scanner_view_scope_label`
- `build_document.available_scanner_scope_labels`
- `build_document.selected_scanner_scope_labels`
- `build_document.scanner_target_grid_trials`
- `build_document.current_optimizer_oos_started_at`
- `build_document.current_scanner_train_period`
- `build_document.active_scanner_train_period`
- `build_document.active_scanner_oos_started_at`
- `build_document.scanner_search_space`
- `build_document.build_selected_scanner_scope_jobs`
- `build_document.current_scanner_request_signature`
- `build_document.copy_pair_to_selectors`
- `build_document.scanner_is_running`
- `build_document.scanner_busy_message`
- `build_document.running_heavy_job_label`
- `build_document.load_tester_cointegration_snapshot`
- `build_document.restore_saved_scan_table`
- `build_document.restore_saved_scanner_table`
- `build_document.sync_symbol_2_filter`
- `build_document._auto_axis_values`
- `build_document._product_value`
- `build_document._build_auto_search_space`
- `build_document._auto_grid_point_counts`
- `build_document._auto_optimization_search_space`
- `build_document.optimization_search_space`
- `build_document.genetic_optimizer_config`
- `build_document.set_optimizer_control_visible`
- `build_document.install_optimizer_tooltips`
- `build_document.sync_stop_mode_ui`
- `build_document.sync_optimization_mode_ui`
- `build_document.reset_optimization_button`
- `build_document.mark_optimization_running`
- `build_document.reset_run_button`
- `build_document.mark_test_running`
- `build_document.clear_tester_poll_callback`
- `build_document.update_optimization_progress`
- `build_document.render_optimization_progress`
- `build_document._serialize_optimization_signature_value`
- `build_document.optimization_signature`
- `build_document.optimization_signature_matches`
- `build_document.optimization_outputs_present`
- `build_document.copy_optimization_trial_to_tester`
- `build_document.apply_optimization_trial`
- `build_document.render_optimization_trial_in_tester`
- `build_document.current_optimization_signature`
- `build_document.optimization_signature_changes`
- `build_document.on_optimization_config_change`
- `build_document.reset_scan_button`
- `build_document.mark_scan_running`
- `build_document.update_scan_progress`
- `build_document.update_scan_partial_result`
- `build_document.selected_scan_row_key`
- `build_document.scan_row_index_by_key`
- `build_document.apply_scan_snapshot`
- `build_document.render_scan_progress`
- `build_document.reset_scanner_button`
- `build_document.mark_scanner_running`
- `build_document.update_scanner_progress`
- `build_document.update_scanner_partial_result`
- `build_document.selected_scanner_row_key`
- `build_document.scanner_row_index_by_key`
- `build_document.apply_scanner_snapshot`
- `build_document.render_scanner_progress`
- `build_document.persist_scanner_result_snapshot`
- `build_document.estimate_scanner_eta`
- `build_document.reset_download_button`
- `build_document.mark_download_running`
- `build_document.update_download_progress`
- `build_document.render_download_progress`
- `build_document.reset_wfa_button`
- `build_document.mark_wfa_running`
- `build_document.update_wfa_progress`
- `build_document.update_wfa_partial_result`
- `build_document.sync_wfa_selection_highlight`
- `build_document.sync_wfa_outputs_visibility`
- `build_document.refresh_wfa_equity_ranges`
- `build_document.refresh_meta_equity_ranges`
- `build_document.active_portfolio_item_ids`
- `build_document.portfolio_allocation_capitals_by_id`
- `build_document.portfolio_display_allocation_capitals`
- `build_document.selected_portfolio_item_id`
- `build_document.build_portfolio_source_data`
- `build_document.update_portfolio_metrics_summary`
- `build_document.build_portfolio_curve_source_data`
- `build_document.portfolio_curve_source_data`
- `build_document.set_portfolio_x_range`
- `build_document.render_portfolio_equity_curve`
- `build_document.render_portfolio_combined_equity`
- `build_document.sync_portfolio_equity_view`
- `build_document.clear_portfolio_equity_outputs`
- `build_document.reset_portfolio_button`
- `build_document.mark_portfolio_running`
- `build_document.clear_portfolio_analysis_outputs`
- `build_document.refresh_portfolio_equity_ranges`
- `build_document.sync_portfolio_oos_cutoff_marker`
- `build_document.clear_portfolio_poll_callback`
- `build_document.run_portfolio_backtests`
- `build_document.run_portfolio_job`
- `build_document.refresh_portfolio_analysis`
- `build_document.refresh_portfolio_table`
- `build_document.on_add_to_portfolio`
- `build_document.on_reload_portfolio`
- `build_document.on_remove_selected_portfolio_items`
- `build_document.on_analyze_portfolio`
- `build_document.poll_portfolio_future`
- `build_document.on_run_portfolio`
- `build_document.on_portfolio_period_change`
- `build_document.on_portfolio_allocation_change`
- `build_document.on_portfolio_selection`
- `build_document.on_portfolio_table_action`
- `build_document._coerce_meta_datetime`
- `build_document.sync_meta_oos_cutoff_marker`
- `build_document.clear_meta_selection_highlight`
- `build_document.sync_meta_selection_highlight`
- `build_document.read_meta_oos_started_at`
- `build_document.same_meta_oos_started_at`
- `build_document.build_meta_model_config`
- `build_document.clear_meta_outputs`
- `build_document.meta_outputs_present`
- `build_document.meta_result_signature`
- `build_document.can_keep_displayed_meta_result`
- `build_document.restore_saved_meta_result`
- `build_document.restore_saved_wfa_result`
- `build_document.apply_wfa_snapshot`
- `build_document.render_wfa_progress`
- `build_document.complete_wfa`
- `build_document.run_wfa_job`
- `build_document.clear_wfa_poll_callback`
- `build_document.poll_wfa_future`
- `build_document.on_run_wfa`
- `build_document.reset_meta_button`
- `build_document.mark_meta_running`
- `build_document.complete_meta`
- `build_document.run_meta_job`
- `build_document.clear_meta_poll_callback`
- `build_document.poll_meta_future`
- `build_document.on_run_meta`
- `build_document.on_meta_selection`
- `build_document.on_meta_ranking_selection`
- `build_document.complete_optimization`
- `build_document.apply_backtest_result`
- `build_document.begin_symbol_change_refresh_suppression`
- `build_document.end_symbol_change_refresh_suppression`
- `build_document.maybe_refresh_price_plots_for_symbol_change`
- `build_document.refresh_instruments`
- `build_document.on_group_change`
- `build_document.on_symbol_1_change`
- `build_document.on_symbol_2_change`
- `build_document.on_leg2_filter_change`
- `build_document.on_leg2_cointegration_kind_change`
- `build_document.on_co_mover_group_change`
- `build_document.on_timeframe_change`
- `build_document.on_scan_universe_change`
- `build_document.on_scanner_universe_change`
- `build_document.on_scan_kind_change`
- `build_document.on_download_group_change`
- `build_document.on_download_scope_change`
- `build_document.on_wfa_config_change`
- `build_document.on_bybit_fee_mode_change`
- `build_document.on_optimization_mode_change`
- `build_document.on_stop_mode_change`
- `build_document.on_opt_stop_mode_change`
- `build_document.on_optimization_range_mode_change`
- `build_document.on_display_settings_change`
- `build_document.on_meta_config_change`
- `build_document.on_section_visibility_change`
- `build_document.on_range_change`
- `build_document.on_portfolio_range_change`
- `build_document.on_trade_selection`
- `build_document.on_wfa_selection`
- `build_document.on_optimization_selection`
- `build_document.on_scan_selection`
- `build_document.on_scanner_selection`
- `build_document.on_run_test`
- `build_document.run_optimization_job`
- `build_document.clear_optimization_poll_callback`
- `build_document.poll_optimization_future`
- `build_document.on_run_optimization`
- `build_document.run_scan_job`
- `build_document.run_scan_job.persist_partial_scan_snapshot`
- `build_document.run_scan_job.handle_partial_scan_result`
- `build_document.run_scanner_job`
- `build_document.run_scanner_job.combined_result`
- `build_document.run_scanner_job.scoped_progress`
- `build_document.run_scanner_job.handle_partial_result`
- `build_document.clear_scan_poll_callback`
- `build_document.clear_scanner_poll_callback`
- `build_document.poll_scan_future`
- `build_document.poll_scanner_future`
- `build_document.on_run_scan`
- `build_document.on_run_scanner`
- `build_document.run_download_job`
- `build_document.clear_download_poll_callback`
- `build_document.poll_download_future`
- `build_document.on_run_download`
- `build_document.on_reset_defaults`
- `build_document.on_session_destroyed`
- `build_document.sync_meta_objective_with_wfa`

### `src/bokeh_app/numeric_inputs.py`

Count: `3`

- `fractional_step_decimals`
- `has_fractional_step`
- `normalize_fractional_value`

### `src/bokeh_app/scanner_estimate.py`

Count: `2`

- `scanner_pair_count`
- `estimate_scanner_runtime_seconds`

### `src/bokeh_app/state.py`

Count: `11`

- `_empty_price_source`
- `_empty_spread_source`
- `_empty_zscore_source`
- `_empty_equity_source`
- `_empty_trades_source`
- `_empty_trade_markers_source`
- `_empty_selected_trade_markers_source`
- `_empty_trade_segments_source`
- `_empty_optimization_source`
- `_empty_scan_source`
- `AppState`

### `src/bokeh_app/table_export.py`

Count: `16`

- `sanitize_filename_part`
- `build_table_export_path`
- `_column_letter`
- `_stringify_cell`
- `_cell_xml`
- `_sheet_xml`
- `_rels_xml`
- `_workbook_xml`
- `_workbook_rels_xml`
- `_content_types_xml`
- `_core_properties_xml`
- `_app_properties_xml`
- `_sheet_name`
- `data_table_to_rows`
- `export_table_to_xlsx`
- `metadata_rows_from_mapping`

### `src/bokeh_app/view_utils.py`

Count: `11`

- `display_symbol_label`
- `_measure_overlay_label_widths`
- `compute_overlay_label_layout`
- `compute_overlay_label_layout._positions`
- `compute_overlay_label_layout._fits_height`
- `coerce_datetime_ms`
- `_collect_window_values`
- `compute_series_bounds`
- `compute_plot_height`
- `compute_relative_plot_height`
- `sync_toggle_button_types`

### `src/bokeh_app/zscore_diagnostics.py`

Count: `9`

- `empty_zscore_metric_source`
- `empty_zscore_hist_source`
- `ZScoreDiagnosticsPayload`
- `_safe_percentile`
- `_safe_skew`
- `_safe_excess_kurtosis`
- `_fmt_number`
- `_fmt_share`
- `build_zscore_diagnostics`

### `src/core_api/main.py`

Count: `2`

- `healthcheck`
- `service_meta`

### `src/core_api/routes_optimizer.py`

Count: `4`

- `optimization_health`
- `_serialize_result`
- `optimize_distance_grid_route`
- `optimize_distance_genetic_route`

### `src/core_api/routes_quotes.py`

Count: `3`

- `health_check`
- `trigger_symbol_sync`
- `trigger_quote_sync`

### `src/core_api/routes_scan.py`

Count: `3`

- `health_check`
- `run_johansen_pair_scan`
- `run_johansen_batch_scan`

### `src/core_api/routes_wfa.py`

Count: `2`

- `health_check`
- `trigger_wfa`

### `src/domain/backtest/distance_engine.py`

Count: `15`

- `_suffix_quotes`
- `load_pair_frame`
- `_DistanceSignalState`
- `_DistanceBacktestContext`
- `_column_or_zeros`
- `prepare_distance_backtest_context`
- `_signal_state`
- `_signal_exit_reason`
- `_build_summary`
- `_empty_result`
- `_empty_metrics`
- `_finalize_metrics`
- `run_distance_backtest_metrics_frame`
- `run_distance_backtest_frame`
- `run_distance_backtest`

### `src/domain/backtest/distance_models.py`

Count: `4`

- `DistanceParameters`
- `DistanceBacktestResult`
- `_LegSpec`
- `_Position`

### `src/domain/backtest/distance_pricing.py`

Count: `9`

- `coerce_leg_spec`
- `normalize_volume`
- `adverse_slippage_offset`
- `buy_spread_offset`
- `price_to_account_pnl`
- `notional_value`
- `margin_basis_per_lot`
- `commission_for_fill`
- `price_with_costs`

### `src/domain/backtest/kernel.py`

Count: `6`

- `_NumbaFallback`
- `_NumbaFallback.njit`
- `_NumbaFallback.njit.decorator`
- `compute_spread`
- `compute_drawdown`
- `rolling_mean_std`

### `src/domain/backtest/metric_formulas.py`

Count: `7`

- `clamp_metric_value`
- `safe_ratio`
- `compute_k_ratio`
- `compute_r_squared`
- `duration_years_from_times`
- `compute_cagr`
- `compute_equity_curve_metrics`

### `src/domain/contracts.py`

Count: `24`

- `Algorithm`
- `OptimizationMode`
- `ScanUniverseMode`
- `Timeframe`
- `NormalizedGroup`
- `WfaMode`
- `WfaPairMode`
- `WfaSelectionSource`
- `WfaWindowUnit`
- `UnitRootTest`
- `StrategyDefaults`
- `PairSelection`
- `IntegerRange`
- `WfaWindowSearchSpace`
- `WfaWindowSearchSpace.resolved_train_unit`
- `WfaWindowSearchSpace.resolved_validation_unit`
- `WfaWindowSearchSpace.resolved_test_unit`
- `WfaWindowSearchSpace.resolved_walk_step_unit`
- `UnitRootGate`
- `BacktestRequest`
- `OptimizationRequest`
- `WfaRequest`
- `CointegrationScanRequest`
- `JohansenPairScanRequest`

### `src/domain/costs/profiles.py`

Count: `10`

- `CommissionProfile`
- `commission_overrides_path`
- `read_commission_overrides`
- `merge_commission_override`
- `_normalized_text`
- `_has_explicit_commission`
- `_bybit_fee_mode`
- `_bybit_is_precious_metal`
- `_bybit_tight_spread_profile`
- `apply_broker_commission_fallback`

### `src/domain/data/catalog_groups.py`

Count: `12`

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

### `src/domain/data/co_movers.py`

Count: `6`

- `CoMoverGroup`
- `CoMoverGroup.label`
- `_available_symbol_set`
- `co_mover_groups_for_symbol`
- `co_mover_group_labels_for_symbol`
- `co_mover_symbols_for_symbol`

### `src/domain/data/instrument_groups.py`

Count: `1`

- `normalize_group`

### `src/domain/data/io.py`

Count: `13`

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

### `src/domain/data/resample.py`

Count: `1`

- `resample_m5_quotes`

### `src/domain/data/timeframes.py`

Count: `1`

- `to_polars_every`

### `src/domain/meta_selector.py`

Count: `19`

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

### `src/domain/meta_selector_ml.py`

Count: `16`

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

### `src/domain/meta_selector_outputs.py`

Count: `3`

- `stitch_equity_chunks`
- `max_drawdown`
- `build_selected_fold_outputs`

### `src/domain/meta_selector_types.py`

Count: `2`

- `MetaSelectorResult`
- `MetaSelectorResult.to_dict`

### `src/domain/optimizer/distance.py`

Count: `13`

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

### `src/domain/optimizer/distance_genetic_core.py`

Count: `6`

- `_execution_signature`
- `random_candidate`
- `mutate_candidate`
- `crossover_candidates`
- `tournament_select`
- `evaluate_candidates_into_cache`

### `src/domain/optimizer/distance_grid.py`

Count: `17`

- `DistanceOptimizationTrial`
- `DistanceOptimizationResult`
- `DistanceOptimizationResult.best_trial`
- `DistanceOptimizationResult.to_dict`
- `_coerce_numeric_list`
- `normalize_distance_search_space`
- `_safe_returns`
- `_omega_ratio`
- `_ulcer_index`
- `_ulcer_performance_index`
- `_k_ratio`
- `_objective_value`
- `_sanitize_objective`
- `_finite_metric`
- `_trial_metrics`
- `run_distance_grid_search_frame`
- `run_distance_grid_search`

### `src/domain/optimizer/distance_metrics.py`

Count: `5`

- `equity_metrics`
- `objective_score`
- `validate_objective_metric`
- `sort_rows`
- `params_from_candidate`

### `src/domain/optimizer/distance_models.py`

Count: `9`

- `DistanceGridSearchSpace`
- `DistanceGeneticConfig`
- `DistanceOptimizationRow`
- `DistanceOptimizationResult`
- `_grid_values`
- `_optional_stop_values`
- `_fixed_float_value`
- `parse_distance_search_space`
- `parse_distance_genetic_config`

### `src/domain/optimizer/distance_parallel.py`

Count: `13`

- `is_cancelled`
- `emit_progress`
- `_chunk_size`
- `_chunk_tasks`
- `resolve_worker_count`
- `_collect_parallel_results`
- `_collect_parallel_results.submit_available`
- `_evaluate_params_chunk`
- `_evaluate_candidate_chunk`
- `evaluate_distance_tasks`
- `evaluate_distance_tasks.submit_chunk`
- `evaluate_candidate_distance_tasks`
- `evaluate_candidate_distance_tasks.submit_chunk`

### `src/domain/portfolio.py`

Count: `19`

- `PortfolioRunRow`
- `PortfolioCurve`
- `PortfolioEquitySummary`
- `PortfolioCorrelationRow`
- `PortfolioAllocationSuggestionRow`
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

### `src/domain/scan/johansen_core.py`

Count: `12`

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

### `src/domain/scan/johansen_models.py`

Count: `5`

- `JohansenScanParameters`
- `JohansenPairScanResult`
- `JohansenUniverseScanRow`
- `JohansenUniverseScanSummary`
- `JohansenUniverseScanResult`

### `src/domain/scan/johansen_universe.py`

Count: `19`

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
- `scan_symbol_frames_johansen.submit_symbol_chunks`
- `scan_symbol_frames_johansen.iter_pair_payloads`
- `scan_symbol_frames_johansen.submit_pair_chunks`
- `scan_universe_johansen`

### `src/domain/scan/optimizer_grid_scan.py`

Count: `17`

- `OptimizerGridScanRow`
- `OptimizerGridScanSummary`
- `OptimizerGridScanResult`
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
- `scan_symbol_frames_optimizer_grid.pair_progress`
- `scan_universe_optimizer_grid`

### `src/domain/scan/unit_root.py`

Count: `10`

- `UnitRootScreenResult`
- `PairUnitRootScreenResult`
- `_clean_series`
- `_is_constant`
- `_difference`
- `_run_adf`
- `_run_kpss`
- `_evaluate_gate`
- `screen_series_for_cointegration`
- `screen_pair_for_cointegration`

### `src/domain/wfa_evaluation.py`

Count: `4`

- `distance_params_from_payload`
- `candidate_params`
- `evaluate_distance_params`
- `run_pair_window_trial`

### `src/domain/wfa_genetic.py`

Count: `3`

- `_empty_wfa_result`
- `_is_cancelled`
- `run_distance_genetic_wfa`

### `src/domain/wfa_request_runner.py`

Count: `1`

- `run_wfa_request`

### `src/domain/wfa_serialization.py`

Count: `7`

- `slice_frame`
- `serialize_time`
- `serialize_pair`
- `serialize_optimization_row`
- `build_fold_history_rows`
- `stitch_pair_oos_equity`
- `combine_pair_equity_series`

### `src/domain/wfa_windowing.py`

Count: `5`

- `WalkWindow`
- `add_months`
- `advance`
- `build_walk_windows`
- `build_train_test_windows`

### `src/mt5_gateway/catalog.py`

Count: `4`

- `CatalogSyncService`
- `CatalogSyncService.__init__`
- `CatalogSyncService.sync`
- `CatalogSyncService.refresh_universe`

### `src/mt5_gateway/client.py`

Count: `9`

- `MT5Client`
- `MT5Client.__init__`
- `MT5Client._module`
- `MT5Client.initialize`
- `MT5Client.shutdown`
- `MT5Client._extract_commission`
- `MT5Client.fetch_instruments`
- `MT5Client.fetch_m5_quotes`
- `MT5Client.fetch_latest_tick`

### `src/mt5_gateway/ingestion.py`

Count: `5`

- `QuoteIngestionService`
- `QuoteIngestionService.__init__`
- `QuoteIngestionService.download_m5`
- `QuoteIngestionService.download_bulk`
- `QuoteIngestionService.ensure_tf`

### `src/mt5_gateway/models.py`

Count: `2`

- `InstrumentInfo`
- `TickSnapshot`

### `src/mt5_gateway/service.py`

Count: `6`

- `QuoteDownloadRequest`
- `MT5GatewayService`
- `MT5GatewayService.__init__`
- `MT5GatewayService.refresh_instruments`
- `MT5GatewayService.download_quotes`
- `MT5GatewayService.stream_latest_ticks`

### `src/storage/catalog.py`

Count: `6`

- `catalog_path`
- `instrument_catalog_path`
- `_empty_catalog_frame`
- `_normalize_catalog`
- `write_instrument_catalog`
- `read_instrument_catalog`

### `src/storage/paths.py`

Count: `9`

- `raw_quotes_root`
- `derived_quotes_root`
- `catalog_root`
- `scans_root`
- `scanner_root`
- `ui_state_path`
- `wfa_root`
- `meta_selector_root`
- `portfolio_root`

### `src/storage/portfolio_store.py`

Count: `16`

- `PortfolioItem`
- `PortfolioItem.params`
- `PortfolioItem.defaults`
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

### `src/storage/quotes.py`

Count: `2`

- `raw_partition_path`
- `write_m5_quotes`

### `src/storage/scan_results.py`

Count: `22`

- `SavedScanSnapshot`
- `SavedScanRunOption`
- `_safe_component`
- `_empty_rows_frame`
- `_rows_to_frame`
- `_scope_value`
- `_kind_root`
- `_run_directory`
- `_latest_scope_directory`
- `_legacy_latest_directory`
- `_summary_matches_request`
- `_read_summary`
- `_snapshot_paths_from_summary`
- `_parse_datetime`
- `_load_snapshot_from_paths`
- `_find_latest_run_snapshot`
- `load_latest_saved_scan_result`
- `list_saved_scan_runs`
- `load_saved_scan_result_by_summary_path`
- `partner_symbols_from_snapshot`
- `persist_johansen_scan_result`
- `snapshot_to_johansen_result`

### `src/storage/scanner_results.py`

Count: `34`

- `ScannerPairProgressRow`
- `OptimizerScannerSnapshot`
- `LoadedOptimizerScannerSnapshot`
- `scanner_results_path`
- `_normalize_scope`
- `_normalize_signature`
- `_normalize_datetime`
- `_normalize_enum_value`
- `_empty_store_frame`
- `_frame_from_records`
- `_load_store_frame`
- `_write_store_frame`
- `_row_value`
- `_optimization_row_to_record`
- `_coerce_progress_rows`
- `_result_row_to_record`
- `_progress_row_to_record`
- `_sort_frame`
- `_summary_from_frame`
- `_summary_from_frame._sum_latest`
- `persist_optimizer_scanner_scope_snapshot`
- `_load_snapshot`
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

### `src/storage/wfa_results.py`

Count: `13`

- `_sanitize_symbol`
- `_pair_key`
- `_serialize_time`
- `_compact_time`
- `_objective_suffix`
- `wfa_pair_history_dir`
- `wfa_pair_history_path`
- `wfa_run_snapshot_dir`
- `wfa_run_snapshot_path`
- `load_wfa_optimization_history`
- `persist_wfa_optimization_history`
- `persist_wfa_run_snapshot`
- `load_wfa_run_snapshot`

### `src/tools/export_instrument_reference.py`

Count: `12`

- `_clean_text`
- `_symbol_no_suffix`
- `infer_meaning`
- `human_family`
- `inventory_frame`
- `catalog_anomalies`
- `available_clusters`
- `_render_cluster_table`
- `_render_inventory_section`
- `render_markdown`
- `write_outputs`
- `main`

### `src/tools/mt5_binary_export.py`

Count: `1`

- `read_codex_binary`

### `src/tools/mt5_export_catalog_sync.py`

Count: `8`

- `_parse_dt`
- `month_partitions_between`
- `chunked`
- `symbol_partitions_exist`
- `resolve_symbols`
- `build_jobs`
- `build_parser`
- `main`

### `src/tools/mt5_sync.py`

Count: `3`

- `_parse_dt`
- `build_parser`
- `main`

### `src/tools/mt5_terminal_export_sync.py`

Count: `18`

- `ExportJob`
- `ExportStatus`
- `ExportStatus.ok`
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

### `src/tools/optimizer_clipboard_to_markdown.py`

Count: `10`

- `_normalize_header`
- `_canonical_columns_for_width`
- `_all_known_headers`
- `_coerce_header_row`
- `_parse_tsv`
- `_parse_vertical_blocks`
- `parse_optimizer_clipboard`
- `render_markdown_table`
- `_read_text`
- `main`

### `src/workers/executor.py`

Count: `4`

- `_apply_cpu_worker_env_caps`
- `build_process_pool`
- `shared_process_pool`
- `shutdown_executor`

### `src/workers/job_models.py`

Count: `2`

- `JobType`
- `JobStatus`

