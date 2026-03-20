### Bokeh UI Blueprint

**Overview**
- The application page splits into a fixed-width left sidebar and a fluid right workspace. Sidebar holds instrument selection, test/optimizer controls, and data utilities while the right pane dedicates space to synchronized charts plus optional panels (optimizer settings, optimization results, cointegration scan, trades table, and the new WFA block).
- Use Bokeh Server to serve a single document with session-specific state so controls, charts, and tables stay aligned across the user session.

**Component tree**
1. Document owns session_state plus ColumnDataSource objects for price1, price2, spread, zscore, equity, trades, optimizer_results, scan_results, and a `wfa_chunk_source` that feeds the WFA table/chart block.
2. Sidebar (column layout) combines grouped Div headings, Select widgets for primary/secondary symbols, timeframe, group filters, algorithm selector, and ROI controls (capital, leverage, margin per leg). Buttons include Refresh Instruments, Download History, Run Test, Start Optimization, and Launch WFA.
3. Chart Panel (row/column stack) uses a gridplot-style layout with sizing_mode="stretch_both". Each figure shares the same Range1d x_range; optional toggles (CheckboxGroup or Toggle) show/hide charts while the layout recomputes children so visible figures expand to fill available height.
4. Optional Blocks appear below core charts when enabled. Each block (optimizer settings, optimization results, cointegration scan, trades table, WFA control/results) is a collapsible card implemented with Div wrappers and visibility flags; hiding a block returns its height back to the chart stack.

**Document/session state**
- Store session_state dict with `selected_algorithm`, `visible_charts`, `visible_panels`, `current_trial_params`, `cointegration_filter`, and the WFA configuration payload (`wfa_mode`, `wfa_windows`, `wfa_selection`).
- ColumnDataSources track raw bars, trades, optimizer results, scan results, and WFA chunks. Each source carries metadata columns (e.g., leg, side, marker_id) so glyph callbacks can highlight trades or cross-reference rows with charts.
- CustomJS callbacks are limited to UI polish (tooltips, highlight toggles); heavy computations run inside FastAPI callbacks or background workers, and updates arrive through ColumnDataSource patches.

**Widget interactions**
- Changing Select widgets triggers a server callback that resamples data, reruns the single-pair backtest, and updates every chart source before recalculating y_range bounds for the visible figures.
- Switching the algorithm updates the parameter controls shown in the sidebar and notifies the optimizer block to refresh its search space; `session_state["selected_algorithm"]` tracks the choice.
- Chart visibility toggles reorganize the chart stack by rewriting `layout.children` so charts maintain their shared x_range without re-rendering the entire document.
- Button clicks (Run Test, Start Optimization, Johansen Scan) enqueue the respective jobs via the API; the frontend polls for updates and patches ColumnDataSources when results arrive.
- The WFA block exposes an anchored/rolling mode toggle plus row controls for Train, Validation, Test window lengths and step size. Launching WFA from the tester menu or optimizer table copies the selected pair(s) and relevant algorithm parameters into the WFA form, preserving the tester period. A "Run WFA" button triggers the background job and streams chunked updates into `wfa_chunk_source`.
- WFA rows are clickable: selecting one copies that window back into the tester controls without changing the base test period and highlights the associated span on all charts. Clicking a chart marker can highlight matching WFA rows via shared timestamps.

**Synced x-range and auto y-range strategy**
- All figures share the same x_range object; attach a `.js_on_change("bounds")` listener that schedules a Python callback to recompute each visible figure's y_range to the min/max of the subset currently in view.
- Use DataRange1d for y_range but override its `_compute_bounds` logic so autoscale stays responsive without overshooting; recalculations occur only when zoom or pan completes (debounced with `curdoc().add_timeout_callback`).
- When a chart hides/unhides, reset its y_range on the full dataset before recomputing the visible span so the axes remain stable.

**Table-click-to-chart / chart-click-to-table**
- Optimization and scan DataTables propagate selection events into the tester form via Python callbacks. Selecting an optimizer row copies its params into the tester, leaves the test period alone, and optionally triggers a new backtest run.
- WFA table rows copy their window configuration back into the main tester, highlight the matching trades on price charts, and surface per-window metrics, keeping the channel between table and charts bidirectional.
- Trades are cross-linked: selecting a trade row highlights entry/exit markers on both legs and vice versa using `.selected.indices` and shared timestamp fields.

**Responsive layout approach**
- The root layout is `row(sidebar, charts_and_panels, sizing_mode="stretch_both")`. Sidebar width stays at 340px but scrolls vertically; the chart column uses `sizing_mode="stretch_both"` plus padding to maintain breathable spacing.
- Inject CSS variables through a top-level Div (for example `:root { --main-bg:#0f172a; --gradient:#030022; --accent:#0ab2ff; }`) to define colors and typography. Apply a subtle gradient background and soft shadows to charts for depth.
- Figures use `min_border=10`, `min_border_left=50`, and hide non-essential toolbar icons. Custom crosshairs and hover tools remain active, and portrait/landscape resizes trigger `layout.request_render()` so elements resize smoothly.

**Risk points**
1. Numba/Numexpr code must run outside Bokeh callbacks; heavy jobs go through background workers and publish results via ColumnDataSource patches.
2. Shared x_range needs a sentinel owner; removing charts without preserving the shared Range1d causes JS errors.
3. Async job updates can cause race conditions; guard ColumnDataSource writes with `doc.add_next_tick_callback`.
4. Sorting tables client-side becomes slow if results exceed a few thousand rows; keep displays paginated or sort server-side.
5. WFA jobs process large windows; disable the Run WFA button while a job is inflight and chunk updates before pushing into the session to avoid flooding the document.

This note should guide the Bokeh layout so the left-side logic, right-side charts, and new WFA experience stay tightly coordinated.


**WFA Aggregate Equity**
- When WFA runs on multiple selected pairs, render an additional stitched summed out-of-sample equity chart for the selected pair basket.
- Keep the aggregate WFA equity synchronized on X with other WFA charts and tables so selecting a walk-forward row highlights the same time segment in the combined curve.
- Show both the combined basket equity and per-pair WFA result rows; clicking a pair row can optionally overlay only that pair on the aggregate chart for comparison.
