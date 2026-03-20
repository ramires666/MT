### Bokeh UI Blueprint

**Overview**
- The application page is split into a fixed-width left sidebar and a fluid right workspace. Sidebar contains instrument selection, test/optimizer controls, and data utilities. Right pane holds chart grid plus optional panels (optimizer settings, optimization table, cointegration scan, trades table).
- Use Bokeh Server to serve a single document with session-specific state; avoid copying UI logic into multiple documents.

**Component tree**
1. Document owns session_state and ColumnDataSource objects for price bars, spread, equity, trades, optimizer table, scan results.
2. Sidebar (column layout) includes grouped Div headings, Select widgets for primary/secondary symbols, timeframes, group filters, algorithm node, and ROI controls (capital, leverage, margin per leg). Add Buttons for Refresh Instruments, Download History, and Run Test.
3. Chart Panel (ow / column stack) uses gridplot-style layout that expands to fill remaining width with sizing_mode="stretch_both". Each chart is a igure with shared x_range (Range1d). Optional toggles (CheckboxGroup or Toggle) show/hide charts and panels dynamically.
4. Optional Blocks appear below core charts when enabled. Each block (optimizer settings, optimization results table, cointegration scan, trades table) is a collapsible panel implemented via Card-like Div wrappers with isible flag; hiding a block shrinks the right workspace so charts regain space.

**Document/session state**
- Store session_state dict with selected_algorithm, isible_charts, isible_panels, current_trial_params, and cointegration_filter. Persist Range1d references so x_range remains shared among charts and can be reset when a new dataset loads.
- ColumnDataSources: price1, price2, spread, zscore, equity, 	rades, optimizer_results, scan_results. Each source includes metadata columns (e.g., leg, side, marker_id).
- Use CustomJS callbacks sparingly; prefer Python callbacks attached to Bokeh server to keep GPU/Numba computations off the client.

**Widget interactions**
- Changing Select widgets triggers a server callback that loads or resamples data, updates all chart sources, and resets autoscale for each visible figure.
- Algorithm switch updates the visible parameter controls and notifies the optimizer block to show/hide relevant fields. The same callback updates session_state["selected_algorithm"].
- CheckboxGroup toggles adjust isible_charts; each toggle callback recomputes the layout by rearranging the chart stack (insert empty Spacers when fewer charts). Use layout.children updates rather than re-rendering the entire document.
- Button clicks (Run Test, Start Optimization, Johansen Scan) enqueue server jobs (via API) and return a job ID; results are polled via periodic callback updating the associated tables and enabling Run Test once data arrives.

**Synced x-range and auto y-range strategy**
- Every time a new dataset is loaded or the user zooms/pans, set all figures to share the same x_range object. Attach a .js_on_change("bounds") listener to the shared x_range that triggers a Python callback customizing each chart's y_range.start/y_range.end to fit the visible portion (computing min/max from the current view window via the ColumnDataSource data subset).
- Use DataRange1d for y_range but override compute_bounds to avoid overshooting; recalculations should happen only when the user releases the pan/zoom (debounced via curdoc().add_timeout_callback). When charts hide/unhide, recompute y_range on the full dataset.

**Table-click-to-chart / chart-click-to-table**
- optimizer_results and scan_results tables (DataTable) are clickable. Add JS callback that sets a session_state["pending_params"] and triggers a Python handler that copies those params into the tester form (without altering test period). Then call the test runner API.
- Rows also highlight linked trades by setting a boolean column (highlighted) that toggles glyph color on the trade table. Conversely, tapping a trade glyph on a price chart sets the corresponding row selected state in the trades table via CustomJS referencing the shared source and .selected.indices list.
- Use a shared # timestamp field on trade markers to correlate the table with chart data for consistent linkage.

**Responsive layout approach**
- The top-level layout is ow(sidebar, charts_and_panels, sizing_mode="stretch_both"). Sidebar has width=340 fixed but scrollable vertically. The chart column uses sizing_mode="stretch_both" and internal Spacers to keep consistent padding.
- Use CSS variables injected via Div (e.g., :root { --main-bg:#0f172a; --accent:#09b0ff; }) to establish color/typography choices beyond defaults. Define a gradient background for the chart area to avoid flat monotony.
- Figure objects adopt min_border=10, min_border_left=50, and custom Toolbar icons hidden except zoom/pan/reset. Autoscale button resets x_range to the full domain and recomputes y_range. When browser size changes, Bokeh's sizing mode ensures charts expand/shrink; run a Resize observer that triggers layout.request_render() for smoother transitions.

**Risk points**
1. Numba/Numexpr backend cannot run inside Bokeh callbacks—move heavy computations to background jobs and feed results into the document via ColumnDataSource updates.
2. Shared x_range requires careful cleanup; if charts are removed, the shared object must still exist to prevent JS errors. Keep a sentinel figure to own the shared range.
3. Async job updates can race; protect ColumnDataSource writes with locks or doc.add_next_tick_callback to prevent partial renders.
4. Sorting tables client-side can degrade once results exceed a few thousand rows; limit displays or page results server-side.

This note should be referenced when implementing Bokeh templates so the left-side logic and right-side charts stay tightly coordinated.
