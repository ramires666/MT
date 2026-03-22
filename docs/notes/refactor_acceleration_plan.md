# Refactor Acceleration Plan

## Goal

Radically reduce WFA and distance-optimizer runtime on CPU without moving to CUDA yet.

Primary targets:

- repeated genetic/grid optimization on the same pair and window
- repeated runs with different `objective_metric`
- repeated WFA folds across the same base history
- keeping current API and result format as stable as possible

## Current Bottlenecks

1. Genetic search itself is cheap; almost all time is spent inside repeated backtest evaluation.
2. The optimizer currently builds full `Polars` result frames and trade tables for every candidate, even when only metrics are needed.
3. Rolling mean/std is computed with a naive `O(n * lookback)` loop.
4. The trade simulation loop is Python-heavy and redoes the same per-window preprocessing for many parameter sets.
5. WFA repeatedly evaluates train/validation/test windows with the same underlying history slices.
6. `bollinger_k` is present in the search space but does not affect entry or exit decisions in the current strategy logic.
7. Parallel execution creates fresh process pools for batches and reserializes the same frame/spec payloads repeatedly.

## Execution Plan

### Stage 1: Quick Wins

- Remove redundant work from optimizer hot paths.
- Stop paying for full `frame/trades` construction during optimization.
- Collapse or neutralize search dimensions that do not affect trading behavior.
- Fix metric/reporting mismatches in WFA output.

Expected result:

- immediate speedup with low implementation risk

### Stage 2: CPU Fast Path

- Convert input data once from `Polars` to `numpy` arrays.
- Add a metrics-only backtest path for optimizer/WFA validation.
- Reuse cached per-window preprocessing for all parameter sets in the same job.
- Move rolling statistics to `numba`.

Expected result:

- large speedup for grid, genetic, and WFA without changing external workflows

### Stage 3: Job-Level Caching

- Cache precomputed window data per `(pair, timeframe, date range)`.
- Cache parameter evaluation results independently from `objective_metric`.
- Re-rank cached metrics instead of rerunning backtests when only the objective changes.

Expected result:

- major acceleration for repeated research loops with different targets

### Stage 4: WFA-Specific Cost Reduction

- Use fast metric evaluation for train and validation folds.
- Keep full backtest artifacts only for selected out-of-sample folds.
- Add cheaper history-generation modes where appropriate.

Expected result:

- much lower WFA runtime and storage overhead

### Stage 5: Optional GPU Follow-Up

Deferred by request.

GPU should be revisited only after the CPU path is rewritten and benchmarked, because the current architecture is too Python- and serialization-heavy to benefit reliably from a direct CUDA port.

## Concrete Refactor Items

1. Add a fast optimizer path that returns only metrics and summary fields.
2. Add reusable per-frame/per-lookback signal caches.
3. Replace naive rolling mean/std with an `O(n)` `numba` implementation.
4. Route optimizer and WFA validation logic through the fast path.
5. Keep the existing full backtest path for UI charts, trades, and stitched equity.
6. Add low-risk catalog/spec caching.
7. Benchmark before and after each stage.

## Success Criteria

- same optimizer rankings for the same input data and objective
- same WFA selected parameters for the same folds
- materially lower runtime for repeated experiments
- no CUDA dependency required for the initial acceleration rollout

## Implemented Status

Completed in the current CPU refactor pass:

- added a metrics-only optimizer/WFA backtest path
- added reusable prepared backtest context and per-lookback signal cache
- moved rolling mean/std to a `numba` `O(n)` kernel
- routed optimizer and WFA validation through the fast metrics path
- deduplicated optimizer evaluations that only differ by `bollinger_k`
- added low-risk caching for quote loading, resampling, and instrument metadata

Still deferred:

- persistent on-disk cache for parameter evaluations across separate runs
- any CUDA / GPU work

## Benchmark Snapshot

Measured on 2026-03-22 in the `coindb` conda environment on:

- broker: `bybit_mt5`
- pair: `US2000` / `NAS100`
- timeframe: `M15`
- range: `2025-01-01` to `2025-02-15`
- aligned rows: `2898`

Observed timings:

- first `load_pair_frame`: `0.47s`
- second `load_pair_frame`: `0.0017s`
- full backtest average: `0.5652s`
- metrics-only backtest average: `0.0023s`
- metrics-only speedup vs full backtest: `246.97x`
- grid optimization (`1200` evaluated trials): `0.5723s`
- genetic optimization, `1` worker (`145` evaluated trials): `0.1920s`
- genetic optimization, `4` workers (`145` evaluated trials): `0.2769s`
- genetic WFA (`2` folds, `49` history rows): `0.1739s`

Immediate conclusion:

- the new CPU fast path already removes the dominant hot spot
- on this workload, extra process-based parallelism is slower than the single-worker path because the evaluation kernel is now very cheap
- next ROI step is persistent cross-run caching, not CUDA
