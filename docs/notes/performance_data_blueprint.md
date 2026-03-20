# Performance and Data Blueprint

## Storage & Lake Layout
- `data/quotes/raw/<broker>/<symbol>/M5/<year>/<month>.parquet`: immutable raw ingestion from MT5 M5 bars; partitioned by year/month/shard for fast range scans.
- `data/quotes/derived/<symbol>/<timeframe>/<year>/<month>.parquet`: cached resamples generated on demand from raw M5 using Arrow/Polars; TTL metadata invalidates caches when raw data refreshes.
- `data/mt5/catalog.parquet`: instrument metadata (symbol, group, digits, point, contract_size, commission profile, margin, spread baseline, tick source, classification).
- `data/mt5/ticks/<symbol>/latest.arrow`: rolling tick buffer for live spreads; used for UI metrics and real-time spread adjustments.
- `data/jobs/*.parquet`: job artifacts (optimization runs, scan results, equity points) keyed by job ID; keeps every run for audit.

## Polars / Arrow Hot Path
- Use `Polars` lazy frames to read parquet slices (`scan_parquet`), filter per symbol/time, and join synchronized legs without pandas.
- Arrow memory-mapped tables feed Numba kernels via `polars.DataFrame.to_numpy()` only for necessary columns; keep columns contiguous to avoid copies.
- `src/domain/data/io.py`: helpers `load_symbols`, `load_m5_range`, `resample_to_tf`, `ensure_raw_range`. Return Polars frames or pre-allocated numpy arrays.
- Resampling module uses Polars `groupby_rolling` or manual chunked loops when Numba required.
- Sync data via integer bar indices, align timestamps before numeric kernels run.

## Numba Kernels
- `src/domain/backtest/kernel.py`: `numba.njit(cache=True)` functions for:
  * `compute_signals(distance_params, price1, price2)`: rolling z-score, Bollinger, entry/exit signals.
  * `simulate_trades(...)`: entry on next bar, trade lifecycle, per-leg pnl, trade markers.
  * `aggregate_equity(...)`: per-leg equity, combined equity, drawdown funnels.
  * `compute_metrics(equity)`: Sharpe, Sortino, Calmar, Omega, K-ratio, Ulcer index, normalized PnL/MaxDrawdown.
- `src/domain/scan`: add unit-root screening kernels before Johansen/Copula pair testing. MVP default is ADF-based I(1) screening on each leg; stricter ADF+KPSS mode stays configurable.
- Keep fallback python implementations in `src/domain/backtest/fallback.py` for diagnostics.

## Multiprocessing & Job Design
- Job types defined in `src/workers/jobs.py`: `BacktestJob`, `OptimizerJob`, `ScanJob`, `ResampleJob`.
- Workers use `ProcessPoolExecutor(maxtasksperchild=1)` to avoid leaks; chunk tasks per symbol pair or trial batch.
- `JobConfig` dataclass carries job metadata and time ranges; pickle only metadata, not data arrays.
- `src/domain/data/shared_buffers.py`: create/attach shared memory buffers; reused for trials referencing the same price arrays.
- Executor `src/workers/executor.py` primes shared buffers, dispatches chunks, updates Redis, writes partial parquet results.

## CUDA Usage Strategy
- CUDA reserved for:
  * Johansen scan over wide universes: `numba.cuda` or CuPy for batched eigen decomposition.
  * Optimizer trial batches exceeding 1k trials with long bars (?10k bars per trial).
- CUDA code lives in `src/domain/gpu` with wrappers that accept device arrays built from shared host memory.
- WSL GPU worker exposes service via gRPC/REST so Windows gateway delegates heavy compute without needing direct GPU access.
- Profiling gating (see below) decides when GPU backend activates; fallback stays CPU-based.

## Profiling & GPU Gatekeeping
- `src/domain/metrics/profiler.py`: decorators `@profile_section` measuring `load`, `compute`, `write`.
- Benchmark triggers:
  * Launch multiprocessing if `trial_count * bar_count / cpu_count > 5e5`.
  * Consider GPU if `trial_count * bar_count > 5e6` and CPU baseline is >1.5? slower than profiled GPU runs.
  * Always record `execution_time`, `memory_peak`, `cache_hits`.
- Run `python -m src.tools.benchmark --pair EURUSDUSDJPY` before enabling GPU; only flip GPU toggle when data transfer overhead is amortized.
- Persist profiling logs under `data/jobs/profiles/<job_id>.json`.

## Module Boundaries
- `src/domain/data`: I/O, resampling, shared memory helpers.
- `src/domain/backtest`: Numba kernels, trade builders, equity aggregators.
- `src/domain/optimizer`: parameter samplers, grid/genetic orchestrator, metrics aggregator.
- `src/domain/scan`: cointegration math, Johansen solver, distance method evaluation.
- `src/domain/gpu`: optional CUDA implementations, WSL worker wrappers.
- `src/workers`: job executors, pooling, Redis updates, profiling hooks.
- `src/tools`: scripts for profiling, benchmarking, dataset management.

## Risks
- Shared memory must be closed in `finally` blocks to avoid handles leaking.
- CUDA requires precise version pinning (match system CUDA 13 and Numba release).
- Resampling must keep UTC timestamps immutable; conversions happen only in UI.
