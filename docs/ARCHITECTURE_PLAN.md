# MT Pair Trading Service Architecture Plan

## Product Goal

Build a web service for pair-trading research and backtesting that:

- uses MetaTrader 5 as the source of instruments, specifications, bars, and live ticks
- stores canonical history as compressed Parquet
- runs pair-trading tests on two instruments simultaneously
- supports Distance, Johansen, and later Copula strategies
- provides synchronized Bokeh charts and sortable result tables
- runs heavy scans and optimizations with a performance-first stack: Polars, Numba, multiprocessing, and optional CUDA
- remains portable to a future multi-user deployment and Dockerized server architecture

## Fixed Decisions

- Runtime baseline: Python 3.13
- UI framework: Bokeh
- API framework: FastAPI
- Primary compute stack: NumPy + Polars + PyArrow + Numba
- Long-running jobs: separate worker processes with Redis
- Result metadata: PostgreSQL
- Raw market data: MT5 M5 bars persisted directly to compressed Parquet
- Time policy: internal storage in UTC; broker display/reference timezone is UTC+2 fixed, no DST
- Default execution assumptions:
  - initial capital: 10000
  - leverage: 1:100
  - slippage: 1 point
  - margin budget per leg: 500 USD
- Commission model: per-instrument broker profile, auto-filled where possible and manually overridable when MT5 metadata is incomplete

## Service Layout

### mt5_gateway
- Windows-only process
- owns the live connection to local MetaTrader 5
- reads symbol catalogs, specifications, M5 bars, and live ticks
- writes canonical quote data and exposes pull APIs to the rest of the stack

### core_api
- FastAPI service
- exposes job submission and result retrieval endpoints
- handles presets, metadata, run history, and UI coordination

### bokeh_app
- main research UI
- renders synchronized charts, controls, tables, and dynamic layout

### worker_cpu
- runs backtests, scan jobs, and optimizers on CPU
- uses Polars, Numba, and multiprocessing

### worker_gpu
- optional WSL worker for GPU-heavy jobs
- reads the same Parquet and DB state as CPU workers

## Storage

### PostgreSQL
Stores:
- instrument catalog
- broker cost profiles
- run metadata
- optimization and scan results
- future session metadata for multi-user support

### Redis
Stores:
- job queue
- progress pub/sub
- short-lived caches

### Parquet Lake
Stores:
- raw M5 history
- derived timeframe caches
- optional exported results

## Data Lifecycle

### Instrument Catalog
Store:
- symbol name
- broker group/path
- normalized category: Forex, Indices, Stocks, Commodities, Crypto, Custom
- digits
- point
- contract size
- volume step, min, max
- trade mode and margin-related attributes
- last refresh timestamp

### Quote Ingestion
Rules:
- fetch into memory from MT5
- persist directly to compressed Parquet
- never write CSV
- use M5 as the canonical raw timeframe
- fill only missing windows on incremental refresh

Raw layout:
- `data/parquet/raw/<broker>/<symbol>/M5/year=<yyyy>/month=<mm>/part-*.parquet`

Derived layout:
- `data/parquet/derived/<broker>/<symbol>/<timeframe>/year=<yyyy>/month=<mm>/part-*.parquet`

### Time Handling
- Parquet and DB timestamps are always UTC
- broker metadata may declare constant UTC+2
- UI can render UTC and broker time views

### Spread and Costs
- historical spread default: MT5 M5 bar spread
- live spread reference: latest tick bid/ask
- slippage: fixed default 1 point
- commission: per-symbol cost profile

## UI Layout

### Left Sidebar
Contains:
- Data: refresh instruments, download or update history, broker and group filters
- Pair Selection: group filter, search, only cointegrated pairs, symbol 1, symbol 2
- Tester: timeframe, test period, algorithm selector, algorithm-specific parameters
- Capital and Costs: capital, leverage, margin budget per leg, slippage, commission mode or overrides
- Run Controls: run test, stop job, save preset

### Right Workspace
Optional blocks:
- Price Chart 1
- Price Chart 2
- Spread / Residual
- Z-score + Bollinger
- Equity
- Trades Table
- Optimizer Settings
- Optimization Results
- Cointegration Scan Settings
- Cointegration Scan Results

Behavior:
- all charts share the same x_range
- zoom and pan are synchronized automatically
- each chart recomputes its own visible y_range from the current x_range
- if a chart is hidden, remaining charts expand vertically
- tables and config panes are collapsible and sortable where applicable

### Interaction Rules
- clicking a trade row highlights entry and exit markers on both leg charts
- clicking an optimization row copies parameters to the tester sidebar and launches a normal test
- clicking a cointegration row copies the selected pair into the tester and optimizer selectors
- tester period remains unchanged when a row from optimization is applied

## Strategies
Implementation order:
1. Distance
2. Johansen
3. Copula

Common parameters:
- symbol 1
- symbol 2
- timeframe
- test period
- capital
- leverage
- margin budget per leg
- slippage
- commission mode
- cooldown or holding controls

Distance parameters:
- formation window
- trading window
- hedge ratio mode
- entry z
- exit z
- stop z
- recalc period

Johansen parameters:
- formation window
- recalc period
- deterministic order
- lag difference count
- rank selection mode
- entry z
- exit z
- stop z

Copula parameters:
- formation window
- refit period
- marginal model
- copula family
- entry quantile
- exit quantile
- tail stop

Execution rules:
- signal on bar t
- execute on next available bar
- compute position sizes from margin budget per leg, leverage, contract size, and symbol volume constraints
- record PnL separately for leg 1, leg 2, and total

## Optimizer
Modes:
- Grid Search
- Genetic Search

Rules:
- optimizer period is independent from tester period
- selecting a trial copies parameters but not optimizer dates into the tester
- every optimization column is sortable
- rows can be selected to replay a normal test
- result metadata includes seed, runtime, trial index, and parameter payload

Objective metrics:
- net profit
- Sharpe
- Sortino
- Calmar
- Omega ratio
- K-ratio
- Ulcer index or ulcer performance score
- profit-to-drawdown
- composite score

## Cointegration Scan
Universe modes:
- all instruments
- market watch
- selected normalized group
- manual symbol subset

Pre-filter gate:
- before running Johansen or other cointegration tests, run a unit-root screen on each leg separately
- default gate for MVP: require both instruments to look I(1) under ADF screening
- practical rule: level series must keep the unit root, first difference must be stationary
- stricter mode later: ADF and KPSS together for cross-checking
- pairs that fail the unit-root gate are excluded from cointegration testing and marked as screened out

Output:
- symbol 1
- symbol 2
- unit-root status for leg 1
- unit-root status for leg 2
- rank
- trace statistic
- max eigen statistic
- threshold or significance flag
- half-life estimate
- sample size
- last z-score
- data coverage

Tester and optimizer can filter pair selection to only cointegrated pairs from a chosen completed scan dataset. Johansen and Copula modes should also apply the same unit-root gate before running a single-pair test.

## Performance Architecture
Rules:
- no pandas in hot paths
- use Polars for ETL, filtering, joins, resampling, and columnar transforms
- use Numba for numerical kernels with loops over bars or trades
- use multiprocessing for scan and optimization batches
- use GPU only after profile data shows a clear win

CPU hot path:
- Polars lazy scans for Parquet
- conversion to contiguous NumPy arrays before heavy kernels
- cached Numba kernels for rolling moments, z-score, Bollinger, trade simulation, equity, drawdown, objective metrics, and unit-root prefilters

Multiprocessing strategy:
- partition scan jobs by pair chunks
- partition optimizer jobs by trial batches or population slices
- keep quote arrays read-only
- avoid serializing large frames repeatedly
- materialize lightweight result records incrementally

GPU candidates:
- large all-pairs Johansen scans
- large optimizer populations
- repeated batched linear algebra workloads

Before moving a workload to GPU:
1. profile CPU baseline
2. verify bottleneck type
3. confirm workload size is large enough
4. compare end-to-end runtime including data movement
5. keep CPU path as correctness baseline

## Multi-User and Deployment Readiness
- keep UI state session-scoped
- keep compute jobs stateless outside DB, Redis, and Parquet artifacts
- isolate MT5 connectivity behind mt5_gateway
- avoid local path assumptions in core domain logic
- prepare for split deployment:
  - Windows host for mt5_gateway
  - Linux or WSL or containers for API, UI, and workers

## Delivery Sequence
1. Foundation: repository scaffold, config system, API and UI entrypoints, domain contracts, storage contracts
2. Market Data: MT5 instrument sync, M5 ingestion, Parquet storage, timeframe derivation, catalog and group normalization
3. Backtest MVP: Distance strategy, synchronized charts, trades overlay, trades table, equity chart
4. Optimizer MVP: grid search, genetic search, sortable results, parameter replay into tester
5. Johansen: backtest, scan jobs, cointegrated pair filters
6. Copula: strategy and optimizer integration
7. Hardening: profiling, CUDA worker for confirmed hotspots, multi-user polish, Dockerization

## Supporting Notes
- `docs/notes/bokeh_ui_blueprint.md`
- `docs/notes/performance_data_blueprint.md`

## WFA Layer

WFA is a first-class optimization mode, not a checkbox inside the standard optimizer.

Supported WFA modes:
- Anchored: optimize on a large training window, validate on a smaller trailing validation window, then evaluate on a final test window.
- Rolling: split the full available history into repeated train, validation, and test slices and walk forward through time step by step.

Core WFA requirements:
- WFA optimizer must search not only strategy parameters but also window sizes for train, validation, and test.
- WFA must support one selected pair or multiple selected pairs in one run.
- Pair selection can come from the tester sidebar directly or from a dedicated action on an optimization results row.
- WFA period handling is independent from the normal tester period.

Anchored mode example:
- large train window
- short validation window such as one week or two weeks or one month
- final test window such as two weeks

Rolling mode example:
- use the full downloaded history such as all of 2025 and early 2026
- walk through the whole period with repeated train, validation, and test windows
- examples: train one or two months, validation two weeks, test one week, then move forward by one step

WFA UI blocks:
- right-side optional WFA settings block
- right-side WFA run results table
- right-side WFA summary block with per-window and aggregate out-of-sample metrics
- optional WFA equity chart for stitched out-of-sample equity
- aggregate multi-pair out-of-sample equity chart when WFA runs on several selected pairs

WFA output tables should include:
- pair or pair set identifier
- WFA mode
- train, validation, and test window sizes
- step size
- selected strategy parameters
- in-sample score
- validation score
- out-of-sample test score
- stitched out-of-sample metrics across all walk-forward windows

Multi-pair WFA behavior:
- run independently per pair and also produce a summed out-of-sample equity curve across the selected pair set
- aggregated multi-pair equity is built from synchronized out-of-sample windows only
- keep both per-pair rankings and the combined equity result in the same WFA run metadata
- selection source is tracked in result metadata so the user can replay a winning pair setup into the tester