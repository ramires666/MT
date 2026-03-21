# MT Pair Trading Service

High-performance pair-trading research and backtesting service with:

- MetaTrader 5 market data ingestion on Windows
- FastAPI control plane
- Bokeh research UI
- Polars + Numba + multiprocessing compute path
- optional CUDA acceleration in WSL for heavy scan and optimization workloads

Current repository state:

- approved architecture blueprint: `docs/ARCHITECTURE_PLAN.md`
- supporting notes: `docs/notes/bokeh_ui_blueprint.md`, `docs/notes/performance_data_blueprint.md`
- initial service skeleton for API, UI, gateway, workers, and shared domain models
