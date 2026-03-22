# MT Pair Trading Service

High-performance pair-trading research and backtesting service with:

- MetaTrader 5 market data ingestion
- FastAPI control plane
- Bokeh research UI
- Polars + Numba + multiprocessing compute path
- optional CUDA acceleration for heavy scan and optimization workloads

Current repository state:

- approved architecture blueprint: `docs/ARCHITECTURE_PLAN.md`
- supporting notes: `docs/notes/bokeh_ui_blueprint.md`, `docs/notes/performance_data_blueprint.md`
- working local API, Bokeh UI, MT5 export helpers, storage layer, and tests

## Components

- `src/core_api/main.py`: FastAPI application
- `src/bokeh_app/main.py`: Bokeh research UI
- `src/tools/mt5_sync.py`: direct MetaTrader5 bridge helper
- `src/tools/mt5_terminal_export_sync.py`: terminal-side export helper
- `src/tools/mt5_export_catalog_sync.py`: batch export from the saved instrument catalog
- `mql5/CodexHistoryExport.mq5`: MQL5 exporter script used by the terminal-side flow

Default local endpoints:

- API: `http://127.0.0.1:8000`
- API health: `http://127.0.0.1:8000/healthz`
- API meta: `http://127.0.0.1:8000/api/v1/meta`
- Bokeh UI: `http://127.0.0.1:5006/bokeh_app`

## Requirements

### Required

- Python `>= 3.13` according to `pyproject.toml`
- `pip`
- a virtual environment

### Required only for MT5 data import

- installed MetaTrader 5 terminal
- for Bybit MT5 historical bars: compiled `CodexHistoryExport` script in the terminal `Scripts` directory

### Not required for the current local startup

`postgres_dsn` and `redis_dsn` exist in settings, but the current startup path does not open PostgreSQL or Redis connections. You do not need those services just to launch the API, the Bokeh UI, or run tests.

## 1. Create the Environment

Use Python 3.13. On Linux or WSL:

```bash
python3.13 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,optimizer]"
```

Optional GPU dependencies:

```bash
python -m pip install -e ".[gpu]"
```

If you prefer to run modules directly from the repo checkout, export `PYTHONPATH=src` in the same shell:

```bash
export PYTHONPATH=src
```

Without either editable install or `PYTHONPATH=src`, imports like `core_api.main` and `app_config` will not resolve.

## 2. Configure Environment Variables

Settings are loaded from `.env` with the prefix `MT_SERVICE_`.

Minimal `.env` example for local work:

```dotenv
MT_SERVICE_ENVIRONMENT=dev
MT_SERVICE_API_HOST=127.0.0.1
MT_SERVICE_API_PORT=8000
MT_SERVICE_BOKEH_HOST=127.0.0.1
MT_SERVICE_BOKEH_PORT=5006
MT_SERVICE_DATA_ROOT=data
MT_SERVICE_DEFAULT_BROKER_ID=bybit_mt5
MT_SERVICE_OPTIMIZER_PARALLEL_WORKERS=8
MT_SERVICE_SCAN_PARALLEL_WORKERS=8
MT_SERVICE_BYBIT_TRADFI_FEE_MODE=tight_spread
```

Set the MT5 terminal path only if you plan to use the downloader or exporter:

WSL example:

```dotenv
MT_SERVICE_MT5_TERMINAL_PATH=/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe
```

Windows example:

```dotenv
MT_SERVICE_MT5_TERMINAL_PATH=C:\Program Files\Bybit MT5 Terminal\terminal64.exe
```

## 3. Run the API

From the repository root:

```bash
source .venv/bin/activate
export PYTHONPATH=src
python -m uvicorn core_api.main:app --host 127.0.0.1 --port 8000 --reload
```

Health checks:

```bash
curl http://127.0.0.1:8000/healthz
curl http://127.0.0.1:8000/api/v1/meta
```

## 4. Run the Bokeh UI

On Linux or WSL:

```bash
source .venv/bin/activate
export PYTHONPATH=src
python -m bokeh serve src/bokeh_app \
  --address 127.0.0.1 \
  --port 5006 \
  --allow-websocket-origin=127.0.0.1:5006 \
  --allow-websocket-origin=localhost:5006
```

Then open:

```text
http://127.0.0.1:5006/bokeh_app
```

On Windows there is also a helper:

```bat
start_bokeh.cmd
```

That script kills the old listener on port `5006`, sets `PYTHONPATH=src`, and starts `bokeh serve` for `src\bokeh_app`.

## 5. Data Import from MetaTrader 5

The repository supports two ingestion paths.

### A. Direct Python bridge

Use this for operations that are supported by the `MetaTrader5` Python package in your terminal build.

Refresh instrument catalog:

```bash
source .venv/bin/activate
export PYTHONPATH=src
python -m tools.mt5_sync \
  --terminal-path "/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe" \
  catalog
```

Download M5 quotes for specific symbols:

```bash
python -m tools.mt5_sync \
  --terminal-path "/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe" \
  quotes \
  --symbol NAS100 \
  --symbol US2000 \
  --from 2026-01-01T00:00:00 \
  --to 2026-03-01T00:00:00
```

### B. Terminal-side exporter for historical bars

This is the important path for Bybit MT5 historical data. The note in `docs/notes/bybit_mt5_bridge_findings.md` shows that the standard external `MetaTrader5` bridge initializes correctly but historical calls such as `copy_rates_range()` fail on the observed Bybit terminal build. The implemented workaround is terminal-side export through `CodexHistoryExport.mq5`.

#### 5.1 Compile and install the exporter

1. Copy `mql5/CodexHistoryExport.mq5` into the terminal `MQL5/Scripts` directory.
2. Open it in MetaEditor.
3. Compile it so the terminal can run `CodexHistoryExport`.

#### 5.2 Export a small symbol list

```bash
source .venv/bin/activate
export PYTHONPATH=src
python -m tools.mt5_terminal_export_sync \
  --terminal-path "/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe" \
  --from 2026-01-01T00:00:00 \
  --to 2026-03-01T00:00:00 \
  --symbol NAS100 \
  --symbol US2000
```

What this command does:

- writes `codex_export_run.ini` into the repo root
- writes `history_job.txt` into the terminal common files area
- launches the MT5 terminal with `/config:...`
- reads exported binary files from the terminal common files directory
- converts them into project parquet partitions under `data/parquet/raw/...`

#### 5.3 Export by catalog groups in batches

First refresh the catalog, then run the batch exporter:

```bash
python -m tools.mt5_export_catalog_sync \
  --terminal-path "/mnt/c/Program Files/Bybit MT5 Terminal/terminal64.exe" \
  --from 2026-01-01T00:00:00 \
  --to 2026-03-01T00:00:00 \
  --all-symbols \
  --group forex \
  --chunk-size 40 \
  --skip-existing
```

Useful flags:

- `--symbol SYMBOL`: export only explicit symbols
- `--all-symbols`: use the saved instrument catalog
- `--group GROUP`: restrict catalog export to one or more normalized groups
- `--limit N`: limit batch size from the catalog selection
- `--chunk-size N`: how many symbols to export per terminal run
- `--skip-existing`: skip symbols whose required monthly partitions already exist

## 6. Where Data Is Stored

The default root is `data/`.

- `data/catalog/<broker>/instrument_catalog.parquet`
- `data/parquet/raw/<broker>/<symbol>/M5/year=YYYY/month=MM/data.parquet`
- `data/scans/...`
- `data/wfa/...`
- `data/meta_selector/...`
- `data/ui_state/bokeh_app_state.json`

## 7. Run Tests

After installing the `dev` extra:

```bash
source .venv/bin/activate
export PYTHONPATH=src
python -m pytest -q
```

Quick smoke test:

```bash
python -m pytest tests/test_health.py -q
```

## 8. Typical Local Workflow

1. Create and activate `.venv`.
2. Install editable dependencies with `.[dev,optimizer]`.
3. Export `PYTHONPATH=src`.
4. Create `.env` if you need non-default ports, paths, or data root.
5. Start the API with `uvicorn`.
6. Start the Bokeh UI with `bokeh serve`.
7. Refresh the instrument catalog or import quotes from MT5.
8. Open `http://127.0.0.1:5006/bokeh_app`.

## 9. Troubleshooting

### `python: command not found`

Use `python3.13` to create the venv. After activation, the command should be `python`.

### `ModuleNotFoundError: No module named 'fastapi'` or similar

Dependencies are not installed in the current environment. Activate the venv and run:

```bash
python -m pip install -e ".[dev,optimizer]"
```

### `ModuleNotFoundError` for `core_api`, `app_config`, or sibling packages

Run from the repo root and make sure one of these is true:

- the project is installed in editable mode
- `PYTHONPATH=src` is exported

### Bokeh opens but the app is empty or fails to import

Make sure you started it from the repository root with:

```bash
export PYTHONPATH=src
python -m bokeh serve src/bokeh_app ...
```

### `MT5 terminal not found`

Check `MT_SERVICE_MT5_TERMINAL_PATH`.

- On Windows use a native Windows path like `C:\Program Files\...`
- On WSL use the mounted path like `/mnt/c/Program Files/...`

### Historical downloads fail through the `MetaTrader5` Python bridge

This is expected on the Bybit terminal build documented in `docs/notes/bybit_mt5_bridge_findings.md`. Use the terminal-side exporter flow instead.
