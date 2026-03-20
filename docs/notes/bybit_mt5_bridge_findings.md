# Bybit MT5 Bridge Findings

Observed on 2026-03-17 with `C:\Program Files\Bybit MT5 Terminal\terminal64.exe`:

- `MetaTrader5.initialize()` succeeds.
- `symbols_get()`, `symbol_info()`, and `symbol_info_tick()` succeed.
- `copy_rates_from_pos()`, `copy_rates_range()`, and `copy_ticks_range()` all fail with `(-1, "Terminal: Call failed")`.

Implication:

- direct history ingestion through the standard external `MetaTrader5` Python bridge is not reliable on this Bybit terminal build for historical bars or ticks.
- terminal-side export through compiled `MQL5` script is currently the working path.

Implemented workaround in repo:

- terminal-side exporter source: `mql5/CodexHistoryExport.mq5`
- binary decoder: `src/tools/mt5_binary_export.py`
- orchestration helper: `src/tools/mt5_terminal_export_sync.py`

Validated exports:

- `NAS100`
- `US2000`
- `XAGUSD`
- `XAUUSD+`

All were exported from `2026-01-01` to `2026-03-17` and converted into the project `Parquet` lake.