from pathlib import Path

import polars as pl

from domain.contracts import PairSelection, Timeframe
from storage.wfa_results import load_wfa_optimization_history, persist_wfa_optimization_history


def test_persist_wfa_optimization_history_appends_and_dedupes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("storage.wfa_results.wfa_root", lambda: tmp_path)
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")

    first_batch = [
        {"wfa_run_id": "run_1", "fold": 1, "trial_id": 1, "lookback_bars": 48, "test_score_log_trades": 1.2},
        {"wfa_run_id": "run_1", "fold": 1, "trial_id": 2, "lookback_bars": 96, "test_score_log_trades": 0.8},
    ]
    second_batch = [
        {"wfa_run_id": "run_1", "fold": 1, "trial_id": 2, "lookback_bars": 96, "test_score_log_trades": 0.9},
        {"wfa_run_id": "run_1", "fold": 2, "trial_id": 1, "lookback_bars": 48, "test_score_log_trades": 1.4},
    ]

    path = persist_wfa_optimization_history("bybit_mt5", pair, Timeframe.M15, first_batch)
    assert path is not None and path.exists()
    persist_wfa_optimization_history("bybit_mt5", pair, Timeframe.M15, second_batch)

    frame = load_wfa_optimization_history("bybit_mt5", pair, Timeframe.M15).sort(["fold", "trial_id"])
    assert frame.height == 3
    updated = frame.filter((pl.col("fold") == 1) & (pl.col("trial_id") == 2))
    assert updated.height == 1
    assert updated.get_column("test_score_log_trades").item() == 0.9
