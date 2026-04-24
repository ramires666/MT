from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from domain.contracts import PairSelection, Timeframe, WfaWindowUnit
from storage.wfa_results import (
    load_wfa_optimization_history,
    load_wfa_run_snapshot,
    persist_wfa_optimization_history,
    persist_wfa_run_snapshot,
)


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


def test_wfa_run_snapshot_uses_objective_metric_in_storage_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("storage.wfa_results.wfa_root", lambda: tmp_path)
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    started_at = datetime(2025, 1, 1, tzinfo=UTC)
    ended_at = datetime(2025, 3, 1, tzinfo=UTC)

    omega_path = persist_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="omega_ratio",
        result={"status": "completed", "objective_metric": "omega_ratio"},
    )
    score_path = persist_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="score_log_trades",
        result={"status": "completed", "objective_metric": "score_log_trades"},
    )

    assert omega_path.exists()
    assert score_path.exists()
    assert omega_path != score_path
    assert "__obj_omega_ratio" in omega_path.name
    assert "__obj_" not in score_path.name

    omega_snapshot = load_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="omega_ratio",
    )
    score_snapshot = load_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="score_log_trades",
    )

    assert omega_snapshot is not None
    assert omega_snapshot["objective_metric"] == "omega_ratio"
    assert score_snapshot is not None
    assert score_snapshot["objective_metric"] == "score_log_trades"


def test_wfa_run_snapshot_uses_algorithm_in_storage_key(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("storage.wfa_results.wfa_root", lambda: tmp_path)
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    started_at = datetime(2025, 1, 1, tzinfo=UTC)
    ended_at = datetime(2025, 3, 1, tzinfo=UTC)

    distance_path = persist_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="net_profit",
        algorithm="distance",
        result={"status": "completed", "objective_metric": "net_profit", "algorithm": "distance"},
    )
    ols_path = persist_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="net_profit",
        algorithm="ols",
        result={"status": "completed", "objective_metric": "net_profit", "algorithm": "ols"},
    )

    assert distance_path.exists()
    assert ols_path.exists()
    assert distance_path != ols_path
    assert "__alg_" not in distance_path.name
    assert "__alg_ols" in ols_path.name

    distance_snapshot = load_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="net_profit",
        algorithm="distance",
    )
    ols_snapshot = load_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="net_profit",
        algorithm="ols",
    )

    assert distance_snapshot is not None
    assert distance_snapshot["algorithm"] == "distance"
    assert ols_snapshot is not None
    assert ols_snapshot["algorithm"] == "ols"


def test_wfa_run_snapshot_loader_tolerates_null_legacy_snapshot(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr("storage.wfa_results.wfa_root", lambda: tmp_path)
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    started_at = datetime(2025, 1, 1, tzinfo=UTC)
    ended_at = datetime(2025, 3, 1, tzinfo=UTC)

    legacy_path = persist_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric=None,
        algorithm=None,
        result={"status": "completed", "objective_metric": "net_profit", "algorithm": "distance"},
    )
    legacy_path.write_text("null", encoding="utf-8")

    snapshot = load_wfa_run_snapshot(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        objective_metric="net_profit",
        algorithm="ols",
    )

    assert snapshot is None
