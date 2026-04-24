from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from domain.contracts import PairSelection, StrategyDefaults, Timeframe, WfaWindowUnit
from domain.optimizer.distance_models import DistanceOptimizationResult, DistanceOptimizationRow
from domain.wfa_genetic import run_distance_genetic_wfa
from domain.wfa_serialization import build_fold_history_rows
from domain.wfa_windowing import WalkWindow


def _sample_frame() -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return pl.DataFrame(
        {
            "time": [base, base + timedelta(minutes=15)],
            "open_1": [100.0, 101.0],
            "close_1": [100.0, 101.0],
            "open_2": [100.0, 100.5],
            "close_2": [100.0, 100.5],
        }
    )


def _optimization_row(trial_id: int) -> DistanceOptimizationRow:
    return DistanceOptimizationRow(
        trial_id=trial_id,
        objective_metric="net_profit",
        objective_score=10.0 - trial_id,
        net_profit=100.0 - trial_id,
        ending_equity=10_100.0 - trial_id,
        max_drawdown=10.0 + trial_id,
        pnl_to_maxdd=2.0,
        omega_ratio=1.5,
        k_ratio=1.1,
        ulcer_index=0.1,
        ulcer_performance=100.0,
        cagr=0.24,
        cagr_to_ulcer=2.4,
        r_squared=0.91,
        hurst_exponent=0.58,
        calmar=1.8,
        trades=5,
        win_rate=0.6,
        lookback_bars=48 + trial_id,
        entry_z=1.5 + (trial_id * 0.1),
        exit_z=0.3,
        stop_z=3.0,
        bollinger_k=2.0,
        gross_profit=120.0,
        spread_cost=5.0,
        slippage_cost=2.0,
        commission_cost=1.0,
        total_cost=8.0,
    )


def test_build_fold_history_rows_respects_top_k(monkeypatch) -> None:
    captured_task_count = 0

    def fake_evaluate_params_parallel(*, tasks, **_kwargs):
        nonlocal captured_task_count
        captured_task_count = len(tasks)
        return ([_optimization_row(int(trial_id)) for trial_id, _params in tasks], False)

    monkeypatch.setattr("domain.wfa_serialization._evaluate_params_parallel", fake_evaluate_params_parallel)

    rows = build_fold_history_rows(
        optimization_rows=[_optimization_row(index) for index in range(1, 6)],
        test_frame=pl.DataFrame({"time": [datetime(2026, 1, 1, tzinfo=UTC)]}),
        pair=PairSelection(symbol_1="US2000", symbol_2="NAS100"),
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        defaults=StrategyDefaults(),
        objective_metric="net_profit",
        spec_1={"point": 0.01, "contract_size": 1.0},
        spec_2={"point": 0.01, "contract_size": 1.0},
        window=WalkWindow(
            index=0,
            train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
            train_ended_at=datetime(2026, 1, 8, tzinfo=UTC),
            validation_started_at=datetime(2026, 1, 8, tzinfo=UTC),
            validation_ended_at=datetime(2026, 1, 15, tzinfo=UTC),
            test_started_at=datetime(2026, 1, 15, tzinfo=UTC),
            test_ended_at=datetime(2026, 1, 22, tzinfo=UTC),
        ),
        wfa_run_id="run_1",
        lookback_units=8,
        test_units=2,
        step_units=2,
        unit=WfaWindowUnit.WEEKS,
        parallel_workers=1,
        history_top_k=2,
    )

    assert captured_task_count == 2
    assert len(rows) == 2
    assert rows[0]["selected_for_fold"] is True
    assert rows[-1]["train_rank"] == 2


def test_run_distance_genetic_wfa_cancellation_keeps_partial_folds_without_persisting(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    frame = _sample_frame()
    windows = [
        WalkWindow(
            index=0,
            train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
            train_ended_at=datetime(2026, 1, 8, tzinfo=UTC),
            validation_started_at=datetime(2026, 1, 8, tzinfo=UTC),
            validation_ended_at=datetime(2026, 1, 15, tzinfo=UTC),
            test_started_at=datetime(2026, 1, 15, tzinfo=UTC),
            test_ended_at=datetime(2026, 1, 22, tzinfo=UTC),
        ),
        WalkWindow(
            index=1,
            train_started_at=datetime(2026, 1, 8, tzinfo=UTC),
            train_ended_at=datetime(2026, 1, 15, tzinfo=UTC),
            validation_started_at=datetime(2026, 1, 15, tzinfo=UTC),
            validation_ended_at=datetime(2026, 1, 22, tzinfo=UTC),
            test_started_at=datetime(2026, 1, 22, tzinfo=UTC),
            test_ended_at=datetime(2026, 1, 29, tzinfo=UTC),
        ),
    ]
    persisted_history_calls = 0
    persisted_snapshot_calls = 0
    should_cancel = False

    monkeypatch.setattr("domain.wfa_genetic.build_train_test_windows", lambda **_kwargs: windows)
    monkeypatch.setattr("domain.wfa_genetic.load_pair_frame", lambda **_kwargs: frame)
    monkeypatch.setattr("domain.wfa_genetic.load_instrument_spec", lambda *_args, **_kwargs: {"point": 0.01, "contract_size": 1.0})
    monkeypatch.setattr("domain.wfa_genetic.slice_frame", lambda _frame, _started_at, _ended_at: frame)
    monkeypatch.setattr(
        "domain.wfa_genetic.optimize_distance_genetic_frame",
        lambda **_kwargs: DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=3,
            rows=[_optimization_row(1)],
            best_trial_id=1,
            cancelled=False,
            failure_reason=None,
        ),
    )
    monkeypatch.setattr(
        "domain.wfa_genetic.build_fold_history_rows",
        lambda **_kwargs: [{"trial_id": 1, "fold": int(_kwargs["window"].index + 1)}],
    )

    def fake_evaluate_distance_params(**_kwargs):
        return {
            "metrics": {"net_profit": 25.0},
            "result": SimpleNamespace(
                summary={
                    "net_pnl": 25.0,
                    "ending_equity": 10_025.0,
                    "max_drawdown": 5.0,
                    "trades": 3,
                    "total_commission": 1.5,
                    "total_cost": 2.5,
                    "gross_pnl": 27.5,
                    "total_spread_cost": 0.5,
                    "total_slippage_cost": 0.5,
                },
                frame=pl.DataFrame(
                    {
                        "time": [datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 15, 0, 15, tzinfo=UTC)],
                        "equity_total": [10_000.0, 10_025.0],
                    }
                ),
            ),
        }

    def fake_persist_history(**_kwargs):
        nonlocal persisted_history_calls
        persisted_history_calls += 1
        return tmp_path / "optimization_history.parquet"

    def fake_persist_snapshot(**_kwargs):
        nonlocal persisted_snapshot_calls
        persisted_snapshot_calls += 1
        return tmp_path / "snapshot.json"

    def progress_callback(completed: int, total: int, _stage: str) -> None:
        nonlocal should_cancel
        if total == 2 and completed >= 1:
            should_cancel = True

    monkeypatch.setattr("domain.wfa_genetic.evaluate_distance_params", fake_evaluate_distance_params)
    monkeypatch.setattr("domain.wfa_genetic.persist_wfa_optimization_history", fake_persist_history)
    monkeypatch.setattr("domain.wfa_genetic.persist_wfa_run_snapshot", fake_persist_snapshot)

    result = run_distance_genetic_wfa(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        defaults=StrategyDefaults(),
        objective_metric="net_profit",
        parameter_search_space={"lookback_bars": [48], "entry_z": [1.5], "exit_z": [0.3], "stop_z": [3.0], "bollinger_k": [2.0]},
        genetic_config={"population_size": 4, "generations": 2, "elite_count": 1, "mutation_rate": 0.2, "random_seed": 7},
        lookback_units=8,
        test_units=2,
        unit=WfaWindowUnit.WEEKS,
        parallel_workers=1,
        history_top_k=1,
        cancel_check=lambda: should_cancel,
        progress_callback=progress_callback,
    )

    assert result["cancelled"] is True
    assert result["status"] == "cancelled"
    assert result["failure_reason"] == "cancelled"
    assert int(result["fold_count"]) == 1
    assert int(result["optimization_history_rows"]) == 1
    assert persisted_history_calls == 0
    assert persisted_snapshot_calls == 0
