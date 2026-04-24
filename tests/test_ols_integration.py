from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import polars as pl

from domain.contracts import Algorithm, PairSelection, StrategyDefaults, Timeframe, WfaWindowUnit
from domain.optimizer.distance_models import DistanceOptimizationResult, DistanceOptimizationRow
from domain.optimizer.ols import optimize_ols_grid_frame
from domain.wfa_genetic import run_distance_genetic_wfa
from domain.wfa_windowing import WalkWindow


def _sample_frame() -> pl.DataFrame:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    return pl.DataFrame(
        {
            "time": [base + timedelta(minutes=15 * idx) for idx in range(4)],
            "open_1": [100.0, 101.0, 102.0, 103.0],
            "high_1": [100.0, 101.0, 102.0, 103.0],
            "low_1": [100.0, 101.0, 102.0, 103.0],
            "close_1": [100.0, 101.0, 102.0, 103.0],
            "tick_volume_1": [100, 100, 100, 100],
            "spread_1": [2, 2, 2, 2],
            "real_volume_1": [10, 10, 10, 10],
            "open_2": [50.0, 50.5, 51.0, 51.5],
            "high_2": [50.0, 50.5, 51.0, 51.5],
            "low_2": [50.0, 50.5, 51.0, 51.5],
            "close_2": [50.0, 50.5, 51.0, 51.5],
            "tick_volume_2": [100, 100, 100, 100],
            "spread_2": [2, 2, 2, 2],
            "real_volume_2": [10, 10, 10, 10],
        }
    )


def _optimization_row(trial_id: int) -> DistanceOptimizationRow:
    return DistanceOptimizationRow(
        trial_id=trial_id,
        objective_metric="net_profit",
        objective_score=20.0,
        net_profit=20.0,
        ending_equity=10_020.0,
        max_drawdown=5.0,
        pnl_to_maxdd=4.0,
        omega_ratio=1.2,
        k_ratio=0.6,
        ulcer_index=0.2,
        ulcer_performance=0.4,
        cagr=0.15,
        cagr_to_ulcer=0.75,
        r_squared=0.91,
        hurst_exponent=0.58,
        calmar=1.1,
        trades=3,
        win_rate=0.67,
        lookback_bars=2,
        entry_z=1.5,
        exit_z=0.3,
        stop_z=3.0,
        bollinger_k=2.0,
        gross_profit=22.0,
        spread_cost=1.0,
        slippage_cost=0.5,
        commission_cost=0.25,
        total_cost=1.75,
    )


def test_optimize_ols_grid_frame_dispatches_ols_algorithm(monkeypatch) -> None:
    captured_algorithms: list[str] = []

    def fake_run_distance_backtest_metrics_frame(*, algorithm, **_kwargs):
        captured_algorithms.append(str(getattr(algorithm, "value", algorithm)))
        return {
            "net_profit": 20.0,
            "ending_equity": 10020.0,
            "max_drawdown": 5.0,
            "pnl_to_maxdd": 4.0,
            "omega_ratio": 1.2,
            "k_ratio": 0.6,
            "ulcer_index": 0.2,
            "ulcer_performance": 0.4,
            "cagr": 0.15,
            "cagr_to_ulcer": 0.75,
            "r_squared": 0.91,
            "hurst_exponent": 0.58,
            "calmar": 1.1,
            "trades": 3,
            "win_rate": 0.67,
            "gross_profit": 22.0,
            "spread_cost": 1.0,
            "slippage_cost": 0.5,
            "commission_cost": 0.25,
            "total_cost": 1.75,
        }

    monkeypatch.setattr("domain.optimizer.distance.run_distance_backtest_metrics_frame", fake_run_distance_backtest_metrics_frame)

    result = optimize_ols_grid_frame(
        frame=_sample_frame(),
        pair=PairSelection(symbol_1="AAA", symbol_2="BBB"),
        defaults=StrategyDefaults(),
        search_space={
            "lookback_bars": [2],
            "entry_z": [1.5],
            "exit_z": [0.3],
            "stop_z": [3.0],
            "bollinger_k": [2.0],
        },
        objective_metric="net_profit",
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1={"point": 0.01, "contract_size": 1.0},
        spec_2={"point": 0.01, "contract_size": 1.0},
    )

    assert captured_algorithms == ["ols"]
    assert result.evaluated_trials == 1
    assert result.best_trial_id == 1
    assert result.rows[0].hurst_exponent == 0.58


def test_run_distance_genetic_wfa_dispatches_ols_algorithm(monkeypatch, tmp_path: Path) -> None:
    pair = PairSelection(symbol_1="AAA", symbol_2="BBB")
    frame = _sample_frame()
    window = WalkWindow(
        index=0,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 1, 8, tzinfo=UTC),
        validation_started_at=datetime(2026, 1, 8, tzinfo=UTC),
        validation_ended_at=datetime(2026, 1, 15, tzinfo=UTC),
        test_started_at=datetime(2026, 1, 15, tzinfo=UTC),
        test_ended_at=datetime(2026, 1, 22, tzinfo=UTC),
    )
    captured_history_algorithms: list[str] = []
    captured_eval_algorithms: list[str] = []
    persisted_snapshot_algorithms: list[str] = []

    monkeypatch.setattr("domain.wfa_genetic.build_train_test_windows", lambda **_kwargs: [window])
    monkeypatch.setattr("domain.wfa_genetic.load_pair_frame", lambda **_kwargs: frame)
    monkeypatch.setattr("domain.wfa_genetic.load_instrument_spec", lambda *_args, **_kwargs: {"point": 0.01, "contract_size": 1.0})
    monkeypatch.setattr("domain.wfa_genetic.slice_frame", lambda _frame, _started_at, _ended_at: frame)
    monkeypatch.setattr(
        "domain.wfa_genetic.optimize_distance_genetic_frame",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("distance optimizer should not be used for OLS WFA")),
    )
    monkeypatch.setattr(
        "domain.wfa_genetic.optimize_ols_genetic_frame",
        lambda **_kwargs: DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=1,
            rows=[_optimization_row(1)],
            best_trial_id=1,
        ),
    )
    monkeypatch.setattr(
        "domain.wfa_genetic.build_fold_history_rows",
        lambda **kwargs: captured_history_algorithms.append(str(getattr(kwargs.get("algorithm"), "value", kwargs.get("algorithm")))) or [],
    )

    def fake_evaluate_distance_params(**kwargs):
        captured_eval_algorithms.append(str(getattr(kwargs.get("algorithm"), "value", kwargs.get("algorithm"))))
        return {
            "metrics": {
                "net_profit": 20.0,
                "cagr": 0.15,
                "cagr_to_ulcer": 0.75,
                "r_squared": 0.91,
                "hurst_exponent": 0.58,
                "calmar": 1.1,
            },
            "result": SimpleNamespace(
                summary={
                    "net_pnl": 20.0,
                    "ending_equity": 10020.0,
                    "max_drawdown": 5.0,
                    "trades": 3,
                    "total_commission": 1.5,
                    "total_cost": 2.5,
                    "gross_pnl": 22.5,
                    "total_spread_cost": 0.5,
                    "total_slippage_cost": 0.5,
                },
                frame=pl.DataFrame(
                    {
                        "time": [datetime(2026, 1, 15, tzinfo=UTC), datetime(2026, 1, 15, 0, 15, tzinfo=UTC)],
                        "equity_total": [10_000.0, 10_020.0],
                    }
                ),
            ),
        }

    monkeypatch.setattr("domain.wfa_genetic.evaluate_distance_params", fake_evaluate_distance_params)
    monkeypatch.setattr("domain.wfa_genetic.persist_wfa_optimization_history", lambda **_kwargs: None)
    monkeypatch.setattr(
        "domain.wfa_genetic.persist_wfa_run_snapshot",
        lambda **kwargs: persisted_snapshot_algorithms.append(str(kwargs.get("algorithm"))) or (tmp_path / "snapshot.json"),
    )

    result = run_distance_genetic_wfa(
        broker="bybit_mt5",
        pair=pair,
        timeframe=Timeframe.M15,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        defaults=StrategyDefaults(),
        objective_metric="net_profit",
        parameter_search_space={
            "lookback_bars": [2],
            "entry_z": [1.5],
            "exit_z": [0.3],
            "stop_z": [3.0],
            "bollinger_k": [2.0],
        },
        genetic_config={"population_size": 4, "generations": 2, "elite_count": 1, "mutation_rate": 0.2, "random_seed": 7},
        lookback_units=8,
        test_units=2,
        unit=WfaWindowUnit.WEEKS,
        parallel_workers=1,
        history_top_k=1,
        algorithm=Algorithm.OLS,
    )

    assert result["algorithm"] == Algorithm.OLS.value
    assert captured_history_algorithms == [Algorithm.OLS.value]
    assert captured_eval_algorithms == [Algorithm.OLS.value]
    assert persisted_snapshot_algorithms == [Algorithm.OLS.value]
