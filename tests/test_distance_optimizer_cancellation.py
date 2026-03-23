import polars as pl

import domain.optimizer.distance as distance_module
from domain.backtest.distance import DistanceParameters
from domain.contracts import PairSelection, StrategyDefaults


def _sample_parameters() -> DistanceParameters:
    return DistanceParameters(
        lookback_bars=10,
        entry_z=1.0,
        exit_z=0.5,
        stop_z=2.0,
        bollinger_k=2.0,
    )


def test_distance_grid_optimization_can_be_cancelled_before_evaluation(monkeypatch) -> None:
    cancel_calls = 0

    def cancel_check() -> bool:
        nonlocal cancel_calls
        cancel_calls += 1
        return True

    def fake_iter(_search_space):
        yield _sample_parameters()

    def fail_evaluate(*_args, **_kwargs):
        raise AssertionError("Optimization should not evaluate a candidate after cancellation.")

    monkeypatch.setattr(distance_module, "iter_distance_parameter_grid", fake_iter)
    monkeypatch.setattr(distance_module, "_evaluate_params", fail_evaluate)

    result = distance_module.optimize_distance_grid_frame(
        frame=pl.DataFrame(),
        pair=PairSelection(symbol_1="AAA", symbol_2="BBB"),
        defaults=StrategyDefaults(initial_capital=1_000.0, leverage=1.0, margin_budget_per_leg=100.0, slippage_points=1.0),
        search_space=distance_module.DistanceGridSearchSpace(
            lookback_bars=(10,),
            entry_z=(1.0,),
            exit_z=(0.5,),
            stop_z=(2.0,),
            bollinger_k=2.0,
        ),
        objective_metric="net_profit",
        point_1=1.0,
        point_2=1.0,
        contract_size_1=1.0,
        contract_size_2=1.0,
        cancel_check=cancel_check,
    )

    assert cancel_calls >= 1
    assert result.evaluated_trials == 0
    assert result.rows == []
