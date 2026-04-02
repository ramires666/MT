from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

import polars as pl

from bokeh_app.adapters import optimizer_scan_results_to_source
from domain.contracts import PairSelection, ScanUniverseMode, StrategyDefaults, Timeframe
from domain.optimizer.distance_models import DistanceOptimizationResult, DistanceOptimizationRow
from domain.scan.optimizer_grid_scan import (
    OptimizerGridScanResult,
    OptimizerGridScanRow,
    OptimizerGridScanSummary,
    scan_symbol_frames_optimizer_grid,
    scan_universe_optimizer_grid,
)


def _quote_frame() -> pl.DataFrame:
    times = [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * index) for index in range(6)]
    values = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    return pl.DataFrame(
        {
            "time": times,
            "open": values,
            "high": values,
            "low": values,
            "close": values,
            "tick_volume": [100] * len(values),
            "spread": [2] * len(values),
            "real_volume": [10] * len(values),
        }
    ).with_columns(pl.col("time").cast(pl.Datetime(time_zone="UTC")))


def _optimization_row(trial_id: int, *, net: float, r_squared: float, trades: int = 12) -> DistanceOptimizationRow:
    return DistanceOptimizationRow(
        trial_id=trial_id,
        objective_metric="net_profit",
        objective_score=float(net),
        net_profit=float(net),
        ending_equity=10_000.0 + float(net),
        max_drawdown=abs(float(net)) / 10.0,
        pnl_to_maxdd=1.0,
        omega_ratio=1.1,
        k_ratio=0.9,
        score_log_trades=0.7,
        ulcer_index=0.2,
        ulcer_performance=0.5,
        cagr=0.3,
        cagr_to_ulcer=1.5,
        r_squared=float(r_squared),
        calmar=0.8,
        beauty_score=1.2,
        trades=int(trades),
        win_rate=0.55,
        lookback_bars=96,
        entry_z=2.0,
        exit_z=0.5,
        stop_z=3.5,
        bollinger_k=2.0,
        gross_profit=float(net) + 5.0,
        spread_cost=1.0,
        slippage_cost=0.5,
        commission_cost=0.25,
        total_cost=1.75,
    )


def test_scan_symbol_frames_optimizer_grid_keeps_top_ten_positive_rows_per_pair(monkeypatch) -> None:
    symbol_frames = {"AAA": _quote_frame(), "BBB": _quote_frame(), "CCC": _quote_frame()}
    specs = {symbol: {"point": 0.01, "contract_size": 1.0} for symbol in symbol_frames}

    def fake_optimize_distance_grid_frame(*, pair, **_kwargs):
        assert isinstance(pair, PairSelection)
        pair_key = (pair.symbol_1, pair.symbol_2)
        if pair_key == ("AAA", "BBB"):
            rows = [_optimization_row(index, net=float(index), r_squared=0.91) for index in range(1, 13)]
            rows.append(_optimization_row(77, net=500.0, r_squared=0.97, trades=0))
            rows.append(_optimization_row(78, net=0.0, r_squared=0.99))
            rows.append(_optimization_row(99, net=999.0, r_squared=0.89))
        elif pair_key == ("AAA", "CCC"):
            rows = [_optimization_row(21, net=50.0, r_squared=0.88)]
        else:
            rows = [_optimization_row(31, net=2.5, r_squared=0.95)]
        rows.sort(key=lambda row: row.objective_score, reverse=True)
        return DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=len(rows),
            rows=rows,
            best_trial_id=rows[0].trial_id if rows else None,
        )

    monkeypatch.setattr("domain.scan.optimizer_grid_scan.optimize_distance_grid_frame", fake_optimize_distance_grid_frame)

    result = scan_symbol_frames_optimizer_grid(
        symbol_frames=symbol_frames,
        specs_by_symbol=specs,
        defaults=StrategyDefaults(),
        search_space={"lookback_bars": [96], "entry_z": [2.0], "exit_z": [0.5], "stop_z": [3.5], "bollinger_k": [2.0]},
        min_r_squared=0.9,
        top_n_per_pair=10,
        parallel_workers=8,
    )

    assert result.failure_reason is None
    assert result.summary.total_symbols_requested == 3
    assert result.summary.loaded_symbols == 3
    assert result.summary.total_pairs_evaluated == 3
    assert result.summary.pairs_with_data == 3
    assert result.summary.pairs_with_hits == 3
    assert result.summary.total_rows == 12
    assert result.summary.total_trials_evaluated == 17
    assert [(row.symbol_1, row.symbol_2, row.pair_rank, row.optimization_row.net_profit) for row in result.rows[:3]] == [
        ("AAA", "BBB", 1, 999.0),
        ("AAA", "CCC", 1, 50.0),
        ("AAA", "BBB", 2, 12.0),
    ]
    assert result.rows[-1].symbol_1 == "BBB"
    assert result.rows[-1].symbol_2 == "CCC"
    assert result.rows[-1].optimization_row.net_profit == 2.5
    assert all(float(row.optimization_row.net_profit) > 0.0 for row in result.rows)
    assert all(int(row.optimization_row.trades) > 0 for row in result.rows)
    assert any(float(row.optimization_row.r_squared) < 0.9 for row in result.rows)


def test_scan_universe_optimizer_grid_reuses_single_shared_executor_for_all_pairs(monkeypatch) -> None:
    created = 0
    seen_executor_ids: list[int] = []

    def fake_resolve_scan_symbols(**_kwargs):
        return ["AAA", "BBB", "CCC"]

    def fake_load_symbol_quote_frame(**_kwargs):
        return _quote_frame()

    def fake_load_instrument_spec(_broker, _symbol):
        return {"point": 0.01, "contract_size": 1.0}

    def fake_build_process_pool(max_workers=None):
        nonlocal created
        created += 1
        return ThreadPoolExecutor(max_workers=max_workers)

    def fake_optimize_distance_grid_frame(*, parallel_executor, **_kwargs):
        assert parallel_executor is not None
        seen_executor_ids.append(id(parallel_executor))
        rows = [_optimization_row(1, net=5.0, r_squared=0.95)]
        return DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=1,
            rows=rows,
            best_trial_id=1,
        )

    monkeypatch.setattr("domain.scan.optimizer_grid_scan.resolve_scan_symbols", fake_resolve_scan_symbols)
    monkeypatch.setattr("domain.scan.optimizer_grid_scan.load_symbol_quote_frame", fake_load_symbol_quote_frame)
    monkeypatch.setattr("domain.scan.optimizer_grid_scan.load_instrument_spec", fake_load_instrument_spec)
    monkeypatch.setattr("workers.executor.build_process_pool", fake_build_process_pool)
    monkeypatch.setattr("domain.scan.optimizer_grid_scan.optimize_distance_grid_frame", fake_optimize_distance_grid_frame)

    result = scan_universe_optimizer_grid(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        universe_mode=ScanUniverseMode.ALL,
        defaults=StrategyDefaults(),
        search_space={"lookback_bars": [96], "entry_z": [2.0], "exit_z": [0.5], "stop_z": [3.5], "bollinger_k": [2.0]},
        parallel_workers=2,
    )

    assert result.summary.total_pairs_evaluated == 3
    assert created == 1
    assert len(seen_executor_ids) == 3
    assert len(set(seen_executor_ids)) == 1


def test_scan_symbol_frames_optimizer_grid_emits_partial_results_after_each_pair(monkeypatch) -> None:
    symbol_frames = {"AAA": _quote_frame(), "BBB": _quote_frame(), "CCC": _quote_frame()}
    specs = {symbol: {"point": 0.01, "contract_size": 1.0} for symbol in symbol_frames}
    partial_counts: list[tuple[int, int]] = []

    def fake_optimize_distance_grid_frame(*, pair, **_kwargs):
        pair_key = (pair.symbol_1, pair.symbol_2)
        rows = [] if pair_key == ("AAA", "CCC") else [_optimization_row(1, net=5.0, r_squared=0.95)]
        return DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=max(1, len(rows)),
            rows=rows,
            best_trial_id=rows[0].trial_id if rows else None,
        )

    monkeypatch.setattr("domain.scan.optimizer_grid_scan.optimize_distance_grid_frame", fake_optimize_distance_grid_frame)

    scan_symbol_frames_optimizer_grid(
        symbol_frames=symbol_frames,
        specs_by_symbol=specs,
        defaults=StrategyDefaults(),
        search_space={"lookback_bars": [96], "entry_z": [2.0], "exit_z": [0.5], "stop_z": [3.5], "bollinger_k": [2.0]},
        min_r_squared=0.9,
        top_n_per_pair=10,
        partial_result_callback=lambda result: partial_counts.append((result.summary.total_pairs_evaluated, result.summary.total_rows)),
    )

    assert partial_counts == [(1, 1), (2, 1), (3, 2)]


def test_scan_symbol_frames_optimizer_grid_partial_results_are_sorted_without_global_trim(monkeypatch) -> None:
    symbol_frames = {"AAA": _quote_frame(), "BBB": _quote_frame(), "CCC": _quote_frame()}
    specs = {symbol: {"point": 0.01, "contract_size": 1.0} for symbol in symbol_frames}
    snapshots: list[list[float]] = []

    def fake_optimize_distance_grid_frame(*, pair, **_kwargs):
        pair_key = (pair.symbol_1, pair.symbol_2)
        if pair_key == ("AAA", "BBB"):
            rows = [_optimization_row(index, net=float(index), r_squared=0.95) for index in range(1, 8)]
        elif pair_key == ("AAA", "CCC"):
            rows = [_optimization_row(20 + index, net=100.0 + float(index), r_squared=0.96) for index in range(1, 8)]
        else:
            rows = [_optimization_row(40 + index, net=200.0 + float(index), r_squared=0.97) for index in range(1, 8)]
        rows.sort(key=lambda row: row.objective_score, reverse=True)
        return DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=len(rows),
            rows=rows,
            best_trial_id=rows[0].trial_id if rows else None,
        )

    monkeypatch.setattr("domain.scan.optimizer_grid_scan.optimize_distance_grid_frame", fake_optimize_distance_grid_frame)

    scan_symbol_frames_optimizer_grid(
        symbol_frames=symbol_frames,
        specs_by_symbol=specs,
        defaults=StrategyDefaults(),
        search_space={"lookback_bars": [96], "entry_z": [2.0], "exit_z": [0.5], "stop_z": [3.5], "bollinger_k": [2.0]},
        min_r_squared=0.9,
        top_n_per_pair=10,
        partial_result_callback=lambda result: snapshots.append([float(row.optimization_row.net_profit) for row in result.rows]),
    )

    assert snapshots[0] == [7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
    assert snapshots[1] == [107.0, 106.0, 105.0, 104.0, 103.0, 102.0, 101.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
    assert snapshots[2] == [
        207.0,
        206.0,
        205.0,
        204.0,
        203.0,
        202.0,
        201.0,
        107.0,
        106.0,
        105.0,
        104.0,
        103.0,
        102.0,
        101.0,
        7.0,
        6.0,
        5.0,
        4.0,
        3.0,
        2.0,
        1.0,
    ]


def test_scan_symbol_frames_optimizer_grid_respects_allowed_pair_keys(monkeypatch) -> None:
    symbol_frames = {"AAA": _quote_frame(), "BBB": _quote_frame(), "CCC": _quote_frame()}
    specs = {symbol: {"point": 0.01, "contract_size": 1.0} for symbol in symbol_frames}
    seen_pairs: list[tuple[str, str]] = []

    def fake_optimize_distance_grid_frame(*, pair, **_kwargs):
        seen_pairs.append((pair.symbol_1, pair.symbol_2))
        rows = [_optimization_row(1, net=5.0, r_squared=0.95)]
        return DistanceOptimizationResult(
            objective_metric="net_profit",
            evaluated_trials=1,
            rows=rows,
            best_trial_id=1,
        )

    monkeypatch.setattr("domain.scan.optimizer_grid_scan.optimize_distance_grid_frame", fake_optimize_distance_grid_frame)

    result = scan_symbol_frames_optimizer_grid(
        symbol_frames=symbol_frames,
        specs_by_symbol=specs,
        defaults=StrategyDefaults(),
        search_space={"lookback_bars": [96], "entry_z": [2.0], "exit_z": [0.5], "stop_z": [3.5], "bollinger_k": [2.0]},
        allowed_pair_keys=["AAA::CCC"],
    )

    assert seen_pairs == [("AAA", "CCC")]
    assert result.summary.total_pair_candidates == 1
    assert result.summary.total_pairs_evaluated == 1
    assert [(row.symbol_1, row.symbol_2) for row in result.rows] == [("AAA", "CCC")]


def test_optimizer_scan_results_to_source_includes_pair_columns_and_ranks() -> None:
    result = OptimizerGridScanResult(
        summary=OptimizerGridScanSummary(
            total_symbols_requested=2,
            loaded_symbols=2,
            total_pair_candidates=1,
            total_pairs_evaluated=1,
            pairs_with_data=1,
            pairs_with_hits=1,
            total_rows=1,
            total_trials_evaluated=1,
            min_r_squared=0.9,
            top_n_per_pair=10,
        ),
        rows=[
            OptimizerGridScanRow(
                symbol_1="AAA",
                symbol_2="BBB",
                pair_rank=1,
                optimization_row=_optimization_row(7, net=12.5, r_squared=0.97),
                universe_scope="indices",
            )
        ],
        universe_symbols=["AAA", "BBB"],
        universe_scope="indices",
        processed_pair_keys=["AAA::BBB"],
    )

    source = optimizer_scan_results_to_source(result)

    assert source["global_rank"] == [1]
    assert source["pair_rank"] == [1]
    assert source["universe_scope"] == ["indices"]
    assert source["symbol_1"] == ["AAA"]
    assert source["symbol_2"] == ["BBB"]
    assert source["timeframe"] == ["M15"]
    assert source["initial_capital"] == [10000.0]
    assert source["leverage"] == [100.0]
    assert source["margin_budget_per_leg"] == [500.0]
    assert source["slippage_points"] == [1.0]
    assert source["fee_mode"] == [""]
    assert source["trial_id"] == [7]
    assert source["net_profit"] == [12.5]
    assert source["r_squared"] == [0.97]
