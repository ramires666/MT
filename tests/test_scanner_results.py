from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from app_config import get_settings
from domain.contracts import StrategyDefaults, Timeframe
from domain.optimizer.distance_models import DistanceOptimizationRow
from domain.scan.optimizer_grid_scan import OptimizerGridScanResult, OptimizerGridScanRow, OptimizerGridScanSummary
from storage.scanner_results import (
    build_optimizer_scanner_request_signature,
    clear_optimizer_scanner_scope,
    load_optimizer_scanner_snapshot,
    persist_optimizer_scanner_snapshot,
    scanner_results_path,
)


def _defaults() -> StrategyDefaults:
    return StrategyDefaults(
        initial_capital=10_000.0,
        leverage=100.0,
        margin_budget_per_leg=500.0,
        slippage_points=1.0,
    )


def _search_space() -> dict[str, object]:
    return {
        "lookback_bars": [96],
        "entry_z": [2.0],
        "exit_z": [0.5],
        "stop_z": [3.5],
        "bollinger_k": [2.0],
    }


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


def _result(
    *,
    scope: str,
    rows: list[OptimizerGridScanRow],
    total_pair_candidates: int,
    total_pairs_evaluated: int,
    pairs_with_data: int,
    pairs_with_hits: int,
    total_trials_evaluated: int,
    processed_pair_keys: list[str],
    cancelled: bool = False,
    failure_reason: str | None = None,
) -> OptimizerGridScanResult:
    return OptimizerGridScanResult(
        summary=OptimizerGridScanSummary(
            total_symbols_requested=3,
            loaded_symbols=3,
            total_pair_candidates=int(total_pair_candidates),
            total_pairs_evaluated=int(total_pairs_evaluated),
            pairs_with_data=int(pairs_with_data),
            pairs_with_hits=int(pairs_with_hits),
            total_rows=len(rows),
            total_trials_evaluated=int(total_trials_evaluated),
            min_r_squared=0.9,
            top_n_per_pair=10,
        ),
        rows=rows,
        universe_symbols=["AAA", "BBB", "CCC"],
        universe_scope=scope,
        processed_pair_keys=processed_pair_keys,
        cancelled=cancelled,
        failure_reason=failure_reason,
    )


def _scan_row(scope: str, symbol_1: str, symbol_2: str, pair_rank: int, *, trial_id: int, net: float, r_squared: float) -> OptimizerGridScanRow:
    return OptimizerGridScanRow(
        symbol_1=symbol_1,
        symbol_2=symbol_2,
        pair_rank=pair_rank,
        optimization_row=_optimization_row(trial_id, net=net, r_squared=r_squared),
        universe_scope=scope,
        timeframe=Timeframe.M15.value,
        initial_capital=10_000.0,
        leverage=100.0,
        margin_budget_per_leg=500.0,
        slippage_points=1.0,
        fee_mode="tight_spread",
    )


def test_scanner_results_overwrite_single_scope(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    defaults = _defaults()
    search_space = _search_space()
    signature = build_optimizer_scanner_request_signature(
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
    )

    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="indices",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="indices",
            rows=[_scan_row("indices", "AAA", "BBB", 1, trial_id=1, net=10.0, r_squared=0.91)],
            total_pair_candidates=3,
            total_pairs_evaluated=1,
            pairs_with_data=1,
            pairs_with_hits=1,
            total_trials_evaluated=1,
            processed_pair_keys=["AAA::BBB"],
        ),
    )
    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="indices",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="indices",
            rows=[_scan_row("indices", "AAA", "BBB", 1, trial_id=2, net=25.0, r_squared=0.98)],
            total_pair_candidates=3,
            total_pairs_evaluated=2,
            pairs_with_data=2,
            pairs_with_hits=1,
            total_trials_evaluated=2,
            processed_pair_keys=["AAA::BBB", "AAA::CCC"],
        ),
    )

    snapshot = load_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        search_signature=signature,
        scope="indices",
    )

    assert snapshot is not None
    assert snapshot.result.summary.total_pairs_evaluated == 2
    assert snapshot.result.summary.total_pair_candidates == 3
    assert [row.optimization_row.trial_id for row in snapshot.result.rows] == [2]
    assert snapshot.result.processed_pair_keys == ["AAA::BBB", "AAA::CCC"]


def test_scanner_results_load_all_scopes_as_aggregate_snapshot(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    defaults = _defaults()
    search_space = _search_space()
    signature = build_optimizer_scanner_request_signature(
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
    )

    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="indices",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="indices",
            rows=[_scan_row("indices", "AAA", "BBB", 1, trial_id=1, net=10.0, r_squared=0.91)],
            total_pair_candidates=3,
            total_pairs_evaluated=2,
            pairs_with_data=2,
            pairs_with_hits=1,
            total_trials_evaluated=2,
            processed_pair_keys=["AAA::BBB", "AAA::CCC"],
        ),
    )
    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="stocks",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="stocks",
            rows=[_scan_row("stocks", "DDD", "EEE", 1, trial_id=3, net=40.0, r_squared=0.95)],
            total_pair_candidates=6,
            total_pairs_evaluated=4,
            pairs_with_data=4,
            pairs_with_hits=1,
            total_trials_evaluated=4,
            processed_pair_keys=["DDD::EEE", "DDD::FFF", "EEE::FFF", "DDD::GGG"],
        ),
    )

    snapshot = load_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        search_signature=signature,
        scope=None,
    )

    assert snapshot is not None
    assert snapshot.scope is None
    assert snapshot.result.universe_scope == "all"
    assert snapshot.result.summary.total_pair_candidates == 9
    assert snapshot.result.summary.total_pairs_evaluated == 6
    assert snapshot.result.summary.total_trials_evaluated == 6
    assert [row.universe_scope for row in snapshot.result.rows] == ["stocks", "indices"]
    assert [row.optimization_row.net_profit for row in snapshot.result.rows] == [40.0, 10.0]
    assert sorted(snapshot.result.processed_pair_keys) == [
        "indices:AAA::BBB",
        "indices:AAA::CCC",
        "stocks:DDD::EEE",
        "stocks:DDD::FFF",
        "stocks:DDD::GGG",
        "stocks:EEE::FFF",
    ]


def test_scanner_results_preserve_partial_progress_without_rows_and_can_clear_scope(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    defaults = _defaults()
    search_space = _search_space()
    signature = build_optimizer_scanner_request_signature(
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
    )

    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="fx",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="fx",
            rows=[],
            total_pair_candidates=10,
            total_pairs_evaluated=3,
            pairs_with_data=3,
            pairs_with_hits=0,
            total_trials_evaluated=3,
            processed_pair_keys=["EURUSD::GBPUSD", "EURUSD::USDJPY", "GBPUSD::USDJPY"],
            cancelled=True,
            failure_reason="no_rows_passed_filters",
        ),
    )

    snapshot = load_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        search_signature=signature,
        scope="fx",
    )
    assert snapshot is not None
    assert snapshot.result.summary.total_pairs_evaluated == 3
    assert snapshot.result.summary.total_rows == 0
    assert snapshot.result.failure_reason == "no_rows_passed_filters"
    assert snapshot.result.cancelled is True
    assert snapshot.result.processed_pair_keys == [
        "EURUSD::GBPUSD",
        "EURUSD::USDJPY",
        "GBPUSD::USDJPY",
    ]

    clear_optimizer_scanner_scope(
        broker="bybit_mt5",
        search_signature=signature,
        scope="fx",
    )
    assert load_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        search_signature=signature,
        scope="fx",
    ) is None


def test_scanner_results_loader_tolerates_late_string_failure_reason_after_null_prefix(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()

    defaults = _defaults()
    search_space = _search_space()
    signature = build_optimizer_scanner_request_signature(
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
    )

    persist_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        timeframe=Timeframe.M15,
        train_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        train_ended_at=datetime(2026, 2, 1, tzinfo=UTC),
        oos_started_at=datetime(2026, 2, 1, 0, 15, tzinfo=UTC),
        scope="fx",
        search_signature=signature,
        defaults=defaults,
        search_space=search_space,
        fee_mode="tight_spread",
        result=_result(
            scope="fx",
            rows=[_scan_row("fx", f"S{index:03d}", f"T{index:03d}", 1, trial_id=index + 1, net=10.0 + index, r_squared=0.91) for index in range(120)],
            total_pair_candidates=140,
            total_pairs_evaluated=120,
            pairs_with_data=120,
            pairs_with_hits=120,
            total_trials_evaluated=120,
            processed_pair_keys=[f"S{index:03d}::T{index:03d}" for index in range(120)],
            cancelled=True,
            failure_reason="cancelled",
        ),
    )

    store_path = scanner_results_path("bybit_mt5")
    frame = pl.read_parquet(store_path)
    assert frame.filter(pl.col("failure_reason") == "cancelled").height > 0

    snapshot = load_optimizer_scanner_snapshot(
        broker="bybit_mt5",
        search_signature=signature,
        scope="fx",
    )

    assert snapshot is not None
    assert snapshot.result.cancelled is True
    assert snapshot.result.failure_reason == "cancelled"
