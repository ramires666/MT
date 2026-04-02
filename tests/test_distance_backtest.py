from datetime import UTC, datetime, timedelta

import polars as pl

from domain.backtest.distance import (
    DistanceParameters,
    prepare_distance_backtest_context,
    run_distance_backtest_frame,
    run_distance_backtest_metrics_frame,
)
from domain.contracts import PairSelection, StrategyDefaults
from domain.optimizer.distance_metrics import equity_metrics


def test_distance_backtest_generates_trade_and_equity_columns() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 100.0, 100.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [2] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [2] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="US2000", symbol_2="NAS100"),
        defaults=StrategyDefaults(),
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=3.0),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1={"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01},
        spec_2={"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01},
    )

    assert result.frame.height == frame.height
    assert {"spread", "zscore", "equity_total", "equity_leg_1", "equity_leg_2", "position"}.issubset(set(result.frame.columns))
    assert result.summary["trades"] >= 1
    assert not result.trades.is_empty()
    assert {"gross_pnl", "spread_cost_total", "slippage_cost_total", "commission_total", "net_pnl"}.issubset(set(result.trades.columns))
    first_trade = result.trades.to_dicts()[0]
    assert float(first_trade["gross_pnl"]) >= float(first_trade["net_pnl"])
    assert float(result.summary["gross_pnl"]) >= float(result.summary["net_pnl"])


def test_distance_backtest_metrics_fast_path_matches_full_result() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 100.0, 100.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [2] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [2] * 12,
            "real_volume_2": [10] * 12,
        }
    )
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    defaults = StrategyDefaults()
    params = DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=3.0)
    spec_1 = {"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01}
    spec_2 = {"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01}

    full = run_distance_backtest_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1=spec_1,
        spec_2=spec_2,
    )
    context = prepare_distance_backtest_context(
        frame=frame,
        pair=pair,
        defaults=defaults,
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1=spec_1,
        spec_2=spec_2,
    )
    fast = run_distance_backtest_metrics_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=params,
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1=spec_1,
        spec_2=spec_2,
        context=context,
    )
    expected = equity_metrics(full)

    assert fast["trades"] == int(full.summary["trades"])
    assert round(float(fast["win_rate"]), 8) == round(float(full.summary["win_rate"]), 8)
    for key in (
        "net_profit",
        "ending_equity",
        "max_drawdown",
        "pnl_to_maxdd",
        "omega_ratio",
        "k_ratio",
        "score_log_trades",
        "ulcer_index",
        "ulcer_performance",
        "cagr",
        "cagr_to_ulcer",
        "r_squared",
        "calmar",
        "beauty_score",
        "gross_profit",
        "spread_cost",
        "slippage_cost",
        "commission_cost",
        "total_cost",
    ):
        assert round(float(fast[key]), 8) == round(float(expected[key]), 8)


def test_distance_backtest_supports_opposite_signal_exit_when_exit_z_is_negative() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 111.0, 110.0, 109.0, 108.0, 108.0, 108.0, 108.0, 108.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [2] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [2] * 12,
            "real_volume_2": [10] * 12,
        }
    )
    pair = PairSelection(symbol_1="US2000", symbol_2="NAS100")
    defaults = StrategyDefaults()
    spec_1 = {"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01}
    spec_2 = {"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01}

    reverted = run_distance_backtest_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=None),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1=spec_1,
        spec_2=spec_2,
    )
    opposite = run_distance_backtest_frame(
        frame=frame,
        pair=pair,
        defaults=defaults,
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=-1.0, stop_z=None),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1=spec_1,
        spec_2=spec_2,
    )

    reverted_first = reverted.trades.to_dicts()[0]
    opposite_first = opposite.trades.to_dicts()[0]

    assert reverted_first["exit_time"] == times[6]
    assert reverted_first["exit_reason"] == "mean_reversion"
    assert opposite_first["exit_time"] == times[7]
    assert opposite_first["exit_reason"] == "opposite_signal"
    assert float(opposite_first["net_pnl"]) > float(reverted_first["net_pnl"])


def test_distance_backtest_applies_per_lot_commission() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 100.0, 100.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [1] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [1] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="US2000", symbol_2="NAS100"),
        defaults=StrategyDefaults(),
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=3.0),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1={"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01, "commission_mode": "per_lot_per_side", "commission_value": 2.5},
        spec_2={"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01, "commission_mode": "per_lot_per_side", "commission_value": 2.5},
    )

    assert float(result.summary["total_commission"]) > 0.0
    assert float(result.trades.to_dicts()[0]["commission_total"]) > 0.0


def test_distance_backtest_applies_entry_only_commission_minimum() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [1.10, 1.10, 1.10, 1.20, 1.22, 1.18, 1.12, 1.10, 1.09, 1.10, 1.10, 1.10]
    close_2 = [1.00] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [1] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [1] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="AUDCAD+", symbol_2="AUDCHF+"),
        defaults=StrategyDefaults(margin_budget_per_leg=500.0),
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=3.0),
        point_1=0.00001,
        point_2=0.00001,
        contract_size_1=100000.0,
        contract_size_2=100000.0,
        spec_1={
            "symbol": "AUDCAD+",
            "point": 0.00001,
            "contract_size": 100000.0,
            "trade_tick_size": 0.00001,
            "trade_tick_value": 1.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "commission_mode": "per_lot_round_turn",
            "commission_value": 0.02,
            "commission_minimum": 0.2,
            "commission_entry_only": True,
        },
        spec_2={
            "symbol": "AUDCHF+",
            "point": 0.00001,
            "contract_size": 100000.0,
            "trade_tick_size": 0.00001,
            "trade_tick_value": 1.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "commission_mode": "per_lot_round_turn",
            "commission_value": 0.02,
            "commission_minimum": 0.2,
            "commission_entry_only": True,
        },
    )

    first_trade = result.trades.to_dicts()[0]
    assert float(first_trade["commission_total"]) >= 0.4
    assert float(result.summary["total_commission"]) >= 0.4


def test_distance_backtest_supports_disabled_stop_z() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 100.0, 100.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [2] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [2] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="US2000", symbol_2="NAS100"),
        defaults=StrategyDefaults(),
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=None),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1={"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01},
        spec_2={"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01},
    )

    assert result.summary["trades"] >= 1
    assert not result.trades.is_empty()
    assert all(reason != "stop_z" for reason in result.trades.get_column("exit_reason").to_list())


def test_distance_backtest_net_matches_gross_minus_costs() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [100.0, 100.0, 100.0, 110.0, 112.0, 108.0, 102.0, 100.0, 99.0, 100.0, 100.0, 100.0]
    close_2 = [100.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [1] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [1] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    defaults = StrategyDefaults()
    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="US2000", symbol_2="NAS100"),
        defaults=defaults,
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=3.0),
        point_1=0.01,
        point_2=0.01,
        contract_size_1=1.0,
        contract_size_2=1.0,
        spec_1={"symbol": "US2000", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01, "commission_mode": "per_lot_per_side", "commission_value": 2.5},
        spec_2={"symbol": "NAS100", "point": 0.01, "contract_size": 1.0, "trade_tick_size": 0.01, "trade_tick_value": 1.0, "volume_min": 0.01, "volume_step": 0.01, "commission_mode": "per_lot_per_side", "commission_value": 2.5},
    )

    summary = result.summary
    gross = float(summary["gross_pnl"])
    total_cost = float(summary["total_cost"])
    net = float(summary["net_pnl"])
    assert abs((gross - total_cost) - net) < 1e-9
    assert abs((float(summary["ending_equity"]) - float(summary["initial_capital"])) - net) < 1e-9


def test_distance_backtest_uses_forex_margin_basis_for_high_price_fx_leg() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(12)]
    close_1 = [1.10, 1.10, 1.10, 1.20, 1.22, 1.18, 1.12, 1.10, 1.09, 1.10, 1.10, 1.10]
    close_2 = [15000.0] * 12
    frame = pl.DataFrame(
        {
            "time": times,
            "open_1": close_1,
            "high_1": close_1,
            "low_1": close_1,
            "close_1": close_1,
            "tick_volume_1": [100] * 12,
            "spread_1": [1] * 12,
            "real_volume_1": [10] * 12,
            "open_2": close_2,
            "high_2": close_2,
            "low_2": close_2,
            "close_2": close_2,
            "tick_volume_2": [100] * 12,
            "spread_2": [1] * 12,
            "real_volume_2": [10] * 12,
        }
    )

    result = run_distance_backtest_frame(
        frame=frame,
        pair=PairSelection(symbol_1="AUDCAD+", symbol_2="USDIDR"),
        defaults=StrategyDefaults(margin_budget_per_leg=500.0, leverage=100.0),
        params=DistanceParameters(lookback_bars=3, entry_z=1.0, exit_z=0.2, stop_z=None),
        point_1=0.00001,
        point_2=0.1,
        contract_size_1=100000.0,
        contract_size_2=100000.0,
        spec_1={
            "symbol": "AUDCAD+",
            "point": 0.00001,
            "contract_size": 100000.0,
            "trade_tick_size": 0.00001,
            "trade_tick_value": 1.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "margin_initial": 100000.0,
            "trade_calc_mode": 5,
        },
        spec_2={
            "symbol": "USDIDR",
            "point": 0.1,
            "contract_size": 100000.0,
            "trade_tick_size": 0.1,
            "trade_tick_value": 1.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "margin_initial": 0.0,
            "trade_calc_mode": 5,
        },
    )

    first_trade = result.trades.to_dicts()[0]
    assert float(first_trade["lots_1"]) == 0.5
    assert float(first_trade["lots_2"]) == 0.5
