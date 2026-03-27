from datetime import UTC, datetime, timedelta

import polars as pl

from bokeh_app.adapters import empty_backtest_sources, result_to_padded_sources, result_to_sources
from domain.backtest.distance import DistanceBacktestResult


def test_empty_backtest_sources_include_gapless_x_fields() -> None:
    sources = empty_backtest_sources()

    assert "x" in sources["price_1"]
    assert "x" in sources["spread"]
    assert "x" in sources["zscore"]
    assert "x" in sources["equity"]
    assert "entry_x" in sources["trades"]
    assert "x" in sources["markers_1"]


def test_result_to_sources_builds_gapless_bar_index_for_charts_and_trades() -> None:
    start = datetime(2026, 3, 2, 0, 0, tzinfo=UTC)
    times = [start, start + timedelta(minutes=15), start + timedelta(hours=10), start + timedelta(days=3)]
    frame = pl.DataFrame({
        "time": times,
        "close_1": [1.0, 1.1, 1.2, 1.3],
        "close_2": [10.0, 10.1, 10.2, 10.3],
        "spread": [0.1, 0.2, 0.3, 0.4],
        "zscore": [0.0, 0.5, -0.5, 0.25],
        "zscore_upper": [1.0, 1.0, 1.0, 1.0],
        "zscore_lower": [-1.0, -1.0, -1.0, -1.0],
        "equity_total": [10000.0, 10020.0, 10010.0, 10040.0],
        "equity_leg_1": [10000.0, 10010.0, 10005.0, 10020.0],
        "equity_leg_2": [10000.0, 10010.0, 10005.0, 10020.0],
    }).with_columns(pl.col("time").cast(pl.Datetime(time_zone="UTC")))
    trades = pl.DataFrame({
        "entry_time": [times[1]],
        "exit_time": [times[3]],
        "spread_side": ["short_spread"],
        "leg_1_side": ["short"],
        "leg_2_side": ["long"],
        "entry_price_1": [1.1],
        "entry_price_2": [10.1],
        "exit_price_1": [1.3],
        "exit_price_2": [10.3],
        "lots_1": [0.5],
        "lots_2": [0.5],
        "gross_pnl": [25.0],
        "spread_cost_total": [1.0],
        "slippage_cost_total": [0.5],
        "commission_total": [0.25],
        "net_pnl": [23.25],
        "exit_reason": ["signal"],
    }).with_columns(
        pl.col("entry_time").cast(pl.Datetime(time_zone="UTC")),
        pl.col("exit_time").cast(pl.Datetime(time_zone="UTC")),
    )
    result = DistanceBacktestResult(
        frame=frame,
        trades=trades,
        summary={"symbol_1": "AUDCAD+", "symbol_2": "USDIDR"},
    )

    sources = result_to_sources(result)

    assert sources["price_1"]["x"] == [0.0, 1.0, 2.0, 3.0]
    assert sources["equity"]["drawdown_width"] == [0.82, 0.82, 0.82, 0.82]
    assert sources["trades"]["entry_x"] == [1.0]
    assert sources["trades"]["exit_x"] == [3.0]
    assert sources["markers_1"]["x"] == [1.0, 3.0]
    assert sources["segments_2"]["x0"] == [1.0]
    assert sources["segments_2"]["x1"] == [3.0]


def test_result_to_padded_sources_keeps_full_axis_and_flat_equity_before_activation() -> None:
    start = datetime(2026, 3, 2, 0, 0, tzinfo=UTC)
    full_times = [start + timedelta(minutes=15 * idx) for idx in range(4)]
    display_frame = pl.DataFrame(
        {
            "time": full_times,
            "close_1": [1.0, 1.1, 1.2, 1.3],
            "close_2": [10.0, 10.1, 10.2, 10.3],
        }
    ).with_columns(pl.col("time").cast(pl.Datetime(time_zone="UTC")))
    slice_frame = pl.DataFrame(
        {
            "time": full_times[2:],
            "close_1": [1.2, 1.3],
            "close_2": [10.2, 10.3],
            "spread": [0.2, 0.3],
            "zscore": [0.5, -0.25],
            "zscore_upper": [1.0, 1.0],
            "zscore_lower": [-1.0, -1.0],
            "equity_total": [10000.0, 10150.0],
            "equity_leg_1": [10000.0, 10080.0],
            "equity_leg_2": [10000.0, 10070.0],
        }
    ).with_columns(pl.col("time").cast(pl.Datetime(time_zone="UTC")))
    trades = pl.DataFrame(
        {
            "entry_time": [full_times[2]],
            "exit_time": [full_times[3]],
            "spread_side": ["short_spread"],
            "leg_1_side": ["short"],
            "leg_2_side": ["long"],
            "entry_price_1": [1.2],
            "entry_price_2": [10.2],
            "exit_price_1": [1.3],
            "exit_price_2": [10.3],
            "lots_1": [0.5],
            "lots_2": [0.5],
            "gross_pnl": [25.0],
            "spread_cost_total": [1.0],
            "slippage_cost_total": [0.5],
            "commission_total": [0.25],
            "net_pnl": [23.25],
            "exit_reason": ["signal"],
        }
    ).with_columns(
        pl.col("entry_time").cast(pl.Datetime(time_zone="UTC")),
        pl.col("exit_time").cast(pl.Datetime(time_zone="UTC")),
    )
    result = DistanceBacktestResult(
        frame=slice_frame,
        trades=trades,
        summary={"symbol_1": "AUDCAD+", "symbol_2": "USDIDR"},
    )

    sources = result_to_padded_sources(result, display_frame, initial_capital=10000.0)

    assert sources["price_1"]["x"] == [0.0, 1.0, 2.0, 3.0]
    assert sources["price_1"]["price"] == [1.0, 1.1, 1.2, 1.3]
    assert sources["equity"]["total"] == [10000.0, 10000.0, 10000.0, 10150.0]
    assert str(sources["spread"]["spread"][0]) == "nan"
    assert sources["spread"]["spread"][2:] == [0.2, 0.3]
    assert sources["trades"]["entry_x"] == [2.0]
    assert sources["trades"]["exit_x"] == [3.0]
