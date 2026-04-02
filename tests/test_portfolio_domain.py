from __future__ import annotations

from datetime import UTC, datetime

from domain.portfolio import (
    PortfolioCurve,
    analyze_portfolio_curves,
    combine_portfolio_equity_curves,
    summarize_portfolio_equity_series,
    latest_portfolio_oos_started_at,
    materialize_portfolio_backtest_allocations,
    portfolio_analysis_window,
    portfolio_strategy_started_at,
    prepend_flat_equity_prefix,
    scale_defaults_for_portfolio_item,
)
from storage.portfolio_store import PortfolioItem
from domain.contracts import Timeframe


def _item(
    *,
    item_id: str,
    source_kind: str = "tester",
    oos_started_at: datetime | None = None,
    context_started_at: datetime | None = None,
    initial_capital: float = 10_000.0,
    margin_budget_per_leg: float = 500.0,
) -> PortfolioItem:
    return PortfolioItem(
        item_id=item_id,
        item_signature=item_id,
        saved_at=datetime(2026, 3, 23, tzinfo=UTC),
        source_kind=source_kind,
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe=Timeframe.M15,
        algorithm="distance",
        lookback_bars=48,
        entry_z=2.0,
        exit_z=0.5,
        stop_z=3.5,
        bollinger_k=2.0,
        initial_capital=initial_capital,
        leverage=100.0,
        margin_budget_per_leg=margin_budget_per_leg,
        slippage_points=1.0,
        fee_mode="tight_spread",
        oos_started_at=oos_started_at,
        context_started_at=context_started_at,
    )


def test_scale_defaults_for_portfolio_item_scales_margin_budget() -> None:
    item = _item(item_id="a", initial_capital=10_000.0, margin_budget_per_leg=500.0)

    defaults = scale_defaults_for_portfolio_item(item, allocation_capital=2_500.0)

    assert defaults.initial_capital == 2_500.0
    assert defaults.margin_budget_per_leg == 125.0
    assert defaults.leverage == 100.0


def test_latest_portfolio_oos_started_at_returns_max_non_null() -> None:
    items = [
        _item(item_id="a", oos_started_at=datetime(2026, 1, 1, tzinfo=UTC)),
        _item(item_id="b", oos_started_at=datetime(2026, 2, 1, tzinfo=UTC)),
        _item(item_id="c", oos_started_at=None),
    ]

    assert latest_portfolio_oos_started_at(items) == datetime(2026, 2, 1, tzinfo=UTC)


def test_portfolio_strategy_started_at_uses_context_for_optimizer_rows() -> None:
    item = _item(
        item_id="a",
        source_kind="optimization_row",
        context_started_at=datetime(2026, 2, 1, tzinfo=UTC),
    )

    assert portfolio_strategy_started_at(
        item,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 1, tzinfo=UTC),
    ) == datetime(2026, 2, 1, tzinfo=UTC)


def test_portfolio_strategy_started_at_keeps_manual_rows_on_selected_period() -> None:
    item = _item(
        item_id="a",
        source_kind="tester_manual",
        context_started_at=datetime(2026, 2, 1, tzinfo=UTC),
    )

    assert portfolio_strategy_started_at(
        item,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 1, tzinfo=UTC),
    ) == datetime(2026, 1, 1, tzinfo=UTC)


def test_portfolio_analysis_window_clips_context_rows_to_in_sample_before_oos() -> None:
    item = _item(
        item_id="a",
        source_kind="optimization_row",
        oos_started_at=datetime(2026, 2, 15, tzinfo=UTC),
        context_started_at=datetime(2026, 2, 1, tzinfo=UTC),
    )

    assert portfolio_analysis_window(
        item,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 1, tzinfo=UTC),
    ) == (
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 15, tzinfo=UTC),
    )


def test_portfolio_analysis_window_uses_pre_oos_history_for_meta_fold_rows() -> None:
    item = _item(
        item_id="a",
        source_kind="meta_selected_fold",
        oos_started_at=datetime(2026, 2, 15, tzinfo=UTC),
        context_started_at=datetime(2026, 2, 15, tzinfo=UTC),
    )

    assert portfolio_analysis_window(
        item,
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 1, tzinfo=UTC),
    ) == (
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 15, tzinfo=UTC),
    )


def test_prepend_flat_equity_prefix_adds_flat_segment_before_context_start() -> None:
    times, equities = prepend_flat_equity_prefix(
        [datetime(2026, 2, 3, tzinfo=UTC)],
        [10_250.0],
        period_started_at=datetime(2026, 1, 1, tzinfo=UTC),
        strategy_started_at=datetime(2026, 2, 1, tzinfo=UTC),
        initial_capital=10_000.0,
    )

    assert times == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
        datetime(2026, 2, 3, tzinfo=UTC),
    ]
    assert equities == [10_000.0, 10_000.0, 10_250.0]


def test_materialize_portfolio_backtest_allocations_uses_fallback_for_zero_rows() -> None:
    allocations = materialize_portfolio_backtest_allocations(
        ["a", "b", "c"],
        allocation_capitals_by_id={"a": 2_500.0, "b": 0.0},
        fallback_allocation_capital=1_250.0,
    )

    assert allocations == {
        "a": 2_500.0,
        "b": 1_250.0,
        "c": 1_250.0,
    }


def test_materialize_portfolio_backtest_allocations_clamps_negative_values() -> None:
    allocations = materialize_portfolio_backtest_allocations(
        ["a"],
        allocation_capitals_by_id={"a": -50.0},
        fallback_allocation_capital=900.0,
    )

    assert allocations == {"a": 900.0}


def test_combine_portfolio_equity_curves_forward_fills_each_pair() -> None:
    combined = combine_portfolio_equity_curves(
        [
            PortfolioCurve(
                item_id="a",
                symbol_1="AUDUSD+",
                symbol_2="CADCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[100.0, 105.0],
            ),
            PortfolioCurve(
                item_id="b",
                symbol_1="NZDUSD+",
                symbol_2="USDCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=200.0,
                times=[
                    datetime(2026, 1, 1, 12, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[200.0, 190.0],
            ),
        ]
    )

    assert combined.get_column("time").to_list() == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, 12, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
    ]
    assert combined.get_column("equity").to_list() == [300.0, 300.0, 295.0]


def test_combine_portfolio_equity_curves_respects_included_item_ids() -> None:
    combined = combine_portfolio_equity_curves(
        [
            PortfolioCurve(
                item_id="a",
                symbol_1="AUDUSD+",
                symbol_2="CADCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[100.0, 110.0],
            ),
            PortfolioCurve(
                item_id="b",
                symbol_1="NZDUSD+",
                symbol_2="USDCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=200.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[200.0, 190.0],
            ),
        ],
        included_item_ids={"b"},
    )

    assert combined.get_column("time").to_list() == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
    ]
    assert combined.get_column("equity").to_list() == [200.0, 190.0]


def test_combine_portfolio_equity_curves_scales_raw_curves_by_allocation() -> None:
    combined = combine_portfolio_equity_curves(
        [
            PortfolioCurve(
                item_id="a",
                symbol_1="AUDUSD+",
                symbol_2="CADCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[100.0, 110.0],
            ),
            PortfolioCurve(
                item_id="b",
                symbol_1="NZDUSD+",
                symbol_2="USDCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=200.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[200.0, 190.0],
            ),
        ],
        allocation_capitals_by_id={"a": 300.0, "b": 100.0},
    )

    assert combined.get_column("time").to_list() == [
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 2, tzinfo=UTC),
    ]
    assert combined.get_column("equity").to_list() == [400.0, 425.0]


def test_combine_portfolio_equity_curves_aggregates_risk_load_and_open_positions() -> None:
    combined = combine_portfolio_equity_curves(
        [
            PortfolioCurve(
                item_id="a",
                symbol_1="AUDUSD+",
                symbol_2="CADCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[100.0, 95.0],
                unrealized_drawdowns=[0.0, -5.0],
                capital_loads=[0.0, 20.0],
                open_positions=[0, 1],
            ),
            PortfolioCurve(
                item_id="b",
                symbol_1="NZDUSD+",
                symbol_2="USDCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=200.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                ],
                equities=[200.0, 190.0],
                unrealized_drawdowns=[0.0, -10.0],
                capital_loads=[0.0, 40.0],
                open_positions=[0, 1],
            ),
        ]
    )

    assert combined.get_column("equity").to_list() == [300.0, 285.0]
    assert combined.get_column("unrealized_drawdown").to_list() == [0.0, -15.0]
    assert combined.get_column("capital_load").to_list() == [0.0, 60.0]
    assert combined.get_column("open_positions").to_list() == [0, 2]


def test_summarize_portfolio_equity_series_reports_combined_metrics() -> None:
    summary = summarize_portfolio_equity_series(
        times=[
            datetime(2026, 1, 1, tzinfo=UTC),
            datetime(2026, 1, 2, tzinfo=UTC),
            datetime(2026, 1, 3, tzinfo=UTC),
        ],
        equities=[1_000.0, 980.0, 1_050.0],
        unrealized_drawdowns=[0.0, -30.0, 0.0],
        capital_loads=[0.0, 200.0, 100.0],
        open_positions=[0, 2, 1],
        trades_count=7,
    )

    assert summary.initial_capital == 1_000.0
    assert summary.net_profit == 50.0
    assert summary.ending_equity == 1_050.0
    assert summary.max_drawdown == 20.0
    assert summary.trades == 7
    assert summary.max_unrealized_drawdown == 30.0
    assert summary.avg_unrealized_drawdown == 30.0
    assert summary.max_capital_load == 200.0
    assert summary.avg_capital_load == 150.0
    assert summary.max_capital_load_pct == 0.2
    assert summary.avg_capital_load_pct == 0.15
    assert summary.max_open_positions == 2
    assert summary.avg_open_positions == 1.5


def test_analyze_portfolio_curves_returns_pairwise_corrs_and_weights() -> None:
    pairwise_rows, suggestion_rows = analyze_portfolio_curves(
        [
            PortfolioCurve(
                item_id="a",
                symbol_1="AUDUSD+",
                symbol_2="CADCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                    datetime(2026, 1, 3, tzinfo=UTC),
                ],
                equities=[100.0, 110.0, 121.0],
            ),
            PortfolioCurve(
                item_id="b",
                symbol_1="NZDUSD+",
                symbol_2="USDCHF+",
                timeframe=Timeframe.M15.value,
                initial_capital=100.0,
                times=[
                    datetime(2026, 1, 1, tzinfo=UTC),
                    datetime(2026, 1, 2, tzinfo=UTC),
                    datetime(2026, 1, 3, tzinfo=UTC),
                ],
                equities=[100.0, 105.0, 110.25],
            ),
        ]
    )

    assert len(pairwise_rows) == 1
    assert pairwise_rows[0].left_item_id == "a"
    assert pairwise_rows[0].right_item_id == "b"
    assert pairwise_rows[0].equity_corr >= 0.99
    assert pairwise_rows[0].return_corr == 0.0
    assert len(suggestion_rows) == 2
    assert abs(sum(row.suggested_weight for row in suggestion_rows) - 1.0) <= 1e-9
    assert all(row.suggested_weight > 0.0 for row in suggestion_rows)
