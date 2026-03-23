from __future__ import annotations

from datetime import UTC, datetime

from domain.portfolio import (
    PortfolioCurve,
    analyze_portfolio_curves,
    combine_portfolio_equity_curves,
    latest_portfolio_oos_started_at,
    scale_defaults_for_portfolio_item,
)
from storage.portfolio_store import PortfolioItem
from domain.contracts import Timeframe


def _item(
    *,
    item_id: str,
    oos_started_at: datetime | None = None,
    initial_capital: float = 10_000.0,
    margin_budget_per_leg: float = 500.0,
) -> PortfolioItem:
    return PortfolioItem(
        item_id=item_id,
        item_signature=item_id,
        saved_at=datetime(2026, 3, 23, tzinfo=UTC),
        source_kind="tester",
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
