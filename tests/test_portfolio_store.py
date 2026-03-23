from __future__ import annotations

from datetime import UTC, datetime

from domain.backtest.distance import DistanceParameters
from domain.contracts import StrategyDefaults, Timeframe
from storage import portfolio_store
from storage.portfolio_store import build_portfolio_item, load_portfolio_items, remove_portfolio_items, upsert_portfolio_item


def test_portfolio_store_upsert_updates_existing_signature(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(portfolio_store, "portfolio_items_path", lambda: tmp_path / "portfolio_items.csv")
    params = DistanceParameters(lookback_bars=48, entry_z=2.0, exit_z=0.5, stop_z=3.5, bollinger_k=2.0)
    defaults = StrategyDefaults(initial_capital=10_000.0, leverage=100.0, margin_budget_per_leg=500.0, slippage_points=1.0)

    first = build_portfolio_item(
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe=Timeframe.M15,
        params=params,
        defaults=defaults,
        fee_mode="tight_spread",
        source_kind="tester",
        oos_started_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    second = build_portfolio_item(
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe=Timeframe.M15,
        params=params,
        defaults=defaults,
        fee_mode="zero_fee",
        source_kind="optimization_row",
        oos_started_at=datetime(2026, 2, 1, tzinfo=UTC),
        saved_at=datetime(2026, 3, 1, tzinfo=UTC),
    )

    _, created_first = upsert_portfolio_item(first)
    stored_second, created_second = upsert_portfolio_item(second)
    items = load_portfolio_items()

    assert created_first is True
    assert created_second is False
    assert len(items) == 1
    assert items[0].item_id == stored_second.item_id
    assert items[0].fee_mode == "zero_fee"
    assert items[0].source_kind == "optimization_row"
    assert items[0].oos_started_at == datetime(2026, 2, 1, tzinfo=UTC)


def test_portfolio_store_remove_selected_items(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(portfolio_store, "portfolio_items_path", lambda: tmp_path / "portfolio_items.csv")
    defaults = StrategyDefaults()

    first = build_portfolio_item(
        symbol_1="AUDUSD+",
        symbol_2="CADCHF+",
        timeframe=Timeframe.M15,
        params=DistanceParameters(lookback_bars=48, entry_z=2.0, exit_z=0.5, stop_z=3.5, bollinger_k=2.0),
        defaults=defaults,
        fee_mode="tight_spread",
        source_kind="tester",
    )
    second = build_portfolio_item(
        symbol_1="USDCAD+",
        symbol_2="AUDUSD+",
        timeframe=Timeframe.H1,
        params=DistanceParameters(lookback_bars=72, entry_z=1.8, exit_z=-1.0, stop_z=None, bollinger_k=2.0),
        defaults=defaults,
        fee_mode="tight_spread",
        source_kind="tester",
    )
    upsert_portfolio_item(first)
    upsert_portfolio_item(second)

    removed = remove_portfolio_items([first.item_id])
    items = load_portfolio_items()

    assert removed == 1
    assert len(items) == 1
    assert items[0].symbol_1 == "USDCAD+"
