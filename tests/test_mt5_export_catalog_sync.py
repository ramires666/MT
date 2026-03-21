from datetime import UTC, datetime

import polars as pl

from tools.mt5_export_catalog_sync import chunked, month_partitions_between, resolve_symbols


def test_month_partitions_between_spans_across_year_boundary() -> None:
    result = month_partitions_between(
        datetime(2025, 12, 15, tzinfo=UTC),
        datetime(2026, 2, 1, tzinfo=UTC),
    )

    assert result == [(2025, 12), (2026, 1), (2026, 2)]


def test_chunked_preserves_order() -> None:
    result = list(chunked(["A", "B", "C", "D", "E"], 2))

    assert result == [["A", "B"], ["C", "D"], ["E"]]


def test_resolve_symbols_filters_catalog_groups(monkeypatch) -> None:
    def fake_catalog(_broker: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol": ["EURUSD", "NAS100", "US2000"],
                "normalized_group": ["forex", "indices", "indices"],
            }
        )

    monkeypatch.setattr("tools.mt5_export_catalog_sync.read_instrument_catalog", fake_catalog)

    result = resolve_symbols("bybit_mt5", [], all_symbols=True, groups=["indices"], limit=1)

    assert result == ["NAS100"]
