from datetime import UTC, datetime

import polars as pl

from app_config import get_settings
from domain.data.catalog_groups import COINTEGRATION_CANDIDATES_GROUP
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
                "path": [r"Forex\EURUSD", r"CFD\Indices\NAS100", r"CFD\Indices\US2000"],
                "normalized_group": ["forex", "indices", "indices"],
            }
        )

    monkeypatch.setattr("tools.mt5_export_catalog_sync.read_instrument_catalog", fake_catalog)

    result = resolve_symbols("bybit_mt5", [], all_symbols=True, groups=[r"CFD\Indices"], limit=1)

    assert result == ["NAS100"]


def test_resolve_symbols_keeps_legacy_normalized_group_filter(monkeypatch) -> None:
    def fake_catalog(_broker: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol": ["EURUSD", "NAS100", "US2000"],
                "path": [r"Forex\EURUSD", r"CFD\Indices\NAS100", r"CFD\Indices\US2000"],
                "normalized_group": ["forex", "indices", "indices"],
            }
        )

    monkeypatch.setattr("tools.mt5_export_catalog_sync.read_instrument_catalog", fake_catalog)

    result = resolve_symbols("bybit_mt5", [], all_symbols=True, groups=["indices"], limit=None)

    assert result == ["NAS100", "US2000"]


def test_resolve_symbols_supports_cointegration_candidates_group(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    candidates_path = tmp_path / "scans" / "bybit_mt5" / "cointegration_pairs_candidates.csv"
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(
        "ID,Symbol1,Symbol2\n1,NAS100,SP500\n2,XAUUSD+,XAGUSD\n",
        encoding="utf-8",
    )

    def fake_catalog(_broker: str) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol": ["EURUSD", "NAS100", "SP500", "XAGUSD"],
                "path": [r"Forex\EURUSD", r"CFD\Indices\NAS100", r"CFD\Indices\SP500", r"Metals\XAGUSD"],
                "normalized_group": ["forex", "indices", "indices", "metals"],
            }
        )

    monkeypatch.setattr("tools.mt5_export_catalog_sync.read_instrument_catalog", fake_catalog)

    result = resolve_symbols("bybit_mt5", [], all_symbols=True, groups=[COINTEGRATION_CANDIDATES_GROUP], limit=None)

    assert result == ["NAS100", "SP500", "XAGUSD"]
    get_settings.cache_clear()
