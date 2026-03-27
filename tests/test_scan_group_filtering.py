import polars as pl

from domain.contracts import ScanUniverseMode
from domain.scan.johansen_universe import resolve_scan_symbols


def test_resolve_scan_symbols_supports_exact_mt5_group(monkeypatch) -> None:
    catalog = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100"],
            "normalized_group": ["commodities", "indices", "indices"],
        }
    )

    monkeypatch.setattr("domain.scan.johansen_universe.load_instrument_catalog_frame", lambda _broker: catalog)

    result = resolve_scan_symbols("bybit_mt5", ScanUniverseMode.GROUP, normalized_group=r"CFD\Indices")

    assert result == ["NAS100", "US500"]


def test_resolve_scan_symbols_keeps_legacy_normalized_group_support(monkeypatch) -> None:
    catalog = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100"],
            "normalized_group": ["commodities", "indices", "indices"],
        }
    )

    monkeypatch.setattr("domain.scan.johansen_universe.load_instrument_catalog_frame", lambda _broker: catalog)

    result = resolve_scan_symbols("bybit_mt5", ScanUniverseMode.GROUP, normalized_group="indices")

    assert result == ["NAS100", "US500"]
