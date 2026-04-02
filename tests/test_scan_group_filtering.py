import polars as pl

from domain.contracts import ScanUniverseMode
from app_config import get_settings
from domain.data.catalog_groups import COINTEGRATION_CANDIDATES_GROUP
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


def test_resolve_scan_symbols_supports_cointegration_candidates_group(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    candidates_path = tmp_path / "scans" / "bybit_mt5" / "cointegration_pairs_candidates.csv"
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(
        "ID,Symbol1,Symbol2\n1,NAS100,SP500\n2,XAUUSD+,XAGUSD\n",
        encoding="utf-8",
    )
    catalog = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100", "SP500", "XAGUSD"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100", r"CFD\Indices\SP500", r"Metals\XAGUSD"],
            "normalized_group": ["commodities", "indices", "indices", "indices", "metals"],
        }
    )

    monkeypatch.setattr("domain.scan.johansen_universe.load_instrument_catalog_frame", lambda _broker: catalog)

    result = resolve_scan_symbols("bybit_mt5", ScanUniverseMode.GROUP, normalized_group=COINTEGRATION_CANDIDATES_GROUP)

    assert result == ["NAS100", "SP500", "XAGUSD"]
    get_settings.cache_clear()
