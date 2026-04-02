import polars as pl

from app_config import get_settings
from domain.data.catalog_groups import (
    ALL_GROUP_OPTION,
    COINTEGRATION_CANDIDATES_GROUP,
    MT5_GROUP_COLUMN,
    cointegration_candidate_pair_keys,
    cointegration_candidate_partner_symbols,
    filter_catalog_by_group,
    list_mt5_group_options,
    mt5_group_path,
    with_mt5_group_column,
)


def test_mt5_group_path_strips_symbol_leaf_from_path() -> None:
    assert mt5_group_path(r"Commodities\COPPER-C") == "Commodities"
    assert mt5_group_path(r"CFD\Indices\US500") == r"CFD\Indices"


def test_with_mt5_group_column_derives_group_from_path() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500"],
        }
    )

    enriched = with_mt5_group_column(frame)

    assert enriched.get_column(MT5_GROUP_COLUMN).to_list() == ["Commodities", r"CFD\Indices"]


def test_list_mt5_group_options_returns_all_plus_exact_groups() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500"],
        }
    )

    assert list_mt5_group_options(frame) == [ALL_GROUP_OPTION, r"CFD\Indices", "Commodities"]


def test_filter_catalog_by_group_supports_exact_mt5_group_and_legacy_normalized_group() -> None:
    frame = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100"],
            "normalized_group": ["commodities", "indices", "indices"],
        }
    )

    exact = filter_catalog_by_group(frame, r"CFD\Indices")
    legacy = filter_catalog_by_group(frame, "indices")

    assert exact.get_column("symbol").to_list() == ["US500", "NAS100"]
    assert legacy.get_column("symbol").to_list() == ["US500", "NAS100"]


def test_list_mt5_group_options_includes_cointegration_candidates_group_when_csv_matches_catalog(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    candidates_path = tmp_path / "scans" / "bybit_mt5" / "cointegration_pairs_candidates.csv"
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(
        "ID,Symbol1,Symbol2\n1,NAS100,SP500\n2,XAUUSD+,XAGUSD\n",
        encoding="utf-8",
    )
    frame = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100", "SP500"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100", r"CFD\Indices\SP500"],
        }
    )

    options = list_mt5_group_options(frame, broker="bybit_mt5")

    assert options == [ALL_GROUP_OPTION, COINTEGRATION_CANDIDATES_GROUP, r"CFD\Indices", "Commodities"]
    get_settings.cache_clear()


def test_filter_catalog_by_group_supports_cointegration_candidates_group(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    candidates_path = tmp_path / "scans" / "bybit_mt5" / "cointegration_pairs_candidates.csv"
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(
        "ID,Symbol1,Symbol2\n1,NAS100,SP500\n2,XAUUSD+,XAGUSD\n",
        encoding="utf-8",
    )
    frame = pl.DataFrame(
        {
            "symbol": ["COPPER-C", "US500", "NAS100", "SP500"],
            "path": [r"Commodities\COPPER-C", r"CFD\Indices\US500", r"CFD\Indices\NAS100", r"CFD\Indices\SP500"],
            "normalized_group": ["commodities", "indices", "indices", "indices"],
        }
    )

    filtered = filter_catalog_by_group(frame, COINTEGRATION_CANDIDATES_GROUP, broker="bybit_mt5")

    assert filtered.get_column("symbol").to_list() == ["NAS100", "SP500"]
    get_settings.cache_clear()


def test_cointegration_candidate_helpers_expose_exact_pairs_and_partners(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_DATA_ROOT", str(tmp_path))
    get_settings.cache_clear()
    candidates_path = tmp_path / "scans" / "bybit_mt5" / "cointegration_pairs_candidates.csv"
    candidates_path.parent.mkdir(parents=True, exist_ok=True)
    candidates_path.write_text(
        "ID,Symbol1,Symbol2\n1,NAS100,SP500\n2,NAS100,QQQ\n3,XAUUSD+,XAGUSD\n",
        encoding="utf-8",
    )

    pair_keys = cointegration_candidate_pair_keys("bybit_mt5")
    partners = cointegration_candidate_partner_symbols(
        "bybit_mt5",
        symbol="NAS100",
        allowed_symbols={"NAS100", "SP500", "QQQ", "XAUUSD+"},
    )

    assert pair_keys == ("NAS100::QQQ", "NAS100::SP500", "XAGUSD::XAUUSD+")
    assert partners == ("QQQ", "SP500")
    get_settings.cache_clear()
