import polars as pl

from domain.data.catalog_groups import (
    ALL_GROUP_OPTION,
    MT5_GROUP_COLUMN,
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
