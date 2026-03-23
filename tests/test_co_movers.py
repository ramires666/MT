from domain.data.co_movers import (
    ALL_CO_MOVERS_LABEL,
    co_mover_group_labels_for_symbol,
    co_mover_groups_for_symbol,
    co_mover_symbols_for_symbol,
)


def test_co_mover_groups_for_symbol_filters_to_matching_available_universe() -> None:
    groups = co_mover_groups_for_symbol(
        "AUDUSD+",
        available_symbols=["AUDUSD+", "AUDNZD+", "NZDUSD+"],
    )

    assert [group.label for group in groups] == [
        "FX: Commodity currencies",
        "FX: Antipodean relative value",
    ]


def test_co_mover_group_labels_include_all_matching_option() -> None:
    labels = co_mover_group_labels_for_symbol(
        "AUDUSD+",
        available_symbols=["AUDUSD+", "AUDNZD+", "NZDUSD+"],
    )

    assert labels == [
        ALL_CO_MOVERS_LABEL,
        "FX: Commodity currencies",
        "FX: Antipodean relative value",
    ]


def test_co_mover_symbols_can_be_limited_to_specific_group() -> None:
    symbols = co_mover_symbols_for_symbol(
        "AUDUSD+",
        available_symbols=["AUDUSD+", "AUDNZD+", "NZDUSD+"],
        group_label="FX: Commodity currencies",
    )

    assert symbols == ["NZDUSD+"]


def test_co_mover_symbols_union_all_matching_groups_by_default() -> None:
    symbols = co_mover_symbols_for_symbol(
        "AUDUSD+",
        available_symbols=["AUDUSD+", "AUDNZD+", "NZDUSD+"],
    )

    assert symbols == ["AUDNZD+", "NZDUSD+"]


def test_co_mover_groups_empty_when_symbol_has_no_available_mates() -> None:
    assert co_mover_group_labels_for_symbol("CVX", available_symbols=["CVX"]) == []
    assert co_mover_symbols_for_symbol("CVX", available_symbols=["CVX"]) == []
