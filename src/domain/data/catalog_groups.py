from __future__ import annotations

import polars as pl

from domain.contracts import NormalizedGroup


ALL_GROUP_OPTION = "all"
MT5_GROUP_COLUMN = "mt5_group_path"


def mt5_group_path(path: object | None) -> str:
    raw = str(path or "").strip().replace("/", "\\")
    parts = [part.strip() for part in raw.split("\\") if part.strip()]
    if not parts:
        return ""
    if len(parts) == 1:
        return parts[0]
    return "\\".join(parts[:-1])


def with_mt5_group_column(frame: pl.DataFrame, *, path_column: str = "path") -> pl.DataFrame:
    if MT5_GROUP_COLUMN in frame.columns:
        return frame
    if path_column not in frame.columns:
        return frame.with_columns(pl.lit("", dtype=pl.String).alias(MT5_GROUP_COLUMN))
    return frame.with_columns(
        pl.Series(
            MT5_GROUP_COLUMN,
            [mt5_group_path(value) for value in frame.get_column(path_column).to_list()],
            dtype=pl.String,
        )
    )


def list_mt5_group_options(frame: pl.DataFrame) -> list[str]:
    catalog = with_mt5_group_column(frame)
    if catalog.is_empty():
        return [ALL_GROUP_OPTION]
    values = sorted({str(value).strip() for value in catalog.get_column(MT5_GROUP_COLUMN).to_list() if str(value).strip()})
    return [ALL_GROUP_OPTION, *values] if values else [ALL_GROUP_OPTION]


def filter_catalog_by_group(frame: pl.DataFrame, selected_group: str | None) -> pl.DataFrame:
    catalog = with_mt5_group_column(frame)
    group_value = str(selected_group or "").strip()
    if not group_value or group_value == ALL_GROUP_OPTION:
        return catalog
    if group_value in {item.value for item in NormalizedGroup} and "normalized_group" in catalog.columns:
        return catalog.filter(
            (pl.col(MT5_GROUP_COLUMN) == group_value) | (pl.col("normalized_group") == group_value)
        )
    return catalog.filter(pl.col(MT5_GROUP_COLUMN) == group_value)
