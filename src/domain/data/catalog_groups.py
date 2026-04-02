from __future__ import annotations

import csv
from functools import lru_cache
from pathlib import Path

import polars as pl

from domain.contracts import NormalizedGroup
from storage.paths import scans_root


ALL_GROUP_OPTION = "all"
COINTEGRATION_CANDIDATES_GROUP = "cointegration_pairs_candidates"
MT5_GROUP_COLUMN = "mt5_group_path"


def cointegration_candidates_path(broker: str) -> Path:
    return scans_root() / str(broker or "").strip() / "cointegration_pairs_candidates.csv"


@lru_cache(maxsize=32)
def _read_cointegration_candidate_pairs_cached(path_text: str, modified_ns: int, size: int) -> tuple[tuple[str, str], ...]:
    path = Path(path_text)
    pairs: set[tuple[str, str]] = set()
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            left = str(row.get("Symbol1", "") or "").strip()
            right = str(row.get("Symbol2", "") or "").strip()
            if not left or not right or left == right:
                continue
            pairs.add((left, right))
    return tuple(sorted(pairs))


def is_cointegration_candidates_group(selected_group: str | None) -> bool:
    return str(selected_group or "").strip() == COINTEGRATION_CANDIDATES_GROUP


def cointegration_candidate_pairs(broker: str | None) -> tuple[tuple[str, str], ...]:
    broker_value = str(broker or "").strip()
    if not broker_value:
        return ()
    path = cointegration_candidates_path(broker_value)
    if not path.exists():
        return ()
    stat = path.stat()
    return _read_cointegration_candidate_pairs_cached(str(path), int(stat.st_mtime_ns), int(stat.st_size))


def cointegration_candidate_symbols(broker: str | None) -> tuple[str, ...]:
    symbols: set[str] = set()
    for left, right in cointegration_candidate_pairs(broker):
        symbols.add(left)
        symbols.add(right)
    return tuple(sorted(symbols))


def cointegration_candidate_pair_keys(
    broker: str | None,
    *,
    allowed_symbols: set[str] | None = None,
) -> tuple[str, ...]:
    allowed = set(str(symbol) for symbol in (allowed_symbols or set()))
    filtered: set[str] = set()
    for left, right in cointegration_candidate_pairs(broker):
        if allowed and (left not in allowed or right not in allowed):
            continue
        normalized_left, normalized_right = sorted((left, right))
        filtered.add(f"{normalized_left}::{normalized_right}")
    return tuple(sorted(filtered))


def cointegration_candidate_partner_symbols(
    broker: str | None,
    *,
    symbol: str,
    allowed_symbols: set[str] | None = None,
) -> tuple[str, ...]:
    current_symbol = str(symbol or "").strip()
    if not current_symbol:
        return ()
    allowed = set(str(item) for item in (allowed_symbols or set()))
    partners: set[str] = set()
    for left, right in cointegration_candidate_pairs(broker):
        if left == current_symbol:
            candidate = right
        elif right == current_symbol:
            candidate = left
        else:
            continue
        if allowed and candidate not in allowed:
            continue
        partners.add(candidate)
    return tuple(sorted(partners))


def cointegration_candidate_signature(broker: str | None) -> str:
    broker_value = str(broker or "").strip()
    if not broker_value:
        return ""
    path = cointegration_candidates_path(broker_value)
    if not path.exists():
        return ""
    stat = path.stat()
    return f"{path.name}:{int(stat.st_mtime_ns)}:{int(stat.st_size)}"


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


def list_mt5_group_options(frame: pl.DataFrame, broker: str | None = None) -> list[str]:
    catalog = with_mt5_group_column(frame)
    special_options: list[str] = []
    if broker:
        candidate_symbols = set(cointegration_candidate_symbols(broker))
        if candidate_symbols:
            catalog_symbols = set(catalog.get_column("symbol").to_list()) if "symbol" in catalog.columns else set()
            if candidate_symbols.intersection(catalog_symbols):
                special_options.append(COINTEGRATION_CANDIDATES_GROUP)
    if catalog.is_empty():
        return [ALL_GROUP_OPTION, *special_options] if special_options else [ALL_GROUP_OPTION]
    values = sorted({str(value).strip() for value in catalog.get_column(MT5_GROUP_COLUMN).to_list() if str(value).strip()})
    merged = [ALL_GROUP_OPTION, *special_options, *values]
    return merged if len(merged) > 1 else [ALL_GROUP_OPTION]


def filter_catalog_by_group(frame: pl.DataFrame, selected_group: str | None, broker: str | None = None) -> pl.DataFrame:
    catalog = with_mt5_group_column(frame)
    group_value = str(selected_group or "").strip()
    if not group_value or group_value == ALL_GROUP_OPTION:
        return catalog
    if is_cointegration_candidates_group(group_value):
        candidate_symbols = list(cointegration_candidate_symbols(broker))
        if not candidate_symbols:
            return catalog.head(0)
        return catalog.filter(pl.col("symbol").is_in(candidate_symbols))
    if group_value in {item.value for item in NormalizedGroup} and "normalized_group" in catalog.columns:
        return catalog.filter(
            (pl.col(MT5_GROUP_COLUMN) == group_value) | (pl.col("normalized_group") == group_value)
        )
    return catalog.filter(pl.col(MT5_GROUP_COLUMN) == group_value)
