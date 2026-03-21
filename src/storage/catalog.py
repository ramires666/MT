from __future__ import annotations

from pathlib import Path

import polars as pl

from mt5_gateway.models import InstrumentInfo
from storage.paths import catalog_root


CATALOG_COLUMNS: list[tuple[str, object]] = [
    ("symbol", pl.String),
    ("path", pl.String),
    ("description", pl.String),
    ("normalized_group", pl.String),
    ("digits", pl.Int64),
    ("point", pl.Float64),
    ("contract_size", pl.Float64),
    ("volume_min", pl.Float64),
    ("volume_max", pl.Float64),
    ("volume_step", pl.Float64),
    ("currency_base", pl.String),
    ("currency_profit", pl.String),
    ("currency_margin", pl.String),
    ("trade_calc_mode", pl.Int64),
    ("trade_tick_size", pl.Float64),
    ("trade_tick_value", pl.Float64),
    ("trade_tick_value_profit", pl.Float64),
    ("trade_tick_value_loss", pl.Float64),
    ("margin_initial", pl.Float64),
    ("margin_maintenance", pl.Float64),
    ("margin_hedged", pl.Float64),
    ("commission_mode", pl.String),
    ("commission_value", pl.Float64),
    ("commission_currency", pl.String),
    ("commission_source_field", pl.String),
]


def catalog_path(broker: str) -> Path:
    return catalog_root() / broker / "instrument_catalog.parquet"


def instrument_catalog_path(broker: str) -> Path:
    return catalog_path(broker)


def _empty_catalog_frame() -> pl.DataFrame:
    schema = {column: dtype for column, dtype in CATALOG_COLUMNS}
    return pl.DataFrame(schema=schema)


def _normalize_catalog(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.is_empty() and not frame.columns:
        return _empty_catalog_frame()

    for column, dtype in CATALOG_COLUMNS:
        if column not in frame.columns:
            default_value = "" if dtype == pl.String else 0
            frame = frame.with_columns(pl.lit(default_value, dtype=dtype).alias(column))
    return frame.select([pl.col(column).cast(dtype, strict=False) for column, dtype in CATALOG_COLUMNS]).sort("symbol")


def write_instrument_catalog(instruments: list[InstrumentInfo], broker: str) -> Path:
    output_path = catalog_path(broker)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame = pl.DataFrame(
        {
            "symbol": [item.symbol for item in instruments],
            "path": [item.path for item in instruments],
            "description": [item.description for item in instruments],
            "normalized_group": [item.normalized_group.value for item in instruments],
            "digits": [item.digits for item in instruments],
            "point": [item.point for item in instruments],
            "contract_size": [item.contract_size for item in instruments],
            "volume_min": [item.volume_min for item in instruments],
            "volume_max": [item.volume_max for item in instruments],
            "volume_step": [item.volume_step for item in instruments],
            "currency_base": [item.currency_base for item in instruments],
            "currency_profit": [item.currency_profit for item in instruments],
            "currency_margin": [item.currency_margin for item in instruments],
            "trade_calc_mode": [item.trade_calc_mode for item in instruments],
            "trade_tick_size": [item.trade_tick_size for item in instruments],
            "trade_tick_value": [item.trade_tick_value for item in instruments],
            "trade_tick_value_profit": [item.trade_tick_value_profit for item in instruments],
            "trade_tick_value_loss": [item.trade_tick_value_loss for item in instruments],
            "margin_initial": [item.margin_initial for item in instruments],
            "margin_maintenance": [item.margin_maintenance for item in instruments],
            "margin_hedged": [item.margin_hedged for item in instruments],
            "commission_mode": [item.commission_mode for item in instruments],
            "commission_value": [item.commission_value for item in instruments],
            "commission_currency": [item.commission_currency for item in instruments],
            "commission_source_field": [item.commission_source_field for item in instruments],
        }
    )
    _normalize_catalog(frame).write_parquet(output_path, compression="zstd", statistics=True)
    return output_path


def read_instrument_catalog(broker: str) -> pl.DataFrame:
    path = catalog_path(broker)
    if not path.exists():
        return _empty_catalog_frame()
    return _normalize_catalog(pl.read_parquet(path))
