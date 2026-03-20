from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from domain.contracts import Timeframe
from domain.costs.profiles import apply_broker_commission_fallback, merge_commission_override, read_commission_overrides
from domain.data.resample import resample_m5_quotes
from storage.catalog import read_instrument_catalog
from storage.paths import derived_quotes_root, raw_quotes_root


SPEC_DEFAULTS: dict[str, Any] = {
    "symbol": "",
    "path": "",
    "description": "",
    "normalized_group": "custom",
    "digits": 0,
    "point": 0.0,
    "contract_size": 1.0,
    "volume_min": 0.0,
    "volume_max": 0.0,
    "volume_step": 0.0,
    "currency_base": "",
    "currency_profit": "",
    "currency_margin": "",
    "trade_calc_mode": 0,
    "trade_tick_size": 0.0,
    "trade_tick_value": 0.0,
    "trade_tick_value_profit": 0.0,
    "trade_tick_value_loss": 0.0,
    "margin_initial": 0.0,
    "margin_maintenance": 0.0,
    "margin_hedged": 0.0,
    "commission_mode": "none",
    "commission_value": 0.0,
    "commission_currency": "",
    "commission_source_field": "",
    "commission_minimum": 0.0,
    "commission_entry_only": False,
}


def raw_symbol_root(broker: str, symbol: str) -> Path:
    return raw_quotes_root() / broker / symbol / "M5"


def derived_symbol_root(broker: str, symbol: str, timeframe: str) -> Path:
    return derived_quotes_root() / broker / symbol / timeframe


def scan_raw_quotes(path: Path) -> pl.LazyFrame:
    return pl.scan_parquet(str(path / "**" / "*.parquet"))


def _empty_quote_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "time": pl.Datetime(time_zone="UTC"),
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "tick_volume": pl.Int64,
            "spread": pl.Int64,
            "real_volume": pl.Int64,
        }
    )


def load_raw_quotes_range(broker: str, symbol: str, started_at: datetime, ended_at: datetime) -> pl.DataFrame:
    root = raw_symbol_root(broker, symbol)
    if not root.exists():
        return _empty_quote_frame()

    return (
        scan_raw_quotes(root)
        .filter(pl.col("time") >= started_at)
        .filter(pl.col("time") <= ended_at)
        .collect()
        .sort("time")
    )


def load_quotes_range(
    broker: str,
    symbol: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
) -> pl.DataFrame:
    raw = load_raw_quotes_range(broker=broker, symbol=symbol, started_at=started_at, ended_at=ended_at)
    if raw.is_empty():
        return raw
    return resample_m5_quotes(raw, timeframe=timeframe)


def load_instrument_catalog_frame(broker: str) -> pl.DataFrame:
    return read_instrument_catalog(broker)


def load_instrument_spec(broker: str, symbol: str) -> dict[str, float | int | str]:
    catalog = load_instrument_catalog_frame(broker)
    row = catalog.filter(pl.col("symbol") == symbol)
    if row.is_empty():
        base = dict(SPEC_DEFAULTS)
        base["symbol"] = symbol
        return base
    merged = dict(SPEC_DEFAULTS)
    merged.update(row.to_dicts()[0])
    overrides = read_commission_overrides(broker)
    merged = merge_commission_override(merged, overrides.get(symbol))
    return apply_broker_commission_fallback(broker, merged)


def list_symbols(broker: str) -> list[str]:
    catalog = load_instrument_catalog_frame(broker)
    return catalog.get_column("symbol").to_list()
