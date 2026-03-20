from __future__ import annotations

import polars as pl

from domain.contracts import Timeframe
from domain.data.timeframes import to_polars_every


def resample_m5_quotes(frame: pl.DataFrame, timeframe: Timeframe) -> pl.DataFrame:
    if timeframe == Timeframe.M5:
        return frame.sort("time")

    every = to_polars_every(timeframe)
    sorted_frame = frame.sort("time")

    aggregations = [
        pl.col("open").first().alias("open"),
        pl.col("high").max().alias("high"),
        pl.col("low").min().alias("low"),
        pl.col("close").last().alias("close"),
    ]

    available_columns = set(sorted_frame.columns)
    if "tick_volume" in available_columns:
        aggregations.append(pl.col("tick_volume").sum().alias("tick_volume"))
    if "real_volume" in available_columns:
        aggregations.append(pl.col("real_volume").sum().alias("real_volume"))
    if "spread" in available_columns:
        aggregations.append(pl.col("spread").mean().round(0).cast(pl.Int64).alias("spread"))

    return (
        sorted_frame.group_by_dynamic("time", every=every, closed="left", label="left")
        .agg(aggregations)
        .drop_nulls(subset=["open", "high", "low", "close"])
        .sort("time")
    )