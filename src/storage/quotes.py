from __future__ import annotations

from pathlib import Path

import polars as pl

from storage.paths import raw_quotes_root


def raw_partition_path(broker: str, symbol: str, year: int, month: int) -> Path:
    return raw_quotes_root() / broker / symbol / "M5" / f"year={year:04d}" / f"month={month:02d}" / "data.parquet"


def write_m5_quotes(frame: pl.DataFrame, broker: str, symbol: str) -> list[Path]:
    if frame.is_empty():
        return []

    stamped = frame.with_columns(
        pl.col("time").dt.year().alias("year"),
        pl.col("time").dt.month().alias("month"),
    )

    written_paths: list[Path] = []
    for partition in stamped.partition_by(["year", "month"], maintain_order=True):
        year = int(partition[0, "year"])
        month = int(partition[0, "month"])
        output_path = raw_partition_path(broker=broker, symbol=symbol, year=year, month=month)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        (
            partition
            .drop("year", "month")
            .sort("time")
            .write_parquet(output_path, compression="zstd", statistics=True)
        )
        written_paths.append(output_path)
    return written_paths