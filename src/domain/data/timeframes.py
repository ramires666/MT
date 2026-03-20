from __future__ import annotations

from domain.contracts import Timeframe


_TIMEFRAME_TO_POLARS_EVERY: dict[Timeframe, str] = {
    Timeframe.M5: "5m",
    Timeframe.M15: "15m",
    Timeframe.M30: "30m",
    Timeframe.H1: "1h",
    Timeframe.H4: "4h",
    Timeframe.D1: "1d",
}


def to_polars_every(timeframe: Timeframe) -> str:
    return _TIMEFRAME_TO_POLARS_EVERY[timeframe]