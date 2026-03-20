from datetime import UTC, datetime

import polars as pl

from domain.contracts import Timeframe
from domain.data.resample import resample_m5_quotes


def test_resample_m5_to_m15() -> None:
    frame = pl.DataFrame(
        {
            "time": [
                datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 5, tzinfo=UTC),
                datetime(2026, 1, 1, 0, 10, tzinfo=UTC),
            ],
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "tick_volume": [100, 110, 120],
            "spread": [2, 4, 6],
            "real_volume": [10, 11, 12],
        }
    )

    result = resample_m5_quotes(frame, Timeframe.M15)

    assert result.height == 1
    assert result[0, "open"] == 10.0
    assert result[0, "high"] == 13.0
    assert result[0, "low"] == 9.0
    assert result[0, "close"] == 12.5
    assert result[0, "tick_volume"] == 330
    assert result[0, "real_volume"] == 33
    assert result[0, "spread"] == 4