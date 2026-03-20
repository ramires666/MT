from datetime import UTC, datetime
from pathlib import Path
import shutil

import polars as pl

from storage.quotes import write_m5_quotes


def test_write_m5_quotes_partitions_by_month(monkeypatch) -> None:
    sandbox = Path(".test_quote_storage")
    if sandbox.exists():
        shutil.rmtree(sandbox)
    sandbox.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(sandbox)

    frame = pl.DataFrame(
        {
            "time": [
                datetime(2026, 1, 1, 0, 0, tzinfo=UTC),
                datetime(2026, 2, 1, 0, 0, tzinfo=UTC),
            ],
            "open": [1.0, 2.0],
            "high": [1.1, 2.1],
            "low": [0.9, 1.9],
            "close": [1.05, 2.05],
            "tick_volume": [10, 20],
            "spread": [2, 3],
            "real_volume": [1, 2],
        }
    )

    written = write_m5_quotes(frame=frame, broker="test", symbol="ABC")

    assert len(written) == 2
    assert all(path.exists() for path in written)

    monkeypatch.chdir(Path(__file__).resolve().parents[1])
    shutil.rmtree(sandbox, ignore_errors=True)