from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Sequence

import polars as pl

from mt5_gateway.client import MT5Client
from storage.quotes import write_m5_quotes


class QuoteIngestionService:
    def __init__(self, client: MT5Client, broker_name: str) -> None:
        self._client = client
        self._broker_name = broker_name

    def download_m5(self, symbol: str, started_at: datetime, ended_at: datetime) -> Sequence[Path]:
        frame = self._client.fetch_m5_quotes(symbol, started_at, ended_at)
        return write_m5_quotes(frame, self._broker_name, symbol)

    def download_bulk(self, symbols: Sequence[str], started_at: datetime, ended_at: datetime) -> dict[str, Sequence[Path]]:
        outputs: dict[str, Sequence[Path]] = {}
        for symbol in symbols:
            outputs[symbol] = self.download_m5(symbol, started_at, ended_at)
        return outputs

    def ensure_tf(self, frame: pl.DataFrame, timeframe: str) -> pl.DataFrame:
        # TODO: wire in resampling pipeline once derived caches are implemented.
        return frame
