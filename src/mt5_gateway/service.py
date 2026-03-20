from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from app_config import get_settings
from mt5_gateway.client import MT5Client
from storage.catalog import write_instrument_catalog
from storage.quotes import write_m5_quotes


@dataclass(slots=True)
class QuoteDownloadRequest:
    symbol: str
    started_at: datetime
    ended_at: datetime
    timeframe: str = "M5"


class MT5GatewayService:
    """Thin orchestration layer around the Windows-side MT5 client."""

    def __init__(self, client: MT5Client, broker: str | None = None) -> None:
        settings = get_settings()
        self.client = client
        self.broker = broker or settings.default_broker_id

    def refresh_instruments(self) -> Path:
        instruments = self.client.fetch_instruments()
        return write_instrument_catalog(instruments=instruments, broker=self.broker)

    def download_quotes(self, request: QuoteDownloadRequest) -> list[Path]:
        if request.timeframe != "M5":
            raise ValueError("Only canonical M5 download is supported at the gateway layer")
        frame = self.client.fetch_m5_quotes(
            symbol=request.symbol,
            started_at=request.started_at,
            ended_at=request.ended_at,
        )
        return write_m5_quotes(frame=frame, broker=self.broker, symbol=request.symbol)

    def stream_latest_ticks(self, symbols: list[str]) -> None:
        raise NotImplementedError("Live tick streaming will be added after the initial ingest flow")