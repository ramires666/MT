from __future__ import annotations

from pathlib import Path
from typing import Sequence

from mt5_gateway.client import MT5Client
from mt5_gateway.models import InstrumentInfo
from storage.catalog import write_instrument_catalog


class CatalogSyncService:
    def __init__(self, client: MT5Client, broker_name: str, catalog_file: Path | None = None) -> None:
        self._client = client
        self._broker_name = broker_name
        self._catalog_file = catalog_file

    def sync(self, group_filter: str | None = None) -> tuple[list[InstrumentInfo], Path]:
        instruments = self._client.fetch_instruments(group_filter)
        written = write_instrument_catalog(instruments, broker=self._broker_name)
        return instruments, written

    def refresh_universe(self, groups: Sequence[str]) -> list[tuple[str, list[InstrumentInfo]]]:
        results: list[tuple[str, list[InstrumentInfo]]] = []
        for group in groups:
            subset = self._client.fetch_instruments(group)
            results.append((group, subset))
        return results