from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import polars as pl

from domain.data.instrument_groups import normalize_group
from mt5_gateway.models import InstrumentInfo, TickSnapshot


class MT5Client:
    def __init__(
        self,
        terminal_path: str | None = None,
        login: int | None = None,
        password: str | None = None,
        server: str | None = None,
    ) -> None:
        self.terminal_path = terminal_path
        self.login = login
        self.password = password
        self.server = server
        self._mt5: Any | None = None

    def _module(self):
        if self._mt5 is None:
            try:
                import MetaTrader5 as mt5
            except ImportError as exc:  # pragma: no cover - depends on local MT5 install
                raise RuntimeError("MetaTrader5 package is not installed in this environment") from exc
            self._mt5 = mt5
        return self._mt5

    def initialize(self) -> None:
        mt5 = self._module()
        kwargs: dict[str, Any] = {}
        if self.terminal_path:
            kwargs["path"] = self.terminal_path
        if self.login is not None:
            kwargs["login"] = self.login
        if self.password is not None:
            kwargs["password"] = self.password
        if self.server is not None:
            kwargs["server"] = self.server
        if not mt5.initialize(**kwargs):
            raise RuntimeError(f"MetaTrader5 initialize failed: {mt5.last_error()}")

    def shutdown(self) -> None:
        mt5 = self._module()
        mt5.shutdown()

    @staticmethod
    def _extract_commission(info_dict: dict[str, Any]) -> tuple[str, float, str, str]:
        known_keys = [
            "trade_commission",
            "commission",
            "commission_value",
            "trade_commission_value",
        ]
        for key in known_keys:
            value = info_dict.get(key)
            if isinstance(value, (int, float)) and float(value) > 0.0:
                return "per_lot_per_side", float(value), str(info_dict.get("currency_profit", "") or ""), key
        for key, value in info_dict.items():
            if "commission" not in key.lower():
                continue
            if isinstance(value, (int, float)) and float(value) > 0.0:
                return "per_lot_per_side", float(value), str(info_dict.get("currency_profit", "") or ""), key
        return "none", 0.0, str(info_dict.get("currency_profit", "") or ""), ""

    def fetch_instruments(self, group_filter: str | None = None) -> list[InstrumentInfo]:
        mt5 = self._module()
        raw_symbols = mt5.symbols_get(group=group_filter) if group_filter else mt5.symbols_get()
        if raw_symbols is None:
            raise RuntimeError(f"MetaTrader5 symbols_get failed: {mt5.last_error()}")

        instruments: list[InstrumentInfo] = []
        for item in raw_symbols:
            path = getattr(item, "path", "")
            description = getattr(item, "description", "")
            symbol = getattr(item, "name", "")
            info = mt5.symbol_info(symbol)
            info_dict = info._asdict() if info is not None and hasattr(info, "_asdict") else {}
            commission_mode, commission_value, commission_currency, commission_source_field = self._extract_commission(info_dict)
            instruments.append(
                InstrumentInfo(
                    symbol=symbol,
                    path=path,
                    description=description,
                    normalized_group=normalize_group(path=path, description=description, symbol=symbol),
                    digits=int(info_dict.get("digits", getattr(item, "digits", 0)) or 0),
                    point=float(info_dict.get("point", getattr(item, "point", 0.0)) or 0.0),
                    contract_size=float(info_dict.get("trade_contract_size", getattr(item, "trade_contract_size", 0.0)) or 0.0),
                    volume_min=float(info_dict.get("volume_min", getattr(item, "volume_min", 0.0)) or 0.0),
                    volume_max=float(info_dict.get("volume_max", getattr(item, "volume_max", 0.0)) or 0.0),
                    volume_step=float(info_dict.get("volume_step", getattr(item, "volume_step", 0.0)) or 0.0),
                    currency_base=str(info_dict.get("currency_base", "") or ""),
                    currency_profit=str(info_dict.get("currency_profit", "") or ""),
                    currency_margin=str(info_dict.get("currency_margin", "") or ""),
                    trade_calc_mode=int(info_dict.get("trade_calc_mode", 0) or 0),
                    trade_tick_size=float(info_dict.get("trade_tick_size", 0.0) or 0.0),
                    trade_tick_value=float(info_dict.get("trade_tick_value", 0.0) or 0.0),
                    trade_tick_value_profit=float(info_dict.get("trade_tick_value_profit", 0.0) or 0.0),
                    trade_tick_value_loss=float(info_dict.get("trade_tick_value_loss", 0.0) or 0.0),
                    margin_initial=float(info_dict.get("margin_initial", 0.0) or 0.0),
                    margin_maintenance=float(info_dict.get("margin_maintenance", 0.0) or 0.0),
                    margin_hedged=float(info_dict.get("margin_hedged", 0.0) or 0.0),
                    commission_mode=commission_mode,
                    commission_value=commission_value,
                    commission_currency=commission_currency,
                    commission_source_field=commission_source_field,
                )
            )
        return instruments

    def fetch_m5_quotes(self, symbol: str, started_at: datetime, ended_at: datetime) -> pl.DataFrame:
        mt5 = self._module()
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"MetaTrader5 symbol_select failed for {symbol}: {mt5.last_error()}")

        rates = mt5.copy_rates_range(symbol, mt5.TIMEFRAME_M5, started_at, ended_at)
        if rates is None:
            raise RuntimeError(f"MetaTrader5 copy_rates_range failed for {symbol}: {mt5.last_error()}")
        if len(rates) == 0:
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

        frame = pl.from_dicts(rates.tolist())
        return frame.with_columns(
            pl.from_epoch("time", time_unit="s").dt.replace_time_zone("UTC").alias("time")
        ).select("time", "open", "high", "low", "close", "tick_volume", "spread", "real_volume")

    def fetch_latest_tick(self, symbol: str) -> TickSnapshot:
        mt5 = self._module()
        if not mt5.symbol_select(symbol, True):
            raise RuntimeError(f"MetaTrader5 symbol_select failed for {symbol}: {mt5.last_error()}")
        tick = mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"MetaTrader5 symbol_info_tick failed: {mt5.last_error()}")
        timestamp_utc = datetime.fromtimestamp(getattr(tick, "time", 0), tz=UTC)
        return TickSnapshot(
            symbol=symbol,
            timestamp_utc=timestamp_utc,
            bid=float(getattr(tick, "bid", 0.0)),
            ask=float(getattr(tick, "ask", 0.0)),
            last=float(getattr(tick, "last", 0.0)),
        )
