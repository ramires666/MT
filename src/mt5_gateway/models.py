from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from domain.contracts import NormalizedGroup


@dataclass(slots=True)
class InstrumentInfo:
    symbol: str
    path: str
    description: str
    normalized_group: NormalizedGroup
    digits: int
    point: float
    contract_size: float
    volume_min: float
    volume_max: float
    volume_step: float
    currency_base: str = ""
    currency_profit: str = ""
    currency_margin: str = ""
    trade_calc_mode: int = 0
    trade_tick_size: float = 0.0
    trade_tick_value: float = 0.0
    trade_tick_value_profit: float = 0.0
    trade_tick_value_loss: float = 0.0
    margin_initial: float = 0.0
    margin_maintenance: float = 0.0
    margin_hedged: float = 0.0
    commission_mode: str = "none"
    commission_value: float = 0.0
    commission_currency: str = ""
    commission_source_field: str = ""


@dataclass(slots=True)
class TickSnapshot:
    symbol: str
    timestamp_utc: datetime
    bid: float
    ask: float
    last: float
