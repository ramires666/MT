from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from app_config import get_settings
from storage.paths import catalog_root


@dataclass(slots=True)
class CommissionProfile:
    mode: str = "none"
    value: float = 0.0
    currency: str = ""
    source: str = "catalog"
    minimum: float = 0.0
    entry_only: bool = False


DEFAULT_COMMISSION_PROFILE = CommissionProfile()
BYBIT_ZERO_FEE_MODE = "zero_fee"
BYBIT_TIGHT_SPREAD_MODE = "tight_spread"
BYBIT_SUPPORTED_FEE_MODES = {BYBIT_ZERO_FEE_MODE, BYBIT_TIGHT_SPREAD_MODE}
BYBIT_SPECIAL_INDEX_FEES = {
    "NIKKEI225": 0.1,
    "HK50": 1.5,
    "HKTECH": 0.5,
}
BYBIT_PRECIOUS_METAL_PREFIXES = ("XAU", "XAG")


def commission_overrides_path(broker: str) -> Path:
    return catalog_root() / broker / "commission_overrides.json"


def read_commission_overrides(broker: str) -> dict[str, dict[str, Any]]:
    path = commission_overrides_path(broker)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, dict):
        return {}
    normalized: dict[str, dict[str, Any]] = {}
    for symbol, raw in payload.items():
        if not isinstance(symbol, str) or not isinstance(raw, Mapping):
            continue
        normalized[symbol] = {
            "commission_mode": str(raw.get("commission_mode", raw.get("mode", "none"))),
            "commission_value": float(raw.get("commission_value", raw.get("value", 0.0)) or 0.0),
            "commission_currency": str(raw.get("commission_currency", raw.get("currency", "")) or ""),
            "commission_source_field": str(raw.get("commission_source_field", "override") or "override"),
            "commission_minimum": float(raw.get("commission_minimum", raw.get("minimum", 0.0)) or 0.0),
            "commission_entry_only": bool(raw.get("commission_entry_only", raw.get("entry_only", False))),
        }
    return normalized


def merge_commission_override(spec: Mapping[str, Any], override: Mapping[str, Any] | None) -> dict[str, Any]:
    merged = dict(spec)
    if not override:
        return merged
    merged.update({
        "commission_mode": str(override.get("commission_mode", merged.get("commission_mode", "none")) or "none"),
        "commission_value": float(override.get("commission_value", merged.get("commission_value", 0.0)) or 0.0),
        "commission_currency": str(override.get("commission_currency", merged.get("commission_currency", "")) or ""),
        "commission_source_field": str(override.get("commission_source_field", merged.get("commission_source_field", "override")) or "override"),
        "commission_minimum": float(override.get("commission_minimum", merged.get("commission_minimum", 0.0)) or 0.0),
        "commission_entry_only": bool(override.get("commission_entry_only", merged.get("commission_entry_only", False))),
    })
    return merged


def _normalized_text(value: Any) -> str:
    return str(value or "").strip()


def _has_explicit_commission(spec: Mapping[str, Any]) -> bool:
    mode = _normalized_text(spec.get("commission_mode")).lower()
    value = float(spec.get("commission_value", 0.0) or 0.0)
    return mode not in {"", "none"} and value > 0.0


def _bybit_fee_mode() -> str:
    mode = _normalized_text(get_settings().bybit_tradfi_fee_mode).lower()
    if mode in BYBIT_SUPPORTED_FEE_MODES:
        return mode
    return BYBIT_ZERO_FEE_MODE


def _bybit_is_precious_metal(spec: Mapping[str, Any]) -> bool:
    symbol = _normalized_text(spec.get("symbol")).upper()
    path = _normalized_text(spec.get("path")).upper()
    description = _normalized_text(spec.get("description")).upper()
    return symbol.startswith(BYBIT_PRECIOUS_METAL_PREFIXES) or "GOLD" in path or "SILVER" in path or "GOLD" in description or "SILVER" in description


def _bybit_tight_spread_profile(spec: Mapping[str, Any]) -> CommissionProfile:
    symbol = _normalized_text(spec.get("symbol")).upper()
    normalized_group = _normalized_text(spec.get("normalized_group")).lower()
    if normalized_group == "stocks":
        return CommissionProfile(
            mode="per_lot_round_turn",
            value=0.02,
            currency="USDT",
            source="bybit_official_tight_spread_us_stock_cfds",
            minimum=0.2,
            entry_only=True,
        )
    if symbol in BYBIT_SPECIAL_INDEX_FEES:
        return CommissionProfile(
            mode="per_lot_round_turn",
            value=float(BYBIT_SPECIAL_INDEX_FEES[symbol]),
            currency="USDT",
            source="bybit_official_tight_spread_special_indices",
            entry_only=True,
        )
    if normalized_group == "forex" or _bybit_is_precious_metal(spec):
        return CommissionProfile(
            mode="per_lot_round_turn",
            value=6.0,
            currency="USDT",
            source="bybit_official_tight_spread_forex_precious_metals",
            entry_only=True,
        )
    if normalized_group in {"indices", "commodities"}:
        return CommissionProfile(
            mode="per_lot_round_turn",
            value=3.0,
            currency="USDT",
            source="bybit_official_tight_spread_indices_commodities_oil",
            entry_only=True,
        )
    return DEFAULT_COMMISSION_PROFILE


def apply_broker_commission_fallback(broker: str, spec: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(spec)
    if _has_explicit_commission(merged):
        return merged
    if broker != "bybit_mt5":
        return merged

    fee_mode = _bybit_fee_mode()
    if fee_mode == BYBIT_ZERO_FEE_MODE:
        merged.update(
            {
                "commission_mode": "none",
                "commission_value": 0.0,
                "commission_currency": "",
                "commission_source_field": "bybit_official_zero_fee_default",
                "commission_minimum": 0.0,
                "commission_entry_only": False,
            }
        )
        return merged

    profile = _bybit_tight_spread_profile(merged)
    if profile.mode == "none" or profile.value <= 0.0:
        return merged
    merged.update(
        {
            "commission_mode": profile.mode,
            "commission_value": profile.value,
            "commission_currency": profile.currency,
            "commission_source_field": profile.source,
            "commission_minimum": profile.minimum,
            "commission_entry_only": profile.entry_only,
        }
    )
    return merged
