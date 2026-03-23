from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any


def fractional_step_decimals(step: Any) -> int:
    try:
        step_decimal = Decimal(str(step)).normalize()
    except (InvalidOperation, TypeError, ValueError):
        return 0
    if not step_decimal.is_finite() or step_decimal <= 0:
        return 0
    return max(0, -step_decimal.as_tuple().exponent)


def has_fractional_step(step: Any) -> bool:
    return fractional_step_decimals(step) > 0


def normalize_fractional_value(
    value: Any,
    *,
    step: Any,
    low: Any | None = None,
    high: Any | None = None,
) -> float | None:
    if value in (None, ""):
        return None
    try:
        current = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if not current.is_finite():
        return None

    decimals = fractional_step_decimals(step)
    if decimals > 0:
        quantum = Decimal(1).scaleb(-decimals)
        current = current.quantize(quantum, rounding=ROUND_HALF_UP)

    if low not in (None, ""):
        try:
            low_decimal = Decimal(str(low))
        except (InvalidOperation, TypeError, ValueError):
            low_decimal = None
        if low_decimal is not None and low_decimal.is_finite():
            current = max(current, low_decimal)

    if high not in (None, ""):
        try:
            high_decimal = Decimal(str(high))
        except (InvalidOperation, TypeError, ValueError):
            high_decimal = None
        if high_decimal is not None and high_decimal.is_finite():
            current = min(current, high_decimal)

    normalized = float(current)
    if normalized == 0.0:
        return 0.0
    return normalized
