from __future__ import annotations

from datetime import UTC, datetime
from math import isfinite
from typing import Sequence


DEFAULT_BOUNDS = (0.0, 1.0)


def coerce_datetime_ms(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        current = value if value.tzinfo else value.replace(tzinfo=UTC)
        return current.timestamp() * 1000.0
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError(f"Unsupported datetime-like value: {value!r}")


def _collect_window_values(
    times: Sequence[object],
    columns: Sequence[Sequence[object]],
    x_start: float | None,
    x_end: float | None,
) -> list[float]:
    values: list[float] = []
    for index, time_value in enumerate(times):
        current_ms = coerce_datetime_ms(time_value)
        if current_ms is None:
            continue
        if x_start is not None and current_ms < x_start:
            continue
        if x_end is not None and current_ms > x_end:
            continue

        for column in columns:
            if index >= len(column):
                continue
            current = column[index]
            if current is None:
                continue
            if isinstance(current, (int, float)) and isfinite(float(current)):
                values.append(float(current))
    return values


def compute_series_bounds(
    times: Sequence[object],
    columns: Sequence[Sequence[object]],
    x_start: object | None,
    x_end: object | None,
    *,
    pad_ratio: float = 0.05,
    fallback: tuple[float, float] = DEFAULT_BOUNDS,
) -> tuple[float, float]:
    if not times:
        return fallback

    start_ms = coerce_datetime_ms(x_start)
    end_ms = coerce_datetime_ms(x_end)
    window_values = _collect_window_values(times, columns, start_ms, end_ms)
    if not window_values:
        window_values = _collect_window_values(times, columns, None, None)
    if not window_values:
        return fallback

    low = min(window_values)
    high = max(window_values)
    span = high - low
    pad = span * pad_ratio if span > 0 else max(abs(high) * pad_ratio, 1.0)
    return low - pad, high + pad


def compute_plot_height(visible_plot_count: int, visible_block_count: int, *, viewport_height: int = 2160) -> int:
    if visible_plot_count <= 0:
        return 0
    reserved_height = 140 + (160 * visible_block_count)
    available_height = max(440, viewport_height - reserved_height)
    plot_height = max(180, available_height // visible_plot_count)
    if visible_plot_count == 1:
        return min(plot_height, max(1, viewport_height // 3))
    return plot_height
