from __future__ import annotations

from datetime import UTC, datetime
from math import isfinite
from typing import Sequence


DEFAULT_BOUNDS = (0.0, 1.0)


def display_symbol_label(symbol: object | None, *, fallback: str, placeholder: str | None = None) -> str:
    label = str(symbol or "").strip()
    if not label:
        return fallback
    if placeholder is not None and label == placeholder:
        return fallback
    return label


def _measure_overlay_label_widths(
    texts: Sequence[str],
    *,
    font_px: int,
    padding_px: int,
) -> list[float]:
    char_px = float(font_px) * 0.62
    widths: list[float] = []
    for text in texts:
        lines = text.splitlines() or [text]
        max_len = max(len(line) for line in lines)
        widths.append(max_len * char_px + float(padding_px))
    return widths


def compute_overlay_label_layout(
    texts: Sequence[str],
    plot_width: int,
    *,
    base_font_px: int,
    plot_height: int | None = None,
    left_margin: int = 16,
    right_margin: int = 16,
    gap: int = 18,
    padding_px: int = 18,
    baseline_y: int = 20,
    top_margin: int = 72,
    min_font_px: int = 8,
    max_rows: int = 3,
    vertical_anchor: str = "bottom",
) -> tuple[int, list[tuple[int, int]]]:
    if not texts:
        return max(int(min_font_px), int(base_font_px)), []

    available_width = max(320, int(plot_width) - int(left_margin) - int(right_margin))
    available_height = max(
        80,
        int(plot_height) if plot_height is not None else (int(top_margin) + int(baseline_y) + 160),
    )
    requested_font_px = max(int(min_font_px), int(base_font_px))
    font_candidates: list[int] = []
    seen_font_sizes: set[int] = set()
    for candidate in (requested_font_px, requested_font_px - 1, requested_font_px - 2, requested_font_px - 3, min_font_px):
        candidate_int = max(int(min_font_px), int(candidate))
        if candidate_int not in seen_font_sizes:
            seen_font_sizes.add(candidate_int)
            font_candidates.append(candidate_int)

    fallback_font_px = font_candidates[-1] if font_candidates else requested_font_px
    fallback_widths = _measure_overlay_label_widths(texts, font_px=fallback_font_px, padding_px=padding_px)
    fallback_columns = max(1, (len(texts) + max(1, int(max_rows)) - 1) // max(1, int(max_rows)))

    def _positions(widths: Sequence[float], columns: int, font_px: int) -> list[tuple[int, int]]:
        row_count = max(1, (len(widths) + columns - 1) // columns)
        row_height = max(28, int(font_px * 3.1))
        positions: list[tuple[int, int]] = []
        anchor = "top" if str(vertical_anchor or "").lower() == "top" else "bottom"
        top_baseline = max(int(baseline_y) + (row_count - 1) * row_height, available_height - int(top_margin))
        for row_index, start in enumerate(range(0, len(widths), columns)):
            row_widths = list(widths[start : start + columns])
            row_total = sum(row_widths) + int(gap) * max(0, len(row_widths) - 1)
            if row_total > available_width and row_widths:
                squeeze_ratio = max(
                    0.82,
                    (available_width - int(gap) * max(0, len(row_widths) - 1)) / max(1.0, sum(row_widths)),
                )
                row_widths = [width * squeeze_ratio for width in row_widths]
                row_total = sum(row_widths) + int(gap) * max(0, len(row_widths) - 1)
            cursor_x = int(left_margin) + max(0.0, (available_width - row_total) / 2.0)
            if anchor == "top":
                y = top_baseline - row_index * row_height
            else:
                y = int(baseline_y) + (row_count - row_index - 1) * row_height
            for width in row_widths:
                positions.append((int(cursor_x), int(y)))
                cursor_x += width + int(gap)
        return positions

    def _fits_height(columns: int, font_px: int) -> bool:
        row_count = max(1, (len(texts) + columns - 1) // columns)
        row_height = max(28, int(font_px * 3.1))
        required_span = (row_count - 1) * row_height
        return required_span <= max(0, available_height - int(top_margin) - int(baseline_y))

    for font_px in font_candidates:
        widths = _measure_overlay_label_widths(texts, font_px=font_px, padding_px=padding_px)
        min_columns = max(1, (len(widths) + max(1, int(max_rows)) - 1) // max(1, int(max_rows)))
        for columns in range(len(widths), min_columns - 1, -1):
            if not _fits_height(columns, font_px):
                continue
            row_totals = []
            for start in range(0, len(widths), columns):
                row_widths = widths[start : start + columns]
                row_totals.append(sum(row_widths) + int(gap) * max(0, len(row_widths) - 1))
            if row_totals and max(row_totals) <= available_width:
                return font_px, _positions(widths, columns, font_px)

    return fallback_font_px, _positions(fallback_widths, fallback_columns, fallback_font_px)


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


def compute_relative_plot_height(
    plot_key: str,
    base_height: int,
    *,
    equity_visible: bool,
    min_height: int = 160,
    emphasis_delta: int = 24,
) -> int:
    height = max(int(min_height), int(base_height))
    if not equity_visible:
        return height
    if plot_key == "equity":
        return height + int(emphasis_delta)
    if plot_key in {"price_1", "price_2"}:
        return max(int(min_height), height - int(emphasis_delta))
    return height
