from datetime import UTC, datetime, timedelta

from bokeh_app.view_utils import (
    compute_overlay_label_layout,
    compute_plot_height,
    compute_relative_plot_height,
    compute_series_bounds,
    display_symbol_label,
)


def test_compute_series_bounds_respects_visible_window() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(4)]
    lower, upper = compute_series_bounds(
        times,
        [[10.0, 20.0, 30.0, 40.0]],
        times[1],
        times[2],
        pad_ratio=0.1,
    )

    assert round(lower, 1) == 19.0
    assert round(upper, 1) == 31.0


def test_compute_series_bounds_falls_back_to_full_series_when_window_is_empty() -> None:
    times = [datetime(2026, 1, 1, 0, 0, tzinfo=UTC) + timedelta(minutes=5 * idx) for idx in range(4)]
    lower, upper = compute_series_bounds(
        times,
        [[1.0, 2.0, float("nan"), 4.0]],
        times[-1] + timedelta(hours=1),
        times[-1] + timedelta(hours=2),
        pad_ratio=0.0,
    )

    assert lower == 1.0
    assert upper == 4.0


def test_compute_plot_height_shrinks_as_more_blocks_are_visible() -> None:
    one_block = compute_plot_height(visible_plot_count=4, visible_block_count=1, viewport_height=1080)
    three_blocks = compute_plot_height(visible_plot_count=4, visible_block_count=3, viewport_height=1080)

    assert one_block > three_blocks
    assert three_blocks >= 160


def test_compute_plot_height_caps_single_plot_height() -> None:
    viewport_height = 1080
    height = compute_plot_height(visible_plot_count=1, visible_block_count=0, viewport_height=viewport_height)

    assert height <= viewport_height // 3
    assert height >= 180


def test_compute_relative_plot_height_emphasizes_equity_over_price() -> None:
    base_height = 320

    assert compute_relative_plot_height("equity", base_height, equity_visible=True) == 344
    assert compute_relative_plot_height("price_1", base_height, equity_visible=True) == 296
    assert compute_relative_plot_height("price_2", base_height, equity_visible=True) == 296
    assert compute_relative_plot_height("spread", base_height, equity_visible=True) == 320


def test_compute_relative_plot_height_keeps_uniform_height_without_equity() -> None:
    base_height = 320

    assert compute_relative_plot_height("equity", base_height, equity_visible=False) == 320
    assert compute_relative_plot_height("price_1", base_height, equity_visible=False) == 320


def test_display_symbol_label_uses_symbol_when_available() -> None:
    assert display_symbol_label("BTCUSD", fallback="Price 1", placeholder="-- refresh from MT5 --") == "BTCUSD"


def test_display_symbol_label_falls_back_for_empty_or_placeholder_values() -> None:
    assert display_symbol_label("", fallback="Price 1", placeholder="-- refresh from MT5 --") == "Price 1"
    assert display_symbol_label("-- refresh from MT5 --", fallback="Price 1", placeholder="-- refresh from MT5 --") == "Price 1"


def test_compute_overlay_label_layout_wraps_rows_before_shrinking_font() -> None:
    texts = [
        "MA / VISA\nTrades: 37",
        "Gross: 26596.07\nSpread: 4014.08",
        "Slip: 194.93\nComm: 194.93",
        "Net: 22192.94\nEnding: 32192.94",
        "Max DD: -2424.48\nWin: 89.2%",
        "Cap: 10000.00\nPeak: 32762.79",
    ]

    font_px, positions = compute_overlay_label_layout(texts, 640, base_font_px=16)

    assert font_px == 16
    assert len(positions) == len(texts)
    assert len({y for _x, y in positions}) > 1


def test_compute_overlay_label_layout_keeps_single_row_when_plot_is_wide() -> None:
    texts = ["One\nA", "Two\nB", "Three\nC"]

    font_px, positions = compute_overlay_label_layout(texts, 1400, base_font_px=14)

    assert font_px == 14
    assert len({y for _x, y in positions}) == 1


def test_compute_overlay_label_layout_supports_top_anchor_for_short_plots() -> None:
    texts = [
        "MA / VISA\nTrades: 37",
        "Gross: 26596.07\nSpread: 4014.08",
        "Slip: 194.93\nComm: 194.93",
        "Net: 22192.94\nEnding: 32192.94",
        "Max DD: -2424.48\nWin: 89.2%",
        "Cap: 10000.00\nPeak: 32762.79",
    ]

    font_px, positions = compute_overlay_label_layout(
        texts,
        640,
        base_font_px=16,
        plot_height=280,
        vertical_anchor="top",
    )

    assert font_px >= 8
    assert len(positions) == len(texts)
    assert min(y for _x, y in positions) > 80
