from datetime import UTC, datetime, timedelta

from bokeh_app.view_utils import compute_plot_height, compute_series_bounds


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
