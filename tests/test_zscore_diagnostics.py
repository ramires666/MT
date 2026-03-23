from __future__ import annotations

from datetime import UTC, datetime, timedelta

import polars as pl

from bokeh_app.zscore_diagnostics import build_zscore_diagnostics


def test_build_zscore_diagnostics_returns_metrics_and_histogram() -> None:
    times = [datetime(2026, 1, 1, tzinfo=UTC) + timedelta(minutes=15 * idx) for idx in range(7)]
    frame = pl.DataFrame(
        {
            "time": times,
            "zscore": [-2.5, -1.0, 0.0, 0.5, 1.2, 2.1, None],
        }
    )

    payload = build_zscore_diagnostics(frame, entry_z=2.0, exit_z=0.5, stop_z=3.0, bins=12)

    assert payload.metrics_source["metric"][0] == "Valid Bars"
    assert payload.metrics_source["value"][0] == "6"
    assert "Entry trigger is <b>|z| >= 2.00</b>" in payload.summary_html
    assert payload.histogram_x_start < 0.0
    assert payload.histogram_x_end > 0.0
    assert len(payload.histogram_source["left"]) == 12
    assert sum(int(count) for count in payload.histogram_source["count"]) == 6


def test_build_zscore_diagnostics_handles_negative_exit_mode() -> None:
    frame = pl.DataFrame({"zscore": [-2.0, -1.5, -0.2, 0.1, 1.1, 1.8, 2.4]})

    payload = build_zscore_diagnostics(frame, entry_z=1.5, exit_z=-1.0, stop_z=None, bins=10)

    assert payload.exit_mode == "opposite_signal"
    assert "opposite-signal threshold" in payload.summary_html
    assert "z >= +|Exit|" in payload.metrics_source["metric"]
    assert "z <= -|Exit|" in payload.metrics_source["metric"]
