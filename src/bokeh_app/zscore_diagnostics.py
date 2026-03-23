from __future__ import annotations

from dataclasses import dataclass
from math import sqrt

import numpy as np
import polars as pl


def empty_zscore_metric_source() -> dict[str, list[object]]:
    return {"metric": [], "value": [], "note": []}


def empty_zscore_hist_source() -> dict[str, list[object]]:
    return {"left": [], "right": [], "count": [], "share": [], "label": []}


@dataclass(slots=True)
class ZScoreDiagnosticsPayload:
    metrics_source: dict[str, list[object]]
    histogram_source: dict[str, list[object]]
    summary_html: str
    histogram_x_start: float
    histogram_x_end: float
    histogram_y_end: float
    entry_threshold: float
    exit_threshold: float | None
    stop_threshold: float | None
    exit_mode: str


def _safe_percentile(values: np.ndarray, q: float) -> float:
    if values.size == 0:
        return 0.0
    return float(np.percentile(values, q))


def _safe_skew(values: np.ndarray, mean: float, std: float) -> float:
    if values.size == 0 or std <= 1e-12:
        return 0.0
    standardized = (values - mean) / std
    return float(np.mean(np.power(standardized, 3)))


def _safe_excess_kurtosis(values: np.ndarray, mean: float, std: float) -> float:
    if values.size == 0 or std <= 1e-12:
        return 0.0
    standardized = (values - mean) / std
    return float(np.mean(np.power(standardized, 4)) - 3.0)


def _fmt_number(value: float, decimals: int = 4) -> str:
    return f"{float(value):.{decimals}f}"


def _fmt_share(value: float) -> str:
    return f"{float(value) * 100.0:.2f}%"


def build_zscore_diagnostics(
    frame: pl.DataFrame,
    *,
    entry_z: float,
    exit_z: float,
    stop_z: float | None,
    bins: int = 41,
) -> ZScoreDiagnosticsPayload:
    if frame.is_empty() or "zscore" not in frame.columns:
        return ZScoreDiagnosticsPayload(
            metrics_source=empty_zscore_metric_source(),
            histogram_source=empty_zscore_hist_source(),
            summary_html="<p>No z-score data available on the current tester period.</p>",
            histogram_x_start=-1.0,
            histogram_x_end=1.0,
            histogram_y_end=1.0,
            entry_threshold=abs(float(entry_z)),
            exit_threshold=abs(float(exit_z)),
            stop_threshold=None if stop_z is None else abs(float(stop_z)),
            exit_mode="opposite_signal" if exit_z < 0.0 else "mean_reversion",
        )

    raw = frame.get_column("zscore").to_numpy().astype(np.float64)
    finite_mask = np.isfinite(raw)
    values = raw[finite_mask]
    nan_count = int(raw.size - values.size)
    if values.size == 0:
        return ZScoreDiagnosticsPayload(
            metrics_source=empty_zscore_metric_source(),
            histogram_source=empty_zscore_hist_source(),
            summary_html="<p>All z-score values are NaN on the current tester period.</p>",
            histogram_x_start=-1.0,
            histogram_x_end=1.0,
            histogram_y_end=1.0,
            entry_threshold=abs(float(entry_z)),
            exit_threshold=abs(float(exit_z)),
            stop_threshold=None if stop_z is None else abs(float(stop_z)),
            exit_mode="opposite_signal" if exit_z < 0.0 else "mean_reversion",
        )

    abs_values = np.abs(values)
    mean = float(values.mean())
    std = float(values.std(ddof=0))
    rms = float(sqrt(float(np.mean(np.square(values)))))
    median = _safe_percentile(values, 50)
    last_finite = float(values[-1])
    positive_share = float(np.mean(values > 0.0))
    negative_share = float(np.mean(values < 0.0))
    zero_crossings = int(np.count_nonzero(np.signbit(values[1:]) != np.signbit(values[:-1]))) if values.size >= 2 else 0

    entry_threshold = abs(float(entry_z))
    exit_threshold = abs(float(exit_z))
    stop_threshold = None if stop_z is None else abs(float(stop_z))
    exit_mode = "opposite_signal" if exit_z < 0.0 else "mean_reversion"

    metrics_rows = [
        ("Valid Bars", str(int(values.size)), "finite z-score bars"),
        ("NaN Bars", str(nan_count), "bars without a valid z-score"),
        ("Last Finite Z", _fmt_number(last_finite, 3), "most recent finite z-score"),
        ("Mean", _fmt_number(mean), "distribution center"),
        ("Std", _fmt_number(std), "population std"),
        ("Median", _fmt_number(median), "50th percentile"),
        ("Min", _fmt_number(float(values.min())), "left tail"),
        ("Max", _fmt_number(float(values.max())), "right tail"),
        ("Abs Mean", _fmt_number(float(abs_values.mean())), "average |z|"),
        ("Abs P90", _fmt_number(_safe_percentile(abs_values, 90)), "90th percentile of |z|"),
        ("Abs P95", _fmt_number(_safe_percentile(abs_values, 95)), "95th percentile of |z|"),
        ("RMS", _fmt_number(rms), "root mean square"),
        ("Skew", _fmt_number(_safe_skew(values, mean, std)), "shape asymmetry"),
        ("Excess Kurtosis", _fmt_number(_safe_excess_kurtosis(values, mean, std)), "tail heaviness"),
        ("P01", _fmt_number(_safe_percentile(values, 1)), "1st percentile"),
        ("P05", _fmt_number(_safe_percentile(values, 5)), "5th percentile"),
        ("P10", _fmt_number(_safe_percentile(values, 10)), "10th percentile"),
        ("P25", _fmt_number(_safe_percentile(values, 25)), "25th percentile"),
        ("P75", _fmt_number(_safe_percentile(values, 75)), "75th percentile"),
        ("P90", _fmt_number(_safe_percentile(values, 90)), "90th percentile"),
        ("P95", _fmt_number(_safe_percentile(values, 95)), "95th percentile"),
        ("P99", _fmt_number(_safe_percentile(values, 99)), "99th percentile"),
        ("Positive Share", _fmt_share(positive_share), "z > 0"),
        ("Negative Share", _fmt_share(negative_share), "z < 0"),
        ("|z| <= 1.0", _fmt_share(float(np.mean(abs_values <= 1.0))), "quiet regime share"),
        ("|z| <= 2.0", _fmt_share(float(np.mean(abs_values <= 2.0))), "inside ±2 sigma"),
        ("|z| >= 2.0", _fmt_share(float(np.mean(abs_values >= 2.0))), "outside ±2 sigma"),
        ("|z| >= 3.0", _fmt_share(float(np.mean(abs_values >= 3.0))), "outside ±3 sigma"),
        ("|z| >= Entry", _fmt_share(float(np.mean(abs_values >= entry_threshold))), f"entry threshold {entry_threshold:.2f}"),
        ("Zero Crossings", str(zero_crossings), "sign flips on finite bars"),
    ]
    if exit_z >= 0.0:
        metrics_rows.append(
            ("|z| <= Exit", _fmt_share(float(np.mean(abs_values <= exit_threshold))), f"mean-reversion band {exit_threshold:.2f}")
        )
    else:
        metrics_rows.append(
            ("z >= +|Exit|", _fmt_share(float(np.mean(values >= exit_threshold))), f"short-spread opposite signal {exit_threshold:.2f}")
        )
        metrics_rows.append(
            ("z <= -|Exit|", _fmt_share(float(np.mean(values <= -exit_threshold))), f"long-spread opposite signal {exit_threshold:.2f}")
        )
    if stop_threshold is not None:
        metrics_rows.append(
            ("|z| >= Stop", _fmt_share(float(np.mean(abs_values >= stop_threshold))), f"stop threshold {stop_threshold:.2f}")
        )

    metrics_source = {
        "metric": [metric for metric, _value, _note in metrics_rows],
        "value": [value for _metric, value, _note in metrics_rows],
        "note": [note for _metric, _value, note in metrics_rows],
    }

    max_abs = max(
        1.0,
        float(abs_values.max()),
        entry_threshold,
        exit_threshold,
        stop_threshold or 0.0,
    )
    histogram_limit = max_abs * 1.05
    edges = np.linspace(-histogram_limit, histogram_limit, max(5, int(bins)) + 1)
    counts, histogram_edges = np.histogram(values, bins=edges)
    total = max(1, int(counts.sum()))
    share = counts.astype(np.float64) / float(total)
    histogram_source = {
        "left": histogram_edges[:-1].tolist(),
        "right": histogram_edges[1:].tolist(),
        "count": counts.astype(int).tolist(),
        "share": share.tolist(),
        "label": [
            f"{float(histogram_edges[idx]):.2f} .. {float(histogram_edges[idx + 1]):.2f}"
            for idx in range(len(histogram_edges) - 1)
        ],
    }

    exit_text = (
        f"opposite-signal threshold at ±{exit_threshold:.2f}"
        if exit_z < 0.0
        else f"mean-reversion band |z| <= {exit_threshold:.2f}"
    )
    stop_text = "stop disabled" if stop_threshold is None else f"stop threshold |z| >= {stop_threshold:.2f}"
    summary_html = (
        f"<p>Z-score diagnostics on <b>{int(values.size)}</b> finite bars. "
        f"Entry trigger is <b>|z| >= {entry_threshold:.2f}</b>, exit uses <b>{exit_text}</b>, {stop_text}. "
        f"Histogram is normalized to <b>share of valid bars</b>, not raw count.</p>"
    )

    return ZScoreDiagnosticsPayload(
        metrics_source=metrics_source,
        histogram_source=histogram_source,
        summary_html=summary_html,
        histogram_x_start=-histogram_limit,
        histogram_x_end=histogram_limit,
        histogram_y_end=max(0.05, float(share.max()) * 1.15 if share.size else 1.0),
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        stop_threshold=stop_threshold,
        exit_mode=exit_mode,
    )
