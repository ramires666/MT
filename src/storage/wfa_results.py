from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

import polars as pl

from domain.contracts import PairSelection, Timeframe, WfaWindowUnit
from storage.paths import wfa_root


def _sanitize_symbol(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", value or "unknown")


def _pair_key(pair: PairSelection) -> str:
    return f"{_sanitize_symbol(pair.symbol_1)}__{_sanitize_symbol(pair.symbol_2)}"


def _serialize_time(moment: datetime) -> str:
    current = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return current.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _compact_time(moment: datetime) -> str:
    current = moment if moment.tzinfo else moment.replace(tzinfo=UTC)
    return current.astimezone(UTC).strftime("%Y%m%dT%H%M")


def wfa_pair_history_dir(broker: str, pair: PairSelection, timeframe: Timeframe) -> Path:
    return wfa_root() / broker / timeframe.value / _pair_key(pair)


def wfa_pair_history_path(broker: str, pair: PairSelection, timeframe: Timeframe) -> Path:
    return wfa_pair_history_dir(broker, pair, timeframe) / "optimization_history.parquet"


def wfa_run_snapshot_dir(broker: str, pair: PairSelection, timeframe: Timeframe) -> Path:
    return wfa_pair_history_dir(broker, pair, timeframe) / "snapshots"


def wfa_run_snapshot_path(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    *,
    started_at: datetime,
    ended_at: datetime,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
) -> Path:
    filename = (
        f"{_compact_time(started_at)}__{_compact_time(ended_at)}"
        f"__{unit.value}__lb{int(lookback_units)}__test{int(test_units)}__step{int(step_units)}.json"
    )
    return wfa_run_snapshot_dir(broker, pair, timeframe) / filename


def load_wfa_optimization_history(broker: str, pair: PairSelection, timeframe: Timeframe) -> pl.DataFrame:
    path = wfa_pair_history_path(broker, pair, timeframe)
    if not path.exists():
        return pl.DataFrame()
    return pl.read_parquet(path)


def persist_wfa_optimization_history(
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    rows: Sequence[Mapping[str, object]],
) -> Path | None:
    if not rows:
        return None
    output_path = wfa_pair_history_path(broker, pair, timeframe)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_frame = pl.DataFrame(rows)
    if output_path.exists():
        existing = pl.read_parquet(output_path)
        combined = pl.concat([existing, new_frame], how="diagonal_relaxed")
        dedupe_keys = [column for column in ["wfa_run_id", "fold", "trial_id"] if column in combined.columns]
        if dedupe_keys:
            combined = combined.unique(subset=dedupe_keys, keep="last", maintain_order=True)
    else:
        combined = new_frame
    combined.write_parquet(output_path, compression="zstd", statistics=True)
    return output_path


def persist_wfa_run_snapshot(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
    result: Mapping[str, Any],
) -> Path:
    output_path = wfa_run_snapshot_path(
        broker,
        pair,
        timeframe,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=lookback_units,
        test_units=test_units,
        step_units=step_units,
        unit=unit,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = dict(result)
    payload.update(
        {
            "started_at": _serialize_time(started_at),
            "ended_at": _serialize_time(ended_at),
            "lookback_units": int(lookback_units),
            "test_units": int(test_units),
            "step_units": int(step_units),
            "unit": unit.value,
            "timeframe": timeframe.value,
            "symbol_1": pair.symbol_1,
            "symbol_2": pair.symbol_2,
            "saved_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
    )
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return output_path


def load_wfa_run_snapshot(
    *,
    broker: str,
    pair: PairSelection,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    lookback_units: int,
    test_units: int,
    step_units: int,
    unit: WfaWindowUnit,
) -> dict[str, Any] | None:
    path = wfa_run_snapshot_path(
        broker,
        pair,
        timeframe,
        started_at=started_at,
        ended_at=ended_at,
        lookback_units=lookback_units,
        test_units=test_units,
        step_units=step_units,
        unit=unit,
    )
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))
