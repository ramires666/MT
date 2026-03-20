from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence

import polars as pl

from domain.contracts import ScanUniverseMode, Timeframe
from domain.scan.johansen import JohansenUniverseScanResult
from storage.paths import scans_root


DEFAULT_SCAN_KIND = 'johansen'
COINTEGRATION_KIND_OPTIONS = ('johansen', 'distance', 'copula')

SCAN_ROW_SCHEMA: dict[str, pl.DataType] = {
    'symbol_1': pl.String,
    'symbol_2': pl.String,
    'sample_size': pl.Int64,
    'eligible_for_cointegration': pl.Boolean,
    'unit_root_leg_1': pl.String,
    'unit_root_leg_2': pl.String,
    'rank': pl.Int64,
    'threshold_passed': pl.Boolean,
    'trace_stat_0': pl.Float64,
    'max_eigen_stat_0': pl.Float64,
    'hedge_ratio': pl.Float64,
    'half_life_bars': pl.Float64,
    'last_zscore': pl.Float64,
    'failure_reason': pl.String,
}


@dataclass(slots=True)
class SavedScanSnapshot:
    scan_kind: str
    timeframe: Timeframe
    scope: str
    universe_mode: str | None
    normalized_group: str | None
    created_at: datetime | None
    summary: dict[str, Any]
    all_pairs_path: Path
    passed_pairs_path: Path
    summary_path: Path
    all_pairs: pl.DataFrame
    passed_pairs: pl.DataFrame


def _safe_component(value: str) -> str:
    return ''.join(char if char.isalnum() or char in ('-', '_') else '_' for char in value)



def _empty_rows_frame() -> pl.DataFrame:
    return pl.DataFrame(schema=SCAN_ROW_SCHEMA)



def _rows_to_frame(result: JohansenUniverseScanResult) -> pl.DataFrame:
    if not result.rows:
        return _empty_rows_frame()

    columns = {
        'symbol_1': [row.symbol_1 for row in result.rows],
        'symbol_2': [row.symbol_2 for row in result.rows],
        'sample_size': [int(row.sample_size) for row in result.rows],
        'eligible_for_cointegration': [bool(row.eligible_for_cointegration) for row in result.rows],
        'unit_root_leg_1': [row.unit_root_leg_1 for row in result.rows],
        'unit_root_leg_2': [row.unit_root_leg_2 for row in result.rows],
        'rank': [int(row.rank) for row in result.rows],
        'threshold_passed': [bool(row.threshold_passed) for row in result.rows],
        'trace_stat_0': [None if row.trace_stat_0 is None else float(row.trace_stat_0) for row in result.rows],
        'max_eigen_stat_0': [None if row.max_eigen_stat_0 is None else float(row.max_eigen_stat_0) for row in result.rows],
        'hedge_ratio': [None if row.hedge_ratio is None else float(row.hedge_ratio) for row in result.rows],
        'half_life_bars': [None if row.half_life_bars is None else float(row.half_life_bars) for row in result.rows],
        'last_zscore': [None if row.last_zscore is None else float(row.last_zscore) for row in result.rows],
        'failure_reason': [row.failure_reason for row in result.rows],
    }
    return pl.DataFrame(columns, schema=SCAN_ROW_SCHEMA)



def _scope_value(universe_mode: ScanUniverseMode | str | None, normalized_group: str | None) -> str:
    return normalized_group or (universe_mode.value if isinstance(universe_mode, ScanUniverseMode) else str(universe_mode or 'all'))



def _kind_root(*, broker: str, scan_kind: str) -> Path:
    return scans_root() / broker / _safe_component(scan_kind)



def _run_directory(
    *,
    broker: str,
    scan_kind: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    universe_mode: ScanUniverseMode,
    normalized_group: str | None,
    created_at: datetime,
) -> Path:
    start_label = started_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')
    end_label = ended_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')
    created_label = created_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')
    scope = _scope_value(universe_mode, normalized_group)
    return (
        _kind_root(broker=broker, scan_kind=scan_kind)
        / timeframe.value
        / f'{start_label}_{end_label}'
        / _safe_component(scope)
        / created_label
    )



def _latest_scope_directory(*, broker: str, scan_kind: str, timeframe: Timeframe, scope: str) -> Path:
    return _kind_root(broker=broker, scan_kind=scan_kind) / 'latest' / timeframe.value / _safe_component(scope)



def _legacy_latest_directory(*, broker: str, scan_kind: str) -> Path:
    return _kind_root(broker=broker, scan_kind=scan_kind) / 'latest'



def _summary_matches_scope(payload: dict[str, Any], timeframe: Timeframe, scope: str) -> bool:
    payload_timeframe = str(payload.get('timeframe') or '')
    payload_scope = str(payload.get('normalized_group') or payload.get('universe_mode') or 'all')
    return payload_timeframe == timeframe.value and payload_scope == scope



def _read_summary(summary_path: Path) -> dict[str, Any]:
    return json.loads(summary_path.read_text(encoding='utf-8'))



def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(UTC)
    except ValueError:
        return None



def _load_snapshot_from_paths(
    *,
    scan_kind: str,
    timeframe: Timeframe,
    scope: str,
    summary_path: Path,
    all_pairs_path: Path,
    passed_pairs_path: Path,
) -> SavedScanSnapshot:
    summary = _read_summary(summary_path)
    return SavedScanSnapshot(
        scan_kind=scan_kind,
        timeframe=timeframe,
        scope=scope,
        universe_mode=summary.get('universe_mode'),
        normalized_group=summary.get('normalized_group'),
        created_at=_parse_datetime(summary.get('created_at')),
        summary=summary,
        all_pairs_path=all_pairs_path,
        passed_pairs_path=passed_pairs_path,
        summary_path=summary_path,
        all_pairs=pl.read_parquet(all_pairs_path) if all_pairs_path.exists() else _empty_rows_frame(),
        passed_pairs=pl.read_parquet(passed_pairs_path) if passed_pairs_path.exists() else _empty_rows_frame(),
    )



def _find_latest_run_snapshot(*, broker: str, scan_kind: str, timeframe: Timeframe, scope: str) -> SavedScanSnapshot | None:
    timeframe_root = _kind_root(broker=broker, scan_kind=scan_kind) / timeframe.value
    if not timeframe_root.exists():
        return None

    candidates: list[tuple[datetime, Path, dict[str, Any]]] = []
    for summary_path in timeframe_root.rglob('summary.json'):
        try:
            payload = _read_summary(summary_path)
        except (OSError, json.JSONDecodeError):
            continue
        if not _summary_matches_scope(payload, timeframe, scope):
            continue
        created_at = _parse_datetime(payload.get('created_at')) or datetime.fromtimestamp(summary_path.stat().st_mtime, tz=UTC)
        candidates.append((created_at, summary_path, payload))

    if not candidates:
        return None

    _created_at, summary_path, payload = max(candidates, key=lambda item: item[0])
    all_pairs_path = Path(str(payload.get('all_pairs_path') or summary_path.with_name('all_pairs.parquet')))
    passed_pairs_path = Path(str(payload.get('passed_pairs_path') or summary_path.with_name('passed_pairs.parquet')))
    if not all_pairs_path.exists() or not passed_pairs_path.exists():
        return None
    return _load_snapshot_from_paths(
        scan_kind=scan_kind,
        timeframe=timeframe,
        scope=scope,
        summary_path=summary_path,
        all_pairs_path=all_pairs_path,
        passed_pairs_path=passed_pairs_path,
    )



def load_latest_saved_scan_result(
    *,
    broker: str,
    scan_kind: str,
    timeframe: Timeframe,
    universe_mode: ScanUniverseMode,
    normalized_group: str | None,
    allow_all_fallback: bool = False,
) -> SavedScanSnapshot | None:
    requested_scope = _scope_value(universe_mode, normalized_group)
    candidate_scopes = [requested_scope]
    if allow_all_fallback and requested_scope != ScanUniverseMode.ALL.value:
        candidate_scopes.append(ScanUniverseMode.ALL.value)

    for scope in candidate_scopes:
        latest_dir = _latest_scope_directory(broker=broker, scan_kind=scan_kind, timeframe=timeframe, scope=scope)
        summary_path = latest_dir / 'summary.json'
        all_pairs_path = latest_dir / 'all_pairs.parquet'
        passed_pairs_path = latest_dir / 'passed_pairs.parquet'
        if summary_path.exists() and all_pairs_path.exists() and passed_pairs_path.exists():
            return _load_snapshot_from_paths(
                scan_kind=scan_kind,
                timeframe=timeframe,
                scope=scope,
                summary_path=summary_path,
                all_pairs_path=all_pairs_path,
                passed_pairs_path=passed_pairs_path,
            )

        snapshot = _find_latest_run_snapshot(
            broker=broker,
            scan_kind=scan_kind,
            timeframe=timeframe,
            scope=scope,
        )
        if snapshot is not None:
            return snapshot

    legacy_dir = _legacy_latest_directory(broker=broker, scan_kind=scan_kind)
    legacy_summary = legacy_dir / 'summary.json'
    legacy_all_pairs = legacy_dir / 'all_pairs.parquet'
    legacy_passed_pairs = legacy_dir / 'passed_pairs.parquet'
    if legacy_summary.exists() and legacy_all_pairs.exists() and legacy_passed_pairs.exists():
        try:
            payload = _read_summary(legacy_summary)
        except (OSError, json.JSONDecodeError):
            payload = {}
        legacy_scope = str(payload.get('normalized_group') or payload.get('universe_mode') or 'all')
        if _summary_matches_scope(payload, timeframe, requested_scope) or (allow_all_fallback and legacy_scope == 'all'):
            return _load_snapshot_from_paths(
                scan_kind=scan_kind,
                timeframe=timeframe,
                scope=legacy_scope,
                summary_path=legacy_summary,
                all_pairs_path=legacy_all_pairs,
                passed_pairs_path=legacy_passed_pairs,
            )
    return None



def partner_symbols_from_snapshot(
    snapshot: SavedScanSnapshot,
    *,
    symbol_1: str,
    allowed_symbols: Sequence[str] | None = None,
) -> list[str]:
    if snapshot.passed_pairs.is_empty():
        return []

    allowed = set(allowed_symbols or [])
    partners: set[str] = set()
    for leg_1, leg_2 in snapshot.passed_pairs.select('symbol_1', 'symbol_2').iter_rows():
        if leg_1 == symbol_1:
            partners.add(str(leg_2))
        elif leg_2 == symbol_1:
            partners.add(str(leg_1))
    if allowed:
        partners &= allowed
    return sorted(partners)



def persist_johansen_scan_result(
    *,
    broker: str,
    timeframe: Timeframe,
    started_at: datetime,
    ended_at: datetime,
    universe_mode: ScanUniverseMode,
    normalized_group: str | None,
    symbols: Sequence[str] | None,
    result: JohansenUniverseScanResult,
    created_at: datetime | None = None,
) -> dict[str, Path]:
    created = created_at or datetime.now(UTC)
    scan_kind = DEFAULT_SCAN_KIND
    run_dir = _run_directory(
        broker=broker,
        scan_kind=scan_kind,
        timeframe=timeframe,
        started_at=started_at,
        ended_at=ended_at,
        universe_mode=universe_mode,
        normalized_group=normalized_group,
        created_at=created,
    )
    run_dir.mkdir(parents=True, exist_ok=True)

    all_rows = _rows_to_frame(result)
    passed_rows = all_rows.filter(pl.col('threshold_passed')) if not all_rows.is_empty() else all_rows

    all_pairs_path = run_dir / 'all_pairs.parquet'
    passed_pairs_path = run_dir / 'passed_pairs.parquet'
    summary_path = run_dir / 'summary.json'

    all_rows.write_parquet(all_pairs_path, compression='zstd', statistics=True)
    passed_rows.write_parquet(passed_pairs_path, compression='zstd', statistics=True)
    scope = _scope_value(universe_mode, normalized_group)
    summary_payload: dict[str, Any] = {
        'scan_kind': scan_kind,
        'broker': broker,
        'timeframe': timeframe.value,
        'started_at': started_at.astimezone(UTC).isoformat(),
        'ended_at': ended_at.astimezone(UTC).isoformat(),
        'universe_mode': universe_mode.value,
        'normalized_group': normalized_group,
        'scope': scope,
        'symbols': list(symbols or []),
        'created_at': created.astimezone(UTC).isoformat(),
        'summary': asdict(result.summary),
        'universe_symbols': result.universe_symbols,
        'all_pairs_path': str(all_pairs_path),
        'passed_pairs_path': str(passed_pairs_path),
    }
    summary_path.write_text(json.dumps(summary_payload, indent=2), encoding='utf-8')

    latest_dir = _latest_scope_directory(broker=broker, scan_kind=scan_kind, timeframe=timeframe, scope=scope)
    latest_dir.mkdir(parents=True, exist_ok=True)
    latest_all_pairs_path = latest_dir / 'all_pairs.parquet'
    latest_passed_pairs_path = latest_dir / 'passed_pairs.parquet'
    latest_summary_path = latest_dir / 'summary.json'
    all_rows.write_parquet(latest_all_pairs_path, compression='zstd', statistics=True)
    passed_rows.write_parquet(latest_passed_pairs_path, compression='zstd', statistics=True)
    latest_summary_path.write_text(json.dumps(summary_payload, indent=2), encoding='utf-8')

    legacy_latest_dir = _legacy_latest_directory(broker=broker, scan_kind=scan_kind)
    legacy_latest_dir.mkdir(parents=True, exist_ok=True)
    legacy_latest_all_pairs_path = legacy_latest_dir / 'all_pairs.parquet'
    legacy_latest_passed_pairs_path = legacy_latest_dir / 'passed_pairs.parquet'
    legacy_latest_summary_path = legacy_latest_dir / 'summary.json'
    all_rows.write_parquet(legacy_latest_all_pairs_path, compression='zstd', statistics=True)
    passed_rows.write_parquet(legacy_latest_passed_pairs_path, compression='zstd', statistics=True)
    legacy_latest_summary_path.write_text(json.dumps(summary_payload, indent=2), encoding='utf-8')

    return {
        'run_dir': run_dir,
        'all_pairs': all_pairs_path,
        'passed_pairs': passed_pairs_path,
        'summary': summary_path,
        'latest_dir': latest_dir,
        'latest_all_pairs': latest_all_pairs_path,
        'latest_passed_pairs': latest_passed_pairs_path,
        'latest_summary': latest_summary_path,
        'legacy_latest_dir': legacy_latest_dir,
        'legacy_latest_all_pairs': legacy_latest_all_pairs_path,
        'legacy_latest_passed_pairs': legacy_latest_passed_pairs_path,
        'legacy_latest_summary': legacy_latest_summary_path,
    }
