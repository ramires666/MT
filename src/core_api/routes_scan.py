from __future__ import annotations

from dataclasses import asdict

from fastapi import APIRouter

from app_config import get_settings
from domain.contracts import CointegrationScanRequest, JohansenPairScanRequest
from domain.scan.johansen import JohansenScanParameters, scan_pair_johansen, scan_universe_johansen
from storage.scan_results import persist_johansen_scan_result

router = APIRouter(prefix='/scan', tags=['scan'])


@router.get('/healthz')
def health_check() -> dict[str, str]:
    return {'status': 'ok', 'component': 'scan'}


@router.post('/johansen/pair')
def run_johansen_pair_scan(request: JohansenPairScanRequest) -> dict[str, object]:
    settings = get_settings()
    result = scan_pair_johansen(
        broker=settings.default_broker_id,
        pair=request.pair,
        timeframe=request.timeframe,
        started_at=request.started_at,
        ended_at=request.ended_at,
        unit_root_gate=request.unit_root_gate,
        params=JohansenScanParameters(
            det_order=request.det_order,
            k_ar_diff=request.k_ar_diff,
            significance_level=request.significance_level,
            use_log_prices=request.use_log_prices,
            zscore_lookback_bars=request.zscore_lookback_bars,
        ),
    )
    payload = asdict(result)
    payload['pair'] = request.pair.model_dump()
    payload['unit_root'] = {
        'eligible_for_cointegration': result.unit_root.eligible_for_cointegration,
        'leg_1': asdict(result.unit_root.leg_1),
        'leg_2': asdict(result.unit_root.leg_2),
    }
    return payload


@router.post('/johansen/batch')
def run_johansen_batch_scan(request: CointegrationScanRequest) -> dict[str, object]:
    settings = get_settings()
    result = scan_universe_johansen(
        broker=settings.default_broker_id,
        timeframe=request.timeframe,
        started_at=request.started_at,
        ended_at=request.ended_at,
        universe_mode=request.universe_mode,
        normalized_group=request.normalized_group,
        symbols=request.symbols,
        unit_root_gate=request.unit_root_gate,
        params=JohansenScanParameters(),
        parallel_workers=settings.scan_parallel_workers,
    )
    saved_paths = persist_johansen_scan_result(
        broker=settings.default_broker_id,
        timeframe=request.timeframe,
        started_at=request.started_at,
        ended_at=request.ended_at,
        universe_mode=request.universe_mode,
        normalized_group=request.normalized_group,
        symbols=request.symbols,
        result=result,
    )
    return {
        'summary': asdict(result.summary),
        'rows': [asdict(row) for row in result.rows if row.threshold_passed],
        'universe_symbols': result.universe_symbols,
        'storage': {key: str(value) for key, value in saved_paths.items()},
    }
