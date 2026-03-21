from dataclasses import asdict

from fastapi import APIRouter, HTTPException

from app_config import get_settings
from domain.contracts import Algorithm, OptimizationMode, OptimizationRequest
from domain.optimizer import optimize_distance_genetic, optimize_distance_grid

router = APIRouter(prefix='/optimization', tags=['optimization'])


@router.get('/healthz')
def optimization_health() -> dict[str, str]:
    return {'component': 'optimization', 'status': 'ok'}


def _serialize_result(result) -> dict[str, object]:
    return {
        'objective_metric': result.objective_metric,
        'evaluated_trials': result.evaluated_trials,
        'best_trial_id': result.best_trial_id,
        'cancelled': result.cancelled,
        'failure_reason': result.failure_reason,
        'rows': [asdict(row) for row in result.rows],
    }


@router.post('/distance/grid')
def optimize_distance_grid_route(request: OptimizationRequest) -> dict[str, object]:
    if request.algorithm != Algorithm.DISTANCE:
        raise HTTPException(status_code=400, detail='Only distance optimization is wired right now.')
    if request.mode != OptimizationMode.GRID:
        raise HTTPException(status_code=400, detail='Only grid mode is wired on this route.')

    settings = get_settings()
    try:
        result = optimize_distance_grid(
            broker=settings.default_broker_id,
            pair=request.pair,
            timeframe=request.timeframe,
            started_at=request.started_at,
            ended_at=request.ended_at,
            defaults=request.defaults,
            search_space=request.search_space,
            objective_metric=request.objective_metric,
            parallel_workers=settings.optimizer_parallel_workers,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _serialize_result(result)


@router.post('/distance/genetic')
def optimize_distance_genetic_route(request: OptimizationRequest) -> dict[str, object]:
    if request.algorithm != Algorithm.DISTANCE:
        raise HTTPException(status_code=400, detail='Only distance optimization is wired right now.')
    if request.mode != OptimizationMode.GENETIC:
        raise HTTPException(status_code=400, detail='Only genetic mode is wired on this route.')

    settings = get_settings()
    try:
        result = optimize_distance_genetic(
            broker=settings.default_broker_id,
            pair=request.pair,
            timeframe=request.timeframe,
            started_at=request.started_at,
            ended_at=request.ended_at,
            defaults=request.defaults,
            search_space=request.search_space,
            objective_metric=request.objective_metric,
            config=request.algorithm_params,
            parallel_workers=settings.optimizer_parallel_workers,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return _serialize_result(result)
