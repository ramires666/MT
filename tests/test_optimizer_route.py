from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from core_api.main import app
from domain.optimizer import DistanceOptimizationResult, DistanceOptimizationRow


@pytest.fixture
def client() -> TestClient:
    with TestClient(app) as test_client:
        yield test_client


def test_optimization_health_route(client: TestClient) -> None:
    response = client.get('/api/v1/optimization/healthz')

    assert response.status_code == 200
    assert response.json()['component'] == 'optimization'


def test_distance_grid_route_returns_trials(monkeypatch, client: TestClient) -> None:
    def fake_optimize_distance_grid(**_kwargs) -> DistanceOptimizationResult:
        return DistanceOptimizationResult(
            objective_metric='net_profit',
            evaluated_trials=1,
            rows=[
                DistanceOptimizationRow(
                    trial_id=1,
                    objective_metric='net_profit',
                    objective_score=123.45,
                    net_profit=123.45,
                    ending_equity=10123.45,
                    max_drawdown=45.0,
                    pnl_to_maxdd=2.7433,
                    omega_ratio=1.8,
                    k_ratio=0.7,
                    score_log_trades=2.53,
                    ulcer_index=0.9,
                    ulcer_performance=13.2,
                    cagr=0.18,
                    cagr_to_ulcer=0.2,
                    r_squared=0.91,
                    calmar=1.3,
                    beauty_score=0.182,
                    trades=12,
                    win_rate=0.6,
                    lookback_bars=96,
                    entry_z=2.0,
                    exit_z=0.5,
                    stop_z=3.5,
                    bollinger_k=2.0,
                )
            ],
            best_trial_id=1,
        )

    monkeypatch.setattr('core_api.routes_optimizer.optimize_distance_grid', fake_optimize_distance_grid)

    response = client.post(
        '/api/v1/optimization/distance/grid',
        json={
            'pair': {'symbol_1': 'US2000', 'symbol_2': 'NAS100'},
            'algorithm': 'distance',
            'mode': 'grid',
            'timeframe': 'M15',
            'started_at': '2026-01-01T00:00:00Z',
            'ended_at': '2026-03-17T00:00:00Z',
            'objective_metric': 'net_profit',
            'search_space': {
                'lookback_bars': {'start': 48, 'stop': 96, 'step': 48},
                'entry_z': {'start': 1.5, 'stop': 2.0, 'step': 0.5},
                'exit_z': {'start': 0.5, 'stop': 0.5, 'step': 0.1},
                'stop_z': {'start': 3.5, 'stop': 3.5, 'step': 0.1},
                'bollinger_k': {'start': 2.0, 'stop': 2.0, 'step': 0.1},
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body['evaluated_trials'] == 1
    assert body['best_trial_id'] == 1
    assert body['rows'][0]['lookback_bars'] == 96
    assert body['rows'][0]['objective_score'] == 123.45


def test_distance_genetic_route_returns_rows(monkeypatch, client: TestClient) -> None:
    def fake_optimize_distance_genetic(**_kwargs) -> DistanceOptimizationResult:
        return DistanceOptimizationResult(
            objective_metric='omega_ratio',
            evaluated_trials=6,
            rows=[
                DistanceOptimizationRow(
                    trial_id=4,
                    objective_metric='omega_ratio',
                    objective_score=1.91,
                    net_profit=321.0,
                    ending_equity=10321.0,
                    max_drawdown=80.0,
                    pnl_to_maxdd=4.0125,
                    omega_ratio=1.91,
                    k_ratio=0.55,
                    score_log_trades=1.24,
                    ulcer_index=0.4,
                    ulcer_performance=802.5,
                    cagr=0.26,
                    cagr_to_ulcer=0.65,
                    r_squared=0.88,
                    calmar=1.9,
                    beauty_score=0.572,
                    trades=18,
                    win_rate=0.61,
                    lookback_bars=72,
                    entry_z=1.8,
                    exit_z=0.4,
                    stop_z=3.2,
                    bollinger_k=2.0,
                )
            ],
            best_trial_id=4,
        )

    monkeypatch.setattr('core_api.routes_optimizer.optimize_distance_genetic', fake_optimize_distance_genetic)

    response = client.post(
        '/api/v1/optimization/distance/genetic',
        json={
            'pair': {'symbol_1': 'US2000', 'symbol_2': 'NAS100'},
            'algorithm': 'distance',
            'mode': 'genetic',
            'timeframe': 'M15',
            'started_at': '2026-01-01T00:00:00Z',
            'ended_at': '2026-03-17T00:00:00Z',
            'objective_metric': 'omega_ratio',
            'search_space': {
                'lookback_bars': {'start': 48, 'stop': 96, 'step': 24},
                'entry_z': {'start': 1.5, 'stop': 2.0, 'step': 0.5},
                'exit_z': {'start': 0.3, 'stop': 0.5, 'step': 0.2},
                'stop_z': {'start': 3.0, 'stop': 3.5, 'step': 0.5},
                'bollinger_k': {'start': 2.0, 'stop': 2.0, 'step': 0.1},
            },
            'algorithm_params': {
                'population_size': 10,
                'generations': 4,
                'elite_count': 2,
                'mutation_rate': 0.2,
                'random_seed': 17,
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body['evaluated_trials'] == 6
    assert body['best_trial_id'] == 4
    assert body['rows'][0]['objective_metric'] == 'omega_ratio'
    assert body['rows'][0]['objective_score'] == 1.91
