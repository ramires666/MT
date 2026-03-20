from fastapi.testclient import TestClient

from core_api.main import app
from domain.scan.johansen import JohansenUniverseScanResult, JohansenUniverseScanRow, JohansenUniverseScanSummary


client = TestClient(app)


def test_scan_health_route() -> None:
    response = client.get('/api/v1/scan/healthz')

    assert response.status_code == 200
    assert response.json()['component'] == 'scan'


def test_scan_batch_route_returns_summary(monkeypatch) -> None:
    def fake_scan_universe_johansen(**_kwargs) -> JohansenUniverseScanResult:
        return JohansenUniverseScanResult(
            summary=JohansenUniverseScanSummary(
                total_symbols_requested=3,
                loaded_symbols=3,
                prefiltered_i1_symbols=2,
                total_pairs_evaluated=1,
                threshold_passed_pairs=1,
                screened_out_pairs=0,
            ),
            rows=[
                JohansenUniverseScanRow(
                    symbol_1='US2000',
                    symbol_2='NAS100',
                    sample_size=100,
                    eligible_for_cointegration=True,
                    unit_root_leg_1='I(1)',
                    unit_root_leg_2='I(1)',
                    rank=1,
                    threshold_passed=True,
                    trace_stat_0=12.5,
                    max_eigen_stat_0=10.1,
                    hedge_ratio=0.9,
                    half_life_bars=22.0,
                    last_zscore=0.8,
                    failure_reason=None,
                )
            ],
            universe_symbols=['US2000', 'NAS100', 'XAUUSD+'],
        )

    monkeypatch.setattr('core_api.routes_scan.scan_universe_johansen', fake_scan_universe_johansen)

    monkeypatch.setattr(
        'core_api.routes_scan.persist_johansen_scan_result',
        lambda **_kwargs: {
            'latest_passed_pairs': 'data/scans/bybit_mt5/johansen/latest/passed_pairs.parquet',
            'latest_all_pairs': 'data/scans/bybit_mt5/johansen/latest/all_pairs.parquet',
        },
    )

    response = client.post(
        '/api/v1/scan/johansen/batch',
        json={
            'timeframe': 'M15',
            'started_at': '2026-01-01T00:00:00Z',
            'ended_at': '2026-03-17T00:00:00Z',
            'universe_mode': 'all',
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body['summary']['prefiltered_i1_symbols'] == 2
    assert body['rows'][0]['symbol_1'] == 'US2000'
    assert body['storage']['latest_passed_pairs'].endswith('passed_pairs.parquet')
