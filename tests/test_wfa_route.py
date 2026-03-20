from core_api.main import app
from fastapi.testclient import TestClient

client = TestClient(app)


def test_wfa_run_route_accepts_payload(monkeypatch) -> None:
    payload = {
        "pairs": [{"symbol_1": "US2000", "symbol_2": "NAS100"}],
        "pair_mode": "single",
        "selection_source": "tester_menu",
        "algorithm": "distance",
        "timeframe": "M15",
        "started_at": "2025-01-01T00:00:00Z",
        "ended_at": "2026-03-17T00:00:00Z",
        "wfa_mode": "anchored",
        "objective_metric": "omega_ratio",
        "window_search": {
            "unit": "weeks",
            "train": {"start": 4, "stop": 12, "step": 4},
            "validation": {"start": 1, "stop": 4, "step": 1},
            "test": {"start": 1, "stop": 2, "step": 1},
            "walk_step": {"start": 1, "stop": 1, "step": 1}
        },
        "defaults": {
            "initial_capital": 10000,
            "leverage": 100,
            "margin_budget_per_leg": 500,
            "slippage_points": 1
        },
        "algorithm_params": {"entry_z": 2.0, "exit_z": 0.5},
        "parameter_search_space": {"entry_z": [1.5, 2.0, 2.5]}
    }

    def fake_run_wfa_request(_broker: str, request):
        return {
            "status": "completed",
            "mode": request.wfa_mode.value,
            "pair_mode": request.pair_mode.value,
            "pair_count": len(request.pairs),
            "selection_source": request.selection_source.value,
            "trial_count": 1,
            "best_trial": {"trial_id": 1},
            "window_trials": [{"trial_id": 1}],
            "failure_reason": None,
        }

    monkeypatch.setattr("core_api.routes_wfa.run_wfa_request", fake_run_wfa_request)
    response = client.post("/api/v1/wfa/run", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "completed"
    assert body["mode"] == "anchored"
    assert body["pair_count"] == 1
    assert body["trial_count"] == 1
