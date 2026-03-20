from datetime import UTC, datetime

from domain.contracts import CointegrationScanRequest, ScanUniverseMode, UnitRootTest


def test_cointegration_scan_request_has_unit_root_gate_defaults() -> None:
    request = CointegrationScanRequest(
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        ended_at=datetime(2026, 3, 17, tzinfo=UTC),
        universe_mode=ScanUniverseMode.ALL,
    )

    assert request.unit_root_gate.test is UnitRootTest.ADF
    assert request.unit_root_gate.require_i1 is True
    assert request.unit_root_gate.difference_order == 1
