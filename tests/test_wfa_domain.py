from datetime import UTC, datetime

from domain.contracts import (
    Algorithm,
    PairSelection,
    StrategyDefaults,
    WfaMode,
    WfaPairMode,
    WfaRequest,
    WfaSelectionSource,
    WfaWindowSearchSpace,
    IntegerRange,
    Timeframe,
)
from domain.wfa import build_walk_windows, run_wfa_request


def test_build_walk_windows_anchored_and_rolling() -> None:
    started_at = datetime(2025, 1, 1, tzinfo=UTC)
    ended_at = datetime(2025, 3, 1, tzinfo=UTC)

    anchored = build_walk_windows(
        started_at=started_at,
        ended_at=ended_at,
        mode=WfaMode.ANCHORED,
        timeframe=Timeframe.M15,
        train_units=4,
        validation_units=1,
        test_units=1,
        walk_step_units=1,
        train_unit="weeks",
        validation_unit="weeks",
        test_unit="weeks",
        walk_step_unit="weeks",
    )
    rolling = build_walk_windows(
        started_at=started_at,
        ended_at=ended_at,
        mode=WfaMode.ROLLING,
        timeframe=Timeframe.M15,
        train_units=4,
        validation_units=1,
        test_units=1,
        walk_step_units=1,
        train_unit="weeks",
        validation_unit="weeks",
        test_unit="weeks",
        walk_step_unit="weeks",
    )

    assert anchored
    assert rolling
    assert anchored[0].train_started_at == started_at
    assert anchored[1].train_started_at == started_at
    assert rolling[0].train_started_at == started_at
    assert rolling[1].train_started_at > started_at


def test_run_wfa_request_rejects_non_distance() -> None:
    request = WfaRequest(
        pairs=[PairSelection(symbol_1="US2000", symbol_2="NAS100")],
        pair_mode=WfaPairMode.SINGLE,
        selection_source=WfaSelectionSource.TESTER_MENU,
        algorithm=Algorithm.JOHANSEN,
        timeframe=Timeframe.M15,
        started_at=datetime(2025, 1, 1, tzinfo=UTC),
        ended_at=datetime(2025, 3, 1, tzinfo=UTC),
        wfa_mode=WfaMode.ANCHORED,
        objective_metric="omega_ratio",
        window_search=WfaWindowSearchSpace(
            unit="weeks",
            train=IntegerRange(start=4, stop=4, step=1),
            validation=IntegerRange(start=1, stop=1, step=1),
            test=IntegerRange(start=1, stop=1, step=1),
            walk_step=IntegerRange(start=1, stop=1, step=1),
        ),
        defaults=StrategyDefaults(),
    )

    try:
        run_wfa_request("bybit_mt5", request)
    except ValueError as exc:
        assert "Only distance WFA" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected ValueError for non-distance WFA")


def test_build_walk_windows_supports_mixed_units() -> None:
    started_at = datetime(2025, 1, 1, tzinfo=UTC)
    ended_at = datetime(2025, 4, 15, tzinfo=UTC)

    windows = build_walk_windows(
        started_at=started_at,
        ended_at=ended_at,
        mode=WfaMode.ANCHORED,
        timeframe=Timeframe.M15,
        train_units=1,
        validation_units=1,
        test_units=1,
        walk_step_units=1,
        train_unit="months",
        validation_unit="weeks",
        test_unit="weeks",
        walk_step_unit="weeks",
    )

    assert windows
    first = windows[0]
    assert first.train_started_at == started_at
    assert first.train_ended_at.month == 2 and first.train_ended_at.day == 1
    assert first.validation_ended_at.month == 2 and first.validation_ended_at.day == 8
    assert first.test_ended_at.month == 2 and first.test_ended_at.day == 15
