from bokeh_app.scanner_estimate import estimate_scanner_runtime_seconds, scanner_pair_count


def test_scanner_pair_count_uses_combinations_without_repetition() -> None:
    assert scanner_pair_count(0) == 0
    assert scanner_pair_count(1) == 0
    assert scanner_pair_count(2) == 1
    assert scanner_pair_count(5) == 10


def test_estimate_scanner_runtime_seconds_grows_with_pairs_and_trials() -> None:
    small = estimate_scanner_runtime_seconds(pair_count=10, trials_per_pair=100, workers=8)
    larger_pairs = estimate_scanner_runtime_seconds(pair_count=20, trials_per_pair=100, workers=8)
    larger_trials = estimate_scanner_runtime_seconds(pair_count=10, trials_per_pair=200, workers=8)

    assert small > 0.0
    assert larger_pairs > small
    assert larger_trials > small


def test_estimate_scanner_runtime_seconds_shrinks_with_more_workers() -> None:
    slower = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=4)
    faster = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8)

    assert faster < slower


def test_estimate_scanner_runtime_seconds_discounts_completed_pairs() -> None:
    full = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8)
    partial = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8, completed_pairs=40)
    done = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8, completed_pairs=100)

    assert partial < full
    assert done == 0.0


def test_estimate_scanner_runtime_seconds_respects_completed_pairs() -> None:
    full = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8)
    resumed = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8, completed_pairs=40)
    finished = estimate_scanner_runtime_seconds(pair_count=100, trials_per_pair=500, workers=8, completed_pairs=100)

    assert resumed < full
    assert finished == 0.0
