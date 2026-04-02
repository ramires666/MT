from __future__ import annotations


PAIR_SETUP_SECONDS = 0.35
TRIAL_EVAL_SECONDS = 0.01
PIPELINE_OVERHEAD_FACTOR = 1.10


def scanner_pair_count(symbol_count: int) -> int:
    normalized = max(0, int(symbol_count))
    if normalized < 2:
        return 0
    return normalized * (normalized - 1) // 2


def estimate_scanner_runtime_seconds(
    *,
    pair_count: int,
    trials_per_pair: int,
    workers: int,
    completed_pairs: int = 0,
) -> float:
    normalized_pairs = max(0, int(pair_count))
    normalized_trials = max(0, int(trials_per_pair))
    normalized_workers = max(1, int(workers))
    normalized_completed = min(normalized_pairs, max(0, int(completed_pairs)))
    remaining_pairs = max(0, normalized_pairs - normalized_completed)
    if remaining_pairs <= 0 or normalized_trials <= 0:
        return 0.0
    setup_seconds = float(remaining_pairs) * PAIR_SETUP_SECONDS
    trial_seconds = (
        float(remaining_pairs * normalized_trials)
        * TRIAL_EVAL_SECONDS
        * PIPELINE_OVERHEAD_FACTOR
        / float(normalized_workers)
    )
    return setup_seconds + trial_seconds
