from __future__ import annotations

from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor


def build_process_pool(max_workers: int | None = None) -> Executor:
    try:
        return ProcessPoolExecutor(max_workers=max_workers)
    except (PermissionError, OSError):
        return ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fallback-cpu")
