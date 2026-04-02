from __future__ import annotations

from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
import os
from typing import Iterator


CPU_WORKER_THREAD_CAPS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
}


def _apply_cpu_worker_env_caps() -> None:
    for name, value in CPU_WORKER_THREAD_CAPS.items():
        os.environ.setdefault(name, value)


def build_process_pool(max_workers: int | None = None) -> Executor:
    _apply_cpu_worker_env_caps()
    try:
        return ProcessPoolExecutor(max_workers=max_workers)
    except (PermissionError, OSError):
        return ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fallback-cpu")


@contextmanager
def shared_process_pool(max_workers: int | None = None) -> Iterator[Executor | None]:
    if max_workers is None or int(max_workers) <= 1:
        yield None
        return
    executor = build_process_pool(max_workers=max_workers)
    try:
        yield executor
    finally:
        shutdown_executor(executor)


def shutdown_executor(
    executor: Executor | None,
    *,
    wait: bool = True,
    cancel_futures: bool = False,
    force_kill: bool = False,
) -> None:
    if executor is None:
        return

    process_pool = executor if isinstance(executor, ProcessPoolExecutor) else None
    processes = list((getattr(process_pool, "_processes", None) or {}).values()) if process_pool is not None else []

    try:
        executor.shutdown(wait=wait, cancel_futures=cancel_futures)
    except Exception:
        pass

    if not force_kill or process_pool is None:
        return

    for process in processes:
        try:
            if process.is_alive():
                process.terminate()
        except Exception:
            continue
    for process in processes:
        try:
            if process.is_alive():
                process.kill()
        except Exception:
            continue
    for process in processes:
        try:
            process.join(timeout=0.2)
        except Exception:
            continue
