from enum import StrEnum


class JobType(StrEnum):
    BACKTEST = "backtest"
    OPTIMIZATION = "optimization"
    COINTEGRATION_SCAN = "cointegration_scan"
    QUOTE_SYNC = "quote_sync"


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
