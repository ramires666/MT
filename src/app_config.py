from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="MT_SERVICE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "mt-pair-trading-service"
    environment: str = "dev"
    api_host: str = "127.0.0.1"
    api_port: int = 8000
    bokeh_host: str = "127.0.0.1"
    bokeh_port: int = 5006
    postgres_dsn: str = "postgresql://postgres:postgres@localhost:5432/mt_pair"
    redis_dsn: str = "redis://localhost:6379/0"
    data_root: Path = Field(default=Path("data"))
    default_broker_id: str = "bybit_mt5"
    mt5_terminal_path: str | None = None
    mt5_common_root: str | None = None
    broker_timezone_name: str = "UTC+2-fixed"
    broker_timezone_offset_minutes: int = 120
    default_initial_capital: float = 10_000.0
    default_leverage: float = 100.0
    default_margin_budget_per_leg: float = 500.0
    default_slippage_points: float = 1.0
    optimizer_parallel_workers: int = 8
    wfa_history_top_k: int = 32
    scan_parallel_workers: int = 8
    bybit_tradfi_fee_mode: str = "tight_spread"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
