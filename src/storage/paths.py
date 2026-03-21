from __future__ import annotations

from pathlib import Path

from app_config import get_settings


def raw_quotes_root() -> Path:
    return get_settings().data_root / "parquet" / "raw"


def derived_quotes_root() -> Path:
    return get_settings().data_root / "parquet" / "derived"


def catalog_root() -> Path:
    return get_settings().data_root / "catalog"


def scans_root() -> Path:
    return get_settings().data_root / "scans"


def ui_state_path() -> Path:
    return get_settings().data_root / "ui_state" / "bokeh_app_state.json"


def wfa_root() -> Path:
    return get_settings().data_root / "wfa"


def meta_selector_root() -> Path:
    return get_settings().data_root / "meta_selector"
