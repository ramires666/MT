from app_config import get_settings
from domain.costs.profiles import apply_broker_commission_fallback


def _base_spec(symbol: str, normalized_group: str, path: str, description: str) -> dict[str, object]:
    return {
        "symbol": symbol,
        "normalized_group": normalized_group,
        "path": path,
        "description": description,
        "commission_mode": "none",
        "commission_value": 0.0,
        "commission_currency": "",
        "commission_source_field": "",
        "commission_minimum": 0.0,
        "commission_entry_only": False,
    }


def test_bybit_zero_fee_mode_is_default(monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_BYBIT_TRADFI_FEE_MODE", "zero_fee")
    get_settings.cache_clear()
    resolved = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("AUDCAD+", "forex", r"Forex+\Forex Major\AUDCAD+", "Australian Dollar vs Canadian Dollar"),
    )
    assert resolved["commission_mode"] == "none"
    assert float(resolved["commission_value"]) == 0.0
    assert float(resolved["commission_minimum"]) == 0.0
    assert resolved["commission_entry_only"] is False
    assert resolved["commission_source_field"] == "bybit_official_zero_fee_default"
    get_settings.cache_clear()


def test_bybit_tight_spread_assigns_forex_and_precious_metals(monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_BYBIT_TRADFI_FEE_MODE", "tight_spread")
    get_settings.cache_clear()
    forex = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("AUDCAD+", "forex", r"Forex+\Forex Major\AUDCAD+", "Australian Dollar vs Canadian Dollar"),
    )
    metal = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("XAUUSD+", "commodities", r"Gold+\XAUUSD+", "Gold US Dollar"),
    )
    assert forex["commission_mode"] == "per_lot_round_turn"
    assert float(forex["commission_value"]) == 6.0
    assert forex["commission_entry_only"] is True
    assert metal["commission_mode"] == "per_lot_round_turn"
    assert float(metal["commission_value"]) == 6.0
    assert metal["commission_entry_only"] is True
    get_settings.cache_clear()


def test_bybit_tight_spread_assigns_indices_oil_and_stock_cfds(monkeypatch) -> None:
    monkeypatch.setenv("MT_SERVICE_BYBIT_TRADFI_FEE_MODE", "tight_spread")
    get_settings.cache_clear()
    nas100 = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("NAS100", "indices", r"CFDs\Indices Major\NAS100", "NAS100 Cash"),
    )
    nikkei = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("Nikkei225", "indices", r"Nikkei\Nikkei225", "Nikkei Index Cash CFD (JPY)"),
    )
    hk50 = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("HK50", "indices", r"CFDs\Indices Minor\HK50", "Hang Seng Index Cash CFD (HKD)"),
    )
    oil = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("UKOUSD", "commodities", r"Oil\UKOUSD", "Brent Crude Oil Cash"),
    )
    stock = apply_broker_commission_fallback(
        "bybit_mt5",
        _base_spec("AAPL", "stocks", r"US Stocks\AAPL", "Apple Inc CFD"),
    )
    assert float(nas100["commission_value"]) == 3.0
    assert nas100["commission_source_field"] == "bybit_official_tight_spread_indices_commodities_oil"
    assert float(nikkei["commission_value"]) == 0.1
    assert float(hk50["commission_value"]) == 1.5
    assert float(oil["commission_value"]) == 3.0
    assert float(stock["commission_value"]) == 0.02
    assert float(stock["commission_minimum"]) == 0.2
    assert stock["commission_entry_only"] is True
    get_settings.cache_clear()
