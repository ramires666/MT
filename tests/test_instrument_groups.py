from domain.contracts import NormalizedGroup
from domain.data.instrument_groups import normalize_group


def test_normalize_forex_from_symbol_shape() -> None:
    assert normalize_group(path=None, symbol="EURUSD") == NormalizedGroup.FOREX


def test_normalize_indices_from_path() -> None:
    assert normalize_group(path="CFD\\Indices\\US500", symbol="US500") == NormalizedGroup.INDICES


def test_normalize_commodities_from_symbol_prefix() -> None:
    assert normalize_group(path=None, symbol="XAUUSD") == NormalizedGroup.COMMODITIES


def test_normalize_crypto_from_description() -> None:
    assert normalize_group(path=None, description="Crypto majors", symbol="BTCUSD") == NormalizedGroup.CRYPTO