from __future__ import annotations

import re

from domain.contracts import NormalizedGroup


_GROUP_PATTERNS: list[tuple[NormalizedGroup, tuple[str, ...]]] = [
    (NormalizedGroup.CRYPTO, ("crypto", "bitcoin", "ethereum", "btc", "eth", "sol", "xrp", "ada")),
    (NormalizedGroup.COMMODITIES, ("commodity", "metals", "metal", "gold", "silver", "xau", "xag", "oil", "brent", "wti", "gas")),
    (NormalizedGroup.INDICES, ("index", "indices", "cash", "nasdaq", "spx", "sp500", "us500", "us100", "dj", "dax", "ftse", "nikkei", "hk50")),
    (NormalizedGroup.STOCKS, ("stock", "stocks", "equity", "equities", "share", "shares", "nyse", "nasdaq shares")),
    (NormalizedGroup.FOREX, ("forex", "fx", "majors", "minors", "exotics", "currency", "currencies")),
]


def normalize_group(path: str | None, description: str | None = None, symbol: str | None = None) -> NormalizedGroup:
    haystack = " ".join(part for part in (path, description, symbol) if part).lower()
    compact_symbol = re.sub(r"[^a-z0-9]", "", (symbol or "").lower())

    for normalized_group, markers in _GROUP_PATTERNS:
        if any(marker in haystack for marker in markers):
            return normalized_group

    if len(compact_symbol) == 6 and compact_symbol.isalpha():
        return NormalizedGroup.FOREX

    if compact_symbol.startswith(("xau", "xag", "brent", "wti", "ukoil", "usoil")):
        return NormalizedGroup.COMMODITIES

    if compact_symbol.startswith(("btc", "eth", "sol", "xrp", "ada")):
        return NormalizedGroup.CRYPTO

    return NormalizedGroup.CUSTOM