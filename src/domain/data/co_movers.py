from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable


ALL_CO_MOVERS_LABEL = "All matching co-movers"


@dataclass(frozen=True, slots=True)
class CoMoverGroup:
    section: str
    name: str
    symbols: tuple[str, ...]
    why: str
    notes: str

    @property
    def label(self) -> str:
        return f"{self.section}: {self.name}"


CO_MOVER_GROUPS: tuple[CoMoverGroup, ...] = (
    CoMoverGroup(
        section="FX",
        name="Commodity currencies",
        symbols=("AUDUSD+", "NZDUSD+"),
        why="Both usually benefit from risk-on and commodity-linked global growth.",
        notes="Clean directional co-move; often smoother than mixing with CAD.",
    ),
    CoMoverGroup(
        section="FX",
        name="Antipodean relative value",
        symbols=("AUDNZD+", "AUDUSD+", "NZDUSD+"),
        why="AUD and NZD are closely linked but react differently to China, dairy and rate differentials.",
        notes="AUDNZD+ is the clean spread; AUDUSD+ vs NZDUSD+ is the beta expression.",
    ),
    CoMoverGroup(
        section="FX",
        name="JPY risk-on basket",
        symbols=("AUDJPY+", "CADJPY+", "GBPJPY+", "USDJPY+"),
        why="JPY crosses often move together as a global risk appetite / carry basket.",
        notes="AUDJPY+ and CADJPY+ are usually cleaner than GBPJPY+.",
    ),
    CoMoverGroup(
        section="FX",
        name="European bloc",
        symbols=("EURUSD+", "GBPUSD+", "EURGBP+"),
        why="EUR and GBP share broad USD flow but diverge on UK-vs-Eurozone macro.",
        notes="EURGBP+ is the clean relative-value leg; EURUSD+ and GBPUSD+ are the common beta pair.",
    ),
    CoMoverGroup(
        section="FX",
        name="Scandis",
        symbols=("USDNOK+", "USDSEK+"),
        why="NOK and SEK are both European cyclicals and often co-move vs USD.",
        notes="NOK usually carries more oil sensitivity than SEK.",
    ),
    CoMoverGroup(
        section="FX",
        name="EM USD basket",
        symbols=("USDMXN+", "USDZAR+", "USDBRL+", "USDTRY+"),
        why="These often move with broad USD strength, EM risk and rates pressure.",
        notes="Good cluster for screening, but single names can break on idiosyncratic policy shocks.",
    ),
    CoMoverGroup(
        section="Indices",
        name="US large-cap beta",
        symbols=("SP500", "DJ30"),
        why="Both track the same broad US macro and earnings cycle.",
        notes="DJ30 is more old-economy / industrial; SP500 is broader.",
    ),
    CoMoverGroup(
        section="Indices",
        name="US tech vs broad market",
        symbols=("NAS100", "SP500"),
        why="Same US risk beta, but NAS100 carries more growth-duration and big-tech concentration.",
        notes="Useful for tech leadership / de-risking spreads.",
    ),
    CoMoverGroup(
        section="Indices",
        name="US small-cap vs large-cap",
        symbols=("US2000", "SP500"),
        why="Both are US equities, but US2000 is more domestic and credit-sensitive.",
        notes="Strong regime pair for risk-on / risk-off dispersion.",
    ),
    CoMoverGroup(
        section="Indices",
        name="Core Europe",
        symbols=("GER40", "EU50", "FRA40"),
        why="Heavy overlap in European cyclical and exporter exposure.",
        notes="GER40 vs EU50 is usually the cleanest screen.",
    ),
    CoMoverGroup(
        section="Indices",
        name="Greater China",
        symbols=("HK50", "CHINA50", "CHINAH"),
        why="All three reflect China / Hong Kong equity sentiment with different composition mixes.",
        notes="HK50 and CHINAH often move closest.",
    ),
    CoMoverGroup(
        section="Energy",
        name="Crude benchmark spread",
        symbols=("UKOUSD", "USOUSD"),
        why="Brent and WTI are the classic global oil pair and usually share the same macro driver.",
        notes="Watch storage / transport regime shifts when the spread widens structurally.",
    ),
    CoMoverGroup(
        section="Energy",
        name="Refined products vs crude",
        symbols=("GAS-C", "GASOIL-C", "UKOUSD", "USOUSD"),
        why="Gasoline and gasoil often track crude with refining-margin dispersion.",
        notes="Not as stable as Brent/WTI, but good for sector clusters.",
    ),
    CoMoverGroup(
        section="Energy",
        name="Oil equities vs crude",
        symbols=("CVX", "OXY", "UKOUSD", "USOUSD"),
        why="Oil producers usually share a large crude beta plus company-specific equity risk.",
        notes="CVX is higher-quality integrated beta; OXY is usually more leveraged to the oil cycle.",
    ),
    CoMoverGroup(
        section="Metals",
        name="Gold vs silver",
        symbols=("XAUUSD+", "XAGUSD"),
        why="Both react to real yields, USD and safe-haven demand.",
        notes="Silver usually carries more industrial beta and more volatility.",
    ),
    CoMoverGroup(
        section="Metals",
        name="Precious metal cross-currency gold",
        symbols=("XAUUSD+", "XAUEUR+", "XAUAUD+", "XAUJPY+"),
        why="Same underlying gold, different FX quote currency.",
        notes="Useful when you want gold exposure stripped or combined with FX views.",
    ),
    CoMoverGroup(
        section="Metals",
        name="Platinum group metals",
        symbols=("XPTUSD", "XPDUSD"),
        why="Both are automotive / industrial precious metals and often co-move on the same supply-demand cycle.",
        notes="Palladium is usually the wilder leg.",
    ),
    CoMoverGroup(
        section="Agriculture",
        name="Softs",
        symbols=("Coffee-C", "Cocoa-C", "Sugar-C", "Cotton-C"),
        why="These share agricultural weather, seasonality and soft-commodity flow regimes.",
        notes="Coffee-C and Cocoa-C are often the cleanest same-bucket pair in this catalog.",
    ),
    CoMoverGroup(
        section="Agriculture",
        name="Grain proxy",
        symbols=("Wheat-C",),
        why="Only one grain-like contract is present locally, so use it as a sector reference instead of a pair.",
        notes="If more grain symbols appear later, pair within grains first.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Banks / brokers",
        symbols=("JPM", "BAC", "WFC", "GS", "MS"),
        why="Shared sensitivity to rates, credit and financial conditions.",
        notes="JPM vs BAC and GS vs MS are the two cleanest same-industry pairs here.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Semiconductors",
        symbols=("AMD", "AVGO", "MU", "QCOM", "TSM"),
        why="Shared cycle in chips, AI capex and electronics demand.",
        notes="AMD/QCOM and AVGO/QCOM are practical co-move screens; MU is more memory-cycle specific.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Megacap tech",
        symbols=("AAPL", "MSFT", "META", "NFLX"),
        why="All are large US growth names driven by the same duration / tech-factor regime.",
        notes="AAPL vs MSFT is the cleanest mega-cap pair here.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="EV / autos",
        symbols=("TSLA", "RIVN", "NIO", "XPEV", "LI", "F", "GM"),
        why="Shared auto beta with a clear split between legacy OEMs and EV growth names.",
        notes="NIO/XPEV/LI is the clean China EV cluster; F/GM is the classic legacy pair.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Travel",
        symbols=("AALG", "DAL", "ABNB", "BKNG"),
        why="All benefit from the same travel-demand cycle but with different business models.",
        notes="AALG vs DAL is the clean airline pair; ABNB vs BKNG is a cleaner platform pair.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Consumer staples / retail",
        symbols=("KO", "PEP", "WMT", "TGT", "HD", "MCD", "SBUX", "NKE", "COST"),
        why="Shared consumer demand factor with clear subsectors inside it.",
        notes="KO/PEP, WMT/TGT and MCD/SBUX are the cleanest same-lane pairs.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Pharma / healthcare",
        symbols=("ABBVIE", "JNJ", "LLY", "MRK"),
        why="Large-cap defensive healthcare with common policy and rates sensitivity.",
        notes="MRK/JNJ and ABBVIE/MRK are usually cleaner than including LLY, which can be more single-theme driven.",
    ),
    CoMoverGroup(
        section="Stocks",
        name="Defense / industrial",
        symbols=("LMT", "NOC", "CAT"),
        why="Shared US industrial / capex / government spending sensitivity, though defense and machinery are not identical.",
        notes="LMT/NOC is the true same-subsector pair; CAT is better treated as an industrial beta reference.",
    ),
)


def _available_symbol_set(available_symbols: Iterable[str] | None) -> set[str] | None:
    if available_symbols is None:
        return None
    return {str(symbol) for symbol in available_symbols}


def co_mover_groups_for_symbol(symbol: str, *, available_symbols: Iterable[str] | None = None) -> list[CoMoverGroup]:
    allowed = _available_symbol_set(available_symbols)
    groups: list[CoMoverGroup] = []
    for group in CO_MOVER_GROUPS:
        if symbol not in group.symbols:
            continue
        if allowed is not None:
            mates = [candidate for candidate in group.symbols if candidate != symbol and candidate in allowed]
            if not mates:
                continue
        groups.append(group)
    return groups


def co_mover_group_labels_for_symbol(symbol: str, *, available_symbols: Iterable[str] | None = None) -> list[str]:
    groups = co_mover_groups_for_symbol(symbol, available_symbols=available_symbols)
    if not groups:
        return []
    return [ALL_CO_MOVERS_LABEL, *[group.label for group in groups]]


def co_mover_symbols_for_symbol(
    symbol: str,
    *,
    available_symbols: Iterable[str] | None = None,
    group_label: str | None = None,
) -> list[str]:
    allowed = _available_symbol_set(available_symbols)
    groups = co_mover_groups_for_symbol(symbol, available_symbols=available_symbols)
    if not groups:
        return []
    selected_groups = groups
    if group_label and group_label != ALL_CO_MOVERS_LABEL:
        selected_groups = [group for group in groups if group.label == group_label]
        if not selected_groups:
            return []
    related: set[str] = set()
    for group in selected_groups:
        for candidate in group.symbols:
            if candidate == symbol:
                continue
            if allowed is not None and candidate not in allowed:
                continue
            related.add(candidate)
    return sorted(related)
