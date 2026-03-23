# Bybit MT5 instrument map and co-move pairs

Generated from local catalog `data/catalog/bybit_mt5/instrument_catalog.parquet` on 2026-03-23 08:34:25 UTC.

## What this file is

- Local inventory only: `366` instruments currently present in this workspace.
- This file separates the raw catalog group from a path-based human family, because the raw catalog contains some classification mistakes.
- `+` at the end of a symbol is a broker suffix, not a different economic asset.
- `-C` in this catalog usually means a cash commodity contract.

## Family counts

- Energy: `5`
- FX: `84`
- Indices: `21`
- Metals: `9`
- Other Commodities: `2`
- Softs / Agriculture: `5`
- US Stocks: `240`

## Catalog quality notes

- Raw `normalized_group` disagrees with path-based family for `23` symbols.
- This matters for pair discovery: some FX, metals and stocks are locally tagged into the wrong group.

| Symbol | Raw catalog group | Human family | Raw path |
| --- | --- | --- | --- |
| `AUDJPY+` | `indices` | FX | `Forex+\Forex Major\AUDJPY+` |
| `CADJPY+` | `indices` | FX | `Forex+\Forex Major\CADJPY+` |
| `COPPER-C` | `custom` | Metals | `Commodities\COPPER-C` |
| `Cocoa-C` | `indices` | Softs / Agriculture | `Commodities\Cocoa-C` |
| `Coffee-C` | `indices` | Softs / Agriculture | `Commodities\Coffee-C` |
| `Cotton-C` | `indices` | Softs / Agriculture | `Commodities\Cotton-C` |
| `FSLR` | `crypto` | US Stocks | `Stocks\US\FSLR` |
| `GAS-C` | `crypto` | Energy | `Commodities\GAS-C` |
| `GS` | `commodities` | US Stocks | `Stocks\US\GS` |
| `IR` | `crypto` | US Stocks | `Stocks\US\IR` |
| `NETH25` | `crypto` | Indices | `CFDs\Indices Minor\NETH25` |
| `NZDJPY+` | `indices` | FX | `Forex+\Forex Minor\Forex Minor -Forex\NZDJPY+` |
| `OJ-C` | `indices` | Other Commodities | `Commodities\OJ-C` |
| `PSIX` | `crypto` | US Stocks | `Stocks\US\PSIX` |
| `SGDJPY+` | `indices` | FX | `Forex+\Forex Minor\Forex Minor -Forex\SGDJPY+` |
| `Soybean-C` | `indices` | Other Commodities | `Commodities\Soybean-C` |
| `Sugar-C` | `indices` | Softs / Agriculture | `Commodities\Sugar-C` |
| `USDJPY` | `indices` | FX | `Forex Major\Forex Major\USDJPY` |
| `USDJPY+` | `indices` | FX | `Forex+\Forex Major\USDJPY+` |
| `WPM` | `commodities` | US Stocks | `Stocks\US\WPM` |
| `Wheat-C` | `indices` | Softs / Agriculture | `Commodities\Wheat-C` |
| `XPDUSD` | `indices` | Metals | `Commodities\XPDUSD` |
| `XPTUSD` | `forex` | Metals | `Commodities\XPTUSD` |

## Co-move clusters and practical pair ideas

These are not guaranteed stationary pairs. They are the first places worth scanning because they share the same macro driver, sector beta or quote-currency regime.

### Agriculture

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| Softs | `Coffee-C, Cocoa-C, Sugar-C, Cotton-C` | These share agricultural weather, seasonality and soft-commodity flow regimes. | Coffee-C and Cocoa-C are often the cleanest same-bucket pair in this catalog. |
| Grain proxy | `Wheat-C` | Only one grain-like contract is present locally, so use it as a sector reference instead of a pair. | If more grain symbols appear later, pair within grains first. |

### Energy

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| Crude benchmark spread | `UKOUSD, USOUSD` | Brent and WTI are the classic global oil pair and usually share the same macro driver. | Watch storage / transport regime shifts when the spread widens structurally. |
| Refined products vs crude | `GAS-C, GASOIL-C, UKOUSD, USOUSD` | Gasoline and gasoil often track crude with refining-margin dispersion. | Not as stable as Brent/WTI, but good for sector clusters. |
| Oil equities vs crude | `CVX, OXY, UKOUSD, USOUSD` | Oil producers usually share a large crude beta plus company-specific equity risk. | CVX is higher-quality integrated beta; OXY is usually more leveraged to the oil cycle. |

### FX

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| Commodity currencies | `AUDUSD+, NZDUSD+` | Both usually benefit from risk-on and commodity-linked global growth. | Clean directional co-move; often smoother than mixing with CAD. |
| Antipodean relative value | `AUDNZD+, AUDUSD+, NZDUSD+` | AUD and NZD are closely linked but react differently to China, dairy and rate differentials. | AUDNZD+ is the clean spread; AUDUSD+ vs NZDUSD+ is the beta expression. |
| JPY risk-on basket | `AUDJPY+, CADJPY+, GBPJPY+, USDJPY+` | JPY crosses often move together as a global risk appetite / carry basket. | AUDJPY+ and CADJPY+ are usually cleaner than GBPJPY+. |
| European bloc | `EURUSD+, GBPUSD+, EURGBP+` | EUR and GBP share broad USD flow but diverge on UK-vs-Eurozone macro. | EURGBP+ is the clean relative-value leg; EURUSD+ and GBPUSD+ are the common beta pair. |
| Scandis | `USDNOK+, USDSEK+` | NOK and SEK are both European cyclicals and often co-move vs USD. | NOK usually carries more oil sensitivity than SEK. |
| EM USD basket | `USDMXN+, USDZAR+, USDBRL+, USDTRY+` | These often move with broad USD strength, EM risk and rates pressure. | Good cluster for screening, but single names can break on idiosyncratic policy shocks. |

### Indices

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| US large-cap beta | `SP500, DJ30` | Both track the same broad US macro and earnings cycle. | DJ30 is more old-economy / industrial; SP500 is broader. |
| US tech vs broad market | `NAS100, SP500` | Same US risk beta, but NAS100 carries more growth-duration and big-tech concentration. | Useful for tech leadership / de-risking spreads. |
| US small-cap vs large-cap | `US2000, SP500` | Both are US equities, but US2000 is more domestic and credit-sensitive. | Strong regime pair for risk-on / risk-off dispersion. |
| Core Europe | `GER40, EU50, FRA40` | Heavy overlap in European cyclical and exporter exposure. | GER40 vs EU50 is usually the cleanest screen. |
| Greater China | `HK50, CHINA50, CHINAH` | All three reflect China / Hong Kong equity sentiment with different composition mixes. | HK50 and CHINAH often move closest. |

### Metals

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| Gold vs silver | `XAUUSD+, XAGUSD` | Both react to real yields, USD and safe-haven demand. | Silver usually carries more industrial beta and more volatility. |
| Precious metal cross-currency gold | `XAUUSD+, XAUEUR+, XAUAUD+, XAUJPY+` | Same underlying gold, different FX quote currency. | Useful when you want gold exposure stripped or combined with FX views. |
| Platinum group metals | `XPTUSD, XPDUSD` | Both are automotive / industrial precious metals and often co-move on the same supply-demand cycle. | Palladium is usually the wilder leg. |

### Stocks

| Cluster | Symbols | Why they usually move together | Notes |
| --- | --- | --- | --- |
| Banks / brokers | `JPM, BAC, WFC, GS, MS` | Shared sensitivity to rates, credit and financial conditions. | JPM vs BAC and GS vs MS are the two cleanest same-industry pairs here. |
| Semiconductors | `AMD, AVGO, MU, QCOM, TSM` | Shared cycle in chips, AI capex and electronics demand. | AMD/QCOM and AVGO/QCOM are practical co-move screens; MU is more memory-cycle specific. |
| Megacap tech | `AAPL, MSFT, META, NFLX` | All are large US growth names driven by the same duration / tech-factor regime. | AAPL vs MSFT is the cleanest mega-cap pair here. |
| EV / autos | `TSLA, RIVN, NIO, XPEV, LI, F, GM` | Shared auto beta with a clear split between legacy OEMs and EV growth names. | NIO/XPEV/LI is the clean China EV cluster; F/GM is the classic legacy pair. |
| Travel | `AALG, DAL, ABNB, BKNG` | All benefit from the same travel-demand cycle but with different business models. | AALG vs DAL is the clean airline pair; ABNB vs BKNG is a cleaner platform pair. |
| Consumer staples / retail | `KO, PEP, WMT, TGT, HD, MCD, SBUX, NKE, COST` | Shared consumer demand factor with clear subsectors inside it. | KO/PEP, WMT/TGT and MCD/SBUX are the cleanest same-lane pairs. |
| Pharma / healthcare | `ABBVIE, JNJ, LLY, MRK` | Large-cap defensive healthcare with common policy and rates sensitivity. | MRK/JNJ and ABBVIE/MRK are usually cleaner than including LLY, which can be more single-theme driven. |
| Defense / industrial | `LMT, NOC, CAT` | Shared US industrial / capex / government spending sensitivity, though defense and machinery are not identical. | LMT/NOC is the true same-subsector pair; CAT is better treated as an industrial beta reference. |

## Full local inventory

Use the TSV file for filtering and sorting in an editor or spreadsheet:

- `docs/notes/bybit_mt5_instrument_inventory.tsv`

## FX (84)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `AUDCAD+` | Australian Dollar vs Canadian Dollar | Australian Dollar vs Canadian Dollar | `forex` | `Forex+\Forex Major\AUDCAD+` |
| `AUDCHF+` | Australian Dollar vs Swiss Franc | Australian Dollar vs Swiss Franc | `forex` | `Forex+\Forex Major\AUDCHF+` |
| `AUDCNH+` | AU Dollar vs Chinese Yuan | AU Dollar vs Chinese Yuan | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\AUDCNH+` |
| `AUDJPY+` | Australian Dollar vs Japanese Yen | Australian Dollar vs Japanese Yen | `indices` | `Forex+\Forex Major\AUDJPY+` |
| `AUDNZD+` | Australian Dollar vs New Zealand Dollar | Australian Dollar vs New Zealand Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\AUDNZD+` |
| `AUDSGD+` | Australian Dollar vs Singapore Dollar | Australian Dollar vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\AUDSGD+` |
| `AUDUSD` | Australian Dollar vs US Dollar | Australian Dollar vs US Dollar | `forex` | `Forex Major\Forex Major\AUDUSD` |
| `AUDUSD+` | Australian Dollar vs US Dollar | Australian Dollar vs US Dollar | `forex` | `Forex+\Forex Major\AUDUSD+` |
| `CADCHF+` | Canadian Dollar vs Swiss Franc | Canadian Dollar vs Swiss Franc | `forex` | `Forex+\Forex Major\CADCHF+` |
| `CADJPY+` | Canadian Dollar vs Japanese Yen | Canadian Dollar vs Japanese Yen | `indices` | `Forex+\Forex Major\CADJPY+` |
| `CHFJPY+` | Swiss Franc vs Japanese Yen | Swiss Franc vs Japanese Yen | `forex` | `Forex+\Forex Major\CHFJPY+` |
| `CHFSGD+` | Swiss Franc vs Singapore Dollar | Swiss Franc vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\CHFSGD+` |
| `EURAUD+` | Euro vs Australian Dollar | Euro vs Australian Dollar | `forex` | `Forex+\Forex Major\EURAUD+` |
| `EURCAD+` | Euro vs Canadian Dollar | Euro vs Canadian Dollar | `forex` | `Forex+\Forex Major\EURCAD+` |
| `EURCHF+` | Euro vs Swiss Franc | Euro vs Swiss Franc | `forex` | `Forex+\Forex Major\EURCHF+` |
| `EURCZK+` | Euro vs Czech Koruna | Euro vs Czech Koruna | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\EURCZK+` |
| `EURDKK+` | Euro vs Danish Krone | Euro vs Danish Krone | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\EURDKK+` |
| `EURGBP+` | Euro vs Great Britain Pound | Euro vs Great Britain Pound | `forex` | `Forex+\Forex Major\EURGBP+` |
| `EURHUF+` | Euro vs Hungarian Forint | Euro vs Hungarian Forint | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\EURHUF+` |
| `EURJPY+` | Euro vs Japanese Yen | Euro vs Japanese Yen | `forex` | `Forex+\Forex Major\EURJPY+` |
| `EURNOK+` | Euro vs Norwegian Krone | Euro vs Norwegian Krone | `forex` | `Forex+\Forex Exotic\EURNOK+` |
| `EURNZD+` | Euro vs New Zealand Dollar | Euro vs New Zealand Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\EURNZD+` |
| `EURPLN+` | Euro vs Polish Zloty | Euro vs Polish Zloty | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\EURPLN+` |
| `EURSEK+` | Euro vs Swedish Krona | Euro vs Swedish Krona | `forex` | `Forex+\Forex Exotic\EURSEK+` |
| `EURSGD+` | Euro vs Singapore Dollar | Euro vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\EURSGD+` |
| `EURTRY+` | Euro vs Turkish Lira | Euro vs Turkish Lira | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\EURTRY+` |
| `EURUSD` | Euro vs US Dollar | Euro vs US Dollar | `forex` | `Forex Major\Forex Major\EURUSD` |
| `EURUSD+` | Euro vs US Dollar | Euro vs US Dollar | `forex` | `Forex+\Forex Major\EURUSD+` |
| `GBPAUD+` | Great Britain Pound vs Australian Dollar | Great Britain Pound vs Australian Dollar | `forex` | `Forex+\Forex Major\GBPAUD+` |
| `GBPCAD+` | Great Britain Pound vs Canadian Dollar | Great Britain Pound vs Canadian Dollar | `forex` | `Forex+\Forex Major\GBPCAD+` |
| `GBPCHF+` | Great Britain Pound vs Swiss Franc | Great Britain Pound vs Swiss Franc | `forex` | `Forex+\Forex Major\GBPCHF+` |
| `GBPJPY+` | Great Britain Pound vs Japanese Yen | Great Britain Pound vs Japanese Yen | `forex` | `Forex+\Forex Major\GBPJPY+` |
| `GBPNZD+` | Great Britain Pound vs New Zealand Dollar | Great Britain Pound vs New Zealand Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\GBPNZD+` |
| `GBPSGD+` | Great Britain Pound vs Singapore Dollar | Great Britain Pound vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\GBPSGD+` |
| `GBPUSD` | Great Britain Pound vs US Dollar | Great Britain Pound vs US Dollar | `forex` | `Forex Major\Forex Major\GBPUSD` |
| `GBPUSD+` | Great Britain Pound vs US Dollar | Great Britain Pound vs US Dollar | `forex` | `Forex+\Forex Major\GBPUSD+` |
| `NZDCAD+` | New Zealand Dollar vs Canadian Dollar | New Zealand Dollar vs Canadian Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\NZDCAD+` |
| `NZDCHF+` | New Zealand Dollar vs Swiss Franc | New Zealand Dollar vs Swiss Franc | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\NZDCHF+` |
| `NZDJPY+` | New Zealand Dollar vs Japanese Yen | New Zealand Dollar vs Japanese Yen | `indices` | `Forex+\Forex Minor\Forex Minor -Forex\NZDJPY+` |
| `NZDSGD+` | New Zealand Dollar vs Singapore Dollar | New Zealand Dollar vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\NZDSGD+` |
| `NZDUSD` | New Zealand Dollar vs US Dollar | New Zealand Dollar vs US Dollar | `forex` | `Forex Major\Forex Minor\NZDUSD` |
| `NZDUSD+` | New Zealand Dollar vs US Dollar | New Zealand Dollar vs US Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\NZDUSD+` |
| `SGDJPY+` | Singapore Dollar vs Japanese Yen | Singapore Dollar vs Japanese Yen | `indices` | `Forex+\Forex Minor\Forex Minor -Forex\SGDJPY+` |
| `USDBRL` | US Dollar vs Brazilian | US Dollar vs Brazilian | `forex` | `Forex Major\Forex Exotic2\USDBRL` |
| `USDBRL+` | US Dollar vs Brazilian | US Dollar vs Brazilian | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDBRL+` |
| `USDCAD` | US Dollar vs Canadian Dollar | US Dollar vs Canadian Dollar | `forex` | `Forex Major\Forex Major\USDCAD` |
| `USDCAD+` | US Dollar vs Canadian Dollar | US Dollar vs Canadian Dollar | `forex` | `Forex+\Forex Major\USDCAD+` |
| `USDCHF` | US Dollar vs Swiss Franc | US Dollar vs Swiss Franc | `forex` | `Forex Major\Forex Major\USDCHF` |
| `USDCHF+` | US Dollar vs Swiss Franc | US Dollar vs Swiss Franc | `forex` | `Forex+\Forex Major\USDCHF+` |
| `USDCLP` | US Dollar vs Chile Peso | US Dollar vs Chile Peso | `forex` | `Forex Major\Forex Major\USDCLP` |
| `USDCLP+` | US Dollar vs Chile Peso | US Dollar vs Chile Peso | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDCLP+` |
| `USDCNH+` | US Dollar vs Chinese Yuan | US Dollar vs Chinese Yuan | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\USDCNH+` |
| `USDCOP` | US Dollar vs Colombia Peso | US Dollar vs Colombia Peso | `forex` | `Forex Major\Forex Major\USDCOP` |
| `USDCOP+` | US Dollar vs Colombia Peso | US Dollar vs Colombia Peso | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDCOP+` |
| `USDCZK` | US Dollar vs Czech Koruna | US Dollar vs Czech Koruna | `forex` | `Forex Major\Forex Exotic2\USDCZK` |
| `USDCZK+` | US Dollar vs Czech Koruna | US Dollar vs Czech Koruna | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDCZK+` |
| `USDDKK+` | US Dollar vs Danish Krone | US Dollar vs Danish Krone | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDDKK+` |
| `USDHKD` | US Dollar vs Hong Kong Dollar | US Dollar vs Hong Kong Dollar | `forex` | `Forex Major\Forex Minor\USDHKD` |
| `USDHUF+` | US Dollar vs Hungarian Forint | US Dollar vs Hungarian Forint | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDHUF+` |
| `USDIDR` | US Dollar vs Indonesian Rupiah | US Dollar vs Indonesian Rupiah | `forex` | `Forex Major\Forex Major\USDIDR` |
| `USDIDR+` | US Dollar vs Indonesian Rupiah | US Dollar vs Indonesian Rupiah | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDIDR+` |
| `USDILS` | US dollar vs Israeli Shekel | US dollar vs Israeli Shekel | `forex` | `Forex Major\Forex Major\USDILS` |
| `USDILS+` | US dollar vs Israeli Shekel | US dollar vs Israeli Shekel | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDILS+` |
| `USDINR` | US Dollar vs Indian Rupee | US Dollar vs Indian Rupee | `forex` | `Forex Major\Forex Exotic2\USDINR` |
| `USDINR+` | US Dollar vs Indian Rupee | US Dollar vs Indian Rupee | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDINR+` |
| `USDJPY` | US Dollar vs Japanese Yen | US Dollar vs Japanese Yen | `indices` | `Forex Major\Forex Major\USDJPY` |
| `USDJPY+` | US Dollar vs Japanese Yen | US Dollar vs Japanese Yen | `indices` | `Forex+\Forex Major\USDJPY+` |
| `USDKRW` | US Dollar vs South Korean won | US Dollar vs South Korean won | `forex` | `Forex Major\Forex Major\USDKRW` |
| `USDKRW+` | US Dollar vs South Korean won | US Dollar vs South Korean won | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDKRW+` |
| `USDMXN+` | US Dollar vs Mexican Peso | US Dollar vs Mexican Peso | `forex` | `Forex+\Forex Exotic\USDMXN+` |
| `USDNOK+` | US Dollar vs Norwegian Krone | US Dollar vs Norwegian Krone | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\USDNOK+` |
| `USDPLN` | US Dollar vs Polish Zloty | US Dollar vs Polish Zloty | `forex` | `Forex Major\Forex Exotic2\USDPLN` |
| `USDPLN+` | US Dollar vs Polish Zloty | US Dollar vs Polish Zloty | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\USDPLN+` |
| `USDSEK+` | US Dollar vs Swedish Krona | US Dollar vs Swedish Krona | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\USDSEK+` |
| `USDSGD` | US Dollar vs Singapore Dollar | US Dollar vs Singapore Dollar | `forex` | `Forex Major\Forex Minor\USDSGD` |
| `USDSGD+` | US Dollar vs Singapore Dollar | US Dollar vs Singapore Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -Forex\USDSGD+` |
| `USDTHB` | United States Dollar vs Thai Baht | United States Dollar vs Thai Baht | `forex` | `Forex Major\Forex Major\USDTHB` |
| `USDTHB+` | United States Dollar vs Thai Baht | United States Dollar vs Thai Baht | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDTHB+` |
| `USDTRY+` | US Dollar vs Turkish Lira | US Dollar vs Turkish Lira | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDTRY+` |
| `USDTWD` | US Dollar vs Taiwan Dollar | US Dollar vs Taiwan Dollar | `forex` | `Forex Major\Forex Major\USDTWD` |
| `USDTWD+` | US Dollar vs Taiwan Dollar | US Dollar vs Taiwan Dollar | `forex` | `Forex+\Forex Minor\Forex Minor -CFD\USDTWD+` |
| `USDZAR` | US Dollar vs South African Rand | US Dollar vs South African Rand | `forex` | `Forex Major\Forex Exotic\USDZAR` |
| `USDZAR+` | US Dollar vs South African Rand | US Dollar vs South African Rand | `forex` | `Forex+\Forex Exotic\USDZAR+` |
| `USTUSD` | - | - | `forex` | `Forex Major\Forex Minor\USTUSD` |

## Indices (21)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `BVSPX` | Bovespa Cash CFD (BRL) | Bovespa Cash CFD (BRL) | `indices` | `CFDs\Indices Minor\BVSPX` |
| `CHINA50` | China A50 Index Cash CFD (USD) | China A50 Index Cash CFD (USD) | `indices` | `CFDs\Indices Minor\CHINA50` |
| `CHINAH` | Hong Kong China H-shares Cash | Hong Kong China H-shares Cash | `indices` | `CFDs\Indices Minor\CHINAH` |
| `DJ30` | Dow Jones Index Cash CFD (USD) | Dow Jones Index Cash CFD (USD) | `indices` | `CFDs\Indices Major\DJ30` |
| `ES35` | ES35 Index Cash | ES35 Index Cash | `indices` | `CFDs\Indices Minor\ES35` |
| `EU50` | EUSTX50 Cash | EUSTX50 Cash | `indices` | `CFDs\Indices Major\EU50` |
| `FRA40` | France 40 Index | France 40 Index | `indices` | `CFDs\Indices Major\FRA40` |
| `GER40` | GER40 Cash | GER40 Cash | `indices` | `CFDs\Indices Major\GER40` |
| `HK50` | Hang Seng Index Cash CFD (HKD) | Hang Seng Index Cash CFD (HKD) | `indices` | `CFDs\Indices Minor\HK50` |
| `HKTECH` | Hang Seng TECH Index CASH CFD (HKD) | Hang Seng TECH Index CASH CFD (HKD) | `indices` | `CFDs\Indices Minor\HKTECH` |
| `NAS100` | NAS100 Cash | NAS100 Cash | `indices` | `CFDs\Indices Major\NAS100` |
| `NETH25` | Netherlands 25 Cash | Netherlands 25 Cash | `crypto` | `CFDs\Indices Minor\NETH25` |
| `Nikkei225` | Nikkei Index Cash CFD (JPY) | Nikkei Index Cash CFD (JPY) | `indices` | `Nikkei\Nikkei225` |
| `SA40` | South Africa 40 - CASH | South Africa 40 - CASH | `indices` | `CFDs\Indices Minor\SA40` |
| `SGP20` | Singapore 20 Index Cash CFD | Singapore 20 Index Cash CFD | `indices` | `CFDs\Indices Minor\SGP20` |
| `SP500` | S&P Index Cash CFD (USD) | S&P Index Cash CFD (USD) | `indices` | `CFDs\Indices Major\SP500` |
| `SPI200` | S&P/ASX 200 Index Cash CFD (AUD) | S&P/ASX 200 Index Cash CFD (AUD) | `indices` | `CFDs\Indices Major\SPI200` |
| `SWI20` | Switzerland 20 Cash | Switzerland 20 Cash | `indices` | `CFDs\Indices Minor\SWI20` |
| `TWINDEX` | Taiwan RIC Index Cash CFD | Taiwan RIC Index Cash CFD | `indices` | `CFDs\Indices Minor\TWINDEX` |
| `UK100` | UK 100 Cash | UK 100 Cash | `indices` | `CFDs\Indices Major\UK100` |
| `US2000` | US SMALL CAP 2000 - CASH | US SMALL CAP 2000 - CASH | `indices` | `CFDs\Indices Minor\US2000` |

## Energy (5)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `GAS-C` | Gasoline | Gasoline | `crypto` | `Commodities\GAS-C` |
| `GASOIL-C` | Low Sulphur Gasoil - Cash | Low Sulphur Gasoil - Cash | `commodities` | `Commodities\GASOIL-C` |
| `NG-C` | Natural Gas | Natural Gas | `commodities` | `Commodities\NG-C` |
| `UKOUSD` | Brent Crude Oil Cash | Brent Crude Oil Cash | `commodities` | `Oil\UKOUSD` |
| `USOUSD` | WTI Crude Oil Cash | WTI Crude Oil Cash | `commodities` | `Oil\USOUSD` |

## Metals (9)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `COPPER-C` | Copper | Copper | `custom` | `Commodities\COPPER-C` |
| `XAGAUD` | Silver vs Australian Dollar | Silver vs Australian Dollar | `commodities` | `Silver\XAGAUD` |
| `XAGUSD` | Silver US Dollar | Silver US Dollar | `commodities` | `Silver\XAGUSD` |
| `XAUAUD+` | Gold vs Australian Dollar | Gold vs Australian Dollar | `commodities` | `Gold+\XAUAUD+` |
| `XAUEUR+` | Gold / Euro | Gold / Euro | `commodities` | `Gold+\XAUEUR+` |
| `XAUJPY+` | Gold vs Japanese Yen | Gold vs Japanese Yen | `commodities` | `Gold+\XAUJPY+` |
| `XAUUSD+` | Gold US Dollar | Gold US Dollar | `commodities` | `Gold+\XAUUSD+` |
| `XPDUSD` | Palladium - Cash | Palladium - Cash | `indices` | `Commodities\XPDUSD` |
| `XPTUSD` | Platinum vs US Dollar | Platinum vs US Dollar | `forex` | `Commodities\XPTUSD` |

## Softs / Agriculture (5)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `Cocoa-C` | US Cocoa - Cash | US Cocoa - Cash | `indices` | `Commodities\Cocoa-C` |
| `Coffee-C` | Coffee Arabica - Cash | Coffee Arabica - Cash | `indices` | `Commodities\Coffee-C` |
| `Cotton-C` | Cotton - Cash | Cotton - Cash | `indices` | `Commodities\Cotton-C` |
| `Sugar-C` | Sugar Raw - Cash | Sugar Raw - Cash | `indices` | `Commodities\Sugar-C` |
| `Wheat-C` | US Wheat (SRW) - Cash | US Wheat (SRW) - Cash | `indices` | `Commodities\Wheat-C` |

## US Stocks (240)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `AALG` | AMERICAN AIRLINES GROUP INC | AMERICAN AIRLINES GROUP INC | `stocks` | `Stocks\US\AALG` |
| `AAPL` | Apple Inc. | Apple Inc. | `stocks` | `Stocks\US\AAPL` |
| `ABBVIE` | ABBVIE INC | ABBVIE INC | `stocks` | `Stocks\US\ABBVIE` |
| `ABNB` | Airbnb Inc | Airbnb Inc | `stocks` | `Stocks\US\ABNB` |
| `ACN` | Accenture PLC - Class A | Accenture PLC - Class A | `stocks` | `Stocks\US\ACN` |
| `ADBE` | Adobe Inc | Adobe Inc | `stocks` | `Stocks\US\ADBE` |
| `AFG` | American Financial Group Inc | American Financial Group Inc | `stocks` | `Stocks\US\AFG` |
| `ALIBABA` | ALIBABA GROUP HOLDING-SP ADR | ALIBABA GROUP HOLDING-SP ADR | `stocks` | `Stocks\US\ALIBABA` |
| `AMAT` | Applied Materials Inc | Applied Materials Inc | `stocks` | `Stocks\US\AMAT` |
| `AMAZON` | AMAZON.COM INC | AMAZON.COM INC | `stocks` | `Stocks\US\AMAZON` |
| `AMD` | Advanced Micro Devices / AMD | Advanced Micro Devices / AMD | `stocks` | `Stocks\US\AMD` |
| `AMGN` | Amgen Inc | Amgen Inc | `stocks` | `Stocks\US\AMGN` |
| `APP` | AppLovin Corp - Class A | AppLovin Corp - Class A | `stocks` | `Stocks\US\APP` |
| `ASML-US` | ASML Holding NV | ASML Holding NV | `stocks` | `Stocks\US\ASML-US` |
| `AT&T` | AT&T INC | AT&T INC | `stocks` | `Stocks\US\AT&T` |
| `AVGO` | Broadcom Inc | Broadcom Inc | `stocks` | `Stocks\US\AVGO` |
| `AXP` | American Express Co | American Express Co | `stocks` | `Stocks\US\AXP` |
| `BAC` | BANK OF AMERICA CORP | BANK OF AMERICA CORP | `stocks` | `Stocks\US\BAC` |
| `BAIDU` | BAIDU INC - SPON ADR | BAIDU INC - SPON ADR | `stocks` | `Stocks\US\BAIDU` |
| `BBD` | Banco Bradesco | Banco Bradesco | `stocks` | `Stocks\US\BBD` |
| `BBWI` | Bath & Body Works Inc | Bath & Body Works Inc | `stocks` | `Stocks\US\BBWI` |
| `BE` | Bloom Energy Corp | Bloom Energy Corp | `stocks` | `Stocks\US\BE` |
| `BEN` | Franklin Resources Inc | Franklin Resources Inc | `stocks` | `Stocks\US\BEN` |
| `BITF` | Bitfarms Ltd | Bitfarms Ltd | `stocks` | `Stocks\US\BITF` |
| `BKNG` | Booking Holdings Inc | Booking Holdings Inc | `stocks` | `Stocks\US\BKNG` |
| `BLK` | Blackrock Inc | Blackrock Inc | `stocks` | `Stocks\US\BLK` |
| `BLSH` | Bullish Inc. | Bullish Inc. | `stocks` | `Stocks\US\BLSH` |
| `BMBL` | Bumble Inc | Bumble Inc | `stocks` | `Stocks\US\BMBL` |
| `BMNR` | BitMine Immersion Technologies Inc | BitMine Immersion Technologies Inc | `stocks` | `Stocks\US\BMNR` |
| `BNTX` | BIONTECH SE-ADR | BIONTECH SE-ADR | `stocks` | `Stocks\US\BNTX` |
| `BOEING` | BOEING CO/THE | BOEING CO/THE | `stocks` | `Stocks\US\BOEING` |
| `BRKB` | Berkshire Hathaway Inc - Class B | Berkshire Hathaway Inc - Class B | `stocks` | `Stocks\US\BRKB` |
| `BUD` | ANHEUSER-BUSCH INBEV SPN ADR | ANHEUSER-BUSCH INBEV SPN ADR | `stocks` | `Stocks\US\BUD` |
| `BYND` | Beyond Meat | Beyond Meat | `stocks` | `Stocks\US\BYND` |
| `CAH` | Cardinal Health Inc | Cardinal Health Inc | `stocks` | `Stocks\US\CAH` |
| `CAT` | Caterpillar Inc | Caterpillar Inc | `stocks` | `Stocks\US\CAT` |
| `CDNS` | Cadence Design Systems Inc | Cadence Design Systems Inc | `stocks` | `Stocks\US\CDNS` |
| `CIM` | Chimera Investment Corp | Chimera Investment Corp | `stocks` | `Stocks\US\CIM` |
| `CISCO` | CISCO SYSTEMS INC | CISCO SYSTEMS INC | `stocks` | `Stocks\US\CISCO` |
| `CITI` | CITIGROUP INC | CITIGROUP INC | `stocks` | `Stocks\US\CITI` |
| `CL` | Colgate-Palmolive Co | Colgate-Palmolive Co | `stocks` | `Stocks\US\CL` |
| `CLS` | Celestica Inc | Celestica Inc | `stocks` | `Stocks\US\CLS` |
| `CLSK` | CleanSpark Inc | CleanSpark Inc | `stocks` | `Stocks\US\CLSK` |
| `CMCSA` | COMCAST CORP-CLASS A | COMCAST CORP-CLASS A | `stocks` | `Stocks\US\CMCSA` |
| `COHR` | Coherent Corp | Coherent Corp | `stocks` | `Stocks\US\COHR` |
| `COIN` | COINBASE GLOBAL INC -CLASS A | COINBASE GLOBAL INC -CLASS A | `stocks` | `Stocks\US\COIN` |
| `COR` | Cencora Inc. | Cencora Inc. | `stocks` | `Stocks\US\COR` |
| `COST` | Costco Wholesale Corp | Costco Wholesale Corp | `stocks` | `Stocks\US\COST` |
| `CRCL` | Circle Internet Group Inc | Circle Internet Group Inc | `stocks` | `Stocks\US\CRCL` |
| `CRDO` | Credo Technology Group Holding Ltd | Credo Technology Group Holding Ltd | `stocks` | `Stocks\US\CRDO` |
| `CRM` | SALESFORCE.COM INC | SALESFORCE.COM INC | `stocks` | `Stocks\US\CRM` |
| `CRWD` | Crowdstrike Holdings Inc | Crowdstrike Holdings Inc | `stocks` | `Stocks\US\CRWD` |
| `CTRA` | Coterra Energy Inc | Coterra Energy Inc | `stocks` | `Stocks\US\CTRA` |
| `CVX` | CHEVRON CORP | CHEVRON CORP | `stocks` | `Stocks\US\CVX` |
| `DAL` | Delta Air Lines Inc | Delta Air Lines Inc | `stocks` | `Stocks\US\DAL` |
| `DAVE` | Dave Inc | Dave Inc | `stocks` | `Stocks\US\DAVE` |
| `DDOG` | Datadog Inc - Class A | Datadog Inc - Class A | `stocks` | `Stocks\US\DDOG` |
| `DELL` | Dell Technologies Inc | Dell Technologies Inc | `stocks` | `Stocks\US\DELL` |
| `DISNEY` | WALT DISNEY CO/THE | WALT DISNEY CO/THE | `stocks` | `Stocks\US\DISNEY` |
| `DLTR` | Dollar Tree Inc | Dollar Tree Inc | `stocks` | `Stocks\US\DLTR` |
| `DOCU` | DocuSign Inc | DocuSign Inc | `stocks` | `Stocks\US\DOCU` |
| `DXC` | DXC Technology Corp | DXC Technology Corp | `stocks` | `Stocks\US\DXC` |
| `EQR` | Equity Residential | Equity Residential | `stocks` | `Stocks\US\EQR` |
| `ESS` | Essex Property Trust Inc | Essex Property Trust Inc | `stocks` | `Stocks\US\ESS` |
| `EXPE` | Expedia Group Inc | Expedia Group Inc | `stocks` | `Stocks\US\EXPE` |
| `EXXON` | EXXON MOBIL CORP | EXXON MOBIL CORP | `stocks` | `Stocks\US\EXXON` |
| `F` | Ford Motor Co | Ford Motor Co | `stocks` | `Stocks\US\F` |
| `FCEL` | FuelCell Energy Inc | FuelCell Energy Inc | `stocks` | `Stocks\US\FCEL` |
| `FDX` | FedEx Corp | FedEx Corp | `stocks` | `Stocks\US\FDX` |
| `FIG` | Figma Inc | Figma Inc | `stocks` | `Stocks\US\FIG` |
| `FITB` | Fifth Third Bancorp | Fifth Third Bancorp | `stocks` | `Stocks\US\FITB` |
| `FOX` | Fox Corp - Class B | Fox Corp - Class B | `stocks` | `Stocks\US\FOX` |
| `FSLR` | First Solar Inc | First Solar Inc | `crypto` | `Stocks\US\FSLR` |
| `FTNT` | Fortinet Inc | Fortinet Inc | `stocks` | `Stocks\US\FTNT` |
| `FUTU` | Futu Holdings Ltd | Futu Holdings Ltd | `stocks` | `Stocks\US\FUTU` |
| `GILD` | Gilead Sciences Inc | Gilead Sciences Inc | `stocks` | `Stocks\US\GILD` |
| `GLXY` | Galaxy Digital Inc. | Galaxy Digital Inc. | `stocks` | `Stocks\US\GLXY` |
| `GM` | General Motors Co | General Motors Co | `stocks` | `Stocks\US\GM` |
| `GNRC` | Generac Holdings Inc | Generac Holdings Inc | `stocks` | `Stocks\US\GNRC` |
| `GOOG` | ALPHABET INC-CL C | ALPHABET INC-CL C | `stocks` | `Stocks\US\GOOG` |
| `GRAB` | Grab Holdings Ltd (ADRs) | Grab Holdings Ltd (ADRs) | `stocks` | `Stocks\US\GRAB` |
| `GS` | Goldman Sachs Group Inc | Goldman Sachs Group Inc | `commodities` | `Stocks\US\GS` |
| `HD` | HOME DEPOT INC | HOME DEPOT INC | `stocks` | `Stocks\US\HD` |
| `HON` | HONEYWELL INTERNATIONAL INC | HONEYWELL INTERNATIONAL INC | `stocks` | `Stocks\US\HON` |
| `HOOD` | ROBINHOOD MARKETS INC - A | ROBINHOOD MARKETS INC - A | `stocks` | `Stocks\US\HOOD` |
| `HPE` | Hewlett Packard Enterprise Co / HPE | Hewlett Packard Enterprise Co / HPE | `stocks` | `Stocks\US\HPE` |
| `HSBCn` | HSBC HOLDINGS PLC-SPONS ADR | HSBC HOLDINGS PLC-SPONS ADR | `stocks` | `Stocks\US\HSBCn` |
| `HTHT` | Huazhu Group Ltd | Huazhu Group Ltd | `stocks` | `Stocks\US\HTHT` |
| `HUT` | Hut 8 Mining Corp | Hut 8 Mining Corp | `stocks` | `Stocks\US\HUT` |
| `IBM` | INTL BUSINESS MACHINES CORP | INTL BUSINESS MACHINES CORP | `stocks` | `Stocks\US\IBM` |
| `IBN` | ICICI Bank Ltd (ADRs) | ICICI Bank Ltd (ADRs) | `stocks` | `Stocks\US\IBN` |
| `ICL` | ICL Group Ltd (ADRs) | ICL Group Ltd (ADRs) | `stocks` | `Stocks\US\ICL` |
| `INSM` | Insmed Incorporated | Insmed Incorporated | `stocks` | `Stocks\US\INSM` |
| `INTEL` | INTEL CORP | INTEL CORP | `stocks` | `Stocks\US\INTEL` |
| `INTU` | Intuit Inc | Intuit Inc | `stocks` | `Stocks\US\INTU` |
| `IP` | International Paper Co | International Paper Co | `stocks` | `Stocks\US\IP` |
| `IR` | Ingersoll Rand Inc | Ingersoll Rand Inc | `crypto` | `Stocks\US\IR` |
| `ISRG` | Intuitive Surgical Inc | Intuitive Surgical Inc | `stocks` | `Stocks\US\ISRG` |
| `ITW` | Illinois Tool Works / ITW | Illinois Tool Works / ITW | `stocks` | `Stocks\US\ITW` |
| `JBL` | Jabil Inc | Jabil Inc | `stocks` | `Stocks\US\JBL` |
| `JD` | JD.COM INC-ADR | JD.COM INC-ADR | `stocks` | `Stocks\US\JD` |
| `JNJ` | JOHNSON & JOHNSON | JOHNSON & JOHNSON | `stocks` | `Stocks\US\JNJ` |
| `JPM` | JPMORGAN CHASE & CO | JPMORGAN CHASE & CO | `stocks` | `Stocks\US\JPM` |
| `KO` | COCA-COLA CO/THE | COCA-COLA CO/THE | `stocks` | `Stocks\US\KO` |
| `LAC` | Lithium Americas Corp | Lithium Americas Corp | `stocks` | `Stocks\US\LAC` |
| `LAUR` | Laureate Education Inc | Laureate Education Inc | `stocks` | `Stocks\US\LAUR` |
| `LBTYK` | Liberty Global Ltd. Class C | Liberty Global Ltd. Class C | `stocks` | `Stocks\US\LBTYK` |
| `LI` | LI AUTO INC - ADR | LI AUTO INC - ADR | `stocks` | `Stocks\US\LI` |
| `LITE` | Lumentum Holdings Inc | Lumentum Holdings Inc | `stocks` | `Stocks\US\LITE` |
| `LLY` | Eli Lilly & Co | Eli Lilly & Co | `stocks` | `Stocks\US\LLY` |
| `LMT` | Lockheed Martin Corp | Lockheed Martin Corp | `stocks` | `Stocks\US\LMT` |
| `LPL` | LG Display Co Ltd (ADRs) | LG Display Co Ltd (ADRs) | `stocks` | `Stocks\US\LPL` |
| `LRCX` | Lam Research Corp | Lam Research Corp | `stocks` | `Stocks\US\LRCX` |
| `LULU` | LULULEMON ATHLETICA INC | LULULEMON ATHLETICA INC | `stocks` | `Stocks\US\LULU` |
| `LYB` | LyondellBasell Industries - Class A | LyondellBasell Industries - Class A | `stocks` | `Stocks\US\LYB` |
| `LYFT` | Lyft Inc | Lyft Inc | `stocks` | `Stocks\US\LYFT` |
| `MA` | MASTERCARD INC - A | MASTERCARD INC - A | `stocks` | `Stocks\US\MA` |
| `MARA` | Marathon Digital Holdings, Inc | Marathon Digital Holdings, Inc | `stocks` | `Stocks\US\MARA` |
| `MAT` | Mattel Inc | Mattel Inc | `stocks` | `Stocks\US\MAT` |
| `MCD` | MCDONALD'S CORP | MCDONALD'S CORP | `stocks` | `Stocks\US\MCD` |
| `MCK` | Mckesson Corp | Mckesson Corp | `stocks` | `Stocks\US\MCK` |
| `MELI` | Mercado Libre | Mercado Libre | `stocks` | `Stocks\US\MELI` |
| `MET` | Metlife Inc | Metlife Inc | `stocks` | `Stocks\US\MET` |
| `META` | Meta Platforms Inc | Meta Platforms Inc | `stocks` | `Stocks\US\META` |
| `MFA` | MFA Financial Inc | MFA Financial Inc | `stocks` | `Stocks\US\MFA` |
| `MMM` | 3M CO | 3M CO | `stocks` | `Stocks\US\MMM` |
| `MOH` | Molina Healthcare Inc | Molina Healthcare Inc | `stocks` | `Stocks\US\MOH` |
| `MPLX` | MPLX LP | MPLX LP | `stocks` | `Stocks\US\MPLX` |
| `MRK` | Merck & Co Inc | Merck & Co Inc | `stocks` | `Stocks\US\MRK` |
| `MRNA` | MODERNA INC | MODERNA INC | `stocks` | `Stocks\US\MRNA` |
| `MRVL` | Marvell Technology Group Ltd | Marvell Technology Group Ltd | `stocks` | `Stocks\US\MRVL` |
| `MS` | Morgan Stanley | Morgan Stanley | `stocks` | `Stocks\US\MS` |
| `MSFT` | MICROSOFT CORP | MICROSOFT CORP | `stocks` | `Stocks\US\MSFT` |
| `MSTR` | Strategy Inc. | Strategy Inc. | `stocks` | `Stocks\US\MSTR` |
| `MU` | Micron Technology Inc | Micron Technology Inc | `stocks` | `Stocks\US\MU` |
| `NEM` | Newmont Mining | Newmont Mining | `stocks` | `Stocks\US\NEM` |
| `NFLX` | NETFLIX INC | NETFLIX INC | `stocks` | `Stocks\US\NFLX` |
| `NIO` | NIO INC - ADR | NIO INC - ADR | `stocks` | `Stocks\US\NIO` |
| `NKE` | Nike Inc | Nike Inc | `stocks` | `Stocks\US\NKE` |
| `NOC` | Northrop Grumman Corp | Northrop Grumman Corp | `stocks` | `Stocks\US\NOC` |
| `NOW` | ServiceNow Inc | ServiceNow Inc | `stocks` | `Stocks\US\NOW` |
| `NTAP` | Netapp Inc | Netapp Inc | `stocks` | `Stocks\US\NTAP` |
| `NTES` | NETEASE INC-ADR | NETEASE INC-ADR | `stocks` | `Stocks\US\NTES` |
| `NVIDIA` | NVIDIA CORP | NVIDIA CORP | `stocks` | `Stocks\US\NVIDIA` |
| `NVS` | NOVARTIS AG-SPONSORED ADR | NOVARTIS AG-SPONSORED ADR | `stocks` | `Stocks\US\NVS` |
| `NWS` | News Corp - Class B | News Corp - Class B | `stocks` | `Stocks\US\NWS` |
| `OKLO` | Oklo Inc | Oklo Inc | `stocks` | `Stocks\US\OKLO` |
| `OKTA` | Okta Inc | Okta Inc | `stocks` | `Stocks\US\OKTA` |
| `ORCL` | ORACLE CORP | ORACLE CORP | `stocks` | `Stocks\US\ORCL` |
| `OXY` | Occidental Petroleum Corp | Occidental Petroleum Corp | `stocks` | `Stocks\US\OXY` |
| `PDD` | Pinduoduo Inc (ADRs) | Pinduoduo Inc (ADRs) | `stocks` | `Stocks\US\PDD` |
| `PEG` | Public Service Enterprise Group / PSEG | Public Service Enterprise Group / PSEG | `stocks` | `Stocks\US\PEG` |
| `PENN` | Penn National Gaming Inc | Penn National Gaming Inc | `stocks` | `Stocks\US\PENN` |
| `PEP` | PEPSICO INC | PEPSICO INC | `stocks` | `Stocks\US\PEP` |
| `PFIZER` | PFIZER INC | PFIZER INC | `stocks` | `Stocks\US\PFIZER` |
| `PG` | PROCTER & GAMBLE CO/THE | PROCTER & GAMBLE CO/THE | `stocks` | `Stocks\US\PG` |
| `PGR` | Progressive Corp | Progressive Corp | `stocks` | `Stocks\US\PGR` |
| `PLAY` | Dave & Buster's Entertainmen | Dave & Buster's Entertainmen | `stocks` | `Stocks\US\PLAY` |
| `PLTR` | Palantir Technologies Inc | Palantir Technologies Inc | `stocks` | `Stocks\US\PLTR` |
| `PM` | PHILIP MORRIS INTERNATIONAL | PHILIP MORRIS INTERNATIONAL | `stocks` | `Stocks\US\PM` |
| `POOL` | Pool Corp | Pool Corp | `stocks` | `Stocks\US\POOL` |
| `PPL` | PPL Corp | PPL Corp | `stocks` | `Stocks\US\PPL` |
| `PSIX` | Power Solutions International Inc | Power Solutions International Inc | `crypto` | `Stocks\US\PSIX` |
| `PVH` | PVH Corp | PVH Corp | `stocks` | `Stocks\US\PVH` |
| `PYPL` | PayPal Holdings Inc | PayPal Holdings Inc | `stocks` | `Stocks\US\PYPL` |
| `QCOM` | QUALCOMM Inc | QUALCOMM Inc | `stocks` | `Stocks\US\QCOM` |
| `QQQ` | Invesco QQQ Trust Series 1 ETF | Invesco QQQ Trust Series 1 ETF | `stocks` | `Stocks\US\QQQ` |
| `RACE` | Ferrari NV | Ferrari NV | `stocks` | `Stocks\US\RACE` |
| `RBLX` | Roblox Corp - Class A | Roblox Corp - Class A | `stocks` | `Stocks\US\RBLX` |
| `REGN` | Regeneron Pharmaceuticals | Regeneron Pharmaceuticals | `stocks` | `Stocks\US\REGN` |
| `RF` | Regions Financial Corp | Regions Financial Corp | `stocks` | `Stocks\US\RF` |
| `RIOT` | Riot Platforms, Inc | Riot Platforms, Inc | `stocks` | `Stocks\US\RIOT` |
| `RIVN` | Rivian Automotive Inc | Rivian Automotive Inc | `stocks` | `Stocks\US\RIVN` |
| `RKLB` | Rocket Lab Corp | Rocket Lab Corp | `stocks` | `Stocks\US\RKLB` |
| `ROKU` | Roku Inc | Roku Inc | `stocks` | `Stocks\US\ROKU` |
| `ROST` | Ross Stores Inc | Ross Stores Inc | `stocks` | `Stocks\US\ROST` |
| `RSG` | Republic Services Inc | Republic Services Inc | `stocks` | `Stocks\US\RSG` |
| `SBET` | SharpLink Gaming Inc | SharpLink Gaming Inc | `stocks` | `Stocks\US\SBET` |
| `SBUX` | STARBUCKS CORP | STARBUCKS CORP | `stocks` | `Stocks\US\SBUX` |
| `SE` | SEA LTD-ADR | SEA LTD-ADR | `stocks` | `Stocks\US\SE` |
| `SHOP` | SHOPIFY INC - CLASS A | SHOPIFY INC - CLASS A | `stocks` | `Stocks\US\SHOP` |
| `SIG` | Signet Jewelers Ltd | Signet Jewelers Ltd | `stocks` | `Stocks\US\SIG` |
| `SMG` | ScottsMiracle-Gro Co | ScottsMiracle-Gro Co | `stocks` | `Stocks\US\SMG` |
| `SNAP` | Snap Inc | Snap Inc | `stocks` | `Stocks\US\SNAP` |
| `SNOW` | SNOWFLAKE INC-CLASS A | SNOWFLAKE INC-CLASS A | `stocks` | `Stocks\US\SNOW` |
| `SNPS` | Synopsys Inc | Synopsys Inc | `stocks` | `Stocks\US\SNPS` |
| `SOFI` | SoFi Technologies Inc | SoFi Technologies Inc | `stocks` | `Stocks\US\SOFI` |
| `SPCE` | VIRGIN GALACTIC HOLDINGS INC | VIRGIN GALACTIC HOLDINGS INC | `stocks` | `Stocks\US\SPCE` |
| `SPG` | Simon Property Group Inc | Simon Property Group Inc | `stocks` | `Stocks\US\SPG` |
| `SPGI` | S&P Global Inc | S&P Global Inc | `stocks` | `Stocks\US\SPGI` |
| `SPOT` | Spotify Technology SA | Spotify Technology SA | `stocks` | `Stocks\US\SPOT` |
| `SRE` | Sempra Energy | Sempra Energy | `stocks` | `Stocks\US\SRE` |
| `STRL` | Sterling Infrastructure Inc | Sterling Infrastructure Inc | `stocks` | `Stocks\US\STRL` |
| `STX` | Seagate | Seagate | `stocks` | `Stocks\US\STX` |
| `STZ` | Constellation Brands Inc - Class A | Constellation Brands Inc - Class A | `stocks` | `Stocks\US\STZ` |
| `SWK` | Stanley Black & Decker Inc | Stanley Black & Decker Inc | `stocks` | `Stocks\US\SWK` |
| `TCOM` | Trip.com Group Ltd | Trip.com Group Ltd | `stocks` | `Stocks\US\TCOM` |
| `TEAM` | Atlassian Corp PLC - Class A | Atlassian Corp PLC - Class A | `stocks` | `Stocks\US\TEAM` |
| `TFC` | Truist Financial Corp | Truist Financial Corp | `stocks` | `Stocks\US\TFC` |
| `TGT` | Target Corp | Target Corp | `stocks` | `Stocks\US\TGT` |
| `TLN` | Talen Energy Corp | Talen Energy Corp | `stocks` | `Stocks\US\TLN` |
| `TME` | Tencent Music Entertainment Group – ADR | Tencent Music Entertainment Group – ADR | `stocks` | `Stocks\US\TME` |
| `TOYOTA` | TOYOTA MOTOR CORP -SPON ADR | TOYOTA MOTOR CORP -SPON ADR | `stocks` | `Stocks\US\TOYOTA` |
| `TQQQ` | ProShares UltraPro QQQ ETF | ProShares UltraPro QQQ ETF | `stocks` | `Stocks\US\TQQQ` |
| `TRMB` | TRIMBLE INC | TRIMBLE INC | `stocks` | `Stocks\US\TRMB` |
| `TROW` | T. Rowe Price Group Inc | T. Rowe Price Group Inc | `stocks` | `Stocks\US\TROW` |
| `TRV` | The Travelers Companies Inc | The Travelers Companies Inc | `stocks` | `Stocks\US\TRV` |
| `TSLA` | Tesla | Tesla | `stocks` | `Stocks\US\TSLA` |
| `TSM` | TAIWAN SEMICONDUCTOR-SP ADR | TAIWAN SEMICONDUCTOR-SP ADR | `stocks` | `Stocks\US\TSM` |
| `TTD` | The Trade Desk Inc - Class A | The Trade Desk Inc - Class A | `stocks` | `Stocks\US\TTD` |
| `TTWO` | Take-Two Interactive SoftwareInc | Take-Two Interactive SoftwareInc | `stocks` | `Stocks\US\TTWO` |
| `TWLO` | Twilio Inc - Class A | Twilio Inc - Class A | `stocks` | `Stocks\US\TWLO` |
| `TXN` | Texas Instruments Inc | Texas Instruments Inc | `stocks` | `Stocks\US\TXN` |
| `U` | UNITY SOFTWARE INC | UNITY SOFTWARE INC | `stocks` | `Stocks\US\U` |
| `UBER` | Uber Technologies Inc | Uber Technologies Inc | `stocks` | `Stocks\US\UBER` |
| `UL` | Unilever Plc | Unilever Plc | `stocks` | `Stocks\US\UL` |
| `ULTA` | Ulta Beauty Inc | Ulta Beauty Inc | `stocks` | `Stocks\US\ULTA` |
| `UMC` | United Microelectronics Corp (ADRs) | United Microelectronics Corp (ADRs) | `stocks` | `Stocks\US\UMC` |
| `UNH` | UNITEDHEALTH GROUP INC | UNITEDHEALTH GROUP INC | `stocks` | `Stocks\US\UNH` |
| `UPS` | United Parcel Service Inc | United Parcel Service Inc | `stocks` | `Stocks\US\UPS` |
| `UPST` | Upstart Holdings Inc | Upstart Holdings Inc | `stocks` | `Stocks\US\UPST` |
| `URNM` | Sprott Uranium Miners ETF | Sprott Uranium Miners ETF | `stocks` | `Stocks\US\URNM` |
| `USB` | U.S. Bancorp | U.S. Bancorp | `stocks` | `Stocks\US\USB` |
| `VISA` | VISA INC-CLASS A SHARES | VISA INC-CLASS A SHARES | `stocks` | `Stocks\US\VISA` |
| `VRT` | Vertiv Holdings Co | Vertiv Holdings Co | `stocks` | `Stocks\US\VRT` |
| `VZ` | VERIZON COMMUNICATIONS INC | VERIZON COMMUNICATIONS INC | `stocks` | `Stocks\US\VZ` |
| `WDAY` | Workday Inc - Class A | Workday Inc - Class A | `stocks` | `Stocks\US\WDAY` |
| `WDC` | Western Digital Corp | Western Digital Corp | `stocks` | `Stocks\US\WDC` |
| `WEN` | Wendy's Co | Wendy's Co | `stocks` | `Stocks\US\WEN` |
| `WFC` | WELLS FARGO & CO | WELLS FARGO & CO | `stocks` | `Stocks\US\WFC` |
| `WM` | Waste Management Inc | Waste Management Inc | `stocks` | `Stocks\US\WM` |
| `WMT` | WALMART INC | WALMART INC | `stocks` | `Stocks\US\WMT` |
| `WPM` | Wheaton Precious Metals Corp | Wheaton Precious Metals Corp | `commodities` | `Stocks\US\WPM` |
| `WU` | Western Union Co | Western Union Co | `stocks` | `Stocks\US\WU` |
| `WY` | Weyerhaeuser Co | Weyerhaeuser Co | `stocks` | `Stocks\US\WY` |
| `XPEV` | XPENG INC - ADR | XPENG INC - ADR | `stocks` | `Stocks\US\XPEV` |
| `YUM` | Yum! Brands Inc | Yum! Brands Inc | `stocks` | `Stocks\US\YUM` |
| `YUMC` | Yum China Holdings Inc | Yum China Holdings Inc | `stocks` | `Stocks\US\YUMC` |
| `ZIM` | Zim Integrated Shipping Services Ltd (ADRs) | Zim Integrated Shipping Services Ltd (ADRs) | `stocks` | `Stocks\US\ZIM` |
| `ZM` | ZOOM VIDEO COMMUNICATIONS-A | ZOOM VIDEO COMMUNICATIONS-A | `stocks` | `Stocks\US\ZM` |

## Other Commodities (2)

| Symbol | Meaning / what it is | Catalog description | Catalog group | Raw path |
| --- | --- | --- | --- | --- |
| `OJ-C` | Orange Juice - Cash | Orange Juice - Cash | `indices` | `Commodities\OJ-C` |
| `Soybean-C` | Soybean - Cash | Soybean - Cash | `indices` | `Commodities\Soybean-C` |
