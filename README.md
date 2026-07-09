# EU Analytics Bot

A Telegram bot that turns plain-language questions — in **English or Italian** — into
charts and short analytic commentary, backed by official **European Central Bank**
and **Eurostat** data.

> _"Inflation Italy vs Germany since 2020"_ · _"Disoccupazione Francia Spagna"_ ·
> _"Popolazione Puglia"_ · _"/search tourism nights"_

Author: **Giulio Albano** — University of Bari (UNIBA), PhD in Economics and Finance of
Public Administrations.

---

## What it does

- **Natural language, two languages.** Type a question; the bot detects the indicator,
  the countries, and the time window. Synonyms and country names are recognised in both
  English and Italian.
- **Multi-country comparison.** `Unemployment Italy vs France` draws one line per country
  on a single chart. Any number of countries, any comparable indicator.
- **Regional (NUTS-2).** `Popolazione Puglia`, `Età media Lombardia vs Lazio`, or a raw
  NUTS code like `ITF4`. When an indicator has a regional dataset variant, the bot uses it.
- **Search any Eurostat dataset.** `/search <keywords>` browses the full Eurostat
  catalogue (~9 000 datasets), not just the curated list, and charts a result on tap.
- **AI commentary (optional).** With an OpenAI key, each chart gets a headline, three
  insights, and a bottom line. Without a key, a deterministic numeric summary is used.
- **Menus by source.** Inline menus split cleanly into 🏦 ECB, 🇪🇺 Eurostat, and 💱 FX.
- **Graceful on nonsense.** Unrecognised queries get a helpful "try this instead" hint
  rather than a wrong chart.
- **Every query is logged** to `data/interactions.csv` (opens in Excel).

## Data & indicators

| Group | Examples | Source |
|---|---|---|
| Prices | Inflation (HICP), house prices, labour cost | ECB / Eurostat |
| Growth & output | Real GDP, GDP per capita, GDP growth, industrial production, hours worked | ECB / Eurostat |
| Labour & society | Unemployment, employment, poverty, population, median age | Eurostat |
| Public finance | Government debt, deficit/surplus | Eurostat |
| Money & rates | Deposit rate, main refinancing rate, cost of borrowing, M3, loans, 10Y yield curve (AAA), 10Y bond yield per country | ECB / Eurostat |
| Markets | EUR/USD, GBP, JPY, CHF, PLN, TRY | ECB |
| Anything else | Any Eurostat dataset via `/search` | Eurostat |

Both providers expose data via the **SDMX 2.1 REST** standard. ECB series are `FLOW.KEY`
(e.g. `ICP.M.U2.N.000000.4.ANR`); Eurostat datasets are queried by id with dimension
filters and decoded from JSON-stat.

## Architecture

```
main.py                        entrypoint (UTF-8 stdout, starts polling)
modules/
  telegram_bot.py              commands, menus, multi-geo orchestration, /search
  llm_router.py                message → structured query plan
  ai_parser.py                 indicator catalog, EN/IT synonyms, country/region/period
                               detection, ECB country-aware series, unknown-query handling
  fetchers/
    ebc_adapter.py             ECB fetch (ecbdata + REST CSV fallback), period-keyed cache
    eurostat_adapter.py        Eurostat fetch + correct JSON-stat row-major decode
  eurostat_search.py           catalogue search + fetch-any-dataset
  plotter.py                   Matplotlib time-series (single & multi-line)
  data_commenter.py            AI or numeric summary, provider-aware citation
  interaction_log.py           append-only CSV query log
config/
  settings.py                  environment variables
```

**Request lifecycle:** message → router → parser (indicator + countries/regions + period)
→ per-geo fetch and normalise → chart → commentary → reply. A comparison simply fetches
one series per geography and plots them together.

## Setup

Requires Python 3.10+.

```bash
python -m venv .venv
# Windows:  .venv\Scripts\activate
# macOS/Linux:  source .venv/bin/activate
pip install -r requirements.txt
```

Configuration (via `.env`, see [`.env.example`](.env.example)):

| Variable | Required | Purpose |
|---|---|---|
| `TELEGRAM_TOKEN` | ✅ | BotFather token |
| `OPENAI_API_KEY` | optional | Enables AI commentary and fuzzy indicator routing |

```bash
cp .env.example .env   # then edit with your own values
```

> Only one bot instance may poll updates at a time — stop any previous session first.
> The real `.env` is git-ignored; never commit tokens or keys.

## Run

```bash
python main.py
```

## Usage

Type a question, tap a menu button (🚀 Start), or search a dataset:

```
Inflation Euro area since 2020
Inflation Italy vs Germany since 2020        # comparison → two lines
Disoccupazione Francia Spagna                 # Italian, comparison
GDP per capita Euro area
Rendimento decennale Italia                   # 10Y bond yield, per country
Popolazione Puglia                            # regional (NUTS-2)
Età media Lombardia vs Lazio                  # regional comparison
EUR/USD exchange rate
/search life expectancy                       # any Eurostat dataset
```

## Robustness

- **Eurostat JSON-stat** is decoded row-major from `id`/`size`/sparse `value`, so
  multi-dimension datasets align correctly.
- **Fully-pinned dimensions** per indicator avoid mixing multiple series.
- **ECB caching** is keyed by series *and* period with a daily TTL.
- **Annual ECB dates** are cast to strings before parsing (avoids the int-year → 1970 trap).
- **Stale datasets** keep their data instead of being emptied by the default time window.
- **Backoff retries** absorb Eurostat throttling on multi-country requests.

## Limits & notes

- ECB per-country series exist only where the flow supports it (e.g. inflation ICP);
  ECB GDP (MNA) stays euro-area — use Eurostat GDP growth for country breakdowns.
- `/search` picks a single representative series per dataset (longest available); a
  dimension-filter UI would be the next step.
- Data availability and revisions follow the ECB and Eurostat portals.
- AI commentary via OpenAI incurs cost; validate and monitor your key.

## References & attribution

- ECB Data Portal (SDMX): https://data.ecb.europa.eu/ — CC BY 4.0
- Eurostat: https://ec.europa.eu/eurostat — CC BY 4.0
- SDMX standard: https://sdmx.org/ · Aiogram: https://docs.aiogram.dev/
