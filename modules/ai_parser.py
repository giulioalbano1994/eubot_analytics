"""
=============================================================
Module: ai_parser_unified.py  (v10 – Stable ECB + Eurostat Engine)
=============================================================
🧩 Features
-------------------------------------------------------------
✅ Fix: Exchange rates now correctly detected (EUR/USD, euro dollar, etc.)
✅ ECB + Eurostat unified interpreter
✅ Robust fallback with LLM optional
✅ Fetchers with JSON→CSV fallback for ECB
✅ Works seamlessly with telegram_bot + ebc_adapter
=============================================================
"""

import os, re, io, logging, itertools, requests, pandas as pd
from datetime import datetime, timedelta
from openai import OpenAI
import matplotlib.pyplot as plt

plt.style.use("seaborn-v0_8-whitegrid")
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# 🔐 Setup
# -------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None
USE_LLM = bool(client)

ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

# -------------------------------------------------------------
# 🌍 Country codes
# -------------------------------------------------------------
COUNTRY_CODES = {
    # English
    "euro area": "EA", "eurozone": "EA", "european union": "EU27_2020",
    "italy": "IT", "france": "FR", "germany": "DE", "spain": "ES",
    "portugal": "PT", "belgium": "BE", "netherlands": "NL", "austria": "AT",
    "greece": "GR", "ireland": "IE", "finland": "FI", "luxembourg": "LU",
    "denmark": "DK", "sweden": "SE", "poland": "PL", "romania": "RO",
    "czech": "CZ", "czechia": "CZ", "hungary": "HU", "slovakia": "SK",
    "slovenia": "SI", "croatia": "HR", "bulgaria": "BG",
    # Italian (user's language)
    "area euro": "EA", "zona euro": "EA", "unione europea": "EU27_2020",
    "italia": "IT", "francia": "FR", "germania": "DE", "spagna": "ES",
    "portogallo": "PT", "belgio": "BE", "paesi bassi": "NL", "olanda": "NL",
    "grecia": "GR", "irlanda": "IE", "finlandia": "FI", "lussemburgo": "LU",
    "danimarca": "DK", "svezia": "SE", "polonia": "PL", "romania": "RO",
    "ungheria": "HU", "slovacchia": "SK", "slovenia": "SI", "croazia": "HR",
    "bulgaria": "BG",
}

def detect_countries(text: str) -> str:
    text = text.lower()
    for name, code in COUNTRY_CODES.items():
        if name in text:
            return code
    return "EA"

def _ecb_geo(country: str) -> str:
    """ECB REF_AREA: euro area is 'U2'; countries use their ISO-2 code as-is."""
    return "U2" if country == "EA" else country

def detect_all_countries(text: str) -> list[str]:
    """All countries named in the query, e.g. 'francia vs italia' -> ['FR','IT'].
    Order-deduplicated; empty if none named."""
    text = text.lower()
    codes = [code for name, code in COUNTRY_CODES.items() if name in text]
    return list(dict.fromkeys(codes))  # dedup, keep first occurrence

# Italian NUTS-2 regions (+ direct code passthrough). Enables regional queries
# like "popolazione Puglia" or "median age Lombardia vs Lazio".
REGION_CODES = {
    "piemonte": "ITC1", "valle d'aosta": "ITC2", "liguria": "ITC3", "lombardia": "ITC4",
    "bolzano": "ITH1", "trento": "ITH2", "veneto": "ITH3", "friuli": "ITH4",
    "emilia-romagna": "ITH5", "emilia romagna": "ITH5",
    "toscana": "ITI1", "umbria": "ITI2", "marche": "ITI3", "lazio": "ITI4",
    "abruzzo": "ITF1", "molise": "ITF2", "campania": "ITF3", "puglia": "ITF4",
    "basilicata": "ITF5", "calabria": "ITF6", "sicilia": "ITG1", "sardegna": "ITG2",
}

def detect_all_regions(text: str) -> list[str]:
    """NUTS-2 region codes named in the query (Italian regions or raw codes).
    Word-boundary match: 'popolazione' must not trigger 'lazio'."""
    t = text.lower()
    codes = [code for name, code in REGION_CODES.items()
             if re.search(rf"\b{re.escape(name)}\b", t)]
    # also accept an explicit NUTS-2 code typed directly, e.g. 'ITF4', 'DEA2'
    codes += re.findall(r"\b([A-Z]{2}[0-9A-Z]{2})\b", text)
    return list(dict.fromkeys(codes))

# -------------------------------------------------------------
# ⏱️ Period detection
# -------------------------------------------------------------
def detect_period(text: str) -> dict:
    today = datetime.today()
    text = text.lower()
    if m := re.search(r"since\s*(\d{4})", text):
        return {"startPeriod": f"{m.group(1)}-01"}
    if m := re.search(r"last\s*(\d+)\s*year", text):
        start = today - timedelta(days=int(m.group(1)) * 365)
        return {"startPeriod": start.strftime("%Y-%m")}
    if m := re.search(r"last\s*(\d+)\s*month", text):
        start = today - timedelta(days=int(m.group(1)) * 30)
        return {"startPeriod": start.strftime("%Y-%m")}
    return {"startPeriod": (today - timedelta(days=5 * 365)).strftime("%Y-%m")}

# -------------------------------------------------------------
# 🧾 Indicator catalog
# -------------------------------------------------------------
INDICATOR_CATALOG = {
    # ==== ECONOMIC (Eurostat + ECB) ====
    "gdp_real": {
        "provider": "ECB",
        "flow": "MNA",
        "series": "Q.N.I8.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.N",
        "freq": "Q",
        "label": "Real GDP (chain-linked)"
    },
    "gdp_per_capita": {
        "provider": "ECB",
        "flow": "MNA",
        "series": "A.N.I9.W2.S1.S1.B.B1GQ._Z._Z._Z.PE_R_POP.V._Z",
        "freq": "A",
        "label": "GDP per capita (Purchasing Power Standards, Euro area)"
    },
    "inflation": {
        "provider": "ECB",
        "flow": "ICP",
        "series": "M.U2.N.000000.4.ANR",
        "geo_template": "M.{geo}.N.000000.4.ANR",  # {geo}=U2 (EA) or ISO-2 country
        "freq": "M",
        "label": "Inflation (HICP YoY)"
    },
    "unemployment": {
        "provider": "Eurostat",
        "dataset": "une_rt_m",
        "params": {"sex": "T", "age": "Y25-74", "unit": "PC_ACT", "s_adj": "SA"},
        "label": "Unemployment rate"
    },
    "employment": {
        "provider": "Eurostat",
        "dataset": "lfsi_emp_a",
        "params": {"indic_em": "EMP_LFS", "sex": "T", "age": "Y20-64", "unit": "PC_POP"},
        "label": "Employment rate"
    },
    "poverty_rate": {
    "provider": "Eurostat",
    "dataset": "ilc_peps01",
    "params": {"unit":"PC","sex":"T","age":"TOTAL"},
    "label": "Population at risk of poverty or social exclusion (% of total)"
    },
    "debt_gdp": {
    "provider": "Eurostat",
    "dataset": "gov_10q_ggdebt",
    "params": {"sector":"S13","na_item":"GD","unit":"PC_GDP"},
    "label": "Government debt (% GDP)"
    },
    "gov_deficit": {
        "provider": "Eurostat",
        "dataset": "gov_10dd_edpt1",
        "params": {"sector": "S13", "na_item": "B9", "unit": "PC_GDP"},
        "label": "Government deficit/surplus (% GDP)"
    },
    "gdp_growth": {
        "provider": "Eurostat",
        "dataset": "namq_10_gdp",
        "params": {"unit": "CLV_PCH_PRE", "s_adj": "SCA", "na_item": "B1GQ"},
        "label": "GDP growth (QoQ, %)"
    },
    "house_prices": {
        "provider": "Eurostat",
        "dataset": "prc_hpi_q",
        "params": {"purchase": "TOTAL", "unit": "RCH_A"},
        "label": "House price index (YoY, %)"
    },
    "labour_cost": {
        "provider": "Eurostat",
        "dataset": "lc_lci_r2_a",
        "params": {"lcstruct": "D1_D4_MD5", "nace_r2": "B-S", "unit": "I20"},
        "label": "Labour cost index (2020=100)"
    },
    "lt_yield": {
        "provider": "Eurostat",
        "dataset": "irt_lt_mcby_m",
        "params": {"int_rt": "MCBY"},
        "label": "Long-term government bond yield (Maastricht, %)"
    },
    "industrial_production": {
        "provider": "Eurostat",
        "dataset": "sts_inpr_m",
        "params": {"indic_bt": "PRD", "nace_r2": "B-D", "s_adj": "SCA", "unit": "I15"},
        "label": "Industrial production index"
    },
    "population": {
        "provider": "Eurostat",
        "dataset": "demo_pjan",
        "params": {"sex": "T", "age": "TOTAL", "unit": "NR"},
        "regional": {"dataset": "demo_r_d2jan", "params": {"sex": "T", "age": "TOTAL"}},
        "label": "Population (1 January)"
    },
    "median_age": {
        "provider": "Eurostat",
        "dataset": "demo_pjanind",
        "params": {"indic_de": "MEDAGEPOP"},
        "regional": {"dataset": "demo_r_pjanind2", "params": {"indic_de": "MEDAGEPOP"}},
        "label": "Median age of population"
    },

    # ==== FINANCIAL (ECB tested and stable) ====
    "deposit_rate": {
        "provider": "ECB",
        "flow": "FM",
        "series": "D.U2.EUR.4F.KR.DFR.LEV",
        "freq": "D",
        "label": "Deposit Facility Rate"
    },
    "refinancing_rate": {
        "provider": "ECB",
        "flow": "FM",
        "series": "B.U2.EUR.4F.KR.MRR_FR.LEV",
        "freq": "D",
        "label": "Main Refinancing Operations – Fixed Rate Tenders"
    },
    "borrowing_households": {
        "provider": "ECB",
        "flow": "MIR",
        "series": "M.U2.B.A2C.AM.R.A.2250.EUR.N",
        "freq": "M",
        "label": "Cost of borrowing for households (house purchase)"
    },
    "yield_curve_10y": {
        "provider": "ECB",
        "flow": "YC",
        "series": "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y",
        "freq": "D",
        "label": "Yield curve 10-year AAA government bond"
    },
    "money_supply": {
        "provider": "ECB",
        "flow": "BSI",
        "series": "M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
        "freq": "M",
        "label": "Money supply (M3)"
    },
    "loans_households": {
        "provider": "ECB",
        "flow": "BSI",
        "series": "M.U2.N.A.A20.A.1.U2.2240.Z01.E",
        "freq": "M",
        "label": "Loans to households"
    },

    # ==== MARKETS ====
    "exchange_rate": {
        "provider": "ECB",
        "flow": "EXR",
        "pattern": "D.{pair}.EUR.SP00.A",
        "freq": "D",
        "label": "Exchange rate EUR/{pair}"
    },
    "hours_worked": {
        "provider": "ECB",
        "flow": "ENA",
        "series": "Q.Y.I8.W2.S1.S1._Z.EMP._Z._T._Z.HW._Z.N",
        "freq": "Q",
        "label": "Hours worked"
    }
}


# -------------------------------------------------------------
# 🗣️ Synonyms
# -------------------------------------------------------------
SYNONYMS = {
    # ==== ECONOMIC ====  (English + Italian — user writes both)
    "gdp_per_capita": [
        "gdp per capita", "income per person", "purchasing power", "pps gdp", "per capita gdp",
        "pil pro capite", "reddito pro capite", "potere d'acquisto",
    ],
    "gdp_real": [
        "gdp", "real gdp", "volume gdp", "gdp constant prices", "gross domestic product",
        "pil", "prodotto interno lordo",
    ],
    "gdp_growth": [
        "gdp growth", "economic growth", "growth rate", "gdp change",
        "crescita del pil", "crescita economica", "crescita pil",
    ],
    "inflation": [
        "inflation", "hicp", "prices", "consumer prices", "price level",
        "inflazione", "prezzi al consumo", "prezzi", "ipca",
    ],
    "unemployment": [
        "unemployment", "jobless", "jobless rate",
        "disoccupazione", "tasso di disoccupazione", "senza lavoro",
    ],
    "employment": [
        "employment", "jobs", "workforce", "employment rate",
        "occupazione", "tasso di occupazione",
    ],
    "poverty_rate": [
        "poverty", "income inequality", "social exclusion", "at risk of poverty",
        "povertà", "rischio povertà", "esclusione sociale",
    ],
    "debt_gdp": [
        "public debt", "government debt", "debt to gdp", "fiscal debt",
        "debito pubblico", "debito", "debito/pil", "debito su pil",
    ],
    "gov_deficit": [
        "deficit", "budget deficit", "fiscal deficit", "government deficit", "public deficit", "surplus",
        "deficit pubblico", "disavanzo", "deficit di bilancio", "saldo di bilancio",
    ],
    "house_prices": [
        "house price", "house prices", "housing prices", "home prices", "property prices", "real estate prices",
        "prezzi delle case", "prezzi immobili", "prezzi immobiliari", "mercato immobiliare",
    ],
    "labour_cost": [
        "labour cost", "labor cost", "wages", "wage growth", "cost of labour", "compensation",
        "costo del lavoro", "salari", "stipendi", "retribuzioni",
    ],
    "industrial_production": [
        "industrial production", "industry output", "manufacturing index", "industrial index",
        "produzione industriale", "indice industriale",
    ],
    "hours_worked": [
        "hours worked", "working hours", "labour hours",
        "ore lavorate", "ore di lavoro",
    ],
    "median_age": [
        "median age", "average age", "ageing", "aging",
        "età media", "eta media", "invecchiamento",
    ],
    "population": [
        "population", "inhabitants", "demographics", "how many people",
        "popolazione", "abitanti", "residenti", "numero di abitanti",
    ],

    # ==== FINANCIAL ====
    "deposit_rate": [
        "deposit rate", "ecb deposit", "deposit facility", "facility rate",
        "tasso di deposito", "tasso sui depositi", "deposito bce",
    ],
    "refinancing_rate": [
        "refinancing rate", "main refinancing", "refi rate", "ecb tender", "mro", "refinancing operations",
        "tasso di rifinanziamento", "rifinanziamento principale",
    ],
    "borrowing_households": [
        "cost of borrowing", "household borrowing", "mortgage rate", "home loan", "housing loan", "loan rate",
        "costo del credito", "mutuo", "tasso sui mutui", "prestito casa",
    ],
    "yield_curve_10y": [  # ECB AAA euro-area benchmark curve (EA only)
        "yield curve", "aaa yield", "aaa curve", "curva dei rendimenti", "curva aaa",
    ],
    "lt_yield": [  # Maastricht long-term yield, per country (Eurostat)
        "10-year yield", "bond yield", "government bond", "sovereign yield", "long-term rate",
        "10-year bond", "government bond yield", "maastricht",
        "rendimento decennale", "rendimento titoli di stato", "titoli di stato", "btp", "tasso decennale",
    ],
    "money_supply": [
        "money supply", "m3", "liquidity", "monetary aggregate",
        "offerta di moneta", "massa monetaria", "liquidità",
    ],
    "loans_households": [
        "loans to households", "household loans", "consumer credit", "personal loans", "mortgages",
        "prestiti alle famiglie", "prestiti", "credito al consumo", "mutui",
    ],

    # ==== MARKETS ====
    "exchange_rate": [
        "exchange rate", "eur to", "eur/", "eur ", "eurusd", "eur usd",
        "euro dollar", "euro pound", "euro yen", "currency", "forex", "fx", "eur gbp", "eur jpy",
        "tasso di cambio", "cambio", "euro dollaro", "euro sterlina", "euro yen", "valuta",
    ],
}


def match_indicator(text: str) -> list[str]:
    """Return matched indicator keys, longest (most specific) synonym first,
    so 'pil pro capite' beats bare 'pil' and 'senza lavoro' beats 'lavoro'."""
    text = text.lower()
    hits = []  # (synonym_len, key)
    for key, synonyms in SYNONYMS.items():
        best = max((len(s) for s in synonyms if s in text), default=0)
        if best:
            hits.append((best, key))
    hits.sort(reverse=True)
    return [key for _, key in hits]

# -------------------------------------------------------------
# 🧠 LLM classifier (optional)
# -------------------------------------------------------------
def llm_detect_category(text: str) -> str | None:
    if not client:
        return None
    try:
        system = (
            "Classify the user's query into one of these indicators:\n"
            f"{list(SYNONYMS.keys())}\nReturn only the keyword."
        )
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "system", "content": system},
                      {"role": "user", "content": text}],
            temperature=0,
            max_tokens=15,
        )
        out = resp.choices[0].message.content.lower().strip()
        for k in SYNONYMS.keys():
            if k in out:
                return k
        return None
    except Exception as e:
        logger.warning(f"⚠️ LLM detect error: {e}")
        return None

# -------------------------------------------------------------
# 🌐 ECB Fetcher (JSON→CSV fallback)
# -------------------------------------------------------------
def _parse_sdmx_json(j):
    series = next(iter(j["data"]["dataSets"][0]["series"].values()))
    obs = series.get("observations", {})
    times = [v["id"] for v in j["data"]["structure"]["dimensions"]["observation"][0]["values"]]
    data = [(times[i], v[0] if isinstance(v, list) else v) for i, (_, v) in enumerate(obs.items()) if i < len(times)]
    df = pd.DataFrame(data, columns=["time", "value"])
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df.dropna().sort_values("time")

def _parse_ecb_csv(text):
    lines = text.splitlines()
    start = next(i for i, line in enumerate(lines) if "OBS_VALUE" in line)
    df = pd.read_csv(io.StringIO("\n".join(lines[start:])))
    time_col = next((c for c in df.columns if "TIME" in c), df.columns[0])
    val_col = next((c for c in df.columns if c.upper() == "OBS_VALUE"), None)
    df = pd.DataFrame({"time": pd.to_datetime(df[time_col], errors="coerce"),
                       "value": pd.to_numeric(df[val_col], errors="coerce")})
    return df.dropna()

def fetch_ecb(flow, key):
    url_json = f"{ECB_BASE}/{flow}/{key}?format=sdmx-json"
    headers = {"Accept": "application/vnd.sdmx.data+json;version=2.1.0"}
    r = requests.get(url_json, headers=headers, timeout=30)
    if r.status_code != 200:
        url_csv = f"{ECB_BASE}/{flow}/{key}?format=csvdata"
        rc = requests.get(url_csv, timeout=30)
        if rc.status_code != 200:
            print(f"❌ ECB {flow}/{key} → {rc.status_code}")
            return pd.DataFrame()
        df = _parse_ecb_csv(rc.text)
        print(f"⚠️ ECB {flow}/{key} → fallback CSV ({len(df)} obs)")
        return df
    try:
        df = _parse_sdmx_json(r.json())
        print(f"✅ ECB {flow}/{key} → {len(df)} obs")
        return df
    except Exception:
        return pd.DataFrame()

# -------------------------------------------------------------
# 🇪🇺 Eurostat fetcher
# -------------------------------------------------------------
def eurostat_fetch(dataset, params, geo):
    url = EUROSTAT_BASE + dataset
    qs = {"format": "JSON", "lang": "EN", "geo": geo}
    qs.update(params)
    r = requests.get(url, params=qs, timeout=60)
    if r.status_code != 200:
        print(f"❌ Eurostat {dataset} {geo} → {r.status_code}")
        return pd.DataFrame()
    j = r.json()
    if "value" not in j:
        return pd.DataFrame()
    dims = j["dimension"]
    labels = {d: dims[d]["category"]["label"] for d in dims if d not in ["id","size"]}
    keys = [list(dims[d]["category"]["index"].keys()) for d in dims if d not in ["id","size"]]
    combos = list(itertools.product(*keys))
    vals = list(j["value"].values())
    recs = []
    for i, combo in enumerate(combos):
        if i >= len(vals): break
        rcd = {d: labels[d][combo[j]] for j, d in enumerate(labels.keys())}
        rcd["value"] = vals[i]; recs.append(rcd)
    df = pd.DataFrame(recs)
    if "time" not in df.columns:
        df.rename(columns={"TIME_PERIOD": "time"}, inplace=True)
    df["date"] = pd.to_datetime(df["time"].replace(
        {"-Q1": "-01-01", "-Q2": "-04-01", "-Q3": "-07-01", "-Q4": "-10-01"}, regex=True),
        errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    print(f"✅ Eurostat {dataset} {geo} → {len(df)} obs")
    return df

# -------------------------------------------------------------
# 🧭 Unified Interpreter
# -------------------------------------------------------------
def interpret_query_with_ai(user_text: str):
    """Detects explicit FX pairs first, then falls back to ECB/Eurostat indicators."""
    logger.info(f"🔮 Interpreting query: {user_text}")
    text = user_text.lower()
    country = detect_countries(text)
    geos = detect_all_countries(text)   # [] if none named; may be 1 or many
    params = detect_period(text)

    # 0️⃣ Priority: FX pairs (EUR/USD, euro dollar, etc.)
    fx_aliases = {
        "usd": "USD", "dollar": "USD",
        "gbp": "GBP", "pound": "GBP",
        "jpy": "JPY", "yen": "JPY",
        "chf": "CHF", "franc": "CHF",
        "pln": "PLN", "zloty": "PLN",
        "try": "TRY", "lira": "TRY",
        "sek": "SEK", "krone": "SEK",
        "nok": "NOK", "krone": "NOK",
        "huf": "HUF", "forint": "HUF",
        "cny": "CNY", "yuan": "CNY"
    }
    for alias, code in fx_aliases.items():
        if re.search(rf"(eur|euro)[/\s-]*{alias}", text):
            meta = INDICATOR_CATALOG["exchange_rate"]
            logger.info(f"✅ Matched FX pair → {code}")
            return {
                "provider": "ECB",
                "flow": meta["flow"],
                "series": meta["pattern"].format(pair=code),
                "freq": meta["freq"],
                "indicator": meta["label"].format(pair=code),
                "params": params,
                "geos": ["EA"],  # FX is a currency pair, not a per-country series
            }

    # 1️⃣ Try match synonyms or LLM
    matches = match_indicator(text)
    if not matches and USE_LLM:
        cat = llm_detect_category(user_text)
        if cat:
            matches = [cat]
    if not matches:
        # No indicator recognized → let the bot show a helpful hint instead of
        # silently charting inflation.
        return {"provider": "unknown", "query": user_text, "geos": geos}

    key = matches[0]
    meta = INDICATOR_CATALOG[key]

    if meta["provider"] == "ECB":
        geo_template = meta.get("geo_template")
        # Per-country only if the series supports it; else euro-area single line.
        plan_geos = (geos or ["EA"]) if geo_template else ["EA"]
        series = meta["series"]
        if geo_template:
            series = geo_template.format(geo=_ecb_geo(plan_geos[0]))
        return {"provider": "ECB", "flow": meta["flow"], "series": series,
                "geo_template": geo_template, "freq": meta["freq"],
                "indicator": meta["label"], "params": params, "geos": plan_geos}
    elif meta["provider"] == "Eurostat":
        regions = detect_all_regions(user_text)
        if regions and meta.get("regional"):  # NUTS-2 regional variant
            r = meta["regional"]
            return {"provider": "Eurostat", "dataset": r["dataset"],
                    "eu_params": r["params"], "params": params,
                    "indicator": f"{meta['label']} — regional", "geos": regions}
        plan_geos = geos or ["EA"]
        return {"provider": "Eurostat", "dataset": meta["dataset"],
                "eu_params": meta["params"], "params": params,
                "indicator": meta["label"], "geos": plan_geos}

    logger.warning("⚠️ Defaulting to Euro area inflation.")
    return {"provider": "ECB", "flow": "ICP",
            "series": "M.U2.N.000000.4.ANR", "freq": "M",
            "indicator": "Inflation (Euro area, default)",
            "params": params}

# -------------------------------------------------------------
# 🚀 Fetch function
# -------------------------------------------------------------
def fetch_data_auto(query_dict):
    provider = query_dict["provider"]
    if provider == "ECB":
        df = fetch_ecb(query_dict["flow"], query_dict["series"])
        df["country"] = "EA"
        df["indicator"] = query_dict["indicator"]
        return df
    elif provider == "Eurostat":
        geo = query_dict["params"].get("geo", "EA")
        df = eurostat_fetch(query_dict["dataset"], query_dict["params"], geo)
        df["country"] = geo
        df["indicator"] = query_dict["indicator"]
        return df
    else:
        print("❌ Unknown provider.")
        return pd.DataFrame()

# -------------------------------------------------------------
# 🧪 Local test
# -------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        "GDP Euro area since 2015",
        "Inflation Euro area",
        "Unemployment Italy",
        "Poverty rate France",
        "Deposit rate ECB",
        "Exchange rate euro dollar",
        "EUR/JPY exchange rate",
        "Exchange rate euro lira",
    ]
    for t in tests:
        print(f"\n🗨️ {t}")
        q = interpret_query_with_ai(t)
        print(q)
