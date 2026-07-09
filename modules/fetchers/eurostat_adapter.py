# modules/fetchers/eurostat_adapter.py
import itertools
import logging
import requests
import pandas as pd

logger = logging.getLogger(__name__)

EUROSTAT_BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

# Fallback per “Euro area” che su Eurostat è EA20 (dal 2023), poi EA19…
_EA_ALIASES = ["EA20", "EA19", "EA", "U2"]

def _expand_eurostat_json(j):
    """Decode Eurostat JSON-stat 2.0 into tidy records.

    JSON-stat stores observations in a sparse dict keyed by the ROW-MAJOR flat
    index over the dimensions listed in j['id'] with cardinalities j['size'].
    The old version faked this with itertools.product over dict-key order, which
    misaligns time↔value on multi-dimension datasets (GDP, house prices, …).
    """
    if not j.get("value"):
        return pd.DataFrame()
    dims = j["dimension"]
    ids = j.get("id") or [d for d in dims if d not in ("id", "size")]
    sizes = j.get("size") or [len(dims[d]["category"]["index"]) for d in ids]
    idx2code = {d: {v: k for k, v in dims[d]["category"]["index"].items()} for d in ids}
    strides = [1] * len(sizes)
    for i in range(len(sizes) - 2, -1, -1):
        strides[i] = strides[i + 1] * sizes[i + 1]
    out = []
    for flat_str, val in j["value"].items():
        flat = int(flat_str)
        rec = {d: idx2code[d][(flat // strides[k]) % sizes[k]] for k, d in enumerate(ids)}
        rec["OBS_VALUE"] = val
        out.append(rec)
    return pd.DataFrame(out)

def _to_period(df: pd.DataFrame) -> pd.DataFrame:
    """Normalizza TIME → TIME_PERIOD (datetime) e OBS_VALUE numerico."""
    if "time" in df.columns:
        df = df.rename(columns={"time":"TIME_PERIOD"})
    elif "TIME_PERIOD" not in df.columns:
        # Individua colonna tempo tipica (“time” o simili)
        for c in df.columns:
            if c.lower() in ("time","time_period","period"):
                df = df.rename(columns={c:"TIME_PERIOD"})
                break
    # trimestri → date, anni → 01-01, mesi → primo giorno
    s = df["TIME_PERIOD"].astype(str)
    s = s.str.replace("-Q1","-01-01").str.replace("-Q2","-04-01") \
         .str.replace("-Q3","-07-01").str.replace("-Q4","-10-01")
    # Se è solo anno, aggiungi “-01-01”
    s = s.where(s.str.contains("-"), s + "-01-01")
    df["TIME_PERIOD"] = pd.to_datetime(s, errors="coerce")
    df["OBS_VALUE"] = pd.to_numeric(df.get("OBS_VALUE"), errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD","OBS_VALUE"]).sort_values("TIME_PERIOD")
    return df

def fetch_eurostat_data(dataset: str, params: dict) -> pd.DataFrame:
    """
    Scarica un dataset Eurostat e restituisce:
    columns = [TIME_PERIOD, OBS_VALUE, COUNTRY]
    """
    geo = params.get("geo")
    tries = [geo] if geo else []
    # Se geo è “EA”/“U2” prova alias; se è un paese, usa quello soltanto
    if geo in ("EA","U2", None):
        tries = _EA_ALIASES
    seen_any = False

    for g in tries:
        qs = {"format":"JSON", "lang":"EN", **{k:v for k,v in params.items() if k!="geo"}, "geo": g}
        url = EUROSTAT_BASE + dataset
        r = requests.get(url, params=qs, timeout=60)
        if r.status_code != 200:
            logger.warning(f"Eurostat {dataset} geo={g} → HTTP {r.status_code}")
            continue
        seen_any = True
        df_raw = _expand_eurostat_json(r.json())
        if df_raw.empty: 
            continue
        # mappa “time” → TIME_PERIOD, cast numeri
        df = _to_period(df_raw)
        if df.empty: 
            continue
        # EUROSTAT usa “geo” come etichetta già espansa
        country_col = "geo" if "geo" in df.columns else "GEO"
        if country_col not in df.columns:
            # proviamo a ricavarla da label/dimensioni
            df[country_col] = g
        df = df.rename(columns={country_col:"COUNTRY"})
        df = df[["TIME_PERIOD","OBS_VALUE","COUNTRY"]]
        logger.info(f"✅ Eurostat {dataset} ({g}) → {len(df)} obs")
        return df

    if not seen_any:
        raise SystemError(f"Eurostat {dataset} nessuna risposta valida (params={params})")
    return pd.DataFrame(columns=["TIME_PERIOD","OBS_VALUE","COUNTRY"])
