"""
=============================================================
Module: eurostat_adapter.py  (Enhanced Stable Version)
=============================================================
- Compatibile con API Eurostat 1.0 (SDMX-JSON)
- Caching automatico per query ripetute
- Rilevamento frequenza (A/Q/M)
- Gestione errori robusta
=============================================================
"""

import os
import re
import json
import hashlib
import requests
import itertools
import pandas as pd
from collections import OrderedDict
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"

# Directory cache locale
CACHE_DIR = os.path.join(os.path.dirname(__file__), "_cache_eurostat")
os.makedirs(CACHE_DIR, exist_ok=True)


# -------------------------------------------------------------
# Helper: espansione SDMX JSON → tidy DataFrame
# -------------------------------------------------------------
def _expand_sdmx_json(j):
    """Espande SDMX-JSON (Eurostat API) in DataFrame pulito e leggibile."""
    dims = j["dimension"]
    dim_names = [d for d in dims.keys() if d not in ["id", "size"]]

    codes_by_dim = OrderedDict()
    labels_by_dim = {}
    for dim in dim_names:
        index = dims[dim]["category"]["index"]
        ordered_codes = [code for code, _ in sorted(index.items(), key=lambda kv: kv[1])]
        codes_by_dim[dim] = ordered_codes
        labels_by_dim[dim] = dims[dim]["category"].get("label", {})

    values = list(j["value"].values()) if isinstance(j["value"], dict) else j["value"]
    combos = list(itertools.product(*[codes_by_dim[dim] for dim in dim_names]))

    records = []
    for i, combo in enumerate(combos):
        if i >= len(values):
            break
        rec = {}
        for d_i, dim in enumerate(dim_names):
            code = combo[d_i]
            label = labels_by_dim[dim].get(code, code)
            rec[f"{dim}_code"] = code
            rec[dim] = label
        rec["value"] = values[i]
        records.append(rec)

    df = pd.DataFrame(records)
    code_cols = [f"{d}_code" for d in dim_names]
    label_cols = dim_names
    df = df[code_cols + label_cols + ["value"]]
    return df


# -------------------------------------------------------------
# Cache: salva e ricarica risultati API
# -------------------------------------------------------------
def _cache_key(dataset: str, params: dict) -> str:
    key = dataset + json.dumps(params, sort_keys=True)
    return hashlib.md5(key.encode()).hexdigest()


def _read_cache(key: str) -> pd.DataFrame | None:
    path = os.path.join(CACHE_DIR, f"{key}.parquet")
    if os.path.exists(path):
        try:
            return pd.read_parquet(path)
        except Exception:
            return None
    return None


def _write_cache(key: str, df: pd.DataFrame):
    path = os.path.join(CACHE_DIR, f"{key}.parquet")
    try:
        df.to_parquet(path, index=False)
    except Exception as e:
        logger.warning(f"⚠️ Failed to cache Eurostat data: {e}")


# -------------------------------------------------------------
# Main fetcher: Eurostat API 1.0
# -------------------------------------------------------------
def fetch_eurostat_data(dataset: str, params=None, years_back=5) -> pd.DataFrame:
    """Fetch Eurostat dataset via JSON API 1.0, with local caching."""
    qs = {"format": "JSON", "lang": "EN"}
    if params:
        qs.update(params)

    # Limita periodo a ultimi N anni se non specificato
    start_year = datetime.now().year - years_back
    if "time" not in qs and "sinceTimePeriod" not in qs:
        qs["sinceTimePeriod"] = str(start_year)

    cache_id = _cache_key(dataset, qs)
    cached = _read_cache(cache_id)
    if cached is not None:
        logger.info(f"🗃️ Loaded Eurostat data from cache ({len(cached)} rows).")
        return cached

    url = BASE + dataset
    logger.info(f"📡 Fetching Eurostat data: {url} | params={qs}")

    try:
        r = requests.get(url, params=qs, timeout=60)
        r.raise_for_status()
        j = r.json()
    except Exception as e:
        logger.error(f"Eurostat API error: {e}")
        return pd.DataFrame()

    if "value" not in j:
        logger.warning("⚠️ Eurostat returned no values.")
        return pd.DataFrame()

    df = _expand_sdmx_json(j)

    # Parse date
    if "time" in df.columns:
        df["time"] = df["time"].astype(str)
        df["date"] = pd.to_datetime(
            df["time"]
            .replace({"-Q1": "-01-01", "-Q2": "-04-01", "-Q3": "-07-01", "-Q4": "-10-01"}, regex=True),
            errors="coerce"
        )
    else:
        df["date"] = pd.NaT

    # Rileva frequenza automatica
    if len(df) > 0 and "time" in df.columns:
        first = str(df["time"].iloc[0])
        if "Q" in first:
            freq = "Q"
        elif re.match(r"^\d{4}-\d{2}$", first):
            freq = "M"
        else:
            freq = "A"
        df["freq"] = freq

    # Normalizza colonne
    df.rename(columns={"geo": "COUNTRY", "value": "OBS_VALUE"}, inplace=True)
    df = df.dropna(subset=["date"])
    df = df.sort_values("date")

    tidy = df[["date", "COUNTRY", "OBS_VALUE"]].copy()
    _write_cache(cache_id, tidy)

    logger.info(f"✅ Eurostat data ready ({len(tidy)} rows, freq={df.get('freq', ['?'])[0]}).")
    return tidy


# -------------------------------------------------------------
# Test locale
# -------------------------------------------------------------
if __name__ == "__main__":
    df = fetch_eurostat_data(
        "une_rt_m",
        {"geo": "IT+FR", "sex": "T", "age": "Y25-74", "unit": "PC_ACT", "s_adj": "SA"}
    )
    print(df.tail())
