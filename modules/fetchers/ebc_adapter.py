"""
=====================================================================
Module: ebc_adapter.py  (v4 — Complete ECB Data Engine)
=====================================================================
Robust unified interface to the European Central Bank Data Portal
using the official SDMX 2.1 REST API and the `ecbdata` Python client.
Covers Macro, Monetary, and FX categories with metadata support.
=====================================================================
"""

import io
import time
import pandas as pd
import requests
import logging
from ecbdata import ecbdata
from datetime import datetime
from pathlib import Path

CACHE_TTL = 24 * 3600  # seconds: refetch daily so published revisions land

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://data-api.ecb.europa.eu/service/data"
CACHE_DIR = Path("data/cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------
# Core function
# -------------------------------------------------------------
def fetch_ecb_data(flow: str, key: str, params: dict | None = None, cache: bool = True) -> pd.DataFrame:
    """
    Fetch data from ECB Data Portal (via `ecbdata` or REST fallback).

    Args:
        flow (str): Dataflow ID (e.g. "ICP", "EXR", "MNA", "FM", "BSI")
        key (str): Series key (without flow prefix)
        params (dict, optional): {"startPeriod": "YYYY-MM", "endPeriod": "YYYY-MM"}
        cache (bool): If True, cache results to /data/cache

    Returns:
        pd.DataFrame with standardized columns:
        ['TIME_PERIOD', 'OBS_VALUE', 'COUNTRY', 'FLOW']
    """
    full_series = f"{flow}.{key}"
    start = params.get("startPeriod") if params else None
    end = params.get("endPeriod") if params else None
    last = params.get("lastNObservations") if params else None

    # Cache key MUST include the period params, else a full-history pull masks
    # every later windowed request for the same series.
    tag = "_".join(x for x in (start, end, str(last) if last else "") if x).replace(":", "")
    stem = full_series.replace(".", "_") + (f"__{tag}" if tag else "")
    cache_file = CACHE_DIR / f"{stem}.csv"
    if cache and cache_file.exists() and (time.time() - cache_file.stat().st_mtime) < CACHE_TTL:
        try:
            df = pd.read_csv(cache_file, parse_dates=["TIME_PERIOD"])
            logger.info(f"📂 Loaded from cache: {cache_file.name} ({len(df)} rows)")
            return df
        except Exception:
            pass

    try:
        logger.info(f"📡 [ECB] Fetching {full_series} via ecbdata...")
        df = ecbdata.get_series(full_series, start=start, end=end, lastnobservations=last)

        if df.empty:
            logger.warning(f"⚠️ No data via ecbdata — trying REST CSV fallback.")
            df = _fetch_ecb_csv(flow, key, params)

        if df.empty:
            logger.error(f"❌ No data found for {full_series}")
            return pd.DataFrame()

        df = _normalize_ecb_df(df, key, flow)

        if cache:
            df.to_csv(cache_file, index=False)

        logger.info(f"✅ [ECB] {len(df)} observations fetched for {full_series}")
        return df

    except Exception as e:
        logger.error(f"❌ Exception fetching {full_series}: {e}")
        return pd.DataFrame()


# -------------------------------------------------------------
# REST fallback (CSV)
# -------------------------------------------------------------
def _fetch_ecb_csv(flow: str, key: str, params: dict | None = None) -> pd.DataFrame:
    url = f"{BASE_URL}/{flow}/{key}"
    qs = {"format": "csvdata"}
    if params:
        qs.update(params)

    try:
        r = requests.get(url, params=qs, timeout=60)
        r.raise_for_status()
        df = pd.read_csv(io.StringIO(r.text))
        logger.info(f"📥 REST CSV retrieved successfully for {flow}/{key}")
        return df
    except requests.exceptions.HTTPError as e:
        logger.warning(f"⚠️ HTTP {r.status_code} — {url}")
        return pd.DataFrame()
    except Exception as e:
        logger.warning(f"⚠️ CSV fetch failed for {flow}/{key}: {e}")
        return pd.DataFrame()


# -------------------------------------------------------------
# Normalization helper
# -------------------------------------------------------------
def _normalize_ecb_df(df: pd.DataFrame, key: str, flow: str) -> pd.DataFrame:
    cols = [c.upper() for c in df.columns]
    df.columns = cols

    # Time + value columns
    time_col = next((c for c in cols if "TIME" in c or "PERIOD" in c), None)
    val_col = next((c for c in cols if "OBS" in c or "VALUE" in c), None)

    if not time_col or not val_col:
        raise ValueError("Missing TIME or VALUE columns in ECB dataset")

    df = df.rename(columns={time_col: "TIME_PERIOD", val_col: "OBS_VALUE"})
    df["FLOW"] = flow
    df["COUNTRY"] = _infer_country(df, key)
    # astype(str) first: annual ECB series return TIME_PERIOD as int year (2021),
    # which to_datetime would misread as nanoseconds → 1970. As strings, "2021",
    # "2021-Q1", "2021-01", "2021-01-01" all parse correctly.
    df["TIME_PERIOD"] = pd.to_datetime(df["TIME_PERIOD"].astype(str), errors="coerce")
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD", "OBS_VALUE"]).sort_values("TIME_PERIOD")

    return df[["TIME_PERIOD", "OBS_VALUE", "COUNTRY", "FLOW"]]


def _infer_country(df: pd.DataFrame, key: str) -> str:
    if "REF_AREA" in df.columns:
        return df["REF_AREA"].iloc[0]
    if "GEO" in df.columns:
        return df["GEO"].iloc[0]
    if "COUNTRY" in df.columns:
        return df["COUNTRY"].iloc[0]
    for part in key.split("."):
        if len(part) == 2 and part.isupper():
            return part
    return "U2"  # euro area default


# -------------------------------------------------------------
# Metadata retriever
# -------------------------------------------------------------
def get_ecb_metadata(flow: str, key: str) -> dict:
    """
    Fetch metadata for a specific ECB series.
    Returns title, frequency, unit, and last update date.
    """
    meta_url = f"{BASE_URL}/{flow}/{key}?format=sdmx-json"
    try:
        r = requests.get(meta_url, timeout=40)
        r.raise_for_status()
        j = r.json()

        header = j.get("header", {})
        structures = j.get("structure", {})
        title = header.get("id", full_series := f"{flow}.{key}")
        last_update = header.get("prepared", "")

        dims = structures.get("dimensions", {}).get("observation", [])
        attrs = {d["id"]: d.get("name", "") for d in dims}

        return {
            "title": title,
            "frequency": attrs.get("FREQ", ""),
            "unit": attrs.get("UNIT_MEASURE", ""),
            "last_update": last_update,
            "flow": flow,
        }

    except Exception as e:
        logger.warning(f"⚠️ Metadata fetch failed for {flow}/{key}: {e}")
        return {
            "title": f"{flow}.{key}",
            "frequency": "",
            "unit": "",
            "last_update": "",
            "flow": flow,
        }


# -------------------------------------------------------------
# Availability checker
# -------------------------------------------------------------
def check_ecb_availability(flow: str, key_prefix: str) -> list:
    """
    Returns available combinations (series) for a given prefix.
    Example: check_ecb_availability("EXR", "D.USD.EUR")
    """
    url = f"{BASE_URL}/{flow}/{key_prefix}"
    try:
        r = requests.get(url, params={"format": "sdmx-json"}, timeout=40)
        r.raise_for_status()
        j = r.json()

        obs = j.get("data", {}).get("dataSets", [{}])[0].get("observations", {})
        return list(obs.keys()) if obs else []
    except Exception as e:
        logger.warning(f"⚠️ Availability check failed for {flow}/{key_prefix}: {e}")
        return []


# -------------------------------------------------------------
# Example manual test
# -------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    tests = [
        ("ICP", "M.U2.N.000000.4.ANR", "Inflation EA19"),
        ("MNA", "Q.Y.IT.W2.S1.S1.B.B1GQ._Z._Z._Z.EUR.LR.N", "GDP Italy"),
        ("FM", "D.U2.EUR.4F.KR.DFR.LEV", "Deposit Facility Rate"),
        ("EXR", "D.USD.EUR.SP00.A", "EUR/USD Exchange Rate"),
    ]

    for flow, key, name in tests:
        print(f"\n➡️ {name}")
        df = fetch_ecb_data(flow, key, {"startPeriod": "2019-01"})
        print(df.tail())

        meta = get_ecb_metadata(flow, key)
        print("🧩 Metadata:", meta)
