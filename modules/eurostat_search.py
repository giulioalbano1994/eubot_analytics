"""Search ANY Eurostat dataset by keyword, then fetch it — no extra dependency.

Two pieces:
  search(keyword)   -> [(code, title)] from the Eurostat catalogue (TOC), cached.
  fetch_auto(code)  -> a single clean time series for a default geo, auto-picking
                       one series when the dataset has extra dimensions.

fetch_auto is a best-effort explorer: with no dimension knowledge it grabs the
data for a geo and, if several series come back (unit/sex/age/…), keeps the
longest one and reports which slice it showed. Good enough to eyeball any of
Eurostat's ~9000 datasets; use the curated catalog for the polished indicators.
"""
import logging
import time
from pathlib import Path

import requests
import pandas as pd

from modules.fetchers.eurostat_adapter import (
    EUROSTAT_BASE, _EA_ALIASES, _expand_eurostat_json, _to_period,
)

logger = logging.getLogger(__name__)

TOC_URL = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/txt"
TOC_CACHE = Path("data/cache/eurostat_toc.tsv")
TOC_TTL = 7 * 24 * 3600  # catalogue changes rarely


def _load_toc() -> pd.DataFrame:
    """Catalogue of queryable datasets: columns [code, title]. Cached 7 days."""
    if TOC_CACHE.exists() and time.time() - TOC_CACHE.stat().st_mtime < TOC_TTL:
        return pd.read_csv(TOC_CACHE, sep="\t", dtype=str).fillna("")
    logger.info("Fetching Eurostat catalogue (TOC)…")
    r = requests.get(TOC_URL, params={"lang": "en"}, timeout=90)
    r.raise_for_status()
    rows = []
    for line in r.text.splitlines()[1:]:              # skip header
        parts = [p.strip('"') for p in line.split("\t")]
        if len(parts) >= 3 and parts[2] == "dataset":  # skip folders
            rows.append((parts[1], parts[0].strip()))
    df = pd.DataFrame(rows, columns=["code", "title"])
    TOC_CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(TOC_CACHE, sep="\t", index=False)
    return df


def search(keyword: str, n: int = 8) -> list[tuple[str, str]]:
    """Datasets whose title contains ALL words of the keyword (case-insensitive)."""
    df = _load_toc()
    words = keyword.lower().split()
    if not words:
        return []
    title = df["title"].str.lower()
    mask = title.apply(lambda t: all(w in t for w in words))
    hits = df[mask].head(n)
    return list(zip(hits["code"], hits["title"]))


def fetch_auto(code: str, geo: str = "EA", last_n: int = 120):
    """Return (df[TIME_PERIOD, OBS_VALUE, COUNTRY], selection, geo) for a dataset.

    Tries EU-level geographies first (small responses); if none exist (ENP,
    NUTS-only, non-EU datasets) falls back to a geo-less fetch and keeps the
    single longest series, whatever its geography.
    selection = the dimension slice auto-picked when the dataset has extra dims.
    # ponytail: 'longest series' heuristic + geo-less fallback can be heavy on
    #           datasets with thousands of series; a filter UI would be the upgrade.
    """
    geo_tries = _EA_ALIASES + ["EU27_2020"] if geo in ("EA", "U2", None) else [geo]
    attempts = [{"geo": g} for g in geo_tries] + [{}]  # last: no geo filter
    for geo_filter in attempts:
        qs = {"format": "JSON", "lang": "EN", "lastTimePeriod": last_n, **geo_filter}
        r = requests.get(EUROSTAT_BASE + code, params=qs, timeout=60)
        if r.status_code != 200:
            continue
        raw = _expand_eurostat_json(r.json())
        if raw.empty:
            continue
        df = _to_period(raw)
        if df.empty:
            continue
        # Reduce to ONE series: group by every dimension (incl. geo) and keep the
        # longest. For geo-filtered attempts geo is constant; for the geo-less
        # fallback this is what selects a single geography.
        dim_cols = [c for c in df.columns if c not in ("TIME_PERIOD", "OBS_VALUE")]
        selection = {}
        if dim_cols:
            grp = df.groupby(dim_cols, dropna=False)
            key = max(grp.groups, key=lambda k: len(grp.groups[k]))
            df = grp.get_group(key)
            selection = dict(zip(dim_cols, key if isinstance(key, tuple) else (key,)))
        actual_geo = selection.pop("geo", geo_filter.get("geo", "?"))
        df = df[["TIME_PERIOD", "OBS_VALUE"]].copy()
        df["COUNTRY"] = actual_geo
        return df, selection, actual_geo
    return pd.DataFrame(), {}, None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    for kw in ["tourism nights", "electricity prices", "life expectancy"]:
        res = search(kw, 5)
        print(f"\n🔎 {kw!r}: {len(res)} hits")
        for code, title in res:
            print(f"   {code:22} {title[:60]}")
        if res:
            df, sel, g = fetch_auto(res[0][0])
            n = 0 if df is None or df.empty else len(df)
            print(f"   fetch {res[0][0]} [{g}] -> {n} obs, selection={sel}")
