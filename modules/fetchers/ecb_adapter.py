"""
=============================================================
Module: fetch_ecb_data
=============================================================
Fetches data from the new ECB Data Portal API (SDMX REST 2.2.2)

Supports:
- Single or multiple countries (e.g. "M.IT+BE.N.000000.4.ANR")
- Dynamic time filters (startPeriod, lastNObservations, etc.)
- Standardized output with COUNTRY column

Example:
    fetch_ecb_data("ICP", "M.IT+BE.N.000000.4.ANR", {"startPeriod": "2020-01"})
=============================================================
"""

import requests
import pandas as pd
import logging
from io import StringIO

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

BASE_URL = "https://data-api.ecb.europa.eu/service"


# -------------------------------------------------------------
# Main function
# -------------------------------------------------------------
def fetch_ecb_data(flow: str, key: str, params: dict | None = None) -> pd.DataFrame:
    """
    Fetch ECB data from the new ECB Data Portal API (SDMX REST 2.2.2).

    Args:
        flow (str): Dataflow ID, e.g. "ICP"
        key (str): SDMX series key, e.g. "M.U2.N.000000.4.ANR" or "M.IT+BE.N.000000.4.ANR"
        params (dict): Optional parameters, e.g. {"startPeriod": "2020-01"}

    Returns:
        pd.DataFrame: Columns ['TIME_PERIOD', 'OBS_VALUE', 'COUNTRY']
    """
    if params is None:
        params = {"lastNObservations": 120}

    url = f"{BASE_URL}/data/{flow}/{key}?format=csvdata"
    logger.info(f"📡 Fetching ECB data: {url} | params={params}")

    # Perform request
    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        logger.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
        raise Exception(f"ECB API error: {resp.status_code}")

    # Parse CSV response
    df = pd.read_csv(StringIO(resp.text))

    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        logger.warning("⚠️ Unexpected data format from ECB API.")
        return pd.DataFrame()

    # Extract country from SDMX key (3rd position in pattern M.IT.N...)
    if "KEY" in df.columns:
        df["COUNTRY"] = df["KEY"].astype(str).str.split(".").str[2]
    else:
        # Fallback if API doesn't return full key (use pattern from request)
        try:
            match_part = key.split(".")[1]
            if "+" in match_part:
                df = pd.concat(
                    [df.assign(COUNTRY=c) for c in match_part.split("+")],
                    ignore_index=True
                )
            else:
                df["COUNTRY"] = match_part
        except Exception:
            df["COUNTRY"] = "U2"

    # Clean and sort
    df["TIME_PERIOD"] = pd.to_datetime(df["TIME_PERIOD"], errors="coerce")
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.dropna(subset=["TIME_PERIOD", "OBS_VALUE"]).sort_values("TIME_PERIOD")

    logger.info(f"✅ ECB data fetched successfully ({len(df)} rows, {df['COUNTRY'].nunique()} countries).")

    return df[["TIME_PERIOD", "OBS_VALUE", "COUNTRY"]]


# -------------------------------------------------------------
# Local test
# -------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # 🇮🇹🇧🇪 Example: Italy + Belgium inflation, last 3 years
    df = fetch_ecb_data("ICP", "M.IT+BE.N.000000.4.ANR", {"startPeriod": "2022-01"})
    print(df.tail())
