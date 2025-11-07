"""
=============================================================
Module: fetch_ecb_data
=============================================================
Fetches data from the new ECB Data Portal API (SDMX REST 2.2.2)

Example:
    fetch_ecb_data("ICP", "M.U2.N.000000.4.ANR", {"lastNObservations": 240})

Base endpoint:
    https://data-api.ecb.europa.eu/service/data/{flow}/{key}?format=csvdata
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
        key (str): SDMX series key, e.g. "M.U2.N.000000.4.ANR"
        params (dict): Optional parameters, e.g. {"lastNObservations": 240}

    Returns:
        pd.DataFrame: Columns ['TIME_PERIOD', 'OBS_VALUE']
    """
    if params is None:
        params = {"lastNObservations": 120}

    # ✅ Correct new endpoint
    url = f"{BASE_URL}/data/{flow}/{key}?format=csvdata"
    logger.info(f"📡 Fetching ECB data: {url}")

    resp = requests.get(url, params=params, timeout=30)
    if resp.status_code != 200:
        logger.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
        raise Exception(f"ECB API error: {resp.status_code}")

    # ✅ Parse CSV response
    df = pd.read_csv(StringIO(resp.text))

    # ✅ Sanity check
    if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
        logger.warning("⚠️ Unexpected data format from ECB API.")
        return df

    # ✅ Clean & sort
    df["TIME_PERIOD"] = pd.to_datetime(df["TIME_PERIOD"], errors="coerce")
    df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
    df = df.sort_values("TIME_PERIOD").reset_index(drop=True)

    logger.info(f"✅ ECB data fetched successfully ({len(df)} rows).")
    return df[["TIME_PERIOD", "OBS_VALUE"]]


# -------------------------------------------------------------
# Local test
# -------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    df = fetch_ecb_data("EXR", "D.USD.EUR.SP00.A", {"lastNObservations": 10})
    print(df.tail())
