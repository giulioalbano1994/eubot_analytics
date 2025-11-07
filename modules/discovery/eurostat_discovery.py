"""
Modulo: eurostat_discovery.py
Funzioni per interrogare l’API SDMX/Eurostat:
- elencare dataflow
- interrogare structure definitions per un flow
- listare codelist
Utilissimo per adapter Eurostat.
"""

import requests
import pandas as pd
import logging

BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1"

class EurostatDiscoveryError(Exception):
    pass

def list_dataflows(format: str = "json") -> pd.DataFrame:
    """
    Ritorna un DataFrame con l’elenco dei dataflow disponibili su Eurostat.
    """
    url = f"{BASE_URL}/dataflow?format={format}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise EurostatDiscoveryError(f"HTTP {r.status_code}: {r.text[:200]}")
    j = r.json()
    df = pd.json_normalize(j["structure"]["dataflows"]["dataflow"])
    return df[["@id", "Name", "Description"]]

def get_datastructure(flow_id: str) -> dict:
    """
    Recupera la DSD per un flow Eurostat.
    Es: flow_id = 'une_rt_m'
    """
    url = f"{BASE_URL}/structure/datastructure/{flow_id}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise EurostatDiscoveryError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

# (Funzioni per listare codelist ecc. da aggiungere se serve)
