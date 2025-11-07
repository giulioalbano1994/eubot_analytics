"""
Modulo: ecb_discovery.py
Funzioni per interrogare l’API dell’ECB Data Portal:
- elencare dataflow disponibili
- recuperare la datastructure (DSD) di un flow
- recuperare le codelist (dimensioni possibili per le series key)
Utili per costruire dinamicamente la serie key e validare input utente.
"""

import requests
import pandas as pd
import logging

BASE_URL = "https://data-api.ecb.europa.eu/service"

class ECBDiscoveryError(Exception):
    pass

def list_dataflows(format: str = "jsondata") -> pd.DataFrame:
    """
    Restituisce un DataFrame con tutti i dataflow dell’ECB.
    Colonne: flowRef, agencyId, version, name, description
    """
    url = f"{BASE_URL}/dataflow?format={format}"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise ECBDiscoveryError(f"HTTP {r.status_code}: {r.text[:200]}")
    j = r.json()
    # j["data"]["dataflow"] è lista di dataflow
    df = pd.json_normalize(j["data"]["dataflow"])
    return df[["agencyId", "flowRef", "version", "name.en", "description.en"]]

def get_datastructure(flowRef: str, version: str = "") -> dict:
    """
    Recupera la DSD (datastructure definition) per un dataflow specifico.
    Ritorna il JSON raw della definizione.
    """
    if version:
        url = f"{BASE_URL}/datastructure/{flowRef}/{version}?format=jsondata"
    else:
        url = f"{BASE_URL}/datastructure/{flowRef}?format=jsondata"
    r = requests.get(url, timeout=30)
    if r.status_code != 200:
        raise ECBDiscoveryError(f"HTTP {r.status_code}: {r.text[:200]}")
    return r.json()

def extract_series_key_template(dsd_json: dict) -> str:
    """
    Dato il JSON della DSD, restituisce un template della series key
    con dimensioni nel corretto ordine, separatore '.', es: 'FREQ.COUNTRY.MEASURE.VARIANT'
    Le dimensioni sono lette da dsd_json["structure"]["dimensions"]["series"]
    """
    try:
        dims = dsd_json["structure"]["dimensions"]["series"]
        keys = [dim["id"] for dim in dims]
        return ".".join(keys)
    except Exception as e:
        logging.exception(f"Errore nella estrazione template series key: {e}")
        raise ECBDiscoveryError(f"Parsing DSD failed: {e}")

def list_codelist_for_dimension(dsd_json: dict, dimension_id: str) -> pd.DataFrame:
    """
    Restituisce il codelist per una dimensione della series key.
    Output: DataFrame con code, description
    """
    try:
        # Le codelist sono in dsd_json["structure"]["codelists"]["codelist"]
        cl = []
        for cl_item in dsd_json["structure"]["codelists"]["codelist"]:
            for enumerated in cl_item["enumeratedMember"]:
                if enumerated["codelist"] == dimension_id:
                    cl.append({"code": enumerated["id"], "description": enumerated["name.en"]})
        return pd.DataFrame(cl)
    except Exception as e:
        logging.exception(f"Errore list codelist: {e}")
        raise ECBDiscoveryError(f"Codelist extraction failed: {e}")
