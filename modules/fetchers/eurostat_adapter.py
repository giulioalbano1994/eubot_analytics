"""
=============================================================
Modulo: fetchers.eurostat_adapter
=============================================================
📊 Funzione:
    Interfaccia ufficiale con i dati Eurostat (ESTAT)
    tramite protocollo SDMX 2.1 con `pandaSDMX`.

⚙️ Note:
    - Fonte: https://ec.europa.eu/eurostat/web/main/data/web-services
    - Libreria: pandaSDMX (https://pandasdmx.readthedocs.io/)
    - Supporta la conversione automatica in pandas.DataFrame.

📦 Output:
    pandas.DataFrame con colonne standardizzate:
    ['Date', 'Value', 'GEO', 'FREQ', 'INDICATOR']
=============================================================
"""

import logging
import pandasdmx as sdmx
import pandas as pd

logger = logging.getLogger(__name__)


def fetch_eurostat_data(flow: str, key_dict: dict, params: dict | None = None) -> pd.DataFrame:
    """
    Recupera dati SDMX dal portale Eurostat.

    Args:
        flow (str): codice del dataset Eurostat (es. 'une_rt_m', 'namq_10_gdp')
        key_dict (dict): chiavi SDMX (es. {'GEO':'IT', 'FREQ':'M', 'INDICATOR':'RT'})
        params (dict): parametri aggiuntivi, es. {'startPeriod': '2015'}

    Returns:
        pd.DataFrame: dati convertiti e standardizzati
    """
    try:
        estat = sdmx.Request('ESTAT')
        logger.info(f"📡 Eurostat fetch: flow={flow}, key={key_dict}, params={params}")

        # Effettua la richiesta SDMX
        data_msg = estat.data(flow, key=key_dict, params=params or {})

        # Conversione in DataFrame
        df = data_msg.to_pandas()
        df = df.reset_index()

        # Pulizia nomi e tipi
        df.rename(columns={
            'time': 'Date',
            'OBS_VALUE': 'Value',
            'values': 'Value'
        }, inplace=True, errors='ignore')

        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')

        logger.info(f"✅ Dati Eurostat scaricati ({len(df)} osservazioni)")
        return df

    except Exception as e:
        logger.exception(f"❌ Errore nel fetch Eurostat: {e}")
        return pd.DataFrame()
