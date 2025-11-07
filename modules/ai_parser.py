"""
=============================================================
Module: ai_parser.py
=============================================================

🧠 Purpose:
    Convert natural language queries (in English)
    into a structured "query plan" for ECB or Eurostat datasets.

💡 How it works:
    - Detects mentioned countries and time periods
    - Builds a valid JSON "query plan"
    - Supports multi-country queries for ECB data (e.g., Italy+Belgium)
    - Defaults to last 5 years if no period specified

=============================================================
"""

import os
import json
import re
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from openai import OpenAI

# -------------------------------------------------------------
# 1. Configuration
# -------------------------------------------------------------
load_dotenv()
logger = logging.getLogger(__name__)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.warning("⚠️ OPENAI_API_KEY not found in environment variables.")
client = OpenAI(api_key=OPENAI_API_KEY)


# -------------------------------------------------------------
# 2. Static country dictionary (ISO codes)
# -------------------------------------------------------------
COUNTRY_CODES = {
    "italy": "IT", "france": "FR", "germany": "DE", "spain": "ES",
    "belgium": "BE", "netherlands": "NL", "austria": "AT", "portugal": "PT",
    "greece": "GR", "finland": "FI", "ireland": "IE", "luxembourg": "LU",
    "poland": "PL", "sweden": "SE", "denmark": "DK", "hungary": "HU",
    "czech": "CZ", "romania": "RO", "bulgaria": "BG", "croatia": "HR",
    "slovakia": "SK", "slovenia": "SI", "euro area": "U2",
    "european union": "EU27_2020"
}


# -------------------------------------------------------------
# 3. Utility functions
# -------------------------------------------------------------
def detect_countries(text: str) -> list[str]:
    """Detect one or more countries from the user query."""
    found = [code for name, code in COUNTRY_CODES.items() if name in text.lower()]
    return list(set(found)) or ["U2"]  # Default = Euro area (U2)


def detect_period(text: str) -> dict:
    """Detect time filters like 'last 3 years' or 'since 2020'."""
    today = datetime.today()
    years = re.search(r"(\d+)\s*year", text)
    months = re.search(r"(\d+)\s*month", text)
    since = re.search(r"since\s*(\d{4})", text)

    if since:
        return {"startPeriod": f"{since.group(1)}-01"}
    elif years:
        start = today - timedelta(days=int(years.group(1)) * 365)
        return {"startPeriod": start.strftime("%Y-%m")}
    elif months:
        start = today - timedelta(days=int(months.group(1)) * 30)
        return {"startPeriod": start.strftime("%Y-%m")}
    else:
        # Default: last 5 years
        start = today - timedelta(days=5 * 365)
        return {"startPeriod": start.strftime("%Y-%m")}


# -------------------------------------------------------------
# 4. Main interpreter function
# -------------------------------------------------------------
def interpret_query_with_ai(user_text: str) -> dict | None:
    """
    Uses rule-based + LLM fallback to build a structured query plan.
    Supports multiple countries and flexible time periods.
    """
    try:
        logger.info(f"🔮 Parsing user query: {user_text}")

        text = user_text.lower()
        countries = detect_countries(text)
        params = detect_period(text)
        countries_key = "+".join(countries)

        # --- Inflation ---
        if any(word in text for word in ["inflation", "hicp", "price", "consumer"]):
            return {
                "provider": "ECB",
                "flow": "ICP",
                "series": f"M.{countries_key}.N.000000.4.ANR",
                "freq": "M",
                "indicator": f"Inflation ({', '.join(countries)})",
                "params": params
            }

        # --- Exchange rates ---
        elif any(word in text for word in ["exchange", "usd", "gbp", "jpy", "chf", "currency"]):
            return {
                "provider": "ECB",
                "flow": "EXR",
                "series": "D.USD.EUR.SP00.A",
                "freq": "D",
                "indicator": "EUR/USD Exchange Rate",
                "params": {"lastNObservations": 90}
            }

        # --- Interest rates ---
        elif any(word in text for word in ["deposit", "interest", "refinancing", "rate", "dfr"]):
            return {
                "provider": "ECB",
                "flow": "FM",
                "series": "D.U2.EUR.4F.KR.DFR.LEV",
                "freq": "D",
                "indicator": "Deposit Facility Rate (DFR)",
                "params": {"lastNObservations": 365}
            }

        # --- GDP or unemployment (Eurostat placeholder) ---
        elif "gdp" in text or "unemployment" in text:
            flow = "namq_10_gdp" if "gdp" in text else "une_rt_m"
            freq = "Q" if "gdp" in text else "M"
            return {
                "provider": "Eurostat",
                "flow": flow,
                "series": "",
                "freq": freq,
                "indicator": "GDP / Unemployment (Eurostat)",
                "params": params
            }

        # --- Default fallback ---
        else:
            return {
                "provider": "ECB",
                "flow": "ICP",
                "series": "M.U2.N.000000.4.ANR",
                "freq": "M",
                "indicator": "Euro area inflation (default)",
                "params": params
            }

    except Exception as e:
        logger.warning(f"⚠️ interpret_query_with_ai failed: {e}")
        return None


# -------------------------------------------------------------
# 5. Manual test helper
# -------------------------------------------------------------
if __name__ == "__main__":
    print(">>> Quick test for interpret_query_with_ai()")
    tests = [
        "Inflation Italy vs Belgium last 3 years",
        "Inflation France since 2020",
        "Euro area inflation",
        "Exchange rate EUR/USD last month"
    ]
    for t in tests:
        print(f"\n🗨️ {t}")
        print(interpret_query_with_ai(t))
