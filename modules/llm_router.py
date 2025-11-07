"""
=============================================================
Module: llm_router.py
=============================================================

🧠 Purpose:
    Interpret user messages in natural language and convert them
    into a structured "query plan" for the data fetchers (ECB / Eurostat).

⚙️ Logic:
    1️⃣ Try to interpret the text using the AI parser (OpenAI GPT)
       → modules.ai_parser.interpret_query_with_ai()
    2️⃣ If AI parsing fails, fall back to a static dictionary
    3️⃣ Return a unified dictionary:
        {
          provider, flow, series, freq, indicator, params?
        }

🗣️ Expected input language: English
=============================================================
"""

import logging
from modules.ai_parser import interpret_query_with_ai

# -------------------------------------------------------------
# 1. Logging setup
# -------------------------------------------------------------
logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# 2. Minimal static fallback dictionary
# -------------------------------------------------------------
FALLBACK_PATTERNS = {
    "inflation": {
        "provider": "ECB",
        "flow": "ICP",
        "series": "M.U2.N.000000.4.ANR",
        "freq": "M",
        "indicator": "Euro area inflation (HICP)",
        "params": {"lastNObservations": 24},
    },
    "deposit": {
        "provider": "ECB",
        "flow": "FM",
        "series": "D.U2.EUR.4F.KR.DFR.LEV",
        "freq": "D",
        "indicator": "Deposit Facility Rate (DFR)",
        "params": {"lastNObservations": 180},
    },
    "exchange": {
        "provider": "ECB",
        "flow": "EXR",
        "series": "D.USD.EUR.SP00.A",
        "freq": "D",
        "indicator": "EUR/USD Exchange Rate",
        "params": {"lastNObservations": 60},
    },
    "gdp": {
        "provider": "Eurostat",
        "flow": "namq_10_gdp",
        "freq": "Q",
        "indicator": "Gross Domestic Product (Eurostat)",
        "params": {"startPeriod": "2020"},
    },
    "unemployment": {
        "provider": "Eurostat",
        "flow": "une_rt_m",
        "freq": "M",
        "indicator": "Unemployment rate (Eurostat)",
        "params": {"startPeriod": "2020"},
    },
}

# -------------------------------------------------------------
# 3. Main function
# -------------------------------------------------------------
def parse_message_to_query(text: str) -> dict:
    """
    Parse a natural language query into a structured 'query plan'.

    Steps:
    1. Try with AI parser (GPT)
    2. If it fails, use static keyword fallback
    3. Return a valid dict ready for fetchers
    """
    if not text:
        logger.warning("⚠️ Empty input text received.")
        return {
            "provider": "ECB",
            "flow": "ICP",
            "series": "M.U2.N.000000.4.ANR",
            "freq": "M",
            "indicator": "Euro area inflation (default)",
            "params": {"lastNObservations": 12},
        }

    text_lower = text.lower().strip()
    logger.info(f"🧠 Parsing message: '{text_lower}'")

    # --- 1️⃣ AI-based parsing ---
    ai_result = interpret_query_with_ai(text)
    if ai_result:
        logger.info("✅ AI interpretation successful (via ai_parser)")
        # Normalize optional fields
        ai_result.setdefault("params", {"lastNObservations": 24})
        ai_result.setdefault("series", "")
        return ai_result

    # --- 2️⃣ Static fallback ---
    for keyword, plan in FALLBACK_PATTERNS.items():
        if keyword in text_lower:
            logger.info(f"✅ Fallback match found for '{keyword}': {plan}")
            return plan

    # --- 3️⃣ Default response ---
    fallback = {
        "provider": "ECB",
        "flow": "ICP",
        "series": "M.U2.N.000000.4.ANR",
        "freq": "M",
        "indicator": "Euro area inflation (default)",
        "params": {"lastNObservations": 12},
    }
    logger.warning(f"⚠️ No match found. Using fallback: {fallback}")
    return fallback


# -------------------------------------------------------------
# 4. Test helper (optional)
# -------------------------------------------------------------
if __name__ == "__main__":
    print(">>> Quick test for parse_message_to_query()")
    samples = [
        "Show me the euro area inflation since 2020",
        "EUR/USD exchange rate last month",
        "What is the ECB deposit rate?",
        "Euro area GDP quarterly data",
        "Unemployment in Italy",
    ]
    for q in samples:
        print(f"\n🗨️ Query: {q}")
        result = parse_message_to_query(q)
        print(result)
