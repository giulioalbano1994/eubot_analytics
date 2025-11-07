"""
=============================================================
Module: ai_parser.py
=============================================================

🧠 Purpose:
    Convert natural language queries (in English)
    into a structured "query plan" for ECB or Eurostat datasets.

💡 How it works:
    1. Builds a detailed system prompt for the OpenAI model.
    2. Sends the user text to GPT (default: gpt-4o-mini).
    3. Attempts to parse the model’s reply into valid JSON.
    4. Returns a dictionary ready for the fetcher modules.

⚙️ Dependencies:
    - openai
    - json
    - os
    - logging
    - dotenv
=============================================================
"""

import os
import json
import logging
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()  # ✅ Load environment variables

# -------------------------------------------------------------
# 1. Configuration
# -------------------------------------------------------------
logger = logging.getLogger(__name__)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
if not OPENAI_API_KEY:
    logger.warning("⚠️ OPENAI_API_KEY not found in environment variables.")
client = OpenAI(api_key=OPENAI_API_KEY)

# -------------------------------------------------------------
# 2. System prompt (core logic)
# -------------------------------------------------------------
AI_PROMPT = """
You are a data assistant specialized in European statistics (ECB + Eurostat).
Your job is to convert a user's natural language question (in English)
into a valid JSON query plan for the corresponding statistical API.

Always respond with **valid JSON only** — no explanations, no text around it.

Each JSON must contain:
{
  "provider": "ECB" or "Eurostat",
  "flow": "<dataset id>",
  "series": "<complete SDMX series key if ECB, or leave empty for Eurostat>",
  "freq": "<M|Q|D>",
  "indicator": "<readable name>",
  "params": { optional SDMX filters like startPeriod/endPeriod/lastNObservations }
}

Rules:

### Exchange rates (ECB - EXR)
- Keywords: "exchange rate", "USD", "GBP", "JPY", "CHF", "currency", "Euro Dollar", "Euro Yen"
- provider = "ECB"
- flow = "EXR"
- freq = "D"
- Default series examples:
    * EUR/USD → "D.USD.EUR.SP00.A"
    * EUR/GBP → "D.GBP.EUR.SP00.A"
    * EUR/JPY → "D.JPY.EUR.SP00.A"

### Inflation (ECB - ICP)
- Keywords: "inflation", "HICP", "prices", "consumer index"
- provider = "ECB"
- flow = "ICP"
- series = "M.U2.N.000000.4.ANR"
- freq = "M"
- indicator = "Euro area inflation rate (HICP)"

### Interest and deposit rates (ECB - FM)
- Keywords: "deposit rate", "interest rate", "main refinancing rate", "DFR"
- provider = "ECB"
- flow = "FM"
- freq = "D"
- series examples:
    * "deposit facility" → "D.U2.EUR.4F.KR.DFR.LEV"
    * "main refinancing" → "D.U2.EUR.4F.KR.MRR_FR.LEV"
    * "marginal lending" → "D.U2.EUR.4F.KR.MLR.LEV"

### Money supply (ECB - BSI)
- Keywords: "money supply", "M3", "liquidity"
- provider = "ECB"
- flow = "BSI"
- series = "M.U2.N.A.A20.A1.A.M"
- freq = "M"
- indicator = "Broad money (M3) - Euro area"

### Eurostat (macroeconomic indicators)
- Keywords: "GDP", "unemployment", "employment", "population"
- provider = "Eurostat"
- flow examples:
    * "GDP" → flow="namq_10_gdp", freq="Q"
    * "unemployment" → flow="une_rt_m", freq="M"
    * "population" → flow="demo_pjan", freq="Y"

### Countries
- Map countries when mentioned:
  Italy → IT, Germany → DE, France → FR, Spain → ES, Euro area → EA, European Union → EU27_2020.

### Time filters
- If the question mentions:
    * "since 2020" → params.startPeriod="2020-01"
    * "last 2 years" → params.startPeriod=(today minus 2 years)
    * "last month" → params.lastNObservations=30
    * "last year" → params.lastNObservations=365
- Dates should use ISO format (YYYY-MM or YYYY-MM-DD).

If unsure, guess the most likely series but keep JSON valid.
"""

# -------------------------------------------------------------
# 3. Main function: interpret query
# -------------------------------------------------------------
def interpret_query_with_ai(user_text: str) -> dict | None:
    """
    Sends user text to the LLM and tries to return a structured JSON plan.
    """
    try:
        logger.info(f"🔮 LLM parsing for: {user_text}")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": AI_PROMPT},
                {"role": "user", "content": user_text.strip()}
            ],
            temperature=0.2,
            max_tokens=300,
        )

        content = response.choices[0].message.content.strip()
        logger.info(f"💬 Raw LLM response: {content}")

        # Try parsing JSON
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("Response is not a JSON object.")

        # Validate required keys
        required = ["provider", "flow", "freq", "indicator"]
        for key in required:
            if key not in parsed:
                raise ValueError(f"Missing field: {key}")

        logger.info("✅ AI parsing successful.")
        return parsed

    except Exception as e:
        logger.warning(f"⚠️ LLM interpretation failed: {e}")
        return None


# -------------------------------------------------------------
# 4. Manual test helper
# -------------------------------------------------------------
if __name__ == "__main__":
    print(">>> Quick test for interpret_query_with_ai()")
    test_input = "Show me the EUR/USD exchange rate for the last month"
    result = interpret_query_with_ai(test_input)
    print(result)
