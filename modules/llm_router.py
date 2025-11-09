# ==============================================================
# Module: llm_router.py  (v6 – Unified Intelligent Router)
# ==============================================================
# 🧠 Purpose:
#   Bridge between user messages and structured query plans.
#   Now supports ECB + Eurostat indicators (via ai_parser_unified).
#
# ✅ Features:
#   - Detects chart/comparison/growth intent
#   - Handles multi-country and multi-indicator queries
#   - Uses LLM ONLY for intent understanding (not query codes)
# ==============================================================

import logging
import re
from modules.ai_parser import interpret_query_with_ai

logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# Helper: detect chart / comparison / analysis intent
# -------------------------------------------------------------
def detect_chart_mode(text: str) -> str:
    """Detect whether the user wants a comparison or trend."""
    t = text.lower()
    if re.search(r"\b(compare|vs|versus|between|and)\b", t):
        return "compare"
    if re.search(r"\btrend|evolution|change|growth|rise|increase|fall|decline\b", t):
        return "trend"
    return "single"

def detect_analysis_type(text: str) -> str:
    """Detect if the analysis is focused on growth, decline, or neutral."""
    t = text.lower()
    if any(k in t for k in ["growth", "increase", "rise", "expansion", "improve"]):
        return "growth"
    if any(k in t for k in ["decline", "drop", "decrease", "fall", "slowdown", "recession"]):
        return "decline"
    if any(k in t for k in ["compare", "vs", "versus", "difference"]):
        return "comparison"
    return "neutral"

def detect_countries_in_text(text: str):
    """Extract country names if multiple are present."""
    t = text.lower()
    countries = []
    for c in [
        "italy","france","germany","spain","portugal","belgium",
        "netherlands","austria","greece","ireland","finland","luxembourg",
        "poland","sweden","denmark","romania","hungary","slovakia",
        "slovenia","croatia","bulgaria","euro area","eurozone"
    ]:
        if c in t:
            countries.append(c)
    return list(set(countries))

# -------------------------------------------------------------
# Core Router
# -------------------------------------------------------------
def parse_message_to_query(user_text: str):
    """
    Parse a natural-language message into one or more structured query plans.
    Uses ai_parser_unified.interpret_query_with_ai() for typed indicator lookup.
    """
    if not user_text or not user_text.strip():
        return _default_plan()

    text = user_text.strip()
    logger.info(f"🧠 Routing user message: {text}")

    # 1️⃣ Detect general context
    chart_mode = detect_chart_mode(text)
    analysis_type = detect_analysis_type(text)
    countries = detect_countries_in_text(text)
    multi_country = len(countries) > 1

    # 2️⃣ Try to parse via AI Parser (ECB + Eurostat unified)
    try:
        query_plan = interpret_query_with_ai(text)
    except Exception as e:
        logger.warning(f"⚠️ AI parser failed: {e}")
        query_plan = None

    # 3️⃣ Handle returned plan
    if isinstance(query_plan, dict):
        query_plan["chart_mode"] = chart_mode
        query_plan["analysis_type"] = analysis_type
        if multi_country:
            query_plan["compare_mode"] = "multi-country"
            query_plan["countries"] = countries
        logger.info(f"✅ Routed single query → {query_plan.get('indicator', 'Unknown')}")
        return query_plan

    if isinstance(query_plan, list):
        for qp in query_plan:
            qp["chart_mode"] = chart_mode
            qp["analysis_type"] = analysis_type
            if multi_country:
                qp["compare_mode"] = "multi-country"
                qp["countries"] = countries
        logger.info(f"✅ Routed multi-query ({len(query_plan)} indicators)")
        return query_plan

    # 4️⃣ If everything fails → fallback to inflation Euro area
    logger.warning("⚠️ No valid match found. Using default inflation plan.")
    return _default_plan(chart_mode, analysis_type)

# -------------------------------------------------------------
# Default fallback plan
# -------------------------------------------------------------
def _default_plan(chart_mode="single", analysis_type="neutral"):
    return {
        "provider": "ECB",
        "flow": "ICP",
        "series": "M.U2.N.000000.4.ANR",
        "freq": "M",
        "indicator": "Inflation (Euro area, default)",
        "params": {"lastNObservations": 12},
        "chart_mode": chart_mode,
        "analysis_type": analysis_type,
        "compare_mode": "none"
    }

# -------------------------------------------------------------
# Local test
# -------------------------------------------------------------
if __name__ == "__main__":
    examples = [
        "Inflation in Italy and France since 2020",
        "GDP growth Germany vs Spain",
        "Compare unemployment between France and Italy",
        "ECB deposit rate trend after 2022",
        "Show me euro usd exchange rate last 6 months",
        "Poverty rate in Euro area",
        "Employment and inflation Italy since 2015",
    ]
    for q in examples:
        print(f"\n🗨️ {q}")
        print(parse_message_to_query(q))
