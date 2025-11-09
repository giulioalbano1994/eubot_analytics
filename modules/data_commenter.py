# ==============================================================
# Module: data_commenter.py  —  v3 (AI Summarizer + Citation)
# ==============================================================
# 🧠 Purpose:
#   Generate concise, data-driven bullet points for ECB charts.
#   Uses GPT-4o-mini when available, otherwise falls back to
#   deterministic summaries. Includes official ECB data citation.
# ==============================================================

import os
import pandas as pd
from datetime import datetime
from openai import OpenAI

# --------------------------------------------------------------
# 1️⃣ Setup
# --------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

ECB_CITATION = (
    "_Data source: European Central Bank Data Portal — "
    "https://data.ecb.europa.eu/ — licensed under CC BY 4.0._"
)

# --------------------------------------------------------------
# 2️⃣ Utility: compute simple stats
# --------------------------------------------------------------
def _compute_trend_stats(df: pd.DataFrame, country_col="COUNTRY"):
    """Compute latest value, change, and % change for each country."""
    stats = {}
    for c in df[country_col].unique():
        sub = df[df[country_col] == c].dropna(subset=["OBS_VALUE"]).sort_values("TIME_PERIOD")
        if len(sub) < 2:
            continue
        last, prev = sub["OBS_VALUE"].iloc[-1], sub["OBS_VALUE"].iloc[-2]
        change = last - prev
        pct_change = (change / prev * 100) if prev else 0
        stats[c] = {
            "last": round(float(last), 2),
            "prev": round(float(prev), 2),
            "abs_change": round(float(change), 2),
            "pct_change": round(float(pct_change), 2),
            "arrow": "↑" if change > 0 else "↓" if change < 0 else "→",
        }
    return stats

# --------------------------------------------------------------
# 3️⃣ Prompt builder for GPT
# --------------------------------------------------------------
def _build_prompt(stats: dict, indicator: str, lang: str = "en") -> str:
    now = datetime.now().strftime("%B %Y")
    header = (
        f"You are a professional economic analyst for a Telegram bot. "
        f"Summarize the following indicator: '{indicator}'. "
        f"The current month is {now}. Write in {lang}. "
        f"Focus on trends, differences, and anomalies."
    )
    details = ["Latest available data by country:"]
    for c, s in stats.items():
        details.append(f"- {c}: {s['last']:.2f} ({s['arrow']} {s['abs_change']:+.2f}, {s['pct_change']:+.1f}%)")
    guidelines = (
        "Now write exactly 3 short bullet points (≤20 words each). "
        "Be factual and clear, no speculation. Mention changes or comparisons if visible."
    )
    return "\n".join([header, "", *details, "", guidelines])

# --------------------------------------------------------------
# 4️⃣ Language detector (simple heuristic)
# --------------------------------------------------------------
def _detect_language(indicator: str) -> str:
    """Detect if the indicator name suggests Italian output."""
    if any(word in indicator.lower() for word in ["italia", "inflazione", "prodotto interno lordo", "tasso"]):
        return "it"
    return "en"

# --------------------------------------------------------------
# 5️⃣ Main summarizer
# --------------------------------------------------------------
def summarize_trend(df: pd.DataFrame, indicator_name="Indicator", country_col="COUNTRY") -> str:
    """Generate a short smart summary with 3 bullet points and ECB citation."""
    if df.empty or country_col not in df.columns:
        return f"⚠️ No data available.\n\n{ECB_CITATION}"

    stats = _compute_trend_stats(df, country_col)
    if not stats:
        return f"⚠️ Not enough data to summarize.\n\n{ECB_CITATION}"

    lang = _detect_language(indicator_name)

    # --- GPT summary ---
    if client:
        try:
            prompt = _build_prompt(stats, indicator_name, lang=lang)
            response = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": "You are an expert macroeconomic data analyst."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.4,
                max_tokens=200,
            )
            text = response.choices[0].message.content.strip()
            return f"📊 *Quick Summary*\n{text}\n\n{ECB_CITATION}"
        except Exception as e:
            print(f"⚠️ GPT summarization failed: {e}")

    # --- Fallback (manual summary) ---
    lines = ["📊 *Quick Summary*"]
    for c, s in stats.items():
        arrow = "⬆️" if s["abs_change"] > 0 else "⬇️" if s["abs_change"] < 0 else "➡️"
        lines.append(f"• {c}: {s['last']:.1f} ({arrow} {s['abs_change']:+.1f}, {s['pct_change']:+.1f}%)")
    lines.append("• (AI summary unavailable — using numeric fallback)")
    lines.append("")
    lines.append(ECB_CITATION)
    return "\n".join(lines)

# --------------------------------------------------------------
# 6️⃣ Local test
# --------------------------------------------------------------
if __name__ == "__main__":
    import numpy as np
    df = pd.DataFrame({
        "TIME_PERIOD": pd.date_range("2024-01-01", periods=6, freq="M").tolist() * 2,
        "COUNTRY": ["IT"] * 6 + ["FR"] * 6,
        "OBS_VALUE": np.concatenate([np.linspace(100, 120, 6), np.linspace(80, 95, 6)]),
    })
    print(summarize_trend(df, indicator_name="Inflation rate"))
