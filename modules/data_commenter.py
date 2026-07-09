# ==============================================================
# Module: data_commenter.py  —  v3 (AI Summarizer + Citation)
# ==============================================================
# 🧠 Purpose:
#   Generate concise, data-driven bullet points for ECB charts.
#   Uses GPT-4o-mini when available, otherwise falls back to
#   deterministic summaries. Includes official ECB data citation.
# ==============================================================

import os
import logging
import pandas as pd
from datetime import datetime
from openai import OpenAI

logger = logging.getLogger(__name__)

# --------------------------------------------------------------
# 1️⃣ Setup
# --------------------------------------------------------------
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None

ECB_CITATION = (
    "_Data source: European Central Bank Data Portal — "
    "https://data.ecb.europa.eu/ — licensed under CC BY 4.0._"
)
EUROSTAT_CITATION = (
    "_Data source: Eurostat — https://ec.europa.eu/eurostat — "
    "licensed under CC BY 4.0._"
)

def _citation(provider: str) -> str:
    return EUROSTAT_CITATION if provider == "Eurostat" else ECB_CITATION

# --------------------------------------------------------------
# 2️⃣ Utility: compute simple stats
# --------------------------------------------------------------
def _compute_trend_stats(df: pd.DataFrame, country_col="COUNTRY"):
    """Rich per-country stats over the whole window: latest, change since start,
    latest step, min/max (with dates), and mean."""
    stats = {}
    for c in df[country_col].unique():
        sub = df[df[country_col] == c].dropna(subset=["OBS_VALUE"]).sort_values("TIME_PERIOD")
        if len(sub) < 2:
            continue
        v = sub["OBS_VALUE"].astype(float)
        t = sub["TIME_PERIOD"]
        first, last, prev = v.iloc[0], v.iloc[-1], v.iloc[-2]
        imax, imin = v.idxmax(), v.idxmin()
        step = last - prev
        stats[c] = {
            "first": round(first, 2),
            "last": round(last, 2),
            "abs_change": round(step, 2),                       # latest step
            "pct_change": round((step / prev * 100) if prev else 0, 2),
            "window_change": round(last - first, 2),            # start → now
            "window_pct": round((last - first) / first * 100 if first else 0, 2),
            "min": round(v.min(), 2), "min_date": t[imin].strftime("%Y-%m"),
            "max": round(v.max(), 2), "max_date": t[imax].strftime("%Y-%m"),
            "mean": round(v.mean(), 2),
            "start_date": t.iloc[0].strftime("%Y-%m"),
            "end_date": t.iloc[-1].strftime("%Y-%m"),
            "n": len(sub),
            "arrow": "↑" if step > 0 else "↓" if step < 0 else "→",
        }
    return stats

# --------------------------------------------------------------
# 3️⃣ Prompt builder for GPT
# --------------------------------------------------------------
def _build_prompt(stats: dict, indicator: str, lang: str = "en") -> str:
    now = datetime.now().strftime("%B %Y")
    multi = len(stats) > 1
    header = (
        f"You are a sharp macroeconomic analyst writing for a Telegram audience — "
        f"smart but not academic. Indicator: '{indicator}'. Today: {now}. "
        f"Write in {lang.upper()}. Ground every claim in the numbers below; never invent figures."
    )
    details = ["Data by country (over the charted window):"]
    for c, s in stats.items():
        details.append(
            f"- {c}: latest {s['last']} ({s['end_date']}); started {s['first']} ({s['start_date']}); "
            f"window change {s['window_change']:+} ({s['window_pct']:+.1f}%); "
            f"min {s['min']} ({s['min_date']}), max {s['max']} ({s['max_date']}), avg {s['mean']}; "
            f"latest step {s['abs_change']:+} ({s['pct_change']:+.1f}%)."
        )
    guidelines = (
        "Write:\n"
        "1) A bold one-line headline with ONE relevant emoji capturing the main story.\n"
        f"2) {'3 insights comparing the countries — who is higher/lower, diverging or converging, gap size.' if multi else '3 insights — direction over the window, the peak/trough and when, and the latest move.'}\n"
        "3) A final one-line 'Bottom line:' takeaway.\n"
        "Each line ≤22 words, punchy, concrete, no filler, no disclaimers. Use • for the 3 insights."
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
def summarize_trend(df: pd.DataFrame, indicator_name="Indicator", country_col="COUNTRY",
                    provider="ECB") -> str:
    """Generate a short smart summary with headline + insights and a source citation."""
    citation = _citation(provider)
    if df.empty or country_col not in df.columns:
        return f"⚠️ No data available.\n\n{citation}"

    stats = _compute_trend_stats(df, country_col)
    if not stats:
        return f"⚠️ Not enough data to summarize.\n\n{citation}"

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
                temperature=0.5,
                max_tokens=260,
            )
            text = response.choices[0].message.content.strip()
            return f"{text}\n\n{citation}"
        except Exception as e:
            logger.warning(f"GPT summarization failed: {e}")

    # --- Fallback (deterministic, but with a story) ---
    def _emoji(x):
        return "⬆️" if x > 0 else "⬇️" if x < 0 else "➡️"

    lines = []
    if len(stats) > 1:  # comparison
        hi = max(stats.items(), key=lambda kv: kv[1]["last"])
        lo = min(stats.items(), key=lambda kv: kv[1]["last"])
        gap = hi[1]["last"] - lo[1]["last"]
        lines.append(f"📊 *{indicator_name}*")
        lines.append(f"🥇 Highest now: *{hi[0]}* {hi[1]['last']} · lowest *{lo[0]}* {lo[1]['last']} (gap {gap:+.1f})")
    else:
        c, s = next(iter(stats.items()))
        trend = "rising 📈" if s["window_change"] > 0 else "falling 📉" if s["window_change"] < 0 else "flat ➡️"
        lines.append(f"📊 *{indicator_name}* — {trend} since {s['start_date']}")
    for c, s in stats.items():
        lines.append(
            f"• *{c}*: {s['last']} {_emoji(s['abs_change'])} "
            f"({s['window_pct']:+.1f}% since {s['start_date']}) · range {s['min']}–{s['max']}"
        )
    lines.append("")
    lines.append(citation)
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
