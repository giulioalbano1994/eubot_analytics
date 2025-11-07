"""
=============================================================
# 📊 MODULE: plotter.py
=============================================================

# Purpose:
#   Generate publication-quality time series plots for ECB / Eurostat data.
#
# Features:
#   ✅ Handles single or multiple countries (pivoted DataFrame)
#   ✅ Smart title, legend, and date formatting
#   ✅ Optimized for Telegram (1200x800 PNG, readable fonts)
#
# Dependencies:
#   matplotlib, pandas, io
=============================================================
"""

import io
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import logging

logger = logging.getLogger(__name__)

# -------------------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------------------
def plot_timeseries(df: pd.DataFrame, title: str = "Time Series") -> io.BytesIO:
    """
    Plot one or more time series and return a BytesIO PNG buffer.

    Args:
        df (pd.DataFrame): can be:
            - long format (TIME_PERIOD, OBS_VALUE)
            - wide format (TIME_PERIOD as index, columns = countries)
        title (str): chart title

    Returns:
        io.BytesIO: PNG buffer ready for Telegram send_photo()
    """

    # --- Clean input
    if df.empty:
        raise ValueError("Empty DataFrame received for plotting.")

    # --- Convert to proper format if needed
    if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
        df = df.set_index("TIME_PERIOD")["OBS_VALUE"].to_frame()

    # --- Auto-detect multi-country pivot
    if isinstance(df, pd.Series):
        df = df.to_frame(name="Value")

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [str(c) for c in df.columns]

    # --- Ensure datetime index
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.sort_index()

    # --- Initialize figure
    fig, ax = plt.subplots(figsize=(12, 8))
    plt.style.use("default")

    # --- Plot each series
    for col in df.columns:
        ax.plot(df.index, df[col], label=str(col), linewidth=2)

    # --- Title, grid, legend
    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Time", fontsize=13)
    ax.set_ylabel("Value", fontsize=13)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Country", fontsize=11, loc="best")

    # --- Date formatting
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45, ha="right")

    # --- Tight layout for Telegram display
    plt.tight_layout()

    # --- Save to BytesIO
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=200)
    plt.close(fig)
    buf.seek(0)
    logger.info("✅ Chart generated successfully.")
    return buf


# -------------------------------------------------------------
# LOCAL TEST
# -------------------------------------------------------------
if __name__ == "__main__":
    import numpy as np
    import datetime as dt

    # Example: two countries, mock data
    dates = pd.date_range("2020-01-01", periods=12, freq="M")
    df = pd.DataFrame({
        "TIME_PERIOD": dates,
        "OBS_VALUE": np.random.rand(12) * 10,
        "COUNTRY": ["IT"] * 12
    })
    df2 = df.copy()
    df2["OBS_VALUE"] = np.random.rand(12) * 15
    df2["COUNTRY"] = "BE"
    data = pd.concat([df, df2])

    pivot = data.pivot_table(index="TIME_PERIOD", columns="COUNTRY", values="OBS_VALUE")

    buf = plot_timeseries(pivot, title="Inflation (YoY %)")
    with open("test_chart.png", "wb") as f:
        f.write(buf.getvalue())
    print("✅ Chart saved as test_chart.png")
