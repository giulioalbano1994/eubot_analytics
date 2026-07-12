"""
=============================================================
# 📊 MODULE: plotter.py
=============================================================
Purpose:
  Generate publication-quality plots and thematic maps
  for ECB / Eurostat data.

Features:
  ✅ Time series plotting (Telegram optimized)
  ✅ Automatic choropleth maps (Europe)
  ✅ Smart title, legend, and format
=============================================================
"""

import io
import logging
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

logger = logging.getLogger(__name__)

# =============================================================
# 📈 TIME SERIES PLOT
# =============================================================
PALETTE = ["#2563eb", "#dc2626", "#059669", "#d97706",
           "#7c3aed", "#0891b2", "#db2777", "#65a30d"]
INK, MUTE, GRID = "#1f2937", "#6b7280", "#e5e7eb"


def plot_timeseries(df: pd.DataFrame, title: str = "Time Series") -> io.BytesIO:
    """Plot one or more time series (modern, clean) and return a PNG buffer."""
    if df is None or (hasattr(df, "empty") and df.empty):
        raise ValueError("Empty DataFrame received for plotting.")

    if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
        df = df.set_index("TIME_PERIOD")["OBS_VALUE"].to_frame(name="")
    if isinstance(df, pd.Series):
        df = df.to_frame(name="")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")
    df = df.sort_index()

    n, multi = len(df), df.shape[1] > 1
    marker = "o" if n <= 60 else None  # markers only when points are sparse
    fig, ax = plt.subplots(figsize=(11, 6.2), dpi=200)

    for i, col in enumerate(df.columns):
        c = PALETTE[i % len(PALETTE)]
        ax.plot(df.index, df[col], color=c, linewidth=2.4, label=str(col),
                marker=marker, markersize=5, markerfacecolor="white",
                markeredgecolor=c, markeredgewidth=1.5)

    fig.suptitle(title, fontsize=16, fontweight="bold", color=INK, x=0.04, ha="left", y=0.98)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.tick_params(colors=MUTE)
    ax.margins(x=0.02)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    if multi:
        ax.legend(frameon=False, ncol=min(df.shape[1], 4), loc="best", fontsize=10)
    fig.text(0.98, 0.01, "Source: ECB · Eurostat", ha="right", fontsize=8, color="#b0b4bb")

    buf = io.BytesIO()
    fig.tight_layout(rect=(0, 0.02, 1, 0.95))
    fig.savefig(buf, format="png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    logger.info("✅ Time series chart generated successfully.")
    return buf


# =============================================================
# 🗺️ CHOROPLETH MAP PLOT
# =============================================================
def plot_map(df: pd.DataFrame, indicator: str = "Indicator") -> io.BytesIO:
    """
    Plot a choropleth map of Europe using ISO country codes.
    Requires geopandas (no external shapefiles needed).
    """
    import geopandas as gpd

    if df.empty or "COUNTRY" not in df.columns or "OBS_VALUE" not in df.columns:
        raise ValueError("DataFrame must contain COUNTRY and OBS_VALUE columns.")

    # Take the last available observation for each country
    df_latest = (
        df.sort_values("date")
        .groupby("COUNTRY", as_index=False)
        .last()[["COUNTRY", "OBS_VALUE"]]
    )

    # Load built-in world geometry
    world = gpd.read_file(gpd.datasets.get_path("naturalearth_lowres"))

    # Filter Europe
    europe = world[world["continent"] == "Europe"].copy()
    europe["iso_a2"] = europe["iso_a2"].replace({"GB": "UK"})  # unify naming if needed

    # Merge with data
    merged = europe.merge(df_latest, left_on="iso_a2", right_on="COUNTRY", how="left")

    # Initialize figure
    fig, ax = plt.subplots(figsize=(10, 8))
    merged.plot(
        column="OBS_VALUE",
        cmap="viridis",
        linewidth=0.8,
        ax=ax,
        edgecolor="0.6",
        legend=True,
        missing_kwds={
            "color": "lightgrey",
            "label": "No data",
            "edgecolor": "white",
        },
    )

    ax.set_title(f"{indicator} — Latest available value", fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=200)
    plt.close(fig)
    buf.seek(0)

    logger.info("✅ Map chart generated successfully.")
    return buf


# =============================================================
# 🧪 LOCAL TEST
# =============================================================
if __name__ == "__main__":
    import numpy as np

    # Dummy data for testing
    countries = ["IT", "FR", "DE", "ES", "PT", "BE", "NL"]
    df = pd.DataFrame({
        "COUNTRY": countries,
        "OBS_VALUE": np.random.uniform(80, 120, size=len(countries)),
        "date": pd.Timestamp("2025-01-01")
    })

    # Time series test
    # (This block uses a simple 2-country mock series)
    import datetime as dt
    dates = pd.date_range("2023-01-01", periods=12, freq="M")
    ts = pd.DataFrame({
        "TIME_PERIOD": dates,
        "OBS_VALUE": np.random.rand(12) * 10,
        "COUNTRY": ["IT"] * 12
    })
    ts2 = ts.copy()
    ts2["OBS_VALUE"] = np.random.rand(12) * 15
    ts2["COUNTRY"] = "FR"
    data = pd.concat([ts, ts2])
    pivot = data.pivot_table(index="TIME_PERIOD", columns="COUNTRY", values="OBS_VALUE")

    buf1 = plot_timeseries(pivot, title="Inflation (YoY %)")
    with open("test_chart.png", "wb") as f:
        f.write(buf1.getvalue())

    buf2 = plot_map(df, "GDP per capita (2024)")
    with open("test_map.png", "wb") as f:
        f.write(buf2.getvalue())

    print("✅ Saved test_chart.png and test_map.png")
