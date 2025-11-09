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
def plot_timeseries(df: pd.DataFrame, title: str = "Time Series") -> io.BytesIO:
    """Plot one or more time series and return a PNG buffer."""
    if df.empty:
        raise ValueError("Empty DataFrame received for plotting.")

    if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
        df = df.set_index("TIME_PERIOD")["OBS_VALUE"].to_frame()

    if isinstance(df, pd.Series):
        df = df.to_frame(name="Value")

    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, errors="coerce")

    df = df.sort_index()

    fig, ax = plt.subplots(figsize=(12, 8))
    plt.style.use("default")

    for col in df.columns:
        ax.plot(df.index, df[col], label=str(col), linewidth=2)

    ax.set_title(title, fontsize=16, fontweight="bold", pad=20)
    ax.set_xlabel("Time", fontsize=13)
    ax.set_ylabel("Value", fontsize=13)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Country", fontsize=10, loc="best")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()

    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=200)
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
