import matplotlib.pyplot as plt
import io

def plot_timeseries(df, title="Time Series", y_label="Value"):
    """
    Generates a simple line chart from a dataframe with columns:
    ['TIME_PERIOD', 'OBS_VALUE'] or similar.
    """
    fig, ax = plt.subplots(figsize=(8, 4))

    # Ensure correct columns
    if "TIME_PERIOD" in df.columns and "OBS_VALUE" in df.columns:
        df = df.set_index("TIME_PERIOD")["OBS_VALUE"]

    df.plot(ax=ax, legend=False, linewidth=2, color="tab:blue")
    ax.set_title(title, fontsize=13)
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.set_xlabel("Date")
    ax.set_ylabel(y_label)
    plt.tight_layout()

    buffer = io.BytesIO()
    plt.savefig(buffer, format="png", dpi=150)
    buffer.seek(0)
    plt.close(fig)
    return buffer
