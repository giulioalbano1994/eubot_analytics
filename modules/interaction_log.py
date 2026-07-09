"""Append-only interaction log. One row per user query → data/interactions.csv
Opens directly in Excel. CSV (not .xlsx) so each call appends one line instead of
rewriting a workbook, and needs no extra dependency.
# ponytail: CSV append; switch to openpyxl only if real .xlsx formatting is needed.
"""
import csv
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

LOG_FILE = Path("data/interactions.csv")
_FIELDS = ["timestamp", "user_id", "query", "provider", "indicator",
           "n_obs", "status", "error"]


def log_interaction(**row) -> None:
    """Append one interaction. Never raises — logging must not break the bot."""
    try:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        new = not LOG_FILE.exists()
        record = {k: row.get(k, "") for k in _FIELDS}
        record["timestamp"] = datetime.now().isoformat(timespec="seconds")
        with LOG_FILE.open("a", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=_FIELDS)
            if new:
                w.writeheader()
            w.writerow(record)
    except Exception as e:
        logger.warning(f"interaction log failed: {e}")
