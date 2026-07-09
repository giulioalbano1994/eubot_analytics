import logging
import sys

# Windows consoles default to cp1252, which crashes on emoji in print/log.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8")
    except Exception:
        pass

from modules.telegram_bot import start_bot

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    logging.info("🚀 Avvio ECB-Eurostat DataBot...")

    try:
        start_bot()
    except Exception as e:
        logging.exception(f"❌ Errore critico durante l'avvio del bot: {e}")
