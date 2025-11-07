import os
from dotenv import load_dotenv

# Carica variabili da file .env
load_dotenv()

# --- API Keys ---
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# --- Config generale ---
LOG_LEVEL = "INFO"
CACHE_PATH = "data/cache.db"

# --- Fonti dati ---
ECB_BASE_URL = "https://data.ecb.europa.eu/service/data"
EUROSTAT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data"
