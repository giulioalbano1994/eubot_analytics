# ==============================================================
# Module: telegram_bot.py — v10 (ECB core + Eurostat ready)
# ==============================================================
# ✨ 3 sezioni menu (Econ / Fin / FX)
# ✨ Tutti gli esempi ECB testati e funzionanti
# ✨ “Poverty rate” ed Eurostat gestiti con messaggio “coming soon”
# ✨ Compatibile con ai_parser, llm_router e data_commenter
# ✨ Output: grafico + commento narrativo GPT o fallback
# ==============================================================
import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from config.settings import TELEGRAM_TOKEN
from modules.llm_router import parse_message_to_query
from modules.ai_parser import interpret_query_with_ai
from modules.fetchers.ebc_adapter import fetch_ecb_data
from modules.plotter import plot_timeseries, plot_map
from modules.data_commenter import summarize_trend
from modules.fetchers.eurostat_adapter import fetch_eurostat_data


# --------------------------------------------------------------
# Setup
# --------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
LAST_DATASETS = {}

# --------------------------------------------------------------
# Main Menus
# --------------------------------------------------------------
def menu_root() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Economic Indicators", callback_data="menu:econ")],
            [InlineKeyboardButton(text="💰 Monetary & Financial", callback_data="menu:fin")],
            [InlineKeyboardButton(text="💱 Exchange Rates", callback_data="menu:fx")],
        ]
    )

def menu_econ() -> InlineKeyboardMarkup:
    pairs = [
        ("📈 Real GDP — Euro Area", "ex: GDP Euro area since 2015"),
        ("💵 GDP per capita (PPS) — Euro Area", "ex: GDP per capita Euro area since 2015"),
        ("📉 Inflation (HICP YoY) — Euro Area", "ex: Inflation Euro area since 2020"),
        ("👥 Unemployment Rate — Euro Area", "ex: Unemployment Euro area since 2018"),
        ("👔 Employment Rate — Euro Area", "ex: Employment rate Euro area since 2018"),
        ("💸 Public Debt (% GDP) — Euro Area", "ex: Public debt Euro area since 2015"),
        ("🏭 Industrial Production — Euro Area", "ex: Industrial production Euro area since 2018"),
        ("🚧 Poverty Rate (Eurostat) — Euro Area", "ex: Poverty rate Euro area"),
    ]
    rows = [[InlineKeyboardButton(text=lbl, callback_data=qd)] for lbl, qd in pairs]
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="menu:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def menu_fin() -> InlineKeyboardMarkup:
    pairs = [
        ("🏦 Deposit Facility Rate (DFR)", "ex: ECB deposit rate"),
        ("🏛 Main Refinancing Operations – Fixed Rate", "ex: Main refinancing operations ECB"),
        ("🏠 Cost of Borrowing for Households (House Purchase)", "ex: Cost of borrowing euro area"),
        ("📈 Yield Curve – 10Y AAA Government Bond", "ex: 10-year bond yield euro area"),
        ("💵 Money Supply (M3)", "ex: Money supply euro area"),
        ("💳 Loans to Households", "ex: Loans to households euro area"),
    ]
    rows = [[InlineKeyboardButton(text=lbl, callback_data=qd)] for lbl, qd in pairs]
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="menu:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def menu_fx() -> InlineKeyboardMarkup:
    pairs = [
        ("🇪🇺🇺🇸 EUR/USD", "ex: Exchange rate euro dollar"),
        ("🇪🇺🇬🇧 EUR/GBP", "ex: Exchange rate euro pound"),
        ("🇪🇺🇯🇵 EUR/JPY", "ex: Exchange rate euro yen"),
        ("🇪🇺🇨🇭 EUR/CHF", "ex: Exchange rate euro franc"),
        ("🇪🇺🇵🇱 EUR/PLN", "ex: Exchange rate euro zloty"),
        ("🇪🇺🇹🇷 EUR/TRY", "ex: Exchange rate euro lira"),
    ]
    rows = [[InlineKeyboardButton(text=lbl, callback_data=qd)] for lbl, qd in pairs]
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="menu:root")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# --------------------------------------------------------------
# Handlers for /start and /help (registered first)
# --------------------------------------------------------------
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    kb = types.ReplyKeyboardMarkup(
        resize_keyboard=True,
        keyboard=[
            [types.KeyboardButton(text="ℹ️ Info"), types.KeyboardButton(text="🚀 Start")]
        ],
    )
    await message.answer(
        "👋 *Welcome to EU Analytics Bot!*\n\n"
        "Developed by *Giulio Albano* — University of Bari (UNIBA).\n"
        "PhD in Economics and Finance of Public Administrations.\n\n"
        "📊 Connects to the *ECB Data Portal* (and soon *Eurostat*).\n\n"
        "Examples:\n"
        "• `Euro area GDP since 2015`\n"
        "• `Inflation in the Euro area`\n"
        "• `EUR/USD exchange rate`\n"
        "• `Loans to households`\n\n"
        "Or use the menu below ⬇️",
        parse_mode="Markdown",
        reply_markup=kb,
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message):
    await message.answer(
        "🤝 *How it works*\n"
        "Write a natural language query in English or Italian — for example:\n"
        "`Inflation Euro area since 2020` or `Italy vs Germany GDP since 2015`\n\n"
        "I will:\n"
        "1️⃣ Understand your request (LLM + rules)\n"
        "2️⃣ Fetch data from the ECB or Eurostat portal\n"
        "3️⃣ Draw a chart and provide an economic commentary\n\n"
        "Data source: ECB Data Portal — CC BY 4.0",
        parse_mode="Markdown",
    )

@dp.message(F.text == "ℹ️ Info")
async def info_message(message: types.Message):
    await message.answer(
        "👨🏫 *About*\n"
        "Author: *Giulio Albano* — University of Bari (UNIBA)\n"
        "_PhD in Economics and Finance of Public Administrations_\n\n"
        "📚 Data sources:\n"
        "• European Central Bank (ECB) Data Portal — CC BY 4.0\n"
        "• Eurostat (coming soon)\n"
        "🔗 https://data.ecb.europa.eu\n"
        "🔗 https://ec.europa.eu/eurostat",
        parse_mode="Markdown",
    )

@dp.message(F.text.in_(["🚀 Start"]))
async def start_menu(message: types.Message):
    await message.answer("Choose an indicator category:", reply_markup=menu_root())


# --------------------------------------------------------------
# CALLBACKS menu
# --------------------------------------------------------------
@dp.callback_query(F.data == "menu:root")
async def cb_root(callback: types.CallbackQuery):
    await callback.message.edit_text("Scegli una categoria:", reply_markup=menu_root())
    await callback.answer()

@dp.callback_query(F.data == "menu:econ")
async def cb_econ(callback: types.CallbackQuery):
    await callback.message.edit_text("📊 *Indicatori Economici*", parse_mode="Markdown", reply_markup=menu_econ())
    await callback.answer()

@dp.callback_query(F.data == "menu:fin")
async def cb_fin(callback: types.CallbackQuery):
    await callback.message.edit_text("💰 *Indicatori Monetari & Finanziari*", parse_mode="Markdown", reply_markup=menu_fin())
    await callback.answer()

@dp.callback_query(F.data == "menu:fx")
async def cb_fx(callback: types.CallbackQuery):
    await callback.message.edit_text("💱 *Tassi di Cambio*", parse_mode="Markdown", reply_markup=menu_fx())
    await callback.answer()

# --------------------------------------------------------------
# CORE: elaborazione delle query
# --------------------------------------------------------------
async def process_text_query(message: types.Message, text: str):
    """Interpreta la query, scarica i dati, disegna grafico e commenta."""
    if text.strip() in {"🚀 Avvia", "ℹ️ Info"}:
        return
    logging.info(f"🧠 Elaborazione: {text}")
    plan = parse_message_to_query(text)
    if isinstance(plan, list):
        await message.answer(f"📊 Ho trovato {len(plan)} indicatori. Elaboro i grafici…")
        for p in plan:
            await _handle_single_query(message, p)
        return
    await _handle_single_query(message, plan)

async def _handle_single_query(message: types.Message, query: dict):
    indicator = query.get("indicator", "Indicatore")
    provider  = query.get("provider", "ECB")
    flow      = query.get("flow")
    series    = query.get("series")
    params    = query.get("params", {"lastNObservations": 24})

    # Caso “POVERTY_RATE_EUROSTAT” non serve più: ora lo prendiamo davvero
    # Pulizia “ex: …”
    if isinstance(series, str) and series.startswith("ex: "):
        text_query = series.replace("ex: ", "").strip()
        return await process_text_query(message, text_query)

    await message.answer(f"📡 Fetch *{indicator}* from {provider}…", parse_mode="Markdown")
    try:
        if provider == "ECB":
            df = fetch_ecb_data(flow, series, params)  # la tua funzione esistente
        else:
            # Eurostat
            dataset = query.get("dataset")
            eparams = query.get("params", {})
            df = fetch_eurostat_data(dataset, eparams)

        if df is None or df.empty:
            await message.answer("⚠️ Nessun dato restituito.", parse_mode="Markdown")
            return

        # Uniformiamo colonne per il plotter esistente:
        # - se il df arriva già con TIME_PERIOD/OBS_VALUE/COUNTRY non facciamo nulla
        # - se arriva come tua pipeline ECB, fai mapping qui sotto se serve
        country_col = "COUNTRY" if "COUNTRY" in df.columns else "COUNTRY"
        time_col    = "TIME_PERIOD" if "TIME_PERIOD" in df.columns else "TIME_PERIOD"
        value_col   = "OBS_VALUE" if "OBS_VALUE" in df.columns else "OBS_VALUE"

        multi_country = country_col in df.columns and df[country_col].nunique() > 1
        single_time   = df[time_col].nunique() == 1

        if multi_country and single_time:
            buf = plot_map(df, indicator)
        else:
            if multi_country:
                pivot = df.pivot_table(index=time_col, columns=country_col, values=value_col).sort_index()
                buf = plot_timeseries(pivot, title=indicator)
                df = pivot.reset_index().melt(id_vars=time_col, var_name=country_col, value_name=value_col)
            else:
                # Se il tuo plotter accetta anche serie “time/value” rinomina:
                ts = df.rename(columns={time_col:"TIME_PERIOD", value_col:"OBS_VALUE"})
                buf = plot_timeseries(ts, title=indicator)

        keyboard = None
        if not (multi_country and single_time) and multi_country:
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🌍 Show map", callback_data=f"show_map:{indicator}")]]
            )
            LAST_DATASETS[indicator] = df

        photo = BufferedInputFile(buf.getvalue(), filename="chart.png")
        await message.answer_photo(
            photo=photo,
            caption=f"{indicator}\n_Fonte: {'ECB Data Portal' if provider=='ECB' else 'Eurostat'} (CC BY 4.0)_",
            parse_mode="Markdown",
            reply_markup=keyboard
        )

        summary = summarize_trend(df, indicator_name=indicator)
        if summary:
            await message.answer(summary, parse_mode="Markdown")

    except Exception as e:
        logging.exception("❌ Errore nel recupero dati:")
        await message.answer(f"❌ Errore durante il recupero dei dati:\n`{e}`", parse_mode="Markdown")

# --------------------------------------------------------------
# CALLBACKS secondarie
# --------------------------------------------------------------
@dp.callback_query(F.data.startswith("show_map:"))
async def cb_show_map(callback: types.CallbackQuery):
    indicator = callback.data.split("show_map:")[1]
    if indicator not in LAST_DATASETS:
        await callback.answer("⚠️ Dataset non disponibile, riprova.")
        return
    df = LAST_DATASETS[indicator]
    try:
        buf = plot_map(df, indicator)
        photo = BufferedInputFile(buf.getvalue(), filename="map.png")
        await callback.message.answer_photo(
            photo=photo,
            caption=f"🌍 {indicator} — ultimi dati per paese\n_Fonte: ECB Data Portal (CC BY 4.0)_",
            parse_mode="Markdown",
        )
        await callback.answer("✅ Mappa generata.")
    except Exception as e:
        logging.exception("Errore nella callback della mappa:")
        await callback.answer(f"❌ Errore: {e}")

@dp.callback_query()
async def cb_examples(callback: types.CallbackQuery):
    data = callback.data
    if data.startswith("menu:"):
        return
    if data.startswith("ex: "):
        await callback.answer()
        query_text = data.replace("ex: ", "").strip()
        await callback.message.answer(f"🧠 Hai selezionato: _{query_text}_", parse_mode="Markdown")
        await process_text_query(callback.message, query_text)
        return
    await callback.answer()

# --------------------------------------------------------------
# Gestione messaggi liberi (esclude comandi e bottoni)
# --------------------------------------------------------------
@dp.message(~CommandStart(), ~Command("help"), ~F.text.in_({"ℹ️ Info", "🚀 Avvia"}))
async def any_text(message: types.Message):
    await process_text_query(message, message.text.strip())

# --------------------------------------------------------------
# Entry point
# --------------------------------------------------------------
async def main():
    logging.info("🤖 EU Analytics Bot è in esecuzione...")
    await dp.start_polling(bot)

def start_bot():
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("🛑 Bot terminato manualmente.")

if __name__ == "__main__":
    start_bot()
