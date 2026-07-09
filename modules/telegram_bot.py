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
import time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command, CommandObject
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from config.settings import TELEGRAM_TOKEN
import pandas as pd
from modules.llm_router import parse_message_to_query
from modules.ai_parser import interpret_query_with_ai, _ecb_geo
from modules.fetchers.ebc_adapter import fetch_ecb_data
from modules.plotter import plot_timeseries
from modules.data_commenter import summarize_trend
from modules.fetchers.eurostat_adapter import fetch_eurostat_data
from modules.interaction_log import log_interaction
from modules.eurostat_search import search as eurostat_search, fetch_auto as eurostat_fetch_auto


# --------------------------------------------------------------
# Setup
# --------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# --------------------------------------------------------------
# Menus — data-driven: 6 sections, each a submenu of indicators.
# Each leaf is (button label, query text). The query text is routed through the
# exact same NL pipeline as a typed message, so menu == typing.
# --------------------------------------------------------------
MENUS = {
    "ecb": ("🏦 ECB — Monetary & Markets", [
        ("📉 Inflation (HICP)", "Inflation Euro area since 2020"),
        ("🏦 Deposit facility rate", "ECB deposit rate"),
        ("🏛 Main refinancing rate", "Main refinancing operations ECB"),
        ("🏠 Cost of borrowing (households)", "Cost of borrowing euro area"),
        ("📈 Yield curve 10Y (AAA)", "Yield curve euro area"),
        ("💵 Money supply (M3)", "Money supply euro area"),
        ("💳 Loans to households", "Loans to households euro area"),
        ("📊 Real GDP", "GDP Euro area since 2015"),
        ("💶 GDP per capita (PPS)", "GDP per capita Euro area"),
        ("⏱ Hours worked", "Hours worked Euro area"),
    ]),
    "eurostat": ("🇪🇺 Eurostat — Economy & Society", [
        ("👷 Unemployment (IT vs FR)", "Unemployment Italy vs France since 2018"),
        ("🧑‍💼 Employment rate", "Employment Euro area since 2015"),
        ("🤝 Poverty / social exclusion", "Poverty rate Italy vs Spain"),
        ("💸 Government debt (% GDP)", "Public debt Euro area since 2015"),
        ("📊 Deficit / surplus (% GDP)", "Government deficit Euro area since 2015"),
        ("🏭 Industrial production", "Industrial production Euro area since 2018"),
        ("🚀 GDP growth (QoQ)", "GDP growth Euro area since 2019"),
        ("🏠 House prices (YoY)", "House prices Euro area since 2018"),
        ("💼 Labour cost index", "Labour cost Euro area since 2018"),
        ("🌍 10Y bond yield (per country)", "Bond yield Italy"),
        ("👥 Population", "Population Italy"),
        ("🎂 Median age", "Median age Italy"),
    ]),
    "fx": ("💱 Exchange Rates (ECB)", [
        ("🇪🇺🇺🇸 EUR/USD", "Exchange rate euro dollar"),
        ("🇪🇺🇬🇧 EUR/GBP", "Exchange rate euro pound"),
        ("🇪🇺🇯🇵 EUR/JPY", "Exchange rate euro yen"),
        ("🇪🇺🇨🇭 EUR/CHF", "Exchange rate euro franc"),
        ("🇪🇺🇵🇱 EUR/PLN", "Exchange rate euro zloty"),
        ("🇪🇺🇹🇷 EUR/TRY", "Exchange rate euro lira"),
    ]),
}

# Query text is sent in callback_data (Telegram limit: 64 bytes). Guard it.
assert all(len(f"q:{q}".encode()) <= 64 for _, leaves in MENUS.values() for _, q in leaves), \
    "a menu query exceeds Telegram's 64-byte callback_data limit"


def menu_root() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(text=title, callback_data=f"cat:{key}")]
            for key, (title, _) in MENUS.items()]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def menu_section(key: str) -> InlineKeyboardMarkup:
    _, leaves = MENUS[key]
    rows = [[InlineKeyboardButton(text=lbl, callback_data=f"q:{q}")] for lbl, q in leaves]
    rows.append([InlineKeyboardButton(text="🔙 Back", callback_data="cat:root")])
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
        "📊 Live data from the *ECB Data Portal* and *Eurostat*.\n"
        "Just type naturally — English or Italian.\n\n"
        "Try:\n"
        "• `Inflation Italy vs Germany since 2020`  ← compare!\n"
        "• `Disoccupazione Francia Spagna`\n"
        "• `House prices Euro area`\n"
        "• `Rendimento decennale Italia`\n"
        "• `EUR/USD exchange rate`\n\n"
        "Or tap 🚀 *Start* for the full menu ⬇️",
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
        "• Eurostat — CC BY 4.0\n"
        "🔗 https://data.ecb.europa.eu\n"
        "🔗 https://ec.europa.eu/eurostat",
        parse_mode="Markdown",
    )

@dp.message(F.text.in_(["🚀 Start"]))
async def start_menu(message: types.Message):
    await message.answer("Choose a source / category:", reply_markup=menu_root())


# --------------------------------------------------------------
# Search ANY Eurostat dataset
# --------------------------------------------------------------
async def do_search(message: types.Message, keyword: str):
    keyword = (keyword or "").strip()
    if not keyword:
        await message.answer("Usage: `/search <keywords>` — e.g. `/search tourism nights`",
                             parse_mode="Markdown")
        return
    await message.answer(f"🔎 Searching Eurostat for _{keyword}_…", parse_mode="Markdown")
    try:
        hits = eurostat_search(keyword, 8)
    except Exception as e:
        await message.answer(f"❌ Search failed:\n`{e}`", parse_mode="Markdown")
        return
    if not hits:
        await message.answer("No dataset matched. Try fewer or different words.")
        return
    rows = [[InlineKeyboardButton(text=title[:60], callback_data=f"ds:{code}")]
            for code, title in hits]
    await message.answer(f"Found {len(hits)} datasets — tap to chart:",
                         reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))

@dp.message(Command("search"))
async def cmd_search(message: types.Message, command: CommandObject):
    await do_search(message, command.args or "")

async def _handle_dataset(message: types.Message, code: str):
    uid = message.from_user.id if message.from_user else ""
    await message.answer(f"📡 Fetching dataset `{code}`…", parse_mode="Markdown")
    try:
        df, selection, geo = eurostat_fetch_auto(code)
        if df is None or df.empty:
            await message.answer(
                f"⚠️ Couldn't fetch `{code}` for a default geography — it may need "
                f"specific filters or a region.", parse_mode="Markdown")
            log_interaction(user_id=uid, query=f"dataset:{code}", provider="Eurostat",
                            indicator=code, n_obs=0, status="empty")
            return
        buf = plot_timeseries(df[["TIME_PERIOD", "OBS_VALUE"]], title=code)
        slice_txt = ", ".join(f"{k}={v}" for k, v in selection.items()) if selection else "single series"
        photo = BufferedInputFile(buf.getvalue(), filename="chart.png")
        await message.answer_photo(
            photo=photo,
            caption=f"*{code}*  ·  geo `{geo}`\n_slice: {slice_txt}_\n_Source: Eurostat (CC BY 4.0)_",
            parse_mode="Markdown")
        summary = summarize_trend(df, indicator_name=code, provider="Eurostat")
        if summary:
            await message.answer(summary, parse_mode="Markdown")
        log_interaction(user_id=uid, query=f"dataset:{code}", provider="Eurostat",
                        indicator=code, n_obs=len(df), status="ok")
    except Exception as e:
        logging.exception("❌ dataset fetch error:")
        await message.answer(f"❌ Error fetching `{code}`:\n`{e}`", parse_mode="Markdown")
        log_interaction(user_id=uid, query=f"dataset:{code}", provider="Eurostat",
                        indicator=code, n_obs="", status="error", error=str(e))


# --------------------------------------------------------------
# CALLBACKS menu
# --------------------------------------------------------------
@dp.callback_query(F.data.startswith("cat:"))
async def cb_category(callback: types.CallbackQuery):
    key = callback.data.split("cat:")[1]
    if key == "root":
        await callback.message.edit_text("Choose a category:", reply_markup=menu_root())
    else:
        title, _ = MENUS[key]
        await callback.message.edit_text(
            f"*{title}*\n\n"
            "Tap an indicator, or type your own:\n"
            "• compare: `… Italy vs France`\n"
            "• region: `Popolazione Puglia`\n"
            "• any dataset: `/search tourism nights`",
            parse_mode="Markdown", reply_markup=menu_section(key))
    await callback.answer()

# --------------------------------------------------------------
# CORE: elaborazione delle query
# --------------------------------------------------------------
async def process_text_query(message: types.Message, text: str):
    """Interpreta la query, scarica i dati, disegna grafico e commenta."""
    if text.strip() in {"🚀 Start", "ℹ️ Info"}:
        return
    low = text.strip().lower()
    if low.startswith(("search ", "cerca ")):  # natural-language dataset search
        return await do_search(message, text.strip().split(" ", 1)[1])
    logging.info(f"🧠 Query: {text}")
    plan = parse_message_to_query(text)
    if isinstance(plan, list):
        await message.answer(f"📊 Found {len(plan)} indicators — drawing charts…")
        for p in plan:
            await _handle_single_query(message, p, user_text=text)
        return
    await _handle_single_query(message, plan, user_text=text)

def _fetch_one(query: dict, geo: str) -> pd.DataFrame:
    if query.get("provider", "ECB") == "ECB":
        tmpl = query.get("geo_template")
        series = tmpl.format(geo=_ecb_geo(geo)) if tmpl else query.get("series")
        return fetch_ecb_data(query.get("flow"), series, query.get("params", {}))
    return fetch_eurostat_data(query.get("dataset"), {**query.get("eu_params", {}), "geo": geo})


def _fetch_frame(query: dict) -> pd.DataFrame:
    """Fetch a normalized [TIME_PERIOD, OBS_VALUE, COUNTRY] frame across every
    requested geo, so a 2-country query becomes a 2-line chart. One provider,
    one indicator, N countries."""
    geos = query.get("geos") or ["EA"]
    start = query.get("params", {}).get("startPeriod")
    frames = []
    for geo in geos:
        d = _fetch_one(query, geo)
        for delay in (0.6, 1.5):  # backoff retries — Eurostat drops requests under rapid fire
            if d is not None and not d.empty:
                break
            time.sleep(delay)
            d = _fetch_one(query, geo)
        if d is None or d.empty:
            continue
        d = d.loc[:, ["TIME_PERIOD", "OBS_VALUE"]].copy()
        d["COUNTRY"] = geo
        frames.append(d)
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    if start:  # apply the requested window — but not if it would empty a stale
        windowed = df[df["TIME_PERIOD"] >= pd.to_datetime(start)]  # dataset (e.g. poverty ends 2020)
        if not windowed.empty:
            df = windowed
    return df


def _help_text() -> str:
    return (
        "🤔 I couldn't map that to a dataset. Try, for example:\n\n"
        "• `Inflation Italy since 2020`\n"
        "• `Unemployment France vs Italy`\n"
        "• `GDP per capita Euro area`\n"
        "• `ECB deposit rate`\n"
        "• `EUR/USD exchange rate`\n\n"
        "Or tap 🚀 *Start* for the full menu."
    )


async def _handle_single_query(message: types.Message, query: dict, user_text: str = ""):
    user_id = message.from_user.id if message.from_user else ""
    indicator = query.get("indicator", "Indicator")
    provider  = query.get("provider", "ECB")

    # Unrecognized query → friendly guidance instead of a wrong chart
    if provider == "unknown":
        await message.answer(_help_text(), parse_mode="Markdown")
        log_interaction(user_id=user_id, query=user_text, provider="unknown",
                        indicator="", n_obs=0, status="unknown")
        return

    geos = query.get("geos") or ["EA"]
    title = indicator if not (len(geos) == 1 and geos[0] != "EA") else f"{indicator} — {geos[0]}"

    await message.answer(f"📡 Fetching *{title}*…", parse_mode="Markdown")
    try:
        df = _fetch_frame(query)
        if df is None or df.empty:
            await message.answer(
                f"⚠️ No data for *{title}*. This series may not exist for "
                f"{', '.join(geos)} — try the Euro area or another indicator.",
                parse_mode="Markdown")
            log_interaction(user_id=user_id, query=user_text, provider=provider,
                            indicator=title, n_obs=0, status="empty")
            return

        if df["COUNTRY"].nunique() > 1:  # compare → one line per country
            pivot = df.pivot_table(index="TIME_PERIOD", columns="COUNTRY", values="OBS_VALUE").sort_index()
            buf = plot_timeseries(pivot, title=title)
        else:
            buf = plot_timeseries(df[["TIME_PERIOD", "OBS_VALUE"]], title=title)

        src = "ECB Data Portal" if provider == "ECB" else "Eurostat"
        photo = BufferedInputFile(buf.getvalue(), filename="chart.png")
        await message.answer_photo(
            photo=photo,
            caption=f"*{title}*\n_Source: {src} (CC BY 4.0)_",
            parse_mode="Markdown",
        )

        summary = summarize_trend(df, indicator_name=title, provider=provider)
        if summary:
            await message.answer(summary, parse_mode="Markdown")

        log_interaction(user_id=user_id, query=user_text, provider=provider,
                        indicator=title, n_obs=len(df), status="ok")

    except Exception as e:
        logging.exception("❌ data error:")
        await message.answer(f"❌ Error fetching data:\n`{e}`", parse_mode="Markdown")
        log_interaction(user_id=user_id, query=user_text, provider=provider,
                        indicator=title, n_obs="", status="error", error=str(e))

# --------------------------------------------------------------
# CALLBACK: a menu leaf → run its query through the NL pipeline
# --------------------------------------------------------------
@dp.callback_query(F.data.startswith("q:"))
async def cb_run_query(callback: types.CallbackQuery):
    await callback.answer()
    query_text = callback.data[2:].strip()
    await callback.message.answer(f"🧠 _{query_text}_", parse_mode="Markdown")
    await process_text_query(callback.message, query_text)

@dp.callback_query(F.data.startswith("ds:"))
async def cb_dataset(callback: types.CallbackQuery):
    await callback.answer()
    await _handle_dataset(callback.message, callback.data[3:].strip())

# --------------------------------------------------------------
# Free text (excludes commands and the reply-keyboard buttons)
# --------------------------------------------------------------
@dp.message(~CommandStart(), ~Command("help"), ~Command("search"), ~F.text.in_({"ℹ️ Info", "🚀 Start"}))
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
