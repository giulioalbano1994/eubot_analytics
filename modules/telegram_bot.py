"""
==============================================================
# 📦 MODULE: telegram_bot.py
==============================================================

# Questo modulo gestisce:
# 1️⃣ Connessione Telegram (Bot, Dispatcher)
# 2️⃣ Comandi base (/start, /help, /list, /about)
# 3️⃣ Analisi messaggi testuali o da pulsante (LLM router)
# 4️⃣ Recupero dati BCE via REST + generazione grafico
# 5️⃣ Invio dei risultati (grafico o testo)

==============================================================
"""

# ============================================================== #
# SEZIONE 1️⃣ : IMPORT E CONFIGURAZIONE
# ============================================================== #

import asyncio
import logging
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart, Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from config.settings import TELEGRAM_TOKEN
from modules.llm_router import parse_message_to_query
from modules.fetchers.ecb_adapter import fetch_ecb_data
from modules.plotter import plot_timeseries

# -------------------------------------------------------------- #
# Setup base
# -------------------------------------------------------------- #
bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()

# ============================================================== #
# SEZIONE 2️⃣ : COMANDI BASE (/start, /help, /list, /about)
# ============================================================== #

@dp.message(CommandStart())
async def start_handler(message: types.Message):
    """Welcome message + interactive menu"""
    examples = [
        ("📈 Euro Area Inflation (HICP)", "Show me euro area inflation"),
        ("🏦 Deposit Facility Rate (DFR)", "What is the ECB deposit rate?"),
        ("💶 EUR/USD Exchange Rate", "EUR/USD exchange rate last month"),
        ("💷 EUR/GBP Exchange Rate", "EUR/GBP exchange rate"),
        ("💴 EUR/JPY Exchange Rate", "EUR/JPY exchange rate"),
        ("💰 Broad Money (M3)", "Show me M3 money supply for euro area"),
        ("📊 Euro Area GDP", "Euro area GDP quarterly data"),
        ("👷‍♂️ Unemployment Italy", "Unemployment rate in Italy"),
        ("🇫🇷 Inflation France", "Inflation in France"),
        ("🇩🇪 Inflation Germany", "Inflation in Germany"),
    ]

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=label, callback_data=query)]
            for label, query in examples
        ]
    )

    await message.answer(
        "👋 *Welcome to Euro DataBot!*\n\n"
        "I'm your assistant for ECB and Eurostat data.\n\n"
        "You can type a question like:\n"
        "`show me euro area inflation since 2020`\n\n"
        "Or tap one of these examples 👇",
        reply_markup=keyboard,
        parse_mode="Markdown"
    )


@dp.message(Command("help"))
async def help_handler(message: types.Message):
    await message.answer(
        "ℹ️ *Available commands:*\n"
        "/start - restart conversation\n"
        "/help - show this guide\n"
        "/list - list available indicators\n"
        "/about - project info",
        parse_mode="Markdown"
    )


@dp.message(Command("list"))
async def list_handler(message: types.Message):
    await message.answer(
        "📊 *Currently available indicators:*\n"
        "1️⃣ Euro area inflation (HICP)\n"
        "2️⃣ ECB Deposit Facility Rate (DFR)\n"
        "3️⃣ EUR/USD exchange rate\n"
        "\nMore coming soon: GDP, unemployment, money supply...",
        parse_mode="Markdown"
    )


@dp.message(Command("about"))
async def about_handler(message: types.Message):
    await message.answer(
        "🤖 *Euro DataBot* is an assistant built on ECB and Eurostat data.\n"
        "Made with Python + Aiogram + Pandas + REST SDMX API.\n"
        "Created by *Giulio Albano* for analysis and data dissemination.",
        parse_mode="Markdown"
    )

# ============================================================== #
# SEZIONE 3️⃣ : FUNZIONE CENTRALE DI PROCESSO
# ============================================================== #

async def process_text_query(message: types.Message, text: str):
    """
    Core function: parse natural language query → fetch data → send chart.
    """
    logging.info(f"🧠 Processing text: {text}")
    query = parse_message_to_query(text)
    provider = query.get("provider", "ECB")

    # === ECB case ===
    if provider == "ECB":
        flow = query.get("flow")
        series = query.get("series")
        params = query.get("params", {"lastNObservations": 120})
        indicator = query.get("indicator", flow)

        await message.answer(f"📊 Fetching data from ECB: {indicator} ...")

        try:
            df = fetch_ecb_data(flow, series, params)
            if df.empty:
                await message.answer("⚠️ No data returned from ECB API.")
                return

            chart_buf = plot_timeseries(df, title=indicator)
            photo = BufferedInputFile(chart_buf.getvalue(), filename="chart.png")

            await bot.send_photo(
                chat_id=message.chat.id,
                photo=photo,
                caption=f"Source: ECB — {indicator}"
            )
        except Exception as e:
            logging.exception("Error while fetching ECB data:")
            await message.answer(f"❌ Error while retrieving data:\n`{e}`", parse_mode="Markdown")

    # === Eurostat placeholder ===
    elif provider == "Eurostat":
        await message.answer("🇪🇺 Eurostat data support coming soon!")

    else:
        await message.answer("🙈 I couldn't understand that request. Try another one!")

# ============================================================== #
# SEZIONE 4️⃣ : HANDLER TESTUALE E CALLBACK BUTTON
# ============================================================== #

@dp.message(F.text)
async def handle_query(message: types.Message):
    """Triggered by user typing any text"""
    await process_text_query(message, message.text.strip())


@dp.callback_query()
async def handle_example_callback(callback: types.CallbackQuery):
    """Triggered when clicking on an example button"""
    query_text = callback.data
    await callback.answer()  # closes the loading circle
    await callback.message.answer(f"🧠 You selected: _{query_text}_", parse_mode="Markdown")
    await process_text_query(callback.message, query_text)

# ============================================================== #
# SEZIONE 5️⃣ : AVVIO DEL BOT
# ============================================================== #

async def main():
    logging.info("🤖 Bot is running and listening on Telegram...")
    await dp.start_polling(bot)

def start_bot():
    """Entry point called by main.py"""
    asyncio.run(main())
