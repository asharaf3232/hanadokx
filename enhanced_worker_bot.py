# interactive_hand.py
# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🦾 Interactive Hand Bot | v2.1 (Corrections) 🦾 ---
# =======================================================================================
# v2.1:
#   ✅ [إصلاح] تمت إضافة منطق تنفيذ الصفقات المفقود في دالة execute_trade.
#   ✅ [تحسين] تم تعريب أزرار الواجهة الرئيسية بشكل كامل.
# =======================================================================================

import asyncio
import json
import os
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import aiosqlite
import ccxt.async_support as ccxt
import redis.asyncio as redis
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# --- ⚙️ الإعدادات والتهيئة ⚙️ ---
load_dotenv()

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')
REDIS_URL = os.getenv('REDIS_URL')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))

DB_FILE = 'interactive_hand.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- حالة البوت (يتم تحديثها باستمرار) ---
class BotState:
    def __init__(self):
        self.exchange = None
        self.app = None
        self.connections = {"okx": "Connecting...", "redis": "Connecting..."}

bot_state = BotState()

# --- دوال قاعدة البيانات ---
async def init_database():
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT,
                entry_price REAL, quantity REAL, status TEXT, 
                close_price REAL, pnl_usdt REAL, okx_order_id TEXT
            )
        ''')
        await conn.commit()
    logging.info("Database initialized.")

async def get_active_trades():
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("SELECT * FROM trades WHERE status = 'active' ORDER BY id DESC")
        return await cursor.fetchall()

async def get_closed_trades(limit=10):
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("SELECT * FROM trades WHERE status = 'closed' ORDER BY id DESC LIMIT ?", (limit,))
        return await cursor.fetchall()

# --- منطق التداول واستقبال الإشارات ---
async def execute_trade(signal):
    """
    ✅✅✅ [تم الإصلاح] هذه الدالة تحتوي الآن على منطق تنفيذ الصفقات الكامل.
    """
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = bot_state.exchange.amount_to_precision(symbol, amount_to_buy)

        logging.info(f"Executing trade for {symbol}. Amount: {formatted_amount}")
        order = await bot_state.exchange.create_market_buy_order(symbol, formatted_amount)
        
        await asyncio.sleep(2)
        filled_order = await bot_state.exchange.fetch_order(order['id'], symbol)
        
        actual_price = filled_order.get('average', entry_price)
        actual_quantity = filled_order.get('filled', 0)

        if actual_quantity > 0:
            async with aiosqlite.connect(DB_FILE) as conn:
                await conn.execute(
                    "INSERT INTO trades (timestamp, symbol, entry_price, quantity, status, okx_order_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (datetime.now(EGYPT_TZ).isoformat(), symbol, actual_price, actual_quantity, 'active', order['id'])
                )
                await conn.commit()
            
            msg = (f"✅ **[اليد التفاعلية] تم تنفيذ صفقة**\n"
                   f"`{symbol}` بسعر `~${actual_price:,.4f}`")
            await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode=ParseMode.MARKDOWN)
        else:
            logging.warning(f"Order {order['id']} for {symbol} was placed but not filled.")

    except Exception as e:
        logging.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        error_msg = f"🚨 **[اليد التفاعلية]** فشل تنفيذ صفقة لـ `{signal['symbol']}`.\n**السبب:** {e}"
        await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, error_msg, parse_mode=ParseMode.MARKDOWN)

# --- مستمعات Redis (تعمل في الخلفية) ---
async def trade_signal_listener():
    logging.info("Starting trade signals listener...")
    while True:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            pubsub = redis_client.pubsub()
            await pubsub.subscribe("trade_signals")
            bot_state.connections['redis'] = 'Connected 🟢'
            logging.info("Trade signals listener connected to Redis.")
            async for message in pubsub.listen():
                if message and message['type'] == 'message':
                    signal_data = json.loads(message['data'])
                    logging.info(f"Trade signal received: {signal_data.get('symbol')}")
                    asyncio.create_task(execute_trade(signal_data))
        except Exception as e:
            logging.error(f"Trade listener error: {e}. Reconnecting...")
            bot_state.connections['redis'] = f'Disconnected 🔴 ({type(e).__name__})'
            await asyncio.sleep(15)

async def system_status_listener():
    logging.info("Starting system status listener...")
    while True:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            pubsub = redis_client.pubsub()
            await pubsub.subscribe("system_status")
            logging.info("System status listener connected to Redis.")
            async for message in pubsub.listen():
                if message and message['type'] == 'message':
                    status_data = json.loads(message['data'])
                    if status_data.get('event') == 'SCAN_SKIPPED':
                        reason = status_data.get('reason', 'سبب غير معروف')
                        notification = f"⚠️ **تنبيه:**\nالبوت الرئيسي متوقف مؤقتًا عن البحث عن صفقات.\n\n**السبب:** {reason}"
                        await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, notification, parse_mode=ParseMode.MARKDOWN)
        except Exception as e:
            logging.error(f"Status listener error: {e}. Reconnecting...")
            await asyncio.sleep(15)

# --- أوامر التليجرام (الواجهة التفاعلية) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅✅✅ [تم الإصلاح] الأزرار الآن باللغة العربية وترسل الأوامر الصحيحة.
    """
    keyboard = [
        ["📊 الحالة", "💰 المحفظة"],
        ["📈 الصفقات المفتوحة", "📜 السجل"]
    ]
    await update.message.reply_text(
        "أهلاً بك في بوت اليد التفاعلي.\nاستخدم الأزرار للمتابعة.",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ جاري فحص حالة الاتصالات...")
    try:
        await bot_state.exchange.fetch_time()
        bot_state.connections['okx'] = "Connected 🟢"
    except Exception as e:
        bot_state.connections['okx'] = f"Disconnected 🔴 ({type(e).__name__})"
    
    status_text = (
        f"🩺 **حالة البوت (نبض)**\n\n"
        f"- **الاتصال بـ OKX:** {bot_state.connections['okx']}\n"
        f"- **الاتصال بـ Redis:** {bot_state.connections['redis']}\n\n"
        f"البوت يعمل وينتظر استقبال الإشارات."
    )
    await update.message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("⏳ جاري جلب رصيد المحفظة...")
    try:
        balance = await bot_state.exchange.fetch_total_balance()
        usdt_balance = balance.get('USDT', 0)
        await update.message.reply_text(f"💰 **رصيد المحفظة:**\n\n- إجمالي USDT: `{usdt_balance:,.2f}`")
    except Exception as e:
        await update.message.reply_text(f"❌ فشل جلب المحفظة: {e}")

async def active_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = await get_active_trades()
    if not trades:
        await update.message.reply_text("✅ لا توجد صفقات مفتوحة حاليًا.")
        return
    
    message = "📈 **الصفقات المفتوحة حاليًا:**\n\n"
    for trade in trades:
        message += f"- `{trade['symbol']}` | الدخول: `${trade['entry_price']}`\n"
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    trades = await get_closed_trades()
    if not trades:
        await update.message.reply_text("📚 لا توجد صفقات مغلقة في السجل بعد.")
        return

    message = "📜 **سجل آخر 10 صفقات مغلقة:**\n\n"
    for trade in trades:
        pnl = trade['pnl_usdt'] or 0.0
        emoji = "✅" if pnl >= 0 else "🛑"
        message += f"{emoji} `{trade['symbol']}` | الربح/الخسارة: `${pnl:,.2f}`\n"
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def handle_text_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    ✅✅✅ [تم الإصلاح] هذه الدالة تعالج الضغط على الأزرار العربية.
    """
    text = update.message.text
    if text == "📊 الحالة":
        await status(update, context)
    elif text == "💰 المحفظة":
        await portfolio(update, context)
    elif text == "📈 الصفقات المفتوحة":
        await active_trades(update, context)
    elif text == "📜 السجل":
        await history(update, context)

async def post_init_tasks(app: Application):
    await app.bot.send_message(TELEGRAM_CHAT_ID, "*🦾 بوت اليد التفاعلي بدأ العمل...*")
    await init_database()
    try:
        bot_state.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE})
        await bot_state.exchange.load_markets()
        bot_state.connections['okx'] = 'Connected 🟢'
    except Exception as e:
        bot_state.connections['okx'] = f'Failed to connect 🔴 ({type(e).__name__})'

    asyncio.create_task(trade_signal_listener())
    asyncio.create_task(system_status_listener())

def main():
    bot_state.app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init_tasks).build()
    
    bot_state.app.add_handler(CommandHandler("start", start))
    
    # معالجة الأزرار النصية العربية
    bot_state.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_buttons))

    bot_state.app.run_polling()

if __name__ == '__main__':
    main()
