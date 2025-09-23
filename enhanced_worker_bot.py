# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🦾 Interactive Hand Bot | v2.2 (Definitive Fix) 🦾 ---
# =======================================================================================
# v2.2:
#   ✅ [إصلاح حاسم] إصلاح آلية معالجة الأوامر من الأزرار والتليجرام.
#   ✅ [تحسين] تحسين استقرار مستمعات Redis وإضافة تشخيصات إضافية.
#   ✅ [اكتمال] ملء جميع الدوال المساعدة (المحفظة، السجل، إلخ).
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

DB_FILE = 'interactive_hand_v2.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- حالة البوت ---
class BotState:
    def __init__(self):
        self.exchange = None
        self.app = None
        self.connections = {"okx": "Connecting...", "redis": "Connecting..."}

bot_state = BotState()

# --- دوال قاعدة البيانات ---
async def init_database():
    """ينشئ ويتأكد من وجود جدول الصفقات في قاعدة البيانات."""
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
    """يجلب كل الصفقات المفتوحة حاليًا من قاعدة البيانات."""
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("SELECT * FROM trades WHERE status = 'active' ORDER BY id DESC")
        return await cursor.fetchall()

async def get_closed_trades(limit=10):
    """يجلب آخر الصفقات المغلقة من قاعدة البيانات."""
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("SELECT * FROM trades WHERE status = 'closed' ORDER BY id DESC LIMIT ?", (limit,))
        return await cursor.fetchall()

# --- منطق التداول ---
async def execute_trade(signal):
    """الدالة المسؤولة عن تنفيذ أمر الشراء عند استلام إشارة."""
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        logging.info(f"Attempting to execute trade for {symbol}...")
        
        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = bot_state.exchange.amount_to_precision(symbol, amount_to_buy)

        order = await bot_state.exchange.create_market_buy_order(symbol, formatted_amount)
        
        await asyncio.sleep(2) # انتظار بسيط لتحديث حالة الأمر
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
            
            msg = f"✅ **[اليد] تم تنفيذ صفقة:** `{symbol}`"
            await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, msg, parse_mode=ParseMode.MARKDOWN)
            logging.info(f"Successfully executed and logged trade for {symbol}.")
        else:
            logging.warning(f"Order for {symbol} was placed but not filled.")

    except Exception as e:
        logging.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        error_msg = f"🚨 **[اليد]** فشل تنفيذ صفقة لـ `{signal['symbol']}`.\n**السبب:** {e}"
        await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, error_msg, parse_mode=ParseMode.MARKDOWN)

# --- مستمعات Redis (تعمل في الخلفية) ---
async def redis_listener(channel_name, callback_func):
    """دالة عامة للاستماع لأي قناة Redis وتنفيذ دالة معينة عند استلام رسالة."""
    logging.info(f"Listener starting for Redis channel: {channel_name}")
    while True:
        try:
            r = redis.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe(channel_name)
            if channel_name == "trade_signals":
                 bot_state.connections['redis'] = 'Connected 🟢'
            logging.info(f"Listener connected to channel: {channel_name}")
            async for message in pubsub.listen():
                if message and message['type'] == 'message':
                    logging.info(f"RAW MESSAGE RECEIVED on {channel_name}: {message['data']}")
                    try:
                        data = json.loads(message['data'])
                        asyncio.create_task(callback_func(data))
                    except json.JSONDecodeError:
                        logging.error(f"Could not decode JSON from message on {channel_name}")
        except Exception as e:
            logging.error(f"Redis listener for {channel_name} failed: {e}. Reconnecting in 15s...")
            if channel_name == "trade_signals":
                bot_state.connections['redis'] = 'Disconnected 🔴'
            await asyncio.sleep(15)

async def handle_trade_signal(data):
    """الدالة التي يتم استدعاؤها عند استلام إشارة صفقة."""
    logging.info(f"Handing off trade signal for {data.get('symbol')}")
    await execute_trade(data)

async def handle_system_status(data):
    """الدالة التي يتم استدعاؤها عند استلام رسالة حالة من البوت الرئيسي."""
    if data.get('event') == 'SCAN_SKIPPED':
        reason = data.get('reason', 'سبب غير معروف')
        notification = f"⚠️ **تنبيه:**\nالبوت الرئيسي متوقف مؤقتًا.\n**السبب:** {reason}"
        await bot_state.app.bot.send_message(TELEGRAM_CHAT_ID, notification, parse_mode=ParseMode.MARKDOWN)

# --- أوامر التليجرام (الواجهة التفاعلية) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """أمر البدء، يعرض لوحة الأزرار الرئيسية."""
    keyboard = [["📊 الحالة", "💰 المحفظة"], ["📈 الصفقات المفتوحة", "📜 السجل"]]
    await update.message.reply_text(
        "أهلاً بك في بوت اليد التفاعلي.",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )

async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض حالة اتصال البوت بالخدمات المختلفة."""
    await update.message.reply_text("⏳ جاري فحص حالة الاتصالات...")
    try:
        await bot_state.exchange.fetch_time()
        bot_state.connections['okx'] = "Connected 🟢"
    except Exception:
        bot_state.connections['okx'] = "Disconnected 🔴"
    
    status_text = (f"🩺 **حالة البوت**\n- OKX: {bot_state.connections['okx']}\n- Redis: {bot_state.connections['redis']}")
    await update.message.reply_text(status_text, parse_mode=ParseMode.MARKDOWN)

async def portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض رصيد USDT الإجمالي في المحفظة."""
    await update.message.reply_text("⏳ جاري جلب رصيد المحفظة...")
    try:
        balance = await bot_state.exchange.fetch_total_balance()
        usdt_balance = balance.get('USDT', 0)
        await update.message.reply_text(f"💰 **رصيد المحفظة:**\n\n- إجمالي USDT: `{usdt_balance:,.2f}`")
    except Exception as e:
        await update.message.reply_text(f"❌ فشل جلب المحفظة: {e}")

async def active_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض قائمة بالصفقات المفتوحة حاليًا."""
    trades = await get_active_trades()
    if not trades:
        await update.message.reply_text("✅ لا توجد صفقات مفتوحة حاليًا.")
        return
    
    message = "📈 **الصفقات المفتوحة حاليًا:**\n\n"
    for trade in trades:
        message += f"- `{trade['symbol']}` | سعر الدخول: `${trade['entry_price']}`\n"
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعرض سجل آخر 10 صفقات تم إغلاقها."""
    trades = await get_closed_trades()
    if not trades:
        await update.message.reply_text("📚 لا توجد صفقات مغلقة في السجل بعد.")
        return

    message = "📜 **سجل آخر 10 صفقات مغلقة:**\n\n"
    for trade in trades:
        pnl = trade.get('pnl_usdt') or 0.0
        emoji = "✅" if pnl >= 0 else "🛑"
        message += f"{emoji} `{trade['symbol']}` | الربح/الخسارة: `${pnl:,.2f}`\n"
    await update.message.reply_text(message, parse_mode=ParseMode.MARKDOWN)

async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """يعالج الضغط على الأزرار النصية العربية."""
    text = update.message.text
    if text == "📊 الحالة":
        await status(update, context)
    elif text == "💰 المحفظة":
        await portfolio(update, context)
    elif text == "📈 الصفقات المفتوحة":
        await active_trades(update, context)
    elif text == "📜 السجل":
        await history(update, context)
    else:
        # يمكنك إضافة رد هنا إذا أرسل المستخدم نصًا عاديًا
        await start(update, context)

async def post_init_tasks(app: Application):
    """المهام التي تبدأ مع البوت."""
    bot_state.app = app
    await app.bot.send_message(TELEGRAM_CHAT_ID, "*🦾 بوت اليد v2.2 بدأ العمل...*")
    await init_database()
    try:
        bot_state.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE})
        await bot_state.exchange.load_markets()
        bot_state.connections['okx'] = 'Connected 🟢'
    except Exception as e:
        bot_state.connections['okx'] = f'Failed 🔴 ({type(e).__name__})'

    # تشغيل المستمعات في الخلفية
    asyncio.create_task(redis_listener("trade_signals", handle_trade_signal))
    asyncio.create_task(redis_listener("system_status", handle_system_status))


def main():
    """الدالة الرئيسية التي تنشئ وتسجل الأوامر وتشغل البوت."""
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init_tasks).build()
    
    # تسجيل الأوامر التي تبدأ بـ /
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status))
    app.add_handler(CommandHandler("portfolio", portfolio))
    app.add_handler(CommandHandler("active_trades", active_trades))
    app.add_handler(CommandHandler("history", history))
    
    # تسجيل معالج الأزرار النصية (التي لا تبدأ بـ /)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))

    # بدء تشغيل البوت
    app.run_polling()

if __name__ == '__main__':
    main()
