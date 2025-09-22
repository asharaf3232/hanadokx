# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🤖 OKX Worker UI Bot | v1.1 (مع تأكيدات الاتصال) 🤖 ---
# =======================================================================================
# v1.1 Changelog:
#   ✅ [إضافة] رسالة "نبض قلب" (Heartbeat) دورية كل ساعة للتأكد من استمرارية الاتصال.
#   ✅ [تحسين] رسالة بدء تشغيل أكثر تفصيلاً تؤكد نجاح كل اتصال على حدة.
#   ✅ [تحسين] تقرير التشخيص أصبح يقوم بفحص "حي" للاتصالات ويعرض بيانات إضافية.
# =======================================================================================

import asyncio
import json
import os
import logging
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo
import aiosqlite
import ccxt.async_support as ccxt
import redis.asyncio as redis
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler

# --- ⚙️ الإعدادات والتهيئة ⚙️ ---
load_dotenv()

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHONSE = os.getenv('OKX_API_PASSPHONSE') # خطأ إملائي شائع، تأكد من أنه PASSPHRASE في ملفك
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE', OKX_API_PASSPHONSE)
REDIS_URL = os.getenv('REDIS_URL')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DB_FILE = 'my_trades.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))
HEARTBEAT_INTERVAL_SECONDS = 3600 # [تعديل] إرسال نبض القلب كل ساعة

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class WorkerState:
    def __init__(self):
        self.exchange = None
        self.redis_client = None
        self.telegram_app = None
        self.connections = {"okx": "Connecting...", "redis": "Connecting..."}

worker_state = WorkerState()

async def init_database():
    # ... (الكود كما هو)
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                symbol TEXT,
                entry_price REAL,
                quantity REAL,
                status TEXT, -- 'active', 'closed'
                close_price REAL,
                pnl_usdt REAL,
                okx_order_id TEXT
            )
        ''')
        await conn.commit()
    logging.info("Local database initialized successfully.")

async def execute_trade(signal):
    # ... (الكود كما هو)
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = worker_state.exchange.amount_to_precision(symbol, amount_to_buy)

        logging.info(f"Signal for {symbol}. Placing market buy order for {formatted_amount} units.")
        order = await worker_state.exchange.create_market_buy_order(symbol, formatted_amount)
        
        filled_order = await worker_state.exchange.fetch_order(order['id'], symbol)
        actual_price = filled_order.get('average', entry_price)
        actual_quantity = filled_order.get('filled', 0)

        if actual_quantity > 0:
            async with aiosqlite.connect(DB_FILE) as conn:
                await conn.execute(
                    "INSERT INTO trades (timestamp, symbol, entry_price, quantity, status, okx_order_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (datetime.now(EGYPT_TZ).isoformat(), symbol, actual_price, actual_quantity, 'active', order['id'])
                )
                await conn.commit()
            
            success_msg = (
                f"✅ **تم استلام وتنفيذ إشارة جديدة**\n\n"
                f"**العملة:** `{symbol}`\n"
                f"**سعر التنفيذ الفعلي:** `${actual_price:,.4f}`\n"
                f"**الكمية:** `{actual_quantity:,.4f}`\n"
                f"**التكلفة:** `≈ ${TRADE_SIZE_USDT:,.2f}`"
            )
            await safe_send_message(worker_state.telegram_app.bot, success_msg)
        else:
             logging.warning(f"Order {order['id']} for {symbol} was placed but not filled.")

    except Exception as e:
        logging.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        await safe_send_message(worker_state.telegram_app.bot, f"🚨 فشل تنفيذ صفقة لـ `{signal['symbol']}`. السبب: {e}")

async def redis_listener():
    while True:
        try:
            worker_state.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            await worker_state.redis_client.ping() # التأكد من الاتصال
            pubsub = worker_state.redis_client.pubsub()
            await pubsub.subscribe("trade_signals")
            worker_state.connections["redis"] = "Connected & Listening 🟢"
            # [تعديل] إرسال رسالة تأكيد عند نجاح الاتصال والاستماع
            await safe_send_message(worker_state.telegram_app.bot, "✅ **اتصال Redis ناجح.** البوت يستمع الآن للإشارات.")
            logging.info("Redis listener connected. Waiting for signals...")
            
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=None)
                if message and message['type'] == 'message':
                    try:
                        signal_data = json.loads(message['data'])
                        logging.info(f"Signal received: {signal_data['symbol']}")
                        await execute_trade(signal_data)
                    except Exception as e:
                        logging.error(f"Error processing signal: {e}")

        except Exception as e:
            worker_state.connections["redis"] = f"Disconnected 🔴 ({e})"
            logging.error(f"Redis connection failed: {e}. Reconnecting in 15 seconds...")
            await asyncio.sleep(15)

# --- [تعديل] دالة نبض القلب الدورية ---
async def heartbeat_check(context: ContextTypes.DEFAULT_TYPE):
    """
    تقوم بفحص الاتصالات بشكل دوري وإرسال رسالة تأكيد للمستخدم
    """
    okx_status = "Offline 🔴"
    redis_status = "Offline 🔴"
    
    # فحص OKX
    try:
        await worker_state.exchange.fetch_time()
        okx_status = "Online 🟢"
    except Exception:
        pass # يبقى الحالة Offline
        
    # فحص Redis
    try:
        await worker_state.redis_client.ping()
        redis_status = "Listening 🟢"
    except Exception:
        pass # يبقى الحالة Offline

    message = f"💓 **Heartbeat** | حالة البوت:\n- OKX: {okx_status}\n- Redis: {redis_status}"
    await safe_send_message(context.bot, message)

async def safe_send_message(bot, text, **kwargs):
    # ... (الكود كما هو)
    try:
        await bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        logging.error(f"Telegram Send Error: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (الكود كما هو)
    keyboard = [["Dashboard 🖥️"]]
    await update.message.reply_text("أهلاً بك في **بوت OKX المنفذ**.\nأنا استمع للإشارات من البوت الرئيسي وجاهز لتنفيذها على حسابك.",
                                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
                                    parse_mode=ParseMode.MARKDOWN)

async def show_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (الكود كما هو)
    keyboard = [
        [InlineKeyboardButton("💼 المحفظة", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 الصفقات النشطة", callback_data="show_active_trades")],
        [InlineKeyboardButton("📜 سجل الصفقات", callback_data="show_history")],
        [InlineKeyboardButton("🕵️‍♂️ التشخيص", callback_data="show_diagnostics")],
    ]
    await update.message.reply_text("🖥️ **لوحة التحكم**", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (الكود كما هو)
    query = update.callback_query
    await query.answer("جاري جلب بيانات المحفظة...")
    try:
        balance = await worker_state.exchange.fetch_balance()
        total_usdt = balance.get('USDT', {}).get('total', 0)
        free_usdt = balance.get('USDT', {}).get('free', 0)
        
        assets_str = [f"  - `USDT`: `{total_usdt:,.2f}` (متاح: `{free_usdt:,.2f}`)"]
        
        message = (f"**💼 نظرة عامة على المحفظة**\n"
                   f"━━━━━━━━━━━━━━━━━━━━\n"
                   f"**💰 تفاصيل الأصول:**\n" + "\n".join(assets_str))
        
        await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await query.edit_message_text(f"حدث خطأ: {e}")

async def show_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري إجراء فحص حي...")
    
    # --- [تعديل] إجراء فحص حي ومباشر ---
    okx_status = "Disconnected 🔴"
    okx_time_str = "N/A"
    try:
        server_time_ms = await worker_state.exchange.fetch_time()
        okx_status = "Connected 🟢"
        server_dt = datetime.fromtimestamp(server_time_ms / 1000, EGYPT_TZ)
        okx_time_str = server_dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        okx_status = f"Disconnected 🔴 ({type(e).__name__})"

    redis_status = "Disconnected 🔴"
    try:
        await worker_state.redis_client.ping()
        redis_status = "Connected & Listening 🟢"
    except Exception as e:
        redis_status = f"Disconnected 🔴 ({type(e).__name__})"

    report = (f"🕵️‍♂️ **تقرير التشخيص المباشر**\n\n"
              f"**حالة الاتصالات:**\n"
              f"- اتصال بـ OKX: **{okx_status}**\n"
              f"  - توقيت المنصة: `{okx_time_str}`\n"
              f"- اتصال بـ Redis: **{redis_status}**\n\n"
              f"**إعدادات:**\n"
              f"- حجم الصفقة المحدد: `${TRADE_SIZE_USDT}`")
    await query.edit_message_text(report, parse_mode=ParseMode.MARKDOWN)

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (الكود كما هو)
    if update.message.text == "Dashboard 🖥️":
        await show_dashboard(update, context)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ... (الكود كما هو)
    query = update.callback_query
    if query.data == "show_portfolio":
        await show_portfolio(update, context)
    elif query.data == "show_diagnostics":
        await show_diagnostics(update, context)

async def post_init(application: Application):
    worker_state.telegram_app = application
    await safe_send_message(application.bot, "*🤖 بوت OKX المنفذ قيد التشغيل...*\n\nجاري فحص الاتصالات...")
    
    # فحص أولي للاتصالات
    try:
        worker_state.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHONSE, 'enableRateLimit': True})
        await worker_state.exchange.load_markets()
        worker_state.connections["okx"] = "Connected 🟢"
        await safe_send_message(application.bot, "✅ **اتصال OKX ناجح.**")
    except Exception as e:
        worker_state.connections["okx"] = f"Failed 🔴: {e}"
        await safe_send_message(application.bot, f"❌ **فشل الاتصال بـ OKX:**\n`{e}`")

    await init_database()
    asyncio.create_task(redis_listener())
    
    # --- [تعديل] جدولة مهمة نبض القلب الدورية ---
    jq = application.job_queue
    jq.run_repeating(heartbeat_check, interval=HEARTBEAT_INTERVAL_SECONDS, first=HEARTBEAT_INTERVAL_SECONDS)
    
    logging.info(f"Worker UI Bot is running. Heartbeat scheduled every {HEARTBEAT_INTERVAL_SECONDS} seconds.")

def main():
    if not all([OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHONSE, REDIS_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        print("FATAL: Please check your .env file. All variables are required.")
        return
        
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.run_polling()

if __name__ == '__main__':
    main()
