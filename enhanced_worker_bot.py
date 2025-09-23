# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🤖 OKX Worker Mirror Bot | v3.3 (النسخة النهائية والمستقرة) 🤖 ---
# =======================================================================================
# v3.3 Changelog:
#   ✅ [إصلاح جذري] تم إصلاح خطأ `AttributeError` المتعلق بـ `JobQueue`.
#   ✅ [تحصين] إضافة تحقق للتأكد من وجود `JobQueue` قبل استخدامه لمنع الانهيار.
#   ✅ [استقرار] هذا الإصدار جاهز للعمل بشكل مستقر على منصات مثل Render.
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

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.error import BadRequest, Conflict

# --- ⚙️ الإعدادات والتهيئة ⚙️ ---
load_dotenv()
OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')
REDIS_URL = os.getenv('REDIS_URL')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
DB_FILE = 'my_trades_v3.db'
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
EGYPT_TZ = ZoneInfo("Africa/Cairo")

# --- 🏦 إدارة الحالة والاتصالات 🏦 ---
class WorkerState:
    def __init__(self):
        self.exchange = None
        self.telegram_app = None
        self.brain_dashboard_data = {"status": "Waiting for first update..."}

worker_state = WorkerState()

# --- دالة قفل التفرد ---
async def acquire_lock(redis_client, lock_key, expiry_seconds=30):
    return await redis_client.set(lock_key, "running", ex=expiry_seconds, nx=True)

# --- باقي الدوال (init_database, execute_trade, etc.) ---
# ... (هذه الدوال لم تتغير وموجودة بالكامل هنا)
async def init_database():
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, 
                entry_price REAL, quantity REAL, status TEXT, 
                close_price REAL, pnl_usdt REAL, okx_order_id TEXT
            )''')
        await conn.commit()
    logger.info("Local database initialized.")

async def execute_trade(signal):
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        async with aiosqlite.connect(DB_FILE) as conn:
            cursor = await conn.execute("SELECT id FROM trades WHERE symbol = ? AND status = 'active'", (symbol,))
            if await cursor.fetchone():
                logger.warning(f"Signal for {symbol} ignored. Active trade already exists.")
                await safe_send_message(f"⚠️ تم تجاهل إشارة لـ `{symbol}` لوجود صفقة نشطة بالفعل.")
                return

        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = worker_state.exchange.amount_to_precision(symbol, amount_to_buy)

        order = await worker_state.exchange.create_market_buy_order(symbol, formatted_amount)
        await asyncio.sleep(2)
        filled_order = await worker_state.exchange.fetch_order(order['id'], symbol)
        
        actual_price = filled_order.get('average') or entry_price
        actual_quantity = filled_order.get('filled', 0)

        if actual_quantity > 0:
            async with aiosqlite.connect(DB_FILE) as conn:
                await conn.execute(
                    "INSERT INTO trades (timestamp, symbol, entry_price, quantity, status, okx_order_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (datetime.now(EGYPT_TZ).isoformat(), symbol, actual_price, actual_quantity, 'active', order['id'])
                )
                await conn.commit()
            
            success_msg = (f"✅ **تم تنفيذ إشارة جديدة**\n\n"
                           f"**العملة:** `{symbol}`\n"
                           f"**سعر التنفيذ:** `${actual_price:,.4f}`\n"
                           f"**الكمية:** `{actual_quantity:,.4f}`")
            await safe_send_message(success_msg)
        else:
             await safe_send_message(f"🟡 تم إرسال أمر شراء لـ `{symbol}` ولكنه لم يتأكد بعد.")
    except Exception as e:
        logger.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        await safe_send_message(f"🚨 فشل تنفيذ صفقة لـ `{symbol}`. السبب: {e}")

async def redis_listener():
    while True:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(trade_signals="trade_signals", brain_dashboard="brain_dashboard_update")
            logger.info("Redis connected to channels: trade_signals, brain_dashboard_update")
            await safe_send_message("✅ **اتصال Redis ناجح.** البوت يستمع الآن.")

            async for message in pubsub.listen():
                if message['type'] != 'message': continue
                
                channel = message['channel']
                data = message['data']
                
                if channel == 'trade_signals':
                    try:
                        await execute_trade(json.loads(data))
                    except Exception as e: logger.error(f"Error processing trade signal: {e}")
                
                elif channel == 'brain_dashboard_update':
                    try:
                        worker_state.brain_dashboard_data = json.loads(data)
                    except Exception as e: logger.error(f"Error processing dashboard update: {e}")

        except Exception as e:
            logger.error(f"Redis connection failed: {e}. Reconnecting...")
            await safe_send_message(f"🔴 **انقطع اتصال Redis.** جارِ محاولة إعادة الاتصال...")
            await asyncio.sleep(15)

# --- واجهة تليجرام ---
async def safe_send_message(text, **kwargs):
    try:
        await worker_state.telegram_app.bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except BadRequest as e:
        if "Message is not modified" not in str(e): logger.warning(f"Edit Message Error: {e}")
    except Exception as e: logger.error(f"Telegram Send Error: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["Dashboard 🖥️"]]
    await update.message.reply_text("أهلاً بك في **بوت OKX المنفذ (Mirror Edition)**.",
                                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    await show_dashboard_message(update.message)

async def show_dashboard_message(message_or_update):
    keyboard = [
        [InlineKeyboardButton("🪞 مرآة العقل (Brain Mirror)", callback_data="show_brain_mirror")],
        [InlineKeyboardButton("💼 محفظتي", callback_data="show_portfolio"), InlineKeyboardButton("📈 صفقاتي", callback_data="show_active_trades")],
        [InlineKeyboardButton("📜 السجل", callback_data="show_history"), InlineKeyboardButton("🕵️‍♂️ التشخيص", callback_data="show_diagnostics")],
    ]
    target_message = message_or_update.message if hasattr(message_or_update, 'message') else message_or_update
    
    if hasattr(message_or_update, 'callback_query'):
        await message_or_update.callback_query.edit_message_text("🖥️ **لوحة التحكم**", reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await target_message.reply_text("🖥️ **لوحة التحكم**", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_brain_mirror(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري جلب بيانات العقل...")
    
    data = worker_state.brain_dashboard_data
    
    if "status" in data and data["status"] == "Waiting for first update...":
        await query.edit_message_text("⏳ في انتظار أول تحديث من البوت الرئيسي...", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 تحديث", callback_data="show_brain_mirror")],
            [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ]))
        return

    stats = data.get('overall_stats', {})
    scan_info = data.get('last_scan_info', {})
    mood = data.get('market_mood', {})
    last_update_utc = datetime.fromisoformat(data.get('timestamp_utc'))
    last_update_cairo = last_update_utc.replace(tzinfo=ZoneInfo("UTC")).astimezone(EGYPT_TZ)
    
    status_emoji = "✅" if data.get('trading_enabled', False) else "🚨"
    
    text = (f"**🪞 مرآة العقل الرئيسي**\n*آخر تحديث: {last_update_cairo.strftime('%H:%M:%S')}*\n\n"
            f"--- **الأداء العام** ---\n"
            f"  - **الربح/الخسارة الكلي:** `${stats.get('total_pnl', 0)}`\n"
            f"  - **معدل النجاح:** `{stats.get('win_rate', 0)}%`\n"
            f"  - **إجمالي الصفقات:** `{stats.get('total_trades', 0)}`\n\n"
            f"--- **الحالة والإعدادات** ---\n"
            f"  - **حالة التداول:** `{status_emoji}`\n"
            f"  - **النمط الحالي:** *{data.get('active_preset_name', 'N/A')}*\n"
            f"  - **مزاج السوق:** *{mood.get('reason', 'N/A')}*\n\n"
            f"--- **آخر فحص للسوق** ---\n"
            f"  - **المدة:** `{scan_info.get('duration_seconds', 'N/A')} ثانية`\n"
            f"  - **العملات المفحوصة:** `{scan_info.get('checked_symbols', 'N/A')}`\n")
            
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 تحديث", callback_data="show_brain_mirror")],
        [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
    ]))

async def show_portfolio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري جلب بيانات المحفظة...")
    try:
        balance = await worker_state.exchange.fetch_balance()
        total_usdt = balance.get('USDT', {}).get('total', 0)
        free_usdt = balance.get('USDT', {}).get('free', 0)
        
        message = (f"**💼 محفظتي**\n\n"
                   f"  - `USDT`: `{total_usdt:,.2f}`\n"
                   f"  - *متاح للتداول:* `{free_usdt:,.2f}`")
        
        await query.edit_message_text(message, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
             [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ]))
    except Exception as e:
        await query.edit_message_text(f"حدث خطأ: {e}")

async def show_active_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        active_trades = await (await conn.execute("SELECT * FROM trades WHERE status = 'active' ORDER BY id DESC")).fetchall()
    
    if not active_trades:
        await query.edit_message_text("✅ لا توجد صفقات نشطة حالياً.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ]))
        return

    text = "📈 **صفقاتي النشطة:**\n\n"
    keyboard = []
    
    symbols = [trade['symbol'] for trade in active_trades]
    if symbols:
        tickers = await worker_state.exchange.fetch_tickers(symbols)
        for trade in active_trades:
            current_price = tickers.get(trade['symbol'], {}).get('last', 0)
            pnl = (current_price - trade['entry_price']) * trade['quantity'] if current_price > 0 else 0
            pnl_percent = ((current_price / trade['entry_price']) - 1) * 100 if trade['entry_price'] > 0 else 0
            emoji = "🔼" if pnl >= 0 else "🔽"
            text += f"🔹 `{trade['symbol']}` | PNL: `${pnl:+.2f}` ({pnl_percent:+.2f}%) {emoji}\n"
            keyboard.append([InlineKeyboardButton(f"تفاصيل وإغلاق {trade['symbol']}", callback_data=f"trade_details_{trade['id']}")])
    
    keyboard.append([InlineKeyboardButton("🔄 تحديث", callback_data="show_active_trades")])
    keyboard.append([InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")])
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_trade_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; trade_id = int(query.data.split('_')[2])
    await query.answer()
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trade = await (await conn.execute("SELECT * FROM trades WHERE id = ?", (trade_id,))).fetchone()

    if not trade or trade['status'] != 'active':
        await query.edit_message_text("⚠️ لم يتم العثور على الصفقة أو تم إغلاقها.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📈 العودة للصفقات النشطة", callback_data="show_active_trades")]
        ])); return

    ticker = await worker_state.exchange.fetch_ticker(trade['symbol'])
    current_price = ticker['last']
    pnl = (current_price - trade['entry_price']) * trade['quantity']
    pnl_percent = (current_price / trade['entry_price'] - 1) * 100

    text = (f"**🔍 تفاصيل الصفقة #{trade['id']}**\n\n"
            f"**العملة:** `{trade['symbol']}`\n"
            f"**تاريخ الدخول:** `{datetime.fromisoformat(trade['timestamp']).strftime('%Y-%m-%d %H:%M')}`\n"
            f"**سعر الدخول:** `${trade['entry_price']:,.4f}`\n"
            f"**السعر الحالي:** `${current_price:,.4f}`\n"
            f"**الكمية:** `{trade['quantity']}`\n\n"
            f"**الربح/الخسارة:** `${pnl:+.2f} ({pnl_percent:+.2f}%)`")
    
    keyboard = [
        [InlineKeyboardButton("🚨 إغلاق الصفقة الآن 🚨", callback_data=f"close_trade_{trade['id']}")],
        [InlineKeyboardButton("📈 العودة للصفقات النشطة", callback_data="show_active_trades")]
    ]
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup(keyboard))

async def close_trade_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; trade_id = int(query.data.split('_')[2])
    await query.answer("جاري إرسال أمر الإغلاق...")
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trade = await (await conn.execute("SELECT * FROM trades WHERE id = ? AND status = 'active'", (trade_id,))).fetchone()
        
        if not trade:
            await query.edit_message_text("⚠️ الصفقة مغلقة بالفعل."); return

        sell_order = await worker_state.exchange.create_market_sell_order(trade['symbol'], trade['quantity'])
        await asyncio.sleep(2)
        filled_order = await worker_state.exchange.fetch_order(sell_order['id'], trade['symbol'])
        
        close_price = filled_order.get('average', 0)
        pnl = (close_price - trade['entry_price']) * trade['quantity']
        
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("UPDATE trades SET status = 'closed', close_price = ?, pnl_usdt = ? WHERE id = ?", (close_price, pnl, trade_id)); await conn.commit()

        emoji = "✅" if pnl >= 0 else "🛑"
        await query.edit_message_text(f"{emoji} **تم إغلاق الصفقة بنجاح**\n`{trade['symbol']}` | ربح/خسارة: `${pnl:,.2f}`",
                                      reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📈 العودة للصفقات", callback_data="show_active_trades")]]))
    except Exception as e:
        await query.message.reply_text(f"🚨 فشل إغلاق الصفقة: {e}")

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trades = await (await conn.execute("SELECT * FROM trades WHERE status = 'closed' ORDER BY id DESC LIMIT 15")).fetchall()
        total_pnl = (await (await conn.execute("SELECT SUM(pnl_usdt) FROM trades WHERE status = 'closed'")).fetchone())[0] or 0.0
    
    if not trades:
        await query.edit_message_text("ℹ️ لا يوجد سجل للصفقات المغلقة بعد.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ])); return

    text = f"📜 **سجل آخر 15 صفقة**\n**إجمالي الربح/الخسارة:** `${total_pnl:,.2f}`\n\n"
    for trade in trades:
        emoji = "✅" if trade['pnl_usdt'] >= 0 else "🛑"
        text += f"{emoji} `{trade['symbol']}` | PNL: `${trade['pnl_usdt']:,.2f}`\n"
        
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
    ]))

async def show_diagnostics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري إجراء فحص حي...")
    
    okx_status, okx_time_str = "Disconnected 🔴", "N/A"
    try:
        okx_status = "Connected 🟢"; okx_time_str = datetime.fromtimestamp(await worker_state.exchange.fetch_time() / 1000, EGYPT_TZ).strftime('%H:%M:%S')
    except Exception: okx_status = f"Disconnected 🔴"

    redis_status = "Disconnected 🔴"
    try:
        redis_client = redis.from_url(REDIS_URL); await redis_client.ping(); redis_status = "Connected & Listening 🟢"; await redis_client.close()
    except Exception: pass
    
    report = (f"🕵️‍♂️ **تقرير التشخيص المباشر**\n\n"
            f"**🦾 بوت الذراع (هذا البوت):**\n"
            f"- اتصال بـ OKX: **{okx_status}** (توقيت: `{okx_time_str}`)\n"
            f"- اتصال بـ Redis: **{redis_status}**")
    await query.edit_message_text(report, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]]))

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Dashboard 🖥️":
        await show_dashboard_message(update.message)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; data = query.data
    
    routes = {
        "show_brain_mirror": show_brain_mirror,
        "show_portfolio": show_portfolio,
        "show_active_trades": show_active_trades,
        "show_history": show_history,
        "show_diagnostics": show_diagnostics,
        "back_to_dashboard": show_dashboard_message
    }
    
    if data in routes:
        await routes[data](update, context)
    elif data.startswith("trade_details_"):
        await show_trade_details(update, context)
    elif data.startswith("close_trade_"):
        await close_trade_handler(update, context)

async def post_init(application: Application):
    worker_state.telegram_app = application
    await safe_send_message("*🤖 بوت المرآة (الكامل) قيد التشغيل...*")
    
    try:
        worker_state.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE})
        await worker_state.exchange.load_markets()
    except Exception as e:
        logger.critical(f"FATAL: Could not connect to OKX: {e}")
        await safe_send_message(f"❌ **فشل قاتل في الاتصال بـ OKX:**\n`{e}`"); return

    await init_database()
    asyncio.create_task(redis_listener())
    logger.info("Worker Mirror Bot is running.")

async def main():
    if not all([OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE, REDIS_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Please check your .env file.")
        return
    
    lock_key = f"bot_lock:{TELEGRAM_BOT_TOKEN}"
    redis_client = redis.from_url(REDIS_URL)
    
    if not await acquire_lock(redis_client, lock_key):
        logger.warning(f"Another instance is already running (lock '{lock_key}' is held). This instance will not start.")
        await redis_client.close()
        return

    logger.info(f"Successfully acquired lock '{lock_key}'. This instance is now the primary.")
    
    async def refresh_lock(context: ContextTypes.DEFAULT_TYPE):
        try:
            await redis_client.expire(lock_key, 30)
        except Exception as e:
            logger.error(f"Could not refresh lock: {e}")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    if not app.job_queue:
        logger.critical("JobQueue is not available. Please install dependency: pip install \"python-telegram-bot[job-queue]\"")
        await redis_client.delete(lock_key)
        await redis_client.close()
        return

    app.job_queue.run_repeating(refresh_lock, interval=15)
    
    # ربط المعالجات
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(button_handler))
    
    # الربط بعد إنشاء التطبيق
    await app.post_init(app)

    try:
        logger.info("Starting Telegram polling...")
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        
        # حلقة لا نهائية لإبقاء البرنامج يعمل
        while True:
            await asyncio.sleep(3600)

    except Conflict:
        logger.critical("TELEGRAM CONFLICT: Another instance of the bot is running. Shutting down.")
    except Exception as e:
        logger.critical(f"An unexpected error occurred in the main loop: {e}", exc_info=True)
    finally:
        logger.info("Releasing lock before shutting down...")
        await redis_client.delete(lock_key)
        await redis_client.close()
        logger.info("Lock released. Shutdown complete.")

if __name__ == '__main__':
    asyncio.run(main())

