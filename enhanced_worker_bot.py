# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🤖 OKX Worker Pro Bot | v2.0 (Full UI & Diagnostics) 🤖 ---
# =======================================================================================
# v2.0 Changelog:
#   ✅ [ميزة] متابعة كاملة للصفقات النشطة مع حساب الأرباح والخسائر اللحظية (PNL).
#   ✅ [ميزة] سجل تاريخي للصفقات المغلقة مع ملخص للأداء.
#   ✅ [ميزة] نظام إغلاق يدوي للصفقات النشطة مباشرة من التليجرام.
#   ✅ [ميزة] تشخيص ذكي يستمع لـ "نبضات قلب" البوت الرئيسي للتأكد من أنه حي ويعمل.
#   ✅ [هيكلة] إعادة بناء شاملة للكود ليكون أكثر قوة واستقراراً.
# =======================================================================================

import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import aiosqlite
import ccxt.async_support as ccxt
import redis.asyncio as redis
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.error import BadRequest

# --- ⚙️ الإعدادات والتهيئة ⚙️ ---
load_dotenv()

OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')
REDIS_URL = os.getenv('REDIS_URL')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DB_FILE = 'my_trades_v2.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))
HEARTBEAT_INTERVAL_SECONDS = 3600

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 🏦 إدارة الحالة والاتصالات 🏦 ---
class WorkerState:
    def __init__(self):
        self.exchange = None
        self.redis_client = None
        self.telegram_app = None
        self.last_brain_heartbeat = None
        self.brain_last_scan_info = {}

worker_state = WorkerState()

# --- 🗃️ إدارة قاعدة البيانات المحلية 🗃️ ---
async def init_database():
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT, symbol TEXT, entry_price REAL,
                quantity REAL, status TEXT, close_price REAL,
                pnl_usdt REAL, okx_order_id TEXT
            )''')
        await conn.commit()
    logger.info("Local database initialized successfully.")

# --- 📡 قلب البوت: مستمع Redis ومنفذ الصفقات ---
async def execute_trade(signal):
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        # التأكد من عدم وجود صفقة مفتوحة لنفس العملة
        async with aiosqlite.connect(DB_FILE) as conn:
            cursor = await conn.execute("SELECT id FROM trades WHERE symbol = ? AND status = 'active'", (symbol,))
            if await cursor.fetchone():
                logger.warning(f"Signal for {symbol} ignored. Active trade already exists.")
                await safe_send_message(f"⚠️ تم تجاهل إشارة لـ `{symbol}` لوجود صفقة نشطة بالفعل.")
                return

        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = worker_state.exchange.amount_to_precision(symbol, amount_to_buy)

        logger.info(f"Signal for {symbol}. Placing market buy order for {formatted_amount} units.")
        order = await worker_state.exchange.create_market_buy_order(symbol, formatted_amount)
        
        await asyncio.sleep(2) # انتظار بسيط للتأكد من تحديث بيانات الأمر
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
             logger.warning(f"Order {order['id']} for {symbol} was placed but seems not filled yet.")
             await safe_send_message(f"🟡 تم إرسال أمر شراء لـ `{symbol}` ولكنه لم يتأكد بعد.")

    except Exception as e:
        logger.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        await safe_send_message(f"🚨 فشل تنفيذ صفقة لـ `{signal['symbol']}`. السبب: {e}")

async def redis_listener():
    while True:
        try:
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            worker_state.redis_client = redis_client
            await redis_client.ping()
            
            pubsub = redis_client.pubsub()
            await pubsub.subscribe(trade_signals="trade_signals", brain_heartbeat="brain_heartbeat")
            logger.info("Redis listener connected to channels: trade_signals, brain_heartbeat")
            await safe_send_message("✅ **اتصال Redis ناجح.** البوت يستمع الآن للإشارات ونبضات القلب.")

            async for message in pubsub.listen():
                if message['type'] != 'message': continue
                
                channel = message['channel']
                data = message['data']
                
                if channel == 'trade_signals':
                    try:
                        signal_data = json.loads(data)
                        logger.info(f"Signal received: {signal_data['symbol']}")
                        await execute_trade(signal_data)
                    except Exception as e: logger.error(f"Error processing trade signal: {e}")
                
                elif channel == 'brain_heartbeat':
                    try:
                        heartbeat_data = json.loads(data)
                        worker_state.last_brain_heartbeat = datetime.fromisoformat(heartbeat_data['timestamp_utc'])
                        worker_state.brain_last_scan_info = heartbeat_data.get('last_scan_info', {})
                        # logger.info("Brain heartbeat received.") # Optional: can be noisy
                    except Exception as e: logger.error(f"Error processing brain heartbeat: {e}")

        except Exception as e:
            logger.error(f"Redis connection failed: {e}. Reconnecting in 15 seconds...")
            await safe_send_message(f"🔴 **انقطع اتصال Redis.** جارِ محاولة إعادة الاتصال...")
            await asyncio.sleep(15)

# --- 🤖 واجهة تليجرام 🤖 ---
async def safe_send_message(text, **kwargs):
    try:
        await worker_state.telegram_app.bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e: logger.error(f"Telegram Send Error: {e}")

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["Dashboard 🖥️"]]
    await update.message.reply_text("أهلاً بك في **بوت OKX المنفذ (Pro)**.\n",
                                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True))
    await show_dashboard_message(update.message)

async def show_dashboard_message(message_or_update):
    keyboard = [
        [InlineKeyboardButton("💼 المحفظة", callback_data="show_portfolio")],
        [InlineKeyboardButton("📈 الصفقات النشطة", callback_data="show_active_trades")],
        [InlineKeyboardButton("📜 سجل الصفقات", callback_data="show_history")],
        [InlineKeyboardButton("🕵️‍♂️ التشخيص", callback_data="show_diagnostics")],
    ]
    target_message = message_or_update
    if hasattr(message_or_update, 'message'): # Handle CallbackQuery
        target_message = message_or_update.message
    
    await target_message.reply_text("🖥️ **لوحة التحكم**", reply_markup=InlineKeyboardMarkup(keyboard))

async def show_active_trades(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        cursor = await conn.execute("SELECT * FROM trades WHERE status = 'active' ORDER BY id DESC")
        active_trades = await cursor.fetchall()
    
    if not active_trades:
        await query.edit_message_text("✅ لا توجد صفقات نشطة حالياً.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ]))
        return

    text = "📈 **الصفقات النشطة:**\n\n"
    symbols = [trade['symbol'] for trade in active_trades]
    tickers = await worker_state.exchange.fetch_tickers(symbols)
    
    for trade in active_trades:
        current_price = tickers.get(trade['symbol'], {}).get('last', 0)
        pnl = (current_price - trade['entry_price']) * trade['quantity']
        pnl_percent = ((current_price / trade['entry_price']) - 1) * 100
        emoji = "🔼" if pnl >= 0 else "🔽"
        text += (f"🔹 `{trade['symbol']}` | PNL: `${pnl:+.2f}` ({pnl_percent:+.2f}%) {emoji}\n"
                 f"   *الدخول:* `${trade['entry_price']:,.4f}`\n"
                 f"   [تفاصيل وإغلاق...](callback:trade_details_{trade['id']})\n\n")

    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 تحديث", callback_data="show_active_trades")],
        [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
    ]))

async def close_trade_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; trade_id = int(query.data.split('_')[2])
    await query.answer("جاري إرسال أمر الإغلاق...")
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trade = await (await conn.execute("SELECT * FROM trades WHERE id = ? AND status = 'active'", (trade_id,))).fetchone()
        
        if not trade:
            await query.edit_message_text("⚠️ لم يتم العثور على الصفقة أو تم إغلاقها بالفعل."); return

        sell_order = await worker_state.exchange.create_market_sell_order(trade['symbol'], trade['quantity'])
        await asyncio.sleep(2)
        filled_order = await worker_state.exchange.fetch_order(sell_order['id'], trade['symbol'])
        
        close_price = filled_order.get('average', 0)
        pnl = (close_price - trade['entry_price']) * trade['quantity']
        
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("UPDATE trades SET status = 'closed', close_price = ?, pnl_usdt = ? WHERE id = ?", 
                               (close_price, pnl, trade_id))
            await conn.commit()

        emoji = "✅" if pnl >= 0 else "🛑"
        await query.edit_message_text(f"{emoji} **تم إغلاق الصفقة بنجاح**\n\n"
                                      f"**العملة:** `{trade['symbol']}`\n"
                                      f"**ربح/خسارة:** `${pnl:,.2f}`",
                                      parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
                                          [InlineKeyboardButton("📈 العودة للصفقات النشطة", callback_data="show_active_trades")]
                                      ]))
    except Exception as e:
        logger.error(f"Failed to close trade #{trade_id}: {e}", exc_info=True)
        await query.message.reply_text(f"🚨 فشل إغلاق الصفقة #{trade_id}: {e}")

async def show_history(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trades = await (await conn.execute("SELECT * FROM trades WHERE status = 'closed' ORDER BY id DESC LIMIT 15")).fetchall()
        
        total_pnl_data = await (await conn.execute("SELECT SUM(pnl_usdt) FROM trades WHERE status = 'closed'")).fetchone()
        total_pnl = total_pnl_data[0] or 0.0
    
    if not trades:
        await query.edit_message_text("ℹ️ لا يوجد سجل للصفقات المغلقة بعد.", reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
        ]))
        return

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
    
    # فحص اتصال OKX
    okx_status, okx_time_str = "Disconnected 🔴", "N/A"
    try:
        okx_status = "Connected 🟢"; okx_time_str = datetime.fromtimestamp(await worker_state.exchange.fetch_time() / 1000, EGYPT_TZ).strftime('%H:%M:%S')
    except Exception as e: okx_status = f"Disconnected 🔴"

    # فحص اتصال Redis
    redis_status = "Disconnected 🔴"
    try: await worker_state.redis_client.ping(); redis_status = "Connected & Listening 🟢"
    except Exception: pass

    # فحص نبض قلب البوت الرئيسي
    brain_status, brain_last_heard, brain_scan_info = "Offline 🔴", "Never", "N/A"
    if worker_state.last_brain_heartbeat:
        now_utc = datetime.utcnow()
        time_diff_seconds = (now_utc - worker_state.last_brain_heartbeat.replace(tzinfo=None)).total_seconds()
        brain_last_heard = f"{int(time_diff_seconds)} ثانية مضت"
        if time_diff_seconds < 300: # 5 دقائق
            brain_status = "Online 🟢"
            scan_time = worker_state.brain_last_scan_info.get('start_time', 'N/A')
            scan_symbols = worker_state.brain_last_scan_info.get('checked_symbols', 'N/A')
            brain_scan_info = f"آخر فحص: {scan_time} ({scan_symbols} عملة)"
        else:
            brain_status = f"Delayed 🟡 (آخر نبضة منذ {int(time_diff_seconds/60)} دقيقة)"
            
    report = (f"🕵️‍♂️ **تقرير التشخيص المباشر**\n\n"
              f"**🤖 بوت العقل (الرئيسي):**\n"
              f"- الحالة: **{brain_status}**\n"
              f"- آخر نبضة: *{brain_last_heard}*\n"
              f"- معلومات الفحص: *{brain_scan_info}*\n\n"
              f"**🦾 بوت الذراع (هذا البوت):**\n"
              f"- اتصال بـ OKX: **{okx_status}** (توقيت: `{okx_time_str}`)\n"
              f"- اتصال بـ Redis: **{redis_status}**")
              
    await query.edit_message_text(report, parse_mode=ParseMode.MARKDOWN, reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("🔄 تحديث", callback_data="show_diagnostics")],
        [InlineKeyboardButton("🔙 العودة", callback_data="back_to_dashboard")]
    ]))

async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message.text == "Dashboard 🖥️":
        await show_dashboard_message(update.message)

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query; data = query.data
    
    routes = {
        "show_portfolio": show_portfolio,
        "show_active_trades": show_active_trades,
        "show_history": show_history,
        "show_diagnostics": show_diagnostics,
        "back_to_dashboard": lambda u, c: show_dashboard_message(u)
    }
    
    if data in routes:
        await routes[data](update, context)
    elif data.startswith("close_trade_"):
        await close_trade_handler(update, context)

async def post_init(application: Application):
    worker_state.telegram_app = application
    await safe_send_message("*🤖 بوت OKX المنفذ (Pro) قيد التشغيل...*")
    
    try:
        worker_state.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE})
        await worker_state.exchange.load_markets()
    except Exception as e:
        logger.critical(f"FATAL: Could not connect to OKX: {e}", exc_info=True)
        await safe_send_message(f"❌ **فشل قاتل في الاتصال بـ OKX:**\n`{e}`\n\nالبوت سيتوقف."); return

    await init_database()
    asyncio.create_task(redis_listener())
    logger.info("Worker Pro Bot is running.")

def main():
    if not all([OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE, REDIS_URL, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Please check your .env file. All variables are required.")
        return
        
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.run_polling()

if __name__ == '__main__':
    main()

