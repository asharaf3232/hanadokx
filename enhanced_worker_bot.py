# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🦾 OKX Enhanced Worker Bot | v2.3 (Final Fix) 🦾 ---
# =======================================================================================
#
# --- v2.3 Changelog ---
#   ✅ [إصلاح] إصلاح خطأ 'total' الذي كان يحدث عند عرض المحفظة.
#   ✅ [إصلاح] تصحيح مسار استثناءات مكتبة Redis لتجنب خطأ AttributeError.
#   ✅ [تحسين] تحسين نظام تسجيل الأحداث (Logging) ليكون أكثر استقرارًا.
#
# =======================================================================================

import asyncio
import os
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo
import aiosqlite
import ccxt.async_support as ccxt
import redis
import websockets.exceptions
from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, CallbackQueryHandler
from telegram.error import BadRequest

# --- إعدادات التسجيل (Logging) ---
class SafeFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'trade_id'): record.trade_id = 'N/A'
        if not hasattr(record, 'worker_id'): record.worker_id = 'N/A'
        return super().format(record)

log_formatter = SafeFormatter('%(asctime)s - %(levelname)s - [%(worker_id)s] - [TradeID:%(trade_id)s] - %(message)s')
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
root_logger = logging.getLogger(); root_logger.handlers = [log_handler]; root_logger.setLevel(logging.INFO)

class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'extra' not in kwargs: kwargs['extra'] = {}
        if 'trade_id' not in kwargs['extra']: kwargs['extra']['trade_id'] = 'N/A'
        return msg, kwargs

# --- تحميل متغيرات البيئة ---
load_dotenv()

# --- إعدادات البوت العامل ---
WORKER_ID = os.getenv('WORKER_ID', 'worker_01')
OKX_API_KEY = os.getenv('WORKER_OKX_API_KEY')
OKX_API_SECRET = os.getenv('WORKER_OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('WORKER_OKX_API_PASSPHRASE')
REDIS_URL = os.getenv('REDIS_URL')
WORKER_TELEGRAM_BOT_TOKEN = os.getenv('WORKER_TELEGRAM_BOT_TOKEN')
WORKER_TELEGRAM_CHAT_ID = os.getenv('WORKER_TELEGRAM_CHAT_ID')
TRADE_SIZE_USDT = float(os.getenv('WORKER_TRADE_SIZE_USDT', '15.0'))
RISK_REWARD_RATIO = float(os.getenv('WORKER_RISK_REWARD_RATIO', '2.0'))

REDIS_CHANNEL = "trade_signals"
DB_FILE = f'okx_worker_{WORKER_ID}.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")

logger = ContextAdapter(logging.getLogger(__name__), {'worker_id': WORKER_ID})

# --- الحالة العامة للبوت ---
class BotState:
    def __init__(self):
        self.application = None
        self.exchange = None
        self.redis_client = None
        self.redis_connected = False
        self.last_signal_received_at = None
        self.trade_guardian = None
        self.public_ws = None

bot_data = BotState()
trade_management_lock = asyncio.Lock()

# --- وظائف مساعدة ---
async def safe_send_message(text, **kwargs):
    try:
        if bot_data.application and WORKER_TELEGRAM_CHAT_ID:
            await bot_data.application.bot.send_message(WORKER_TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e:
        logger.error(f"Telegram Send Error: {e}")

async def safe_edit_message(query, text, **kwargs):
    try:
        await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.warning(f"Edit Message Error: {e}")
    except Exception as e:
        logger.error(f"Edit Message Error: {e}")

# --- إدارة قاعدة البيانات ---
async def init_database():
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    entry_price REAL,
                    take_profit REAL,
                    stop_loss REAL,
                    quantity REAL,
                    status TEXT,
                    order_id TEXT,
                    close_price REAL,
                    pnl_usdt REAL
                )
            ''')
            await conn.commit()
        logger.info("Worker database initialized successfully.")
    except Exception as e:
        logger.critical(f"Worker database initialization failed: {e}")

async def log_pending_trade_to_db(signal, buy_order):
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute('''
                INSERT INTO trades (timestamp, symbol, order_id, status, entry_price, take_profit, stop_loss) 
                VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                datetime.now(EGYPT_TZ).isoformat(),
                signal['symbol'],
                buy_order['id'],
                'pending',
                signal['entry_price'],
                signal['take_profit'],
                signal['stop_loss']
            ))
            await conn.commit()
        logger.info(f"Logged pending trade for {signal['symbol']} with order ID {buy_order['id']}.")
        return True
    except Exception as e:
        logger.error(f"DB Log Pending Error: {e}"); return False

# --- منطق تنفيذ وإدارة الصفقات ---
async def activate_trade(order_id, symbol):
    log_ctx = {'trade_id': 'N/A'}
    try:
        order_details = await bot_data.exchange.fetch_order(order_id, symbol)
        filled_price = order_details.get('average', 0.0)
        net_filled_quantity = order_details.get('filled', 0.0)

        if net_filled_quantity <= 0 or filled_price <= 0:
            logger.error(f"Order {order_id} invalid fill data."); return

        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trade = await (await conn.execute("SELECT * FROM trades WHERE order_id = ? AND status = 'pending'", (order_id,))).fetchone()
            if not trade:
                logger.info(f"Activation ignored for {order_id}: Trade not pending."); return
            
            trade = dict(trade)
            log_ctx['trade_id'] = trade['id']
            logger.info(f"Activating trade #{trade['id']} for {symbol}...", extra=log_ctx)
            
            risk = filled_price - trade['stop_loss']
            new_take_profit = filled_price + (risk * RISK_REWARD_RATIO)

            await conn.execute('''
                UPDATE trades SET status = 'active', entry_price = ?, quantity = ?, take_profit = ? 
                WHERE id = ?
            ''', (filled_price, net_filled_quantity, new_take_profit, trade['id']))
            await conn.commit()

        await bot_data.public_ws.subscribe([symbol])
        
        success_msg = (f"✅ **[W:{WORKER_ID}] تم تأكيد الشراء | {symbol}**\n\n"
                       f"🔸 **الصفقة رقم:** #{trade['id']}\n"
                       f"🔸 **سعر التنفيذ:** `${filled_price:,.4f}`\n"
                       f"🎯 **الهدف (TP):** `${new_take_profit:,.4f}`\n"
                       f"🛡️ **الوقف (SL):** `${trade['stop_loss']:,.4f}`")
        await safe_send_message(success_msg)

    except Exception as e:
        logger.error(f"Could not activate trade {order_id}: {e}", exc_info=True, extra=log_ctx)


class TradeGuardian:
    def __init__(self, application):
        self.application = application

    async def handle_ticker_update(self, ticker_data):
        async with trade_management_lock:
            symbol = ticker_data['instId'].replace('-', '/')
            current_price = float(ticker_data['last'])
            try:
                async with aiosqlite.connect(DB_FILE) as conn:
                    conn.row_factory = aiosqlite.Row
                    trade = await (await conn.execute("SELECT * FROM trades WHERE symbol = ? AND status = 'active'", (symbol,))).fetchone()
                    if not trade:
                        return
                    trade = dict(trade)

                if current_price >= trade['take_profit']:
                    await self._close_trade(trade, "ناجحة (TP)", current_price)
                elif current_price <= trade['stop_loss']:
                    await self._close_trade(trade, "فاشلة (SL)", current_price)
            except Exception as e:
                logger.error(f"Guardian Ticker Error for {symbol}: {e}", exc_info=True)

    async def _close_trade(self, trade, reason, close_price):
        symbol, trade_id = trade['symbol'], trade['id']
        log_ctx = {'trade_id': trade_id}
        logger.info(f"Guardian: Closing {symbol}. Reason: {reason}", extra=log_ctx)
        
        try:
            balance = await bot_data.exchange.fetch_balance()
            asset_to_sell = symbol.split('/')[0]
            available_quantity = balance.get(asset_to_sell, {}).get('free', 0.0)

            if available_quantity <= 0:
                logger.warning(f"No balance for {asset_to_sell} to close trade #{trade_id}. Assuming it was manually closed.", extra=log_ctx)
                reason = "مغلقة يدوياً"
            else:
                 await bot_data.exchange.create_market_sell_order(symbol, available_quantity)

            pnl = (close_price - trade['entry_price']) * trade['quantity']
            pnl_percent = (close_price / trade['entry_price'] - 1) * 100 if trade['entry_price'] > 0 else 0
            emoji = "✅" if pnl >= 0 else "🛑"
            
            async with aiosqlite.connect(DB_FILE) as conn:
                await conn.execute("UPDATE trades SET status = ?, close_price = ?, pnl_usdt = ? WHERE id = ?", 
                                   (reason, close_price, pnl, trade_id))
                await conn.commit()
            
            await bot_data.public_ws.unsubscribe([symbol])

            msg = (f"{emoji} **[W:{WORKER_ID}] تم إغلاق الصفقة | #{trade_id} {symbol}**\n"
                   f"**السبب:** {reason}\n"
                   f"**الربح/الخسارة:** `${pnl:,.2f}` ({pnl_percent:+.2f}%)")
            await safe_send_message(msg)

        except Exception as e:
            logger.error(f"Failed to close trade #{trade_id}: {e}", exc_info=True, extra=log_ctx)
            await safe_send_message(f"🚨 **[W:{WORKER_ID}] فشل حرج** 🚨\nفشل إغلاق الصفقة `#{trade_id}`. الرجاء مراجعة المنصة يدوياً.")
            
    async def sync_subscriptions(self):
        try:
            async with aiosqlite.connect(DB_FILE) as conn:
                active_symbols_rows = await (await conn.execute("SELECT DISTINCT symbol FROM trades WHERE status = 'active'")).fetchall()
                active_symbols = [row[0] for row in active_symbols_rows]
            if active_symbols:
                logger.info(f"Guardian: Syncing subscriptions for: {active_symbols}")
                await bot_data.public_ws.subscribe(active_symbols)
        except Exception as e:
            logger.error(f"Guardian Sync Error: {e}")


class PublicWebSocketManager:
    def __init__(self, handler_coro): self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"; self.handler = handler_coro; self.subscriptions = set()
    async def _send_op(self, op, symbols):
        if not symbols or not hasattr(self, 'websocket') or not self.websocket: return
        try: await self.websocket.send(json.dumps({"op": op, "args": [{"channel": "tickers", "instId": s.replace('/', '-')} for s in symbols]}))
        except websockets.exceptions.ConnectionClosed: logger.warning(f"Could not send '{op}' op; ws is closed.")
    async def subscribe(self, symbols):
        new = [s for s in symbols if s not in self.subscriptions]
        if new: await self._send_op('subscribe', new); self.subscriptions.update(new); logger.info(f"👁️ [Guardian] Now watching: {new}")
    async def unsubscribe(self, symbols):
        old = [s for s in symbols if s in self.subscriptions]
        if old: await self._send_op('unsubscribe', old); [self.subscriptions.discard(s) for s in old]; logger.info(f"👁️ [Guardian] Stopped watching: {old}")
    async def _run_loop(self):
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            self.websocket = ws; logger.info("✅ [Guardian's Eyes] Connected.")
            if self.subscriptions: await self.subscribe(list(self.subscriptions))
            async for msg in ws:
                if msg == 'ping': await ws.send('pong'); continue
                data = json.loads(msg)
                if data.get('arg', {}).get('channel') == 'tickers' and 'data' in data:
                    for ticker in data['data']: await self.handler(ticker)
    async def run(self):
        while True:
            try: await self._run_loop()
            except Exception as e: logger.error(f"Public WS failed: {e}. Retrying..."); await asyncio.sleep(5)


# --- منطق استقبال الإشارات وتنفيذها ---
async def execute_trade_from_signal(signal):
    symbol = signal.get('symbol')
    entry_price = signal.get('entry_price')
    
    if not symbol or not entry_price:
        logger.error(f"Received invalid signal: {signal}")
        return

    try:
        logger.info(f"Received valid signal for {symbol}. Preparing to execute.")
        amount = TRADE_SIZE_USDT / entry_price
        order = await bot_data.exchange.create_market_buy_order(symbol, amount)
        logger.info(f"Placed market buy order for {symbol}. Order ID: {order['id']}")
        await log_pending_trade_to_db(signal, order)

    except ccxt.InsufficientFunds as e:
        logger.error(f"Insufficient funds for {symbol}. Error: {e}")
        await safe_send_message(f"🚨 **[W:{WORKER_ID}] رصيد غير كافٍ** 🚨\nفشل تنفيذ صفقة `{symbol}`.")
    except Exception as e:
        logger.error(f"Trade execution failed for {symbol}: {e}", exc_info=True)

# --- مستمع Redis ---
async def redis_listener():
    if not REDIS_URL:
        logger.critical("REDIS_URL is not set in the environment variables. The worker cannot start.")
        return
        
    while True:
        try:
            r = redis.from_url(REDIS_URL, health_check_interval=30)
            await r.ping()
            bot_data.redis_connected = True
            logger.info("Successfully connected to Redis.")
            
            pubsub = r.pubsub()
            await pubsub.subscribe(REDIS_CHANNEL)
            logger.info(f"Subscribed to Redis channel '{REDIS_CHANNEL}'. Waiting for signals...")

            async for message in pubsub.listen():
                if message['type'] == 'message':
                    bot_data.last_signal_received_at = datetime.now(EGYPT_TZ)
                    signal_data = json.loads(message['data'])
                    logger.info(f"Received new signal: {signal_data}")
                    asyncio.create_task(execute_trade_from_signal(signal_data))

        except (redis.ConnectionError, redis.exceptions.InvalidResponse) as e:
            logger.error(f"Redis connection lost: {e}. Reconnecting in 5 seconds...")
            bot_data.redis_connected = False
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"An error occurred in the Redis listener: {e}. Restarting loop in 10 seconds...")
            bot_data.redis_connected = False
            await asyncio.sleep(10)

# --- واجهة تليجرام ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(f"👋 أهلاً بك في بوت العامل **{WORKER_ID}**.\nاضغط /dashboard لعرض لوحة التحكم.")

async def show_dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    status_emoji = "✅" if bot_data.redis_connected else "❌"
    
    keyboard = [
        [InlineKeyboardButton(f"الحالة: متصل بالعقل {status_emoji}", callback_data="status")],
        [InlineKeyboardButton("محفظة هذا الحساب 💼", callback_data="portfolio")],
        [InlineKeyboardButton("الصفقات النشطة 📈", callback_data="active_trades")],
        [InlineKeyboardButton("سجل الصفقات 📜", callback_data="history")]
    ]
    message_text = f"🖥️ **لوحة تحكم العامل: {WORKER_ID}**"
    
    target_message = update.message or update.callback_query.message
    if update.callback_query:
        await safe_edit_message(update.callback_query, message_text, reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await target_message.reply_text(message_text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    status_text = "متصل ✅" if bot_data.redis_connected else "غير متصل ❌"
    last_signal_time = bot_data.last_signal_received_at.strftime('%Y-%m-%d %H:%M:%S') if bot_data.last_signal_received_at else "لم يتم استلام أي إشارة بعد"
    
    text = (f"**📡 حالة الاتصال بالعقل**\n\n"
            f"**معرف العامل:** `{WORKER_ID}`\n"
            f"**حالة Redis:** {status_text}\n"
            f"**آخر إشارة تم استلامها:** {last_signal_time}")
            
    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="status")], [InlineKeyboardButton("🔙 عودة", callback_data="dashboard")]]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer("جاري جلب بيانات المحفظة...")
    try:
        balance = await bot_data.exchange.fetch_balance()
        total_usdt_equity = balance.get('USDT', {}).get('total', 0)
        text = f"**💼 محفظة الحساب ({WORKER_ID})**\n\n**إجمالي الرصيد:** `${total_usdt_equity:,.2f}` USDT\n\n**الأصول الأخرى:**\n"
        
        # --- [إصلاح v2.2] تعديل طريقة قراءة الأرصدة ---
        assets = []
        if 'info' in balance and 'totalEq' in balance['info']:
            for asset_data in balance['info'].get('details', []):
                asset = asset_data.get('ccy')
                total = float(asset_data.get('eq', 0))
                if total > 0.01 and asset != 'USDT': # عرض الأصول التي تزيد قيمتها عن سنت واحد
                    assets.append(f"- **{asset}:** `{total}`")
        
        text += "\n".join(assets) if assets else "لا توجد أصول أخرى."

    except Exception as e:
        logger.error(f"Portfolio fetch error: {e}", exc_info=True)
        text = f"حدث خطأ أثناء جلب المحفظة: {e}"
        
    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="portfolio")], [InlineKeyboardButton("🔙 عودة", callback_data="dashboard")]]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_active_trades_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trades = await (await conn.execute("SELECT * FROM trades WHERE status = 'active' ORDER BY id DESC")).fetchall()
    
    if not trades:
        text = "لا توجد صفقات نشطة حاليًا."
    else:
        text = "📈 **الصفقات النشطة:**\n\n"
        for trade in trades:
            entry_price = trade['entry_price']
            try:
                ticker = await bot_data.exchange.fetch_ticker(trade['symbol'])
                current_price = ticker['last']
                pnl_percent = (current_price / entry_price - 1) * 100
                pnl_str = f"({pnl_percent:+.2f}%)"
            except:
                pnl_str = ""
            text += f"- `#{trade['id']}` **{trade['symbol']}** {pnl_str}\n"

    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="active_trades")], [InlineKeyboardButton("🔙 عودة", callback_data="dashboard")]]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_history_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    async with aiosqlite.connect(DB_FILE) as conn:
        conn.row_factory = aiosqlite.Row
        trades = await (await conn.execute("SELECT * FROM trades WHERE status != 'active' AND status != 'pending' ORDER BY id DESC LIMIT 10")).fetchall()

    if not trades:
        text = "لا يوجد سجل للصفقات المغلقة."
    else:
        text = "📜 **آخر 10 صفقات مغلقة:**\n\n"
        for trade in trades:
            pnl = trade['pnl_usdt'] or 0.0
            emoji = "✅" if pnl >= 0 else "🛑"
            text += f"{emoji} `#{trade['id']}` **{trade['symbol']}** | PNL: `${pnl:,.2f}`\n"
            
    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="history")], [InlineKeyboardButton("🔙 عودة", callback_data="dashboard")]]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    route_map = {
        "dashboard": show_dashboard_command,
        "status": show_status_command,
        "portfolio": show_portfolio_command,
        "active_trades": show_active_trades_command,
        "history": show_history_command,
    }
    if data in route_map:
        await route_map[data](update, context)

# --- التشغيل الرئيسي ---
async def post_init(application: Application):
    bot_data.application = application
    
    if not all([OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE, WORKER_TELEGRAM_BOT_TOKEN, WORKER_TELEGRAM_CHAT_ID]):
        logger.critical("FATAL: Missing one or more required environment variables for the worker.")
        return

    try:
        bot_data.exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE, 'enableRateLimit': True})
        await bot_data.exchange.load_markets()
        logger.info("Successfully connected to OKX.")
    except Exception as e:
        logger.critical(f"Could not connect to OKX: {e}", exc_info=True); return
        
    bot_data.trade_guardian = TradeGuardian(application)
    bot_data.public_ws = PublicWebSocketManager(bot_data.trade_guardian.handle_ticker_update)

    asyncio.create_task(redis_listener())
    asyncio.create_task(bot_data.public_ws.run())
    
    await asyncio.sleep(5)
    await bot_data.trade_guardian.sync_subscriptions()

    await safe_send_message(f"✅ **[W:{WORKER_ID}] بوت العامل بدأ العمل بنجاح.**\nاضغط /dashboard لعرض لوحة التحكم.")
    logger.info(f"--- Worker Bot '{WORKER_ID}' is now fully operational ---")

async def post_shutdown(application: Application):
    if bot_data.exchange: await bot_data.exchange.close()
    if bot_data.redis_client: await bot_data.redis_client.close()
    logger.info(f"--- Worker Bot '{WORKER_ID}' has shut down. ---")

def main():
    asyncio.run(init_database())
    app_builder = Application.builder().token(WORKER_TELEGRAM_BOT_TOKEN)
    app_builder.post_init(post_init).post_shutdown(post_shutdown)
    application = app_builder.build()
    
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("dashboard", show_dashboard_command))
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    
    application.run_polling()

if __name__ == '__main__':
    main()

