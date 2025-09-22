# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🚀 OKX Sniper Bot | v32.2 (Integrated Edition) 🚀 ---
# =======================================================================================
#
# هذا الإصدار يدمج "العقل" و"الأيدي" في كود واحد متكامل، مع الاقتراحات المحسنة.
# يمكن تشغيله كـ broadcaster (عقل) أو worker (يد) بناءً على الإعدادات في .env (MODE=broadcaster أو worker).
#
# --- Integrated Changelog v32.2 ---
#   ✅ [دمج] دمج الكود في ملف واحد مع فصول واضحة.
#   ✅ [تحسين] إضافة UUID للإشارات، قفل Redis لتجنب التكرار.
#   ✅ [تحسين] إعادة اتصال تلقائي لـ Redis مع backoff.
#   ✅ [تحسين] دعم Trailing SL في الـ worker.
#   ✅ [تحسين] Validation للإشارات و base64 encoding.
#   ✅ [تحسين] Caching لـ fetch_ticker.
#   ✅ [تحسين] Acknowledgement من الـ worker إلى قناة 'trade_ack'.
#
# =======================================================================================

import asyncio
import os
import logging
import json
import re
import time
import random
from datetime import datetime, timedelta, timezone, time as dt_time
from zoneinfo import ZoneInfo
import hmac
import base64
from collections import defaultdict, Counter
import copy
import uuid
from functools import lru_cache

import aiosqlite
import websockets
import websockets.exceptions
import httpx
import feedparser
import pandas as pd
import pandas_ta as ta
import ccxt.async_support as ccxt
import redis.asyncio as redis

try:
    import nltk
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    NLTK_AVAILABLE = True
except ImportError:
    NLTK_AVAILABLE = False
    logging.warning("NLTK not found. News sentiment analysis will be disabled.")

try:
    from scipy.signal import find_peaks
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False
    logging.warning("Library 'scipy' not found. RSI Divergence strategy will be disabled.")

from telegram import Update, ReplyKeyboardMarkup, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
from telegram.error import BadRequest, TimedOut, Forbidden
from dotenv import load_dotenv

# --- Core Configuration ---
load_dotenv()

MODE = os.getenv('MODE', 'broadcaster')  # 'broadcaster' for العقل, 'worker' for الأيدي
WORKER_ID = os.getenv('WORKER_ID', 'worker_01') if MODE == 'worker' else 'broadcaster'

OKX_API_KEY = os.getenv('OKX_API_KEY') if MODE == 'broadcaster' else os.getenv('WORKER_OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET') if MODE == 'broadcaster' else os.getenv('WORKER_OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE') if MODE == 'broadcaster' else os.getenv('WORKER_OKX_API_PASSPHRASE')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN') if MODE == 'broadcaster' else os.getenv('WORKER_TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID') if MODE == 'broadcaster' else os.getenv('WORKER_TELEGRAM_CHAT_ID')
GEMINI_API_KEY = os.getenv('GEMINI_API_KEY')
ALPHA_VANTAGE_API_KEY = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_AV_KEY_HERE')
REDIS_URL = os.getenv('REDIS_URL', 'redis://localhost:6379')
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))
RISK_REWARD_RATIO = float(os.getenv('RISK_REWARD_RATIO', '2.0'))
TRAILING_SL_ENABLED = os.getenv('TRAILING_SL_ENABLED', 'True').lower() == 'true'
TRAILING_SL_ACTIVATION_PERCENT = float(os.getenv('TRAILING_SL_ACTIVATION_PERCENT', '1.5'))
TRAILING_SL_CALLBACK_PERCENT = float(os.getenv('TRAILING_SL_CALLBACK_PERCENT', '1.0'))

TIMEFRAME = '15m'
SCAN_INTERVAL_SECONDS = 900
SUPERVISOR_INTERVAL_SECONDS = 120
TIME_SYNC_INTERVAL_SECONDS = 3600
STRATEGY_ANALYSIS_INTERVAL_SECONDS = 21600  # 6 hours

APP_ROOT = '.'
DB_FILE = os.path.join(APP_ROOT, f'okx_{MODE}_{WORKER_ID}.db')
SETTINGS_FILE = os.path.join(APP_ROOT, 'okx_settings.json') if MODE == 'broadcaster' else None

EGYPT_TZ = ZoneInfo("Africa/Cairo")
REDIS_CHANNEL = "trade_signals"
REDIS_ACK_CHANNEL = "trade_ack"

# --- Logging Setup ---
class SafeFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, 'trade_id'): record.trade_id = 'N/A'
        if not hasattr(record, 'worker_id'): record.worker_id = WORKER_ID
        return super().format(record)

log_formatter = SafeFormatter('%(asctime)s - %(levelname)s - [WorkerID:%(worker_id)s] - [TradeID:%(trade_id)s] - %(message)s')
log_handler = logging.StreamHandler()
log_handler.setFormatter(log_formatter)
root_logger = logging.getLogger()
root_logger.handlers = [log_handler]
root_logger.setLevel(logging.INFO)

class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'extra' not in kwargs: kwargs['extra'] = {}
        if 'trade_id' not in kwargs['extra']: kwargs['extra']['trade_id'] = 'N/A'
        return msg, kwargs

logger = ContextAdapter(logging.getLogger("OKX_Bot"), {})

# --- Global Bot State & Locks ---
class BotState:
    def __init__(self):
        self.settings = {}
        self.trading_enabled = True
        self.active_preset_name = "مخصص" if MODE == 'broadcaster' else None
        self.last_signal_time = {}
        self.application = None
        self.exchange = None
        self.market_mood = {"mood": "UNKNOWN", "reason": "تحليل لم يتم بعد"} if MODE == 'broadcaster' else None
        self.private_ws = None
        self.public_ws = None
        self.trade_guardian = None
        self.last_scan_info = {}
        self.all_markets = []
        self.last_markets_fetch = 0
        self.strategy_performance = {} if MODE == 'broadcaster' else None
        self.pending_strategy_proposal = {} if MODE == 'broadcaster' else None
        self.redis_client = None
        self.redis_connected = False
        self.last_signal_received_at = None if MODE == 'worker' else None

bot_data = BotState()
scan_lock = asyncio.Lock()
trade_management_lock = asyncio.Lock()

# --- Default Settings for Broadcaster ---
DEFAULT_SETTINGS = {
    "real_trade_size_usdt": 15.0,
    "max_concurrent_trades": 5,
    "top_n_symbols_by_volume": 300,
    "worker_threads": 10,
    "atr_sl_multiplier": 2.5,
    "risk_reward_ratio": 2.0,
    "trailing_sl_enabled": True,
    "trailing_sl_activation_percent": 1.5,
    "trailing_sl_callback_percent": 1.0,
    "active_scanners": ["momentum_breakout", "breakout_squeeze_pro", "support_rebound", "sniper_pro", "whale_radar", "rsi_divergence", "supertrend_pullback"],
    "market_mood_filter_enabled": True,
    "fear_and_greed_threshold": 30,
    "adx_filter_enabled": True,
    "adx_filter_level": 25,
    "btc_trend_filter_enabled": True,
    "news_filter_enabled": True,
    "asset_blacklist": ["USDC", "DAI", "TUSD", "FDUSD", "USDD", "PYUSD", "USDT", "BNB", "OKB", "KCS", "BGB", "MX", "GT", "HT", "BTC", "ETH"],
    "liquidity_filters": {"min_quote_volume_24h_usd": 1000000, "min_rvol": 1.5},
    "volatility_filters": {"atr_period_for_filter": 14, "min_atr_percent": 0.8},
    "trend_filters": {"ema_period": 200, "htf_period": 50, "enabled": True},
    "spread_filter": {"max_spread_percent": 0.5},
    "rsi_divergence": {"rsi_period": 14, "lookback_period": 35, "peak_trough_lookback": 5, "confirm_with_rsi_exit": True},
    "supertrend_pullback": {"atr_period": 10, "atr_multiplier": 3.0, "swing_high_lookback": 10},
    "multi_timeframe_enabled": True,
    "multi_timeframe_htf": '4h',
    "volume_filter_multiplier": 2.0,
    "close_retries": 3,
    "incremental_notifications_enabled": True,
    "incremental_notification_percent": 2.0,
    "adaptive_intelligence_enabled": True,
    "dynamic_trade_sizing_enabled": True,
    "strategy_proposal_enabled": True,
    "strategy_analysis_min_trades": 10,
    "strategy_deactivation_threshold_wr": 45.0,
    "dynamic_sizing_max_increase_pct": 25.0,
    "dynamic_sizing_max_decrease_pct": 50.0,
} if MODE == 'broadcaster' else None

STRATEGY_NAMES_AR = {
    "momentum_breakout": "زخم اختراقي", "breakout_squeeze_pro": "اختراق انضغاطي",
    "support_rebound": "ارتداد الدعم", "sniper_pro": "القناص المحترف", "whale_radar": "رادار الحيتان",
    "rsi_divergence": "دايفرجنس RSI", "supertrend_pullback": "انعكاس سوبرترند"
} if MODE == 'broadcaster' else None

PRESET_NAMES_AR = {"professional": "احترافي", "strict": "متشدد", "lenient": "متساهل", "very_lenient": "فائق التساهل", "bold_heart": "القلب الجريء"} if MODE == 'broadcaster' else None

SETTINGS_PRESETS = {
    "professional": copy.deepcopy(DEFAULT_SETTINGS),
    "strict": {**copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 3, "risk_reward_ratio": 2.5, "fear_and_greed_threshold": 40, "adx_filter_level": 28, "liquidity_filters": {"min_quote_volume_24h_usd": 2000000, "min_rvol": 2.0}},
    "lenient": {**copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 8, "risk_reward_ratio": 1.8, "fear_and_greed_threshold": 25, "adx_filter_level": 20, "liquidity_filters": {"min_quote_volume_24h_usd": 500000, "min_rvol": 1.2}},
    "very_lenient": {
        **copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 12, "adx_filter_enabled": False,
        "market_mood_filter_enabled": False, "trend_filters": {"ema_period": 200, "htf_period": 50, "enabled": False},
        "liquidity_filters": {"min_quote_volume_24h_usd": 250000, "min_rvol": 1.0},
        "volatility_filters": {"atr_period_for_filter": 14, "min_atr_percent": 0.4}, "spread_filter": {"max_spread_percent": 1.5}
    },
    "bold_heart": {
        **copy.deepcopy(DEFAULT_SETTINGS), "max_concurrent_trades": 15, "risk_reward_ratio": 1.5, "multi_timeframe_enabled": False,
        "market_mood_filter_enabled": False, "adx_filter_enabled": False, "btc_trend_filter_enabled": False, "news_filter_enabled": False,
        "volume_filter_multiplier": 1.0, "liquidity_filters": {"min_quote_volume_24h_usd": 100000, "min_rvol": 1.0},
        "volatility_filters": {"atr_period_for_filter": 14, "min_atr_percent": 0.2}, "spread_filter": {"max_spread_percent": 2.0}
    }
} if MODE == 'broadcaster' else None

# --- Helper, Settings & DB Management for Broadcaster ---
def load_settings():
    if MODE != 'broadcaster': return
    try:
        if os.path.exists(SETTINGS_FILE):
            with open(SETTINGS_FILE, 'r') as f: bot_data.settings = json.load(f)
        else: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    except Exception: bot_data.settings = copy.deepcopy(DEFAULT_SETTINGS)
    default_copy = copy.deepcopy(DEFAULT_SETTINGS)
    for key, value in default_copy.items():
        if isinstance(value, dict):
            if key not in bot_data.settings or not isinstance(bot_data.settings[key], dict): bot_data.settings[key] = {}
            for sub_key, sub_value in value.items(): bot_data.settings[key].setdefault(sub_key, sub_value)
        else: bot_data.settings.setdefault(key, value)
    determine_active_preset()
    save_settings()
    logger.info(f"Settings loaded. Active preset: {bot_data.active_preset_name}")

def determine_active_preset():
    if MODE != 'broadcaster': return
    current_settings_for_compare = {k: v for k, v in bot_data.settings.items() if k in DEFAULT_SETTINGS}
    for name, preset_settings in SETTINGS_PRESETS.items():
        is_match = True
        for key, value in preset_settings.items():
            if key in current_settings_for_compare and current_settings_for_compare[key] != value:
                is_match = False
                break
        if is_match:
            bot_data.active_preset_name = PRESET_NAMES_AR.get(name, "مخصص")
            return
    bot_data.active_preset_name = "مخصص"

def save_settings():
    if MODE != 'broadcaster': return
    with open(SETTINGS_FILE, 'w') as f: json.dump(bot_data.settings, f, indent=4)

async def safe_send_message(bot, text, **kwargs):
    try: await bot.send_message(TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except Exception as e: logger.error(f"Telegram Send Error: {e}")

async def safe_edit_message(query, text, **kwargs):
    try: await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN, **kwargs)
    except BadRequest as e:
        if "Message is not modified" not in str(e): logger.warning(f"Edit Message Error: {e}")
    except Exception as e: logger.error(f"Edit Message Error: {e}")

async def init_database():
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute('CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT, entry_price REAL, take_profit REAL, stop_loss REAL, quantity REAL, status TEXT, reason TEXT, order_id TEXT, highest_price REAL DEFAULT 0, trailing_sl_active BOOLEAN DEFAULT 0, close_price REAL, pnl_usdt REAL, signal_strength INTEGER DEFAULT 1, close_retries INTEGER DEFAULT 0, last_profit_notification_price REAL DEFAULT 0, trade_weight REAL DEFAULT 1.0, signal_id TEXT UNIQUE)')
            cursor = await conn.execute("PRAGMA table_info(trades)")
            columns = [row[1] for row in await cursor.fetchall()]
            if 'signal_strength' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN signal_strength INTEGER DEFAULT 1")
            if 'close_retries' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN close_retries INTEGER DEFAULT 0")
            if 'last_profit_notification_price' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN last_profit_notification_price REAL DEFAULT 0")
            if 'trade_weight' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN trade_weight REAL DEFAULT 1.0")
            if 'signal_id' not in columns: await conn.execute("ALTER TABLE trades ADD COLUMN signal_id TEXT UNIQUE")
            await conn.commit()
        logger.info("Database initialized successfully.")
    except Exception as e: logger.critical(f"Database initialization failed: {e}")

async def log_pending_trade_to_db(signal, buy_order):
    signal_id = signal.get('signal_id')
    try:
        async with aiosqlite.connect(DB_FILE) as conn:
            await conn.execute("INSERT INTO trades (timestamp, symbol, reason, order_id, status, entry_price, take_profit, stop_loss, signal_strength, last_profit_notification_price, trade_weight, signal_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               (datetime.now(EGYPT_TZ).isoformat(), signal['symbol'], signal['reason'], buy_order['id'], 'pending', signal['entry_price'], signal['take_profit'], signal['stop_loss'], signal.get('strength', 1), signal['entry_price'], signal.get('weight', 1.0), signal_id))
            await conn.commit()
        logger.info(f"Logged pending trade for {signal['symbol']} with order ID {buy_order['id']}.")
        return True
    except Exception as e: logger.error(f"DB Log Pending Error: {e}"); return False

# --- Redis Connection with Retry ---
async def redis_connect_with_retry():
    backoff = 1
    while True:
        try:
            bot_data.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            await bot_data.redis_client.ping()
            bot_data.redis_connected = True
            logger.info("Successfully connected to Redis.")
            return
        except Exception as e:
            logger.error(f"Redis connection failed: {e}. Retrying in {backoff} seconds...")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

# --- Broadcast Signal (for Broadcaster) ---
async def broadcast_signal_to_redis(signal):
    if MODE != 'broadcaster': return
    signal['signal_id'] = str(uuid.uuid4())
    encoded = base64.b64encode(json.dumps(signal).encode()).decode()
    await bot_data.redis_client.publish(REDIS_CHANNEL, encoded)
    logger.info(f"📡 Broadcasted signal {signal['signal_id']} for {signal['symbol']} to Redis.")

# --- Listener for Signals (for Worker) ---
async def redis_listener():
    if MODE != 'worker': return
    pubsub = bot_data.redis_client.pubsub()
    await pubsub.subscribe(REDIS_CHANNEL)
    logger.info(f"Subscribed to Redis channel '{REDIS_CHANNEL}'. Waiting for signals...")
    async for message in pubsub.listen():
        if message['type'] == 'message':
            bot_data.last_signal_received_at = datetime.now(EGYPT_TZ)
            try:
                decoded = base64.b64decode(message['data'].encode()).decode()
                signal = json.loads(decoded)
                if 'signal_id' not in signal or not signal.get('symbol') or signal.get('entry_price') <= 0:
                    logger.error("Received invalid signal.")
                    continue
                lock_key = f"signal_lock:{signal['signal_id']}"
                if await bot_data.redis_client.setnx(lock_key, WORKER_ID):
                    logger.info(f"Processing signal {signal['signal_id']} for {signal['symbol']}.")
                    await execute_trade_from_signal(signal)
                    await bot_data.redis_client.publish(REDIS_ACK_CHANNEL, json.dumps({"signal_id": signal['signal_id'], "worker": WORKER_ID, "status": "executed"}))
                else:
                    logger.info(f"Signal {signal['signal_id']} already locked by another worker.")
            except Exception as e:
                logger.error(f"Signal processing error: {e}", exc_info=True)

# --- Execute Trade from Signal (for Worker) ---
async def execute_trade_from_signal(signal):
    if MODE != 'worker': return
    symbol = signal.get('symbol')
    entry_price = signal.get('entry_price')
    if not symbol or not entry_price:
        logger.error(f"Invalid signal: {signal}")
        return
    try:
        amount = TRADE_SIZE_USDT / entry_price
        order = await bot_data.exchange.create_market_buy_order(symbol, amount)
        logger.info(f"Placed market buy order for {symbol}. Order ID: {order['id']}")
        await log_pending_trade_to_db(signal, order)
        await activate_trade(order['id'], symbol)
    except ccxt.InsufficientFunds as e:
        logger.error(f"Insufficient funds for {symbol}. Error: {e}")
        await safe_send_message(bot_data.application.bot, f"🚨 **[W:{WORKER_ID}] رصيد غير كافٍ** 🚨\nفشل تنفيذ صفقة `{symbol}`.")
    except Exception as e:
        logger.error(f"Trade execution failed for {symbol}: {e}", exc_info=True)

async def activate_trade(order_id, symbol):
    log_ctx = {'trade_id': 'N/A'}
    try:
        order_details = await bot_data.exchange.fetch_order(order_id, symbol)
        filled_price = order_details.get('average', 0.0)
        net_filled_quantity = order_details.get('filled', 0.0)

        if net_filled_quantity <= 0 or filled_price <= 0:
            logger.error(f"Order {order_id} invalid fill data.")
            return

        async with aiosqlite.connect(DB_FILE) as conn:
            conn.row_factory = aiosqlite.Row
            trade = await (await conn.execute("SELECT * FROM trades WHERE order_id = ? AND status = 'pending'", (order_id,))).fetchone()
            if not trade:
                logger.info(f"Activation ignored for {order_id}: Trade not pending.")
                return
            
            trade = dict(trade)
            log_ctx['trade_id'] = trade['id']
            logger.info(f"Activating trade #{trade['id']} for {symbol}...", extra=log_ctx)
            
            risk = filled_price - trade['stop_loss']
            new_take_profit = filled_price + (risk * RISK_REWARD_RATIO)

            await conn.execute('UPDATE trades SET status = \'active\', entry_price = ?, quantity = ?, take_profit = ? WHERE id = ?',
                               (filled_price, net_filled_quantity, new_take_profit, trade['id']))
            await conn.commit()

        await bot_data.public_ws.subscribe([symbol])
        
        success_msg = (f"✅ **[W:{WORKER_ID}] تم تأكيد الشراء | {symbol}**\n\n"
                       f"🔸 **الصفقة رقم:** #{trade['id']}\n"
                       f"🔸 **سعر التنفيذ:** `${filled_price:,.4f}`\n"
                       f"🎯 **الهدف (TP):** `${new_take_profit:,.4f}`\n"
                       f"🛡️ **الوقف (SL):** `${trade['stop_loss']:,.4f}`")
        await safe_send_message(bot_data.application.bot, success_msg)

    except Exception as e:
        logger.error(f"Could not activate trade {order_id}: {e}", exc_info=True, extra=log_ctx)

# --- Trade Guardian Class ---
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
                    trade = await (await conn.execute("SELECT * FROM trades WHERE symbol = ? AND status = 'active' ORDER BY id DESC", (symbol,))).fetchone()
                    if not trade:
                        return
                    trade = dict(trade)

                if TRAILING_SL_ENABLED:
                    profit_ratio = (current_price - trade['entry_price']) / trade['entry_price']
                    if profit_ratio >= TRAILING_SL_ACTIVATION_PERCENT / 100 and not trade['trailing_sl_active']:
                        async with aiosqlite.connect(DB_FILE) as conn:
                            await conn.execute("UPDATE trades SET trailing_sl_active = 1, highest_price = ? WHERE id = ?", (current_price, trade['id']))
                            await conn.commit()
                    if trade['trailing_sl_active']:
                        new_sl = current_price * (1 - TRAILING_SL_CALLBACK_PERCENT / 100)
                        if new_sl > trade['stop_loss']:
                            async with aiosqlite.connect(DB_FILE) as conn:
                                await conn.execute("UPDATE trades SET stop_loss = ? WHERE id = ?", (new_sl, trade['id']))
                                await conn.commit()
                            logger.info(f"Updated SL for {symbol} to {new_sl}")

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
            await safe_send_message(bot_data.application.bot, msg)

        except Exception as e:
            logger.error(f"Failed to close trade #{trade_id}: {e}", exc_info=True, extra=log_ctx)
            await safe_send_message(bot_data.application.bot, f"🚨 **[W:{WORKER_ID}] فشل حرج** 🚨\nفشل إغلاق الصفقة `#{trade_id}`. الرجاء مراجعة المنصة يدوياً.")
            
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

# --- Public WebSocket Manager ---
class PublicWebSocketManager:
    def __init__(self, handler_coro):
        self.ws_url = "wss://ws.okx.com:8443/ws/v5/public"
        self.handler = handler_coro
        self.subscriptions = set()
        self.websocket = None

    async def _send_op(self, op, symbols):
        if not symbols or not self.websocket: return
        try:
            await self.websocket.send(json.dumps({"op": op, "args": [{"channel": "tickers", "instId": s.replace('/', '-')} for s in symbols]}))
        except websockets.exceptions.ConnectionClosed: logger.warning(f"Could not send '{op}' op; ws is closed.")

    async def subscribe(self, symbols):
        new = [s for s in symbols if s not in self.subscriptions]
        if new:
            await self._send_op('subscribe', new)
            self.subscriptions.update(new)
            logger.info(f"👁️ [Guardian] Now watching: {new}")

    async def unsubscribe(self, symbols):
        old = [s for s in symbols if s in self.subscriptions]
        if old:
            await self._send_op('unsubscribe', old)
            [self.subscriptions.discard(s) for s in old]
            logger.info(f"👁️ [Guardian] Stopped watching: {old}")

    async def _run_loop(self):
        async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
            self.websocket = ws
            logger.info("✅ [Guardian's Eyes] Connected.")
            if self.subscriptions:
                await self.subscribe(list(self.subscriptions))
            async for msg in ws:
                if msg == 'ping':
                    await ws.send('pong')
                    continue
                data = json.loads(msg)
                if data.get('arg', {}).get('channel') == 'tickers' and 'data' in data:
                    for ticker in data['data']:
                        await self.handler(ticker)

    async def run(self):
        while True:
            try:
                await self._run_loop()
            except Exception as e:
                logger.error(f"Public WS failed: {e}. Retrying...")
                await asyncio.sleep(5)

# --- Telegram Handlers for Worker Mode ---
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if MODE != 'worker': return
    await update.message.reply_text(f"👋 أهلاً بك في بوت العامل **{WORKER_ID}**.\nاضغط /dashboard لعرض لوحة التحكم.")

async def show_dashboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if MODE != 'worker': return
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
    if MODE != 'worker': return
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
    if MODE != 'worker': return
    query = update.callback_query
    await query.answer("جاري جلب بيانات المحفظة...")
    try:
        balance = await bot_data.exchange.fetch_balance()
        total_usdt_equity = balance.get('USDT', {}).get('total', 0)
        text = f"**💼 محفظة الحساب ({WORKER_ID})**\n\n**إجمالي الرصيد:** `${total_usdt_equity:,.2f}` USDT\n\n**الأصول الأخرى:**\n"
        
        assets = []
        if 'info' in balance and 'details' in balance['info']:
            for asset_data in balance['info']['details']:
                asset = asset_data.get('ccy')
                total = float(asset_data.get('eq', 0))
                if total > 0.01 and asset != 'USDT':
                    assets.append(f"- **{asset}:** `{total}`")
        
        text += "\n".join(assets) if assets else "لا توجد أصول أخرى."

    except Exception as e:
        logger.error(f"Portfolio fetch error: {e}", exc_info=True)
        text = f"حدث خطأ أثناء جلب المحفظة: {e}"
        
    keyboard = [[InlineKeyboardButton("🔄 تحديث", callback_data="portfolio")], [InlineKeyboardButton("🔙 عودة", callback_data="dashboard")]]
    await safe_edit_message(query, text, reply_markup=InlineKeyboardMarkup(keyboard))

async def show_active_trades_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if MODE != 'worker': return
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
    if MODE != 'worker': return
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
    if MODE != 'worker': return
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

# --- Caching for Ticker Fetch ---
@lru_cache(maxsize=100)
async def fetch_ticker_cached(symbol):
    return await bot_data.exchange.fetch_ticker(symbol)

# --- Post Init & Shutdown ---
async def post_init(application: Application):
    bot_data.application = application
    if not all([OKX_API_KEY, OKX_API_SECRET, OKX_API_PASSPHRASE, TELEGRAM_BOT_TOKEN]):
        logger.critical("FATAL: Missing critical API keys.")
        return
    if NLTK_AVAILABLE:
        try: nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError: logger.info("Downloading NLTK data..."); nltk.download('vader_lexicon', quiet=True)
    await redis_connect_with_retry()
    config = {'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE, 'enableRateLimit': True}
    bot_data.exchange = ccxt.okx(config)
    await bot_data.exchange.load_markets()
    await bot_data.exchange.fetch_balance()
    logger.info("✅ Successfully connected to OKX.")
    bot_data.trade_guardian = TradeGuardian(application)
    bot_data.public_ws = PublicWebSocketManager(bot_data.trade_guardian.handle_ticker_update)
    asyncio.create_task(bot_data.public_ws.run())
    logger.info("Waiting 5s for WebSocket connections..."); await asyncio.sleep(5)
    await bot_data.trade_guardian.sync_subscriptions()
    if MODE == 'broadcaster':
        load_settings()
        jq = application.job_queue
        jq.run_repeating(perform_scan, interval=SCAN_INTERVAL_SECONDS, first=10, name="perform_scan")
        jq.run_repeating(the_supervisor_job, interval=SUPERVISOR_INTERVAL_SECONDS, first=30, name="the_supervisor_job")
        jq.run_repeating(update_strategy_performance, interval=STRATEGY_ANALYSIS_INTERVAL_SECONDS, first=60, name="update_strategy_performance")
        jq.run_repeating(propose_strategy_changes, interval=STRATEGY_ANALYSIS_INTERVAL_SECONDS, first=120, name="propose_strategy_changes")
    elif MODE == 'worker':
        asyncio.create_task(redis_listener())
    try: await application.bot.send_message(TELEGRAM_CHAT_ID, "*🤖 OKX Bot | إصدار متكامل - بدأ العمل...*", parse_mode=ParseMode.MARKDOWN)
    except Forbidden: logger.critical(f"FATAL: Bot not authorized for chat ID {TELEGRAM_CHAT_ID}.")
    logger.info("--- OKX Sniper Bot is now fully operational ---")

async def post_shutdown(application: Application):
    if bot_data.exchange: await bot_data.exchange.close()
    if bot_data.redis_client: await bot_data.redis_client.close()
    logger.info("Bot has shut down.")

# --- Main Entry Point ---
def main():
    asyncio.run(init_database())
    app_builder = Application.builder().token(TELEGRAM_BOT_TOKEN)
    app_builder.post_init(post_init).post_shutdown(post_shutdown)
    application = app_builder.build()
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("scan", manual_scan_command) if MODE == 'broadcaster' else CommandHandler("dashboard", show_dashboard_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, universal_text_handler) if MODE == 'broadcaster' else None)
    application.add_handler(CallbackQueryHandler(button_callback_handler))
    application.run_polling()

if __name__ == '__main__':
    main()
