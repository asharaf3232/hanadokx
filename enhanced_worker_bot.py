# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🤖 OKX Worker Bot for Render | v1.2 🤖 ---
# =======================================================================================
# نسخة محسنة ومجهزة للعمل مباشرة على منصة Render.
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

from telegram import Update
from telegram.constants import ParseMode
from telegram.ext import Application, CommandHandler, ContextTypes

# --- ⚙️ الإعدادات والتهيئة ⚙️ ---
load_dotenv()

# --- قراءة المتغيرات من بيئة Render ---
OKX_API_KEY = os.getenv('OKX_API_KEY')
OKX_API_SECRET = os.getenv('OKX_API_SECRET')
OKX_API_PASSPHRASE = os.getenv('OKX_API_PASSPHRASE')
REDIS_URL = os.getenv('REDIS_URL') # يجب أن يكون رابط Upstash هنا
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

DB_FILE = 'my_trades_render.db'
EGYPT_TZ = ZoneInfo("Africa/Cairo")
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# --- متغيرات حالة البوت ---
exchange = None
redis_client = None
application = None

async def init_database():
    """ينشئ قاعدة بيانات SQLite لتخزين سجل الصفقات."""
    async with aiosqlite.connect(DB_FILE) as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT, symbol TEXT,
                entry_price REAL, quantity REAL, status TEXT, okx_order_id TEXT
            )
        ''')
        await conn.commit()
    logging.info("Database initialized.")

async def safe_send_message(text, **kwargs):
    """يرسل رسالة تليجرام مع معالجة الأخطاء."""
    try:
        await application.bot.send_message(
            TELEGRAM_CHAT_ID, text, parse_mode=ParseMode.MARKDOWN, **kwargs
        )
    except Exception as e:
        logging.error(f"Telegram Send Error: {e}")

async def execute_trade(signal):
    """الدالة الأساسية لتنفيذ صفقة عند استلام إشارة."""
    try:
        symbol = signal['symbol']
        entry_price = signal['entry_price']
        
        amount_to_buy = TRADE_SIZE_USDT / entry_price
        formatted_amount = exchange.amount_to_precision(symbol, amount_to_buy)

        logging.info(f"Executing trade for {symbol}. Amount: {formatted_amount}")
        order = await exchange.create_market_buy_order(symbol, formatted_amount)
        
        await asyncio.sleep(2) # انتظار بسيط لتحديث حالة الأمر في المنصة
        filled_order = await exchange.fetch_order(order['id'], symbol)
        
        actual_price = filled_order.get('average', entry_price)
        actual_quantity = filled_order.get('filled', 0)

        if actual_quantity > 0:
            async with aiosqlite.connect(DB_FILE) as conn:
                await conn.execute(
                    "INSERT INTO trades (timestamp, symbol, entry_price, quantity, status, okx_order_id) VALUES (?, ?, ?, ?, ?, ?)",
                    (datetime.now(EGYPT_TZ).isoformat(), symbol, actual_price, actual_quantity, 'active', order['id'])
                )
                await conn.commit()
            
            msg = (f"✅ **[اليد على Render] تم تنفيذ صفقة**\n"
                   f"`{symbol}` بسعر `~${actual_price:,.4f}`")
            await safe_send_message(msg)
        else:
            logging.warning(f"Order {order['id']} for {symbol} was placed but not filled.")

    except Exception as e:
        logging.error(f"Trade execution failed for {signal['symbol']}: {e}", exc_info=True)
        await safe_send_message(f"🚨 **[اليد على Render] فشل تنفيذ صفقة لـ `{signal['symbol']}`. السبب: {e}")

async def redis_listener():
    """يستمع بشكل دائم لقناة Redis وينتظر الإشارات."""
    while True:
        try:
            r = redis.from_url(REDIS_URL, decode_responses=True)
            pubsub = r.pubsub()
            await pubsub.subscribe("trade_signals")
            logging.info("Redis listener connected successfully to Upstash.")
            await safe_send_message("✅ **[اليد على Render]** تم الاتصال بـ Redis بنجاح، وجاهز لاستقبال الإشارات.")
            
            async for message in pubsub.listen():
                if message and message['type'] == 'message':
                    try:
                        signal_data = json.loads(message['data'])
                        logging.info(f"Signal received: {signal_data['symbol']}")
                        # نفذ الصفقة في مهمة منفصلة لتجنب إيقاف المستمع
                        asyncio.create_task(execute_trade(signal_data))
                    except Exception as e:
                        logging.error(f"Error processing signal data: {e}")
        except Exception as e:
            logging.error(f"Redis connection failed: {e}. Reconnecting in 15 seconds...")
            await safe_send_message(f"🔴 **[اليد على Render]** فشل الاتصال بـ Redis. جاري إعادة المحاولة...")
            await asyncio.sleep(15)

async def start_bot():
    """الدالة الرئيسية لبدء تشغيل كل مكونات البوت."""
    global exchange, application
    
    # تهيئة تطبيق تليجرام
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    await safe_send_message("*🤖 [اليد على Render] بدء التشغيل...*")
    await init_database()

    # الاتصال بـ OKX
    try:
        exchange = ccxt.okx({'apiKey': OKX_API_KEY, 'secret': OKX_API_SECRET, 'password': OKX_API_PASSPHRASE})
        await exchange.load_markets()
        logging.info("OKX connection successful.")
        await safe_send_message("✅ **[اليد على Render]** تم الاتصال بمنصة OKX بنجاح.")
    except Exception as e:
        logging.error(f"OKX connection failed: {e}")
        await safe_send_message(f"🔴 **[اليد على Render]** فشل الاتصال بمنصة OKX.")
        return # إيقاف البوت إذا فشل الاتصال بالمنصة

    # بدء مستمع Redis في الخلفية
    asyncio.create_task(redis_listener())
    
    # هذا السطر يبقي السكريبت يعمل بشكل دائم
    # لا حاجة لـ application.run_polling() لأننا لا نستقبل أوامر من المستخدم
    await asyncio.Event().wait()


if __name__ == '__main__':
    if not all([OKX_API_KEY, REDIS_URL, TELEGRAM_BOT_TOKEN]):
        print("FATAL: Missing critical environment variables.")
    else:
        asyncio.run(start_bot())
