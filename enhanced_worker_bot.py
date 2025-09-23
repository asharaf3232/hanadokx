# -*- coding: utf-8 -*-
# =======================================================================================
# --- 🦾 Interactive Hand Bot | v2.3 (Render Proof) 🦾 ---
# =======================================================================================
# v2.3:
#   ✅ [إصلاح حاسم] تم تعديل آلية التشغيل لتكون مقاومة لخطأ Conflict على Render.
#   ✅ [بوت هجين] يعمل دائمًا كمنفذ صفقات، ويحاول تشغيل الواجهة التفاعلية.
#   ✅ [استقرار] في حالة التعارض، يوقف الواجهة فقط ويستمر في العمل بالخلفية.
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
from telegram.error import Conflict

# --- ⚙️ الإعدادات والتهيئة (كما هي) ---
load_dotenv()
OKX_API_KEY = os.getenv('OKX_API_KEY')
# ... (باقي المتغيرات كما هي)
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')
REDIS_URL = os.getenv('REDIS_URL')
TRADE_SIZE_USDT = float(os.getenv('TRADE_SIZE_USDT', '15.0'))
DB_FILE = 'interactive_hand_v2.3.db'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- دوال قاعدة البيانات ومنطق التداول (بدون تغيير) ---
# ... (انسخ دوال init_database, get_active_trades, get_closed_trades, execute_trade من الكود السابق)
async def init_database():
    # ...
    pass

async def execute_trade(signal):
    # ...
    pass
# ...

# --- مستمعات Redis (بدون تغيير) ---
async def redis_listener(channel_name, callback_func):
    # ... (انسخها من الكود السابق)
    pass

async def handle_trade_signal(data):
    # ... (انسخها من الكود السابق)
    pass
async def handle_system_status(data):
    # ... (انسخها من الكود السابق)
    pass


# --- أوامر التليجرام (بدون تغيير) ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [["📊 الحالة", "💰 المحفظة"], ["📈 الصفقات المفتوحة", "📜 السجل"]]
    await update.message.reply_text(
        "أهلاً بك في بوت اليد التفاعلي.",
        reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    )
# ... (انسخ باقي أوامر التليجرام: status, portfolio, active_trades, history, handle_text)
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # ...
    pass
# ...

# --- ✅ [تعديل جوهري] دالة التشغيل الرئيسية ---
async def main():
    """الدالة الرئيسية التي تدير تشغيل البوت بشكل مقاوم للأخطاء."""
    
    # تهيئة التطبيق
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    # تسجيل الأوامر
    app.add_handler(CommandHandler("start", start))
    # ... (أضف باقي الأوامر هنا)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    
    # تهيئة الاتصالات والمهام الأساسية
    await init_database()
    # ... (هنا يمكنك وضع أي تهيئة أخرى مثل الاتصال بـ OKX)
    
    # بدء تشغيل المهام الخلفية (مستمعات Redis). هذه ستعمل دائمًا.
    logging.info("Starting background listeners (Redis)...")
    asyncio.create_task(redis_listener("trade_signals", handle_trade_signal))
    asyncio.create_task(redis_listener("system_status", handle_system_status))
    logging.info("Background listeners are running.")

    # محاولة تشغيل الجزء التفاعلي (Polling)
    try:
        logging.info("Attempting to start Telegram polling...")
        await app.initialize()
        await app.updater.start_polling()
        await app.start()
        logging.info("Telegram polling started successfully. Bot is fully interactive.")
        await app.bot.send_message(TELEGRAM_CHAT_ID, "*🦾 بوت اليد v2.3 بدأ العمل (الوضع التفاعلي).*")
        # إبقاء البرنامج يعمل إلى الأبد
        await asyncio.Event().wait()

    except Conflict:
        logging.warning("Conflict detected. Another instance is running the interactive part.")
        logging.warning("This instance will continue running in SILENT WORKER MODE.")
        await app.bot.send_message(TELEGRAM_CHAT_ID, "*🦾 بوت اليد v2.3 بدأ العمل (وضع المنفذ الصامت بسبب وجود نسخة أخرى).*")
        # البرنامج سيبقى يعمل لأن المهام الخلفية لا تزال نشطة
        await asyncio.Event().wait()
        
    finally:
        # الإغلاق الآمن عند التوقف
        if app.updater and app.updater.is_running:
            await app.updater.stop()
        if app.running:
            await app.stop()
        # ... (أضف await exchange.close() هنا إذا لزم الأمر)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped manually.")

