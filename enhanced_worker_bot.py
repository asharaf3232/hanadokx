# test_telegram.py
import os
import asyncio
from telegram import Bot
from dotenv import load_dotenv

# لتحميل المتغيرات عند الاختبار المحلي
load_dotenv()

# قراءة المتغيرات من البيئة
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

async def main():
    """يحاول إرسال رسالة واحدة فقط ويطبع النتيجة."""
    if not TOKEN or not CHAT_ID:
        print("!!! ERROR: TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set.")
        return

    # طباعة معلومات للمساعدة في التشخيص
    print("--- Starting Telegram Test ---")
    print(f"Attempting to send a message...")
    # طباعة آخر 4 حروف من التوكن للتأكد من أنه صحيح
    print(f"Bot Token Hint: ...{TOKEN[-4:]}") 
    print(f"Using Chat ID: {CHAT_ID}")
    print("----------------------------")

    try:
        # إنشاء اتصال مباشر بالبوت
        bot = Bot(token=TOKEN)
        # إرسال الرسالة
        await bot.send_message(chat_id=CHAT_ID, text="!!! Hello from Render Test Script !!!")
        print("\n✅✅✅ SUCCESS: Message sent successfully!")
        print("This confirms your TOKEN and CHAT_ID are CORRECT.")

    except Exception as e:
        print(f"\n❌❌❌ FAILED: An error occurred.")
        print(f"Error Details: {e}")
        print("This confirms the problem is with your TOKEN or CHAT_ID variables.")

if __name__ == "__main__":
    asyncio.run(main())
