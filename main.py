# =============================================================================
# main.py — نقطة التشغيل الرئيسية
# =============================================================================
#
# يُشغَّل مباشرة: python main.py
# متوافق مع: Hugging Face Spaces / Windows / Linux
#
# الإضافات الجديدة (Hugging Face Spaces):
#   • Flask web server يعمل على المنفذ 7860
#   • تشغيل Flask في thread منفصل لتجنب التعارض مع asyncio
#   • مسار "/" يرجع "Bot is alive" لربطه بـ cron-job.org
#
# =============================================================================

import asyncio
import logging
import sys
import threading
from pathlib import Path

from flask import Flask

# ── إعداد الـ logging قبل أي import آخر ──────────────────────────────────────
# يُنشئ مجلد logs/ تلقائياً وينسخ السجلات للملف والـ stdout معاً.

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "bot.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

# ── Imports البوت ─────────────────────────────────────────────────────────────

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import BOT_TOKEN
from database import init_database
from handlers import router


# =============================================================================
# Flask Web Server — للإبقاء على البوت حياً على Hugging Face Spaces
# =============================================================================

flask_app = Flask(__name__)


@flask_app.route("/")
def health_check():
    """
    مسار بسيط يُستخدم للتحقق من أن البوت يعمل.
    يمكن ربطه بـ cron-job.org لمنع الـ Space من النوم.
    """
    return "Bot is alive", 200


def run_flask():
    """
    تشغيل Flask في thread منفصل.
    use_reloader=False ضروري لتجنب تشغيل الـ thread مرتين.
    """
    logger.info("🌐 جاري تشغيل Flask على المنفذ 7860...")
    flask_app.run(host="0.0.0.0", port=7860, use_reloader=False)


# =============================================================================
# الدالة الرئيسية
# =============================================================================

async def main() -> None:
    """
    تهيئة وتشغيل البوت.
    الخطوات:
      1. تهيئة قاعدة البيانات (إنشاء الجداول إذا لم تكن موجودة)
      2. إنشاء Bot و Dispatcher
      3. تسجيل الـ router الذي يحتوي على جميع الـ handlers
      4. بدء الـ polling لاستقبال الرسائل
    """
    logger.info("🚀 جاري تشغيل البوت...")

    # تهيئة قاعدة البيانات
    init_database()

    # إنشاء Bot بإعدادات HTML كـ parse_mode افتراضي
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    # إنشاء Dispatcher وتسجيل الـ router
    dp = Dispatcher()
    dp.include_router(router)

    logger.info("✅ البوت يعمل الآن. اضغط Ctrl+C للإيقاف.")

    # بدء الـ polling — يستقبل فقط أنواع التحديثات المستخدمة فعلياً
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


# =============================================================================
# نقطة الدخول
# =============================================================================

if __name__ == "__main__":
    # ── تشغيل Flask في thread منفصل (daemon=True يضمن إغلاقه مع البرنامج) ──
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # ── تشغيل البوت في الـ event loop الرئيسي ──
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("⛔ تم إيقاف البوت.")
