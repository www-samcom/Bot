# =============================================================================
# handlers.py — معالجات البوت الكاملة
# =============================================================================
#
# مبني على aiogram 3.x مع FSM (Finite State Machine) للمحادثات متعددة الخطوات.
#
# عند التحويل لـ web app / API:
#   • كل handler يصبح endpoint في FastAPI/Flask
#   • FSM States تصبح session state في المتصفح
#   • InlineKeyboard تصبح مكونات React/Vue
#   • callback_data يصبح API endpoint path
#
# الهيكل:
#   1.  FSM States — حالات المحادثات المتعددة الخطوات
#   2.  Keyboards — لوحات المفاتيح المضمّنة
#   3.  دوال مساعدة
#   4.  القائمة الرئيسية و /start
#   5.  تسجيل المبيعات (FSM)
#   6.  تسجيل المصاريف (FSM)
#   7.  إدارة المخزون
#   8.  التقارير
#   9.  الإعدادات
#   10. الحساب السريع (FSM)
#   11. المصاريف الثابتة (FSM)
#   12. معلومات الدفع (شام كاش)
#   13. لوحة الأدمن
#
# =============================================================================

import logging
import json
import os
from datetime import datetime

from aiogram import Router, F, Bot
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.filters import CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.exceptions import TelegramBadRequest

import database as db
from config import ADMIN_IDS, MSG_NO_PERMISSION, DEFAULT_PAYMENT_INFO, QR_CODE_PATH

logger = logging.getLogger(__name__)
router = Router()


# =============================================================================
# 1. FSM States — حالات المحادثات متعددة الخطوات
# =============================================================================
# كل StatesGroup تمثل محادثة من خطوات متعاقبة.
# عند التحويل لـ web app تصبح هذه الخطوات steps في wizard component.

class SaleStates(StatesGroup):
    """خطوات تسجيل مبيعة جديدة (4 خطوات)."""
    waiting_product_name = State()   # الخطوة 1: اسم المنتج
    waiting_quantity     = State()   # الخطوة 2: الكمية
    waiting_price        = State()   # الخطوة 3: السعر
    waiting_payment      = State()   # الخطوة 4: طريقة الدفع
    waiting_customer     = State()   # الخطوة 5: اسم العميل (اختياري)


class ExpenseStates(StatesGroup):
    """خطوات تسجيل مصروف جديد (3 خطوات)."""
    waiting_category    = State()   # الخطوة 1: الفئة
    waiting_description = State()   # الخطوة 2: الوصف
    waiting_amount      = State()   # الخطوة 3: المبلغ
    waiting_payment     = State()   # الخطوة 4: طريقة الدفع


class ProductStates(StatesGroup):
    """خطوات إضافة منتج جديد."""
    waiting_name  = State()
    waiting_price = State()
    waiting_cost  = State()
    waiting_stock = State()
    waiting_unit  = State()


class StockAddStates(StatesGroup):
    """خطوات إضافة مخزون لمنتج موجود."""
    waiting_product_name = State()
    waiting_quantity     = State()


class QuickCalcStates(StatesGroup):
    """خطوات الحساب السريع (حساب مؤقت بدون حفظ)."""
    waiting_sales    = State()
    waiting_expenses = State()


class FixedExpenseStates(StatesGroup):
    """خطوات إضافة وتعديل المصاريف الثابتة."""
    # إضافة مصروف ثابت
    waiting_name   = State()
    waiting_amount = State()
    waiting_type   = State()
    # تعديل مصروف ثابت
    waiting_edit_id     = State()
    waiting_edit_name   = State()
    waiting_edit_amount = State()
    waiting_edit_type   = State()


class EditSaleStates(StatesGroup):
    """خطوات تعديل مبيعة مسجّلة."""
    waiting_sale_id    = State()   # اختيار المبيعة
    waiting_field      = State()   # اختيار الحقل المراد تعديله
    waiting_new_value  = State()   # إدخال القيمة الجديدة


class EditExpenseStates(StatesGroup):
    """خطوات تعديل مصروف مسجّل."""
    waiting_expense_id = State()   # اختيار المصروف
    waiting_field      = State()   # اختيار الحقل المراد تعديله
    waiting_new_value  = State()   # إدخال القيمة الجديدة


class AdminStates(StatesGroup):
    """خطوات عمليات الأدمن."""
    waiting_user_id   = State()   # البحث عن مستخدم لتفعيل اشتراكه
    waiting_plan_days = State()   # اختيار خطة الاشتراك
    waiting_broadcast = State()   # كتابة رسالة جماعية


class PartnerStates(StatesGroup):
    """خطوات ضبط نسبة الشريك."""
    waiting_percentage = State()


class ButtonRenameStates(StatesGroup):
    """خطوات إعادة تسمية الأزرار."""
    waiting_new_name = State()


class PriceStates(StatesGroup):
    """خطوات تعديل أسعار خطط الاشتراك."""
    waiting_monthly  = State()
    waiting_yearly   = State()
    waiting_lifetime = State()


# =============================================================================
# 2. Keyboards — لوحات المفاتيح المضمّنة
# =============================================================================
# كل دالة kb_* تعيد InlineKeyboardMarkup.
# عند التحويل لـ web app تصبح هذه الدوال مكونات UI.

def kb_main_menu(user_id: int = 0, is_admin_user: bool = False) -> InlineKeyboardMarkup:
    """القائمة الرئيسية — تدعم أسماء مخصصة للأزرار (من لوحة الأدمن)."""
    bn = db.get_button_names(user_id) if user_id else {}
    rows = [
        [
            InlineKeyboardButton(text=bn.get("btn_quick_calc", "⚡ حساب سريع"),     callback_data="menu_quick_calc"),
            InlineKeyboardButton(text=bn.get("btn_sale",       "💰 تسجيل مبيعة"),  callback_data="menu_sale"),
        ],
        [
            InlineKeyboardButton(text=bn.get("btn_expense",   "💸 تسجيل مصروف"),  callback_data="menu_expense"),
            InlineKeyboardButton(text=bn.get("btn_inventory",  "📦 إدارة المخزون"), callback_data="menu_inventory"),
        ],
        [
            InlineKeyboardButton(text=bn.get("btn_fixed_exp", "📌 مصاريف ثابتة"), callback_data="menu_fixed_expenses"),
            InlineKeyboardButton(text=bn.get("btn_reports",   "📊 التقارير"),      callback_data="menu_reports"),
        ],
        [
            InlineKeyboardButton(text="✏️ تعديل/حذف مبيعة",  callback_data="menu_manage_sales"),
            InlineKeyboardButton(text="✏️ تعديل/حذف مصروف", callback_data="menu_manage_expenses"),
        ],
        [
            InlineKeyboardButton(text=bn.get("btn_settings",  "⚙️ الإعدادات"),    callback_data="menu_settings"),
        ],
    ]
    if is_admin_user:
        rows.append([
            InlineKeyboardButton(text="👑 لوحة الأدمن", callback_data="admin_panel"),
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_back_main() -> InlineKeyboardMarkup:
    """زر الرجوع للقائمة الرئيسية."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🏠 القائمة الرئيسية", callback_data="menu_main"),
    ]])


def kb_cancel() -> InlineKeyboardMarkup:
    """زر إلغاء لوقف أي FSM جارٍ."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm"),
    ]])


def kb_skip_or_cancel(skip_data: str = "skip") -> InlineKeyboardMarkup:
    """زرّا تخطي + إلغاء للخطوات الاختيارية في FSM."""
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="⏭ تخطي",   callback_data=skip_data),
        InlineKeyboardButton(text="❌ إلغاء",  callback_data="cancel_fsm"),
    ]])


def kb_payment_methods() -> InlineKeyboardMarkup:
    """أزرار اختيار طريقة الدفع (كاش / تحويل / آجل)."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💵 كاش",    callback_data="pay_كاش"),
            InlineKeyboardButton(text="🏦 تحويل",  callback_data="pay_تحويل"),
            InlineKeyboardButton(text="📋 آجل",    callback_data="pay_آجل"),
        ],
        [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
    ])


def kb_expense_categories(categories: list) -> InlineKeyboardMarkup:
    """
    لوحة مفاتيح ديناميكية لفئات المصاريف.
    تعرض الفئات زرّين في كل صف.
    callback_data = "expcat_{id}_{name[:15]}"
    """
    rows = []
    row  = []
    for cat in categories:
        row.append(InlineKeyboardButton(
            text=cat["name"],
            callback_data=f"expcat_{cat['id']}_{cat['name'][:15]}",
        ))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    rows.append([InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def kb_reports(user_id: int = 0) -> InlineKeyboardMarkup:
    """قائمة التقارير — تدعم أسماء مخصصة للأزرار."""
    bn = db.get_button_names(user_id) if user_id else {}
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=bn.get("btn_today", "📅 اليوم"),      callback_data="report_today"),
            InlineKeyboardButton(text=bn.get("btn_month", "📆 هذا الشهر"),  callback_data="report_month"),
        ],
        [
            InlineKeyboardButton(text=bn.get("btn_year",  "📅 هذا العام"),  callback_data="report_year"),
            InlineKeyboardButton(text=bn.get("btn_all",   "📈 إجمالي كامل"), callback_data="report_all"),
        ],
        [
            InlineKeyboardButton(text=bn.get("btn_top",   "🏆 أكثر مبيعاً"), callback_data="report_top"),
        ],
        [InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")],
    ])


def kb_inventory_menu() -> InlineKeyboardMarkup:
    """قائمة إدارة المخزون."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ إضافة منتج",      callback_data="inv_add_product"),
            InlineKeyboardButton(text="📥 إضافة مخزون",     callback_data="inv_add_stock"),
        ],
        [
            InlineKeyboardButton(text="📋 قائمة المنتجات",  callback_data="inv_list"),
            InlineKeyboardButton(text="⚠️ مخزون منخفض",    callback_data="inv_low"),
        ],
        [InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")],
    ])


def kb_fixed_expenses_menu() -> InlineKeyboardMarkup:
    """قائمة المصاريف الثابتة."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ إضافة مصروف ثابت",  callback_data="fe_add"),
            InlineKeyboardButton(text="📋 قائمة المصاريف",     callback_data="fe_list"),
        ],
        [
            InlineKeyboardButton(text="📊 ملخص المصاريف",      callback_data="fe_summary"),
        ],
        [InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")],
    ])


def kb_admin_panel() -> InlineKeyboardMarkup:
    """لوحة تحكم الأدمن."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ تفعيل اشتراك",     callback_data="adm_activate"),
            InlineKeyboardButton(text="👥 قائمة المستخدمين", callback_data="adm_users"),
        ],
        [
            InlineKeyboardButton(text="📊 تقرير عام",         callback_data="adm_global_report"),
            InlineKeyboardButton(text="⏰ اشتراكات منتهية",   callback_data="adm_expired"),
        ],
        [
            InlineKeyboardButton(text="📢 رسالة جماعية",      callback_data="adm_broadcast"),
            InlineKeyboardButton(text="🔘 تخصيص الأزرار",     callback_data="adm_buttons"),
        ],
        [
            InlineKeyboardButton(text="💳 معلومات الدفع",     callback_data="adm_payment_info"),
            InlineKeyboardButton(text="💰 تعديل الأسعار",     callback_data="adm_edit_prices"),
        ],
        [InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")],
    ])


def kb_activate_plan() -> InlineKeyboardMarkup:
    """خيارات خطط تفعيل الاشتراك."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📅 شهري (30 يوم)",   callback_data="plan_monthly_30"),
            InlineKeyboardButton(text="📆 سنوي (365 يوم)",  callback_data="plan_yearly_365"),
        ],
        [
            InlineKeyboardButton(text="♾️ مدى الحياة",     callback_data="plan_lifetime_0"),
            InlineKeyboardButton(text="🔬 تجريبي (7 أيام)",callback_data="plan_trial_7"),
        ],
        [
            InlineKeyboardButton(text="🚫 إلغاء الاشتراك", callback_data="plan_deactivate"),
        ],
        [InlineKeyboardButton(text="❌ رجوع", callback_data="cancel_fsm")],
    ])


# =============================================================================
# 3. دوال مساعدة
# =============================================================================

def is_admin(user_id: int) -> bool:
    """التحقق من أن المستخدم مشرف (موجود في ADMIN_IDS)."""
    return user_id in ADMIN_IDS


async def safe_edit(
    callback: CallbackQuery,
    text: str,
    markup: InlineKeyboardMarkup = None,
) -> None:
    """
    تعديل رسالة موجودة بأمان.
    يتجاهل خطأ 'message is not modified' (يحدث عند إعادة إرسال نفس المحتوى).
    """
    try:
        await callback.message.edit_text(text, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest:
        pass  # المحتوى لم يتغير — طبيعي
    await callback.answer()


async def check_subscription(
    user_id: int,
    callback: CallbackQuery = None,
    message: Message = None,
) -> bool:
    """
    التحقق من صلاحية اشتراك المستخدم.
    عند انتهاء الاشتراك يعرض رسالة مع زر الدفع ويعيد False.
    يعيد True إذا كان الاشتراك نشطاً.
    """
    if db.is_subscription_active(user_id):
        return True

    markup = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💳 معلومات الدفع / تجديد", callback_data="show_payment_info")],
        [InlineKeyboardButton(text="🏠 القائمة الرئيسية",       callback_data="menu_main")],
    ])
    expired_text = (
        "⚠️ <b>انتهت صلاحية اشتراكك.</b>\n\n"
        "لتجديد الاشتراك اضغط الزر أدناه\nأو تواصل مع الإدمن مباشرةً."
    )
    if callback:
        await safe_edit(callback, expired_text, markup)
    elif message:
        await message.answer(expired_text, reply_markup=markup, parse_mode="HTML")
    return False


def fmt_money(amount: float, currency: str = "ل.س") -> str:
    """
    تنسيق المبلغ مع رمز العملة.
    مثال: fmt_money(1500.5, "ل.س") → "1,500.50 ل.س"
    """
    return f"{amount:,.2f} {currency}"


def get_currency(user_id: int) -> str:
    """جلب رمز العملة للمستخدم من إعداداته."""
    settings = db.get_user_settings(user_id)
    return settings.get("currency", "ل.س")


def expense_type_ar(t: str) -> str:
    """ترجمة نوع المصروف الثابت للعربية."""
    return {"daily": "يومي 📆", "monthly": "شهري 📅", "yearly": "سنوي 🗓"}.get(t, t)


# أسماء خطط الاشتراك بالعربية
PLAN_NAMES_AR: dict = {
    "trial":    "تجريبي",
    "monthly":  "شهري",
    "yearly":   "سنوي",
    "lifetime": "مدى الحياة",
}


# =============================================================================
# 4. القائمة الرئيسية و /start
# =============================================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """
    معالج أمر /start.
    - يُسجّل المستخدم الجديد ويُنشئ له فترة تجريبية.
    - يُحدّث بيانات المستخدم القديم.
    - يعرض القائمة الرئيسية مع معلومات الاشتراك.
    """
    await state.clear()
    user = message.from_user

    db.setup_new_user(
        user_id=user.id,
        username=user.username,
        full_name=user.full_name,
        language_code=user.language_code or "ar",
    )

    sub = db.get_subscription(user.id)
    sub_line = ""
    if sub:
        plan = PLAN_NAMES_AR.get(sub["plan"], sub["plan"])
        end  = sub["end_date"][:10] if sub["end_date"] else "♾️"
        sub_line = f"\n📌 اشتراكك: <b>{plan}</b> — {end}"

    await message.answer(
        f"👋 أهلاً <b>{user.full_name or user.username}</b>!{sub_line}\n\n"
        f"اختر من القائمة:",
        reply_markup=kb_main_menu(user_id=user.id, is_admin_user=is_admin(user.id)),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "menu_main")
async def cb_main_menu(callback: CallbackQuery, state: FSMContext):
    """العودة للقائمة الرئيسية وإلغاء أي FSM جارٍ."""
    await state.clear()
    uid = callback.from_user.id
    await safe_edit(
        callback,
        "🏠 <b>القائمة الرئيسية</b>",
        kb_main_menu(user_id=uid, is_admin_user=is_admin(uid)),
    )


@router.callback_query(F.data == "cancel_fsm")
async def cb_cancel(callback: CallbackQuery, state: FSMContext):
    """إلغاء أي محادثة FSM جارية والعودة للقائمة الرئيسية."""
    await state.clear()
    uid = callback.from_user.id
    await safe_edit(
        callback,
        "❌ تم الإلغاء.\n\n🏠 <b>القائمة الرئيسية:</b>",
        kb_main_menu(user_id=uid, is_admin_user=is_admin(uid)),
    )


# =============================================================================
# 5. FSM — تسجيل المبيعات
# =============================================================================

@router.callback_query(F.data == "menu_sale")
async def cb_start_sale(callback: CallbackQuery, state: FSMContext):
    """بدء تسجيل مبيعة جديدة — الخطوة 1: اسم المنتج."""
    if not await check_subscription(callback.from_user.id, callback=callback):
        return
    await state.set_state(SaleStates.waiting_product_name)
    await safe_edit(
        callback,
        "💰 <b>تسجيل مبيعة جديدة</b>\n\n"
        "📝 <b>الخطوة 1/4</b>\nأدخل اسم المنتج:",
        kb_cancel(),
    )


@router.message(SaleStates.waiting_product_name)
async def fsm_sale_product_name(message: Message, state: FSMContext):
    """
    الخطوة 1: استقبال اسم المنتج.
    - إذا وُجد في المخزون: يُخزن بياناته (سعر التكلفة، المخزون، السعر المعتاد).
    - إذا لم يُوجد: يُسجَّل كمنتج جديد بدون خصم مخزون.
    """
    user_id = message.from_user.id
    name    = message.text.strip()

    if not name:
        await message.answer("⚠️ الاسم لا يمكن أن يكون فارغاً.", reply_markup=kb_cancel())
        return

    products = db.get_products(user_id)
    matched  = next((p for p in products if p["name"].lower() == name.lower()), None)

    if matched:
        await state.update_data(
            product_name=matched["name"],
            product_id=matched["id"],
            cost_price=matched["cost_price"],
            default_price=matched["selling_price"],
            stock=matched["stock_quantity"],
            unit=matched["unit"],
        )
        currency = get_currency(user_id)
        extra = (
            f"\n📦 مخزون متاح: <b>{matched['stock_quantity']} {matched['unit']}</b>"
            f"\n💡 السعر المعتاد: <b>{fmt_money(matched['selling_price'], currency)}</b>"
        )
    else:
        await state.update_data(
            product_name=name, product_id=None,
            cost_price=0.0, default_price=0.0,
            stock=None, unit="قطعة",
        )
        extra = "\n<i>📦 منتج جديد — لن يُخصم من المخزون</i>"

    await state.set_state(SaleStates.waiting_quantity)
    await message.answer(
        f"✅ المنتج: <b>{name}</b>{extra}\n\n"
        f"📝 <b>الخطوة 2/4</b>\nأدخل الكمية (يدعم الكسور مثل: 1.5):",
        reply_markup=kb_cancel(),
        parse_mode="HTML",
    )


@router.message(SaleStates.waiting_quantity)
async def fsm_sale_quantity(message: Message, state: FSMContext):
    """
    الخطوة 2: استقبال الكمية.
    يتحقق من أن الكمية لا تتجاوز المخزون المتاح.
    """
    try:
        qty = float(message.text.strip().replace(",", "."))
        if qty <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل كمية صحيحة (مثال: 1 أو 2.5)", reply_markup=kb_cancel())
        return

    data = await state.get_data()

    # التحقق من توفّر الكمية في المخزون
    if data.get("product_id") and data.get("stock") is not None:
        available = data["stock"]
        if qty > available:
            await message.answer(
                f"❌ <b>الكمية غير كافية!</b>\n\n"
                f"📦 المتوفر: <b>{available} {data['unit']}</b>\n"
                f"🛒 المطلوب: <b>{qty} {data['unit']}</b>\n\n"
                f"أدخل كمية لا تتجاوز <b>{available}</b>:",
                reply_markup=kb_cancel(),
                parse_mode="HTML",
            )
            return

    await state.update_data(quantity=qty)
    await state.set_state(SaleStates.waiting_price)

    hint = ""
    if data.get("default_price"):
        hint = f"\n💡 السعر المعتاد: <b>{data['default_price']}</b>"

    await message.answer(
        f"📝 <b>الخطوة 3/4</b>\nأدخل سعر الوحدة:{hint}",
        reply_markup=kb_cancel(),
        parse_mode="HTML",
    )


@router.message(SaleStates.waiting_price)
async def fsm_sale_price(message: Message, state: FSMContext):
    """الخطوة 3: استقبال سعر الوحدة."""
    try:
        price = float(message.text.strip().replace(",", "."))
        if price < 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل سعراً صحيحاً (مثال: 10 أو 9.99)", reply_markup=kb_cancel())
        return

    await state.update_data(unit_price=price)
    await state.set_state(SaleStates.waiting_payment)
    await message.answer("📝 <b>الخطوة 4/4</b>\nاختر طريقة الدفع:", reply_markup=kb_payment_methods(), parse_mode="HTML")


@router.callback_query(SaleStates.waiting_payment, F.data.startswith("pay_"))
async def fsm_sale_payment(callback: CallbackQuery, state: FSMContext):
    """الخطوة 4: استقبال طريقة الدفع، ثم طلب اسم العميل."""
    payment = callback.data.split("_", 1)[1]
    await state.update_data(payment_method=payment)
    await state.set_state(SaleStates.waiting_customer)
    await safe_edit(
        callback,
        "👤 أدخل اسم العميل (اختياري):",
        kb_skip_or_cancel("skip_customer"),
    )


@router.callback_query(SaleStates.waiting_customer, F.data == "skip_customer")
async def fsm_sale_skip_customer(callback: CallbackQuery, state: FSMContext):
    """تخطي إدخال اسم العميل وإنهاء تسجيل المبيعة."""
    await state.update_data(customer_name=None)
    await _finalize_sale(callback.message, state, callback.from_user.id)
    await callback.answer()


@router.message(SaleStates.waiting_customer)
async def fsm_sale_customer(message: Message, state: FSMContext):
    """استقبال اسم العميل وإنهاء تسجيل المبيعة."""
    await state.update_data(customer_name=message.text.strip() or None)
    await _finalize_sale(message, state, message.from_user.id)


async def _finalize_sale(message: Message, state: FSMContext, user_id: int):
    """
    [داخلي] حفظ المبيعة في قاعدة البيانات.
    يخصم المخزون تلقائياً إذا كان المنتج مرتبطاً بالمخزون.
    يعرض ملخص المبيعة مع تنبيه المخزون المنخفض إذا وُجد.
    """
    data = await state.get_data()
    await state.clear()

    currency = get_currency(user_id)
    qty      = data["quantity"]
    price    = data["unit_price"]
    cost     = data.get("cost_price", 0.0)
    total    = qty * price
    profit   = qty * (price - cost)

    try:
        sale_id = db.record_sale(
            user_id=user_id,
            product_name=data["product_name"],
            quantity=qty,
            unit_price=price,
            cost_price=cost,
            product_id=data.get("product_id"),
            payment_method=data.get("payment_method", "كاش"),
            customer_name=data.get("customer_name"),
        )

        # تنبيه إذا أصبح مخزون المنتج منخفضاً بعد البيع
        low_warn = ""
        if data.get("product_id"):
            low_items = db.get_low_stock_products(user_id)
            if any(p["id"] == data["product_id"] for p in low_items):
                prod     = next(p for p in low_items if p["id"] == data["product_id"])
                low_warn = (
                    f"\n\n⚠️ <b>تنبيه مخزون:</b> «{prod['name']}» "
                    f"وصل للحد الأدنى ({prod['stock_quantity']} {prod['unit']})"
                )

        await message.answer(
            f"✅ <b>تم تسجيل المبيعة #{sale_id}</b>\n"
            f"{'─'*26}\n"
            f"🛍 المنتج:    <b>{data['product_name']}</b>\n"
            f"📦 الكمية:    <b>{qty}</b>\n"
            f"💵 السعر:     <b>{fmt_money(price, currency)}</b>\n"
            f"💰 الإجمالي:  <b>{fmt_money(total, currency)}</b>\n"
            f"📈 الربح:     <b>{fmt_money(profit, currency)}</b>\n"
            f"💳 الدفع:     <b>{data.get('payment_method', 'كاش')}</b>"
            f"{low_warn}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💰 مبيعة أخرى", callback_data="menu_sale"),
                InlineKeyboardButton(text="🏠 القائمة",    callback_data="menu_main"),
            ]]),
            parse_mode="HTML",
        )
    except ValueError as e:
        await message.answer(f"❌ <b>خطأ:</b> {e}", reply_markup=kb_back_main(), parse_mode="HTML")
    except Exception as e:
        logger.error("خطأ في حفظ المبيعة: %s", e, exc_info=True)
        await message.answer("❌ حدث خطأ غير متوقع. حاول مجدداً.", reply_markup=kb_back_main())


# =============================================================================
# 6. FSM — تسجيل المصاريف
# =============================================================================

@router.callback_query(F.data == "menu_expense")
async def cb_start_expense(callback: CallbackQuery, state: FSMContext):
    """بدء تسجيل مصروف جديد — الخطوة 1: اختيار الفئة."""
    user_id = callback.from_user.id
    if not await check_subscription(user_id, callback=callback):
        return

    categories = db.get_expense_categories(user_id)
    await state.set_state(ExpenseStates.waiting_category)

    if categories:
        await safe_edit(
            callback,
            "💸 <b>تسجيل مصروف جديد</b>\n\n📝 <b>الخطوة 1/3</b>\nاختر نوع المصروف:",
            kb_expense_categories(categories),
        )
    else:
        await safe_edit(
            callback,
            "💸 <b>تسجيل مصروف</b>\n\n📝 أدخل نوع المصروف:",
            kb_cancel(),
        )


@router.callback_query(ExpenseStates.waiting_category, F.data.startswith("expcat_"))
async def fsm_expense_category_btn(callback: CallbackQuery, state: FSMContext):
    """استقبال الفئة من زر في القائمة."""
    parts    = callback.data.split("_", 2)
    cat_id   = int(parts[1])
    cat_name = parts[2]
    await state.update_data(category_id=cat_id, category_name=cat_name)
    await state.set_state(ExpenseStates.waiting_description)
    await safe_edit(
        callback,
        f"✅ الفئة: <b>{cat_name}</b>\n\n📝 <b>الخطوة 2/3</b>\nأدخل وصف المصروف:",
        kb_cancel(),
    )


@router.message(ExpenseStates.waiting_category)
async def fsm_expense_category_text(message: Message, state: FSMContext):
    """استقبال الفئة كنص (عندما لا توجد فئات مسجّلة)."""
    name = message.text.strip()
    await state.update_data(category_id=None, category_name=name)
    await state.set_state(ExpenseStates.waiting_description)
    await message.answer(
        f"✅ الفئة: <b>{name}</b>\n\n📝 <b>الخطوة 2/3</b>\nأدخل وصف المصروف:",
        reply_markup=kb_cancel(),
        parse_mode="HTML",
    )


@router.message(ExpenseStates.waiting_description)
async def fsm_expense_description(message: Message, state: FSMContext):
    """الخطوة 2: استقبال وصف المصروف."""
    desc = message.text.strip()
    if not desc:
        await message.answer("⚠️ الوصف لا يمكن أن يكون فارغاً.", reply_markup=kb_cancel())
        return
    await state.update_data(description=desc)
    await state.set_state(ExpenseStates.waiting_amount)
    await message.answer("📝 <b>الخطوة 3/3</b>\nأدخل قيمة المصروف:", reply_markup=kb_cancel(), parse_mode="HTML")


@router.message(ExpenseStates.waiting_amount)
async def fsm_expense_amount(message: Message, state: FSMContext):
    """الخطوة 3: استقبال مبلغ المصروف."""
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل قيمة صحيحة (مثال: 50 أو 149.99)", reply_markup=kb_cancel())
        return
    await state.update_data(amount=amount)
    await state.set_state(ExpenseStates.waiting_payment)
    await message.answer("💳 اختر طريقة الدفع:", reply_markup=kb_payment_methods())


@router.callback_query(ExpenseStates.waiting_payment, F.data.startswith("pay_"))
async def fsm_expense_payment(callback: CallbackQuery, state: FSMContext):
    """الخطوة الأخيرة: استقبال طريقة الدفع وحفظ المصروف."""
    payment = callback.data.split("_", 1)[1]
    data    = await state.get_data()
    await state.clear()

    user_id  = callback.from_user.id
    currency = get_currency(user_id)

    try:
        expense_id = db.record_expense(
            user_id=user_id,
            description=data["description"],
            amount=data["amount"],
            category_id=data.get("category_id"),
            payment_method=payment,
        )
        await safe_edit(
            callback,
            f"✅ <b>تم تسجيل المصروف #{expense_id}</b>\n"
            f"{'─'*26}\n"
            f"📂 الفئة:   <b>{data.get('category_name', 'غير محددة')}</b>\n"
            f"📝 الوصف:   <b>{data['description']}</b>\n"
            f"💸 المبلغ:  <b>{fmt_money(data['amount'], currency)}</b>\n"
            f"💳 الدفع:   <b>{payment}</b>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="💸 مصروف آخر", callback_data="menu_expense"),
                InlineKeyboardButton(text="🏠 القائمة",   callback_data="menu_main"),
            ]]),
        )
    except Exception as e:
        logger.error("خطأ في حفظ المصروف: %s", e, exc_info=True)
        await safe_edit(callback, "❌ حدث خطأ. حاول مجدداً.", kb_back_main())


# =============================================================================
# 7. إدارة المخزون
# =============================================================================

@router.callback_query(F.data == "menu_inventory")
async def cb_inventory_menu(callback: CallbackQuery, state: FSMContext):
    """عرض قائمة المخزون."""
    await state.clear()
    if not await check_subscription(callback.from_user.id, callback=callback):
        return
    await safe_edit(callback, "📦 <b>إدارة المخزون</b>", kb_inventory_menu())


# ── إضافة منتج جديد ──

@router.callback_query(F.data == "inv_add_product")
async def cb_add_product_start(callback: CallbackQuery, state: FSMContext):
    """بدء إضافة منتج جديد — الخطوة 1: الاسم."""
    await state.set_state(ProductStates.waiting_name)
    await safe_edit(callback, "📦 <b>إضافة منتج جديد</b>\n\nأدخل اسم المنتج:", kb_cancel())


@router.message(ProductStates.waiting_name)
async def fsm_product_name(message: Message, state: FSMContext):
    """استقبال اسم المنتج مع التحقق من عدم التكرار."""
    name = message.text.strip()
    if not name:
        await message.answer("⚠️ الاسم لا يمكن أن يكون فارغاً.", reply_markup=kb_cancel())
        return
    if any(p["name"].lower() == name.lower() for p in db.get_products(message.from_user.id)):
        await message.answer(f"⚠️ المنتج «{name}» موجود مسبقاً.", reply_markup=kb_back_main())
        await state.clear()
        return
    await state.update_data(name=name)
    await state.set_state(ProductStates.waiting_price)
    await message.answer("💵 أدخل سعر البيع:", reply_markup=kb_cancel())


@router.message(ProductStates.waiting_price)
async def fsm_product_price(message: Message, state: FSMContext):
    """استقبال سعر البيع."""
    try:
        price = float(message.text.strip().replace(",", "."))
        if price < 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل سعراً صحيحاً.", reply_markup=kb_cancel())
        return
    await state.update_data(selling_price=price)
    await state.set_state(ProductStates.waiting_cost)
    await message.answer("💰 أدخل سعر التكلفة:", reply_markup=kb_skip_or_cancel("skip_cost"))


@router.callback_query(ProductStates.waiting_cost, F.data == "skip_cost")
async def fsm_product_skip_cost(callback: CallbackQuery, state: FSMContext):
    """تخطي سعر التكلفة (يُضبط على 0)."""
    await state.update_data(cost_price=0.0)
    await state.set_state(ProductStates.waiting_stock)
    await safe_edit(callback, "📦 أدخل الكمية الأولية:", kb_skip_or_cancel("skip_stock"))


@router.message(ProductStates.waiting_cost)
async def fsm_product_cost(message: Message, state: FSMContext):
    """استقبال سعر التكلفة."""
    try:
        cost = float(message.text.strip().replace(",", "."))
        if cost < 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل تكلفة صحيحة.", reply_markup=kb_cancel())
        return
    await state.update_data(cost_price=cost)
    await state.set_state(ProductStates.waiting_stock)
    await message.answer("📦 أدخل الكمية الأولية:", reply_markup=kb_skip_or_cancel("skip_stock"))


@router.callback_query(ProductStates.waiting_stock, F.data == "skip_stock")
async def fsm_product_skip_stock(callback: CallbackQuery, state: FSMContext):
    """تخطي الكمية الأولية (تبدأ من 0)."""
    await state.update_data(stock_quantity=0.0)
    await state.set_state(ProductStates.waiting_unit)
    await safe_edit(callback, "📏 أدخل وحدة القياس (قطعة / كيلو / لتر ...):", kb_skip_or_cancel("skip_unit"))


@router.message(ProductStates.waiting_stock)
async def fsm_product_stock(message: Message, state: FSMContext):
    """استقبال الكمية الأولية."""
    try:
        stock = float(message.text.strip().replace(",", "."))
        if stock < 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل كمية صحيحة.", reply_markup=kb_cancel())
        return
    await state.update_data(stock_quantity=stock)
    await state.set_state(ProductStates.waiting_unit)
    await message.answer("📏 أدخل وحدة القياس (قطعة / كيلو / لتر ...):", reply_markup=kb_skip_or_cancel("skip_unit"))


@router.callback_query(ProductStates.waiting_unit, F.data == "skip_unit")
async def fsm_product_skip_unit(callback: CallbackQuery, state: FSMContext):
    """تخطي وحدة القياس (تُضبط على 'قطعة')."""
    await state.update_data(unit="قطعة")
    await _finalize_product(callback.message, state, callback.from_user.id)
    await callback.answer()


@router.message(ProductStates.waiting_unit)
async def fsm_product_unit(message: Message, state: FSMContext):
    """استقبال وحدة القياس وإنهاء إضافة المنتج."""
    await state.update_data(unit=message.text.strip() or "قطعة")
    await _finalize_product(message, state, message.from_user.id)


async def _finalize_product(message: Message, state: FSMContext, user_id: int):
    """[داخلي] حفظ المنتج في قاعدة البيانات وعرض ملخص الإضافة."""
    data = await state.get_data()
    await state.clear()
    currency = get_currency(user_id)
    try:
        pid = db.add_product(
            user_id=user_id,
            name=data["name"],
            selling_price=data["selling_price"],
            cost_price=data.get("cost_price", 0.0),
            stock_quantity=data.get("stock_quantity", 0.0),
            unit=data.get("unit", "قطعة"),
        )
        await message.answer(
            f"✅ <b>تم إضافة المنتج #{pid}</b>\n"
            f"{'─'*26}\n"
            f"🏷 الاسم:      <b>{data['name']}</b>\n"
            f"💵 سعر البيع:  <b>{fmt_money(data['selling_price'], currency)}</b>\n"
            f"💰 التكلفة:    <b>{fmt_money(data.get('cost_price', 0.0), currency)}</b>\n"
            f"📦 المخزون:    <b>{data.get('stock_quantity', 0.0)} {data.get('unit', 'قطعة')}</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ منتج آخر", callback_data="inv_add_product"),
                InlineKeyboardButton(text="🏠 القائمة",  callback_data="menu_main"),
            ]]),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("خطأ في إضافة المنتج: %s", e, exc_info=True)
        await message.answer("❌ حدث خطأ. حاول مجدداً.", reply_markup=kb_back_main())


# ── إضافة مخزون لمنتج موجود ──

@router.callback_query(F.data == "inv_add_stock")
async def cb_add_stock_start(callback: CallbackQuery, state: FSMContext):
    """بدء إضافة مخزون لمنتج موجود."""
    user_id = callback.from_user.id
    if not db.get_products(user_id):
        await safe_edit(
            callback,
            "⚠️ لا توجد منتجات بعد. أضف منتجاً أولاً.",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ إضافة منتج", callback_data="inv_add_product"),
                InlineKeyboardButton(text="🏠 رجوع",       callback_data="menu_main"),
            ]]),
        )
        return
    await state.set_state(StockAddStates.waiting_product_name)
    await safe_edit(callback, "📥 <b>إضافة مخزون</b>\n\nأدخل اسم المنتج:", kb_cancel())


@router.message(StockAddStates.waiting_product_name)
async def fsm_addstock_product(message: Message, state: FSMContext):
    """استقبال اسم المنتج للبحث في قائمة المنتجات الموجودة."""
    user_id = message.from_user.id
    name    = message.text.strip()
    matched = next(
        (p for p in db.get_products(user_id) if p["name"].lower() == name.lower()),
        None,
    )
    if not matched:
        await message.answer(f"⚠️ المنتج «{name}» غير موجود. تأكد من الاسم.", reply_markup=kb_cancel())
        return
    await state.update_data(
        product_id=matched["id"],
        product_name=matched["name"],
        unit=matched["unit"],
        current_stock=matched["stock_quantity"],
    )
    await state.set_state(StockAddStates.waiting_quantity)
    await message.answer(
        f"📦 <b>{matched['name']}</b>\n"
        f"المخزون الحالي: <b>{matched['stock_quantity']} {matched['unit']}</b>\n\n"
        f"أدخل الكمية المضافة:",
        reply_markup=kb_cancel(),
        parse_mode="HTML",
    )


@router.message(StockAddStates.waiting_quantity)
async def fsm_addstock_qty(message: Message, state: FSMContext):
    """استقبال الكمية المضافة وتحديث المخزون."""
    try:
        qty = float(message.text.strip().replace(",", "."))
        if qty <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل كمية صحيحة أكبر من صفر.", reply_markup=kb_cancel())
        return

    data    = await state.get_data()
    await state.clear()
    user_id = message.from_user.id

    try:
        new_qty = db.update_stock(
            user_id=user_id,
            product_id=data["product_id"],
            quantity_change=qty,
            change_type="purchase",
            notes="إضافة مخزون يدوي",
        )
        await message.answer(
            f"✅ <b>تم تحديث المخزون</b>\n"
            f"{'─'*26}\n"
            f"🏷 المنتج:          <b>{data['product_name']}</b>\n"
            f"➕ الكمية المضافة:  <b>{qty} {data['unit']}</b>\n"
            f"📦 المخزون الجديد:  <b>{new_qty} {data['unit']}</b>",
            reply_markup=kb_back_main(),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error("خطأ في تحديث المخزون: %s", e)
        await message.answer(f"❌ خطأ: {e}", reply_markup=kb_back_main())


# ── قائمة المنتجات ──

@router.callback_query(F.data == "inv_list")
async def cb_product_list(callback: CallbackQuery):
    """عرض قائمة جميع المنتجات مع المخزون والسعر."""
    user_id  = callback.from_user.id
    products = db.get_products(user_id)
    currency = get_currency(user_id)

    if not products:
        await safe_edit(
            callback,
            "📦 لا توجد منتجات مضافة بعد.",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ إضافة منتج", callback_data="inv_add_product"),
                InlineKeyboardButton(text="🏠 رجوع",       callback_data="menu_main"),
            ]]),
        )
        return

    lines = ["📋 <b>قائمة المنتجات:</b>\n"]
    for p in products[:15]:
        icon = "✅" if p["stock_quantity"] > p.get("min_stock_alert", 0) else "⚠️"
        lines.append(
            f"{icon} <b>{p['name']}</b>\n"
            f"   💵 {fmt_money(p['selling_price'], currency)}  📦 {p['stock_quantity']} {p['unit']}"
        )
    if len(products) > 15:
        lines.append(f"\n<i>... و{len(products)-15} منتج آخر</i>")

    await safe_edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 المخزون", callback_data="menu_inventory"),
            InlineKeyboardButton(text="🏠 القائمة", callback_data="menu_main"),
        ]]),
    )


# ── مخزون منخفض ──

@router.callback_query(F.data == "inv_low")
async def cb_low_stock(callback: CallbackQuery):
    """عرض المنتجات التي وصلت للحد الأدنى من المخزون."""
    low = db.get_low_stock_products(callback.from_user.id)
    if not low:
        await safe_edit(callback, "✅ لا توجد منتجات بمخزون منخفض.", kb_inventory_menu())
        return
    lines = [f"⚠️ <b>منتجات بمخزون منخفض ({len(low)}):</b>\n"]
    for p in low:
        lines.append(
            f"🔴 <b>{p['name']}</b>\n"
            f"   متوفر: {p['stock_quantity']}  |  الحد: {p['min_stock_alert']} {p['unit']}"
        )
    await safe_edit(callback, "\n".join(lines), kb_inventory_menu())


# =============================================================================
# 8. تعديل وحذف المبيعات والمصاريف
# =============================================================================

# ── عرض آخر المبيعات مع أزرار التعديل/الحذف ──

@router.callback_query(F.data == "menu_manage_sales")
async def cb_manage_sales(callback: CallbackQuery, state: FSMContext):
    """عرض آخر 10 مبيعات مع أزرار تعديل وحذف."""
    await state.clear()
    user_id  = callback.from_user.id
    if not await check_subscription(user_id, callback=callback):
        return
    currency = get_currency(user_id)
    sales    = db.get_sales(user_id, limit=10)
    if not sales:
        await safe_edit(callback, "📋 لا توجد مبيعات مسجّلة بعد.", kb_back_main())
        return
    lines = ["📋 <b>آخر المبيعات:</b>\n"]
    rows  = []
    for s in sales:
        date  = s["sale_date"][:10]
        total = s["quantity"] * s["unit_price"]
        lines.append(f"#{s['id']} | <b>{s['product_name']}</b> | {total:,.0f} | {date}")
        rows.append([
            InlineKeyboardButton(text=f"✏️ #{s['id']}", callback_data=f"sale_edit_{s['id']}"),
            InlineKeyboardButton(text=f"🗑 #{s['id']}", callback_data=f"sale_del_{s['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")])
    await safe_edit(callback, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))


# ── حذف مبيعة ──

@router.callback_query(F.data.startswith("sale_del_"))
async def cb_sale_delete_confirm(callback: CallbackQuery):
    """طلب تأكيد حذف المبيعة."""
    sale_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    sale    = db.get_sale(user_id, sale_id)
    if not sale:
        await callback.answer("❌ المبيعة غير موجودة.", show_alert=True)
        return
    total = sale["quantity"] * sale["unit_price"]
    await safe_edit(
        callback,
        f"🗑 <b>تأكيد حذف المبيعة #{sale_id}</b>\n"
        f"{'─'*26}\n"
        f"🛍 {sale['product_name']} | الكمية: {sale['quantity']} | الإجمالي: {total:,.0f}\n\n"
        f"⚠️ سيتم إعادة الكمية للمخزون تلقائياً إن كانت مرتبطة بمنتج.",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ نعم، احذف", callback_data=f"sale_del_confirm_{sale_id}"),
                InlineKeyboardButton(text="❌ إلغاء",     callback_data="menu_manage_sales"),
            ]
        ]),
    )


@router.callback_query(F.data.startswith("sale_del_confirm_"))
async def cb_sale_delete_execute(callback: CallbackQuery):
    """تنفيذ حذف المبيعة."""
    sale_id = int(callback.data.split("_")[3])
    ok      = db.delete_sale(callback.from_user.id, sale_id)
    if ok:
        await safe_edit(
            callback,
            f"✅ <b>تم حذف المبيعة #{sale_id} بنجاح.</b>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📋 قائمة المبيعات", callback_data="menu_manage_sales"),
                InlineKeyboardButton(text="🏠 القائمة",        callback_data="menu_main"),
            ]]),
        )
    else:
        await safe_edit(callback, "❌ خطأ في الحذف.", kb_back_main())


# ── تعديل مبيعة (FSM) ──

@router.callback_query(F.data.startswith("sale_edit_"))
async def cb_sale_edit_start(callback: CallbackQuery, state: FSMContext):
    """عرض تفاصيل المبيعة مع أزرار اختيار الحقل المراد تعديله."""
    sale_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    sale    = db.get_sale(user_id, sale_id)
    if not sale:
        await callback.answer("❌ المبيعة غير موجودة.", show_alert=True)
        return
    currency = get_currency(user_id)
    await state.update_data(edit_sale_id=sale_id, sale_data=sale)
    await state.set_state(EditSaleStates.waiting_field)
    await safe_edit(
        callback,
        f"✏️ <b>تعديل المبيعة #{sale_id}</b>\n"
        f"{'─'*26}\n"
        f"🛍 المنتج:   <b>{sale['product_name']}</b>\n"
        f"📦 الكمية:   <b>{sale['quantity']}</b>\n"
        f"💵 السعر:    <b>{fmt_money(sale['unit_price'], currency)}</b>\n"
        f"💳 الدفع:    <b>{sale['payment_method']}</b>\n"
        f"👤 العميل:   <b>{sale['customer_name'] or '—'}</b>\n\n"
        f"اختر الحقل الذي تريد تعديله:",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🛍 اسم المنتج", callback_data="salefield_product_name"),
                InlineKeyboardButton(text="📦 الكمية",     callback_data="salefield_quantity"),
            ],
            [
                InlineKeyboardButton(text="💵 السعر",      callback_data="salefield_unit_price"),
                InlineKeyboardButton(text="🏭 التكلفة",    callback_data="salefield_cost_price"),
            ],
            [
                InlineKeyboardButton(text="💳 طريقة الدفع", callback_data="salefield_payment_method"),
                InlineKeyboardButton(text="👤 اسم العميل",  callback_data="salefield_customer_name"),
            ],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
        ]),
    )


@router.callback_query(EditSaleStates.waiting_field, F.data.startswith("salefield_"))
async def cb_sale_field_selected(callback: CallbackQuery, state: FSMContext):
    """استقبال الحقل المختار وطلب القيمة الجديدة."""
    field = callback.data.split("_", 1)[1]
    await state.update_data(edit_field=field)
    await state.set_state(EditSaleStates.waiting_new_value)

    field_labels = {
        "product_name":   "اسم المنتج",
        "quantity":       "الكمية",
        "unit_price":     "سعر الوحدة",
        "cost_price":     "سعر التكلفة",
        "payment_method": "طريقة الدفع (كاش / تحويل / آجل)",
        "customer_name":  "اسم العميل",
    }
    label = field_labels.get(field, field)

    if field == "payment_method":
        await safe_edit(
            callback,
            f"💳 اختر طريقة الدفع الجديدة:",
            InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="💵 كاش",   callback_data="saleval_كاش"),
                    InlineKeyboardButton(text="🏦 تحويل", callback_data="saleval_تحويل"),
                    InlineKeyboardButton(text="📋 آجل",   callback_data="saleval_آجل"),
                ],
                [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
            ]),
        )
    else:
        await safe_edit(
            callback,
            f"✏️ أدخل القيمة الجديدة لـ <b>{label}</b>:",
            kb_cancel(),
        )


@router.callback_query(EditSaleStates.waiting_new_value, F.data.startswith("saleval_"))
async def cb_sale_payment_value(callback: CallbackQuery, state: FSMContext):
    """استقبال طريقة الدفع الجديدة من الأزرار."""
    new_val = callback.data.split("_", 1)[1]
    await _apply_sale_edit(callback.message, state, callback.from_user.id, new_val)
    await callback.answer()


@router.message(EditSaleStates.waiting_new_value)
async def fsm_sale_new_value(message: Message, state: FSMContext):
    """استقبال القيمة الجديدة نصياً وتطبيق التعديل."""
    await _apply_sale_edit(message, state, message.from_user.id, message.text.strip())


async def _apply_sale_edit(message: Message, state: FSMContext, user_id: int, raw_value: str):
    """[داخلي] التحقق من القيمة وتطبيق تعديل المبيعة."""
    data  = await state.get_data()
    field = data["edit_field"]
    sale  = data["sale_data"]

    # تحويل القيمة حسب نوع الحقل
    numeric_fields = {"quantity", "unit_price", "cost_price"}
    if field in numeric_fields:
        try:
            value = float(raw_value.replace(",", "."))
            if value < 0:
                raise ValueError()
        except ValueError:
            await message.answer("⚠️ أدخل قيمة رقمية صحيحة.", reply_markup=kb_cancel())
            return
    else:
        value = raw_value or None

    # تحديث القيمة في بيانات المبيعة المؤقتة
    sale[field] = value

    await state.clear()
    ok = db.update_sale(
        user_id=user_id,
        sale_id=data["edit_sale_id"],
        product_name=sale["product_name"],
        quantity=sale["quantity"],
        unit_price=sale["unit_price"],
        cost_price=sale["cost_price"],
        payment_method=sale["payment_method"],
        customer_name=sale.get("customer_name"),
        notes=sale.get("notes"),
    )
    if ok:
        currency = get_currency(user_id)
        await message.answer(
            f"✅ <b>تم تعديل المبيعة #{data['edit_sale_id']}</b>\n"
            f"{'─'*26}\n"
            f"🛍 {sale['product_name']} | "
            f"📦 {sale['quantity']} | "
            f"💵 {fmt_money(sale['unit_price'], currency)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📋 قائمة المبيعات", callback_data="menu_manage_sales"),
                InlineKeyboardButton(text="🏠 القائمة",        callback_data="menu_main"),
            ]]),
            parse_mode="HTML",
        )
    else:
        await message.answer("❌ خطأ في التعديل.", reply_markup=kb_back_main())


# ── عرض آخر المصاريف مع أزرار التعديل/الحذف ──

@router.callback_query(F.data == "menu_manage_expenses")
async def cb_manage_expenses(callback: CallbackQuery, state: FSMContext):
    """عرض آخر 10 مصاريف مع أزرار تعديل وحذف."""
    await state.clear()
    user_id  = callback.from_user.id
    if not await check_subscription(user_id, callback=callback):
        return
    currency = get_currency(user_id)
    expenses = db.get_expenses(user_id, limit=10)
    if not expenses:
        await safe_edit(callback, "📋 لا توجد مصاريف مسجّلة بعد.", kb_back_main())
        return
    lines = ["📋 <b>آخر المصاريف:</b>\n"]
    rows  = []
    for e in expenses:
        date = e["expense_date"][:10]
        lines.append(f"#{e['id']} | <b>{e['description'][:20]}</b> | {e['amount']:,.0f} | {date}")
        rows.append([
            InlineKeyboardButton(text=f"✏️ #{e['id']}", callback_data=f"exp_edit_{e['id']}"),
            InlineKeyboardButton(text=f"🗑 #{e['id']}", callback_data=f"exp_del_{e['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")])
    await safe_edit(callback, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))


# ── حذف مصروف ──

@router.callback_query(F.data.startswith("exp_del_") & ~F.data.startswith("exp_del_confirm_"))
async def cb_expense_delete_confirm(callback: CallbackQuery):
    """طلب تأكيد حذف المصروف."""
    expense_id = int(callback.data.split("_")[2])
    user_id    = callback.from_user.id
    expense    = db.get_expense(user_id, expense_id)
    if not expense:
        await callback.answer("❌ المصروف غير موجود.", show_alert=True)
        return
    await safe_edit(
        callback,
        f"🗑 <b>تأكيد حذف المصروف #{expense_id}</b>\n"
        f"{'─'*26}\n"
        f"📝 {expense['description']} | المبلغ: {expense['amount']:,.0f}\n\n"
        f"⚠️ هذا الإجراء لا يمكن التراجع عنه.",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ نعم، احذف", callback_data=f"exp_del_confirm_{expense_id}"),
                InlineKeyboardButton(text="❌ إلغاء",     callback_data="menu_manage_expenses"),
            ]
        ]),
    )


@router.callback_query(F.data.startswith("exp_del_confirm_"))
async def cb_expense_delete_execute(callback: CallbackQuery):
    """تنفيذ حذف المصروف."""
    expense_id = int(callback.data.split("_")[3])
    ok         = db.delete_expense(callback.from_user.id, expense_id)
    if ok:
        await safe_edit(
            callback,
            f"✅ <b>تم حذف المصروف #{expense_id} بنجاح.</b>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📋 قائمة المصاريف", callback_data="menu_manage_expenses"),
                InlineKeyboardButton(text="🏠 القائمة",         callback_data="menu_main"),
            ]]),
        )
    else:
        await safe_edit(callback, "❌ خطأ في الحذف.", kb_back_main())


# ── تعديل مصروف (FSM) ──

@router.callback_query(F.data.startswith("exp_edit_"))
async def cb_expense_edit_start(callback: CallbackQuery, state: FSMContext):
    """عرض تفاصيل المصروف مع أزرار اختيار الحقل المراد تعديله."""
    expense_id = int(callback.data.split("_")[2])
    user_id    = callback.from_user.id
    expense    = db.get_expense(user_id, expense_id)
    if not expense:
        await callback.answer("❌ المصروف غير موجود.", show_alert=True)
        return
    currency = get_currency(user_id)
    await state.update_data(edit_expense_id=expense_id, expense_data=expense)
    await state.set_state(EditExpenseStates.waiting_field)
    await safe_edit(
        callback,
        f"✏️ <b>تعديل المصروف #{expense_id}</b>\n"
        f"{'─'*26}\n"
        f"📝 الوصف:    <b>{expense['description']}</b>\n"
        f"💸 المبلغ:   <b>{fmt_money(expense['amount'], currency)}</b>\n"
        f"💳 الدفع:    <b>{expense['payment_method']}</b>\n\n"
        f"اختر الحقل الذي تريد تعديله:",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📝 الوصف",        callback_data="expfield_description"),
                InlineKeyboardButton(text="💸 المبلغ",       callback_data="expfield_amount"),
            ],
            [
                InlineKeyboardButton(text="💳 طريقة الدفع",  callback_data="expfield_payment_method"),
            ],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
        ]),
    )


@router.callback_query(EditExpenseStates.waiting_field, F.data.startswith("expfield_"))
async def cb_expense_field_selected(callback: CallbackQuery, state: FSMContext):
    """استقبال الحقل المختار وطلب القيمة الجديدة."""
    field = callback.data.split("_", 1)[1]
    await state.update_data(edit_field=field)
    await state.set_state(EditExpenseStates.waiting_new_value)

    if field == "payment_method":
        await safe_edit(
            callback,
            "💳 اختر طريقة الدفع الجديدة:",
            InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="💵 كاش",   callback_data="expval_كاش"),
                    InlineKeyboardButton(text="🏦 تحويل", callback_data="expval_تحويل"),
                    InlineKeyboardButton(text="📋 آجل",   callback_data="expval_آجل"),
                ],
                [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
            ]),
        )
    else:
        field_labels = {"description": "الوصف", "amount": "المبلغ"}
        label = field_labels.get(field, field)
        await safe_edit(callback, f"✏️ أدخل القيمة الجديدة لـ <b>{label}</b>:", kb_cancel())


@router.callback_query(EditExpenseStates.waiting_new_value, F.data.startswith("expval_"))
async def cb_expense_payment_value(callback: CallbackQuery, state: FSMContext):
    """استقبال طريقة الدفع الجديدة من الأزرار."""
    new_val = callback.data.split("_", 1)[1]
    await _apply_expense_edit(callback.message, state, callback.from_user.id, new_val)
    await callback.answer()


@router.message(EditExpenseStates.waiting_new_value)
async def fsm_expense_new_value(message: Message, state: FSMContext):
    """استقبال القيمة الجديدة نصياً وتطبيق التعديل."""
    await _apply_expense_edit(message, state, message.from_user.id, message.text.strip())


async def _apply_expense_edit(message: Message, state: FSMContext, user_id: int, raw_value: str):
    """[داخلي] التحقق من القيمة وتطبيق تعديل المصروف."""
    data    = await state.get_data()
    field   = data["edit_field"]
    expense = data["expense_data"]

    if field == "amount":
        try:
            value = float(raw_value.replace(",", "."))
            if value <= 0:
                raise ValueError()
        except ValueError:
            await message.answer("⚠️ أدخل قيمة رقمية صحيحة.", reply_markup=kb_cancel())
            return
    else:
        value = raw_value or expense[field]

    expense[field] = value
    await state.clear()

    ok = db.update_expense(
        user_id=user_id,
        expense_id=data["edit_expense_id"],
        description=expense["description"],
        amount=expense["amount"],
        category_id=expense.get("category_id"),
        payment_method=expense["payment_method"],
        notes=expense.get("notes"),
    )
    if ok:
        currency = get_currency(user_id)
        await message.answer(
            f"✅ <b>تم تعديل المصروف #{data['edit_expense_id']}</b>\n"
            f"{'─'*26}\n"
            f"📝 {expense['description']} | 💸 {fmt_money(expense['amount'], currency)}",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📋 قائمة المصاريف", callback_data="menu_manage_expenses"),
                InlineKeyboardButton(text="🏠 القائمة",         callback_data="menu_main"),
            ]]),
            parse_mode="HTML",
        )
    else:
        await message.answer("❌ خطأ في التعديل.", reply_markup=kb_back_main())


# =============================================================================
# 9. التقارير
# =============================================================================

@router.callback_query(F.data == "menu_reports")
async def cb_reports_menu(callback: CallbackQuery):
    """عرض قائمة التقارير."""
    if not await check_subscription(callback.from_user.id, callback=callback):
        return
    await safe_edit(callback, "📊 <b>التقارير</b>\nاختر الفترة:", kb_reports(user_id=callback.from_user.id))


@router.callback_query(F.data.in_({"report_today", "report_month", "report_all", "report_year"}))
async def cb_profit_report(callback: CallbackQuery):
    """
    عرض تقرير الأرباح لفترة محددة.
    يدعم: اليوم / هذا الشهر / هذا العام / إجمالي كامل.
    تقرير اليوم يتضمن مقارنة بالأمس.
    """
    user_id  = callback.from_user.id
    currency = get_currency(user_id)
    now      = datetime.utcnow()

    if callback.data == "report_today":
        start = now.strftime("%Y-%m-%dT00:00:00")
        label = "📅 تقرير اليوم"
    elif callback.data == "report_month":
        start = now.strftime("%Y-%m-01T00:00:00")
        label = "📆 تقرير هذا الشهر"
    elif callback.data == "report_year":
        start = now.strftime("%Y-01-01T00:00:00")
        label = f"📅 تقرير عام {now.year}"
    else:  # report_all
        start = None
        label = "📈 التقرير الإجمالي"

    try:
        s    = db.get_profit_summary(user_id, start_date=start)
        icon = "📈" if s["net_profit"] >= 0 else "📉"

        # مقارنة تقرير اليوم بالأمس
        comparison_line = ""
        if callback.data == "report_today":
            cmp     = db.get_daily_comparison(user_id)
            diff_s  = cmp["diff_sales"]
            diff_p  = cmp["diff_profit"]
            arrow_s = "▲" if diff_s >= 0 else "▼"
            arrow_p = "▲" if diff_p >= 0 else "▼"
            comparison_line = (
                f"\n{'─'*26}\n"
                f"🔄 <b>مقارنة بالأمس:</b>\n"
                f"  مبيعات أمس: <b>{fmt_money(cmp['yesterday']['total_sales'], currency)}</b>\n"
                f"  {arrow_s} المبيعات: <b>{abs(diff_s)}%</b>  |  {arrow_p} الربح: <b>{abs(diff_p)}%</b>"
            )

        # حصة الشريك (إذا كانت مفعّلة)
        partner_line = ""
        if s["partner_pct"] > 0:
            partner_line = (
                f"\n{'─'*26}\n"
                f"🤝 حصة الشريك ({s['partner_pct']}%): <b>{fmt_money(s['partner_amount'], currency)}</b>\n"
                f"👤 صافي حصتك: <b>{fmt_money(s['owner_profit'], currency)}</b>"
            )

        await safe_edit(
            callback,
            f"<b>{label}</b>\n"
            f"{'─'*26}\n"
            f"💰 المبيعات:    <b>{fmt_money(s['total_sales'], currency)}</b>\n"
            f"🏭 التكلفة:     <b>{fmt_money(s['total_cost'], currency)}</b>\n"
            f"📊 ربح إجمالي: <b>{fmt_money(s['gross_profit'], currency)}</b>\n"
            f"💸 المصاريف:   <b>{fmt_money(s['total_expenses'], currency)}</b>\n"
            f"{'─'*26}\n"
            f"{icon} <b>صافي الربح: {fmt_money(s['net_profit'], currency)}</b>\n"
            f"{'─'*26}\n"
            f"🛒 {s['sales_count']} مبيعة  |  📋 {s['expenses_count']} مصروف"
            f"{partner_line}"
            f"{comparison_line}",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 التقارير", callback_data="menu_reports"),
                InlineKeyboardButton(text="🏠 القائمة",  callback_data="menu_main"),
            ]]),
        )
    except Exception as e:
        logger.error("خطأ في التقرير: %s", e)
        await safe_edit(callback, "❌ خطأ في جلب التقرير.", kb_back_main())


@router.callback_query(F.data == "report_top")
async def cb_top_products(callback: CallbackQuery):
    """عرض أكثر 5 منتجات مبيعاً."""
    user_id  = callback.from_user.id
    currency = get_currency(user_id)
    try:
        top = db.get_top_selling_products(user_id, limit=5)
        if not top:
            await safe_edit(callback, "📊 لا توجد مبيعات مسجلة بعد.", kb_reports(user_id=user_id))
            return
        medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣"]
        lines  = ["🏆 <b>أكثر المنتجات مبيعاً:</b>\n"]
        for i, p in enumerate(top):
            lines.append(
                f"{medals[i]} <b>{p['product_name']}</b>\n"
                f"   📦 {p['total_qty']} وحدة  |  💰 {fmt_money(p['total_revenue'], currency)}"
            )
        await safe_edit(
            callback,
            "\n".join(lines),
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 التقارير", callback_data="menu_reports"),
                InlineKeyboardButton(text="🏠 القائمة",  callback_data="menu_main"),
            ]]),
        )
    except Exception as e:
        logger.error("خطأ في تقرير المنتجات: %s", e)
        await safe_edit(callback, "❌ خطأ.", kb_back_main())


# =============================================================================
# 9. الإعدادات
# =============================================================================

@router.callback_query(F.data == "menu_settings")
async def cb_settings(callback: CallbackQuery):
    """عرض إعدادات المستخدم الحالية."""
    user_id  = callback.from_user.id
    settings = db.get_user_settings(user_id)
    sub      = db.get_subscription(user_id)

    plan    = PLAN_NAMES_AR.get(sub["plan"] if sub else "", "غير محدد")
    end     = f"ينتهي: {sub['end_date'][:10]}" if (sub and sub["end_date"]) else "♾️ بلا انتهاء"
    notify  = "✅ مفعّل" if settings.get("notify_low_stock") else "❌ معطّل"

    partner_pct  = float(settings.get("partner_percentage") or 0.0)
    partner_line = f"\n🤝 نسبة الشريك: <b>{partner_pct}%</b>" if partner_pct > 0 else ""

    await safe_edit(
        callback,
        f"⚙️ <b>الإعدادات</b>\n"
        f"{'─'*26}\n"
        f"📌 الاشتراك:       <b>{plan}</b> — {end}\n"
        f"💱 العملة:         <b>{settings.get('currency', 'ل.س')}</b>\n"
        f"🔔 تنبيه المخزون:  <b>{notify}</b>"
        f"{partner_line}",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="💱 تغيير العملة",    callback_data="set_currency"),
                InlineKeyboardButton(text="🔔 تبديل التنبيهات", callback_data="set_toggle_notify"),
            ],
            [
                InlineKeyboardButton(text="🤝 نسبة الشريك",     callback_data="set_partner"),
                InlineKeyboardButton(text="💳 تجديد الاشتراك",  callback_data="show_payment_info"),
            ],
            [InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main")],
        ]),
    )


@router.callback_query(F.data == "set_currency")
async def cb_set_currency(callback: CallbackQuery):
    """عرض قائمة اختيار العملة."""
    await safe_edit(
        callback,
        "💱 اختر العملة:",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="ل.س (ليرة سورية)", callback_data="currency_ل.س"),
                InlineKeyboardButton(text="$ (دولار أمريكي)",  callback_data="currency_$"),
            ],
            [
                InlineKeyboardButton(text="€ (يورو)",          callback_data="currency_€"),
                InlineKeyboardButton(text="ر.س (ريال سعودي)",  callback_data="currency_ر.س"),
            ],
            [InlineKeyboardButton(text="🔙 رجوع", callback_data="menu_settings")],
        ]),
    )


@router.callback_query(F.data.startswith("currency_"))
async def cb_apply_currency(callback: CallbackQuery):
    """تطبيق العملة المختارة."""
    currency = callback.data.split("_", 1)[1]
    db.upsert_user_settings(callback.from_user.id, currency=currency)
    await safe_edit(callback, f"✅ تم تغيير العملة إلى <b>{currency}</b>", kb_back_main())


@router.callback_query(F.data == "set_toggle_notify")
async def cb_toggle_notify(callback: CallbackQuery):
    """تبديل حالة تنبيه المخزون المنخفض (تفعيل/تعطيل)."""
    user_id = callback.from_user.id
    current = db.get_user_settings(user_id).get("notify_low_stock", 1)
    new_val = 0 if current else 1
    db.upsert_user_settings(user_id, notify_low_stock=new_val)
    status = "مفعّل ✅" if new_val else "معطّل ❌"
    await safe_edit(callback, f"🔔 تنبيه المخزون المنخفض: <b>{status}</b>", kb_back_main())


@router.callback_query(F.data == "set_partner")
async def cb_set_partner(callback: CallbackQuery, state: FSMContext):
    """بدء تعديل نسبة الشريك."""
    user_id     = callback.from_user.id
    current_pct = float(db.get_user_settings(user_id).get("partner_percentage") or 0.0)
    await state.set_state(PartnerStates.waiting_percentage)
    await safe_edit(
        callback,
        f"🤝 <b>نسبة الشريك</b>\n\n"
        f"النسبة الحالية: <b>{current_pct}%</b>\n\n"
        f"أدخل النسبة المئوية (0 لإلغاء الشريك، مثال: 20):",
        kb_cancel(),
    )


@router.message(PartnerStates.waiting_percentage)
async def fsm_partner_percentage(message: Message, state: FSMContext):
    """
    استقبال نسبة الشريك.
    المقبول: 0 لإلغاء الشريك، أو بين 10% و90%.
    """
    try:
        pct = float(message.text.strip().replace(",", "."))
        if pct != 0 and (pct < 10 or pct > 90):
            raise ValueError()
    except ValueError:
        await message.answer(
            "⚠️ أدخل نسبة بين <b>10%</b> و <b>90%</b>\nأو أدخل <b>0</b> لإلغاء الشريك.",
            reply_markup=kb_cancel(),
            parse_mode="HTML",
        )
        return

    await state.clear()
    db.upsert_user_settings(message.from_user.id, partner_percentage=pct)

    if pct == 0:
        await message.answer("✅ تم إلغاء الشريك.", reply_markup=kb_back_main())
    else:
        await message.answer(
            f"✅ نسبة الشريك: <b>{pct}%</b>\n"
            f"👤 حصتك: <b>{100 - pct}%</b>\n"
            f"ستظهر الحصص في تقارير الأرباح.",
            reply_markup=kb_back_main(),
            parse_mode="HTML",
        )


# =============================================================================
# 10. FSM — الحساب السريع
# =============================================================================

@router.callback_query(F.data == "menu_quick_calc")
async def cb_quick_calc(callback: CallbackQuery, state: FSMContext):
    """
    بدء الحساب السريع.
    أداة لحساب الربح فوراً بدون حفظ البيانات في قاعدة البيانات.
    """
    await state.clear()
    await state.set_state(QuickCalcStates.waiting_sales)
    await safe_edit(
        callback,
        "⚡ <b>الحساب السريع</b>\n\n"
        "💡 احسب الربح الصافي فوراً بدون حفظ البيانات.\n\n"
        "📝 <b>الخطوة 1/2</b>\n"
        "أدخل إجمالي المبيعات وتكلفتها:\n"
        "<code>مبيعات تكلفة</code>\n\n"
        "مثال: <code>500 200</code>\n"
        "أو المبيعات فقط: <code>500</code>",
        kb_cancel(),
    )


@router.message(QuickCalcStates.waiting_sales)
async def fsm_quick_calc_sales(message: Message, state: FSMContext):
    """الخطوة 1: استقبال المبيعات والتكلفة."""
    parts = message.text.strip().split()
    try:
        sales = float(parts[0].replace(",", "."))
        cost  = float(parts[1].replace(",", ".")) if len(parts) > 1 else 0.0
        if sales < 0 or cost < 0:
            raise ValueError()
    except (ValueError, IndexError):
        await message.answer(
            "⚠️ الصيغة غير صحيحة.\nمثال: <code>500 200</code>",
            reply_markup=kb_cancel(),
            parse_mode="HTML",
        )
        return
    await state.update_data(sales=sales, cost=cost)
    await state.set_state(QuickCalcStates.waiting_expenses)
    await message.answer(
        f"✅ المبيعات: <b>{sales:,.2f}</b>  |  التكلفة: <b>{cost:,.2f}</b>\n\n"
        "📝 <b>الخطوة 2/2</b>\nأدخل إجمالي المصاريف (أو اضغط تخطي):",
        reply_markup=kb_skip_or_cancel("skip_qcalc_exp"),
        parse_mode="HTML",
    )


@router.callback_query(QuickCalcStates.waiting_expenses, F.data == "skip_qcalc_exp")
async def fsm_quick_calc_skip_exp(callback: CallbackQuery, state: FSMContext):
    """تخطي المصاريف وعرض نتيجة الحساب."""
    data = await state.get_data()
    await state.clear()
    await _show_quick_calc_result(callback.message, data["sales"], data["cost"], 0.0)
    await callback.answer()


@router.message(QuickCalcStates.waiting_expenses)
async def fsm_quick_calc_expenses(message: Message, state: FSMContext):
    """الخطوة 2: استقبال المصاريف وعرض نتيجة الحساب."""
    try:
        expenses = float(message.text.strip().replace(",", "."))
        if expenses < 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل قيمة صحيحة.", reply_markup=kb_cancel())
        return
    data = await state.get_data()
    await state.clear()
    await _show_quick_calc_result(message, data["sales"], data["cost"], expenses)


async def _show_quick_calc_result(
    message: Message,
    sales: float,
    cost: float,
    expenses: float,
) -> None:
    """[داخلي] حساب وعرض نتيجة الحساب السريع."""
    gross_profit = sales - cost
    net_profit   = gross_profit - expenses
    icon         = "📈" if net_profit >= 0 else "📉"
    await message.answer(
        f"⚡ <b>نتيجة الحساب السريع</b>\n"
        f"{'─'*26}\n"
        f"💰 المبيعات:    <b>{sales:,.2f}</b>\n"
        f"🏭 التكلفة:     <b>{cost:,.2f}</b>\n"
        f"📊 ربح إجمالي: <b>{gross_profit:,.2f}</b>\n"
        f"💸 المصاريف:   <b>{expenses:,.2f}</b>\n"
        f"{'─'*26}\n"
        f"{icon} <b>صافي الربح: {net_profit:,.2f}</b>\n"
        f"{'─'*26}\n"
        f"⚠️ <i>هذا الحساب مؤقت ولم يُحفظ في السجلات</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="⚡ حساب آخر", callback_data="menu_quick_calc"),
            InlineKeyboardButton(text="🏠 القائمة",  callback_data="menu_main"),
        ]]),
        parse_mode="HTML",
    )


# =============================================================================
# 11. FSM — المصاريف الثابتة
# =============================================================================

@router.callback_query(F.data == "menu_fixed_expenses")
async def cb_fixed_expenses_menu(callback: CallbackQuery, state: FSMContext):
    """عرض قائمة المصاريف الثابتة."""
    await state.clear()
    if not await check_subscription(callback.from_user.id, callback=callback):
        return
    await safe_edit(callback, "📌 <b>المصاريف الثابتة</b>", kb_fixed_expenses_menu())


# ── إضافة مصروف ثابت ──

@router.callback_query(F.data == "fe_add")
async def cb_fe_add_start(callback: CallbackQuery, state: FSMContext):
    """بدء إضافة مصروف ثابت جديد."""
    await state.set_state(FixedExpenseStates.waiting_name)
    await safe_edit(callback, "📌 <b>إضافة مصروف ثابت</b>\n\nأدخل اسم المصروف:", kb_cancel())


@router.message(FixedExpenseStates.waiting_name)
async def fsm_fe_name(message: Message, state: FSMContext):
    """استقبال اسم المصروف الثابت."""
    name = message.text.strip()
    if not name:
        await message.answer("⚠️ الاسم لا يمكن أن يكون فارغاً.", reply_markup=kb_cancel())
        return
    await state.update_data(name=name)
    await state.set_state(FixedExpenseStates.waiting_amount)
    await message.answer("💵 أدخل قيمة المصروف:", reply_markup=kb_cancel())


@router.message(FixedExpenseStates.waiting_amount)
async def fsm_fe_amount(message: Message, state: FSMContext):
    """استقبال قيمة المصروف الثابت."""
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError()
    except ValueError:
        await message.answer("⚠️ أدخل قيمة صحيحة.", reply_markup=kb_cancel())
        return
    await state.update_data(amount=amount)
    await state.set_state(FixedExpenseStates.waiting_type)
    await message.answer(
        "🗓 اختر نوع التكرار:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📆 يومي",   callback_data="fe_type_daily"),
                InlineKeyboardButton(text="📅 شهري",   callback_data="fe_type_monthly"),
                InlineKeyboardButton(text="🗓 سنوي",   callback_data="fe_type_yearly"),
            ],
            [InlineKeyboardButton(text="❌ إلغاء", callback_data="cancel_fsm")],
        ]),
    )


@router.callback_query(FixedExpenseStates.waiting_type, F.data.startswith("fe_type_"))
async def fsm_fe_type(callback: CallbackQuery, state: FSMContext):
    """استقبال نوع التكرار وحفظ المصروف الثابت."""
    expense_type = callback.data.split("_", 2)[2]  # daily / monthly / yearly
    data         = await state.get_data()
    await state.clear()
    user_id = callback.from_user.id
    try:
        fid = db.add_fixed_expense(user_id, data["name"], data["amount"], expense_type)
        await safe_edit(
            callback,
            f"✅ <b>تم إضافة المصروف الثابت #{fid}</b>\n"
            f"{'─'*26}\n"
            f"📝 الاسم:  <b>{data['name']}</b>\n"
            f"💵 القيمة: <b>{data['amount']:,.2f}</b>\n"
            f"🗓 النوع:  <b>{expense_type_ar(expense_type)}</b>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ إضافة آخر",         callback_data="fe_add"),
                InlineKeyboardButton(text="📌 المصاريف الثابتة",  callback_data="menu_fixed_expenses"),
            ]]),
        )
    except Exception as e:
        logger.error("خطأ في إضافة المصروف الثابت: %s", e)
        await safe_edit(callback, "❌ حدث خطأ.", kb_back_main())


# ── قائمة وإدارة المصاريف الثابتة ──

@router.callback_query(F.data == "fe_list")
async def cb_fe_list(callback: CallbackQuery):
    """عرض قائمة المصاريف الثابتة مع أزرار التعديل والحذف."""
    user_id  = callback.from_user.id
    expenses = db.get_fixed_expenses(user_id)

    if not expenses:
        await safe_edit(
            callback,
            "📌 لا توجد مصاريف ثابتة بعد.",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="➕ إضافة", callback_data="fe_add"),
                InlineKeyboardButton(text="🏠 رجوع",  callback_data="menu_fixed_expenses"),
            ]]),
        )
        return

    lines = ["📋 <b>المصاريف الثابتة:</b>\n"]
    rows  = []
    for e in expenses:
        lines.append(f"• <b>{e['name']}</b> — {e['amount']:,.2f} ({expense_type_ar(e['expense_type'])})")
        rows.append([
            InlineKeyboardButton(text=f"✏️ {e['name'][:15]}", callback_data=f"fe_edit_{e['id']}"),
            InlineKeyboardButton(text="🗑 حذف",                callback_data=f"fe_del_{e['id']}"),
        ])
    rows.append([InlineKeyboardButton(text="🔙 رجوع", callback_data="menu_fixed_expenses")])
    await safe_edit(callback, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("fe_del_"))
async def cb_fe_delete(callback: CallbackQuery):
    """حذف مصروف ثابت وتحديث القائمة."""
    user_id    = callback.from_user.id
    expense_id = int(callback.data.split("_")[2])
    ok = db.delete_fixed_expense(user_id, expense_id)
    if ok:
        await callback.answer("✅ تم الحذف.")
        # إعادة عرض القائمة المحدّثة
        expenses = db.get_fixed_expenses(user_id)
        if not expenses:
            await safe_edit(
                callback,
                "📌 لا توجد مصاريف ثابتة بعد.",
                InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="➕ إضافة", callback_data="fe_add"),
                    InlineKeyboardButton(text="🏠 رجوع",  callback_data="menu_fixed_expenses"),
                ]]),
            )
        else:
            lines = ["📋 <b>المصاريف الثابتة:</b>\n"]
            rows  = []
            for e in expenses:
                lines.append(f"• <b>{e['name']}</b> — {e['amount']:,.2f} ({expense_type_ar(e['expense_type'])})")
                rows.append([
                    InlineKeyboardButton(text=f"✏️ {e['name'][:15]}", callback_data=f"fe_edit_{e['id']}"),
                    InlineKeyboardButton(text="🗑 حذف",                callback_data=f"fe_del_{e['id']}"),
                ])
            rows.append([InlineKeyboardButton(text="🔙 رجوع", callback_data="menu_fixed_expenses")])
            await safe_edit(callback, "\n".join(lines), InlineKeyboardMarkup(inline_keyboard=rows))
    else:
        await callback.answer("❌ خطأ في الحذف.", show_alert=True)


@router.callback_query(F.data.startswith("fe_edit_"))
async def cb_fe_edit(callback: CallbackQuery, state: FSMContext):
    """بدء تعديل مصروف ثابت — الخطوة 1: تعديل الاسم."""
    expense_id = int(callback.data.split("_")[2])
    expenses   = db.get_fixed_expenses(callback.from_user.id)
    e = next((x for x in expenses if x["id"] == expense_id), None)
    if not e:
        await callback.answer("❌ المصروف غير موجود.", show_alert=True)
        return
    await state.update_data(
        edit_fe_id=expense_id,
        edit_fe_name=e["name"],
        edit_fe_amount=e["amount"],
        edit_fe_type=e["expense_type"],
    )
    await state.set_state(FixedExpenseStates.waiting_edit_name)
    await safe_edit(
        callback,
        f"✏️ <b>تعديل: {e['name']}</b>\n\n"
        f"أدخل الاسم الجديد (أو اضغط تخطي للإبقاء على الاسم الحالي):",
        kb_skip_or_cancel("skip_fe_name"),
    )


@router.callback_query(FixedExpenseStates.waiting_edit_name, F.data == "skip_fe_name")
async def fsm_fe_skip_name(callback: CallbackQuery, state: FSMContext):
    """تخطي تعديل الاسم."""
    await state.set_state(FixedExpenseStates.waiting_edit_amount)
    await safe_edit(callback, "💵 أدخل القيمة الجديدة:", kb_skip_or_cancel("skip_fe_amount"))


@router.message(FixedExpenseStates.waiting_edit_name)
async def fsm_fe_edit_name(message: Message, state: FSMContext):
    """استقبال الاسم الجديد للمصروف الثابت."""
    name = message.text.strip()
    if name:
        await state.update_data(edit_fe_name=name)
    await state.set_state(FixedExpenseStates.waiting_edit_amount)
    await message.answer("💵 أدخل القيمة الجديدة:", reply_markup=kb_skip_or_cancel("skip_fe_amount"))


@router.callback_query(FixedExpenseStates.waiting_edit_amount, F.data == "skip_fe_amount")
async def fsm_fe_skip_amount(callback: CallbackQuery, state: FSMContext):
    """تخطي تعديل القيمة."""
    await state.set_state(FixedExpenseStates.waiting_edit_type)
    await safe_edit(
        callback,
        "🗓 اختر النوع الجديد:",
        InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📆 يومي",  callback_data="fe_etype_daily"),
                InlineKeyboardButton(text="📅 شهري",  callback_data="fe_etype_monthly"),
                InlineKeyboardButton(text="🗓 سنوي",  callback_data="fe_etype_yearly"),
            ],
            [InlineKeyboardButton(text="⏭ تخطي", callback_data="fe_etype_skip")],
        ]),
    )


@router.message(FixedExpenseStates.waiting_edit_amount)
async def fsm_fe_edit_amount(message: Message, state: FSMContext):
    """استقبال القيمة الجديدة للمصروف الثابت."""
    try:
        amount = float(message.text.strip().replace(",", "."))
        if amount <= 0:
            raise ValueError()
        await state.update_data(edit_fe_amount=amount)
    except ValueError:
        await message.answer("⚠️ أدخل قيمة صحيحة.", reply_markup=kb_cancel())
        return
    await state.set_state(FixedExpenseStates.waiting_edit_type)
    await message.answer(
        "🗓 اختر النوع الجديد:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📆 يومي",  callback_data="fe_etype_daily"),
                InlineKeyboardButton(text="📅 شهري",  callback_data="fe_etype_monthly"),
                InlineKeyboardButton(text="🗓 سنوي",  callback_data="fe_etype_yearly"),
            ],
            [InlineKeyboardButton(text="⏭ تخطي", callback_data="fe_etype_skip")],
        ]),
    )


@router.callback_query(FixedExpenseStates.waiting_edit_type, F.data.startswith("fe_etype_"))
async def fsm_fe_edit_type(callback: CallbackQuery, state: FSMContext):
    """استقبال النوع الجديد وحفظ التعديلات."""
    etype = callback.data.split("_")[2]  # daily / monthly / yearly / skip
    data  = await state.get_data()

    if etype != "skip":
        await state.update_data(edit_fe_type=etype)
        data["edit_fe_type"] = etype

    await state.clear()

    ok = db.update_fixed_expense(
        callback.from_user.id,
        data["edit_fe_id"],
        data["edit_fe_name"],
        data["edit_fe_amount"],
        data["edit_fe_type"],
    )
    if ok:
        await safe_edit(
            callback,
            f"✅ <b>تم التعديل بنجاح</b>\n"
            f"📝 {data['edit_fe_name']} — {data['edit_fe_amount']:,.2f} ({expense_type_ar(data['edit_fe_type'])})",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="📋 قائمة المصاريف",    callback_data="fe_list"),
                InlineKeyboardButton(text="🏠 رجوع",              callback_data="menu_fixed_expenses"),
            ]]),
        )
    else:
        await safe_edit(callback, "❌ خطأ في التعديل.", kb_back_main())


@router.callback_query(F.data == "fe_summary")
async def cb_fe_summary(callback: CallbackQuery):
    """عرض ملخص المصاريف الثابتة مع المعادل اليومي والشهري والسنوي."""
    user_id  = callback.from_user.id
    currency = get_currency(user_id)
    expenses = db.get_fixed_expenses(user_id)

    if not expenses:
        await safe_edit(callback, "📌 لا توجد مصاريف ثابتة.", kb_fixed_expenses_menu())
        return

    daily   = db.get_fixed_expenses_total(user_id, "daily")
    monthly = db.get_fixed_expenses_total(user_id, "monthly")
    yearly  = db.get_fixed_expenses_total(user_id, "yearly")

    lines = ["📊 <b>ملخص المصاريف الثابتة</b>\n" + "─"*26]
    for e in expenses:
        lines.append(f"• <b>{e['name']}</b>: {e['amount']:,.2f} ({expense_type_ar(e['expense_type'])})")

    lines += [
        f"\n{'─'*26}",
        f"📆 المعادل اليومي:  <b>{fmt_money(daily,   currency)}</b>",
        f"📅 المعادل الشهري:  <b>{fmt_money(monthly, currency)}</b>",
        f"🗓 المعادل السنوي:  <b>{fmt_money(yearly,  currency)}</b>",
    ]
    await safe_edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 رجوع", callback_data="menu_fixed_expenses"),
        ]]),
    )


# =============================================================================
# 12. معلومات الدفع (شام كاش)
# =============================================================================

def _get_payment_info() -> dict:
    """
    جلب معلومات الدفع.
    يبحث أولاً في global_settings ثم يعود للقيم الافتراضية من config.py.
    """
    try:
        value = db.get_global_setting("payment_info")
        info  = json.loads(value) if value else {}
        return info if info else DEFAULT_PAYMENT_INFO
    except Exception:
        return DEFAULT_PAYMENT_INFO


def _save_payment_info(info: dict) -> None:
    """حفظ معلومات الدفع في global_settings."""
    db.set_global_setting("payment_info", json.dumps(info, ensure_ascii=False))


@router.callback_query(F.data == "show_payment_info")
async def cb_show_payment_info(callback: CallbackQuery, bot: Bot):
    """
    عرض معلومات الدفع (شام كاش) مع QR كود.
    يُرسل صورة QR إذا وُجدت، وإلا يُرسل نصاً فقط.
    """
    info        = _get_payment_info()
    plan_prices = info.get("plan_prices", {})

    # بناء قسم أسعار الخطط
    prices_text = ""
    if plan_prices:
        prices_text = "\n\n💳 <b>أسعار الخطط:</b>\n"
        plan_labels = {"monthly": "شهري 📅", "yearly": "سنوي 🗓", "lifetime": "مدى الحياة ♾️"}
        for plan, price in plan_prices.items():
            label = plan_labels.get(plan, plan)
            prices_text += f"  • {label}: <b>{price}</b>\n"

    caption = (
        f"💳 <b>معلومات الدفع — شام كاش</b>\n"
        f"{'─'*26}\n"
        f"📱 رقم الحساب:\n"
        f"<code>{info.get('account_number', 'غير محدد')}</code>\n"
        f"👤 الاسم: <b>{info.get('account_name', 'غير محدد')}</b>\n"
        f"{prices_text}"
        f"{'─'*26}\n"
        f"📌 يمكنك الدفع بمسح QR أو بإدخال الكود يدوياً.\n"
        f"بعد الدفع أرسل لقطة الشاشة للمسؤول للتفعيل."
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="🏠 رجوع", callback_data="menu_main"),
    ]])

    # حذف الرسالة القديمة قبل إرسال الصورة
    try:
        await callback.message.delete()
    except Exception:
        pass

    # إرسال صورة QR إذا كانت موجودة، وإلا نص فقط
    if QR_CODE_PATH and os.path.exists(QR_CODE_PATH):
        from aiogram.types import FSInputFile
        await bot.send_photo(
            chat_id=callback.from_user.id,
            photo=FSInputFile(QR_CODE_PATH),
            caption=caption,
            parse_mode="HTML",
            reply_markup=kb,
        )
    else:
        await bot.send_message(
            chat_id=callback.from_user.id,
            text=caption,
            parse_mode="HTML",
            reply_markup=kb,
        )
    await callback.answer()


# =============================================================================
# 13. لوحة تحكم الأدمن
# =============================================================================

# ── القائمة الرئيسية للأدمن ──

@router.callback_query(F.data == "admin_panel")
async def cb_admin_panel(callback: CallbackQuery, state: FSMContext):
    """عرض لوحة الأدمن مع إحصائيات سريعة."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    await state.clear()
    users  = db.get_all_users_stats()
    active = sum(1 for u in users if u.get("status") == "active")
    await safe_edit(
        callback,
        f"👑 <b>لوحة تحكم الأدمن</b>\n"
        f"{'─'*26}\n"
        f"👥 إجمالي المستخدمين: <b>{len(users)}</b>\n"
        f"✅ اشتراكات نشطة:     <b>{active}</b>",
        kb_admin_panel(),
    )


# ── تفعيل اشتراك ──

@router.callback_query(F.data == "adm_activate")
async def cb_admin_activate_start(callback: CallbackQuery, state: FSMContext):
    """الخطوة 1: طلب user_id للمستخدم المراد تفعيل اشتراكه."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    await state.set_state(AdminStates.waiting_user_id)
    await safe_edit(callback, "✅ <b>تفعيل اشتراك</b>\n\nأدخل Telegram user_id للمستخدم:", kb_cancel())


@router.message(AdminStates.waiting_user_id)
async def fsm_admin_user_id(message: Message, state: FSMContext):
    """الخطوة 2: البحث عن المستخدم وعرض خيارات الخطط."""
    if not is_admin(message.from_user.id):
        await message.answer(MSG_NO_PERMISSION)
        await state.clear()
        return
    try:
        target_id = int(message.text.strip())
    except ValueError:
        await message.answer("⚠️ أدخل user_id صحيحاً (أرقام فقط).", reply_markup=kb_cancel())
        return

    user = db.get_user(target_id)
    if not user:
        await message.answer(f"⚠️ المستخدم {target_id} غير مسجّل في البوت.", reply_markup=kb_cancel())
        return

    name = user["full_name"] or user["username"] or str(target_id)
    sub  = db.get_subscription(target_id)

    if sub:
        cur_plan   = PLAN_NAMES_AR.get(sub["plan"], sub["plan"])
        cur_status = "✅ نشط" if sub["status"] == "active" else "❌ منتهي/ملغى"
        cur_end    = sub["end_date"][:10] if sub["end_date"] else "♾️"
        sub_info   = f"\n📌 الاشتراك الحالي: <b>{cur_plan}</b> — {cur_status} — {cur_end}"
    else:
        sub_info = "\n📌 لا يوجد اشتراك مسبق"

    await state.update_data(target_user_id=target_id, target_name=name)
    await state.set_state(AdminStates.waiting_plan_days)
    await message.answer(
        f"👤 المستخدم: <b>{name}</b> ({target_id}){sub_info}\n\nاختر الإجراء:",
        reply_markup=kb_activate_plan(),
        parse_mode="HTML",
    )


@router.callback_query(AdminStates.waiting_plan_days, F.data.startswith("plan_"))
async def fsm_admin_activate_plan(callback: CallbackQuery, state: FSMContext):
    """تنفيذ تفعيل / إلغاء الاشتراك."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return

    data = await state.get_data()

    # إلغاء الاشتراك
    if callback.data == "plan_deactivate":
        await state.clear()
        ok = db.deactivate_subscription(data["target_user_id"])
        if ok:
            await safe_edit(
                callback,
                f"🚫 <b>تم إلغاء اشتراك</b>\n"
                f"{'─'*26}\n"
                f"👤 المستخدم: <b>{data.get('target_name')}</b> ({data['target_user_id']})\n"
                f"⏱ تاريخ الإلغاء: <b>{datetime.utcnow().strftime('%Y-%m-%d')}</b>",
                kb_admin_panel(),
            )
        else:
            await safe_edit(
                callback,
                f"⚠️ المستخدم <b>{data.get('target_name')}</b> لا يملك اشتراكاً نشطاً.",
                kb_admin_panel(),
            )
        return

    # تفعيل خطة جديدة: callback_data = "plan_{plan}_{days}"
    parts = callback.data.split("_")  # ["plan", "monthly", "30"]
    plan  = parts[1]
    days  = int(parts[2])
    await state.clear()

    try:
        db.activate_subscription(data["target_user_id"], plan, days)
        await safe_edit(
            callback,
            f"✅ <b>تم تفعيل الاشتراك</b>\n"
            f"{'─'*26}\n"
            f"👤 المستخدم: <b>{data.get('target_name')}</b>\n"
            f"📌 الخطة:    <b>{PLAN_NAMES_AR.get(plan, plan)}</b>\n"
            f"⏳ المدة:    <b>{'مدى الحياة ♾️' if days == 0 else f'{days} يوم'}</b>",
            kb_admin_panel(),
        )
    except Exception as e:
        logger.error("خطأ في تفعيل الاشتراك: %s", e)
        await safe_edit(callback, f"❌ خطأ: {e}", kb_admin_panel())


# ── قائمة المستخدمين ──

@router.callback_query(F.data == "adm_users")
async def cb_admin_users(callback: CallbackQuery):
    """عرض قائمة جميع المستخدمين مع حالة اشتراكاتهم."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return

    users = db.get_all_users_stats()
    if not users:
        await safe_edit(callback, "👥 لا يوجد مستخدمون بعد.", kb_admin_panel())
        return

    plan_short = {"trial": "تج", "monthly": "شهري", "yearly": "سنوي", "lifetime": "♾️"}
    status_icon = {"active": "✅", "expired": "❌", "suspended": "🔴"}

    lines = [f"👥 <b>المستخدمون ({len(users)}):</b>\n"]
    for u in users[:20]:
        plan = plan_short.get(u.get("plan"), "؟")
        icon = status_icon.get(u.get("status"), "❓")
        name = u.get("full_name") or u.get("username") or str(u["user_id"])
        lines.append(f"{icon} <b>{name}</b> | {plan} | 🛒{u['sales_count']} 💸{u['expenses_count']}")

    if len(users) > 20:
        lines.append(f"\n<i>...و{len(users)-20} آخرون</i>")

    await safe_edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔙 الأدمن", callback_data="admin_panel"),
        ]]),
    )


# ── التقرير العام ──

@router.callback_query(F.data == "adm_global_report")
async def cb_admin_global_report(callback: CallbackQuery):
    """عرض تقرير إجمالي لجميع المستخدمين."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    try:
        users          = db.get_all_users_stats()
        total_sales    = 0.0
        total_expenses = 0.0
        for u in users:
            s = db.get_profit_summary(u["user_id"])
            total_sales    += s["total_sales"]
            total_expenses += s["total_expenses"]
        net = total_sales - total_expenses
        await safe_edit(
            callback,
            f"📊 <b>التقرير العام</b>\n"
            f"{'─'*26}\n"
            f"👥 المستخدمون:      <b>{len(users)}</b>\n"
            f"💰 إجمالي المبيعات: <b>{total_sales:,.2f}</b>\n"
            f"💸 إجمالي المصاريف: <b>{total_expenses:,.2f}</b>\n"
            f"📈 صافي الأرباح:   <b>{net:,.2f}</b>",
            InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 الأدمن", callback_data="admin_panel"),
            ]]),
        )
    except Exception as e:
        logger.error("خطأ في التقرير العام: %s", e)
        await safe_edit(callback, "❌ خطأ في التقرير.", kb_admin_panel())


# ── الاشتراكات المنتهية ──

@router.callback_query(F.data == "adm_expired")
async def cb_admin_expired(callback: CallbackQuery):
    """عرض قائمة المستخدمين الذين انتهت اشتراكاتهم."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return

    expired = db.get_expired_subscriptions()
    if not expired:
        await safe_edit(callback, "✅ لا توجد اشتراكات منتهية.", kb_admin_panel())
        return

    lines = [f"⏰ <b>اشتراكات منتهية ({len(expired)}):</b>\n"]
    for u in expired:
        name = u.get("full_name") or u.get("username") or str(u["user_id"])
        lines.append(f"❌ <b>{name}</b> ({u['user_id']})\n   انتهى: {u['end_date'][:10]}")

    await safe_edit(
        callback,
        "\n".join(lines),
        InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ تفعيل اشتراك", callback_data="adm_activate"),
            InlineKeyboardButton(text="🔙 الأدمن",       callback_data="admin_panel"),
        ]]),
    )


# ── رسالة جماعية ──

@router.callback_query(F.data == "adm_broadcast")
async def cb_admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    """بدء إرسال رسالة جماعية لجميع المستخدمين."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    await state.set_state(AdminStates.waiting_broadcast)
    await safe_edit(callback, "📢 <b>رسالة جماعية</b>\n\nأدخل نص الرسالة:", kb_cancel())


@router.message(AdminStates.waiting_broadcast)
async def fsm_admin_broadcast(message: Message, state: FSMContext, bot: Bot):
    """إرسال الرسالة الجماعية لجميع المستخدمين المسجّلين."""
    if not is_admin(message.from_user.id):
        await message.answer(MSG_NO_PERMISSION)
        await state.clear()
        return

    await state.clear()
    text  = message.text.strip()
    users = db.get_all_users_stats()
    sent  = failed = 0

    for u in users:
        try:
            await bot.send_message(
                u["user_id"],
                f"📢 <b>إشعار من الإدارة:</b>\n\n{text}",
                parse_mode="HTML",
            )
            sent += 1
        except Exception:
            failed += 1

    await message.answer(
        f"📢 <b>اكتمل الإرسال</b>\n✅ نجح: {sent}  |  ❌ فشل: {failed}",
        reply_markup=kb_admin_panel(),
        parse_mode="HTML",
    )


# ── تخصيص أسماء الأزرار ──

# خريطة مفاتيح الأزرار وأوصافها (للعرض في لوحة الأدمن)
BUTTON_KEYS: dict = {
    "btn_quick_calc": "حساب سريع",
    "btn_sale":        "تسجيل مبيعة",
    "btn_expense":     "تسجيل مصروف",
    "btn_inventory":   "إدارة المخزون",
    "btn_fixed_exp":   "مصاريف ثابتة",
    "btn_reports":     "التقارير",
    "btn_settings":    "الإعدادات",
    "btn_today":       "تقرير اليوم",
    "btn_month":       "تقرير الشهر",
    "btn_year":        "تقرير السنة",
    "btn_all":         "التقرير الإجمالي",
    "btn_top":         "أكثر مبيعاً",
}


def kb_button_list(user_id: int) -> InlineKeyboardMarkup:
    """لوحة أزرار إدارة أسماء الأزرار (لوحة الأدمن)."""
    bn   = db.get_button_names(user_id)
    rows = []
    for key, label in BUTTON_KEYS.items():
        current_name = bn.get(key, label)
        rows.append([InlineKeyboardButton(
            text=f"✏️ {current_name}",
            callback_data=f"renamekey_{key}",
        )])
    rows.append([InlineKeyboardButton(text="🔙 الأدمن", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@router.callback_query(F.data == "adm_buttons")
async def cb_admin_buttons(callback: CallbackQuery, state: FSMContext):
    """عرض قائمة تخصيص أسماء الأزرار."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    await state.clear()
    await safe_edit(
        callback,
        "🔘 <b>تخصيص أسماء الأزرار</b>\n\nاختر الزر الذي تريد تغيير اسمه:",
        kb_button_list(callback.from_user.id),
    )


@router.callback_query(F.data.startswith("renamekey_"))
async def cb_rename_key_selected(callback: CallbackQuery, state: FSMContext):
    """اختيار زر محدد لإعادة تسميته."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    btn_key = callback.data.split("_", 1)[1]
    if btn_key not in BUTTON_KEYS:
        await callback.answer("مفتاح غير صالح.", show_alert=True)
        return
    bn           = db.get_button_names(callback.from_user.id)
    current_name = bn.get(btn_key, BUTTON_KEYS[btn_key])
    await state.update_data(rename_key=btn_key)
    await state.set_state(ButtonRenameStates.waiting_new_name)
    await safe_edit(
        callback,
        f"✏️ <b>تغيير اسم الزر</b>\n\n"
        f"الاسم الحالي: <b>{current_name}</b>\n\n"
        f"أدخل الاسم الجديد (حد أقصى 30 حرفاً):",
        kb_cancel(),
    )


@router.message(ButtonRenameStates.waiting_new_name)
async def fsm_button_new_name(message: Message, state: FSMContext):
    """حفظ الاسم الجديد للزر."""
    if not is_admin(message.from_user.id):
        await message.answer(MSG_NO_PERMISSION)
        await state.clear()
        return
    new_name = message.text.strip()
    if not new_name or len(new_name) > 30:
        await message.answer("⚠️ الاسم فارغ أو أطول من 30 حرفاً.", reply_markup=kb_cancel())
        return
    data    = await state.get_data()
    await state.clear()
    btn_key = data.get("rename_key")
    db.set_button_name(message.from_user.id, btn_key, new_name)
    await message.answer(
        f"✅ تم تغيير الاسم إلى: <b>{new_name}</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔘 تعديل أزرار أخرى", callback_data="adm_buttons"),
            InlineKeyboardButton(text="🔙 الأدمن",            callback_data="admin_panel"),
        ]]),
        parse_mode="HTML",
    )


# ── معلومات الدفع (لوحة الأدمن) ──

@router.callback_query(F.data == "adm_payment_info")
async def cb_admin_payment_info(callback: CallbackQuery):
    """عرض معلومات الدفع الحالية في لوحة الأدمن."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    info        = _get_payment_info()
    plan_prices = info.get("plan_prices", {})
    monthly     = plan_prices.get("monthly",  "غير محدد")
    yearly      = plan_prices.get("yearly",   "غير محدد")
    lifetime    = plan_prices.get("lifetime", "غير محدد")

    await safe_edit(
        callback,
        f"💳 <b>إعداد معلومات الدفع (شام كاش)</b>\n"
        f"{'─'*26}\n"
        f"📱 رقم الحساب: <code>{info.get('account_number', 'غير محدد')}</code>\n"
        f"👤 الاسم:       <b>{info.get('account_name', 'غير محدد')}</b>\n"
        f"📅 الشهري:      <b>{monthly}</b>\n"
        f"🗓 السنوي:      <b>{yearly}</b>\n"
        f"♾️ مدى الحياة:  <b>{lifetime}</b>\n\n"
        f"استخدم الأمر: <code>/setpayment رقم اسم شهري سنوي مدىحياة</code>",
        InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💰 تعديل الأسعار", callback_data="adm_edit_prices")],
            [InlineKeyboardButton(text="🔙 الأدمن",         callback_data="admin_panel")],
        ]),
    )


# ── تعديل أسعار الخطط (FSM) ──

@router.callback_query(F.data == "adm_edit_prices")
async def cb_adm_edit_prices_start(callback: CallbackQuery, state: FSMContext):
    """بدء تعديل أسعار الخطط — الخطوة 1: السعر الشهري."""
    if not is_admin(callback.from_user.id):
        await callback.answer(MSG_NO_PERMISSION, show_alert=True)
        return
    info    = _get_payment_info()
    current = info.get("plan_prices", {}).get("monthly", "غير محدد")
    await state.set_state(PriceStates.waiting_monthly)
    await safe_edit(
        callback,
        f"💰 <b>تعديل أسعار الخطط</b>\n{'─'*26}\n"
        f"الخطوة 1/3\n\n"
        f"📅 السعر الشهري الحالي: <b>{current}</b>\n\n"
        f"أرسل السعر الشهري الجديد:",
        kb_cancel(),
    )


@router.message(PriceStates.waiting_monthly)
async def fsm_price_monthly(message: Message, state: FSMContext):
    """استقبال السعر الشهري."""
    if not is_admin(message.from_user.id):
        return
    await state.update_data(monthly=message.text.strip())
    info    = _get_payment_info()
    current = info.get("plan_prices", {}).get("yearly", "غير محدد")
    await state.set_state(PriceStates.waiting_yearly)
    await message.answer(
        f"✅ الشهري: <b>{message.text.strip()}</b>\n\n"
        f"الخطوة 2/3\n\n"
        f"🗓 السعر السنوي الحالي: <b>{current}</b>\n\n"
        f"أرسل السعر السنوي الجديد:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(PriceStates.waiting_yearly)
async def fsm_price_yearly(message: Message, state: FSMContext):
    """استقبال السعر السنوي."""
    if not is_admin(message.from_user.id):
        return
    await state.update_data(yearly=message.text.strip())
    info    = _get_payment_info()
    current = info.get("plan_prices", {}).get("lifetime", "غير محدد")
    await state.set_state(PriceStates.waiting_lifetime)
    await message.answer(
        f"✅ السنوي: <b>{message.text.strip()}</b>\n\n"
        f"الخطوة 3/3\n\n"
        f"♾️ سعر مدى الحياة الحالي: <b>{current}</b>\n\n"
        f"أرسل سعر مدى الحياة الجديد:",
        parse_mode="HTML",
        reply_markup=kb_cancel(),
    )


@router.message(PriceStates.waiting_lifetime)
async def fsm_price_lifetime(message: Message, state: FSMContext):
    """استقبال سعر مدى الحياة وحفظ جميع الأسعار."""
    if not is_admin(message.from_user.id):
        return
    data     = await state.get_data()
    monthly  = data["monthly"]
    yearly   = data["yearly"]
    lifetime = message.text.strip()
    await state.clear()

    info = _get_payment_info()
    info["plan_prices"] = {
        "monthly":  monthly,
        "yearly":   yearly,
        "lifetime": lifetime,
    }
    try:
        _save_payment_info(info)
        await message.answer(
            f"✅ <b>تم تحديث الأسعار!</b>\n"
            f"{'─'*26}\n"
            f"📅 الشهري:      <b>{monthly}</b>\n"
            f"🗓 السنوي:      <b>{yearly}</b>\n"
            f"♾️ مدى الحياة:  <b>{lifetime}</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="🔙 لوحة الأدمن", callback_data="admin_panel"),
            ]]),
        )
    except Exception as e:
        await message.answer(f"❌ خطأ في الحفظ: {e}")


# ── أمر /setpayment (اختصار للأدمن المتقدم) ──

@router.message(F.text.startswith("/setpayment"))
async def cmd_set_payment(message: Message):
    """
    أمر مباشر لتحديث معلومات الدفع دفعة واحدة.
    الصيغة: /setpayment رقم_الحساب اسم_الحساب سعر_شهري سعر_سنوي سعر_مدى_حياة
    """
    if not is_admin(message.from_user.id):
        await message.answer(MSG_NO_PERMISSION)
        return
    parts = message.text.strip().split(maxsplit=5)
    if len(parts) < 6:
        await message.answer(
            "⚠️ الصيغة:\n<code>/setpayment رقم_الحساب اسم_الحساب سعر_شهري سعر_سنوي سعر_مدى_حياة</code>",
            parse_mode="HTML",
        )
        return
    _, account_number, account_name, monthly, yearly, lifetime = parts
    info = {
        "account_number": account_number,
        "account_name":   account_name,
        "plan_prices":    {"monthly": monthly, "yearly": yearly, "lifetime": lifetime},
    }
    try:
        _save_payment_info(info)
        await message.answer(
            f"✅ <b>تم تحديث معلومات الدفع</b>\n"
            f"📱 {account_number} | 👤 {account_name}\n"
            f"📅 {monthly} | 🗓 {yearly} | ♾️ {lifetime}",
            parse_mode="HTML",
        )
    except Exception as e:
        await message.answer(f"❌ خطأ: {e}")


# =============================================================================
# معالج الرسائل غير المعروفة (Fallback)
# =============================================================================

@router.message(StateFilter(None))
async def fallback_message(message: Message):
    """
    يُعالج أي رسالة لا تطابق أي handler آخر.
    يُوجّه المستخدم لاستخدام /start.
    """
    await message.answer(
        "❓ لم أفهم رسالتك.\nاستخدم /start للقائمة الرئيسية.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🏠 القائمة الرئيسية", callback_data="menu_main"),
        ]]),
    )
