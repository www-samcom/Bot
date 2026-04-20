# =============================================================================
# database.py — طبقة قاعدة البيانات (Data Access Layer)
# =============================================================================
#
# هذا الملف يحتوي على جميع عمليات قاعدة البيانات.
# مبني بأسلوب DAL (Data Access Layer) بحيث يمكن استبداله بـ:
#   • SQLAlchemy ORM عند التحويل لـ web app
#   • Firebase Firestore أو PostgreSQL مستقبلاً
#   • كل دالة تعيد dict أو list[dict] — لا تعيد sqlite3.Row مباشرة
#
# الهيكل:
#   1.  إعداد الاتصال بقاعدة البيانات
#   2.  مخطط الجداول (Schema SQL)
#   3.  تهيئة قاعدة البيانات
#   4.  إدارة المستخدمين
#   5.  إدارة الاشتراكات
#   6.  إدارة المنتجات والمخزون
#   7.  تسجيل المبيعات
#   8.  تسجيل المصاريف
#   9.  التقارير والأرباح
#   10. إدارة الفئات (مصاريف / منتجات)
#   11. إعدادات المستخدم
#   12. المصاريف الثابتة
#   13. الإعدادات العامة (global_settings)
#   14. دوال الأدمن
#
# =============================================================================

import sqlite3
import logging
from pathlib import Path
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import Optional, Generator

from config import DB_PATH, DEFAULT_TRIAL_DAYS

logger = logging.getLogger(__name__)


# =============================================================================
# 1. إعداد الاتصال بقاعدة البيانات
# =============================================================================

def get_connection() -> sqlite3.Connection:
    """
    إنشاء اتصال مباشر بقاعدة البيانات.
    - يُنشئ مجلد data/ تلقائياً إذا لم يكن موجوداً.
    - row_factory = sqlite3.Row يسمح بالوصول للحقول بالاسم.
    - WAL journal يحسّن الأداء عند القراءة/الكتابة المتزامنة.
    """
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(
        str(DB_PATH),
        check_same_thread=False,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")   # تفعيل Foreign Keys
    conn.execute("PRAGMA journal_mode = WAL")   # أداء أفضل
    return conn


@contextmanager
def db_context() -> Generator[sqlite3.Connection, None, None]:
    """
    Context Manager لإدارة الاتصال تلقائياً.

    - يعمل commit تلقائياً عند النجاح.
    - يعمل rollback تلقائياً عند الخطأ.
    - يغلق الاتصال دائماً في النهاية.

    الاستخدام:
        with db_context() as conn:
            conn.execute("INSERT INTO ...")
    """
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception as exc:
        conn.rollback()
        logger.error("خطأ في قاعدة البيانات: %s", exc, exc_info=True)
        raise
    finally:
        conn.close()


# =============================================================================
# 2. مخطط الجداول (Schema SQL)
# =============================================================================

SCHEMA_SQL = """

-- ── جدول المستخدمين ──────────────────────────────────────────────────────────
-- يخزن بيانات كل مستخدم سجّل في البوت.
CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER PRIMARY KEY,      -- Telegram user_id
    username        TEXT,                     -- @username (اختياري)
    full_name       TEXT,                     -- الاسم الكامل
    language_code   TEXT    DEFAULT 'ar',     -- رمز اللغة
    is_active       INTEGER DEFAULT 1,        -- 1=نشط, 0=موقوف
    created_at      TEXT    DEFAULT (datetime('now')),
    updated_at      TEXT    DEFAULT (datetime('now'))
);

-- ── جدول الاشتراكات ──────────────────────────────────────────────────────────
-- لكل مستخدم اشتراك واحد فقط (UNIQUE على user_id).
-- plan: trial / monthly / yearly / lifetime
-- status: active / expired / suspended
CREATE TABLE IF NOT EXISTS subscriptions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL UNIQUE,
    plan            TEXT    DEFAULT 'trial',
    status          TEXT    DEFAULT 'active',
    start_date      TEXT    NOT NULL,
    end_date        TEXT,                     -- NULL = مدى الحياة
    notes           TEXT,
    created_at      TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- ── جدول فئات المنتجات ───────────────────────────────────────────────────────
-- كل مستخدم له فئاته الخاصة (UNIQUE على user_id + name).
CREATE TABLE IF NOT EXISTS product_categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    name            TEXT    NOT NULL,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(user_id, name),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- ── جدول المنتجات / المخزون ──────────────────────────────────────────────────
-- اسم المنتج فريد لكل مستخدم (UNIQUE على user_id + name).
CREATE TABLE IF NOT EXISTS products (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    category_id     INTEGER,
    name            TEXT    NOT NULL,
    description     TEXT,
    unit            TEXT    DEFAULT 'قطعة',   -- وحدة القياس
    cost_price      REAL    DEFAULT 0.0,      -- سعر التكلفة
    selling_price   REAL    DEFAULT 0.0,      -- سعر البيع
    stock_quantity  REAL    DEFAULT 0.0,      -- الكمية الحالية
    min_stock_alert REAL    DEFAULT 0.0,      -- حد التنبيه (0=معطّل)
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT    DEFAULT (datetime('now')),
    updated_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(user_id, name),
    FOREIGN KEY (user_id)     REFERENCES users(user_id)          ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES product_categories(id)  ON DELETE SET NULL
);

-- ── جدول المبيعات ─────────────────────────────────────────────────────────────
-- total_amount و profit عمودان VIRTUAL (محسوبان تلقائياً، غير مخزَّنين).
CREATE TABLE IF NOT EXISTS sales (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    product_id      INTEGER,                  -- NULL إذا المنتج غير مسجّل في المخزون
    product_name    TEXT    NOT NULL,
    quantity        REAL    NOT NULL DEFAULT 1.0,
    unit_price      REAL    NOT NULL DEFAULT 0.0,
    total_amount    REAL    GENERATED ALWAYS AS (quantity * unit_price)         VIRTUAL,
    cost_price      REAL    DEFAULT 0.0,
    profit          REAL    GENERATED ALWAYS AS (quantity * (unit_price - cost_price)) VIRTUAL,
    payment_method  TEXT    DEFAULT 'كاش',    -- كاش / تحويل / آجل
    customer_name   TEXT,
    notes           TEXT,
    sale_date       TEXT    DEFAULT (datetime('now')),
    created_at      TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id)    REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)   ON DELETE SET NULL
);

-- ── جدول فئات المصاريف ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS expense_categories (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    name            TEXT    NOT NULL,
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT    DEFAULT (datetime('now')),
    UNIQUE(user_id, name),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- ── جدول المصاريف ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS expenses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    category_id     INTEGER,
    description     TEXT    NOT NULL,
    amount          REAL    NOT NULL DEFAULT 0.0,
    payment_method  TEXT    DEFAULT 'كاش',
    receipt_ref     TEXT,                     -- رقم الإيصال (اختياري)
    notes           TEXT,
    expense_date    TEXT    DEFAULT (datetime('now')),
    created_at      TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id)     REFERENCES users(user_id)         ON DELETE CASCADE,
    FOREIGN KEY (category_id) REFERENCES expense_categories(id) ON DELETE SET NULL
);

-- ── جدول سجل حركة المخزون ────────────────────────────────────────────────────
-- يسجّل كل تغيير في المخزون (بيع / شراء / تعديل يدوي).
-- change_type: sale / purchase / manual_add / manual_remove
CREATE TABLE IF NOT EXISTS inventory_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    product_id      INTEGER NOT NULL,
    change_type     TEXT    NOT NULL,         -- نوع الحركة
    quantity_change REAL    NOT NULL,         -- موجب=إضافة, سالب=خصم
    quantity_before REAL    NOT NULL DEFAULT 0.0,
    quantity_after  REAL    NOT NULL DEFAULT 0.0,
    reference_id    INTEGER,                  -- id المبيعة أو الطلب المرتبط
    notes           TEXT,
    created_at      TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id)    REFERENCES users(user_id) ON DELETE CASCADE,
    FOREIGN KEY (product_id) REFERENCES products(id)   ON DELETE CASCADE
);

-- ── جدول إعدادات المستخدم ────────────────────────────────────────────────────
-- إعداد واحد لكل مستخدم (PRIMARY KEY = user_id).
CREATE TABLE IF NOT EXISTS user_settings (
    user_id             INTEGER PRIMARY KEY,
    currency            TEXT    DEFAULT 'ل.س',       -- رمز العملة
    date_format         TEXT    DEFAULT '%Y-%m-%d',
    notify_low_stock    INTEGER DEFAULT 1,            -- 1=مفعّل, 0=معطّل
    custom_buttons      TEXT    DEFAULT '{}',         -- JSON (غير مستخدم حالياً)
    partner_percentage  REAL    DEFAULT 0.0,          -- نسبة الشريك %
    extra_data          TEXT    DEFAULT '{}',         -- JSON لبيانات إضافية مستقبلية
    updated_at          TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- ── جدول المصاريف الثابتة ────────────────────────────────────────────────────
-- مصاريف متكررة (إيجار / رواتب ...).
-- expense_type: daily / monthly / yearly
CREATE TABLE IF NOT EXISTS fixed_expenses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    name            TEXT    NOT NULL,
    amount          REAL    NOT NULL DEFAULT 0.0,
    expense_type    TEXT    DEFAULT 'monthly',
    is_active       INTEGER DEFAULT 1,
    created_at      TEXT    DEFAULT (datetime('now')),
    updated_at      TEXT    DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- ── جدول الإعدادات العامة للنظام ─────────────────────────────────────────────
-- مخزن key-value للإعدادات الإدارية (معلومات الدفع، أسماء الأزرار ...).
-- القيمة value مخزّنة كـ JSON string.
CREATE TABLE IF NOT EXISTS global_settings (
    key             TEXT PRIMARY KEY,
    value           TEXT DEFAULT '{}'
);

-- ── فهارس لتحسين أداء الاستعلامات ───────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_sales_user_date
    ON sales(user_id, sale_date);

CREATE INDEX IF NOT EXISTS idx_expenses_user_date
    ON expenses(user_id, expense_date);

CREATE INDEX IF NOT EXISTS idx_products_user
    ON products(user_id, is_active);

CREATE INDEX IF NOT EXISTS idx_inventory_product
    ON inventory_log(product_id, created_at);
"""


# =============================================================================
# 3. تهيئة قاعدة البيانات
# =============================================================================

def init_database() -> None:
    """
    تهيئة قاعدة البيانات وإنشاء جميع الجداول إن لم تكن موجودة.
    يُستدعى مرة واحدة عند بدء تشغيل البوت.
    يتعامل مع ترقيات (migrations) الإصدارات القديمة تلقائياً.
    """
    conn = get_connection()
    try:
        # إنشاء جميع الجداول والفهارس
        conn.executescript(SCHEMA_SQL)

        # Migration: إضافة عمود partner_percentage إذا كانت قاعدة بيانات قديمة
        try:
            conn.execute(
                "ALTER TABLE user_settings ADD COLUMN partner_percentage REAL DEFAULT 0.0"
            )
            conn.commit()
            logger.info("Migration: تم إضافة عمود partner_percentage")
        except Exception:
            pass  # العمود موجود مسبقاً — طبيعي

        # Migration: إضافة جدول fixed_expenses إذا لم يكن موجوداً
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS fixed_expenses (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id         INTEGER NOT NULL,
                    name            TEXT    NOT NULL,
                    amount          REAL    NOT NULL DEFAULT 0.0,
                    expense_type    TEXT    DEFAULT 'monthly',
                    is_active       INTEGER DEFAULT 1,
                    created_at      TEXT    DEFAULT (datetime('now')),
                    updated_at      TEXT    DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
                )
            """)
            conn.commit()
        except Exception:
            pass

        # Migration: إضافة جدول global_settings إذا لم يكن موجوداً
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS global_settings (
                    key   TEXT PRIMARY KEY,
                    value TEXT DEFAULT '{}'
                )
            """)
            conn.commit()
        except Exception:
            pass

    finally:
        conn.close()

    logger.info("✅ قاعدة البيانات جاهزة: %s", DB_PATH)


# =============================================================================
# 4. إدارة المستخدمين
# =============================================================================

def upsert_user(
    user_id: int,
    username: str = None,
    full_name: str = None,
    language_code: str = "ar",
) -> None:
    """
    إضافة مستخدم جديد أو تحديث بياناته إذا كان موجوداً (INSERT OR UPDATE).
    يُستدعى عند كل /start لضمان تحديث البيانات.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        conn.execute("""
            INSERT INTO users (user_id, username, full_name, language_code, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username      = excluded.username,
                full_name     = excluded.full_name,
                language_code = excluded.language_code,
                updated_at    = excluded.updated_at
        """, (user_id, username, full_name, language_code, now, now))


def get_user(user_id: int) -> Optional[dict]:
    """
    جلب بيانات مستخدم واحد بالـ user_id.
    يعيد dict أو None إذا لم يُوجد.
    """
    with db_context() as conn:
        row = conn.execute(
            "SELECT * FROM users WHERE user_id = ?", (user_id,)
        ).fetchone()
        return dict(row) if row else None


def setup_new_user(
    user_id: int,
    username: str = None,
    full_name: str = None,
    language_code: str = "ar",
) -> None:
    """
    إعداد المستخدم الجديد كاملاً في خطوة واحدة.
    تُستدعى عند أول /start وتنفّذ تلقائياً:
      1. تسجيل بيانات المستخدم
      2. إنشاء اشتراك تجريبي (إذا لم يكن لديه اشتراك)
      3. إضافة فئات المصاريف الافتراضية
      4. إضافة فئات المنتجات الافتراضية
      5. إنشاء سجل الإعدادات
    """
    from config import DEFAULT_EXPENSE_CATEGORIES, DEFAULT_PRODUCT_CATEGORIES

    upsert_user(user_id, username, full_name, language_code)

    if not get_subscription(user_id):
        create_trial_subscription(user_id)

    if not get_expense_categories(user_id):
        for cat in DEFAULT_EXPENSE_CATEGORIES:
            add_expense_category(user_id, cat)

    if not get_product_categories(user_id):
        for cat in DEFAULT_PRODUCT_CATEGORIES:
            add_product_category(user_id, cat)

    upsert_user_settings(user_id)
    logger.info("✅ مستخدم جديد: %s", user_id)


# =============================================================================
# 5. إدارة الاشتراكات
# =============================================================================

def create_trial_subscription(user_id: int) -> None:
    """إنشاء اشتراك تجريبي للمستخدم الجديد بمدة DEFAULT_TRIAL_DAYS أيام."""
    start = datetime.utcnow()
    end   = start + timedelta(days=DEFAULT_TRIAL_DAYS)
    with db_context() as conn:
        conn.execute("""
            INSERT OR IGNORE INTO subscriptions (user_id, plan, status, start_date, end_date)
            VALUES (?, 'trial', 'active', ?, ?)
        """, (user_id, start.isoformat(), end.isoformat()))


def get_subscription(user_id: int) -> Optional[dict]:
    """
    جلب تفاصيل اشتراك مستخدم.
    يعيد dict أو None إذا لم يكن له اشتراك.
    """
    with db_context() as conn:
        row = conn.execute(
            "SELECT * FROM subscriptions WHERE user_id = ?", (user_id,)
        ).fetchone()
        return dict(row) if row else None


def is_subscription_active(user_id: int) -> bool:
    """
    التحقق من صلاحية اشتراك المستخدم.
    - يعيد True إذا كان الاشتراك نشطاً وغير منتهٍ.
    - يُحدّث status إلى 'expired' تلقائياً إذا انتهت المدة.
    """
    with db_context() as conn:
        row = conn.execute(
            "SELECT status, end_date FROM subscriptions WHERE user_id = ?",
            (user_id,)
        ).fetchone()
        if not row:
            return False
        status   = row["status"]
        end_date = row["end_date"]

    if status != "active":
        return False

    if end_date is None:
        return True  # اشتراك مدى الحياة

    now = datetime.utcnow().isoformat()
    if now > end_date:
        # انتهت المدة — تحديث تلقائي
        try:
            with db_context() as conn:
                conn.execute("""
                    UPDATE subscriptions
                    SET status = 'expired'
                    WHERE user_id = ? AND status = 'active'
                """, (user_id,))
            logger.info("⏰ انتهى اشتراك المستخدم تلقائياً: %s", user_id)
        except Exception as e:
            logger.warning("تحذير: فشل تحديث حالة الاشتراك لـ %s: %s", user_id, e)
        return False

    return True


def activate_subscription(user_id: int, plan: str, days: int) -> None:
    """
    تفعيل أو تجديد اشتراك مستخدم (يستخدمها الأدمن).
    المعاملات:
      plan  — monthly / yearly / lifetime / trial
      days  — عدد الأيام (0 = مدى الحياة بلا انتهاء)
    """
    start   = datetime.utcnow()
    end     = None if days == 0 else (start + timedelta(days=days)).isoformat()
    with db_context() as conn:
        conn.execute("""
            INSERT INTO subscriptions (user_id, plan, status, start_date, end_date)
            VALUES (?, ?, 'active', ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                plan       = excluded.plan,
                status     = 'active',
                start_date = excluded.start_date,
                end_date   = excluded.end_date
        """, (user_id, plan, start.isoformat(), end))


def deactivate_subscription(user_id: int) -> bool:
    """
    إلغاء اشتراك مستخدم بواسطة الأدمن (يضبط status = 'suspended').
    يعيد True إذا نجحت العملية، False إذا لم يكن هناك اشتراك نشط.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            UPDATE subscriptions
            SET status = 'suspended', end_date = ?, notes = 'أُلغي بواسطة الأدمن'
            WHERE user_id = ? AND status = 'active'
        """, (now, user_id))
        return cursor.rowcount > 0


# =============================================================================
# 6. إدارة المنتجات والمخزون
# =============================================================================

def add_product(
    user_id: int,
    name: str,
    selling_price: float,
    cost_price: float = 0.0,
    stock_quantity: float = 0.0,
    unit: str = "قطعة",
    category_id: int = None,
    min_stock_alert: float = 0.0,
) -> int:
    """
    إضافة منتج جديد.
    يعيد id المنتج المضاف.
    يرفع استثناء إذا كان اسم المنتج مكرراً للمستخدم نفسه.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            INSERT INTO products
                (user_id, category_id, name, unit, cost_price, selling_price,
                 stock_quantity, min_stock_alert, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, category_id, name, unit, cost_price, selling_price,
              stock_quantity, min_stock_alert, now, now))
        return cursor.lastrowid


def get_product(user_id: int, product_id: int) -> Optional[dict]:
    """
    جلب منتج واحد بالـ id مع التحقق أنه يخص المستخدم.
    يعيد dict أو None.
    """
    with db_context() as conn:
        row = conn.execute("""
            SELECT * FROM products
            WHERE id = ? AND user_id = ? AND is_active = 1
        """, (product_id, user_id)).fetchone()
        return dict(row) if row else None


def get_products(user_id: int, category_id: int = None) -> list:
    """
    جلب قائمة منتجات المستخدم مع اسم الفئة.
    يمكن تصفيتها بـ category_id (اختياري).
    """
    with db_context() as conn:
        if category_id:
            rows = conn.execute("""
                SELECT p.*, pc.name AS category_name
                FROM products p
                LEFT JOIN product_categories pc ON p.category_id = pc.id
                WHERE p.user_id = ? AND p.category_id = ? AND p.is_active = 1
                ORDER BY p.name
            """, (user_id, category_id)).fetchall()
        else:
            rows = conn.execute("""
                SELECT p.*, pc.name AS category_name
                FROM products p
                LEFT JOIN product_categories pc ON p.category_id = pc.id
                WHERE p.user_id = ? AND p.is_active = 1
                ORDER BY p.name
            """, (user_id,)).fetchall()
        return [dict(r) for r in rows]


def get_low_stock_products(user_id: int) -> list:
    """
    جلب المنتجات التي وصلت للحد الأدنى في المخزون.
    فقط المنتجات التي min_stock_alert > 0 (مفعّل التنبيه لها).
    """
    with db_context() as conn:
        rows = conn.execute("""
            SELECT * FROM products
            WHERE user_id = ?
              AND is_active = 1
              AND min_stock_alert > 0
              AND stock_quantity <= min_stock_alert
            ORDER BY stock_quantity ASC
        """, (user_id,)).fetchall()
        return [dict(r) for r in rows]


def _update_stock_within_conn(
    conn: sqlite3.Connection,
    user_id: int,
    product_id: int,
    quantity_change: float,
    change_type: str,
    reference_id: int = None,
    notes: str = None,
) -> float:
    """
    [داخلي] تحديث المخزون داخل اتصال موجود.
    مصمم للاستدعاء ضمن transaction خارجية (مثل record_sale).

    المعاملات:
      quantity_change — موجب = إضافة، سالب = خصم
      change_type     — sale / purchase / manual_add / manual_remove

    يرفع ValueError إذا ستصبح الكمية سالبة.
    يعيد الكمية الجديدة بعد التحديث.
    """
    now = datetime.utcnow().isoformat()

    row = conn.execute(
        "SELECT stock_quantity FROM products WHERE id = ? AND user_id = ?",
        (product_id, user_id)
    ).fetchone()

    if not row:
        raise ValueError(f"المنتج {product_id} غير موجود للمستخدم {user_id}")

    qty_before = row["stock_quantity"]
    qty_after  = qty_before + quantity_change

    if qty_after < 0:
        raise ValueError(
            f"المخزون غير كافٍ. متوفر: {qty_before}، مطلوب: {abs(quantity_change)}"
        )

    # تحديث الكمية في جدول المنتجات
    conn.execute("""
        UPDATE products
        SET stock_quantity = ?, updated_at = ?
        WHERE id = ? AND user_id = ?
    """, (qty_after, now, product_id, user_id))

    # تسجيل الحركة في سجل المخزون
    conn.execute("""
        INSERT INTO inventory_log
            (user_id, product_id, change_type, quantity_change,
             quantity_before, quantity_after, reference_id, notes, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (user_id, product_id, change_type, quantity_change,
          qty_before, qty_after, reference_id, notes, now))

    return qty_after


def update_stock(
    user_id: int,
    product_id: int,
    quantity_change: float,
    change_type: str,
    reference_id: int = None,
    notes: str = None,
) -> float:
    """
    تحديث مخزون منتج وتسجيل الحركة في inventory_log.
    للاستخدام الخارجي (مثل إضافة مخزون يدوياً).

    المعاملات:
      quantity_change — موجب = إضافة، سالب = خصم
      change_type     — sale / purchase / manual_add / manual_remove

    يرفع ValueError إذا ستصبح الكمية سالبة.
    يعيد الكمية الجديدة بعد التحديث.
    """
    with db_context() as conn:
        return _update_stock_within_conn(
            conn, user_id, product_id, quantity_change,
            change_type, reference_id, notes
        )


# =============================================================================
# 7. تسجيل المبيعات
# =============================================================================

def record_sale(
    user_id: int,
    product_name: str,
    quantity: float,
    unit_price: float,
    cost_price: float = 0.0,
    product_id: int = None,
    payment_method: str = "كاش",
    customer_name: str = None,
    notes: str = None,
    sale_date: str = None,
) -> int:
    """
    تسجيل عملية بيع وخصم المخزون في transaction واحدة.

    المعاملات:
      product_id — إذا كان موجوداً يُخصم من مخزون المنتج تلقائياً
      sale_date  — إذا لم يُحدَّد يُستخدم وقت الآن

    يعيد id المبيعة المسجّلة.
    ملاحظة: إذا كان المخزون غير كافٍ تُسجَّل المبيعة مع تحذير في الـ log (لا ترفض).
    """
    now = datetime.utcnow().isoformat()
    if not sale_date:
        sale_date = now

    with db_context() as conn:
        # تسجيل المبيعة
        cursor = conn.execute("""
            INSERT INTO sales
                (user_id, product_id, product_name, quantity, unit_price,
                 cost_price, payment_method, customer_name, notes, sale_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, product_id, product_name, quantity, unit_price,
              cost_price, payment_method, customer_name, notes, sale_date, now))
        sale_id = cursor.lastrowid

        # خصم المخزون في نفس الـ transaction (إذا كان المنتج مرتبطاً بالمخزون)
        if product_id:
            try:
                _update_stock_within_conn(
                    conn=conn,
                    user_id=user_id,
                    product_id=product_id,
                    quantity_change=-quantity,
                    change_type="sale",
                    reference_id=sale_id,
                    notes=f"بيع للعميل: {customer_name or 'غير محدد'}",
                )
            except ValueError as e:
                # مخزون غير كافٍ — نسجّل المبيعة ونُنبّه في الـ log فقط
                logger.warning("تحذير مخزون في المبيعة #%s: %s", sale_id, e)

    return sale_id


def get_sales(
    user_id: int,
    start_date: str = None,
    end_date: str = None,
    limit: int = 50,
) -> list:
    """
    جلب سجل مبيعات المستخدم مرتبة تنازلياً (الأحدث أولاً).
    المعاملات:
      start_date / end_date — ISO 8601 للتصفية بالتاريخ (اختياري)
      limit                 — الحد الأقصى للنتائج (افتراضي 50)
    """
    query  = "SELECT * FROM sales WHERE user_id = ?"
    params: list = [user_id]

    if start_date:
        query += " AND sale_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND sale_date <= ?"
        params.append(end_date)

    query += " ORDER BY sale_date DESC LIMIT ?"
    params.append(limit)

    with db_context() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


# =============================================================================
# 8. تسجيل المصاريف
# =============================================================================

def record_expense(
    user_id: int,
    description: str,
    amount: float,
    category_id: int = None,
    payment_method: str = "كاش",
    receipt_ref: str = None,
    notes: str = None,
    expense_date: str = None,
) -> int:
    """
    تسجيل مصروف جديد.
    يعيد id المصروف المسجّل.
    """
    now = datetime.utcnow().isoformat()
    if not expense_date:
        expense_date = now

    with db_context() as conn:
        cursor = conn.execute("""
            INSERT INTO expenses
                (user_id, category_id, description, amount, payment_method,
                 receipt_ref, notes, expense_date, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user_id, category_id, description, amount, payment_method,
              receipt_ref, notes, expense_date, now))
        return cursor.lastrowid


def get_sale(user_id: int, sale_id: int) -> Optional[dict]:
    """جلب مبيعة واحدة بالـ id مع التحقق أنها تخص المستخدم."""
    with db_context() as conn:
        row = conn.execute(
            "SELECT * FROM sales WHERE id = ? AND user_id = ?",
            (sale_id, user_id)
        ).fetchone()
        return dict(row) if row else None


def delete_sale(user_id: int, sale_id: int) -> bool:
    """
    حذف مبيعة وإعادة الكمية للمخزون إن كان المنتج مرتبطاً.
    يعيد True إذا نجح الحذف.
    """
    with db_context() as conn:
        row = conn.execute(
            "SELECT product_id, quantity FROM sales WHERE id = ? AND user_id = ?",
            (sale_id, user_id)
        ).fetchone()
        if not row:
            return False
        # إعادة الكمية للمخزون إذا كانت المبيعة مرتبطة بمنتج
        if row["product_id"]:
            try:
                _update_stock_within_conn(
                    conn, user_id, row["product_id"],
                    quantity_change=row["quantity"],
                    change_type="manual_add",
                    notes=f"إعادة مخزون بعد حذف مبيعة #{sale_id}",
                )
            except Exception as e:
                logger.warning("تحذير: فشل إعادة المخزون عند حذف المبيعة: %s", e)
        conn.execute("DELETE FROM sales WHERE id = ? AND user_id = ?", (sale_id, user_id))
        return True


def update_sale(
    user_id: int,
    sale_id: int,
    product_name: str,
    quantity: float,
    unit_price: float,
    cost_price: float,
    payment_method: str,
    customer_name: str = None,
    notes: str = None,
) -> bool:
    """
    تعديل بيانات مبيعة موجودة (بدون تعديل المخزون لتجنب التعقيد).
    يعيد True إذا نجح التعديل.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            UPDATE sales
            SET product_name   = ?,
                quantity       = ?,
                unit_price     = ?,
                cost_price     = ?,
                payment_method = ?,
                customer_name  = ?,
                notes          = ?
            WHERE id = ? AND user_id = ?
        """, (product_name, quantity, unit_price, cost_price,
              payment_method, customer_name, notes, sale_id, user_id))
        return cursor.rowcount > 0


def get_expense(user_id: int, expense_id: int) -> Optional[dict]:
    """جلب مصروف واحد بالـ id مع التحقق أنه يخص المستخدم."""
    with db_context() as conn:
        row = conn.execute(
            "SELECT * FROM expenses WHERE id = ? AND user_id = ?",
            (expense_id, user_id)
        ).fetchone()
        return dict(row) if row else None


def delete_expense(user_id: int, expense_id: int) -> bool:
    """حذف مصروف. يعيد True إذا نجح الحذف."""
    with db_context() as conn:
        cursor = conn.execute(
            "DELETE FROM expenses WHERE id = ? AND user_id = ?",
            (expense_id, user_id)
        )
        return cursor.rowcount > 0


def update_expense(
    user_id: int,
    expense_id: int,
    description: str,
    amount: float,
    category_id: int = None,
    payment_method: str = "كاش",
    notes: str = None,
) -> bool:
    """
    تعديل بيانات مصروف موجود.
    يعيد True إذا نجح التعديل.
    """
    with db_context() as conn:
        cursor = conn.execute("""
            UPDATE expenses
            SET description    = ?,
                amount         = ?,
                category_id    = ?,
                payment_method = ?,
                notes          = ?
            WHERE id = ? AND user_id = ?
        """, (description, amount, category_id, payment_method, notes,
              expense_id, user_id))
        return cursor.rowcount > 0


def get_expenses(
    user_id: int,
    start_date: str = None,
    end_date: str = None,
    limit: int = 50,
) -> list:
    """
    جلب سجل مصاريف المستخدم مع اسم الفئة.
    المعاملات:
      start_date / end_date — ISO 8601 للتصفية بالتاريخ (اختياري)
      limit                 — الحد الأقصى للنتائج (افتراضي 50)
    """
    query = """
        SELECT e.*, ec.name AS category_name
        FROM expenses e
        LEFT JOIN expense_categories ec ON e.category_id = ec.id
        WHERE e.user_id = ?
    """
    params: list = [user_id]

    if start_date:
        query += " AND e.expense_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND e.expense_date <= ?"
        params.append(end_date)

    query += " ORDER BY e.expense_date DESC LIMIT ?"
    params.append(limit)

    with db_context() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


# =============================================================================
# 9. التقارير والأرباح
# =============================================================================

def get_profit_summary(
    user_id: int,
    start_date: str = None,
    end_date: str = None,
) -> dict:
    """
    حساب ملخص الأرباح لفترة زمنية محددة.

    يعيد dict يحتوي على:
      total_sales    — إجمالي المبيعات
      total_cost     — إجمالي التكاليف
      gross_profit   — الربح الإجمالي (total_sales - total_cost)
      total_expenses — إجمالي المصاريف التشغيلية
      net_profit     — صافي الربح (gross_profit - total_expenses)
      partner_pct    — نسبة الشريك %
      partner_amount — حصة الشريك بالعملة
      owner_profit   — صافي حصة المالك
      sales_count    — عدد المبيعات
      expenses_count — عدد المصاريف
    """
    # بناء شروط التصفية بالتاريخ
    date_filter_sales    = ""
    date_filter_expenses = ""
    params_sales:    list = [user_id]
    params_expenses: list = [user_id]

    if start_date:
        date_filter_sales    += " AND sale_date >= ?"
        date_filter_expenses += " AND expense_date >= ?"
        params_sales.append(start_date)
        params_expenses.append(start_date)
    if end_date:
        date_filter_sales    += " AND sale_date <= ?"
        date_filter_expenses += " AND expense_date <= ?"
        params_sales.append(end_date)
        params_expenses.append(end_date)

    with db_context() as conn:
        sales_row = conn.execute(f"""
            SELECT
                COALESCE(SUM(quantity * unit_price), 0.0) AS total_sales,
                COALESCE(SUM(quantity * cost_price), 0.0) AS total_cost,
                COUNT(*)                                   AS sales_count
            FROM sales
            WHERE user_id = ? {date_filter_sales}
        """, params_sales).fetchone()

        expenses_row = conn.execute(f"""
            SELECT
                COALESCE(SUM(amount), 0.0) AS total_expenses,
                COUNT(*)                   AS expenses_count
            FROM expenses
            WHERE user_id = ? {date_filter_expenses}
        """, params_expenses).fetchone()

        sales_data    = dict(sales_row)
        expenses_data = dict(expenses_row)

    total_sales    = sales_data["total_sales"]
    total_cost     = sales_data["total_cost"]
    gross_profit   = total_sales - total_cost
    total_expenses = expenses_data["total_expenses"]
    net_profit     = gross_profit - total_expenses

    # حساب حصة الشريك (إذا كانت مفعّلة)
    settings       = get_user_settings(user_id)
    partner_pct    = float(settings.get("partner_percentage") or 0.0)
    partner_amount = round(net_profit * partner_pct / 100, 2) if partner_pct > 0 else 0.0
    owner_profit   = round(net_profit - partner_amount, 2)

    return {
        "total_sales":      round(total_sales, 2),
        "total_cost":       round(total_cost, 2),
        "gross_profit":     round(gross_profit, 2),
        "total_expenses":   round(total_expenses, 2),
        "net_profit":       round(net_profit, 2),
        "partner_pct":      partner_pct,
        "partner_amount":   partner_amount,
        "owner_profit":     owner_profit,
        "sales_count":      sales_data["sales_count"],
        "expenses_count":   expenses_data["expenses_count"],
    }


def get_top_selling_products(user_id: int, limit: int = 5, start_date: str = None) -> list:
    """
    جلب أكثر المنتجات مبيعاً مرتبة تنازلياً.
    يعيد list of dict تحتوي على:
      product_name, total_qty, total_revenue, transactions
    """
    query  = """
        SELECT
            product_name,
            SUM(quantity)              AS total_qty,
            SUM(quantity * unit_price) AS total_revenue,
            COUNT(*)                   AS transactions
        FROM sales
        WHERE user_id = ?
    """
    params: list = [user_id]

    if start_date:
        query  += " AND sale_date >= ?"
        params.append(start_date)

    query += " GROUP BY product_name ORDER BY total_qty DESC LIMIT ?"
    params.append(limit)

    with db_context() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(r) for r in rows]


def get_daily_comparison(user_id: int) -> dict:
    """
    مقارنة أداء اليوم الحالي بأداء الأمس.
    يعيد dict يحتوي على:
      today     — {total_sales, total_expenses, net_profit, sales_count}
      yesterday — {total_sales, total_expenses, net_profit, sales_count}
      diff_sales  — نسبة التغيير في المبيعات %
      diff_profit — نسبة التغيير في الربح %
    """
    today     = datetime.utcnow().strftime("%Y-%m-%d")
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    def _day_summary(date_str: str) -> dict:
        """حساب ملخص يوم محدد."""
        start = f"{date_str}T00:00:00"
        end   = f"{date_str}T23:59:59"
        with db_context() as conn:
            s = conn.execute("""
                SELECT
                    COALESCE(SUM(quantity * unit_price), 0.0) AS total_sales,
                    COALESCE(SUM(quantity * cost_price), 0.0) AS total_cost,
                    COUNT(*) AS sales_count
                FROM sales
                WHERE user_id = ? AND sale_date BETWEEN ? AND ?
            """, (user_id, start, end)).fetchone()
            e = conn.execute("""
                SELECT COALESCE(SUM(amount), 0.0) AS total_expenses
                FROM expenses
                WHERE user_id = ? AND expense_date BETWEEN ? AND ?
            """, (user_id, start, end)).fetchone()
            s_d = dict(s)
            e_d = dict(e)

        net = s_d["total_sales"] - s_d["total_cost"] - e_d["total_expenses"]
        return {
            "total_sales":    round(s_d["total_sales"],    2),
            "total_expenses": round(e_d["total_expenses"], 2),
            "net_profit":     round(net, 2),
            "sales_count":    s_d["sales_count"],
        }

    def _percent_diff(new_val: float, old_val: float) -> float:
        """حساب نسبة التغيير بين قيمتين."""
        if old_val == 0:
            return 100.0 if new_val > 0 else 0.0
        return round((new_val - old_val) / abs(old_val) * 100, 1)

    today_data     = _day_summary(today)
    yesterday_data = _day_summary(yesterday)

    return {
        "today":       today_data,
        "yesterday":   yesterday_data,
        "diff_sales":  _percent_diff(today_data["total_sales"],  yesterday_data["total_sales"]),
        "diff_profit": _percent_diff(today_data["net_profit"],   yesterday_data["net_profit"]),
    }


# =============================================================================
# 10. إدارة الفئات (مصاريف / منتجات)
# =============================================================================

def add_expense_category(user_id: int, name: str) -> int:
    """
    إضافة فئة مصروف جديدة.
    INSERT OR IGNORE — لا يرفع خطأ إذا كانت الفئة موجودة مسبقاً.
    يعيد id الفئة (أو 0 إذا كانت موجودة).
    """
    with db_context() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO expense_categories (user_id, name)
            VALUES (?, ?)
        """, (user_id, name))
        return cursor.lastrowid


def get_expense_categories(user_id: int) -> list:
    """جلب فئات المصاريف النشطة للمستخدم مرتبة أبجدياً."""
    with db_context() as conn:
        rows = conn.execute("""
            SELECT * FROM expense_categories
            WHERE user_id = ? AND is_active = 1
            ORDER BY name
        """, (user_id,)).fetchall()
        return [dict(r) for r in rows]


def add_product_category(user_id: int, name: str) -> int:
    """
    إضافة فئة منتج جديدة.
    INSERT OR IGNORE — لا يرفع خطأ إذا كانت الفئة موجودة مسبقاً.
    يعيد id الفئة (أو 0 إذا كانت موجودة).
    """
    with db_context() as conn:
        cursor = conn.execute("""
            INSERT OR IGNORE INTO product_categories (user_id, name)
            VALUES (?, ?)
        """, (user_id, name))
        return cursor.lastrowid


def get_product_categories(user_id: int) -> list:
    """جلب فئات المنتجات النشطة للمستخدم مرتبة أبجدياً."""
    with db_context() as conn:
        rows = conn.execute("""
            SELECT * FROM product_categories
            WHERE user_id = ? AND is_active = 1
            ORDER BY name
        """, (user_id,)).fetchall()
        return [dict(r) for r in rows]


# =============================================================================
# 11. إعدادات المستخدم
# =============================================================================

def get_user_settings(user_id: int) -> dict:
    """
    جلب إعدادات المستخدم.
    يعيد dict من قاعدة البيانات، أو قيماً افتراضية إذا لم يكن السجل موجوداً.
    """
    with db_context() as conn:
        row = conn.execute(
            "SELECT * FROM user_settings WHERE user_id = ?", (user_id,)
        ).fetchone()
        if row:
            return dict(row)

    # إعدادات افتراضية عند غياب السجل
    return {
        "user_id":           user_id,
        "currency":          "ل.س",
        "date_format":       "%Y-%m-%d",
        "notify_low_stock":  1,
        "custom_buttons":    "{}",
        "partner_percentage": 0.0,
        "extra_data":        "{}",
    }


def upsert_user_settings(user_id: int, **kwargs) -> None:
    """
    تحديث إعداد واحد أو أكثر للمستخدم (INSERT + UPDATE في نفس الـ transaction).

    الاستخدام:
      upsert_user_settings(123, currency='$')
      upsert_user_settings(123, notify_low_stock=0, partner_percentage=20.0)

    الحقول المسموح بتعديلها:
      currency, date_format, notify_low_stock, custom_buttons,
      extra_data, partner_percentage
    """
    allowed_fields = {
        "currency", "date_format", "notify_low_stock",
        "custom_buttons", "extra_data", "partner_percentage",
    }
    # تصفية الحقول غير المسموح بها
    fields = {k: v for k, v in kwargs.items() if k in allowed_fields}
    fields["updated_at"] = datetime.utcnow().isoformat()

    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values     = list(fields.values()) + [user_id]

    with db_context() as conn:
        # ضمان وجود السجل أولاً
        conn.execute(
            "INSERT OR IGNORE INTO user_settings (user_id) VALUES (?)", (user_id,)
        )
        # تحديث الحقول المطلوبة
        if fields:
            conn.execute(
                f"UPDATE user_settings SET {set_clause} WHERE user_id = ?",
                values
            )


# =============================================================================
# 12. المصاريف الثابتة
# =============================================================================

def add_fixed_expense(
    user_id: int,
    name: str,
    amount: float,
    expense_type: str = "monthly",
) -> int:
    """
    إضافة مصروف ثابت جديد.
    المعاملات:
      expense_type — daily / monthly / yearly
    يعيد id المصروف المضاف.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            INSERT INTO fixed_expenses (user_id, name, amount, expense_type, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (user_id, name, amount, expense_type, now, now))
        return cursor.lastrowid


def get_fixed_expenses(user_id: int) -> list:
    """جلب جميع المصاريف الثابتة النشطة للمستخدم مرتبة بالنوع ثم الاسم."""
    with db_context() as conn:
        rows = conn.execute("""
            SELECT * FROM fixed_expenses
            WHERE user_id = ? AND is_active = 1
            ORDER BY expense_type, name
        """, (user_id,)).fetchall()
        return [dict(r) for r in rows]


def update_fixed_expense(
    user_id: int,
    expense_id: int,
    name: str,
    amount: float,
    expense_type: str,
) -> bool:
    """
    تعديل مصروف ثابت.
    يعيد True إذا وُجد السجل وتم تعديله، False إذا لم يُوجد.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            UPDATE fixed_expenses
            SET name = ?, amount = ?, expense_type = ?, updated_at = ?
            WHERE id = ? AND user_id = ? AND is_active = 1
        """, (name, amount, expense_type, now, expense_id, user_id))
        return cursor.rowcount > 0


def delete_fixed_expense(user_id: int, expense_id: int) -> bool:
    """
    حذف ناعم (Soft Delete) لمصروف ثابت — يضبط is_active = 0.
    يعيد True إذا نجح الحذف.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        cursor = conn.execute("""
            UPDATE fixed_expenses
            SET is_active = 0, updated_at = ?
            WHERE id = ? AND user_id = ?
        """, (now, expense_id, user_id))
        return cursor.rowcount > 0


def get_fixed_expenses_total(user_id: int, period: str = "monthly") -> float:
    """
    حساب إجمالي المصاريف الثابتة بعد تحويلها لفترة موحّدة.

    المعاملات:
      period — daily / monthly / yearly

    المنطق:
      مصروف يومي × 30 = شهري | مصروف شهري / 30 = يومي
      مصروف شهري × 12 = سنوي | مصروف سنوي / 12 = شهري
    """
    expenses = get_fixed_expenses(user_id)
    total    = 0.0

    for e in expenses:
        t   = e["expense_type"]
        amt = e["amount"]

        if period == "daily":
            if t == "daily":    total += amt
            elif t == "monthly": total += amt / 30
            elif t == "yearly":  total += amt / 365

        elif period == "monthly":
            if t == "daily":    total += amt * 30
            elif t == "monthly": total += amt
            elif t == "yearly":  total += amt / 12

        elif period == "yearly":
            if t == "daily":    total += amt * 365
            elif t == "monthly": total += amt * 12
            elif t == "yearly":  total += amt

    return round(total, 2)


# =============================================================================
# 13. الإعدادات العامة للنظام (global_settings)
# =============================================================================

def get_button_names(user_id: int = 0) -> dict:
    """
    جلب أسماء الأزرار المخصصة من global_settings.
    تُطبَّق على جميع المستخدمين (ليس فقط المستخدم المحدد).
    يعيد dict يحتوي على جميع مفاتيح الأزرار مع أسمائها الحالية.
    """
    import json

    BUTTON_DEFAULTS = {
        "btn_quick_calc": "⚡ حساب سريع",
        "btn_sale":        "💰 تسجيل مبيعة",
        "btn_expense":     "💸 تسجيل مصروف",
        "btn_inventory":   "📦 إدارة المخزون",
        "btn_fixed_exp":   "📌 مصاريف ثابتة",
        "btn_reports":     "📊 التقارير",
        "btn_settings":    "⚙️ الإعدادات",
        "btn_today":       "📅 اليوم",
        "btn_month":       "📆 هذا الشهر",
        "btn_year":        "📅 هذا العام",
        "btn_all":         "📈 إجمالي كامل",
        "btn_top":         "🏆 أكثر مبيعاً",
    }

    try:
        with db_context() as conn:
            row = conn.execute(
                "SELECT value FROM global_settings WHERE key = 'custom_buttons'"
            ).fetchone()
            custom = json.loads(row["value"]) if row else {}
    except Exception:
        custom = {}

    return {k: custom.get(k, v) for k, v in BUTTON_DEFAULTS.items()}


def set_button_name(admin_id: int, btn_key: str, new_name: str) -> None:
    """
    تحديث اسم زر في global_settings.
    يؤثر فوراً على جميع المستخدمين.
    """
    import json

    try:
        with db_context() as conn:
            row = conn.execute(
                "SELECT value FROM global_settings WHERE key = 'custom_buttons'"
            ).fetchone()
            custom = json.loads(row["value"]) if row else {}
    except Exception:
        custom = {}

    custom[btn_key] = new_name

    with db_context() as conn:
        conn.execute("""
            INSERT INTO global_settings (key, value) VALUES ('custom_buttons', ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (json.dumps(custom, ensure_ascii=False),))


def get_global_setting(key: str) -> Optional[str]:
    """
    جلب إعداد عام واحد من global_settings بالمفتاح.
    يعيد None إذا لم يُوجد.
    """
    try:
        with db_context() as conn:
            row = conn.execute(
                "SELECT value FROM global_settings WHERE key = ?", (key,)
            ).fetchone()
            return row["value"] if row else None
    except Exception:
        return None


def set_global_setting(key: str, value: str) -> None:
    """
    حفظ إعداد عام في global_settings (INSERT OR UPDATE).
    القيمة value يُفضَّل أن تكون JSON string للبيانات المركّبة.
    """
    with db_context() as conn:
        conn.execute("""
            INSERT INTO global_settings (key, value) VALUES (?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))


# =============================================================================
# 14. دوال الأدمن
# =============================================================================

def get_all_users_stats() -> list:
    """
    جلب إحصائيات جميع المستخدمين مع معلومات الاشتراك (للأدمن).
    يعيد list of dict مرتبة بتاريخ التسجيل (الأحدث أولاً).
    """
    with db_context() as conn:
        rows = conn.execute("""
            SELECT
                u.user_id,
                u.username,
                u.full_name,
                u.created_at,
                s.plan,
                s.status,
                s.end_date,
                (SELECT COUNT(*) FROM sales    WHERE user_id = u.user_id) AS sales_count,
                (SELECT COUNT(*) FROM expenses WHERE user_id = u.user_id) AS expenses_count
            FROM users u
            LEFT JOIN subscriptions s ON u.user_id = s.user_id
            ORDER BY u.created_at DESC
        """).fetchall()
        return [dict(r) for r in rows]


def get_expired_subscriptions() -> list:
    """
    جلب الاشتراكات التي انتهت مدتها (status=active لكن end_date < الآن).
    يُستخدم في لوحة الأدمن لمعرفة من يحتاج تجديد.
    """
    now = datetime.utcnow().isoformat()
    with db_context() as conn:
        rows = conn.execute("""
            SELECT u.user_id, u.username, u.full_name, s.plan, s.end_date
            FROM subscriptions s
            JOIN users u ON u.user_id = s.user_id
            WHERE s.status = 'active'
              AND s.end_date IS NOT NULL
              AND s.end_date < ?
        """, (now,)).fetchall()
        return [dict(r) for r in rows]
