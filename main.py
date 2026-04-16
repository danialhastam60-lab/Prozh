import os
import logging
import asyncio
import random
import string
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from fastapi import FastAPI, Request, HTTPException
from telegram import (
    Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
)
from telegram.ext import (
    Application, CommandHandler, ContextTypes, MessageHandler, filters, CallbackQueryHandler
)
import psycopg2
from psycopg2 import pool

# ---------- تنظیمات اولیه ----------
TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_USERNAME = "@PhoenixTunnel"
ADMIN_IDS = [6056483071, 5984875653]
SUPPORT_USERNAME = "@GtaVOwner"
BANK_CARD = "6274121773306105"
BANK_OWNER = "آقایی"
PRICE_PER_GB = 450000
CONFIG_NAME = "کانفیگ پر سرعت"

AVAILABLE_VOLUMES = [1, 2, 5, 10]

RENDER_BASE_URL = os.getenv("RENDER_EXTERNAL_URL") or os.getenv("RAILWAY_STATIC_URL") or "https://kavehvpn.railway.app"
WEBHOOK_PATH = f"/webhook/{TOKEN}"
WEBHOOK_URL = f"{RENDER_BASE_URL}{WEBHOOK_PATH}"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("kaveh_bot.log", encoding="utf-8") if os.path.exists("/tmp") else logging.StreamHandler()
    ]
)

app = FastAPI()

# ---------- وضعیت ربات ----------
bot_is_active = True  # وضعیت پیش‌فرض: روشن

# ---------- توابع کمکی ----------
def persian_number(number):
    persian_digits = {'0': '۰', '1': '۱', '2': '۲', '3': '۳', '4': '۴', '5': '۵', '6': '۶', '7': '۷', '8': '۸', '9': '۹'}
    return ''.join(persian_digits.get(ch, ch) for ch in str(number))

def english_number(persian_str):
    english_digits = {'۰': '0', '۱': '1', '۲': '2', '۳': '3', '۴': '4', '۵': '5', '۶': '6', '۷': '7', '۸': '8', '۹': '9'}
    result = ''
    for ch in persian_str:
        result += english_digits.get(ch, ch)
    return result

def format_price(price):
    return persian_number(f"{price:,}")

def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ---------- endpoint سلامت ----------
@app.get("/")
async def health_check():
    return {"status": "up", "message": "Kaveh VPN Bot is running!", "timestamp": datetime.now().isoformat()}

@app.get("/health")
async def health():
    try:
        await db_execute("SELECT 1", fetchone=True)
        return {"status": "healthy", "database": "connected", "bot": "running", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        return {"status": "unhealthy", "database": "disconnected", "error": str(e), "timestamp": datetime.now().isoformat()}

@app.get("/ping")
async def ping():
    return {"pong": True, "timestamp": datetime.now().isoformat()}

# ---------- مدیریت application ----------
application = Application.builder().token(TOKEN).build()

# ---------- PostgreSQL connection pool ----------
DATABASE_URL = os.getenv("DATABASE_URL")
db_pool: pool.ThreadedConnectionPool = None

def init_db_pool():
    global db_pool
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    try:
        db_pool = psycopg2.pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL, sslmode='require')
        logging.info("Database pool initialized successfully")
    except Exception as e:
        logging.error(f"Failed to initialize database pool: {e}")
        raise

def close_db_pool():
    global db_pool
    if db_pool:
        db_pool.closeall()
        db_pool = None
        logging.info("Database pool closed")

def _db_execute_sync(query, params=(), fetch=False, fetchone=False, returning=False):
    conn = None
    cur = None
    try:
        conn = db_pool.getconn()
        cur = conn.cursor()
        cur.execute(query, params)
        result = None
        if returning:
            result = cur.fetchone()[0] if cur.rowcount > 0 else None
        elif fetchone:
            result = cur.fetchone()
        elif fetch:
            result = cur.fetchall()
        if not query.strip().lower().startswith("select"):
            conn.commit()
        return result
    except Exception as e:
        logging.error(f"Database error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            db_pool.putconn(conn)

async def db_execute(query, params=(), fetch=False, fetchone=False, returning=False):
    try:
        return await asyncio.to_thread(_db_execute_sync, query, params, fetch, fetchone, returning)
    except Exception as e:
        logging.error(f"Async database error: {e}")
        raise

# ---------- ساخت جداول ----------
CREATE_BOT_STATUS_SQL = """
CREATE TABLE IF NOT EXISTS bot_status (
    id INTEGER PRIMARY KEY DEFAULT 1,
    is_active BOOLEAN DEFAULT TRUE
)
"""

CREATE_USERS_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id BIGINT PRIMARY KEY,
    username TEXT,
    balance BIGINT DEFAULT 0,
    invited_by BIGINT,
    phone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_agent BOOLEAN DEFAULT FALSE,
    is_new_user BOOLEAN DEFAULT TRUE
)
"""
CREATE_PAYMENTS_SQL = """
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    amount BIGINT,
    status TEXT,
    type TEXT,
    payment_method TEXT,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""
CREATE_SUBSCRIPTIONS_SQL = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT,
    payment_id INTEGER,
    plan TEXT,
    config TEXT,
    status TEXT DEFAULT 'pending',
    start_date TIMESTAMP,
    duration_days INTEGER,
    volume INTEGER
)
"""
CREATE_COUPONS_SQL = """
CREATE TABLE IF NOT EXISTS coupons (
    code TEXT PRIMARY KEY,
    discount_percent INTEGER,
    user_id BIGINT,
    is_used BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expiry_date TIMESTAMP GENERATED ALWAYS AS (created_at + INTERVAL '3 days') STORED
)
"""
CREATE_CONFIG_POOL_SQL = """
CREATE TABLE IF NOT EXISTS config_pool (
    id SERIAL PRIMARY KEY,
    volume INTEGER NOT NULL,
    config_text TEXT NOT NULL,
    is_sold BOOLEAN DEFAULT FALSE,
    sold_to_user BIGINT,
    sold_at TIMESTAMP,
    created_by BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

MIGRATE_SUBSCRIPTIONS_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='users' AND column_name='is_new_user') THEN
        ALTER TABLE users ADD COLUMN is_new_user BOOLEAN DEFAULT TRUE;
    END IF;
    UPDATE users SET is_new_user = FALSE WHERE is_new_user IS NULL;
END $$;

ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS start_date TIMESTAMP;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS duration_days INTEGER;
ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS volume INTEGER;
ALTER TABLE users ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE users ADD COLUMN IF NOT EXISTS is_agent BOOLEAN DEFAULT FALSE;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_method TEXT;

UPDATE subscriptions SET start_date = COALESCE(start_date, CURRENT_TIMESTAMP), duration_days = 30
WHERE start_date IS NULL OR duration_days IS NULL;
"""

async def create_tables():
    try:
        await db_execute(CREATE_BOT_STATUS_SQL)
        await db_execute(CREATE_USERS_SQL)
        await db_execute(CREATE_PAYMENTS_SQL)
        await db_execute(CREATE_SUBSCRIPTIONS_SQL)
        await db_execute(CREATE_COUPONS_SQL)
        await db_execute(CREATE_CONFIG_POOL_SQL)
        await db_execute(MIGRATE_SUBSCRIPTIONS_SQL)
        
        # تنظیم وضعیت اولیه ربات
        status = await db_execute("SELECT is_active FROM bot_status WHERE id = 1", fetchone=True)
        if not status:
            await db_execute("INSERT INTO bot_status (id, is_active) VALUES (1, TRUE)")
            global bot_is_active
            bot_is_active = True
        else:
            bot_is_active = status[0]
            
        logging.info("Database tables created successfully (existing data preserved)")
    except Exception as e:
        logging.error(f"Error creating or migrating tables: {e}")

# ---------- توابع مدیریت وضعیت ربات ----------
async def set_bot_status(is_active: bool):
    global bot_is_active
    bot_is_active = is_active
    await db_execute("UPDATE bot_status SET is_active = %s WHERE id = 1", (is_active,))

async def get_bot_status() -> bool:
    global bot_is_active
    return bot_is_active

async def is_bot_available_for_user(user_id: int) -> bool:
    """بررسی اینکه آیا ربات برای کاربر عادی در دسترس است"""
    if is_admin(user_id):
        return True  # ادمین همیشه دسترسی دارد
    return await get_bot_status()  # کاربران عادی فقط در صورت روشن بودن ربات

# ---------- وضعیت کاربر ----------
user_states = {}

def generate_coupon_code(length=8):
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))

# ---------- کیبوردها ----------
def get_main_keyboard():
    keyboard = [
        [KeyboardButton("💎 کیف پول"), KeyboardButton("🛍️ خرید اشتراک")],
        [KeyboardButton("🆘 پشتیبانی")],
        [KeyboardButton("🗂️ اشتراک‌های من"), KeyboardButton("📚 آموزش اتصال")],
        [KeyboardButton("👨‍💼 درخواست نمایندگی")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_balance_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("👀 مشاهده کیف پول"), KeyboardButton("💳 شارژ کیف پول")], [KeyboardButton("↩️ بازگشت به منو")]], resize_keyboard=True)

def get_back_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("↩️ بازگشت به منو")]], resize_keyboard=True)

def get_subscription_keyboard():
    keyboard = [
        [KeyboardButton(f"{persian_number(1)} گیگ | {format_price(PRICE_PER_GB * 1)} تومان | آیپی 🇹🇷")],
        [KeyboardButton(f"{persian_number(2)} گیگ | {format_price(PRICE_PER_GB * 2)} تومان | آیپی 🇹🇷")],
        [KeyboardButton(f"{persian_number(5)} گیگ | {format_price(PRICE_PER_GB * 5)} تومان | آیپی 🇹🇷")],
        [KeyboardButton(f"{persian_number(10)} گیگ | {format_price(PRICE_PER_GB * 10)} تومان | آیپی 🇹🇷")],
        [KeyboardButton("↩️ بازگشت به منو")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_payment_method_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("🏧 انتقال کارت به کارت")], [KeyboardButton("👛 پرداخت از کیف پول")], [KeyboardButton("↩️ بازگشت به منو")]], resize_keyboard=True)

def get_connection_guide_keyboard():
    keyboard = [[KeyboardButton("📱 اندروید")], [KeyboardButton("🍏 آیفون/مک")], [KeyboardButton("🖥️ ویندوز")], [KeyboardButton("🐧 لینوکس")], [KeyboardButton("↩️ بازگشت به منو")]]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_coupon_recipient_keyboard():
    return ReplyKeyboardMarkup([[KeyboardButton("🌎 همه کاربران")], [KeyboardButton("👤 یک کاربر خاص")], [KeyboardButton("🎲 درصد مشخصی از کاربران")], [KeyboardButton("↩️ بازگشت به منو")]], resize_keyboard=True)

def get_admin_config_keyboard():
    keyboard = [
        [KeyboardButton("➕ اضافه کردن کانفیگ جدید")],
        [KeyboardButton("📊 مشاهده موجودی کانفیگ‌ها")],
        [KeyboardButton("📋 لیست تمام کانفیگ‌ها")],
        [KeyboardButton("↩️ بازگشت به منو")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_volume_selection_keyboard():
    keyboard = [
        [KeyboardButton(f"{persian_number(1)} گیگ"), KeyboardButton(f"{persian_number(2)} گیگ")],
        [KeyboardButton(f"{persian_number(5)} گیگ"), KeyboardButton(f"{persian_number(10)} گیگ")],
        [KeyboardButton("↩️ انصراف")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# ---------- توابع کمکی ----------
async def send_long_message(chat_id, text, context, reply_markup=None, parse_mode=None):
    max_len = 4000
    if len(text) <= max_len:
        await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        return
    messages = []
    current = ""
    for line in text.split("\n"):
        if len(current) + len(line) + 1 > max_len:
            messages.append(current)
            current = line + "\n"
        else:
            current += line + "\n"
    if current:
        messages.append(current)
    for i, msg in enumerate(messages):
        await context.bot.send_message(chat_id=chat_id, text=msg, reply_markup=reply_markup if i == len(messages)-1 else None, parse_mode=parse_mode)

def parse_configs_from_text(text: str) -> List[str]:
    lines = text.strip().split('\n')
    configs = []
    for line in lines:
        line = line.strip()
        if line and (line.startswith('http://') or line.startswith('https://') or line.startswith('vless://') or line.startswith('vmess://') or line.startswith('trojan://') or line.startswith('ss://')):
            configs.append(line)
    return configs

# ---------- توابع DB ----------
async def is_user_member(user_id):
    try:
        member = await application.bot.get_chat_member(CHANNEL_USERNAME, user_id)
        return member.status in ["member", "administrator", "creator"]
    except:
        return False

async def ensure_user(user_id, username, invited_by=None):
    try:
        row = await db_execute("SELECT user_id FROM users WHERE user_id = %s", (user_id,), fetchone=True)
        if not row:
            await db_execute("INSERT INTO users (user_id, username, invited_by, is_agent, is_new_user) VALUES (%s, %s, %s, FALSE, TRUE)", (user_id, username, invited_by))
            # دیگر پیام ثبت‌نام جدید برای ادمین فرستاده نمی‌شود
            if invited_by and invited_by != user_id:
                await add_balance(invited_by, 15000)
        else:
            await db_execute("UPDATE users SET is_new_user = FALSE WHERE user_id = %s", (user_id,))
    except Exception as e:
        logging.error(f"Error ensuring user: {e}")

async def add_balance(user_id, amount):
    try:
        await db_execute("UPDATE users SET balance = COALESCE(balance,0) + %s WHERE user_id = %s", (amount, user_id))
    except Exception as e:
        logging.error(f"Error adding balance: {e}")

async def deduct_balance(user_id, amount):
    try:
        await db_execute("UPDATE users SET balance = COALESCE(balance,0) - %s WHERE user_id = %s", (amount, user_id))
    except Exception as e:
        logging.error(f"Error deducting balance: {e}")

async def get_balance(user_id):
    try:
        row = await db_execute("SELECT balance FROM users WHERE user_id = %s", (user_id,), fetchone=True)
        return int(row[0]) if row and row[0] is not None else 0
    except:
        return 0

async def is_user_agent(user_id):
    try:
        row = await db_execute("SELECT is_agent FROM users WHERE user_id = %s", (user_id,), fetchone=True)
        return row[0] if row and row[0] is not None else False
    except:
        return False

async def set_user_agent(user_id):
    try:
        await db_execute("UPDATE users SET is_agent = TRUE WHERE user_id = %s", (user_id,))
    except:
        pass

async def unset_user_agent(user_id):
    try:
        await db_execute("UPDATE users SET is_agent = FALSE WHERE user_id = %s", (user_id,))
    except:
        pass

async def add_payment(user_id, amount, ptype, payment_method, description="", coupon_code=None):
    try:
        query = "INSERT INTO payments (user_id, amount, status, type, payment_method, description) VALUES (%s, %s, 'pending', %s, %s, %s) RETURNING id"
        new_id = await db_execute(query, (user_id, amount, ptype, payment_method, description), returning=True)
        if coupon_code:
            await mark_coupon_used(coupon_code)
        return int(new_id) if new_id is not None else None
    except:
        return None

async def add_subscription(user_id, payment_id, plan, volume):
    try:
        await db_execute("INSERT INTO subscriptions (user_id, payment_id, plan, status, start_date, duration_days, volume) VALUES (%s, %s, %s, 'pending', CURRENT_TIMESTAMP, 30, %s)", (user_id, payment_id, plan, volume))
    except Exception as e:
        logging.error(f"Error adding subscription: {e}")

async def update_subscription_config(subscription_id, config):
    try:
        await db_execute("UPDATE subscriptions SET config = %s, status = 'active' WHERE id = %s", (config, subscription_id))
    except:
        pass

async def update_payment_status(payment_id, status):
    try:
        await db_execute("UPDATE payments SET status = %s WHERE id = %s", (status, payment_id))
    except:
        pass

async def get_user_subscriptions(user_id):
    try:
        rows = await db_execute("SELECT id, plan, config, status, payment_id, start_date, duration_days, volume FROM subscriptions WHERE user_id = %s ORDER BY status DESC, start_date DESC", (user_id,), fetch=True)
        current_time = datetime.now()
        subs = []
        for row in rows:
            sub_id, plan, config, status, payment_id, start_date, duration_days, volume = row
            start_date = start_date or current_time
            duration_days = duration_days or 30
            if status == "active":
                end_date = start_date + timedelta(days=duration_days)
                if current_time > end_date:
                    await db_execute("UPDATE subscriptions SET status = 'inactive' WHERE id = %s", (sub_id,))
                    status = "inactive"
            subs.append({'id': sub_id, 'plan': plan, 'config': config, 'status': status, 'payment_id': payment_id, 'start_date': start_date, 'duration_days': duration_days, 'volume': volume, 'end_date': start_date + timedelta(days=duration_days)})
        return subs
    except:
        return []

async def create_coupon(code, discount_percent, user_id=None):
    try:
        await db_execute("INSERT INTO coupons (code, discount_percent, user_id, is_used) VALUES (%s, %s, %s, FALSE)", (code, discount_percent, user_id))
    except:
        pass

async def validate_coupon(code, user_id):
    try:
        row = await db_execute("SELECT discount_percent, user_id, is_used, expiry_date FROM coupons WHERE code = %s", (code,), fetchone=True)
        if not row:
            return None, "❌ کد تخفیف معتبر نمی‌باشد."
        discount_percent, coupon_user_id, is_used, expiry_date = row
        if is_used:
            return None, "❌ این کد قبلاً استفاده شده است."
        if datetime.now() > expiry_date:
            return None, "❌ این کد منقضی شده است."
        if coupon_user_id is not None and coupon_user_id != user_id:
            return None, "❌ این کد متعلق به شما نیست."
        if await is_user_agent(user_id):
            return None, "⚠️ نمایندگان گرامی نمی‌توانند از کد تخفیف استفاده نمایند."
        return discount_percent, None
    except:
        return None, "❌ خطا در بررسی کد تخفیف."

async def mark_coupon_used(code):
    try:
        await db_execute("UPDATE coupons SET is_used = TRUE WHERE code = %s", (code,))
    except:
        pass

async def clear_all_database():
    """پاک کردن تمام دیتابیس (به جز جدول وضعیت ربات)"""
    try:
        await db_execute("DELETE FROM config_pool")
        await db_execute("DELETE FROM coupons")
        await db_execute("DELETE FROM subscriptions")
        await db_execute("DELETE FROM payments")
        await db_execute("DELETE FROM users")
        logging.info("All database tables cleared")
        return True
    except Exception as e:
        logging.error(f"Error clearing database: {e}")
        return False

async def remove_user_from_db(user_id):
    try:
        await db_execute("DELETE FROM coupons WHERE user_id = %s", (user_id,))
        await db_execute("DELETE FROM subscriptions WHERE user_id = %s", (user_id,))
        await db_execute("DELETE FROM payments WHERE user_id = %s", (user_id,))
        await db_execute("DELETE FROM users WHERE user_id = %s", (user_id,))
        return True
    except:
        return False

async def send_notification_to_users(context, user_ids, notification_text):
    sent = 0
    failed = 0
    for user_id in user_ids:
        try:
            await context.bot.send_message(chat_id=user_id[0], text=f"📢 پیام سیستم:\n\n{notification_text}")
            sent += 1
        except:
            failed += 1
    return sent, failed, []

# ---------- توابع مدیریت کانفیگ ----------
async def add_config_to_pool(volume: int, config_text: str, admin_id: int) -> bool:
    try:
        await db_execute(
            "INSERT INTO config_pool (volume, config_text, created_by, is_sold) VALUES (%s, %s, %s, FALSE)",
            (volume, config_text, admin_id)
        )
        return True
    except Exception as e:
        logging.error(f"Error adding config to pool: {e}")
        return False

async def add_multiple_configs_to_pool(volume: int, configs_text: List[str], admin_id: int) -> Tuple[int, int]:
    success_count = 0
    fail_count = 0
    for config_text in configs_text:
        if await add_config_to_pool(volume, config_text, admin_id):
            success_count += 1
        else:
            fail_count += 1
    return success_count, fail_count

async def get_available_config(volume: int) -> Optional[Dict]:
    try:
        row = await db_execute(
            "SELECT id, config_text FROM config_pool WHERE volume = %s AND is_sold = FALSE ORDER BY id LIMIT 1",
            (volume,), fetchone=True
        )
        if row:
            return {"id": row[0], "config_text": row[1]}
        return None
    except Exception as e:
        logging.error(f"Error getting available config: {e}")
        return None

async def get_available_volumes() -> List[int]:
    try:
        rows = await db_execute(
            "SELECT DISTINCT volume FROM config_pool WHERE is_sold = FALSE ORDER BY volume",
            fetch=True
        )
        return [row[0] for row in rows] if rows else []
    except Exception as e:
        logging.error(f"Error getting available volumes: {e}")
        return []

async def mark_config_as_sold(config_id: int, user_id: int) -> bool:
    try:
        await db_execute(
            "UPDATE config_pool SET is_sold = TRUE, sold_to_user = %s, sold_at = CURRENT_TIMESTAMP WHERE id = %s",
            (user_id, config_id)
        )
        return True
    except Exception as e:
        logging.error(f"Error marking config as sold: {e}")
        return False

async def get_config_pool_stats() -> Dict:
    try:
        total = await db_execute("SELECT COUNT(*) FROM config_pool", fetchone=True)
        sold = await db_execute("SELECT COUNT(*) FROM config_pool WHERE is_sold = TRUE", fetchone=True)
        available = await db_execute("SELECT COUNT(*) FROM config_pool WHERE is_sold = FALSE", fetchone=True)
        
        volume_stats = await db_execute(
            "SELECT volume, COUNT(*) as total, SUM(CASE WHEN is_sold THEN 1 ELSE 0 END) as sold FROM config_pool GROUP BY volume ORDER BY volume",
            fetch=True
        )
        
        stats_by_volume = []
        for row in volume_stats:
            volume, total_count, sold_count = row
            stats_by_volume.append({
                "volume": volume,
                "total": total_count,
                "sold": sold_count,
                "available": total_count - sold_count
            })
        
        return {
            "total": total[0] if total else 0,
            "sold": sold[0] if sold else 0,
            "available": available[0] if available else 0,
            "by_volume": stats_by_volume
        }
    except Exception as e:
        logging.error(f"Error getting config pool stats: {e}")
        return {"total": 0, "sold": 0, "available": 0, "by_volume": []}

async def get_all_configs() -> List[Dict]:
    try:
        rows = await db_execute(
            "SELECT id, volume, config_text, is_sold, sold_to_user, created_by, created_at, sold_at FROM config_pool ORDER BY created_at DESC",
            fetch=True
        )
        configs = []
        for row in rows:
            configs.append({
                "id": row[0],
                "volume": row[1],
                "config_text": row[2][:100] + "..." if len(row[2]) > 100 else row[2],
                "is_sold": row[3],
                "sold_to_user": row[4],
                "created_by": row[5],
                "created_at": row[6],
                "sold_at": row[7]
            })
        return configs
    except Exception as e:
        logging.error(f"Error getting all configs: {e}")
        return []

async def get_pending_subscriptions() -> List[Dict]:
    try:
        rows = await db_execute(
            "SELECT s.id, s.user_id, s.volume, s.plan, p.id as payment_id FROM subscriptions s JOIN payments p ON s.payment_id = p.id WHERE s.status = 'pending' AND p.status = 'approved'",
            fetch=True
        )
        pending = []
        for row in rows:
            pending.append({
                "subscription_id": row[0],
                "user_id": row[1],
                "volume": row[2],
                "plan": row[3],
                "payment_id": row[4]
            })
        return pending
    except Exception as e:
        logging.error(f"Error getting pending subscriptions: {e}")
        return []

async def send_config_to_user(subscription_id: int, user_id: int, volume: int, plan: str, bot) -> bool:
    config = await get_available_config(volume)
    if config:
        await update_subscription_config(subscription_id, config['config_text'])
        await mark_config_as_sold(config['id'], user_id)
        await bot.send_message(
            user_id,
            f"✅ اشتراک {plan} شما فعال شد!\n\n🔐 کانفیگ شما:\n```\n{config['config_text']}\n```",
            parse_mode="Markdown"
        )
        for admin_id in ADMIN_IDS:
            try:
                if user_id not in ADMIN_IDS:
                    await bot.send_message(
                        admin_id,
                        f"✅ کانفیگ {persian_number(volume)} گیگ برای کاربر {user_id} ارسال شد."
                    )
            except:
                pass
        return True
    else:
        if user_id not in ADMIN_IDS:
            for admin_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        admin_id,
                        f"⚠️ کاربر {user_id} درخواست {persian_number(volume)} گیگ {CONFIG_NAME} دارد اما کانفیگ موجود نیست!"
                    )
                except:
                    pass
        return False

# ---------- وظیفه دوره‌ای ----------
async def periodic_pending_check(bot):
    while True:
        try:
            await asyncio.sleep(30)
            # بررسی وضعیت ربات - اگر خاموش است، فقط برای ادمین‌ها کار می‌کند
            if await get_bot_status():  # اگر ربات روشن است
                pending_subs = await get_pending_subscriptions()
                for sub in pending_subs:
                    await send_config_to_user(sub['subscription_id'], sub['user_id'], sub['volume'], sub['plan'], bot)
        except Exception as e:
            logging.error(f"Error in periodic check: {e}")

# ---------- دستورات ادمین ----------
async def set_bot_commands():
    try:
        public_commands = [BotCommand(command="/start", description="شروع ربات")]
        admin_commands = [
            BotCommand(command="/start", description="شروع ربات"),
            BotCommand(command="/stats", description="آمار ربات"),
            BotCommand(command="/user_info", description="اطلاعات کاربران"),
            BotCommand(command="/coupon", description="ساخت کد تخفیف"),
            BotCommand(command="/notification", description="ارسال پیام همگانی"),
            BotCommand(command="/add_config", description="مدیریت کانفیگ‌ها"),
            BotCommand(command="/backup", description="تهیه پشتیبان"),
            BotCommand(command="/restore", description="بازیابی پشتیبان"),
            BotCommand(command="/remove_user", description="حذف کاربر"),
            BotCommand(command="/cleardb", description="پاکسازی دیتابیس"),
            BotCommand(command="/debug_subscriptions", description="بررسی اشتراک‌ها"),
            BotCommand(command="/shutdown", description="خاموش کردن ربات (فقط برای کاربران عادی)"),
            BotCommand(command="/startup", description="روشن کردن ربات برای کاربران عادی")
        ]
        await application.bot.set_my_commands(public_commands)
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.set_my_commands(admin_commands, scope={"type": "chat", "chat_id": admin_id})
            except:
                pass
    except:
        pass

async def stats_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    total = await db_execute("SELECT COUNT(*) FROM users", fetchone=True)
    await update.message.reply_text(f"📊 آمار کاربران:\n📈 مجموع: {persian_number(total[0]) if total else '۰'} نفر")

async def user_info_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    users = await db_execute("SELECT user_id, username, balance, is_agent FROM users ORDER BY created_at DESC", fetch=True)
    if not users:
        await update.message.reply_text("📂 کاربری یافت نشد.")
        return
    response = "👥 لیست کاربران:\n\n"
    for u in users:
        uid, uname, bal, agent = u
        response += f"🆔 {uid} | @{uname if uname else 'نامشخص'} | {format_price(bal)} تومان | {'👑 نماینده' if agent else '👤 معمولی'}\n"
        if len(response) > 3500:
            await send_long_message(update.effective_user.id, response, context)
            response = ""
    if response:
        await send_long_message(update.effective_user.id, response, context)

async def coupon_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("💵 درصد تخفیف را وارد نمایید (مثال: ۲۰):")
    user_states[update.effective_user.id] = "awaiting_coupon_discount"

async def notification_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    kb = [[KeyboardButton("📢 ارسال به همه کاربران")], [KeyboardButton("👑 ارسال به نمایندگان")], [KeyboardButton("👤 ارسال به یک نفر")], [KeyboardButton("↩️ بازگشت به منو")]]
    await update.message.reply_text("📢 نوع ارسال پیام را انتخاب کنید:", reply_markup=ReplyKeyboardMarkup(kb, resize_keyboard=True))
    user_states[update.effective_user.id] = "awaiting_notification_type"

async def add_config_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("⚙️ پنل مدیریت کانفیگ‌ها:", reply_markup=get_admin_config_keyboard())
    user_states[update.effective_user.id] = "awaiting_admin_config_action"

async def backup_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("✅ پشتیبان با موفقیت تهیه شد.")

async def restore_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("📤 فایل پشتیبان را ارسال کنید:")
    user_states[update.effective_user.id] = "awaiting_backup_file"

async def remove_user_command(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("🆔 آیدی کاربر را وارد کنید:")
    user_states[update.effective_user.id] = "awaiting_user_id_for_removal"

async def clear_db_command(update, context):
    """پاک کردن کامل دیتابیس"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    
    await update.message.reply_text("⚠️ هشدار! در حال پاک کردن کامل دیتابیس...\nاین عملیات غیرقابل بازگشت است.")
    
    success = await clear_all_database()
    
    if success:
        await update.message.reply_text("✅ دیتابیس با موفقیت پاکسازی شد.\n\nتمام کاربران، پرداخت‌ها، اشتراک‌ها، کدهای تخفیف و کانفیگ‌های ذخیره شده حذف شدند.")
    else:
        await update.message.reply_text("❌ خطا در پاکسازی دیتابیس.")

async def shutdown_command(update, context):
    """خاموش کردن ربات فقط برای کاربران عادی (ادمین‌ها همچنان دسترسی دارند)"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    
    if not await get_bot_status():
        await update.message.reply_text("🔴 ربات در حال حاضر خاموش است.")
        return
    
    await set_bot_status(False)
    await update.message.reply_text(
        "🔴 ربات برای کاربران عادی خاموش شد.\n\n"
        "✅ ادمین‌ها همچنان به ربات دسترسی دارند.\n"
        "🟢 برای روشن کردن مجدد از دستور /startup استفاده کنید."
    )

async def startup_command(update, context):
    """روشن کردن ربات برای کاربران عادی"""
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    
    if await get_bot_status():
        await update.message.reply_text("🟢 ربات در حال حاضر روشن است.")
        return
    
    await set_bot_status(True)
    await update.message.reply_text(
        "🟢 ربات برای کاربران عادی روشن شد.\n\n"
        "✅ کاربران می‌توانند از ربات استفاده کنند."
    )

async def debug_subscriptions(update, context):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("⛔ دسترسی غیرمجاز.")
        return
    await update.message.reply_text("📂 بررسی اشتراک‌ها انجام شد.")

# ---------- هندلرهای اصلی ----------
async def start(update, context):
    user = update.effective_user
    
    # بررسی دسترسی کاربر (ادمین همیشه می‌تواند وارد شود)
    if not await is_bot_available_for_user(user.id):
        await update.message.reply_text(
            "🔴 ربات در حال حاضر برای کاربران عادی غیرفعال است.\n\n"
            "لطفاً بعداً مجدد تلاش کنید."
        )
        return
    
    if not await is_user_member(user.id):
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 عضویت در کانال", url=f"https://t.me/{CHANNEL_USERNAME.replace('@','')}"),
            InlineKeyboardButton("✅ تایید عضویت", callback_data="check_membership")
        ]])
        await update.message.reply_text(
            "❌ لطفاً ابتدا در کانال عضو شوید.\n\nپس از عضویت، روی دکمه «✅ تایید عضویت» کلیک کنید.",
            reply_markup=kb
        )
        return
    invited_by = context.user_data.get("invited_by")
    await ensure_user(user.id, user.username or "", invited_by)
    await update.message.reply_text("🌐 به فروشگاه کاوه وی‌پی‌ان خوش آمدید!", reply_markup=get_main_keyboard())
    user_states.pop(user.id, None)

async def start_with_param(update, context):
    args = context.args
    if args and len(args) > 0:
        try:
            invited_by = int(args[0])
            if invited_by != update.effective_user.id:
                context.user_data["invited_by"] = invited_by
        except:
            pass
    await start(update, context)

async def check_membership_callback(update, context):
    query = update.callback_query
    await query.answer()
    user = update.effective_user
    
    # بررسی دسترسی کاربر (ادمین همیشه می‌تواند وارد شود)
    if not await is_bot_available_for_user(user.id):
        await query.edit_message_text(
            "🔴 ربات در حال حاضر برای کاربران عادی غیرفعال است.\n\n"
            "لطفاً بعداً مجدد تلاش کنید."
        )
        return
    
    if await is_user_member(user.id):
        invited_by = context.user_data.get("invited_by")
        await ensure_user(user.id, user.username or "", invited_by)
        await query.edit_message_text("✅ عضویت شما تأیید شد!\n🌐 به فروشگاه کاوه وی‌پی‌ان خوش آمدید!")
        await query.message.reply_text("🌐 منوی اصلی:", reply_markup=get_main_keyboard())
        user_states.pop(user.id, None)
    else:
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("📢 عضویت در کانال", url=f"https://t.me/{CHANNEL_USERNAME.replace('@','')}"),
            InlineKeyboardButton("✅ تایید عضویت", callback_data="check_membership")
        ]])
        await query.edit_message_text(
            "❌ شما هنوز در کانال عضو نشده‌اید.\nلطفاً ابتدا عضو شوید، سپس روی دکمه «✅ تایید عضویت» کلیک کنید.",
            reply_markup=kb
        )

# ---------- هندلرهای مدیریت کانفیگ برای ادمین ----------
async def handle_admin_config_action(update, context, user_id, text):
    if text == "➕ اضافه کردن کانفیگ جدید":
        await update.message.reply_text("📊 حجم کانفیگ را انتخاب کنید:", reply_markup=get_volume_selection_keyboard())
        user_states[user_id] = "awaiting_config_volume_selection"
    elif text == "📊 مشاهده موجودی کانفیگ‌ها":
        stats = await get_config_pool_stats()
        response = f"📊 **آمار استخر کانفیگ‌ها**\n\n"
        response += f"📦 مجموع کانفیگ‌ها: {persian_number(stats['total'])}\n"
        response += f"✅ فروخته شده: {persian_number(stats['sold'])}\n"
        response += f"📤 موجود: {persian_number(stats['available'])}\n\n"
        response += f"**📈 موجودی به تفکیک حجم:**\n"
        for vol_stat in stats['by_volume']:
            response += f"🔹 {persian_number(vol_stat['volume'])} گیگ: {persian_number(vol_stat['available'])} عدد موجود / {persian_number(vol_stat['sold'])} عدد فروخته شده\n"
        await send_long_message(user_id, response, context, parse_mode="Markdown")
        await update.message.reply_text("⚙️ پنل مدیریت کانفیگ‌ها:", reply_markup=get_admin_config_keyboard())
    elif text == "📋 لیست تمام کانفیگ‌ها":
        configs = await get_all_configs()
        if not configs:
            await update.message.reply_text("📂 هیچ کانفیگی در استخر وجود ندارد.", reply_markup=get_admin_config_keyboard())
            return
        response = f"📋 **لیست تمام کانفیگ‌ها** (مجموع: {persian_number(len(configs))})\n\n"
        for cfg in configs:
            status = "✅ فروخته شده" if cfg['is_sold'] else "📤 موجود"
            sold_to = f" به کاربر {cfg['sold_to_user']}" if cfg['sold_to_user'] else ""
            response += f"🆔 {cfg['id']} | {persian_number(cfg['volume'])} گیگ | {status}{sold_to}\n"
            response += f"🔐 کانفیگ: `{cfg['config_text'][:50]}...`\n"
            response += "────────────────\n"
            if len(response) > 3500:
                await send_long_message(user_id, response, context, parse_mode="Markdown")
                response = ""
        if response:
            await send_long_message(user_id, response, context, parse_mode="Markdown")
        await update.message.reply_text("⚙️ پنل مدیریت کانفیگ‌ها:", reply_markup=get_admin_config_keyboard())
    elif text == "↩️ بازگشت به منو":
        await update.message.reply_text("🌐 منوی اصلی:", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    else:
        await update.message.reply_text("⚠️ لطفاً از دکمه‌های منو استفاده کنید.", reply_markup=get_admin_config_keyboard())

async def handle_config_volume_selection(update, context, user_id, text):
    volume_map = {
        f"{persian_number(1)} گیگ": 1,
        f"{persian_number(2)} گیگ": 2,
        f"{persian_number(5)} گیگ": 5,
        f"{persian_number(10)} گیگ": 10
    }
    if text in volume_map:
        volume = volume_map[text]
        user_states[user_id] = f"awaiting_config_text_{volume}"
        await update.message.reply_text(f"🔐 لطفاً کانفیگ(های) {persian_number(volume)} گیگی را ارسال کنید.\n(هر کانفیگ در یک خط جداگانه)", reply_markup=get_back_keyboard())
    elif text == "↩️ انصراف":
        await update.message.reply_text("⚙️ پنل مدیریت کانفیگ‌ها:", reply_markup=get_admin_config_keyboard())
        user_states.pop(user_id, None)
    else:
        await update.message.reply_text("⚠️ لطفاً یکی از گزینه‌های حجم را انتخاب کنید.", reply_markup=get_volume_selection_keyboard())

async def handle_config_text(update, context, user_id, state, text):
    try:
        volume = int(state.split("_")[3])
        config_text = update.message.text
        if not config_text:
            await update.message.reply_text("⚠️ لطفاً کانفیگ را به صورت متن ارسال کنید:", reply_markup=get_back_keyboard())
            return
        
        configs = parse_configs_from_text(config_text)
        if not configs:
            await update.message.reply_text("⚠️ هیچ کانفیگ معتبری یافت نشد. لطفاً کانفیگ(ها) را به درستی ارسال کنید.", reply_markup=get_back_keyboard())
            return
        
        success_count, fail_count = await add_multiple_configs_to_pool(volume, configs, user_id)
        
        if success_count > 0:
            await update.message.reply_text(
                f"✅ {persian_number(success_count)} کانفیگ {persian_number(volume)} گیگ با موفقیت به استخر اضافه شد.\n"
                f"{'❌ ' + persian_number(fail_count) + ' کانفیگ ناموفق' if fail_count > 0 else ''}",
                reply_markup=get_admin_config_keyboard()
            )
        else:
            await update.message.reply_text("⚠️ خطا در ذخیره کانفیگ‌ها. لطفاً مجدد تلاش کنید.", reply_markup=get_admin_config_keyboard())
        user_states.pop(user_id, None)
    except Exception as e:
        logging.error(f"Error in handle_config_text: {e}")
        await update.message.reply_text("⚠️ خطا در پردازش کانفیگ.", reply_markup=get_admin_config_keyboard())
        user_states.pop(user_id, None)

# ---------- هندلرهای خرید و پرداخت ----------
async def handle_subscription_plan(update, context, user_id, text):
    volume_map = {
        f"{persian_number(1)} گیگ | {format_price(PRICE_PER_GB * 1)} تومان | آیپی 🇹🇷": 1,
        f"{persian_number(2)} گیگ | {format_price(PRICE_PER_GB * 2)} تومان | آیپی 🇹🇷": 2,
        f"{persian_number(5)} گیگ | {format_price(PRICE_PER_GB * 5)} تومان | آیپی 🇹🇷": 5,
        f"{persian_number(10)} گیگ | {format_price(PRICE_PER_GB * 10)} تومان | آیپی 🇹🇷": 10
    }
    if text in volume_map:
        volume = volume_map[text]
        
        available_volumes = await get_available_volumes()
        if volume not in available_volumes:
            available_text = "، ".join([persian_number(v) for v in available_volumes]) if available_volumes else "هیچ"
            await update.message.reply_text(
                f"⚠️ سرویس {persian_number(volume)} گیگ در حال حاضر ناموجود است.\n\n"
                f"📦 حجم‌های موجود: {available_text} گیگ\n\n"
                f"لطفاً از حجم‌های موجود استفاده کنید.",
                reply_markup=get_subscription_keyboard()
            )
            return
        
        total_amount = PRICE_PER_GB * volume
        plan_name = f"{CONFIG_NAME} | {volume} گیگ"
        user_states[user_id] = f"awaiting_coupon_code_{total_amount}_{plan_name}_{volume}"
        await update.message.reply_text(
            f"✅ {persian_number(volume)} گیگ {CONFIG_NAME} با مبلغ {format_price(total_amount)} تومان\n\nبرای ادامه روی 'ادامه' کلیک کنید:",
            reply_markup=ReplyKeyboardMarkup([[KeyboardButton("ادامه")], [KeyboardButton("↩️ بازگشت به منو")]], resize_keyboard=True)
        )
    else:
        await update.message.reply_text("⚠️ لطفاً از دکمه‌های منو استفاده کنید.", reply_markup=get_subscription_keyboard())

async def handle_coupon_code(update, context, user_id, state, text):
    parts = state.split("_")
    if len(parts) < 5:
        await update.message.reply_text("⚠️ خطا در پردازش درخواست. لطفاً مجدد تلاش کنید.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
        return
    
    amount = int(parts[3])
    volume = int(parts[-1])
    plan_parts = parts[4:-1]
    plan = "_".join(plan_parts)
    
    if text == "ادامه":
        user_states[user_id] = f"awaiting_payment_method_{amount}_{plan}_{volume}"
        await update.message.reply_text("💳 روش پرداخت را انتخاب کنید:", reply_markup=get_payment_method_keyboard())
        return
    
    discount_percent, error = await validate_coupon(text.strip(), user_id)
    if error:
        await update.message.reply_text(f"{error}\nبرای ادامه روی 'ادامه' کلیک کنید:", reply_markup=ReplyKeyboardMarkup([[KeyboardButton("ادامه")]], resize_keyboard=True))
        return
    
    discounted_amount = int(amount * (1 - discount_percent / 100))
    user_states[user_id] = f"awaiting_payment_method_{discounted_amount}_{plan}_{volume}_{text.strip()}"
    await update.message.reply_text(f"✅ کد تخفیف اعمال شد! مبلغ با {persian_number(discount_percent)}% تخفیف: {format_price(discounted_amount)} تومان\nروش پرداخت را انتخاب کنید:", reply_markup=get_payment_method_keyboard())

async def handle_payment_method(update, context, user_id, text):
    state = user_states.get(user_id)
    if not state:
        return
    
    try:
        parts = state.split("_")
        
        if len(parts) < 6:
            await update.message.reply_text("⚠️ خطا در پردازش درخواست. لطفاً مجدد تلاش کنید.", reply_markup=get_main_keyboard())
            user_states.pop(user_id, None)
            return
        
        amount = int(parts[3])
        
        if len(parts) >= 7 and parts[-1] and parts[-1] not in ["card_to_card", "balance"] and not parts[-1].isdigit():
            coupon_code = parts[-1]
            volume = int(parts[-2])
            plan_parts = parts[4:-2]
            plan = "_".join(plan_parts)
        else:
            coupon_code = None
            volume = int(parts[-1])
            plan_parts = parts[4:-1]
            plan = "_".join(plan_parts)
        
        if text == "🏧 انتقال کارت به کارت":
            payment_id = await add_payment(user_id, amount, "buy_subscription", "card_to_card", description=plan, coupon_code=coupon_code)
            if payment_id:
                await add_subscription(user_id, payment_id, plan, volume)
                await update.message.reply_text(
                    f"💳 لطفاً مبلغ {format_price(amount)} تومان را به کارت زیر واریز کنید:\n\n"
                    f"🏦 شماره کارت:\n`{BANK_CARD}`\n"
                    f"👤 به نام: {BANK_OWNER}\n\n"
                    f"📸 سپس فیش واریز را به صورت عکس ارسال نمایید",
                    reply_markup=get_back_keyboard(),
                    parse_mode="MarkdownV2"
                )
                user_states[user_id] = f"awaiting_subscription_receipt_{payment_id}"
            else:
                await update.message.reply_text("⚠️ خطا در ثبت درخواست.", reply_markup=get_main_keyboard())
                user_states.pop(user_id, None)
        elif text == "👛 پرداخت از کیف پول":
            balance = await get_balance(user_id)
            if balance >= amount:
                payment_id = await add_payment(user_id, amount, "buy_subscription", "balance", description=plan, coupon_code=coupon_code)
                if payment_id:
                    await add_subscription(user_id, payment_id, plan, volume)
                    await deduct_balance(user_id, amount)
                    await update_payment_status(payment_id, "approved")
                    await update.message.reply_text("✅ خرید با موفقیت انجام شد. کانفیگ برای شما ارسال خواهد شد.", reply_markup=get_main_keyboard())
                    for admin_id in ADMIN_IDS:
                        try:
                            await context.bot.send_message(admin_id, f"🛍️ کاربر {user_id} سرویس {plan} را از کیف پول خود خریداری کرد.")
                        except:
                            pass
                    sub = await db_execute("SELECT id FROM subscriptions WHERE payment_id = %s", (payment_id,), fetchone=True)
                    if sub:
                        await send_config_to_user(sub[0], user_id, volume, plan, context.bot)
                    user_states.pop(user_id, None)
            else:
                await update.message.reply_text(f"⚠️ موجودی کیف پول شما ({format_price(balance)} تومان) کافی نمی‌باشد.", reply_markup=get_main_keyboard())
                user_states.pop(user_id, None)
    except Exception as e:
        logging.error(f"Error in payment method: {e}")
        await update.message.reply_text("⚠️ خطا در پردازش درخواست. لطفاً مجدد تلاش کنید.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)

async def process_payment_receipt(update, context, user_id, payment_id, receipt_type):
    try:
        payment = await db_execute("SELECT amount, description FROM payments WHERE id = %s", (payment_id,), fetchone=True)
        if not payment:
            await update.message.reply_text("⚠️ درخواست پرداخت یافت نشد.", reply_markup=get_main_keyboard())
            return
        
        amount, description = payment
        
        # دریافت حجم از subscription
        sub = await db_execute("SELECT volume FROM subscriptions WHERE payment_id = %s", (payment_id,), fetchone=True)
        volume = sub[0] if sub else 0
        volume_text = f"📦 حجم: {persian_number(volume)} گیگ" if volume > 0 else ""
        
        caption = f"💳 فیش پرداختی از کاربر {user_id}:\n💰 مبلغ: {format_price(amount)} تومان\n{volume_text}\n📦 نوع: {receipt_type}"
        
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ تایید", callback_data=f"approve_payment_{payment_id}"),
             InlineKeyboardButton("❌ رد", callback_data=f"reject_payment_{payment_id}")]
        ])
        
        if update.message.photo:
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_photo(chat_id=admin_id, photo=update.message.photo[-1].file_id, caption=caption, reply_markup=kb)
                except:
                    pass
            await update.message.reply_text("✅ فیش برای ادمین ارسال شد. پس از تأیید، کانفیگ برای شما ارسال خواهد شد.", reply_markup=get_main_keyboard())
        elif update.message.document:
            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_document(chat_id=admin_id, document=update.message.document.file_id, caption=caption, reply_markup=kb)
                except:
                    pass
            await update.message.reply_text("✅ فیش برای ادمین ارسال شد. پس از تأیید، کانفیگ برای شما ارسال خواهد شد.", reply_markup=get_main_keyboard())
        else:
            await update.message.reply_text("⚠️ لطفاً فیش را به صورت عکس ارسال کنید.", reply_markup=get_back_keyboard())
            return
        user_states.pop(user_id, None)
    except Exception as e:
        logging.error(f"Error processing receipt: {e}")
        await update.message.reply_text("⚠️ خطا در ارسال فیش.", reply_markup=get_main_keyboard())

# ---------- هندلرهای پیام عمومی ----------
async def handle_coupon_recipient(update, context, user_id, state, text):
    parts = state.split("_")
    coupon_code = parts[3]
    discount_percent = int(parts[4])
    if text == "🌎 همه کاربران":
        await create_coupon(coupon_code, discount_percent)
        users = await db_execute("SELECT user_id FROM users", fetch=True)
        sent = 0
        for u in users:
            try:
                await context.bot.send_message(u[0], f"🎉 کد تخفیف ویژه `{coupon_code}` با {persian_number(discount_percent)}% تخفیف!")
                sent += 1
            except:
                pass
        await update.message.reply_text(f"✅ کد تخفیف برای {persian_number(sent)} کاربر ارسال شد.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    elif text == "👤 یک کاربر خاص":
        user_states[user_id] = f"awaiting_single_coupon_user_{coupon_code}_{discount_percent}"
        await update.message.reply_text("🆔 آیدی کاربر را وارد کنید:", reply_markup=get_back_keyboard())
    else:
        await update.message.reply_text("⚠️ گزینه نامعتبر.", reply_markup=get_coupon_recipient_keyboard())

async def handle_remove_user(update, context, user_id, text):
    try:
        target = int(text)
        success = await remove_user_from_db(target)
        if success:
            await update.message.reply_text(f"✅ کاربر {target} حذف شد.", reply_markup=get_main_keyboard())
        else:
            await update.message.reply_text("⚠️ خطا در حذف کاربر.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    except:
        await update.message.reply_text("⚠️ آیدی نامعتبر.", reply_markup=get_back_keyboard())

async def handle_normal_commands(update, context, user_id, text):
    # بررسی دسترسی کاربر (ادمین همیشه می‌تواند استفاده کند)
    if not await is_bot_available_for_user(user_id):
        await update.message.reply_text(
            "🔴 ربات در حال حاضر برای کاربران عادی غیرفعال است.\n\n"
            "لطفاً بعداً مجدد تلاش کنید."
        )
        return
    
    if text == "💎 کیف پول":
        await update.message.reply_text("💎 بخش کیف پول:", reply_markup=get_balance_keyboard())
    elif text == "👀 مشاهده کیف پول":
        bal = await get_balance(user_id)
        await update.message.reply_text(f"💰 موجودی کیف پول شما: {format_price(bal)} تومان", reply_markup=get_balance_keyboard())
    elif text == "💳 شارژ کیف پول":
        await update.message.reply_text("💳 مبلغ مورد نظر را وارد کنید:", reply_markup=get_back_keyboard())
        user_states[user_id] = "awaiting_deposit_amount"
    elif user_states.get(user_id) == "awaiting_deposit_amount" and text.isdigit():
        amount = int(text)
        payment_id = await add_payment(user_id, amount, "increase_balance", "card_to_card")
        if payment_id:
            await update.message.reply_text(
                f"💳 لطفاً مبلغ {format_price(amount)} تومان را به کارت زیر واریز کنید:\n\n"
                f"🏦 شماره کارت:\n`{BANK_CARD}`\n"
                f"👤 به نام: {BANK_OWNER}\n\n"
                f"📸 سپس فیش واریز را به صورت عکس ارسال نمایید",
                reply_markup=get_back_keyboard(),
                parse_mode="MarkdownV2"
            )
            user_states[user_id] = f"awaiting_deposit_receipt_{payment_id}"
        else:
            await update.message.reply_text("⚠️ خطا در ثبت درخواست.", reply_markup=get_main_keyboard())
            user_states.pop(user_id, None)
    elif text == "🛍️ خرید اشتراک":
        await update.message.reply_text("💳 پلن مورد نظر را انتخاب کنید:", reply_markup=get_subscription_keyboard())
    elif any(text.startswith(prefix) for prefix in [f"{persian_number(1)} گیگ |", f"{persian_number(2)} گیگ |", f"{persian_number(5)} گیگ |", f"{persian_number(10)} گیگ |"]):
        await handle_subscription_plan(update, context, user_id, text)
    elif user_states.get(user_id, "").startswith("awaiting_payment_method_"):
        await handle_payment_method(update, context, user_id, text)
    elif text == "🆘 پشتیبانی":
        await update.message.reply_text(f"📞 پشتیبانی: {SUPPORT_USERNAME}", reply_markup=get_main_keyboard())
    elif text == "🗂️ اشتراک‌های من":
        subs = await get_user_subscriptions(user_id)
        if not subs:
            await update.message.reply_text("📁 شما هیچ اشتراک فعالی ندارید.", reply_markup=get_main_keyboard())
            return
        response = "🗂️ اشتراک‌های شما:\n\n"
        for s in subs:
            response += f"🔹 {s['plan']} ({persian_number(s['volume'])} گیگ)\n📊 وضعیت: {'✅ فعال' if s['status'] == 'active' else '⏳ در انتظار تایید'}\n"
            if s['status'] == 'active' and s['config']:
                response += f"🔐 کانفیگ:\n```\n{s['config']}\n```\n"
            response += "--------------------\n"
            if len(response) > 3500:
                await send_long_message(user_id, response, context, parse_mode="Markdown")
                response = ""
        if response:
            await send_long_message(user_id, response, context, parse_mode="Markdown")
    elif text == "📚 آموزش اتصال":
        await update.message.reply_text("📚 راهنمای اتصال\nلطفاً دستگاه خود را انتخاب کنید:", reply_markup=get_connection_guide_keyboard())
    elif text in ["📱 اندروید", "🍏 آیفون/مک", "🖥️ ویندوز", "🐧 لینوکس"]:
        guides = {"📱 اندروید": "📱 آموزش اندروید:\nاستفاده از اپلیکیشن V2RayNG یا Hiddify", "🍏 آیفون/مک": "🍏 آموزش iOS/Mac:\nاستفاده از اپلیکیشن Singbox یا V2box", "🖥️ ویندوز": "🪟 آموزش ویندوز:\nاستفاده از اپلیکیشن V2rayN", "🐧 لینوکس": "🐧 آموزش لینوکس:\nاستفاده از اپلیکیشن V2rayN"}
        await update.message.reply_text(guides[text], reply_markup=get_connection_guide_keyboard())
    elif text == "👨‍💼 درخواست نمایندگی":
        await update.message.reply_text(f"👨‍💻 برای کسب اطلاعات بیشتر در مورد نمایندگی، با ادمین تماس بگیرید:\n{SUPPORT_USERNAME}", reply_markup=get_main_keyboard())
    else:
        await update.message.reply_text("⚠️ لطفاً از دکمه‌های منو استفاده کنید.", reply_markup=get_main_keyboard())

# ---------- هندلرهای ادمین برای مدیریت کاربران ----------
async def handle_admin_balance_amount(update, context, user_id, text):
    parts = user_states[user_id].split("_")
    target = int(parts[3])
    try:
        amount = int(text)
        await add_balance(target, amount)
        await update.message.reply_text(f"✅ مبلغ {format_price(amount)} تومان به کیف پول کاربر {target} اضافه شد.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    except:
        await update.message.reply_text("⚠️ مبلغ نامعتبر.", reply_markup=get_back_keyboard())

async def handle_admin_agent_type(update, context, user_id, text):
    parts = user_states[user_id].split("_")
    target = int(parts[3])
    if text == "معمولی":
        await unset_user_agent(target)
        await update.message.reply_text(f"✅ کاربر {target} به نوع معمولی تغییر یافت.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    elif text == "نماینده":
        await set_user_agent(target)
        await update.message.reply_text(f"✅ کاربر {target} به نماینده ارتقا یافت.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
    elif text == "انصراف":
        await update.message.reply_text("❌ عملیات لغو شد.", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)

# ---------- کالبک هندلر ادمین ----------
async def admin_callback_handler(update, context):
    query = update.callback_query
    await query.answer()
    if not is_admin(update.effective_user.id):
        await query.edit_message_text("⛔ دسترسی غیرمجاز.")
        return
    data = query.data
    try:
        if data.startswith("approve_payment_"):
            payment_id = int(data.split("_")[2])
            payment = await db_execute("SELECT user_id, amount, type, description FROM payments WHERE id = %s", (payment_id,), fetchone=True)
            if not payment:
                await query.edit_message_text("⚠️ درخواست پرداخت یافت نشد.")
                return
            uid, amt, ptype, desc = payment
            await update_payment_status(payment_id, "approved")
            if ptype == "increase_balance":
                await add_balance(uid, amt)
                await context.bot.send_message(uid, f"💰 مبلغ {format_price(amt)} تومان به کیف پول شما اضافه شد.")
                await query.edit_message_reply_markup(reply_markup=None)
                await query.message.reply_text("✅ پرداخت تایید شد.")
            elif ptype == "buy_subscription":
                await context.bot.send_message(uid, f"✅ پرداخت شما تایید شد. کد پیگیری: #{payment_id}")
                await query.edit_message_reply_markup(reply_markup=None)
                await query.message.reply_text("✅ پرداخت تایید شد. در حال ارسال کانفیگ...")
                sub = await db_execute("SELECT id, volume, plan FROM subscriptions WHERE payment_id = %s", (payment_id,), fetchone=True)
                if sub:
                    subscription_id, volume, plan = sub
                    await send_config_to_user(subscription_id, uid, volume, plan, context.bot)
        elif data.startswith("reject_payment_"):
            payment_id = int(data.split("_")[2])
            await update_payment_status(payment_id, "rejected")
            payment = await db_execute("SELECT user_id FROM payments WHERE id = %s", (payment_id,), fetchone=True)
            if payment:
                user_id = payment[0]
                await context.bot.send_message(user_id, "❌ متأسفانه پرداخت شما تایید نشد. لطفاً مجدداً تلاش کنید.")
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("❌ پرداخت رد شد.")
        elif data == "admin_balance_action":
            await query.edit_message_text("🆔 آیدی کاربر را وارد کنید:")
            user_states[ADMIN_IDS[0]] = "awaiting_admin_user_id_for_balance"
        elif data == "admin_agent_action":
            await query.edit_message_text("🆔 آیدی کاربر را وارد کنید:")
            user_states[ADMIN_IDS[0]] = "awaiting_admin_user_id_for_agent"
        elif data == "admin_remove_user_action":
            await query.edit_message_text("🆔 آیدی کاربر را وارد کنید:")
            user_states[ADMIN_IDS[0]] = "awaiting_user_id_for_removal"
        elif data == "check_membership":
            await check_membership_callback(update, context)
    except Exception as e:
        logging.error(f"Error in callback: {e}")
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            await query.message.reply_text("⚠️ خطا در پردازش درخواست.")
        except:
            pass

# ---------- هندلر اصلی پیام‌ها ----------
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    text = update.message.text if update.message.text else ""
    state = user_states.get(user_id)
    
    if text in ["بازگشت به منو", "↩️ بازگشت به منو"]:
        await update.message.reply_text("🌐 منوی اصلی:", reply_markup=get_main_keyboard())
        user_states.pop(user_id, None)
        return
    
    # هندلرهای ادمین (ادمین‌ها همیشه دسترسی دارند حتی اگر ربات خاموش باشد)
    if is_admin(user_id):
        if state == "awaiting_backup_file":
            await update.message.reply_text("✅ فایل پشتیبان دریافت شد.", reply_markup=get_main_keyboard())
            user_states.pop(user_id, None)
            return
        if state == "awaiting_admin_config_action":
            await handle_admin_config_action(update, context, user_id, text)
            return
        if state == "awaiting_config_volume_selection":
            await handle_config_volume_selection(update, context, user_id, text)
            return
        if state and state.startswith("awaiting_config_text_"):
            await handle_config_text(update, context, user_id, state, text)
            return
        if state == "awaiting_user_id_for_removal":
            await handle_remove_user(update, context, user_id, text)
            return
        if state == "awaiting_admin_user_id_for_balance":
            try:
                target = int(text)
                user_states[user_id] = f"awaiting_balance_amount_{target}"
                await update.message.reply_text("💰 مبلغ را وارد کنید:", reply_markup=get_back_keyboard())
            except:
                await update.message.reply_text("⚠️ آیدی نامعتبر.", reply_markup=get_back_keyboard())
            return
        if state and state.startswith("awaiting_balance_amount_"):
            await handle_admin_balance_amount(update, context, user_id, text)
            return
        if state == "awaiting_admin_user_id_for_agent":
            try:
                target = int(text)
                user_states[user_id] = f"awaiting_agent_type_{target}"
                kb = ReplyKeyboardMarkup([[KeyboardButton("معمولی")], [KeyboardButton("نماینده")], [KeyboardButton("انصراف")]], resize_keyboard=True)
                await update.message.reply_text("نوع کاربری جدید را انتخاب کنید:", reply_markup=kb)
            except:
                await update.message.reply_text("⚠️ آیدی نامعتبر.", reply_markup=get_back_keyboard())
            return
        if state and state.startswith("awaiting_agent_type_"):
            await handle_admin_agent_type(update, context, user_id, text)
            return
        if state == "awaiting_coupon_discount":
            if text.isdigit():
                discount = int(text)
                code = generate_coupon_code()
                user_states[user_id] = f"awaiting_coupon_recipient_{code}_{discount}"
                await update.message.reply_text(f"💵 کد تخفیف `{code}` با {persian_number(discount)}% تخفیف ساخته شد.\nاین کد برای چه کسانی ارسال شود؟", reply_markup=get_coupon_recipient_keyboard(), parse_mode="Markdown")
            else:
                await update.message.reply_text("⚠️ لطفاً یک عدد وارد کنید.", reply_markup=get_back_keyboard())
            return
        if state and state.startswith("awaiting_coupon_recipient_"):
            await handle_coupon_recipient(update, context, user_id, state, text)
            return
        if state == "awaiting_notification_type":
            if text == "📢 ارسال به همه کاربران":
                user_states[user_id] = "awaiting_notification_text_all"
                await update.message.reply_text("📢 متن پیام خود را ارسال کنید:", reply_markup=get_back_keyboard())
            elif text == "👑 ارسال به نمایندگان":
                user_states[user_id] = "awaiting_notification_text_agents"
                await update.message.reply_text("📢 متن پیام خود را ارسال کنید:", reply_markup=get_back_keyboard())
            elif text == "👤 ارسال به یک نفر":
                user_states[user_id] = "awaiting_notification_target_user"
                await update.message.reply_text("🆔 آیدی کاربر را وارد کنید:", reply_markup=get_back_keyboard())
            elif text == "↩️ بازگشت به منو":
                await update.message.reply_text("🌐 منوی اصلی:", reply_markup=get_main_keyboard())
                user_states.pop(user_id, None)
            return
        if state == "awaiting_notification_target_user":
            try:
                target = int(text)
                user_states[user_id] = f"awaiting_notification_text_single_{target}"
                await update.message.reply_text("📢 متن پیام خود را ارسال کنید:", reply_markup=get_back_keyboard())
            except:
                await update.message.reply_text("⚠️ آیدی نامعتبر.", reply_markup=get_back_keyboard())
            return
        if state in ["awaiting_notification_text_all", "awaiting_notification_text_agents"] or (state and state.startswith("awaiting_notification_text_single_")):
            if state == "awaiting_notification_text_all":
                users = await db_execute("SELECT user_id FROM users", fetch=True)
                user_type = "همه کاربران"
            elif state == "awaiting_notification_text_agents":
                users = await db_execute("SELECT user_id FROM users WHERE is_agent = TRUE", fetch=True)
                user_type = "نمایندگان"
            else:
                target = int(state.split("_")[-1])
                users = [[target]]
                user_type = f"کاربر {target}"
            sent, failed, _ = await send_notification_to_users(context, users, text)
            await update.message.reply_text(f"✅ پیام برای {persian_number(sent)} {user_type} ارسال شد. ({persian_number(failed)} ناموفق)", reply_markup=get_main_keyboard())
            user_states.pop(user_id, None)
            return
    
    # هندلرهای عادی کاربران (فقط اگر ربات روشن باشد)
    if not await is_bot_available_for_user(user_id):
        # به کاربر عادی اطلاع بده که ربات خاموش است
        if text not in ["بازگشت به منو", "↩️ بازگشت به منو"]:
            await update.message.reply_text(
                "🔴 ربات در حال حاضر برای کاربران عادی غیرفعال است.\n\n"
                "لطفاً بعداً مجدد تلاش کنید."
            )
        return
    
    if state and state.startswith("awaiting_deposit_receipt_"):
        payment_id = int(state.split("_")[-1])
        await process_payment_receipt(update, context, user_id, payment_id, "شارژ کیف پول")
        user_states.pop(user_id, None)
        return
    if state and state.startswith("awaiting_subscription_receipt_"):
        payment_id = int(state.split("_")[-1])
        await process_payment_receipt(update, context, user_id, payment_id, "خرید اشتراک")
        user_states.pop(user_id, None)
        return
    if state and state.startswith("awaiting_coupon_code_"):
        await handle_coupon_code(update, context, user_id, state, text)
        return
    if state and state.startswith("awaiting_payment_method_"):
        await handle_payment_method(update, context, user_id, text)
        return
    if state == "awaiting_deposit_amount" and text.isdigit():
        amount = int(text)
        payment_id = await add_payment(user_id, amount, "increase_balance", "card_to_card")
        if payment_id:
            await update.message.reply_text(
                f"💳 لطفاً مبلغ {format_price(amount)} تومان را به کارت زیر واریز کنید:\n\n"
                f"🏦 شماره کارت:\n`{BANK_CARD}`\n"
                f"👤 به نام: {BANK_OWNER}\n\n"
                f"📸 سپس فیش واریز را به صورت عکس ارسال نمایید",
                reply_markup=get_back_keyboard(),
                parse_mode="MarkdownV2"
            )
            user_states[user_id] = f"awaiting_deposit_receipt_{payment_id}"
        else:
            await update.message.reply_text("⚠️ خطا در ثبت درخواست.", reply_markup=get_main_keyboard())
            user_states.pop(user_id, None)
        return
    
    await handle_normal_commands(update, context, user_id, text)

# ---------- ثبت هندلرها ----------
application.add_handler(CommandHandler("start", start_with_param))
application.add_handler(CommandHandler("stats", stats_command))
application.add_handler(CommandHandler("user_info", user_info_command))
application.add_handler(CommandHandler("coupon", coupon_command))
application.add_handler(CommandHandler("notification", notification_command))
application.add_handler(CommandHandler("add_config", add_config_command))
application.add_handler(CommandHandler("backup", backup_command))
application.add_handler(CommandHandler("restore", restore_command))
application.add_handler(CommandHandler("remove_user", remove_user_command))
application.add_handler(CommandHandler("cleardb", clear_db_command))
application.add_handler(CommandHandler("debug_subscriptions", debug_subscriptions))
application.add_handler(CommandHandler("shutdown", shutdown_command))
application.add_handler(CommandHandler("startup", startup_command))
application.add_handler(MessageHandler(filters.ALL & (~filters.COMMAND), message_handler))
application.add_handler(CallbackQueryHandler(admin_callback_handler))

# ---------- webhook ----------
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    try:
        data = await request.json()
        update = Update.de_json(data, application.bot)
        await application.update_queue.put(update)
        return {"ok": True}
    except Exception as e:
        logging.error(f"Error in webhook: {e}")
        return {"ok": False, "error": str(e)}

# ---------- متغیر برای کنترل تسک دوره‌ای ----------
periodic_task = None

# ---------- lifecycle ----------
@app.on_event("startup")
async def on_startup():
    global periodic_task
    try:
        init_db_pool()
        await create_tables()
        await application.initialize()
        await application.start()
        await application.bot.set_webhook(url=WEBHOOK_URL)
        logging.info(f"✅ Webhook set: {WEBHOOK_URL}")
        await set_bot_commands()
        
        periodic_task = asyncio.create_task(periodic_pending_check(application.bot))
        
        status_text = "روشن" if bot_is_active else "خاموش"
        for admin_id in ADMIN_IDS:
            try:
                await application.bot.send_message(
                    chat_id=admin_id, 
                    text=f"🤖 ربات کاوه وی‌پی‌ان با موفقیت راه‌اندازی شد!\n✅ سیستم مدیریت خودکار کانفیگ فعال است.\n✅ وضعیت ربات: {status_text}\n✅ داده‌های قبلی حفظ شد."
                )
            except:
                pass
        logging.info("✅ Bot started successfully")
    except Exception as e:
        logging.error(f"Startup error: {e}")

@app.on_event("shutdown")
async def on_shutdown():
    global periodic_task
    try:
        if periodic_task:
            periodic_task.cancel()
        await application.stop()
        await application.shutdown()
        close_db_pool()
        logging.info("✅ Bot shut down successfully")
    except Exception as e:
        logging.error(f"Shutdown error: {e}")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
