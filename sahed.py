#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
📡 SHAHED 110 NEWS V11.0 (Custom Bulletin & Deep Context AI)
============================================================
"""

import asyncio
import logging
import sqlite3
import requests
import re
import time
import os
import html
import hashlib
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse
import threading
from rubpy import Client

# ============================================================
# ⚙️ تنظیمات اصلی
# ============================================================
SESSION_NAME = "shahed_news"
# امنیت V12 (بند ۱۰): مقدار پیش‌فرض دقیقاً همان مقدار قبلی است تا هیچ رفتاری تغییر نکند؛
# توصیه می‌شود EITAA_TOKEN/EITAA_CHAT_ID از طریق Environment Variable ست شوند.
EITAA_TOKEN = os.environ.get("SHAHED_EITAA_TOKEN", "bot274957:6b06ce02-8940-4947-977b-f64a63896e30")
EITAA_CHAT_ID = os.environ.get("SHAHED_EITAA_CHAT_ID", "11247431")

POLL_INTERVAL = 3
SEND_INTERVAL = 60  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "shahed_news_mobile.db")
MEDIA_DIR = os.path.join(BASE_DIR, "temp_media")
os.makedirs(MEDIA_DIR, exist_ok=True)

# ------------------------------------------------------------------
# 🎬 Media Engine — ثابت‌های پیکربندی (فقط مرتبط با Media؛ سایر تنظیمات دست‌نخورده است)
# ------------------------------------------------------------------
MEDIA_DOWNLOAD_RETRIES_PER_PASS = 3      # تعداد تلاش Download در هر بار پردازش صف
MEDIA_DOWNLOAD_TIMEOUT_SEC = 35.0        # Timeout هر تلاش Download
MEDIA_DOWNLOAD_BACKOFF_BASE = 2.0        # پایه Exponential Backoff برای Download
MEDIA_UPLOAD_RETRIES_PER_PASS = 3        # تعداد تلاش Upload در هر بار پردازش صف
MEDIA_UPLOAD_BACKOFF_BASE = 2.0          # پایه Exponential Backoff برای Upload
MEDIA_MAX_TOTAL_ATTEMPTS = 5             # سقف کلی تلاش‌ها (شامل تلاش‌های بعد از Restart) پیش از شکست نهایی Job
MEDIA_JOB_RETENTION_HOURS = 72           # مدت نگهداری رکورد Jobهای کاملاً تمام‌شده در دیتابیس

LATEST_CHANNEL_MESSAGES = {}
SYSTEM_STATUS = {
    "gemini": "آماده به کار",
    "active_token": "در انتظار اولین درخواست...",
}

news_queue = asyncio.Queue()
bot_loop = None  # برای اجرای تسک‌های غیرهمزمان از داخل پنل وب
ACTIVE_MONITORS = {} # {source_id: asyncio.Task}
RUBY_CLIENT = None # ذخیره کلاینت روبیکا برای دسترسی در هندلرهای وب

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("SHAHED_PANEL")

# ============================================================
# 🗄️ دیتابیس و مدیریت پیشرفته توکن‌ها
# ============================================================
def init_db():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("CREATE TABLE IF NOT EXISTS source_state (source_name TEXT PRIMARY KEY, last_message_id INTEGER)")
    # اضافه شدن timestamp برای کوئری‌های زمانی دقیق (ذخیره به وقت جهانی UTC)
    conn.execute("CREATE TABLE IF NOT EXISTS hourly_news_buffer (id INTEGER PRIMARY KEY AUTOINCREMENT, source_name TEXT, text TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("""CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, 
                    date TEXT, 
                    total_news INT DEFAULT 0, 
                    gemini_used INT DEFAULT 0, 
                    backup_used INT DEFAULT 0)""")
    conn.execute("CREATE TABLE IF NOT EXISTS ai_tokens (id INTEGER PRIMARY KEY AUTOINCREMENT, token TEXT UNIQUE)")
    conn.execute("CREATE TABLE IF NOT EXISTS sources (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, guid TEXT UNIQUE, is_active INTEGER DEFAULT 1)")
    conn.execute("CREATE TABLE IF NOT EXISTS admin_commands (id INTEGER PRIMARY KEY AUTOINCREMENT, command_text TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.execute("CREATE TABLE IF NOT EXISTS manual_override (id INTEGER PRIMARY KEY, forced_token_id INTEGER)")
    
    # Migration: اضافه کردن ستون is_active اگر وجود ندارد
    try:
        conn.execute("ALTER TABLE sources ADD COLUMN is_active INTEGER DEFAULT 1")
    except: pass

    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM sources")
    if cursor.fetchone()[0] == 0:
        default_sources = [
            ("shahed_news110", "c0EBrzQ09f6e2074b5ef60697213baf3", 1),
            ("IRAN_PLES", "c0ByrGb05c345a2b2ffc3afcb9b7b8c8", 1),
            ("online_news_irw", "c0Cd9Ks0cc54702d974c11b05043cbc5", 1),
            ("SEPAHPASD", "c0C6ebG0b70e0eda432645e0e5f3f6a2", 1),
        ]
        cursor.executemany("INSERT OR IGNORE INTO sources (name, guid, is_active) VALUES (?, ?, ?)", default_sources)
    
    conn.commit()
    conn.close()

# ============================================================
# 🎬 Media Engine — جدول اختصاصی Media (کاملاً مجزا از جدول‌های فعلی)
# ============================================================
def init_media_db():
    """
    فقط یک جدول جدید و مرتبط با Media ایجاد می‌کند.
    هیچ جدول یا داده فعلی پروژه تغییر داده نمی‌شود.
    """
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS media_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    message_id INTEGER NOT NULL,
                    media_type TEXT,
                    file_id TEXT,
                    file_name TEXT,
                    mime_type TEXT,
                    extension TEXT,
                    caption TEXT,
                    file_path TEXT,
                    file_size INTEGER,
                    file_hash TEXT,
                    stage TEXT DEFAULT 'RECEIVED',
                    download_status TEXT DEFAULT 'PENDING',
                    send_status TEXT DEFAULT 'PENDING',
                    download_attempts INTEGER DEFAULT 0,
                    upload_attempts INTEGER DEFAULT 0,
                    error TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(source_name, message_id)
                )""")
    conn.commit()
    conn.close()

class TokenManager:
    def __init__(self):
        self.cooldown_duration = 3600
        self.exhausted_map = {}

    def get_tokens_from_db(self):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        rows = conn.execute("SELECT id, token FROM ai_tokens").fetchall()
        conn.close()
        return [{"id": r[0], "token": r[1].strip()} for r in rows if r[1].strip()]

    def get_forced_token_id(self):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        row = conn.execute("SELECT forced_token_id FROM manual_override WHERE id = 1").fetchone()
        conn.close()
        return row[0] if row else None

    def set_forced_token_id(self, tid):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        conn.execute("INSERT OR REPLACE INTO manual_override (id, forced_token_id) VALUES (1, ?)", (tid,))
        conn.commit()
        conn.close()

    def get_token_status_list(self):
        tokens = self.get_tokens_from_db()
        forced_id = self.get_forced_token_id()
        now = time.time()
        status_list = []
        for item in tokens:
            t = item["token"]
            tid = item["id"]
            masked = t[:6] + "..." + t[-4:] if len(t) > 10 else "***"
            is_forced = (tid == forced_id)
            
            if t in self.exhausted_map:
                remaining = int(self.cooldown_duration - (now - self.exhausted_map[t]))
                if remaining > 0:
                    mins = remaining // 60
                    status_list.append({"id": tid, "masked": masked, "status": "محدود (Cooldown)", "time_left": f"{mins} دقیقه", "forced": is_forced})
                else:
                    del self.exhausted_map[t]
                    status_list.append({"id": tid, "masked": masked, "status": "فعال و آزاد", "time_left": "-", "forced": is_forced})
            else:
                status_list.append({"id": tid, "masked": masked, "status": "فعال و آزاد", "time_left": "-", "forced": is_forced})
        return status_list

    def get_valid_token_info(self):
        tokens = self.get_tokens_from_db()
        if not tokens: return None, None
            
        now = time.time()
        expired_tokens = [t for t, fail_time in self.exhausted_map.items() if now - fail_time > self.cooldown_duration]
        for t in expired_tokens: del self.exhausted_map[t]

        forced_id = self.get_forced_token_id()
        if forced_id:
            for item in tokens:
                if item["id"] == forced_id and item["token"] not in self.exhausted_map:
                    t = item["token"]
                    return t, f"توکن دستی پیشاهنگ #{forced_id} ({t[:6]}...)"

        for item in tokens:
            t = item["token"]
            if t not in self.exhausted_map:
                return t, f"توکن خودکار #{item['id']} ({t[:6]}...)"
        return None, None

    def mark_exhausted(self, token):
        if token:
            self.exhausted_map[token] = time.time()

    def force_reset_token(self, token_id):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        row = conn.execute("SELECT token FROM ai_tokens WHERE id = ?", (token_id,)).fetchone()
        conn.close()
        if row:
            t = row[0].strip()
            if t in self.exhausted_map: del self.exhausted_map[t]

    def delete_token(self, token_id):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        row = conn.execute("SELECT token FROM ai_tokens WHERE id = ?", (token_id,)).fetchone()
        if row:
            t = row[0].strip()
            if t in self.exhausted_map: del self.exhausted_map[t]
        conn.execute("DELETE FROM ai_tokens WHERE id = ?", (token_id,))
        forced_id = self.get_forced_token_id()
        if forced_id == token_id:
            conn.execute("DELETE FROM manual_override WHERE id = 1")
        conn.commit()
        conn.close()

    def test_specific_token(self, token_id):
        conn = sqlite3.connect(DB_FILE, timeout=30)
        row = conn.execute("SELECT token FROM ai_tokens WHERE id = ?", (token_id,)).fetchone()
        conn.close()
        if not row: return "❌ توکن یافت نشد!"
        
        token = row[0].strip()
        url = "https://generativelanguage.googleapis.com/v1beta/interactions"
        data = {"model": "gemini-3.6-flash", "input": [{"type": "text", "text": "سلام"}]}
        headers = {"Content-Type": "application/json", "x-goog-api-key": token}
        try:
            r = requests.post(url, headers=headers, json=data, timeout=45)
            if r.ok: return "✅ توکن کاملاً سالم است!"
            elif r.status_code == 429: return "⚠️ توکن دارای محدودیت سهمیه است."
            else: return f"❌ خطای گوگل: {r.status_code}"
        except Exception as e:
            return f"⚠️ خطای شبکه: {str(e)}"

# ============================================================
# 📝 Exclusive News (اخبار اختصاصی) — جدول کاملاً مجزا و جدید
# قانون طلایی ۲: AI اختیاری + تأیید انسانی اجباری قبل از انتشار
# ============================================================
def init_exclusive_db():
    """فقط یک جدول جدید و مرتبط با اخبار اختصاصی می‌سازد - هیچ جدول فعلی دست‌نخورده می‌ماند."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS exclusive_news (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    raw_text TEXT NOT NULL,
                    source TEXT,
                    status_label TEXT DEFAULT 'رسمی',
                    custom_hashtags TEXT,
                    media_path TEXT,
                    ai_rewrite_enabled INTEGER DEFAULT 0,
                    ai_rewritten_text TEXT,
                    ai_rewritten INTEGER DEFAULT 0,
                    human_approved INTEGER DEFAULT 0,
                    published INTEGER DEFAULT 0,
                    published_at DATETIME,
                    content_hash TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )""")
    conn.commit()
    conn.close()


def _exclusive_hash(title, text):
    return hashlib.sha256((title.strip() + "|" + text.strip()).encode("utf-8")).hexdigest()


def create_exclusive_draft(title, raw_text, source, status_label, custom_hashtags, ai_rewrite_enabled, media_path=None):
    title = (title or "").strip()
    raw_text = (raw_text or "").strip()
    if not title or not raw_text:
        return None, "عنوان و متن خبر نمی‌توانند خالی باشند."

    content_hash = _exclusive_hash(title, raw_text)
    conn = sqlite3.connect(DB_FILE, timeout=30)
    # Duplicate Protection: هشدار به‌جای رد قطعی (برای جلوگیری از حذف اشتباه اخبار مشابه اما متفاوت)
    dup = conn.execute("SELECT id FROM exclusive_news WHERE content_hash = ?", (content_hash,)).fetchone()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO exclusive_news
           (title, raw_text, source, status_label, custom_hashtags, media_path, ai_rewrite_enabled, content_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (title, raw_text, source or "", status_label or "رسمی", custom_hashtags or "", media_path, int(bool(ai_rewrite_enabled)), content_hash),
    )
    conn.commit()
    nid = cur.lastrowid
    conn.close()
    warn = "⚠️ توجه: خبر مشابهی (متن/عنوان یکسان) قبلاً ثبت شده بود (#%d) - این خبر جدید هم ذخیره شد." % dup[0] if dup else None
    log.info("[EXCLUSIVE] Draft created #%s", nid)
    return nid, warn


def get_exclusive_by_id(news_id):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM exclusive_news WHERE id = ?", (news_id,)).fetchone()
    conn.close()
    return row


def get_all_exclusive(limit=30):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM exclusive_news ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
    conn.close()
    return rows


def update_exclusive(news_id, **fields):
    if not fields: return
    fields = dict(fields)
    fields["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [news_id]
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute(f"UPDATE exclusive_news SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


def delete_exclusive(news_id):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("DELETE FROM exclusive_news WHERE id = ?", (news_id,))
    conn.commit()
    conn.close()


EXCLUSIVE_REWRITE_SYSTEM_PROMPT = (
    "تو یک ویراستار حرفه‌ای خبری هستی. وظیفه تو فقط بازنویسی (Rewrite) متن خبر داده‌شده با لحن حرفه‌ای "
    "و رسمی خبری است - نه چیز دیگری.\n"
    "قوانین اجباری و غیرقابل‌نقض:\n"
    "۱. هیچ واقعیت، عدد، نام، تاریخ، آمار یا نقل‌قول جدیدی که در متن اصلی نیامده اختراع نکن.\n"
    "۲. هیچ اطلاعات تأییدنشده یا حدسی اضافه نکن.\n"
    "۳. اگر بخشی از متن اصلی مبهم است، همان ابهام را در بازنویسی حفظ کن - آن را قطعی یا روشن نکن.\n"
    "۴. فقط لحن، ساختار جمله و روانی متن را بهبود بده؛ معنا و محتوا باید دقیقاً همان متن اصلی باشد.\n"
    "۵. خروجی فقط باید متن بازنویسی‌شده باشد، بدون توضیح اضافه، بدون مقدمه، بدون خط اول اضافی.\n"
)


def call_exclusive_ai_rewrite(raw_text: str) -> str:
    """
    AI Rewrite اختیاری فقط برای اخبار اختصاصی - مسیر کاملاً جدا از Normal News.
    این تابع هرگز مستقیماً چیزی منتشر نمی‌کند؛ فقط متن بازنویسی‌شده را برمی‌گرداند
    تا در پنل Preview شود و منتظر تأیید انسانی بماند.
    """
    if len(raw_text.strip()) < 5:
        return ""

    total_attempts = len(token_manager.get_tokens_from_db())
    if total_attempts == 0:
        log.warning("[EXCLUSIVE] AI rewrite requested but no AI token available")
        return ""

    log.info("[EXCLUSIVE] AI rewrite requested")
    for _ in range(max(1, total_attempts)):
        token, token_label = token_manager.get_valid_token_info()
        if not token: break

        full_prompt = f"{EXCLUSIVE_REWRITE_SYSTEM_PROMPT}\n\nمتن اصلی خبر:\n{raw_text.strip()}"
        url = "https://generativelanguage.googleapis.com/v1beta/interactions"
        data = {"model": "gemini-3.6-flash", "input": [{"type": "text", "text": full_prompt}]}
        headers = {"Content-Type": "application/json", "x-goog-api-key": token}
        try:
            r = requests.post(url, headers=headers, json=data, timeout=45)
            if r.ok:
                result = r.json()
                for step in result.get("steps", []):
                    if step.get("type") == "model_output":
                        for content in step.get("content", []):
                            if content.get("type") == "text":
                                ans = content.get("text", "").strip()
                                if ans:
                                    log_stat("gemini")
                                    return ans
            else:
                token_manager.mark_exhausted(token)
        except Exception as e:
            log.warning("[EXCLUSIVE] خطای AI rewrite: %s", e)
            token_manager.mark_exhausted(token)

    return ""  # شکست خاموش: پنل باید بگوید Rewrite ناموفق بود، نه چیزی جعل کند


def format_exclusive_for_publish(row) -> str:
    """فرمت‌دهی نهایی خبر اختصاصی برای انتشار - از همان ساختار هشتگ Normal News الگو می‌گیرد و صفر هشتگ تکراری دارد."""
    body_raw = row["ai_rewritten_text"] if (row["ai_rewrite_enabled"] and row["ai_rewritten_text"]) else row["raw_text"]
    clean_body, body_source_tags = extract_and_strip_hashtags(body_raw)

    status_tag = row["status_label"] if row["status_label"] and row["status_label"].startswith("#") else f"#{row['status_label'] or 'اختصاصی'}"
    custom_tags_raw = (row["custom_hashtags"] or "").strip()
    source_line = f"\n\n🔗 منبع: {row['source']}" if row["source"] else ""

    top_tags = dedupe_hashtags(["#اختصاصی", status_tag])
    bottom_tags = dedupe_hashtags(body_source_tags, custom_tags_raw, ["#شاهد_۱۱۰"])
    bottom_tags = [t for t in bottom_tags if t not in top_tags]

    top_line = " ".join(top_tags)
    bottom_line = " ".join(bottom_tags)

    return (
        f"{top_line}\n\n"
        f"{row['title']}\n\n"
        f"{clean_body}{source_line}\n\n"
        f"────────────\n"
        f"{bottom_line}\n"
        f"📡 @shahed_news110"
    )


def publish_exclusive_news(news_id):
    """
    انتشار نهایی خبر اختصاصی - فقط اگر human_approved=1 باشد.
    از همان مقصد فعلی (send_to_eitaa / send_file_to_eitaa) استفاده می‌کند - هیچ مقصد جدیدی ساخته نمی‌شود.
    """
    row = get_exclusive_by_id(news_id)
    if not row:
        return False, "خبر اختصاصی یافت نشد."
    if not row["human_approved"]:
        return False, "این خبر هنوز تأیید انسانی (Human Approval) نشده - قبل از تأیید قابل انتشار نیست."
    if row["published"]:
        return False, "این خبر قبلاً منتشر شده است."

    final_text = format_exclusive_for_publish(row)
    log.info("[EXCLUSIVE] Publishing #%s", news_id)

    ok = False
    if row["media_path"] and os.path.exists(row["media_path"]):
        ok = send_file_to_eitaa(row["media_path"], final_text)
    else:
        ok = send_to_eitaa(final_text)

    if ok:
        update_exclusive(news_id, published=1, published_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return True, "✅ خبر اختصاصی با موفقیت منتشر شد."
    return False, "❌ ارسال به ایتا ناموفق بود - وضعیت شبکه/سرویس را بررسی کنید."


init_db()
init_media_db()
init_exclusive_db()
token_manager = TokenManager()

def get_db_sources(active_only=False):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    query = "SELECT id, name, guid, is_active FROM sources"
    if active_only: query += " WHERE is_active = 1"
    rows = conn.execute(query).fetchall()
    conn.close()
    # در صورت active_only، دیکشنری نام به GUID برگردانده می‌شود برای سازگاری با کدهای قبلی
    if active_only:
        return {row[1]: row[2] for row in rows}
    return [{"id": r[0], "name": r[1], "guid": r[2], "active": r[3]} for r in rows]

def add_source_to_db(name, guid):
    if not name.strip() or not guid.strip(): return
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("INSERT OR IGNORE INTO sources (name, guid, is_active) VALUES (?, ?, 1)", (name.strip(), guid.strip()))
    conn.commit()
    conn.close()
    if bot_loop and RUBY_CLIENT:
        asyncio.run_coroutine_threadsafe(sync_monitors(RUBY_CLIENT), bot_loop)

def toggle_source_db(sid, status):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("UPDATE sources SET is_active = ? WHERE id = ?", (status, sid))
    conn.commit()
    conn.close()
    if bot_loop and RUBY_CLIENT:
        asyncio.run_coroutine_threadsafe(sync_monitors(RUBY_CLIENT), bot_loop)

def delete_source_db(sid):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("DELETE FROM sources WHERE id = ?", (sid,))
    conn.commit()
    conn.close()
    if bot_loop and RUBY_CLIENT:
        asyncio.run_coroutine_threadsafe(sync_monitors(RUBY_CLIENT), bot_loop)

async def sync_monitors(app):
    """هماهنگ‌سازی تسک‌های مانیتورینگ با دیتابیس بدون ری‌استارت."""
    global ACTIVE_MONITORS
    sources = get_db_sources()
    current_ids = {s["id"] for s in sources if s["active"]}
    
    # متوقف کردن مواردی که غیرفعال یا حذف شده‌اند
    to_stop = set(ACTIVE_MONITORS.keys()) - current_ids
    for sid in to_stop:
        task = ACTIVE_MONITORS.pop(sid)
        task.cancel()
        log.info(f"🛑 مانیتور متوقف شد (ID: {sid})")

    # شروع مواردی که جدید یا فعال شده‌اند و در حال اجرا نیستند
    for s in sources:
        if s["active"] and s["id"] not in ACTIVE_MONITORS:
            task = asyncio.create_task(monitor_source(app, s["name"], s["guid"]))
            ACTIVE_MONITORS[s["id"]] = task
            log.info(f"▶️ مانیتور جدید شروع شد: {s['name']} (ID: {s['id']})")


# ------------------------------------------------------------------
# 🔎 Resolve آیدی کانال (GUID مستقیم / یوزرنیم / لینک) — فقط برای افزودن منبع
# ------------------------------------------------------------------
# نکته صداقت فنی: نام دقیق متد Resolve یوزرنیم در نسخه نصب‌شده Rubpy در این
# محیط قابل تأیید مستقیم نبود (به همان دلیل ذکرشده در گزارش Media Engine:
# عدم دسترسی شبکه برای نصب/اجرای واقعی). به همین دلیل به‌جای فرض قطعی یک
# نام متد، چند نام متد شناخته‌شده و رایج در کتابخانه‌های Rubika به‌صورت
# Introspective امتحان می‌شوند و فقط اگر واقعاً روی Client موجود باشند
# فراخوانی می‌شوند. اگر هیچ‌کدام وجود نداشتند، خطای صریح و صادقانه برگردانده
# می‌شود (نه یک نتیجه ساختگی) و از ادمین خواسته می‌شود GUID را مستقیم بدهد.
async def resolve_source_identifier(app, raw_input: str):
    """
    ورودی می‌تواند GUID مستقیم، یوزرنیم (با یا بدون @) یا لینک کانال روبیکا باشد.
    خروجی: dict {"guid": ..., "title": ..., "error": ...}
    """
    raw_input = (raw_input or "").strip()
    if not raw_input:
        return {"guid": None, "title": None, "error": "ورودی خالی است"}

    # حالت ۱: خود ورودی احتمالاً GUID مستقیم است (بدون '/', '@', فاصله و طولانی)
    looks_like_guid = (
        "/" not in raw_input and "@" not in raw_input and " " not in raw_input
        and len(raw_input) >= 20 and re.match(r"^[A-Za-z0-9]+$", raw_input)
    )
    if looks_like_guid:
        return {"guid": raw_input, "title": None, "error": None}

    # حالت ۲: یوزرنیم یا لینک - استخراج یوزرنیم خالص
    username = raw_input.replace("https://", "").replace("http://", "")
    for prefix in ("rubika.ir/", "www.rubika.ir/"):
        if username.lower().startswith(prefix):
            username = username[len(prefix):]
    username = username.strip("/").lstrip("@").strip()

    if not username:
        return {"guid": None, "title": None, "error": "امکان استخراج یوزرنیم از ورودی وجود نداشت"}

    candidate_methods = ["get_object_by_username", "getObjectByUsername"]
    last_error = None
    for method_name in candidate_methods:
        method = getattr(app, method_name, None)
        if not callable(method):
            continue
        try:
            result = await method(username)
        except Exception as e:
            last_error = str(e)
            log.warning("خطا در فراخوانی %s برای یوزرنیم '%s': %s", method_name, username, e)
            continue

        nested = _media_get(result, "channel", "chat", "group", "user")
        guid = _media_get(result, "object_guid", "guid", "channel_guid") or _media_get(nested, "object_guid", "guid")
        title = _media_get(result, "title", "name") or _media_get(nested, "title", "name") or username

        if guid:
            return {"guid": str(guid), "title": str(title), "error": None}

    return {
        "guid": None,
        "title": None,
        "error": (
            "امکان Resolve خودکار یوزرنیم/لینک با نسخه نصب‌شده Rubpy موجود در سرور پیدا نشد"
            + (f" (خطای آخر: {last_error})" if last_error else " (متد شناخته‌شده‌ای روی Client پیدا نشد)")
            + " — لطفاً GUID کانال را مستقیماً وارد کنید."
        ),
    }


def resolve_and_add_source(raw_identifier, custom_name=None):
    """
    Wrapper همزمان (Sync) برای استفاده در هندلر وب: ورودی (GUID/یوزرنیم/لینک) را
    Resolve کرده و در صورت موفقیت منبع را اضافه و مانیتورها را همگام می‌کند.
    خروجی: پیام وضعیت برای نمایش در بنر پنل وب.
    """
    raw_identifier = (raw_identifier or "").strip()
    if not raw_identifier:
        return "⚠️ آیدی/GUID کانال وارد نشده است."

    if not bot_loop or not RUBY_CLIENT:
        return "⚠️ کلاینت روبیکا هنوز آماده نیست - کمی صبر کنید و دوباره تلاش کنید."

    future = asyncio.run_coroutine_threadsafe(
        resolve_source_identifier(RUBY_CLIENT, raw_identifier), bot_loop
    )
    try:
        result = future.result(timeout=20)
    except Exception as e:
        return f"❌ خطا در Resolve کردن آیدی کانال: {e}"

    if result.get("error"):
        return f"❌ {result['error']}"

    guid = result["guid"]
    name = (custom_name or "").strip() or (result.get("title") or "").strip() or guid

    add_source_to_db(name, guid)
    return f"✅ منبع «{name}» با GUID {guid} اضافه شد و مانیتورینگ آن شروع شد."

def add_token_to_db(token):
    if not token.strip(): return
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("INSERT OR IGNORE INTO ai_tokens (token) VALUES (?)", (token.strip(),))
    conn.commit()
    conn.close()

def add_admin_command(cmd_text):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("DELETE FROM admin_commands")
    if cmd_text.strip():
        conn.execute("INSERT INTO admin_commands (command_text) VALUES (?)", (cmd_text.strip(),))
    conn.commit()
    conn.close()

def get_latest_admin_command():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    row = conn.execute("SELECT command_text FROM admin_commands ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row[0] if row else ""

def log_stat(provider_type="gemini"):
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_FILE, timeout=30)
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, total_news, gemini_used, backup_used FROM stats WHERE date = ?", (today,))
        row = cur.fetchone()
        if not row:
            cur.execute("INSERT INTO stats (date, total_news, gemini_used, backup_used) VALUES (?, 1, ?, ?)", 
                        (today, 1 if provider_type=="gemini" else 0, 0 if provider_type=="gemini" else 1))
        else:
            rid, t, g, b = row
            if provider_type == "gemini": g += 1
            else: b += 1
            cur.execute("UPDATE stats SET total_news = ?, gemini_used = ?, backup_used = ? WHERE id = ?", (t+1, g, b, rid))
        conn.commit()
    finally:
        conn.close()

def get_stats_data():
    today = datetime.now().strftime("%Y-%m-%d")
    conn = sqlite3.connect(DB_FILE, timeout=30)
    row = conn.execute("SELECT total_news, gemini_used, backup_used FROM stats WHERE date = ?", (today,)).fetchone()
    conn.close()
    return {"total": row[0], "gemini": row[1], "backup": row[2]} if row else {"total": 0, "gemini": 0, "backup": 0}

def get_last_id(source_name):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    row = conn.execute("SELECT last_message_id FROM source_state WHERE source_name = ?", (source_name,)).fetchone()
    conn.close()
    return int(row[0]) if row and row[0] is not None else None

def save_last_id(source_name, message_id):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("INSERT INTO source_state (source_name, last_message_id) VALUES (?, ?) ON CONFLICT(source_name) DO UPDATE SET last_message_id = excluded.last_message_id", (source_name, int(message_id)))
    conn.commit()
    conn.close()

def add_to_hourly_buffer(source_name, text):
    if not text.strip(): return
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute("INSERT INTO hourly_news_buffer (source_name, text) VALUES (?, ?)", (source_name, text))
    conn.commit()
    conn.close()

# توابع جدید استخراج اخبار بر اساس زمان
def get_recent_news(hours_back):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    rows = conn.execute(f"SELECT text FROM hourly_news_buffer WHERE timestamp >= datetime('now', '-{hours_back} hours')").fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_news_between(hours_min, hours_max):
    # مثلا برای گرفتن اخبار 1 تا 3 ساعت قبل (بدون تداخل با یک ساعت اخیر)
    conn = sqlite3.connect(DB_FILE, timeout=30)
    rows = conn.execute(f"SELECT text FROM hourly_news_buffer WHERE timestamp <= datetime('now', '-{hours_min} hours') AND timestamp >= datetime('now', '-{hours_max} hours')").fetchall()
    conn.close()
    return [r[0] for r in rows]

def cleanup_old_news(hours_to_keep=48):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute(f"DELETE FROM hourly_news_buffer WHERE timestamp < datetime('now', '-{hours_to_keep} hours')")
    conn.commit()
    conn.close()

# ============================================================
# 🧠 هوش مصنوعی تحلیلی (عمیق و سفارشی)
# ============================================================
def get_system_prompt(is_custom=False, custom_hours=1):
    if is_custom:
        base = (
            f"تو یک استراتژیست ارشد رسانه و تحلیلگر انقلابی هستی. "
            f"مجموعه‌ای از اخبار {custom_hours} ساعت گذشته به تو داده می‌شود. "
            f"یک بولتن خبری جامع، راهبردی و حماسی با لحن صمیمی و طنز کوچه-بازاری بنویس. پایان‌بندی جذاب فراموش نشود."
        )
    else:
        base = (
            "تو یک استراتژیست ارشد رسانه و تحلیلگر انقلابی هستی. "
            "وظیفه تو نوشتن بولتن 'اخبار یک ساعت اخیر' است. برای درک بهتر پس‌زمینه اتفاقات، چکیده‌ای از اخبار ۳ ساعت گذشته نیز به عنوان 'گریز' در اختیار تو قرار می‌گیرد. "
            "تمرکز اصلی و خبرهای محوری را روی اخبار جدید (بخش اول متن) بگذار و در تحلیل‌هایت با طعنه و طنز هوشمندانه به اخبار گذشته (بخش دوم متن) گریز بزن. "
            "لحن باید حماسی، صمیمی، طنز کوچه-بازاری و پرانرژی باشد. از روده‌درازی پرهیز کن."
        )
        
    admin_cmd = get_latest_admin_command()
    if admin_cmd:
        base += f"\n\n🚨 **دستور ویژه و آنی مدیر:** {admin_cmd}"
    return base

def call_custom_ai(prompt_text: str, is_custom=False, custom_hours=1) -> str:
    if len(prompt_text.strip()) < 10: return ""
    
    total_attempts = len(token_manager.get_tokens_from_db())
    if total_attempts == 0:
        SYSTEM_STATUS["active_token"] = "⚠️ هیچ توکنی ثبت نشده است!"
        return ""

    for _ in range(max(1, total_attempts)):
        token, token_label = token_manager.get_valid_token_info()
        if not token: break
            
        SYSTEM_STATUS["active_token"] = f"🟢 در حال استفاده از {token_label}"
        full_prompt = f"{get_system_prompt(is_custom, custom_hours)}\n\nمتن اخبار جهت تحلیل:\n{prompt_text}"
        
        url = "https://generativelanguage.googleapis.com/v1beta/interactions"
        data = {"model": "gemini-3.6-flash", "input": [{"type": "text", "text": full_prompt}]}
        headers = {"Content-Type": "application/json", "x-goog-api-key": token}
        
        try:
            r = requests.post(url, headers=headers, json=data, timeout=45)
            if r.ok:
                result = r.json()
                for step in result.get("steps", []):
                    if step.get("type") == "model_output":
                        for content in step.get("content", []):
                            if content.get("type") == "text":
                                ans = content.get("text", "").strip()
                                if ans:
                                    SYSTEM_STATUS["gemini"] = "فعال و متصل"
                                    log_stat("gemini")
                                    return ans
            else:
                token_manager.mark_exhausted(token)
        except Exception as e:
            token_manager.mark_exhausted(token)

    SYSTEM_STATUS["gemini"] = "خطا / حالت امن"
    return ""

def safe_text_cleaner(text: str) -> str:
    if not text: return ""
    cleaned = re.sub(r"@[a-zA-Z0-9_]+", "", text)
    cleaned = re.sub(r"http\S+", "", cleaned)
    log_stat("backup")
    return cleaned.strip()

_HASHTAG_RE = re.compile(r"#[\w\u0600-\u06FF_]+")


def extract_and_strip_hashtags(text: str):
    """
    هشتگ‌های موجود در متن خام منبع را استخراج و از بدنه متن حذف می‌کند تا
    با هشتگ‌های Rule-Based خودِ سیستم قاطی/تکراری نشوند.
    خروجی: (متن پاک‌شده, لیست هشتگ‌های یافت‌شده)
    """
    if not text:
        return text, []
    found = _HASHTAG_RE.findall(text)
    cleaned = _HASHTAG_RE.sub("", text)
    # پاکسازی خط‌های خالی/فاصله‌های اضافه که از حذف هشتگ باقی می‌ماند
    cleaned = re.sub(r"[ \t]{2,}", " ", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned, found


def dedupe_hashtags(*groups):
    """
    از چند لیست/رشته هشتگ، یک لیست یکتا و مرتب (بدون تکرار، حساس به تنوع نویسه‌های عربی/فارسی) می‌سازد.
    ترتیب اولین‌بار دیده‌شدن حفظ می‌شود تا هشتگ‌های مهم‌تر (وضعیت) جلوتر از موضوعی بمانند.
    """
    seen = set()
    result = []
    for group in groups:
        if not group:
            continue
        items = group if isinstance(group, (list, tuple, set)) else group.split()
        for tag in items:
            tag = tag.strip()
            if not tag:
                continue
            if not tag.startswith("#"):
                tag = "#" + tag
            # نرمال‌سازی ی/ک عربی به فارسی برای جلوگیری از تکرار ظاهری یکسان با انکدینگ متفاوت
            norm = tag.replace("ي", "ی").replace("ك", "ک")
            if norm not in seen:
                seen.add(norm)
                result.append(tag)
    return result


def select_status_hashtags(text: str, extra_hint_tags=None) -> list:
    """
    قانون طلایی ۱ (V12): انتخاب هشتگ‌های بالای خبر کاملاً Rule-Based و Deterministic.
    هیچ فراخوانی AI در این تابع وجود ندارد - سریع، قابل‌پیش‌بینی و بدون وابستگی به سرویس خارجی.
    خروجی: لیست هشتگ‌های وضعیت (بین ۱ تا ۳ مورد) بر اساس کلیدواژه‌های متن خبر.
    """
    if not text or not text.strip():
        return ["#فوری"]

    t = text.strip()
    hint_text = t + " " + " ".join(extra_hint_tags or [])
    tags = []

    urgent_kw = ["فوری", "لحظاتی پیش", "هم‌اکنون", "دقایقی پیش", "breaking"]
    official_kw = ["اعلام کرد", "بیانیه رسمی", "سخنگو", "وزارت", "رسانه ملی", "خبرگزاری رسمی", "دولت", "رسمی اعلام"]
    unofficial_kw = ["شنیده‌ها", "به گفته منابع", "گفته می‌شود", "گزارش‌های غیررسمی", "منابع نزدیک"]
    warning_kw = ["هشدار", "خطر", "توصیه می‌شود", "احتیاط", "تخلیه", "امنیتی هشدار"]
    exclusive_kw = ["اختصاصی", "ویژه شاهد", "گزارش اختصاصی"]
    important_kw = ["مهم", "حیاتی", "سرنوشت‌ساز", "بزرگ", "تاریخی"]

    if any(k in hint_text for k in exclusive_kw):
        tags.append("#اختصاصی")
    if any(k in hint_text for k in urgent_kw):
        tags.append("#فوری")
    if any(k in hint_text for k in warning_kw):
        tags.append("#هشدار")
    if any(k in hint_text for k in official_kw):
        tags.append("#رسمی")
    elif any(k in hint_text for k in unofficial_kw):
        tags.append("#غیررسمی")
    if any(k in hint_text for k in important_kw) and "#مهم" not in tags:
        tags.append("#مهم")

    if not tags:
        tags = ["#فوری"]

    # سقف ۳ هشتگ وضعیت در بالای خبر تا خروجی شلوغ نشود + یکتاسازی نهایی
    return dedupe_hashtags(tags)[:3]


def append_smart_tags(text: str) -> str:
    """
    فرمت‌دهی نهایی اخبار عادی طبق قانون طلایی V12:
    #هشتگ‌های‌وضعیت (بالا - Rule-Based)
    متن خبر (بدون هشتگ‌های خامِ منبع - تا تکراری نشود)
    #هشتگ‌های‌موضوعی (پایین - Rule-Based)
    @کانال_فعلی (بدون تغییر)
    این تابع همچنان صفر وابستگی به AI دارد و کل پیام صفر هشتگ تکراری دارد.
    """
    if not text.strip(): return ""

    # ۱) هشتگ‌های خودِ منبع را از بدنه جدا می‌کنیم تا با هشتگ‌های ما تکراری نشوند
    #    اما همچنان به‌عنوان سرنخ برای تشخیص وضعیت (فوری/رسمی/...) استفاده می‌شوند
    clean_text, source_tags = extract_and_strip_hashtags(text)

    status_tags = select_status_hashtags(clean_text, extra_hint_tags=source_tags)

    topic_tag_rules = {
        "#جبهه_شمال": ["لبنان", "حزب‌الله", "حیفا", "تل‌آویو", "سید حسن", "الجلیل"],
        "#طوفان_الاقصی": ["غزه", "فلسطین", "حماس", "رفح", "قسام"],
        "#یمن_مقتدر": ["یمن", "انصارالله", "دریای سرخ", "صنعا"],
        "#اقتدار_ایران": ["سپاه", "ارتش", "ایران", "موشکی", "وعده صادق"],
        "#مقاومت_عراق": ["عراق", "حشد", "عین الاسد"]
    }
    found_topic_tags = ["#محور_مقاومت"]
    for tag, keywords in topic_tag_rules.items():
        if any(word in clean_text for word in keywords):
            found_topic_tags.append(tag)
    # به‌علاوه هر هشتگ موضوعی که خودِ منبع گذاشته و جزو هشتگ‌های وضعیتی ما نیست، حفظ می‌شود
    found_topic_tags.extend(source_tags)

    admin_cmd = get_latest_admin_command()
    admin_note = f"\n💬 **پیام مدیر:** {admin_cmd}\n" if admin_cmd else ""

    # ۲) یکتاسازی نهایی: هیچ هشتگی هم‌زمان بالا و پایین یا دوبار در یک بخش تکرار نمی‌شود
    top_tags = dedupe_hashtags(status_tags)
    bottom_tags = dedupe_hashtags(found_topic_tags, ["#شاهد_۱۱۰"])
    bottom_tags = [t for t in bottom_tags if t not in top_tags]  # حذف تداخل بالا/پایین

    top_line = " ".join(top_tags)
    bottom_line = " ".join(bottom_tags)

    return (
        f"{top_line}\n\n"
        f"{clean_text}{admin_note}\n\n"
        f"────────────\n"
        f"{bottom_line}\n"
        f"📡 @shahed_news110"
    )

# ============================================================
# 🌐 پنل وب (بخش بولتن سفارشی اضافه شد)
# ============================================================
class ReusableTCPServer(HTTPServer): allow_reuse_address = True

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query = parse_qs(parsed_url.query)

        if path == '/':
            stats = get_stats_data()
            g_color = "#10b981" if "متصل" in SYSTEM_STATUS["gemini"] else "#f59e0b"
            latest_cmd = get_latest_admin_command()

            test_msg = query.get('msg', [''])[0]
            test_banner = f"<div style='background: rgba(30,41,59,0.9); border: 2px solid #00f0ff; border-radius: 12px; padding: 14px; margin-bottom: 20px; text-align: center; color: #fff;'><b>پیام سیستم:</b> {html.escape(test_msg)}</div>" if test_msg else ""

            tokens_status = token_manager.get_token_status_list()
            tokens_html = "".join([f"""<div class="token-row">
                <div><span class="highlight">توکن #{t['id']}</span> {'<span class="badge" style="background:#f59e0b">دستی</span>' if t['forced'] else ''}<br><span style="font-size:11px; color:#94a3b8;">وضعیت: {t['status']}</span></div>
                <div style="display:flex; gap:4px;"><a href="/force_token?id={t['id']}"><button class="btn-small" style="background:#f59e0b; color:#000;">سوییچ</button></a><a href="/test_token?id={t['id']}"><button class="btn-small" style="background:#00f0ff; color:#000;">تست</button></a></div>
                </div>""" for t in tokens_status]) if tokens_status else "<div style='text-align:center; color:#64748b;'>توکنی نیست!</div>"

            html_page = f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <meta http-equiv="refresh" content="10">
    <title>مرکز فرماندهی شاهد ۱۱۰</title>
    <style>
        @import url('https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css');
        body {{ background: linear-gradient(135deg, #050508, #101018); color: #e2e8f0; font-family: 'Vazirmatn'; padding: 15px; }}
        h1 {{ text-align: center; color: #00f0ff; text-shadow: 0 0 15px rgba(0, 240, 255, 0.7); }}
        .glass-card {{ background: rgba(18,24,38,0.5); backdrop-filter: blur(16px); border: 1px solid rgba(255,255,255,0.08); border-radius: 18px; padding: 20px; margin-bottom: 20px; box-shadow: 0 10px 35px rgba(0,0,0,0.5); }}
        h3 {{ margin: 0 0 15px 0; color: #c7d2fe; border-bottom: 1px solid rgba(255,255,255,0.06); padding-bottom: 10px; font-size:15px;}}
        input, textarea, button {{ width: 100%; padding: 11px; margin-top: 8px; background: rgba(8,12,20,0.8); border: 1px solid rgba(255,255,255,0.12); border-radius: 10px; color: #fff; font-family: 'Vazirmatn'; box-sizing: border-box; }}
        button {{ background: #6366f1; font-weight: bold; cursor: pointer; }}
        .btn-small {{ padding: 5px 10px; font-size: 11px; width: auto; margin-top: 0; }}
        .token-row {{ background: rgba(0,0,0,0.35); border: 1px solid rgba(255,255,255,0.05); border-radius: 10px; padding: 12px; margin-top: 10px; display: flex; justify-content: space-between; align-items: center; }}
        .badge {{ padding: 3px 8px; border-radius: 10px; font-size: 10px; color: #000; }}
        .highlight {{ color: #00f0ff; }}
    </style>
</head>
<body>
    <h1>⚡ مرکز فرماندهی شاهد ۱۱۰</h1>
    {test_banner}
    
    <div class="glass-card">
        <h3>🚀 ارسال بولتن سفارشی و فوری</h3>
        <p style="font-size:12px; color:#94a3b8;">شما می‌توانید بازه زمانی اخبار (مثلاً ۲۴ ساعت گذشته) را تعیین کنید تا هوش مصنوعی همین الان در پس‌زمینه تحلیل کرده و به ایتا بفرستد.</p>
        <form action="/trigger_custom" method="GET">
            <input type="number" name="hours" placeholder="تعداد ساعت اخبار (مثلاً 24 برای دیروز تا الان)..." required min="1" max="48">
            <button type="submit" style="background: #a855f7;">ارسال فوری بولتن سفارشی به ایتا</button>
        </form>
    </div>

    <div class="glass-card">
        <h3>🤖 وضعیت زنده سیستم</h3>
        <div>وضعیت هوش مصنوعی: <span style="color:{g_color}; font-weight:bold;">{SYSTEM_STATUS['gemini']}</span></div>
        <div style="margin-top:10px;">توکن فعال: <span style="color:#10b981;">{SYSTEM_STATUS['active_token']}</span></div>
    </div>

    <div class="glass-card">
        <h3>🔑 مدیریت توکن‌ها</h3>
        <form action="/add_token" method="GET">
            <input type="text" name="token" placeholder="توکن جدید جمنای..." required>
            <button type="submit">افزودن</button>
        </form>
        {tokens_html}
    </div>

    <div class="glass-card">
        <h3>📺 مدیریت منابع (روبیکا)</h3>
        <form action="/add_source" method="GET" style="display: flex; gap: 8px; margin-bottom: 8px; flex-wrap: wrap;">
            <input type="text" name="identifier" placeholder="آیدی/یوزرنیم کانال (@channel) یا لینک یا GUID مستقیم" required style="flex: 2; min-width: 220px;">
            <input type="text" name="name" placeholder="نام دلخواه (اختیاری)" style="flex: 1; min-width: 140px;">
            <button type="submit" style="width: auto; padding: 0 20px;">+ افزودن</button>
        </form>
        <p style="font-size:11px; color:#64748b; margin-top:0; margin-bottom:15px;">می‌توانید یوزرنیم (با یا بدون @)، لینک کانال یا GUID مستقیم را وارد کنید؛ در صورت خالی بودن «نام دلخواه»، نام کانال به‌صورت خودکار تشخیص داده می‌شود.</p>
        
        <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 13px;">
                <thead>
                    <tr style="border-bottom: 2px solid rgba(255,255,255,0.1); color: #94a3b8;">
                        <th style="padding: 10px; text-align: right;">نام منبع</th>
                        <th style="padding: 10px; text-align: center;">وضعیت</th>
                        <th style="padding: 10px; text-align: left;">عملیات</th>
                    </tr>
                </thead>
                <tbody>
                    {"".join([f'''<tr style="border-bottom: 1px solid rgba(255,255,255,0.05);">
                        <td style="padding: 10px;">{s['name']}<br><span style="font-size:10px; color:#64748b;">{s['guid']}</span></td>
                        <td style="padding: 10px; text-align: center;">
                            <span class="badge" style="background: {'#10b981' if s['active'] else '#64748b'}; color: #fff;">
                                {'فعال' if s['active'] else 'غیرفعال'}
                            </span>
                        </td>
                        <td style="padding: 10px; text-align: left;">
                            <a href="/toggle_source?id={s['id']}&status={0 if s['active'] else 1}"><button class="btn-small" style="background: #f59e0b;">{ 'خاموش' if s['active'] else 'روشن'}</button></a>
                            <a href="/delete_source?id={s['id']}" onclick="return confirm('حذف شود؟')"><button class="btn-small" style="background: #ef4444;">حذف</button></a>
                        </td>
                    </tr>''' for s in get_db_sources()])}
                </tbody>
            </table>
        </div>
    </div>

    <div class="glass-card">
        <h3>📝 اتاق اخبار اختصاصی (Exclusive News)</h3>
        <form action="/exclusive_create" method="POST">
            <input type="text" name="title" placeholder="عنوان خبر" required>
            <textarea name="text" rows="4" placeholder="متن خبر" required></textarea>
            <input type="text" name="source" placeholder="منبع (اختیاری)">
            <input type="text" name="custom_hashtags" placeholder="هشتگ‌های دلخواه پایین خبر (مثال: #ایران #سیاسی)">
            <select name="status_label" style="margin-top:8px;">
                <option value="رسمی">رسمی</option>
                <option value="غیررسمی">غیررسمی</option>
                <option value="هشدار">هشدار</option>
                <option value="فوری">فوری</option>
            </select>
            <label style="display:flex; align-items:center; gap:8px; margin-top:10px; font-size:13px;">
                <input type="checkbox" name="ai_rewrite_enabled" style="width:auto;"> فعال‌سازی AI Rewrite اختیاری (نیازمند تأیید انسانی قبل از انتشار)
            </label>
            <button type="submit" style="background:#a855f7; margin-top:12px;">ثبت پیش‌نویس</button>
        </form>

        <div style="margin-top:18px;">
            {"".join([f'''<div class="token-row" style="flex-direction:column; align-items:stretch;">
                <div><span class="highlight">#{r['id']}</span> {html.escape(r['title'])}
                    <span class="badge" style="background:{'#10b981' if r['published'] else ('#6366f1' if r['human_approved'] else '#64748b')}">
                    {'منتشرشده' if r['published'] else ('تأییدشده' if r['human_approved'] else 'پیش‌نویس')}</span>
                    {'<span class="badge" style="background:#f59e0b;color:#000;">AI Rewritten</span>' if r['ai_rewritten'] else ''}
                </div>
                <div style="display:flex; gap:6px; margin-top:8px; flex-wrap:wrap;">
                    <a href="/exclusive_preview?id={r['id']}"><button class="btn-small" style="background:#00f0ff;color:#000;">Preview</button></a>
                    {'<a href="/exclusive_ai_rewrite?id=' + str(r['id']) + '"><button class="btn-small" style="background:#f59e0b;color:#000;">AI Rewrite</button></a>' if r['ai_rewrite_enabled'] and not r['published'] else ''}
                    {'<a href="/exclusive_approve?id=' + str(r['id']) + '"><button class="btn-small" style="background:#6366f1;">تأیید (Approve)</button></a>' if not r['human_approved'] and not r['published'] else ''}
                    {'<a href="/exclusive_publish?id=' + str(r['id']) + '"><button class="btn-small" style="background:#10b981;">انتشار</button></a>' if r['human_approved'] and not r['published'] else ''}
                    <a href="/exclusive_delete?id={r['id']}" onclick="return confirm('حذف شود؟')"><button class="btn-small" style="background:#ef4444;">حذف</button></a>
                </div>
            </div>''' for r in get_all_exclusive()]) or "<div style='color:#64748b;'>هنوز خبر اختصاصی ثبت نشده.</div>"}
        </div>
    </div>

    <div class="glass-card">
        <h3>🐯 Tiger Media — وضعیت Jobها</h3>
        <div style="display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px;">
            {"".join([f'<span class="badge" style="background:#1f2937; color:#e2e8f0;">{k}: {v}</span>' for k, v in get_media_job_counts().items()])}
        </div>
        <div style="max-height:320px; overflow-y:auto;">
            {"".join([f'''<div class="token-row">
                <div><span class="highlight">Job #{j['id']}</span> {j['media_type'] or '-'} | {j['source_name']}#{j['message_id']}<br>
                <span style="font-size:11px; color:#94a3b8;">وضعیت: {get_unified_media_status(j['stage'], j['download_status'], j['send_status'])} | تلاش دانلود: {j['download_attempts']} | تلاش ارسال: {j['upload_attempts']}</span></div>
                <div>{'<a href="/media_cancel?id=' + str(j['id']) + '"><button class="btn-small" style="background:#ef4444;">لغو</button></a>' if get_unified_media_status(j['stage'], j['download_status'], j['send_status']) not in ('SENT','CANCELLED') else ''}</div>
                </div>''' for j in list_recent_media_jobs()]) or "<div style='color:#64748b;'>Job ای ثبت نشده.</div>"}
        </div>
    </div>

    <div class="glass-card">
        <h3>📊 System Status</h3>
        <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px; font-size:13px;">
            <div>🗄️ Database: <span style="color:#10b981;">متصل ({DB_FILE})</span></div>
            <div>🤖 AI: <span style="color:{g_color};">{SYSTEM_STATUS['gemini']}</span></div>
            <div>📤 Eitaa: <span style="color:#10b981;">پیکربندی‌شده (Chat ID فعلی حفظ شده)</span></div>
            <div>📡 Rubika: <span style="color:#10b981;">{len(get_db_sources())} منبع ثبت‌شده</span></div>
            <div>🎬 Media Engine: <span style="color:#10b981;">{sum(get_media_job_counts().values())} Job کل</span></div>
            <div>⏱️ Scheduler: <span style="color:#94a3b8;">توسط news_trigger_engine.py مستقل مدیریت می‌شود</span></div>
        </div>
    </div>
</body>
</html>"""
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html_page.encode('utf-8'))

        elif path == '/trigger_custom':
            hrs = query.get('hours', ['1'])[0]
            if hrs.isdigit() and bot_loop:
                # اجرای تسک در پس‌زمینه بدون مسدود کردن پنل
                asyncio.run_coroutine_threadsafe(generate_custom_bulletin(int(hrs)), bot_loop)
            import urllib.parse
            self.send_response(303)
            self.send_header('Location', '/?msg=' + urllib.parse.quote('دستور اجرای بولتن سفارشی به صف ارسال رفت.'))
            self.end_headers()
            
        elif path == '/add_token':
            t = query.get('token', [''])[0]
            if t: add_token_to_db(t)
            self.send_response(303); self.send_header('Location', '/'); self.end_headers()
        elif path == '/force_token':
            t = query.get('id', [''])[0]
            if t.isdigit(): token_manager.set_forced_token_id(int(t))
            self.send_response(303); self.send_header('Location', '/'); self.end_headers()

        elif path == '/add_source':
            identifier = query.get('identifier', [''])[0]
            custom_name = query.get('name', [''])[0]
            msg_text = resolve_and_add_source(identifier, custom_name)
            import urllib.parse
            self.send_response(303)
            self.send_header('Location', '/?msg=' + urllib.parse.quote(msg_text))
            self.end_headers()
            
        elif path == '/toggle_source':
            sid = query.get('id', [''])[0]
            status = query.get('status', ['0'])[0]
            if sid.isdigit(): toggle_source_db(int(sid), int(status))
            self.send_response(303); self.send_header('Location', '/'); self.end_headers()
            
        elif path == '/delete_source':
            sid = query.get('id', [''])[0]
            if sid.isdigit(): delete_source_db(int(sid))
            self.send_response(303); self.send_header('Location', '/'); self.end_headers()

        # ===================== V12: Exclusive News =====================
        elif path == '/exclusive_ai_rewrite':
            nid = query.get('id', [''])[0]
            msg_text = "شناسه نامعتبر."
            if nid.isdigit():
                row = get_exclusive_by_id(int(nid))
                if row:
                    log.info("[EXCLUSIVE] AI rewrite requested #%s", nid)
                    rewritten = call_exclusive_ai_rewrite(row["raw_text"])
                    if rewritten:
                        update_exclusive(int(nid), ai_rewritten_text=rewritten, ai_rewritten=1)
                        msg_text = "✅ بازنویسی AI انجام شد - در Preview بررسی کنید."
                    else:
                        msg_text = "⚠️ بازنویسی AI ناموفق بود (توکن/سرویس در دسترس نیست). متن اصلی دست‌نخورده باقی ماند."
                else:
                    msg_text = "خبر یافت نشد."
            import urllib.parse
            self.send_response(303); self.send_header('Location', '/?tab=exclusive&msg=' + urllib.parse.quote(msg_text)); self.end_headers()

        elif path == '/exclusive_preview':
            nid = query.get('id', [''])[0]
            row = get_exclusive_by_id(int(nid)) if nid.isdigit() else None
            if not row:
                self.send_response(404); self.end_headers(); self.wfile.write("یافت نشد".encode('utf-8')); return
            final_text = format_exclusive_for_publish(row)
            page = f"""<!DOCTYPE html><html lang="fa" dir="rtl"><head><meta charset="UTF-8">
            <style>body{{background:#0b0f1a;color:#e2e8f0;font-family:'Vazirmatn',sans-serif;padding:20px;}}
            pre{{white-space:pre-wrap;background:#111827;padding:16px;border-radius:10px;border:1px solid #333;}}
            a{{color:#00f0ff;}}</style></head><body>
            <h2>👁️ Preview خبر اختصاصی #{row['id']}</h2>
            <div>وضعیت: AI Rewritten: {"بله" if row['ai_rewritten'] else "خیر"} |
            Human Approved: {"بله" if row['human_approved'] else "خیر"} |
            Published: {"بله" if row['published'] else "خیر"}</div>
            <pre>{html.escape(final_text)}</pre>
            <a href="/">← بازگشت به پنل</a></body></html>"""
            self.send_response(200); self.send_header("Content-type", "text/html; charset=utf-8"); self.end_headers()
            self.wfile.write(page.encode('utf-8'))

        elif path == '/exclusive_approve':
            nid = query.get('id', [''])[0]
            msg_text = "شناسه نامعتبر."
            if nid.isdigit() and get_exclusive_by_id(int(nid)):
                update_exclusive(int(nid), human_approved=1)
                log.info("[EXCLUSIVE] Approved #%s", nid)
                msg_text = "✅ خبر تأیید شد و آماده انتشار است."
            import urllib.parse
            self.send_response(303); self.send_header('Location', '/?tab=exclusive&msg=' + urllib.parse.quote(msg_text)); self.end_headers()

        elif path == '/exclusive_publish':
            nid = query.get('id', [''])[0]
            msg_text = "شناسه نامعتبر."
            if nid.isdigit():
                log.info("[EXCLUSIVE] Publishing requested #%s", nid)
                ok, msg_text = publish_exclusive_news(int(nid))
            import urllib.parse
            self.send_response(303); self.send_header('Location', '/?tab=exclusive&msg=' + urllib.parse.quote(msg_text)); self.end_headers()

        elif path == '/exclusive_delete':
            nid = query.get('id', [''])[0]
            if nid.isdigit(): delete_exclusive(int(nid))
            self.send_response(303); self.send_header('Location', '/?tab=exclusive'); self.end_headers()

        # ===================== V12: Tiger Media =====================
        elif path == '/media_cancel':
            jid = query.get('id', [''])[0]
            msg_text = "شناسه نامعتبر."
            if jid.isdigit():
                ok, msg_text = cancel_media_job(int(jid))
            import urllib.parse
            self.send_response(303); self.send_header('Location', '/?tab=media&msg=' + urllib.parse.quote(msg_text)); self.end_headers()

    def _parse_multipart(self):
        """
        پارسر ساده و مستقل multipart/form-data (بدون وابستگی به ماژول منسوخ cgi).
        خروجی: (fields: dict[str,str], files: dict[str, (filename, bytes)])
        """
        content_type = self.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type or 'boundary=' not in content_type:
            return {}, {}
        boundary = content_type.split('boundary=')[-1].strip().strip('"').encode('utf-8')
        length = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(length)

        fields, files = {}, {}
        parts = body.split(b'--' + boundary)
        for part in parts:
            part = part.strip(b'\r\n')
            if not part or part == b'--':
                continue
            if b'\r\n\r\n' not in part:
                continue
            headers_raw, content = part.split(b'\r\n\r\n', 1)
            content = content.rstrip(b'\r\n')
            headers_text = headers_raw.decode('utf-8', errors='ignore')
            name_match = re.search(r'name="([^"]+)"', headers_text)
            if not name_match:
                continue
            field_name = name_match.group(1)
            filename_match = re.search(r'filename="([^"]*)"', headers_text)
            if filename_match:
                filename = filename_match.group(1)
                if filename:
                    files[field_name] = (filename, content)
            else:
                fields[field_name] = content.decode('utf-8', errors='ignore')
        return fields, files

    def do_POST(self):
        """
        Endpoint جدید POST (فقط برای فرم اخبار اختصاصی - متن طولانی در URL امن/عملی نیست).
        هیچ Endpoint قدیمی GET تغییر نکرده - این کاملاً افزوده است.
        """
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        if path == '/exclusive_create':
            length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(length).decode('utf-8', errors='ignore')
            form = parse_qs(body)
            title = form.get('title', [''])[0]
            raw_text = form.get('text', [''])[0]
            source = form.get('source', [''])[0]
            status_label = form.get('status_label', ['رسمی'])[0]
            custom_hashtags = form.get('custom_hashtags', [''])[0]
            ai_rewrite_enabled = form.get('ai_rewrite_enabled', [''])[0] == 'on'

            nid, warn = create_exclusive_draft(title, raw_text, source, status_label, custom_hashtags, ai_rewrite_enabled)
            if nid is None:
                msg_text = warn or "خطا در ثبت خبر."
            else:
                msg_text = warn or f"✅ خبر اختصاصی #{nid} به‌صورت پیش‌نویس ثبت شد."
            import urllib.parse
            self.send_response(303)
            self.send_header('Location', '/?tab=exclusive&msg=' + urllib.parse.quote(msg_text))
            self.end_headers()
        elif path == '/media_manual_upload':
            fields, files = self._parse_multipart()
            caption = fields.get('caption', '')
            if 'file' not in files:
                msg_text = "❌ فایلی انتخاب نشد."
            else:
                filename, file_bytes = files['file']
                job_id, err = save_manual_media_upload(file_bytes, filename, caption)
                if job_id is None:
                    msg_text = f"❌ {err}"
                elif err:
                    msg_text = f"✅ Job #{job_id} ثبت شد. {err}"
                else:
                    msg_text = f"✅ فایل ذخیره و برای ارسال به صف Tiger Media افزوده شد (Job #{job_id})."
            import urllib.parse
            self.send_response(303)
            self.send_header('Location', '/?tab=media&msg=' + urllib.parse.quote(msg_text))
            self.end_headers()
        else:
            self.send_response(404); self.end_headers()

def run_web_panel():
    port = 8080
    for _ in range(5):
        try:
            server = ReusableTCPServer(('0.0.0.0', port), DashboardHandler)
            log.info("🌐 پنل وب روی پورت %d فعال شد", port)
            server.serve_forever()
            break
        except OSError as e:
            if e.errno == 98: port += 1
            else: break

# ============================================================
# 📤 ارتباطات ایتا و روبیکا
# ============================================================
def send_to_eitaa(text):
    if not text: return False
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, data={"chat_id": EITAA_CHAT_ID, "text": text}, timeout=10)
        return resp.status_code == 200
    except: return False

def send_file_to_eitaa(file_path, caption):
    url = f"https://eitaayar.ir/api/{EITAA_TOKEN}/sendFile"
    try:
        with open(file_path, "rb") as f:
            resp = requests.post(url, data={"chat_id": EITAA_CHAT_ID, "caption": caption}, files={"file": (os.path.basename(file_path), f)}, timeout=45)
            return resp.status_code == 200
    except: return False

def get_media_ext(msg):
    msg_type = str(getattr(msg, "type", None) or (msg.get("type") if isinstance(msg, dict) else "")).lower()
    if 'video' in msg_type: return '.mp4'
    if 'image' in msg_type or 'photo' in msg_type: return '.jpg'
    return None

async def get_clean_messages(app, guid):
    try:
        res = await asyncio.wait_for(app.get_messages(guid, 0, 15), timeout=7.0)
        raw_list = getattr(res, "messages", None) or (res.get("messages", []) if isinstance(res, dict) else [])
        parsed = []
        for m in raw_list:
            m_id = getattr(m, "message_id", None) or (m.get("message_id") if isinstance(m, dict) else None)
            txt = str(getattr(m, "text", "") or (m.get("text", "") if isinstance(m, dict) else "")).strip()
            if m_id: parsed.append({"id": int(str(m_id)), "text": txt, "type": getattr(m, "type", None) or m.get("type"), "raw": m})
        return parsed
    except: return []

# ============================================================
# 🎬 Media Engine (تعمیر و ارتقای اختصاصی این نسخه)
# ------------------------------------------------------------
# نکته صداقت فنی (طبق درخواست صریح کاربر):
# در محیط اجرای این تغییرات، Network برای bash غیرفعال است و امکان
# pip install واقعی rubpy==7.3.5 یا اجرای زنده در برابر سرورهای واقعی
# Rubika/Eitaa وجود ندارد. به همین دلیل، به‌جای حدس‌زدن نام دقیق فیلدهای
# داخلی شیء Message در rubpy 7.3.5، از یک لایه Introspection دفاعی
# (_get) استفاده شده که فقط از مقادیری استفاده می‌کند که واقعاً روی خود
# پیام دریافتی موجود باشند؛ در غیر این صورت مقدار None می‌ماند (بدون
# ساخت مقدار ساختگی). فراخوانی واقعی دانلود همان app.download(raw_msg)
# فعلی پروژه است که از قبل در تولید استفاده می‌شده و تغییر داده نشده؛
# فقط با Timeout/Retry/Backoff/Validation/Atomic-Write احاطه شده است.
# جزئیات کامل در گزارش نهایی (NOT VERIFIED بخش‌های تأییدنشده) آمده است.
# ============================================================

def _media_get(obj, *names):
    """دسترسی امن و Introspective به فیلدهای یک Message/Media بدون حدس‌زدن ساختار."""
    if obj is None:
        return None
    for n in names:
        if isinstance(obj, dict):
            if n in obj and obj[n] is not None:
                return obj[n]
        else:
            v = getattr(obj, n, None)
            if v is not None:
                return v
    return None


def detect_media_info(raw_msg):
    """
    تشخیص واقعی نوع Media از روی پیام دریافتی.
    - رفتار پایه تشخیص نوع (photo/video) دقیقاً بر همان منطق فعلی و
      از قبل کارکرده get_media_ext تکیه دارد (بدون تغییر آن تابع).
    - Document/Voice/Music/Gif به‌صورت افزوده (غیرمخرب) شناسایی می‌شوند.
    - هیچ مقدار ساختگی برای file_id/file_name/mime_type ساخته نمی‌شود؛
      اگر در پیام موجود نبودند مقدار None باقی می‌ماند.
    خروجی: dict شامل media_type/file_id/file_name/mime_type/extension یا None اگر Media نیست.
    """
    msg_type_raw = _media_get(raw_msg, "type")
    msg_type = str(msg_type_raw or "").lower()

    media_type = None
    if "video" in msg_type:
        media_type = "video"
    elif "image" in msg_type or "photo" in msg_type or "picture" in msg_type:
        media_type = "photo"
    elif "gif" in msg_type:
        media_type = "gif"
    elif "voice" in msg_type or "music" in msg_type or "audio" in msg_type:
        media_type = "audio"
    elif "file" in msg_type or "document" in msg_type:
        media_type = "document"

    if not media_type:
        return None  # پیام Media نیست (یا با اطلاعات موجود روی Message قابل تشخیص نبود)

    file_inline = _media_get(raw_msg, "file_inline", "file", "media", "document")

    file_id = _media_get(file_inline, "file_id") or _media_get(raw_msg, "file_id")
    file_name = _media_get(file_inline, "file_name") or _media_get(raw_msg, "file_name")
    mime_type = _media_get(file_inline, "mime") or _media_get(raw_msg, "mime")

    extension = None
    if file_name and "." in str(file_name):
        extension = os.path.splitext(str(file_name))[1] or None
    if not extension:
        # حفظ کامل منطق فعلی و از قبل کار‌کرده پروژه برای پسوند photo/video
        extension = get_media_ext(raw_msg)
    if not extension and media_type == "photo":
        extension = ".jpg"
    elif not extension and media_type == "video":
        extension = ".mp4"
    # برای document/audio/gif در صورت نبود پسوند واقعی، مقدار None باقی می‌ماند (بدون حدس)

    return {
        "media_type": media_type,
        "file_id": str(file_id) if file_id is not None else None,
        "file_name": str(file_name) if file_name is not None else None,
        "mime_type": str(mime_type) if mime_type is not None else None,
        "extension": extension,
    }


# ---------------- دیتابیس Media Job ----------------

def create_or_get_media_job(source_name, message_id, media_type, file_id, file_name, mime_type, extension, caption):
    """
    ایجاد Job جدید یا بازگرداندن Job موجود (Duplicate Protection واقعی
    از طریق UNIQUE(source_name, message_id)).
    خروجی: شناسه (id) رکورد Job.
    """
    conn = sqlite3.connect(DB_FILE, timeout=30)
    try:
        conn.execute(
            """INSERT OR IGNORE INTO media_jobs
               (source_name, message_id, media_type, file_id, file_name, mime_type, extension, caption, stage)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'MEDIA_DETECTED')""",
            (source_name, int(message_id), media_type, file_id, file_name, mime_type, extension, caption),
        )
        conn.commit()
        row = conn.execute(
            "SELECT id FROM media_jobs WHERE source_name = ? AND message_id = ?",
            (source_name, int(message_id)),
        ).fetchone()
        if row:
            log.info("[MEDIA] Job created #%s", row[0])
        return row[0] if row else None
    finally:
        conn.close()


def get_media_job_by_id(job_id):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    row = conn.execute(
        """SELECT id, source_name, message_id, media_type, file_id, file_name, mime_type,
                  extension, caption, file_path, file_size, download_status, send_status,
                  download_attempts, upload_attempts, stage
           FROM media_jobs WHERE id = ?""",
        (job_id,),
    ).fetchone()
    conn.close()
    return row


def update_media_job(job_id, **fields):
    if not fields:
        return
    fields = dict(fields)
    fields["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [job_id]
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute(f"UPDATE media_jobs SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


def get_recoverable_media_jobs():
    """Jobهایی که Download موفق بوده ولی Upload هنوز کامل نشده (برای Recovery بعد از Restart)."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    rows = conn.execute(
        """SELECT id, source_name, message_id, file_path, caption
           FROM media_jobs
           WHERE download_status = 'SUCCESS' AND send_status != 'SUCCESS'"""
    ).fetchall()
    conn.close()
    return rows


def cleanup_old_media_jobs_and_files(hours_to_keep=MEDIA_JOB_RETENTION_HOURS):
    """پاکسازی دوره‌ای رکوردهای Media Job که کاملاً تمام (موفق یا شکست نهایی) و قدیمی هستند."""
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.execute(
        f"""DELETE FROM media_jobs
            WHERE stage IN ('COMPLETED', 'SEND_FAILED_FINAL', 'DOWNLOAD_FAILED_FINAL')
              AND updated_at < datetime('now', '-{int(hours_to_keep)} hours')"""
    )
    conn.commit()
    conn.close()


def cleanup_media_file(job_id, file_path):
    """حذف فایل Temporary فقط بعد از موفقیت نهایی ارسال یا شکست نهایی قطعی Job."""
    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
            log.info("🧹 فایل موقت Media پاکسازی شد [Job #%s]", job_id)
    except Exception as e:
        log.warning("خطا در پاکسازی فایل Media [Job #%s]: %s", job_id, e)


def get_unified_media_status(stage, download_status, send_status):
    """
    نگاشت وضعیت داخلی فعلی (stage/download_status/send_status) به ۷ وضعیت استاندارد
    Tiger Media V12، بدون تغییر منطق داخلی موجود:
    PENDING, PROCESSING, UPLOADING, SENT, FAILED, RETRYING, CANCELLED
    """
    if stage == "CANCELLED":
        return "CANCELLED"
    if send_status == "SUCCESS" or stage == "COMPLETED":
        return "SENT"
    if stage in ("DOWNLOAD_FAILED_FINAL", "SEND_FAILED_FINAL"):
        return "FAILED"
    if stage == "SEND_STARTED" or stage == "FILE_VALIDATED":
        return "UPLOADING"
    if stage in ("DOWNLOAD_STARTED",):
        return "PROCESSING"
    if download_status == "FAILED" or send_status == "FAILED":
        return "RETRYING"
    return "PENDING"


def list_recent_media_jobs(limit=25):
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT id, source_name, message_id, media_type, stage, download_status, send_status,
                  download_attempts, upload_attempts, error, created_at, updated_at
           FROM media_jobs ORDER BY id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return rows


def get_media_job_counts():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    rows = conn.execute("SELECT stage, download_status, send_status FROM media_jobs").fetchall()
    conn.close()
    counts = {"PENDING": 0, "PROCESSING": 0, "UPLOADING": 0, "SENT": 0, "FAILED": 0, "RETRYING": 0, "CANCELLED": 0}
    for stage, dl, sd in rows:
        counts[get_unified_media_status(stage, dl, sd)] += 1
    return counts


def _guess_manual_media_type(filename: str):
    ext = os.path.splitext(filename)[1].lower()
    if ext in (".jpg", ".jpeg", ".png", ".webp"): return "photo", (ext if ext != ".jpeg" else ".jpg")
    if ext in (".mp4", ".mov", ".mkv"): return "video", ext
    if ext in (".mp3", ".ogg", ".wav", ".m4a"): return "audio", ext
    if ext in (".gif",): return "gif", ext
    return "document", (ext or ".dat")


def save_manual_media_upload(file_bytes: bytes, original_filename: str, caption: str):
    """
    آپلود دستی رسانه از پنل ادمین (بدون نیاز به منبع Rubika).
    از همان جدول/State Machine Tiger Media استفاده می‌کند - Job جدید با
    source_name='manual_upload' ساخته می‌شود و چون فایل از قبل روی دیسک است،
    مرحله Download اصلاً لازم نیست (مستقیم FILE_VALIDATED).
    خروجی: (job_id, error_message_or_None)
    """
    if not file_bytes:
        return None, "فایل خالی است."

    media_type, ext = _guess_manual_media_type(original_filename or "")
    # message_id مصنوعی و یکتا (بر اساس زمان به میلی‌ثانیه) تا با UNIQUE(source_name, message_id) تداخل نکند
    synthetic_message_id = int(time.time() * 1000)

    job_id = create_or_get_media_job(
        source_name="manual_upload",
        message_id=synthetic_message_id,
        media_type=media_type,
        file_id=None,
        file_name=original_filename,
        mime_type=None,
        extension=ext,
        caption=caption or "",
    )
    if not job_id:
        return None, "خطا در ثبت Job در دیتابیس."

    os.makedirs(MEDIA_DIR, exist_ok=True)
    file_path = os.path.join(MEDIA_DIR, f"{job_id}{ext}")
    tmp_path = file_path + ".part"
    try:
        with open(tmp_path, "wb") as f:
            f.write(file_bytes)
            f.flush()
            os.fsync(f.fileno())
        if os.path.getsize(tmp_path) == 0:
            raise ValueError("فایل آپلودشده Zero-byte است.")
        os.replace(tmp_path, file_path)
    except Exception as e:
        if os.path.exists(tmp_path):
            try: os.remove(tmp_path)
            except Exception: pass
        update_media_job(job_id, stage="DOWNLOAD_FAILED_FINAL", download_status="FAILED", error=str(e))
        return job_id, f"خطا در ذخیره فایل: {e}"

    file_size = os.path.getsize(file_path)
    file_hash = hashlib.sha256(file_bytes).hexdigest()
    update_media_job(
        job_id, download_status="SUCCESS", stage="FILE_VALIDATED",
        file_path=file_path, file_size=file_size, file_hash=file_hash, error=None,
    )
    log.info("[MEDIA] Job created via manual upload #%s (%s)", job_id, media_type)

    # اگر ربات در حال اجراست، بلافاصله ارسال را روی همان Event Loop زمان‌بندی کن؛
    # در غیر این صورت، Job با download_status=SUCCESS باقی می‌ماند و توسط
    # recover_pending_media_jobs در دفعه بعد Restart (یا اگر Worker دوره‌ای اضافه شود) ارسال خواهد شد.
    if bot_loop:
        asyncio.run_coroutine_threadsafe(upload_media_file(job_id, file_path, caption or ""), bot_loop)
        return job_id, None
    else:
        return job_id, "⚠️ ربات در حال اجرا نیست - فایل ذخیره شد و در اجرای بعدی ارسال خواهد شد."


def cancel_media_job(job_id):
    """
    Cancel یک Media Job: فقط برای Jobهایی که هنوز به SENT/COMPLETED نرسیده‌اند معنا دارد.
    Job لغوشده دیگر توسط process_media_job/Recovery پردازش نمی‌شود (بدون خراب کردن News Pipeline).
    """
    job = get_media_job_by_id(job_id)
    if not job: return False, "Job یافت نشد."
    if job[12] == "SUCCESS":  # send_status
        return False, "این Job قبلاً با موفقیت ارسال شده - قابل لغو نیست."
    update_media_job(job_id, stage="CANCELLED", error="لغو دستی توسط ادمین")
    log.info("[MEDIA] Job #%s Cancelled", job_id)
    return True, "✅ Job لغو شد."


def cleanup_orphan_media_files():
    """
    فایل‌های Temporary روی دیسک که به هیچ Media Job فعالی (نیمه‌کاره) مرتبط
    نیستند را بعد از Restart پاکسازی می‌کند. به Jobهای نیمه‌کاره واقعی دست نمی‌زند.
    """
    try:
        conn = sqlite3.connect(DB_FILE, timeout=30)
        active_paths = {
            r[0] for r in conn.execute(
                "SELECT file_path FROM media_jobs WHERE file_path IS NOT NULL AND send_status != 'SUCCESS'"
            ).fetchall()
        }
        conn.close()
        if not os.path.isdir(MEDIA_DIR):
            return
        for fname in os.listdir(MEDIA_DIR):
            fpath = os.path.join(MEDIA_DIR, fname)
            if not os.path.isfile(fpath):
                continue
            if fpath not in active_paths:
                try:
                    os.remove(fpath)
                    log.info("🧹 فایل Orphan Media پاکسازی شد: %s", fname)
                except Exception as e:
                    log.warning("خطا در پاکسازی فایل Orphan %s: %s", fname, e)
    except Exception as e:
        log.warning("خطا در بررسی فایل‌های Orphan Media: %s", e)


# ---------------- Download Engine (مستقل) ----------------

async def download_media_file(app, job_id, raw_msg, target_ext):
    """
    Download مستقل و قابل‌اعتماد یک Media با Timeout/Retry/Exponential-Backoff/
    Zero-byte-detection/Temporary-file/Atomic-rename.
    خروجی: مسیر فایل نهایی در صورت موفقیت، یا None در صورت شکست کل تلاش‌های این Pass.
    """
    ext = target_ext or ".dat"
    file_path = os.path.join(MEDIA_DIR, f"{job_id}{ext}")
    tmp_path = file_path + ".part"

    job = get_media_job_by_id(job_id)
    base_attempts = job[13] if job else 0

    for i in range(1, MEDIA_DOWNLOAD_RETRIES_PER_PASS + 1):
        total_attempt = base_attempts + i
        update_media_job(job_id, stage="DOWNLOAD_STARTED", download_attempts=total_attempt)
        try:
            file_bytes = await asyncio.wait_for(app.download(raw_msg), timeout=MEDIA_DOWNLOAD_TIMEOUT_SEC)

            if not file_bytes:
                raise ValueError("خروجی Download خالی/None بود")
            if len(file_bytes) == 0:
                raise ValueError("فایل دانلودشده Zero-byte است")

            with open(tmp_path, "wb") as f:
                f.write(file_bytes)
                f.flush()
                os.fsync(f.fileno())

            if os.path.getsize(tmp_path) == 0:
                raise ValueError("فایل موقت Zero-byte است")

            os.replace(tmp_path, file_path)  # Atomic rename - جلوگیری از overwrite ناقص

            file_size = os.path.getsize(file_path)
            file_hash = hashlib.sha256(file_bytes).hexdigest()

            update_media_job(
                job_id, download_status="SUCCESS", stage="FILE_VALIDATED",
                file_path=file_path, file_size=file_size, file_hash=file_hash, error=None,
            )
            log.info("📥 Download Media موفق [Job #%s] تلاش کل #%d", job_id, total_attempt)
            return file_path

        except Exception as e:
            err_msg = f"تلاش {total_attempt} ناموفق: {e}"
            log.warning("⚠️ خطای Download Media [Job #%s]: %s", job_id, err_msg)
            update_media_job(job_id, download_status="FAILED", stage="DOWNLOAD_FAILED", error=err_msg)
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
            if i < MEDIA_DOWNLOAD_RETRIES_PER_PASS:
                await asyncio.sleep(MEDIA_DOWNLOAD_BACKOFF_BASE ** i)

    return None


# ---------------- Upload Engine (مستقل از Download) ----------------

async def upload_media_file(job_id, file_path, caption):
    """
    Upload مستقل به ایتا. اگر Download قبلاً موفق بوده، این تابع هیچ‌گاه
    دوباره Download انجام نمی‌دهد و فقط از فایل محلی موجود استفاده می‌کند.
    """
    job = get_media_job_by_id(job_id)
    base_attempts = job[14] if job else 0

    for i in range(1, MEDIA_UPLOAD_RETRIES_PER_PASS + 1):
        total_attempt = base_attempts + i
        update_media_job(job_id, stage="SEND_STARTED", upload_attempts=total_attempt)
        log.info("[MEDIA] Uploading [Job #%s]", job_id)
        try:
            if not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
                raise ValueError("فایل محلی برای Upload معتبر نیست (وجود ندارد یا Zero-byte)")

            ok = await asyncio.to_thread(send_file_to_eitaa, file_path, caption)
            if not ok:
                raise ValueError("پاسخ ناموفق از ایتا (sendFile)")

            update_media_job(job_id, send_status="SUCCESS", stage="SEND_SUCCESS", error=None)
            log.info("[MEDIA] Sent [Job #%s]", job_id)
            log.info("📤 Upload Media موفق [Job #%s] تلاش کل #%d", job_id, total_attempt)
            return True

        except Exception as e:
            err_msg = f"تلاش {total_attempt} ناموفق: {e}"
            log.warning("⚠️ خطای Upload Media [Job #%s]: %s", job_id, err_msg)
            update_media_job(job_id, send_status="FAILED", stage="SEND_FAILED", error=err_msg)
            if i < MEDIA_UPLOAD_RETRIES_PER_PASS:
                log.info("[MEDIA] Retry %d/%d [Job #%s]", i, MEDIA_UPLOAD_RETRIES_PER_PASS, job_id)
                await asyncio.sleep(MEDIA_UPLOAD_BACKOFF_BASE ** i)

    return False


# ---------------- State Machine اصلی یک Media Job ----------------

async def process_media_job(app, job_id, raw_msg, caption):
    """
    اجرای کامل State Machine یک Media Job:
    RECEIVED -> MEDIA_DETECTED -> DOWNLOAD_STARTED -> DOWNLOAD_SUCCESS/DOWNLOAD_FAILED
             -> FILE_VALIDATED -> SEND_STARTED -> SEND_SUCCESS/SEND_FAILED -> COMPLETED

    قانون حیاتی (Acceptance Test اصلی این نسخه):
    اگر Download قبلاً SUCCESS بوده و فایل محلی هنوز معتبر است، این تابع
    هرگز دوباره Download انجام نمی‌دهد؛ فقط Upload دوباره تلاش می‌شود.
    """
    job = get_media_job_by_id(job_id)
    if not job:
        return False

    (_id, source_name, message_id, media_type, file_id, file_name, mime_type,
     extension, db_caption, file_path, file_size, download_status, send_status,
     download_attempts, upload_attempts, stage) = job

    if send_status == "SUCCESS":
        log.info("⏭️ Media تکراری/قبلاً ارسال‌شده - نادیده گرفته شد [Job #%s | %s#%s]",
                  job_id, source_name, message_id)
        return True

    if stage == "CANCELLED":
        log.info("⏭️ Media Job لغوشده - پردازش نادیده گرفته شد [Job #%s]", job_id)
        return False

    final_caption = db_caption if db_caption else caption

    # --- مرحله Download ---
    file_already_valid = (
        download_status == "SUCCESS" and file_path and os.path.exists(file_path) and os.path.getsize(file_path) > 0
    )
    if file_already_valid:
        log.info("♻️ فایل قبلاً Download شده - از Download مجدد صرف‌نظر شد [Job #%s]", job_id)
    else:
        if download_attempts >= MEDIA_MAX_TOTAL_ATTEMPTS:
            update_media_job(job_id, stage="DOWNLOAD_FAILED_FINAL", error="سقف کلی تلاش‌های Download تمام شد")
            log.error("❌ شکست نهایی Download [Job #%s]", job_id)
            return False

        ext = extension or get_media_ext(raw_msg) or ".dat"
        downloaded_path = await download_media_file(app, job_id, raw_msg, ext)
        if not downloaded_path:
            refreshed = get_media_job_by_id(job_id)
            if refreshed and refreshed[13] >= MEDIA_MAX_TOTAL_ATTEMPTS:
                update_media_job(job_id, stage="DOWNLOAD_FAILED_FINAL")
                log.error("❌ شکست نهایی Download بعد از رسیدن به سقف کلی تلاش‌ها [Job #%s]", job_id)
            return False
        file_path = downloaded_path

    # --- مرحله Upload (کاملاً مستقل از Download) ---
    refreshed = get_media_job_by_id(job_id)
    current_upload_attempts = refreshed[14] if refreshed else upload_attempts
    if current_upload_attempts >= MEDIA_MAX_TOTAL_ATTEMPTS:
        update_media_job(job_id, stage="SEND_FAILED_FINAL", error="سقف کلی تلاش‌های Upload تمام شد")
        cleanup_media_file(job_id, file_path)
        log.error("❌ شکست نهایی Upload [Job #%s]", job_id)
        return False

    ok = await upload_media_file(job_id, file_path, final_caption)
    if ok:
        update_media_job(job_id, stage="COMPLETED")
        cleanup_media_file(job_id, file_path)
        return True

    refreshed = get_media_job_by_id(job_id)
    if refreshed and refreshed[14] >= MEDIA_MAX_TOTAL_ATTEMPTS:
        update_media_job(job_id, stage="SEND_FAILED_FINAL")
        cleanup_media_file(job_id, file_path)
        log.error("❌ شکست نهایی Upload بعد از رسیدن به سقف کلی تلاش‌ها [Job #%s]", job_id)
    return False


async def recover_pending_media_jobs(app):
    """
    Recovery بعد از Restart: Jobهایی که Download موفق بوده ولی هنوز کامل
    ارسال نشده‌اند، بدون Download مجدد مستقیماً وارد تلاش Upload می‌شوند.
    """
    jobs = get_recoverable_media_jobs()
    if not jobs:
        return
    log.info("♻️ %d Media Job نیمه‌کاره برای Recovery بعد از Restart پیدا شد", len(jobs))
    for job_id, source_name, message_id, file_path, caption in jobs:
        if not file_path or not os.path.exists(file_path) or os.path.getsize(file_path) == 0:
            update_media_job(
                job_id, stage="DOWNLOAD_FAILED_FINAL", download_status="FAILED",
                error="فایل موقت بعد از Restart موجود نبود (Orphan/از دست رفته)",
            )
            log.warning("⚠️ فایل Job #%s بعد از Restart یافت نشد - Recovery ممکن نشد", job_id)
            continue
        try:
            await process_media_job(app, job_id, raw_msg=None, caption=caption or "")
        except Exception as e:
            log.error("خطا در Recovery Media Job #%s: %s", job_id, e)
        await asyncio.sleep(1)


async def media_cleanup_task():
    """پاکسازی دوره‌ای Jobهای کاملاً تمام‌شده Media - کاملاً مستقل از Hourly Analyzer/Bulletin."""
    while True:
        await asyncio.sleep(3600)
        try:
            await asyncio.to_thread(cleanup_old_media_jobs_and_files, MEDIA_JOB_RETENTION_HOURS)
        except Exception as e:
            log.warning("خطا در پاکسازی دوره‌ای Media Jobs: %s", e)


# ============================================================
# ⚡ موتور پردازش مستقیم اخبار
# ============================================================
async def process_queue_worker(app):
    while True:
        source_name, m_id, raw_text, msg_type, raw_msg = await news_queue.get()
        try:
            log.info("[NORMAL] News received [%s | msg %s]", source_name, m_id)
            if raw_text:
                final_text = append_smart_tags(safe_text_cleaner(raw_text))
            else:
                final_text = "📸 #گزارش_تصویری \n\n────────────\n#محور_مقاومت #شاهد_۱۱۰\n📡 @shahed_news110"
            log.info("[NORMAL] AI bypassed - rule-based formatting only")
            log.info("[NORMAL] Formatting completed")

            sent = False

            try:
                media_info = detect_media_info(raw_msg)
            except Exception as det_err:
                log.error("خطا در تشخیص Media [%s | msg %s]: %s", source_name, m_id, det_err)
                media_info = None

            if media_info:
                # --- مسیر Media Engine جدید ---
                job_id = create_or_get_media_job(
                    source_name, m_id, media_info["media_type"], media_info["file_id"],
                    media_info["file_name"], media_info["mime_type"], media_info["extension"],
                    final_text,
                )
                try:
                    sent = await process_media_job(app, job_id, raw_msg, final_text)
                except Exception as media_err:
                    log.error("خطا در پردازش Media Job #%s [%s | msg %s]: %s",
                              job_id, source_name, m_id, media_err)
                    sent = False
            else:
                # --- مسیر متن (دقیقاً همان منطق قبلی، بدون تغییر) ---
                log.info("[NORMAL] Sending...")
                sent = await asyncio.to_thread(send_to_eitaa, final_text)

            if sent:
                save_last_id(source_name, m_id)
                await asyncio.to_thread(add_to_hourly_buffer, source_name, raw_text)
                log.info("[NORMAL] Sent successfully")
                log.info("✅ خبر ارسال شد [%s]", source_name)
        except Exception as err:
            log.error("خطا در صف: %s", err)
        finally:
            news_queue.task_done()
            await asyncio.sleep(SEND_INTERVAL)

# ============================================================
# 👁️ رصد کانال‌ها
# ============================================================
async def monitor_source(app, source_name, guid):
    log.info("📺 رصد فعال شد: %s", source_name)
    last_id = get_last_id(source_name)
    try:
        while True:
            try:
                msgs = await get_clean_messages(app, guid)
                if msgs:
                    if last_id is None: 
                        last_id = max([m["id"] for m in msgs])
                        save_last_id(source_name, last_id)
                    else:
                        new_msgs = sorted([m for m in msgs if m["id"] > last_id], key=lambda x: x["id"])
                        for m in new_msgs:
                            await news_queue.put((source_name, m["id"], m["text"], m["type"], m["raw"]))
                            last_id = m["id"]
            except asyncio.CancelledError:
                raise
            except: pass
            await asyncio.sleep(POLL_INTERVAL)
    except asyncio.CancelledError:
        log.info(f"🛑 رصد منبع {source_name} لغو شد.")
        raise

# ============================================================
# ⏰ تسک‌های تحلیلی (ساعتی + سفارشی)
# ============================================================
async def hourly_analyzer_task():
    while True:
        now = datetime.now()
        seconds_to_wait = (60 - now.minute - 1) * 60 + (60 - now.second)
        if seconds_to_wait <= 0: seconds_to_wait = 3600

        await asyncio.sleep(seconds_to_wait)
        
        # پاکسازی اخبار قدیمی‌تر از 48 ساعت برای سبک شدن دیتابیس
        await asyncio.to_thread(cleanup_old_news, 48)

        # دریافت اخبار 1 ساعت اخیر (برای تحلیل اصلی)
        main_news_list = await asyncio.to_thread(get_recent_news, 1)
        if not main_news_list: continue
        
        # دریافت اخبار 1 تا 3 ساعت قبل (برای پس‌زمینه و گریز زدن)
        context_news_list = await asyncio.to_thread(get_news_between, 1, 3)

        combined_main = "\n\n---\n\n".join(main_news_list)
        combined_context = "\n\n---\n\n".join(context_news_list) if context_news_list else "(خبری برای پس‌زمینه ثبت نشده است)"
        
        ai_prompt_data = f"📌 [اخبار اصلی - یک ساعت اخیر]:\n{combined_main}\n\n🔍 [پس‌زمینه جهت گریز زدن - ۱ تا ۳ ساعت گذشته]:\n{combined_context}"
        
        analysis_text = await asyncio.to_thread(call_custom_ai, ai_prompt_data, False, 1)
        if not analysis_text: analysis_text = safe_text_cleaner(combined_main[:800] + "\n...(خلاصه اخبار)...")

        bulletin_header = f"🚨 **#دیده‌بان_ساعت | بولتن خبری و تحلیل راهبردی** 🚨\n⏰ ساعت: {datetime.now().strftime('%H:%M')}\n\n"
        final_analysis = append_smart_tags(bulletin_header + analysis_text)
        await asyncio.to_thread(send_to_eitaa, final_analysis)


async def generate_custom_bulletin(hours):
    log.info(f"شروع ساخت بولتن سفارشی برای {hours} ساعت گذشته...")
    news_list = await asyncio.to_thread(get_recent_news, hours)
    if not news_list:
        log.warning("برای بازه درخواستی خبری در دیتابیس موجود نیست.")
        return
        
    combined_news = "\n\n---\n\n".join(news_list)
    ai_prompt_data = f"📌 [اخبار ثبت شده]:\n{combined_news}"
    
    analysis_text = await asyncio.to_thread(call_custom_ai, ai_prompt_data, True, hours)
    if not analysis_text: analysis_text = safe_text_cleaner(combined_news[:800] + "\n...(خلاصه)...")

    bulletin_header = f"🔥 **#بولتن_ویژه | تحلیل جامع {hours} ساعت گذشته** 🔥\n⏰ زمان انتشار: {datetime.now().strftime('%H:%M')}\n\n"
    final_analysis = append_smart_tags(bulletin_header + analysis_text)
    
    success = await asyncio.to_thread(send_to_eitaa, final_analysis)
    if success: log.info("✅ بولتن سفارشی با موفقیت به ایتا ارسال شد.")

# ============================================================
# 🚀 اجرای اصلی
# ============================================================
async def main():
    global bot_loop, RUBY_CLIENT
    bot_loop = asyncio.get_running_loop()
    
    log.info("📡 SHAHED 110 NEWS V11.0 - استارت خورد")
    
    web_thread = threading.Thread(target=run_web_panel, daemon=True)
    web_thread.start()

    session_path = os.path.join(BASE_DIR, SESSION_NAME)
    RUBY_CLIENT = Client(session_path)
    
    try:
        await RUBY_CLIENT.start()

        # --- Media Engine: پاکسازی Orphan و Recovery Jobهای نیمه‌کاره بعد از Restart ---
        await asyncio.to_thread(cleanup_orphan_media_files)
        await recover_pending_media_jobs(RUBY_CLIENT)

        asyncio.create_task(process_queue_worker(RUBY_CLIENT))
        
        # مدیریت داینامیک منابع
        await sync_monitors(RUBY_CLIENT)
        
        tasks = []
        tasks.append(asyncio.create_task(hourly_analyzer_task()))
        tasks.append(asyncio.create_task(media_cleanup_task()))
        
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        pass
    finally:
        await RUBY_CLIENT.stop()

if __name__ == "__main__":
    asyncio.run(main())
