# -*- coding: utf-8 -*-
"""
============================================================
🔔 News Trigger Engine — ماژول کاملاً مستقل برای شاهدنیوز
============================================================
این فایل هیچ خطی از sahed.py را تغییر نمی‌دهد و کاملاً جدا کار می‌کند.

قابلیت: بررسی اخبار ذخیره‌شده توسط sahed.py و ارسال یک رسانهٔ
از پیش تعیین‌شده وقتی یک عبارت در تعداد مشخصی خبر متفاوت،
طی یک بازهٔ زمانی مشخص، تکرار شود.

اجرای مستقل:
    python news_trigger_engine.py

Integration اختیاری داخل sahed.py (فقط در صورت تمایل خودتان،
هیچ خطی از sahed.py برای این کار لازم نیست تغییر کند):
    from news_trigger_engine import TriggerEngine
    TriggerEngine().start()

پنل وب مستقل این ماژول روی پورت 8090 (یا اولین پورت آزاد بعدی) بالا می‌آید،
چون پنل فعلی sahed.py (پورت 8080) یک کلاس یکپارچه است و دستکاری نشده است.
============================================================
"""

import os
import re
import time
import html
import sqlite3
import logging
import threading
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

# ------------------------------------------------------------------
# ⚙️ اتصال امن به sahed.py — فقط خواندنِ تنظیمات/توابع موجود
# هیچ Token یا مقدار جعلی ساخته نمی‌شود؛ اگر sahed.py در دسترس نبود
# سیستم صادقانه در حالت "ارسال غیرفعال" کار می‌کند (نه یک مقدار ساختگی).
# ------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

try:
    from sahed import DB_FILE as SAHED_DB_FILE, send_file_to_eitaa, BASE_DIR as SAHED_BASE_DIR
    BASE_DIR = SAHED_BASE_DIR
    SAHED_AVAILABLE = True
except Exception as _imp_err:
    SAHED_DB_FILE = os.path.join(BASE_DIR, "shahed_news_mobile.db")
    send_file_to_eitaa = None
    SAHED_AVAILABLE = False
    _SAHED_IMPORT_ERROR = str(_imp_err)

# ------------------------------------------------------------------
# ⚙️ ثابت‌های پیکربندی این ماژول (کاملاً مجزا از sahed.py)
# ------------------------------------------------------------------
TRIGGER_DB_FILE = os.path.join(BASE_DIR, "trigger_engine.db")
TRIGGER_MEDIA_DIR = os.path.join(BASE_DIR, "trigger_media")
os.makedirs(TRIGGER_MEDIA_DIR, exist_ok=True)

TRIGGER_PANEL_PORT = 8090
DEFAULT_SCHEDULER_INTERVAL_SECONDS = 30
MEDIA_SEND_MAX_RETRIES = 3
MEDIA_SEND_RETRY_DELAY_SECONDS = 5
MAX_UPLOAD_SIZE_BYTES = 50 * 1024 * 1024  # 50MB
ALLOWED_MEDIA_EXTENSIONS = {".mp3", ".m4a", ".wav", ".ogg", ".mp4", ".jpg", ".jpeg", ".png", ".webp"}
TRIGGER_EVENTS_RETENTION_HOURS = 168  # 7 روز

TIME_WINDOW_PRESETS_MINUTES = [5, 10, 15, 30, 60, 120, 180, 360, 720, 1440]
COOLDOWN_PRESETS_HOURS = [0.5, 1, 2, 3, 6, 12, 24]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("TRIGGER_ENGINE")

if not SAHED_AVAILABLE:
    log.warning("⚠️ ماژول sahed.py در دسترس نیست (%s) - ارسال رسانه غیرفعال خواهد بود تا وقتی کنار sahed.py اجرا شود.",
                _SAHED_IMPORT_ERROR)


# ============================================================
# 🗃️ Database — trigger_engine.db (کاملاً مجزا از دیتابیس شاهدنیوز)
# ============================================================
def init_trigger_db():
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute("""CREATE TABLE IF NOT EXISTS triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    keyword TEXT NOT NULL,
                    min_count INTEGER NOT NULL DEFAULT 20,
                    window_seconds INTEGER NOT NULL DEFAULT 3600,
                    media_filename TEXT,
                    destination TEXT DEFAULT 'sahed_default',
                    enabled INTEGER DEFAULT 1,
                    cooldown_seconds INTEGER DEFAULT 21600,
                    last_triggered_at DATETIME,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS trigger_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trigger_id INTEGER NOT NULL,
                    triggered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    match_count INTEGER,
                    media_filename TEXT,
                    status TEXT DEFAULT 'pending',
                    is_test INTEGER DEFAULT 0,
                    is_dry_run INTEGER DEFAULT 0,
                    error TEXT
                )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS trigger_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )""")
    conn.commit()
    conn.close()


def get_setting(key, default=None):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    row = conn.execute("SELECT value FROM trigger_settings WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key, value):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute(
        "INSERT INTO trigger_settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )
    conn.commit()
    conn.close()


def get_scheduler_interval():
    try:
        return max(5, int(get_setting("scheduler_interval_seconds", DEFAULT_SCHEDULER_INTERVAL_SECONDS)))
    except (TypeError, ValueError):
        return DEFAULT_SCHEDULER_INTERVAL_SECONDS


def is_global_dry_run():
    return get_setting("global_dry_run", "0") == "1"


def recover_stuck_events_on_startup():
    """Crash Safety: اگر Event وسط ارسال (pending/sending) مانده بود، آن را failed علامت می‌زند
    تا در چرخهٔ بعدی Scheduler، شرط از نو و صحیح بررسی شود (نه ارسال کورکورانه مجدد)."""
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute(
        "UPDATE trigger_events SET status='failed', error='Interrupted by restart (crash safety)' "
        "WHERE status IN ('pending','sending')"
    )
    conn.commit()
    conn.close()


def cleanup_old_trigger_data(hours_to_keep=TRIGGER_EVENTS_RETENTION_HOURS):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute(f"DELETE FROM trigger_events WHERE triggered_at < datetime('now', '-{int(hours_to_keep)} hours')")
    conn.commit()
    conn.close()


# ---------------- CRUD تریگرها ----------------

def create_trigger(name, keyword, min_count, window_seconds, media_filename, cooldown_seconds, enabled=1):
    name = (name or "").strip()
    keyword = (keyword or "").strip()
    if not name or not keyword:
        return None, "نام و عبارت (Keyword) نمی‌توانند خالی باشند."
    min_count = max(1, int(min_count or 1))
    window_seconds = max(10, int(window_seconds or 3600))
    cooldown_seconds = max(0, int(cooldown_seconds or 0))
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO triggers (name, keyword, min_count, window_seconds, media_filename, cooldown_seconds, enabled)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (name, keyword, min_count, window_seconds, media_filename or None, cooldown_seconds, int(bool(enabled))),
    )
    conn.commit()
    tid = cur.lastrowid
    conn.close()
    return tid, None


def get_all_triggers():
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM triggers ORDER BY id DESC").fetchall()
    conn.close()
    return rows


def get_trigger_by_id(trigger_id):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM triggers WHERE id = ?", (trigger_id,)).fetchone()
    conn.close()
    return row


def get_active_triggers():
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM triggers WHERE enabled = 1").fetchall()
    conn.close()
    return rows


def toggle_trigger(trigger_id, enabled):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute("UPDATE triggers SET enabled = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                 (int(bool(enabled)), trigger_id))
    conn.commit()
    conn.close()


def delete_trigger(trigger_id):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute("DELETE FROM triggers WHERE id = ?", (trigger_id,))
    conn.execute("DELETE FROM trigger_events WHERE trigger_id = ?", (trigger_id,))
    conn.commit()
    conn.close()


def set_trigger_last_triggered(trigger_id, when):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute("UPDATE triggers SET last_triggered_at = ?, updated_at = ? WHERE id = ?",
                 (when.strftime("%Y-%m-%d %H:%M:%S"), when.strftime("%Y-%m-%d %H:%M:%S"), trigger_id))
    conn.commit()
    conn.close()


# ---------------- رویدادها (Trigger Events) ----------------

def create_trigger_event(trigger_id, match_count, media_filename, is_test, is_dry_run):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO trigger_events (trigger_id, match_count, media_filename, status, is_test, is_dry_run)
           VALUES (?, ?, ?, 'pending', ?, ?)""",
        (trigger_id, match_count, media_filename, int(bool(is_test)), int(bool(is_dry_run))),
    )
    conn.commit()
    event_id = cur.lastrowid
    conn.close()
    return event_id


def update_trigger_event(event_id, status=None, error=None):
    fields = {}
    if status is not None:
        fields["status"] = status
    if error is not None:
        fields["error"] = error
    if not fields:
        return
    set_clause = ", ".join(f"{k} = ?" for k in fields)
    values = list(fields.values()) + [event_id]
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.execute(f"UPDATE trigger_events SET {set_clause} WHERE id = ?", values)
    conn.commit()
    conn.close()


def get_recent_events(limit=25):
    conn = sqlite3.connect(TRIGGER_DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """SELECT e.*, t.name AS trigger_name
           FROM trigger_events e LEFT JOIN triggers t ON t.id = e.trigger_id
           ORDER BY e.id DESC LIMIT ?""",
        (limit,),
    ).fetchall()
    conn.close()
    return rows


# ============================================================
# 🔤 Normalization متن فارسی + تطبیق عبارت (بدون تغییر معنی متن)
# ============================================================
_ARABIC_YE, _PERSIAN_YE = "\u064a", "\u06cc"
_ARABIC_KAF, _PERSIAN_KAF = "\u0643", "\u06a9"
_ZWNJ = "\u200c"
_PUNCT_PATTERN = re.compile(r"[!\.\,،؛:؟\?\"'`~\(\)\[\]{}<>«»_#\-–—/\\|]+")
_MULTI_SPACE = re.compile(r"\s+")


def normalize_fa_text(text):
    """نرمال‌سازی سطحی متن فارسی (بدون تغییر معنا): یکسان‌سازی ی/ي، ک/ك، حذف نیم‌فاصله
    و علائم نگارشی، و فشرده‌سازی فاصله‌ها. برای مقایسه عبارت‌ها استفاده می‌شود."""
    if not text:
        return ""
    t = text.replace(_ARABIC_YE, _PERSIAN_YE).replace(_ARABIC_KAF, _PERSIAN_KAF)
    t = t.replace(_ZWNJ, " ")
    t = _PUNCT_PATTERN.sub(" ", t)
    t = t.lower()
    t = _MULTI_SPACE.sub(" ", t).strip()
    return t


def keyword_matches(normalized_news_text, raw_keyword):
    """پشتیبانی از چند بخش با '+' (مثل 'زلزله + کرمانشاه') به‌عنوان شرط AND.
    برای عبارت واحد (اکثر Triggerها) فقط یک substring match ساده انجام می‌شود."""
    parts = [normalize_fa_text(p) for p in (raw_keyword or "").split("+")]
    parts = [p for p in parts if p]
    if not parts:
        return False
    return all(p in normalized_news_text for p in parts)


# ============================================================
# 📊 شمارش خبرهای منطبق از دیتابیس شاهدنیوز (فقط خواندن - Read Only)
# ============================================================
def count_matching_news(keyword, window_seconds, sahed_db_file=None):
    """
    اخبار جدول hourly_news_buffer شاهدنیوز را در بازهٔ زمانی داده‌شده می‌خواند
    (اتصال Read-Only - هیچ نوشتنی روی دیتابیس اصلی انجام نمی‌شود) و شناسه‌های
    یکتای اخباری که عبارت را دارند برمی‌گرداند. شناسهٔ ستون id (خودِ شاهدنیوز)
    تضمین می‌کند هر خبر فقط یک‌بار شمرده شود، حتی اگر تابع چند بار صدا زده شود.
    """
    db_file = sahed_db_file or SAHED_DB_FILE
    if not os.path.exists(db_file):
        log.warning("دیتابیس شاهدنیوز یافت نشد: %s", db_file)
        return []
    try:
        conn = sqlite3.connect(f"file:{db_file}?mode=ro", uri=True, timeout=15)
        try:
            rows = conn.execute(
                "SELECT id, text FROM hourly_news_buffer WHERE timestamp >= datetime('now', ?)",
                (f"-{int(window_seconds)} seconds",),
            ).fetchall()
        finally:
            conn.close()
    except Exception as e:
        log.warning("خطا در خواندن Read-Only اخبار شاهدنیوز: %s", e)
        return []

    matched_ids = [nid for nid, text in rows if keyword_matches(normalize_fa_text(text or ""), keyword)]
    return matched_ids


# ============================================================
# 📁 مدیریت امن فایل‌های رسانه (جلوگیری از Path Traversal)
# ============================================================
def sanitize_filename(name):
    name = os.path.basename(name or "")
    name = re.sub(r"[^A-Za-z0-9آ-ی\-\._ ]+", "_", name)
    name = name.strip().strip(".")
    return name[:150]


def list_available_media():
    try:
        files = sorted(os.listdir(TRIGGER_MEDIA_DIR))
    except FileNotFoundError:
        return []
    return [f for f in files if os.path.splitext(f)[1].lower() in ALLOWED_MEDIA_EXTENSIONS]


def resolve_media_path(filename):
    """فقط اجازهٔ خواندن فایل‌های داخل trigger_media/ با پسوند مجاز - بدون امکان Path Traversal."""
    if not filename:
        return None
    safe_name = os.path.basename(filename)
    if safe_name != filename or safe_name in ("", ".", ".."):
        return None
    ext = os.path.splitext(safe_name)[1].lower()
    if ext not in ALLOWED_MEDIA_EXTENSIONS:
        return None
    full_path = os.path.abspath(os.path.join(TRIGGER_MEDIA_DIR, safe_name))
    media_root = os.path.abspath(TRIGGER_MEDIA_DIR) + os.sep
    if not full_path.startswith(media_root):
        return None
    if not os.path.isfile(full_path):
        return None
    return full_path


# ============================================================
# 🔥 موتور اصلی ارزیابی و اجرای Trigger
# ============================================================
def fire_trigger(trg, match_count, is_test=False, is_dry_run=False):
    event_id = create_trigger_event(trg["id"], match_count, trg["media_filename"], is_test, is_dry_run)

    if is_dry_run:
        log.info("[DRY-RUN] Would trigger: %s | %d/%d | Media: %s",
                  trg["name"], match_count, trg["min_count"], trg["media_filename"])
        update_trigger_event(event_id, status="success", error="DRY_RUN - ارسال واقعی انجام نشد")
        return True

    media_path = resolve_media_path(trg["media_filename"])
    if not media_path:
        update_trigger_event(event_id, status="failed", error="فایل رسانه یافت نشد یا نامعتبر است")
        log.error("[TRIGGER] %s | [STATUS] FAILED - فایل رسانه '%s' یافت نشد", trg["name"], trg["media_filename"])
        return False

    if send_file_to_eitaa is None:
        update_trigger_event(event_id, status="failed", error="sahed.py در دسترس نیست - ارسال ممکن نشد")
        log.error("[TRIGGER] %s | [STATUS] FAILED - ماژول ارسال شاهدنیوز در دسترس نیست", trg["name"])
        return False

    update_trigger_event(event_id, status="sending")
    caption = f"🔔 {trg['name']}" + (" [TEST MODE]" if is_test else "")

    ok, last_err = False, None
    for attempt in range(1, MEDIA_SEND_MAX_RETRIES + 1):
        try:
            ok = bool(send_file_to_eitaa(media_path, caption))
        except Exception as e:
            ok, last_err = False, str(e)
        if ok:
            break
        if attempt < MEDIA_SEND_MAX_RETRIES:
            time.sleep(MEDIA_SEND_RETRY_DELAY_SECONDS)

    if ok:
        update_trigger_event(event_id, status="success")
        if not is_test:
            set_trigger_last_triggered(trg["id"], datetime.now())
        log.info("[TRIGGER] %s\n[COUNT] %d/%d\n[ACTION] Sending %s\n[STATUS] SUCCESS",
                  trg["name"], match_count, trg["min_count"], trg["media_filename"])
        return True
    else:
        update_trigger_event(event_id, status="failed", error=last_err or "ارسال ناموفق بعد از چند تلاش")
        log.error("[TRIGGER] %s | [STATUS] FAILED - %s", trg["name"], last_err)
        return False


def evaluate_single_trigger(trg, force_test=False, force_dry_run=None):
    now = datetime.now()

    if not force_test and trg["last_triggered_at"]:
        try:
            last = datetime.strptime(trg["last_triggered_at"], "%Y-%m-%d %H:%M:%S")
            remaining = trg["cooldown_seconds"] - (now - last).total_seconds()
            if remaining > 0:
                return  # هنوز در Cooldown
        except (TypeError, ValueError):
            pass

    matched_ids = count_matching_news(trg["keyword"], trg["window_seconds"])
    count = len(matched_ids)
    dry_run = is_global_dry_run() if force_dry_run is None else force_dry_run

    if not force_test and count < trg["min_count"]:
        return  # هنوز به آستانه نرسیده

    fire_trigger(trg, count, is_test=force_test, is_dry_run=dry_run)


def evaluate_and_fire_triggers():
    for trg in get_active_triggers():
        try:
            evaluate_single_trigger(trg)
        except Exception as e:
            log.error("خطا در ارزیابی Trigger '%s': %s", trg["name"], e)


def scheduler_loop(stop_event):
    last_cleanup = 0
    while not stop_event.is_set():
        try:
            evaluate_and_fire_triggers()
            if time.time() - last_cleanup > 3600:
                cleanup_old_trigger_data()
                last_cleanup = time.time()
        except Exception as e:
            log.error("خطای غیرمنتظره در چرخهٔ Scheduler: %s", e)
        stop_event.wait(get_scheduler_interval())


# ============================================================
# 🌐 پنل وب مستقل (پورت جدا از پنل اصلی شاهدنیوز)
# ============================================================
class ReusableTCPServer(HTTPServer):
    allow_reuse_address = True


def _fmt_seconds_to_human(seconds):
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} ثانیه"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f} دقیقه"
    hours = minutes / 60
    return f"{hours:.1f} ساعت"


def parse_multipart(content_type, body: bytes):
    """Parser مینیمال multipart/form-data (بدون وابستگی به ماژول منسوخ cgi)."""
    fields, files = {}, {}
    m = re.search(r"boundary=(.+)", content_type or "")
    if not m:
        return fields, files
    boundary = m.group(1).strip().strip('"').encode()
    delimiter = b"--" + boundary
    for part in body.split(delimiter):
        part = part.strip(b"\r\n")
        if not part or part == b"--":
            continue
        if b"\r\n\r\n" not in part:
            continue
        header_blob, content = part.split(b"\r\n\r\n", 1)
        content = content[:-2] if content.endswith(b"\r\n") else content
        headers = header_blob.decode(errors="ignore")
        name_m = re.search(r'name="([^"]+)"', headers)
        if not name_m:
            continue
        field_name = name_m.group(1)
        filename_m = re.search(r'filename="([^"]*)"', headers)
        if filename_m and filename_m.group(1):
            files[field_name] = (filename_m.group(1), content)
        else:
            fields[field_name] = content.decode(errors="ignore")
    return fields, files


def render_dashboard(msg=""):
    triggers = get_all_triggers()
    events = get_recent_events(25)
    media_files = list_available_media()
    now = datetime.now()

    rows_html = ""
    for t in triggers:
        count = 0
        cooldown_remaining = 0
        if t["enabled"]:
            count = len(count_matching_news(t["keyword"], t["window_seconds"]))
        if t["last_triggered_at"]:
            try:
                last = datetime.strptime(t["last_triggered_at"], "%Y-%m-%d %H:%M:%S")
                remaining = t["cooldown_seconds"] - (now - last).total_seconds()
                cooldown_remaining = max(0, int(remaining))
            except (TypeError, ValueError):
                pass

        if not t["enabled"]:
            status_html = '<span style="color:#94a3b8;">⏸ خاموش</span>'
        elif cooldown_remaining > 0:
            status_html = f'<span style="color:#f59e0b;">⏳ Cooldown ({_fmt_seconds_to_human(cooldown_remaining)})</span>'
        elif count >= t["min_count"]:
            status_html = '<span style="color:#22c55e;font-weight:bold;">🔥 TRIGGERED (چرخهٔ بعدی)</span>'
        else:
            status_html = '<span style="color:#38bdf8;">⏱ در انتظار</span>'

        toggle_label = "خاموش کردن" if t["enabled"] else "روشن کردن"
        toggle_val = 0 if t["enabled"] else 1

        rows_html += f"""
        <tr>
            <td>{html.escape(t['name'])}</td>
            <td style="direction:rtl;">{html.escape(t['keyword'])}</td>
            <td>{count} / {t['min_count']}</td>
            <td>{_fmt_seconds_to_human(t['window_seconds'])}</td>
            <td>{_fmt_seconds_to_human(t['cooldown_seconds'])}</td>
            <td>{html.escape(t['media_filename'] or '—')}</td>
            <td>{status_html}</td>
            <td style="white-space:nowrap;">
                <a href="/toggle_trigger?id={t['id']}&enabled={toggle_val}"><button>{toggle_label}</button></a>
                <a href="/test_trigger?id={t['id']}"><button style="background:#0ea5e9;">Test</button></a>
                <a href="/dry_run_trigger?id={t['id']}"><button style="background:#8b5cf6;">Dry-Run</button></a>
                <a href="/delete_trigger?id={t['id']}" onclick="return confirm('حذف این Trigger؟');"><button style="background:#ef4444;">حذف</button></a>
            </td>
        </tr>"""

    if not rows_html:
        rows_html = '<tr><td colspan="8" style="text-align:center;color:#64748b;">هنوز هیچ Triggerی ساخته نشده</td></tr>'

    events_html = ""
    status_colors = {"success": "#22c55e", "failed": "#ef4444", "sending": "#f59e0b", "pending": "#94a3b8"}
    for e in events:
        color = status_colors.get(e["status"], "#94a3b8")
        tag = ""
        if e["is_test"]:
            tag += ' <span style="color:#0ea5e9;">[TEST]</span>'
        if e["is_dry_run"]:
            tag += ' <span style="color:#8b5cf6;">[DRY-RUN]</span>'
        events_html += f"""
        <tr>
            <td>{e['triggered_at']}</td>
            <td>{html.escape(e['trigger_name'] or '—')}{tag}</td>
            <td>{e['match_count']}</td>
            <td style="color:{color};font-weight:bold;">{e['status'].upper()}</td>
            <td style="color:#f87171;">{html.escape(e['error'] or '')}</td>
        </tr>"""
    if not events_html:
        events_html = '<tr><td colspan="5" style="text-align:center;color:#64748b;">هنوز رویدادی ثبت نشده</td></tr>'

    media_options = "".join(f'<option value="{html.escape(m)}">{html.escape(m)}</option>' for m in media_files)
    window_datalist = "".join(f'<option value="{m}">' for m in TIME_WINDOW_PRESETS_MINUTES)
    cooldown_datalist = "".join(f'<option value="{c}">' for c in COOLDOWN_PRESETS_HOURS)

    sahed_status = "🟢 متصل به sahed.py" if SAHED_AVAILABLE else "🔴 sahed.py در دسترس نیست (ارسال غیرفعال)"
    dry_run_on = is_global_dry_run()
    dry_run_label = "خاموش کردن Dry-Run کلی" if dry_run_on else "روشن کردن Dry-Run کلی"
    dry_run_val = 0 if dry_run_on else 1

    banner = f'<div style="background:#1e293b;border:1px solid #334155;padding:10px;border-radius:8px;margin-bottom:15px;">{html.escape(msg)}</div>' if msg else ""

    return f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
<meta charset="UTF-8">
<title>News Trigger Engine</title>
<style>
    body {{ background:#0f172a; color:#e2e8f0; font-family: Tahoma, sans-serif; padding:20px; }}
    h1 {{ color:#38bdf8; }}
    h2 {{ color:#94a3b8; border-bottom:1px solid #334155; padding-bottom:6px; margin-top:30px;}}
    table {{ width:100%; border-collapse: collapse; margin-top:10px; }}
    th, td {{ border:1px solid #334155; padding:8px; text-align:center; font-size:13px; }}
    th {{ background:#1e293b; }}
    input, select, button {{ background:#1e293b; color:#e2e8f0; border:1px solid #334155; border-radius:6px; padding:6px; }}
    button {{ cursor:pointer; }}
    form.inline {{ display:flex; gap:8px; flex-wrap:wrap; align-items:center; margin-bottom:12px; }}
    label {{ font-size:12px; color:#94a3b8; display:block; margin-bottom:2px; }}
    .field {{ display:flex; flex-direction:column; }}
</style>
</head>
<body>
    <h1>🔔 News Trigger Engine</h1>
    <p>{sahed_status}</p>
    {banner}

    <h2>➕ افزودن Trigger جدید</h2>
    <form class="inline" action="/add_trigger" method="GET">
        <div class="field"><label>نام</label><input type="text" name="name" required></div>
        <div class="field"><label>عبارت (Keyword) — برای AND چند بخش را با + جدا کنید</label><input type="text" name="keyword" required style="direction:rtl;width:220px;"></div>
        <div class="field"><label>حداقل تعداد خبر</label><input type="number" name="min_count" value="20" min="1" required style="width:80px;"></div>
        <div class="field"><label>بازهٔ زمانی (دقیقه)</label><input type="number" name="window_minutes" value="60" min="1" list="window_list" required style="width:90px;"><datalist id="window_list">{window_datalist}</datalist></div>
        <div class="field"><label>Cooldown (ساعت)</label><input type="number" name="cooldown_hours" value="6" min="0" step="0.5" list="cooldown_list" style="width:80px;"><datalist id="cooldown_list">{cooldown_datalist}</datalist></div>
        <div class="field"><label>رسانه (از trigger_media/)</label><select name="media_filename">{media_options}</select></div>
        <div class="field"><label>فعال؟</label><input type="checkbox" name="enabled" checked></div>
        <button type="submit">افزودن Trigger</button>
    </form>

    <h2>📁 آپلود رسانه جدید به trigger_media/</h2>
    <form class="inline" action="/upload_media" method="POST" enctype="multipart/form-data">
        <input type="file" name="media_file" required accept=".mp3,.m4a,.wav,.ogg,.mp4,.jpg,.jpeg,.png,.webp">
        <button type="submit">آپلود</button>
    </form>
    <p style="font-size:11px;color:#64748b;">پسوندهای مجاز: {", ".join(sorted(ALLOWED_MEDIA_EXTENSIONS))} — حداکثر حجم {MAX_UPLOAD_SIZE_BYTES // (1024*1024)}MB</p>

    <h2>📋 لیست Triggerها</h2>
    <table>
        <tr><th>نام</th><th>عبارت</th><th>تعداد</th><th>بازهٔ زمانی</th><th>Cooldown</th><th>رسانه</th><th>وضعیت</th><th>عملیات</th></tr>
        {rows_html}
    </table>

    <h2>🧾 رویدادهای اخیر</h2>
    <table>
        <tr><th>زمان</th><th>Trigger</th><th>تعداد</th><th>وضعیت</th><th>خطا</th></tr>
        {events_html}
    </table>

    <h2>⚙️ تنظیمات کلی</h2>
    <p>بازهٔ بررسی Scheduler فعلی: {get_scheduler_interval()} ثانیه</p>
    <form class="inline" action="/set_scheduler_interval" method="GET">
        <input type="number" name="seconds" value="{get_scheduler_interval()}" min="5" style="width:80px;">
        <button type="submit">به‌روزرسانی بازهٔ Scheduler</button>
    </form>
    <a href="/toggle_dry_run?enabled={dry_run_val}"><button style="background:{'#8b5cf6' if not dry_run_on else '#334155'};">{dry_run_label}</button></a>
    <p style="font-size:11px;color:#64748b;">Dry-Run کلی یعنی حتی اگر شرط برقرار شود، هیچ رسانه‌ای واقعاً ارسال نمی‌شود؛ فقط در رویدادها ثبت می‌شود.</p>
</body>
</html>"""


class TriggerDashboardHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # از شلوغ شدن لاگ اصلی جلوگیری می‌کند؛ لاگ منطقی از log.info خودمان می‌آید

    def _redirect_with_msg(self, msg):
        import urllib.parse as _up
        self.send_response(303)
        self.send_header("Location", "/?msg=" + _up.quote(msg))
        self.end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        if path == "/":
            msg = query.get("msg", [""])[0]
            body = render_dashboard(msg).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if path == "/add_trigger":
            name = query.get("name", [""])[0]
            keyword = query.get("keyword", [""])[0]
            min_count = query.get("min_count", ["20"])[0]
            window_minutes = query.get("window_minutes", ["60"])[0]
            cooldown_hours = query.get("cooldown_hours", ["6"])[0]
            media_filename = query.get("media_filename", [""])[0]
            enabled = 1 if query.get("enabled", [""])[0] == "on" else 0
            try:
                window_seconds = int(float(window_minutes) * 60)
                cooldown_seconds = int(float(cooldown_hours) * 3600)
                tid, err = create_trigger(name, keyword, min_count, window_seconds,
                                           media_filename, cooldown_seconds, enabled)
                msg = f"✅ Trigger «{name}» ساخته شد." if tid else f"❌ {err}"
            except (ValueError, TypeError) as e:
                msg = f"❌ مقدار ورودی نامعتبر است: {e}"
            self._redirect_with_msg(msg)
            return

        if path == "/toggle_trigger":
            tid = query.get("id", [""])[0]
            enabled = query.get("enabled", ["0"])[0]
            if tid.isdigit():
                toggle_trigger(int(tid), int(enabled))
            self._redirect_with_msg("✅ وضعیت Trigger به‌روزرسانی شد.")
            return

        if path == "/delete_trigger":
            tid = query.get("id", [""])[0]
            if tid.isdigit():
                delete_trigger(int(tid))
            self._redirect_with_msg("🗑️ Trigger حذف شد.")
            return

        if path == "/test_trigger":
            tid = query.get("id", [""])[0]
            if tid.isdigit():
                trg = get_trigger_by_id(int(tid))
                if trg:
                    count = len(count_matching_news(trg["keyword"], trg["window_seconds"]))
                    ok = fire_trigger(trg, count, is_test=True, is_dry_run=False)
                    self._redirect_with_msg("✅ ارسال تستی انجام شد." if ok else "❌ ارسال تستی ناموفق بود (رویدادها را ببینید).")
                    return
            self._redirect_with_msg("❌ Trigger یافت نشد.")
            return

        if path == "/dry_run_trigger":
            tid = query.get("id", [""])[0]
            if tid.isdigit():
                trg = get_trigger_by_id(int(tid))
                if trg:
                    count = len(count_matching_news(trg["keyword"], trg["window_seconds"]))
                    fire_trigger(trg, count, is_test=True, is_dry_run=True)
                    self._redirect_with_msg(f"🧪 Dry-Run انجام شد: {count}/{trg['min_count']} — چیزی ارسال نشد.")
                    return
            self._redirect_with_msg("❌ Trigger یافت نشد.")
            return

        if path == "/set_scheduler_interval":
            seconds = query.get("seconds", [""])[0]
            if seconds.isdigit():
                set_setting("scheduler_interval_seconds", max(5, int(seconds)))
                self._redirect_with_msg("✅ بازهٔ Scheduler به‌روزرسانی شد.")
                return
            self._redirect_with_msg("❌ مقدار نامعتبر است.")
            return

        if path == "/toggle_dry_run":
            enabled = query.get("enabled", ["0"])[0]
            set_setting("global_dry_run", "1" if enabled == "1" else "0")
            self._redirect_with_msg("✅ حالت Dry-Run کلی به‌روزرسانی شد.")
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        if self.path == "/upload_media":
            try:
                length = int(self.headers.get("Content-Length", 0))
                body = self.rfile.read(length) if length > 0 else b""
                if length > MAX_UPLOAD_SIZE_BYTES:
                    self._redirect_with_msg("❌ حجم فایل بیش از حد مجاز است.")
                    return
                content_type = self.headers.get("Content-Type", "")
                _, files = parse_multipart(content_type, body)
                if "media_file" not in files:
                    self._redirect_with_msg("⚠️ فایلی انتخاب نشد.")
                    return
                orig_name, data = files["media_file"]
                safe = sanitize_filename(orig_name)
                ext = os.path.splitext(safe)[1].lower()
                if not safe or ext not in ALLOWED_MEDIA_EXTENSIONS:
                    self._redirect_with_msg("❌ پسوند فایل مجاز نیست.")
                    return
                if len(data) == 0:
                    self._redirect_with_msg("❌ فایل خالی است.")
                    return
                dest = os.path.join(TRIGGER_MEDIA_DIR, safe)
                with open(dest, "wb") as f:
                    f.write(data)
                self._redirect_with_msg(f"✅ فایل «{safe}» آپلود شد.")
            except Exception as e:
                log.error("خطا در آپلود رسانه: %s", e)
                self._redirect_with_msg(f"❌ خطا در آپلود: {e}")
            return

        self.send_response(404)
        self.end_headers()


def run_trigger_panel():
    port = TRIGGER_PANEL_PORT
    for _ in range(5):
        try:
            server = ReusableTCPServer(("0.0.0.0", port), TriggerDashboardHandler)
            log.info("🌐 پنل Trigger Engine روی پورت %d فعال شد", port)
            server.serve_forever()
            break
        except OSError as e:
            if e.errno == 98:
                port += 1
            else:
                raise


# ============================================================
# 🔌 نقطهٔ اتصال عمومی (Integration API)
# ============================================================
class TriggerEngine:
    """
    API عمومی برای اجرای مستقل یا Integration با sahed.py.
    استفاده (اختیاری، بدون نیاز به تغییر sahed.py):
        from news_trigger_engine import TriggerEngine
        TriggerEngine().start()
    """

    def __init__(self):
        self._stop_event = threading.Event()
        self._scheduler_thread = None
        self._panel_thread = None

    def start(self, run_panel=True):
        init_trigger_db()
        recover_stuck_events_on_startup()
        cleanup_old_trigger_data()

        self._scheduler_thread = threading.Thread(target=scheduler_loop, args=(self._stop_event,), daemon=True)
        self._scheduler_thread.start()

        if run_panel:
            self._panel_thread = threading.Thread(target=run_trigger_panel, daemon=True)
            self._panel_thread.start()

        log.info("🔔 News Trigger Engine شروع به کار کرد (Scheduler هر %d ثانیه)", get_scheduler_interval())

    def stop(self):
        self._stop_event.set()
        log.info("🛑 News Trigger Engine متوقف شد")


if __name__ == "__main__":
    engine = TriggerEngine()
    engine.start()
    log.info("🚀 News Trigger Engine به‌صورت کاملاً مستقل اجرا شد. پنل: http://localhost:%d", TRIGGER_PANEL_PORT)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        engine.stop()
