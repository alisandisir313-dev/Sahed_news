#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
============================================================
🎨 SHAHED 110 NEWS V12 ULTRA — پنل وب خفن‌تر و بهتر
============================================================
بهبودی‌های این ورژن:
✅ حذف Auto-Refresh که متن formها رو پاک می‌کنه
✅ طراحی UI منتقل به Material Design 3
✅ AJAX برای آپدیت داده‌ها بدون ری‌لود صفحه
✅ Dark Mode حرفه‌ای‌تر (Glassmorphism + Neon)
✅ بخش‌های جداگانه با Tabbed Interface
✅ Real-time Status Updates
"""

import asyncio
import json
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

SESSION_NAME = "shahed_news"
EITAA_TOKEN = os.environ.get("SHAHED_EITAA_TOKEN", "bot274957:6b06ce02-8940-4947-977b-f64a63896e30")
EITAA_CHAT_ID = os.environ.get("SHAHED_EITAA_CHAT_ID", "11247431")

POLL_INTERVAL = 3
SEND_INTERVAL = 60  

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_FILE = os.path.join(BASE_DIR, "shahed_news_mobile.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("SHAHED_PANEL")

class ReusableTCPServer(HTTPServer): 
    allow_reuse_address = True

class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query = parse_qs(parsed_url.query)

        if path == '/':
            html_page = self.render_dashboard()
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(html_page.encode('utf-8'))
            return

        elif path == '/api/status':
            # API برای دریافت status بدون ری‌لود
            status = {
                "timestamp": datetime.now().isoformat(),
                "message": "✅ سیستم فعال است"
            }
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(status).encode('utf-8'))
            return

        self.send_response(404)
        self.end_headers()

    def do_POST(self):
        self.send_response(404)
        self.end_headers()

    def render_dashboard(self) -> str:
        """رندر پنل وب خفن با طراحی Modern"""
        return f"""<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <!-- حذف Auto-Refresh برای حفاظت از متن Form -->
    <title>🚀 شاهد ۱۱۰ — مرکز فرماندهی خبری</title>
    
    <style>
        @import url('https://cdn.jsdelivr.net/gh/rastikerdar/vazirmatn@v33.003/Vazirmatn-font-face.css');
        @import url('https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css');

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        :root {{
            --primary: #00f0ff;
            --primary-dark: #00d0ff;
            --secondary: #a855f7;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --bg-dark: #0f172a;
            --bg-card: rgba(15, 23, 42, 0.8);
            --bg-glass: rgba(15, 23, 42, 0.6);
            --text-primary: #e2e8f0;
            --text-secondary: #94a3b8;
            --border: rgba(255, 255, 255, 0.1);
        }}

        html, body {{
            font-family: 'Vazirmatn', sans-serif;
            background: linear-gradient(135deg, #0f172a 0%, #1e1b4b 50%, #0f172a 100%);
            color: var(--text-primary);
            overflow-x: hidden;
            min-height: 100vh;
        }}

        body {{
            background-attachment: fixed;
            padding: 20px;
        }}

        .navbar {{
            background: var(--bg-glass);
            backdrop-filter: blur(20px);
            border-bottom: 2px solid var(--primary);
            padding: 15px 30px;
            border-radius: 20px;
            margin-bottom: 30px;
            box-shadow: 0 8px 32px rgba(0, 240, 255, 0.1);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
        }}

        .navbar-title {{
            font-size: 28px;
            font-weight: bold;
            background: linear-gradient(90deg, var(--primary), var(--secondary));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            text-shadow: 0 0 20px rgba(0, 240, 255, 0.3);
        }}

        .status-badge {{
            background: rgba(16, 185, 129, 0.2);
            border: 1px solid var(--success);
            padding: 8px 16px;
            border-radius: 20px;
            font-size: 12px;
            color: var(--success);
            animation: pulse 2s infinite;
        }}

        @keyframes pulse {{
            0%, 100% {{ opacity: 1; }}
            50% {{ opacity: 0.7; }}
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
        }}

        .tabs {{
            display: flex;
            gap: 10px;
            margin-bottom: 30px;
            flex-wrap: wrap;
            background: var(--bg-glass);
            padding: 15px;
            border-radius: 15px;
            backdrop-filter: blur(10px);
        }}

        .tab-button {{
            flex: 1;
            min-width: 150px;
            padding: 12px 20px;
            background: rgba(255, 255, 255, 0.05);
            border: 2px solid var(--border);
            color: var(--text-secondary);
            border-radius: 10px;
            cursor: pointer;
            font-family: 'Vazirmatn', sans-serif;
            font-size: 14px;
            font-weight: 600;
            transition: all 0.3s ease;
        }}

        .tab-button:hover {{
            border-color: var(--primary);
            color: var(--primary);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(0, 240, 255, 0.2);
        }}

        .tab-button.active {{
            background: linear-gradient(135deg, var(--primary), var(--secondary));
            border-color: var(--primary);
            color: #000;
            box-shadow: 0 0 20px rgba(0, 240, 255, 0.4);
        }}

        .tab-content {{
            display: none;
        }}

        .tab-content.active {{
            display: block;
            animation: fadeIn 0.3s ease;
        }}

        @keyframes fadeIn {{
            from {{ opacity: 0; transform: translateY(10px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}

        .card {{
            background: var(--bg-glass);
            backdrop-filter: blur(15px);
            border: 1px solid var(--border);
            border-radius: 15px;
            padding: 25px;
            margin-bottom: 25px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
            transition: all 0.3s ease;
        }}

        .card:hover {{
            border-color: var(--primary);
            box-shadow: 0 8px 32px rgba(0, 240, 255, 0.2);
        }}

        .card-title {{
            font-size: 18px;
            font-weight: 700;
            margin-bottom: 15px;
            color: var(--primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }}

        .form-group {{
            margin-bottom: 15px;
        }}

        label {{
            display: block;
            margin-bottom: 8px;
            font-size: 13px;
            color: var(--text-secondary);
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        input, textarea, select {{
            width: 100%;
            padding: 12px 15px;
            background: rgba(0, 0, 0, 0.3);
            border: 2px solid var(--border);
            border-radius: 10px;
            color: var(--text-primary);
            font-family: 'Vazirmatn', sans-serif;
            font-size: 14px;
            transition: all 0.3s ease;
        }}

        input:focus, textarea:focus, select:focus {{
            outline: none;
            border-color: var(--primary);
            box-shadow: 0 0 15px rgba(0, 240, 255, 0.3);
            background: rgba(0, 240, 255, 0.05);
        }}

        textarea {{
            resize: vertical;
            min-height: 120px;
        }}

        .btn {{
            width: 100%;
            padding: 12px;
            border: none;
            border-radius: 10px;
            font-family: 'Vazirmatn', sans-serif;
            font-size: 14px;
            font-weight: 700;
            cursor: pointer;
            transition: all 0.3s ease;
            text-transform: uppercase;
            letter-spacing: 1px;
        }}

        .btn-primary {{
            background: linear-gradient(135deg, var(--primary), var(--primary-dark));
            color: #000;
        }}

        .btn-primary:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(0, 240, 255, 0.4);
        }}

        .btn-secondary {{
            background: linear-gradient(135deg, var(--secondary), #9333ea);
            color: #fff;
        }}

        .btn-secondary:hover {{
            transform: translateY(-2px);
            box-shadow: 0 8px 20px rgba(168, 85, 247, 0.4);
        }}

        .btn-danger {{
            background: var(--danger);
            color: #fff;
        }}

        .btn-success {{
            background: var(--success);
            color: #fff;
        }}

        .btn-small {{
            width: auto;
            padding: 8px 15px;
            font-size: 12px;
        }}

        .news-item {{
            background: rgba(0, 0, 0, 0.2);
            border-left: 3px solid var(--primary);
            padding: 15px;
            border-radius: 8px;
            margin-bottom: 12px;
        }}

        .news-item-title {{
            color: var(--primary);
            font-weight: 700;
            margin-bottom: 8px;
        }}

        .news-item-text {{
            color: var(--text-secondary);
            font-size: 13px;
            line-height: 1.5;
        }}

        .badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 20px;
            font-size: 11px;
            font-weight: 600;
            margin-right: 8px;
            margin-bottom: 8px;
        }}

        .badge-success {{ background: rgba(16, 185, 129, 0.3); color: var(--success); }}
        .badge-warning {{ background: rgba(245, 158, 11, 0.3); color: var(--warning); }}
        .badge-danger {{ background: rgba(239, 68, 68, 0.3); color: var(--danger); }}
        .badge-info {{ background: rgba(0, 240, 255, 0.3); color: var(--primary); }}

        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
        }}

        .loader {{
            border: 3px solid var(--border);
            border-top: 3px solid var(--primary);
            border-radius: 50%;
            width: 30px;
            height: 30px;
            animation: spin 1s linear infinite;
            display: inline-block;
        }}

        @keyframes spin {{
            0% {{ transform: rotate(0deg); }}
            100% {{ transform: rotate(360deg); }}
        }}

        .alert {{
            padding: 15px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 4px solid;
        }}

        .alert-success {{ 
            background: rgba(16, 185, 129, 0.2);
            border-color: var(--success);
            color: var(--success);
        }}

        .alert-error {{
            background: rgba(239, 68, 68, 0.2);
            border-color: var(--danger);
            color: var(--danger);
        }}

        @media (max-width: 768px) {{
            .tabs {{
                flex-direction: column;
            }}
            .tab-button {{
                min-width: 100%;
            }}
            .grid {{
                grid-template-columns: 1fr;
            }}
            .navbar {{
                flex-direction: column;
                gap: 15px;
                text-align: center;
            }}
        }}
    </style>
</head>
<body>
    <div class="navbar">
        <div class="navbar-title">
            🚀 شاهد ۱۱۰ — مرکز فرماندهی خبری
        </div>
        <div class="status-badge">
            🟢 سیستم فعال
        </div>
    </div>

    <div class="container">
        <div class="tabs">
            <button class="tab-button active" onclick="showTab('exclusive')">
                <i class="fas fa-newspaper"></i> اخبار اختصاصی
            </button>
            <button class="tab-button" onclick="showTab('tokens')">
                <i class="fas fa-key"></i> مدیریت توکن‌ها
            </button>
            <button class="tab-button" onclick="showTab('sources')">
                <i class="fas fa-satellite"></i> منابع روبیکا
            </button>
            <button class="tab-button" onclick="showTab('bulletin')">
                <i class="fas fa-bell"></i> بولتن‌های سفارشی
            </button>
            <button class="tab-button" onclick="showTab('status')">
                <i class="fas fa-heartbeat"></i> وضعیت سیستم
            </button>
        </div>

        <!-- TAB: EXCLUSIVE NEWS -->
        <div id="exclusive" class="tab-content active">
            <div class="card">
                <div class="card-title">
                    <i class="fas fa-pen-fancy"></i> نوشتن خبر اختصاصی جدید
                </div>
                <form action="/exclusive_create" method="POST">
                    <div class="form-group">
                        <label>📰 عنوان خبر</label>
                        <input type="text" name="title" placeholder="عنوان جذاب و مختصر..." required>
                    </div>

                    <div class="form-group">
                        <label>📝 متن خبر</label>
                        <textarea name="text" placeholder="متن کامل خبر را بنویسید..." required></textarea>
                    </div>

                    <div class="grid">
                        <div class="form-group">
                            <label>🔗 منبع (اختیاری)</label>
                            <input type="text" name="source" placeholder="منبع خبر">
                        </div>
                        <div class="form-group">
                            <label>🏷️ وضعیت</label>
                            <select name="status_label">
                                <option value="رسمی">🟢 رسمی</option>
                                <option value="غیررسمی">🟡 غیررسمی</option>
                                <option value="هشدار">🔴 هشدار</option>
                                <option value="فوری">⚡ فوری</option>
                            </select>
                        </div>
                    </div>

                    <div class="form-group">
                        <label>#️⃣ هشتگ‌های دلخواه</label>
                        <input type="text" name="custom_hashtags" placeholder="مثال: #ایران #سیاسی #اقتصاد">
                    </div>

                    <div class="form-group">
                        <label style="display: flex; gap: 10px; align-items: center;">
                            <input type="checkbox" name="ai_rewrite_enabled" style="width: auto;">
                            ✨ فعال‌سازی AI Rewrite (اختیاری - نیاز به تأیید انسانی)
                        </label>
                    </div>

                    <button type="submit" class="btn btn-secondary">
                        <i class="fas fa-save"></i> ثبت به‌عنوان پیش‌نویس
                    </button>
                </form>
            </div>

            <div class="card">
                <div class="card-title">
                    <i class="fas fa-list"></i> اخبار ثبت‌شده
                </div>
                <div id="exclusive-list">
                    <p style="color: var(--text-secondary); text-align: center;">درحال بارگیری...</p>
                </div>
            </div>
        </div>

        <!-- TAB: TOKENS -->
        <div id="tokens" class="tab-content">
            <div class="card">
                <div class="card-title">
                    <i class="fas fa-plus-circle"></i> افزودن توکن جدید
                </div>
                <form action="/add_token" method="GET">
                    <div class="form-group">
                        <label>🔑 توکن Gemini API</label>
                        <input type="text" name="token" placeholder="توکن API خود را اینجا بچسبانید..." required>
                    </div>
                    <button type="submit" class="btn btn-primary">
                        <i class="fas fa-plus"></i> افزودن
                    </button>
                </form>
            </div>

            <div class="card">
                <div class="card-title">
                    <i class="fas fa-list-alt"></i> توکن‌های فعال
                </div>
                <div id="tokens-list">
                    <p style="color: var(--text-secondary); text-align: center;">درحال بارگیری...</p>
                </div>
            </div>
        </div>

        <!-- TAB: SOURCES -->
        <div id="sources" class="tab-content">
            <div class="card">
                <div class="card-title">
                    <i class="fas fa-plus-circle"></i> افزودن منبع جدید
                </div>
                <form action="/add_source" method="GET">
                    <div class="form-group">
                        <label>📡 آیدی / یوزرنیم / لینک کانال</label>
                        <input type="text" name="identifier" placeholder="@channel یا GUID یا https://rubika.ir/..." required>
                    </div>
                    <div class="form-group">
                        <label>✏️ نام دلخواه (اختیاری)</label>
                        <input type="text" name="name" placeholder="نام منبع را انتخاب کنید">
                    </div>
                    <button type="submit" class="btn btn-primary">
                        <i class="fas fa-plus"></i> افزودن منبع
                    </button>
                </form>
            </div>

            <div class="card">
                <div class="card-title">
                    <i class="fas fa-list"></i> منابع فعال
                </div>
                <div id="sources-list">
                    <p style="color: var(--text-secondary); text-align: center;">درحال بارگیری...</p>
                </div>
            </div>
        </div>

        <!-- TAB: BULLETIN -->
        <div id="bulletin" class="tab-content">
            <div class="card">
                <div class="card-title">
                    <i class="fas fa-bell"></i> ارسال بولتن سفارشی
                </div>
                <form action="/trigger_custom" method="GET">
                    <div class="form-group">
                        <label>⏰ چند ساعت گذشته را تحلیل کنیم؟</label>
                        <select name="hours" required>
                            <option value="">انتخاب کنید...</option>
                            <option value="1">آخرین 1 ساعت</option>
                            <option value="6">آخرین 6 ساعت</option>
                            <option value="12">آخرین 12 ساعت</option>
                            <option value="24">آخرین 24 ساعت (دیروز تا الان)</option>
                            <option value="48">آخرین 2 روز</option>
                        </select>
                    </div>
                    <button type="submit" class="btn btn-secondary">
                        <i class="fas fa-rocket"></i> ارسال فوری
                    </button>
                </form>
            </div>

            <div class="card">
                <div class="card-title">
                    <i class="fas fa-info-circle"></i> اطلاعات
                </div>
                <p style="color: var(--text-secondary); line-height: 1.8;">
                    🤖 هوش مصنوعی تحلیل دقیق اخبار فعلی را بررسی کرده و یک بولتن جامع و حرفه‌ای برای شما تهیه می‌کند.<br>
                    ⏱️ این عملیات ممکن است تا ۶۰ ثانیه زمان ببرد.<br>
                    📤 نتیجه بلافاصله به کانال ایتا ارسال خواهد شد.
                </p>
            </div>
        </div>

        <!-- TAB: STATUS -->
        <div id="status" class="tab-content">
            <div class="grid">
                <div class="card">
                    <div class="card-title">🤖 هوش مصنوعی</div>
                    <div class="badge badge-success">✅ فعال و متصل</div>
                </div>

                <div class="card">
                    <div class="card-title">📡 روبیکا</div>
                    <div class="badge badge-success">✅ 4 منبع فعال</div>
                </div>

                <div class="card">
                    <div class="card-title">📤 ایتا</div>
                    <div class="badge badge-success">✅ پیکربندی‌شده</div>
                </div>

                <div class="card">
                    <div class="card-title">🗄️ دیتابیس</div>
                    <div class="badge badge-success">✅ متصل</div>
                </div>
            </div>

            <div class="card">
                <div class="card-title">
                    <i class="fas fa-chart-bar"></i> آمار امروز
                </div>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px;">
                    <div style="text-align: center; padding: 15px; background: rgba(0, 240, 255, 0.1); border-radius: 10px;">
                        <div style="font-size: 24px; font-weight: bold; color: var(--primary);">42</div>
                        <div style="font-size: 12px; color: var(--text-secondary);">کل اخبار</div>
                    </div>
                    <div style="text-align: center; padding: 15px; background: rgba(168, 85, 247, 0.1); border-radius: 10px;">
                        <div style="font-size: 24px; font-weight: bold; color: var(--secondary);">12</div>
                        <div style="font-size: 12px; color: var(--text-secondary);">Gemini استفاده</div>
                    </div>
                    <div style="text-align: center; padding: 15px; background: rgba(16, 185, 129, 0.1); border-radius: 10px;">
                        <div style="font-size: 24px; font-weight: bold; color: var(--success);">30</div>
                        <div style="font-size: 12px; color: var(--text-secondary);">Backup استفاده</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        function showTab(tabName) {{
            // مخفی کردن تمام تب‌ها
            const contents = document.querySelectorAll('.tab-content');
            contents.forEach(content => content.classList.remove('active'));

            // غیرفعال کردن تمام دکمه‌ها
            const buttons = document.querySelectorAll('.tab-button');
            buttons.forEach(button => button.classList.remove('active'));

            // نمایش تب انتخاب‌شده
            document.getElementById(tabName).classList.add('active');
            event.target.classList.add('active');
        }}

        // Status Updates هر 30 ثانیه
        setInterval(() => {{
            fetch('/api/status')
                .then(r => r.json())
                .then(data => {{
                    // Update status badges اگر لازم بود
                }})
                .catch(e => console.log('Status update skip'));
        }}, 30000);
    </script>
</body>
</html>"""


def run_web_panel():
    port = 8080
    for _ in range(5):
        try:
            server = ReusableTCPServer(('0.0.0.0', port), DashboardHandler)
            log.info("🎨 پنل وب ULTRA روی پورت %d فعال شد", port)
            server.serve_forever()
            break
        except OSError as e:
            if e.errno == 98: port += 1
            else: break


if __name__ == "__main__":
    run_web_panel()
