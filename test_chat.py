#!/usr/bin/env python3
import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

if not BOT_TOKEN or not CHAT_ID:
    raise RuntimeError("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in .env")

url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
payload = {
    "chat_id": CHAT_ID,
    "text": "✅ Hello from my test bot via .env!",
    "parse_mode": "HTML",
    "disable_web_page_preview": True
}

resp = requests.post(url, json=payload, timeout=10)
print("Status:", resp.status_code)
print("Response:", resp.json())
