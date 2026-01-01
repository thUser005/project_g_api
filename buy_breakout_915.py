import os
import json
import math
import time
import requests
import traceback
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from telegram_sender import send_message
from download_obj_data import download_main

# ================= CONFIG =================
CAPITAL = 20_000
BREAKOUT_PCT = 0.03
TARGET_PCT = 0.03
STOPLOSS_PCT = 0.01
INTERVAL_MINUTES = 3
EXCHANGE = "NSE"

MAX_WORKERS = 15
MAX_RETRIES = 3          # API retry
FALLBACK_ROUNDS = 3      # 🔁 full re-scan retries

IST = timezone(timedelta(hours=5, minutes=30))
TEST_FLAG = False
RUN_AFTER = (9, 20)      # (hour, minute)

# ================= DOWNLOAD MASTER =================
try:
    download_main()
except Exception as e:
    print("⚠️ obj_data download error:", e)

# ================= DB =================
MONGO_URL = os.getenv("MONGO_URL")
if not MONGO_URL:
    raise RuntimeError("❌ MONGO_URL not set")

DB = "trading"
COL = "daily_signals"

# ================= UTILS =================
def to_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() == "true"
    return False

def wait_until_917_if_needed():
    if TEST_FLAG:
        print("🧪 TEST MODE: Skipping time wait")
        return

    now = datetime.now(IST)
    run_time = now.replace(
        hour=RUN_AFTER[0],
        minute=RUN_AFTER[1],
        second=0,
        microsecond=0
    )

    if now < run_time:
        wait_seconds = (run_time - now).total_seconds()
        mins = round(wait_seconds / 60, 2)
        print(f"⏳ Waiting until {RUN_AFTER[0]:02}:{RUN_AFTER[1]:02} IST ({mins} mins)...")
        time.sleep(wait_seconds)
    else:
        print("⏰ Time reached — starting immediately")

def notify_exception(context):
    msg = (
        f"❌ ERROR in BUY Breakout Automation\n\n"
        f"📍 Context: {context}\n"
        f"🕒 Time: {datetime.now(IST)}\n\n"
        f"{traceback.format_exc()}"
    )
    send_message(msg[:4096])

# ================= TIME =================
def today():
    return datetime.now(IST).strftime("%Y-%m-%d")

def to_ms(dt):
    return int(dt.timestamp() * 1000)

def market_range():
    d = today()
    s = datetime.strptime(d, "%Y-%m-%d").replace(hour=9, minute=15, tzinfo=IST)
    e = datetime.strptime(d, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=IST)
    return to_ms(s), to_ms(e)

# ================= RUN MODE =================
def get_run_mode(col, trade_date):
    doc = col.find_one({"trade_date": trade_date})
    if not doc:
        return "MORNING"
    if not doc.get("run_flags", {}).get("morning_done"):
        return "MORNING"
    return "AFTERNOON"

# ================= FETCH =================
def fetch_candles_with_retry(symbol, start, end):
    url = (
        f"https://groww.in/v1/api/charting_service/v2/chart/"
        f"delayed/exchange/{EXCHANGE}/segment/CASH/{symbol}"
    )

    params = {
        "startTimeInMillis": start,
        "endTimeInMillis": end,
        "intervalInMinutes": INTERVAL_MINUTES,
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json().get("candles", [])
        except Exception:
            print(f"⚠️ {symbol} API retry {attempt}/{MAX_RETRIES}")

    return []

def extract_day_high_low(candles):
    highs = [c[2] for c in candles if len(c) >= 4]
    lows = [c[3] for c in candles if len(c) >= 4]
    if not highs or not lows:
        return None, None
    return round(max(highs), 2), round(min(lows), 2)

# ================= WORKER =================
def process_stock(stock, start, end, run_mode):
    try:
        if not to_bool(stock.get("nse_available")):
            return None

        symbol = stock.get("nse_code")
        if not symbol:
            return None

        candles = fetch_candles_with_retry(symbol, start, end)
        if not candles:
            return symbol   # 👈 failed → retry later

        first = candles[0]
        if len(first) < 6:
            return symbol

        open_price = first[1]
        volume = first[5]

        if open_price <= 0 or volume < 100:
            return symbol

        entry = round(open_price * (1 + BREAKOUT_PCT), 2)
        qty = math.floor(CAPITAL / entry)
        if qty <= 0:
            return symbol

        target = round(entry * (1 + TARGET_PCT), 2)
        stoploss = round(entry * (1 - STOPLOSS_PCT), 2)

        day_high = day_low = None
        if run_mode == "AFTERNOON":
            day_high, day_low = extract_day_high_low(candles)

        return {
            "symbol": symbol,
            "open": round(open_price, 2),
            "entry": entry,
            "target": target,
            "stoploss": stoploss,
            "qty": qty,
            "day_high": day_high,
            "day_low": day_low,
            "status": "PENDING"
        }

    except Exception:
        notify_exception(f"PROCESS_STOCK: {symbol}")
        return symbol

# ================= MAIN =================
def run_buy():
    try:
        trade_date = today()
        start, end = market_range()

        client = MongoClient(MONGO_URL)
        col = client[DB][COL]
        col.create_index("trade_date", unique=True)

        run_mode = get_run_mode(col, trade_date)

        with open("obj_data.json", "r", encoding="utf-8") as f:
            all_stocks = json.load(f)

        buy_signals = []
        failed_symbols = set()
        current_stocks = all_stocks

        print(f"🚀 BUY Scan | {trade_date} | Mode: {run_mode}")
        print(f"📦 Total stocks: {len(all_stocks)}")

        for round_no in range(1, FALLBACK_ROUNDS + 1):
            print(f"🔁 Fallback round {round_no} | Stocks: {len(current_stocks)}")

            next_failed = set()

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_stock, stock, start, end, run_mode): stock
                    for stock in current_stocks
                }

                for future in as_completed(futures):
                    result = future.result()
                    if isinstance(result, dict):
                        buy_signals.append(result)
                    elif isinstance(result, str):
                        next_failed.add(result)

            if not next_failed:
                break

            failed_symbols = next_failed
            current_stocks = [
                s for s in all_stocks
                if s.get("nse_code") in failed_symbols
            ]

            time.sleep(2)  # polite delay

        if failed_symbols:
            send_message(
                f"⚠️ BUY Scan {trade_date}\n"
                f"Unavailable after retries ({len(failed_symbols)}):\n"
                + ", ".join(sorted(failed_symbols)[:50])
            )

        # ================= SAVE =================
        payload = {
            "$set": {
                "buy_signals": buy_signals,
                "updated_at": datetime.now(timezone.utc),
                f"run_flags.{run_mode.lower()}_done": True
            },
            "$setOnInsert": {
                "trade_date": trade_date,
                "created_at": datetime.now(timezone.utc),
                "capital": CAPITAL,
                "margin": 5
            }
        }

        col.update_one({"trade_date": trade_date}, payload, upsert=True)

        print(f"✅ BUY signals saved: {len(buy_signals)}")

        if not buy_signals:
            send_message(f"ℹ️ No BUY signals for {trade_date}")

    except Exception:
        notify_exception("MAIN RUN")

# ================= ENTRY =================
if __name__ == "__main__":
    wait_until_917_if_needed()
    run_buy()
