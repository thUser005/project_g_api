import os
import json
import math
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
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))

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

# ================= RUN MODE (DB FIRST) =================
def get_run_mode(col, trade_date):
    """
    Priority:
    1️⃣ If no document → MORNING
    2️⃣ If morning_done != True → MORNING
    3️⃣ Else → AFTERNOON
    """

    doc = col.find_one({"trade_date": trade_date})

    if not doc:
        return "MORNING"

    flags = doc.get("run_flags", {})
    if not flags.get("morning_done"):
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
            print(f"⚠️ {symbol} retry {attempt}/{MAX_RETRIES}")

    return []

def extract_day_high_low(candles):
    highs, lows = [], []
    for c in candles:
        if len(c) >= 4:
            highs.append(c[2])
            lows.append(c[3])

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
            return None

        first = candles[0]
        if len(first) < 6:
            return None

        open_price = first[1]
        volume = first[5]

        if open_price is None or open_price <= 0:
            return None

        if volume is None or volume < 100:
            return None

        entry = round(open_price * (1 + BREAKOUT_PCT), 2)
        target = round(entry * (1 + TARGET_PCT), 2)
        stoploss = round(entry * (1 - STOPLOSS_PCT), 2)

        qty = math.floor(CAPITAL / entry)
        if qty <= 0:
            return None

        day_high, day_low = None, None
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
        return None

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
            stocks = json.load(f)

        buy_signals = []
        lock = Lock()

        print(f"🚀 BUY Scan | {trade_date} | Mode: {run_mode}")
        print(f"📦 Stocks: {len(stocks)}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_stock, stock, start, end, run_mode)
                for stock in stocks
            ]

            for future in as_completed(futures):
                r = future.result()
                if r:
                    with lock:
                        buy_signals.append(r)

        if run_mode == "MORNING":
            col.update_one(
                {"trade_date": trade_date},
                {
                    "$set": {
                        "buy_signals": buy_signals,
                        "updated_at": datetime.utcnow(),
                        "run_flags.morning_done": True
                    },
                    "$setOnInsert": {
                        "trade_date": trade_date,
                        "created_at": datetime.utcnow(),
                        "capital": CAPITAL,
                        "margin": 5
                    }
                },
                upsert=True
            )

        else:  # AFTERNOON
            col.update_one(
                {"trade_date": trade_date},
                {
                    "$set": {
                        "buy_signals": buy_signals,
                        "updated_at": datetime.utcnow(),
                        "run_flags.afternoon_done": True
                    }
                },
                upsert=True
            )

        print(f"✅ BUY signals saved: {len(buy_signals)}")

        if not buy_signals:
            send_message(f"ℹ️ No BUY signals for {trade_date}")

    except Exception:
        notify_exception("MAIN RUN")

# ================= ENTRY =================
if __name__ == "__main__":
    run_buy()
