import os
import json
import math
import requests
import traceback
from datetime import datetime, timedelta, timezone, time
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from download_obj_data import download_main
from telegram_sender import send_message

# ================= CONFIG =================
CAPITAL = 20_000
TARGET_PCT = 0.03
STOPLOSS_PCT = 0.02
INTERVAL_MINUTES = 3
EXCHANGE = "NSE"

MAX_WORKERS = 15
MAX_RETRIES = 3
TEST_FLAG = False   # 🔁 True = run immediately, False = wait till 09:17 IST
RUN_AFTER = (10, 0) # (hour, minute)

IST = timezone(timedelta(hours=5, minutes=30))
 
try:
         
    download_main()
except Exception as e:
    print("Error : ",e)

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

def wait_until_10_if_needed():
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
        print(f"⏳ Waiting until 10.00 IST ({mins} mins)...")
        time.sleep(wait_seconds)
    else:
        print("⏰ Time ≥ 10.00 IST — starting immediately")

def notify_exception(context):
    msg = (
        f"❌ ERROR in SELL Breakdown Automation\n\n"
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

def get_run_mode():
    now = datetime.now(IST).time()

    if now.hour == 10:
        return "MORNING"

    if now.hour == 15:
        return "AFTERNOON"

    return "UNKNOWN"

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

        # -------- MORNING HIGH (till 10:00) --------
        morning_high = None
        for ts, o, h, l, c, v in candles:
            dt = datetime.fromtimestamp(ts / 1000, IST)
            if dt.time() <= time(10, 0):
                morning_high = h if morning_high is None else max(morning_high, h)

        if morning_high is None:
            return None

        # -------- SELL BREAKDOWN --------
        for ts, o, h, l, c, v in candles:
            dt = datetime.fromtimestamp(ts / 1000, IST)
            if dt.time() < time(10, 0):
                continue

            if l < morning_high:
                entry = round(c, 2)
                target = round(entry * (1 - TARGET_PCT), 2)
                stoploss = round(entry * (1 + STOPLOSS_PCT), 2)
                qty = math.floor(CAPITAL / entry)

                if qty <= 0:
                    return None

                day_high, day_low = None, None
                if run_mode == "AFTERNOON":
                    day_high, day_low = extract_day_high_low(candles)

                return {
                    "symbol": symbol,
                    "morning_high": round(morning_high, 2),
                    "entry": entry,
                    "target": target,
                    "stoploss": stoploss,
                    "qty": qty,
                    "entry_time": dt.strftime("%H:%M"),
                    "day_high": day_high,
                    "day_low": day_low,
                    "status": "PENDING"
                }

        return None

    except Exception:
        notify_exception(f"PROCESS_STOCK {stock.get('nse_code')}")
        return None

# ================= MAIN =================
def run_sell():
    try:
        run_mode = get_run_mode()
        start, end = market_range()
        trade_date = today()

        with open("obj_data.json", "r", encoding="utf-8") as f:
            stocks = json.load(f)

        sell_signals = []
        lock = Lock()

        print(f"🚀 SELL Scan | {trade_date} | Mode: {run_mode}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = [
                ex.submit(process_stock, stock, start, end, run_mode)
                for stock in stocks
            ]

            for f in as_completed(futures):
                r = f.result()
                if r:
                    with lock:
                        sell_signals.append(r)

        client = MongoClient(MONGO_URL)
        col = client[DB][COL]
        col.create_index("trade_date", unique=True)

        existing = col.find_one({"trade_date": trade_date})

        if run_mode == "MORNING":
            col.update_one(
                {"trade_date": trade_date},
                {
                    "$set": {
                        "sell_signals": sell_signals,
                        "updated_at": datetime.utcnow(),
                        "run_flags.sell_morning_done": True
                    },
                    "$setOnInsert": {
                        "trade_date": trade_date,
                        "created_at": datetime.utcnow()
                    }
                },
                upsert=True
            )

        elif run_mode == "AFTERNOON":
            if not existing:
                col.insert_one({
                    "trade_date": trade_date,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "run_flags": {
                        "sell_morning_done": False,
                        "sell_afternoon_done": True
                    },
                    "sell_signals": sell_signals
                })
            else:
                col.update_one(
                    {"trade_date": trade_date},
                    {
                        "$set": {
                            "sell_signals": sell_signals,
                            "updated_at": datetime.utcnow(),
                            "run_flags.sell_afternoon_done": True
                        }
                    }
                )

        print(f"✅ SELL signals saved: {len(sell_signals)}")

        if not sell_signals:
            send_message(f"ℹ️ No SELL signals for {trade_date}")

    except Exception:
        notify_exception("MAIN RUN")

# ================= ENTRY =================
if __name__ == "__main__":
    wait_until_10_if_needed()
    run_sell()
