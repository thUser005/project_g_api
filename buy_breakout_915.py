import os
import json
import math
import requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# ================= CONFIG =================
CAPITAL = 20_000
BREAKOUT_PCT = 0.03
TARGET_PCT = 0.03
INTERVAL_MINUTES = 3
EXCHANGE = "NSE"

MAX_WORKERS = 15
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))

MONGO_URI = os.environ["MONGO_URI"]
DB = "trading"
COL = "daily_signals"

# ================= TIME =================
def today():
    return datetime.now(IST).strftime("%Y-%m-%d")

def to_ms(dt):
    return int(dt.timestamp() * 1000)

def market_range():
    d = today()
    s = datetime.strptime(d, "%Y-%m-%d").replace(
        hour=9, minute=15, tzinfo=IST
    )
    e = datetime.strptime(d, "%Y-%m-%d").replace(
        hour=15, minute=30, tzinfo=IST
    )
    return to_ms(s), to_ms(e)

# ================= FETCH WITH RETRY =================
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
        except Exception as e:
            print(f"⚠️ {symbol} | retry {attempt}/{MAX_RETRIES} | {e}")

    return None  # fail after retries

# ================= WORKER =================
def process_stock(stock, start, end):
    try:
        if stock.get("nse_available") != "True":
            return None

        symbol = stock["nse_code"]

        candles = fetch_candles_with_retry(symbol, start, end)
        if not candles:
            return None

        open_price = candles[0][1]
        entry = round(open_price * (1 + BREAKOUT_PCT), 2)
        target = round(entry * (1 + TARGET_PCT), 2)
        qty = math.floor(CAPITAL / entry)

        return {
            "symbol": symbol,
            "open": round(open_price, 2),
            "entry": entry,
            "target": target,
            "qty": qty,
            "entry_time": "09:15",
            "exit_time": None,
            "hit": None,
            "pnl": None,
            "status": "OPEN"
        }

    except Exception as e:
        # absolute safety — skip stock
        print(f"❌ {stock.get('nse_code')} | skipped | {e}")
        return None

# ================= MAIN =================
def run():
    start, end = market_range()
    trade_date = today()

    with open("obj_data.json", "r", encoding="utf-8") as f:
        stocks = json.load(f)

    buy_signals = []
    lock = Lock()

    print(f"🚀 BUY Breakout Scan started for {trade_date}")
    print(f"📦 Total stocks: {len(stocks)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(process_stock, stock, start, end)
            for stock in stocks
        ]

        for future in as_completed(futures):
            result = future.result()
            if result:
                with lock:
                    buy_signals.append(result)

    # ================= SAVE TO MONGO =================
    client = MongoClient(MONGO_URI)
    col = client[DB][COL]

    col.update_one(
        {"trade_date": trade_date},
        {
            "$setOnInsert": {
                "trade_date": trade_date,
                "created_at": datetime.utcnow(),
                "capital": CAPITAL,
                "margin": 5
            },
            "$set": {
                "buy_signals": buy_signals
            }
        },
        upsert=True
    )

    print(f"✅ BUY signals saved: {len(buy_signals)}")

if __name__ == "__main__":
    run()
