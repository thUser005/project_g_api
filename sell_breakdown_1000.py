import os
import json
import math
import requests
import traceback
from datetime import datetime, timedelta, timezone, time
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from telegram_sender import send_photo, send_message
from table_to_image import table_to_png

# ================= CONFIG =================
CAPITAL = 20_000
TARGET_PCT = 0.03
STOPLOSS_PCT = 0.02          # ✅ 2% SL ABOVE entry (SELL logic)
INTERVAL_MINUTES = 3
EXCHANGE = "NSE"

MAX_WORKERS = 15
MAX_RETRIES = 3

IST = timezone(timedelta(hours=5, minutes=30))

MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("❌ MONGO_URI not set")

DB = "trading"
COL = "daily_signals"

# ================= UTILS =================
def to_bool(v):
    if isinstance(v, bool):
        return v
    if isinstance(v, str):
        return v.lower() == "true"
    return False


def notify_exception(context: str):
    msg = (
        f"❌ ERROR in SELL Breakdown Automation\n\n"
        f"📍 Context: {context}\n"
        f"🕒 Time: {datetime.now(IST)}\n\n"
        f"📄 Traceback:\n"
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
        except Exception:
            print(f"⚠️ {symbol} | retry {attempt}/{MAX_RETRIES}")

    return None

# ================= WORKER =================
def process_stock(stock, start, end):
    try:
        if not to_bool(stock.get("nse_available")):
            return None

        symbol = stock.get("nse_code")
        if not symbol:
            return None

        candles = fetch_candles_with_retry(symbol, start, end)
        if not candles:
            return None

        # -------------------------------
        # MORNING HIGH (09:15–10:00)
        # -------------------------------
        morning_high = None
        for ts, o, h, l, c, v in candles:
            dt = datetime.fromtimestamp(ts, IST)
            if dt.time() <= time(10, 0):
                morning_high = h if morning_high is None else max(morning_high, h)

        if morning_high is None:
            return None

        # -------------------------------
        # SELL BREAKDOWN (10:00+)
        # -------------------------------
        for ts, o, h, l, c, v in candles:
            dt = datetime.fromtimestamp(ts, IST)
            if dt.time() < time(10, 0):
                continue

            if l < morning_high:
                entry = round(c, 2)
                target = round(entry * (1 - TARGET_PCT), 2)
                stoploss = round(entry * (1 + STOPLOSS_PCT), 2)   # ✅ SELL SL

                qty = math.floor(CAPITAL / entry)
                if qty <= 0:
                    return None

                return {
                    "symbol": symbol,
                    "entry": entry,
                    "target": target,
                    "stoploss": stoploss,
                    "qty": qty,
                    "entry_time": dt.strftime("%H:%M"),
                    "exit_time": None,
                    "hit": None,
                    "pnl": None,
                    "status": "PENDING"
                }

        return None

    except Exception:
        notify_exception(f"PROCESS_STOCK: {stock.get('nse_code')}")
        return None

# ================= MAIN =================
def run():
    try:
        start, end = market_range()
        trade_date = today()

        with open("obj_data.json", "r", encoding="utf-8") as f:
            stocks = json.load(f)

        sell_signals = []
        lock = Lock()

        print(f"🚀 SELL Breakdown Scan started for {trade_date}")
        print(f"📦 Total stocks: {len(stocks)}")

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [
                executor.submit(process_stock, stock, start, end)
                for stock in stocks
            ]

            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        with lock:
                            sell_signals.append(result)
                except Exception:
                    notify_exception("THREAD EXECUTION")

        # ================= SAVE TO MONGO =================
        try:
            client = MongoClient(MONGO_URI)
            col = client[DB][COL]

            col.create_index("trade_date", unique=True)

            col.update_one(
                {"trade_date": trade_date},
                {"$set": {"sell_signals": sell_signals}},
                upsert=True
            )
        except Exception:
            notify_exception("MONGO SAVE")
            return

        print(f"✅ SELL signals saved: {len(sell_signals)}")

        # ================= TELEGRAM =================
        if not sell_signals:
            send_message(f"ℹ️ No SELL signals for {trade_date}")
            return

        headers = [
            "SYMBOL", "ENTRY", "TARGET",
            "SL", "QTY", "ENTRY TIME", "STATUS"
        ]

        rows = [
            [
                s["symbol"],
                s["entry"],
                s["target"],
                s["stoploss"],
                s["qty"],
                s["entry_time"],
                s["status"]
            ]
            for s in sell_signals
        ]

        image_path = "sell_breakdown_signals.png"

        try:
            table_to_png(
                headers=headers,
                rows=rows,
                output_path=image_path,
                title=f"SELL BREAKDOWN SIGNALS — {trade_date}"
            )

            send_photo(
                image_path=image_path,
                caption=f"📉 SELL Breakdown Signals ({trade_date})"
            )
        except Exception:
            notify_exception("TELEGRAM IMAGE SEND")

        print("📤 Telegram alert sent")

    except Exception:
        notify_exception("MAIN RUN")

# ================= ENTRY =================
if __name__ == "__main__":
    run()
