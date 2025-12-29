import os, json, math, requests
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

# ================= CONFIG =================
CAPITAL = 20_000
BREAKOUT_PCT = 0.03
TARGET_PCT = 0.03
INTERVAL_MINUTES = 3
EXCHANGE = "NSE"

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
    s = datetime.strptime(d, "%Y-%m-%d").replace(hour=9, minute=15, tzinfo=IST)
    e = datetime.strptime(d, "%Y-%m-%d").replace(hour=15, minute=30, tzinfo=IST)
    return to_ms(s), to_ms(e)

# ================= FETCH =================
def fetch_candles(symbol, start, end):
    url = f"https://groww.in/v1/api/charting_service/v2/chart/delayed/exchange/{EXCHANGE}/segment/CASH/{symbol}"
    params = {
        "startTimeInMillis": start,
        "endTimeInMillis": end,
        "intervalInMinutes": INTERVAL_MINUTES
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("candles", [])

# ================= MAIN =================
def run():
    start, end = market_range()
    trade_date = today()

    with open("obj_data.json") as f:
        stocks = json.load(f)

    buy_signals = []

    for s in stocks:
        if s.get("nse_available") != "True":
            continue

        symbol = s["nse_code"]
        candles = fetch_candles(symbol, start, end)
        if not candles:
            continue

        open_price = candles[0][1]
        entry = round(open_price * (1 + BREAKOUT_PCT), 2)
        target = round(entry * (1 + TARGET_PCT), 2)

        qty = math.floor(CAPITAL / entry)

        buy_signals.append({
            "symbol": symbol,
            "open": open_price,
            "entry": entry,
            "target": target,
            "qty": qty,
            "entry_time": "09:15",
            "exit_time": None,
            "hit": None,
            "pnl": None,
            "status": "OPEN"
        })

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
