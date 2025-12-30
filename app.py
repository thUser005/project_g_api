from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify

from buy_breakout_915 import run_buy
from sell_breakdown_1000 import run_sell

# -------------------------------------------------
# APP SETUP
# -------------------------------------------------
app = Flask(__name__)

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))


def run_time_based_logic():
    now = datetime.now(IST)
    hour = now.hour
    minute = now.minute

    log = {
        "current_time_ist": now.strftime("%H:%M:%S"),
        "action": "none"
    }

    # BUY window: 09:15 – 09:20
    if hour == 9 and 15 <= minute <= 20:
        run_buy()
        log["action"] = "BUY"
        return log

    # SELL window: 10:00 – 10:02
    if hour == 10 and minute <= 2:
        run_sell()
        log["action"] = "SELL"
        return log

    return log


# -------------------------------------------------
# ROUTES
# -------------------------------------------------
@app.route("/")
def health():
    return jsonify({
        "status": "ok",
        "message": "Trading service running"
    })


@app.route("/run")
def run_now():
    """
    Manual trigger (optional)
    """
    result = run_time_based_logic()
    return jsonify(result)


# -------------------------------------------------
# ENTRY POINT (local testing only)
# -------------------------------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
