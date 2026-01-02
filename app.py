import time
import os
import requests
from datetime import datetime, time as dtime, timedelta, timezone
from dotenv import load_dotenv

# ===============================
# LOAD ENV
# ===============================
load_dotenv()

# ===============================
# CONFIG
# ===============================
GITHUB_REPO = "thUser005/project_g_api"
PAT_TOKEN = os.getenv("PAT_TOKEN")

if not PAT_TOKEN:
    raise RuntimeError("❌ PAT_TOKEN not set in environment")

IST = timezone(timedelta(hours=5, minutes=30))

# ===============================
# WORKFLOWS
# ===============================
BUY_WORKFLOW  = "buy.yml"
SELL_WORKFLOW = "sell.yml"

# ===============================
# TRIGGERS (IST)
# ===============================
TRIGGERS = [
    ("09:17", BUY_WORKFLOW),
    ("10:00", SELL_WORKFLOW),
    ("15:40", BUY_WORKFLOW),
    ("15:40", SELL_WORKFLOW),
]

MARKET_START = dtime(9, 0)
MARKET_END   = dtime(16, 0)

# ===============================
# STATE
# ===============================
triggered_today = set()
last_date = None
last_status_minute = None
last_minute = None

# ===============================
# HELPERS
# ===============================
def now_ist():
    return datetime.now(IST)

def parse_time(t_str):
    h, m = map(int, t_str.split(":"))
    return dtime(h, m)

def seconds_left(now, target_time):
    target_dt = datetime.combine(now.date(), target_time, tzinfo=IST)
    if target_dt < now:
        return None
    return int((target_dt - now).total_seconds())

def format_seconds(sec):
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02}:{m:02}:{s:02}"

def trigger_workflow(workflow):
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{workflow}/dispatches"
    r = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {PAT_TOKEN}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        json={"ref": "main"},
        timeout=10,
    )

    if r.status_code == 204:
        print(f"✅ Triggered {workflow}")
    else:
        print(f"❌ Failed {workflow}: {r.status_code} {r.text}")

# ===============================
# MAIN LOOP
# ===============================
print("🚀 Scheduler started (IST)")

while True:
    now = now_ist()
    today = now.date()
    current_time = now.time()
    current_minute = now.strftime("%H:%M")

    # ---- daily reset ----
    if last_date != today:
        triggered_today.clear()
        last_date = today
        print(f"🔄 New trading day: {today}")

    # ---- market hours only ----
    if not (MARKET_START <= current_time <= MARKET_END):
        time.sleep(30)
        continue

    # ---- prevent same-minute duplicate ----
    if current_minute == last_minute:
        time.sleep(5)
        continue
    last_minute = current_minute

    # ---- find next pending trigger ----
    next_trigger = None
    next_seconds = None

    for t, workflow in TRIGGERS:
        key = f"{t}-{workflow}"
        if key in triggered_today:
            continue

        secs = seconds_left(now, parse_time(t))
        if secs is None:
            continue

        if next_seconds is None or secs < next_seconds:
            next_seconds = secs
            next_trigger = (t, workflow)

    # ---- status log (once per minute) ----
    if next_trigger and current_minute != last_status_minute:
        t, wf = next_trigger
        label = "BUY" if wf == BUY_WORKFLOW else "SELL"
        print(
            f"⏳ Next trigger: {label} ({wf}) at {t} | "
            f"Time left: {format_seconds(next_seconds)}"
        )
        last_status_minute = current_minute

    # ---- execute trigger ----
    for t, workflow in TRIGGERS:
        key = f"{t}-{workflow}"
        if current_minute == t and key not in triggered_today:
            print(f"⏰ {t} → Triggering {workflow}")
            trigger_workflow(workflow)
            triggered_today.add(key)
            time.sleep(65)  # hard safety

    time.sleep(15)
