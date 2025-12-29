import requests
import os

keys_data = os.environ["TELEGRAM_DATA"]
BOT_TOKEN =   keys_data.split("_")[0]
CHAT_ID = keys_data.split("_")[1]

BASE_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"

def send_photo(image_path: str, caption: str = ""):
    url = f"{BASE_URL}/sendPhoto"

    with open(image_path, "rb") as img:
        files = {"photo": img}
        data = {
            "chat_id": CHAT_ID,
            "caption": caption
        }

        r = requests.post(url, data=data, files=files, timeout=20)
        r.raise_for_status()

def send_message(text: str):
    url = f"{BASE_URL}/sendMessage"
    r = requests.post(
        url,
        json={"chat_id": CHAT_ID, "text": text},
        timeout=20
    )
    r.raise_for_status()
