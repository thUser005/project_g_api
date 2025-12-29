import requests
import json
from pathlib import Path

# =====================================================
# CONFIG
# =====================================================
FILE_ID = "1Aob4F-0PMwjv5_kchqgPmdQZQyby7YCY"
OUTPUT_FILE = "obj_data.json"

BASE_URL = "https://drive.google.com/uc?export=download"

# =====================================================
# GOOGLE DRIVE HELPERS
# =====================================================
def get_confirm_token(response):
    """
    Google Drive adds a download warning cookie for large files.
    This extracts the confirmation token if present.
    """
    for key, value in response.cookies.items():
        if key.startswith("download_warning"):
            return value
    return None


def save_response_content(response, destination):
    CHUNK_SIZE = 32768
    with open(destination, "wb") as f:
        for chunk in response.iter_content(CHUNK_SIZE):
            if chunk:
                f.write(chunk)


def download_file_from_google_drive(file_id, destination):
    session = requests.Session()

    response = session.get(BASE_URL, params={"id": file_id}, stream=True)
    token = get_confirm_token(response)

    if token:
        response = session.get(
            BASE_URL,
            params={"id": file_id, "confirm": token},
            stream=True
        )

    save_response_content(response, destination)


# =====================================================
# MAIN
# =====================================================
def main():
    print("⬇️ Downloading obj_data.json from Google Drive...")

    download_file_from_google_drive(FILE_ID, OUTPUT_FILE)

    if not Path(OUTPUT_FILE).exists():
        raise RuntimeError("❌ Download failed")

    # Quick validation
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"✅ Downloaded obj_data.json")
    print(f"📦 Total records: {len(data)}")


if __name__ == "__main__":
    main()
