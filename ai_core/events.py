import json, time
import requests
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, S3_BUCKET, AWS_REGION, LATEST_MANIFEST_KEY, CHANGELOG_KEY, TELEGRAM_OFFSET_KEY
from .actions import s3, read_s3_text, write_s3_text

def get_latest_manifest() -> dict | None:
    try:
        obj = s3().get_object(Bucket=S3_BUCKET, Key=LATEST_MANIFEST_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception:
        return None

def get_changelog_lines(limit: int = 20) -> list[dict]:
    try:
        obj = s3().get_object(Bucket=S3_BUCKET, Key=CHANGELOG_KEY)
        raw = obj["Body"].read().decode("utf-8").strip().splitlines()
        items = [json.loads(x) for x in raw[-limit:]]
        return items
    except Exception:
        return []

# --- Telegram updates (long polling) ---
def _get_offset() -> int:
    t = read_s3_text(TELEGRAM_OFFSET_KEY)
    try:
        return int(t) if t else 0
    except:
        return 0

def _set_offset(ofs: int):
    write_s3_text(TELEGRAM_OFFSET_KEY, str(ofs))

def fetch_telegram_updates() -> list[dict]:
    if not TELEGRAM_BOT_TOKEN:
        return []
    offset = _get_offset()
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates",
            params={"timeout": 5, "offset": offset+1},
            timeout=15
        )
        data = r.json()
        if not data.get("ok"):
            return []
        updates = data.get("result", [])
        if updates:
            _set_offset(updates[-1]["update_id"])
        return updates
    except Exception:
        return []

