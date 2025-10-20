import json, time, traceback, os
from datetime import datetime
from .config import S3_BUCKET, LATEST_MANIFEST_KEY
from .actions import tg, presign, rerun_pipeline
from .events import get_latest_manifest, get_changelog_lines, fetch_telegram_updates
from .rules import detect_anomalies

MEM_PATH = "ai_core/memory.jsonl"

def log_memory(event: dict):
    os.makedirs("ai_core", exist_ok=True)
    event["ts"] = datetime.utcnow().isoformat()+"Z"
    with open(MEM_PATH, "a", encoding="utf-8") as f:
        f.write(json.dumps(event, ensure_ascii=False) + "\n")

def handle_command(cmd: str) -> str:
    cmd = cmd.strip().lower()
    if cmd in ("/help", "help"):
        return ("Commands:\n"
                "/status – show latest metrics\n"
                "/last – show last artifacts\n"
                "/rerun – rerun nightly pipeline\n"
                "/help – this help")
    if cmd in ("/rerun", "rerun"):
        try:
            rerun_pipeline()
            log_memory({"type":"rerun_triggered"})
            return "🔁 Pipeline rerun dispatched."
        except Exception as e:
            return f"❌ Rerun failed: {e}"
    if cmd in ("/last", "last"):
        man = get_latest_manifest()
        if not man:
            return "No latest manifest."
        csv_key = man.get("csv_key") or man.get("parquet_key") or ""
        rep_key = man.get("report_key") or ""
        links = []
        if csv_key:
            links.append("Data: " + presign(csv_key))
        if rep_key:
            links.append("Report: " + presign(rep_key))
        return "\n".join(links) or "No links."
    if cmd in ("/status", "status"):
        man = get_latest_manifest()
        if not man:
            return "No latest manifest."
        s = man.get("stats", {})
        return (f"Rows: {man.get('rows')}\n"
                f"T1D:{s.get('t1d')} T2D:{s.get('t2d')}\n"
                f"Mean age:{s.get('mean_age')} Female:{s.get('female_ratio')}\n"
                f"HbA1c mean:{s.get('hba1c_mean') or s.get('hba1c')}")
    return "Unknown command. Type /help"

def process_telegram_updates():
    for upd in fetch_telegram_updates():
        msg = upd.get("message") or upd.get("channel_post") or {}
        text = (msg.get("text") or "").strip()
        if not text:
            continue
        reply = handle_command(text)
        tg(reply)
        log_memory({"type":"telegram_cmd", "cmd": text, "reply": reply})

def brain_tick():
    # 1) commands
    process_telegram_updates()

    # 2) read latest state
    man = get_latest_manifest()
    alerts = detect_anomalies(man)

    # 3) act
    if alerts:
        msg = "⚠️ BioSynthX Brain alerts:\n" + "\n".join(f"- {a}" for a in alerts)
        tg(msg)
        log_memory({"type":"alerts", "alerts": alerts})

        # Автоматично рестартиране при критични проблеми
        critical = any("Low row count" in a for a in alerts)
        if critical:
            try:
                rerun_pipeline()
                tg("🔁 Brain: auto-rerun dispatched due to critical condition.")
                log_memory({"type":"auto_rerun"})
            except Exception as e:
                tg(f"❌ Brain: auto-rerun failed: {e}")
                log_memory({"type":"auto_rerun_failed", "err": str(e)})

    # 4) optional: кратък daily digest (пример)
    if man:
        s = man.get("stats", {})
        digest = (f"🧠 Brain heartbeat OK\n"
                  f"Rows:{man.get('rows')} | T1D:{s.get('t1d')} T2D:{s.get('t2d')}")
        tg(digest)
        log_memory({"type":"heartbeat", "rows": man.get("rows")})

def main():
    try:
        brain_tick()
    except Exception:
        err = traceback.format_exc()
        tg("❌ Brain crash:\n" + err[-1200:])
        log_memory({"type":"brain_error", "err": err})

if __name__ == "__main__":
    main()

