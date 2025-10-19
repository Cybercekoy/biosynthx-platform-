import os, json, datetime, hashlib, requests
import boto3
from prefect import flow, task

S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "eu-central-1")
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID", "")

def tg(msg: str):
    if TG_TOKEN and TG_CHAT:
        try:
            requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                data={"chat_id": TG_CHAT, "text": msg[:4000]},
                timeout=10
            )
        except Exception:
            pass

def s3():
    return boto3.client("s3", region_name=AWS_REGION)

def stamp():
    return datetime.datetime.utcnow().strftime("%Y.%m.%d")

@task
def generate_demo_artifact():
    payload = {
        "dataset": "biosynthx_demo",
        "generated_at_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "records": 1000,
        "note": "demo artifact; replace with real synthetic generation"
    }
    data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    sha = hashlib.sha256(data).hexdigest()
    return data, sha

@task
def upload_to_s3(data: bytes, sha: str):
    folder = f"artifacts/v{stamp()}"
    obj_key = f"{folder}/demo_artifact.json"
    man_key = f"{folder}/manifest.json"
    c = s3()
    c.put_object(Bucket=S3_BUCKET, Key=obj_key, Body=data, ContentType="application/json")
    manifest = {
        "artifact": obj_key,
        "sha256": sha,
        "size_bytes": len(data),
        "created_utc": datetime.datetime.utcnow().isoformat() + "Z"
    }
    c.put_object(Bucket=S3_BUCKET, Key=man_key,
                 Body=json.dumps(manifest).encode("utf-8"),
                 ContentType="application/json")
    return obj_key, man_key

@flow
def nightly_pipeline():
    try:
        data, sha = generate_demo_artifact()
        obj_key, man_key = upload_to_s3(data, sha)
        tg(f"✅ BioSynthX Nightly OK\ns3://{S3_BUCKET}/{obj_key}\nmanifest: s3://{S3_BUCKET}/{man_key}\nSHA256: {sha[:12]}…")
    except Exception as e:
        tg(f"❌ BioSynthX Nightly FAILED: {e}")
        raise

if __name__ == "__main__":
    nightly_pipeline()

