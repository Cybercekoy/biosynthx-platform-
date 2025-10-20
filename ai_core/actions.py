import json, time
import requests, boto3
from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, AWS_REGION, S3_BUCKET, GH_TOKEN, GH_REPO, WORKFLOW_FILE

def s3():
    return boto3.client("s3", region_name=AWS_REGION)

def tg(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text[:4000]},
            timeout=20,
        )
    except Exception:
        pass

def presign(key: str, expires: int = 7*24*3600) -> str:
    return s3().generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires
    )

def read_s3_text(key: str) -> str | None:
    try:
        obj = s3().get_object(Bucket=S3_BUCKET, Key=key)
        return obj["Body"].read().decode("utf-8")
    except Exception:
        return None

def write_s3_text(key: str, text: str):
    s3().put_object(Bucket=S3_BUCKET, Key=key, Body=text.encode("utf-8"), ContentType="text/plain")

def rerun_pipeline():
    
    if not (GH_TOKEN and GH_REPO):
        raise RuntimeError("GH_TOKEN/GH_REPO not set")
    url = f"https://api.github.com/repos/{GH_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"
    headers = {"Authorization": f"Bearer {GH_TOKEN}", "Accept": "application/vnd.github+json"}
    payload = {"ref": "main"}
    r = requests.post(url, headers=headers, json=payload, timeout=20)
    if r.status_code not in (201, 204):
        raise RuntimeError(f"workflow_dispatch failed: {r.status_code} {r.text}")
    return True

