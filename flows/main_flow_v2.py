# flows/main_flow_v2.py
# -*- coding: utf-8 -*-

import os
import io
import json
import hashlib
import mimetypes
import tempfile
from datetime import timezone
from pathlib import Path
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import boto3
import requests

# ---------------------------
# Конфиг / помощни функции
# ---------------------------

def get_env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None:
        raise ValueError(f"ENV {name} is empty — set it in GitHub Secrets (e.g., ${name}).")
    return v

def utc_now():
    return pd.Timestamp.utcnow().tz_localize("UTC")

def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def df_to_jsonl_bytes(df: pd.DataFrame) -> bytes:
    # Конвертираме Timestamp към ISO
    recs = []
    for r in df.to_dict(orient="records"):
        out = {}
        for k, v in r.items():
            if isinstance(v, pd.Timestamp):
                out[k] = v.tz_convert("UTC").isoformat() if v.tzinfo else pd.Timestamp(v).tz_localize("UTC").isoformat()
            else:
                out[k] = v
        recs.append(out)
    buf = io.StringIO()
    for row in recs:
        buf.write(json.dumps(row, ensure_ascii=False))
        buf.write("\n")
    return buf.getvalue().encode("utf-8")

def guess_content_type(key: str) -> str:
    mt, _ = mimetypes.guess_type(key)
    return mt or "application/octet-stream"

# ---------------------------
# S3 качване
# ---------------------------

class S3Client:
    def __init__(self, bucket: str, region: str):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region)

    def upload_bytes(self, key: str, data: bytes, content_type: Optional[str] = None) -> str:
        ct = content_type or guess_content_type(key)
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data, ContentType=ct)
        return f"s3://{self.bucket}/{key}"

    def upload_file(self, local_path: Path, key: str, content_type: Optional[str] = None) -> str:
        ct = content_type or guess_content_type(key)
        with open(local_path, "rb") as f:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=f, ContentType=ct)
        return f"s3://{self.bucket}/{key}"

# ---------------------------
# Генератор на синтетични данни
# ---------------------------

REQUIRED_COLS = [
    "activity", "alcohol", "coronary_hd", "created_at", "diabetes_type",
    "diastolic", "dyslipidemia", "glucose", "heart_rate", "height_cm",
    "hypertension", "insulin", "pregnancy", "sleep_hours", "smoker",
    "systolic", "weight_kg",
]

def generate_synthetic_patients(n: int = 50_000, seed: int = 2025) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    age = rng.integers(18, 90, size=n)
    height_cm = rng.normal(170, 10, size=n).clip(140, 210)
    weight_kg = rng.normal(75, 15, size=n).clip(40, 160)
    bmi = weight_kg / (height_cm / 100.0) ** 2

    systolic = rng.normal(120, 15, size=n).clip(85, 220)
    diastolic = rng.normal(80, 10, size=n).clip(50, 140)
    heart_rate = rng.normal(72, 10, size=n).clip(40, 180)

    glucose = rng.normal(95, 15, size=n).clip(60, 250)
    insulin = rng.normal(10, 5, size=n).clip(2, 60)

    # категориални
    smoker = rng.random(n) < 0.25
    alcohol = rng.choice(["none", "low", "moderate", "high"], size=n, p=[0.3, 0.4, 0.25, 0.05])
    activity = rng.choice(["low", "medium", "high"], size=n, p=[0.4, 0.45, 0.15])
    sleep_hours = rng.normal(7.1, 1.2, size=n).clip(3, 12)

    diabetes_type = rng.choice(["none", "type1", "type2", "gestational"], size=n, p=[0.8, 0.05, 0.14, 0.01])
    hypertension = (systolic > 140) | (diastolic > 90)
    dyslipidemia = rng.random(n) < 0.2
    coronary_hd = rng.random(n) < 0.08

    sex = rng.choice(["M", "F"], size=n, p=[0.5, 0.5])
    # pregnancy: само F, възраст 18-50 и ~3% шанс
    pregnancy = ((sex == "F") & (age >= 18) & (age <= 50) & (rng.random(n) < 0.03)).astype(int)

    created_at = pd.to_datetime(pd.Timestamp.utcnow()).tz_localize("UTC")

    df = pd.DataFrame({
        "age": age.astype(int),
        "height_cm": np.round(height_cm, 1),
        "weight_kg": np.round(weight_kg, 1),
        "bmi": np.round(bmi, 1),
        "systolic": np.round(systolic, 1),
        "diastolic": np.round(diastolic, 1),
        "heart_rate": np.round(heart_rate, 1),
        "glucose": np.round(glucose, 1),
        "insulin": np.round(insulin, 1),
        "smoker": smoker.astype(int),
        "alcohol": alcohol,
        "activity": activity,
        "sleep_hours": np.round(sleep_hours, 1),
        "pregnancy": pregnancy,
        "hypertension": hypertension.astype(int),
        "dyslipidemia": dyslipidemia.astype(int),
        "diabetes_type": diabetes_type,
        "coronary_hd": coronary_hd.astype(int),
        "created_at": created_at,  # един и същи stamp е ок за nightly
    })

    # гаранция за required
    for col in REQUIRED_COLS:
        if col not in df.columns:
            if col in ("pregnancy", "smoker", "hypertension", "dyslipidemia", "coronary_hd"):
                df[col] = 0
            elif col in ("sleep_hours",):
                df[col] = 7.0
            elif col in ("height_cm",):
                df[col] = 170.0
            elif col in ("weight_kg",):
                df[col] = 75.0
            elif col in ("systolic",):
                df[col] = 120.0
            elif col in ("diastolic",):
                df[col] = 80.0
            elif col in ("glucose",):
                df[col] = 95.0
            elif col in ("insulin",):
                df[col] = 10.0
            elif col in ("alcohol",):
                df[col] = "low"
            elif col in ("activity",):
                df[col] = "medium"
            elif col in ("diabetes_type",):
                df[col] = "none"
            elif col in ("created_at",):
                df[col] = pd.Timestamp.utcnow().tz_localize("UTC")
            else:
                df[col] = None

    # нормализиране на типове
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    return df

# ---------------------------
# Статистики + Whitepaper
# ---------------------------

def compute_stats(df: pd.DataFrame) -> pd.DataFrame:
    # Няколко базови метрики
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    desc = df[numeric_cols].describe().T
    desc["missing"] = df[numeric_cols].isna().sum()
    return desc

def build_whitepaper_md(version_day: str, stats_df: pd.DataFrame,
                        parquet_key: str, jsonl_key: str, manifest_key: str) -> str:
    try:
        stats_md = stats_df.to_markdown(index=True)
    except Exception:
        stats_md = str(stats_df)

    md = f"""# BioSynthX Артефакти — {version_day}

**Версия:** `{version_day}`

## Файлове
- **Parquet:** `{parquet_key}`
- **JSONL:** `{jsonl_key}`
- **Manifest:** `{manifest_key}`

## Метрики (numeric describe)
{stats_md}

_Генерирано автоматично от Nightly Pipeline._
"""
    return md

def build_whitepaper_pdf(md_text: str) -> Optional[bytes]:
    """
    Лек PDF чрез reportlab. Ако липсва reportlab – връща None.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer
        from reportlab.lib.units import cm

        styles = getSampleStyleSheet()
        story = []
        for para in md_text.split("\n\n"):
            story.append(Paragraph(para.replace("\n", "<br/>"), styles["Normal"]))
            story.append(Spacer(1, 0.4 * cm))

        buf = io.BytesIO()
        doc = SimpleDocTemplate(buf, pagesize=A4)
        doc.build(story)
        return buf.getvalue()
    except Exception:
        return None

# ---------------------------
# Telegram
# ---------------------------

def telegram_send(msg: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        print("⚠️  Telegram не е конфигуриран (прескачам).")
        return
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        payload = {"chat_id": chat_id, "text": msg}
        requests.post(url, json=payload, timeout=10)
    except Exception as e:
        print(f"⚠️  Telegram грешка: {e}")

# ---------------------------
# Main pipeline
# ---------------------------

def main():
    # ENV
    region = get_env("AWS_REGION", "eu-north-1")
    bucket = get_env("S3_BUCKET")
    s3 = S3Client(bucket, region)

    # Версия = Y.m.d (UTC)
    version_day = utc_now().strftime("%Y.%m.%d")
    base_prefix = f"artifacts/{version_day}"

    print(f"▶️  Генерирам данни…")
    df = generate_synthetic_patients(n=50_000, seed=2025)
    stats = compute_stats(df)

    # Пътища на ключовете в S3
    parquet_key = f"{base_prefix}/data.parquet"
    jsonl_key   = f"{base_prefix}/data.jsonl"
    manifest_key = f"{base_prefix}/manifest.json"
    white_md_key = f"{base_prefix}/whitepaper.md"
    white_pdf_key = f"{base_prefix}/whitepaper.pdf"

    # Запис/качване Parquet (локален temp файл)
    print("💾 Пиша Parquet…")
    with tempfile.TemporaryDirectory() as tmpdir:
        parquet_path = Path(tmpdir) / "data.parquet"
        df.to_parquet(parquet_path, engine="pyarrow", index=False)
        with open(parquet_path, "rb") as f:
            parquet_bytes = f.read()
        parquet_sha = sha256_bytes(parquet_bytes)
        s3.upload_file(parquet_path, parquet_key, content_type="application/octet-stream")

    # JSONL – без локален файл
    print("💾 Пиша JSONL…")
    jsonl_bytes = df_to_jsonl_bytes(df)
    jsonl_sha = sha256_bytes(jsonl_bytes)
    s3.upload_bytes(jsonl_key, jsonl_bytes, content_type="application/x-ndjson")

    # Whitepaper MD + PDF
    print("📝 Whitepaper…")
    md_text = build_whitepaper_md(version_day, stats, parquet_key, jsonl_key, manifest_key)
    s3.upload_bytes(white_md_key, md_text.encode("utf-8"), content_type="text/markdown")

    pdf_bytes = build_whitepaper_pdf(md_text)
    white_pdf_s3 = ""
    if pdf_bytes:
        s3.upload_bytes(white_pdf_key, pdf_bytes, content_type="application/pdf")
        white_pdf_s3 = f"s3://{bucket}/{white_pdf_key}"

    # Manifest
    print("🧾 Manifest…")
    manifest = {
        "version_day": version_day,
        "s3_prefix": base_prefix,
        "files": {
            "parquet": {"key": parquet_key, "sha256": parquet_sha},
            "jsonl": {"key": jsonl_key, "sha256": jsonl_sha},
            "whitepaper_md": {"key": white_md_key},
            "whitepaper_pdf": {"key": white_pdf_key} if pdf_bytes else {},
        },
        "schema": list(df.columns),
        "row_count": int(df.shape[0]),
        "created_at_utc": utc_now().isoformat(),
    }
    man_bytes = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
    man_sha = sha256_bytes(man_bytes)
    s3.upload_bytes(manifest_key, man_bytes, content_type="application/json")

    # Telegram
    msg = (
        "✅ BioSynthX Nightly OK\n"
        f"parquet: s3://{bucket}/{parquet_key}\n"
        f"jsonl:   s3://{bucket}/{jsonl_key}\n"
        f"manifest: s3://{bucket}/{manifest_key}\n"
        f"rows: {df.shape[0]}, sha256(parquet): {parquet_sha[:12]}…, sha256(jsonl): {jsonl_sha[:12]}…"
    )
    telegram_send(msg)
    print("✅ Готово.")

if __name__ == "__main__":
    main()



