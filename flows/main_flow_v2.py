# flows/main_flow_v2.py
# BioSynthX v2: nightly generator + artifacts + whitepaper + Telegram links

import os
import io
import json
import math
import time
import uuid
import shutil
import random
import string
import datetime
import subprocess
from typing import Dict, Tuple

import boto3
import pandas as pd
import numpy as np
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


# ------------------------------
# ENV + validation
# ------------------------------
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
AWS_REGION = os.getenv("AWS_REGION", "").strip()
S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

if not AWS_REGION:
    raise ValueError("AWS_REGION is empty — set it in GitHub Secrets (e.g., eu-north-1)")
if not S3_BUCKET:
    raise ValueError("S3_BUCKET is empty — set it in GitHub Secrets (e.g., biosynthx-artifacts)")

# ------------------------------
# Helpers
# ------------------------------
def s3():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID or None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY or None,
    )

def ymd(dt: datetime.datetime | None = None) -> str:
    dt = dt or datetime.datetime.utcnow()
    return dt.strftime("%Y.%m.%d")

def ym(dt: datetime.datetime | None = None) -> str:
    dt = dt or datetime.datetime.utcnow()
    return dt.strftime("%Y.%m")

def ts_utc() -> str:
    return datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def presign(key: str, expires: int = 7 * 24 * 3600) -> str:
    """Generate presigned GET URL for an S3 object key in our bucket."""
    return s3().generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )

def tg(text: str):
    """Send Telegram message; silently ignore errors."""
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=20,
        )
    except Exception:
        pass


# ------------------------------
# Synthetic data generation
# ------------------------------
def generate_synthetic_patients(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    ages = rng.integers(18, 90, size=n)
    sex = rng.choice(["F", "M"], size=n, p=[0.52, 0.48])
    # ~10% Type 1, ~25% Type 2
    t1d = rng.binomial(1, 0.10, size=n)
    t2d = rng.binomial(1, 0.25, size=n)
    # keep exclusivity: if t1d==1 -> t2d=0 (toy)
    t2d = np.where(t1d == 1, 0, t2d)

    bmi = np.round(rng.normal(27.5, 5.2, size=n), 1)
    bp_sys = np.round(rng.normal(128, 16, size=n)).astype(int)
    bp_dia = np.round(rng.normal(82, 10, size=n)).astype(int)

    df = pd.DataFrame(
        {
            "patient_id": [f"P{100000+i}" for i in range(n)],
            "age": ages,
            "sex": sex,
            "t1d": t1d,
            "t2d": t2d,
            "bmi": bmi,
            "bp_sys": bp_sys,
            "bp_dia": bp_dia,
        }
    )
    return df


def compute_stats(df: pd.DataFrame) -> Dict:
    stats = {
        "rows": int(len(df)),
        "t1d": int(df["t1d"].sum()),
        "t2d": int(df["t2d"].sum()),
        "mean_age": float(df["age"].mean()),
        "female_ratio": float((df["sex"] == "F").mean()),
        "mean_bmi": float(df["bmi"].mean()),
        "bp_sys_mean": float(df["bp_sys"].mean()),
        "bp_dia_mean": float(df["bp_dia"].mean()),
    }
    return stats


# ------------------------------
# Build daily PDF (short report)
# ------------------------------
def build_daily_pdf(stats: Dict, out_path: str):
    ensure_dir(os.path.dirname(out_path))
    c = canvas.Canvas(out_path, pagesize=A4)
    w, h = A4
    y = h - 80
    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, y, "BioSynthX Daily Report")
    y -= 24
    c.setFont("Helvetica", 11)
    c.drawString(72, y, f"UTC Date: {ts_utc()}")
    y -= 24
    for k, v in [
        ("Rows", stats["rows"]),
        ("T1D", stats["t1d"]),
        ("T2D", stats["t2d"]),
        ("Mean age", f"{stats['mean_age']:.1f}"),
        ("Female ratio", f"{stats['female_ratio']:.2f}"),
        ("Mean BMI", f"{stats['mean_bmi']:.1f}"),
        ("BP SYS mean", f"{stats['bp_sys_mean']:.1f}"),
        ("BP DIA mean", f"{stats['bp_dia_mean']:.1f}"),
    ]:
        c.drawString(72, y, f"{k}: {v}")
        y -= 18
    c.showPage()
    c.save()


# ------------------------------
# White Paper (Markdown -> PDF via pandoc)
# ------------------------------
def build_whitepaper(stats: Dict, manifest: Dict, day_folder: str) -> Tuple[str, str | None]:
    md_path = os.path.join(day_folder, "whitepaper.md")
    pdf_path = os.path.join(day_folder, "whitepaper.pdf")

    md = f"""# BioSynthX White Paper — Synthetic Diabetes Cohort

**Generated (UTC):** {ts_utc()}

## 1. Summary
- Rows: **{manifest['rows']}**
- T1D: **{manifest['stats']['t1d']}**, T2D: **{manifest['stats']['t2d']}**
- Mean age: **{manifest['stats']['mean_age']:.1f}**
- Female ratio: **{manifest['stats']['female_ratio']:.2f}**

## 2. Columns
{", ".join(manifest["columns"])}

## 3. Notes
This dataset is synthetic and provided for demo/evaluation purposes.
"""
    ensure_dir(day_folder)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    # Try to build PDF with pandoc; keep MD if pandoc missing
    try:
        subprocess.check_call(["pandoc", md_path, "-o", pdf_path])
    except Exception:
        pdf_path = None

    return md_path, pdf_path


# ------------------------------
# Upload logic
# ------------------------------
def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    s3().put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)

def upload_file(local_path: str, key: str):
    s3().upload_file(local_path, S3_BUCKET, key)

def upload_bundle(
    df: pd.DataFrame, stats: Dict, local_csv: str, local_pdf: str, day_folder_local: str
) -> Dict[str, str]:
    """
    Upload CSV + manifest + PDF into versioned folder.
    Also update latest/ and append to changelog.
    Returns S3 keys for {csv, manifest, pdf, whitepaper_md, whitepaper_pdf?}.
    """
    now = datetime.datetime.utcnow()
    version_day = f"v{ymd(now)}"
    version_month = f"v{ym(now)}"

    # Keys
    csv_key = f"datasets/{version_month}/patients_{ymd(now)}.csv"
    pdf_key = f"artifacts/{version_day}/daily_report.pdf"
    man_key = f"artifacts/{version_day}/manifest.json"

    # Upload CSV
    upload_file(local_csv, csv_key)

    # Manifest
    manifest = {
        "generated_utc": ts_utc(),
        "rows": stats["rows"],
        "stats": stats,
        "columns": df.columns.tolist(),
        "csv_key": csv_key,
        "report_key": pdf_key,
        "manifest_version": 2,
    }
    upload_bytes(man_key, json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"), "application/json")

    # Upload PDF
    upload_file(local_pdf, pdf_key)

    # Latest pointers
    latest_csv = "datasets/latest/patients.csv"
    latest_man = "datasets/latest/manifest.json"
    s3().copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": csv_key}, Key=latest_csv)
    s3().copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": man_key}, Key=latest_man)

    # Changelog append (JSON Lines)
    changelog_key = "artifacts/changelog.jsonl"
    entry = {
        "ts": ts_utc(),
        "csv": csv_key,
        "manifest": man_key,
        "pdf": pdf_key,
        "rows": stats["rows"],
        "t1d": stats["t1d"],
        "t2d": stats["t2d"],
    }
    body = (json.dumps(entry, ensure_ascii=False) + "\n").encode("utf-8")
    try:
        old = s3().get_object(Bucket=S3_BUCKET, Key=changelog_key)["Body"].read()
        body = old + body
    except s3().exceptions.NoSuchKey:  # type: ignore[attr-defined]
        pass
    upload_bytes(changelog_key, body, "application/json")

    # Whitepaper build & upload
    wp_md, wp_pdf = build_whitepaper(stats, manifest, day_folder_local)
    wp_md_key = f"artifacts/{version_day}/whitepaper.md"
    upload_file(wp_md, wp_md_key)
    wp_pdf_key = None
    if wp_pdf:
        wp_pdf_key = f"artifacts/{version_day}/whitepaper.pdf"
        upload_file(wp_pdf, wp_pdf_key)

    return {
        "csv": csv_key,
        "manifest": man_key,
        "pdf": pdf_key,
        "whitepaper_md": wp_md_key,
        "whitepaper_pdf": wp_pdf_key or "",
    }


# ------------------------------
# Main flow
# ------------------------------
def main():
    # 1) Generate data
    df = generate_synthetic_patients(n=1200, seed=2025)
    stats = compute_stats(df)

    # 2) Save local artifacts
    day_folder_local = os.path.join("artifacts", f"v{ymd()}")
    ensure_dir(day_folder_local)

    local_csv = os.path.join(day_folder_local, f"patients_{ymd()}.csv")
    df.to_csv(local_csv, index=False)

    local_pdf = os.path.join(day_folder_local, "daily_report.pdf")
    build_daily_pdf(stats, local_pdf)

    # 3) Upload everything
    s3_keys = upload_bundle(df, stats, local_csv, local_pdf, day_folder_local)

    # 4) Presigned links
    csv_url = presign(s3_keys["csv"])
    man_url = presign(s3_keys["manifest"])
    pdf_url = presign(s3_keys["pdf"])
    wp_url = presign(s3_keys["whitepaper_pdf"]) if s3_keys.get("whitepaper_pdf") else "PDF not generated"

    # 5) Notify
    msg = (
        "✅ BioSynthX v2 OK\n"
        f"CSV: {csv_url}\n"
        f"Manifest: {man_url}\n"
        f"Daily Report: {pdf_url}\n"
        f"White Paper: {wp_url}\n"
        f"Rows: {stats['rows']} | T1D:{stats['t1d']} T2D:{stats['t2d']}\n"
    )
    tg(msg)

    print("Done.")


if __name__ == "__main__":
    main()
