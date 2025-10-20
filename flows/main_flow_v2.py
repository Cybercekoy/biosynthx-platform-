# flows/main_flow_v2.py
# BioSynthX v2 (50k) — rich synthetic cohort + S3 artifacts + whitepaper + Telegram

import os
import io
import json
import math
import time
import uuid
import random
import string
import shutil
import datetime
import subprocess
from typing import Dict, Tuple, List

import boto3
import pandas as pd
import numpy as np
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas


# =========================
# ENV + validation
# =========================
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


# =========================
# Helpers
# =========================
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
    return s3().generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires,
    )

def tg(text: str):
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


# =========================
# Synthetic cohort (50k)
# =========================
COUNTRIES = ["BG", "RO", "GR", "RS", "MK", "TR", "AL", "HU", "DE", "AT", "PL", "IT", "ES", "FR", "UK", "NL", "SE"]
INSURANCE = ["public", "private", "mixed"]
EXERCISE = ["none", "low", "moderate", "high"]
ALCOHOL = ["none", "light", "moderate", "heavy"]

ORAL_T2D = ["metformin", "sulfonylurea", "dpp4", "sglt2", "tzds"]
INSULINS = ["glargine", "detemir", "aspart", "lispro", "regular"]
LIPIDS = ["statin", "ezetimibe", "pcsk9"]
BP_DRUGS = ["acei", "arb", "bb", "ccb", "thiazide"]

def random_meds(n: int, rng: np.random.Generator) -> List[str]:
    meds: List[str] = []
    # 0–3 oral agents
    if rng.random() < 0.6:
        k = rng.integers(0, 4)
        meds.extend(rng.choice(ORAL_T2D, size=k, replace=False).tolist())
    # insulin sometimes
    if rng.random() < 0.3:
        k = rng.integers(1, 3)
        meds.extend(rng.choice(INSULINS, size=k, replace=False).tolist())
    # lipids
    if rng.random() < 0.4:
        meds.append(rng.choice(LIPIDS))
    # blood pressure meds
    if rng.random() < 0.45:
        k = rng.integers(1, 3)
        meds.extend(rng.choice(BP_DRUGS, size=k, replace=False).tolist())
    # unique + sorted
    meds = sorted(list(set(meds)))
    return meds

def generate_synthetic_patients(n: int = 50_000, seed: int = 2025) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # Demographics
    age = rng.integers(18, 90, size=n)
    sex = rng.choice(["F", "M"], size=n, p=[0.52, 0.48])
    country = rng.choice(COUNTRIES, size=n)
    insurance = rng.choice(INSURANCE, size=n, p=[0.65, 0.20, 0.15])

    # Lifestyle
    smoker = rng.binomial(1, 0.27, size=n)
    alcohol = rng.choice(ALCOHOL, size=n, p=[0.35, 0.42, 0.19, 0.04])
    exercise = rng.choice(EXERCISE, size=n, p=[0.18, 0.38, 0.34, 0.10])

    # Vitals
    height_cm = np.clip(rng.normal(170, 9.5, n), 145, 205).round(1)
    weight_kg = np.clip(rng.normal(78, 16, n), 42, 170).round(1)
    bmi = (weight_kg / (height_cm / 100) ** 2).round(1)
    bp_sys = np.clip(rng.normal(128, 16, n), 90, 210).round().astype(int)
    bp_dia = np.clip(rng.normal(81, 11, n), 55, 130).round().astype(int)
    heart_rate = np.clip(rng.normal(74, 10, n), 45, 130).round().astype(int)

    # Labs
    hba1c = np.clip(rng.normal(6.2, 1.4, n), 4.8, 14.0).round(1)
    fasting_glucose = np.clip(rng.normal(6.0, 1.5, n), 3.5, 20.0).round(1)  # mmol/L
    ldl = np.clip(rng.normal(3.1, 0.9, n), 1.3, 7.0).round(1)
    hdl = np.clip(rng.normal(1.2, 0.3, n), 0.6, 2.5).round(1)
    trig = np.clip(rng.normal(1.6, 0.8, n), 0.4, 8.0).round(1)
    chol = (ldl + hdl + trig / 2).round(1)
    creatinine = np.clip(rng.normal(85, 25, n), 40, 300).round(0)  # μmol/L
    # eGFR (very rough MDRD-like mock from creatinine, age, sex)
    egfr = np.clip(175 * (creatinine / 88.4) ** -1.154 * (age ** -0.203) * np.where(sex == "F", 0.742, 1.0), 10, 150).round(0)

    # Diagnoses (probabilities depend on labs/vitals)
    t2d_prob = 1 / (1 + np.exp(-(hba1c - 6.5)))  # higher HbA1c → higher odds
    t2d = (rng.random(n) < t2d_prob * 0.9).astype(int)
    t1d = ((rng.random(n) < 0.03) & (age < 40)).astype(int)
    t2d = np.where(t1d == 1, 0, t2d)

    hypertension = ((bp_sys >= 140) | (bp_dia >= 90)).astype(int)
    dyslipidemia = ((ldl >= 4.1) | (trig >= 2.3) | (hdl < 1.0)).astype(int)

    # CKD stage from eGFR
    ckd_stage = np.select(
        [
            egfr >= 90,
            (egfr >= 60) & (egfr < 90),
            (egfr >= 45) & (egfr < 60),
            (egfr >= 30) & (egfr < 45),
            (egfr >= 15) & (egfr < 30),
            egfr < 15,
        ],
        [1, 2, 3, 4, 5, 5],
    ).astype(int)

    # Dates (within last 5 years)
    base = datetime.datetime.utcnow()
    first_visit = np.array([base - datetime.timedelta(days=int(x)) for x in rng.integers(30, 5 * 365, size=n)])
    last_visit = np.array([fv + datetime.timedelta(days=int(d)) for fv, d in zip(first_visit, rng.integers(7, 365, size=n))])
    next_followup = np.array([lv + datetime.timedelta(days=int(d)) for lv, d in zip(last_visit, rng.integers(30, 240, size=n))])

    # Pregnancy flag (only women 18–50; low prevalence)
    pregnancy = ((sex == "F") & (age >= 18) & (age <= 50) & (rng.random(n) < 0.03)).astype(int)


    # Medications (string list for storage)
    meds = [ ";".join(random_meds(1, np.random.default_rng(int(x)))) for x in rng.integers(0, 2**31 - 1, size=n) ]

    # Encounters count in last 12 months
    encounters_12m = np.clip(np.round(rng.normal(3.0 + 2.0 * t2d + 1.5 * hypertension, 2.0, n)), 0, 25).astype(int)

    df = pd.DataFrame(
        {
            "patient_id": [f"P{100000+i}" for i in range(n)],
            "age": age,
            "sex": sex,
            "country": country,
            "insurance": insurance,
            "smoker": smoker,
            "alcohol": alcohol,
            "exercise": exercise,
            "height_cm": height_cm,
            "weight_kg": weight_kg,
            "bmi": bmi,
            "bp_sys": bp_sys,
            "bp_dia": bp_dia,
            "heart_rate": heart_rate,
            "hba1c": hba1c,
            "fasting_glucose_mmol": fasting_glucose,
            "ldl": ldl,
            "hdl": hdl,
            "triglycerides": trig,
            "cholesterol": chol,
            "creatinine_umol": creatinine,
            "egfr": egfr,
            "t1d": t1d,
            "t2d": t2d,
            "hypertension": hypertension,
            "dyslipidemia": dyslipidemia,
            "ckd_stage": ckd_stage,
            "pregnancy": pregnancy,
            "medications": meds,
            "encounters_12m": encounters_12m,
            "first_visit": pd.to_datetime(first_visit),
            "last_visit": pd.to_datetime(last_visit),
            "next_followup": pd.to_datetime(next_followup),
        }
    )

    return df


def compute_stats(df: pd.DataFrame) -> Dict:
    return {
        "rows": int(len(df)),
        "t1d": int(df["t1d"].sum()),
        "t2d": int(df["t2d"].sum()),
        "hypertension": int(df["hypertension"].sum()),
        "dyslipidemia": int(df["dyslipidemia"].sum()),
        "mean_age": float(df["age"].mean()),
        "female_ratio": float((df["sex"] == "F").mean()),
        "mean_bmi": float(df["bmi"].mean()),
        "bp_sys_mean": float(df["bp_sys"].mean()),
        "bp_dia_mean": float(df["bp_dia"].mean()),
        "hba1c_mean": float(df["hba1c"].mean()),
        "egfr_mean": float(df["egfr"].mean()),
    }


# =========================
# PDF daily report
# =========================
def build_daily_pdf(stats: Dict, out_path: str):
    ensure_dir(os.path.dirname(out_path))
    c = canvas.Canvas(out_path, pagesize=A4)
    w, h = A4
    y = h - 80
    c.setFont("Helvetica-Bold", 16)
    c.drawString(72, y, "BioSynthX Daily Report (50k)")
    y -= 24
    c.setFont("Helvetica", 11)
    c.drawString(72, y, f"UTC: {ts_utc()}")
    y -= 20
    for k, v in [
        ("Rows", stats["rows"]),
        ("T1D", stats["t1d"]),
        ("T2D", stats["t2d"]),
        ("Hypertension", stats["hypertension"]),
        ("Dyslipidemia", stats["dyslipidemia"]),
        ("Mean age", f"{stats['mean_age']:.1f}"),
        ("Female ratio", f"{stats['female_ratio']:.2f}"),
        ("Mean BMI", f"{stats['mean_bmi']:.1f}"),
        ("BP mean", f"{stats['bp_sys_mean']:.0f}/{stats['bp_dia_mean']:.0f}"),
        ("HbA1c mean", f"{stats['hba1c_mean']:.1f}%"),
        ("eGFR mean", f"{stats['egfr_mean']:.0f}"),
    ]:
        c.drawString(72, y, f"{k}: {v}")
        y -= 16
    c.showPage()
    c.save()


# =========================
# Whitepaper (md → pdf)
# =========================
def build_whitepaper(stats: Dict, manifest: Dict, day_folder: str) -> Tuple[str, str | None]:
    md_path = os.path.join(day_folder, "whitepaper.md")
    pdf_path = os.path.join(day_folder, "whitepaper.pdf")

    md = f"""# BioSynthX White Paper — 50k Synthetic Cohort

**Generated (UTC):** {ts_utc()}

## 1. Summary
- Rows: **{manifest['rows']}**
- T1D: **{manifest['stats']['t1d']}**, T2D: **{manifest['stats']['t2d']}**
- Hypertension: **{manifest['stats']['hypertension']}**, Dyslipidemia: **{manifest['stats']['dyslipidemia']}**
- BMI mean: **{manifest['stats']['mean_bmi']:.1f}**, HbA1c mean: **{manifest['stats']['hba1c_mean']:.1f}%**

## 2. Columns
{", ".join(manifest["columns"])}

## 3. Notes
This dataset is fully synthetic, non-personal, and generated for research/demo.
"""
    ensure_dir(day_folder)
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    try:
        subprocess.check_call(["pandoc", md_path, "-o", pdf_path])
    except Exception:
        pdf_path = None

    return md_path, pdf_path


# =========================
# S3 upload helpers
# =========================
def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    s3().put_object(Bucket=S3_BUCKET, Key=key, Body=data, ContentType=content_type)

def upload_file(local_path: str, key: str):
    s3().upload_file(local_path, S3_BUCKET, key)


def upload_bundle(
    df: pd.DataFrame, stats: Dict, local_parquet: str, local_jsonl: str, local_pdf: str, day_folder_local: str
) -> Dict[str, str]:
    now = datetime.datetime.utcnow()
    version_day = f"v{ymd(now)}"
    version_month = f"v{ym(now)}"

    parquet_key = f"datasets/{version_month}/patients_{ymd(now)}.parquet"
    jsonl_key = f"datasets/{version_month}/patients_{ymd(now)}.jsonl"
    pdf_key = f"artifacts/{version_day}/daily_report.pdf"
    man_key = f"artifacts/{version_day}/manifest.json"

    # upload files
    upload_file(local_parquet, parquet_key)
    upload_file(local_jsonl, jsonl_key)
    upload_file(local_pdf, pdf_key)

    manifest = {
        "generated_utc": ts_utc(),
        "rows": stats["rows"],
        "stats": stats,
        "columns": df.columns.tolist(),
        "parquet_key": parquet_key,
        "jsonl_key": jsonl_key,
        "report_key": pdf_key,
        "manifest_version": 2,
    }
    upload_bytes(man_key, json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"), "application/json")

    # latest pointers
    latest_parquet = "datasets/latest/patients.parquet"
    latest_manifest = "datasets/latest/manifest.json"
    s3().copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": parquet_key}, Key=latest_parquet)
    s3().copy_object(Bucket=S3_BUCKET, CopySource={"Bucket": S3_BUCKET, "Key": man_key}, Key=latest_manifest)

    # changelog
    changelog_key = "artifacts/changelog.jsonl"
    entry = {
        "ts": ts_utc(),
        "parquet": parquet_key,
        "jsonl": jsonl_key,
        "manifest": man_key,
        "pdf": pdf_key,
        "rows": stats["rows"],
        "t1d": stats["t1d"],
        "t2d": stats["t2d"],
        "hypertension": stats["hypertension"],
        "dyslipidemia": stats["dyslipidemia"],
    }
    try:
        old = s3().get_object(Bucket=S3_BUCKET, Key=changelog_key)["Body"].read()
        body = old + (json.dumps(entry) + "\n").encode("utf-8")
    except s3().exceptions.NoSuchKey:  # type: ignore[attr-defined]
        body = (json.dumps(entry) + "\n").encode("utf-8")
    upload_bytes(changelog_key, body, "application/json")

    # whitepaper
    wp_md, wp_pdf = build_whitepaper(stats, manifest, day_folder_local)
    wp_md_key = f"artifacts/{version_day}/whitepaper.md"
    upload_file(wp_md, wp_md_key)
    wp_pdf_key = None
    if wp_pdf:
        wp_pdf_key = f"artifacts/{version_day}/whitepaper.pdf"
        upload_file(wp_pdf, wp_pdf_key)

    return {
        "parquet": parquet_key,
        "jsonl": jsonl_key,
        "manifest": man_key,
        "pdf": pdf_key,
        "whitepaper_md": wp_md_key,
        "whitepaper_pdf": wp_pdf_key or "",
    }


# =========================
# Main
# =========================
def main():
    # 1) Generate 50k rich cohort
    df = generate_synthetic_patients(n=50_000, seed=2025)
    stats = compute_stats(df)

    # 2) Local artifacts
    day_folder_local = os.path.join("artifacts", f"v{ymd()}")
    ensure_dir(day_folder_local)

    local_parquet = os.path.join(day_folder_local, f"patients_{ymd()}.parquet")
    local_jsonl = os.path.join(day_folder_local, f"patients_{ymd()}.jsonl")
    local_pdf = os.path.join(day_folder_local, "daily_report.pdf")

    # Save Parquet (snappy)
    df.to_parquet(local_parquet, engine="pyarrow", compression="snappy", index=False)

    # Save NDJSON (chunked, so memory stays low)
    with open(local_jsonl, "w", encoding="utf-8") as f:
        for chunk_start in range(0, len(df), 10_000):
            chunk = df.iloc[chunk_start:chunk_start + 10_000]
            records = chunk.to_dict(orient="records")
            for rec in records:
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    # Build daily PDF
    build_daily_pdf(stats, local_pdf)

    # 3) Upload
    keys = upload_bundle(df, stats, local_parquet, local_jsonl, local_pdf, day_folder_local)

    # 4) Presigned URLs + Telegram
    parquet_url = presign(keys["parquet"])
    jsonl_url = presign(keys["jsonl"])
    man_url = presign(keys["manifest"])
    pdf_url = presign(keys["pdf"])
    wp_url = presign(keys["whitepaper_pdf"]) if keys.get("whitepaper_pdf") else "PDF not generated"

    msg = (
        "✅ BioSynthX v2 (50k) — Nightly OK\n"
        f"Parquet: {parquet_url}\n"
        f"JSONL: {jsonl_url}\n"
        f"Manifest: {man_url}\n"
        f"Daily PDF: {pdf_url}\n"
        f"White Paper: {wp_url}\n"
        f"Rows: {stats['rows']} | T1D:{stats['t1d']} T2D:{stats['t2d']} | HTN:{stats['hypertension']} DL:{stats['dyslipidemia']}\n"
    )
    tg(msg)

    print("Done.")


if __name__ == "__main__":
    main()
