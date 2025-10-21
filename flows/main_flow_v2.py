import os
import json
import hashlib
import subprocess
from pathlib import Path
from datetime import datetime, date
import tempfile
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd
import boto3
from prefect import flow
import prefect


# =========================================================
# JSON-safe encoder: Timestamp / NumPy / NaN → сериализируеми
# =========================================================
def json_default(o):
    if isinstance(o, (np.integer, np.int32, np.int64, pd.Int64Dtype)):
        return int(o)
    if isinstance(o, (np.floating, np.float32, np.float64)):
        # NaN -> None
        try:
            if np.isnan(o):
                return None
        except Exception:
            pass
        return float(o)
    if isinstance(o, (np.ndarray,)):
        return o.tolist()
    if isinstance(o, (pd.Timestamp, np.datetime64, datetime, date)):
        return pd.Timestamp(o).isoformat()
    try:
        if pd.isna(o):
            return None
    except Exception:
        pass
    return str(o)


# =========================================================
# AWS S3 utils
# =========================================================
def s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )


def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    bucket = os.getenv("S3_BUCKET")
    s3_client().put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    return f"s3://{bucket}/{key}"


def upload_file(local_path: str, key: str) -> str:
    bucket = os.getenv("S3_BUCKET")
    s3_client().upload_file(local_path, bucket, key)
    return f"s3://{bucket}/{key}"


# =========================================================
# Synthetic data generator (50k high-detail)
# =========================================================
 import numpy as np
import pandas as pd

def generate_synthetic_patients(n: int = 50_000, seed: int = 2025) -> pd.DataFrame:
    """
    Генерира синтетични пациенти с всички задължителни колони.
    """
    rng = np.random.default_rng(seed)

    # Демография
    age = rng.integers(18, 90, size=n)
    height_cm = rng.normal(170, 10, size=n).clip(140, 210).round(1)
    weight_kg = rng.normal(75, 15, size=n).clip(40, 200).round(1)
    bmi = (weight_kg / ((height_cm / 100) ** 2)).round(1)

    # Витални показатели
    systolic = rng.normal(122, 14, size=n).clip(90, 210).round(0)
    diastolic = rng.normal(78, 10, size=n).clip(50, 130).round(0)
    heart_rate = rng.normal(72, 10, size=n).clip(40, 140).round(0)

    # Метаболитни
    glucose = rng.normal(5.3, 1.2, size=n).clip(3.0, 25.0).round(1)    # mmol/L
    insulin = rng.normal(8, 6, size=n).clip(0, 60).round(1)            # μIU/mL
    dyslipidemia = (rng.random(n) < 0.22).astype(int)

    # Навици / начин на живот
    smoker = (rng.random(n) < 0.23).astype(int)
    alcohol = (rng.random(n) < 0.35).astype(int)
    activity = rng.integers(0, 4, size=n)  # 0=ниска, 1=умерена, 2=висока, 3=спорт

    sleep_hours = rng.normal(7.0, 1.1, size=n).clip(3.0, 12.0).round(1)

    # Рискови/състояния
    hypertension = (systolic >= 140).astype(int)
    coronary_hd = ((rng.random(n) < 0.07) | ((age > 55) & (hypertension == 1))).astype(int)

    # Диабет – вид: 0=няма, 1=тип 1, 2=тип 2 (основно тип 2)
    p_diab = np.where(age < 30, 0.06, 0.17)
    has_diabetes = (rng.random(n) < p_diab).astype(int)
    diabetes_type = np.where(
        has_diabetes == 0, 0,
        np.where(rng.random(n) < 0.12, 1, 2)
    )

    # Бременност (валидно само за част от популацията; моделно допускане)
    pregnancy = ((rng.random(n) < 0.03) & (age.between(18, 50))).astype(int)

    # Времеви печат
    created_at = pd.Timestamp.utcnow().isoformat()

    df = pd.DataFrame({
        "age": age,
        "height_cm": height_cm,
        "weight_kg": weight_kg,
        "bmi": bmi,
        "systolic": systolic,
        "diastolic": diastolic,
        "heart_rate": heart_rate,
        "glucose": glucose,
        "insulin": insulin,
        "diabetes_type": diabetes_type,
        "smoker": smoker,
        "alcohol": alcohol,
        "activity": activity,
        "sleep_hours": sleep_hours,
        "pregnancy": pregnancy,
        "hypertension": hypertension,
        "dyslipidemia": dyslipidemia,
        "coronary_hd": coronary_hd,
        "created_at": created_at,
    })

    # Подреждаме колоните в точно този ред (както Brain ги очаква)
    required_order = [
        "age","height_cm","weight_kg","bmi",
        "systolic","diastolic","heart_rate","glucose","insulin",
        "diabetes_type","smoker","alcohol","activity","sleep_hours",
        "pregnancy","hypertension","dyslipidemia","coronary_hd",
        "created_at",
    ]
    df = df[required_order]
    return df


# =========================================================
# Stats & manifest
# =========================================================
def compute_stats(df: pd.DataFrame) -> Dict[str, Any]:
    stats = {
        "count": int(len(df)),
        "by_sex": df["sex"].value_counts(dropna=False).to_dict(),
        "age": {
            "min": float(df["age"].min()),
            "max": float(df["age"].max()),
            "mean": float(df["age"].mean()),
        },
        "bmi": {
            "min": float(df["bmi"].min()),
            "max": float(df["bmi"].max()),
            "mean": float(df["bmi"].mean()),
        },
        "hypertension_rate": float(df["hypertension"].mean()),
        "diabetes_type": df["diabetes_type"].value_counts(dropna=False).to_dict(),
        "smoker_rate": float(df["smoker"].mean()),
    }
    return stats


# =========================================================
# Writers
# =========================================================
def write_jsonl(df: pd.DataFrame, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        for rec in df.to_dict(orient="records"):
            f.write(json.dumps(rec, ensure_ascii=False, default=json_default) + "\n")


def write_parquet(df: pd.DataFrame, path: Path):
    # pyarrow е инсталиран в Actions
    df.to_parquet(path, index=False)


# =========================================================
# Whitepaper (Markdown + опционално PDF през pandoc)
# =========================================================
def build_whitepaper_md(stats: Dict[str, Any], manifest: Dict[str, Any]) -> str:
    lines = [
        "# BioSynthX Synthetic Cohort — Nightly Whitepaper",
        "",
        f"**Generated at:** {manifest['generated_at']}",
        f"**Records:** {manifest['records']}",
        "",
        "## Demographics",
        f"- Sex distribution: `{stats['by_sex']}`",
        f"- Age: min `{stats['age']['min']}`, mean `{stats['age']['mean']:.2f}`, max `{stats['age']['max']}`",
        "",
        "## Clinical",
        f"- BMI: min `{stats['bmi']['min']:.1f}`, mean `{stats['bmi']['mean']:.2f}`, max `{stats['bmi']['max']:.1f}`",
        f"- Hypertension rate: `{stats['hypertension_rate']:.3f}`",
        f"- Diabetes types: `{stats['diabetes_type']}`",
        f"- Smokers: `{stats['smoker_rate']:.3f}`",
        "",
        "## Artifacts",
        f"- S3 prefix: `{manifest['s3_prefix']}`",
    ]
    return "\n".join(lines) + "\n"


def try_pandoc_to_pdf(md_path: Path, pdf_path: Path) -> bool:
    try:
        subprocess.run(
            ["pandoc", str(md_path), "-o", str(pdf_path)],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        return True
    except Exception:
        return False


# =========================================================
# Telegram helper (optional)
# =========================================================
def notify_telegram(message: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return
    try:
        import requests

        url = f"https://api.telegram.org/bot{token}/sendMessage"
        requests.post(url, json={"chat_id": chat_id, "text": message[:4000]})
    except Exception:
        pass


# =========================================================
# Main Flow
# =========================================================
@flow(name="BioSynthX Nightly Pipeline v2 (full)")
def main():
    log = prefect.get_run_logger()
    n = int(os.getenv("BSX_N_PATIENTS", "50000"))
    seed = int(os.getenv("BSX_SEED", "2025"))

    log.info(f"🚀 Generating synthetic cohort: n={n}, seed={seed}")
    df = generate_synthetic_patients(n=n, seed=seed)
    stats = compute_stats(df)

    day = datetime.now().strftime("%Y.%m.%d")
    prefix = f"artifacts/{day}"
    tmp = Path(tempfile.mkdtemp())

    # ---- write artifacts locally
    jsonl_path = tmp / f"patients_{day}.jsonl"
    parquet_path = tmp / f"patients_{day}.parquet"
    manifest_path = tmp / f"manifest_{day}.json"
    md_path = tmp / f"whitepaper_{day}.md"
    pdf_path = tmp / f"whitepaper_{day}.pdf"

    # JSONL / Parquet
    write_jsonl(df, jsonl_path)
    write_parquet(df, parquet_path)

    # Manifest
    manifest = {
        "generated_at": datetime.now().isoformat(),
        "records": int(len(df)),
        "columns": df.columns.tolist(),
        "s3_prefix": prefix,
        "hash": sha256_file(jsonl_path),
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2, default=json_default)

    # Whitepaper MD (+ optional PDF)
    md_text = build_whitepaper_md(stats, manifest)
    md_path.write_text(md_text, encoding="utf-8")

    has_pdf = try_pandoc_to_pdf(md_path, pdf_path)

    # ---- upload to S3
    log.info("☁️ Uploading artifacts to S3…")
    parquet_key = f"{prefix}/patients.parquet"
    jsonl_key = f"{prefix}/patients.jsonl"
    man_key = f"{prefix}/manifest.json"
    wp_md_key = f"{prefix}/whitepaper.md"
    wp_pdf_key = f"{prefix}/whitepaper.pdf"

    parquet_uri = upload_file(str(parquet_path), parquet_key)
    jsonl_uri = upload_file(str(jsonl_path), jsonl_key)
    man_uri = upload_file(str(manifest_path), man_key)
    wp_md_uri = upload_file(str(md_path), wp_md_key)

    wp_pdf_uri = ""
    if has_pdf and pdf_path.exists():
        wp_pdf_uri = upload_file(str(pdf_path), wp_pdf_key)

    # changelog append (one JSON per day)
    changelog_key = f"changelog/{day}.json"
    change = {
        "date": datetime.now().isoformat(),
        "records": int(len(df)),
        "parquet": parquet_uri,
        "jsonl": jsonl_uri,
        "manifest": man_uri,
        "whitepaper_md": wp_md_uri,
        "whitepaper_pdf": wp_pdf_uri,
    }
    upload_bytes(changelog_key, (json.dumps(change, ensure_ascii=False, default=json_default) + "\n").encode("utf-8"),
                 "application/json")

    # Telegram (optional)
    notify_telegram(
        "✅ BioSynthX v2 OK\n"
        f"{jsonl_uri}\nmanifest: {man_uri}\nsha256: {manifest['hash']}\n"
        f"MD: {wp_md_uri}\nPDF: {wp_pdf_uri or '(none)'}"
    )

    log.info("🎉 Done v2 (full).")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


if __name__ == "__main__":
    main()

