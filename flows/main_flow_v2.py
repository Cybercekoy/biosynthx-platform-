# flows/main_flow_v2.py
# BioSynthX – Data Pipeline v2
# Генерира синтетични пациенти (по подразбиране 50 000), записва Parquet и JSONL,
# билдва whitepaper (Markdown + опционално PDF), качва в S3 и праща Telegram съобщение.

from __future__ import annotations

import os
import io
import json
import math
import shutil
import hashlib
import subprocess
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List

import numpy as np
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import requests


# ---------- Конфигурация от средата ----------

AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")
TG_BOT = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")

UTC = timezone.utc
TODAY = datetime.now(UTC).date()
VERSION_DAY = TODAY.strftime("%Y.%m.%d")   # напр. 2025.10.21
S3_PREFIX = f"artifacts/{VERSION_DAY}"
LOCAL_DAY_DIR = Path(".artifacts") / VERSION_DAY
LOCAL_DAY_DIR.mkdir(parents=True, exist_ok=True)


# ---------- Утилити ----------

def sha256_str(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_iso(value: Any) -> Any:
    """Прави Timestamp/датите JSON-сериализируеми."""
    if isinstance(value, (datetime, pd.Timestamp)):
        return pd.Timestamp(value).tz_convert(UTC).isoformat() if pd.Timestamp(value).tzinfo else pd.Timestamp(value).tz_localize(UTC).isoformat()
    return value


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=lambda x: ensure_iso(x))


def telegram_notify(text: str) -> None:
    if not TG_BOT or not TG_CHAT:
        print("ℹ️ Telegram не е конфигуриран (пропускам).")
        return
    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{TG_BOT}/sendMessage",
            json={"chat_id": TG_CHAT, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if resp.status_code != 200:
            print("⚠️ Telegram error:", resp.text)
    except Exception as e:
        print("⚠️ Telegram exception:", e)


# ---------- S3 ----------

def s3_client():
    return boto3.client("s3", region_name=AWS_REGION)


def upload_file(local_path: Path, s3_key: str) -> str:
    assert S3_BUCKET, "S3_BUCKET е празен!"
    s3 = s3_client()
    s3.upload_file(str(local_path), S3_BUCKET, s3_key)
    return f"s3://{S3_BUCKET}/{s3_key}"


def upload_bytes(s3_key: str, content: bytes, content_type: str = "application/octet-stream") -> str:
    assert S3_BUCKET, "S3_BUCKET е празен!"
    s3 = s3_client()
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=content, ContentType=content_type)
    return f"s3://{S3_BUCKET}/{s3_key}"


# ---------- Генериране на синтетични данни ----------

REQUIRED_COLUMNS = [
    "activity", "alcohol", "coronary_hd", "created_at", "diabetes_type", "diastolic",
    "dyslipidemia", "glucose", "heart_rate", "height_cm", "hypertension", "insulin",
    "pregnancy", "sleep_hours", "smoker", "systolic", "weight_kg",
]

ALL_COLUMNS = REQUIRED_COLUMNS + [
    "age", "bmi", "sex"
]


def generate_synthetic_patients(n: int = 50_000, seed: int = 2025) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # Основни демографски
    age = rng.integers(18, 90, size=n)
    sex = rng.choice(["M", "F"], size=n, p=[0.49, 0.51])

    # Антропометрия
    height_cm = rng.normal(170, 10, size=n).clip(140, 200).round(1)
    weight_kg = rng.normal(75, 15, size=n).clip(40, 180).round(1)
    bmi = (weight_kg / ((height_cm / 100) ** 2)).round(1)

    # Витални показатели
    systolic = (rng.normal(122, 15, size=n) + (age - 45) * 0.2 + (bmi - 25) * 0.6).round(0).astype(int)
    diastolic = (rng.normal(78, 10, size=n) + (bmi - 25) * 0.3).round(0).astype(int)
    heart_rate = rng.normal(72, 8, size=n).clip(45, 140).round(0).astype(int)

    # Лайфстайл и състояния
    smoker = rng.random(n) < 0.26
    alcohol = rng.integers(0, 15, size=n)  # напитки/седмица
    activity = rng.integers(0, 7, size=n)  # дни спорт/седмица
    sleep_hours = rng.normal(7.1, 1.1, size=n).clip(3, 12).round(1)

    # Метаболитни показатели
    insulin = rng.normal(12, 6, size=n).clip(2, 60).round(1)
    glucose = (rng.normal(5.2, 0.7, size=n) + (bmi - 25) * 0.02 + smoker * 0.1).round(2)

    # Диагнози (зависими от рискови фактори)
    hypertension = (systolic >= 140) | (diastolic >= 90)
    dyslipidemia = (bmi >= 28) | (rng.random(n) < 0.12)
    diabetes_type = np.where(
        glucose >= 7.0,
        np.where(rng.random(n) < 0.85, "T2D", "T1D"),
        "none",
    )
    # Коронарна болест риск
    base_chd = 0.04 + (age - 40) * 0.002 + (bmi - 25) * 0.003 + smoker * 0.05 + hypertension * 0.05 + (diabetes_type != "none") * 0.08
    coronary_hd = rng.random(n) < np.clip(base_chd, 0, 0.9)

    # Бременност – само при жени и възраст 18-50
    can_preg = (sex == "F") & (age >= 18) & (age <= 50)
    pregnancy = (rng.random(n) < 0.03) & can_preg

    # Време на създаване
    created_at = pd.to_datetime(datetime.now(UTC))  # tz-aware

    df = pd.DataFrame({
        "age": age,
        "sex": sex,
        "height_cm": height_cm,
        "weight_kg": weight_kg,
        "bmi": bmi.round(1),
        "systolic": systolic,
        "diastolic": diastolic,
        "heart_rate": heart_rate,
        "smoker": smoker.astype(bool),
        "alcohol": alcohol.astype(int),
        "activity": activity.astype(int),
        "sleep_hours": sleep_hours,
        "insulin": insulin,
        "glucose": glucose,
        "hypertension": hypertension.astype(bool),
        "dyslipidemia": dyslipidemia.astype(bool),
        "diabetes_type": diabetes_type,
        "coronary_hd": coronary_hd.astype(bool),
        "pregnancy": pregnancy.astype(bool),
        "created_at": created_at,
    })

    # Колоните в стабилен ред
    df = df[ALL_COLUMNS]
    return df


# ---------- Статистики и качество ----------

@dataclass
class DataQuality:
    generated_rows: int
    required_columns_ok: bool
    missing_required: List[str]
    min_rows_ok: bool
    min_rows_threshold: int
    recency_ok: bool
    recency_hint: str

    def summary_mark(self) -> str:
        ok = self.required_columns_ok and self.min_rows_ok and self.recency_ok
        return "PASS" if ok else "FAIL"


def compute_stats(df: pd.DataFrame) -> Dict[str, Any]:
    stats = {
        "rows": int(len(df)),
        "cols": int(df.shape[1]),
        "created_at_min": df["created_at"].min(),
        "created_at_max": df["created_at"].max(),
        "age_mean": float(df["age"].mean()),
        "bmi_mean": float(df["bmi"].mean()),
        "systolic_mean": float(df["systolic"].mean()),
        "diastolic_mean": float(df["diastolic"].mean()),
        "hypertension_rate": float(df["hypertension"].mean()),
        "diabetes_rate": float((df["diabetes_type"] != "none").mean()),
        "smoker_rate": float(df["smoker"].mean()),
        "chd_rate": float(df["coronary_hd"].mean()),
    }
    return stats


def check_quality(df: pd.DataFrame, min_rows: int = 50_000, recency_days: int = 1) -> DataQuality:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    required_ok = len(missing) == 0
    min_ok = len(df) >= min_rows

    if "created_at" in df.columns and len(df):
        latest = pd.Timestamp(df["created_at"].max()).tz_convert(UTC) if pd.Timestamp(df["created_at"].max()).tzinfo else pd.Timestamp(df["created_at"].max()).tz_localize(UTC)
        cutoff = pd.Timestamp(datetime.now(UTC) - timedelta(days=recency_days))
        recency_ok = latest >= cutoff
        recency_hint = f"latest={latest.isoformat()} cutoff={cutoff.isoformat()}"
    else:
        recency_ok = False
        recency_hint = "missing created_at"

    return DataQuality(
        generated_rows=int(len(df)),
        required_columns_ok=required_ok,
        missing_required=missing,
        min_rows_ok=min_ok,
        min_rows_threshold=min_rows,
        recency_ok=recency_ok,
        recency_hint=recency_hint,
    )


# ---------- Запис локално ----------

def save_parquet_jsonl(df: pd.DataFrame, base_dir: Path) -> Tuple[Path, Path]:
    pq_path = base_dir / "patients.parquet"
    js_path = base_dir / "patients.jsonl"

    # Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, pq_path, compression="snappy")

    # JSONL (Timestamp → ISO)
    with js_path.open("w", encoding="utf-8") as f:
        for _, row in df.iterrows():
            obj = {k: ensure_iso(v) for k, v in row.to_dict().items()}
            f.write(json_dumps(obj) + "\n")

    return pq_path, js_path


# ---------- Whitepaper (Markdown + опц. PDF чрез pandoc) ----------

def have_pandoc() -> bool:
    try:
        subprocess.run(["pandoc", "-v"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except Exception:
        return False


def build_whitepaper(stats: Dict[str, Any], manifest: Dict[str, Any], base_dir: Path) -> Tuple[Path, Optional[Path]]:
    md = base_dir / "whitepaper.md"
    pdf = base_dir / "whitepaper.pdf"

    md.write_text(
        f"""# BioSynthX Артефакти – {VERSION_DAY}

Дата/час на билд: **{utcnow_iso()}**

## Резюме

- Редове: **{stats['rows']:,}**
- Колони: **{stats['cols']}**
- Средна възраст: **{stats['age_mean']:.1f}**
- Среден BMI: **{stats['bmi_mean']:.1f}**
- Хипертония (дял): **{stats['hypertension_rate']:.2%}**
- Диабет (дял): **{stats['diabetes_rate']:.2%}**
- Пушачи (дял): **{stats['smoker_rate']:.2%}**
- Коронарна болест (дял): **{stats['chd_rate']:.2%}**

## Manifest

```json
{json_dumps(manifest)}


