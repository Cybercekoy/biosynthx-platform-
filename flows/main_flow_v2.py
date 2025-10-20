import os, json, datetime, hashlib, io
from dataclasses import dataclass
import numpy as np
import pandas as pd
import boto3, requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from prefect import flow, task

# --- ENV ---
S3_BUCKET   = os.getenv("S3_BUCKET")
raise ValueError("AWS_REGION is empty — set it in GitHub Secrets (e.g., $AWS_REGION)")
S3_ENDPOINT = os.getenv("S3_ENDPOINT_URL", "")  # optional for non-AWS S3
TG_TOKEN    = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT     = os.getenv("TELEGRAM_CHAT_ID", "")

# --- HELPERS ---
def s3():
    if not AWS_REGION:
 raise ValueError("AWS_REGION is empty – set it in GitHub Secrets (e.g.,$
    params = {"region_name": AWS_REGION}
    if S3_ENDPOINT:
        params["endpoint_url"] = S3_ENDPOINT
    return boto3.client("s3", **params)
            
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
        
def ym() -> str:
    return datetime.datetime.utcnow().strftime("%Y.%m")
  def ymd() -> str:
    return datetime.datetime.utcnow().strftime("%Y.%m.%d")
                
def presign(key, expires=7*24*3600):
    return s3().generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=expires
    )
  
@dataclass
class Stats:    
    n:int; t1d:int; t2d:int; mean_age:float; female:float
    
# --- TASKS ---
@task(name="generate_dataset")
def generate_dataset(n:int=1000, t1d_ratio:float=0.35, seed:int=42) -> pd.DataF$
    rng = np.random.default_rng(seed)
    sex = rng.choice(["F","M"], size=n, p=[0.52, 0.48])
   ages = rng.integers(12, 90, size=n)
    dx   = rng.choice(["T1D","T2D"], size=n, p=[t1d_ratio, 1-t1d_ratio])
    a1c  = (dx == "T1D") * rng.normal(8.1, 1.2, size=n) + (dx == "T2D") * rng.n$
    a1c  = np.clip(a1c, 5.0, 14.0)
    insulin = (dx == "T1D") & (rng.random(size=n) < 0.92) | ((dx == "T2D") & (r$
    cgm_days = np.where(dx=="T1D", rng.integers(15, 30, size=n), rng.integers(0$
    df = pd.DataFrame({
        "patient_id": [f"PX{100000+i}" for i in range(n)],
        "sex": sex,
        "age": ages.astype(int),
"diagnosis": dx,
        "hba1c": a1c.round(2),
        "insulin": insulin.astype(int),
        "cgm_days_last30": cgm_days.astype(int)
    })
    return df
    
@task(name="validate_dataset")
def validate_dataset(df: pd.DataFrame) -> Stats:
     required = ["patient_id","sex","age","diagnosis","hba1c","insulin","cgm_days_last30"]
      missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if df["age"].min() < 0 or df["age"].max() > 120:
        raise ValueError("Age out of range.")
    if not df["diagnosis"].isin(["T1D","T2D"]).all():
        raise ValueError("Invalid diagnosis values.")
    if (df["hba1c"] < 4.0).any() or (df["hba1c"] > 20.0).any():
        raise ValueError("HbA1c out of allowed bounds.")
    if df["patient_id"].duplicated().any():
 raise ValueError("Duplicate patient_id found.")   
    n = len(df)
    t1d = int((df["diagnosis"]=="T1D").sum())
    t2d = n - t1d
    mean_age = float(df["age"].mean())
    female = float((df["sex"]=="F").mean())
    return Stats(n=n, t1d=t1d, t2d=t2d, mean_age=mean_age, female=female)
    
@task(name="materialize_and_manifest")
def materialize_and_manifest(df: pd.DataFrame, stats: Stats):
          ym_folder = f"datasets/v{ym()}"
    os.makedirs(ym_folder, exist_ok=True)
    csv_path = f"{ym_folder}/patients.csv"   
    pq_path  = f"{ym_folder}/patients.parquet"
    man_path = f"{ym_folder}/manifest.json"
    df.to_csv(csv_path, index=False)
    try:
        df.to_parquet(pq_path, index=False)  # requires pyarrow or fastparquet
    except Exception:
        pq_path = None
  man = {
        "dataset": "biosynthx_t1d_t2d",  
        "period": ym(),
        "rows": int(len(df)),
        "columns": list(df.columns),
        "stats": {
            "t1d": stats.t1d, "t2d": stats.t2d,
            "mean_age": round(stats.mean_age,2),
            "female_ratio": round(stats.female,3)
        },
"created_utc": datetime.datetime.utcnow().isoformat() + "Z",
        "files": {"csv": csv_path, "parquet": pq_path}
    }
    with open(man_path, "w", encoding="utf-8") as f:
        json.dump(man, f, ensure_ascii=False, indent=2)
    return csv_path, pq_path, man_path, man
            
@task(name="generate_pdf_report")
def generate_pdf_report(stats: Stats) -> str:
    folder = f"artifacts/v{ymd()}"
 os.makedirs(folder, exist_ok=True)
    pdf_path = f"{folder}/daily_report.pdf"
    c = canvas.Canvas(pdf_path, pagesize=A4)
    w, h = A4
    y = h - 50
    def line(text, dy=24):
        nonlocal y
        c.drawString(50, y, text)
        y -= dy
     c.setFont("Helvetica-Bold", 16); line("BioSynthX — Daily Synthetic Data Report")
    c.setFont("Helvetica", 11); line(f"Date (UTC): {datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}")
    line("")
    c.setFont("Helvetica-Bold", 13); line("Dataset: biosynthx_t1d_t2d")
    c.setFont("Helvetica", 12)
    line(f"Total patients: {stats.n}")
    line(f"T1D: {stats.t1d}  |  T2D: {stats.t2d}")
    line(f"Mean age: {stats.mean_age:.2f}")
    line(f"Female ratio: {stats.female:.3f}")
    line("")
    line("Notes:")
    line("- Synthetic data for demo; add clinical rules/constraints as needed.", dy=18)
    c.showPage(); c.save()
    return pdf_path

@task(name="upload_all_to_s3")
def upload_all_to_s3(paths_and_manifest):
    csv_path, pq_path, man_path, man = paths_and_manifest
    c = s3()
    csv_key = csv_path
    pq_key  = pq_path if pq_path else None
    man_key = man_path
    pdf_key = f"artifacts/v{ymd()}/daily_report.pdf"
    c.upload_file(csv_path, S3_BUCKET, csv_key)
    if pq_key:
        c.upload_file(pq_path, S3_BUCKET, pq_key)
    c.upload_file(man_path, S3_BUCKET, man_key)
    local_pdf = f"artifacts/v{ymd()}/daily_report.pdf"
    if os.path.exists(local_pdf):
        c.upload_file(local_pdf, S3_BUCKET, pdf_key)
    out = {
        "csv": f"s3://{S3_BUCKET}/{csv_key}",
        "parquet": f"s3://{S3_BUCKET}/{pq_key}" if pq_key else None,
      "manifest": f"s3://{S3_BUCKET}/{man_key}",
        "pdf": f"s3://{S3_BUCKET}/{pdf_key}",
    }
    return out, man
    
@flow(name="nightly-pipeline-v2")
def nightly_pipeline_v2():
    try:
        df = generate_dataset()
        stats = validate_dataset(df)
        bundle = materialize_and_manifest(df, stats)
        _ = generate_pdf_report(stats)
        s3_uris, manifest = upload_all_to_s3(bundle)
        msg = (
            "✅ BioSynthX v2 OK\n"
            f"CSV: {s3_uris['csv']}\n"
            f"Parquet: {s3_uris['parquet']}\n"
            f"Manifest: {s3_uris['manifest']}\n"
            f"Report: {s3_uris['pdf']}\n"
            f"Rows: {manifest['rows']} | T1D:{manifest['stats']['t1d']} T2D:{manifest['stats']['t2d']}\n"
        )
        tg(msg)
    except Exception as e:
        tg(f"❌ BioSynthX v2 FAILED: {e}")
        raise
if __name__ == "__main__":
    nightly_pipeline_v2()
