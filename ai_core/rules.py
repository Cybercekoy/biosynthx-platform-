def detect_anomalies(manifest: dict) -> list[str]:
   
    alerts = []
    if not manifest:
        alerts.append("No latest manifest found.")
        return alerts

    stats = manifest.get("stats", {})
    rows = manifest.get("rows", 0)
    mean_age = stats.get("mean_age", 0)
    female_ratio = stats.get("female_ratio", 0)
    hba1c_mean = stats.get("hba1c_mean", None) or stats.get("hba1c", None)

    if rows < 1000:
        alerts.append(f"Low row count: {rows} < 1000")
    if not (0.35 <= female_ratio <= 0.65):
        alerts.append(f"Female ratio out of band: {female_ratio}")
    if mean_age < 15 or mean_age > 90:
        alerts.append(f"Mean age abnormal: {mean_age}")
    if hba1c_mean is not None and not (4.5 <= float(hba1c_mean) <= 10.5):
        alerts.append(f"HbA1c mean suspicious: {hba1c_mean}")

    return alerts

