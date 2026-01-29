#!/usr/bin/env python3
import pandas as pd

IN_PATH = "dataset_labeled_v1.csv"
OUT_PATH = "dataset_labeled_v1_causal.csv"

BUCKET_S = 5

SHIFTS = {
    "flow_z_300s": 300,
    "flow_adv_900s": 900,

    "flow_share_ema_60s": 60,
    "flow_share_ema_300s": 300,
    "flow_share_ema_900s": 900,

    "flow_accel_60_300": 300,
    "flow_accel_300_900": 900,

    "rv_300s": 300,

    "ret_60s": 60,
    "ret_300s": 300,
    "ret_900s": 900,
}

df = pd.read_csv(IN_PATH).sort_values("bucket_time_ms").reset_index(drop=True)

for col, horizon_s in SHIFTS.items():
    if col in df.columns:
        rows = horizon_s // BUCKET_S
        df[col] = df[col].shift(rows)

# drop rows made invalid by shifting
df = df.dropna().reset_index(drop=True)

df.to_csv(OUT_PATH, index=False)
print(f"Wrote causal dataset: {OUT_PATH} ({len(df)} rows)")
