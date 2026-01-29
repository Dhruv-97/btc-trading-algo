#!/usr/bin/env python3
import pandas as pd
import numpy as np

IN_PATH = "cvd_features_stream_2.csv"
OUT_PATH = "dataset_labeled_v1.csv"

HORIZON_S = 300
BUCKET_S = 5
LAG = HORIZON_S // BUCKET_S  # 60 rows

df = pd.read_csv(IN_PATH)

# sort by time to guarantee proper shifting
df = df.sort_values("bucket_time_ms").reset_index(drop=True)

# keep only warmed-up rows (avoid cold-start artifacts)
if "is_warmed_up" in df.columns:
    df = df[df["is_warmed_up"] == 1].copy()

# create future return label (log return over horizon)
close = df["close"].astype(float)
df["fut_logret_300s"] = np.log(close.shift(-LAG)) - np.log(close)

# binary target: up/down
df["y_up_300s"] = (df["fut_logret_300s"] > 0).astype(int)

# drop rows at the end that don't have future data
df = df.dropna(subset=["fut_logret_300s"]).reset_index(drop=True)

df.to_csv(OUT_PATH, index=False)
print(f"Wrote {OUT_PATH} with {len(df):,} rows")
