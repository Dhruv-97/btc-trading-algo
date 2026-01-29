#!/usr/bin/env python3
"""
BTC Confluence v1.1 — FLOW-BASED DIRECTION

Changes vs v1:
- Direction comes from flow_z_300s (pressure), NOT VWAP side

Confluences:
A) Location: abs(price - session_vwap)/vwap <= VWAP_BAND
B) Regime:   rv_300s > EMA(rv_300s, 900s)
C) Timing:   abs(flow_z_300s) >= Z_THR

Direction:
- sign(flow_z_300s)

Exit:
- Fixed horizon (300s)
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit

# =========================
# Config
# =========================
DATA_PATH = "dataset_labeled_v1_causal.csv"
N_SPLITS = 5
BUCKET_SECONDS = 5

VWAP_BAND = 0.0010      # 10 bps (same as last run)
Z_THR = 1.0
VOL_EMA_SECONDS = 900
HORIZON_SECONDS = 300

ROUND_TRIP_COST_BPS = 0.0

# =========================
# Helpers
# =========================
def ema(x, span):
    return pd.Series(x).ewm(span=span, adjust=False).mean().to_numpy()

def logret(a, b):
    if a <= 0 or b <= 0:
        return np.nan
    return np.log(b / a)

def safe_mean(x):
    return float(np.mean(x)) if len(x) else np.nan

def safe_median(x):
    return float(np.median(x)) if len(x) else np.nan

def pick_volume_proxy(df, bucket_seconds):
    if "notional" in df.columns:
        return df["notional"].values, "notional"
    if "notional_per_sec" in df.columns:
        return df["notional_per_sec"].values * bucket_seconds, "notional_per_sec*bucket"
    if "volume" in df.columns:
        return df["volume"].values, "volume"
    if "quote_volume" in df.columns:
        return df["quote_volume"].values, "quote_volume"
    if "base_volume" in df.columns:
        return df["base_volume"].values, "base_volume"
    raise ValueError("No usable volume proxy found")

def compute_session_vwap(df, vol):
    ts = pd.to_datetime(df["bucket_time_ms"], unit="ms", utc=True)
    day = ts.dt.floor("D")
    price = df["close"].values
    vol = np.where(vol > 0, vol, 0.0)

    pv = price * vol
    g = pd.DataFrame({"day": day, "pv": pv, "v": vol})

    cum_pv = g.groupby("day")["pv"].cumsum().to_numpy()
    cum_v = g.groupby("day")["v"].cumsum().to_numpy()

    return np.where(cum_v > 0, cum_pv / cum_v, np.nan)

# =========================
# Load data
# =========================
df = (
    pd.read_csv(DATA_PATH)
      .sort_values("bucket_time_ms")
      .reset_index(drop=True)
)

close = df["close"].values.astype(float)
flow_z = df["flow_z_300s"].values.astype(float)

# Volatility
rv_300s = df["rv_300s"].values.astype(float)
rv_ema = ema(rv_300s, span=int(VOL_EMA_SECONDS // BUCKET_SECONDS))

# VWAP
vol_proxy, vol_name = pick_volume_proxy(df, BUCKET_SECONDS)
vwap = compute_session_vwap(df, vol_proxy)

# Direction from flow
direction_all = np.sign(flow_z)

# Horizon
HORIZON_ROWS = int(HORIZON_SECONDS // BUCKET_SECONDS)
cost = ROUND_TRIP_COST_BPS / 10_000.0

# =========================
# Walk-forward test
# =========================
tscv = TimeSeriesSplit(n_splits=N_SPLITS)
fold_rows = []

for fold, (_, test_idx) in enumerate(tscv.split(df), 1):
    price = close[test_idx]
    vw = vwap[test_idx]
    flow = flow_z[test_idx]
    rv = rv_300s[test_idx]
    rv_e = rv_ema[test_idx]
    direction = direction_all[test_idx]

    # Gates
    with np.errstate(divide="ignore", invalid="ignore"):
        gate_vwap = np.abs(price - vw) / vw <= VWAP_BAND
    gate_vol = rv > rv_e
    gate_flow = np.abs(flow) >= Z_THR

    eligible = gate_vwap & gate_vol & gate_flow & (direction != 0)

    trade_rets = []

    for j in range(len(test_idx) - HORIZON_ROWS):
        if not eligible[j]:
            continue

        entry = test_idx[j]
        exit_ = test_idx[j + HORIZON_ROWS]

        r = logret(close[entry], close[exit_]) * direction[j]
        trade_rets.append(r - cost)

    trade_rets = np.array(trade_rets)

    wins = np.sum(trade_rets > 0)

    fold_rows.append({
        "fold": fold,
        "trades": len(trade_rets),
        "entry_rate": len(trade_rets) / len(test_idx),
        "mean_logret": safe_mean(trade_rets),
        "median_logret": safe_median(trade_rets),
        "win_rate": wins / len(trade_rets) if len(trade_rets) else np.nan,
        "eligible_share": np.mean(eligible)
    })

# =========================
# Report
# =========================
print("CONFIG:")
print(f"  VWAP_BAND: {VWAP_BAND*1e4:.1f} bps")
print(f"  Z_THR: {Z_THR}")
print(f"  HORIZON_SECONDS: {HORIZON_SECONDS}")
print(f"  VWAP volume proxy: {vol_name}")
print()

for r in fold_rows:
    print(
        f"Fold {r['fold']}: trades={r['trades']}, "
        f"entry_rate={r['entry_rate']:.3f}, "
        f"mean_logret={r['mean_logret']:.6f}, "
        f"median_logret={r['median_logret']:.6f}, "
        f"win_rate={r['win_rate']:.3f}"
    )

print("\nSUMMARY:")
print(f"Avg entry_rate: {np.mean([r['entry_rate'] for r in fold_rows]):.3f}")
print(f"Mean logret: {np.nanmean([r['mean_logret'] for r in fold_rows]):.6f}")
print(f"Median logret: {np.nanmedian([r['median_logret'] for r in fold_rows]):.6f}")
print(f"Avg win_rate: {np.nanmean([r['win_rate'] for r in fold_rows]):.3f}")
