#!/usr/bin/env python3
"""
BTCUSDT aggTrade -> 5s EVENT-TIME buckets -> ML-ready features (stationary flow + price + activity)

All fixes combined:
1) No wall-clock flushing. Buckets advance ONLY from event time (prevents synthetic empty tails).
2) Correct aggTrade trade counting using (l - f + 1).
3) Gate flow_z_300s and flow_adv_900s until rolling windows are warmed up and stable.
4) Explicit feed-gap detection + flags in output (gap_ms, data_gap).
5) WebSocket reconnect loop + connection logging.
6) Robust CSV header handling in append mode.
7) Optional dropped-message counter (queue overload visibility).

Dependency:
  pip install -U websocket-client
"""

import csv
import json
import math
import os
import threading
import time
from dataclasses import dataclass
from collections import deque
from queue import Queue, Empty, Full
from typing import Any, Dict, List, Optional

import websocket  # websocket-client


# =========================
# Config
# =========================
WS_URL = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
SYMBOL = "BTCUSDT"

FEATURE_VERSION = "v1"

BUCKET_S = 5
BUCKET_MS = BUCKET_S * 1000

EMA_HORIZONS_S = [60, 300, 900]  # 1m, 5m, 15m

FLOW_Z_WINDOW_S = 300  # zscore window for deltaN
ADV_WINDOW_S = 900     # activity window for abs notional
RV_WINDOW_S = 300      # realized vol window (returns)

CSV_PATH = "cvd_features_stream_2.csv"  # set None to disable
QUEUE_MAXSIZE = 50_000

EPS = 1e-12

# Drop cold-start rows (training convenience). Leave False for live inference logging.
DROP_UNWARMED_ROWS = False

# Feed gap threshold (ms). If event-time gaps exceed this, flag.
FEED_GAP_THRESHOLD_MS = 3 * BUCKET_MS


# =========================
# Sink: streaming CSV writer
# =========================
class CsvSink:
    def __init__(self, path: str, fieldnames: List[str]):
        self.path = path
        self.fieldnames = fieldnames

        file_has_data = os.path.exists(path) and os.path.getsize(path) > 0
        self._f = open(path, "a", newline="")
        self._w = csv.DictWriter(self._f, fieldnames=fieldnames)

        if not file_has_data:
            self._w.writeheader()
            self._f.flush()

    def append(self, row: Dict[str, Any]) -> None:
        out = {k: row.get(k, 0) for k in self.fieldnames}
        self._w.writerow(out)
        self._f.flush()

    def close(self) -> None:
        self._f.close()


# =========================
# Aggregated bucket structure
# =========================
@dataclass
class Bucket:
    bucket_time_ms: int
    open: float
    high: float
    low: float
    close: float
    buy_vol: float
    sell_vol: float
    buy_notional: float
    sell_notional: float
    num_trades: int


# =========================
# Bucket aggregator (event-time)
# =========================
class BucketAggregator:
    def __init__(self, bucket_ms: int):
        self.bucket_ms = int(bucket_ms)
        self.current_bucket_ms: Optional[int] = None

        # current bucket accumulators
        self.open = self.high = self.low = self.close = 0.0
        self.buy_vol = self.sell_vol = 0.0
        self.buy_notional = self.sell_notional = 0.0
        self.num_trades = 0

        self.last_price: Optional[float] = None

    def _start_bucket(self, bucket_ms: int, price: float) -> None:
        self.current_bucket_ms = int(bucket_ms)
        self.open = self.high = self.low = self.close = float(price)
        self.buy_vol = self.sell_vol = 0.0
        self.buy_notional = self.sell_notional = 0.0
        self.num_trades = 0

    def _finalize_bucket(self) -> Bucket:
        return Bucket(
            bucket_time_ms=int(self.current_bucket_ms),
            open=float(self.open),
            high=float(self.high),
            low=float(self.low),
            close=float(self.close),
            buy_vol=float(self.buy_vol),
            sell_vol=float(self.sell_vol),
            buy_notional=float(self.buy_notional),
            sell_notional=float(self.sell_notional),
            num_trades=int(self.num_trades),
        )

    def on_trade(self, event: Dict[str, Any]) -> List[Bucket]:
        """
        Process one aggTrade event. Returns 0+ finalized buckets (including gap-filled buckets).
        Late/out-of-order events are dropped for determinism.
        """
        t_ms = int(event["T"])
        price = float(event["p"])
        qty = float(event["q"])
        notional = price * qty
        is_buyer_maker = bool(event["m"])  # True => sell-initiated

        # FIX: aggTrade message may represent multiple trades
        try:
            n_trades = int(event["l"]) - int(event["f"]) + 1
            if n_trades < 1:
                n_trades = 1
        except Exception:
            n_trades = 1

        self.last_price = price
        event_bucket_ms = (t_ms // self.bucket_ms) * self.bucket_ms

        out: List[Bucket] = []

        if self.current_bucket_ms is None:
            self._start_bucket(event_bucket_ms, price)
            return out  # no finalized bucket yet

        # Drop out-of-order
        if event_bucket_ms < self.current_bucket_ms:
            return out

        # If we jumped forward, finalize and emit intermediate buckets
        while event_bucket_ms > self.current_bucket_ms:
            out.append(self._finalize_bucket())
            next_bucket = self.current_bucket_ms + self.bucket_ms
            # Start next bucket using last close (flat price during empty interval)
            self._start_bucket(next_bucket, self.close)

        # Update OHLC
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price

        # Accumulate
        if is_buyer_maker:
            self.sell_vol += qty
            self.sell_notional += notional
        else:
            self.buy_vol += qty
            self.buy_notional += notional

        self.num_trades += n_trades
        return out

    def flush_until_event_time(self, event_time_ms: int) -> List[Bucket]:
        """
        Finalize buckets up to (but not including) the bucket that contains event_time_ms.
        CRITICAL: this is EVENT-TIME flush, not wall-clock flush.
        """
        out: List[Bucket] = []
        if self.current_bucket_ms is None or self.last_price is None:
            return out

        target_bucket_ms = (int(event_time_ms) // self.bucket_ms) * self.bucket_ms
        while target_bucket_ms > self.current_bucket_ms:
            out.append(self._finalize_bucket())
            next_bucket = self.current_bucket_ms + self.bucket_ms
            self._start_bucket(next_bucket, self.close)
        return out


# =========================
# Feature computer (incremental)
# =========================
class FeatureComputer:
    def __init__(
        self,
        bucket_s: int,
        ema_horizons_s: List[int],
        flow_z_window_s: int,
        adv_window_s: int,
        rv_window_s: int,
    ):
        self.bucket_s = int(bucket_s)

        # EMA alphas (time-based)
        self.ema_h = list(ema_horizons_s)
        self.alpha = {}
        for h in self.ema_h:
            periods = max(1, int(round(h / self.bucket_s)))
            self.alpha[h] = 2.0 / (periods + 1.0)

        # EMA state for flow_share (bounded) and price
        self.flow_share_ema = {h: None for h in self.ema_h}
        self.price_ema = {h: None for h in self.ema_h}

        # Rolling windows
        self.flow_z_n = max(1, int(round(flow_z_window_s / self.bucket_s)))
        self.adv_n = max(1, int(round(adv_window_s / self.bucket_s)))
        self.rv_n = max(1, int(round(rv_window_s / self.bucket_s)))

        self.deltaN_hist = deque(maxlen=self.flow_z_n)
        self.absN_hist = deque(maxlen=self.adv_n)
        self.ret_hist = deque(maxlen=self.rv_n)

        self.close_hist = deque(maxlen=max(self.adv_n, self.rv_n, int(round(900 / self.bucket_s)) + 5))

    @staticmethod
    def _ema(prev: Optional[float], x: float, a: float) -> float:
        return x if prev is None else (a * x + (1.0 - a) * prev)

    @staticmethod
    def _safe_log(x: float) -> float:
        return math.log(max(x, EPS))

    @staticmethod
    def _rolling_mean(xs: deque) -> float:
        return float(sum(xs) / len(xs)) if xs else 0.0

    @staticmethod
    def _rolling_std(xs: deque) -> float:
        n = len(xs)
        if n < 2:
            return 0.0
        m = sum(xs) / n
        var = sum((x - m) ** 2 for x in xs) / (n - 1)
        return float(math.sqrt(max(var, 0.0)))

    def update(self, b: Bucket, gap_ms: int, data_gap: int) -> Dict[str, Any]:
        buyN = b.buy_notional
        sellN = b.sell_notional
        absN = buyN + sellN
        deltaN = buyN - sellN

        buyV = b.buy_vol
        sellV = b.sell_vol
        absV = buyV + sellV
        deltaV = buyV - sellV

        flow_share = deltaN / (absN + EPS)

        o, c = b.open, b.close
        ret_5s = self._safe_log(c) - self._safe_log(o) if o > 0 else 0.0

        # histories
        self.deltaN_hist.append(deltaN)
        self.absN_hist.append(absN)
        self.ret_hist.append(ret_5s)
        self.close_hist.append(c)

        def lag_return(seconds: int) -> float:
            lag = int(round(seconds / self.bucket_s))
            if len(self.close_hist) > lag and self.close_hist[-1 - lag] > 0:
                return self._safe_log(self.close_hist[-1]) - self._safe_log(self.close_hist[-1 - lag])
            return 0.0

        ret_60s = lag_return(60)
        ret_300s = lag_return(300)
        ret_900s = lag_return(900)

        rv_300s = self._rolling_std(self.ret_hist)

        # candle
        rng = max(b.high - b.low, 0.0)
        body = abs(c - o)
        upper_wick = max(b.high - max(o, c), 0.0)
        lower_wick = max(min(o, c) - b.low, 0.0)

        range_pct = (rng / c) * 100.0 if c > 0 else 0.0
        body_pct = body / (rng + EPS)
        upper_wick_pct = upper_wick / (rng + EPS)
        lower_wick_pct = lower_wick / (rng + EPS)

        # gated z/adv
        deltaN_std = self._rolling_std(self.deltaN_hist)
        if len(self.deltaN_hist) < self.flow_z_n or deltaN_std < 1e-6:
            flow_z_300s = 0.0
        else:
            flow_z_300s = deltaN / (deltaN_std + EPS)

        absN_mean = self._rolling_mean(self.absN_hist)
        if len(self.absN_hist) < self.adv_n or absN_mean < 1e-6:
            flow_adv_900s = 0.0
        else:
            flow_adv_900s = deltaN / (absN_mean + EPS)

        # EMAs
        for h in self.ema_h:
            a = self.alpha[h]
            self.flow_share_ema[h] = self._ema(self.flow_share_ema[h], flow_share, a)
            self.price_ema[h] = self._ema(self.price_ema[h], c, a)

        fs_60 = float(self.flow_share_ema[60]) if self.flow_share_ema[60] is not None else 0.0
        fs_300 = float(self.flow_share_ema[300]) if self.flow_share_ema[300] is not None else 0.0
        fs_900 = float(self.flow_share_ema[900]) if self.flow_share_ema[900] is not None else 0.0

        flow_accel_60_300 = fs_60 - fs_300
        flow_accel_300_900 = fs_300 - fs_900

        trades_per_sec = b.num_trades / float(self.bucket_s)
        notional_per_sec = absN / float(self.bucket_s)
        vol_per_sec = absV / float(self.bucket_s)

        avg_trade_notional = absN / float(b.num_trades) if b.num_trades > 0 else 0.0
        avg_trade_vol = absV / float(b.num_trades) if b.num_trades > 0 else 0.0

        p60 = float(self.price_ema[60]) if self.price_ema[60] is not None else c
        p300 = float(self.price_ema[300]) if self.price_ema[300] is not None else c
        p900 = float(self.price_ema[900]) if self.price_ema[900] is not None else c

        price_trend_60_300 = p60 - p300
        price_trend_300_900 = p300 - p900

        is_warmed_up = int((len(self.deltaN_hist) >= self.flow_z_n) and (len(self.absN_hist) >= self.adv_n))

        return {
            "bucket_time_ms": b.bucket_time_ms,
            "feature_version": FEATURE_VERSION,
            "open": o, "high": b.high, "low": b.low, "close": c,

            "ret_5s": ret_5s,
            "ret_60s": ret_60s,
            "ret_300s": ret_300s,
            "ret_900s": ret_900s,
            "rv_300s": rv_300s,

            "range_pct": range_pct,
            "body_pct": body_pct,
            "upper_wick_pct": upper_wick_pct,
            "lower_wick_pct": lower_wick_pct,

            "price_ema_60s": p60,
            "price_ema_300s": p300,
            "price_ema_900s": p900,
            "price_trend_60_300": price_trend_60_300,
            "price_trend_300_900": price_trend_300_900,

            "delta_notional": deltaN,
            "total_notional": absN,
            "delta_vol": deltaV,
            "total_vol": absV,

            "flow_share": flow_share,
            "flow_share_ema_60s": fs_60,
            "flow_share_ema_300s": fs_300,
            "flow_share_ema_900s": fs_900,
            "flow_accel_60_300": flow_accel_60_300,
            "flow_accel_300_900": flow_accel_300_900,

            "flow_z_300s": flow_z_300s,
            "flow_adv_900s": flow_adv_900s,

            "num_trades": b.num_trades,
            "trades_per_sec": trades_per_sec,
            "notional_per_sec": notional_per_sec,
            "vol_per_sec": vol_per_sec,
            "avg_trade_notional": avg_trade_notional,
            "avg_trade_vol": avg_trade_vol,

            "buy_share_notional": (buyN / (absN + EPS)) if absN > 0 else 0.0,
            "sell_share_notional": (sellN / (absN + EPS)) if absN > 0 else 0.0,

            "is_warmed_up": is_warmed_up,

            # NEW: feed gap metadata
            "gap_ms": int(gap_ms),
            "data_gap": int(data_gap),
        }


# =========================
# Live inference hook (placeholder)
# =========================
class ModelSink:
    def __init__(self):
        self.model = None

    def on_features(self, row: Dict[str, Any]) -> None:
        pass


# =========================
# WebSocket ingestion (with reconnect)
# =========================
events_q: Queue = Queue(maxsize=QUEUE_MAXSIZE)
dropped_events = 0

def on_message(ws, message: str) -> None:
    global dropped_events
    try:
        data = json.loads(message)
        try:
            events_q.put_nowait(data)
        except Full:
            dropped_events += 1
            if dropped_events % 1000 == 0:
                print(f"Dropped events: {dropped_events}")
    except Exception:
        pass

def on_error(ws, error) -> None:
    print("WS Error:", error)

def on_close(ws, code, msg) -> None:
    print("WS Closed:", code, msg)

def on_open(ws) -> None:
    print("WS Connected")

def ws_forever() -> None:
    while True:
        try:
            ws = websocket.WebSocketApp(
                WS_URL,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
                on_open=on_open,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            print("WS loop exception:", e)
        time.sleep(1)


# =========================
# Main
# =========================
def main() -> None:
    threading.Thread(target=ws_forever, daemon=True).start()

    agg = BucketAggregator(BUCKET_MS)
    feat = FeatureComputer(
        bucket_s=BUCKET_S,
        ema_horizons_s=EMA_HORIZONS_S,
        flow_z_window_s=FLOW_Z_WINDOW_S,
        adv_window_s=ADV_WINDOW_S,
        rv_window_s=RV_WINDOW_S,
    )

    fieldnames = [
        "bucket_time_ms",
        "FEATURE_VERSION",
        "open", "high", "low", "close",
        "ret_5s", "ret_60s", "ret_300s", "ret_900s",
        "rv_300s",
        "range_pct", "body_pct", "upper_wick_pct", "lower_wick_pct",
        "price_ema_60s", "price_ema_300s", "price_ema_900s",
        "price_trend_60_300", "price_trend_300_900",
        "delta_notional", "total_notional", "delta_vol", "total_vol",
        "flow_share", "flow_share_ema_60s", "flow_share_ema_300s", "flow_share_ema_900s",
        "flow_accel_60_300", "flow_accel_300_900",
        "flow_z_300s", "flow_adv_900s",
        "num_trades", "trades_per_sec",
        "notional_per_sec", "vol_per_sec",
        "avg_trade_notional", "avg_trade_vol",
        "buy_share_notional", "sell_share_notional",
        "is_warmed_up",
        "gap_ms", "data_gap",
    ]

    csv_sink: Optional[CsvSink] = CsvSink(CSV_PATH, fieldnames) if CSV_PATH else None
    model_sink = ModelSink()

    last_status_ms = 0
    prev_event_T_ms: Optional[int] = None

    try:
        while True:
            # BLOCK until we actually get an event; no wall-clock bucket emission
            try:
                event = events_q.get(timeout=5.0)
            except Empty:
                # No events received; do nothing (no synthetic buckets).
                continue

            t_ms = int(event["T"])

            # Feed-gap detection using event time
            if prev_event_T_ms is None:
                gap_ms = 0
                data_gap = 0
            else:
                gap_ms = max(0, t_ms - prev_event_T_ms)
                data_gap = 1 if gap_ms >= FEED_GAP_THRESHOLD_MS else 0
                if data_gap:
                    print(f"FEED GAP detected: {gap_ms/1000:.1f}s (event-time)")

            prev_event_T_ms = t_ms

            # Finalize buckets caused by time-jumps (event-time)
            for b in agg.on_trade(event):
                row = feat.update(b, gap_ms=gap_ms, data_gap=data_gap)
                if DROP_UNWARMED_ROWS and not row["is_warmed_up"]:
                    continue
                if csv_sink:
                    csv_sink.append(row)
                model_sink.on_features(row)

                # status
                if row["bucket_time_ms"] - last_status_ms >= 60_000:
                    last_status_ms = row["bucket_time_ms"]
                    ts = time.strftime("%H:%M:%S", time.localtime(row["bucket_time_ms"] / 1000))
                    print(
                        f"[{ts}] close={row['close']:.2f} "
                        f"fs60={row['flow_share_ema_60s']:+.4f} "
                        f"z300={row['flow_z_300s']:+.2f} "
                        f"adv900={row['flow_adv_900s']:+.4f} "
                        f"ret300={row['ret_300s']:+.4f} "
                        f"trades={row['num_trades']} "
                        f"gap_ms={row['gap_ms']}"
                    )

            # Flush up to latest EVENT time (not wall clock)
            for b in agg.flush_until_event_time(t_ms):
                row = feat.update(b, gap_ms=gap_ms, data_gap=data_gap)
                if DROP_UNWARMED_ROWS and not row["is_warmed_up"]:
                    continue
                if csv_sink:
                    csv_sink.append(row)
                model_sink.on_features(row)

                if row["bucket_time_ms"] - last_status_ms >= 60_000:
                    last_status_ms = row["bucket_time_ms"]
                    ts = time.strftime("%H:%M:%S", time.localtime(row["bucket_time_ms"] / 1000))
                    print(
                        f"[{ts}] close={row['close']:.2f} "
                        f"fs60={row['flow_share_ema_60s']:+.4f} "
                        f"z300={row['flow_z_300s']:+.2f} "
                        f"adv900={row['flow_adv_900s']:+.4f} "
                        f"ret300={row['ret_300s']:+.4f} "
                        f"trades={row['num_trades']} "
                        f"gap_ms={row['gap_ms']}"
                    )

    except KeyboardInterrupt:
        if csv_sink:
            csv_sink.close()
        print("Stopped.")


if __name__ == "__main__":
    main()
