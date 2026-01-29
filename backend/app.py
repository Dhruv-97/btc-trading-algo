# backend/app.py
import asyncio
import json
import threading
import time
from queue import Queue, Empty
from typing import Any, Dict, List, Tuple

import httpx
import websocket  # pip install websocket-client
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# ============================================================
# Config
# ============================================================
SYMBOL = "BTCUSDT"

# Binance.US endpoints
WS_URL = "wss://stream.binance.us:9443/ws/btcusdt@depth@100ms"
SNAP_URL = "https://api.binance.us/api/v3/depth"
SNAP_LIMIT = 5000

# Broadcast settings
FPS = 20

# Showcase settings
LADDER_LEVELS = 100       # bucketed rows for the table
DEPTH_LEVELS = 200       # raw rows for the curve (smoother)
BUCKET = 5.0             # bucket size for ladder only (set 0.0 for raw ladder)

# ============================================================
# FastAPI app
# ============================================================
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# WebSocket hub
# ============================================================
class Hub:
    def __init__(self):
        self.clients: set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.add(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.discard(ws)

    async def broadcast(self, msg: dict):
        if not self.clients:
            return
        data = json.dumps(msg)
        dead: List[WebSocket] = []
        for ws in self.clients:
            try:
                await ws.send_text(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

hub = Hub()

# ============================================================
# Binance.US book engine
# ============================================================
events_q: "Queue[dict]" = Queue()
ws_ready = threading.Event()

state: Dict[str, Any] = {
    "book": None,      # {"lastUpdateId": int, "bids": {price: qty}, "asks": {price: qty}}
    "synced": False,
    "syncing": False,
}

def clear_queue(q: Queue) -> int:
    n = 0
    while True:
        try:
            q.get_nowait()
            n += 1
        except Empty:
            break
    return n

def grab_snapshot(symbol: str = SYMBOL, limit: int = SNAP_LIMIT) -> dict:
    r = httpx.get(SNAP_URL, params={"symbol": symbol, "limit": limit}, timeout=10)
    r.raise_for_status()
    return r.json()

def build_book_from_snapshot() -> dict:
    snap = grab_snapshot()
    return {
        "lastUpdateId": int(snap["lastUpdateId"]),
        "bids": {float(p): float(q) for p, q in snap.get("bids", [])},
        "asks": {float(p): float(q) for p, q in snap.get("asks", [])},
    }

def apply_depth_event(book: dict, data: dict) -> None:
    for p, q in data.get("b", []):
        p = float(p); q = float(q)
        if q == 0.0:
            book["bids"].pop(p, None)
        else:
            book["bids"][p] = q

    for p, q in data.get("a", []):
        p = float(p); q = float(q)
        if q == 0.0:
            book["asks"].pop(p, None)
        else:
            book["asks"][p] = q

    book["lastUpdateId"] = int(data["u"])

def on_message(ws, message: str):
    try:
        events_q.put(json.loads(message))
    except Exception:
        pass

def on_error(ws, error):
    print("WS Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WS Closed", close_status_code, close_msg)
    ws_ready.clear()

def on_open(ws):
    print("WS Connected")
    ws_ready.set()

def start_ws_forever():
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
            print("WS run_forever exception:", repr(e))
        time.sleep(2)

def bridge_sync_worker():
    try:
        state["syncing"] = True
        state["synced"] = False

        ok = ws_ready.wait(timeout=10)
        if not ok:
            raise RuntimeError("WS not ready (timeout)")

        clear_queue(events_q)

        book = build_book_from_snapshot()
        lastUpdateId = book["lastUpdateId"]
        state["book"] = book

        data = events_q.get()
        while int(data["u"]) <= lastUpdateId:
            data = events_q.get()

        target = lastUpdateId + 1
        while not (int(data["U"]) <= target <= int(data["u"])):
            data = events_q.get()

        apply_depth_event(book, data)

        state["synced"] = True
        state["syncing"] = False
        print("SYNC OK lastUpdateId=", book["lastUpdateId"])

    except Exception as e:
        print("SYNC failed:", repr(e))
        state["synced"] = False
        state["syncing"] = False

def drain_events_into_book() -> None:
    book = state["book"]
    if book is None or not state["synced"]:
        return

    while True:
        try:
            data = events_q.get_nowait()
        except Empty:
            break

        if int(data["U"]) > int(book["lastUpdateId"]) + 1:
            state["synced"] = False
            if not state["syncing"]:
                clear_queue(events_q)
                threading.Thread(target=bridge_sync_worker, daemon=True).start()
            break

        apply_depth_event(book, data)

# ============================================================
# Ladder bucketing (for prettier table only)
# ============================================================
def aggregate_side(side: Dict[float, float], is_bid: bool, bucket: float) -> Dict[float, float]:
    if bucket <= 0:
        return dict(side)

    out: Dict[float, float] = {}
    b = float(bucket)

    for p, q in side.items():
        if q <= 0:
            continue
        if is_bid:
            bp = (p // b) * b
        else:
            bp = ((p + b - 1e-9) // b) * b  # ceil-ish
        out[bp] = out.get(bp, 0.0) + float(q)

    return out

def top_levels(agg: Dict[float, float], is_bid: bool, levels: int) -> List[Tuple[float, float]]:
    items = sorted(agg.items(), key=lambda t: t[0], reverse=is_bid)
    return items[:levels]

# ============================================================
# Payload: raw bests + raw depth arrays + bucketed ladder arrays
# ============================================================
def snapshot_payload() -> Dict[str, Any]:
    drain_events_into_book()

    book = state["book"]
    if book is None or not state["synced"]:
        return {
            "ts": time.time(),
            "symbol": SYMBOL,
            "bucket": BUCKET,
            "ladder_levels": LADDER_LEVELS,
            "depth_levels": DEPTH_LEVELS,
            "best_bid": None,
            "best_ask": None,
            "mid": None,
            "spread": None,
            "bids": [],
            "asks": [],
            "depth_bids": [],
            "depth_asks": [],
        }

    # RAW best bid/ask (unbucketed)
    raw_best_bid = max(book["bids"]) if book["bids"] else None
    raw_best_ask = min(book["asks"]) if book["asks"] else None
    mid = (raw_best_bid + raw_best_ask) / 2.0 if (raw_best_bid is not None and raw_best_ask is not None) else None
    spread = (raw_best_ask - raw_best_bid) if (raw_best_bid is not None and raw_best_ask is not None) else None

    # RAW depth arrays for the curve (smoother)
    depth_bids = sorted(book["bids"].items(), key=lambda t: t[0], reverse=True)[:DEPTH_LEVELS]
    depth_asks = sorted(book["asks"].items(), key=lambda t: t[0])[:DEPTH_LEVELS]

    # BUCKETED ladder arrays for the table
    bids_agg = aggregate_side(book["bids"], is_bid=True, bucket=BUCKET)
    asks_agg = aggregate_side(book["asks"], is_bid=False, bucket=BUCKET)
    bids = top_levels(bids_agg, is_bid=True, levels=LADDER_LEVELS)
    asks = top_levels(asks_agg, is_bid=False, levels=LADDER_LEVELS)

    return {
        "ts": time.time(),
        "symbol": SYMBOL,
        "bucket": BUCKET,
        "ladder_levels": LADDER_LEVELS,
        "depth_levels": DEPTH_LEVELS,
        "best_bid": raw_best_bid,
        "best_ask": raw_best_ask,
        "mid": mid,
        "spread": spread,
        "bids": bids,              # bucketed ladder
        "asks": asks,              # bucketed ladder
        "depth_bids": depth_bids,  # raw depth curve
        "depth_asks": depth_asks,  # raw depth curve
    }

# ============================================================
# Publisher + WS endpoint
# ============================================================
async def publisher_loop():
    period = 1.0 / FPS
    while True:
        payload = snapshot_payload()
        await hub.broadcast(payload)
        await asyncio.sleep(period)

@app.on_event("startup")
async def _startup():
    threading.Thread(target=start_ws_forever, daemon=True).start()
    threading.Thread(target=bridge_sync_worker, daemon=True).start()
    asyncio.create_task(publisher_loop())

@app.websocket("/ws/orderbook")
async def ws_orderbook(ws: WebSocket):
    await hub.connect(ws)
    try:
        while True:
            await asyncio.sleep(60)
    except WebSocketDisconnect:
        hub.disconnect(ws)
    except Exception:
        hub.disconnect(ws)
