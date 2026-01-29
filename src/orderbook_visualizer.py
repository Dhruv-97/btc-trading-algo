import websocket
import json
import httpx
import threading
from queue import Queue, Empty

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation

# -----------------------------
# Config
# -----------------------------
SYMBOL = "BTCUSDT"
WS_URL = "wss://stream.binance.us:9443/ws/btcusdt@depth@100ms"  # diff depth stream
SNAP_LIMIT = 5000                                               # deep snapshot
LEVELS = 60                                                     # ladder rows to display
BUCKET = 5.0                                                    # aggregate to nearest multiple of 5
PLOT_INTERVAL_MS = 150

# -----------------------------
# Shared
# -----------------------------
events_q = Queue()
ws_ready = threading.Event()

state = {
    "book": None,      # {"lastUpdateId": int, "bids": {price: qty}, "asks": {price: qty}}
    "synced": False,   # True once snapshot+bridge applied
    "syncing": False,  # prevents multiple sync workers
}

# -----------------------------
# REST snapshot
# -----------------------------
def grab_snapshot(symbol=SYMBOL, limit=SNAP_LIMIT):
    r = httpx.get(
        "https://api.binance.us/api/v3/depth",
        params={"symbol": symbol, "limit": limit},
        timeout=10
    )
    r.raise_for_status()
    return r.json()

def build_book_from_snapshot():
    snap = grab_snapshot()
    return {
        "lastUpdateId": snap["lastUpdateId"],
        "bids": {float(p): float(q) for p, q in snap.get("bids", [])},
        "asks": {float(p): float(q) for p, q in snap.get("asks", [])},
    }

# -----------------------------
# WebSocket
# -----------------------------
def on_message(ws, message):
    data = json.loads(message)
    events_q.put(data)

def on_error(ws, error):
    print("WS Error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WS Closed", close_status_code, close_msg)

def on_open(ws):
    print("WS Connected")
    ws_ready.set()

def start_ws():
    ws = websocket.WebSocketApp(
        WS_URL,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open,
    )
    ws.run_forever(ping_interval=20, ping_timeout=10)

def clear_queue(q: Queue) -> int:
    n = 0
    while True:
        try:
            q.get_nowait()
            n += 1
        except Empty:
            break
    return n

# -----------------------------
# Book apply + sync
# -----------------------------
def apply_depth_event(book, data):
    # bids
    for p, q in data.get("b", []):
        p = float(p); q = float(q)
        if q == 0.0:
            book["bids"].pop(p, None)
        else:
            book["bids"][p] = q

    # asks
    for p, q in data.get("a", []):
        p = float(p); q = float(q)
        if q == 0.0:
            book["asks"].pop(p, None)
        else:
            book["asks"][p] = q

    book["lastUpdateId"] = data["u"]

def bridge_sync_worker():
    """
    Correct deep book sync per Binance:
      - get snapshot with lastUpdateId
      - discard events with u <= lastUpdateId
      - find first event where U <= lastUpdateId+1 <= u
      - apply it, then steady-state require U == lastUpdateId+1 (or <= with no gaps)
    """
    try:
        state["syncing"] = True
        state["synced"] = False

        ws_ready.wait(timeout=10)
        dropped = clear_queue(events_q)
        if dropped:
            print(f"SYNC: dropped {dropped} pre-snapshot events")

        book = build_book_from_snapshot()
        lastUpdateId = book["lastUpdateId"]
        state["book"] = book

        # get first event after snapshot
        print(f"SYNC: snapshot lastUpdateId={lastUpdateId}")
        data = events_q.get()
        while data["u"] <= lastUpdateId:
            data = events_q.get()

        # find bridging event
        target = lastUpdateId + 1
        while not (data["U"] <= target <= data["u"]):
            data = events_q.get()

        apply_depth_event(book, data)

        state["synced"] = True
        state["syncing"] = False
        print("SYNC: synced=True")

    except Exception as e:
        print("SYNC failed:", e)
        state["synced"] = False
        state["syncing"] = False

# -----------------------------
# Aggregation (bucket-to-nearest-5)
# -----------------------------
def round_to_bucket(price: float, bucket: float) -> float:
    return round(price / bucket) * bucket

def aggregate_side(side_map: dict[float, float], is_bid: bool, bucket: float) -> dict[float, float]:
    """
    side_map: price->qty
    returns bucket_price->sum_qty
    Note: This aggregates the FULL local book (deep), not just top-N.
    """
    agg = {}
    for p, qty in side_map.items():
        bp = round_to_bucket(p, bucket)
        agg[bp] = agg.get(bp, 0.0) + qty
    return agg

# -----------------------------
# Main + plotting
# -----------------------------
if __name__ == "__main__":
    threading.Thread(target=start_ws, daemon=True).start()

    # Start sync in background
    threading.Thread(target=bridge_sync_worker, daemon=True).start()

    # Plot setup: fixed ladder rows by index, with price labels
    fig, ax = plt.subplots()
    y = list(range(LEVELS))

    bid_bars = ax.barh(y, [0.0] * LEVELS, height=0.8, label="bids")
    ask_bars = ax.barh(y, [0.0] * LEVELS, height=0.8, label="asks")

    ax.set_title(f"{SYMBOL} Deep Book Ladder | snap={SNAP_LIMIT} | bucket={BUCKET:g}")
    ax.set_xlabel("Quantity (bids left, asks right)")
    ax.axvline(0, linewidth=1)
    ax.legend(loc="upper right")

    ax.set_yticks(y)
    ax.set_yticklabels([""] * LEVELS)

    bid_text = [ax.text(0, i, "", va="center", ha="right") for i in y]
    ask_text = [ax.text(0, i, "", va="center", ha="left") for i in y]
    ax.set_ylim(-1, LEVELS)

    def update(_):
        book = state["book"]
        if book is None:
            return ()

        # Apply all pending WS events (only if synced)
        if state["synced"]:
            while True:
                try:
                    data = events_q.get_nowait()
                except Empty:
                    break

                # Gap detection: if the next update doesn't connect, resync
                if data["U"] > book["lastUpdateId"] + 1:
                    print("Missed updates -> resync")
                    state["synced"] = False
                    if not state["syncing"]:
                        clear_queue(events_q)
                        threading.Thread(target=bridge_sync_worker, daemon=True).start()
                    break

                apply_depth_event(book, data)

        # Aggregate FULL book into $5 buckets
        bid_agg = aggregate_side(book["bids"], is_bid=True, bucket=BUCKET)
        ask_agg = aggregate_side(book["asks"], is_bid=False, bucket=BUCKET)

        # Choose levels to display near top of book
        # best bid = max price, best ask = min price
        if not bid_agg or not ask_agg:
            return ()

        best_bid = max(bid_agg.keys())
        best_ask = min(ask_agg.keys())

        # Collect closest LEVELS buckets around the inside market:
        # bids: descending from best_bid
        # asks: ascending from best_ask
        bids = sorted([x for x in bid_agg.items() if x[0] <= best_bid], key=lambda t: t[0], reverse=True)[:LEVELS]
        asks = sorted([x for x in ask_agg.items() if x[0] >= best_ask], key=lambda t: t[0])[:LEVELS]

        # Pad
        bids = (bids + [(0.0, 0.0)] * LEVELS)[:LEVELS]
        asks = (asks + [(0.0, 0.0)] * LEVELS)[:LEVELS]

        bid_qtys = [-qty for _, qty in bids]
        ask_qtys = [qty for _, qty in asks]

        for i in range(LEVELS):
            bid_bars[i].set_width(bid_qtys[i])
            ask_bars[i].set_width(ask_qtys[i])

        max_q = max(max(abs(x) for x in bid_qtys), max(ask_qtys))
        if max_q <= 0:
            max_q = 1.0
        ax.set_xlim(-max_q * 1.2, max_q * 1.2)

        x_left = -max_q * 0.02
        x_right = max_q * 0.02

        for i in range(LEVELS):
            bp, _ = bids[i]
            ap, _ = asks[i]
            bid_text[i].set_position((x_left, i))
            ask_text[i].set_position((x_right, i))
            bid_text[i].set_text(f"{bp:.0f}")
            ask_text[i].set_text(f"{ap:.0f}")

        return ()

    ani = FuncAnimation(fig, update, interval=PLOT_INTERVAL_MS, blit=False)
    plt.show()
