import websocket
import json
import httpx
import threading
from queue import Queue, Empty
from collections import defaultdict

buffer = Queue()

def top_n(side: dict[float, float], n: int, reverse: bool):
    # bids: reverse=True, asks: reverse=False
    return sorted(side.items(), key=lambda x: x[0], reverse=reverse)[:n]
# def print_book(book, levels=5000):
#     print("BIDS")
#     for p, q in book["bids"][:levels]:
#         print(f"{float(p):,.2f}  {float(q):,.6f}")
#     print("\nASKS")
#     for p, q in book["asks"][:levels]:
#         print(f"{float(p):,.2f}  {float(q):,.6f}")

def grab_snapshot(symbol="BTCUSDT", limit=5000):
    r = httpx.get("https://api.binance.com/api/v3/depth", params={
        "symbol": symbol,
        "limit": limit
    }, timeout=10)
    r.raise_for_status()
    book = r.json()
    return book

def run_once_on_message(ws, message):
    data = json.loads(message)
    print(data)
    ws.close()  # Close after receiving the first message

def on_message(ws, message):
    data = json.loads(message)
    buffer.put(data)

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

def on_open(ws):
    print("Connected to Binance WebSocket")

def start_ws():
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
        on_open=on_open
    )

    ws.run_forever(ping_interval=20, ping_timeout=10)




if __name__ == "__main__":
    # WebSocket URL for BTCUSDT depth stream
    # Using depth20 = top 20 price levels, updated every 100ms


    # Run WebSocket in a separate thread
    socketThread = threading.Thread(target=start_ws, daemon=True)
    socketThread.start()

    data  = buffer.get()
    U = data.get('U')


    while True:
        book = grab_snapshot()
        lastUpdateId = book["lastUpdateId"]

        # build book from snapshot
        localBook = {
            "lastUpdateId": lastUpdateId,
            "bids": {float(p): float(q) for p, q in book.get("bids", [])},
            "asks": {float(p): float(q) for p, q in book.get("asks", [])},
        }

        # advance WS events past snapshot
        while data["u"] <= lastUpdateId:
            data = buffer.get()

        # find first event that bridges boundary
        while not (data["U"] <= lastUpdateId + 1 <= data["u"]):
            data = buffer.get()

        # apply first bridging event
        for p, q in data.get("b", []):
            p = float(p); q = float(q)
            if q == 0.0: localBook["bids"].pop(p, None)
            else:        localBook["bids"][p] = q
        for p, q in data.get("a", []):
            p = float(p); q = float(q)
            if q == 0.0: localBook["asks"].pop(p, None)
            else:        localBook["asks"][p] = q
        localBook["lastUpdateId"] = data["u"]

        data = buffer.get()  # move to next event

        # steady state
        while True:
            if data["U"] > localBook["lastUpdateId"] + 1:
                print("Missed updates, resyncing...")
                break

            # apply
            for p, q in data.get("b", []):
                p = float(p); q = float(q)
                if q == 0.0: localBook["bids"].pop(p, None)
                else:        localBook["bids"][p] = q
            for p, q in data.get("a", []):
                p = float(p); q = float(q)
                if q == 0.0: localBook["asks"].pop(p, None)
                else:        localBook["asks"][p] = q

            localBook["lastUpdateId"] = data["u"]
            data = buffer.get()

            print("BIDS:", top_n(localBook["bids"], 10, reverse=True))
            print("ASKS:", top_n(localBook["asks"], 10, reverse=False))