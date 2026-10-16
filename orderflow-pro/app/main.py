import asyncio
import json
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import httpx
import websockets
from dotenv import load_dotenv
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from .analytics import FootprintAggregator, SignalEngine
from .orderbook import OrderBook

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR.parent / ".env")
STATIC_DIR = BASE_DIR / "static"
REST = "https://api.kraken.com/0/public"
WS_URL = "wss://ws.kraken.com/v2"
# Keep the existing UI symbol IDs while mapping them to Kraken's native WS symbols.
PAIR_MAP = {"BTCUSDT": "BTC/USD", "ETHUSDT": "ETH/USD", "SOLUSDT": "SOL/USD"}
REST_PAIR_MAP = {"BTCUSDT": "XBTUSD", "ETHUSDT": "ETHUSD", "SOLUSDT": "SOLUSD"}
requested = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
SYMBOLS = [s for s in requested if s in PAIR_MAP] or ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
INTERVAL = os.getenv("KLINE_INTERVAL", "1m")
HISTORY_DAYS = max(1, min(30, int(os.getenv("HISTORY_DAYS", "30"))))
MAX_DEPTH_LEVELS = max(20, min(500, int(os.getenv("MAX_DEPTH_LEVELS", "100"))))
HEATMAP_RETENTION_MINUTES = max(5, min(240, int(os.getenv("HEATMAP_RETENTION_MINUTES", "60"))))
INTERVAL_MS = 60_000

state = {}
clients = set()
tasks = []


def new_symbol_state(symbol):
    return {
        "symbol": symbol, "candles": deque(maxlen=50000), "book": OrderBook(),
        "footprint": FootprintAggregator(interval_ms=INTERVAL_MS, tick_size=0.1 if symbol == "BTCUSDT" else 0.01),
        "engine": SignalEngine(), "heatmap": deque(maxlen=HEATMAP_RETENTION_MINUTES * 30),
        "last_trade": None, "last_trade_ts": 0, "last_candle_ts": 0,
        "connection": "starting", "last_alert_key": None, "last_publish_ms": 0,
        "last_heatmap_ms": 0, "error": None, "trades_recent": deque(maxlen=2000),
    }


for sym in SYMBOLS:
    state[sym] = new_symbol_state(sym)


def interval_minutes(value):
    return {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}.get(value.lower(), 1)


def normalize_ohlc(row):
    return {"time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
            "low": float(row[3]), "close": float(row[4]), "volume": float(row[6]),
            "closed": True, "source": "historical"}


async def fetch_history(symbol):
    """Fetch the recent OHLC candles exposed by Kraken's public REST endpoint."""
    st = state[symbol]
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(f"{REST}/OHLC", params={
                "pair": REST_PAIR_MAP[symbol], "interval": interval_minutes(INTERVAL)
            })
            response.raise_for_status()
            payload = response.json()
        if payload.get("error"):
            raise RuntimeError("; ".join(payload["error"]))
        result = payload.get("result", {})
        rows = next((v for k, v in result.items() if k != "last" and isinstance(v, list)), [])
        seen = {c["time"]: c for c in (normalize_ohlc(row) for row in rows)}
        st["candles"].clear()
        st["candles"].extend(seen[t] for t in sorted(seen))
        st["last_candle_ts"] = st["candles"][-1]["time"] if st["candles"] else 0
        if not rows:
            st["error"] = "Kraken returned no historical candles for this market."
    except Exception as exc:
        st["error"] = f"Kraken candle history: {exc}"


def update_live_ohlc(symbol, row):
    stamp = row.get("interval_begin") or row.get("timestamp")
    start = int(datetime.fromisoformat(stamp.replace("Z", "+00:00")).timestamp())
    c = {"time": start, "open": float(row["open"]), "high": float(row["high"]),
         "low": float(row["low"]), "close": float(row["close"]), "volume": float(row["volume"]),
         "closed": False, "source": "live"}
    bars = state[symbol]["candles"]
    if bars and bars[-1]["time"] == c["time"]:
        bars[-1] = c
    elif not bars or bars[-1]["time"] < c["time"]:
        bars.append(c)
    state[symbol]["last_candle_ts"] = c["time"]


async def broadcast(payload):
    msg = json.dumps(payload, separators=(",", ":"))
    dead = []
    for ws in list(clients):
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        clients.discard(ws)


async def publish_symbol(symbol):
    st = state[symbol]
    now_ms = int(time.time() * 1000)
    candles = list(st["candles"])
    footprint = st["footprint"].latest(120)
    signal = st["engine"].evaluate(symbol, candles, footprint, st["book"].valid, now_ms)
    if signal["eligible"]:
        key = (signal["side"], candles[-1]["time"] if candles else 0)
        if key != st["last_alert_key"]:
            st["last_alert_key"] = key
            await broadcast({"type": "alert", "symbol": symbol, **signal})
    book = st["book"].top(50)
    if st["book"].valid and now_ms - st["last_heatmap_ms"] >= 2000:
        st["heatmap"].append({"ts": now_ms, "bids": book["bids"][:20], "asks": book["asks"][:20]})
        st["last_heatmap_ms"] = now_ms
    if now_ms - st["last_publish_ms"] < 250:
        return
    st["last_publish_ms"] = now_ms
    await broadcast({"type": "state", "symbol": symbol, "connection": st["connection"],
        "error": st["error"], "lastTrade": st["last_trade"], "candles": candles[-500:],
        "book": book, "footprint": footprint[-30:], "heatmap": list(st["heatmap"]),
        "signal": signal, "serverTime": now_ms,
        "history": {"days_requested": HISTORY_DAYS, "candle_source": "Kraken REST + WebSocket v2",
                    "orderbook_history": "runtime only; starts when app starts",
                    "footprint_history": "runtime only; starts when trades are received"}})


async def market_worker(symbol):
    """Subscribe to Kraken v2 trades and OHLC for one market."""
    st = state[symbol]
    pair = PAIR_MAP[symbol]
    while True:
        try:
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                st["connection"] = "connecting"
                st["error"] = None
                await ws.send(json.dumps({"method": "subscribe", "params": {
                    "channel": "trade", "symbol": [pair], "snapshot": True
                }}))
                await ws.send(json.dumps({"method": "subscribe", "params": {
                    "channel": "ohlc", "symbol": [pair], "interval": interval_minutes(INTERVAL), "snapshot": True
                }}))
                st["connection"] = "live"
                async for raw in ws:
                    packet = json.loads(raw)
                    if packet.get("success") is False:
                        raise RuntimeError(packet.get("error", "Kraken subscription failed"))
                    channel = packet.get("channel")
                    if channel not in ("trade", "ohlc"):
                        continue
                    for ev in packet.get("data", []):
                        if channel == "trade":
                            price, qty = float(ev["price"]), float(ev["qty"])
                            ts = int(datetime.fromisoformat(ev["timestamp"].replace("Z", "+00:00")).timestamp() * 1000)
                            side = ev.get("side", "buy").lower()
                            buyer_is_maker = side == "sell"
                            st["last_trade"] = {"price": price, "qty": qty,
                                "buyerIsMaker": buyer_is_maker, "side": side, "time": ts}
                            st["last_trade_ts"] = ts
                            st["footprint"].add_trade(price, qty, buyer_is_maker, ts)
                            st["trades_recent"].append({"price": price, "qty": qty,
                                "buyerIsMaker": buyer_is_maker, "time": ts})
                        else:
                            update_live_ohlc(symbol, ev)
                    await publish_symbol(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["connection"] = "reconnecting"
            st["error"] = f"Kraken market feed: {exc}"
            await asyncio.sleep(2)


async def apply_kraken_book_event(book, data, message_type):
    """Apply Kraken v2 L2 book snapshots and incremental updates."""
    if not isinstance(data, dict):
        return
    if message_type == "snapshot":
        book.load_snapshot({"lastUpdateId": int(time.time() * 1000),
                            "bids": [[x["price"], x["qty"]] for x in data.get("bids", [])],
                            "asks": [[x["price"], x["qty"]] for x in data.get("asks", [])]})
        book.reason = "live snapshot"
        return
    if not book.valid:
        book.reason = "waiting for fresh book snapshot"
        return
    for key, target in (("bids", book.bids), ("asks", book.asks)):
        for level in data.get(key, []):
            price, qty = float(level["price"]), float(level["qty"])
            if qty == 0:
                target.pop(price, None)
            else:
                target[price] = qty
    if book.bids and book.asks and max(book.bids) >= min(book.asks):
        book.valid = False
        book.reason = "crossed/invalid book; resync required"
        return
    book.valid = True
    book.reason = "live"


async def depth_worker(symbol):
    """Consume Kraken v2 L2 book snapshots and updates; reconnect resynchronizes."""
    st = state[symbol]
    pair = PAIR_MAP[symbol]
    while True:
        try:
            st["book"].valid = False
            st["book"].reason = "connecting to Kraken order book"
            async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                await ws.send(json.dumps({"method": "subscribe", "params": {
                    "channel": "book", "symbol": [pair], "depth": MAX_DEPTH_LEVELS, "snapshot": True
                }}))
                async for raw in ws:
                    packet = json.loads(raw)
                    if packet.get("success") is False:
                        raise RuntimeError(packet.get("error", "Kraken book subscription failed"))
                    if packet.get("channel") != "book":
                        continue
                    message_type = packet.get("type", "")
                    for ev in packet.get("data", []):
                        await apply_kraken_book_event(st["book"], ev, message_type)
                    await publish_symbol(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["book"].valid = False
            st["book"].reason = "disconnected; waiting for fresh snapshot"
            st["connection"] = "reconnecting"
            st["error"] = f"Kraken order book: {exc}"
            await asyncio.sleep(2)


async def publisher_worker():
    while True:
        for sym in SYMBOLS:
            await publish_symbol(sym)
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app):
    global tasks
    await asyncio.gather(*(fetch_history(sym) for sym in SYMBOLS), return_exceptions=True)
    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(market_worker(sym)))
        tasks.append(asyncio.create_task(depth_worker(sym)))
    tasks.append(asyncio.create_task(publisher_worker()))
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="OrderFlow Pro", version="0.2.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok", "symbols": SYMBOLS, "data_mode": "public Kraken Spot REST + WebSocket v2",
            "order_execution": False, "states": {s: {"connection": state[s]["connection"],
            "book_valid": state[s]["book"].valid, "book_reason": state[s]["book"].reason,
            "last_trade_ts": state[s]["last_trade_ts"], "error": state[s]["error"]} for s in SYMBOLS}}


@app.get("/api/state/{symbol}")
async def get_state(symbol: str):
    symbol = symbol.upper()
    if symbol not in state:
        return JSONResponse({"error": "unknown symbol", "allowed": SYMBOLS}, status_code=404)
    st = state[symbol]
    return {"symbol": symbol, "candles": list(st["candles"])[-500:],
            "book": st["book"].top(50), "footprint": st["footprint"].latest(30),
            "heatmap": list(st["heatmap"]), "lastTrade": st["last_trade"],
            "connection": st["connection"], "error": st["error"]}


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    clients.add(ws)
    try:
        for sym in SYMBOLS:
            st = state[sym]
            await ws.send_text(json.dumps({"type": "state", "symbol": sym,
                "connection": st["connection"], "error": st["error"],
                "lastTrade": st["last_trade"], "candles": list(st["candles"])[-500:],
                "book": st["book"].top(50), "footprint": st["footprint"].latest(30),
                "heatmap": list(st["heatmap"]), "history": {
                    "days_requested": HISTORY_DAYS,
                    "candle_source": "Kraken REST + WebSocket v2",
                    "orderbook_history": "runtime only; starts when app starts",
                    "footprint_history": "runtime only; starts when trades are received"}}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
