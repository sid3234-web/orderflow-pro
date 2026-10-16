import asyncio
import json
import os
import time
from collections import defaultdict, deque
from contextlib import asynccontextmanager
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
REST = "https://api.binance.com"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="
SYMBOLS = [s.strip().upper() for s in os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",") if s.strip()]
INTERVAL = os.getenv("KLINE_INTERVAL", "1m")
HISTORY_DAYS = max(1, min(30, int(os.getenv("HISTORY_DAYS", "30"))))
MAX_DEPTH_LEVELS = max(20, min(500, int(os.getenv("MAX_DEPTH_LEVELS", "100"))))
HEATMAP_RETENTION_MINUTES = max(5, min(240, int(os.getenv("HEATMAP_RETENTION_MINUTES", "60"))))
INTERVAL_MS = 60_000 if INTERVAL == "1m" else 60_000

state = {}
clients = set()
tasks = []
http_client = None


def new_symbol_state(symbol):
    return {
        "symbol": symbol, "candles": deque(maxlen=50000), "book": OrderBook(),
        "footprint": FootprintAggregator(interval_ms=INTERVAL_MS, tick_size=0.1 if symbol.endswith("USDT") else 0.01),
        "engine": SignalEngine(), "heatmap": deque(maxlen=HEATMAP_RETENTION_MINUTES * 60),
        "last_trade": None, "last_trade_ts": 0, "last_candle_ts": 0,
        "connection": "starting", "last_alert_key": None, "last_publish_ms": 0, "last_heatmap_ms": 0, "error": None,
        "trades_recent": deque(maxlen=2000),
    }


for sym in SYMBOLS:
    state[sym] = new_symbol_state(sym)


def normalize_kline(k):
    return {"time": int(k[0] // 1000), "open": float(k[1]), "high": float(k[2]),
            "low": float(k[3]), "close": float(k[4]), "volume": float(k[5]),
            "closed": True, "source": "historical"}


async def fetch_history(symbol):
    """Load up to 30 days of 1m OHLCV in 1000-candle pages."""
    st = state[symbol]
    end = int(time.time() * 1000)
    start = end - HISTORY_DAYS * 24 * 60 * 60 * 1000
    rows = []
    async with httpx.AsyncClient(timeout=20) as client:
        cursor = start
        while cursor < end:
            response = await client.get(f"{REST}/api/v3/klines",
                params={"symbol": symbol, "interval": INTERVAL, "startTime": cursor,
                        "endTime": end, "limit": 1000})
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break
            rows.extend(batch)
            cursor = int(batch[-1][0]) + 60_000
            if len(batch) < 1000:
                break
            await asyncio.sleep(0.12)
    seen = {}
    for k in rows:
        c = normalize_kline(k)
        seen[c["time"]] = c
    st["candles"].clear()
    st["candles"].extend(seen[t] for t in sorted(seen))
    st["last_candle_ts"] = st["candles"][-1]["time"] if st["candles"] else 0


def update_live_kline(symbol, k):
    st = state[symbol]
    c = {"time": int(k["t"] // 1000), "open": float(k["o"]), "high": float(k["h"]),
         "low": float(k["l"]), "close": float(k["c"]), "volume": float(k["v"]),
         "closed": bool(k["x"]), "source": "live"}
    bars = st["candles"]
    if bars and bars[-1]["time"] == c["time"]:
        bars[-1] = c
    elif not bars or bars[-1]["time"] < c["time"]:
        bars.append(c)
    st["last_candle_ts"] = c["time"]


async def broadcast(payload):
    dead = []
    msg = json.dumps(payload, separators=(",", ":"))
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
    # Only create a fresh alert on a side transition; never execute trades.
    if signal["eligible"]:
        key = (signal["side"], candles[-1]["time"] if candles else 0)
        if key != st["last_alert_key"]:
            st["last_alert_key"] = key
            await broadcast({"type": "alert", "symbol": symbol, **signal})
    book = st["book"].top(50)
    if st["book"].valid and now_ms - st["last_heatmap_ms"] >= 2000:
        st["heatmap"].append({"ts": now_ms, "bids": book["bids"][:20], "asks": book["asks"][:20]})
        st["last_heatmap_ms"] = now_ms
    # Evaluate alert rules on incoming updates, but throttle large UI state messages.
    if now_ms - st["last_publish_ms"] < 250:
        return
    st["last_publish_ms"] = now_ms
    await broadcast({"type": "state", "symbol": symbol, "connection": st["connection"],
        "error": st["error"], "lastTrade": st["last_trade"], "candles": candles[-500:],
        "book": book, "footprint": footprint[-30:], "heatmap": list(st["heatmap"]),
        "signal": signal, "serverTime": now_ms,
        "history": {"days_requested": HISTORY_DAYS, "candle_source": "Binance REST + live stream",
                    "orderbook_history": "runtime only; starts when app starts",
                    "footprint_history": "runtime only; starts when trades are received"}})


async def rest_resync(symbol):
    st = state[symbol]
    st["book"].valid = False
    st["book"].reason = "fetching depth snapshot"
    # Buffer depth events before snapshot to avoid losing events during snapshot fetch.
    while True:
        try:
            async with websockets.connect(WS_BASE + f"{symbol.lower()}@depth@100ms",
                                          ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                buffer = []
                async def collect():
                    async for raw in ws:
                        msg = json.loads(raw)
                        ev = msg.get("data", msg)
                        buffer.append(ev)
                        if len(buffer) > 5000:
                            del buffer[:1000]
                collector = asyncio.create_task(collect())
                await asyncio.sleep(0.7)
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.get(f"{REST}/api/v3/depth",
                                         params={"symbol": symbol, "limit": 1000})
                    r.raise_for_status()
                    st["book"].load_snapshot(r.json())
                # Drop events entirely before snapshot; first bridging event must cover lastUpdateId+1.
                buffer[:] = [e for e in buffer if int(e.get("u", 0)) > st["book"].last_update_id]
                bridge_index = None
                for i, e in enumerate(buffer):
                    if int(e.get("U", 0)) <= st["book"].last_update_id + 1 <= int(e.get("u", 0)):
                        bridge_index = i
                        break
                if bridge_index is None:
                    st["book"].valid = False
                    st["book"].reason = "no bridging depth event; resyncing"
                    collector.cancel()
                    await asyncio.sleep(0.5)
                    continue
                for ev in buffer[bridge_index:]:
                    result = st["book"].apply_diff(ev)
                    if result == "resync":
                        break
                if not st["book"].valid:
                    collector.cancel()
                    await asyncio.sleep(0.5)
                    continue
                # The collector is replaced by a direct consumer. Queue buffered tail then consume stream.
                collector.cancel()
                # Reconnect loop below starts a fresh stream; force resnapshot to avoid a gap.
                st["book"].valid = False
                st["book"].reason = "reconnecting after buffered sync"
                await asyncio.sleep(0.3)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["error"] = f"depth resync: {exc}"
            st["book"].valid = False
            st["book"].reason = "depth connection/resync failed"
            await asyncio.sleep(2)


async def depth_worker(symbol):
    """Snapshot + buffered diff synchronization and continuous diff consumption."""
    st = state[symbol]
    stream_url = WS_BASE + f"{symbol.lower()}@depth@100ms"
    while True:
        try:
            st["connection"] = "connecting"
            async with websockets.connect(stream_url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                st["connection"] = "live"
                buffer = []
                async def receive_until_snapshot():
                    async for raw in ws:
                        ev = json.loads(raw).get("data", {})
                        buffer.append(ev)
                        if len(buffer) > 8000:
                            del buffer[:2000]
                receiver = asyncio.create_task(receive_until_snapshot())
                await asyncio.sleep(0.5)
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.get(f"{REST}/api/v3/depth", params={"symbol": symbol, "limit": 1000})
                    r.raise_for_status()
                    st["book"].load_snapshot(r.json())
                buffer[:] = [e for e in buffer if int(e.get("u", 0)) > st["book"].last_update_id]
                bridge = next((i for i,e in enumerate(buffer)
                    if int(e.get("U", 0)) <= st["book"].last_update_id + 1 <= int(e.get("u", 0))), None)
                if bridge is None:
                    receiver.cancel()
                    st["book"].valid = False
                    st["book"].reason = "no bridging event; resync"
                    await asyncio.sleep(0.5)
                    continue
                synced = True
                for ev in buffer[bridge:]:
                    if st["book"].apply_diff(ev) == "resync":
                        synced = False
                        break
                if not synced:
                    receiver.cancel()
                    await asyncio.sleep(0.5)
                    continue
                # Drain the already-buffered event list and then consume the stream in one receiver.
                receiver.cancel()
                st["book"].valid = False
                st["book"].reason = "restarting stream after initial sync"
                # A fresh snapshot/stream cycle is safer than silently dropping events in this starter.
                await asyncio.sleep(0.2)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["error"] = str(exc)
            st["connection"] = "reconnecting"
            st["book"].valid = False
            st["book"].reason = "disconnected; resync required"
            await asyncio.sleep(2)


async def market_worker(symbol):
    """Combined trade/kline stream, footprint aggregation and live candle updates."""
    st = state[symbol]
    streams = f"{symbol.lower()}@trade/{symbol.lower()}@kline_{INTERVAL}"
    url = WS_BASE + streams
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                st["connection"] = "live"
                st["error"] = None
                async for raw in ws:
                    packet = json.loads(raw)
                    ev = packet.get("data", {})
                    kind = ev.get("e")
                    if kind == "trade":
                        p, q, maker, ts = float(ev["p"]), float(ev["q"]), bool(ev["m"]), int(ev["T"])
                        st["last_trade"] = {"price": p, "qty": q, "buyerIsMaker": maker, "time": ts}
                        st["last_trade_ts"] = ts
                        st["footprint"].add_trade(p, q, maker, ts)
                        st["trades_recent"].append({"price": p, "qty": q, "buyerIsMaker": maker, "time": ts})
                    elif kind == "kline":
                        update_live_kline(symbol, ev["k"])
                    await publish_symbol(symbol)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["connection"] = "reconnecting"
            st["error"] = str(exc)
            await asyncio.sleep(2)


async def depth_worker_fixed(symbol):
    """Correct continuous snapshot/diff loop using a queue to preserve events."""
    st = state[symbol]
    url = WS_BASE + f"{symbol.lower()}@depth@100ms"
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5) as ws:
                queue = asyncio.Queue(maxsize=10000)
                async def pump():
                    async for raw in ws:
                        ev = json.loads(raw).get("data", {})
                        try:
                            queue.put_nowait(ev)
                        except asyncio.QueueFull:
                            while not queue.empty():
                                queue.get_nowait()
                            raise RuntimeError("depth buffer overflow; resync required")
                pump_task = asyncio.create_task(pump())
                # Buffer events while obtaining snapshot.
                buffered = []
                await asyncio.sleep(0.5)
                while not queue.empty():
                    buffered.append(queue.get_nowait())
                async with httpx.AsyncClient(timeout=15) as client:
                    r = await client.get(f"{REST}/api/v3/depth", params={"symbol": symbol, "limit": 1000})
                    r.raise_for_status()
                    st["book"].load_snapshot(r.json())
                # Events with u <= snapshot are stale. Find first event spanning lastUpdateId + 1.
                buffered = [e for e in buffered if int(e.get("u", 0)) > st["book"].last_update_id]
                idx = next((i for i,e in enumerate(buffered)
                    if int(e.get("U", 0)) <= st["book"].last_update_id + 1 <= int(e.get("u", 0))), None)
                if idx is None:
                    pump_task.cancel()
                    st["book"].valid = False
                    st["book"].reason = "awaiting depth bridge; resyncing"
                    await asyncio.sleep(0.25)
                    continue
                ok = True
                for ev in buffered[idx:]:
                    if st["book"].apply_diff(ev) == "resync":
                        ok = False
                        break
                if not ok:
                    pump_task.cancel()
                    await asyncio.sleep(0.25)
                    continue
                st["book"].valid = True
                st["book"].reason = "live"
                # Continue consuming queue; gaps invalidate and trigger full resync.
                while True:
                    ev = await asyncio.wait_for(queue.get(), timeout=15)
                    result = st["book"].apply_diff(ev)
                    if result == "resync":
                        break
                    await publish_symbol(symbol)
                pump_task.cancel()
                st["book"].valid = False
                st["book"].reason = "depth gap detected; resyncing"
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            st["book"].valid = False
            st["book"].reason = f"reconnecting: {exc}"
            st["connection"] = "reconnecting"
            st["error"] = str(exc)
            await asyncio.sleep(1.5)


async def publisher_worker():
    while True:
        for sym in SYMBOLS:
            await publish_symbol(sym)
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app):
    global tasks
    # Load REST history concurrently; tolerate endpoint failures and keep UI available.
    await asyncio.gather(*(fetch_history(sym) for sym in SYMBOLS), return_exceptions=True)
    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(market_worker(sym)))
        tasks.append(asyncio.create_task(depth_worker_fixed(sym)))
    tasks.append(asyncio.create_task(publisher_worker()))
    yield
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


app = FastAPI(title="OrderFlow Pro", version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
async def health():
    return {"status": "ok", "symbols": SYMBOLS, "data_mode": "public Binance Spot market data",
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
        # Initial state for every configured symbol
        for sym in SYMBOLS:
            st = state[sym]
            await ws.send_text(json.dumps({"type": "state", "symbol": sym,
                "connection": st["connection"], "error": st["error"],
                "lastTrade": st["last_trade"], "candles": list(st["candles"])[-500:],
                "book": st["book"].top(50), "footprint": st["footprint"].latest(30),
                "heatmap": list(st["heatmap"]), "history": {
                    "days_requested": HISTORY_DAYS,
                    "orderbook_history": "runtime only; starts when app starts",
                    "footprint_history": "runtime only; starts when trades are received"}}))
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        clients.discard(ws)
