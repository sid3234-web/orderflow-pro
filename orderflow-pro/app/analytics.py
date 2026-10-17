"""Trade aggregation and a transparent multi-confirmation signal engine."""
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Deque


@dataclass
class FootprintBar:
    start_ms: int
    end_ms: int
    levels: dict
    buy_volume: float
    sell_volume: float
    delta: float
    trade_count: int

    def as_dict(self):
        return {
            "start": self.start_ms, "end": self.end_ms,
            "levels": [{"price": p, "buy": v["buy"], "sell": v["sell"]}
                       for p, v in sorted(self.levels.items())],
            "buy": self.buy_volume, "sell": self.sell_volume,
            "delta": self.delta, "trades": self.trade_count,
        }


class FootprintAggregator:
    """Aggregate live aggressor trades into minute bars and price levels."""
    def __init__(self, interval_ms=60_000, tick_size=1.0, max_bars=1200):
        self.interval_ms = interval_ms
        self.tick_size = tick_size
        self.max_bars = max_bars
        self._bars = {}
        self._order: Deque[int] = deque()

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, timestamp_ms: int):
        start = (int(timestamp_ms) // self.interval_ms) * self.interval_ms
        if start not in self._bars:
            self._bars[start] = {"levels": defaultdict(lambda: {"buy": 0.0, "sell": 0.0}),
                                 "buy": 0.0, "sell": 0.0, "trades": 0}
            self._order.append(start)
        bar = self._bars[start]
        tick_price = round(round(float(price) / self.tick_size) * self.tick_size, 10)
        side = "sell" if buyer_is_maker else "buy"
        bar["levels"][tick_price][side] += float(qty)
        bar[side] += float(qty)
        bar["trades"] += 1
        while len(self._order) > self.max_bars:
            self._bars.pop(self._order.popleft(), None)

    def latest(self, limit=30):
        out = []
        for start in list(self._order)[-limit:]:
            b = self._bars[start]
            out.append(FootprintBar(start, start + self.interval_ms, b["levels"],
                                    b["buy"], b["sell"], b["buy"] - b["sell"],
                                    b["trades"]).as_dict())
        return out


class SignalEngine:
    """Explainable confluence rules. Never submits or manages orders."""
    def __init__(self):
        self.last_emitted = {}

    @staticmethod
    def _wait(reason, now_ms):
        return {"side": "WAIT", "score": 50, "reasons": [reason],
                "eligible": False, "confirmations": 0,
                "timestamp": now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)}

    def evaluate(self, symbol, candles, footprint, book_valid, now_ms=None,
                 book_data=None, last_trade_ts=None, strict=True):
        now = now_ms or int(datetime.now(timezone.utc).timestamp() * 1000)
        if not book_valid:
            return self._wait("Order book invalid or resynchronizing; signals blocked.", now)
        if len(candles) < 21:
            return self._wait("Need at least 21 candles to evaluate trend and liquidity sweeps.", now)
        if not last_trade_ts or now - int(last_trade_ts) > 15_000:
            return self._wait("Live trade is stale or missing; waiting for fresh exchange data.", now)
        if candles[-1].get("source") != "live":
            return self._wait("Waiting for a live exchange candle; historical candles alone cannot trigger alerts.", now)

        recent = candles[-21:]
        closes = [float(c["close"]) for c in recent]
        last = closes[-1]
        prior = recent[:-1]
        prior_high = max(float(c["high"]) for c in prior if "high" in c)
        prior_low = min(float(c["low"]) for c in prior if "low" in c)
        last_candle = recent[-1]
        bullish_sweep = float(last_candle.get("low", last)) < prior_low and last > prior_low
        bearish_sweep = float(last_candle.get("high", last)) > prior_high and last < prior_high

        mean = sum(closes[-20:]) / 20
        momentum = (last - closes[-5]) / closes[-5] if closes[-5] else 0.0
        fp = footprint[-1] if footprint else None
        delta = float(fp["delta"]) if fp else 0.0
        total = float(fp["buy"] + fp["sell"]) if fp else 0.0
        delta_ratio = delta / total if total else 0.0

        bids = (book_data or {}).get("bids", [])
        asks = (book_data or {}).get("asks", [])
        bid_qty = sum(float(level[1]) for level in bids[:20])
        ask_qty = sum(float(level[1]) for level in asks[:20])
        denom = bid_qty + ask_qty
        book_imbalance = (bid_qty - ask_qty) / denom if denom else 0.0

        long_points, short_points = 0, 0
        reasons = []
        if last > mean:
            long_points += 1; reasons.append("Trend: price is above the 20-bar mean.")
        elif last < mean:
            short_points += 1; reasons.append("Trend: price is below the 20-bar mean.")
        else:
            reasons.append("Trend: price is at the 20-bar mean.")
        if momentum > 0.0005:
            long_points += 1; reasons.append("Momentum: positive over the recent bars.")
        elif momentum < -0.0005:
            short_points += 1; reasons.append("Momentum: negative over the recent bars.")
        else:
            reasons.append("Momentum: neutral; no directional confirmation.")
        if delta_ratio > 0.10:
            long_points += 1; reasons.append("Footprint: buyers dominate aggressor volume.")
        elif delta_ratio < -0.10:
            short_points += 1; reasons.append("Footprint: sellers dominate aggressor volume.")
        else:
            reasons.append("Footprint: delta is mixed or too small.")
        if book_imbalance > 0.15:
            long_points += 1; reasons.append("Order book: bid depth outweighs ask depth.")
        elif book_imbalance < -0.15:
            short_points += 1; reasons.append("Order book: ask depth outweighs bid depth.")
        else:
            reasons.append("Order book: depth imbalance is neutral.")
        if bullish_sweep:
            long_points += 2; reasons.append("Liquidity: candle swept a prior low and reclaimed it.")
        if bearish_sweep:
            short_points += 2; reasons.append("Liquidity: candle swept a prior high and rejected below it.")
        if not bullish_sweep and not bearish_sweep:
            reasons.append("Liquidity: no confirmed sweep-and-reclaim/rejection on the latest candle.")

        direction = "BUY" if long_points > short_points else "SELL" if short_points > long_points else "WAIT"
        winning = max(long_points, short_points)
        losing = min(long_points, short_points)
        score = max(0, min(100, 50 + (winning - losing) * 10))
        confirmations = winning
        minimum = 4 if strict else 3
        eligible = direction != "WAIT" and confirmations >= minimum and winning - losing >= 2
        if not eligible:
            reasons.append(f"WAIT: need {minimum} aligned confirmations and a clear directional edge (currently {confirmations}).")
            direction = "WAIT"
        return {
            "side": direction, "score": score, "reasons": reasons, "eligible": eligible,
            "confirmations": confirmations, "delta_ratio": round(delta_ratio, 4),
            "book_imbalance": round(book_imbalance, 4),
            "liquidity_sweep": "bullish" if bullish_sweep else "bearish" if bearish_sweep else None,
            "entry_reference": last, "timestamp": now,
        }
