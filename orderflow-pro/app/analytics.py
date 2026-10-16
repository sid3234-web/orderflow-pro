"""Trade aggregation and transparent, deliberately simple signal rules."""
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
    """Aggregates trades into minute bars and price ticks.

    Binance trade event 'm' is isBuyerMaker: true means the buyer was maker,
    so the trade is classified as seller-aggressed; false is buyer-aggressed.
    """
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
            old = self._order.popleft()
            self._bars.pop(old, None)

    def latest(self, limit=30):
        out = []
        for start in list(self._order)[-limit:]:
            b = self._bars[start]
            out.append(FootprintBar(start, start + self.interval_ms, b["levels"],
                                    b["buy"], b["sell"], b["buy"] - b["sell"],
                                    b["trades"]).as_dict())
        return out


class SignalEngine:
    """Small explainable demo rules. No machine learning and no trade execution."""
    def __init__(self):
        self.last_emitted = {}

    def evaluate(self, symbol, candles, footprint, book_valid, now_ms=None):
        if not book_valid:
            return {"side": "WAIT", "score": 50, "reasons": ["Order book invalid or resynchronizing."],
                    "eligible": False, "timestamp": now_ms or int(datetime.now(timezone.utc).timestamp()*1000)}
        if len(candles) < 20:
            return {"side": "WAIT", "score": 50, "reasons": ["Need at least 20 candles for trend context."],
                    "eligible": False, "timestamp": now_ms or int(datetime.now(timezone.utc).timestamp()*1000)}
        closes = [float(c["close"]) for c in candles[-20:]]
        last = closes[-1]
        mean = sum(closes) / len(closes)
        momentum = (last - closes[-5]) / closes[-5] if closes[-5] else 0
        fp = footprint[-1] if footprint else None
        delta = float(fp["delta"]) if fp else 0.0
        # Delta is normalized to the current bar's traded volume.
        total = float(fp["buy"] + fp["sell"]) if fp else 0.0
        delta_ratio = delta / total if total else 0.0
        score = 50
        reasons = []
        if last > mean:
            score += 15; reasons.append("Last close is above the 20-bar mean.")
        else:
            score -= 15; reasons.append("Last close is below the 20-bar mean.")
        if momentum > 0.0005:
            score += 15; reasons.append("Short-term momentum is positive.")
        elif momentum < -0.0005:
            score -= 15; reasons.append("Short-term momentum is negative.")
        else:
            reasons.append("Short-term momentum is neutral.")
        if delta_ratio > 0.10:
            score += 15; reasons.append("Latest footprint shows positive aggressor delta.")
        elif delta_ratio < -0.10:
            score -= 15; reasons.append("Latest footprint shows negative aggressor delta.")
        else:
            reasons.append("Latest footprint delta is mixed or unavailable.")
        score = max(0, min(100, score))
        side = "BUY" if score >= 75 else "SELL" if score <= 25 else "WAIT"
        eligible = side != "WAIT"
        return {"side": side, "score": score, "reasons": reasons, "eligible": eligible,
                "delta_ratio": round(delta_ratio, 4),
                "timestamp": now_ms or int(datetime.now(timezone.utc).timestamp()*1000)}
