"""Local order book reconstruction from Binance Spot depth snapshots and diff events."""
from dataclasses import dataclass, field
from typing import Dict, Optional


@dataclass
class OrderBook:
    bids: Dict[float, float] = field(default_factory=dict)
    asks: Dict[float, float] = field(default_factory=dict)
    last_update_id: Optional[int] = None
    valid: bool = False
    reason: str = "waiting for snapshot"

    def load_snapshot(self, snapshot: dict) -> None:
        try:
            last_id = int(snapshot["lastUpdateId"])
            bids = {float(p): float(q) for p, q in snapshot["bids"] if float(q) > 0}
            asks = {float(p): float(q) for p, q in snapshot["asks"] if float(q) > 0}
        except (KeyError, TypeError, ValueError) as exc:
            self.valid = False
            self.reason = f"invalid snapshot: {exc}"
            raise ValueError(self.reason) from exc
        self.bids, self.asks = bids, asks
        self.last_update_id = last_id
        self.valid = True
        self.reason = "snapshot loaded"

    @staticmethod
    def _apply_side(side: Dict[float, float], updates) -> None:
        for price_s, qty_s in updates:
            price, qty = float(price_s), float(qty_s)
            if qty <= 0:
                side.pop(price, None)
            else:
                side[price] = qty

    def apply_diff(self, event: dict) -> str:
        """Return applied, stale, or resync. Requires Binance U/u update IDs."""
        if not self.valid or self.last_update_id is None:
            self.reason = "waiting for valid snapshot"
            return "resync"
        try:
            first_id, final_id = int(event["U"]), int(event["u"])
        except (KeyError, TypeError, ValueError):
            self.valid = False
            self.reason = "depth event missing update IDs"
            return "resync"

        if final_id <= self.last_update_id:
            return "stale"

        # Once synchronized, the next event must bridge last_update_id + 1.
        if first_id > self.last_update_id + 1:
            self.valid = False
            self.reason = f"depth update gap: expected <= {self.last_update_id + 1}, got {first_id}"
            return "resync"

        self._apply_side(self.bids, event.get("b", []))
        self._apply_side(self.asks, event.get("a", []))
        self.last_update_id = final_id

        if self.bids and self.asks and max(self.bids) >= min(self.asks):
            self.valid = False
            self.reason = "crossed/invalid book; resync required"
            return "resync"

        self.reason = "live"
        return "applied"

    def top(self, limit: int = 20) -> dict:
        return {
            "bids": sorted(self.bids.items(), reverse=True)[:limit],
            "asks": sorted(self.asks.items())[:limit],
            "lastUpdateId": self.last_update_id,
            "valid": self.valid,
            "reason": self.reason,
        }
