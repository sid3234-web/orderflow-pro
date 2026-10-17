import unittest
from app.analytics import FootprintAggregator, SignalEngine


class AnalyticsTests(unittest.TestCase):
    def test_aggressor_classification(self):
        fp = FootprintAggregator(interval_ms=60000, tick_size=1)
        fp.add_trade(100.2, 2, True, 60001)
        fp.add_trade(100.4, 3, False, 60002)
        bar = fp.latest(1)[0]
        self.assertEqual(bar["sell"], 2)
        self.assertEqual(bar["buy"], 3)
        self.assertEqual(bar["delta"], 1)
        self.assertEqual(bar["levels"][0]["price"], 100)

    def test_signal_waits_when_book_invalid(self):
        engine = SignalEngine()
        candles = [{"close": 100} for _ in range(30)]
        result = engine.evaluate("BTCUSDT", candles, [], False, now_ms=123)
        self.assertEqual(result["side"], "WAIT")
        self.assertFalse(result["eligible"])

    def test_signal_waits_for_fresh_live_trade(self):
        engine = SignalEngine()
        now = 2_000_000
        candles = [{"close": 100, "high": 101, "low": 99, "source": "live"} for _ in range(25)]
        result = engine.evaluate("BTCUSDT", candles, [], True, now_ms=now,
                                 book_data={"bids": [[99, 10]], "asks": [[101, 10]]},
                                 last_trade_ts=now - 30_000)
        self.assertEqual(result["side"], "WAIT")
        self.assertFalse(result["eligible"])

    def test_signal_needs_confluence_and_reports_sweep(self):
        engine = SignalEngine()
        now = 2_000_000
        candles = []
        for i in range(20):
            close = 100 + i
            candles.append({"open": close - .2, "close": close, "high": 110,
                            "low": 90, "source": "historical"})
        candles.append({"open": 119, "close": 121, "high": 122, "low": 89,
                        "source": "live"})
        fp = [{"buy": 80, "sell": 20, "delta": 60}]
        book = {"bids": [[120, 100], [119, 80]], "asks": [[122, 10], [123, 10]]}
        result = engine.evaluate("BTCUSDT", candles, fp, True, now_ms=now,
                                 book_data=book, last_trade_ts=now - 100)
        self.assertEqual(result["side"], "BUY")
        self.assertTrue(result["eligible"])
        self.assertEqual(result["liquidity_sweep"], "bullish")
        self.assertGreaterEqual(result["confirmations"], 4)


if __name__ == "__main__":
    unittest.main()
