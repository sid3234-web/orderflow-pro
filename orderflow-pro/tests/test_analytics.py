import unittest
from app.analytics import FootprintAggregator, SignalEngine


class AnalyticsTests(unittest.TestCase):
    def test_aggressor_classification(self):
        fp = FootprintAggregator(interval_ms=60000, tick_size=1)
        fp.add_trade(100.2, 2, True, 60001)   # buyer maker -> sell aggressor
        fp.add_trade(100.4, 3, False, 60002)  # buyer taker -> buy aggressor
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


if __name__ == "__main__":
    unittest.main()
