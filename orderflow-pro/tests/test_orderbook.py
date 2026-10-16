import unittest
from app.orderbook import OrderBook


class OrderBookTests(unittest.TestCase):
    def test_snapshot_and_update(self):
        b = OrderBook()
        b.load_snapshot({"lastUpdateId": 100, "bids": [["99", "2"]], "asks": [["101", "3"]]})
        self.assertTrue(b.valid)
        self.assertEqual(b.apply_diff({"U": 101, "u": 102, "b": [["99", "0"], ["100", "4"]], "a": []}), "applied")
        self.assertNotIn(99.0, b.bids)
        self.assertEqual(b.bids[100.0], 4.0)
        self.assertEqual(b.last_update_id, 102)

    def test_stale_update_ignored(self):
        b = OrderBook()
        b.load_snapshot({"lastUpdateId": 100, "bids": [["99", "2"]], "asks": [["101", "3"]]})
        self.assertEqual(b.apply_diff({"U": 90, "u": 100, "b": [["99", "9"]], "a": []}), "stale")
        self.assertEqual(b.bids[99.0], 2.0)

    def test_gap_invalidates_book(self):
        b = OrderBook()
        b.load_snapshot({"lastUpdateId": 100, "bids": [["99", "2"]], "asks": [["101", "3"]]})
        self.assertEqual(b.apply_diff({"U": 103, "u": 104, "b": [], "a": []}), "resync")
        self.assertFalse(b.valid)


if __name__ == "__main__":
    unittest.main()
