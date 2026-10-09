import unittest

from demo import memory, ui


class MemoryTest(unittest.TestCase):
    def test_bounded_stream_stays_flat_and_leaks_are_visible(self):
        ui.say = lambda *a, **k: None  # quiet
        peaks = {name[:1]: peak for name, peak, _ in memory.run(count=600, kb=20)}
        payload = 600 * 20 / 1024
        self.assertGreater(peaks["1"], payload * 0.9)   # list holds everything
        self.assertGreater(peaks["2"], payload * 0.9)   # unbounded queue holds everything
        self.assertLess(peaks["3"], payload * 0.1)      # bounded stream: flat
        self.assertGreater(peaks["4"], payload * 0.9)   # a global brings the leak back


if __name__ == "__main__":
    unittest.main()
