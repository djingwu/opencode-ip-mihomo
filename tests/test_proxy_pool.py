import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import proxy_pool


class ProxyPoolSchedulingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.list_file = root / "proxies.txt"
        self.state_file = root / "proxy_state.json"
        self.egress_file = root / "egress_state.json"
        self.patchers = [
            patch.object(proxy_pool, "PROXY_LIST_FILE", self.list_file),
            patch.object(proxy_pool, "PROXY_STATE_FILE", self.state_file),
            patch.object(proxy_pool, "EGRESS_STATE_FILE", self.egress_file),
        ]
        for patcher in self.patchers:
            patcher.start()

    def tearDown(self):
        for patcher in reversed(self.patchers):
            patcher.stop()
        self.tmp.cleanup()

    def write_proxies(self, values):
        self.list_file.write_text("\n".join(values) + "\n", encoding="utf-8")

    def test_cooling_proxy_is_skipped(self):
        proxies = ["http://a:1", "http://b:2"]
        self.write_proxies(proxies)
        proxy_pool.write_proxy_state({
            proxies[0]: {"status": "fail", "cooldown_until": time.time() + 300},
        })
        self.assertEqual(proxy_pool.select_active_proxy(proxies), proxies[1])

    def test_all_cooling_proxies_fall_back(self):
        proxies = ["http://a:1", "http://b:2"]
        self.write_proxies(proxies)
        future = time.time() + 300
        proxy_pool.write_proxy_state({proxy: {"cooldown_until": future} for proxy in proxies})
        self.assertIsNone(proxy_pool.select_active_proxy(proxies))

    def test_mihomo_hold_prevents_immediate_proxy_override(self):
        proxies = ["http://a:1"]
        self.write_proxies(proxies)
        proxy_pool.set_mihomo_active(reason="rotation", hold=True, mihomo_remaining=2)
        self.assertIsNone(proxy_pool.select_active_proxy(proxies))
        self.assertEqual(proxy_pool.routing_snapshot()["mihomo_remaining"], 2)

    def test_rotation_reports_wrap_after_last_proxy(self):
        proxies = ["http://a:1", "http://b:2"]
        self.write_proxies(proxies)
        self.assertEqual(proxy_pool.next_proxy_rotation(proxies[0], proxies), (proxies[1], False))
        self.assertEqual(proxy_pool.next_proxy_rotation(proxies[1], proxies), (proxies[0], True))

    def test_socks5_list_is_normalized(self):
        self.write_proxies(["socks5://user:pass@example.com:1080"])
        self.assertEqual(proxy_pool.read_proxy_list(), ["socks5h://user:pass@example.com:1080"])


if __name__ == "__main__":
    unittest.main()
