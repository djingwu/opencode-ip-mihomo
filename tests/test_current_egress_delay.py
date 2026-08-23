import sys
import types
import unittest
from unittest.mock import patch

if "curl_cffi" not in sys.modules:
    sys.modules["curl_cffi"] = types.SimpleNamespace(requests=types.SimpleNamespace())

import panel_manager as panel


class CurrentEgressDelayTests(unittest.TestCase):
    def test_proxy_only_environment_measures_active_proxy(self):
        proxy = "socks5h://user:pass@proxy.example:1080"
        with (
            patch.object(panel, "_read_proxy_file", return_value=[proxy]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": False}),
            patch.object(panel.proxy_pool, "select_active_proxy", return_value=proxy),
            patch.object(
                panel,
                "check_proxies",
                return_value={"ok": True, "results": [{"addr": proxy, "ok": True, "latency": 146}]},
            ) as check,
            patch.object(panel, "mihomo_get") as mihomo_get,
        ):
            result = panel.test_current_node_delay()

        self.assertEqual(result, {"ok": True, "egress": "proxy", "node": proxy, "delay": 146})
        check.assert_called_once_with([proxy])
        mihomo_get.assert_not_called()

    def test_mihomo_hold_measures_current_rotator_node(self):
        with (
            patch.object(panel, "_read_proxy_file", return_value=["http://proxy.example:8080"]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": True}),
            patch.object(panel.proxy_pool, "select_active_proxy") as select_proxy,
            patch.object(panel, "mihomo_get", return_value={"now": "subscription1-node-a"}),
            patch.object(panel, "mihomo_test_delay", return_value={"subscription1-node-a": 88}),
            patch.object(panel, "_record_node_health") as record,
        ):
            result = panel.test_current_node_delay()

        self.assertEqual(
            result,
            {"ok": True, "egress": "mihomo", "node": "subscription1-node-a", "delay": 88},
        )
        select_proxy.assert_not_called()
        record.assert_called_once_with({"subscription1-node-a": 88})

    def test_mihomo_controller_false_negative_uses_real_outbound_probe(self):
        with (
            patch.object(panel, "_read_proxy_file", return_value=[]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": True}),
            patch.object(panel, "mihomo_get", side_effect=[{"now": "free-node-a"}, {"now": "free-node-a"}]),
            patch.object(panel, "mihomo_test_delay", return_value={"free-node-a": -1}),
            patch.object(panel, "_mihomo_outbound_delay", return_value=236),
            patch.object(panel, "_record_node_health") as record,
        ):
            result = panel.test_current_node_delay()

        self.assertTrue(result["ok"])
        self.assertEqual(result["node"], "free-node-a")
        self.assertEqual(result["delay"], 236)
        self.assertEqual(result["probe"], "outbound-fallback")
        self.assertIn("实际出站链路", result["note"])
        record.assert_called_once_with({"free-node-a": 236})

    def test_mihomo_controller_and_outbound_probe_both_fail(self):
        with (
            patch.object(panel, "_read_proxy_file", return_value=[]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": True}),
            patch.object(panel, "mihomo_get", side_effect=[{"now": "free-node-a"}, {"now": "free-node-a"}]),
            patch.object(panel, "mihomo_test_delay", return_value={"free-node-a": -1}),
            patch.object(panel, "_mihomo_outbound_delay", return_value=-1),
            patch.object(panel, "_record_node_health") as record,
        ):
            result = panel.test_current_node_delay()

        self.assertFalse(result["ok"])
        self.assertEqual(result["delay"], -1)
        record.assert_called_once_with({"free-node-a": -1})

    def test_stale_mihomo_hold_without_subscription_falls_back_to_proxy(self):
        proxy = "http://proxy.example:8080"
        with (
            patch.object(panel, "_read_proxy_file", return_value=[proxy]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": True}),
            patch.object(panel, "mihomo_get", return_value={"now": "ROTATOR"}),
            patch.object(panel.proxy_pool, "eligible_proxies", return_value=[proxy]),
            patch.object(panel.proxy_pool, "set_active_proxy") as set_active,
            patch.object(
                panel,
                "check_proxies",
                return_value={"ok": True, "results": [{"addr": proxy, "ok": True, "latency": 210}]},
            ),
        ):
            result = panel.test_current_node_delay()

        self.assertTrue(result["ok"])
        self.assertEqual(result["egress"], "proxy")
        self.assertEqual(result["delay"], 210)
        set_active.assert_called_once_with(proxy, reason="current-delay fallback without ROTATOR node")

    def test_no_usable_egress_returns_clear_error(self):
        with (
            patch.object(panel, "_read_proxy_file", return_value=[]),
            patch.object(panel.proxy_pool, "routing_snapshot", return_value={"mode": "mihomo", "hold_mihomo": False}),
            patch.object(panel.proxy_pool, "select_active_proxy", return_value=None),
            patch.object(panel, "mihomo_get", return_value={}),
            patch.object(panel.proxy_pool, "eligible_proxies", return_value=[]),
        ):
            result = panel.test_current_node_delay()

        self.assertFalse(result["ok"])
        self.assertEqual(result["egress"], "none")
        self.assertIn("没有可测速", result["error"])


if __name__ == "__main__":
    unittest.main()
