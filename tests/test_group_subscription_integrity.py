import sys
import types
import unittest
from unittest.mock import patch

# 仅测试状态判定逻辑；开发机未安装 curl_cffi 时提供最小导入桩。
if "curl_cffi" not in sys.modules:
    sys.modules["curl_cffi"] = types.SimpleNamespace(requests=types.SimpleNamespace())

import panel_manager as panel


class GroupSubscriptionIntegrityTests(unittest.TestCase):
    def test_deleted_subscription_only_invalidates_its_group_nodes(self):
        state = {
            "groups": {"mixed": ["A-node", "B-node"]},
            "group_sources": {
                "mixed": {"A-node": ["subscription1"], "B-node": ["subscription2"]}
            },
            "excluded_nodes": [],
        }
        cfg = {"proxy-providers": {"subscription2": {}}}
        live = {"B-node": ["subscription2"]}
        missing = panel._find_missing_group_nodes(
            state["groups"]["mixed"],
            {**state, "_checking_group": "mixed"},
            cfg,
            live,
        )
        self.assertEqual(missing, ["A-node"])

    def test_same_named_node_survives_when_another_subscription_still_provides_it(self):
        state = {
            "groups": {"mixed": ["shared"]},
            "group_sources": {"mixed": {"shared": ["subscription1", "subscription2"]}},
            "_checking_group": "mixed",
        }
        cfg = {"proxy-providers": {"subscription2": {}}}
        live = {"shared": ["subscription2"]}
        self.assertEqual(panel._find_missing_group_nodes(["shared"], state, cfg, live), [])


    def test_legacy_group_without_source_metadata_uses_live_nodes(self):
        state = {
            "groups": {"legacy": ["gone-node"]},
            "_checking_group": "legacy",
        }
        cfg = {"proxy-providers": {"subscription2": {}}}
        live = {"other-node": ["subscription2"]}
        self.assertEqual(
            panel._find_missing_group_nodes(["gone-node"], state, cfg, live),
            ["gone-node"],
        )

    def test_apply_group_returns_deletion_diagnostic_instead_of_empty_group(self):
        state = {
            "groups": {"old": ["old-node"]},
            "group_sources": {"old": {"old-node": ["subscription1"]}},
            "excluded_nodes": [],
        }
        cfg = {"proxy-providers": {}}
        with patch.object(panel, "load_state", return_value=state), \
             patch.object(panel, "refresh_subscriptions", return_value={"ok": True}), \
             patch.object(panel, "load_config", return_value=cfg), \
             patch.object(panel, "mihomo_get", return_value={"providers": {}}):
            result = panel.apply_group("old")
        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "group_nodes_missing")
        self.assertEqual(result["missing_subscriptions"], ["subscription1"])
        self.assertEqual(result["missing_nodes"], ["old-node"])

    def test_batch_delay_parallel_probes_every_node_independently(self):
        with patch.object(panel, "_mihomo_group_delays", side_effect=AssertionError("group probe must not be used")), \
             patch.object(panel, "_mihomo_parallel_single_delays", return_value={"node-a": 42, "node-b": 73}) as parallel:
            result = panel.mihomo_test_delay(["node-a", "node-b"], group_first=True)

        self.assertEqual(result, {"node-a": 42, "node-b": 73})
        parallel.assert_called_once_with(["node-a", "node-b"])

    def test_extract_healthy_group_creates_new_sequential_snapshot(self):
        nodes = {
            "nodes": [
                {"name": "node-a"},
                {"name": "node-b"},
                {"name": "node-c"},
            ]
        }
        with patch.object(panel, "list_nodes", return_value=nodes), \
             patch.object(panel, "mihomo_test_delay", return_value={"node-a": 58, "node-b": -1, "node-c": 121}), \
             patch.object(panel, "_record_node_health") as record, \
             patch.object(panel, "load_state", return_value={"groups": {"可用节点组1": ["old"]}}), \
             patch.object(panel, "save_group", return_value={"ok": True, "group": "可用节点组2", "node_count": 2}) as save:
            result = panel.extract_healthy_group()

        self.assertTrue(result["ok"])
        self.assertEqual(result["group"], "可用节点组2")
        self.assertEqual(result["nodes"], ["node-a", "node-c"])
        self.assertEqual(result["measured_count"], 3)
        self.assertEqual(result["healthy_count"], 2)
        self.assertEqual(result["failed_count"], 1)
        record.assert_called_once_with({"node-a": 58, "node-b": -1, "node-c": 121})
        save.assert_called_once_with("可用节点组2", ["node-a", "node-c"])

    def test_extract_healthy_group_does_not_create_empty_group(self):
        with patch.object(panel, "list_nodes", return_value={"nodes": [{"name": "node-a"}]}), \
             patch.object(panel, "mihomo_test_delay", return_value={"node-a": -1}), \
             patch.object(panel, "_record_node_health") as record, \
             patch.object(panel, "save_group") as save:
            result = panel.extract_healthy_group()

        self.assertFalse(result["ok"])
        self.assertEqual(result["code"], "no_healthy_nodes")
        record.assert_called_once_with({"node-a": -1})
        save.assert_not_called()

    def test_active_group_reports_snapshot_visible_missing_and_excluded_counts(self):
        state = {
            "groups": {"low-latency": ["free-a", "free-b", "free-c"]},
            "active_group": "low-latency",
            "excluded_nodes": ["free-b"],
            "enabled_nodes": ["free-a", "free-b", "free-c"],
            "node_health": {},
        }
        cfg = {
            "proxy-providers": {
                "free": {"type": "file", "path": "./providers/free.yaml"}
            }
        }
        live = {
            "providers": {
                "free": {
                    "proxies": [
                        {"name": "free-a", "type": "Vless", "alive": True, "history": [{"delay": 35}]},
                        {"name": "free-b", "type": "Vless", "alive": False, "history": []},
                    ]
                }
            }
        }
        with patch.object(panel, "load_config", return_value=cfg), \
             patch.object(panel, "load_state", return_value=state), \
             patch.object(panel, "save_state", return_value=True), \
             patch.object(panel, "mihomo_get", return_value=live):
            result = panel.list_nodes()

        self.assertEqual([node["name"] for node in result["nodes"]], ["free-a"])
        self.assertEqual(result["active_group_node_count"], 3)
        self.assertEqual(result["active_group_provider_count"], 2)
        self.assertEqual(result["active_group_visible_count"], 1)
        self.assertEqual(result["active_group_missing_nodes"], ["free-c"])
        self.assertEqual(result["active_group_excluded_nodes"], ["free-b"])



if __name__ == "__main__":
    unittest.main()
