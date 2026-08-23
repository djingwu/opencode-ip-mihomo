import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

if "curl_cffi" not in sys.modules:
    sys.modules["curl_cffi"] = types.SimpleNamespace(requests=types.SimpleNamespace())

import panel_manager as panel
from mihomo.generate_mihomo_config import build_config


class FreeProviderRecoveryTests(unittest.TestCase):
    def test_empty_rotator_group_uses_free_provider(self):
        cfg = {
            "proxy-providers": {"free": {"type": "file"}},
            "proxy-groups": [{"name": "ROTATOR", "type": "select", "use": []}],
        }
        changed = panel._ensure_rotator_group_sources(cfg)
        self.assertTrue(changed)
        self.assertEqual(cfg["proxy-groups"][0]["use"], ["free"])
        self.assertNotIn("proxies", cfg["proxy-groups"][0])

    def test_empty_config_uses_direct_sentinel(self):
        cfg = {
            "proxy-providers": {},
            "proxy-groups": [{"name": "ROTATOR", "type": "select"}],
        }
        panel._ensure_rotator_group_sources(cfg)
        self.assertEqual(cfg["proxy-groups"][0]["proxies"], ["DIRECT"])
        self.assertNotIn("use", cfg["proxy-groups"][0])

    def test_node_list_falls_back_to_free_yaml_cache_when_controller_down(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "free.yaml"
            cache.write_text(
                "proxies:\n  - name: free-hk-1\n    type: socks5\n    server: example.com\n    port: 443\n",
                encoding="utf-8",
            )
            cfg = {
                "proxy-providers": {
                    "free": {"type": "file", "path": "./providers/free.yaml"}
                }
            }
            state = {"enabled_nodes": [], "groups": {}, "active_group": None, "node_health": {}}
            with (
                patch.object(panel, "load_config", return_value=cfg),
                patch.object(panel, "mihomo_get", return_value=None),
                patch.object(panel, "load_state", return_value=state),
                patch.object(panel, "_resolve_provider_file", return_value=cache),
            ):
                result = panel.list_nodes()

        self.assertEqual([node["name"] for node in result["nodes"]], ["free-hk-1"])
        self.assertEqual(result["nodes"][0]["source"], "cache")
        self.assertEqual(result["cached_node_count"], 1)
        self.assertFalse(result["controller_ready"])

    def test_empty_generator_config_is_parseable(self):
        cfg = build_config([], "select", "secret", "")
        group = cfg["proxy-groups"][0]
        self.assertEqual(group.get("proxies"), ["DIRECT"])
        self.assertNotIn("use", group)

    def test_stale_explicit_rotator_nodes_fall_back_to_provider_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "free.yaml"
            cache.write_text(
                "proxies:\n  - name: free-new\n    type: socks5\n    server: example.com\n    port: 443\n",
                encoding="utf-8",
            )
            cfg = {
                "proxy-providers": {"free": {"type": "file", "path": "./providers/free.yaml"}},
                "proxy-groups": [{"name": "ROTATOR", "type": "select", "proxies": ["free-old"]}],
            }
            with patch.object(panel, "_resolve_provider_file", return_value=cache):
                changed = panel._ensure_rotator_group_sources(cfg)
        self.assertTrue(changed)
        self.assertEqual(cfg["proxy-groups"][0].get("use"), ["free"])
        self.assertNotIn("proxies", cfg["proxy-groups"][0])

    def test_normalize_removes_unroutable_free_cache_nodes(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "free.yaml"
            cache.write_text(
                "proxies:\n"
                "  - name: local-helper\n    type: socks5\n    server: 127.0.0.1\n    port: 1080\n"
                "  - name: public-node\n    type: socks5\n    server: 82.110.41.202\n    port: 6882\n",
                encoding="utf-8",
            )
            cfg = {
                "proxy-providers": {"free": {"type": "file", "path": "./providers/free.yaml"}},
                "proxy-groups": [{"name": "ROTATOR", "type": "select", "use": ["free"]}],
            }
            with (
                patch.object(panel, "load_config", return_value=cfg),
                patch.object(panel, "save_config", return_value=True),
                patch.object(panel, "_resolve_provider_file", return_value=cache),
            ):
                result = panel.normalize_mihomo_config()
            import yaml
            stored = yaml.safe_load(cache.read_text(encoding="utf-8"))

        self.assertTrue(result["changed"])
        self.assertIn("free-cache", result["repaired"])
        self.assertEqual([item["name"] for item in stored["proxies"]], ["public-node"])

    def test_refresh_keeps_free_provider_and_restores_deleted_free_nodes(self):
        cfg = {
            "proxy-providers": {"free": {"type": "file", "path": "./providers/free.yaml"}},
            "proxy-groups": [{"name": "ROTATOR", "type": "select", "use": ["free"]}],
        }
        state = {"excluded_nodes": ["free-node"], "groups": {}, "enabled_nodes": []}
        with (
            patch.object(panel, "load_config", return_value=cfg),
            patch.object(panel, "load_state", return_value=state),
            patch.object(panel, "save_config", return_value=True),
            patch.object(panel, "save_state", return_value=True),
            patch.object(panel, "_provider_cache_node_names", return_value={"free-node"}),
            patch.object(panel, "mihomo_reload", return_value=True),
            patch.object(panel, "mihomo_provider_healthcheck", return_value=True) as healthcheck,
        ):
            result = panel.refresh_subscriptions()
        self.assertTrue(result["ok"])
        self.assertEqual(state["excluded_nodes"], [])
        healthcheck.assert_called_once_with("free")

    def test_explicit_provider_nodes_migrate_to_use_filter_before_mihomo_start(self):
        with tempfile.TemporaryDirectory() as directory:
            cache = Path(directory) / "free.yaml"
            cache.write_text(
                "proxies:\n"
                "  - name: 免费-🏳️ ShatakVPN 112525\n"
                "    type: socks5\n"
                "    server: 82.110.41.202\n"
                "    port: 6882\n",
                encoding="utf-8",
            )
            cfg = {
                "proxy-providers": {
                    "free": {"type": "file", "path": "./providers/free.yaml"}
                },
                "proxy-groups": [
                    {
                        "name": "ROTATOR",
                        "type": "select",
                        "proxies": ["免费-🏳️ ShatakVPN 112525"],
                    }
                ],
            }
            with patch.object(panel, "_resolve_provider_file", return_value=cache):
                changed = panel._ensure_rotator_group_sources(cfg)

        group = cfg["proxy-groups"][0]
        self.assertTrue(changed)
        self.assertEqual(group["use"], ["free"] )
        self.assertNotIn("proxies", group)
        self.assertEqual(
            group["filter"],
            "^(?:免费-🏳️ ShatakVPN 112525)$",
        )

    def test_selection_filter_escapes_regex_characters(self):
        pattern = panel._selection_filter(["HK (01)+fast", "US[2]"])
        self.assertRegex("HK (01)+fast", pattern)
        self.assertRegex("US[2]", pattern)
        self.assertNotRegex("prefix HK (01)+fast", pattern)

    def test_startup_warmup_waits_for_controller_then_checks_free_provider(self):
        cfg = {
            "proxy-providers": {
                "free": {"type": "file", "health-check": {"enable": True}}
            }
        }
        with (
            patch.object(panel, "load_config", return_value=cfg),
            patch.object(panel, "mihomo_get", side_effect=[None, {"providers": {}}, {"providers": {"free": {}}}]),
            patch.object(panel, "mihomo_provider_healthcheck", return_value=True) as check,
            patch.object(panel.time, "sleep"),
        ):
            result = panel.warmup_provider_healthchecks(attempts=3, delay=0.01)

        self.assertTrue(result["ok"])
        self.assertEqual(result["attempts"], 3)
        self.assertEqual(result["triggered"], ["free"] )
        check.assert_called_once_with("free")


if __name__ == "__main__":
    unittest.main()
