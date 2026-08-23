from __future__ import annotations

import sys
import types
import unittest
from concurrent.futures import ThreadPoolExecutor as RealThreadPoolExecutor
from unittest.mock import patch

if "curl_cffi" not in sys.modules:
    sys.modules["curl_cffi"] = types.SimpleNamespace(requests=types.SimpleNamespace())

import panel_manager as panel


class _Response:
    status_code = 200
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


class PanelNodeDelayTests(unittest.TestCase):
    def test_batch_node_probe_does_not_reuse_rotator_group_result(self):
        direct = {"free-node": 123, "subscription-node": 88}
        with (
            patch.object(panel, "_mihomo_parallel_single_delays", return_value=direct) as parallel,
            patch.object(panel, "_mihomo_group_delays", side_effect=AssertionError("group probe must not be used")),
        ):
            result = panel.mihomo_test_delay(
                proxies=["free-node", "subscription-node"],
                group_first=True,
            )

        self.assertEqual(result, direct)
        parallel.assert_called_once_with(["free-node", "subscription-node"])


    def test_parallel_probe_handles_500_nodes_in_bounded_batches(self):
        names = [f"node-{index}" for index in range(500)]
        progress = []

        def executor_factory(*args, **kwargs):
            return RealThreadPoolExecutor(*args, **kwargs)

        with (
            patch.object(panel, "LATENCY_BATCH_SIZE", 40),
            patch.object(panel, "LATENCY_BATCH_PAUSE", 0),
            patch.object(panel, "LATENCY_WORKERS", 8),
            patch.object(panel, "_mihomo_single_delay", return_value=25) as single,
            patch.object(panel, "ThreadPoolExecutor", side_effect=executor_factory) as executors,
        ):
            result = panel._mihomo_parallel_single_delays(
                names,
                progress_callback=lambda completed, total: progress.append((completed, total)),
            )

        self.assertEqual(len(result), 500)
        self.assertTrue(all(delay == 25 for delay in result.values()))
        self.assertEqual(single.call_count, 500)
        self.assertEqual(executors.call_count, 13)
        self.assertEqual(progress[-1], (500, 500))

    def test_extract_group_reuses_live_health_and_only_probes_remaining_nodes(self):
        nodes = [
            {
                "name": f"node-{index}",
                "source": "mihomo",
                "alive": index < 450,
                "status": "ok" if index < 450 else "untested",
                "delay": 20 + index if index < 450 else None,
            }
            for index in range(500)
        ]
        remaining = [f"node-{index}" for index in range(450, 500)]
        measured = {name: 100 for name in remaining}

        with (
            patch.object(panel, "list_nodes", return_value={"nodes": nodes}),
            patch.object(panel, "mihomo_test_delay", return_value=measured) as probe,
            patch.object(panel, "_record_node_health"),
            patch.object(panel, "load_state", return_value={"groups": {}}),
            patch.object(panel, "save_group", return_value={"ok": True, "group": "可用节点组1", "node_count": 500}) as save,
        ):
            result = panel.extract_healthy_group()

        self.assertTrue(result["ok"])
        self.assertEqual(result["measured_count"], 500)
        self.assertEqual(result["cached_healthy_count"], 450)
        self.assertEqual(result["probed_count"], 50)
        self.assertEqual(result["healthy_count"], 500)
        probe.assert_called_once()
        self.assertEqual(probe.call_args.kwargs["proxies"], remaining)
        self.assertFalse(probe.call_args.kwargs["group_first"])
        save.assert_called_once()
        self.assertEqual(len(save.call_args.args[1]), 500)

    def test_single_probe_retries_after_transient_failure(self):
        responses = [
            _Response({"delay": -1}),
            _Response({"delay": -1}),
            _Response({"delay": 246}),
        ]
        with (
            patch.object(panel, "LATENCY_ATTEMPTS", 2),
            patch.object(panel, "LATENCY_RETRY_DELAY", 0),
            patch.object(panel.requests, "get", side_effect=responses, create=True) as request,
        ):
            result = panel._mihomo_single_delay("免费节点 / 香港")

        self.assertEqual(result, 246)
        self.assertEqual(request.call_count, 3)
        first_call = request.call_args_list[0]
        self.assertIn("/proxies/%E5%85%8D%E8%B4%B9%E8%8A%82%E7%82%B9%20%2F%20%E9%A6%99%E6%B8%AF/delay", first_call.args[0])
        self.assertEqual(first_call.kwargs["params"]["timeout"], panel.LATENCY_TIMEOUT_MS)


if __name__ == "__main__":
    unittest.main()

