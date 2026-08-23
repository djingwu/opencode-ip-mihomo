import sys
import types
import unittest
from unittest.mock import patch

if "curl_cffi" not in sys.modules:
    sys.modules["curl_cffi"] = types.SimpleNamespace(requests=types.SimpleNamespace())

import panel_manager as panel
from proxy_utils import normalize_proxy_url


class ProxyUrlNormalizationTests(unittest.TestCase):
    def test_socks5_uses_remote_dns(self):
        self.assertEqual(
            normalize_proxy_url("socks5://user:pass@proxy.example:1080"),
            "socks5h://user:pass@proxy.example:1080",
        )

    def test_existing_socks5h_is_unchanged(self):
        self.assertEqual(
            normalize_proxy_url("socks5h://user:pass@proxy.example:1080"),
            "socks5h://user:pass@proxy.example:1080",
        )

    def test_http_proxy_is_unchanged(self):
        self.assertEqual(
            normalize_proxy_url("http://proxy.example:8080"),
            "http://proxy.example:8080",
        )

    def test_whitespace_is_removed(self):
        self.assertEqual(
            normalize_proxy_url("  socks5://127.0.0.1:1080  "),
            "socks5h://127.0.0.1:1080",
        )

    def test_panel_health_check_uses_remote_dns(self):
        response = types.SimpleNamespace(
            status_code=204,
            elapsed=types.SimpleNamespace(total_seconds=lambda: 0.123),
        )
        with patch.object(panel.requests, "get", return_value=response, create=True) as mocked_get:
            result = panel._test_one_proxy("socks5://user:pass@proxy.example:1080")
        self.assertTrue(result["ok"])
        self.assertEqual(result["latency"], 123)
        self.assertEqual(
            mocked_get.call_args.kwargs["proxies"],
            {
                "http": "socks5h://user:pass@proxy.example:1080",
                "https": "socks5h://user:pass@proxy.example:1080",
            },
        )


if __name__ == "__main__":
    unittest.main()

