import unittest
from unittest.mock import patch

from free_nodes import (
    COUNTRY_OPTIONS,
    DEFAULT_URL,
    _parse_free_node_text,
    fetch_free_nodes,
    infer_region,
    is_routable_proxy_server,
    select_nodes_by_region,
    summarize_regions,
)


class FreeNodeRegionTests(unittest.TestCase):
    def setUp(self):
        self.nodes = [
            {"name": "免费-🇯🇵 Tokyo", "region": infer_region("🇯🇵 Tokyo")},
            {"name": "免费-🇺🇸 USA", "region": infer_region("🇺🇸 USA")},
            {"name": "免费-🇩🇪 Germany", "region": infer_region("🇩🇪 Germany")},
            {"name": "免费-unknown", "region": infer_region("unclassified")},
        ]

    def test_infers_common_regions(self):
        self.assertEqual(self.nodes[0]["region"], "asia")
        self.assertEqual(self.nodes[1]["region"], "north-america")
        self.assertEqual(self.nodes[2]["region"], "europe")
        self.assertEqual(self.nodes[3]["region"], "unknown")

    def test_auto_region_sampling_keeps_geographic_diversity(self):
        selected = select_nodes_by_region(self.nodes, 3, "auto")
        self.assertEqual({node["region"] for node in selected}, {"asia", "north-america", "europe"})
        self.assertEqual(summarize_regions(selected), {"north-america": 1, "europe": 1, "asia": 1})

    def test_country_options_match_configforge_web_selector(self):
        values = [item["value"] for item in COUNTRY_OPTIONS]
        self.assertIn("hk", values)
        self.assertIn("jp", values)
        self.assertIn("us", values)
        self.assertNotIn("hong-kong", values)

    def test_manual_hong_kong_reads_country_all_file_without_name_filtering(self):
        country_text = "".join(
            f"vless://uuid-{index}@same.example.com:443?security=tls#Node-{index}\n"
            for index in range(60)
        )
        requested_urls = []

        def fake_fetch(url):
            requested_urls.append(url)
            return country_text if "/configs/hk/all.txt" in url else None

        with patch("free_nodes.fetch_text", side_effect=fake_fetch):
            selected = fetch_free_nodes(DEFAULT_URL, 50, "manual", "hk")
        self.assertEqual(len(selected), 50)
        self.assertTrue(any("/configs/hk/all.txt" in url for url in requested_urls))
        self.assertTrue(all(node["region"] == "hk" for node in selected))

    def test_local_and_private_literal_endpoints_are_rejected(self):
        self.assertFalse(is_routable_proxy_server("127.0.0.1"))
        self.assertFalse(is_routable_proxy_server("localhost"))
        self.assertFalse(is_routable_proxy_server("10.0.0.8"))
        self.assertTrue(is_routable_proxy_server("82.110.41.202"))
        self.assertTrue(is_routable_proxy_server("proxy.example.com"))

    def test_parser_drops_local_helper_endpoints(self):
        text = (
            "socks://127.0.0.1:1080#Local-helper\n"
            "vless://uuid@example.com:443?security=tls#Public-node\n"
        )
        nodes = _parse_free_node_text(text)
        self.assertEqual([node["server"] for node in nodes], ["example.com"])

    def test_same_server_port_with_different_credentials_is_not_deduplicated(self):
        text = (
            "vless://uuid-1@same.example.com:443?security=tls#One\n"
            "vless://uuid-2@same.example.com:443?security=tls#Two\n"
        )
        nodes = _parse_free_node_text(text)
        self.assertEqual(len(nodes), 2)

    def test_vless_ws_parameters_are_url_decoded_and_preserved(self):
        text = (
            "vless://uuid@example.com:443?security=tls&type=ws&host=cdn.example.com&"
            "path=%2Fwebsocket%3Ftoken%3Dabc&fp=chrome#Node\n"
        )
        node = _parse_free_node_text(text)[0]
        self.assertEqual(node["ws-opts"]["path"], "/websocket?token=abc")
        self.assertEqual(node["ws-opts"]["headers"]["Host"], "cdn.example.com")
        self.assertEqual(node["client-fingerprint"], "chrome")

    def test_duplicate_names_are_made_unique_for_mihomo(self):
        text = (
            "vless://uuid-1@one.example.com:443?security=tls#Same\n"
            "vless://uuid-2@two.example.com:443?security=tls#Same\n"
        )
        nodes = _parse_free_node_text(text)
        self.assertEqual([node["name"] for node in nodes], ["免费-Same", "免费-Same #2"])


if __name__ == "__main__":
    unittest.main()
