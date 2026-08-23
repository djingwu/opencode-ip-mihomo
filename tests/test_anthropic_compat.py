import ast
import json
import unittest
from pathlib import Path
from typing import Optional


def _load_helpers():
    source = Path(__file__).parents[1].joinpath("server.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"normalize_anthropic_request", "_parse_stream_event"}
    }
    namespace = {"json": json, "Optional": Optional, "ANTHROPIC_MIN_MAX_TOKENS": 128}
    module = ast.Module(body=[wanted["normalize_anthropic_request"], wanted["_parse_stream_event"]], type_ignores=[])
    exec(compile(module, "server.py", "exec"), namespace)
    return namespace


_helpers = _load_helpers()
normalize_anthropic_request = _helpers["normalize_anthropic_request"]
parse_stream_event = _helpers["_parse_stream_event"]


class AnthropicCompatibilityTests(unittest.TestCase):
    def test_plain_string_content_is_normalized_to_text_block(self):
        body = {"model": "hy3-free", "messages": [{"role": "user", "content": "hi"}]}
        normalized = normalize_anthropic_request(body)
        self.assertEqual(normalized["messages"][0]["content"], [{"type": "text", "text": "hi"}])
        self.assertEqual(body["messages"][0]["content"], "hi")

    def test_small_max_tokens_is_raised_for_reasoning_free_models(self):
        normalized = normalize_anthropic_request({"max_tokens": 20, "messages": []})
        self.assertGreaterEqual(normalized["max_tokens"], 128)

    def test_anthropic_delta_object_yields_text(self):
        event_type, payload, delta = parse_stream_event(
            b'data: {"type":"content_block_delta","delta":{"type":"text_delta","text":"Hello"}}'
        )
        self.assertEqual(event_type, "content_block_delta")
        self.assertEqual(payload["type"], "content_block_delta")
        self.assertEqual(delta, "Hello")

    def test_openai_done_sentinel_is_detected_for_compatibility_tail(self):
        self.assertEqual(parse_stream_event(b"data: [DONE]"), ("__done__", None, ""))


if __name__ == "__main__":
    unittest.main()
