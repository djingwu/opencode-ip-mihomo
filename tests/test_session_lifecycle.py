import ast
import threading
import unittest
from pathlib import Path


class _FakeSession:
    def __init__(self):
        self.closed = False
        self.close_calls = 0

    def close(self):
        self.closed = True
        self.close_calls += 1


def _load_session_helpers():
    source = Path(__file__).parents[1].joinpath("server.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    wanted = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_invalidate_session", "_reset_request_session"}
    }
    namespace = {
        "_session_pool": {},
        "_session_pool_lock": threading.Lock(),
    }
    module = ast.Module(body=[wanted["_invalidate_session"], wanted["_reset_request_session"]], type_ignores=[])
    exec(compile(module, "server.py", "exec"), namespace)
    return namespace["_reset_request_session"], namespace["_session_pool"]


_reset_request_session, _session_pool = _load_session_helpers()


class SessionLifecycleTests(unittest.TestCase):
    def setUp(self):
        _session_pool.clear()

    def test_invalidating_pooled_session_removes_closed_object(self):
        session = _FakeSession()
        _session_pool["chat"] = session
        _reset_request_session("chat", session, pooled=True)
        self.assertNotIn("chat", _session_pool)
        self.assertTrue(session.closed)
        self.assertEqual(session.close_calls, 1)

    def test_expected_session_does_not_evict_newer_pooled_session(self):
        old_session = _FakeSession()
        new_session = _FakeSession()
        _session_pool["chat"] = new_session
        _reset_request_session("chat", old_session, pooled=True)
        self.assertIs(_session_pool["chat"], new_session)
        self.assertFalse(new_session.closed)
        self.assertFalse(old_session.closed)

    def test_non_pooled_session_is_closed_for_retry(self):
        session = _FakeSession()
        _reset_request_session("chat", session, pooled=False)
        self.assertTrue(session.closed)
        self.assertEqual(session.close_calls, 1)


if __name__ == "__main__":
    unittest.main()
