from __future__ import annotations

import threading
import time


class EgressRatePolicy:
    """Keep an egress below the upstream RPM limit without bursty retries.

    The policy deliberately spaces request starts instead of using a token bucket:
    an idle service must not be allowed to send a burst that immediately recreates
    the same 429 condition.  The state is process-local because proxy-server is
    intentionally deployed as a single worker for the upstream's concurrency-one
    contract.
    """

    def __init__(self, requests_per_minute: float, rotate_after_429: int = 2) -> None:
        self.interval = 60.0 / requests_per_minute if requests_per_minute > 0 else 0.0
        self.rotate_after_429 = max(1, int(rotate_after_429))
        self._next_allowed: dict[str, float] = {}
        self._rate_limit_streaks: dict[str, int] = {}
        self._lock = threading.Lock()

    def reserve(self, egress_key: str, now: float | None = None) -> float:
        """Reserve the next request slot and return the required wait seconds."""
        if self.interval <= 0:
            return 0.0
        now = time.monotonic() if now is None else now
        with self._lock:
            next_allowed = self._next_allowed.get(egress_key, now)
            wait_seconds = max(0.0, next_allowed - now)
            self._next_allowed[egress_key] = max(now, next_allowed) + self.interval
            return wait_seconds

    def record_success(self, egress_key: str) -> None:
        with self._lock:
            self._rate_limit_streaks.pop(egress_key, None)

    def record_rate_limit(self, egress_key: str) -> tuple[int, bool]:
        """Return the consecutive-429 count and whether rotation is warranted."""
        with self._lock:
            streak = self._rate_limit_streaks.get(egress_key, 0) + 1
            self._rate_limit_streaks[egress_key] = streak
            return streak, streak >= self.rotate_after_429


def classify_upstream_429(response) -> tuple[str, int | None, object]:
    """Preserve upstream limit semantics; a 429 is not evidence of an IP block."""
    retry_after = response.headers.get("retry-after")
    try:
        retry_seconds = max(0, int(float(retry_after))) if retry_after else None
    except (TypeError, ValueError):
        retry_seconds = None

    try:
        payload = response.json()
    except Exception:
        payload = {"error": {"type": "upstream_rate_limit", "message": response.text}}

    error = payload.get("error", {}) if isinstance(payload, dict) else {}
    error_type = str(error.get("type", "upstream_rate_limit"))
    if error_type in {"FreeUsageLimitError", "GoUsageLimitError", "BlackUsageLimitError"}:
        category = "quota"
    elif error_type == "RateLimitError":
        category = "rate_limit"
    else:
        category = "upstream_rate_limit"
    return category, retry_seconds, payload
