"""Shared custom proxy-pool scheduling state.

The proxy panel, proxy-server and rotator run in separate processes/containers.
This module keeps their view of proxy health and the currently selected egress
in the shared data directory consistent.
"""
from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from proxy_utils import normalize_proxy_url

PROXY_LIST_FILE = Path(os.environ.get("PROXY_LIST_FILE", "/app/data/proxies.txt"))
PROXY_STATE_FILE = Path(os.environ.get("PROXY_STATE_FILE", "/app/data/proxy_state.json"))
EGRESS_STATE_FILE = Path(os.environ.get("EGRESS_STATE_FILE", "/app/data/egress_state.json"))
PROXY_COOLDOWN_BASE = int(os.environ.get("PROXY_COOLDOWN_BASE", "60"))
PROXY_COOLDOWN_FACTOR = int(os.environ.get("PROXY_COOLDOWN_FACTOR", "30"))
PROXY_LATENCY_THRESHOLD_MS = int(os.environ.get("PROXY_LATENCY_THRESHOLD_MS", "300"))
# 选路文件读缓存（秒）：proxies.txt/proxy_state.json/egress_state.json 按 mtime 失效。
# 三个进程共享 ./data 卷，但这些文件由面板/轮换低频写，热路径读缓存 1s 足够；
# 写路径每次清缓存，保证切换出口后最多 1s 可见。
PROXY_FILE_CACHE_TTL = max(0.2, float(os.environ.get("PROXY_FILE_CACHE_TTL", "1.0")))
_file_cache: Dict[str, tuple] = {}
_file_cache_lock = threading.Lock()


def _cached_file(path: Path):
    now = time.monotonic()
    key = str(path)
    with _file_cache_lock:
        entry = _file_cache.get(key)
        if entry is not None and (now - entry[0]) < PROXY_FILE_CACHE_TTL:
            return entry[2], True
    try:
        mtime = path.stat().st_mtime if path.exists() else -1.0
    except Exception:
        mtime = -1.0
    with _file_cache_lock:
        entry = _file_cache.get(key)
        if entry is not None and entry[1] == mtime and (now - entry[0]) < PROXY_FILE_CACHE_TTL * 10:
            # 文件未变：缓存值依然有效，刷新时间戳避免每个请求都 stat
            _file_cache[key] = (now, entry[1], entry[2])
            return entry[2], True
    return (mtime, now, None), False


def _store_file_cache(path: Path, meta, value) -> None:
    with _file_cache_lock:
        _file_cache[str(path)] = (meta[1], meta[0], value)


def _invalidate_file_cache(path: Path) -> None:
    with _file_cache_lock:
        _file_cache.pop(str(path), None)


def read_proxy_list() -> List[str]:
    cached, hit = _cached_file(PROXY_LIST_FILE)
    if hit:
        return list(cached) if isinstance(cached, list) else []
    meta = cached
    if not PROXY_LIST_FILE.exists():
        _store_file_cache(PROXY_LIST_FILE, meta, [])
        return []
    try:
        with PROXY_LIST_FILE.open("r", encoding="utf-8") as handle:
            values = [normalize_proxy_url(line) for line in handle if line.strip() and not line.lstrip().startswith("#")]
        result = list(dict.fromkeys(value for value in values if value))
        _store_file_cache(PROXY_LIST_FILE, meta, result)
        return list(result)
    except Exception:
        return []


def _read_json(path: Path, default: Any) -> Any:
    cached, hit = _cached_file(path)
    if hit:
        return cached if isinstance(cached, (dict, list)) else default
    meta = cached
    try:
        if path.exists():
            with path.open("r", encoding="utf-8") as handle:
                value = json.load(handle)
            _store_file_cache(path, meta, value)
            return value
    except Exception:
        pass
    return default


def _write_json(path: Path, value: Any) -> bool:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        temp = path.with_suffix(path.suffix + ".tmp")
        with temp.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2)
        temp.replace(path)
        _invalidate_file_cache(path)
        return True
    except Exception:
        return False


def read_proxy_state() -> Dict[str, Any]:
    value = _read_json(PROXY_STATE_FILE, {})
    return value if isinstance(value, dict) else {}


def write_proxy_state(value: Dict[str, Any]) -> bool:
    return _write_json(PROXY_STATE_FILE, value)


def read_egress_state() -> Dict[str, Any]:
    value = _read_json(EGRESS_STATE_FILE, {})
    return value if isinstance(value, dict) else {}


def write_egress_state(value: Dict[str, Any]) -> bool:
    return _write_json(EGRESS_STATE_FILE, value)


def _cooldown_until(state: Dict[str, Any], proxy: str) -> float:
    value = state.get(proxy, {})
    if not isinstance(value, dict):
        return 0.0
    try:
        return float(value.get("cooldown_until") or 0)
    except (TypeError, ValueError):
        return 0.0


def is_proxy_eligible(proxy: str, state: Optional[Dict[str, Any]] = None, now: Optional[float] = None) -> bool:
    state = state if state is not None else read_proxy_state()
    now = time.time() if now is None else now
    return _cooldown_until(state, proxy) <= now


def eligible_proxies(proxies: Optional[Iterable[str]] = None) -> List[str]:
    values = list(proxies) if proxies is not None else read_proxy_list()
    state = read_proxy_state()
    now = time.time()
    return [proxy for proxy in values if is_proxy_eligible(proxy, state, now)]


def active_proxy() -> Optional[str]:
    state = read_egress_state()
    value = state.get("active_proxy")
    return normalize_proxy_url(value) if value else None


def routing_snapshot() -> Dict[str, Any]:
    state = read_egress_state()
    return {
        "mode": state.get("mode", "mihomo"),
        "active_proxy": active_proxy(),
        "updated_at": state.get("updated_at", 0),
        "hold_mihomo": state.get("hold_mihomo", False),
        "mihomo_remaining": int(state.get("mihomo_remaining") or 0),
    }


def set_active_proxy(proxy: str, *, verified_ip: Optional[str] = None, reason: str = "") -> bool:
    state = read_egress_state()
    state.update({
        "mode": "proxy",
        "active_proxy": normalize_proxy_url(proxy),
        "hold_mihomo": False,
        "mihomo_remaining": 0,
        "updated_at": int(time.time()),
    })
    if verified_ip:
        state["verified_ip"] = verified_ip
    if reason:
        state["reason"] = reason
    return write_egress_state(state)


def set_mihomo_active(
    *,
    verified_ip: Optional[str] = None,
    reason: str = "",
    hold: bool = True,
    mihomo_remaining: int = 0,
) -> bool:
    state = read_egress_state()
    state.update({
        "mode": "mihomo",
        "active_proxy": None,
        "hold_mihomo": bool(hold),
        "mihomo_remaining": max(0, int(mihomo_remaining)),
        "updated_at": int(time.time()),
    })
    if verified_ip:
        state["verified_ip"] = verified_ip
    if reason:
        state["reason"] = reason
    return write_egress_state(state)


def _proxy_latency_ok(entry: Dict[str, Any]) -> bool:
    """Return True if the proxy's latency is within acceptable range."""
    if not isinstance(entry, dict):
        return True
    try:
        latency = entry.get("latency")
        if latency is None:
            return True  # No latency data yet, allow it
        return float(latency) <= PROXY_LATENCY_THRESHOLD_MS
    except (TypeError, ValueError):
        return True


def select_active_proxy(proxies: Optional[Iterable[str]] = None) -> Optional[str]:
    values = list(proxies) if proxies is not None else read_proxy_list()
    values = [normalize_proxy_url(value) for value in values if value]
    values = list(dict.fromkeys(values))
    if not values:
        return None
    state = read_proxy_state()
    now = time.time()
    available = [value for value in values if is_proxy_eligible(value, state, now)]
    # Filter out high-latency proxies (keep low-latency ones for better performance)
    if available:
        low_latency = [p for p in available if _proxy_latency_ok(state.get(p, {}))]
        if low_latency:
            available = low_latency
    if not available:
        return None
    route = read_egress_state()
    if route.get("mode") == "mihomo" and route.get("hold_mihomo") is True:
        return None
    current = active_proxy()
    if current in available:
        return current
    # A stale/failed active proxy is replaced with the first available one.
    set_active_proxy(available[0], reason="automatic healthy-proxy selection")
    return available[0]


def next_proxy_after(current: Optional[str], proxies: Optional[Iterable[str]] = None) -> Optional[str]:
    values = list(proxies) if proxies is not None else read_proxy_list()
    values = list(dict.fromkeys(normalize_proxy_url(value) for value in values if value))
    if not values:
        return None
    available = eligible_proxies(values)
    if not available:
        return None
    if current in values:
        start = values.index(current)
        ordered = values[start + 1:] + values[:start]
    else:
        ordered = values
    for value in ordered:
        if value in available:
            return value
    return None


def next_proxy_rotation(
    current: Optional[str],
    proxies: Optional[Iterable[str]] = None,
) -> tuple[Optional[str], bool]:
    """Return the next eligible proxy and whether selecting it wraps the pool."""
    values = list(proxies) if proxies is not None else read_proxy_list()
    values = list(dict.fromkeys(normalize_proxy_url(value) for value in values if value))
    available = eligible_proxies(values)
    if not available:
        return None, False
    if current not in values:
        return available[0], False
    current_index = values.index(current)
    for index in range(current_index + 1, len(values)):
        if values[index] in available:
            return values[index], False
    for index in range(0, current_index + 1):
        if values[index] in available:
            return values[index], True
    return None, False


def mark_proxy_success(proxy: Optional[str]) -> None:
    if not proxy:
        return
    proxy = normalize_proxy_url(proxy)
    state = read_proxy_state()
    entry = state.get(proxy)
    # 已是干净状态就跳过写盘：原来每次成功请求都全量重写 proxy_state.json
    if isinstance(entry, dict) and entry.get("status") == "ok" and not entry.get("fail_count") and "cooldown_until" not in entry:
        return
    entry = state.setdefault(proxy, {})
    if not isinstance(entry, dict):
        entry = {}
        state[proxy] = entry
    entry.update({"status": "ok", "fail_count": 0})
    entry.pop("cooldown_until", None)
    write_proxy_state(state)


def mark_proxy_failure(proxy: Optional[str]) -> int:
    if not proxy:
        return 0
    proxy = normalize_proxy_url(proxy)
    state = read_proxy_state()
    entry = state.setdefault(proxy, {})
    if not isinstance(entry, dict):
        entry = {}
        state[proxy] = entry
    count = int(entry.get("fail_count") or 0) + 1
    entry.update({
        "status": "fail",
        "latency": None,
        "fail_count": count,
        "cooldown_until": time.time() + PROXY_COOLDOWN_BASE + count * PROXY_COOLDOWN_FACTOR,
    })
    write_proxy_state(state)
    return count


def proxy_from_mapping(proxies: Optional[Dict[str, str]]) -> Optional[str]:
    if not proxies:
        return None
    value = proxies.get("https") or proxies.get("http")
    if not value:
        return None
    value = normalize_proxy_url(value)
    return value if value in read_proxy_list() else None

def clear_active_proxy_if_matches(proxy: str) -> bool:
    proxy = normalize_proxy_url(proxy)
    state = read_egress_state()
    if normalize_proxy_url(state.get("active_proxy") or "") != proxy:
        return False
    state.update({
        "mode": "proxy",
        "active_proxy": None,
        "hold_mihomo": False,
        "mihomo_remaining": 0,
        "updated_at": int(time.time()),
    })
    return write_egress_state(state)


def egress_status(proxies: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    values = list(proxies) if proxies is not None else read_proxy_list()
    values = list(dict.fromkeys(normalize_proxy_url(value) for value in values if value))
    available = eligible_proxies(values)
    route = routing_snapshot()
    return {
        **route,
        "total": len(values),
        "eligible_count": len(available),
        "fallback_to_mihomo": len(available) == 0,
    }
